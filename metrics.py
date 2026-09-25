#!/usr/bin/env python3
"""
Метрики расхода радара: память, процессор, аптайм, нагрузка и вердикт «A или B».

Зачем: пользователь хочет сначала попробовать схему A (постоянный слушатель) и понять,
сколько она жрёт, а уже потом решать, переходить ли на схему B (проходы по расписанию).
Поэтому здесь — измерения, а не обещания.

Принципы (ТЗ §15.2):
  * никаких обязательных внешних зависимостей: psutil опционален, без него часть полей
    получается из /proc (Linux) или остаётся «н/д» (Windows без psutil);
  * все функции детерминированы при явной подстановке значений — это нужно офлайн-тестам
    (selftest_panel.py, ТЗ §15.5);
  * вердикт считается по доступным данным: если памяти не видно, решение принимается
    по процессору и ошибкам, а отсутствие данных честно помечается.

Пороги вердикта (ТЗ §15.3) вынесены в константы — их можно подстроить под свой контейнер.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path

# -----------------------------------------------------------------------------
# Опциональный psutil: панель и метрики обязаны работать и без него
# -----------------------------------------------------------------------------

try:                                        # pragma: no cover - зависит от окружения
    import psutil                           # type: ignore
except Exception:                           # noqa: BLE001 - psutil может быть битым/неполным
    psutil = None                           # type: ignore

PSUTIL_AVAILABLE = psutil is not None

# Момент старта процесса: считаем от импорта модуля (радар запускается сразу после него).
STARTED_MONOTONIC = time.monotonic()
STARTED_PROCESS_TIME = time.process_time()

# -----------------------------------------------------------------------------
# Пороги вердикта «A или B» (ТЗ §15.3)
# -----------------------------------------------------------------------------

RSS_OK_MB = 150.0          # ≤ — «A подходит»
RSS_WARN_MB = 250.0        # > — «рекомендую B»
CPU_OK_PERCENT = 5.0       # ≤ — «A подходит»
CPU_WARN_PERCENT = 15.0    # > — «рекомендую B»
FLOOD_WARN_PER_DAY = 3     # столько FloodWait за сутки уже считается «часто»

VERDICT_A = "A подходит: ресурсов мало"
VERDICT_A_WATCH = "A возможна, но следи: близко к лимитам lite-контейнера"
VERDICT_B = "рекомендую B (проходы по расписанию)"
VERDICT_UNKNOWN = "н/д: нет данных о ресурсах (поставь psutil или запусти на Linux)"

# Заголовок metrics.csv (ТЗ §15.2) — порядок колонок фиксирован, его проверяют тесты.
METRICS_CSV_HEADER = ("ts,rss_mb,cpu_percent,uptime_h,msgs_total,msgs_last_hour,"
                      "api_calls,db_mb,forwarded_today,accounts,mode")

METRICS_COLUMNS = ("ts", "rss_mb", "cpu_percent", "uptime_s", "msgs_total", "msgs_last_hour",
                   "api_calls", "db_mb", "hits_total", "forwarded_today", "accounts", "mode")


# -----------------------------------------------------------------------------
# Время и размеры
# -----------------------------------------------------------------------------

def uptime_seconds(started_monotonic: float | None = None) -> float:
    """Сколько секунд живёт процесс (от старта радара)."""
    base = STARTED_MONOTONIC if started_monotonic is None else started_monotonic
    return max(0.0, time.monotonic() - base)


def format_duration(seconds: float | None, short: bool = False) -> str:
    """Человекочитаемая длительность: 6 ч 12 мин / 26 ч 40 мин / 45 с."""
    if seconds is None:
        return "н/д"
    seconds = max(0, int(seconds))
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, secs = divmod(rest, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days} д" if short else f"{days} дн")
    if hours or days:
        parts.append(f"{hours} ч")
    if minutes or hours or days:
        parts.append(f"{minutes} мин")
    if not parts:
        parts.append(f"{secs} с")
    return " ".join(parts)


def format_number(value: float | int | None) -> str:
    """Число с неразрывными пробелами-разделителями: 41208 -> «41 208»."""
    if value is None:
        return "н/д"
    if isinstance(value, float):
        text = f"{value:,.1f}"
    else:
        text = f"{int(value):,}"
    return text.replace(",", " ")


def rss_mb(process: object | None = None) -> float | None:
    """Текущий RSS процесса в МБ.

    Порядок источников (ТЗ §15.1):
      1) psutil — работает везде;
      2) /proc/self/status (VmRSS) — Linux без psutil;
      3) None — «н/д» (Windows без psutil).
    """
    if process is not None:                 # подстановка значения в тестах
        return float(process)
    if PSUTIL_AVAILABLE:
        try:                                # pragma: no cover - зависит от окружения
            return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
        except Exception:                   # noqa: BLE001
            pass
    return _rss_mb_from_proc()


def _rss_mb_from_proc(pid: int | None = None) -> float | None:
    """VmRSS из /proc/self/status — способ без зависимостей на Linux/контейнере."""
    path = Path("/proc") / (str(pid) if pid else "self") / "status"
    try:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("VmRSS:"):
                kilo = float(line.split()[1])
                return kilo / 1024.0
    except (OSError, ValueError, IndexError):
        return None
    return None


def peak_rss_mb() -> float | None:
    """Пиковый RSS за прогон: нужен, чтобы выбрать класс контейнера (lite = 256 МБ)."""
    if PSUTIL_AVAILABLE:
        try:                                # pragma: no cover - зависит от окружения
            proc = psutil.Process(os.getpid())
            peak = getattr(proc.memory_info(), "peak", None)
            if peak:
                return peak / (1024 * 1024)
        except Exception:                   # noqa: BLE001
            pass
    try:
        import resource                     # stdlib: UNIX-пик памяти в КБ
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    except Exception:                       # noqa: BLE001 - Windows: модуля нет
        return None


def cpu_percent(interval: float | None = None) -> float | None:
    """Мгновенная загрузка CPU процессом (psutil). Без psutil — None («н/д»)."""
    if not PSUTIL_AVAILABLE:
        return None
    try:                                    # pragma: no cover - зависит от окружения
        return psutil.Process(os.getpid()).cpu_percent(interval=interval)
    except Exception:                       # noqa: BLE001
        return None


def cpu_percent_average(process_time: float | None = None, uptime_s: float | None = None,
                        started_process_time: float | None = None) -> float | None:
    """Средний CPU с момента старта: process_time / uptime * 100 (одно ядро = 100 %).

    Работает без psutil — поэтому средний процент доступен всегда, даже в лёгком контейнере.
    Возвращает None, если аптайм нулевой (делить нельзя).
    """
    if process_time is None:
        base = STARTED_PROCESS_TIME if started_process_time is None else started_process_time
        process_time = max(0.0, time.process_time() - base)
    if uptime_s is None:
        uptime_s = uptime_seconds()
    if not uptime_s or uptime_s <= 0:
        return None
    return round(process_time / uptime_s * 100.0, 2)


def db_size_mb(path: str | Path) -> tuple[float, float]:
    """Размер базы и её WAL-журнала в МБ: (основной файл, -wal + -shm)."""
    main = Path(path)
    total = main.stat().st_size if main.exists() else 0
    journal = 0
    for suffix in ("-wal", "-shm"):
        side = Path(str(main) + suffix)
        if side.exists():
            journal += side.stat().st_size
    return round(total / (1024 * 1024), 2), round(journal / (1024 * 1024), 2)


def project_size_mb(root: str | Path = ".", skip: tuple[str, ...] = (".git", "__pycache__")) -> float:
    """Сколько места занимает папка проекта (для оценки диска контейнера)."""
    total = 0
    for current, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d not in skip]
        for name in files:
            try:
                total += (Path(current) / name).stat().st_size
            except OSError:
                continue
    return round(total / (1024 * 1024), 2)


# -----------------------------------------------------------------------------
# Срез метрик и вердикт
# -----------------------------------------------------------------------------

def sample(*, ts: str | None = None, rss: float | None = None, peak_rss: float | None = None,
           cpu: float | None = None, uptime_s: float | None = None,
           msgs_total: int = 0, msgs_last_hour: int = 0, api_calls: int = 0,
           db_path: str | Path | None = None, db_mb: float | None = None,
           hits_total: int = 0, forwarded_today: int = 0, accounts: int = 1,
           mode: str = "A", measure_rss: bool = True) -> dict:
    """Строка среза метрик (ТЗ §7.2, таблица metrics).

    Всё, что можно, берётся из аргументов: тесты подставляют фейковые значения и получают
    детерминированный результат (ТЗ §15.5, п.1). measure_rss=False запрещает трогать
    psutil/proc — тоже для тестов.
    """
    if ts is None:
        ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if uptime_s is None:
        uptime_s = round(uptime_seconds(), 1)
    if measure_rss:
        if rss is None:
            rss = rss_mb()
        if peak_rss is None:
            peak_rss = peak_rss_mb()
        if cpu is None:
            cpu = cpu_percent_average(uptime_s=uptime_s)
    if db_mb is None and db_path:
        db_mb, _wal = db_size_mb(db_path)
    return {
        "ts": ts,
        "rss_mb": None if rss is None else round(float(rss), 2),
        "rss_peak_mb": None if peak_rss is None else round(float(peak_rss), 2),
        "cpu_percent": None if cpu is None else round(float(cpu), 2),
        "uptime_s": round(float(uptime_s), 1),
        "msgs_total": int(msgs_total),
        "msgs_last_hour": int(msgs_last_hour),
        "api_calls": int(api_calls),
        "db_mb": None if db_mb is None else round(float(db_mb), 2),
        "hits_total": int(hits_total),
        "forwarded_today": int(forwarded_today),
        "accounts": int(accounts),
        "mode": mode or "A",
    }


def verdict(rss: float | None = None, cpu: float | None = None, floods: int = 0,
            session_errors: int = 0, *, rss_ok: float = RSS_OK_MB, rss_warn: float = RSS_WARN_MB,
            cpu_ok: float = CPU_OK_PERCENT, cpu_warn: float = CPU_WARN_PERCENT,
            flood_warn: int = FLOOD_WARN_PER_DAY) -> tuple[str, str]:
    """Вердикт «A или B» (ТЗ §15.3). Возвращает (короткий вердикт, пояснение с цифрами).

    Правила:
      * RSS ≤ 150 МБ и CPU ≤ 5 % и нет FloodWait/ошибок сессии  → A подходит;
      * 150 < RSS ≤ 250 МБ или CPU ≤ 15 %                       → A возможна, но следи;
      * RSS > 250 МБ или CPU > 15 % или частые FloodWait        → рекомендую B.
    Если данных нет вовсе — честно пишем «н/д», а не выдумываем вердикт.
    """
    floods = int(floods or 0)
    session_errors = int(session_errors or 0)
    reasons: list[str] = []

    if rss is not None:
        reasons.append(f"RSS {rss:g} МБ")
    if cpu is not None:
        reasons.append(f"CPU {cpu:g} %")
    if floods:
        reasons.append(f"FloodWait {floods}")
    if session_errors:
        reasons.append(f"ошибок сессии {session_errors}")

    heavy_flood = floods >= flood_warn or session_errors > 0
    if rss is None and cpu is None:
        if heavy_flood:
            return VERDICT_B, "; ".join(reasons) or "частые FloodWait"
        return VERDICT_UNKNOWN, "нет psutil и нет /proc — память и CPU не измерить"

    if rss is not None and rss > rss_warn:
        return VERDICT_B, "; ".join(reasons)
    if cpu is not None and cpu > cpu_warn:
        return VERDICT_B, "; ".join(reasons)
    if heavy_flood:
        return VERDICT_B, "; ".join(reasons)

    rss_ok_flag = rss is None or rss <= rss_ok
    cpu_ok_flag = cpu is None or cpu <= cpu_ok
    if rss_ok_flag and cpu_ok_flag:
        return VERDICT_A, "; ".join(reasons) or "данных мало, но лимиты не превышены"

    return VERDICT_A_WATCH, "; ".join(reasons)


def format_usage(data: dict, *, floods: int = 0, errors: int = 0, queue: int = 0,
                 per_account: list[tuple[str, int, int]] | None = None,
                 peak_msgs_per_min: tuple[int, str] | None = None,
                 project_mb: float | None = None, wal_mb: float | None = None,
                 day_label: str | None = None, session_errors: int = 0) -> str:
    """Текст /usage по шаблону ТЗ §15.4 (plain text, без разметки).

    data — словарь из sample(). Отсутствующие значения показываются как «н/д», а если
    psutil не установлен, добавляется честная пометка (ТЗ §14.7, п.8).
    """
    mode = data.get("mode") or "A"
    mode_text = "A (слушатель)" if mode == "A" else "B (проходы по расписанию)"
    lines = [f"🧮 Расход и нагрузка · режим {mode_text} · "
             f"аптайм {format_duration(data.get('uptime_s'))}"]

    rss = data.get("rss_mb")
    peak = data.get("rss_peak_mb")
    cpu = data.get("cpu_percent")
    memory = f"Память: {format_number(rss)} МБ" + (f" (пик {peak:g})" if peak else "")
    lines.append(f"{memory} · процессор: "
                 + (f"{cpu:g} % (среднее с запуска)" if cpu is not None else "н/д"))

    db_mb = data.get("db_mb")
    disk = f"База: {format_number(db_mb)} МБ"
    if wal_mb:
        disk += f" (WAL {wal_mb:g})"
    if project_mb:
        disk += f" · диск под проект: {project_mb:g} МБ"
    lines.append(disk)

    msgs = (f"Сообщений: {format_number(data.get('msgs_total'))} всего · "
            f"за час {format_number(data.get('msgs_last_hour'))}")
    if peak_msgs_per_min and peak_msgs_per_min[0]:
        msgs += f" · пик {peak_msgs_per_min[0]}/мин ({peak_msgs_per_min[1]})"
    lines.append(msgs)

    lines.append(f"API-вызовов: {format_number(data.get('api_calls'))} · "
                 f"FloodWait: {floods} · Ошибки: {errors}")

    forwarded = data.get("forwarded_today") or 0
    limit_total = sum(limit for _name, _sent, limit in (per_account or []) if limit)
    tail = f"переслано сегодня {forwarded}" + (f"/{limit_total}" if limit_total else "")
    if per_account:
        tail += " (" + ", ".join(
            f"{name} {sent}" + (f"/{limit}" if limit else "") for name, sent, limit in per_account
        ) + ")"
    if queue:
        tail += f" · в очереди {queue}"
    lines.append(f"Находок: {format_number(data.get('hits_total'))} · {tail}")

    text, reason = verdict(rss=rss, cpu=cpu, floods=floods, session_errors=session_errors)
    lines.append("")
    lines.append(f"Вердикт: {text}" + (f" ({reason})" if reason else ""))
    if day_label:
        lines.append(f"Сутки: {day_label} → RSS {format_number(rss)}, CPU "
                     f"{(str(cpu) + ' %') if cpu is not None else 'н/д'}, "
                     f"{format_number(data.get('msgs_total'))} сообщений")
    if not PSUTIL_AVAILABLE:
        lines.append("psutil не установлен: часть показателей «н/д» (поставь: pip install psutil)")
    return "\n".join(lines)


# -----------------------------------------------------------------------------
# metrics.csv (ТЗ §15.2)
# -----------------------------------------------------------------------------

def metrics_csv_row(data: dict) -> str:
    """Одна строка metrics.csv в порядке METRICS_CSV_HEADER."""
    uptime_h = round((data.get("uptime_s") or 0) / 3600.0, 2)

    def cell(key: str) -> str:
        value = uptime_h if key == "uptime_h" else data.get(key)
        if value is None:
            return ""
        if isinstance(value, float):
            return f"{value:g}"
        return str(value)

    order = ("ts", "rss_mb", "cpu_percent", "uptime_h", "msgs_total", "msgs_last_hour",
             "api_calls", "db_mb", "forwarded_today", "accounts", "mode")
    return ",".join(cell(key) for key in order)


def write_metrics_csv(path: str | Path, rows: list[dict], *, append: bool = False) -> int:
    """Пишет metrics.csv (utf-8, разделитель «,»). Возвращает число строк."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    need_header = append and target.exists() and target.stat().st_size > 0
    with open(target, "a" if append else "w", encoding="utf-8", newline="") as fh:
        if not append or need_header or not target.exists():
            fh.write(METRICS_CSV_HEADER + "\n")
        for row in rows:
            fh.write(metrics_csv_row(row) + "\n")
    return len(rows)


def metrics_since(rows: list[dict], hours: float = 24.0,
                  now: datetime | None = None) -> list[dict]:
    """Отбирает срезы за последние N часов (для вердикта «за сутки»)."""
    moment = now or datetime.now(timezone.utc)
    cutoff = moment.timestamp() - hours * 3600
    picked: list[dict] = []
    for row in rows:
        try:
            stamp = datetime.fromisoformat(str(row.get("ts"))).timestamp()
        except (TypeError, ValueError):
            continue
        if stamp >= cutoff:
            picked.append(row)
    return picked


def aggregate(rows: list[dict]) -> dict:
    """Сводка по срезам: среднее/пик RSS и CPU, сумма сообщений (для вердикта за сутки)."""
    if not rows:
        return {}
    rss = [r["rss_mb"] for r in rows if r.get("rss_mb") is not None]
    cpu = [r["cpu_percent"] for r in rows if r.get("cpu_percent") is not None]
    peak = [r.get("rss_peak_mb") for r in rows if r.get("rss_peak_mb") is not None]
    return {
        "rss_avg": round(sum(rss) / len(rss), 2) if rss else None,
        "rss_max": round(max(rss), 2) if rss else None,
        "rss_peak": round(max(peak), 2) if peak else None,
        "cpu_avg": round(sum(cpu) / len(cpu), 2) if cpu else None,
        "cpu_max": round(max(cpu), 2) if cpu else None,
        "msgs_total": max((int(r.get("msgs_total") or 0) for r in rows), default=0),
        "samples": len(rows),
    }
