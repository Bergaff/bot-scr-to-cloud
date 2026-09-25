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

import asyncio
import os
import sys
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
    """Вердикт «A или B» (ТЗ §15.3). Возвращает (вердикт, пояснение с цифрами и порогами).

    Правила (за последние сутки работы):
      * RSS ≤ 150 МБ и CPU ≤ 5 % и нет FloodWait/ошибок сессии  → A подходит;
      * 150 < RSS ≤ 250 МБ или CPU ≤ 15 %                       → A возможна, но следи;
      * RSS > 250 МБ или CPU > 15 % или частые FloodWait        → рекомендую B.
    Если данных нет вовсе — честно пишем «н/д», а не выдумываем вердикт.
    """
    floods = int(floods or 0)
    session_errors = int(session_errors or 0)
    heavy_flood = floods >= flood_warn or session_errors > 0
    flood_reason = (f"FloodWait {floods} за сутки" if floods >= flood_warn
                    else (f"ошибок сессии {session_errors}" if session_errors else ""))

    if rss is None and cpu is None:
        if heavy_flood:
            return VERDICT_B, flood_reason
        return VERDICT_UNKNOWN, "нет psutil и нет /proc — память и CPU не измерить"

    if rss is not None and rss > rss_warn:
        reason = f"RSS {rss:g} МБ > {rss_warn:g} МБ"
        return VERDICT_B, reason + (f"; {flood_reason}" if flood_reason else "")
    if cpu is not None and cpu > cpu_warn:
        reason = f"CPU {cpu:g} % > {cpu_warn:g} %"
        return VERDICT_B, reason + (f"; {flood_reason}" if flood_reason else "")
    if heavy_flood:
        return VERDICT_B, flood_reason

    fits_rss = rss is None or rss <= rss_ok
    fits_cpu = cpu is None or cpu <= cpu_ok
    parts = []
    if rss is not None:
        parts.append(f"RSS {rss:g} МБ ≤ {rss_ok:g}" if fits_rss
                     else f"RSS {rss:g} МБ > {rss_ok:g} (но ≤ {rss_warn:g})")
    if cpu is not None:
        parts.append(f"CPU {cpu:g} % ≤ {cpu_ok:g}" if fits_cpu
                     else f"CPU {cpu:g} % > {cpu_ok:g} (но ≤ {cpu_warn:g})")
    if fits_rss and fits_cpu:
        return VERDICT_A, ", ".join(parts)
    return VERDICT_A_WATCH, ", ".join(parts)


def plural(value: int, one: str, few: str, many: str) -> str:
    """Русское склонение после числа: 1 срез, 3 среза, 11 срезов, 21 срез."""
    number = abs(int(value)) % 100
    tail = number % 10
    if 11 <= number <= 14:
        return many
    if tail == 1:
        return one
    if 2 <= tail <= 4:
        return few
    return many


def _mb(value: float | None) -> str:
    """Мегабайты без лишнего хвоста: 62.0 -> «62», 11.2 -> «11.2», None -> «н/д»."""
    if value is None:
        return "н/д"
    return f"{float(value):g}"


def format_usage(data: dict, *, floods: int = 0, errors: int = 0, queue: int = 0,
                 per_account: list[tuple[str, int, int]] | None = None,
                 peak_msgs_per_min: tuple[int, str] | None = None,
                 project_mb: float | None = None, wal_mb: float | None = None,
                 day_label: str | None = None, session_errors: int = 0,
                 daily: dict | None = None) -> str:
    """Текст /usage по шаблону ТЗ §15.4 (plain text, без разметки).

    data — живой срез из sample(); daily — сводка по строкам metrics за сутки (aggregate()).
    Живые значения важнее для «сейчас», суточные — для процессора и вердикта: именно они
    отвечают на вопрос «сколько жрёт схема A за сутки». Отсутствующее показывается как «н/д»,
    а без psutil добавляется честная пометка (ТЗ §14.7, п.8).
    """
    daily = daily or {}
    mode = data.get("mode") or "A"
    mode_text = "A (слушатель)" if mode == "A" else "B (проходы по расписанию)"
    lines = [f"🧮 Расход и нагрузка · режим {mode_text} · "
             f"аптайм {format_duration(data.get('uptime_s'))}"]
    lines.append("")

    rss = data.get("rss_mb") if data.get("rss_mb") is not None else daily.get("rss_avg")
    peak_rss = data.get("rss_peak_mb") or daily.get("rss_peak") or daily.get("rss_max")
    memory = f"Память: {_mb(rss)} МБ" + (f" (пик {peak_rss:g})" if peak_rss else "")
    if daily.get("cpu_avg") is not None:
        cpu, cpu_note = daily["cpu_avg"], "среднее за сутки"
    else:
        cpu, cpu_note = data.get("cpu_percent"), "среднее с запуска"
    cpu_text = f"{cpu:g} % ({cpu_note})" if cpu is not None else "н/д"
    lines.append(f"{memory} · процессор: {cpu_text}")

    db_mb = data.get("db_mb")
    disk = f"База: {_mb(db_mb)} МБ"
    if wal_mb:
        disk += f" (WAL {wal_mb:g})"
    if project_mb:
        disk += f" · диск под проект: {project_mb:g} МБ"
    lines.append(disk)

    msgs = (f"Сообщений: {format_number(data.get('msgs_total'))} всего · "
            f"за час {format_number(data.get('msgs_last_hour'))}")
    if peak_msgs_per_min and peak_msgs_per_min[0]:
        rate, stamp = peak_msgs_per_min
        msgs += f" · пик {rate}/мин" + (f" ({stamp})" if stamp else "")
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

    verdict_rss = daily.get("rss_avg") if daily.get("rss_avg") is not None else rss
    text, reason = verdict(rss=verdict_rss, cpu=cpu, floods=floods, session_errors=session_errors)
    lines.append("")
    lines.append(f"Вердикт: {text}" + (f" ({reason})" if reason else ""))
    if day_label:
        samples = daily.get("samples") or 0
        note = f"Сутки: {day_label} → RSS {_mb(verdict_rss)} МБ"
        note += f", CPU {((str(cpu) + ' %') if cpu is not None else 'н/д')}"
        total = int(data.get("msgs_total") or 0)
        note += f", {format_number(total)} {plural(total, 'сообщение', 'сообщения', 'сообщений')}"
        if samples:
            note += f", {samples} {plural(samples, 'срез', 'среза', 'срезов')}"
        lines.append(note)
    elif daily:
        lines.append("Сутки: срезы метрик уже пишутся, дата появится после первых суток работы")
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
    """Пишет metrics.csv (utf-8, разделитель «,»). Возвращает число записанных строк.

    append=True — дописывает в существующий файл и НЕ повторяет заголовок: файл растёт
    сутки за сутками, а открыть его можно обычным Excel.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    has_content = append and target.exists() and target.stat().st_size > 0
    with open(target, "a" if append else "w", encoding="utf-8", newline="") as fh:
        if not has_content:
            fh.write(METRICS_CSV_HEADER + "\n")
        for row in rows:
            fh.write(metrics_csv_row(row) + "\n")
    return len(rows)


def read_metrics_csv(path: str | Path) -> list[dict]:
    """Читает metrics.csv обратно (для ретеншна и для вердикта по файлу, а не по базе)."""
    import csv as _csv

    target = Path(path)
    if not target.exists():
        return []
    rows: list[dict] = []
    with open(target, encoding="utf-8", newline="") as fh:
        for row in _csv.DictReader(fh):
            rows.append(row)
    return rows


def trim_metrics_csv(path: str | Path, days: int = 30, now: datetime | None = None) -> int:
    """Ретеншн metrics.csv: убирает строки старше N дней. Возвращает число удалённых."""
    target = Path(path)
    if not target.exists():
        return 0
    moment = now or datetime.now(timezone.utc)
    cutoff = moment.timestamp() - max(0, int(days)) * 86400
    kept, removed = [], 0
    for row in read_metrics_csv(target):
        try:
            stamp = datetime.fromisoformat(str(row.get("ts"))).timestamp()
        except (TypeError, ValueError):
            kept.append(row)                 # строку без времени не فهمели — не удаляем
            continue
        if stamp >= cutoff:
            kept.append(row)
        else:
            removed += 1
    if removed:
        parsed = []
        for row in kept:
            parsed.append({key: _coerce(row.get(key)) for key in
                           ("ts", "rss_mb", "cpu_percent", "uptime_s", "msgs_total",
                            "msgs_last_hour", "api_calls", "db_mb", "forwarded_today",
                            "accounts", "mode")})
        for row, source in zip(parsed, kept):      # uptime_h в файле -> uptime_s для записи
            if source.get("uptime_s") in (None, "") and source.get("uptime_h"):
                try:
                    row["uptime_s"] = float(source["uptime_h"]) * 3600.0
                except ValueError:
                    row["uptime_s"] = 0.0
        write_metrics_csv(target, parsed, append=False)
    return removed


def _coerce(value):
    """Значение из CSV: число, если похоже на число, иначе строка как есть."""
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except ValueError:
        return value
    return int(number) if number.is_integer() and "." not in str(value) else number


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
    """Сводка по срезам: среднее/максимум/пик RSS и CPU, сообщения, число срезов."""
    if not rows:
        return {}
    rss = [float(r["rss_mb"]) for r in rows if r.get("rss_mb") is not None]
    cpu = [float(r["cpu_percent"]) for r in rows if r.get("cpu_percent") is not None]
    peak = [float(r["rss_peak_mb"]) for r in rows if r.get("rss_peak_mb") is not None]
    return {
        "rss_avg": round(sum(rss) / len(rss), 2) if rss else None,
        "rss_max": round(max(rss), 2) if rss else None,
        "rss_peak": round(max(peak), 2) if peak else None,
        "cpu_avg": round(sum(cpu) / len(cpu), 2) if cpu else None,
        "cpu_max": round(max(cpu), 2) if cpu else None,
        "msgs_total": max((int(r.get("msgs_total") or 0) for r in rows), default=0),
        "db_mb": max((float(r.get("db_mb")) for r in rows if r.get("db_mb") is not None),
                     default=None),
        "samples": len(rows),
    }


def daily_label(rows: list[dict] | None = None, now: datetime | None = None) -> str:
    """Подпись «Сутки: 2026-09-24» для /usage — дата последнего среза либо сегодня."""
    if rows:
        last = str(rows[-1].get("ts") or "")[:10]
        if last:
            return last
    moment = now or datetime.now(timezone.utc)
    return moment.astimezone().date().isoformat()


def peak_msgs_per_min(store, day: str | None = None) -> tuple[int, str]:
    """Пик сообщений в минуту за день: (число, «ЧЧ:ММ»). Хранится в bot_state коллектором."""
    key = f"peak_msgs:{day or datetime.now().astimezone().date().isoformat()}"
    raw = str(store.bot_state_get(key) or "")
    rate, _sep, stamp = raw.partition("|")
    try:
        return int(float(rate)), stamp
    except ValueError:
        return 0, ""


# -----------------------------------------------------------------------------
# Сборщик метрик (ТЗ §15.2)
# -----------------------------------------------------------------------------

class MetricsCollector:
    """Периодический срез ресурсов процесса: таблица metrics + строка в metrics.csv.

    Один на процесс (не по одному на аккаунт): ресурсы потребляет процесс целиком.
    Работает и в схеме A (живой слушатель, раз в --metrics-interval минут), и в схеме B
    (--once): там срез пишется один раз в конце прохода — иначе расход не измерить.

    Внешние данные приходят колбэками, чтобы модуль не знал про Telethon и Monitor:
      msgs_provider() -> (прочитано всего, прочитано за час)
      paced           -> объект со счётчиком .calls (API-вызовы Telethon)
    """

    def __init__(self, store, *, db_path: str | None = None, mode: str = "A", accounts: int = 1,
                 interval_minutes: float = 15.0, csv_path: str | Path = "metrics.csv",
                 csv_every_minutes: float = 60.0, retention_days: int = 30,
                 paced=None, msgs_provider=None, sleep=None, clock=None, verbose: bool = True):
        self.store = store
        self.db_path = db_path or getattr(store, "path", None)
        self.mode = (mode or "A").upper()
        self.accounts = accounts
        self.interval_minutes = max(0.5, float(interval_minutes or 15.0))
        self.csv_path = Path(csv_path)
        self.csv_every_minutes = max(1.0, float(csv_every_minutes or 60.0))
        self.retention_days = retention_days
        self.paced = paced
        self.msgs_provider = msgs_provider
        self._sleep = sleep or asyncio.sleep
        self._clock = clock or time.monotonic
        self.verbose = verbose
        self.stopped = False
        self._last_total: int | None = None
        self._last_stamp: float | None = None
        self._last_csv: float = 0.0
        self.peak_rate: int = 0
        self.peak_stamp: str = ""
        self.samples: list[dict] = []

    # --- что меряем

    def api_calls(self) -> int:
        """Сколько раз сходили в Telegram (счётчик живёт в Paced.wait)."""
        return int(getattr(self.paced, "calls", 0) or 0)

    def messages(self) -> tuple[int, int]:
        """(прочитано всего, за последний час)."""
        if self.msgs_provider is not None:
            try:
                total, last_hour = self.msgs_provider()
                return int(total or 0), int(last_hour or 0)
            except Exception:                       # noqa: BLE001 - метрики не должны ронять радар
                pass
        try:
            return int(self.store.scanned_total()), 0
        except Exception:                           # noqa: BLE001
            return 0, 0

    def collect(self, uptime_s: float | None = None) -> dict:
        """Один срез: всё, что можно измерить прямо сейчас (ТЗ §15.1)."""
        total, last_hour = self.messages()
        row = sample(msgs_total=total, msgs_last_hour=last_hour, api_calls=self.api_calls(),
                     db_path=self.db_path, hits_total=self._hits_total(),
                     forwarded_today=self._forwarded_today(), accounts=self.accounts,
                     mode=self.mode, uptime_s=uptime_s)
        self._track_peak(total)
        self.samples.append(row)
        return row

    def _hits_total(self) -> int:
        try:
            return int(self.store.hits_total())
        except Exception:                           # noqa: BLE001
            return 0

    def _forwarded_today(self) -> int:
        try:
            return int(self.store.forwarded_today())
        except Exception:                           # noqa: BLE001
            return 0

    def _track_peak(self, total: int) -> None:
        """Пик сообщений/минуту по разнице двух срезов (первый срез даёт только базу отсчёта)."""
        now = self._clock()
        if self._last_total is not None and self._last_stamp is not None:
            minutes = (now - self._last_stamp) / 60.0
            if minutes > 0 and total >= self._last_total:
                rate = int(round((total - self._last_total) / minutes))
                if rate > self.peak_rate:
                    self.peak_rate = rate
                    self.peak_stamp = datetime.now().astimezone().strftime("%H:%M")
                    self.store.bot_state_set(
                        f"peak_msgs:{datetime.now().astimezone().date().isoformat()}",
                        f"{self.peak_rate}|{self.peak_stamp}")
        self._last_total, self._last_stamp = total, now

    # --- куда пишем

    def write(self, row: dict | None = None) -> dict:
        """Сохраняет срез в базу и, раз в час, дописывает строку в metrics.csv."""
        row = row or self.collect()
        try:
            self.store.log_metric(row)
        except Exception as exc:                    # noqa: BLE001
            if self.verbose:
                print(f"[!] метрики в базу не записаны: {type(exc).__name__} {exc}", file=sys.stderr)
        self.csv_tick(row)
        return row

    def csv_tick(self, row: dict) -> bool:
        """Раз в csv_every_minutes — строка в metrics.csv (человекочитаемый след за сутки)."""
        now = self._clock()
        if self._last_csv and now - self._last_csv < self.csv_every_minutes * 60.0:
            return False
        self._last_csv = now
        try:
            write_metrics_csv(self.csv_path, [row], append=True)
        except OSError as exc:
            if self.verbose:
                print(f"[!] metrics.csv не записан: {exc}", file=sys.stderr)
            return False
        return True

    def retention(self) -> tuple[dict, int]:
        """Ретеншн: база (heartbeats/metrics/errors) и metrics.csv — старше N дней долой."""
        removed_db: dict = {}
        removed_csv = 0
        try:
            removed_db = self.store.retention_cleanup(days=self.retention_days)
        except Exception as exc:                    # noqa: BLE001
            if self.verbose:
                print(f"[!] ретеншн базы не выполнен: {type(exc).__name__}", file=sys.stderr)
        try:
            removed_csv = trim_metrics_csv(self.csv_path, days=self.retention_days)
        except OSError as exc:
            if self.verbose:
                print(f"[!] ретеншн metrics.csv не выполнен: {exc}", file=sys.stderr)
        return removed_db, removed_csv

    async def loop(self, stop_event=None) -> None:
        """Живой режим: срез раз в --metrics-interval минут, ретеншн раз в сутки."""
        last_retention = 0.0
        while not self.stopped and (stop_event is None or not stop_event.is_set()):
            await self._sleep(self.interval_minutes * 60.0)
            if self.stopped:
                break
            try:
                self.write()
            except Exception as exc:                # noqa: BLE001 - радар важнее метрик
                if self.verbose:
                    print(f"[!] сбой сбора метрик: {type(exc).__name__} {exc}", file=sys.stderr)
            now = self._clock()
            if now - last_retention >= 86400.0:
                last_retention = now
                self.retention()
