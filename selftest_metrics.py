#!/usr/bin/env python3
"""
Офлайн-тесты метрик расхода (ТЗ §15.5): сеть, Telegram, аккаунт и psutil НЕ нужны.

Обязательные шесть проверок §15.5:
  1. строка metrics из фейковых значений — детерминированно;
  2. средний CPU из process_time()/uptime — подстановка значений;
  3. вердикт A/B на четырёх наборах (граничные 150/250 МБ и 5/15 %);
  4. metrics.csv — заголовок + строка, разделитель «,», кодировка utf-8;
  5. без psutil метрики не падают, поля «н/д», вердикт по доступным данным;
  6. ретеншн — строки старше 31 дня удаляются, свежие остаются.

Плюс то, что вокруг них: счётчик API-вызовов в Paced, сборщик срезов, «раз в час» для CSV,
пик сообщений в минуту, полный текст /usage по шаблону §15.4 и флаги --metrics-interval.

Запуск:  python3 selftest_metrics.py
"""
from __future__ import annotations

import asyncio
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import metrics as metrics_module
from core_telegram import Paced
from metrics import (METRICS_CSV_HEADER, MetricsCollector, aggregate, daily_label, format_usage,
                     metrics_csv_row, metrics_since, peak_msgs_per_min, read_metrics_csv, sample,
                     trim_metrics_csv, verdict, write_metrics_csv)
from monitor import HitStore, build_collector, build_parser, message_counters, print_metrics

WORKDIR = Path("tests") / f"_tmp_metrics_{os.getpid()}"   # свой каталог на прогон
NOW = datetime.now(timezone.utc).replace(microsecond=0)


def section(title: str) -> None:
    print(f"\n── {title} " + "─" * max(0, 66 - len(title)))


class FakeClock:
    """Время и сон под контролем: цикл сбора проверяется без реальных ожиданий."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += max(0.0, seconds)
        await asyncio.sleep(0)   # обязательно отдаём управление: иначе цикл сбора не остановим


FAKE_ROW = {
    "ts": "2026-09-24T20:00:00+00:00", "rss_mb": 62.0, "rss_peak_mb": 78.0, "cpu_percent": 1.4,
    "uptime_s": 96000.0, "msgs_total": 3480, "msgs_last_hour": 214, "api_calls": 1902,
    "db_mb": 11.2, "hits_total": 61, "forwarded_today": 44, "accounts": 2, "mode": "A",
}


def make_store(name: str) -> HitStore:
    return HitStore(str(WORKDIR / f"{name}.sqlite3"))


async def main() -> None:
    checks: list[tuple[str, bool, str]] = []
    shutil.rmtree(WORKDIR, ignore_errors=True)
    WORKDIR.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------- 1. строка metrics (§15.5 п.1)
    section("1. Срез метрик из фейковых значений")
    row = sample(ts=FAKE_ROW["ts"], rss=62.0, peak_rss=78.0, cpu=1.4, uptime_s=96000.0,
                 msgs_total=3480, msgs_last_hour=214, api_calls=1902, db_mb=11.2,
                 hits_total=61, forwarded_today=44, accounts=2, mode="A", measure_rss=False)
    checks.append(("срез детерминирован: все поля равны подставленным",
                   row == FAKE_ROW, str(row)))
    checks.append(("порядок и состав полей совпадает со схемой таблицы metrics (§7.2)",
                   set(row) - {"rss_peak_mb"} == {"ts", "rss_mb", "cpu_percent", "uptime_s",
                                                  "msgs_total", "msgs_last_hour", "api_calls",
                                                  "db_mb", "hits_total", "forwarded_today",
                                                  "accounts", "mode"},
                   ", ".join(sorted(row))))
    no_measure = sample(ts=FAKE_ROW["ts"], uptime_s=10.0, msgs_total=5, measure_rss=False)
    checks.append(("без явных значений и без замеров поля остаются None (а не выдуманными)",
                   no_measure["rss_mb"] is None and no_measure["cpu_percent"] is None,
                   str(no_measure)))

    store = make_store("metrics")
    store.log_metric(FAKE_ROW)
    back = store.metrics_recent(hours=24 * 30)
    checks.append(("срез ложится в таблицу metrics и читается обратно",
                   len(back) == 1 and float(back[0]["rss_mb"]) == 62.0
                   and int(back[0]["msgs_total"]) == 3480,
                   str(back[0]) if back else "(пусто)"))

    # ---------------------------------------------------- 2. средний CPU (§15.5 п.2)
    section("2. Средний CPU из process_time/uptime")
    checks.append(("1.5 с CPU за 100 с аптайма = 1.5 %",
                   metrics_module.cpu_percent_average(process_time=1.5, uptime_s=100.0) == 1.5,
                   str(metrics_module.cpu_percent_average(process_time=1.5, uptime_s=100.0))))
    checks.append(("30 с CPU за 600 с аптайма = 5.0 % (граница «A подходит»)",
                   metrics_module.cpu_percent_average(process_time=30.0, uptime_s=600.0) == 5.0,
                   str(metrics_module.cpu_percent_average(process_time=30.0, uptime_s=600.0))))
    checks.append(("при нулевом аптайме — None, а не деление на ноль",
                   metrics_module.cpu_percent_average(process_time=1.0, uptime_s=0.0) is None,
                   "None"))
    checks.append(("без psutil средний CPU всё равно считается (это stdlib)",
                   metrics_module.cpu_percent_average(process_time=2.0, uptime_s=1000.0) == 0.2,
                   "0.2 %"))

    # ---------------------------------------------------- 3. вердикт A/B (§15.5 п.3)
    section("3. Вердикт «A или B»")
    cases = [
        (dict(rss=62.0, cpu=1.4), metrics_module.VERDICT_A, "RSS 62 МБ ≤ 150, CPU 1.4 % ≤ 5"),
        (dict(rss=150.0, cpu=5.0), metrics_module.VERDICT_A, "ровно на границе — ещё A"),
        (dict(rss=200.0, cpu=9.0), metrics_module.VERDICT_A_WATCH, "между 150 и 250 МБ"),
        (dict(rss=250.0, cpu=15.0), metrics_module.VERDICT_A_WATCH, "верхняя граница — ещё не B"),
        (dict(rss=251.0, cpu=4.0), metrics_module.VERDICT_B, "RSS > 250 МБ"),
        (dict(rss=62.0, cpu=15.1), metrics_module.VERDICT_B, "CPU > 15 %"),
        (dict(rss=62.0, cpu=1.4, floods=5), metrics_module.VERDICT_B, "частые FloodWait"),
        (dict(rss=62.0, cpu=1.4, session_errors=1), metrics_module.VERDICT_B, "ошибка сессии"),
    ]
    for kwargs, expected, note in cases:
        text, reason = verdict(**kwargs)
        checks.append((f"вердикт {note}: {expected.split(':')[0]}", text == expected,
                       f"{text} ({reason})"))
    unknown_text, unknown_reason = verdict(rss=None, cpu=None)
    checks.append(("без данных о ресурсах вердикт честный «н/д», а не выдуманный",
                   unknown_text == metrics_module.VERDICT_UNKNOWN and "psutil" in unknown_reason,
                   unknown_reason[:60]))
    checks.append(("без psutil, но с частыми FloodWait — всё равно recommending B",
                   verdict(rss=None, cpu=None, floods=9)[0] == metrics_module.VERDICT_B,
                   "floods=9"))

    # ---------------------------------------------------- 4. metrics.csv (§15.5 п.4)
    section("4. metrics.csv")
    csv_path = WORKDIR / "metrics.csv"
    write_metrics_csv(csv_path, [FAKE_ROW])
    raw = csv_path.read_bytes()
    lines = raw.decode("utf-8").splitlines()
    checks.append(("заголовок metrics.csv ровно как в §15.2",
                   lines[0] == METRICS_CSV_HEADER, lines[0]))
    checks.append(("строка CSV: разделитель «,», порядок колонок, uptime в часах",
                   lines[1] == "2026-09-24T20:00:00+00:00,62,1.4,26.67,3480,214,1902,11.2,44,2,A",
                   lines[1]))
    checks.append(("файл в utf-8 без BOM и с LF (открывается и в Excel, и в блокноте)",
                   not raw.startswith(b"\xef\xbb\xbf") and b"\r\n" not in raw,
                   f"{len(raw)} байт"))
    checks.append(("в заголовке 11 колонок и они те же, что в строке",
                   len(METRICS_CSV_HEADER.split(",")) == 11
                   and len(lines[1].split(",")) == 11, METRICS_CSV_HEADER))
    second = dict(FAKE_ROW, ts="2026-09-24T21:00:00+00:00", rss_mb=70.0)
    write_metrics_csv(csv_path, [second], append=True)
    lines = csv_path.read_text(encoding="utf-8").splitlines()
    checks.append(("append дописывает строку и НЕ повторяет заголовок",
                   len(lines) == 3 and lines[0] == METRICS_CSV_HEADER
                   and lines[2].startswith("2026-09-24T21:00:00"), f"{len(lines)} строк"))
    checks.append(("None в срезе -> пустая ячейка, а не «None»",
                   metrics_csv_row(dict(FAKE_ROW, rss_mb=None)).split(",")[1] == "",
                   metrics_csv_row(dict(FAKE_ROW, rss_mb=None))[:60]))
    parsed = read_metrics_csv(csv_path)
    checks.append(("metrics.csv читается обратно (нужно для ретеншна и вердикта по файлу)",
                   len(parsed) == 2 and parsed[0]["ts"] == FAKE_ROW["ts"], str(len(parsed))))

    # ---------------------------------------------------- 5. без psutil (§15.5 п.5)
    section("5. Работа без psutil")
    saved = (metrics_module.PSUTIL_AVAILABLE, metrics_module.rss_mb, metrics_module.peak_rss_mb,
             metrics_module.cpu_percent_average)
    metrics_module.PSUTIL_AVAILABLE = False
    metrics_module.rss_mb = lambda process=None: None
    metrics_module.peak_rss_mb = lambda: None
    metrics_module.cpu_percent_average = lambda *a, **k: None
    try:
        bare_store = make_store("bare")
        collector = MetricsCollector(bare_store, db_path=str(WORKDIR / "bare.sqlite3"),
                                     csv_path=WORKDIR / "bare.csv", interval_minutes=15,
                                     msgs_provider=lambda: (0, 0))
        bare_row = collector.write()
        checks.append(("без psutil сборщик не падает и пишет срез",
                       bare_row["rss_mb"] is None and bare_row["cpu_percent"] is None
                       and len(bare_store.metrics_recent(hours=1)) == 1,
                       str(bare_row)))
        text = format_usage(bare_row, floods=0, errors=0, daily={},
                            day_label=None)
        checks.append(("в /usage без psutil память и CPU показаны как «н/д»",
                       "Память: н/д МБ" in text and "процессор: н/д" in text,
                       text.splitlines()[2][:60]))
        checks.append(("честная пометка «psutil не установлен»",
                       "psutil не установлен" in text, text.splitlines()[-1][:70]))
        checks.append(("вердикт без psutil считается и не падает",
                       "Вердикт:" in text and metrics_module.VERDICT_UNKNOWN in text,
                       next(line for line in text.splitlines()
                            if line.startswith("Вердикт"))[:70]))
    finally:
        (metrics_module.PSUTIL_AVAILABLE, metrics_module.rss_mb, metrics_module.peak_rss_mb,
         metrics_module.cpu_percent_average) = saved

    checks.append(("с psutil-заглушкой rss_mb возвращает подставленное значение",
                   metrics_module.rss_mb(process=62.0) == 62.0, "62.0"))

    # ---------------------------------------------------- 6. ретеншн (§15.5 п.6)
    section("6. Ретеншн")
    retention_store = make_store("retention")
    old_ts = (NOW - timedelta(days=31)).isoformat(timespec="seconds")
    fresh_ts = (NOW - timedelta(days=1)).isoformat(timespec="seconds")
    retention_store.log_metric(dict(FAKE_ROW, ts=old_ts))
    retention_store.log_metric(dict(FAKE_ROW, ts=fresh_ts))
    removed = retention_store.retention_cleanup(days=30)
    left = [row["ts"] for row in retention_store.metrics_recent(hours=24 * 60)]
    checks.append(("срез старше 31 дня удалён из таблицы metrics, свежий остался",
                   removed["metrics"] == 1 and left == [fresh_ts], str(left)))

    csv_retention = WORKDIR / "retention.csv"
    write_metrics_csv(csv_retention, [dict(FAKE_ROW, ts=old_ts)])
    write_metrics_csv(csv_retention, [dict(FAKE_ROW, ts=fresh_ts)], append=True)
    trimmed = trim_metrics_csv(csv_retention, days=30)
    kept_lines = csv_retention.read_text(encoding="utf-8").splitlines()
    checks.append(("ретеншн metrics.csv: старая строка удалена, свежая и заголовок остались",
                   trimmed == 1 and len(kept_lines) == 2 and kept_lines[0] == METRICS_CSV_HEADER
                   and kept_lines[1].startswith(fresh_ts[:10]),
                   f"удалено {trimmed}, осталось строк {len(kept_lines) - 1}"))
    checks.append(("повторный ретеншн ничего не удаляет (идемпотентно)",
                   trim_metrics_csv(csv_retention, days=30) == 0, "0"))
    middle_ts = (NOW - timedelta(hours=12)).isoformat(timespec="seconds")
    picked = metrics_since([{"ts": old_ts}, {"ts": middle_ts}, {"ts": fresh_ts}], hours=24)
    checks.append(("metrics_since отбирает срезы внутри окна (ровно сутки — уже граница)",
                   [row["ts"] for row in picked] == [middle_ts], f"{len(picked)} из 3"))

    # ---------------------------------------------------- 7. счётчик API-вызовов (§15.1)
    section("7. Счётчик API-вызовов")
    paced = Paced(0.0, 0.0)
    checks.append(("у Paced есть счётчик вызовов и сначала он нулевой", paced.calls == 0, "0"))

    async def two_calls():
        await paced.wait()
        await paced.wait()

    await two_calls()
    checks.append(("каждый вызов API увеличивает счётчик (метрика «API-вызовов»)",
                   paced.calls == 2, str(paced.calls)))
    collector_with_paced = MetricsCollector(make_store("api"), paced=paced,
                                            csv_path=WORKDIR / "api.csv",
                                            msgs_provider=lambda: (100, 10))
    checks.append(("срез забирает число API-вызовов из Paced",
                   collector_with_paced.collect()["api_calls"] == 2,
                   str(collector_with_paced.api_calls())))

    # ---------------------------------------------------- 8. сборщик и его расписание
    section("8. Сборщик срезов")
    loop_store = make_store("loop")
    clock = FakeClock()
    counters = {"total": 1000}
    collector = MetricsCollector(loop_store, db_path=str(WORKDIR / "loop.sqlite3"),
                                 csv_path=WORKDIR / "loop.csv", interval_minutes=15,
                                 csv_every_minutes=60, mode="A", accounts=2, paced=Paced(0.0, 0.0),
                                 msgs_provider=lambda: (counters["total"], 10),
                                 clock=clock, sleep=clock.sleep)
    first = collector.write()
    checks.append(("первый срез пишется в базу и сразу в metrics.csv",
                   len(loop_store.metrics_recent(hours=1)) == 1 and first in collector.samples
                   and len(read_metrics_csv(WORKDIR / "loop.csv")) == 1,
                   f"в базе {len(loop_store.metrics_recent(hours=1))}, в csv 1"))
    counters["total"] = 1600          # +600 сообщений за 15 минут = 40/мин
    clock.now += 15 * 60
    second = collector.write()
    checks.append(("второй срез собран, а CSV не вырос (час ещё не прошёл)",
                   len(collector.samples) == 2 and second["msgs_total"] == 1600
                   and len(read_metrics_csv(WORKDIR / "loop.csv")) == 1,
                   f"срезов {len(collector.samples)}, в csv {len(read_metrics_csv(WORKDIR / 'loop.csv'))}"))
    peak, peak_stamp = peak_msgs_per_min(loop_store)
    checks.append(("пик сообщений/минуту считается по разнице срезов и хранится в bot_state",
                   peak == 40 and len(peak_stamp) == 5 and peak_stamp[2] == ":",
                   f"{peak}/мин в {peak_stamp}"))
    clock.now += 45 * 60              # всего час с первого среза
    counters["total"] = 1700
    collector.write()
    checks.append(("раз в час строка metrics.csv дописывается",
                   len(read_metrics_csv(WORKDIR / "loop.csv")) == 2,
                   f"{len(read_metrics_csv(WORKDIR / 'loop.csv'))} строк"))
    checks.append(("свежие строки CSV не повторяют заголовок",
                   (WORKDIR / "loop.csv").read_text(encoding="utf-8").count(METRICS_CSV_HEADER) == 1,
                   "заголовок один"))

    before = len(collector.samples)

    async def run_loop():
        """Цикл сбора крутится на фейковых снах и сам снимается по стоп-флагу."""
        task = asyncio.create_task(collector.loop())
        for _ in range(3):                 # даём циклу сделать пару срезов
            await asyncio.sleep(0)
        collector.stopped = True           # стоп-флаг снимает задачу на следующей итерации
        finished = False
        for _ in range(6):
            await asyncio.sleep(0)
            if task.done():
                finished = not task.cancelled()
                break
        if not finished:
            task.cancel()                  # страховка: если флаг не сработал, тест это увидит
            try:
                await task
            except asyncio.CancelledError:
                pass
        return finished

    finished = await run_loop()
    checks.append(("цикл сбора спит ровно интервал --metrics-interval (15 мин = 900 с)",
                   bool(clock.sleeps) and abs(clock.sleeps[0] - 900.0) < 0.001,
                   f"первый сон {clock.sleeps[0] if clock.sleeps else '—'} с"))
    checks.append(("каждая итерация цикла даёт новый срез",
                   len(collector.samples) > before, f"{before} → {len(collector.samples)}"))
    checks.append(("цикл завершается сам по стоп-флагу (не висит и не требует cancel)",
                   finished, "task.done() без cancel"))

    # ---------------------------------------------------- 9. сводка за сутки и /usage
    section("9. Сводка за сутки и полный /usage")
    day_store = make_store("day")
    for index, (rss, cpu) in enumerate([(62.0, 1.4), (78.0, 2.2), (70.0, 1.0)]):
        day_store.log_metric(dict(
            FAKE_ROW, ts=(NOW - timedelta(minutes=45 - index * 15)).isoformat(timespec="seconds"),
            rss_mb=rss, rss_peak_mb=rss + 6, cpu_percent=cpu, msgs_total=3000 + index * 240))
    rows = day_store.metrics_recent(hours=24)
    summary = aggregate(rows)
    checks.append(("сводка за сутки: средний и максимальный RSS, средний CPU, число срезов",
                   summary["rss_avg"] == 70.0 and summary["rss_max"] == 78.0
                   and summary["cpu_avg"] == 1.53 and summary["samples"] == 3,
                   str(summary)))
    checks.append(("в сводку попадают только срезы за последние 24 часа",
                   aggregate(metrics_since(rows + [dict(FAKE_ROW, ts=(
                       NOW - timedelta(days=3)).isoformat(timespec="seconds"))], hours=24)
                   )["samples"] == 3, "старый срез отброшен"))
    checks.append(("подпись «Сутки:» берёт дату последнего среза",
                   daily_label(rows) == str(rows[-1]["ts"])[:10], daily_label(rows)))

    live = sample(ts=NOW.isoformat(timespec="seconds"), rss=62.0, peak_rss=78.0, cpu=1.4,
                  uptime_s=96000.0, msgs_total=3480, msgs_last_hour=214, api_calls=1902,
                  db_mb=11.2, hits_total=61, forwarded_today=44, accounts=2, mode="A",
                  measure_rss=False)
    text = format_usage(live, floods=0, errors=0, queue=3,
                        per_account=[("main", 28, 120), ("second", 16, 60)],
                        peak_msgs_per_min=(12, "20:10"), project_mb=14.0, wal_mb=0.4,
                        day_label=daily_label(rows), daily=summary)
    expected_pieces = [
        "🧮 Расход и нагрузка · режим A (слушатель) · аптайм 1 дн 2 ч 40 мин",
        "Память: 62 МБ (пик 78) · процессор: 1.53 % (среднее за сутки)",
        "База: 11.2 МБ (WAL 0.4) · диск под проект: 14 МБ",
        "Сообщений: 3 480 всего · за час 214 · пик 12/мин (20:10)",
        "API-вызовов: 1 902 · FloodWait: 0 · Ошибки: 0",
        "Находок: 61 · переслано сегодня 44/180 (main 28/120, second 16/60) · в очереди 3",
        f"Вердикт: {metrics_module.VERDICT_A} (RSS 70 МБ ≤ 150, CPU 1.53 % ≤ 5)",
    ]
    for piece in expected_pieces:
        checks.append((f"/usage по шаблону §15.4: «{piece[:44]}…»", piece in text,
                       piece if piece in text else text[:120]))
    day_line = next((line for line in text.splitlines() if line.startswith("Сутки:")), "")
    checks.append(("в /usage есть строка «Сутки: дата → RSS, CPU, сообщения, срезы»",
                   day_line.startswith(f"Сутки: {daily_label(rows)} →")
                   and "RSS 70 МБ," in day_line and "CPU 1.53 %" in day_line
                   and "3 480 сообщений" in day_line and "3 среза" in day_line,
                   day_line[:90]))
    checks.append(("/usage — plain text: никакой разметки Markdown/HTML",
                   "*" not in text and "<b>" not in text and "_" not in text, "plain"))

    checks.append(("склонение после числа русское: 1 срез, 3 среза, 11 срезов, 21 срез",
                   [metrics_module.plural(n, "срез", "среза", "срезов")
                    for n in (1, 3, 11, 21)] == ["срез", "среза", "срезов", "срез"],
                   ", ".join(metrics_module.plural(n, "срез", "среза", "срезов")
                             for n in (1, 3, 11, 21))))

    # ---------------------------------------------------- 10. интеграция с радаром
    section("10. Интеграция с радаром")
    parser = build_parser()
    bare = parser.parse_args([])
    checks.append(("--metrics-interval по умолчанию 15 минут, файл — metrics.csv",
                   bare.metrics_interval == 15.0 and bare.metrics_csv == "metrics.csv",
                   f"{bare.metrics_interval} мин, {bare.metrics_csv}"))
    parsed = parser.parse_args(["--metrics-interval", "5", "--metrics-csv", "out/m.csv"])
    checks.append(("--metrics-interval и --metrics-csv принимаются",
                   parsed.metrics_interval == 5.0 and parsed.metrics_csv == "out/m.csv",
                   f"{parsed.metrics_interval}/{parsed.metrics_csv}"))

    class Args:
        db = str(WORKDIR / "args.sqlite3")
        metrics_interval = 15.0
        metrics_csv = str(WORKDIR / "args.csv")

    args_store = make_store("args")
    built = build_collector(Args(), args_store, [], "A", Paced(0.0, 0.0))
    checks.append(("build_collector собирает сборщик с настройками из флагов",
                   built is not None and built.interval_minutes == 15.0
                   and str(built.csv_path).endswith("args.csv") and built.mode == "A",
                   f"interval={built.interval_minutes}, mode={built.mode}"))

    class OffArgs(Args):
        metrics_interval = 0.0

    checks.append(("--metrics-interval 0 выключает метрики (сборщик не создаётся)",
                   build_collector(OffArgs(), args_store, [], "A", Paced(0.0, 0.0)) is None, "None"))

    counter_store = make_store("counters")
    counter_store.bump_stats("granica_polska", scanned=1500, matched=20, saved=4,
                             account="main")
    counter_store.log_heartbeat("pulse", account="main", detail="прочитано 900, найдено 3",
                                ts=(NOW - timedelta(minutes=70)).isoformat(timespec="seconds"))
    counter_store.log_heartbeat("pulse", account="main", detail="прочитано 1500, найдено 4",
                                ts=(NOW - timedelta(minutes=5)).isoformat(timespec="seconds"))

    class Account:
        name = "main"

    total, hour = message_counters(counter_store, [Account()])
    checks.append(("«прочитано всего» берётся из stats, «за час» — из разницы пульсов",
                   total == 1500 and hour == 600, f"всего {total}, за час {hour}"))
    checks.append(("без пульсов «за час» честно равно нулю (а не выдуманному числу)",
                   message_counters(make_store("nopulse"), [Account()]) == (0, 0), "(0, 0)"))
    try:
        print_metrics(FAKE_ROW)
        printed = True
    except Exception as exc:                          # noqa: BLE001
        printed = False
        print("   print_metrics упал:", exc)
    checks.append(("строка расхода печатается в конце прогона (схема B видит её в логе)",
                   printed, "print_metrics"))

    # ---------------------------------------------------- итог
    section("Итог")
    for name, ok, detail in checks:
        print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail else ""))
    failed = [name for name, ok, _ in checks if not ok]
    print("\nИТОГ:", "всё ок" if not failed else f"провалено: {failed}")
    shutil.rmtree(WORKDIR, ignore_errors=True)
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] прервано")
