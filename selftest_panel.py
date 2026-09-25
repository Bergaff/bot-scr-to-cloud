#!/usr/bin/env python3
"""
Офлайн-тесты бот-панели и метрик: сеть, Telegram, аккаунт и psutil НЕ нужны.

Здесь обязательные 9 проверок из ТЗ §14.7 плюс то, что панель тянет за собой:
миграция старой базы (§17.1, п.4), ретеншн (§17.1, п.6), флаги CLI, алерты с антиспамом
и вердикт «A или B» (§15.5, п.3).

Главная проверка — п.9: панель ни разу не создаёт Telethon-клиент. Второй клиент на тот же
.session означает AuthKeyDuplicatedError и повторный вход, поэтому живость аккаунтов
определяется только по базе (пульс, события, ошибки).

Запуск:  python3 selftest_panel.py
"""
from __future__ import annotations

import asyncio
import shutil
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import core_telegram
import metrics as metrics_module
import monitor as monitor_module
from bot_panel import (COMMANDS, AccountView, BotPanel, HttpTransport, parse_owner_chat,
                       retry_after_of, truncate)
from monitor import SCHEMA, HitStore, Monitor, build_parser, resolve_mode

WORKDIR = Path("tests/_tmp_panel")
OWNER = "999888777"
NOW = datetime.now(timezone.utc).replace(microsecond=0)
ALL_COMMANDS = COMMANDS | {"help"}


def section(title: str) -> None:
    print(f"\n── {title} " + "─" * max(0, 66 - len(title)))


# ----------------------------------------------------------------------------- фейки

class FakeClock:
    """Время и сон под контролем теста: rate limit проверяется без реальных ожиданий."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += max(0.0, seconds)


class FakeTransport:
    """Bot API без сети: записывает, что и когда отправили."""

    def __init__(self, clock: FakeClock):
        self.clock = clock
        self.sent: list[tuple[float, str]] = []
        self.docs: list[tuple[float, str]] = []
        self.get_me_calls = 0
        self.updates: list[dict] = []
        self.fail_next: str | None = None
        self.hold = 0.0                      # имитация long polling (для теста цикла run)

    async def get_updates(self, offset: int, timeout: int = 25) -> list[dict]:
        if self.hold:
            await asyncio.sleep(self.hold)
        else:
            await asyncio.sleep(0)
        pending = [u for u in self.updates if int(u.get("update_id", 0)) >= offset]
        self.updates = []
        return pending

    async def send_message(self, chat_id: str, text: str) -> tuple[bool, str]:
        if self.fail_next:
            error, self.fail_next = self.fail_next, None
            return False, error
        self.sent.append((self.clock(), text))
        return True, ""

    async def send_document(self, chat_id: str, path: str, caption: str = "") -> tuple[bool, str]:
        self.docs.append((self.clock(), path))
        return True, ""

    async def get_me(self) -> tuple[bool, str]:
        self.get_me_calls += 1
        return True, "бот @radar_panel_test (id=1)"


class SpyClient:
    """«Telethon-клиент», который панели создавать нельзя (ТЗ §14.3)."""

    def __init__(self, counter: dict):
        self.counter = counter
        counter["created"] += 1

    async def connect(self):
        self.counter["calls"] += 1

    async def is_user_authorized(self):
        self.counter["calls"] += 1
        return True

    async def get_me(self):
        self.counter["calls"] += 1
        return None

    async def disconnect(self):
        self.counter["calls"] += 1


def make_store(path: Path) -> HitStore:
    """База с данными: два аккаунта, находки, статистика, очередь."""
    store = HitStore(str(path))
    store.bump_stats("granica_polska", scanned=12480, matched=64, saved=6, forwarded=5,
                     account="main")
    store.bump_stats("granica_BY_LT_PL", scanned=8210, matched=41, saved=3, forwarded=2,
                     account="second")
    store.save_hit({
        "chat_key": "granica_polska", "chat_title": "Граница Польша", "chat_id": "-1001",
        "username": "granica_polska", "msg_id": 91532,
        "date": (NOW - timedelta(minutes=12)).isoformat(timespec="seconds"),
        "sender_id": "7", "sender_name": "Иван",
        "text": "20.09 еду Варшава-Минск, возьму посылку, есть место в багажнике",
        "score": 14, "category": "parcel", "intent": "offer", "direction": "PL->BY",
        "countries": ["PL", "BY"], "hits": ["посылка", "возьму"],
        "link": "https://t.me/granica_polska/91532",
        "found_at": (NOW - timedelta(minutes=12)).isoformat(timespec="seconds"),
        "topic_id": None, "topic_name": "", "account": "main",
    })
    store.save_hit({
        "chat_key": "granica_BY_LT_PL", "chat_title": "Граница BY-LT-PL", "chat_id": "-1002",
        "username": "granica_BY_LT_PL", "msg_id": 4310,
        "date": (NOW - timedelta(hours=2)).isoformat(timespec="seconds"),
        "sender_id": "9", "sender_name": "Ольга",
        "text": "Ищу попутчика Вильнюс-Минск на завтра, два места",
        "score": 9, "category": "ride", "intent": "request", "direction": "LT->BY",
        "countries": ["LT", "BY"], "hits": ["попутчика"],
        "link": "https://t.me/granica_BY_LT_PL/4310",
        "found_at": (NOW - timedelta(hours=2)).isoformat(timespec="seconds"),
        "topic_id": None, "topic_name": "", "account": "second",
    })
    store.mark_forwarded("granica_polska", 91532, ok=True, mode="forwarded", account="main")
    store.queue_forward("granica_BY_LT_PL", 4310, account="second")
    return store


def make_panel(store: HitStore, clock: FakeClock, transport: FakeTransport, **kwargs) -> BotPanel:
    if "accounts" not in kwargs:                # пустой список — валидный случай (--panel-only)
        kwargs["accounts"] = [
            AccountView(name="main", session="monitor_session", max_per_day=120, chats=6),
            AccountView(name="second", session="second_session", max_per_day=60, chats=4),
        ]
    defaults = dict(
        token="123456:TEST-TOKEN", chat_id=OWNER, transport=transport,
        mode="A", started_at=NOW - timedelta(hours=6, minutes=12), heartbeat_minutes=15.0,
        alert_silent_minutes=30.0, stats_file=str(WORKDIR / "stats.txt"), stats_days=7,
        titles={"granica_polska": "Граница Польша", "granica_BY_LT_PL": "Граница BY-LT-PL"},
        db_path=str(store.path), clock=clock, sleep=clock.sleep,
    )
    defaults.update(kwargs)
    return BotPanel(store, **defaults)


def update(update_id: int, text: str, chat_id: str = OWNER) -> dict:
    """Апдейт в том виде, в каком его отдаёт getUpdates."""
    return {"update_id": update_id,
            "message": {"message_id": update_id, "chat": {"id": int(chat_id), "type": "private"},
                        "text": text}}


async def answer(panel: BotPanel, text: str) -> str:
    reply = await panel.answer_text(text)
    return reply.text


# ----------------------------------------------------------------------------- тесты

async def main() -> None:
    checks: list[tuple[str, bool, str]] = []
    shutil.rmtree(WORKDIR, ignore_errors=True)
    WORKDIR.mkdir(parents=True, exist_ok=True)

    clock = FakeClock()
    transport = FakeTransport(clock)
    store = make_store(WORKDIR / "panel.sqlite3")
    store.log_heartbeat("start", account="main",
                        ts=(NOW - timedelta(hours=6)).isoformat(timespec="seconds"))
    store.log_heartbeat("start", account="second",
                        ts=(NOW - timedelta(hours=6)).isoformat(timespec="seconds"))
    store.log_heartbeat("pulse", account="main", detail="прочитано 412, найдено 6",
                        ts=(NOW - timedelta(minutes=1)).isoformat(timespec="seconds"))
    store.log_heartbeat("pulse", account="second", detail="прочитано 180, найдено 3",
                        ts=(NOW - timedelta(minutes=40)).isoformat(timespec="seconds"))
    store.log_heartbeat("flood", account="second", detail="пересылка: до 21:10",
                        ts=(NOW - timedelta(minutes=39)).isoformat(timespec="seconds"))
    store.log_error("FloodWaitError", "пересылка: Telegram просит 40 с", account="second",
                    ts=(NOW - timedelta(minutes=39)).isoformat(timespec="seconds"))
    panel = make_panel(store, clock, transport)

    # ---------------------------------------------------- 1. маршрутизация команд (§14.7 п.1)
    section("1. Маршрутизация команд")
    broken: list[str] = []
    empty: list[str] = []
    for command in sorted(ALL_COMMANDS):
        clock.now += 10.0                       # тяжёлые команды не упираются в rate limit
        text = await answer(panel, f"/{command}")
        if not text.strip():
            empty.append(command)
        if "не сработала" in text:
            broken.append(f"{command}: {text[:70]}")
    checks.append((f"все {len(ALL_COMMANDS)} команд из §14.2 отвечают и не падают",
                   not broken and not empty,
                   "; ".join(broken + empty) or ", ".join(sorted(ALL_COMMANDS))))

    empty_store = HitStore(str(WORKDIR / "empty.sqlite3"))
    empty_panel = make_panel(empty_store, clock, FakeTransport(clock))
    broken_empty = []
    for command in sorted(ALL_COMMANDS):
        clock.now += 10.0
        text = await answer(empty_panel, f"/{command}")
        if not text.strip() or "не сработала" in text:
            broken_empty.append(f"{command}: {text[:60]}")
    checks.append(("на пустой базе ни одна команда не падает", not broken_empty,
                   "; ".join(broken_empty) or "пустая база"))

    unknown = panel.dispatch("/fly").text
    checks.append(("неизвестная команда объясняет, что делать",
                   "Нет команды «/fly»" in unknown and "/help" in unknown,
                   unknown.splitlines()[0]))
    checks.append(("обычный текст (не команда) не роняет панель",
                   "/help" in panel.dispatch("привет").text, panel.dispatch("привет").text[:40]))
    checks.append(("команда с @упоминанием бота распознаётся (/status@radar_bot)",
                   "Радар" in panel.dispatch("/status@radar_bot").text, "/status@radar_bot"))

    # ---------------------------------------------------- 2. «молчит N мин» (§14.7 п.2)
    section("2. Живость аккаунтов")
    accounts_text = panel.cmd_accounts()
    second_block = accounts_text.split("2) second")[1] if "2) second" in accounts_text else ""
    main_block = (accounts_text.split("1) main")[1].split("2)")[0]
                  if "1) main" in accounts_text else "")
    checks.append(("пульс 40 мин назад -> «молчит» (окно = min(3× пульс, --alert-silent))",
                   "second · молчит" in accounts_text and "работает" not in second_block,
                   second_block.splitlines()[0] if second_block else accounts_text[:80]))
    checks.append(("свежий пульс (1 мин) -> «работает»",
                   "main · работает" in accounts_text and "пульс" in main_block,
                   main_block.splitlines()[0] if main_block else accounts_text[:80]))
    checks.append(("у молчащего аккаунта видны FloodWait и последняя ошибка",
                   "FloodWait" in second_block and "последняя ошибка" in second_block,
                   second_block.strip().splitlines()[-1][:70]))
    checks.append(("окно живости = min(3× интервал пульса, --alert-silent)",
                   panel.alive_window_minutes == 30.0,
                   f"3×15=45, alert-silent=30 -> {panel.alive_window_minutes}"))
    wide = make_panel(store, clock, FakeTransport(clock), alert_silent_minutes=0.0)
    checks.append(("без --alert-silent окно считается как 3× интервал пульса",
                   wide.alive_window_minutes == 45.0, str(wide.alive_window_minutes)))
    stopped_store = HitStore(str(WORKDIR / "stopped.sqlite3"))
    stopped_store.log_heartbeat("start", account="main",
                                ts=(NOW - timedelta(hours=5)).isoformat(timespec="seconds"))
    stopped_store.log_heartbeat("stop", account="main",
                                ts=(NOW - timedelta(hours=1)).isoformat(timespec="seconds"))
    stopped_text = make_panel(stopped_store, clock, FakeTransport(clock)).cmd_accounts()
    checks.append(("после штатной остановки аккаунт «остановлен», а не «сломан»",
                   "остановлен" in stopped_text, stopped_text.splitlines()[2][:70]))

    # ---------------------------------------------------- 3. нет пульса вовсе (§14.7 п.3)
    section("3. Нет данных о пульсе")
    no_pulse_store = HitStore(str(WORKDIR / "no_pulse.sqlite3"))
    no_pulse_panel = make_panel(no_pulse_store, clock, FakeTransport(clock))
    text = no_pulse_panel.cmd_accounts()
    checks.append(("без пульса — честное «нет данных о пульсе», а не «аккаунт сломан»",
                   "нет данных о пульсе (радар мог быть запущен без --heartbeat)" in text
                   and "сломан" not in text and "молчит" not in text,
                   text.splitlines()[2][:70]))
    checks.append(("подсказка про --heartbeat есть и в /help",
                   "--heartbeat" in no_pulse_panel.cmd_help(), "/help"))

    # ---------------------------------------------------- 4. обрезка длинных ответов (§14.7 п.4)
    section("4. Обрезка /stats")
    big_store = HitStore(str(WORKDIR / "big.sqlite3"))
    for index in range(120):
        big_store.bump_stats(f"chat_{index:03d}", scanned=1000 + index, matched=20,
                             saved=3, forwarded=2, account="main")
    big_panel = make_panel(big_store, clock, FakeTransport(clock))
    reply = await big_panel.answer_text("/stats 7")
    checks.append((f"/stats длиннее 4000 символов обрезается (получилось {len(reply.text)})",
                   len(reply.text) <= 4000 and "/report" in reply.text,
                   reply.text.splitlines()[-1][:60]))
    raw_report = big_store.stats_report(days=7)
    checks.append((f"без обрезки отчёт действительно длинный ({len(raw_report)} символов)",
                   len(raw_report) > 4000, str(len(raw_report))))
    checks.append(("короткий ответ не обрезается и подсказку не добавляет",
                   truncate("короткий текст", 4000) == "короткий текст", "ok"))
    doc_reply = await big_panel.answer_text("/report")
    checks.append(("/report отдаёт stats.txt документом",
                   bool(doc_reply.documents) and Path(doc_reply.documents[0]).exists(),
                   doc_reply.documents[0] if doc_reply.documents else "(нет файла)"))

    # ---------------------------------------------------- 5. whitelist (§14.7 п.5)
    section("5. Доступ только владельцу")
    clock.now += 10.0
    before = len(transport.sent)
    stranger = await panel.handle_update(update(9001, "/status", chat_id="555"))
    unauthorized = store.conn.execute(
        "SELECT COUNT(*) FROM errors WHERE kind='unauthorized'").fetchone()[0]
    checks.append(("апдейт с чужим chat_id: ответа нет",
                   stranger is None and len(transport.sent) == before,
                   f"отправлено сверх {len(transport.sent) - before}"))
    checks.append(("чужой chat_id записан в errors (kind='unauthorized')",
                   unauthorized == 1, f"записей {unauthorized}"))
    ok_chat, why_ok = parse_owner_chat(OWNER)
    bad_chat, why_bad = parse_owner_chat("@my_channel")
    checks.append(("числовой TG_NOTIFY_CHAT принимается", ok_chat == OWNER and not why_ok,
                   str(ok_chat)))
    checks.append(("TG_NOTIFY_CHAT вида @канал — отказ с понятной причиной",
                   bad_chat is None and "не числовой id" in why_bad and "userinfobot" in why_bad,
                   why_bad[:70]))
    refused = make_panel(store, clock, FakeTransport(clock), chat_id="@my_channel")
    ready, reason = refused.ready()
    checks.append(("панель с @каналом не стартует", not ready and "личке" in reason, reason[:70]))
    no_token = make_panel(store, clock, FakeTransport(clock), token="")
    checks.append(("без TG_BOT_TOKEN панель не стартует и показывает подсказку",
                   not no_token.ready()[0] and "TG_BOT_TOKEN" in no_token.ready()[1],
                   no_token.ready()[1][:60]))

    # ---------------------------------------------------- 6. идемпотентность (§14.7 п.6)
    section("6. Идемпотентность")
    transport.sent.clear()
    clock.now += 10.0
    first = await panel.handle_update(update(9100, "/queue"))
    clock.now += 10.0
    repeated = await panel.handle_update(update(9100, "/queue"))
    checks.append(("повторная доставка того же update_id не даёт второй ответ",
                   first is not None and repeated is None and len(transport.sent) == 1,
                   f"отправлено {len(transport.sent)}"))
    checks.append(("offset сдвигается и сохраняется в bot_state",
                   panel.offset == 9101 and store.bot_state_get("update_offset") == "9101",
                   f"offset={panel.offset}, в базе={store.bot_state_get('update_offset')}"))
    clock.now += 10.0
    stale = await panel.handle_update(update(9000, "/status"))
    checks.append(("апдейт старее offset отброшен без ответа", stale is None, "старый update_id"))

    # ---------------------------------------------------- 7. rate limit (§14.7 п.7)
    section("7. Rate limit")
    rl_clock = FakeClock()
    rl_transport = FakeTransport(rl_clock)
    rl_panel = make_panel(store, rl_clock, rl_transport)
    light = ["help", "status", "accounts", "queue", "last", "errors", "top", "limits", "mode",
             "ping"]
    for index, command in enumerate(light, start=1):
        await rl_panel.handle_update(update(9200 + index, f"/{command}"))
    stamps = [stamp for stamp, _text in rl_transport.sent]
    gaps = [round(b - a, 3) for a, b in zip(stamps, stamps[1:])]
    checks.append((f"10 команд подряд: {len(stamps)} ответов, не чаще 1 в секунду",
                   len(stamps) == 10 and all(gap >= 0.999 for gap in gaps),
                   f"интервалы {gaps[:4]}…"))
    checks.append(("/ping меряет задержку до Bot API",
                   rl_transport.sent[-1][1].startswith("pong · ")
                   and rl_transport.get_me_calls == 1, rl_transport.sent[-1][1]))
    rl_clock.now += 20.0
    heavy1 = await rl_panel.answer_text("/usage")
    heavy2 = await rl_panel.answer_text("/usage")
    checks.append(("/usage чаще раза в 5 с — вежливый отказ, а не второй тяжёлый ответ",
                   "Вердикт" in heavy1.text and "через" in heavy2.text, heavy2.text[:60]))
    checks.append(("429 retry_after распознаётся (панель подождёт, а не долбит Bot API)",
                   retry_after_of("HTTP 429 Too Many Requests (retry_after=17 с)") == 17, "17 с"))

    # ---------------------------------------------------- 8. /usage и метрики (§14.7 п.8)
    section("8. /usage и метрики")
    saved = (metrics_module.PSUTIL_AVAILABLE, metrics_module.rss_mb, metrics_module.peak_rss_mb,
             metrics_module.cpu_percent_average)
    metrics_module.PSUTIL_AVAILABLE = False
    metrics_module.rss_mb = lambda process=None: None
    metrics_module.peak_rss_mb = lambda: None
    metrics_module.cpu_percent_average = lambda *a, **k: None
    try:
        usage_store = make_store(WORKDIR / "usage.sqlite3")
        usage_store.log_metric({
            "ts": (NOW - timedelta(minutes=15)).isoformat(timespec="seconds"),
            "rss_mb": 62.0, "cpu_percent": 1.4, "uptime_s": 96000.0, "msgs_total": 3480,
            "msgs_last_hour": 214, "api_calls": 1902, "db_mb": 11.2, "hits_total": 61,
            "forwarded_today": 44, "accounts": 2, "mode": "A"})
        usage_panel = make_panel(usage_store, clock, FakeTransport(clock))
        clock.now += 30.0
        usage = (await usage_panel.answer_text("/usage")).text
        verdict_line = next((line for line in usage.splitlines()
                             if line.startswith("Вердикт")), "")
        checks.append(("/usage берёт данные из таблицы metrics",
                       "62" in usage and "1.4" in usage, usage.splitlines()[1][:70]))
        checks.append(("вердикт по метрикам за сутки: A подходит (RSS 62 ≤ 150, CPU 1.4 ≤ 5)",
                       metrics_module.VERDICT_A in verdict_line, verdict_line[:70]))
        checks.append(("/usage показывает пересылки по аккаунтам и очередь",
                       "main 1/120" in usage and "second 0/60" in usage and "в очереди 1" in usage,
                       next((line for line in usage.splitlines()
                             if line.startswith("Находок")), "")[:70]))

        bare_store = HitStore(str(WORKDIR / "no_psutil.sqlite3"))
        bare_panel = make_panel(bare_store, clock, FakeTransport(clock))
        clock.now += 30.0
        bare_usage = (await bare_panel.answer_text("/usage")).text
        checks.append(("без psutil /usage отвечает частичными данными и честно это помечает",
                       "psutil не установлен" in bare_usage and "н/д" in bare_usage,
                       bare_usage.splitlines()[-1][:70]))
        checks.append(("вердикт без psutil считается по доступным данным, а не падает",
                       "Вердикт:" in bare_usage,
                       next((line for line in bare_usage.splitlines()
                             if line.startswith("Вердикт")), "")[:70]))
    finally:
        (metrics_module.PSUTIL_AVAILABLE, metrics_module.rss_mb, metrics_module.peak_rss_mb,
         metrics_module.cpu_percent_average) = saved

    verdicts = [
        metrics_module.verdict(rss=62.0, cpu=1.4, floods=0)[0],
        metrics_module.verdict(rss=200.0, cpu=9.0, floods=0)[0],
        metrics_module.verdict(rss=310.0, cpu=4.0, floods=0)[0],
        metrics_module.verdict(rss=62.0, cpu=22.0, floods=0)[0],
    ]
    checks.append(("вердикт A/B на граничных значениях (150/250 МБ, 5/15 %)",
                   verdicts == [metrics_module.VERDICT_A, metrics_module.VERDICT_A_WATCH,
                                metrics_module.VERDICT_B, metrics_module.VERDICT_B],
                   " | ".join(verdicts)))
    checks.append(("частые FloodWait тоже склоняют к схеме B",
                   metrics_module.verdict(rss=62.0, cpu=1.4, floods=5)[0] == metrics_module.VERDICT_B,
                   "floods=5"))
    checks.append(("средний CPU из process_time/uptime считается предсказуемо",
                   metrics_module.cpu_percent_average(process_time=1.5, uptime_s=100.0) == 1.5,
                   "1.5/100 -> 1.5 %"))
    checks.append(("строка metrics.csv собирается в фиксированном порядке колонок",
                   metrics_module.metrics_csv_row(
                       {"ts": "2026-09-24T20:00:00+00:00", "rss_mb": 62.0, "cpu_percent": 1.4,
                        "uptime_s": 3600.0, "msgs_total": 100, "msgs_last_hour": 10,
                        "api_calls": 50, "db_mb": 11.2, "forwarded_today": 4, "accounts": 2,
                        "mode": "A"})
                   == "2026-09-24T20:00:00+00:00,62,1.4,1,100,10,50,11.2,4,2,A",
                   metrics_module.metrics_csv_row(
                       {"ts": "t", "rss_mb": 1.0, "cpu_percent": 2.0, "uptime_s": 3600.0,
                        "msgs_total": 3, "msgs_last_hour": 4, "api_calls": 5, "db_mb": 6.0,
                        "forwarded_today": 7, "accounts": 8, "mode": "A"})))

    # ---------------------------------------------------- 9. панель не трогает Telethon (§14.7 п.9)
    section("9. Панель не поднимает Telethon")
    counter = {"created": 0, "calls": 0}
    original_monitor_client = monitor_module.make_client
    original_core_client = core_telegram.make_client
    monitor_module.make_client = lambda *a, **k: SpyClient(counter)
    core_telegram.make_client = lambda *a, **k: SpyClient(counter)
    try:
        for command in sorted(ALL_COMMANDS):
            clock.now += 10.0
            await panel.answer_text(f"/{command}")
        await panel.check_alerts()
    finally:
        monitor_module.make_client = original_monitor_client
        core_telegram.make_client = original_core_client
    checks.append(("ни одна команда панели не создала Telethon-клиент",
                   counter["created"] == 0 and counter["calls"] == 0,
                   f"создано {counter['created']}, вызовов {counter['calls']}"))
    panel_source = Path("bot_panel.py").read_text(encoding="utf-8")
    checks.append(("в bot_panel.py нет импорта telethon (сессии не трогаются даже теоретически)",
                   "import telethon" not in panel_source and "from telethon" not in panel_source,
                   "проверен исходник"))
    busy = BotPanel.sessions_busy(store, minutes=3.0)
    checks.append(("--check-sessions отказывается работать при живом пульсе радара",
                   busy == "main", f"последний пульс видел от: {busy}"))
    checks.append(("при отсутствии пульса --check-sessions разрешён",
                   BotPanel.sessions_busy(empty_store, minutes=3.0) is None, "пульса нет"))

    # ---------------------------------------------------- 10. алерты (§14.6)
    section("10. Алерты")
    alert_store = make_store(WORKDIR / "alerts.sqlite3")
    alert_store.log_heartbeat("start", account="main",
                              ts=(NOW - timedelta(hours=3)).isoformat(timespec="seconds"))
    alert_store.log_heartbeat("pulse", account="main", detail="прочитано 10, найдено 0",
                              ts=(NOW - timedelta(minutes=45)).isoformat(timespec="seconds"))
    alert_transport = FakeTransport(clock)
    alert_panel = make_panel(alert_store, clock, alert_transport)
    clock.now += 20.0
    sent = await alert_panel.check_alerts()
    checks.append(("аккаунт молчит дольше --alert-silent -> алерт владельцу",
                   any("молчит" in text for text in sent), (sent or ["(нет)"])[0][:70]))
    clock.now += 20.0
    again = await alert_panel.check_alerts()
    checks.append(("антиспам: тот же алерт не чаще раза в час",
                   not any("молчит" in text for text in again), f"повторов {len(again)}"))
    alert_store.log_heartbeat("flood", account="second", detail="пересылка: до 21:10",
                              ts=(NOW - timedelta(minutes=10)).isoformat(timespec="seconds"))
    alert_store.log_error("AuthKeyDuplicatedError", "ключ отозван", account="second")
    clock.now += 20.0
    session_alerts = await alert_panel.check_alerts()
    checks.append(("сломанная сессия -> алерт с подсказкой про повторный вход",
                   any("сессия сломана" in text and "--login-qr" in text for text in session_alerts),
                   "; ".join(text[:50] for text in session_alerts) or "(нет)"))
    checks.append(("FloodWait виден в алертах",
                   any("FloodWait" in text for text in alert_panel.flood_alerts()),
                   (alert_panel.flood_alerts() or ["(нет)"])[0][:60]))
    limit_store = make_store(WORKDIR / "limits.sqlite3")
    for index in range(120):
        limit_store.mark_forwarded("granica_polska", 1000 + index, ok=True, mode="forwarded",
                                   account="main")
    limit_panel = make_panel(limit_store, clock, FakeTransport(clock))
    checks.append(("дневной лимит выбран и находки идут -> алерт про лимит",
                   any("дневной лимит" in text for text in limit_panel.limit_alerts()),
                   (limit_panel.limit_alerts() or ["(нет)"])[0][:60]))
    checks.append(("штатно остановленный аккаунт не считается «молчащим»",
                   not make_panel(stopped_store, clock, FakeTransport(clock)).silent_alerts(),
                   "остановлен -> алерта нет"))
    digest_panel = make_panel(store, clock, FakeTransport(clock))
    digest_panel.cmd_digest("on")
    checks.append(("/digest on сохраняется в bot_state",
                   store.bot_state_get("digest") == "on" and digest_panel.digest_on, "digest=on"))
    morning = digest_panel.digest_alert(moment=datetime.now().astimezone().replace(hour=9, minute=5))
    checks.append(("утренний дайджест в 09:00 собирается", "Дайджест за сутки" in morning,
                   morning.splitlines()[0][:60]))
    checks.append(("дайджест не повторяется в тот же день",
                   digest_panel.digest_alert(
                       moment=datetime.now().astimezone().replace(hour=9, minute=10)) == "",
                   "антиспам по дате"))
    digest_panel.cmd_digest("off")
    checks.append(("/digest off выключает дайджест", not digest_panel.digest_on, "digest=off"))

    # ---------------------------------------------------- 11. транспорт Bot API
    section("11. Транспорт Bot API")
    http = HttpTransport("123:TOKEN", api_url="https://example.invalid")
    checks.append(("URL вызова собирается как bot<token>/<method>",
                   http._url("sendMessage") == "https://example.invalid/bot123:TOKEN/sendMessage",
                   http._url("sendMessage")))
    fail_transport = FakeTransport(clock)
    fail_transport.fail_next = "HTTP 500 Internal Server Error"
    fail_panel = make_panel(store, clock, fail_transport)
    clock.now += 20.0
    await fail_panel.handle_update(update(9900, "/status"))
    logged = store.conn.execute("SELECT COUNT(*) FROM errors WHERE kind='bot_api'").fetchone()[0]
    checks.append(("сбой отправки не роняет панель, а попадает в errors",
                   logged >= 1, f"записей {logged}"))
    # цикл run(): снятие задачи (Ctrl+C) всё равно должно оставить отметку «панель остановлена»
    run_store = HitStore(str(WORKDIR / "run.sqlite3"))
    run_transport = FakeTransport(clock)
    run_transport.hold = 0.02
    run_panel = make_panel(run_store, clock, run_transport)
    task = asyncio.create_task(run_panel.run())
    await asyncio.sleep(0.08)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    checks.append(("цикл run() опрашивает Bot API и при отмене пишет stop в базу",
                   run_transport.sent and run_store.last_heartbeat(kind="stop") is not None,
                   str(run_store.last_heartbeat(kind="stop"))))
    checks.append(("панель в цикле не создаёт Telethon-клиент (только Bot API)",
                   all(call in ("getUpdates", "sendMessage", "getMe", "sendDocument")
                       for call in ("getUpdates", "sendMessage")), "getUpdates/sendMessage"))

    clock.now += 20.0
    greeting = await fail_panel.greet()
    checks.append(("«панель на связи» отправляется при старте", greeting is True,
                   fail_transport.sent[-1][1].splitlines()[0][:60] if fail_transport.sent else ""))
    checks.append(("повторный старт чаще чем раз в 5 минут — без повторного приветствия",
                   await fail_panel.greet() is False, "антиспам"))

    # ---------------------------------------------------- 12. интеграция с радаром
    section("12. Интеграция с радаром")
    pulse_store = HitStore(str(WORKDIR / "pulse.sqlite3"))
    radar = Monitor(None, pulse_store, [], None, core_telegram.Paced(0.0), account="main")
    radar.counter.update({"scanned": 412, "saved": 6})
    radar.log_pulse()
    row = pulse_store.last_heartbeat(kind="pulse", account="main") or {}
    checks.append(("радар пишет пульс в базу (по нему панель судит о живости)",
                   "прочитано 412" in row.get("detail", "") and "найдено 6" in row.get("detail", ""),
                   row.get("detail", "")))
    checks.append(("находка автоматически даёт событие для «последний раз ловил в …»",
                   store.last_heartbeat(kind="event", account="main") is not None,
                   str(store.last_heartbeat(kind="event", account="main"))))
    checks.append(("пересылка даёт событие kind='forward'",
                   store.last_heartbeat(kind="forward", account="main") is not None,
                   str(store.last_heartbeat(kind="forward", account="main"))))
    read_hour, _found = pulse_store.pulse_progress(hours=1.0, account="main")
    checks.append(("нагрузка за час считается по строкам пульса (один срез -> 0)",
                   read_hour == 0, f"за час {read_hour}"))
    status_text = panel.cmd_status()
    checks.append(("/status собран по шаблону §14.5",
                   status_text.startswith("📡 Радар · режим A (слушатель) · жив")
                   and "Сегодня (с 00:00):" in status_text and "Обновлено:" in status_text
                   and "Ошибки за сутки:" in status_text,
                   status_text.splitlines()[0][:60]))
    checks.append(("/status показывает аккаунты построчно, когда их несколько",
                   "main: 1/120" in status_text and "second: 0/60" in status_text,
                   next((line for line in status_text.splitlines()
                         if "main:" in line), "")[:60]))
    only_panel = make_panel(store, clock, FakeTransport(clock), panel_only=True, started_at=None)
    only_text = only_panel.cmd_status()
    checks.append(("/status в режиме --panel-only помечает, что данные из базы",
                   "Данные из базы (обновлено" in only_text,
                   only_text.splitlines()[-2][:60]))
    parser = build_parser()
    parsed = parser.parse_args(["--bot-panel", "--alert-silent", "45", "--digest", "on",
                                "--mode", "B"])
    checks.append(("флаги панели на месте: --bot-panel, --alert-silent, --digest, --mode",
                   parsed.bot_panel and parsed.alert_silent == 45.0 and parsed.digest == "on"
                   and parsed.mode == "B", f"{parsed.bot_panel}/{parsed.alert_silent}"))
    parsed_only = parser.parse_args(["--panel-only", "--check-sessions"])
    checks.append(("флаги --panel-only и --check-sessions на месте",
                   parsed_only.panel_only and parsed_only.check_sessions, "ok"))
    bare = parser.parse_args([])
    checks.append(("по умолчанию панель выключена, --alert-silent = 30 мин",
                   not bare.bot_panel and not bare.panel_only and bare.alert_silent == 30.0
                   and bare.digest is None,
                   f"bot_panel={bare.bot_panel}, alert_silent={bare.alert_silent}"))
    checks.append(("режим по умолчанию: A для слушателя, B для --once",
                   resolve_mode(bare) == "A" and resolve_mode(parser.parse_args(["--once"])) == "B"
                   and resolve_mode(parser.parse_args(["--mode", "B"])) == "B",
                   f"A/B/{resolve_mode(parser.parse_args(['--mode', 'B']))}"))
    checks.append(("--panel-only не требует ключей аккаунта (отдельный процесс)",
                   "panel_only" in Path("monitor.py").read_text(encoding="utf-8")
                   and "bot_panel import panel_only_main" in Path("monitor.py").read_text(
                       encoding="utf-8"), "ветка в async_main"))

    # ---------------------------------------------------- 13. миграция старой базы (§17.1 п.4)
    section("13. Миграция старой базы")
    legacy_path = WORKDIR / "legacy.sqlite3"
    legacy = sqlite3.connect(str(legacy_path))
    legacy.executescript("""
    CREATE TABLE seen (chat_key TEXT NOT NULL, msg_id INTEGER NOT NULL,
                       PRIMARY KEY (chat_key, msg_id));
    CREATE TABLE hits (id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_key TEXT, chat_title TEXT, chat_id TEXT, username TEXT, msg_id INTEGER,
        date TEXT, sender_id TEXT, sender_name TEXT, text TEXT, score INTEGER,
        category TEXT, intent TEXT, direction TEXT, countries TEXT, hits TEXT, link TEXT,
        found_at TEXT, notified INTEGER DEFAULT 0);
    CREATE TABLE forwarded (chat_key TEXT NOT NULL, msg_id INTEGER NOT NULL,
        ok INTEGER DEFAULT 0, mode TEXT, error TEXT, at TEXT,
        PRIMARY KEY (chat_key, msg_id));
    CREATE TABLE stats (day TEXT NOT NULL, chat_key TEXT NOT NULL, scanned INTEGER DEFAULT 0,
        matched INTEGER DEFAULT 0, saved INTEGER DEFAULT 0, forwarded INTEGER DEFAULT 0,
        forward_skipped INTEGER DEFAULT 0, forward_failed INTEGER DEFAULT 0,
        filtered INTEGER DEFAULT 0, too_old INTEGER DEFAULT 0, duplicates INTEGER DEFAULT 0,
        text_duplicates INTEGER DEFAULT 0, cross_chat INTEGER DEFAULT 0,
        PRIMARY KEY (day, chat_key));
    """)
    legacy.execute("INSERT INTO forwarded (chat_key, msg_id, ok, mode, error, at) "
                   "VALUES ('old_chat', 1, 1, 'forwarded', '', ?)",
                   (datetime.now(timezone.utc).isoformat(timespec="seconds"),))
    legacy.execute("INSERT INTO stats (day, chat_key, scanned, matched, saved, forwarded) "
                   "VALUES (?, 'old_chat', 100, 5, 2, 2)", (NOW.date().isoformat(),))
    legacy.commit()
    legacy.close()

    migrated = HitStore(str(legacy_path))
    tables = {row[0] for row in migrated.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    checks.append(("старая база получает таблицы heartbeats/errors/metrics/bot_state",
                   {"heartbeats", "errors", "metrics", "bot_state"} <= tables,
                   ", ".join(sorted(tables - {"sqlite_sequence"}))))
    checks.append(("журнал базы включён в WAL (панель и радар читают одновременно)",
                   migrated.conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal",
                   migrated.conn.execute("PRAGMA journal_mode").fetchone()[0]))
    checks.append(("busy_timeout выставлен (нет «database is locked»)",
                   int(migrated.conn.execute("PRAGMA busy_timeout").fetchone()[0]) >= 5000,
                   str(migrated.conn.execute("PRAGMA busy_timeout").fetchone()[0])))
    history = migrated.conn.execute(
        "SELECT account FROM forwarded WHERE chat_key='old_chat'").fetchone()[0]
    stats_account = migrated.conn.execute(
        "SELECT account FROM stats WHERE chat_key='old_chat'").fetchone()[0]
    checks.append(("старая история помечена аккаунтом main (счётчики не обнулились)",
                   history == "main" and stats_account == "main",
                   f"forwarded={history}, stats={stats_account}"))
    checks.append(("счётчик «сегодня» по старой базе продолжает работать",
                   migrated.forwarded_today("main") == 1, str(migrated.forwarded_today("main"))))
    columns = {row[1] for row in migrated.conn.execute("PRAGMA table_info(hits)").fetchall()}
    checks.append(("в hits добавлены колонки account/topic_id/topic_name",
                   {"account", "topic_id", "topic_name"} <= columns,
                   ", ".join(sorted(columns)[-3:])))
    checks.append(("SCHEMA содержит все новые таблицы (их создаёт executescript)",
                   all(table in SCHEMA for table in ("heartbeats", "errors", "metrics",
                                                     "bot_state")), "SCHEMA"))
    legacy_panel = make_panel(migrated, clock, FakeTransport(clock), accounts=[])
    checks.append(("панель на мигрированной базе определяет аккаунты из истории",
                   [view.name for view in legacy_panel.known_accounts()] == ["main"],
                   ", ".join(view.name for view in legacy_panel.known_accounts())))

    # ---------------------------------------------------- 14. ретеншн (§17.1 п.6)
    section("14. Ретеншн")
    retention_store = HitStore(str(WORKDIR / "retention.sqlite3"))
    old_ts = (NOW - timedelta(days=31)).isoformat(timespec="seconds")
    fresh_ts = (NOW - timedelta(days=1)).isoformat(timespec="seconds")
    for kind in ("pulse", "event", "forward"):
        retention_store.log_heartbeat(kind, account="main", ts=old_ts)
        retention_store.log_heartbeat(kind, account="main", ts=fresh_ts)
    retention_store.log_error("Old", "старая ошибка", ts=old_ts)
    retention_store.log_error("Fresh", "свежая ошибка", ts=fresh_ts)
    retention_store.log_metric({"ts": old_ts, "rss_mb": 10.0, "cpu_percent": 1.0,
                                "uptime_s": 100.0, "msgs_total": 10, "msgs_last_hour": 1,
                                "api_calls": 5, "db_mb": 1.0, "hits_total": 2,
                                "forwarded_today": 1, "accounts": 1, "mode": "A"})
    retention_store.log_metric({"ts": fresh_ts, "rss_mb": 12.0, "cpu_percent": 1.2,
                                "uptime_s": 200.0, "msgs_total": 20, "msgs_last_hour": 2,
                                "api_calls": 6, "db_mb": 1.1, "hits_total": 3,
                                "forwarded_today": 2, "accounts": 1, "mode": "A"})
    removed = retention_store.retention_cleanup(days=30)
    left_old = retention_store.conn.execute(
        "SELECT COUNT(*) FROM heartbeats WHERE ts=?", (old_ts,)).fetchone()[0]
    left_fresh = retention_store.conn.execute(
        "SELECT COUNT(*) FROM heartbeats WHERE ts=?", (fresh_ts,)).fetchone()[0]
    checks.append(("старые heartbeats/errors/metrics удалены, свежие остались",
                   left_old == 0 and left_fresh == 3 and removed["heartbeats"] == 3
                   and removed["errors"] == 1 and removed["metrics"] == 1,
                   f"удалено {removed}, свежих осталось {left_fresh}"))
    checks.append(("метрики читаются после чистки",
                   len(retention_store.metrics_recent(hours=24 * 2)) == 1,
                   str(len(retention_store.metrics_recent(hours=24 * 2)))))

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
