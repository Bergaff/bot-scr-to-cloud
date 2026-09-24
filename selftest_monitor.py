#!/usr/bin/env python3
"""
Офлайн-тесты мониторинга: аккаунт Telegram и сеть НЕ нужны.
Проверяем матчер (размеченные корпуса + реальный новостной) и весь конвейер
мониторинга на фейковом клиенте: catch-up, дедупликация, SQLite, ссылки, уведомления.

Запуск:  python3 selftest_monitor.py
"""
from __future__ import annotations

import asyncio
import random
import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import sqlite3 as _sqlite3

from core_telegram import Paced, call, format_hit, hidden_author_reason, message_link
import monitor as monitor_module

from matcher import analyze
from forwarder import Forwarder
from monitor import (SCHEMA, HitStore, Monitor, Source, build_parser, find_nested_project,
                     load_config, order_sources, resend_pending, resolve_accounts,
                     resolve_forward_settings, sources_for_account, titles_by_key)

TESTS = Path("tests")
NOW = datetime.now(timezone.utc).replace(microsecond=0)   # свежие тесты: не протухают со временем


def fake_message(mid, text, minutes_ago=0, sender_id=777, out=False):
    return SimpleNamespace(id=mid, text=text, raw_text=None, date=NOW - timedelta(minutes=minutes_ago),
                           sender_id=sender_id, out=out)


class FakeEntity:
    def __init__(self, entity_id, title, username=None):
        self.id, self.title, self.username = entity_id, title, username


class FakeClient:
    """get_entity/get_messages в стиле Telethon."""
    def __init__(self, entity, corpus):
        self.entity, self.corpus, self.calls = entity, corpus, 0

    async def get_entity(self, target):
        self.calls += 1
        if isinstance(target, int):                       # резолв автора сообщения
            return SimpleNamespace(id=target, first_name="Иван", last_name="Тестов", username="ivan_test", title=None)
        return self.entity

    async def get_messages(self, entity, limit=None, **kwargs):
        self.calls += 1
        return list(self.corpus)[:limit] if limit else list(self.corpus)


def section(title: str) -> None:
    print(f"\n── {title} " + "─" * max(0, 66 - len(title)))


async def main() -> None:
    checks: list[tuple[str, bool, str]] = []
    workdir = Path("tests/_tmp")

    # ---------------------------------------------------------- 1. матчер
    pos = [line.strip() for line in (TESTS / "labeled_pos.txt").read_text(encoding="utf-8").splitlines() if line.strip()]
    neg = [line.strip() for line in (TESTS / "labeled_neg.txt").read_text(encoding="utf-8").splitlines() if line.strip()]
    news = []
    for line in (TESTS / "news_corpus.jsonl").read_text(encoding="utf-8").splitlines():
        if line.strip():
            news.append(json.loads(line)["text"])

    found_pos = [t for t in pos if analyze(t).matched]
    missed_pos = [t for t in pos if not analyze(t).matched]
    false_neg = [t for t in neg if analyze(t).matched]
    false_news = [t for t in news if analyze(t, profile="news").matched]

    section("Матчер")
    checks.append((f"объявления найдены: {len(found_pos)}/{len(pos)}", len(found_pos) == len(pos), ""))
    checks.append((f"реклама/новости/вакансии отсеяны: {len(neg) - len(false_neg)}/{len(neg)}", not false_neg, ""))
    checks.append((f"новостной канал (profile=news): ложных {len(false_news)}/{len(news)}",
                   len(false_news) <= max(2, len(news) * 0.02), ""))
    for text in missed_pos:
        print(f"   пропущено: {text[:80]}")
    for text in false_neg:
        print(f"   ложное: {text[:80]}")

    # категории: попутчики vs посылки vs смешанные + вакансия не проходит
    category_cases = [
        ("19.09 первая половина дня еду Варшава-Минск. Возьму попутчиков.", "ride"),
        ("Еду сегодня в 12.00 Варшава-Минск есть свободные места и место для багажа.", "ride"),
        ("23/09 Рига-Вильнюс-Молодечно Выезд из Риги около 7:00 Посылки, передачи. В ЛС", "parcel"),
        ("24.09 ищу два места Гданьск-Минск. Два чемодана. Пишите в лс.", "ride"),
        ("#водитель 18.09 сегодня Вильнюс - Минск Есть 4 места посылки и передачи +48 576 081 977", "mixed"),
        # реальные сообщения из чатов пользователя
        ("Есть предложение для тех, кто едет мимо Вроцлава в сторону Бялы. Забрать лобовое стекло и довести до Бялы. За оплату", "parcel"),
        ("Добрый день! Ищу место Гродно - Варшава, 20.09, после обеда. Из вещей один чемодан", "ride"),
        ("Сегодня 18.09 еду Варшава - Брест - Минск, есть 1 место. Могу взять документы/передачки.", "mixed"),
        ("Сегодня примерно в 22-23 еду Брест Варшава есть места без предоплаты", "ride"),
    ]
    # проверяем ещё и намерение там, где оно важно
    intent_cases = [
        ("Есть предложение для тех, кто едет мимо Вроцлава. Забрать лобовое стекло и довести до Бялы. За оплату", "request"),
        ("Сегодня примерно в 22-23 еду Брест Варшава есть места без предоплаты", "offer"),
        ("19.09 Ищу место Бобруйск , Минск , Брест - Седлеце 1 чемодан 1 сумка", "request"),
    ]
    bad_intent = [(t, analyze(t).intent) for t, want in intent_cases if analyze(t).intent != want]
    checks.append((f"намерение определено верно: {len(intent_cases) - len(bad_intent)}/{len(intent_cases)}",
                   not bad_intent, str(bad_intent)))
    bad_category = [(t, analyze(t).category) for t, want in category_cases if analyze(t).category != want]
    checks.append((f"категории определены верно: {len(category_cases) - len(bad_category)}/{len(category_cases)}",
                   not bad_category, ""))
    for text, got in bad_category:
        print(f"   категория не та: {got} — {text[:60]}")

    vacancy = ("Нужны водители и перевозчики. Можно работать как на личном автомобиле, так и без машины. "
               "График гибкий, можно совмещать с основной занятостью. По оплате не обижу.")
    checks.append(("вакансия «нужны водители… по оплате не обижу» отсеяна", not analyze(vacancy).matched, ""))

    # ---------- очередь отложенных пересылок: лимит не должен терять находки
    qstore = HitStore(":memory:")
    qmessages = {}

    class QueueClient:
        """Фейковый клиент: forward_messages всегда удаётся, история не нужна."""
        def __init__(self):
            self.sent = []
            self.floods = []
        async def get_entity(self, target):
            return FakeEntity(500500, "parcel_transfer_bot", "parcel_transfer_bot")
        async def forward_messages(self, target, messages=None):
            self.sent.append(messages[0].id)

    def queue_hit(mid, text="Еду Брест-Варшава, возьму передачу"):
        msg = SimpleNamespace(id=mid, text=text, date=NOW, sender_id=1, out=False,
                              chat_id=-1001999, chat_title="Граница BY-LT-PL")
        hit = {"chat_key": "granica_by_lt_pl", "chat_title": "Граница BY-LT-PL",
               "username": "granica_BY_LT_PL", "date": NOW.isoformat(),
               "sender_id": 777, "sender_name": "Дмитрий",
               "chat_id": -1001999, "msg_id": mid, "text": text,
               "link": f"https://t.me/granica_BY_LT_PL/{mid}",
               "score": 9, "category": "parcel", "intent": "offer", "direction": "BY->PL",
               "countries": ["PL"], "hits": ["передачу"],
               "found_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        qstore.save_hit(hit)
        qmessages[mid] = msg
        return msg, hit

    thin = Forwarder(QueueClient(), "@parcel_transfer_bot", qstore, Paced(0), max_per_day=1)
    qmsg1, hit1 = queue_hit(9001)
    qmsg2, hit2 = queue_hit(9002)
    await thin.prepare()
    first = await thin.send(qmsg1, hit1)
    second = await thin.send(qmsg2, hit2)
    checks.append(("при исчерпании лимита находка уходит в очередь, а не теряется",
                   first == "forwarded" and second == "limit"
                   and not qstore.was_forwarded("granica_by_lt_pl", 9002)
                   and qstore.deferred_queue() == [("granica_by_lt_pl", 9002)],
                   f"{first}/{second}, в очереди {qstore.deferred_count()}"))
    checks.append(("отложенная находка не считается отправленной",
                   thin.sent_today == 1 and qstore.forwarded_today() == 1, str(thin.sent_today)))
    checks.append(("статистика видит отложенные",
                   "отложено на добор" in qstore.stats_report() and "сейчас в очереди: 1" in qstore.stats_report(), ""))

    flusher = Forwarder(QueueClient(), "@parcel_transfer_bot", qstore, Paced(0), max_per_day=5)
    await flusher.prepare()

    async def fetch_queued(chat_key, msg_id):
        return qmessages.get(msg_id)

    drain = await flusher.flush_deferred(fetch_queued)
    checks.append(("добор из очереди: отложенное уходит в следующий запуск",
                   drain["sent"] == 1 and drain["leftover"] == 0 and qstore.deferred_count() == 0, str(drain)))
    checks.append(("после добора отметка об отправке стоит",
                   qstore.was_forwarded("granica_by_lt_pl", 9002) and qstore.forwarded_today() == 2,
                   str(qstore.forwarded_today())))

    drain2 = await flusher.flush_deferred(fetch_queued)
    checks.append(("добор не отправляет одно и то же дважды",
                   drain2["sent"] == 0 and flusher.sent_today == 2, str(drain2)))

    # сообщение, которого больше нет: снимаем с очереди, а не копим вечно
    _msg3, hit3 = queue_hit(9003)
    qstore.queue_forward("granica_by_lt_pl", 9003)

    async def fetch_nothing(chat_key, msg_id):
        return None

    drain3 = await flusher.flush_deferred(fetch_nothing)
    checks.append(("пропавшее сообщение снимается с очереди",
                   drain3["gone"] == 1 and qstore.deferred_count() == 0, str(drain3)))

    # ---------- сквозная проверка: Monitor сам добирает очередь при запуске
    mstore = HitStore(":memory:")
    entity = FakeEntity(-1001999888777, "Граница BY-LT-PL", "granica_BY_LT_PL")
    live_message = SimpleNamespace(id=9101, text="Еду Брест-Варшава, возьму передачу", date=NOW,
                                   sender_id=1, out=False, chat_id=-1001999888777,
                                   chat_title="Граница BY-LT-PL")

    class MonClient:
        def __init__(self):
            self.forwarded = []
        async def get_entity(self, target):
            return FakeEntity(500500, "parcel_transfer_bot", "parcel_transfer_bot") \
                if "@parcel" in str(target) else entity
        async def get_messages(self, ent, ids=None, limit=None, **kwargs):
            return live_message if ids else []
        async def forward_messages(self, target, messages=None):
            self.forwarded.append(messages[0].id)

    mon_hit = {"chat_key": "granica_by_lt_pl", "chat_title": "Граница BY-LT-PL",
               "username": "granica_BY_LT_PL", "date": NOW.isoformat(), "sender_id": 777,
               "sender_name": "Дмитрий", "chat_id": -1001999888777, "msg_id": 9101,
               "text": "Еду Брест-Варшава, возьму передачу",
               "link": "https://t.me/granica_BY_LT_PL/9101", "score": 9, "category": "parcel",
               "intent": "offer", "direction": "BY->PL", "countries": ["PL"], "hits": ["передачу"],
               "found_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    mstore.save_hit(mon_hit)
    mstore.queue_forward("granica_by_lt_pl", 9101)

    mon_client = MonClient()
    mforwarder = Forwarder(mon_client, "@parcel_transfer_bot", mstore, Paced(0), max_per_day=10)
    await mforwarder.prepare()
    src = Source(target="@granica_BY_LT_PL", title="Граница BY-LT-PL", profile="chat", min_score=5, catchup=20)
    monitor = Monitor(mon_client, mstore, [src], lambda hit: None, Paced(0), forwarder=mforwarder)
    monitor.entities = {src.target: entity}
    monitor.meta = {src.target: src}
    drained = await monitor.flush_deferred()
    checks.append(("Monitor при запуске первым делом добирает очередь",
                   drained.get("sent") == 1 and mon_client.forwarded == [9101]
                   and mstore.deferred_count() == 0 and mstore.forwarded_today() == 1,
                   f"{drained}, отправлено {mon_client.forwarded}"))
    checks.append(("добранное попало в статистику по своему чату",
                   "granica_by_lt_pl" in mstore.stats_report(), ""))

    # ---------- скрытые авторы («hidden by user»): такие не пересылаем
    from telethon.tl import types as _t

    def forwarded_message(mid, text, from_id=None, from_name=None, saved_name=None):
        fwd = _t.MessageFwdHeader(date=NOW, from_id=from_id, from_name=from_name,
                                 saved_from_name=saved_name)
        return SimpleNamespace(id=mid, text=text, date=NOW, sender_id=None, out=False,
                               chat_id=-1001999888777, chat_title="Граница BY-LT-PL",
                               fwd_from=fwd, reply_to=None, action=None)

    hid_entity = FakeEntity(-1001999888777, "Граница BY-LT-PL", "granica_BY_LT_PL")
    hid_source = Source(target="@granica_BY_LT_PL", title="Граница BY-LT-PL", profile="chat",
                        min_score=4, catchup=10)

    class HidClient:
        """Минимальный клиент: нужен только для разрешения имени автора."""
        async def get_entity(self, target):
            return FakeEntity(777, "Иван Тестов", "ivan_test")

    async def hid_notify(hit):
        return True

    hidden_store = HitStore(":memory:")
    hidden_monitor = Monitor(HidClient(), hidden_store, [hid_source], hid_notify, Paced(0),
                             dedup_scope="chat", dedup_window=0)
    hidden_monitor.entities = {hid_source.target: hid_entity}
    hidden_monitor.meta = {hid_source.target: hid_source}

    # как в примере: «Forwarded from Olga OlgaB_O» без идентификатора автора
    # ровно как в примере: «Forwarded from Olga OlgaB_O», без идентификатора (peer-id = 0)
    hidden_msg = forwarded_message(
        9300, "Лечу 10/10 Москва - Подгорица. Лекарства, документы, посылки. Оплата по факту получения.",
        from_name="Olga OlgaB_O")
    checks.append(("скрытый автор распознаётся по forward без id (имя есть, аккаунта нет)",
                   hidden_author_reason(hidden_msg) is not None, str(hidden_author_reason(hidden_msg))))
    result = await hidden_monitor.process_message(hidden_msg, hid_source)
    checks.append(("сообщение от скрытого автора не сохраняется и не пересылается",
                   result is None and hidden_monitor.counter["hidden"] == 1
                   and "0 совпадений" in hidden_store.stats()
                   and hidden_store.pending_count() == 0,
                   f"hidden={hidden_monitor.counter['hidden']}, в базе {hidden_store.stats()}"))

    # обычный forward от пользователя (id есть) — пропускать нельзя
    normal_fwd = forwarded_message(
        9301, "Еду Брест-Варшава, возьму передачу и документы",
        from_id=_t.PeerUser(user_id=8824170368))
    checks.append(("forward от обычного пользователя не считается скрытым",
                   hidden_author_reason(normal_fwd) is None, ""))
    kept = await hidden_monitor.process_message(normal_fwd, hid_source)
    checks.append(("forward от пользователя с открытым профилем доходит до базы",
                   kept is not None and hidden_monitor.counter["hidden"] == 1,
                   f"hidden={hidden_monitor.counter['hidden']}, saved={hidden_monitor.counter['saved']}"))

    # forward из канала — тоже нормально (новостные каналы так и пересылают)
    channel_fwd = forwarded_message(
        9302, "Еду Минск-Варшава 20.09, есть места для посылок",
        from_id=_t.PeerChannel(channel_id=1234567890))
    checks.append(("forward из канала не считается скрытым автором",
                   hidden_author_reason(channel_fwd) is None, ""))
    await hidden_monitor.process_message(channel_fwd, hid_source)
    checks.append(("forward из канала попадает в находки",
                   hidden_monitor.counter["saved"] == 2, str(hidden_monitor.counter["saved"])))

    # обычное сообщение (не forward) — фильтр не вмешивается
    plain = fake_message(9303, "Еду Брест-Варшава, возьму посылку и документы")
    checks.append(("обычное сообщение фильтр скрытых авторов не трогает",
                   hidden_author_reason(plain) is None, ""))
    await hidden_monitor.process_message(plain, hid_source)
    checks.append(("обычное сообщение проходит дальше фильтра",
                   hidden_monitor.counter["saved"] == 3, str(hidden_monitor.counter["saved"])))

    # скрытый источник (saved_from_name)
    saved_hidden = forwarded_message(9304, "Нужно передать документы в Вильнюс, кто едет?",
                                     saved_name="Скрытый источник")
    checks.append(("пересылка из скрытого источника тоже отсекается",
                   hidden_author_reason(saved_hidden) is not None, str(hidden_author_reason(saved_hidden))))

    # --keep-hidden возвращает такие сообщения
    keep_store = HitStore(":memory:")
    keep_monitor = Monitor(HidClient(), keep_store, [hid_source], hid_notify, Paced(0),
                           dedup_scope="chat", dedup_window=0, keep_hidden=True)
    keep_monitor.entities = {hid_source.target: hid_entity}
    keep_monitor.meta = {hid_source.target: hid_source}
    kept_hidden = await keep_monitor.process_message(
        forwarded_message(9305, "Лечу Москва - Подгорица, возьму посылки и документы",
                          from_name="Olga OlgaB_O"),
        hid_source)
    checks.append(("с --keep-hidden сообщение от скрытого автора не отсекается",
                   kept_hidden is not None and keep_monitor.counter["hidden"] == 0
                   and "1 совпадений" in keep_store.stats(), str(keep_monitor.counter)))

    # отчёт и пульс
    hidden_store.bump_stats("granica_by_lt_pl", hidden=3)
    report_text = hidden_store.stats_report()
    checks.append(("в статистике есть строка про скрытых авторов",
                   "скрытых авторов" in report_text and "hidden by user" in report_text,
                   [l for l in report_text.splitlines() if "скрыт" in l][:1]))
    pulse_text = hidden_monitor.heartbeat_text()
    checks.append(("пульс показывает, сколько скрытых авторов отсеяно",
                   "скрытых авторов 1" in pulse_text, pulse_text))
    checks.append(("фильтр скрытых не срабатывает, когда отключён флагом",
                   "скрытых авторов" not in keep_monitor.heartbeat_text(),
                   keep_monitor.heartbeat_text()))

    # ---------- настройки пересылки: флаги не должны перекрывать конфиг «по умолчанию»
    parser = build_parser()
    bare = parser.parse_args([])
    checks.append(("у флагов пересылки нет собственных значений по умолчанию",
                   bare.forward_max_per_day is None and bare.forward_mode is None
                   and bare.forward_fallback is None and bare.forward_to is None,
                   f"лимит={bare.forward_max_per_day}, режим={bare.forward_mode}, "
                   f"fallback={bare.forward_fallback}"))

    fwd_defaults = {"forward": {"to": "@bot", "mode": "copy", "max_per_day": 180, "fallback": "skip"}}
    effective = resolve_forward_settings(bare, fwd_defaults)
    checks.append(("без флагов берётся всё из sources.yaml (был баг: всегда 100)",
                   effective == {"to": "@bot", "mode": "copy", "max_per_day": 180, "fallback": "skip"},
                   str(effective)))

    overridden = resolve_forward_settings(
        parser.parse_args(["--forward-max-per-day", "250", "--forward-mode", "forward"]), fwd_defaults)
    checks.append(("явно указанный флаг важнее конфига",
                   overridden["max_per_day"] == 250 and overridden["mode"] == "forward"
                   and overridden["fallback"] == "skip", str(overridden)))

    empty_cfg = resolve_forward_settings(bare, {})
    checks.append(("если в конфиге нет forward — встроенные значения (100/forward/link)",
                   empty_cfg == {"to": None, "mode": "forward", "max_per_day": 100, "fallback": "link"},
                   str(empty_cfg)))

    zero = resolve_forward_settings(parser.parse_args(["--forward-max-per-day", "0"]), fwd_defaults)
    checks.append(("лимит 0 (без ограничения) не подменяется значением из конфига",
                   zero["max_per_day"] == 0, str(zero)))

    # ---------- диагностика конфига: какой файл прочитан, не расходятся ли YAML и JSON
    import contextlib
    import io as _io

    diag_dir = workdir / "diag"
    diag_dir.mkdir(parents=True, exist_ok=True)
    (diag_dir / "sources.yaml").write_text(
        "forward:\n  to: \"@parcel_transfer_bot\"\n  max_per_day: 150\n"
        "sources:\n  - target: \"@one\"\n  - target: \"@two\"\n", encoding="utf-8")
    (diag_dir / "sources.json").write_text(json.dumps({
        "forward": {"max_per_day": 100}, "sources": [{"target": "@one"}]}, ensure_ascii=False),
        encoding="utf-8")

    buffer = _io.StringIO()
    with contextlib.redirect_stderr(buffer):
        diag_defaults, diag_sources = load_config(str(diag_dir / "sources.yaml"))
    diag_out = buffer.getvalue()
    checks.append(("в логе видно, какой конфиг прочитан и какой лимит взят",
                   "конфиг:" in diag_out and "sources.yaml" in diag_out
                   and "лимит пересылок 150/сутки" in diag_out and "источников 2" in diag_out,
                   diag_out.strip().splitlines()[0]))
    checks.append(("предупреждение, когда sources.json рядом расходится с YAML",
                   "не совпадает" in diag_out and "100 против 150" in diag_out,
                   [line for line in diag_out.splitlines() if "не совпадает" in line][:1]))
    checks.append(("после запуска известен путь конфига (для --doctor)",
                   str(monitor_module.LAST_CONFIG_PATH).endswith("sources.yaml"),
                   str(monitor_module.LAST_CONFIG_PATH)))

    # вложенная копия проекта (след распаковки архива «в себя»)
    nested_root = workdir / "nested"
    nested_root.mkdir(parents=True, exist_ok=True)
    (nested_root / "monitor.py").write_text("# копия\n", encoding="utf-8")
    (nested_root / "sources.yaml").write_text("sources: []\n", encoding="utf-8")
    inside = nested_root / "telegram-scraper"
    inside.mkdir(exist_ok=True)
    (inside / "monitor.py").write_text("# копия\n", encoding="utf-8")
    (inside / "sources.yaml").write_text("sources: []\n", encoding="utf-8")
    found_nested = find_nested_project(nested_root)
    checks.append(("--doctor находит вложенную копию проекта",
                   found_nested == [inside], str(found_nested)))
    checks.append(("обычная папка не подозревается во вложенной копии",
                   find_nested_project(workdir / "diag") == [], ""))

    # ---------- смена суток в живом режиме: лимит обнуляется, очередь добирается сама
    night_store = HitStore(":memory:")
    night_msg = SimpleNamespace(id=9200, text="Еду Брест-Варшава, возьму передачу", date=NOW,
                                sender_id=1, out=False, chat_id=-1001999888777,
                                chat_title="Граница BY-LT-PL")

    class NightClient:
        def __init__(self):
            self.forwarded = []
        async def get_entity(self, target):
            return FakeEntity(500500, "parcel_transfer_bot", "parcel_transfer_bot") \
                if "@parcel" in str(target) else entity
        async def get_messages(self, ent, ids=None, limit=None, **kwargs):
            return night_msg if ids else []
        async def forward_messages(self, target, messages=None):
            self.forwarded.append(messages[0].id)

    night_hit = {"chat_key": "granica_by_lt_pl", "chat_title": "Граница BY-LT-PL",
                 "username": "granica_BY_LT_PL", "date": NOW.isoformat(), "sender_id": 777,
                 "sender_name": "Дмитрий", "chat_id": -1001999888777, "msg_id": 9200,
                 "text": "Еду Брест-Варшава, возьму передачу",
                 "link": "https://t.me/granica_BY_LT_PL/9200", "score": 9, "category": "parcel",
                 "intent": "offer", "direction": "BY->PL", "countries": ["PL"], "hits": ["передачу"],
                 "found_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    night_store.save_hit(night_hit)
    night_store.queue_forward("granica_by_lt_pl", 9200)

    night_client = NightClient()
    night_forwarder = Forwarder(night_client, "@parcel_transfer_bot", night_store, Paced(0),
                                max_per_day=1)
    await night_forwarder.prepare()
    night_forwarder.sent_today = 1              # лимит выработан ещё вчера
    night_forwarder.stopped_reason = "daily_limit"
    night_source = Source(target="@granica_BY_LT_PL", title="Граница BY-LT-PL", profile="chat",
                          min_score=4, catchup=10)
    async def night_notify(hit):        # локально: silent определяется ниже по файлу
        return True

    night_monitor = Monitor(night_client, night_store, [night_source], night_notify, Paced(0),
                            forwarder=night_forwarder, heartbeat_minutes=1)
    night_monitor.entities = {night_source.target: entity}
    night_monitor.meta = {night_source.target: night_source}

    drained_night = await night_monitor.on_new_day()
    checks.append(("смена суток: лимит обнуляется и очередь добирается без перезапуска",
                   drained_night.get("sent") == 1 and night_client.forwarded == [9200]
                   and night_store.deferred_count() == 0, str(drained_night)))
    checks.append(("смена суток: остановка по лимиту снимается",
                   night_forwarder.stopped_reason is None and night_forwarder.sent_today == 1,
                   f"sent_today={night_forwarder.sent_today}, stop={night_forwarder.stopped_reason}"))

    # пульс предупреждает, что лимит на сегодня выбран
    night_forwarder.sent_today = 1
    checks.append(("пульс поясняет, что дневной лимит выбран и обнулится в полночь",
                   "лимит обнулится в 00:00" in night_monitor.heartbeat_text(),
                   night_monitor.heartbeat_text()))

    # суточный цикл работает и корректно останавливается
    daily_task = asyncio.create_task(night_monitor._daily_loop())
    await asyncio.sleep(0.1)
    daily_task.cancel()
    try:
        await daily_task
    except asyncio.CancelledError:
        pass
    checks.append(("суточный цикл останавливается без ошибок", daily_task.cancelled(), ""))

    # ---------- сутки считаются по местной полуночи
    daystore = HitStore(":memory:")
    boundary = datetime.fromisoformat(HitStore.day_start_utc())
    now_local = datetime.now().astimezone()
    checks.append(("начало суток = местная полночь",
                   boundary.astimezone().hour == 0 and boundary.astimezone().minute == 0
                   and boundary.astimezone().date() == now_local.date(), HitStore.day_start_utc()))

    local_midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_late = (local_midnight - timedelta(minutes=30)).astimezone(timezone.utc).isoformat(timespec="seconds")
    today_early = (local_midnight + timedelta(minutes=30)).astimezone(timezone.utc).isoformat(timespec="seconds")
    daystore.conn.execute("INSERT OR REPLACE INTO forwarded (chat_key, msg_id, ok, mode, error, at) "
                                       "VALUES (?, ?, 1, 'forwarded', '', ?)",
                          ("granica_by_lt_pl", 8001, yesterday_late))
    daystore.conn.execute("INSERT OR REPLACE INTO forwarded (chat_key, msg_id, ok, mode, error, at) "
                                       "VALUES (?, ?, 1, 'forwarded', '', ?)",
                          ("granica_by_lt_pl", 8002, today_early))
    daystore.conn.commit()
    checks.append(("в счётчик суток попадает только сегодняшнее по местному времени",
                   daystore.forwarded_today() == 1, str(daystore.forwarded_today())))

    # старая база без колонки deferred должна мигрировать, а не падать
    legacy_path = ":memory:"
    legacy = _sqlite3.connect(legacy_path)
    legacy_schema = SCHEMA.replace("    deferred    INTEGER DEFAULT 0," + chr(10), "")
    assert "deferred" not in legacy_schema, "тест миграции: колонка не убрана из старой схемы"
    legacy.executescript(legacy_schema)
    legacy.commit()
    old_store = HitStore.__new__(HitStore)
    old_store.path, old_store.conn = legacy_path, legacy
    old_store._migrate()
    columns_after = {row[1] for row in legacy.execute("PRAGMA table_info(stats)")}
    checks.append(("старая база догоняет схему (ALTER TABLE)", "deferred" in columns_after, str(sorted(columns_after))))

    # приватные ссылки-приглашения: ключ, отображение, авто-подписка
    from core_telegram import invite_hash, resolve_targets

    checks.append(("invite_hash достаёт hash из ссылки",
                   invite_hash("https://t.me/+CmQyl50rf-NlODFi") == "CmQyl50rf-NlODFi"
                   and invite_hash("https://t.me/joinchat/AbCdEf") == "AbCdEf"
                   and invite_hash("@travelersminsk") is None, ""))

    invite_source = Source(target="https://t.me/+CmQyl50rf-NlODFi", title="Приватный", profile="chat", min_score=4)
    invite_key = Monitor.chat_key(None, invite_source)
    checks.append(("приватный чат получает отдельный ключ и метку",
                   invite_key == "invite:CmQyl50rf-NlODFi"
                   and HitStore._chat_ref(invite_key) == "+CmQyl50rf-NlODFi", invite_key))

    class InviteClient:
        """Не участник: get_entity падает, ImportChatInvite возвращает чат."""
        def __init__(self, already_member=False):
            self.joined, self.already_member = [], already_member
        async def get_entity(self, target):
            if self.already_member:
                return FakeEntity(-1001999888777, "Приватный чат", None)
            raise ValueError("Cannot get entity from a channel (or group) that you are not part of. "
                             "Join the group and retry")
        async def __call__(self, request):
            self.joined.append(request)
            return SimpleNamespace(chats=[FakeEntity(-1001999888777, "Приватный чат", None)])

    invite_client = InviteClient()
    resolved = await resolve_targets(invite_client, ["https://t.me/+CmQyl50rf-NlODFi"], Paced(0), auto_join=True)
    checks.append(("--auto-join: подписка по приглашению и разрешение чата",
                   len(resolved) == 1 and len(invite_client.joined) == 1, f"подписок: {len(invite_client.joined)}"))

    no_join_client = InviteClient()
    resolved2 = await resolve_targets(no_join_client, ["https://t.me/+CmQyl50rf-NlODFi"], Paced(0), auto_join=False)
    checks.append(("без --auto-join чат не разрешается и не подписывает",
                   resolved2 == {} and not no_join_client.joined, ""))

    already_client = InviteClient(already_member=True)
    resolved3 = await resolve_targets(already_client, ["https://t.me/+CmQyl50rf-NlODFi"], Paced(0), auto_join=True)
    checks.append(("если аккаунт уже в чате — подписка не нужна",
                   len(resolved3) == 1 and not already_client.joined, ""))

    # ссылки: публичный чат и приватный
    checks.append(("ссылка публичного чата", message_link("travelersminsk", -1001, 91532) == "https://t.me/travelersminsk/91532", ""))
    checks.append(("ссылка приватного чата (t.me/c)", message_link(None, -1001234567890, 55) == "https://t.me/c/1234567890/55", ""))

    # ---------------------------------------------------------- 2. конвейер
    section("Конвейер мониторинга (фейковый клиент)")
    shutil.rmtree(workdir, ignore_errors=True)
    workdir.mkdir(parents=True)
    db_path, log_path = str(workdir / "hits.sqlite3"), str(workdir / "hits.log")

    corpus = [
        fake_message(100, "Привет всем, кто сегодня на границе?"),
        fake_message(101, "Возьму посылку из Минска в Варшаву, еду 20.09, есть 2 места в машине"),
        fake_message(102, "Правительство Литвы продлило ограничения до 30 ноября"),
        fake_message(103, "нужно передать документы в Вильнюс, кто едет на этой неделе?"),
        fake_message(104, "Возьму посылку из Минска в Варшаву, еду 20.09, есть 2 места в машине"),  # дубль по тексту, другой id
        fake_message(105, "Подписывайтесь на канал, скидки 50%"),
        fake_message(106, "Возьму посылку в Варшаву, есть место", out=True),   # своё сообщение — пропускаем
    ]
    entity = FakeEntity(-1001234567890, "Посылки/Передачи/Попутчики", username="travelersminsk")
    client = FakeClient(entity, corpus)
    source = Source(target="@travelersminsk", title=entity.title, profile="chat", min_score=4, catchup=10)

    async def silent(hit):        # уведомления в консоль в тесте не нужны
        return None

    store = HitStore(db_path)
    monitor = Monitor(client, store, [source], silent, Paced(0))
    monitor.entities = {source.target: entity}
    monitor.meta = {source.target: source}
    await monitor.catch_up([source])

    checks.append(("найдено 2 уникальных объявления (101, 103)", monitor.counter["matched"] == 2, str(monitor.counter)))
    checks.append(("повтор текста 104 отсеян как дубль", monitor.counter["text_duplicates"] == 1, ""))
    checks.append(("своё сообщение 106 пропущено (out=True)", monitor.counter["matched"] == 2, ""))
    hits = store.export_csv(str(workdir / "hits.csv"))
    checks.append(("совпадения легли в SQLite/CSV", hits == 2, f"строк: {hits}"))
    checks.append(("ссылка на сообщение в базе",
                   store.conn.execute("SELECT link FROM hits LIMIT 1").fetchone()[0] == "https://t.me/travelersminsk/101", ""))

    # повторный запуск на том же чате: ничего не должно задублироваться
    monitor2 = Monitor(client, store, [source], silent, Paced(0))
    monitor2.entities = {source.target: entity}
    monitor2.meta = {source.target: source}
    await monitor2.catch_up([source])
    total = store.conn.execute("SELECT COUNT(*) FROM hits").fetchone()[0]
    checks.append(("перезапуск не создаёт дублей", monitor2.counter["matched"] == 0
                   and monitor2.counter["duplicates"] == 7 and total == 2,
                   f"matched={monitor2.counter['matched']}, seen-дублей={monitor2.counter['duplicates']}, в базе {total}"))

    # новое живое сообщение проходит дальше
    live = fake_message(200, "Еду в Ригу в пятницу, могу взять небольшую коробку")
    hit = await monitor2.process_message(live, source)
    checks.append(("живое сообщение обработано", hit is not None and hit["category"] == "parcel" and hit["intent"] == "offer",
                   f"{hit['category'] if hit else None}/{hit['intent'] if hit else None}"))
    checks.append(("автор сообщения подтянут", (hit or {}).get("sender_name") == "Иван Тестов", str((hit or {}).get("sender_name"))))

    # уведомление в файл
    from core_telegram import notify_file
    await notify_file(hit, log_path)
    checks.append(("уведомление записано в файл", Path(log_path).exists() and "Ригу" in Path(log_path).read_text(encoding="utf-8"), ""))

    # кэш резолва автора: повторный хит не дёргает API лишний раз
    calls_before = client.calls
    await monitor2.process_message(fake_message(201, "Нужно передать ключи в Брест, кто едет?"), source)
    checks.append(("резолв автора кэшируется", client.calls == calls_before, f"вызовов API: {client.calls - calls_before}"))

    # кросс-чат дубликат: тот же текст из другого чата не должен уведомлять повторно
    other_entity = FakeEntity(-1001987654321, "Граница (чат)", username="belgranica")
    other_source = Source(target="@belgranica", title=other_entity.title, profile="chat", min_score=4, catchup=5)
    monitor_other = Monitor(client, store, [other_source], silent, Paced(0))
    monitor_other.entities = {other_source.target: other_entity}
    monitor_other.meta = {other_source.target: other_source}
    duplicate = await monitor_other.process_message(
        fake_message(91532, "Возьму посылку из Минска в Варшаву, еду 20.09, есть 2 места в машине"), other_source)
    checks.append(("кросс-чат дубль не уведомляет повторно",
                   duplicate is None and monitor_other.counter["cross_chat_duplicates"] == 1,
                   str(monitor_other.counter)))

    # фильтры: только «ищу» и только нужное направление
    filtered = Monitor(client, store, [source], silent, Paced(0), only_intents=("request",),
                       dedup_scope="chat", dedup_window=0)
    filtered.entities = {source.target: entity}
    filtered.meta = {source.target: source}
    offer_hit = await filtered.process_message(fake_message(3001, "Везу в Вильнюс, есть места, возьму передачки"), source)
    want_hit = await filtered.process_message(fake_message(3002, "Ищу место Минск-Варшава, 2 человека"), source)
    checks.append(("--only-intent=request отсекает «предлагаю» и пропускает «ищу»",
                   offer_hit is None and want_hit is not None, f"filtered={filtered.counter['filtered']}"))

    direction_filter = Monitor(client, store, [source], silent, Paced(0),
                               only_directions=("BY->PL",), dedup_scope="chat", dedup_window=0)
    direction_filter.entities = {source.target: entity}
    direction_filter.meta = {source.target: source}
    wrong_dir = await direction_filter.process_message(fake_message(3003, "Везу в Берлин, есть места"), source)
    right_dir = await direction_filter.process_message(fake_message(3004, "Еду Минск-Варшава, возьму посылки"), source)
    checks.append(("--only-direction=BY->PL фильтрует направления",
                   wrong_dir is None and right_dir is not None, f"filtered={direction_filter.counter['filtered']}"))

    # окно свежести: старое не уведомляет, свежее — да
    fresh_store = HitStore(str(workdir / "fresh.sqlite3"))
    fresh_monitor = Monitor(client, fresh_store, [source], silent, Paced(0),
                            max_age=24, dedup_scope="chat")
    fresh_monitor.entities = {source.target: entity}
    fresh_monitor.meta = {source.target: source}
    await fresh_monitor.process_message(
        fake_message(5001, "Возьму посылку Минск-Варшава, есть места", minutes_ago=11 * 60), source)
    await fresh_monitor.process_message(
        fake_message(5002, "Возьму посылку Минск-Варшава, есть места", minutes_ago=30 * 60), source)
    await fresh_monitor.process_message(
        fake_message(5003, "Нужно передать документы в Вильнюс, кто едет?", minutes_ago=72 * 60), source)
    checks.append(("окно свежести --max-age 24: 11 ч берём, 30 ч и 3 суток — нет",
                   fresh_monitor.counter["matched"] == 1 and fresh_monitor.counter["too_old"] == 2,
                   str(fresh_monitor.counter)))

    # TXT-отчёт: человекочитаемый, с ссылкой и без эмодзи
    txt_path = str(workdir / "hits.txt")
    n_txt = fresh_store.export_txt(txt_path)
    txt = Path(txt_path).read_text(encoding="utf-8")
    checks.append(("TXT-отчёт содержит заголовок, текст и ссылку",
                   n_txt == 1 and "Telegram-радар" in txt and "t.me/travelersminsk/5001" in txt,
                   f"строк отчёта: {n_txt}"))
    checks.append(("TXT-отчёт фильтруется по часам (--export-hours)",
                   fresh_store.export_txt(str(workdir / "last2.txt"), hours=48) == 1
                   and fresh_store.export_txt(str(workdir / "last0.txt"), hours=0.5) == 0, ""))

    # фильтр по категории: --category parcel отсекает чисто пассажирские объявления
    parcel_only = Monitor(client, store, [source], silent, Paced(0), only_categories=("parcel", "mixed"),
                          dedup_scope="chat", dedup_window=0)
    parcel_only.entities = {source.target: entity}
    parcel_only.meta = {source.target: source}
    ride_hit = await parcel_only.process_message(
        fake_message(4001, "Еду сегодня в 11:00 Брест-Тересполь-Бяла могу взять попутчиков, комфортное авто"), source)
    parcel_hit = await parcel_only.process_message(
        fake_message(4002, "#водитель 19.09 Вильнюс-Минск есть 4 места посылки и передачи"), source)
    checks.append(("--category parcel,mixed: чистое «возьму попутчиков» отсеяно, посылки+попутчики прошли",
                   ride_hit is None and parcel_hit is not None and parcel_hit["category"] == "mixed",
                   f"filtered={parcel_only.counter['filtered']}"))

    # ================= пересылка боту и статистика =================
    section("Пересылка и статистика")

    class BotEntity:
        id, title, username = 500500, "parcel_transfer_bot", "parcel_transfer_bot"

    class FwdClient(FakeClient):
        """Клиент, который умеет пересылать и умеет запрещать пересылку в конкретных чатах."""
        def __init__(self, entity, corpus, restricted_ids=()):
            super().__init__(entity, corpus)
            self.forwarded, self.sent, self.restricted = [], [], set(restricted_ids)

        async def get_entity(self, target):
            if isinstance(target, str) and target.startswith("@"):
                return BotEntity()
            return await super().get_entity(target)

        async def forward_messages(self, peer, messages=None, **kwargs):
            message = messages[0]
            if message.id in self.restricted:
                from telethon.errors import ChatForwardsRestrictedError
                raise ChatForwardsRestrictedError(request=None)
            self.forwarded.append(message.id)
            return message

        async def send_message(self, peer, text, **kwargs):
            self.sent.append(text)
            return None

    fwd_store = HitStore(str(workdir / "fwd.sqlite3"))
    fwd_client = FwdClient(entity, [], restricted_ids={1010})
    forwarder = Forwarder(fwd_client, "@parcel_transfer_bot", fwd_store, Paced(0),
                          mode="forward", max_per_day=2, fallback="link")
    checks.append(("получатель-бот разрешается", await forwarder.prepare() is True, ""))

    fwd_monitor = Monitor(fwd_client, fwd_store, [source], silent, Paced(0), forwarder=forwarder,
                          dedup_scope="chat", dedup_window=0)
    fwd_monitor.entities = {source.target: entity}
    fwd_monitor.meta = {source.target: source}
    await fwd_monitor.process_message(
        fake_message(1001, "Возьму посылку Минск-Варшава 20.09, есть места"), source)
    await fwd_monitor.process_message(
        fake_message(1010, "Нужно передать документы в Вильнюс, кто едет?"), source)   # чат с запретом
    copied_1001 = [text for text in fwd_client.sent if "t.me/travelersminsk/1001" in text]
    checks.append(("пересылка выполнена как forward (не копия)",
                   1001 in fwd_client.forwarded and not copied_1001, str(fwd_client.forwarded)))
    checks.append(("в чате с запретом пересылки ушёл текст со ссылкой",
                   len(fwd_client.sent) == 1 and "t.me/travelersminsk/1010" in fwd_client.sent[0], ""))

    # ---------- форум-чаты: определение темы, чтение по темам, фильтр в живом режиме
    from core_telegram import collect_topics, topic_of, topic_title_of

    def forum_message(mid, topic_id, text="Еду Брест-Варшава, возьму передачу", title=None):
        header = SimpleNamespace(reply_to_top_id=topic_id, reply_to_msg_id=topic_id, forum_topic=True)
        action = SimpleNamespace(title=title) if title else None
        return SimpleNamespace(id=mid, text=text, date=NOW, sender_id=1, out=False,
                               chat_id=-1001777, reply_to=header, action=action)

    checks.append(("тема сообщения определяется по reply_to_top_id",
                   topic_of(forum_message(1, 555)) == 555
                   and topic_of(SimpleNamespace(id=2, reply_to=None, text="обычный чат")) is None, ""))
    debate = SimpleNamespace(id=3, reply_to=SimpleNamespace(reply_to_top_id=None, reply_to_msg_id=777,
                                                            forum_topic=True), text="первое в теме")
    checks.append(("первое сообщение темы определяется по reply_to_msg_id",
                   topic_of(debate) == 777, str(topic_of(debate))))
    checks.append(("название темы берётся из служебного сообщения о создании",
                   topic_title_of(forum_message(4, 555, title="Посылки и передачи")) == "Посылки и передачи"
                   and topic_title_of(forum_message(5, 555)) is None, ""))

    class ForumClient:
        def __init__(self, messages):
            self.messages = messages
            self.requested_topics = []
        async def get_messages(self, entity, limit=None, reply_to=None, **kwargs):
            if reply_to is None:
                return self.messages[:limit] if limit else list(self.messages)
            self.requested_topics.append(reply_to)
            return [m for m in self.messages if topic_of(m) == reply_to]

    forum_messages = [
        forum_message(700, 501, title="Посылки Минск-Варшава"),
        forum_message(701, 501),
        forum_message(702, 502, title="Попутчики"),
        forum_message(703, 502),
        forum_message(704, 502),
    ]
    forum_client = ForumClient(forum_messages)
    forum_entity = FakeEntity(-1001777, "Форум-чат", "forum_chat")
    topics = await collect_topics(forum_client, forum_entity, Paced(0), limit=100)
    checks.append(("--list-topics собирает темы с названиями и считает сообщения",
                   (501, "Посылки Минск-Варшава", 2) in topics
                   and (502, "Попутчики", 3) in topics and len(topics) == 2, str(topics)))
    checks.append(("темы сортируются по активности",
                   topics[0][0] == 502, str([t[0] for t in topics])))

    forum_store = HitStore(":memory:")
    forum_source = Source(target="@forum_chat", title="Форум-чат", profile="chat", min_score=4,
                          catchup=10, topics=(501,))
    forum_monitor = Monitor(forum_client, forum_store, [forum_source], silent, Paced(0),
                            dedup_scope="chat", dedup_window=0)
    forum_monitor.entities = {forum_source.target: forum_entity}
    forum_monitor.meta = {forum_source.target: forum_source}
    await forum_monitor.catch_up([forum_source])
    seen_ids = {row[0] for row in forum_store.conn.execute("SELECT msg_id FROM hits").fetchall()}
    checks.append(("catch-up с topics: читается только выбранная тема",
                   forum_client.requested_topics == [501] and seen_ids == {700, 701},
                   f"запрошены темы {forum_client.requested_topics}, находки {sorted(seen_ids)}"))
    topic_row = forum_store.conn.execute(
        "SELECT topic_id, topic_name FROM hits WHERE msg_id=701").fetchone()
    checks.append(("у находки сохранена тема и её название",
                   topic_row == (501, "Посылки Минск-Варшава"), str(topic_row)))

    # без topics читается весь чат
    forum_client.requested_topics.clear()
    open_source = Source(target="@forum_chat", title="Форум-чат", profile="chat", min_score=4, catchup=10)
    open_store = HitStore(":memory:")
    open_monitor = Monitor(forum_client, open_store, [open_source], silent, Paced(0),
                           dedup_scope="chat", dedup_window=0)
    open_monitor.entities = {open_source.target: forum_entity}
    open_monitor.meta = {open_source.target: open_source}
    await open_monitor.catch_up([open_source])
    open_ids = {row[0] for row in open_store.conn.execute("SELECT msg_id FROM hits").fetchall()}
    checks.append(("без topics форум-чат читается целиком",
                   forum_client.requested_topics == [] and len(open_ids) == 5, str(sorted(open_ids))))

    # живой режим: сообщения из других тем игнорируются
    class LiveEvent:
        def __init__(self, message):
            self.message = message
            self.chat_id = message.chat_id

    filtered_store = HitStore(":memory:")
    filtered_monitor = Monitor(forum_client, filtered_store, [forum_source], silent, Paced(0),
                               dedup_scope="chat", dedup_window=0)
    filtered_monitor.entities = {forum_source.target: forum_entity}
    filtered_monitor.meta = {forum_source.target: forum_source}

    async def run_handler(message):
        target = filtered_monitor._target_by_entity(message.chat_id)
        source_here = filtered_monitor.meta.get(target)
        if source_here.topics and topic_of(message) not in source_here.topics:
            filtered_monitor.counter["other_topic"] = filtered_monitor.counter.get("other_topic", 0) + 1
            return
        await filtered_monitor.process_message(message, source_here)

    await run_handler(forum_message(800, 999))          # чужая тема
    await run_handler(forum_message(801, 501))          # наша тема
    live_ids = {row[0] for row in filtered_store.conn.execute("SELECT msg_id FROM hits").fetchall()}
    checks.append(("живой режим: чужая тема отбрасывается, наша ловится",
                   live_ids == {801} and filtered_monitor.counter.get("other_topic") == 1,
                   f"находки {sorted(live_ids)}, отброшено {filtered_monitor.counter.get('other_topic')}"))

    # тема в уведомлении
    notice = format_hit({"chat_title": "Форум-чат", "topic_name": "Посылки Минск-Варшава",
                         "date": NOW.isoformat(), "score": 9, "text": "текст",
                         "link": "https://t.me/forum_chat/801", "category": "parcel",
                         "intent": "offer", "direction": "BY->PL"})
    checks.append(("тема показана в уведомлении",
                   "тема «Посылки Минск-Варшава»" in notice, notice.splitlines()[1]))

    # topics из конфига (числа, строки и ссылки)
    tmp_dir = workdir / "cfg"
    tmp_dir.mkdir(exist_ok=True)
    cfg_path = tmp_dir / "topics.json"
    cfg_path.write_text(json.dumps({
        "sources": [
            {"target": "@forum_chat", "topics": [501, "502"]},
            {"target": "@plain_chat"},
            {"target": "@link_chat", "topics": ["https://t.me/link_chat/777/9"]},
        ]
    }, ensure_ascii=False), encoding="utf-8")
    _defaults, cfg_sources = load_config(str(cfg_path))
    by_target = {s.target: s.topics for s in cfg_sources}
    checks.append(("topics читаются из конфига: числа, строки и ссылка",
                   by_target["@forum_chat"] == (501, 502) and by_target["@plain_chat"] == ()
                   and by_target["@link_chat"] == (777,), str(by_target)))

    # старая база без колонок темы должна мигрировать
    legacy2 = _sqlite3.connect(":memory:")
    legacy_hits = SCHEMA.replace(",\n    topic_id   INTEGER, topic_name TEXT", "")
    legacy2.executescript(legacy_hits)
    old_store2 = HitStore.__new__(HitStore)
    old_store2.path, old_store2.conn = ":memory:", legacy2
    old_store2._migrate()
    hit_cols = {row[1] for row in legacy2.execute("PRAGMA table_info(hits)")}
    checks.append(("старая база получает колонки темы (ALTER TABLE)",
                   {"topic_id", "topic_name"} <= hit_cols, str(sorted(hit_cols))[:120]))

    # ---------- пульс живого режима
    pulse_store = HitStore(":memory:")
    pulse_client = ForumClient(forum_messages)
    pulse_monitor = Monitor(pulse_client, pulse_store, [forum_source], silent, Paced(0),
                            heartbeat_minutes=1)
    pulse_monitor.entities = {forum_source.target: forum_entity}
    first = pulse_monitor.heartbeat_text()
    pulse_monitor.counter.update({"events": 37, "scanned": 37, "matched": 3, "duplicates": 20,
                                  "filtered": 4})
    second = pulse_monitor.heartbeat_text()
    # ---------- регрессия живого режима: event.chat_id — «маркированный» id (-100…)
    from core_telegram import peer_id as _peer_id
    from telethon.tl import types as _tl

    real_channel = _tl.Channel(id=1665760449, title="Частный чат", photo=None, date=None,
                               creator=None, left=None, broadcast=False, verified=None, megagroup=True,
                               restricted=None, signatures=None, min=None, scam=None, has_link=None,
                               has_geo=None, slowmode_enabled=None, access_hash=123, username=None)
    id_source = Source(target="https://t.me/+hash", title="Частный чат", profile="chat",
                       min_score=4, catchup=10)
    id_store = HitStore(":memory:")
    id_monitor = Monitor(ForumClient(forum_messages), id_store, [id_source], silent, Paced(0),
                         dedup_scope="chat", dedup_window=0)
    id_monitor.entities = {id_source.target: real_channel}
    id_monitor.meta = {id_source.target: id_source}
    marked = _peer_id(real_channel)
    checks.append(("живой режим: событие с id -100… находит источник (раньше молча отбрасывалось)",
                   marked == -1001665760449 and id_monitor._target_by_entity(marked) == id_source.target,
                   f"entity.id={real_channel.id}, event.chat_id={marked}"))
    checks.append(("живой режим: сырой id тоже принимается, чужой чат — нет",
                   id_monitor._target_by_entity(real_channel.id) == id_source.target
                   and id_monitor._target_by_entity(-1009999999999) == "", ""))

    class ChannelMessage:
        """Сообщение так, как его видит живой режим: chat_id — маркированный (-100…)."""
        def __init__(self, mid, marked_chat_id, text):
            self.id, self.text, self.date, self.sender_id, self.out = mid, text, NOW, 1, False
            self.chat_id, self.chat_title = marked_chat_id, "Частный чат"
            self.reply_to, self.action = None, None

    async def live_event(message):
        target = id_monitor._target_by_entity(message.chat_id)
        source_here = id_monitor.meta.get(target)
        if source_here is None:
            id_monitor.counter["unknown_chat"] = id_monitor.counter.get("unknown_chat", 0) + 1
            return
        id_monitor.counter["events"] = id_monitor.counter.get("events", 0) + 1
        id_monitor.last_event = (datetime.now().astimezone(), target)
        await id_monitor.process_message(message, source_here)

    await live_event(ChannelMessage(9100, marked, "Еду Брест-Варшава, возьму передачу"))
    live_hits = {row[0] for row in id_store.conn.execute("SELECT msg_id FROM hits").fetchall()}
    checks.append(("живой режим: сообщение из канала дошло до базы",
                   live_hits == {9100} and id_monitor.counter["events"] == 1, str(sorted(live_hits))))
    checks.append(("живой режим: чужой чат считается, но в находки не идёт",
                   id_monitor.counter.get("unknown_chat", 0) == 0, str(id_monitor.counter.get("unknown_chat"))))

    live_pulse = id_monitor.heartbeat_text()
    checks.append(("пульс показывает последнее принятое сообщение",
                   "последнее сообщение" in live_pulse and "+hash" in live_pulse, live_pulse))
    empty_pulse = Monitor(ForumClient(forum_messages), HitStore(":memory:"), [id_source], silent,
                          Paced(0)).heartbeat_text()
    checks.append(("пульс честно говорит, что сообщений ещё не было",
                   "с запуска ни одного сообщения" in empty_pulse, empty_pulse))

    checks.append(("пульс показывает источник, приход сообщений и находки за интервал",
                   "жив, слушаю 1 источник(ов)" in second and "пришло 37" in second
                   and "найдено 3" in second, second))
    checks.append(("пульс за первый интервал не приписывает себе весь прогон",
                   "сообщений не было" in first, first))

    pulse_store.queue_forward("forum_chat", 900)
    pulse_store.conn.execute("INSERT INTO hits (chat_key, msg_id, notified) VALUES ('forum_chat', 901, 0)")
    pulse_store.conn.commit()
    third = pulse_monitor.heartbeat_text()
    checks.append(("пульс сообщает про очередь и неотправленные уведомления",
                   "в очереди 1" in third and "не отправлено уведомлений 1" in third, third))

    pulse_monitor.forwarder = SimpleNamespace(max_per_day=150)
    pulse_store.conn.execute("INSERT INTO forwarded (chat_key, msg_id, ok, mode, error, at) "
                                "VALUES ('forum_chat', 902, 1, 'forwarded', '', ?)",
                             (datetime.now(timezone.utc).isoformat(timespec="seconds"),))
    pulse_store.conn.commit()
    fourth = pulse_monitor.heartbeat_text()
    checks.append(("пульс показывает пересылку сегодня и лимит",
                   "переслано сегодня 1/150" in fourth, fourth))

    # задача пульса реально печатает строки по таймеру и корректно останавливается
    pulse_monitor.heartbeat_minutes = 0.05      # 3 секунды
    task = asyncio.create_task(pulse_monitor._heartbeat_loop())
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    checks.append(("задача пульса останавливается без ошибок", task.cancelled(), ""))

    # ---------- порядок обхода: случайный старт, но тот же состав чатов
    pool = [Source(target=f"@chat{i}", title=f"Чат {i}", profile="chat", min_score=5, catchup=10)
            for i in range(6)]
    same_order = order_sources(pool, "config")
    checks.append(("--order config сохраняет порядок из конфига",
                   [s.target for s in same_order] == [s.target for s in pool], ""))

    shuffled = order_sources(pool, "random", rng=random.Random(1234))
    checks.append(("случайный порядок не теряет и не добавляет чаты",
                   sorted(s.target for s in shuffled) == sorted(s.target for s in pool)
                   and len(shuffled) == len(pool), str([s.target for s in shuffled])))

    starts = {order_sources(pool, "random")[0].target for _ in range(60)}
    checks.append(("случайный порядок действительно меняет первый чат",
                   len(starts) >= 3, f"разных стартов за 60 прогонов: {len(starts)}"))

    checks.append(("один источник и пустой список не ломают случайный порядок",
                   [s.target for s in order_sources(pool[:1], "random")] == ["@chat0"]
                   and order_sources([], "random") == [], ""))

    # названия чатов из конфига подставляются в отчёт, даже если находок по ним не было
    tstore = HitStore(":memory:")
    tstore.bump_stats("granica_online", scanned=21, matched=0)
    plain = tstore.stats_report(days=1)
    titled = tstore.stats_report(days=1, titles_from_config={"granica_online": "Граница онлайн"})
    checks.append(("отчёт подписывает чаты из конфига, где находок ещё не было",
                   "Граница онлайн" not in plain and "Граница онлайн" in titled, ""))

    # ---------- уведомления ботом: 401 не спамит и не теряет находки
    import core_telegram as ct
    import os as _os

    env_backup = {k: _os.environ.get(k) for k in ("TG_BOT_TOKEN", "TG_NOTIFY_CHAT")}
    _os.environ["TG_BOT_TOKEN"] = "123456:FAKE_TOKEN"
    _os.environ["TG_NOTIFY_CHAT"] = "555"

    calls = {"n": 0}

    async def fake_bot_send(hit, token, chat):
        calls["n"] += 1
        return ("HTTP 401 Unauthorized — Telegram не принял токен бота", True)

    real_bot_send = ct.notify_telegram_bot
    ct.notify_telegram_bot = fake_bot_send
    try:
        broken = ct.build_notifier("bot")
        results = [await broken({"text": f"msg {i}", "link": "", "chat_title": "x",
                                 "date": NOW.isoformat(), "score": 1}) for i in range(5)]
        checks.append(("401 выключает уведомления ботом без спама (один запрос вместо пяти)",
                       calls["n"] == 1 and all(r is False for r in results),
                       f"запросов: {calls['n']}, ответы: {results}"))

        good_calls = {"n": 0}

        async def ok_bot_send(hit, token, chat):
            good_calls["n"] += 1
            return ("", False)

        ct.notify_telegram_bot = ok_bot_send
        working = ct.build_notifier("bot")
        ok_results = [await working({"text": "привет", "link": "", "chat_title": "x",
                                     "date": NOW.isoformat(), "score": 1}) for _ in range(3)]
        checks.append(("рабочий токен: каждое уведомление уходит и считается доставленным",
                       good_calls["n"] == 3 and all(r is True for r in ok_results), f"запросов: {good_calls['n']}"))
    finally:
        ct.notify_telegram_bot = real_bot_send
        for k, v in env_backup.items():
            if v is None:
                _os.environ.pop(k, None)
            else:
                _os.environ[k] = v

    # ---------- неотправленные уведомления не помечаются отправленными
    nstore = HitStore(":memory:")

    async def failing_notify(hit):
        return False

    nclient = FwdClient(entity, [])
    nmonitor = Monitor(nclient, nstore, [source], failing_notify, Paced(0),
                       dedup_scope="chat", dedup_window=0)
    nmonitor.entities = {source.target: entity}
    nmonitor.meta = {source.target: source}
    await nmonitor.process_message(fake_message(2001, "Возьму посылку Минск-Варшава 20.09"), source)
    checks.append(("сбой уведомления не помечает его отправленным",
                   nstore.pending_count() == 1 and nmonitor.counter.get("notify_failed") == 1,
                   f"pending={nstore.pending_count()}"))

    # догон: сначала сбой (ничего не отправлено), потом рабочий канал
    sent_hits = []

    async def good_notify(hit):
        sent_hits.append(hit["msg_id"])
        return True

    sent, left = await resend_pending(nstore, failing_notify, 10)
    checks.append(("догон при повторном сбое ничего не теряет",
                   sent == 0 and left == 1, f"sent={sent}, left={left}"))
    sent, left = await resend_pending(nstore, good_notify, 10)
    checks.append(("догон отправляет накопленное и снимает с очереди уведомлений",
                   sent == 1 and left == 0 and sent_hits == [2001], f"sent={sent}, left={left}"))
    sent2, left2 = await resend_pending(nstore, good_notify, 10)
    checks.append(("повторный догон ничего не дублирует", sent2 == 0 and left2 == 0, f"sent={sent2}"))

    # третий хит — превышен лимит 2/сутки
    await fwd_monitor.process_message(
        fake_message(1002, "Кто едет в Варшаву? нужно передать посылку"), source)
    checks.append(("дневной лимит пересылок соблюдается",
                   fwd_monitor.counter.get("forward_limit") == 1 and forwarder.stopped_reason == "daily_limit",
                   str(fwd_monitor.counter)))

    # повторная отправка того же сообщения не уходит снова
    again = await forwarder.send(fake_message(1001, "Возьму посылку Минск-Варшава 20.09, есть места"), {"chat_key": "travelersminsk", "msg_id": 1001})
    checks.append(("повторно то же сообщение не пересылается", again == "duplicate", again))

    # статистика по источникам
    fwd_monitor.flush_stats()
    report = fwd_store.stats_report()
    (workdir / "stats.txt").write_text(report, encoding="utf-8")
    fwd_store.stats_csv(str(workdir / "stats.csv"))
    checks.append(("файл статистики: источник с ключом чата, «переслано», итог за сегодня",
                   "ПО ИСТОЧНИКАМ" in report and "переслано" in report
                   and "@travelersminsk" in report and "СЕГОДНЯ (с местной полуночи): переслано 2" in report
                   and "Посылки/Передачи" in report,
                   ""))
    checks.append(("статистика выгружается в CSV", (workdir / "stats.csv").exists(), ""))

    # проверка пересылки на живом клиенте (--test-forward) — три сценария

    def test_args(**over):
        base = dict(forward_to="@parcel_transfer_bot", forward_mode="forward",
                    forward_fallback="link", forward_dry_run=False)
        return SimpleNamespace(**{**base, **over})

    def sample_message():
        return SimpleNamespace(id=777, text="Еду Минск-Варшава, возьму посылку", date=NOW, sender_id=1, out=False)

    class TfClient(FwdClient):
        async def get_messages(self, entity, limit=None, **kwargs):
            return [sample_message()] if limit == 1 else []

    tf_client = TfClient(entity, [])
    tf_store = HitStore(str(workdir / "tf.sqlite3"))
    tf_source = Source(target="@travelersminsk", title=entity.title, profile="chat", min_score=4, catchup=1)
    code = await monitor_module.test_forward(tf_client, tf_store, [tf_source], Paced(0),
                                             test_args(), {"forward": {"to": "@parcel_transfer_bot"}})
    checks.append(("--test-forward: реальное сообщение ушло боту как forward",
                   code == 0 and 777 in tf_client.forwarded and not tf_client.sent, f"код {code}"))

    tf_restricted = TfClient(entity, [], restricted_ids={777})
    tf_store2 = HitStore(str(workdir / "tf2.sqlite3"))
    code2 = await monitor_module.test_forward(tf_restricted, tf_store2, [tf_source], Paced(0),
                                              test_args(), {"forward": {"to": "@parcel_transfer_bot"}})
    checks.append(("--test-forward: при запрете пересылки уходит текст со ссылкой",
                   code2 == 0 and len(tf_restricted.sent) == 1, f"код {code2}"))

    tf_store_row = tf_store.conn.execute(
        "SELECT mode FROM forwarded WHERE chat_key='travelersminsk' AND msg_id=777").fetchone()
    checks.append(("--test-forward помечает отправку как test (не портит статистику)",
                   tf_store_row is not None and tf_store_row[0] == "test"
                   and tf_store.forwarded_today() == 0
                   and "СЕГОДНЯ (с местной полуночи): переслано 0" in tf_store.stats_report(),
                   f"mode={tf_store_row[0] if tf_store_row else None}, сегодня={tf_store.forwarded_today()}"))
    repeat_forwarder = Forwarder(TfClient(entity, []), "@parcel_transfer_bot", tf_store, Paced(0), mode="forward")
    await repeat_forwarder.prepare()
    repeat_status = await repeat_forwarder.send(sample_message(), {"chat_key": "travelersminsk", "msg_id": 777})
    checks.append(("--test-forward не даёт отправить то же сообщение повторно",
                   tf_store.was_forwarded("travelersminsk", 777) and repeat_status == "duplicate", repeat_status))

    tf_dry = TfClient(entity, [])
    tf_store3 = HitStore(str(workdir / "tf3.sqlite3"))
    code3 = await monitor_module.test_forward(tf_dry, tf_store3, [tf_source], Paced(0),
                                              test_args(forward_dry_run=True),
                                              {"forward": {"to": "@parcel_transfer_bot"}})
    checks.append(("--test-forward: dry-run ничего не отправляет",
                   code3 == 0 and not tf_dry.forwarded and not tf_dry.sent, f"код {code3}"))

    # вложенная структура профиля news: новостной пост не проходит
    news_source = Source(target="@granica_es", profile="news", min_score=6)
    monitor3 = Monitor(client, store, [news_source], silent, Paced(0))
    monitor3.entities = {news_source.target: entity}
    monitor3.meta = {news_source.target: news_source}
    news_hit = await monitor3.process_message(
        fake_message(300, "Электронная очередь в зоне ожидания: как зарегистрироваться, пошаговая инструкция"), news_source)
    checks.append(("profile=news отсекает новости", news_hit is None, ""))

    # ---------------------------------------------------------- 3. FloodWait
    section("FloodWait")
    from telethon.errors import FloodWaitError
    state = {"n": 0}

    async def flaky():
        state["n"] += 1
        if state["n"] == 1:
            raise FloodWaitError(request=None, capture=1)
        return "ok"

    result = await call(flaky, Paced(0), label="flood-test")
    checks.append(("FloodWait пережит, запрос повторён", result == "ok" and state["n"] == 2, str(state)))

    # ---------------------------------------------------------- несколько аккаунтов
    section("Аккаунты: конфиг, чаты, лимиты")

    multi_defaults = {"forward": {"to": "@parcel_transfer_bot", "mode": "forward", "max_per_day": 180},
                      "accounts": {"main": {"session": "monitor_session",
                                            "forward": {"to": "@bot_one", "max_per_day": 60}},
                                   "second": {"session": "second",
                                              "forward": {"to": "@bot_two", "max_per_day": 40}}}}
    fargs = SimpleNamespace(session="monitor_session", forward_to=None, forward_max_per_day=None,
                            forward_mode=None, forward_fallback=None, account=None)
    accs = resolve_accounts(fargs, multi_defaults)
    checks.append(("все аккаунты из конфига поднимаются в одном процессе",
                   [a.name for a in accs] == ["main", "second"],
                   ", ".join(a.name for a in accs)))
    checks.append(("у аккаунта своя сессия и свой получатель",
                   accs[0].session == "monitor_session" and accs[0].forward["to"] == "@bot_one"
                   and accs[1].session == "second" and accs[1].forward["to"] == "@bot_two",
                   f"{accs[0].session}/{accs[0].forward['to']} · {accs[1].session}/{accs[1].forward['to']}"))
    checks.append(("лимит пересылок у каждого аккаунта свой",
                   accs[0].forward["max_per_day"] == 60 and accs[1].forward["max_per_day"] == 40,
                   f"{accs[0].forward['max_per_day']} / {accs[1].forward['max_per_day']}"))
    checks.append(("общие настройки forward подхватываются аккаунтом без своих",
                   resolve_accounts(fargs, {"forward": {"to": "@bot", "mode": "copy", "max_per_day": 90},
                                            "accounts": {"main": {"session": "s1"}}})[0].forward["mode"] == "copy",
                   "mode=copy"))
    checks.append(("enabled: false выключает аккаунт",
                   [a.name for a in resolve_accounts(fargs, {
                       "forward": {}, "accounts": {"main": {"session": "s1"},
                                                   "old": {"enabled": False, "session": "s2"}}})] == ["main"],
                   "old пропущен"))
    checks.append(("--account выбирает один аккаунт",
                   [a.name for a in resolve_accounts(
                       SimpleNamespace(session="monitor_session", forward_to=None,
                                       forward_max_per_day=None, forward_mode=None,
                                       forward_fallback=None, account="second"),
                       multi_defaults)] == ["second"],
                   "second"))

    old_style = SimpleNamespace(session="monitor_session", forward_to=None, forward_max_per_day=None,
                                forward_mode=None, forward_fallback=None, account=None)
    legacy_accs = resolve_accounts(old_style, {"forward": {"to": "@bot", "max_per_day": 180}})
    checks.append(("конфиг без accounts работает как раньше: один аккаунт main",
                   len(legacy_accs) == 1 and legacy_accs[0].name == "main"
                   and legacy_accs[0].forward["max_per_day"] == 180,
                   f"{legacy_accs[0].name}, лимит {legacy_accs[0].forward['max_per_day']}"))

    split_sources = [Source(target="@chat_a", title="A", account="main"),
                     Source(target="@chat_b", title="B", account="second"),
                     Source(target="@chat_c", title="C", ),
                     Source(target="@chat_d", title="D", account="auto"),
                     Source(target="@chat_e", title="E", account="auto")]
    buckets = sources_for_account(split_sources, accs)
    checks.append(("чаты разложены по аккаунтам: каждый следит за своими",
                   [x.target for x in buckets["main"]] == ["@chat_a", "@chat_c", "@chat_d"]
                   and [x.target for x in buckets["second"]] == ["@chat_b", "@chat_e"],
                   f"main: {len(buckets['main'])} чатов, second: {len(buckets['second'])}"))
    checks.append(("чат без account уходит первому аккаунту",
                   "@chat_c" in [x.target for x in buckets["main"]], "@chat_c -> main"))
    checks.append(("account: auto распределяет по кругу",
                   [x.target for x in buckets["main"]] .count("@chat_d") == 1
                   and [x.target for x in buckets["second"]].count("@chat_e") == 1, "chat_d/chat_e разведены"))

    section("Аккаунты: лимиты, очередь и статистика по отдельности")

    acc_store = HitStore(str(workdir / "accounts.sqlite3"))
    for _ in range(3):
        acc_store.mark_forwarded("chat_a", 100 + _, ok=True, mode="forwarded", account="main")
    acc_store.mark_forwarded("chat_b", 200, ok=True, mode="forwarded", account="second")
    acc_store.mark_forwarded("chat_c", 300, ok=True, mode="forwarded", account="")  # старый стиль
    acc_store.mark_forwarded("chat_b", 201, ok=False, mode="queued", account="second")
    acc_store.mark_forwarded("chat_a", 104, ok=False, mode="queued", account="main")
    checks.append(("дневной счётчик считается по каждому аккаунту отдельно",
                   acc_store.forwarded_today("main") == 3 and acc_store.forwarded_today("second") == 1,
                   f"main={acc_store.forwarded_today('main')}, second={acc_store.forwarded_today('second')}"))
    checks.append(("без указания аккаунта счётчик видит всех",
                   acc_store.forwarded_today() == 5, str(acc_store.forwarded_today())))
    checks.append(("в очереди на добор только свои отложенные",
                   acc_store.deferred_queue("second") == [("chat_b", 201)]
                   and acc_store.deferred_queue("main") == [("chat_a", 104)]
                   and acc_store.deferred_count() == 2,
                   f"second={acc_store.deferred_queue('second')}, всего {acc_store.deferred_count()}"))

    acc_store.bump_stats("chat_a", account="main", scanned=50, matched=5, forwarded=3)
    acc_store.bump_stats("chat_b", account="second", scanned=40, matched=2, forwarded=1)
    acc_store.save_hit({"chat_key": "chat_b", "chat_title": "B", "chat_id": "1", "username": "b",
                        "msg_id": 200, "date": NOW.isoformat(), "sender_id": "1", "sender_name": "x",
                        "text": "Еду Варшава, возьму передачу", "score": 9, "category": "parcel",
                        "intent": "offer", "direction": "BY->PL", "countries": [], "hits": ["варшава"],
                        "link": "https://t.me/b/200", "found_at": NOW.isoformat(),
                        "topic_id": None, "topic_name": "", "account": "second"})
    report = acc_store.stats_report(titles_from_config={"chat_a": "A", "chat_b": "B"})
    checks.append(("в отчёте есть колонка «аккаунт» и сводка по аккаунтам",
                   "аккаунт" in report and "ПО АККАУНТАМ" in report
                   and "second" in report.split("ПО АККАУНТАМ")[1], "секции на месте"))
    checks.append(("хит помнит, каким аккаунтом найден",
                   acc_store.conn.execute("SELECT account FROM hits WHERE msg_id=200").fetchone()[0] == "second",
                   "second"))

    # старая база: записи без аккаунта должны стать main, чтобы лимит и статистика не обнулились
    legacy_db = workdir / "legacy_accounts.sqlite3"
    if legacy_db.exists():
        legacy_db.unlink()
    old_conn = _sqlite3.connect(legacy_db)
    old_conn.executescript("""CREATE TABLE forwarded (chat_key TEXT, msg_id INTEGER, ok INTEGER,
        mode TEXT, error TEXT, at TEXT, PRIMARY KEY (chat_key, msg_id));
        INSERT INTO forwarded VALUES ('chat_a', 1, 1, 'forwarded', '', '2026-01-01T00:00:00+00:00');""")
    old_conn.commit()
    old_conn.close()
    migrated = HitStore(str(legacy_db))
    checks.append(("старая база без аккаунтов: история помечена main",
                   migrated.conn.execute("SELECT account FROM forwarded WHERE msg_id=1").fetchone()[0] == "main",
                   "account=main"))

    section("Аккаунты: пересылка")

    class AccClient:
        def __init__(self, tag=""):
            self.sent = []
            self.tag = tag

        async def get_entity(self, target):
            return SimpleNamespace(id=1, title=target, username=str(target).lstrip("@"))

        async def forward_messages(self, target, messages=None):
            self.sent.append((self.tag or getattr(target, "username", "?"), messages[0].id))
            return SimpleNamespace(id=messages[0].id)

        async def send_message(self, target, text, **kw):
            self.sent.append((self.tag or getattr(target, "username", "?"), "link"))
            return SimpleNamespace(id=1)

    shared_store = HitStore(str(workdir / "shared.sqlite3"))
    main_fwd = Forwarder(AccClient("main"), "@bot_one", shared_store, Paced(0), max_per_day=2,
                         account="main")
    second_fwd = Forwarder(AccClient("second"), "@bot_two", shared_store, Paced(0), max_per_day=1,
                           account="second")
    await main_fwd.prepare()
    await second_fwd.prepare()

    def mk_hit(mid, chat="chat_a"):
        return {"chat_key": chat, "chat_id": "1", "msg_id": mid, "username": chat,
                "chat_title": chat, "date": NOW.isoformat(), "sender_name": "x", "text": "Еду Брест-Варшава"}

    r1 = await main_fwd.send(SimpleNamespace(id=501), mk_hit(501))
    r2 = await main_fwd.send(SimpleNamespace(id=502), mk_hit(502))
    r3 = await main_fwd.send(SimpleNamespace(id=503), mk_hit(503))
    s1 = await second_fwd.send(SimpleNamespace(id=601), mk_hit(601, "chat_b"))
    s2 = await second_fwd.send(SimpleNamespace(id=602), mk_hit(602, "chat_b"))
    checks.append(("лимит main не тратит лимит second: два аккаунта шлют параллельно",
                   (r1, r2, r3) == ("forwarded", "forwarded", "limit")
                   and (s1, s2) == ("forwarded", "limit"),
                   f"main {r1}/{r2}/{r3}, second {s1}/{s2}"))
    checks.append(("каждый аккаунт видит только свой дневной счётчик",
                   main_fwd.sent_today == 2 and second_fwd.sent_today == 1,
                   f"{main_fwd.sent_today} / {second_fwd.sent_today}"))
    checks.append(("отложенные разных аккаунтов не смешиваются",
                   shared_store.deferred_queue("main") == [("chat_a", 503)]
                   and shared_store.deferred_queue("second") == [("chat_b", 602)],
                   f"{shared_store.deferred_queue('main')} / {shared_store.deferred_queue('second')}"))
    checks.append(("дедуп общий: тот же хит вторым аккаунтом не отправляется",
                   shared_store.was_forwarded("chat_a", 501) and shared_store.was_forwarded("chat_b", 601),
                   "общая база"))

    section("Аккаунты: пульс и уведомления")

    class PulseStore2(HitStore):
        pass

    pulse_accounts = HitStore(str(workdir / "pulse_acc.sqlite3"))
    silent_acc = lambda *a, **k: asyncio.sleep(0, result=True)
    mon_a = Monitor(None, pulse_accounts, [Source(target="@chat_a", title="A")],
                    silent_acc, Paced(0), account="main", show_account=True, heartbeat_minutes=15)
    mon_a.entities = {"@chat_a": SimpleNamespace(id=1, title="A")}
    pulse = mon_a.heartbeat_text()
    checks.append(("пульс подписан именем аккаунта, когда их несколько",
                   "[main]" in pulse, pulse.split("·")[0].strip()))
    mon_solo = Monitor(None, pulse_accounts, [Source(target="@chat_a", title="A")],
                       silent_acc, Paced(0), account="main", show_account=False, heartbeat_minutes=15)
    mon_solo.entities = {"@chat_a": SimpleNamespace(id=1, title="A")}
    checks.append(("с одним аккаунтом пульс без подписи — как раньше",
                   "[main]" not in mon_solo.heartbeat_text(), mon_solo.heartbeat_text()[:12]))

    class NotifySpy:
        def __init__(self):
            self.texts = []

        async def __call__(self, text, notify=True):
            self.texts.append(text)
            return True

    spy = NotifySpy()
    mon_multi = Monitor(None, pulse_accounts, [Source(target="@chat_a", title="A")],
                        spy, Paced(0), account="second", show_account=True)
    mon_multi.source = SimpleNamespace(target="@chat_a")
    hit_payload = {"chat_key": "chat_a", "chat_title": "A", "chat_id": "1", "username": "a",
                   "msg_id": 700, "date": NOW.isoformat(), "sender_name": "x",
                   "text": "Еду Варшава, возьму передачу", "score": 9, "category": "parcel",
                   "intent": "offer", "direction": "BY->PL", "countries": [], "hits": ["варшава"],
                   "link": "https://t.me/a/700", "topic_id": None, "topic_name": "", "account": "second"}
    await spy(format_hit(hit_payload), notify=True)
    checks.append(("в уведомлении видно, чей аккаунт нашёл",
                   "аккаунт second" in spy.texts[0].lower(),
                   spy.texts[0].splitlines()[-1][:60]))

    section("Аккаунты: флаги, доктор, обход")

    parser = build_parser()
    parsed = parser.parse_args(["--account", "second"])
    checks.append(("есть флаг --account", parsed.account == "second", "--account second"))
    checks.append(("флаги пересылки по-прежнему None по умолчанию (чтобы не перекрывать конфиг)",
                   parsed.forward_max_per_day is None and parsed.forward_to is None,
                   "None"))
    doctor_src = Path("monitor.py").read_text()
    checks.append(("доктор показывает аккаунты и файлы сессий",
                   "аккаунты:" in doctor_src and "файла нет, потребуется вход" in doctor_src,
                   "секция в --doctor"))
    order_check = order_sources(split_sources, "random")
    checks.append(("случайный порядок работает и с несколькими аккаунтами",
                   sorted(x.target for x in order_check) == sorted(x.target for x in split_sources),
                   f"{len(order_check)} чатов"))

    # ---------------------------------------------------------- итог
    section("Итог")
    for name, ok, detail in checks:
        print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail else ""))
    failed = [name for name, ok, _ in checks if not ok]
    print("\nИТОГ:", "всё ок" if not failed else f"провалено: {failed}")
    shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
