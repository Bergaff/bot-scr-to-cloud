#!/usr/bin/env python3
"""
Офлайн-тест логики scraper.py: подсовываем фейковый Telegram-клиент,
поэтому аккаунт и API ID/HASH не нужны. Проверяем поиск, локальный фильтр,
границы дат, инкрементальный state.json и выгрузку.

Запуск:  python3 selftest.py
"""
from __future__ import annotations

import asyncio
import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import scraper

NOW = datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc)


def fake_message(mid: int, text: str, days_ago: int = 0, views: int = 100):
    return SimpleNamespace(
        id=mid, text=text, date=NOW - timedelta(days=days_ago), views=views,
        forwards=1, sender_id=7, reply_to_msg_id=None, media=None,
    )


CORPUS = [
    fake_message(500, "Ключевую ставку снова повысили: ипотека дорожает", 1),
    fake_message(499, "Просто мем про котиков", 2),
    fake_message(498, "Кредитный калькулятор и выгодный кредит", 3),
    fake_message(497, "Поставка оборудования в офис", 4),
    fake_message(100, "Старая новость про ипотеку (вне окна дат)", 400),
]


class FakeClient:
    def __init__(self, corpus):
        self.corpus = corpus

    async def get_entity(self, chat):
        return SimpleNamespace(username=chat, title="Fake channel")

    def iter_messages(self, entity, **kwargs):  # Telethon отдаёт async-итератор
        limit, until = kwargs.get("limit"), kwargs.get("offset_date")

        async def gen():
            sent = 0
            for message in self.corpus:  # новые -> старые, как в реальном API
                if until and message.date >= until:
                    continue
                yield message
                sent += 1
                if limit and sent >= limit:
                    break

        return gen()


class FakeSearchClient(FakeClient):
    def __init__(self, corpus):
        super().__init__(corpus)
        self.calls = 0

    async def get_messages(self, entity, limit=None, search=None):
        self.calls += 1
        needle = scraper.norm(search)
        return [m for m in self.corpus if needle in scraper.norm(m.text)][:limit]


async def run() -> None:
    if Path("state.json").exists():
        Path("state.json").unlink()
    checks: list[tuple[str, bool, str]] = []

    # 1. history + фильтр по словам и по датам
    matchers = scraper.build_matchers(["ипотека", "кредит"])
    rows = await scraper.scrape_history(
        FakeClient(CORPUS), "@fake", ["ипотека", "кредит"], matchers, None,
        NOW - timedelta(days=10), None, scraper.Paced(0),
    )
    ids = sorted(r["message_id"] for r in rows)
    checks.append(("фильтр по словам (ставку/ипотеку/кредит)", ids == [498, 500], str(ids)))
    checks.append(("граница --since отсекла старое сообщение", 100 not in ids, str(ids)))
    checks.append(("метки ключевых слов проставлены", rows[0]["keywords"] != [], str(rows[0])))

    # 2. инкрементальность: повторный прогон не должен ничего добавить
    rows2 = await scraper.scrape_history(
        FakeClient(CORPUS), "@fake", ["ипотека", "кредит"], matchers, None,
        NOW - timedelta(days=10), None, scraper.Paced(0),
    )
    checks.append(("state.json: повторный прогон = 0 новых", rows2 == [], f"{len(rows2)} строк"))

    # 3. выгрузка
    scraper.export(rows, "out/selftest.jsonl")
    written = [json.loads(line) for line in Path("out/selftest.jsonl").read_text(encoding="utf-8").splitlines()]
    checks.append(("экспорт jsonl+csv", len(written) == len(rows) and Path("out/selftest.csv").exists(),
                   f"{len(written)} строк"))

    # 4. режим search (серверный поиск) + FloodWait-ретрай
    client = FakeSearchClient(CORPUS)
    found = await scraper.scrape_search(client, "@fake", ["ипотека", "кредит"], 50, scraper.Paced(0))
    checks.append(("режим search: 2 слова = 2 запроса", client.calls == 2, f"{client.calls} запросов, {len(found)} строк"))

    # 5. обработка FloodWait: первый вызов падает, второй проходит
    from telethon.errors import FloodWaitError
    flaky_state = {"n": 0}

    async def flaky():
        flaky_state["n"] += 1
        if flaky_state["n"] == 1:
            raise FloodWaitError(request=None, capture=1)  # Telegram просит 1 секунду
        return "ok"

    result = await scraper.call(flaky, scraper.Paced(0), label="flood-test")
    checks.append(("FloodWait пережит и запрос повторён", result == "ok" and flaky_state["n"] == 2, str(flaky_state)))

    if Path("state.json").exists():
        Path("state.json").unlink()
    shutil.rmtree("out", ignore_errors=True)

    print("\n=== результаты офлайн-теста ===")
    for name, ok, detail in checks:
        print(f"{'PASS' if ok else 'FAIL'}  {name}  ({detail})")
    failed = [n for n, ok, _ in checks if not ok]
    print("\nИТОГ:", "всё ок" if not failed else f"провалено: {failed}")


if __name__ == "__main__":
    asyncio.run(run())
