#!/usr/bin/env python3
"""
Telegram-скрапер на Telethon 1.x (стабильная ветка) с бережным темпом и
обработкой FloodWait. Ищет по словам двумя способами:

  1) mode=search  -> серверный поиск внутри чата (messages.search): быстро, отдаёт
                     только совпадения. Один запрос на слово на канал.
  2) mode=history -> выкачивает историю и фильтрует локально регулярками/леммами:
                     нужно, когда слов много, нужна морфология или полный срез.

Примеры:
  # поиск по словам
  python3 scraper.py --mode search --channels @channel1,channel2 \
      --keywords "кредит,ипотека" --limit 500 --out out/search.jsonl

  # история канала с локальным фильтром
  python3 scraper.py --mode history --channels @channel1 --since 2026-01-01 \
      --keywords "кредит,ставка" --out out/history.jsonl

  # только выкачать историю без фильтра
  python3 scraper.py --mode history --channels @channel1 --limit 3000 --out out/raw.jsonl
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import random
import re
import sys
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core_telegram import load_dotenv
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.functions.channels import GetFullChannelRequest

try:  # прокси не обязателен
    import socks
except ImportError:  # pragma: no cover
    socks = None

STATE_PATH = Path("state.json")


# ---------------------------------------------------------------- утилиты

def norm(text: str) -> str:
    """Нормализация под поиск: NFC, нижний регистр, ё->е, схлопывание пробелов."""
    text = unicodedata.normalize("NFC", text or "").lower().replace("ё", "е")
    return re.sub(r"\s+", " ", text)


VOWELS = "аеёиоуыэюяйь"


def stem(word: str, cut: int = 2) -> str:
    """Грубый стемминг без зависимостей (для русского):
      - срезаем до 2 окончаний (гласные/й/ь),
      - отдельно срезаем конечное 'к' у длинных слов, чтобы поймать чередование
        к->ч: 'ипотека' -> 'ипоте' -> ловит 'ипотечный'/'ипотеку'.
    Короткие слова не трогаем, чтобы 'банк' не превратился в 'бан'."""
    word = norm(word).strip()
    for _ in range(cut):
        if len(word) > 4 and word[-1] in VOWELS:
            word = word[:-1]
    if len(word) > 5 and word.endswith("к"):
        word = word[:-1]
    return word


def build_matchers(keywords: list[str], regex: bool = False) -> list[tuple[str, re.Pattern]]:
    """Слово / 'фраза из слов' / регулярка (--regex) -> список (метка, паттерн).

    Обычное слово превращается в стем + до 4 букв хвоста, поэтому 'кредит' ловит
    'кредита/кредитный/кредиторов', 'ипотека' — 'ипотеку/ипотечный', а фраза
    'ключевая ставка' — 'ключевую ставку'. Нужна точная морфология — --regex
    или pymorphy3 (см. README)."""
    matchers: list[tuple[str, re.Pattern]] = []
    for kw in keywords:
        if regex:
            matchers.append((kw, re.compile(kw, re.IGNORECASE | re.UNICODE)))
            continue
        parts = [stem(part) for part in norm(kw).split() if part]
        body = r"\s+".join(re.escape(part) + r"[а-я]{0,4}" for part in parts)
        pattern = r"(?<![0-9a-zа-я])" + body + r"(?![0-9a-zа-я])"
        matchers.append((kw, re.compile(pattern, re.IGNORECASE | re.UNICODE)))
    return matchers


def match_keywords(text: str, matchers: list[tuple[str, re.Pattern]]) -> list[str]:
    flat = norm(text)
    return [label for label, pattern in matchers if pattern.search(flat)]


class Paced:
    """Тормоз для API: гарантирует паузу между вызовами + джиттер (чтобы не бить ровным ритмом)."""

    def __init__(self, base_delay: float = 2.0, jitter: float = 0.7):
        self.base, self.jitter, self._last = base_delay, jitter, 0.0

    async def wait(self) -> None:
        gap = self.base + random.uniform(0, self.jitter)
        elapsed = asyncio.get_running_loop().time() - self._last
        if elapsed < gap:
            await asyncio.sleep(gap - elapsed)
        self._last = asyncio.get_running_loop().time()


async def call(factory, paced: Paced, retries: int = 4, label: str = ""):
    """Вызов API с уважением к FloodWait: спим ровно столько, сколько просят (+20% буфер)."""
    for attempt in range(retries):
        await paced.wait()
        try:
            return await factory()
        except FloodWaitError as exc:
            wait = exc.seconds * 1.2 + 5
            print(f"[flood] {label}: ждём {wait:.0f} с (запрос Telegram: {exc.seconds} с)", file=sys.stderr)
            await asyncio.sleep(wait)
            paced.base = min(paced.base * 1.5, 30)  # после флуда — сразу снижаем темп
        except RPCError as exc:
            if attempt == retries - 1:
                raise
            wait = 5 * (attempt + 1)
            print(f"[err] {label}: {type(exc).__name__} {exc} -> ретрай через {wait} с", file=sys.stderr)
            await asyncio.sleep(wait)
    raise RuntimeError(f"{label}: не удалось после {retries} попыток")


def msg_text(message) -> str:
    """Telethon 1.x: .text/.raw_text; v2: .text. Старое .message тоже поддержим."""
    for attr in ("text", "raw_text", "message"):
        value = getattr(message, attr, None)
        if isinstance(value, str) and value:
            return value
    return ""


def to_dict(message, channel: str, keywords: list[str]) -> dict:
    return {
        "source": channel,
        "message_id": message.id,
        "date": message.date.astimezone(timezone.utc).isoformat(),
        "text": msg_text(message),
        "views": getattr(message, "views", None),
        "forwards": getattr(message, "forwards", None),
        "sender_id": getattr(message, "sender_id", None),
        "reply_to": getattr(message, "reply_to_msg_id", None),
        "has_media": bool(getattr(message, "media", None)),
        "url": f"https://t.me/{channel.lstrip('@')}/{message.id}",
        "keywords": keywords,
        "collected_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def export(rows: list[dict], out_path: str) -> None:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    csv_path = path.with_suffix(".csv")
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"[+] {len(rows)} сообщений -> {path} и {csv_path}", file=sys.stderr)


# ---------------------------------------------------------------- логика

async def scrape_search(client, chat: str, keywords: list[str], limit: int, paced: Paced) -> list[dict]:
    entity = await call(lambda: client.get_entity(chat), paced, label=f"get_entity({chat})")
    rows: list[dict] = []
    for kw in keywords:
        print(f"[*] поиск '{kw}' в {chat}", file=sys.stderr)
        batch = await call(
            lambda kw=kw: client.get_messages(entity, limit=limit, search=kw),
            paced,
            label=f"search({chat},{kw})",
        )
        for message in batch or []:
            rows.append(to_dict(message, chat, [kw]))
    return rows


async def scrape_history(
    client, chat: str, keywords: list[str], matchers, limit: int | None,
    since: datetime | None, until: datetime | None, paced: Paced,
) -> list[dict]:
    entity = await call(lambda: client.get_entity(chat), paced, label=f"get_entity({chat})")
    state = json.loads(STATE_PATH.read_text()) if STATE_PATH.exists() else {}
    last_id = int(state.get(chat, 0))
    rows: list[dict] = []
    scanned = 0
    kwargs = {"offset_date": until, "limit": limit}
    async for message in client.iter_messages(entity, **kwargs):
        if message.id <= last_id:          # уже выкачано в прошлый запуск
            break
        if message.date < since:
            break
        scanned += 1
        if scanned % 200 == 0:             # мягкий тормоз раз в 200 сообщений
            await paced.wait()
            print(f"  ...обработано {scanned} (id={message.id})", file=sys.stderr)
        text = msg_text(message)
        hits = match_keywords(text, matchers) if matchers else []
        if matchers and not hits:
            continue
        rows.append(to_dict(message, chat, hits))
    if rows:
        state[chat] = max(row["message_id"] for row in rows)
        STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2))
    return rows


async def main() -> None:
    load_dotenv()

    ap = argparse.ArgumentParser(description="Telegram scraper (Telethon, бережный режим)")
    ap.add_argument("--channels", required=True, help="@канал1,@канал2")
    ap.add_argument("--keywords", default="", help="слова через запятую; пусто = без фильтра (mode=history)")
    ap.add_argument("--regex", action="store_true", help="трактовать ключи как регулярки")
    ap.add_argument("--mode", choices=["search", "history"], default="search")
    ap.add_argument("--limit", type=int, default=500, help="макс. сообщений на канал (search) / всего (history)")
    ap.add_argument("--since", help="YYYY-MM-DD")
    ap.add_argument("--until", help="YYYY-MM-DD")
    ap.add_argument("--delay", type=float, default=2.0, help="базовая пауза между запросами, сек")
    ap.add_argument("--proxy", help="socks5://user:pass@host:1080 (опционально)")
    ap.add_argument("--session", default=os.getenv("TG_SESSION", "scraper_session"))
    ap.add_argument("--api-id", type=int, default=os.getenv("TG_API_ID") and int(os.getenv("TG_API_ID")))
    ap.add_argument("--api-hash", default=os.getenv("TG_API_HASH"))
    ap.add_argument("--out", default="out/messages.jsonl")
    args = ap.parse_args()

    api_id = args.api_id or int(input("api_id: ").strip())
    api_hash = args.api_hash or input("api_hash: ").strip()

    proxy = None
    if args.proxy:
        if socks is None:
            sys.exit("Нужен PySocks: pip install pysocks  (или запусти без --proxy)")
        match = re.match(r"(socks5|socks4|http)://(?:([^:]+):([^@]+)@)?([^:]+):(\d+)", args.proxy)
        if not match:
            sys.exit("Формат прокси: socks5://user:pass@host:port")
        scheme, user, password, host, port = match.groups()
        proxy = {
            "proxy_type": {"socks5": socks.SOCKS5, "socks4": socks.SOCKS4, "http": socks.HTTP}[scheme],
            "addr": host, "port": int(port), "username": user, "password": password, "rdns": True,
        }

    channels = [c.strip() for c in args.channels.split(",") if c.strip()]
    keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
    matchers = build_matchers(keywords, args.regex) if keywords else []
    since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc) if args.since else datetime(1970, 1, 1, tzinfo=timezone.utc)
    until = (datetime.fromisoformat(args.until) + timedelta(days=1)).replace(tzinfo=timezone.utc) if args.until else None
    paced = Paced(args.delay)

    client = TelegramClient(args.session, api_id, api_hash, proxy=proxy, flood_sleep_threshold=0)
    await client.start()  # первый раз спросит телефон и код (и 2FA-пароль), далее сессия сохранена
    me = await client.get_me()
    print(f"[+] вошли как {me.first_name} (id={me.id}); режим={args.mode}, пауза≈{args.delay} с", file=sys.stderr)

    rows: list[dict] = []
    for chat in channels:
        try:
            if args.mode == "search":
                rows.extend(await scrape_search(client, chat, keywords, args.limit, paced))
            else:
                rows.extend(await scrape_history(client, chat, keywords, matchers, args.limit, since, until, paced))
        except RPCError as exc:
            print(f"[!] {chat}: {type(exc).__name__} {exc}", file=sys.stderr)
            if "PEER_FLOOD" in str(exc).upper():
                print("[!] PEER_FLOOD — это уже ограничение аккаунта. Останавливаюсь, "
                      "продолжи не раньше чем через 24-48 ч и с меньшим темпом.", file=sys.stderr)
                break
        except Exception as exc:  # noqa: BLE001
            print(f"[!] {chat}: {exc}", file=sys.stderr)

    await client.disconnect()
    if not rows:
        print("[-] ничего не найдено", file=sys.stderr)
        return
    export(rows, args.out)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] прервано пользователем (сессия и state сохранены)", file=sys.stderr)
