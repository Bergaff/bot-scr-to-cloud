#!/usr/bin/env python3
"""
Поиск по словам в ПУБЛИЧНЫХ каналах через веб-превью t.me/s/<channel>?q=<слово>.
Аккаунт Telegram НЕ нужен -> забанить нечего. Только публичные каналы (есть username).

Пример:
  python3 web_search.py --channels telegram,durov --keywords "update,release" --limit 2 --out found.jsonl
"""
from __future__ import annotations

import argparse
import html as html_lib
import json
import re
import sys
import time
import urllib.parse
import urllib.request

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

TEXT_DIV = re.compile(
    r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)'
    r'(?=<div class="tgme_widget_message_footer|</div>\s*</div>\s*</div>)',
    re.S,
)
TAGS = re.compile(r"<[^>]+>")


def clean(fragment: str) -> str:
    fragment = re.sub(r"<br\s*/?>", "\n", fragment)
    fragment = TAGS.sub("", fragment)
    return html_lib.unescape(fragment).strip()


def fetch(url: str, timeout: int = 20, retries: int = 3) -> str:
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "en,ru;q=0.8"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", "replace")
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"{url} -> {last}")


def parse_page(page: str, channel: str) -> list[dict]:
    out: list[dict] = []
    for chunk in page.split("tgme_widget_message_wrap")[1:]:
        post = re.search(r'data-post="([^"/]+)/(\d+)"', chunk)
        if not post:
            continue
        dt = re.search(r'<time[^>]*datetime="([^"]+)"', chunk)
        views = re.search(r'tgme_widget_message_views[^>]*>([^<]+)<', chunk)
        text_m = TEXT_DIV.search(chunk)
        out.append(
            {
                "channel": post.group(1),
                "message_id": int(post.group(2)),
                "date": dt.group(1) if dt else None,
                "views": views.group(1).strip() if views else None,
                "text": clean(text_m.group(1)) if text_m else "",
                "url": f"https://t.me/{post.group(1)}/{post.group(2)}",
            }
        )
    return out


def search_channel(channel: str, query: str, pages: int, delay: float) -> list[dict]:
    channel = channel.strip().lstrip("@").rstrip("/")
    found: list[dict] = []
    before = None
    for page_no in range(pages):
        params = {"q": query}
        if before:
            params["before"] = str(before)
        url = f"https://t.me/s/{channel}?" + urllib.parse.urlencode(params)
        page = fetch(url)
        batch = parse_page(page, channel)
        if not batch:
            break
        found.extend(batch)
        new_before = min(m["message_id"] for m in batch)
        if before is not None and new_before >= before:
            break
        before = new_before
        print(f"  [{channel}] стр.{page_no + 1}: +{len(batch)} сообщ. (before={before})", file=sys.stderr)
        time.sleep(delay)
    return found


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--channels", required=True, help="через запятую: telegram,durov")
    ap.add_argument("--keywords", required=True, help="через запятую; по каждому идёт отдельный запрос")
    ap.add_argument("--limit", type=int, default=1, help="сколько страниц по ~20 сообщений на слово")
    ap.add_argument("--delay", type=float, default=2.0, help="пауза между запросами, сек")
    ap.add_argument("--out", default="found.jsonl")
    args = ap.parse_args()

    channels = [c for c in (c.strip() for c in args.channels.split(",")) if c]
    keywords = [k for k in (k.strip() for k in args.keywords.split(",")) if k]
    seen: set[tuple[str, int]] = set()
    rows: list[dict] = []

    for ch in channels:
        for kw in keywords:
            print(f"[*] {ch} <- '{kw}'", file=sys.stderr)
            try:
                for item in search_channel(ch, kw, args.limit, args.delay):
                    key = (item["channel"], item["message_id"])
                    if key in seen:
                        continue
                    seen.add(key)
                    item["keyword"] = kw
                    rows.append(item)
            except Exception as exc:  # noqa: BLE001
                print(f"[!] {ch}: {exc}", file=sys.stderr)

    with open(args.out, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"[+] найдено {len(rows)} сообщений -> {args.out}")


if __name__ == "__main__":
    main()
