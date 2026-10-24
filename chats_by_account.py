#!/usr/bin/env python3
"""
Список «какие чаты на каком аккаунте» из sources.yaml -> ACCOUNTS.md.

    python chats_by_account.py            # перезаписать ACCOUNTS.md
    python chats_by_account.py --check    # код возврата 1, если ACCOUNTS.md устарел

Нужен, чтобы вручную пройти по чатам аккаунта (войти, выйти, вступить) и ничего не забыть.
Важно: если в R2 лежит config/sources.yaml, он перекрывает репозиторий — реальную привязку в облаке
показывает /chats в боте; этот файл отражает sources.yaml из репозитория.
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "ACCOUNTS.md"


def link_of(target: str) -> tuple[str, bool]:
    """(ссылка для открытия, приватное ли приглашение)."""
    target = str(target).strip()
    if target.startswith("http"):
        return target, "joinchat" in target or "/+" in target
    return "https://t.me/" + target.lstrip("@"), False


def build(config: dict | None = None) -> str:
    config = config if config is not None else yaml.safe_load((ROOT / "sources.yaml").read_text(encoding="utf-8"))
    sources = config.get("sources") or []
    declared = list((config.get("accounts") or {}).keys())
    names = declared + sorted({str(s.get("account") or "main") for s in sources} - set(declared))
    lines = ["# Какие чаты на каком аккаунте", "",
             "Файл собирается из `sources.yaml` командой `python chats_by_account.py` — руками не править.",
             "Если в R2 лежит `config/sources.yaml`, он перекрывает репозиторий: реальную картину в облаке даёт `/chats`.",
             "", "| аккаунт | чатов | из них приватные ссылки |", "|---|---|---|"]
    per = {name: [s for s in sources if str(s.get("account") or "main") == name] for name in names}
    for name in names:
        private = sum(1 for s in per[name] if link_of(s["target"])[1])
        lines.append(f"| {name} | {len(per[name])} | {private} |")
    for name in names:
        lines += ["", f"## {name} — {len(per[name])} чатов", "", "| # | чат | ссылка | порог |", "|---|---|---|---|"]
        ordered = sorted(per[name], key=lambda s: (not link_of(s["target"])[1], str(s.get("title") or s["target"]).lower()))
        for number, source in enumerate(ordered, start=1):
            url, private = link_of(source["target"])
            title = str(source.get("title") or ("приватный чат (без названия)" if private else source["target"]))
            title = title.replace("|", "/")
            lines.append(f"| {number} | {title}{' 🔑' if private else ''} | {url} | {source.get('min_score', '—')} |")
    lines += ["", "🔑 — приватная ссылка-приглашение: заходить только по ней, по имени чат не найдётся.", ""]
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    text = build()
    if "--check" in argv:
        return 0 if OUT.exists() and OUT.read_text(encoding="utf-8") == text else 1
    OUT.write_text(text, encoding="utf-8")
    print(f"записано {OUT.name}: {text.count(chr(10))} строк")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
