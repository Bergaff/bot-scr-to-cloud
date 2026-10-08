#!/usr/bin/env python3
"""
Версия установленного обновления — чтобы после деплоя видеть, что в облаке работает новый код.

Что показывается (бот: /version, строка в /status, хвост лога прохода):
  * RELEASE  — номер релиза, который правится ВРУЧНУЮ вместе с CHANGELOG (одна строка на релиз);
  * код      — 8 символов sha256 по всем рабочим файлам радара: считается сам, поэтому меняется
               даже если про RELEASE забыли — деплой всё равно будет виден;
  * конфиг   — то же по sources.yaml, который РЕАЛЬНО читает проход (из репозитория или из R2);
  * коммит   — если сборка передала его в переменную окружения (RADAR_COMMIT и аналоги).

Как проверить деплой: до выкладки запомни «код», после выкладки и одного прохода (до 10 минут)
пришли /version — если «код» и RELEASE изменились, новый образ работает.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

RELEASE = "2026.10.08-1"

# Новое — сверху. Одна строка на релиз: что изменилось для пользователя.
CHANGELOG = [
    ("2026.10.08-1", "@granica_BY_LT_PL перенесён из main в second: нагрузка и лимиты делятся ровнее"),
    ("2026.10.07-1", "файл сессии на Диске находится по Telegram-id (SESSION_FILE_SECOND=6721679210); в ошибке показан весь список файлов папки"),
    ("2026.10.06-2", "аккаунты сами вступают в чаты и подают заявки; новые аккаунты подхватываются из accounts; enabled: false"),
    ("2026.10.06-1", "очередь старше суток чистится сама; /boost и кнопки пробивают лимит; авто-окно лимита в 16:00; сессии Pyrogram с Диска конвертируются"),
    ("2026.10.05-3", "RADAR_ON=0 наконец действует: Worker не будит контейнер; выключатели не стираются деплоем"),
    ("2026.10.05-2", "сессии с Диска: одна ссылка на папку + SESSION_FILE_MAIN / SESSION_FILE_SECOND с именами файлов"),
    ("2026.10.05-1", "сессии подгружаются с Google Диска сами; в боте Telegram-id аккаунтов, /chats и видимый выход из аккаунта"),
    ("2026.10.04-1", "сломанная сессия одного аккаунта не останавливает остальные; бот сообщает, что делать"),
    ("2026.10.03-2", "бот отвечает на команды в начале прохода, а не только в конце (раньше при обрыве молчал)"),
    ("2026.10.03-1", "проходы больше не обрываются по таймауту: очередь добирается порциями, состояние сохраняется даже при обрыве"),
    ("2026.10.02-3", "команда /version; +7 чатов для second; не больше 3 вступлений за проход"),
    ("2026.10.02-2", "матчер: пакет/конверт/оказия, «кто-то летит», «еду + маршрут + посылки»"),
    ("2026.10.02-1", "статистика: судьба находок, причины отсева фильтра, дубли; фильтр ?->PL"),
]

# Файлы, от которых зависит поведение радара в контейнере.
CODE_FILES = (
    "monitor.py", "matcher.py", "forwarder.py", "core_telegram.py", "bot_panel.py", "metrics.py",
    "release.py", "deploy/cloud_entry.py", "deploy/r2_state.py", "deploy/session_sync.py",
)
CONFIG_FILE = "sources.yaml"
COMMIT_ENV = ("RADAR_COMMIT", "WORKERS_CI_COMMIT_SHA", "GIT_COMMIT", "SOURCE_COMMIT")

ROOT = Path(__file__).resolve().parent


def _digest(paths: list[Path]) -> str:
    """8 символов sha256 по именам и содержимому файлов; «?» — если ни один не прочитался."""
    digest = hashlib.sha256()
    found = False
    for path in paths:
        try:
            data = path.read_bytes()
        except OSError:
            continue
        found = True
        digest.update(path.name.encode("utf-8"))
        digest.update(data)
    return digest.hexdigest()[:8] if found else "?"


def code_hash(root: Path | None = None) -> str:
    base = Path(root) if root else ROOT
    return _digest([base / name for name in CODE_FILES])


def config_hash(root: Path | None = None) -> str:
    base = Path(root) if root else ROOT
    cwd_copy = Path.cwd() / CONFIG_FILE            # проход запускается из рабочей папки
    path = cwd_copy if cwd_copy.exists() else base / CONFIG_FILE
    return _digest([path])


def commit(env: dict | None = None) -> str:
    env = os.environ if env is None else env
    for name in COMMIT_ENV:
        value = (env.get(name) or "").strip()
        if value:
            return value[:8]
    return ""


def describe(root: Path | None = None) -> dict:
    return {"release": RELEASE, "code": code_hash(root), "config": config_hash(root),
            "commit": commit(), "changelog": list(CHANGELOG)}


def short_line(root: Path | None = None) -> str:
    """Одна строка для /status и лога: «релиз 2026.10.02-3 · код a1b2c3d4»."""
    info = describe(root)
    return f"релиз {info['release']} · код {info['code']}" + (f" · коммит {info['commit']}" if info["commit"] else "")


if __name__ == "__main__":
    print(short_line())
