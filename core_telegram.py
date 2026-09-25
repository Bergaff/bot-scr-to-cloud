#!/usr/bin/env python3
"""
Общие кирпичи для Telegram-инструментов: тормоз с джиттером, вызовы с уважением
к FloodWait, разрешение target -> entity, форматирование совпадений, уведомления.

Telethon импортируется ЛЕНИВО (внутри функций), поэтому модуль можно
использовать в офлайн-тестах без библиотеки и без аккаунта.
"""
from __future__ import annotations

import asyncio
import os
import random
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

def load_dotenv(path: str = ".env", override: bool = False) -> int:
    """Простой загрузчик .env (без зависимостей): KEY=VALUE, строки с # игнорируются.
    Уже заданные переменные окружения имеют приоритет, если override=False."""
    env_file = Path(path)
    if not env_file.exists():
        return 0
    loaded = 0
    for line in env_file.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip('"').strip("'")
        if key and (override or key not in os.environ):
            os.environ[key] = value
            loaded += 1
    return loaded


HEADERS = {
    "parcel": "📦 Посылка/передача",
    "ride": "🚗 Попутчик/пассажир",
    "mixed": "📦🚗 Посылки + попутчики",
    "any": "❓ Сигнал",
}
INTENTS = {"offer": "предлагаю", "request": "ищу", "any": "объявление"}


def display_name(entity, fallback: str = "аккаунт") -> str:
    """Имя для вывода: у части аккаунтов нет first_name (только username или ничего)."""
    if entity is None:
        return fallback
    parts = [getattr(entity, "first_name", None), getattr(entity, "last_name", None)]
    name = " ".join(part for part in parts if part).strip()
    if name:
        return name
    if getattr(entity, "username", None):
        return f"@{entity.username}"
    title = getattr(entity, "title", None)
    return title or f"id={getattr(entity, 'id', '?')}"


class Paced:
    """Гарантирует паузу между вызовами API + джиттер (ровный робот-ритм тоже палится)."""

    def __init__(self, base_delay: float = 2.0, jitter: float = 0.7):
        self.base, self.jitter, self._last = base_delay, jitter, 0.0
        self.calls = 0          # сколько раз сходили в Telegram: метрика расхода (ТЗ §15.1)

    async def wait(self) -> None:
        gap = self.base + random.uniform(0, self.jitter)
        loop = asyncio.get_running_loop()
        elapsed = loop.time() - self._last
        if elapsed < gap:
            await asyncio.sleep(gap - elapsed)
        self._last = loop.time()
        self.calls += 1         # каждый вызов API проходит через тормоз, поэтому счёт здесь точен


# Наблюдатели FloodWait: радар подключает сюда запись в базу, чтобы бот-панель показывала
# «ограничение до 22:10» (ТЗ §14.1). Хук не обязан быть: без него всё работает как раньше.
FLOOD_HOOKS: list = []


def add_flood_hook(hook) -> None:
    """Подключает наблюдателя FloodWait: hook(label, seconds, wait_seconds)."""
    if callable(hook) and hook not in FLOOD_HOOKS:
        FLOOD_HOOKS.append(hook)


def notify_flood(label: str, seconds: float, wait: float) -> None:
    """Оповещает наблюдателей о FloodWait. Сбой наблюдателя не должен ронять вызов API."""
    for hook in list(FLOOD_HOOKS):
        try:
            hook(label, seconds, wait)
        except Exception:                   # noqa: BLE001
            pass


async def call(factory, paced: Paced, retries: int = 4, label: str = ""):
    """Вызов API: при FloodWait спим ровно столько, сколько просит Telegram, плюс буфер."""
    from telethon.errors import FloodWaitError, RPCError  # ленивый импорт

    for attempt in range(retries):
        await paced.wait()
        try:
            return await factory()
        except FloodWaitError as exc:
            wait = exc.seconds * 1.2 + 5
            print(f"[flood] {label}: ждём {wait:.0f} с (Telegram просит {exc.seconds} с)", file=sys.stderr)
            notify_flood(label, exc.seconds, wait)     # бот-панель увидит ограничение (ТЗ §7.3)
            await asyncio.sleep(wait)
            paced.base = min(paced.base * 1.5, 30)
        except RPCError as exc:
            if attempt == retries - 1:
                raise
            print(f"[err] {label}: {type(exc).__name__} {exc}", file=sys.stderr)
            await asyncio.sleep(5 * (attempt + 1))
    raise RuntimeError(f"{label}: не удалось после {retries} попыток")


def make_client(session: str, api_id: int, api_hash: str, delay: float = 2.0, proxy: str | None = None):
    """Создаёт Telethon-клиент. flood_sleep_threshold=0 — FloodWait обрабатываем сами,
    чтобы видеть в логах, где именно Telegram притормозил."""
    from telethon import TelegramClient

    kwargs: dict = {"flood_sleep_threshold": 0}
    if proxy:
        import socks

        match = re.match(r"(socks5|socks4|http)://(?:([^:]+):([^@]+)@)?([^:]+):(\d+)", proxy)
        if not match:
            raise SystemExit("Формат прокси: socks5://user:pass@host:port (нужен pip install pysocks)")
        scheme, user, password, host, port = match.groups()
        kwargs["proxy"] = {
            "proxy_type": {"socks5": socks.SOCKS5, "socks4": socks.SOCKS4, "http": socks.HTTP}[scheme],
            "addr": host, "port": int(port), "username": user, "password": password, "rdns": True,
        }
    return TelegramClient(session, api_id, api_hash, **kwargs)


def invite_hash(target: str) -> str | None:
    """Достаёт hash из ссылки-приглашения: t.me/+ABCD → 'ABCD'. None для обычных ссылок."""
    raw = target.strip()
    if "t.me/+" in raw or "joinchat/" in raw or raw.startswith("+"):
        return raw.rstrip("/").split("/")[-1].lstrip("+")
    return None


async def resolve_targets(client, targets: list[str], paced: Paced,
                          auto_join: bool = False) -> dict[str, object]:
    """'@chat' / 'https://t.me/name' / 'https://t.me/+invite' / '-100…' -> entity.

    Для приватных ссылок-приглашений: если аккаунт уже в чате — разрешается сразу;
    если нет и включён auto_join — подписываемся (ImportChatInvite) и только потом читаем.
    Ошибки по одной цели не роняют остальные."""
    resolved: dict[str, object] = {}
    for raw in targets:
        target = raw.strip()
        if not target:
            continue
        try:
            entity = await call(lambda t=target: client.get_entity(t), paced, label=f"get_entity({target})")
        except Exception as exc:  # noqa: BLE001
            hash_ = invite_hash(target)
            not_member = "not part of" in str(exc) or "Cannot get entity" in str(exc)
            if hash_ and not_member and auto_join:
                entity = await _join_by_invite(client, target, hash_, paced)
                if entity is None:
                    continue
            else:
                print(f"[!] не смог разрешить {target}: {type(exc).__name__} {exc}", file=sys.stderr)
                if hash_ and not_member:
                    print("    Это ссылка-приглашение, а аккаунт в чате не состоит. "
                          "Вступи вручную или запусти с --auto-join.", file=sys.stderr)
                else:
                    print("    проверь, что аккаунт подписан на этот чат и username верный", file=sys.stderr)
                continue
        resolved[target] = entity
        title = getattr(entity, "title", None) or getattr(entity, "username", target)
        print(f"[+] {target} -> «{title}» (id={getattr(entity, 'id', '?')})", file=sys.stderr)
    return resolved


async def _join_by_invite(client, target: str, hash_: str, paced: Paced):
    """Подписка по ссылке-приглашению (используется только при --auto-join)."""
    from telethon.errors import UserAlreadyParticipantError
    from telethon.tl.functions.messages import ImportChatInviteRequest

    try:
        updates = await call(lambda: client(ImportChatInviteRequest(hash_)), paced,
                             label=f"join({target})", retries=2)
        chat = getattr(updates, "chats", [None])[0]
        print(f"[+] подписался по приглашению: {target} -> «{getattr(chat, 'title', target)}»",
              file=sys.stderr)
        return chat
    except UserAlreadyParticipantError:
        print(f"[i] {target}: аккаунт уже участник, но entity не разрешился — "
              "запусти ещё раз, обычно помогает со второго раза.", file=sys.stderr)
    except Exception as exc:  # noqa: BLE001
        print(f"[!] не удалось вступить по {target}: {type(exc).__name__} {exc}", file=sys.stderr)
    return None


def peer_id(entity) -> int | None:
    """id чата в том виде, в каком его присылает Telegram в событиях.

    Важно: у каналов и супергрупп `entity.id` положительный (1665760449), а в апдейтах
    приходит «маркированный» id — с префиксом -100 (-1001665760449). Именно из-за этой
    разницы живой режим раньше не сопоставлял сообщения с источниками из конфига.
    """
    if entity is None:
        return None
    try:
        from telethon import utils
        return utils.get_peer_id(entity)
    except Exception:  # noqa: BLE001
        value = getattr(entity, "id", None)
        return int(value) if value is not None else None


def message_link(chat_username: str | None, chat_id: int | None, msg_id: int) -> str:
    """Ссылка на сообщение: для публичных чатов — /username/id, для приватных — /c/id/id."""
    if chat_username:
        return f"https://t.me/{chat_username}/{msg_id}"
    if chat_id:
        internal = str(chat_id)
        if internal.startswith("-100"):
            internal = internal[4:]
        elif internal.startswith("-"):
            internal = internal[1:]
        return f"https://t.me/c/{internal}/{msg_id}"
    return ""


def topic_of(message) -> int | None:
    """id темы форума у сообщения (None для обычных чатов).

    В Telethon 1.45 у Message нет готового topic_id: берём reply_to.reply_to_top_id,
    а для самого первого сообщения темы — reply_to_msg_id при forum_topic=True.
    """
    header = getattr(message, "reply_to", None)
    top = getattr(header, "reply_to_top_id", None)
    if top:
        return int(top)
    if getattr(header, "forum_topic", False):
        rid = getattr(header, "reply_to_msg_id", None)
        if rid:
            return int(rid)
    top = getattr(message, "reply_to_top_id", None)
    return int(top) if top else None


def topic_title_of(message) -> str | None:
    """Название темы, если это служебное сообщение о её создании (MessageActionTopicCreate)."""
    action = getattr(message, "action", None)
    title = getattr(action, "title", None)
    return str(title) if title else None


async def collect_topics(client, entity, paced, limit: int = 300) -> list[tuple[int, str, int]]:
    """Темы форум-чата: [(id темы, название, сообщений в выборке)].

    В Telethon 1.45 нет GetForumTopics, поэтому идём по последним сообщениям: имена дают
    служебные сообщения о создании темы, остальные темы видны по reply_to_top_id.
    """
    messages = await call(lambda: client.get_messages(entity, limit=limit), paced,
                          label=f"get_messages(topics:{getattr(entity, 'title', entity)})")
    titles: dict[int, str] = {}
    counts: dict[int, int] = {}
    order: list[int] = []
    for message in messages or []:
        topic_id = topic_of(message)
        if topic_id is None:
            continue
        if topic_id not in counts:
            counts[topic_id] = 0
            order.append(topic_id)
        counts[topic_id] += 1
        title = topic_title_of(message)
        if title and topic_id not in titles:
            titles[topic_id] = title
    result = [(topic_id, titles.get(topic_id, f"тема {topic_id}"), counts[topic_id]) for topic_id in order]
    result.sort(key=lambda row: (-row[2], row[0]))
    return result


def hidden_author_reason(message) -> str | None:
    """Почему автора исходного сообщения нельзя открыть в Telegram («hidden by user»).

    Как это выглядит в данных: сообщение переслано от пользователя, который скрыл профиль,
    и Telegram отдаёт только имя (from_name) без идентификатора (from_id). В веб-клиенте у такого
    имени нет peer-id (data-peer-id="0") и стоит класс hidden-profile, а по нажатию видно
    «Hidden by user»: написать человеку нельзя, поэтому для радара такие объявления бесполезны.

    Возвращает текст причины (для лога) либо None, если автор в порядке.
    """
    if hasattr(message, "fwd_from"):
        fwd = message.fwd_from
    else:
        fwd = getattr(message, "forward", None)
        fwd = getattr(fwd, "fwd_from", fwd) if fwd is not None else None
    if fwd is None:
        return None

    from_id = getattr(fwd, "from_id", None)
    from_name = getattr(fwd, "from_name", None)
    if from_id is None and from_name:
        imported = " (импортировано из другого мессенджера)" if getattr(fwd, "imported", None) else ""
        return (f"переслано от скрытого пользователя «{from_name}»{imported}: "
                "профиль закрыт, написать автору нельзя")

    saved_id = getattr(fwd, "saved_from_id", None)
    saved_name = getattr(fwd, "saved_from_name", None)
    if saved_id is None and saved_name:
        return f"переслано из скрытого источника «{saved_name}»: автор недоступен"
    return None


def format_hit(hit: dict, explain: bool = False) -> str:
    """Текст уведомления (plain text, годится и для консоли, и для файла)."""
    when = hit.get("date") or ""
    try:
        when = datetime.fromisoformat(when).astimezone(timezone.utc).strftime("%d.%m %H:%M UTC")
    except Exception:  # noqa: BLE001
        pass
    category = HEADERS.get(hit.get("category", "any"), HEADERS["any"])
    intent = INTENTS.get(hit.get("intent", "any"), "")
    direction = hit.get("direction") or "?"
    chat_title = hit.get("chat_title") or hit.get("chat")
    topic_name = hit.get("topic_name")
    if topic_name:
        chat_title = f"{chat_title} · тема «{topic_name}»"
    text = " ".join((hit.get("text") or "").split())
    if len(text) > 400:
        text = text[:400] + "…"
    who = hit.get("account")
    lines = [
        f"{category} · {intent} · {direction}" + (f" · аккаунт {who}" if who else ""),
        f"{chat_title} · {when} · счёт {hit.get('score')}",
        "",
        text,
        "",
        hit.get("link", ""),
    ]
    if explain and hit.get("hits"):
        lines.append(f"(правила: {', '.join(hit['hits'])})")
    return "\n".join(line for line in lines if line != "")


# ------------------------------------------------------------------ уведомления

async def notify_console(hit: dict, explain: bool = False) -> None:
    print("\n" + "─" * 72)
    print(format_hit(hit, explain))
    sys.stdout.flush()


async def notify_file(hit: dict, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(f"{datetime.now(timezone.utc).isoformat(timespec='seconds')}\t{format_hit(hit)}\n{'=' * 72}\n")


BOT_TOKEN_HINT = (
    "Проверь TG_BOT_TOKEN: он копируется целиком из @BotFather (Мой бот → API Token), "
    "вид 123456789:AAExample... — без кавычек и пробелов. И TG_NOTIFY_CHAT: свой id (узнать "
    "у @userinfobot) или @канал; в ЛС бот сможет писать только после того, как ты нажал у него Start."
)


def bot_error_text(exc: Exception) -> tuple[str, bool]:
    """Читаемый текст ошибки Bot API. Второе значение — фатальная ли (токен/доступ): повторять нет смысла."""
    import urllib.error

    if isinstance(exc, urllib.error.HTTPError):
        if exc.code in (401, 403):
            return (f"HTTP {exc.code} Unauthorized — Telegram не принял токен бота "
                    f"(скопирован не полностью, не тот или бот удалён)", True)
        return (f"HTTP {exc.code} {getattr(exc, 'reason', '')}".strip(), False)
    return (f"{type(exc).__name__}: {exc}", False)


async def bot_get_me(token: str) -> tuple[bool, str]:
    """Проверка токена бота без отправки сообщения: getMe. Возвращает (ок?, что ответил Telegram)."""
    import json as _json
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen(f"https://api.telegram.org/bot{token}/getMe", timeout=20) as response:
            data = _json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        return False, bot_error_text(exc)[0]
    except Exception as exc:  # noqa: BLE001
        return False, f"сеть недоступна или заблокирован api.telegram.org ({type(exc).__name__}: {exc})"
    if data.get("ok"):
        bot = data.get("result", {})
        return True, f"бот @{bot.get('username')} (id={bot.get('id')})"
    return False, str(data.get("description") or data)


async def notify_telegram_bot(hit: dict, token: str, chat: str) -> tuple[str, bool]:
    """Уведомление через бота. Нужны TG_BOT_TOKEN и TG_NOTIFY_CHAT (создай бота у @BotFather).
    В тексте — та же ссылка на сообщение, но гиперссылкой.

    Возвращает ('', False) при успехе либо (текст ошибки, фатальная ли): при 401/403 повторять
    по каждому сообщению бессмысленно — вызывающий код выключает уведомления ботом до конца прогона.
    """
    import json as _json
    import urllib.request

    link = hit.get("link", "")
    text = format_hit(hit).replace(link, "").strip()
    if link:
        text += "\n\n<a href=\"" + link + "\">открыть сообщение ↗</a>"
    payload = _json.dumps({
        "chat_id": chat, "text": text[:4000], "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }).encode()
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage", data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            response.read()
    except Exception as exc:  # noqa: BLE001
        return bot_error_text(exc)
    return "", False


def build_notifier(mode: str, notify_file_path: str = "hits.log", explain: bool = False):
    """mode: console | file | bot | both | none

    Возвращает async-функцию notify(hit) -> bool: True — доставлено (или доставлять некуда),
    False — не доставлено, такое уведомление остаётся неотправленным и его можно догнать
    командой --resend-pending. При 401/403 (неверный токен бота) уведомления ботом
    выключаются на весь прогон — вместо потока одинаковых ошибок одно понятное сообщение.
    """
    token = os.getenv("TG_BOT_TOKEN")
    chat = os.getenv("TG_NOTIFY_CHAT")
    state = {"bot_ok": True, "told_missing": False, "told_fatal": False, "failures": 0}

    async def notify(hit: dict) -> bool:
        delivered = True
        if mode in ("console", "both"):
            await notify_console(hit, explain)
        if mode in ("file", "both"):
            await notify_file(hit, notify_file_path)
        if mode in ("bot", "both") and mode != "none":
            if not (token and chat):
                delivered = False
                if not state["told_missing"]:
                    state["told_missing"] = True
                    print("[!] TG_BOT_TOKEN / TG_NOTIFY_CHAT не заданы — уведомления ботом пропускаю "
                          "(находки всё равно уходят пересылкой). Подсказки: --test-notify", file=sys.stderr)
            elif state["bot_ok"]:
                error, fatal = await notify_telegram_bot(hit, token, chat)
                if error:
                    delivered = False
                    state["failures"] += 1
                    if fatal:
                        state["bot_ok"] = False
                        if not state["told_fatal"]:
                            state["told_fatal"] = True
                            print(f"[!] Уведомления ботом выключены до конца прогона: {error}", file=sys.stderr)
                            print(f"    {BOT_TOKEN_HINT}", file=sys.stderr)
                            print("    Сами находки не теряются: они уходят пересылкой, а уведомления "
                                  "остаются неотправленными — догнать можно так: "
                                  "start.bat --resend-pending 50 --notify bot", file=sys.stderr)
                    else:
                        print(f"[!] уведомление боту не ушло: {error}", file=sys.stderr)
                else:
                    state["failures"] = 0
            else:
                # бот уже отключён (401 и подобное): уведомление считаем НЕдоставленным,
                # чтобы после исправления токена его можно было догнать через --resend-pending
                delivered = False
        return delivered

    notify.bot_state = state          # для диагностики и тестов
    return notify
