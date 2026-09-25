#!/usr/bin/env python3
"""
Бот-панель радара: статистика и состояние аккаунтов через Telegram (ТЗ §14).

Зачем: пользователь хочет «через бот в тг видеть статистику и работают ли мои аккаунты».
Бот перестаёт быть только громкоговорителем и становится пультом.

Два режима (оба обязательны, ТЗ §14.4):
  1) встроенный — `monitor.py --bot-panel`: отдельная asyncio-задача в процессе радара,
     у панели есть живые объекты (аккаунты, лимиты, счётчики) и та же база;
  2) отдельный процесс — `monitor.py --panel-only` или `python bot_panel.py`: панель сама
     читает базу (WAL позволяет читать, пока радар пишет) и помечает в /status,
     что данные из базы и когда они обновлены.

Чего панель НЕ делает (ТЗ §14.3, §18.1):
  * не открывает Telethon-клиенты и не трогает файлы .session — «жив ли аккаунт»
    определяется по косвенным признакам: пульс, события, ошибки, FloodWait;
  * не пишет в Telegram от имени аккаунта — только от бота (Bot API).

Без новых зависимостей: Bot API вызывается через urllib в отдельном потоке
(asyncio.to_thread), чтобы не блокировать цикл Telethon.
"""
from __future__ import annotations

import asyncio
import io
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core_telegram import BOT_TOKEN_HINT
import metrics as metrics_module

API_URL = "https://api.telegram.org"
MESSAGE_LIMIT = 4000            # реальный лимит Bot API 4096 — берём с запасом на пометки
POLL_TIMEOUT = 25               # long polling, сек (ТЗ §14.4)
HEAVY_COMMANDS = {"usage", "stats", "report", "export"}

# Ошибки сессии, после которых аккаунт сам не оживёт: нужен повторный вход (ТЗ §14.6).
SESSION_FATAL_KINDS = ("AuthKeyDuplicatedError", "PhoneNumberBannedError", "SessionRevoked",
                       "SessionPasswordNeededError", "UserDeactivatedBanError")

HELP_LINES = [
    ("status", "общий статус: режим, аптайм, аккаунты, находки, очередь, ошибки"),
    ("accounts", "по каждому аккаунту: жив ли, пульс, счётчик/лимит, ошибки"),
    ("stats [N]", "статистика за N дней (по умолчанию 7)"),
    ("report", "прислать файл stats.txt документом"),
    ("queue", "очередь отложенных пересылок: сколько и что самое старое"),
    ("last [N]", "последние N находок (по умолчанию 5)"),
    ("sources", "по каждому чату: прочитано, найдено за сутки, ошибки"),
    ("errors [N]", "последние N ошибок (по умолчанию 5)"),
    ("usage", "расход ресурсов и вердикт «A или B»"),
    ("ping", "проверка связи с ботом"),
    ("mode", "текущий режим работы (A — слушатель / B — проходы)"),
    ("digest on|off", "утренний дайджест в 09:00 местного времени"),
    ("export [N]", "stats.csv за N дней документом (+ находки за 24 ч)"),
    ("top", "топ чатов за сутки по находкам"),
    ("limits", "лимиты пересылок и сколько осталось по аккаунтам"),
    ("why <id|ссылка>", "почему сообщение взяли или не взяли"),
]

COMMANDS = {line[0].split()[0] for line in HELP_LINES}


# -----------------------------------------------------------------------------
# Мелкие помощники
# -----------------------------------------------------------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def local_time(value: str | datetime | None) -> str:
    """UTC ISO-строка -> местное время «21:12» (для человека в телефоне)."""
    stamp = value
    if isinstance(stamp, str):
        try:
            stamp = datetime.fromisoformat(stamp)
        except ValueError:
            return ""
    if not isinstance(stamp, datetime):
        return ""
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone().strftime("%H:%M")


def minutes_ago(value: str | datetime | None) -> float | None:
    """Сколько минут назад было событие (None — если время не разобрать)."""
    stamp = value
    if isinstance(stamp, str):
        try:
            stamp = datetime.fromisoformat(stamp)
        except ValueError:
            return None
    if not isinstance(stamp, datetime):
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return max(0.0, (utc_now() - stamp).total_seconds() / 60.0)


def ago_text(minutes: float | None) -> str:
    """«1 мин назад» / «34 мин назад» / «2 ч назад»."""
    if minutes is None:
        return "нет данных"
    if minutes < 1:
        return "только что"
    if minutes < 60:
        return f"{int(minutes)} мин назад"
    if minutes < 60 * 24:
        hours = int(minutes // 60)
        rest = int(minutes % 60)
        return f"{hours} ч" + (f" {rest} мин назад" if rest else " назад")
    return f"{int(minutes // 1440)} дн назад"


def truncate(text: str, limit: int = MESSAGE_LIMIT, hint: str = "/report") -> str:
    """Обрезает длинный ответ по границе строки и добавляет подсказку (ТЗ §14.5)."""
    if len(text) <= limit:
        return text
    note = f"\n…\n(обрезано до {limit} символов: полный отчёт — {hint})"
    cut = limit - len(note)
    head = text[:max(0, cut)]
    if "\n" in head:
        head = head[:head.rfind("\n")]
    return head + note


def parse_owner_chat(raw: str | int | None) -> tuple[str | None, str]:
    """Проверяет TG_NOTIFY_CHAT: панель принимает команды только от числового id владельца.

    Возвращает (chat_id, "") либо (None, понятное сообщение) — с каналом по @имени панель
    не работает: команды пришли бы из канала, а отвечать боту некуда (ТЗ §14.4, п.1).
    """
    if raw is None or str(raw).strip() == "":
        return None, ("TG_NOTIFY_CHAT не задан: нужен твой числовой id (узнать — у @userinfobot). "
                      "Команды панели принимаются только в личке с ботом.")
    text = str(raw).strip()
    if re.fullmatch(r"-?\d+", text):
        return text, ""
    return None, (f"TG_NOTIFY_CHAT=«{text}» — это не числовой id. Панель принимает команды только "
                  "в личке с ботом: напиши боту /start, узнай свой id у @userinfobot и впиши его "
                  "в .env (вид: 123456789).")


@dataclass
class Answer:
    """Ответ панели: текст и (необязательно) файлы, которые надо отправить документом."""

    text: str = ""
    documents: list[str] = field(default_factory=list)
    heavy: bool = False
    ping: bool = False        # /ping: текст дополняется замером задержки уже в async-слое


# -----------------------------------------------------------------------------
# Транспорт Bot API (без новых зависимостей)
# -----------------------------------------------------------------------------

class HttpTransport:
    """Вызовы Bot API через urllib. Синхронный HTTP уезжает в отдельный поток."""

    def __init__(self, token: str, api_url: str = API_URL, timeout: int = POLL_TIMEOUT):
        self.token = token
        self.api_url = api_url.rstrip("/")
        self.timeout = timeout
        self.calls: list[str] = []          # для диагностики и тестов

    # --- синхронная часть

    def _url(self, method: str) -> str:
        return f"{self.api_url}/bot{self.token}/{method}"

    def _request(self, method: str, payload: dict | None = None, timeout: int | None = None) -> dict:
        self.calls.append(method)
        data = json.dumps(payload or {}).encode("utf-8")
        request = urllib.request.Request(
            self._url(method), data=data, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(request, timeout=timeout or self.timeout) as response:
            return json.loads(response.read().decode("utf-8", errors="replace"))

    def _request_file(self, method: str, fields: dict, file_field: str, path: str,
                      timeout: int | None = None) -> dict:
        """multipart/form-data руками: sendDocument без библиотеки requests."""
        self.calls.append(method)
        boundary = uuid.uuid4().hex
        body = io.BytesIO()
        for key, value in fields.items():
            body.write(f"--{boundary}\r\n".encode())
            body.write(f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode())
            body.write(f"{value}\r\n".encode("utf-8"))
        name = Path(path).name
        body.write(f"--{boundary}\r\n".encode())
        body.write(f'Content-Disposition: form-data; name="{file_field}"; '
                   f'filename="{name}"\r\n'.encode("utf-8", errors="replace"))
        body.write(b"Content-Type: application/octet-stream\r\n\r\n")
        body.write(Path(path).read_bytes())
        body.write(f"\r\n--{boundary}--\r\n".encode())
        request = urllib.request.Request(
            self._url(method), data=body.getvalue(),
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        )
        with urllib.request.urlopen(request, timeout=timeout or 60) as response:
            return json.loads(response.read().decode("utf-8", errors="replace"))

    # --- асинхронная обёртка

    async def get_updates(self, offset: int, timeout: int | None = None) -> list[dict]:
        wait = timeout or self.timeout
        result = await asyncio.to_thread(
            self._request, "getUpdates",
            {"offset": offset, "timeout": wait, "allowed_updates": ["message"]},
            wait + 10,
        )
        if not result.get("ok"):
            raise RuntimeError(str(result.get("description") or result))
        return result.get("result") or []

    async def send_message(self, chat_id: str, text: str) -> tuple[bool, str]:
        try:
            result = await asyncio.to_thread(
                self._request, "sendMessage",
                {"chat_id": chat_id, "text": text[:4096], "disable_web_page_preview": True},
            )
        except Exception as exc:                                  # noqa: BLE001
            return False, _bot_error_text(exc)
        if result.get("ok"):
            return True, ""
        return False, _describe_result(result)

    async def send_document(self, chat_id: str, path: str, caption: str = "") -> tuple[bool, str]:
        fields = {"chat_id": chat_id}
        if caption:
            fields["caption"] = caption[:1024]
        try:
            result = await asyncio.to_thread(
                self._request_file, "sendDocument", fields, "document", path
            )
        except Exception as exc:                                  # noqa: BLE001
            return False, _bot_error_text(exc)
        if result.get("ok"):
            return True, ""
        return False, _describe_result(result)

    async def get_me(self) -> tuple[bool, str]:
        try:
            result = await asyncio.to_thread(self._request, "getMe", {})
        except Exception as exc:                                  # noqa: BLE001
            return False, _bot_error_text(exc)
        if result.get("ok"):
            bot = result.get("result", {})
            return True, f"бот @{bot.get('username')} (id={bot.get('id')})"
        return False, _describe_result(result)


def _bot_error_text(exc: Exception) -> str:
    """Читаемая причина сбоя Bot API (без трейсбеков — ТЗ §5)."""
    if isinstance(exc, urllib.error.HTTPError):
        try:
            payload = json.loads(exc.read().decode("utf-8", errors="replace"))
            described = _describe_result(payload)
            if described:
                return described
        except Exception:                                         # noqa: BLE001
            pass
        if exc.code in (401, 403):
            return f"HTTP {exc.code} Unauthorized — Telegram не принял токен бота"
        return f"HTTP {exc.code} {getattr(exc, 'reason', '')}".strip()
    return f"{type(exc).__name__}: {exc}"


def _describe_result(result: dict) -> str:
    """Текст ошибки из ответа Bot API + retry_after, если Telegram просит подождать."""
    if not isinstance(result, dict) or result.get("ok"):
        return ""
    text = f"HTTP {result.get('error_code', '?')} {result.get('description', '')}".strip()
    retry = (result.get("parameters") or {}).get("retry_after")
    if retry:
        text += f" (retry_after={retry} с)"
    return text


def retry_after_of(text: str) -> int:
    """Достаёт retry_after из текста ошибки: 0, если его нет."""
    match = re.search(r"retry_after=(\d+)", text or "")
    return int(match.group(1)) if match else 0


# -----------------------------------------------------------------------------
# Что панель знает про аккаунты
# -----------------------------------------------------------------------------

@dataclass
class AccountView:
    """Аккаунт глазами панели: имя, файл сессии, дневной лимит, сколько чатов."""

    name: str
    session: str = ""
    max_per_day: int = 0
    chats: int = 0

    @classmethod
    def from_config(cls, account, chats: int = 0) -> "AccountView":
        """Собирает из AccountConfig (monitor.py) или из словаря."""
        if isinstance(account, dict):
            forward = account.get("forward") or {}
            return cls(name=str(account.get("name") or "main"),
                       session=str(account.get("session") or ""),
                       max_per_day=int(forward.get("max_per_day") or 0),
                       chats=int(chats))
        forward = getattr(account, "forward", None) or {}
        return cls(name=str(getattr(account, "name", "main")),
                   session=str(getattr(account, "session", "") or ""),
                   max_per_day=int(forward.get("max_per_day") or 0),
                   chats=int(chats))


# -----------------------------------------------------------------------------
# Панель
# -----------------------------------------------------------------------------

class BotPanel:
    """Приёмник команд и источник статусов. Работает и внутри радара, и отдельным процессом."""

    def __init__(self, store, *, token: str | None = None, chat_id: str | None = None,
                 transport=None, accounts: list | None = None, mode: str = "A",
                 started_at: datetime | None = None, heartbeat_minutes: float = 15.0,
                 alert_silent_minutes: float = 30.0, digest: bool | None = None,
                 panel_only: bool = False, stats_file: str = "stats.txt", stats_days: int = 7,
                 titles: dict | None = None, db_path: str | None = None,
                 poll_timeout: int = POLL_TIMEOUT, min_send_gap: float = 1.0,
                 heavy_gap: float = 5.0, message_limit: int = MESSAGE_LIMIT,
                 clock=None, sleep=None, api_calls_counter=None):
        self.store = store
        self.token = token if token is not None else (os.getenv("TG_BOT_TOKEN") or "")
        raw_chat = chat_id if chat_id is not None else (os.getenv("TG_NOTIFY_CHAT") or "")
        self.owner_chat, self.owner_error = parse_owner_chat(raw_chat)
        self.transport = transport or (HttpTransport(self.token) if self.token else None)
        self.accounts: list[AccountView] = [
            a if isinstance(a, AccountView) else AccountView.from_config(a) for a in (accounts or [])
        ]
        self.mode = (mode or "A").upper()
        self.started_at = started_at
        self.heartbeat_minutes = float(heartbeat_minutes or 0)
        self.alert_silent_minutes = float(alert_silent_minutes or 0)
        self.panel_only = panel_only
        self.stats_file = stats_file
        self.stats_days = stats_days
        self.titles = titles or {}
        self.db_path = db_path or getattr(store, "path", None)
        self.poll_timeout = poll_timeout
        self.min_send_gap = min_send_gap
        self.heavy_gap = heavy_gap
        self.message_limit = message_limit
        self.api_calls_counter = api_calls_counter      # счётчик вызовов Telethon (для /usage)

        self._clock = clock or time.monotonic
        self._sleep = sleep or asyncio.sleep
        self._last_send = -1e9
        self._last_heavy: dict[str, float] = {}
        self._seen_updates: set[int] = set()
        self.offset = self._load_offset()
        self.stopped = False
        self._greeted = False

        if digest is not None:                        # флаг --digest перекрывает сохранённое
            self.store.bot_state_set("digest", "on" if digest else "off")

    # --- состояние панели в базе

    def _load_offset(self) -> int:
        try:
            return int(self.store.bot_state_get("update_offset", "0") or 0)
        except (TypeError, ValueError):
            return 0

    def _save_offset(self) -> None:
        self.store.bot_state_set("update_offset", str(self.offset))

    @property
    def digest_on(self) -> bool:
        return str(self.store.bot_state_get("digest", "off") or "off").lower() in ("on", "1", "true")

    @property
    def alive_window_minutes(self) -> float:
        """Сколько минут без пульса аккаунт ещё считается живым.

        ТЗ §14.1: «не старше 3× heartbeat-интервала». Дополнительно ограничиваем сверху
        значением --alert-silent: иначе аккаунт выглядел бы «живым» ещё 45 минут после
        реальной остановки, а алерт приходил бы раньше, чем менялся статус.
        """
        interval = self.heartbeat_minutes if self.heartbeat_minutes > 0 else 15.0
        by_pulse = 3.0 * interval
        if self.alert_silent_minutes > 0:
            return min(by_pulse, self.alert_silent_minutes)
        return by_pulse

    # --- запуск

    def ready(self) -> tuple[bool, str]:
        """Можно ли стартовать: нужны токен бота и числовой id владельца."""
        if not self.token:
            return False, f"TG_BOT_TOKEN не задан — панель не запустить. {BOT_TOKEN_HINT}"
        if not self.owner_chat:
            return False, self.owner_error
        if self.transport is None:
            return False, "нет транспорта Bot API (не задан TG_BOT_TOKEN)"
        return True, ""

    async def greet(self) -> bool:
        """«панель на связи» при старте. Антиспам: не чаще раза в 5 минут (ТЗ §14.4, п.5)."""
        last = self.store.bot_state_get("panel_greeting_at")
        if last:
            age = minutes_ago(last)
            if age is not None and age < 5.0:
                return False
        accounts_note = ", ".join(a.name for a in self.accounts) or "без аккаунтов"
        mode_text = "A (слушатель)" if self.mode == "A" else "B (проходы по расписанию)"
        text = (f"📡 панель на связи · режим {mode_text}\n"
                f"Аккаунты: {accounts_note}\n"
                f"Команды: /help · статус: /status · аккаунты: /accounts")
        ok, error = await self._send(text)
        if ok:
            self.store.bot_state_set("panel_greeting_at", utc_now().isoformat(timespec="seconds"))
            self._greeted = True
        elif error:
            self._log_error("bot_api", f"приветствие не ушло: {error}")
        return ok

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        """Основной цикл: long polling + алерты. Сбой Bot API радар не роняет (ТЗ §14.4, п.2)."""
        ok, why = self.ready()
        if not ok:
            raise SystemExit(why)

        if self.offset <= 0:
            # первый запуск: сбрасываем накопившиеся апдейты, чтобы не отвечать на старые команды
            try:
                stale = await self.transport.get_updates(-1, timeout=1)
                if stale:
                    self.offset = int(stale[-1].get("update_id", 0)) + 1
                    self._save_offset()
            except Exception as exc:                              # noqa: BLE001
                self._log_error("bot_api", f"getUpdates при старте: {exc}")

        await self.greet()
        backoff = 5.0
        last_alerts = 0.0
        try:
            while not self.stopped and (stop_event is None or not stop_event.is_set()):
                try:
                    updates = await self.transport.get_updates(self.offset, timeout=self.poll_timeout)
                    backoff = 5.0
                    for update in updates:
                        await self.handle_update(update)
                        if self.stopped:
                            break
                except asyncio.CancelledError:
                    raise
                except Exception as exc:                          # noqa: BLE001
                    self._log_error("bot_api", str(exc))
                    await self._sleep(backoff)
                    backoff = min(backoff * 2, 60.0)
                    continue

                now = self._clock()
                if now - last_alerts >= 60.0:
                    last_alerts = now
                    try:
                        await self.check_alerts()
                        self.retention_check()
                    except Exception as exc:                      # noqa: BLE001
                        self._log_error("alerts", str(exc))
        finally:
            # Ctrl+C снимает задачу — отметка об остановке всё равно должна попасть в базу,
            # иначе панель будет считать радар «живым» ещё полчаса (ТЗ §14.4)
            try:
                self.store.log_heartbeat("stop", account="", detail="панель остановлена")
            except Exception:                                     # noqa: BLE001
                pass

    def retention_check(self, days: int = 30) -> None:
        """Раз в сутки чистит heartbeats/metrics/errors старше N дней (ТЗ §7.2)."""
        today = utc_now().date().isoformat()
        if self.store.bot_state_get("retention_day") == today:
            return
        removed = self.store.retention_cleanup(days=days)
        self.store.bot_state_set("retention_day", today)
        if any(removed.values()):
            print(f"[i] панель: ретеншн — "
                  + ", ".join(f"{table} {count}" for table, count in removed.items() if count),
                  file=sys.stderr)

    async def stop(self) -> None:
        self.stopped = True

    # --- приём апдейтов

    async def handle_updates(self, updates: list[dict]) -> list[Answer]:
        """Пачка апдейтов -> ответы (для тестов и разовых прогонов)."""
        answers = []
        for update in updates:
            answer = await self.handle_update(update)
            if answer is not None:
                answers.append(answer)
        return answers

    async def handle_update(self, update: dict) -> Answer | None:
        """Один апдейт: whitelist, идемпотентность, отправка ответа.

        Возвращает Answer, если ответ ушёл (или не ушёл из-за ошибки транспорта),
        и None, если отвечать некому/нечего.
        """
        update_id = int(update.get("update_id") or 0)
        if update_id and update_id < self.offset:
            return None                                   # уже обработан (Telegram повторил доставку)
        if update_id and update_id in self._seen_updates:
            return None                                   # идемпотентность: дважды не отвечаем
        if update_id:
            self._seen_updates.add(update_id)
            self.offset = update_id + 1
            self._save_offset()

        message = update.get("message") or {}
        chat = message.get("chat") or {}
        chat_id = str(chat.get("id") or "")
        text = (message.get("text") or "").strip()

        if self.owner_chat and chat_id != str(self.owner_chat):
            # чужой chat_id: молча игнорируем, но факт пишем в errors (ТЗ §14.4, п.1)
            self._log_error("unauthorized",
                            f"команда от chat_id={chat_id or '?'}: {text[:120] or '(без текста)'}")
            return None
        if not text:
            return None

        answer = await self.answer_text(text)
        await self._deliver(answer)
        return answer

    async def answer_text(self, text: str) -> Answer:
        """Команда -> ответ, включая асинхронные дополнения (замер задержки для /ping)."""
        answer = self.dispatch(text)
        if answer.ping and not answer.text.startswith("pong ·"):
            answer.text = await self.ping_text()
        return answer

    async def _deliver(self, answer: Answer) -> None:
        """Отправка ответа: rate limit 1 сообщение/сек, тяжёлые команды — 1 раз в 5 сек."""
        if not answer.text and not answer.documents:
            return
        await self._throttle(answer)
        if answer.text:
            ok, error = await self._send(answer.text)
            if error:
                self._log_error("bot_api", f"ответ не ушёл: {error}")
                wait = retry_after_of(error)
                if wait:                                  # Telegram просит подождать (ТЗ §14.4, п.7)
                    await self._sleep(wait)
                if not ok:
                    return
        for path in answer.documents:
            _ok, error = await self._send_document(path)
            if error:
                self._log_error("bot_api", f"файл {Path(path).name} не ушёл: {error}")

    async def _throttle(self, answer: Answer) -> None:
        """Не чаще 1 сообщения в секунду (ТЗ §14.4, п.3)."""
        now = self._clock()
        wait = self.min_send_gap - (now - self._last_send)
        if wait > 0:
            await self._sleep(wait)
        self._last_send = self._clock()

    async def _send(self, text: str) -> tuple[bool, str]:
        if self.transport is None or not self.owner_chat:
            return False, "панель не подключена к боту"
        return await self.transport.send_message(self.owner_chat, text)

    async def _send_document(self, path: str, caption: str = "") -> tuple[bool, str]:
        if self.transport is None or not self.owner_chat:
            return False, "панель не подключена к боту"
        if not Path(path).exists():
            return False, f"файл {path} не найден"
        return await self.transport.send_document(self.owner_chat, path, caption)

    def _log_error(self, kind: str, text: str, account: str | None = None) -> None:
        try:
            self.store.log_error(kind, text, account=account)
        except Exception:                                   # noqa: BLE001
            pass
        print(f"[!] панель: {kind}: {text}", file=sys.stderr)

    # --- маршрутизация команд

    def dispatch(self, text: str) -> Answer:
        """Текст команды -> ответ. Синхронный метод: так его удобно проверять тестами."""
        raw = (text or "").strip()
        if not raw:
            return Answer("Пустая команда. Список: /help")
        parts = raw.split()
        head = parts[0].lstrip("/").lower().split("@")[0]     # /status@my_bot -> status
        args = parts[1:]

        if not raw.startswith("/"):
            return Answer(f"Понимаю только команды: напиши /help (получено «{raw[:40]}»)")

        handler = getattr(self, f"cmd_{head}", None)
        if handler is None:
            known = ", ".join(sorted(COMMANDS))
            return Answer(f"Нет команды «/{head}». Доступны: /help\n{known}")
        try:
            result = handler(*args) if args else handler()
        except TypeError:
            return handler()                                # команда без аргументов
        except Exception as exc:                            # noqa: BLE001 - панель не должна падать
            self._log_error("command", f"/{head}: {type(exc).__name__}: {exc}")
            return Answer(f"⚠️ /{head} не сработала: {type(exc).__name__}. "
                          f"Повтори позже или посмотри /errors")
        if isinstance(result, str):
            return Answer(result, heavy=head in HEAVY_COMMANDS)
        return result

    def _heavy_guard(self, name: str) -> str | None:
        """Тяжёлые команды — не чаще раза в 5 секунд (ТЗ §14.4, п.3)."""
        now = self._clock()
        last = self._last_heavy.get(name)
        if last is not None and now - last < self.heavy_gap:
            left = self.heavy_gap - (now - last)
            return f"/{name} можно повторить через {left:.0f} с (защита от частых запросов)"
        self._last_heavy[name] = now
        return None

    # --- данные: аккаунты, пульс, uptime

    def known_accounts(self) -> list[AccountView]:
        """Аккаунты из конфига; в режиме --panel-only — те, о которых знает база."""
        if self.accounts:
            return list(self.accounts)
        names = self.store.accounts_seen() or ["main"]
        return [AccountView(name=name) for name in names]

    def uptime_seconds(self) -> float | None:
        """Аптайм радара: от старта процесса либо от последнего heartbeat(start) в базе."""
        if self.started_at is not None:
            stamp = self.started_at
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            return max(0.0, (utc_now() - stamp).total_seconds())
        start = self.store.last_heartbeat(kind="start")
        if start and start.get("ts"):
            age = minutes_ago(start["ts"])
            return None if age is None else age * 60.0
        return None

    def account_state(self, view: AccountView) -> dict:
        """Всё, что панель знает про один аккаунт (ТЗ §14.1)."""
        name = view.name
        pulse = self.store.last_heartbeat(kind="pulse", account=name)
        age = self.store.heartbeat_age_minutes("pulse", name)
        start = self.store.last_heartbeat(kind="start", account=name)
        stop = self.store.last_heartbeat(kind="stop", account=name)
        event = self.store.last_heartbeat(kind="event", account=name)
        forward = self.store.last_heartbeat(kind="forward", account=name)
        flood = self.store.last_heartbeat(kind="flood", account=name)
        errors_today = self.store.errors_count(since_hours=24, account=name)
        last_error = self.store.errors_recent(limit=1, account=name, since_hours=24 * 7)

        stopped_after_start = bool(stop and (not start or str(stop.get("ts")) > str(start.get("ts"))))
        window = self.alive_window_minutes
        if stopped_after_start and (age is None or age > window):
            status, alive = "остановлен", False
        elif age is None:
            status, alive = "нет данных о пульсе", False
        elif age <= window:
            status, alive = "работает", True
        else:
            status, alive = f"молчит {int(age)} мин", False

        return {
            "name": name,
            "session": view.session or f"{name}_session",
            "chats": view.chats,
            "max_per_day": view.max_per_day,
            "alive": alive,
            "status": status,
            "pulse_age_minutes": age,
            "pulse": pulse or {},
            "event": event or {},
            "forward": forward or {},
            "flood": flood or {},
            "stopped": stop or {},
            "errors_today": errors_today,
            "last_error": (last_error or [{}])[0] if last_error else {},
            "forwarded_today": self.store.forwarded_today(name),
            "queue": self.store.deferred_count(name),
        }

    def account_states(self) -> list[dict]:
        return [self.account_state(view) for view in self.known_accounts()]

    # --- команды

    def cmd_help(self) -> str:
        lines = ["Команды радара (пиши в личке боту):"]
        lines += [f"/{name} — {description}" for name, description in HELP_LINES]
        lines.append("")
        lines.append("Живость аккаунтов определяется по пульсу (--heartbeat), а не по находкам:")
        lines.append("тихий чат ночью — это норма, а не поломка.")
        return "\n".join(lines)

    def cmd_status(self) -> str:
        states = self.account_states()
        chats_total = sum(state["chats"] for state in states) or len(self.titles)
        read_total, read_hour = self._messages_progress()
        hits_today = self.store.hits_today()
        forwarded_today = self.store.forwarded_today()
        limit_total = sum(state["max_per_day"] for state in states)
        queue = self.store.deferred_count()
        errors_today = self.store.errors_count(since_hours=24)

        mode_text = "A (слушатель)" if self.mode == "A" else "B (проходы по расписанию)"
        uptime = self.uptime_seconds()
        lines = [f"📡 Радар · режим {mode_text} · жив {metrics_module.format_duration(uptime)}"]
        lines.append(f"Аккаунты: {len(states)} · чаты: {chats_total} · "
                     f"прочитано всего: {metrics_module.format_number(read_total)} "
                     f"(за час: {metrics_module.format_number(read_hour)})")
        lines.append("")
        lines.append("Сегодня (с 00:00):")
        lines.append(f"  найдено {hits_today} · переслано {forwarded_today}"
                     + (f"/{limit_total}" if limit_total else "") + f" · в очереди {queue}")
        if len(states) > 1:
            lines.append("  " + " · ".join(
                f"{state['name']}: {state['forwarded_today']}"
                + (f"/{state['max_per_day']}" if state["max_per_day"] else "") for state in states))
        lines.append(self._last_event_line("Последнее событие"))
        lines.append(f"Ошибки за сутки: {errors_today}" + (" (см. /errors)" if errors_today else ""))

        if self.panel_only:
            updated = self.store.last_heartbeat()
            age = minutes_ago(updated.get("ts")) if updated else None
            lines.append(f"Данные из базы (обновлено {ago_text(age)})")
        lines.append(f"Обновлено: {datetime.now().astimezone().strftime('%H:%M')}")
        return "\n".join(lines)

    def cmd_accounts(self) -> str:
        states = self.account_states()
        lines = [f"👥 Аккаунты ({len(states)})"]
        for index, state in enumerate(states, start=1):
            lines.append("")
            mark = "" if state["alive"] else " ⚠️"
            lines.append(f"{index}) {state['name']} · {state['status']}{mark}")
            lines.append(self._account_pulse_line(state))
            event = state["event"]
            chats_note = f"чатов {state['chats']}" if state["chats"] else "чатов н/д"
            event_note = ""
            if event.get("ts"):
                chat = self._chat_label(event.get("chat_key"))
                event_note = f" · последнее событие {local_time(event['ts'])} ({chat})"
            lines.append(f"   {chats_note}{event_note}")
            limit = state["max_per_day"]
            lines.append(f"   переслано сегодня {state['forwarded_today']}"
                         + (f"/{limit}" if limit else "") + f" · в очереди {state['queue']}")
            lines.append(self._account_error_line(state))
        lines.append("")
        lines.append("Молчание = нет пульса. Проверить сессии вручную (радар должен быть "
                     "остановлен): monitor.py --check-sessions")
        return "\n".join(lines)

    def _account_pulse_line(self, state: dict) -> str:
        pulse = state["pulse"]
        age = state["pulse_age_minutes"]
        session = state["session"]
        if not pulse.get("ts"):
            return (f"   нет данных о пульсе (радар мог быть запущен без --heartbeat) · "
                    f"сессия {session}")
        when = local_time(pulse["ts"])
        if state["alive"]:
            return f"   пульс {when} ({ago_text(age)}) · сессия {session}"
        return f"   последний пульс {when} ({ago_text(age)}) · сессия {session}"

    def _account_error_line(self, state: dict) -> str:
        """Строка об ошибках аккаунта — по шаблону §14.5.

        FloodWait не дублируется: если последняя ошибка и есть FloodWait, показываем её
        в виде «последняя ошибка 20:41 · FloodWait (до 21:10)». «Всего за сутки» добавляется
        только когда ошибок больше одной, чтобы не шуметь.
        """
        errors = state["errors_today"]
        flood = state["flood"]
        last = state["last_error"] or {}
        if not errors and not flood.get("ts"):
            return "   ошибок за сутки 0"

        flood_note = ""
        if flood.get("ts"):
            detail = (flood.get("detail") or "").strip()
            flood_note = f"FloodWait ({detail})" if detail else "FloodWait"

        pieces = []
        if last.get("ts"):
            kind = str(last.get("kind") or "?")
            if "FloodWait" in kind and flood_note:
                pieces.append(f"последняя ошибка {local_time(last['ts'])} · {flood_note}")
                flood_note = ""                 # уже показали — второй раз не повторяем
            else:
                piece = f"последняя ошибка {local_time(last['ts'])} · {kind}"
                text = str(last.get("text") or "").strip()
                if text:
                    piece += f": {text[:60]}"
                pieces.append(piece)
        if flood_note:
            pieces.append(f"{flood_note} {local_time(flood.get('ts'))}")
        if errors > 1:
            pieces.append(f"всего за сутки {errors}")
        elif not pieces:
            pieces.append(f"ошибок за сутки {errors}")
        return "   " + " · ".join(pieces)

    def cmd_stats(self, days: str = "") -> Answer:
        blocked = self._heavy_guard("stats")
        if blocked:
            return Answer(blocked, heavy=True)
        parsed = self._parse_days(days)
        report = self.store.stats_report(days=parsed, titles_from_config=self.titles)
        text = truncate(f"📊 Статистика за {parsed} дн.\n\n{report}", self.message_limit)
        return Answer(text, heavy=True)

    def cmd_report(self) -> Answer:
        blocked = self._heavy_guard("report")
        if blocked:
            return Answer(blocked, heavy=True)
        path = self._write_stats_file()
        if not path:
            return Answer("Не удалось сформировать stats.txt (проверь права на папку)", heavy=True)
        return Answer(f"📄 Отчёт за {self.stats_days} дн. — файлом", documents=[path], heavy=True)

    def cmd_queue(self) -> str:
        total = self.store.deferred_count()
        lines = [f"⏳ Очередь отложенных пересылок: {total}"]
        if not total:
            lines.append("Пусто: всё, что нашлось, уже отправлено (или лимит не упирался).")
            return "\n".join(lines)
        for state in self.account_states():
            own = state["queue"]
            if own:
                limit = state["max_per_day"]
                lines.append(f"  {state['name']}: {own}"
                             + (f" (лимит {state['forwarded_today']}/{limit})" if limit else ""))
        oldest = self.store.deferred_oldest()
        if oldest:
            lines.append(f"Самая старая позиция висит с {local_time(oldest)} "
                         f"({ago_text(minutes_ago(oldest))})")
        lines.append("Примеры:")
        for item in self.store.deferred_with_links(limit=5):
            label = self._chat_label(item["chat_key"])
            link = item.get("link") or f"(ссылки нет, id {item['msg_id']})"
            lines.append(f"  {local_time(item.get('at'))} · {label}"
                         + (f" · {item['direction']}" if item.get("direction") else "")
                         + f"\n    {link}")
        lines.append("Добор идёт первым делом при следующем запуске (и сразу после полуночи).")
        return "\n".join(lines)

    def cmd_last(self, count: str = "") -> str:
        limit = self._parse_int(count, default=5, low=1, high=50)
        hits = self.store.hits_last(limit=limit)
        if not hits:
            return f"Последних находок нет (проверил {limit}). Статистика: /stats"
        lines = [f"🕒 Последние находки ({len(hits)}):"]
        for hit in hits:
            when = local_time(hit.get("found_at") or hit.get("date"))
            label = self._chat_label(hit.get("chat_key"), hit.get("chat_title"))
            direction = hit.get("direction") or "?"
            link = hit.get("link") or "(ссылка недоступна)"
            account = hit.get("account") or ""
            lines.append(f"[{when}] {label} · {direction}" + (f" · {account}" if account else "")
                         + f"\n  {link}")
        return "\n".join(lines)

    def cmd_sources(self) -> str:
        scanned = self.store.scanned_by_chat()
        matched = self.store.matched_by_chat()
        today = dict(self.store.hits_by_chat_today())
        last_hit = self.store.last_hit_at_by_chat()
        keys = sorted(set(scanned) | set(today) | set(last_hit),
                      key=lambda key: -(scanned.get(key, 0)))
        if not keys:
            return ("Об источниках пока нет данных: радар ещё ничего не прочитал. "
                    "Запусти: monitor.py --once --catchup 20")
        lines = [f"📋 Источники ({len(keys)})"]
        for key in keys:
            label = self._chat_label(key)
            errors = self._chat_errors(key)
            last = last_hit.get(key)
            lines.append(f"{label} · прочитано {metrics_module.format_number(scanned.get(key, 0))} · "
                         f"совпало {matched.get(key, 0)} · за сутки {today.get(key, 0)} · "
                         f"последняя находка {local_time(last) if last else '—'} · ошибок {errors}")
        lines.append("")
        lines.append("«Прочитано» — накопительно с момента ведения статистики; точное время "
                     "последнего прочитанного сообщения не хранится (это запись на каждое "
                     "сообщение), поэтому показана последняя находка.")
        return "\n".join(lines)

    def cmd_errors(self, count: str = "") -> str:
        limit = self._parse_int(count, default=5, low=1, high=50)
        rows = self.store.errors_recent(limit=limit, since_hours=0)
        if not rows:
            return "Ошибок не записано. Если радар молчит — посмотри /accounts (пульс)."
        lines = [f"⚠️ Последние ошибки ({len(rows)}):"]
        for row in rows:
            who = f" [{row['account']}]" if row.get("account") else ""
            text = (row.get("text") or "").strip()
            lines.append(f"[{local_time(row['ts'])}]{who} {row.get('kind') or '?'}"
                         + (f": {text[:120]}" if text else ""))
        return "\n".join(lines)

    def cmd_usage(self) -> Answer:
        blocked = self._heavy_guard("usage")
        if blocked:
            return Answer(blocked, heavy=True)
        return Answer(self.usage_text(), heavy=True)

    def usage_text(self) -> str:
        """Собирает /usage (ТЗ §15.4): живой срез + сводка по строкам metrics за сутки.

        Живые значения важны для «сейчас», суточные — для процессора и вердикта: именно они
        отвечают на вопрос пользователя «насколько жрёт схема A».
        """
        read_total, read_hour = self._messages_progress()
        stored = self.store.metrics_recent(hours=24)
        daily = metrics_module.aggregate(stored)
        floods = len(self.store.heartbeats_since(hours=24, kind="flood"))
        session_errors = sum(self.store.errors_count(since_hours=24, kind=kind)
                             for kind in SESSION_FATAL_KINDS)
        errors_total = self.store.errors_count(since_hours=24)

        api_calls = self._api_calls()
        if not api_calls and stored:
            api_calls = max(int(row.get("api_calls") or 0) for row in stored)

        data = metrics_module.sample(
            msgs_total=read_total or self.store.scanned_total(),
            msgs_last_hour=read_hour,
            api_calls=api_calls,
            db_path=self.db_path,
            hits_total=self.store.hits_total(),
            forwarded_today=self.store.forwarded_today(),
            accounts=len(self.known_accounts()),
            mode=self.mode,
            uptime_s=self.uptime_seconds(),
        )
        per_account = [(state["name"], state["forwarded_today"], state["max_per_day"])
                       for state in self.account_states()]
        _db_mb, wal_mb = metrics_module.db_size_mb(self.db_path) if self.db_path else (None, None)
        peak = metrics_module.peak_msgs_per_min(self.store)

        return metrics_module.format_usage(
            data, floods=floods, errors=errors_total, session_errors=session_errors,
            queue=self.store.deferred_count(), per_account=per_account,
            peak_msgs_per_min=peak, daily=daily,
            project_mb=metrics_module.project_size_mb(Path(__file__).resolve().parent),
            wal_mb=wal_mb,
            day_label=metrics_module.daily_label(stored) if stored else None,
        )

    def _api_calls(self) -> int:
        """Счётчик API-вызовов Telethon: живьём из Paced, в режиме --panel-only — из базы."""
        counter = self.api_calls_counter
        if counter is None:
            return 0
        for attribute in ("calls", "value"):
            value = getattr(counter, attribute, None)
            if value:
                return int(value)
        return 0

    def cmd_ping(self) -> Answer:
        """/ping: ответ «pong · 0.4 с». Замер делается в async-слое (ping_text),
        потому что синхронно ждать Bot API из работающего цикла нельзя."""
        return Answer(text="pong", ping=True)

    async def ping_text(self) -> str:
        started = self._clock()
        ok, info = False, "нет транспорта Bot API"
        if self.transport is not None and hasattr(self.transport, "get_me"):
            try:
                ok, info = await self.transport.get_me()
            except Exception as exc:                        # noqa: BLE001
                ok, info = False, f"{type(exc).__name__}"
        elapsed = self._clock() - started
        return f"pong · {elapsed:.1f} с" + ("" if ok else f" · {info}")

    def cmd_mode(self) -> str:
        mode_text = "A (слушатель: процесс висит и слушает чаты)" if self.mode == "A" else \
                    "B (проходы по расписанию: --once из планировщика)"
        uptime = self.uptime_seconds()
        lines = [f"🔧 Режим: {mode_text}",
                 f"Считается с: {local_time(self.started_at) if self.started_at else '—'}"
                 + (f" (жив {metrics_module.format_duration(uptime)})" if uptime is not None else "")]
        stored = self.store.metrics_recent(hours=24)
        floods = self.store.errors_count(since_hours=24, kind="FloodWaitError")
        if stored:
            summary = metrics_module.aggregate(stored)
            text, reason = metrics_module.verdict(rss=summary.get("rss_avg"),
                                                  cpu=summary.get("cpu_avg"), floods=floods)
            lines.append(f"Совет метрики: {text}" + (f" ({reason})" if reason else ""))
        else:
            lines.append("Совет метрики: данных за сутки ещё нет — /usage покажет, когда наберётся")
        lines.append("Переключить: режим B = monitor.py --once из планировщика (см. HOSTING.md)")
        return "\n".join(lines)

    def cmd_digest(self, value: str = "") -> str:
        wanted = (value or "").strip().lower()
        if wanted in ("on", "вкл", "1", "true", "да"):
            self.store.bot_state_set("digest", "on")
            return "🌅 Утренний дайджест включён: итоги суток придут в 09:00 местного времени."
        if wanted in ("off", "выкл", "0", "false", "нет"):
            self.store.bot_state_set("digest", "off")
            return "Утренний дайджест выключен."
        current = "включён" if self.digest_on else "выключен"
        return f"Утренний дайджест сейчас {current}. Управление: /digest on или /digest off"

    def cmd_export(self, days: str = "") -> Answer:
        blocked = self._heavy_guard("export")
        if blocked:
            return Answer(blocked, heavy=True)
        parsed = self._parse_days(days)
        documents: list[str] = []
        csv_path = Path(self.stats_file).with_suffix(".csv")
        try:
            self.store.stats_csv(str(csv_path))
            documents.append(str(csv_path))
        except Exception as exc:                            # noqa: BLE001
            return Answer(f"Не удалось собрать stats.csv: {type(exc).__name__}", heavy=True)
        hits_path = Path(self.stats_file).with_name("hits-24h.txt")
        try:
            count = self.store.export_txt(str(hits_path), hours=24.0)
            if count:
                documents.append(str(hits_path))
        except Exception:                                   # noqa: BLE001
            pass
        return Answer(f"📦 Выгрузка: stats.csv (за {parsed} дн. — все строки статистики)"
                      + (f" + находки за 24 ч ({hits_path.name})" if len(documents) > 1 else ""),
                      documents=documents, heavy=True)

    def cmd_top(self) -> str:
        rows = self.store.hits_by_chat_today()
        if not rows:
            return "За сутки находок не было — топать нечего. Общая статистика: /stats"
        lines = ["🏆 Топ чатов за сутки (по находкам):"]
        for index, (key, count) in enumerate(rows[:10], start=1):
            lines.append(f"{index}. {self._chat_label(key)} — {count}")
        return "\n".join(lines)

    def cmd_limits(self) -> str:
        states = self.account_states()
        lines = ["🚦 Лимиты пересылок (в сутки, по аккаунтам):"]
        for state in states:
            limit = state["max_per_day"]
            sent = state["forwarded_today"]
            if limit:
                left = max(0, limit - sent)
                note = f"{sent}/{limit} · осталось {left}"
                if left == 0:
                    note += " — лимит выбран, находки встают в очередь (/queue)"
            else:
                note = f"{sent} · лимит не задан (∞)"
            lines.append(f"  {state['name']}: {note} · в очереди {state['queue']}")
        lines.append("Лимит обнулится в местную полночь, добор очереди пойдёт автоматически.")
        return "\n".join(lines)

    def cmd_why(self, target: str = "") -> str:
        """Почему сообщение взяли (или не взяли): разбор правил по сохранённой находке."""
        if not target:
            return "Нужен id сообщения или ссылка: /why 91532 или /why https://t.me/chat/91532"
        msg_id, chat_key = self._parse_target(target)
        hit = None
        if msg_id is not None:
            if chat_key:
                hit = self.store.get_hit(chat_key, msg_id)
            if hit is None:
                for row in self.store.hits_last(limit=500):
                    if int(row.get("msg_id") or 0) == msg_id and (
                            not chat_key or row.get("chat_key") == chat_key):
                        hit = row
                        break
        if hit is None:
            return (f"В базе нет сообщения {target}: значит, радар его не взял "
                    f"(не хватило баллов, дубль, старое или скрытый автор). "
                    f"Последние находки: /last 10 · пороги: /stats")
        from matcher import analyze
        text = hit.get("text") or ""
        match = analyze(text, profile="news" if hit.get("category") == "news" else "chat")
        lines = [
            f"🔎 {self._chat_label(hit.get('chat_key'), hit.get('chat_title'))} · "
            f"id {hit.get('msg_id')} · {local_time(hit.get('date'))}",
            f"Взяли: счёт {hit.get('score')} · категория {hit.get('category')} · "
            f"намерение {hit.get('intent')} · направление {hit.get('direction') or '?'}",
            f"Сработали слова: {hit.get('hits') or '—'}",
            f"Пересылка: {'отправлено' if self.store.was_forwarded(hit.get('chat_key'), int(hit.get('msg_id') or 0)) else 'не отправлено / в очереди'}",
            f"Ссылка: {hit.get('link') or '—'}",
        ]
        annotations = getattr(match, "annotations", None)
        if annotations:
            lines.append("Разбор правил: " + "; ".join(str(a) for a in annotations)[:600])
        return "\n".join(lines)

    # --- внутренние помощники

    def _messages_progress(self) -> tuple[int, int]:
        """(прочитано всего, прочитано за последний час).

        «Всего» — накопительный счётчик scanned из таблицы stats (переживает перезапуски),
        «за час» — разница строк пульса: радар пишет в пульс, сколько прочитал с прошлого раза.
        """
        total = self.store.scanned_total()
        hour = 0
        for view in self.known_accounts():
            read_hour, _found = self.store.pulse_progress(hours=1.0, account=view.name)
            hour += read_hour
        if not hour:                                  # один аккаунт без имени в старых базах
            read_hour, _found = self.store.pulse_progress(hours=1.0)
            hour = read_hour
        if not hour:
            # пульс только начал писаться (нужно два среза, чтобы посчитать разницу) —
            # тогда берём последний сохранённый срез метрик, если он есть
            rows = self.store.metrics_recent(hours=2)
            if rows:
                hour = int(rows[-1].get("msgs_last_hour") or 0)
        return total, hour

    def _last_event_line(self, prefix: str = "Последнее событие") -> str:
        event = self.store.last_heartbeat(kind="event")
        if not event or not event.get("ts"):
            forward = self.store.last_heartbeat(kind="forward")
            if forward and forward.get("ts"):
                return (f"{prefix}: нет (последняя пересылка {local_time(forward['ts'])} · "
                        f"{self._chat_label(forward.get('chat_key'))})")
            return f"{prefix}: нет (радар пока ничего не поймал)"
        detail = (event.get("detail") or "").strip()
        return (f"{prefix}: {local_time(event['ts'])} · {self._chat_label(event.get('chat_key'))}"
                + (f" · {detail}" if detail else ""))

    def _chat_label(self, chat_key: str | None, title: str | None = None) -> str:
        """Подпись чата: название из конфига, иначе @username, иначе ключ."""
        key = (chat_key or "").strip()
        if not key:
            return title or "—"
        label = self.titles.get(key) or title
        if label:
            return str(label)
        if key.startswith("invite:"):
            return "+" + key.split(":", 1)[1]
        return key if key.startswith(("@", "-")) else f"@{key}"

    def _chat_errors(self, chat_key: str) -> int:
        """Ошибки по конкретному чату за сутки (недоступен, не разрешился, сбой отправки)."""
        rows = self.store.errors_recent(limit=200, since_hours=24)
        return len([row for row in rows if (chat_key or "") in (row.get("text") or "")])

    def _write_stats_file(self) -> str:
        """Собирает stats.txt для /report (тот же текст, что пишет --stats-file)."""
        try:
            report = self.store.stats_report(days=self.stats_days, titles_from_config=self.titles)
            path = Path(self.stats_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(report, encoding="utf-8")
            return str(path)
        except Exception as exc:                            # noqa: BLE001
            self._log_error("report", f"{type(exc).__name__}: {exc}")
            return ""

    @staticmethod
    def _parse_int(value: str, default: int = 5, low: int = 1, high: int = 50) -> int:
        try:
            parsed = int(str(value).strip())
        except (TypeError, ValueError):
            return default
        return max(low, min(high, parsed))

    def _parse_days(self, value: str) -> int:
        return self._parse_int(value, default=self.stats_days or 7, low=1, high=90)

    @staticmethod
    def _parse_target(target: str) -> tuple[int | None, str]:
        """Из ссылки или числа достаёт (msg_id, chat_key)."""
        text = (target or "").strip()
        match = re.search(r"t\.me/(?:c/(\d+)|([A-Za-z0-9_]+))/(\d+)", text)
        if match:
            private, username, msg_id = match.groups()
            return int(msg_id), (username or f"invite:{private}")
        if re.fullmatch(r"-?\d+", text):
            return int(text), ""
        digits = re.findall(r"\d+", text)
        return (int(digits[-1]) if digits else None), ""

    # --- алерты (ТЗ §14.6)

    async def check_alerts(self, moment: datetime | None = None) -> list[str]:
        """Проверяет условия алертов и отправляет их. Возвращает список отправленных текстов."""
        sent: list[str] = []
        for text in self.silent_alerts():
            if await self._send_alert(text, f"silent:{text[:24]}"):
                sent.append(text)
        for text in self.session_alerts():
            if await self._send_alert(text, f"session:{text[:24]}"):
                sent.append(text)
        for text in self.flood_alerts():
            if await self._send_alert(text, f"flood:{text[:24]}"):
                sent.append(text)
        for text in self.limit_alerts():
            if await self._send_alert(text, f"limit:{text[:24]}", once_per_day=True):
                sent.append(text)
        queue_text = self.queue_alert()
        if queue_text and await self._send_alert(queue_text, "queue", once_per_day=True):
            sent.append(queue_text)
        digest_text = self.digest_alert(moment)
        if digest_text and await self._send_alert(digest_text, "digest", once_per_day=True):
            sent.append(digest_text)
        return sent

    async def _send_alert(self, text: str, key: str, once_per_day: bool = False) -> bool:
        """Антиспам алертов: «молчит» — не чаще раза в час, остальные — раз в сутки."""
        state_key = f"alert:{key}"
        last = self.store.bot_state_get(state_key)
        age = minutes_ago(last) if last else None
        window = 24 * 60.0 if once_per_day else 60.0
        if age is not None and age < window:
            return False
        ok, error = await self._send(text)
        if ok:
            self.store.bot_state_set(state_key, utc_now().isoformat(timespec="seconds"))
        elif error:
            self._log_error("bot_api", f"алерт не ушёл: {error}")
        return ok

    def silent_alerts(self) -> list[str]:
        """«Аккаунт молчит»: нет пульса дольше --alert-silent (по умолчанию 30 мин)."""
        if self.alert_silent_minutes <= 0:
            return []
        texts = []
        for state in self.account_states():
            age = state["pulse_age_minutes"]
            if age is None or age <= self.alert_silent_minutes:
                continue
            if state["stopped"].get("ts"):
                continue                      # аккаунт остановлен штатно — это не поломка
            when = local_time(state["pulse"].get("ts"))
            texts.append(f"⚠️ {state['name']} молчит {int(age)} мин (последний пульс {when}). "
                         f"Проверить: /accounts · Подсказка: возможно, сессия занята другим "
                         f"процессом или радар остановлен.")
        return texts

    def session_alerts(self) -> list[str]:
        """«Сессия сломана»: AuthKeyDuplicated / бан / отозванная сессия — однократно."""
        rows = self.store.errors_recent(limit=20, since_hours=24)
        texts = []
        for row in rows:
            kind = str(row.get("kind") or "")
            if not any(fatal in kind or fatal in str(row.get("text") or "")
                       for fatal in SESSION_FATAL_KINDS):
                continue
            who = row.get("account") or "аккаунт"
            texts.append(f"⚠️ {who}: сессия сломана ({kind}, {local_time(row['ts'])}). "
                         f"Нужен повторный вход: monitor.py --login-qr --session {who}_session · "
                         f"Подробности: /errors")
        return texts

    def flood_alerts(self) -> list[str]:
        """«FloodWait»: ограничение Telegram — однократно на событие."""
        rows = self.store.heartbeats_since(hours=24, kind="flood")
        texts = []
        for row in rows[-5:]:
            who = row.get("account") or "радар"
            detail = row.get("detail") or ""
            texts.append(f"⚠️ {who}: FloodWait {local_time(row['ts'])}"
                         + (f" ({detail})" if detail else "")
                         + ". Пауза соблюдается автоматически; если повторяется часто — "
                           "снизь лимит пересылок (/limits).")
        return texts

    def limit_alerts(self) -> list[str]:
        """«Лимит исчерпан»: дневной лимит выбран, но находки продолжают идти."""
        texts = []
        if not self.store.hits_today():
            return texts
        for state in self.account_states():
            limit = state["max_per_day"]
            if limit and state["forwarded_today"] >= limit:
                texts.append(f"⚠️ {state['name']}: дневной лимит пересылок выбран "
                             f"({state['forwarded_today']}/{limit}), находки продолжают приходить — "
                             f"в очереди {state['queue']} (/queue). Добор пойдёт после полуночи.")
        return texts

    def queue_alert(self) -> str:
        """«Очередь растёт»: больше 50 позиций и не уменьшается вторые сутки."""
        total = self.store.deferred_count()
        if total <= 50:
            self.store.bot_state_set("queue_snapshot", f"{total}|{utc_now().date().isoformat()}")
            return ""
        raw = self.store.bot_state_get("queue_snapshot") or ""
        value, _, day = raw.partition("|")
        try:
            previous = int(value)
        except ValueError:
            previous = total
        try:
            age_days = (utc_now().date() - datetime.fromisoformat(day).date()).days if day else 0
        except ValueError:
            age_days = 0
        if age_days < 2 or previous < total:
            return ""
        return (f"⚠️ Очередь пересылок не уменьшается: {total} позиций "
                f"(два дня назад было {previous}). Проверь лимиты /limits и ошибки /errors.")

    def digest_alert(self, moment: datetime | None = None) -> str:
        """Утренний дайджест в 09:00 местного времени (если /digest on)."""
        if not self.digest_on:
            return ""
        now = moment or datetime.now().astimezone()
        if now.hour < 9:
            return ""
        today = now.date().isoformat()
        if self.store.bot_state_get("digest_last_day") == today:
            return ""
        self.store.bot_state_set("digest_last_day", today)
        states = self.account_states()
        per_account = " · ".join(f"{s['name']} {s['forwarded_today']}"
                                 + (f"/{s['max_per_day']}" if s["max_per_day"] else "")
                                 for s in states)
        return (f"🌅 Дайджест за сутки ({today})\n"
                f"Находок сегодня: {self.store.hits_today()} · переслано "
                f"{self.store.forwarded_today()}" + (f" ({per_account})" if per_account else "") + "\n"
                f"В очереди: {self.store.deferred_count()} · ошибок за сутки: "
                f"{self.store.errors_count(since_hours=24)}\n"
                f"Аккаунты: " + ", ".join(f"{s['name']} — {s['status']}" for s in states))

    # --- проверка сессий (только когда радар остановлен, ТЗ §14.3)

    @staticmethod
    def sessions_busy(store, minutes: float = 3.0) -> str | None:
        """Работает ли радар прямо сейчас: имя аккаунта с живым пульсом либо None.

        Живая проверка сессий (get_me) при работающем радаре = второй клиент на тот же
        .session = AuthKeyDuplicatedError. Поэтому сначала смотрим на пульс.
        """
        for name in (store.accounts_seen() or ["main"]):
            age = store.heartbeat_age_minutes("pulse", account=name)
            if age is not None and age <= minutes:
                return name
        return None


# -----------------------------------------------------------------------------
# Отдельный процесс (режим 2, ТЗ §14.4)
# -----------------------------------------------------------------------------

async def panel_only_main(args) -> int:
    """python bot_panel.py [--db hits.sqlite3] — панель без радара и без Telethon."""
    from monitor import HitStore, load_config, titles_by_key

    store = HitStore(args.db)
    accounts: list[AccountView] = []
    titles: dict[str, str] = {}
    chats_by_account: dict[str, int] = {}
    try:
        defaults, sources = load_config(args.config)
        titles = titles_by_key(sources)
        from monitor import resolve_accounts, sources_for_account
        parser_args = type("A", (), {"session": args.session, "account": None,
                                     "forward_to": None, "forward_max_per_day": None,
                                     "forward_mode": None, "forward_fallback": None})()
        configured = resolve_accounts(parser_args, defaults)
        buckets = sources_for_account(sources, configured)
        accounts = [AccountView.from_config(acc, len(buckets.get(acc.name, []))) for acc in configured]
        chats_by_account = {acc.name: len(buckets.get(acc.name, [])) for acc in configured}
    except Exception as exc:                                  # noqa: BLE001
        print(f"[i] панель работает без конфига ({type(exc).__name__}): "
              f"аккаунты возьму из базы", file=sys.stderr)

    heartbeat = args.heartbeat if args.heartbeat is not None else 15.0
    panel = BotPanel(store, accounts=accounts, titles=titles, mode=args.mode,
                     heartbeat_minutes=heartbeat, alert_silent_minutes=args.alert_silent,
                     panel_only=True, stats_file=args.stats_file, stats_days=args.stats_days,
                     db_path=args.db)
    ok, why = panel.ready()
    if not ok:
        print(f"[!] {why}", file=sys.stderr)
        return 1
    print(f"[+] панель запущена отдельно (данные из {args.db}); команды принимает "
          f"chat_id={panel.owner_chat}. Ctrl+C — выход.", file=sys.stderr)
    try:
        await panel.run()
    except KeyboardInterrupt:
        print("\n[!] панель остановлена", file=sys.stderr)
    return 0


def build_panel_parser():
    """CLI отдельной панели (тот же набор, что у --panel-only)."""
    import argparse

    ap = argparse.ArgumentParser(description="Бот-панель радара: статус и статистика через Telegram")
    ap.add_argument("--db", default="hits.sqlite3", help="база радара (читается в режиме WAL)")
    ap.add_argument("--config", default="sources.yaml", help="конфиг источников (для имён чатов)")
    ap.add_argument("--session", default=os.getenv("TG_SESSION", "monitor_session"))
    ap.add_argument("--stats-file", default="stats.txt")
    ap.add_argument("--stats-days", type=int, default=7)
    ap.add_argument("--heartbeat", type=float, default=None, metavar="MIN",
                    help="интервал пульса радара в минутах (по умолчанию 15): "
                         "по нему панель судит, жив ли аккаунт")
    ap.add_argument("--alert-silent", type=float, default=30.0, metavar="MIN",
                    help="через сколько минут без пульса слать алерт «аккаунт молчит»")
    ap.add_argument("--mode", choices=["A", "B"], default="A",
                    help="A — радар-слушатель, B — проходы по расписанию")
    ap.add_argument("--once", action="store_true", help="разовый опрос команд и выход (для проверки)")
    return ap


def main() -> None:
    from core_telegram import load_dotenv

    load_dotenv()
    args = build_panel_parser().parse_args()
    try:
        sys.exit(asyncio.run(panel_only_main(args)))
    except KeyboardInterrupt:
        print("\n[!] панель остановлена", file=sys.stderr)


if __name__ == "__main__":
    main()
