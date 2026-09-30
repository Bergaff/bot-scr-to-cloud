#!/usr/bin/env python3
"""
Пересылка найденных сообщений (не копия!) в чат с ботом или любым получателем.

Как это работает: пересылку выполняет ТВОЙ аккаунт (Telethon), поэтому получателем может быть
бот, канал-архив или второй аккаунт. Твой бот должен быть запущен (ты нажал у него Start) —
тогда он сможет принимать пересланные сообщения.

Особенности:
  * режим forward — честная пересылка (получатель видит, из какого чата пришло);
  * если в чате-источнике отключено сохранение контента (restrict_saving), Telegram запрещает
    пересылку — тогда по настройке fallback отправляется текст со ссылкой на первоисточник
    (fallback=link) либо сообщение пропускается (fallback=skip);
  * лимит отправок в сутки и пауза между отправками, чтобы аккаунт не выглядел спамером;
  * каждая отправка записывается в базу: повторно одно и то же сообщение не уйдёт.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

from core_telegram import call


class Forwarder:
    """Пересылает совпадения получателю (по умолчанию — в чат с ботом)."""

    def __init__(self, client, target: str, store, paced, mode: str = "forward",
                 max_per_day: int = 100, fallback: str = "link", dry_run: bool = False,
                 account: str = ""):
        self.client, self.target_name, self.store = client, target, store
        self.account = account          # чей аккаунт отправляет: у каждого свой дневной лимит
        self.paced, self.mode = paced, mode
        self.max_per_day, self.fallback, self.dry_run = max_per_day, fallback, dry_run
        self.target = None
        self.sent_today = 0
        self.stopped_reason: str | None = None

    # ------------------------------------------------------------------ подготовка

    async def prepare(self) -> bool:
        """Разрешает получателя и считает, сколько уже отправлено сегодня."""
        self.sent_today = self.store.forwarded_today(self.account or None)
        try:
            self.target = await call(lambda: self.client.get_entity(self.target_name),
                                     self.paced, label=f"get_entity({self.target_name})")
        except Exception as exc:  # noqa: BLE001
            print(f"[!] Не удалось найти получателя {self.target_name}: {type(exc).__name__} {exc}",
                  file=sys.stderr)
            print("    Для бота: открой с этого же аккаунта чат с ботом и нажми Start, "
                  "затем проверь username.", file=sys.stderr)
            return False

        title = getattr(self.target, "title", None) or getattr(self.target, "username", self.target_name)
        already = f", сегодня уже отправлено {self.sent_today}" if self.sent_today else ""
        print(f"[i] пересылка включена: {self.target_name} («{title}»), режим {self.mode}, "
              f"лимит {self.max_per_day or '∞'}/сутки{already}", file=sys.stderr)
        if self.mode == "forward":
            print("[i] напоминание: режим forward в чатах с защитой от сохранения невозможно — "
                  "такие сообщения уйдут текстом со ссылкой (fallback=" + self.fallback + ")", file=sys.stderr)
        return True

    # ------------------------------------------------------------------ отправка

    def _cap_reached(self) -> bool:
        return bool(self.max_per_day) and self.sent_today >= self.max_per_day

    async def send(self, message, hit: dict, mode_override: str | None = None) -> str:
        """Отправляет одно совпадение. Возвращает статус:
        forwarded | copied | skipped | limit | duplicate | failed."""
        key, msg_id = hit["chat_key"], hit["msg_id"]
        if self.store.was_forwarded(key, msg_id):
            return "duplicate"
        if self._cap_reached():
            if self.stopped_reason is None:
                self.stopped_reason = "daily_limit"
                print(f"[!] Дневной лимит пересылок ({self.max_per_day}) исчерпан. "
                      "Находки не теряются: они встают в очередь и уйдут в следующий прогон "
                      "(добор идёт первым делом, как только лимит обнулится в местную полночь).",
                      file=sys.stderr)
            self.store.queue_forward(key, msg_id, account=self.account or None)
            self.store.bump_stats(key, account=self.account or None, deferred=1)
            return "limit"
        if self.dry_run:
            self.store.mark_forwarded(key, msg_id, ok=True, mode="dry-run", error="",
                                      account=self.account or None)
            return "forwarded"

        from telethon.errors import ChatForwardsRestrictedError, FloodWaitError, RPCError

        try:
            if self.mode == "forward":
                await self.paced.wait()
                await self.client.forward_messages(self.target, messages=[message])
                status, error = "forwarded", ""
            else:
                await self.paced.wait()
                await self.client.send_message(self.target, self._text_with_link(hit))
                status, error = "copied", ""
        except ChatForwardsRestrictedError:
            # в чате отключено сохранение контента: пересылка запрещена
            if self.fallback == "link":
                await self.paced.wait()
                await self.client.send_message(self.target, self._text_with_link(hit, note=True))
                status, error = "copied", "forwards_restricted"
            else:
                status, error = "skipped", "forwards_restricted"
        except FloodWaitError as exc:
            wait = exc.seconds * 1.2 + 5
            print(f"[flood] пересылка: ждём {wait:.0f} с", file=sys.stderr)
            # панель увидит ограничение: пишем сами (знаем аккаунт и чат), без общего хука —
            # иначе одно событие попало бы в базу дважды
            until = (datetime.now(timezone.utc) + timedelta(seconds=wait)).astimezone().strftime("%H:%M")
            self.store.log_heartbeat("flood", account=self.account or None, chat_key=key,
                                     detail=f"пересылка: до {until}")
            self.store.log_error("FloodWaitError",
                                 f"пересылка: Telegram просит {exc.seconds} с, ждём до {until}",
                                 account=self.account or None)
            import asyncio
            await asyncio.sleep(wait)
            try:
                await self.client.forward_messages(self.target, messages=[message])
                status, error = "forwarded", "after_flood"
            except RPCError as retry_exc:
                status, error = "failed", f"{type(retry_exc).__name__}: {retry_exc}"
        except RPCError as exc:
            status, error = "failed", f"{type(exc).__name__}: {exc}"
            if "PEER_FLOOD" in str(exc).upper():
                self.stopped_reason = "peer_flood"
                print("[!] PEER_FLOOD — Telegram ограничил отправку сообщений с этого аккаунта. "
                      "Пересылку останавливаю; подожди 24–48 ч и снизь лимиты.", file=sys.stderr)

        # mode_override="test" — проверочная отправка: не съедает дневной лимит,
        # но сообщение помечается пересланным, чтобы боту не ушёл дубликат.
        if status in ("forwarded", "copied") and mode_override != "test":
            self.sent_today += 1
        record_mode = mode_override or (status if status != "copied" else f"copy:{self.mode}")
        self.store.mark_forwarded(key, msg_id, ok=status in ("forwarded", "copied"),
                                  mode=record_mode, error=error, account=self.account or None)
        return status

    # ------------------------------------------------------------------ добор очереди

    async def flush_deferred(self, fetch) -> dict:
        """Отправляет то, что не влезло в лимит в прошлые сутки.

        fetch(chat_key, msg_id) — как достать исходное сообщение (даёт Monitor).
        Сообщения, которые уже недоступны (чат удалён/вышел), снимаются с очереди навсегда.
        Порядок: сначала самое старое. Свободен весь дневной лимит, но он общий с новыми находками.
        """
        result = {"sent": 0, "failed": 0, "gone": 0, "leftover": 0}
        queue = self.store.deferred_queue(self.account or None)
        if not queue:
            return result
        free = "∞" if not self.max_per_day else max(0, self.max_per_day - self.sent_today)
        print(f"[i] добор из очереди: ждёт {len(queue)}, свободно отправок сегодня: {free}", file=sys.stderr)
        if self.dry_run:
            result["leftover"] = len(queue)
            print("[i] добор: dry-run — ничего не отправляю, очередь не трогаю", file=sys.stderr)
            return result
        for chat_key, msg_id in queue:
            if self._cap_reached():
                result["leftover"] += 1
                continue
            hit = self.store.get_hit(chat_key, msg_id)
            if hit is None:
                self.store.mark_forwarded(chat_key, msg_id, ok=False, mode="dead", error="hit_missing",
                                          account=self.account or None)
                result["gone"] += 1
                continue
            message = await fetch(chat_key, msg_id)
            if message is None:
                self.store.mark_forwarded(chat_key, msg_id, ok=False, mode="dead", error="message_gone",
                                          account=self.account or None)
                result["gone"] += 1
                continue
            status = await self.send(message, hit)
            if status in ("forwarded", "copied"):
                result["sent"] += 1
                self.store.bump_stats(chat_key, account=self.account or None, forwarded=1)
            elif status == "limit":
                result["leftover"] += 1
            elif status == "duplicate":
                pass
            else:
                result["failed"] += 1
        print(f"[i] добор: отправлено {result['sent']}, не нашлось {result['gone']}, "
              f"ошибок {result['failed']}, осталось в очереди ещё {result['leftover']}", file=sys.stderr)
        return result

    @staticmethod
    def _text_with_link(hit: dict, note: bool = False) -> str:
        head = "🔁 Пересылка запрещена в источнике — текст со ссылкой:" if note else "📨 Найдено:"
        text = (hit.get("text") or "").strip()
        parts = [head, "", text, "", hit.get("link", ""), f"({hit.get('chat_title') or hit.get('chat_key')})"]
        return "\n".join(part for part in parts if part is not None)[:4000]
