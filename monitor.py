#!/usr/bin/env python3
"""
Мониторинг каналов и групп: ищет сообщения про посылки/передачи/попутчиков,
пишет в SQLite, отдаёт ссылку на сообщение и уведомляет.

Режимы:
  python3 monitor.py                      # живой мониторинг (все источники из конфига)
  python3 monitor.py --once               # разовый проход (catch-up) и выход — удобно для cron
  python3 monitor.py --catchup 200        # при старте прочитать последние 200 сообщений в каждом чате
  python3 monitor.py --export hits.csv    # выгрузить найденное из БД в CSV
  python3 monitor.py --notify both        # console + файл;  --notify bot — сообщением от бота

Уведомление через бота: TG_BOT_TOKEN (создать у @BotFather) + TG_NOTIFY_CHAT (тебе в ЛС от бота:
узнать свой id можно у @userinfobot).

Читающих запросов мало (сообщения приходят пушем), поэтому риск для аккаунта низкий:
catch-up ограничен --catchup, паузы между вызовами --delay, FloodWait соблюдается.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import random
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core_telegram import (BOT_TOKEN_HINT, FloodWaitTooLong, Paced, add_flood_hook,
                           build_notifier, call,
                           collect_topics, display_name, format_hit, hidden_author_reason,
                           invite_hash, load_dotenv, make_client, message_link, peer_id,
                           resolve_targets, topic_of, topic_title_of)
from forwarder import FETCH_FAILED
from matcher import analyze, direction_allowed

_MISSING = object()          # «в кэше пачки ничего нет» (None в кэше значит «сообщение удалено»)

HEADERS_TXT = {
    "parcel": "ПОСЫЛКА/ПЕРЕДАЧА", "ride": "ПОПУТЧИК/ПАССАЖИР",
    "mixed": "ПОСЫЛКИ+ПОПУТЧИКИ", "any": "СИГНАЛ",
}
INTENTS_TXT = {"offer": "предлагаю", "request": "ищу", "any": "объявление"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS seen (
    chat_key TEXT NOT NULL,
    msg_id   INTEGER NOT NULL,
    PRIMARY KEY (chat_key, msg_id)
);
CREATE TABLE IF NOT EXISTS hits (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_key   TEXT, chat_title TEXT, chat_id TEXT, username TEXT,
    msg_id     INTEGER, date TEXT, sender_id TEXT, sender_name TEXT,
    text       TEXT, score INTEGER, category TEXT, intent TEXT,
    direction  TEXT, countries TEXT, hits TEXT, link TEXT,
    found_at   TEXT, notified INTEGER DEFAULT 0,
    topic_id   INTEGER, topic_name TEXT, account TEXT
);
CREATE INDEX IF NOT EXISTS idx_hits_chat ON hits(chat_key, msg_id);
CREATE TABLE IF NOT EXISTS text_seen (
    chat_key   TEXT NOT NULL,
    text_hash  TEXT NOT NULL,
    first_seen TEXT NOT NULL,
    PRIMARY KEY (chat_key, text_hash)
);
CREATE TABLE IF NOT EXISTS text_seen_global (
    text_hash  TEXT PRIMARY KEY,
    first_seen TEXT NOT NULL,
    chat_key   TEXT
);
CREATE TABLE IF NOT EXISTS forwarded (
    chat_key TEXT NOT NULL,
    msg_id   INTEGER NOT NULL,
    ok       INTEGER DEFAULT 0,
    mode     TEXT,
    error    TEXT,
    at       TEXT,
    account  TEXT,
    PRIMARY KEY (chat_key, msg_id)
);
CREATE TABLE IF NOT EXISTS stats (
    day         TEXT NOT NULL,
    chat_key    TEXT NOT NULL,
    scanned     INTEGER DEFAULT 0,
    matched     INTEGER DEFAULT 0,
    saved       INTEGER DEFAULT 0,
    forwarded   INTEGER DEFAULT 0,
    forward_skipped INTEGER DEFAULT 0,
    forward_failed  INTEGER DEFAULT 0,
    filtered    INTEGER DEFAULT 0,
    too_old     INTEGER DEFAULT 0,
    duplicates  INTEGER DEFAULT 0,
    text_duplicates INTEGER DEFAULT 0,
    cross_chat  INTEGER DEFAULT 0,
    deferred    INTEGER DEFAULT 0,
    hidden      INTEGER DEFAULT 0,
    filtered_category  INTEGER DEFAULT 0,   -- из «filtered»: не прошло --category
    filtered_intent    INTEGER DEFAULT 0,   --               не прошло --only-intent
    filtered_direction INTEGER DEFAULT 0,   --               не прошло --only-direction
    account     TEXT,
    PRIMARY KEY (day, chat_key)
);
-- «пульс» и события: по ним бот-панель считает, жив ли аккаунт (ТЗ §7.2)
CREATE TABLE IF NOT EXISTS heartbeats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,                        -- UTC ISO
    account TEXT,                            -- main | second | '' (не привязано)
    kind TEXT NOT NULL,                      -- start | pulse | event | forward | error | flood | stop | day
    chat_key TEXT,
    detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_heartbeats_ts ON heartbeats(ts);
-- журнал ошибок для /errors и алертов панели
CREATE TABLE IF NOT EXISTS errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL, account TEXT, kind TEXT, text TEXT
);
CREATE INDEX IF NOT EXISTS idx_errors_ts ON errors(ts);
-- срезы ресурсов: сколько радар жрёт (вердикт «A или B», ТЗ §15)
CREATE TABLE IF NOT EXISTS metrics (
    ts TEXT PRIMARY KEY,
    rss_mb REAL, cpu_percent REAL, uptime_s REAL,
    msgs_total INTEGER, msgs_last_hour INTEGER, api_calls INTEGER,
    db_mb REAL, hits_total INTEGER, forwarded_today INTEGER,
    accounts INTEGER, mode TEXT              -- mode: A (listener) | B (scheduled)
);
-- состояние бот-панели (offset getUpdates, антиспам алертов, /digest)
CREATE TABLE IF NOT EXISTS bot_state (
    key TEXT PRIMARY KEY, value TEXT
);
"""


# ------------------------------------------------------------------ хранилище

class HitStore:
    """SQLite: дедупликация по (чат, id сообщения) + склад совпадений."""

    def __init__(self, path: str = "hits.sqlite3"):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        # WAL + busy_timeout: базу читают одновременно радар и бот-панель (возможно, второй
        # процесс --panel-only). Без этого панель ловила бы «database is locked» (ТЗ §7.2).
        try:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA busy_timeout=5000")
        except sqlite3.Error:                # например :memory: — WAL недоступен, работаем дальше
            pass
        self.conn.executescript(SCHEMA)
        self._migrate()
        self.conn.commit()

    def _migrate(self) -> None:
        """Догоняет схему в уже существующих базах (старые файлы hits.sqlite3)."""
        columns = {row[1] for row in self.conn.execute("PRAGMA table_info(stats)")}
        if "deferred" not in columns:
            self.conn.execute("ALTER TABLE stats ADD COLUMN deferred INTEGER DEFAULT 0")
        if "hidden" not in columns:
            self.conn.execute("ALTER TABLE stats ADD COLUMN hidden INTEGER DEFAULT 0")
        for name in ("filtered_category", "filtered_intent", "filtered_direction"):
            if name not in columns:
                self.conn.execute(f"ALTER TABLE stats ADD COLUMN {name} INTEGER DEFAULT 0")
        fwd_columns = {row[1] for row in self.conn.execute("PRAGMA table_info(forwarded)")}
        if "account" not in fwd_columns:
            # старая история относится к первому аккаунту: назови его main, чтобы лимит
            # «сегодня уже отправлено» и статистика продолжились, а не считались с нуля
            self.conn.execute("ALTER TABLE forwarded ADD COLUMN account TEXT")
            self.conn.execute("UPDATE forwarded SET account='main' WHERE account IS NULL")
        if "account" not in columns:
            self.conn.execute("ALTER TABLE stats ADD COLUMN account TEXT")
            self.conn.execute("UPDATE stats SET account='main' WHERE account IS NULL")
        hit_columns = {row[1] for row in self.conn.execute("PRAGMA table_info(hits)")}
        if "account" not in hit_columns:
            self.conn.execute("ALTER TABLE hits ADD COLUMN account TEXT")
        if "topic_id" not in hit_columns:
            self.conn.execute("ALTER TABLE hits ADD COLUMN topic_id INTEGER")
        if "topic_name" not in hit_columns:
            self.conn.execute("ALTER TABLE hits ADD COLUMN topic_name TEXT")

    def is_seen(self, chat_key: str, msg_id: int) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM seen WHERE chat_key=? AND msg_id=?", (chat_key, msg_id)
        ).fetchone()
        return row is not None

    def mark_seen(self, chat_key: str, msg_id: int) -> None:
        self.conn.execute("INSERT OR IGNORE INTO seen VALUES (?, ?)", (chat_key, msg_id))

    def text_is_duplicate(self, chat_key: str, text: str, window_hours: float,
                          scope: str = "global") -> tuple[bool, str | None]:
        """Один и тот же текст в пределах окна. Возвращает (дубль?, чат-первоисточник).

        scope='global' (по умолчанию) — ловит перепосты одного объявления по разным чатам
        (одни и те же водители пишут в 3-4 чата сразу).
        scope='chat' — сравнивает только внутри одного чата."""
        if window_hours <= 0:
            return False, None
        import hashlib
        digest = hashlib.sha1(re.sub(r"\s+", " ", text.strip().lower()).encode()).hexdigest()
        now = datetime.now(timezone.utc)
        stamp = now.isoformat(timespec="seconds")

        if scope == "global":
            row = self.conn.execute(
                "SELECT first_seen, chat_key FROM text_seen_global WHERE text_hash=?", (digest,)
            ).fetchone()
            if row:
                try:
                    first = datetime.fromisoformat(row[0])
                except ValueError:
                    first = now
                if (now - first).total_seconds() < window_hours * 3600:
                    return True, row[1]
                self.conn.execute("UPDATE text_seen_global SET first_seen=?, chat_key=? WHERE text_hash=?",
                                  (stamp, chat_key, digest))
            else:
                self.conn.execute("INSERT INTO text_seen_global VALUES (?, ?, ?)", (digest, stamp, chat_key))
            self.conn.commit()
            return False, None

        row = self.conn.execute(
            "SELECT first_seen FROM text_seen WHERE chat_key=? AND text_hash=?", (chat_key, digest)
        ).fetchone()
        if row:
            try:
                first = datetime.fromisoformat(row[0])
            except ValueError:
                first = now
            if (now - first).total_seconds() < window_hours * 3600:
                return True, chat_key
            self.conn.execute("UPDATE text_seen SET first_seen=? WHERE chat_key=? AND text_hash=?",
                              (stamp, chat_key, digest))
        else:
            self.conn.execute("INSERT INTO text_seen VALUES (?, ?, ?)", (chat_key, digest, stamp))
        self.conn.commit()
        return False, None

    def save_hit(self, hit: dict) -> bool:
        """True — если такого совпадения ещё не было."""
        row = self.conn.execute(
            "SELECT 1 FROM hits WHERE chat_key=? AND msg_id=?", (hit["chat_key"], hit["msg_id"])
        ).fetchone()
        if row:
            return False
        self.conn.execute(
            """INSERT INTO hits (chat_key, chat_title, chat_id, username, msg_id, date, sender_id,
               sender_name, text, score, category, intent, direction, countries, hits, link, found_at,
               topic_id, topic_name, account)
               VALUES (:chat_key,:chat_title,:chat_id,:username,:msg_id,:date,:sender_id,
               :sender_name,:text,:score,:category,:intent,:direction,:countries,:hits,:link,:found_at,
               :topic_id,:topic_name,:account)""",
             {**hit, "countries": ",".join(hit.get("countries") or []), "hits": ",".join(hit.get("hits") or []),
              "topic_id": hit.get("topic_id"), "topic_name": hit.get("topic_name") or "",
              "account": hit.get("account") or ""},
        )
        # событие для панели: «последний раз ловил в 21:12 (@чат)» (ТЗ §7.3)
        self.conn.execute(
            "INSERT INTO heartbeats (ts, account, kind, chat_key, detail) VALUES (?, ?, 'event', ?, ?)",
            (self._now_utc(), hit.get("account") or "", hit["chat_key"],
             (hit.get("direction") or "")[:80]),
        )
        self.conn.commit()
        return True

    def mark_notified(self, chat_key: str, msg_id: int) -> None:
        self.conn.execute(
            "UPDATE hits SET notified=1 WHERE chat_key=? AND msg_id=?", (chat_key, msg_id)
        )
        self.conn.commit()

    # ---------------- пересылки

    def was_forwarded(self, chat_key: str, msg_id: int) -> bool:
        """Отправлено ли уже. Отложенные по дневному лимиту (mode=queued) — НЕ отправленные:
        они лежат в очереди и уйдут в следующий прогон."""
        row = self.conn.execute(
            "SELECT COALESCE(mode, '') FROM forwarded WHERE chat_key=? AND msg_id=?", (chat_key, msg_id)
        ).fetchone()
        return row is not None and row[0] != "queued"

    # ---------------- очередь отложенных пересылок (дневной лимит)

    def queue_forward(self, chat_key: str, msg_id: int, error: str = "daily_limit",
                      account: str | None = None) -> None:
        """Кладёт находку в очередь: лимит на сегодня исчерпан, уйдёт в следующий прогон.

        account — чей аккаунт может её отправить (он участник чата), иначе любой из конфига.
        """
        self.mark_forwarded(chat_key, msg_id, ok=False, mode="queued", error=error, account=account)

    def deferred_queue(self, account: str | None = None) -> list[tuple[str, int]]:
        """Что ждёт отправки (по порядку появления). account — только для этого аккаунта."""
        query = ("SELECT chat_key, msg_id FROM forwarded WHERE mode='queued'"
                 + (" AND account=?" if account else "") + " ORDER BY at, chat_key, msg_id")
        rows = self.conn.execute(query, (account,) if account else ()).fetchall()
        return [(row[0], int(row[1])) for row in rows]

    def deferred_count(self, account: str | None = None) -> int:
        query = "SELECT COUNT(*) FROM forwarded WHERE mode='queued'" + (" AND account=?" if account else "")
        row = self.conn.execute(query, (account,) if account else ()).fetchone()
        return int(row[0] if row else 0)

    def get_hit(self, chat_key: str, msg_id: int) -> dict | None:
        """Достаёт сохранённое совпадение (для добора из очереди)."""
        cursor = self.conn.execute("SELECT * FROM hits WHERE chat_key=? AND msg_id=?", (chat_key, msg_id))
        row = cursor.fetchone()
        if row is None:
            return None
        return dict(zip([c[0] for c in cursor.description], row))

    @staticmethod
    def day_start_utc() -> str:
        """Начало текущих суток по МЕСТНОМУ времени, переведённое в UTC (счётчик лимита)."""
        local = datetime.now().astimezone().replace(hour=0, minute=0, second=0, microsecond=0)
        return local.astimezone(timezone.utc).isoformat(timespec="seconds")

    def mark_forwarded(self, chat_key: str, msg_id: int, ok: bool, mode: str = "", error: str = "",
                       account: str | None = None) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO forwarded (chat_key, msg_id, ok, mode, error, at, account) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (chat_key, msg_id, 1 if ok else 0, mode, error,
             datetime.now(timezone.utc).isoformat(timespec="seconds"), account),
        )
        if ok and mode not in ("test", "queued", "dry-run"):
            # пересылка состоялась — панель покажет «отправлял в 21:12» (ТЗ §7.3)
            self.conn.execute(
                "INSERT INTO heartbeats (ts, account, kind, chat_key, detail) "
                "VALUES (?, ?, 'forward', ?, ?)",
                (self._now_utc(), account or "", chat_key, f"mode={mode}"[:80]),
            )
        elif not ok and mode.startswith("failed"):
            self.conn.execute(
                "INSERT INTO errors (ts, account, kind, text) VALUES (?, ?, 'forward_failed', ?)",
                (self._now_utc(), account or "", (error or mode)[:500]),
            )
        self.conn.commit()

    # ---------------- окна лимита: «пробой» дневного лимита и авто-окна
    # Дневной лимит считается с начала суток. «Окно» — это сутки, внутри которых лимит
    # можно обнулить ещё раз: вручную (/boost в боте) или автоматически (forward.reset_hours).
    # Время последнего обнуления лежит в bot_state; отправки считаются с него, а не с полуночи.

    @staticmethod
    def _reset_key(account: str | None) -> str:
        return f"limit:reset:{account or '-'}"

    def limit_reset_at(self, account: str | None = None) -> str:
        """Когда лимит обнуляли последний раз (ISO UTC) за текущие сутки; пусто — не обнуляли."""
        value = self.bot_state_get(self._reset_key(account)) or ""
        return value if value >= self.day_start_utc() else ""

    def limit_window_start(self, account: str | None = None) -> str:
        """С какого момента считаются отправки для лимита: полночь или последнее обнуление."""
        return max(self.day_start_utc(), self.limit_reset_at(account))

    def forwarded_window(self, account: str | None = None) -> int:
        """Сколько отправлено в ТЕКУЩЕМ окне лимита (после последнего обнуления)."""
        since = self.day_start_utc()
        reset = self.limit_reset_at(account)       # строго «после» обнуления: отправки той же секунды — старые
        query = ("SELECT COUNT(*) FROM forwarded WHERE ok=1 AND COALESCE(mode,'') != 'test' AND at >= ? AND at > ?"
                 + (" AND account=?" if account else ""))
        params = (since, reset, account) if account else (since, reset)
        row = self.conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def reset_limit(self, account: str | None, auto: bool = False) -> int:
        """Обнулить лимит аккаунта: следующие отправки снова считаются с нуля. Возвращает число обнулений за сутки."""
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.bot_state_set(self._reset_key(account), now)
        counter_key = f"limit:resets:{account or '-'}"
        day = datetime.now().astimezone().strftime("%Y-%m-%d")
        raw = self.bot_state_get(counter_key) or ""
        done = int(raw.split(":")[1]) if raw.startswith(day + ":") else 0
        self.bot_state_set(counter_key, f"{day}:{done + 1}:{'auto' if auto else 'manual'}")
        return done + 1

    def limit_resets_today(self, account: str | None = None) -> int:
        raw = self.bot_state_get(f"limit:resets:{account or '-'}") or ""
        day = datetime.now().astimezone().strftime("%Y-%m-%d")
        return int(raw.split(":")[1]) if raw.startswith(day + ":") else 0

    def expire_queue(self, hours: float, account: str | None = None) -> int:
        """Убрать из очереди позиции, которые висят дольше hours часов (устарели). 0/None — не трогать.

        Позиция не теряется молча: mode='expired', ok=0 — видно в статистике, а повторно в очередь
        это сообщение не попадёт (запись о нём уже есть)."""
        if not hours or hours <= 0:
            return 0
        border = (datetime.now(timezone.utc) - timedelta(hours=float(hours))).isoformat(timespec="seconds")
        query = ("UPDATE forwarded SET mode='expired', error='queue_ttl' "
                 "WHERE mode='queued' AND at < ?" + (" AND account=?" if account else ""))
        cursor = self.conn.execute(query, (border, account) if account else (border,))
        self.conn.commit()
        count = int(cursor.rowcount or 0)
        if count:
            total = int(self.bot_state_get("queue:expired_total") or 0) + count
            self.bot_state_set("queue:expired_total", str(total))
            self.bot_state_set("queue:last_expired", json.dumps(
                {"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"), "count": count,
                 "hours": float(hours)}))
        return count

    def forwarded_today(self, account: str | None = None) -> int:
        """Сколько отправлено сегодня. account — считать только по этому аккаунту (у каждого свой лимит)."""
        since = self.day_start_utc()
        query = ("SELECT COUNT(*) FROM forwarded WHERE ok=1 AND COALESCE(mode,'') != 'test' AND at >= ?"
                 + (" AND account=?" if account else ""))
        row = self.conn.execute(query, (since, account) if account else (since,)).fetchone()
        return int(row[0] if row else 0)

    # ---------------- статистика по источникам

    def bump_stats(self, chat_key: str, day: str | None = None, account: str | None = None,
                   **counters: int) -> None:
        """Накапливает счётчики за день по конкретному чату (можно звать сколько угодно раз)."""
        if not counters:
            return
        day = day or datetime.now().astimezone().strftime("%Y-%m-%d")
        columns = list(counters)
        placeholders = ", ".join("?" for _ in range(len(columns) + 2))
        updates = ", ".join(f"{col} = {col} + excluded.{col}" for col in columns)
        self.conn.execute(
            f"INSERT INTO stats (day, chat_key, account, {', '.join(columns)}) "
            f"VALUES ({', '.join(['?', '?', '?'] + ['?'] * len(columns))}) "
            f"ON CONFLICT(day, chat_key) DO UPDATE SET {updates}, account=COALESCE(excluded.account, account)",
            [day, chat_key, account, *[int(counters[col]) for col in columns]],
        )
        self.conn.commit()

    @staticmethod
    def _chat_ref(chat_key: str) -> str:
        """Идентификатор источника: @username для публичных, +invite для приватных по ссылке."""
        if not chat_key:
            return "?"
        if chat_key.startswith("invite:"):
            return "+" + chat_key.split(":", 1)[1]
        return chat_key if chat_key.startswith(("@", "-")) else f"@{chat_key}"

    @staticmethod
    def _fmt_day(day: str) -> str:
        """2026-09-28 -> 28.09.2026 (как в остальных отчётах: день.месяц.год)."""
        try:
            return datetime.strptime(day, "%Y-%m-%d").strftime("%d.%m.%Y")
        except (TypeError, ValueError):
            return str(day)

    def forward_fate(self) -> dict[str, int]:
        """Что стало с каждой найденной записью: переслана, ждёт, недоступна, ошибка...

        Считается по таблицам hits и forwarded (а не по счётчикам stats), поэтому сходится
        с действительностью и для старых баз: каждая находка попадает ровно в одну строку.
        """
        fate = {"total": 0, "forwarded": 0, "copied_restricted": 0, "copied": 0, "trial": 0,
                "queued": 0, "expired": 0, "dead_gone": 0, "dead_missing": 0, "skipped": 0, "failed": 0,
                "other": 0, "no_record": 0}
        rows = self.conn.execute(
            """SELECT f.ok, f.mode, f.error, COUNT(*)
               FROM hits h LEFT JOIN forwarded f ON f.chat_key = h.chat_key AND f.msg_id = h.msg_id
               GROUP BY f.ok, f.mode, f.error"""
        ).fetchall()
        for ok, mode, error, count in rows:
            count = int(count or 0)
            mode, error = mode or "", error or ""
            fate["total"] += count
            if ok is None:
                bucket = "no_record"
            elif ok == 1 and mode in ("dry-run", "test"):
                bucket = "trial"
            elif ok == 1 and mode.startswith("copy"):
                bucket = "copied_restricted" if error == "forwards_restricted" else "copied"
            elif ok == 1:
                bucket = "forwarded"
            elif mode == "queued":
                bucket = "queued"
            elif mode == "expired":
                bucket = "expired"
            elif mode == "dead":
                bucket = "dead_missing" if error == "hit_missing" else "dead_gone"
            elif mode == "skipped":
                bucket = "skipped"
            elif mode.startswith("failed"):
                bucket = "failed"
            else:
                bucket = "other"
            fate[bucket] += count
        return fate

    def stats_report(self, days: int = 7, titles_from_config: dict | None = None) -> str:
        """Текстовый отчёт: откуда сколько сообщений идёт, по дням и по чатам.

        days — МАКСИМАЛЬНОЕ число дней в разделе «по дням»: если статистика ведётся меньше,
        показывается сколько есть (в заголовке написано, с какого числа данные).
        titles_from_config — chat_key -> название из sources.yaml: подставляет имена чатам,
        по которым ещё не было находок (иначе такие строки были бы без названия).
        """
        titles = {row[0]: (row[1] or row[0]) for row in self.conn.execute(
            "SELECT chat_key, MAX(chat_title) FROM hits GROUP BY chat_key").fetchall()}
        for key, title in (titles_from_config or {}).items():
            if title and not titles.get(key):
                titles[key] = title
        first_day, last_day, days_total = self.conn.execute(
            "SELECT MIN(day), MAX(day), COUNT(DISTINCT day) FROM stats").fetchone()
        if first_day:
            period = f"с {self._fmt_day(first_day)}, {days_total} дн."
            span = (f"Статистика ведётся {days_total} дн.: {self._fmt_day(first_day)} — "
                    f"{self._fmt_day(last_day)}. «За всё время» ниже = за этот период.")
        else:
            period, span = "данных ещё нет", "Статистика ещё не накоплена: радар не делал ни одного прохода."
        lines = [
            "Статистика радара: откуда и сколько сообщений",
            f"Сформирована: {datetime.now().astimezone().strftime('%d.%m.%Y %H:%M')} (местное время)",
            span,
            "=" * 78,
        ]

        totals = self.conn.execute(
            """SELECT chat_key, SUM(scanned), SUM(matched), SUM(text_duplicates), SUM(forwarded),
                      SUM(forward_failed), SUM(filtered), SUM(too_old), SUM(deferred), SUM(hidden),
                      SUM(filtered_category), SUM(filtered_intent), SUM(filtered_direction),
                      SUM(cross_chat), SUM(forward_skipped)
               FROM stats GROUP BY chat_key ORDER BY SUM(scanned) DESC"""
        ).fetchall()
        lines += ["", f"ИТОГО ПО ИСТОЧНИКАМ ({period})", "-" * 78,
                  f"{'источник':20} {'название':31} {'аккаунт':8} {'прочит':>7} {'найдено':>8} "
                  f"{'дубли':>6} {'переслано':>10} {'фильтр':>7} {'старше':>7}"]
        accounts_by_chat = {row[0]: (row[1] or "") for row in self.conn.execute(
            "SELECT chat_key, MAX(account) FROM stats GROUP BY chat_key").fetchall()}
        for (chat, scanned, matched, dupes, forwarded, _failed, filtered, too_old,
             _deferred, _hidden, *_rest) in totals:
            label = (titles.get(chat) or "")[:31]
            who = (accounts_by_chat.get(chat) or "")[:8]
            lines.append(f"{self._chat_ref(chat):20} {label:31} {who:8} {scanned or 0:>7} "
                         f"{matched or 0:>8} {dupes or 0:>6} {forwarded or 0:>10} "
                         f"{filtered or 0:>7} {too_old or 0:>7}")
        hidden_total = sum(row[9] or 0 for row in totals)
        deferred_total = sum(row[8] or 0 for row in totals)
        failed_total = sum(row[5] or 0 for row in totals)
        cross_total = sum(row[13] or 0 for row in totals)
        dupes_total = sum(row[3] or 0 for row in totals)
        if dupes_total:
            lines.append(f"дубли: {dupes_total} (из них тот же текст уже пришёл из другого чата: {cross_total})")
        lines.append(f"отложено на добор (следующий прогон): {deferred_total}, "
                     f"сейчас в очереди: {self.deferred_count()}  <- упёрлись в дневной лимит")
        lines.append(f"ошибок отправки: {failed_total}")
        if hidden_total:
            lines.append(f"пропущено из-за скрытых авторов («hidden by user»): {hidden_total}")

        # какой именно фильтр режет: по первому сработавшему (категория -> намерение -> направление)
        filtered_rows = [row for row in totals if (row[6] or 0) > 0]
        if filtered_rows:
            lines += ["", "ЧТО РЕЖЕТ ФИЛЬТР (считается первый сработавший)", "-" * 78,
                      f"{'источник':20} {'название':31} {'отсеяно':>8} {'категория':>10} "
                      f"{'намерение':>10} {'направление':>12} {'без разбивки':>13}"]
            sums = [0, 0, 0, 0, 0]
            for row in filtered_rows:
                chat, total = row[0], row[6] or 0
                cat, intent, direction = row[10] or 0, row[11] or 0, row[12] or 0
                rest = max(0, total - cat - intent - direction)
                for i, value in enumerate((total, cat, intent, direction, rest)):
                    sums[i] += value
                lines.append(f"{self._chat_ref(chat):20} {(titles.get(chat) or '')[:31]:31} {total:>8} "
                             f"{cat:>10} {intent:>10} {direction:>12} {rest:>13}")
            lines.append(f"{'всего':52} {sums[0]:>8} {sums[1]:>10} {sums[2]:>10} {sums[3]:>12} {sums[4]:>13}")
            if sums[4]:
                lines.append("«без разбивки» — отсеяно до того, как радар стал записывать причину.")

        # куда делись найденные: каждая находка попадает ровно в одну строку, итог сходится
        fate = self.forward_fate()
        if fate["total"] and (fate["total"] != fate["no_record"]):
            lines += ["", "ЧТО СТАЛО С НАЙДЕННЫМИ (по базе на сейчас)", "-" * 78,
                      f"{'найдено и сохранено':62} {fate['total']:>6}",
                      f"{'  переслано как есть':62} {fate['forwarded']:>6}",
                      f"{'  отправлено текстом со ссылкой (в чате запрещена пересылка)':62} "
                      f"{fate['copied_restricted']:>6}"]
            optional = [
                ("copied", "  отправлено копией текста (mode: copy)"),
                ("trial", "  пробные отправки (dry-run / test)"),
                ("queued", "  ждёт в очереди (упёрлись в дневной лимит)"),
                ("expired", "  убрано из очереди: висело дольше суток, устарело"),
                ("dead_gone", "  не нашлось в чате при доборе — сообщение удалено"),
                ("dead_missing", "  не нашлось в базе при доборе"),
                ("skipped", "  пропущено: пересылка запрещена в чате (fallback: skip)"),
                ("failed", "  ошибка отправки"),
                ("other", "  прочее"),
                ("no_record", "  без записи о пересылке (пересылка не запускалась)"),
            ]
            for key, label in optional:
                if fate[key]:
                    lines.append(f"{label:62} {fate[key]:>6}")
            accounted = sum(fate[k] for k in fate if k != "total")
            if accounted != fate["total"]:       # не должно случаться: значит, в базе рассинхрон
                lines.append(f"{'  !! не сходится':62} {fate['total'] - accounted:>6}")

        # сводка по аккаунтам: видно, кто сколько прочитал, нашёл и отправил
        per_account = self.conn.execute(
            """SELECT COALESCE(account, '—') AS acc, SUM(scanned), SUM(matched), SUM(text_duplicates),
                      SUM(forwarded), SUM(hidden)
               FROM stats GROUP BY acc ORDER BY SUM(scanned) DESC"""
        ).fetchall()
        if len(per_account) > 1:
            lines += ["", "ПО АККАУНТАМ", "-" * 78,
                      f"{'аккаунт':14} {'прочит':>7} {'найдено':>8} {'дубли':>6} {'переслано':>10} "
                      f"{'скрытых':>8} {'сегодня':>8}"]
            for acc, scanned, matched, dupes, forwarded, hidden in per_account:
                today = self.forwarded_today(None if acc == "—" else acc)
                lines.append(f"{acc[:14]:14} {scanned or 0:>7} {matched or 0:>8} {dupes or 0:>6} "
                             f"{forwarded or 0:>10} {hidden or 0:>8} {today:>8}")

        # по дням: days — потолок; показываем дни, которые реально есть в базе
        day_list = [row[0] for row in self.conn.execute(
            "SELECT DISTINCT day FROM stats ORDER BY day DESC LIMIT ?", (max(1, int(days)),))]
        if days_total and len(day_list) < days:
            days_caption = (f"последние {days} дн. — данных пока за {len(day_list)} "
                            f"({self._fmt_day(first_day)}–{self._fmt_day(last_day)})")
        else:
            days_caption = f"последние {days} дн."
        lines += ["", f"ПО ДНЯМ ({days_caption})", "-" * 78,
                  f"{'дата':12} {'источник':20} {'название':32} {'прочит':>7} {'найдено':>8} "
                  f"{'дубли':>6} {'переслано':>10} {'фильтр':>7}"]
        if day_list:
            marks = ",".join("?" for _ in day_list)
            rows = self.conn.execute(
                f"SELECT day, chat_key, scanned, matched, text_duplicates, forwarded, filtered "
                f"FROM stats WHERE day IN ({marks}) ORDER BY day DESC, scanned DESC, chat_key",
                day_list).fetchall()
            for day, chat, scanned, matched, dupes, forwarded, filtered in rows:
                label = (titles.get(chat) or "")[:31]
                lines.append(f"{day:12} {self._chat_ref(chat):20} {label:32} "
                             f"{scanned or 0:>7} {matched or 0:>8} {dupes or 0:>6} "
                             f"{forwarded or 0:>10} {filtered or 0:>7}")

        since = self.day_start_utc()
        today = self.conn.execute(
            "SELECT COUNT(*) FROM forwarded WHERE ok=1 AND COALESCE(mode,'') != 'test' AND at >= ?", (since,)
        ).fetchone()[0]
        failed_today = self.conn.execute(
            "SELECT COUNT(*) FROM forwarded WHERE ok=0 AND mode LIKE 'failed%' AND at >= ?", (since,)
        ).fetchone()[0]
        queue_now = self.deferred_count()
        lines += ["", f"СЕГОДНЯ (с местной полуночи): переслано {today}"
                      + (f", ошибок {failed_today}" if failed_today else "")
                      + (f", ждёт отправки {queue_now}" if queue_now else "")]
        lines += ["", "КАК ЧИТАТЬ", "-" * 78,
                  "прочит    — сообщений просмотрено (каждое считается один раз)",
                  "найдено   — прошли правила, не дубль, сохранены как находки",
                  "дубли     — такое объявление уже было (в этом или другом чате): отброшено",
                  "переслано — ушло получателю, ВКЛЮЧАЯ добор очереди прошлых суток: за день",
                  "            может быть больше «найдено» (вчерашнее досылается сегодня)",
                  "фильтр    — подошли по словам, но срезаны --category/--only-intent/--only-direction",
                  "старше    — старше окна свежести (--max-age), не уведомляли"]
        return "\n".join(lines) + "\n"

    def stats_csv(self, path: str) -> int:
        cursor = self.conn.execute("SELECT * FROM stats ORDER BY day DESC, chat_key")
        columns = [c[0] for c in cursor.description]
        rows = cursor.fetchall()
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(columns)
            writer.writerows(rows)
        return len(rows)

    def pending(self) -> list[dict]:
        """Ненайденные... то есть неотправленные уведомления — пригодится после сбоев."""
        cursor = self.conn.execute("SELECT * FROM hits WHERE notified=0 ORDER BY id")
        columns = [c[0] for c in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def pending_count(self) -> int:
        """Сколько совпадений ещё не отправлено в уведомления (например после сбоя бота)."""
        row = self.conn.execute("SELECT COUNT(*) FROM hits WHERE notified=0").fetchone()
        return int(row[0] if row else 0)

    def export_txt(self, path: str, hours: float = 0.0, limit: int = 0) -> int:
        """Человекочитаемый отчёт в .txt: местное время, категория, направление, счёт,
        текст и ссылка. hours>0 — только совпадения за последние N часов."""
        query = "SELECT * FROM hits"
        params: list = []
        if hours > 0:
            since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
            query += " WHERE date >= ?"
            params.append(since)
        query += " ORDER BY date DESC"
        if limit > 0:
            query += " LIMIT ?"
            params.append(limit)
        cursor = self.conn.execute(query, params)
        columns = [c[0] for c in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        total_all = self.conn.execute("SELECT COUNT(*) FROM hits").fetchone()[0]
        header = [
            "Telegram-радар: посылки / передачи / попутчики",
            f"Отчёт сформирован: {datetime.now().astimezone().strftime('%d.%m.%Y %H:%M')} (местное время)",
            (f"В отчёте: {len(rows)} из {total_all} совпадений"
             + (f", только за последние {hours:g} ч" if hours > 0 else "")),
            "=" * 76,
        ]
        blocks = []
        for row in rows:
            try:
                when = datetime.fromisoformat(row["date"]).astimezone().strftime("%d.%m %H:%M")
            except Exception:  # noqa: BLE001
                when = row["date"]
            blocks.append("\n".join([
                f"[{when}] {HEADERS_TXT.get(row['category'], row['category'])} · "
                f"{INTENTS_TXT.get(row['intent'], row['intent'])} · {row['direction'] or '?'} · счёт {row['score']}",
                row["chat_title"] or "",
                (row["text"] or "").strip(),
                row["link"] or "(ссылка недоступна)",
                "-" * 76,
            ]))

        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(header) + "\n\n" + "\n".join(blocks) + "\n")
        return len(rows)

    def export_csv(self, path: str) -> int:
        cursor = self.conn.execute("SELECT * FROM hits ORDER BY date")
        columns = [c[0] for c in cursor.description]
        rows = cursor.fetchall()
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(columns)
            writer.writerows(rows)
        return len(rows)

    def stats(self) -> str:
        total = self.conn.execute("SELECT COUNT(*) FROM hits").fetchone()[0]
        by_cat = self.conn.execute(
            "SELECT category, COUNT(*) FROM hits GROUP BY category ORDER BY 2 DESC"
        ).fetchall()
        return f"{total} совпадений" + (" (" + ", ".join(f"{c}: {n}" for c, n in by_cat) + ")" if by_cat else "")

    # ---------------- пульс, ошибки, состояние панели (ТЗ §7.2, §7.3)

    @staticmethod
    def _now_utc() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def log_heartbeat(self, kind: str, account: str | None = None, chat_key: str | None = None,
                      detail: str | None = None, ts: str | None = None) -> None:
        """Пишет событие жизни радара: start/pulse/event/forward/error/flood/stop/day.

        По этим строкам бот-панель понимает, жив ли аккаунт, — БЕЗ живых проверок сессии
        (открывать второй клиент на тот же .session нельзя: Telegram отзовёт ключ).
        """
        self.conn.execute(
            "INSERT INTO heartbeats (ts, account, kind, chat_key, detail) VALUES (?, ?, ?, ?, ?)",
            (ts or self._now_utc(), account or "", kind, chat_key or "", detail or ""),
        )
        self.conn.commit()

    def last_heartbeat(self, kind: str | None = None, account: str | None = None) -> dict | None:
        """Последняя запись пульса (или другого вида). None — если записей нет вовсе."""
        query = "SELECT ts, account, kind, chat_key, detail FROM heartbeats"
        where, params = [], []
        if kind:
            where.append("kind=?")
            params.append(kind)
        if account:
            where.append("account=?")
            params.append(account)
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY id DESC LIMIT 1"
        row = self.conn.execute(query, params).fetchone()
        if row is None:
            return None
        return {"ts": row[0], "account": row[1], "kind": row[2], "chat_key": row[3], "detail": row[4]}

    def heartbeats_since(self, hours: float = 24.0, kind: str | None = None,
                         account: str | None = None) -> list[dict]:
        """Записи пульса за последние N часов (для «прочитано за час» и алертов)."""
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat(timespec="seconds")
        query = "SELECT ts, account, kind, chat_key, detail FROM heartbeats WHERE ts >= ?"
        params: list = [since]
        if kind:
            query += " AND kind=?"
            params.append(kind)
        if account:
            query += " AND account=?"
            params.append(account)
        query += " ORDER BY id"
        return [{"ts": r[0], "account": r[1], "kind": r[2], "chat_key": r[3], "detail": r[4]}
                for r in self.conn.execute(query, params).fetchall()]

    def heartbeat_age_minutes(self, kind: str = "pulse", account: str | None = None,
                              now: datetime | None = None) -> float | None:
        """Сколько минут назад был последний пульс. None — пульса не было ни разу.

        Именно по этому числу панель пишет «работает» или «молчит N мин»: молчание означает
        «нет пульса», а не «нет находок» (тихий чат ночью — это норма, а не поломка).
        """
        last = self.last_heartbeat(kind=kind, account=account)
        if not last or not last.get("ts"):
            return None
        try:
            stamp = datetime.fromisoformat(last["ts"])
        except ValueError:
            return None
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        moment = now or datetime.now(timezone.utc)
        return max(0.0, (moment - stamp).total_seconds() / 60.0)

    @staticmethod
    def parse_pulse_detail(detail: str | None) -> tuple[int, int]:
        """Из строки пульса «прочитано 412, найдено 7» достаёт (прочитано, найдено)."""
        if not detail:
            return 0, 0
        read = re.search(r"прочитано\s+(\d+)", detail)
        found = re.search(r"найдено\s+(\d+)", detail)
        return int(read.group(1)) if read else 0, int(found.group(1)) if found else 0

    def pulse_progress(self, hours: float = 1.0, account: str | None = None) -> tuple[int, int]:
        """(прочитано за последние N часов, найдено за то же окно) — по строкам пульса.

        Пульс пишет накопительные счётчики прогона, поэтому разница двух срезов и есть
        нагрузка за окно. Если пульса нет (радар запущен без --heartbeat) — нули.
        """
        rows = self.heartbeats_since(hours=max(hours, 0.01) + 24.0, kind="pulse", account=account)
        if not rows:
            return 0, 0
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        newest_read, newest_found = self.parse_pulse_detail(rows[-1].get("detail"))
        base_read, base_found = newest_read, newest_found
        for row in rows:
            try:
                stamp = datetime.fromisoformat(row["ts"])
            except (ValueError, KeyError, TypeError):
                continue
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            if stamp >= cutoff:
                break
            base_read, base_found = self.parse_pulse_detail(row.get("detail"))
        return max(0, newest_read - base_read), max(0, newest_found - base_found)

    def log_error(self, kind: str, text: str = "", account: str | None = None,
                  ts: str | None = None) -> None:
        """Журнал ошибок: сессия, FloodWait, 401 от бота, недоступный чат, чужой chat_id."""
        self.conn.execute(
            "INSERT INTO errors (ts, account, kind, text) VALUES (?, ?, ?, ?)",
            (ts or self._now_utc(), account or "", kind or "", (text or "")[:500]),
        )
        self.conn.commit()

    def errors_recent(self, limit: int = 5, account: str | None = None,
                      since_hours: float = 24.0) -> list[dict]:
        """Последние ошибки (свежие первыми) — для /errors и для /accounts."""
        query = "SELECT ts, account, kind, text FROM errors"
        params: list = []
        where = []
        if since_hours and since_hours > 0:
            where.append("ts >= ?")
            params.append((datetime.now(timezone.utc)
                           - timedelta(hours=since_hours)).isoformat(timespec="seconds"))
        if account:
            where.append("account=?")
            params.append(account)
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(int(limit))
        return [{"ts": r[0], "account": r[1], "kind": r[2], "text": r[3]}
                for r in self.conn.execute(query, params).fetchall()]

    def errors_count(self, since_hours: float = 24.0, account: str | None = None,
                     kind: str | None = None) -> int:
        """Сколько ошибок за окно (по умолчанию сутки) — для /status и алертов."""
        query = "SELECT COUNT(*) FROM errors WHERE ts >= ?"
        params: list = [(datetime.now(timezone.utc)
                         - timedelta(hours=since_hours)).isoformat(timespec="seconds")]
        if account:
            query += " AND account=?"
            params.append(account)
        if kind:
            query += " AND kind=?"
            params.append(kind)
        row = self.conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def bot_state_get(self, key: str, default: str | None = None) -> str | None:
        """Значение из bot_state (offset getUpdates, антиспам алертов, /digest)."""
        row = self.conn.execute("SELECT value FROM bot_state WHERE key=?", (key,)).fetchone()
        return row[0] if row and row[0] is not None else default

    def bot_state_set(self, key: str, value: str) -> None:
        self.conn.execute("INSERT INTO bot_state (key, value) VALUES (?, ?) "
                          "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))
        self.conn.commit()

    def log_metric(self, row: dict) -> None:
        """Сохраняет срез ресурсов (таблица metrics, ТЗ §7.2)."""
        self.conn.execute(
            """INSERT INTO metrics (ts, rss_mb, cpu_percent, uptime_s, msgs_total, msgs_last_hour,
                                    api_calls, db_mb, hits_total, forwarded_today, accounts, mode)
               VALUES (:ts, :rss_mb, :cpu_percent, :uptime_s, :msgs_total, :msgs_last_hour,
                       :api_calls, :db_mb, :hits_total, :forwarded_today, :accounts, :mode)
               ON CONFLICT(ts) DO UPDATE SET rss_mb=excluded.rss_mb, cpu_percent=excluded.cpu_percent,
                       uptime_s=excluded.uptime_s, msgs_total=excluded.msgs_total,
                       msgs_last_hour=excluded.msgs_last_hour, api_calls=excluded.api_calls,
                       db_mb=excluded.db_mb, hits_total=excluded.hits_total,
                       forwarded_today=excluded.forwarded_today, accounts=excluded.accounts,
                       mode=excluded.mode""",
            {key: row.get(key) for key in ("ts", "rss_mb", "cpu_percent", "uptime_s", "msgs_total",
                                           "msgs_last_hour", "api_calls", "db_mb", "hits_total",
                                           "forwarded_today", "accounts", "mode")},
        )
        self.conn.commit()

    def metrics_recent(self, hours: float = 24.0) -> list[dict]:
        """Срезы метрик за окно (для вердикта «за сутки»)."""
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat(timespec="seconds")
        cursor = self.conn.execute("SELECT * FROM metrics WHERE ts >= ? ORDER BY ts", (since,))
        columns = [c[0] for c in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def retention_cleanup(self, days: int = 30) -> dict[str, int]:
        """Ретеншн: чистит heartbeats/metrics/errors старше N дней (ТЗ §7.2).

        Вызывается при старте и раз в сутки, чтобы файл базы не пух месяцами.
        Возвращает, сколько строк удалено из каждой таблицы.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(days=max(0, int(days)))
                  ).isoformat(timespec="seconds")
        removed: dict[str, int] = {}
        for table in ("heartbeats", "metrics", "errors"):
            cursor = self.conn.execute(f"DELETE FROM {table} WHERE ts < ?", (cutoff,))
            removed[table] = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        self.conn.commit()
        return removed

    # ---------------- данные для бот-панели (ТЗ §14.1)

    def hits_today(self) -> int:
        """Сколько находок с местной полуночи."""
        row = self.conn.execute(
            "SELECT COUNT(*) FROM hits WHERE COALESCE(found_at, date) >= ?", (self.day_start_utc(),)
        ).fetchone()
        return int(row[0] if row else 0)

    def hits_total(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM hits").fetchone()
        return int(row[0] if row else 0)

    def hits_last(self, limit: int = 5) -> list[dict]:
        """Последние находки (свежие первыми) — для /last."""
        cursor = self.conn.execute(
            "SELECT * FROM hits ORDER BY COALESCE(found_at, date) DESC, id DESC LIMIT ?", (int(limit),)
        )
        columns = [c[0] for c in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def hits_by_chat_today(self) -> list[tuple[str, int]]:
        """Находки за сутки в разбивке по чатам — для /top и /sources."""
        rows = self.conn.execute(
            "SELECT chat_key, COUNT(*) FROM hits WHERE COALESCE(found_at, date) >= ? "
            "GROUP BY chat_key ORDER BY COUNT(*) DESC", (self.day_start_utc(),)
        ).fetchall()
        return [(row[0], int(row[1])) for row in rows]

    def last_hit_at_by_chat(self) -> dict[str, str]:
        """chat_key -> время последней находки (ISO)."""
        rows = self.conn.execute(
            "SELECT chat_key, MAX(COALESCE(found_at, date)) FROM hits GROUP BY chat_key"
        ).fetchall()
        return {row[0]: (row[1] or "") for row in rows if row[0]}

    def scanned_total(self) -> int:
        """Сколько сообщений прочитано за всё время (сумма по всем чатам и дням)."""
        row = self.conn.execute("SELECT COALESCE(SUM(scanned), 0) FROM stats").fetchone()
        return int(row[0] if row else 0)

    def hits_total(self) -> int:
        """Сколько совпадений сохранено за всё время (для пульса в схеме B)."""
        row = self.conn.execute("SELECT COUNT(*) FROM hits").fetchone()
        return int(row[0] if row else 0)

    def totals_for_chats(self, chat_keys: list[str]) -> tuple[int, int]:
        """(прочитано, найдено) за всё время по указанным чатам.

        Нужно для пульса в разовом проходе: счётчики процесса каждый раз обнуляются, а суммы из базы
        переживают перезапуск (контейнер в облаке живёт один проход — ТЗ §16).
        """
        keys = [k for k in chat_keys if k]
        if not keys:
            return self.scanned_total(), self.hits_total()
        marks = ",".join("?" * len(keys))
        read = self.conn.execute(
            f"SELECT COALESCE(SUM(scanned), 0) FROM stats WHERE chat_key IN ({marks})", keys
        ).fetchone()
        found = self.conn.execute(
            f"SELECT COUNT(*) FROM hits WHERE chat_key IN ({marks})", keys
        ).fetchone()
        return int(read[0] if read else 0), int(found[0] if found else 0)

    def scanned_by_chat(self) -> dict[str, int]:
        """chat_key -> прочитано за всё время (для /sources)."""
        rows = self.conn.execute(
            "SELECT chat_key, COALESCE(SUM(scanned), 0), COALESCE(SUM(matched), 0) FROM stats "
            "GROUP BY chat_key"
        ).fetchall()
        return {row[0]: int(row[1] or 0) for row in rows if row[0]}

    def matched_by_chat(self) -> dict[str, int]:
        rows = self.conn.execute(
            "SELECT chat_key, COALESCE(SUM(matched), 0) FROM stats GROUP BY chat_key"
        ).fetchall()
        return {row[0]: int(row[1] or 0) for row in rows if row[0]}

    def deferred_oldest(self, account: str | None = None) -> str:
        """Когда встала самая старая позиция очереди («висит с 21:40»)."""
        query = "SELECT MIN(at) FROM forwarded WHERE mode='queued'"
        params: list = []
        if account:
            query += " AND account=?"
            params.append(account)
        row = self.conn.execute(query, params).fetchone()
        return str(row[0] or "") if row else ""

    def deferred_with_links(self, limit: int = 5, account: str | None = None) -> list[dict]:
        """Позиции очереди со ссылками на сообщения — для /queue."""
        query = ("SELECT f.chat_key, f.msg_id, f.at, h.link, h.chat_title, h.direction "
                 "FROM forwarded f LEFT JOIN hits h ON h.chat_key=f.chat_key AND h.msg_id=f.msg_id "
                 "WHERE f.mode='queued'")
        params: list = []
        if account:
            query += " AND f.account=?"
            params.append(account)
        query += " ORDER BY f.at, f.chat_key, f.msg_id LIMIT ?"
        params.append(int(limit))
        return [{"chat_key": r[0], "msg_id": r[1], "at": r[2], "link": r[3] or "",
                 "chat_title": r[4] or "", "direction": r[5] or ""}
                for r in self.conn.execute(query, params).fetchall()]

    def forward_failures_today(self, account: str | None = None) -> int:
        """Ошибки отправки за сутки (не путать с очередью по лимиту)."""
        query = ("SELECT COUNT(*) FROM forwarded WHERE ok=0 AND COALESCE(mode,'') LIKE 'failed%' "
                 "AND at >= ?")
        params: list = [self.day_start_utc()]
        if account:
            query += " AND account=?"
            params.append(account)
        row = self.conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def accounts_seen(self) -> list[str]:
        """Имена аккаунтов, о которых база что-то знает (история + пульс)."""
        names: list[str] = []
        for query in ("SELECT DISTINCT account FROM stats WHERE COALESCE(account, '') != ''",
                      "SELECT DISTINCT account FROM forwarded WHERE COALESCE(account, '') != ''",
                      "SELECT DISTINCT account FROM heartbeats WHERE COALESCE(account, '') != ''"):
            for row in self.conn.execute(query).fetchall():
                if row[0] and row[0] not in names:
                    names.append(row[0])
        return names


# ------------------------------------------------------------------ конфиг

@dataclass
class Source:
    target: str
    title: str = ""
    profile: str = "chat"          # chat | news
    min_score: int = 4
    catchup: int = 0
    enabled: bool = True
    topics: tuple[int, ...] = ()    # для форум-чатов: читать только эти темы (пусто — все)
    account: str = ""               # какой аккаунт следит за чатом (пусто — первый из accounts)


@dataclass
class AccountConfig:
    """Один аккаунт Telegram: своя сессия, свои лимиты пересылки, свой список чатов."""
    name: str
    session: str
    forward: dict
    proxy: str | None = None
    enabled: bool = True


def resolve_accounts(args, defaults: dict) -> list[AccountConfig]:
    """Собирает список аккаунтов: из секции accounts в конфиге либо один «main» как раньше.

    Порядок приоритетов для каждого параметра: флаг командной строки → аккаунт в конфиге →
    общий forward в конфиге → встроенное значение.
    """
    shared = defaults.get("forward") or {}
    raw_accounts = defaults.get("accounts") or {}
    accounts: list[AccountConfig] = []

    if raw_accounts:
        for name, item in raw_accounts.items():
            item = item or {}
            if not item.get("enabled", True):
                continue
            forward = {**shared, **(item.get("forward") or {})}
            accounts.append(AccountConfig(
                name=str(name),
                session=str(item.get("session") or f"{name}_session"),
                forward=forward,
                proxy=item.get("proxy"),
            ))
    else:
        # старый конфиг без accounts: один аккаунт, название main — к нему относится история
        accounts.append(AccountConfig(name="main", session=args.session, forward=dict(shared),
                                      proxy=None))

    if not accounts:
        accounts = [AccountConfig(name="main", session=args.session, forward=dict(shared))]

    # флаги командной строки перекрывают настройки того аккаунта, который запускается одним
    limit = getattr(args, "forward_max_per_day", None)
    if limit is not None or getattr(args, "forward_to", None):
        target = accounts[0]
        if getattr(args, "forward_to", None):
            target.forward["to"] = args.forward_to
        if limit is not None:
            target.forward["max_per_day"] = limit
        if getattr(args, "forward_mode", None):
            target.forward["mode"] = args.forward_mode
        if getattr(args, "forward_fallback", None):
            target.forward["fallback"] = args.forward_fallback

    only = getattr(args, "account", None)
    if only:
        picked = [a for a in accounts if a.name == only]
        if not picked:
            sys.exit(f"Аккаунт «{only}» не найден в конфиге. Есть: "
                     + ", ".join(a.name for a in accounts))
        accounts = picked
    return accounts


def sources_for_account(sources: list[Source], accounts: list[AccountConfig]) -> dict[str, list[Source]]:
    """Раскладывает чаты по аккаунтам: account у источника, иначе — первый аккаунт.

    account: auto — распределить по кругу (удобно, когда оба аккаунта состоят в одних чатах
    и хочется развести нагрузку).
    """
    names = [a.name for a in accounts]
    buckets: dict[str, list[Source]] = {name: [] for name in names}
    auto_index = 0
    for source in sources:
        where = (source.account or "").strip()
        if where.lower() == "auto":
            name = names[auto_index % len(names)]
            auto_index += 1
        elif where:
            if where not in buckets:
                sys.exit(f"У источника {source.target} указан аккаунт «{where}», "
                         f"которого нет в accounts. Есть: {', '.join(names)}")
            name = where
        else:
            name = names[0]
        buckets[name].append(source)
    return buckets


DEFAULT_CONFIG = {
    "defaults": {"min_score": 4, "catchup": 30, "profile": "chat"},
    "forward": {},          # куда и как пересылать: to, mode, max_per_day, fallback
    "sources": [],
}


def order_sources(sources: list[Source], mode: str = "random", rng=None) -> list[Source]:
    """Порядок обхода чатов.

    random — каждый запуск начинается со случайного чата: при дневном лимите пересылок
    не получается так, что «хвост» списка систематически голодает. config — строго как в файле.
    """
    items = list(sources)
    if mode == "random" and len(items) > 1:
        (rng or random).shuffle(items)
    return items


LAST_CONFIG_PATH: Path | None = None      # какой файл реально прочитан (для --doctor и логов)


def find_nested_project(root: Path = Path(".")) -> list[Path]:
    """Ищет вложенные копии проекта: так выглядит распакованный «поверх» архив.

    Windows при распаковке telegram-scraper.zip часто создаёт папку с тем же именем внутри
    рабочей. Тогда правки уходят в копию, а запускается файл из корня (или наоборот) —
    отсюда «я поменял лимит, а радар считает по-старому».
    """
    found = []
    try:
        children = list(root.iterdir())
    except OSError:
        return found
    for child in children:
        if child.is_dir() and (child / "monitor.py").exists() and (
                (child / "sources.yaml").exists() or (child / "sources.json").exists()):
            found.append(child)
    return found


def load_config(path: str) -> tuple[dict, list[Source]]:
    """Читает sources.yaml (нужен pyyaml) или sources.json (работает всегда).
    Если YAML не читается — сам переключается на sources.json, а не падает."""
    use = Path(path)
    if not use.exists():
        alt = Path("sources.json")
        if alt.exists() and path.endswith((".yaml", ".yml")):
            print("[i] sources.yaml не найден — использую sources.json", file=sys.stderr)
            use = alt
        else:
            print(f"[i] конфиг {path} не найден — значения по умолчанию (нужен --channels)", file=sys.stderr)
            return DEFAULT_CONFIG, []

    raw = None
    if use.suffix in (".yaml", ".yml"):
        try:
            import yaml
        except ImportError:
            json_alt = Path("sources.json")
            if json_alt.exists():
                print("[!] pyyaml не установлен — читаю sources.json. "
                      "Чтобы работал YAML: pip install -r requirements.txt", file=sys.stderr)
                use = json_alt
            else:
                sys.exit("Нужен pyyaml: pip install -r requirements.txt  (или создай sources.json "
                         "со списком чатов — образец в архиве)")
        else:
            raw = yaml.safe_load(use.read_text(encoding="utf-8")) or {}
    if raw is None and use.suffix == ".json":
        raw = json.loads(use.read_text(encoding="utf-8"))

    global LAST_CONFIG_PATH
    LAST_CONFIG_PATH = use.resolve()

    def as_topics(value) -> tuple[int, ...]:
        """Темы из конфига: [12345, 67890] или ссылка https://t.me/chat/12345/77 (первое число —
        id темы). Пустой список/None — читать чат целиком."""
        if value is None:
            return ()
        items = value if isinstance(value, (list, tuple)) else [value]
        topics: list[int] = []
        for item in items:
            if isinstance(item, bool):
                continue
            if isinstance(item, int):
                topics.append(item)
                continue
            digits = re.findall(r"\d+", str(item))
            if digits:
                topics.append(int(digits[0]))
        return tuple(dict.fromkeys(topics))

    defaults = {**DEFAULT_CONFIG["defaults"], **(raw.get("defaults") or {})}
    defaults["forward"] = {**DEFAULT_CONFIG["forward"], **(raw.get("forward") or {})}
    defaults["accounts"] = raw.get("accounts") or {}
    sources = []
    for item in raw.get("sources") or []:
        if isinstance(item, str):
            item = {"target": item}
        sources.append(Source(
            target=item["target"],
            title=item.get("title", ""),
            profile=item.get("profile", defaults["profile"]),
            min_score=int(item.get("min_score", defaults["min_score"])),
            catchup=int(item.get("catchup", defaults["catchup"])),
            enabled=bool(item.get("enabled", True)),
            topics=as_topics(item.get("topics")),
            account=str(item.get("account") or ""),
        ))
    active = [s for s in sources if s.enabled]
    fmt = "yaml" if use.suffix in (".yaml", ".yml") else "json"
    limit = (defaults.get("forward") or {}).get("max_per_day")
    acc_names = list((defaults.get("accounts") or {}).keys())
    print(f"[i] конфиг: {use.resolve()} ({fmt}), источников {len(active)}, "
          f"аккаунтов {len(acc_names) if acc_names else 1}"
          + (f" ({', '.join(acc_names)})" if acc_names else "")
          + f", лимит пересылок {limit if limit is not None else '—'}/сутки, "
            f"порядок обхода {defaults.get('order', 'random')}", file=sys.stderr)

    # в папке рядом может лежать второй конфиг с другими значениями — предупреждаем,
    # чтобы не искать потом, «почему лимит не тот»
    other = use.parent / ("sources.json" if fmt == "yaml" else "sources.yaml")
    if other.exists():
        try:
            if other.suffix == ".json":
                other_raw = json.loads(other.read_text(encoding="utf-8"))
            else:
                import yaml as _yaml
                other_raw = _yaml.safe_load(other.read_text(encoding="utf-8")) or {}
            other_limit = (other_raw.get("forward") or {}).get("max_per_day")
            other_count = len(other_raw.get("sources") or [])
            notes = []
            if other_limit is not None and limit is not None and int(other_limit) != int(limit):
                notes.append(f"лимит пересылок {other_limit} против {limit}")
            if other_count and other_count != len(active):
                notes.append(f"источников {other_count} против {len(active)}")
            if notes:
                print(f"[!] {other.name} в этой папке не совпадает с {use.name}: "
                      + ", ".join(notes) + f". Сейчас читается {use.name} — "
                      "если запускаешь без pyyaml или поправил только один файл, значения будут другими.",
                      file=sys.stderr)
        except Exception:  # noqa: BLE001
            pass

    return defaults, active


# ------------------------------------------------------------------ мониторинг

class Monitor:
    def __init__(self, client, store: HitStore, sources: list[Source], notifier,
                 paced: Paced, explain: bool = False, skip_out: bool = True, dedup_window: float = 24.0,
                 dedup_scope: str = "global", only_intents: tuple[str, ...] = (),
                 only_directions: tuple[str, ...] = (), max_age: float = 0.0,
                 only_categories: tuple[str, ...] = (), forwarder=None, auto_join: bool = False,
                 heartbeat_minutes: float = 0.0, keep_hidden: bool = False,
                 account: str = "", show_account: bool = False,
                 deadline: float | None = None, flood_wait_limit: float = 0.0):
        self.client, self.store, self.sources = client, store, sources
        self.notify, self.paced, self.explain, self.skip_out = notifier, paced, explain, skip_out
        self.dedup_window = dedup_window
        self.dedup_scope = dedup_scope
        self.only_intents = tuple(only_intents)
        self.only_directions = tuple(only_directions)
        self.max_age_hours = max_age         # 0 — без ограничения по возрасту сообщений
        self.only_categories = tuple(only_categories)   # например ('parcel',) — без чистых попутчиков
        self.forwarder = forwarder                      # пересылка совпадений (может быть None)
        self.auto_join = auto_join                      # подписываться по t.me/+ ссылкам
        self.per_chat: dict[str, dict[str, int]] = {}   # счётчики по каждому чату для статистики
        self.entities: dict[str, object] = {}
        self.meta: dict[str, Source] = {}
        self.sender_cache: dict[int, str] = {}
        self.topic_names: dict[int, str] = {}      # id темы -> название (для уведомлений)
        self.heartbeat_minutes = heartbeat_minutes  # раз в N минут печатать, что радар жив
        self.keep_hidden = keep_hidden              # True — не отсекать авторов со скрытым профилем
        self.account = account                      # имя аккаунта из конфига (main, second, ...)
        self.show_account = show_account            # печатать ли аккаунт в уведомлениях (когда их несколько)
        self.heartbeat_task = None
        self.last_pulse: dict[str, int] = {}        # счётчики на момент прошлого пульса
        self.last_event: tuple | None = None        # (время, чат) последнего принятого сообщения
        self.counter = {"scanned": 0, "matched": 0, "saved": 0, "duplicates": 0,
                        "text_duplicates": 0, "cross_chat_duplicates": 0, "filtered": 0,
                        "too_old": 0, "hidden": 0}
        # Бюджет прохода (time.monotonic). Схема B — это один проход на запуск: если его
        # оборвут по таймауту снаружи, не успеют записаться ни пульс, ни счётчики, ни
        # очередь пересылок. Поэтому радар сам следит за временем и не начинает новые чаты,
        # когда пора заканчивать. deadline=None — читать всё (живой режим, своя машина).
        self.deadline = deadline
        self._queued_cache: dict = {}         # пачка сообщений очереди, прочитанная одним запросом
        self.flood_wait_limit = float(flood_wait_limit or 0.0)
        self.catchup_left: list[str] = []          # чаты, до которых не дошли в этот проход
        self.paced.max_wait = self.flood_budget    # подсказка для call(): сколько можно спать

    # --- бюджет прохода: чтобы его не обрывал таймаут

    def time_left(self) -> float:
        """Сколько секунд осталось на проход (inf, если бюджет не задан)."""
        if self.deadline is None:
            return float("inf")
        return max(self.deadline - time.monotonic(), 0.0)

    def flood_budget(self) -> float | None:
        """Сколько можно спать по FloodWait: остаток бюджета, но не больше лимита.

        None — ждать сколько угодно (своя машина, процесс живёт долго).
        """
        if self.deadline is None and self.flood_wait_limit <= 0:
            return None
        left = self.time_left()
        if self.flood_wait_limit > 0:
            return max(min(left, self.flood_wait_limit), 0.0)
        return max(left, 0.0)

    # --- счётчики для файла статистики

    def _bump(self, chat_key: str, **counters: int) -> None:
        bucket = self.per_chat.setdefault(chat_key, {})
        for key, value in counters.items():
            bucket[key] = bucket.get(key, 0) + value

    def flush_stats(self) -> None:
        """Сливает счётчики прогона в базу (по каждому чату и дню)."""
        for chat_key, counters in self.per_chat.items():
            if counters:
                self.store.bump_stats(chat_key, account=self.account or None, **counters)
        self.per_chat.clear()

    # --- добор отложенных пересылок

    def entity_by_key(self) -> dict[str, object]:
        """chat_key (как в базе) -> сущность чата, по уже разрешённым источникам."""
        mapping: dict[str, object] = {}
        for source in self.sources:
            entity = self.entities.get(source.target)
            if entity is not None:
                mapping[self.chat_key(source)] = entity
        return mapping

    async def fetch_queued(self, chat_key: str, msg_id: int):
        """Достаёт исходное сообщение для добора из очереди.

        Возвращает сообщение; None — сообщения больше нет (удалено) или источник убран из
        конфига; FETCH_FAILED — прочитать не удалось (сеть, флуд): оставляем в очереди.
        """
        mapping = self.entity_by_key()
        entity = mapping.get(chat_key)
        if entity is None:
            target = next((s.target for s in self.sources if self.chat_key(s) == chat_key), None)
            if target is None:
                return None                      # источник убрали из конфига
            try:
                entity = await call(lambda t=target: self.client.get_entity(t), self.paced,
                                    label=f"get_entity({target})")
            except Exception as exc:  # noqa: BLE001
                print(f"[!] добор {chat_key}/{msg_id}: чат недоступен ({type(exc).__name__}), "
                      f"повторим в следующий прогон", file=sys.stderr)
                return FETCH_FAILED
        cached = self._queued_cache.pop((chat_key, msg_id), _MISSING)
        if cached is not _MISSING:
            return cached
        # Одним запросом берём сразу пачку из очереди по этому чату (до 50 id): раньше каждое
        # сообщение стоило отдельного запроса с паузой ~2,5 с — на очереди в сотни это часы.
        ids = [m for k, m in self.store.deferred_queue(self.forwarder.account or None)
               if k == chat_key][:50] if self.forwarder is not None else []
        if msg_id not in ids:
            ids = [msg_id]
        try:
            found = await call(lambda: self.client.get_messages(entity, ids=ids), self.paced,
                               label="get_messages(queued)")
        except Exception as exc:  # noqa: BLE001
            print(f"[!] добор {chat_key}/{msg_id}: {type(exc).__name__} {exc} — "
                  f"повторим в следующий прогон", file=sys.stderr)
            return FETCH_FAILED
        if isinstance(found, (list, tuple)) and len(found) == len(ids):
            for one_id, one in zip(ids, found):
                self._queued_cache[(chat_key, one_id)] = one
            return self._queued_cache.pop((chat_key, msg_id), None)
        if isinstance(found, (list, tuple)):
            found = found[0] if found else None
        return found

    async def flush_deferred(self) -> dict:
        """Отправляет то, что не влезло в лимит раньше. Идёт ПЕРВЫМ делом при запуске."""
        if self.forwarder is None or not self.entities:
            return {}
        before = self.store.deferred_count()
        if not before:
            return {}
        # На добор — не больше половины оставшегося бюджета (и не меньше 30 с на хвост): иначе
        # очередь в сотни находок съедала весь проход, а остальные чаты и запись состояния
        # не успевали. Остаток очереди никуда не девается — уйдёт в следующий проход.
        stop_at = None
        if self.deadline is not None:
            stop_at = time.monotonic() + self.time_left() * 0.5
        result = await self.forwarder.flush_deferred(
            self.fetch_queued,
            should_stop=(lambda: time.monotonic() >= stop_at or self.time_left() < 30)
            if stop_at is not None else None)
        self._queued_cache.clear()
        if result.get("sent"):
            self.counter["forwarded"] = self.counter.get("forwarded", 0) + result["sent"]
        return result

    # --- служебное

    def chat_key(self, source: Source) -> str:
        return source_chat_key(source)         # общая функция: её же использует --check-sources

    async def sender_name(self, sender_id: int | None) -> str | None:
        if not sender_id:
            return None
        if sender_id in self.sender_cache:
            return self.sender_cache[sender_id]
        try:
            entity = await call(lambda: self.client.get_entity(sender_id), self.paced, label="get_entity(sender)")
            name = getattr(entity, "title", None) or " ".join(
                part for part in [getattr(entity, "first_name", None), getattr(entity, "last_name", None)] if part
            ) or getattr(entity, "username", None)
            self.sender_cache[sender_id] = name or str(sender_id)
        except Exception:  # noqa: BLE001
            self.sender_cache[sender_id] = str(sender_id)
        return self.sender_cache[sender_id]

    # --- основной конвейер

    async def process_message(self, message, source: Source) -> dict | None:
        """Скан одного сообщения. None — если мимо (или уже видели)."""
        key = self.chat_key(source)
        if self.store.is_seen(key, message.id):
            self.counter["duplicates"] += 1
            self._bump(key, duplicates=1)
            return None
        self.store.mark_seen(key, message.id)
        self.counter["scanned"] += 1
        self._bump(key, scanned=1)

        if self.skip_out and getattr(message, "out", False):
            return None

        # окно свежести: сообщения старше N часов не уведомляют (помечаем просмотренными)
        if self.max_age_hours > 0:
            age_hours = (datetime.now(timezone.utc) - message.date.astimezone(timezone.utc)).total_seconds() / 3600
            if age_hours > self.max_age_hours:
                self.counter["too_old"] += 1
                self._bump(key, too_old=1)
                return None

        text = getattr(message, "text", None) or getattr(message, "raw_text", None) or ""
        if not isinstance(text, str) or len(text.strip()) < 4:   # медиа без подписи пропускаем
            return None

        match = analyze(text, min_score=source.min_score, explain=True, profile=source.profile)
        if not match.matched:
            return None
        # фильтры: категория, намерение, направление. В статистике видно, КАКОЙ из них срезал
        # сообщение (считается первый сработавший: категория -> намерение -> направление)
        rejected = None
        if self.only_categories and match.category not in self.only_categories:
            rejected = ("category", f"категория {match.category}, нужна {','.join(self.only_categories)}")
        elif self.only_intents and match.intent not in self.only_intents:
            rejected = ("intent", f"намерение {match.intent}, нужно {','.join(self.only_intents)}")
        elif self.only_directions and not direction_allowed(match.direction, self.only_directions):
            rejected = ("direction", f"направление {match.direction}, нужно {','.join(self.only_directions)}")
        if rejected:
            kind, why = rejected
            self.counter["filtered"] += 1
            self._bump(key, filtered=1, **{f"filtered_{kind}": 1})
            if self.explain and self.counter["filtered"] <= 10:
                snippet = " ".join(text.split())[:90]
                print(f"[i] отсеяно фильтром: {why} — «{snippet}»", file=sys.stderr)
            return None

        # автор скрыт («hidden by user»): написать ему нельзя — такое объявление бесполезно
        if not self.keep_hidden:
            reason = hidden_author_reason(message)
            if reason:
                self.counter["hidden"] += 1
                self._bump(key, hidden=1)
                if self.explain and self.counter["hidden"] <= 5:
                    src = self.entities.get(source.target)     # ещё не читали — берём здесь
                    link = message_link(getattr(src, "username", None),
                                        getattr(src, "id", None), message.id)
                    print(f"[i] пропущено, автор скрыт: {link or message.id} — {reason}",
                          file=sys.stderr)
                return None

        duplicate, source_chat = self.store.text_is_duplicate(
            key, text, self.dedup_window, self.dedup_scope)
        if duplicate:
            self.counter["text_duplicates"] += 1
            self._bump(key, text_duplicates=1)
            if source_chat and source_chat != key:
                self.counter["cross_chat_duplicates"] += 1   # тот же текст уже приходил из другого чата
                self._bump(key, cross_chat=1)
            return None
        self.counter["matched"] += 1
        self._bump(key, matched=1)

        entity = self.entities.get(source.target)
        chat_id = getattr(entity, "id", None)
        username = getattr(entity, "username", None)
        topic_id = topic_of(message)
        if topic_id and topic_id not in self.topic_names:
            title = topic_title_of(message)          # сообщение о создании темы
            if title:
                self.topic_names[topic_id] = title
        hit = {
            "chat_key": key,
            "chat_title": source.title or getattr(entity, "title", key),
            "chat_id": str(chat_id) if chat_id else "",
            "username": username or "",
            "msg_id": message.id,
            "date": message.date.astimezone(timezone.utc).isoformat(),
            "sender_id": str(getattr(message, "sender_id", "") or ""),
            "sender_name": await self.sender_name(getattr(message, "sender_id", None)),
            "text": text,
            "score": match.score,
            "category": match.category,
            "intent": match.intent,
            "direction": match.direction,
            "countries": match.countries,
            "hits": match.hit_labels,
            "link": message_link(username, chat_id, message.id),
            "found_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "topic_id": topic_id,
            "topic_name": (self.topic_names.get(topic_id) if topic_id else "") or "",
            "account": self.account if self.show_account else "",
        }
        if self.store.save_hit(hit):
            self.counter["saved"] += 1
            self._bump(key, saved=1)
        delivered = await self.notify(hit)
        if delivered is False:
            # уведомление не ушло (например 401 у бота): НЕ помечаем отправленным —
            # догнать можно командой --resend-pending
            self.counter["notify_failed"] = self.counter.get("notify_failed", 0) + 1
        else:
            self.store.mark_notified(hit["chat_key"], hit["msg_id"])

        # пересылка получателю (боту/каналу) — именно forward, а не копия
        if self.forwarder is not None:
            try:
                status = await self.forwarder.send(message, hit)
            except Exception as exc:  # noqa: BLE001
                status = f"failed:{type(exc).__name__}"
                print(f"[!] пересылка не удалась: {type(exc).__name__} {exc}", file=sys.stderr)
            self.counter["forwarded" if status in ("forwarded", "copied") else f"forward_{status}"] = \
                self.counter.get("forwarded" if status in ("forwarded", "copied") else f"forward_{status}", 0) + 1
            if status in ("forwarded", "copied"):
                self._bump(key, forwarded=1)
            elif status == "skipped":
                # пересылка запрещена в чате и fallback=skip. Отложенное из-за дневного
                # лимита («limit») считается отдельно — колонкой deferred (её пишет Forwarder)
                self._bump(key, forward_skipped=1)
            elif status.startswith("failed"):
                self._bump(key, forward_failed=1)
        return hit

    # --- старт

    async def catch_up(self, sources: list[Source]) -> None:
        """Догоняем хвосты чатов. Когда бюджет прохода на исходе — новые чаты не начинаем:
        недочитанное пойдёт в следующий проход (чаты обходятся в случайном порядке, поэтому
        ни один не застаивается), зато этот проход закончится сам и всё сохранит.
        """
        for source in sources:
            entity = self.entities.get(source.target)
            if entity is None or source.catchup <= 0:
                continue
            if self.time_left() < 5:
                self.catchup_left.append(source.target)
                continue
            if source.topics:
                # форум-чат: читаем только выбранные темы (по каждой — свой хвост)
                collected: list = []
                for topic_id in source.topics:
                    try:
                        part = await call(
                            lambda e=entity, n=source.catchup, t=topic_id:
                                self.client.get_messages(e, limit=n, reply_to=t),
                            self.paced, label=f"catchup({source.target}, тема {topic_id})",
                        )
                    except Exception as exc:  # noqa: BLE001
                        print(f"[!] catch-up {source.target}, тема {topic_id}: {exc}", file=sys.stderr)
                        continue
                    collected.extend(part or [])
                messages = collected
            else:
                try:
                    messages = await call(
                        lambda e=entity, n=source.catchup: self.client.get_messages(e, limit=n),
                        self.paced, label=f"catchup({source.target})",
                    )
                except Exception as exc:  # noqa: BLE001
                    print(f"[!] catch-up {source.target}: {exc}", file=sys.stderr)
                    continue
            scanned_before = self.counter["scanned"]
            too_old_before = self.counter["too_old"]
            found = 0
            for message in sorted(messages or [], key=lambda m: m.id):   # старые -> новые
                if await self.process_message(message, source):
                    found += 1
            # «взято» — сколько забрали из чата, «новых» — сколько увидели впервые:
            # остальное уже попадалось в прошлых проходах (дедупликация по id сообщения)
            fresh = self.counter["scanned"] - scanned_before
            too_old = self.counter["too_old"] - too_old_before
            note = f", старше {self.max_age_hours:g} ч пропущено {too_old}" if self.max_age_hours > 0 else ""
            print(f"[i] catch-up {source.target}: взято {len(messages or [])}, новых {fresh}, "
                  f"совпадений {found}{note}", file=sys.stderr)
        if self.catchup_left:
            print(f"[i] бюджет прохода кончился: не прочитано чатов {len(self.catchup_left)} "
                  f"({', '.join(self.catchup_left[:5])}{'…' if len(self.catchup_left) > 5 else ''}) "
                  f"— дойдут в следующий проход", file=sys.stderr)

    # --- пульс: чтобы долгая тишина не выглядела как зависание

    def heartbeat_text(self) -> str:
        """Одна строка о состоянии: сколько прочитано/найдено с прошлого пульса, что с пересылкой."""
        now = datetime.now().astimezone().strftime("%H:%M")
        previous = self.last_pulse or {}
        delta = {k: self.counter.get(k, 0) - previous.get(k, 0) for k in
                 ("events", "scanned", "matched", "duplicates", "filtered", "too_old")}
        self.last_pulse = dict(self.counter)

        forwarded_today = self.store.forwarded_window(self.account or None) if self.forwarder else self.store.forwarded_today()
        queue = self.store.deferred_count()
        pending = self.store.pending_count()
        limit = getattr(self.forwarder, "max_per_day", 0) if self.forwarder else 0
        delivered = delta["events"] or delta["scanned"]
        if delivered:
            activity = (f"с прошлого пульса: пришло {delivered}, найдено {delta['matched']}, "
                        f"повторов {delta['duplicates']}, мимо {delta['filtered'] + delta['too_old']}")
        else:
            activity = "с прошлого пульса сообщений не было"
        who = f"[{self.account}] " if self.show_account else ""
        parts = [
            f"[{now}] {who}жив, слушаю {len(self.entities)} источник(ов)",
            activity,
            f"всего находок {self.counter.get('saved', 0)}",
        ]
        if self.forwarder is not None:
            parts.append(f"переслано сегодня {forwarded_today}" + (f"/{limit}" if limit else "")
                         + (" (лимит обнулится в 00:00)" if limit and forwarded_today >= limit else ""))
        if queue:
            parts.append(f"в очереди {queue}")
        if pending:
            parts.append(f"не отправлено уведомлений {pending}")
        if self.counter.get("hidden"):
            parts.append(f"скрытых авторов {self.counter['hidden']}")
        if self.counter.get("other_topic"):
            parts.append(f"не в наших темах {self.counter['other_topic']}")
        if self.counter.get("unknown_chat"):
            parts.append(f"не сопоставлено сообщений {self.counter['unknown_chat']}")
        if self.last_event is not None:
            at, target = self.last_event
            parts.append(f"последнее сообщение {at.strftime('%H:%M')} {target}")
        else:
            parts.append("с запуска ни одного сообщения")
        return " · ".join(parts)

    async def on_new_day(self) -> dict:
        """Местная полночь: лимит пересылок обнуляется — добираем очередь без перезапуска.

        Долгий живой прогон (сутки и больше) иначе оставался бы с «выработанным» лимитом:
        счётчик отправок читается один раз при старте, и очередь ждала бы перезапуска.
        """
        if self.forwarder is not None:
            self.forwarder.sent_today = self.store.forwarded_window(self.account or None)   # сразу после полуночи это 0
            self.forwarder.stopped_reason = None
        queue = self.store.deferred_count()
        if self.forwarder is not None:
            print(f"[i] новый день ({datetime.now().astimezone().strftime('%d.%m')}): "
                  f"лимит пересылок обнулён" + (f", в очереди {queue}" if queue else ""), file=sys.stderr)
        drained = await self.flush_deferred()
        if drained.get("sent"):
            print(f"[i] добор после полуночи: отправлено {drained['sent']}, "
                  f"осталось в очереди {self.store.deferred_count()}", file=sys.stderr)
        self.store.log_heartbeat("day", account=self.account or None,
                                 detail=f"лимит обнулён, в очереди {self.store.deferred_count()}")
        return drained

    async def _daily_loop(self) -> None:
        """Следит за сменой местной даты, чтобы обнулить лимит и добрать очередь."""
        last_day = datetime.now().astimezone().date()
        while True:
            await asyncio.sleep(60)
            today = datetime.now().astimezone().date()
            if today != last_day:
                last_day = today
                try:
                    await self.on_new_day()
                except Exception as exc:  # noqa: BLE001
                    print(f"[!] сбой при смене суток: {type(exc).__name__} {exc}", file=sys.stderr)

    def log_pulse(self, read: int | None = None, found: int | None = None) -> None:
        """Пишет пульс в базу: по нему бот-панель понимает, жив ли аккаунт (ТЗ §7.3).

        Важно: живость определяется именно пульсом, а не находками — иначе тихий чат
        ночью выглядел бы как «аккаунт сломался» (ТЗ §14.1).

        Числа по умолчанию берутся из счётчиков процесса: в схеме A процесс живёт долго,
        счётчики накопительные, и разница двух пульсов — нагрузка за окно. В разовом проходе
        (схема B) процесс каждый раз новый, поэтому туда передают суммы из базы — иначе
        разница пульсов всегда была бы нулевой и панель показывала бы «прочитано 0 за час».
        """
        try:
            scanned = self.counter.get("scanned", 0) if read is None else int(read)
            saved = self.counter.get("saved", 0) if found is None else int(found)
            self.store.log_heartbeat(
                "pulse", account=self.account or None,
                detail=f"прочитано {scanned}, найдено {saved}",
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[!] пульс в базу не записан: {type(exc).__name__} {exc}", file=sys.stderr)

    async def _heartbeat_loop(self) -> None:
        interval = max(0.05, self.heartbeat_minutes) * 60
        while True:
            await asyncio.sleep(interval)
            print("[i] " + self.heartbeat_text(), file=sys.stderr)
            self.flush_stats()      # счётчики — в базу: метрики и панель видят свежие цифры
            self.log_pulse()

    async def run(self) -> None:
        from telethon import events

        chat_report: dict = {}
        resolved = await resolve_targets(self.client, [s.target for s in self.sources], self.paced,
                                         auto_join=self.auto_join, report=chat_report)
        if self.account:
            record_account_chats(self.store, self.account, self.sources, resolved, chat_report)
        self.entities = resolved
        self.meta = {s.target: s for s in self.sources if s.target in resolved}

        # панель видит старт аккаунта и сразу получает свежий пульс (ТЗ §7.3)
        self.store.log_heartbeat("start", account=self.account or None,
                                 detail=f"источников {len(resolved)} из {len(self.sources)}")
        self.log_pulse()

        await self.flush_deferred()           # сначала отдаём то, что не влезло в лимит раньше
        await self.catch_up([self.meta[t] for t in resolved])

        chats = [resolved[target] for target in resolved]
        if not chats:
            print("[!] ни один источник не разрешился — мониторить нечего", file=sys.stderr)
            return

        async def handler(event):
            target = self._target_by_entity(event.chat_id)
            source = self.meta.get(target)
            if source is None:
                # так выглядит несопоставленный чат: обычно значит, что источник есть в
                # Telegram, но не нашёлся в конфиге (или id не совпал) — сообщение пропускаем
                if self.counter.get("unknown_chat", 0) < 3:
                    print(f"[!] сообщение из чата id={event.chat_id} не сопоставлено с источниками: "
                          f"пропускаю (проверь список чатов в конфиге)", file=sys.stderr)
                self.counter["unknown_chat"] = self.counter.get("unknown_chat", 0) + 1
                return
            # считаем все принятые сообщения, даже если они не подошли под правила:
            # так в пульсе видно, что радар действительно слышит чаты
            self.counter["events"] = self.counter.get("events", 0) + 1
            self.last_event = (datetime.now().astimezone(), target)
            if source.topics:                      # следим только за выбранными темами
                message_topic = topic_of(event.message)
                if message_topic not in source.topics:
                    self.counter["other_topic"] = self.counter.get("other_topic", 0) + 1
                    return
            try:
                await self.process_message(event.message, source)
            except Exception as exc:  # noqa: BLE001
                print(f"[!] ошибка обработки сообщения: {type(exc).__name__} {exc}", file=sys.stderr)

        self.client.add_event_handler(handler, events.NewMessage(chats=chats))
        self.last_pulse = dict(self.counter)
        daily_task = asyncio.create_task(self._daily_loop())
        if self.heartbeat_minutes > 0:
            self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            topics_note = ", ".join(f"{s.target}: темы {list(s.topics)}" for s in self.sources if s.topics)
            print(f"[+] слушаю {len(chats)} источник(ов), пульс каждые {self.heartbeat_minutes:g} мин"
                  + (f"; фильтр по темам: {topics_note}" if topics_note else "")
                  + ". Ctrl+C — выход.", file=sys.stderr)
            print("[i] " + self.heartbeat_text(), file=sys.stderr)
        else:
            print(f"[+] слушаю {len(chats)} источник(ов). Ctrl+C — выход.", file=sys.stderr)
        try:
            await self.client.run_until_disconnected()
        finally:
            for task in (self.heartbeat_task, daily_task):
                if task is not None:
                    task.cancel()
            self.heartbeat_task = None
            self.flush_stats()
            self.store.log_heartbeat("stop", account=self.account or None, detail="остановлен")

    def _target_by_entity(self, chat_id: int | None) -> str:
        """Источник по id из события.

        Событие приносит «маркированный» id (-100… для каналов и супергрупп), а у сущности
        из get_entity id положительный: сравнивать их напрямую нельзя (иначе живой режим
        молча пропускает все сообщения). Сопоставляем по peer_id, дополнительно принимаем
        и сырой id — на случай чатов старого формата.
        """
        if chat_id is None:
            return ""
        # индекс пересобирается, если состав источников подменили (например, после resolve)
        if getattr(self, "_id_index_source", None) is not self.entities:
            self._id_index: dict[int, str] = {}
            for target, entity in self.entities.items():
                for value in (peer_id(entity), getattr(entity, "id", None)):
                    if value is not None:
                        self._id_index[int(value)] = target
            self._id_index_source = self.entities
        return self._id_index.get(int(chat_id), "")


# ------------------------------------------------------------------ CLI

async def resend_pending(store, notifier, limit: int = 50) -> tuple[int, int]:
    """Догоняет уведомления, которые не ушли раньше (сбои бота, 401 и т.п.).

    Возвращает (отправлено, осталось неотправленных). Порядок — от старых к новым.
    """
    pending = store.pending()
    sent, failed = 0, 0
    for row in pending[:limit]:
        hit = dict(row)
        hit["hits"] = [x for x in (hit.get("hits") or "").split(",") if x]
        hit["countries"] = [x for x in (hit.get("countries") or "").split(",") if x]
        delivered = await notifier(hit)
        if delivered is False:
            failed += 1
            if failed >= 3:
                print("[!] подряд не уходит несколько уведомлений — останавливаю догон, "
                      "исправь настройку и запусти снова", file=sys.stderr)
                break
            continue
        store.mark_notified(hit["chat_key"], hit["msg_id"])
        sent += 1
    return sent, store.pending_count()


def doctor(verbose: bool = True) -> int:
    """Диагностика окружения без сети и без Telegram: что установлено, заданы ли ключи,
    какой конфиг читается, есть ли сессия. Каждый пункт — PASS/WARN/FAIL и что делать."""
    import importlib
    import platform

    fails: list[str] = []
    warns: list[str] = []

    def report(status: str, text: str, hint: str = "") -> None:
        line = f"{status:4} {text}"
        if hint:
            line += f"\n      → {hint}"
        print(line)

    print(f"Папка: {Path.cwd()}")
    print(f"Система: {platform.system()} {platform.release()}, Python {platform.python_version()}\n")

    nested = find_nested_project(Path.cwd())
    if nested:
        print(f"WARN рядом лежит ещё одна копия проекта: {', '.join(str(n) for n in nested)}")
        print("      → обычно это след распаковки архива «в себя»: файлы и конфиг в копии, "
              "а запускается версия из текущей папки (или наоборот).")
        print("      → сверь путь из строки «конфиг:» ниже с тем файлом, который правишь; "
              "лишнюю копию можно удалить.\n")

    version = sys.version_info
    report("PASS" if version >= (3, 10) else "FAIL", f"Python {version.major}.{version.minor}.{version.micro}",
           "" if version >= (3, 10) else "нужен Python 3.10+: python.org/downloads (галочка Add to PATH)")
    if version < (3, 10):
        fails.append("python")

    for module, required, hint in (
        ("telethon", True, "pip install -r requirements.txt"),
        ("yaml", True, "pip install -r requirements.txt  (или используй sources.json — он уже в архиве)"),
        ("socks", False, "pip install pysocks — нужно только для --proxy"),
    ):
        try:
            mod = importlib.import_module(module)
            report("PASS", f"{module} {getattr(mod, '__version__', '')}".strip())
        except ImportError:
            if required:
                report("FAIL", f"{module} не установлен", hint)
                fails.append(module)
            else:
                report("WARN", f"{module} не установлен (необязателен)")

    env_file = Path(".env")
    if env_file.exists():
        report("PASS", ".env найден — ключи подхватятся автоматически")
    else:
        report("WARN", ".env нет", "скопируй .env.example в .env и впиши свои ключи (см. START-HERE.md, шаг 4)")

    api_id, api_hash = os.getenv("TG_API_ID"), os.getenv("TG_API_HASH")
    if not api_id:
        report("FAIL", "TG_API_ID не задан", "запусти мастер: start.bat --login (спросит ключи и запишет в .env); либо впиши сам: set TG_API_ID=1234567 (cmd) / $env:TG_API_ID=\"1234567\" (PowerShell)")
        fails.append("TG_API_ID")
    elif any(sep in api_id for sep in ",;"):
        report("FAIL", f"TG_API_ID содержит несколько значений: {api_id!r}", "ключи приложения общие для всех аккаунтов — значение одно (App api_id). Аккаунты перечисляются через запятую в TG_SESSION и в accounts: в sources.yaml")
        fails.append("TG_API_ID")
    elif not api_id.strip().isdigit() or len(api_id.strip()) < 5:
        report("FAIL", f"TG_API_ID выглядит обрезанным: {api_id!r}", "нужно полное число из my.telegram.org, например 1234567")
        fails.append("TG_API_ID")
    else:
        report("PASS", f"TG_API_ID задан ({api_id.strip()})")

    if not api_hash:
        report("FAIL", "TG_API_HASH не задан", "запусти мастер: start.bat --login (ключи берутся на my.telegram.org/auth?to=apps); либо впиши сам: set TG_API_HASH=... (cmd)")
        fails.append("TG_API_HASH")
    elif any(sep in api_hash for sep in ",;"):
        report("FAIL", f"TG_API_HASH содержит несколько значений: {api_hash!r}", "ключи приложения общие для всех аккаунтов — значение одно (App api_hash, 32 символа). Через запятую перечисляются сессии в TG_SESSION, а не ключи")
        fails.append("TG_API_HASH")
    elif len(api_hash.strip()) != 32:
        report("FAIL", f"TG_API_HASH обрезан: {len(api_hash.strip())} символов вместо 32",
               "скопируй значение целиком из my.telegram.org → API development tools")
        fails.append("TG_API_HASH")
    else:
        report("PASS", f"TG_API_HASH задан ({api_hash.strip()[:4]}…{api_hash.strip()[-2:]}, 32 символа)")

    for config_name in ("sources.yaml", "sources.json"):
        if Path(config_name).exists():
            break
    try:
        defaults, sources = load_config("sources.yaml")
        if sources:
            order_mode = defaults.get("order", "random")
            report("PASS", f"конфиг: {LAST_CONFIG_PATH} — {len(sources)} источник(ов), "
                           f"порядок обхода: {order_mode}")
            for source in sources:
                print(f"      • {source.target:24} profile={source.profile:5} min_score={source.min_score} catchup={source.catchup}")
            raw_accounts = defaults.get("accounts") or {}
            if raw_accounts:
                print("      аккаунты:")
                for name, item in raw_accounts.items():
                    item = item or {}
                    if not item.get("enabled", True):
                        print(f"        • {name}: выключен (enabled: false)")
                        continue
                    session = str(item.get("session") or f"{name}_session")
                    exists = Path(f"{session}.session").exists()
                    limit = ((item.get("forward") or {}).get("max_per_day")
                             or (defaults.get("forward") or {}).get("max_per_day", "—"))
                    report("PASS" if exists else "WARN",
                           f"{name}: сессия {session}.session"
                           + ("" if exists else " — файла нет, потребуется вход "
                                                f"(start.bat --login-qr --session {session})"),
                           f"лимит пересылок {limit}/сутки")
            fwd = defaults.get("forward") or {}
            if fwd.get("to"):
                print(f"      пересылка: {fwd.get('to')} (режим {fwd.get('mode', 'forward')}, "
                      f"лимит {fwd.get('max_per_day', 100)}/сутки с местной полуночи)")
        else:
            report("WARN", "в конфиге нет источников", "добавь чаты в sources.yaml или запусти с --channels @chat1,@chat2")
            warns.append("sources")
    except SystemExit as exc:
        report("FAIL", f"конфиг не читается: {exc}", "pip install -r requirements.txt")
        fails.append("config")

    session = Path(os.getenv("TG_SESSION", "monitor_session") + ".session")
    if session.exists():
        report("PASS", f"сессия уже есть: {session.name} — логин не потребуется")
    else:
        report("WARN", "сессии нет", "первый запуск спросит телефон и код из Telegram (это нормально, один раз)")

    if os.getenv("TG_BOT_TOKEN") and os.getenv("TG_NOTIFY_CHAT"):
        # здесь проверяется только наличие переменных и вид chat_id, без сети: токен может быть
        # неверным (401). По-настоящему это проверяет --test-notify — там запрос getMe к Telegram.
        notify_chat = os.getenv("TG_NOTIFY_CHAT", "").strip()
        if not re.fullmatch(r"-?\d+", notify_chat):
            report("FAIL", f"TG_NOTIFY_CHAT=«{notify_chat}» — это не числовой id",
                   "получатель уведомлений ОДИН: твой чат с ботом (вид 123456789, для группы/канала — отрицательный). Через запятую перечисляются сессии в TG_SESSION, а не получатели. Узнать id: @userinfobot, либо «Старт» своему боту и getUpdates")
            fails.append("TG_NOTIFY_CHAT")
        else:
            report("PASS", f"TG_BOT_TOKEN и TG_NOTIFY_CHAT заданы ({notify_chat}) — проверка наличия и вида, не отправки",
                   "проверить по-настоящему: start.bat --test-notify --notify bot")
    else:
        report("WARN", "уведомления ботом не настроены (необязательно)",
               "шаг 8 в START-HERE.md; сейчас можно писать в консоль: --notify console")

    print("\nИТОГ:", "можно запускать — все обязательные пункты в порядке"
          if not fails else "исправь FAIL-пункты выше (WARN — по желанию)")
    return 1 if fails else 0


def titles_by_key(sources: list[Source]) -> dict[str, str]:
    """chat_key -> название из конфига: чтобы в отчёте были подписаны даже те чаты,
    где находок пока не было (иначе колонка «название» остаётся пустой)."""
    return {Monitor.chat_key(None, source): source.title for source in sources if source.title}


def apply_limit_windows(store, accounts, now: datetime | None = None) -> list[str]:
    """Авто-обнуление лимита в заданные часы (forward.reset_hours, по времени forward.utc_offset).

    Идея: лимит выбран с утра (до 12:00 по Москве), очередь ждёт — после 16:00 лимит обнуляется
    ещё раз, и радар отправляет следующую порцию. Условия обнуления (все сразу):
      * час уже наступил (сегодня, по времени utc_offset, по умолчанию +3 — Минск/Москва);
      * в этом окне лимит действительно выбран (иначе обнулять нечего) и в очереди есть позиции;
      * после этого часа лимит ещё не обнуляли (ни автоматически, ни вручную).
    Возвращает имена аккаунтов, которым лимит обнулён."""
    now = now or datetime.now(timezone.utc)
    done: list[str] = []
    for acc in accounts:
        fwd = acc.forward or {}
        hours = fwd.get("reset_hours") or []
        if isinstance(hours, (int, float)):
            hours = [hours]
        limit = int(fwd.get("max_per_day", 0) or 0)
        if not hours or not limit:
            continue
        tz = timezone(timedelta(hours=float(fwd.get("utc_offset", 3))))
        local = now.astimezone(tz)
        for hour in sorted(int(h) for h in hours):
            border = local.replace(hour=hour % 24, minute=0, second=0, microsecond=0)
            if local < border:
                continue
            border_utc = border.astimezone(timezone.utc).isoformat(timespec="seconds")
            if store.limit_reset_at(acc.name) >= border_utc:
                continue
            if store.deferred_count(acc.name) <= 0 or store.forwarded_window(acc.name) < limit:
                continue
            count = store.reset_limit(acc.name, auto=True)
            done.append(acc.name)
            print(f"[i] [{acc.name}] авто-обнуление лимита в {hour:02d}:00 (обнулений сегодня: {count}): "
                  f"в очереди {store.deferred_count(acc.name)}, снова {limit} отправок", file=sys.stderr)
            break
    return done


def build_forwarder(client, account: AccountConfig, store, paced, args):
    """Пересылка для конкретного аккаунта: свой получатель, свой лимит, свой счётчик.

    Флаги командной строки (--forward-to / --forward-max-per-day / ...) перекрывают конфиг
    только для первого аккаунта — остальные работают по своим настройкам из accounts.
    """
    fwd = dict(account.forward)
    if args.forward_to:
        fwd["to"] = args.forward_to
    if args.forward_max_per_day is not None:
        fwd["max_per_day"] = args.forward_max_per_day
    if args.forward_mode:
        fwd["mode"] = args.forward_mode
    if args.forward_fallback:
        fwd["fallback"] = args.forward_fallback
    if not fwd.get("to"):
        return None

    from forwarder import Forwarder
    return Forwarder(client, fwd["to"], store, paced,
                     mode=fwd.get("mode", "forward"),
                     max_per_day=int(fwd.get("max_per_day", 100)),
                     fallback=fwd.get("fallback", "link"),
                     dry_run=args.forward_dry_run,
                     account=account.name,
                     queue_ttl_hours=float(fwd.get("queue_ttl_hours", 24) or 0))


# Почему аккаунт не вошёл: {имя аккаунта: (тип ошибки, текст)}. Нужно, чтобы проход мог пропустить
# сломанный аккаунт и честно сообщить об этом в бота, а не умирать целиком.
CONNECT_FAILURES: dict[str, tuple[str, str]] = {}


async def connect_client(client, account: AccountConfig, args) -> None:
    """Вход в Telegram с понятными подсказками вместо трейсбеков."""
    try:
        await client.start()
    except Exception as exc:  # noqa: BLE001
        name = type(exc).__name__
        message = {
            "SessionPasswordNeededError": "Включён облачный пароль (2FA).",
            "PasswordHashInvalidError": "Облачный пароль указан неверно.",
            "PhoneCodeInvalidError": "Код из Telegram неверный или устарел (каждый новый запрос обнуляет предыдущий).",
            "PhoneNumberBannedError": "Этот номер заблокирован Telegram для API-входа.",
            "AuthKeyDuplicatedError": "Файл сессии повреждён (использовался с другого IP).",
        }.get(name, f"{name}: {exc}")
        CONNECT_FAILURES[account.name] = (name, message)
        print(f"[!] Аккаунт «{account.name}» ({account.session}.session): {message}", file=sys.stderr)
        if name in ("SessionPasswordNeededError", "PasswordHashInvalidError", "PhoneCodeInvalidError"):
            print("    Надёжный способ войти без кода и SMS:  start.bat --login-qr", file=sys.stderr)
        elif name == "AuthKeyDuplicatedError":
            print(f"    Удали файл {account.session}.session и войди заново: "
                  f"start.bat --login-qr --session {account.session}", file=sys.stderr)
        sys.exit(1)


# --- что бот знает про аккаунты: кто вошёл, в каких чатах состоит, откуда сессия -----------------
# Всё лежит в bot_state внутри базы (а база — в R2), поэтому панель видит это между проходами.
# Ключи: account:<имя>:me / :login / :chats / :session — значения JSON.

CHAT_STATUS_TEXT = {"ok": "читается", "deferred": "вступление отложено", "pending": "ждёт одобрения админа",
                    "not_member": "аккаунт не в чате", "error": "ошибка", "no_login": "аккаунт не вошёл"}


def session_base(session: str) -> str:
    """'sessions/second_session.session' -> 'second_session'."""
    name = Path(str(session or "")).name
    return name[:-len(".session")] if name.endswith(".session") else name


def _state_json(store, key: str) -> dict:
    try:
        data = json.loads(store.bot_state_get(key) or "{}")
        return data if isinstance(data, dict) else {}
    except (ValueError, TypeError):
        return {}


def _state_put(store, key: str, value: dict) -> None:
    try:
        store.bot_state_set(key, json.dumps(value, ensure_ascii=False))
    except Exception as exc:                                  # noqa: BLE001 - справка не должна ронять проход
        print(f"[!] не записал {key} в базу: {type(exc).__name__}", file=sys.stderr)


def record_login(store, account_name: str, ok: bool, kind: str = "", text: str = "") -> bool:
    """Помнит, вошёл ли аккаунт в этом проходе. True — аккаунт ВОССТАНОВИЛСЯ (до этого не входил)."""
    key = f"account:{account_name}:login"
    before = _state_json(store, key)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    state = {"ok": bool(ok), "ts": now}
    if ok:
        state["since"] = now if before.get("ok") is not True else before.get("since", now)
    else:
        state.update(kind=kind, text=text[:300],
                     since=before.get("since", now) if before.get("ok") is False else now)
    _state_put(store, key, state)
    return bool(ok and before.get("ok") is False)


def record_account_me(store, account_name: str, me) -> None:
    """Telegram-аккаунт под этим именем: id, имя, @username — чтобы в боте не гадать, кто это."""
    _state_put(store, f"account:{account_name}:me", {
        "id": getattr(me, "id", None), "name": display_name(me),
        "username": getattr(me, "username", None) or "",
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds")})


def record_account_chats(store, account_name: str, sources, resolved: dict | None = None,
                         report: dict | None = None, no_login: bool = False) -> None:
    """Чаты аккаунта: цель из конфига, название, Telegram-id чата, статус (/chats в боте).

    no_login — аккаунт не вошёл: берём конфиг, а id и названия оставляем из прошлого прохода."""
    key = f"account:{account_name}:chats"
    previous = {item.get("target"): item for item in _state_json(store, key).get("items", [])
                if isinstance(item, dict)}
    resolved = resolved or {}
    report = report or {}
    items = []
    for source in sources:
        target = source.target
        old = previous.get(target, {})
        entity = resolved.get(target)
        if no_login:
            code, note = "no_login", ""
        else:
            code, note = report.get(target, ("ok", "") if entity is not None else ("error", ""))
        title = (getattr(entity, "title", None) or getattr(entity, "first_name", None)
                 or source.title or old.get("title") or "")
        chat_id = peer_id(entity) if entity is not None else old.get("id")
        items.append({"target": target, "title": str(title), "id": chat_id,
                      "status": code, "note": note})
    _state_put(store, key, {"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                            "items": items})


def import_session_sync(store, accounts, path: str = "session_sync.json") -> dict:
    """Отчёт облачной обёртки о подгрузке сессий по ссылкам -> bot_state (для /accounts).

    Возвращает {имя аккаунта: запись} только для аккаунтов, у которых ссылка задана."""
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    sessions = data.get("sessions") if isinstance(data, dict) else None
    if not isinstance(sessions, dict):
        return {}
    out = {}
    for acc in accounts:
        entry = sessions.get(session_base(acc.session))
        if not isinstance(entry, dict) or entry.get("status") == "skipped":
            continue
        entry = dict(entry, checked=data.get("ts", ""))
        _state_put(store, f"account:{acc.name}:session", entry)
        out[acc.name] = entry
    return out


async def print_topics_of_source(client, acc, source: "Source", entity, paced, args) -> bool:
    """Печатает темы одного источника. True — если темы вообще нашлись."""
    topics = await collect_topics(client, entity, paced, limit=args.topics_depth)
    title = source.title or getattr(entity, "title", source.target)
    if not topics:
        print(f"[i] {source.target} («{title}»): тем нет — обычный чат, читается целиком")
        return False
    chosen = set(source.topics)
    who = f"[{acc.name}] " if acc else ""
    print(f"\n{who}{source.target} («{title}») — тем найдено {len(topics)}"
          + (f", выбраны: {sorted(chosen)}" if chosen else " (сейчас читаются все)"))
    print(f"  {'id темы':>12}  {'сообщений':>9}  название")
    for topic_id, topic_name, count in topics[:25]:
        mark = " ← выбрана" if topic_id in chosen else ""
        print(f"  {topic_id:>12}  {count:>9}  {topic_name}{mark}")
    if len(topics) > 25:
        print(f"  … ещё {len(topics) - 25}")
    print(f"  чтобы читать только нужные темы, добавь в sources.yaml:\n"
          f"    - target: \"{source.target}\"\n      topics: [{topics[0][0]}]")
    return True


def resolve_forward_settings(args, defaults: dict) -> dict:
    """Итоговые настройки пересылки: флаг командной строки → sources.yaml → встроенные значения.

    Раньше у флагов стояли «настоящие» значения по умолчанию (100, forward, link), поэтому
    конфиг игнорировался всегда — в логе было «лимит 100/сутки» при 180 в файле.
    """
    cfg = defaults.get("forward") or {}
    # getattr — чтобы функцию можно было звать и с урезанным набором аргументов (тесты, обёртки)
    limit = getattr(args, "forward_max_per_day", None)
    return {
        "to": getattr(args, "forward_to", None) or cfg.get("to"),
        "mode": getattr(args, "forward_mode", None) or cfg.get("mode", "forward"),
        "max_per_day": int(limit) if limit is not None else int(cfg.get("max_per_day", 100)),
        "fallback": getattr(args, "forward_fallback", None) or cfg.get("fallback", "link"),
    }


def message_counters(store: "HitStore", accounts: list | None = None) -> tuple[int, int]:
    """(прочитано всего, прочитано за последний час) — для метрик и бот-панели.

    «Всего» берётся из таблицы stats (переживает перезапуски), «за час» — из разницы строк
    пульса: радар пишет в пульс, сколько прочитал с прошлого раза.
    """
    total = store.scanned_total()
    hour = 0
    for account in accounts or []:
        name = getattr(account, "name", None) or (account.get("name")
                                                  if isinstance(account, dict) else None)
        if name:
            hour += store.pulse_progress(hours=1.0, account=name)[0]
    if not hour:
        hour = store.pulse_progress(hours=1.0)[0]
    if not hour:
        rows = store.metrics_recent(hours=2)
        if rows:
            hour = int(rows[-1].get("msgs_last_hour") or 0)
    return total, hour


def build_collector(args, store: "HitStore", accounts: list, mode: str, paced: Paced):
    """Сборщик метрик расхода (ТЗ §15.2). None — если метрики выключены."""
    interval = float(getattr(args, "metrics_interval", 0) or 0)
    if interval <= 0:
        return None
    from metrics import MetricsCollector

    return MetricsCollector(
        store, db_path=args.db, mode=mode, accounts=len(accounts) or 1,
        interval_minutes=interval, csv_path=getattr(args, "metrics_csv", "metrics.csv"),
        paced=paced, msgs_provider=lambda: message_counters(store, accounts),
    )


def print_metrics(row: dict) -> None:
    """Короткая строка о расходе в конце прогона — чтобы было видно даже без бота."""
    import metrics as metrics_module

    memory = f"{row.get('rss_mb'):g} МБ" if row.get("rss_mb") is not None else "н/д"
    cpu = f"{row.get('cpu_percent'):g} %" if row.get("cpu_percent") is not None else "н/д"
    text, reason = metrics_module.verdict(rss=row.get("rss_mb"), cpu=row.get("cpu_percent"))
    print(f"[i] расход: память {memory}, процессор {cpu}, сообщений {row.get('msgs_total')}, "
          f"API-вызовов {row.get('api_calls')}, база {row.get('db_mb')} МБ -> metrics.csv",
          file=sys.stderr)
    print(f"[i] вердикт по ресурсам: {text}" + (f" ({reason})" if reason else ""), file=sys.stderr)


async def check_sessions(args, defaults: dict, store: "HitStore", api_id: int,
                         api_hash: str) -> int:
    """Живая проверка сессий: connect + get_me по каждому аккаунту (ТЗ §14.3).

    Допустима ТОЛЬКО когда радар не запущен: второй клиент на тот же .session — это
    AuthKeyDuplicatedError и повторный вход. Поэтому сначала смотрим на пульс в базе.
    Интерактивных запросов (телефон/код) здесь нет намеренно: проверяем, а не входим.
    """
    from bot_panel import BotPanel

    busy = BotPanel.sessions_busy(store, minutes=3.0)
    if busy:
        print(f"[!] Отказ: аккаунт «{busy}» только что слал пульс — похоже, радар работает. "
              f"Живая проверка откроет второй клиент на тот же .session, и Telegram отзовёт ключ "
              f"(AuthKeyDuplicatedError). Сначала останови радар (Ctrl+C в его окне), потом "
              f"повтори: start.bat --check-sessions", file=sys.stderr)
        return 1

    accounts = resolve_accounts(args, defaults)
    code = 0
    for acc in accounts:
        session_file = Path(f"{acc.session}.session")
        if not session_file.exists():
            print(f"[!] {acc.name}: файла {session_file.name} нет, потребуется вход: "
                  f"start.bat --login-qr --session {acc.session}", file=sys.stderr)
            code = 1
            continue
        client = make_client(acc.session, api_id, api_hash, delay=args.delay,
                             proxy=acc.proxy or args.proxy)
        try:
            await client.connect()
            authorized = await client.is_user_authorized()
            me = await client.get_me() if authorized else None
            if authorized and me is not None:
                print(f"[+] {acc.name}: сессия {session_file.name} жива — "
                      f"{display_name(me)} (id={me.id})")
            else:
                print(f"[!] {acc.name}: сессия {session_file.name} не авторизована — нужен вход: "
                      f"start.bat --login-qr --session {acc.session}", file=sys.stderr)
                store.log_error("SessionNotAuthorized", "сессия не авторизована", account=acc.name)
                code = 1
        except Exception as exc:  # noqa: BLE001
            name = type(exc).__name__
            print(f"[!] {acc.name}: {name}: {exc}", file=sys.stderr)
            if name == "AuthKeyDuplicatedError":
                print(f"    Удали {session_file.name} и войди заново: "
                      f"start.bat --login-qr --session {acc.session}", file=sys.stderr)
            store.log_error(name, str(exc)[:300], account=acc.name)
            code = 1
        finally:
            try:
                await client.disconnect()
            except Exception:  # noqa: BLE001
                pass
    if code == 0:
        print("[+] Все сессии живы: радар можно запускать (start.bat --bot-panel)")
    return code


# --- диагностика источников: «вступить», «не найден», «не смотрит» ---------------
#
# Три разные беды выглядят одинаково — «находок нет». Проверка разводит их:
#   * join    — аккаунт не состоит в чате: радар до чата просто не доходит, нужен вход в чат;
#   * missing — имя набрано с опечаткой либо чат удалён;
#   * quiet   — чат читается, но прочитано 0 сообщений: не привязан к аккаунту, выключен,
#               отсекается профилем/порогом или в чате давно nothing не пишут.
# Проверка живая (подключается к Telegram), но в базу не пишет и ничего не пересылает.

VERDICT_LABEL = {
    "ok": "читается", "join": "НУЖНО ВСТУПИТЬ", "missing": "НЕ НАЙДЕН",
    "expired": "ССЫЛКА ИСТЕКЛА", "flood": "ПАУЗА ОТ TELEGRAM", "error": "ОШИБКА",
    "empty": "ПУСТО",
}

# Подстроки в тексте ошибки Telegram. Типы исключений не используем: тексты стабильнее,
# и разбор можно проверить тестом, не поднимая Telethon.
# Telethon дублирует смысл и словами, и в snake_case — ловим оба вида
_JOIN_HINTS = ("not part of", "cannot get entity", "channel_private", "channel is private",
               "channel specified is private", "private channel", "user_not_participant",
               "participant_id_invalid", "chat_admin_required")
# Telethon пишет эти ошибки и словами, и в snake_case — ловим оба вида
_MISSING_HINTS = ("username not occupied", "username_not_occupied", "username invalid", "username is invalid",
                  "username_invalid", "username_is_invalid", "no user has", "nobody is using", "there is no")
_FLOOD_HINTS = ("flood", "wait of", "too many requests")
_EXPIRED_HINTS = ("expired", "истек")


def classify_source_error(text: str, target: str = "") -> tuple[str, str]:
    """Вердикт и совет по тексту ошибки Telegram. target нужен, чтобы отличить ссылку-приглашение."""
    low = (text or "").lower()
    if any(hint in low for hint in _FLOOD_HINTS):
        return "flood", "Telegram просит подождать — лимит на запросы, повтори позже"
    if any(hint in low for hint in _EXPIRED_HINTS):
        return "expired", "ссылка-приглашение истекла: попроси свежую у администратора чата"
    if any(hint in low for hint in _MISSING_HINTS):
        return "missing", "такого имени нет: проверь написание (или чат удалён)"
    if any(hint in low for hint in _JOIN_HINTS):
        if invite_hash(target):
            return "join", "аккаунт не в чате: вступи вручную или запусти радар с --auto-join"
        return "join", ("аккаунт не состоит в чате: вступи с него вручную "
                        "(приватный чат без ссылки-приглашения — только так)")
    return "error", "неожиданная ошибка — смотри текст ниже"


def source_chat_key(source: "Source") -> str:
    """Ключ чата в базе: @username без @ в нижнем регистре; для ссылок — invite:<hash>."""
    raw = source.target.strip()
    if "t.me/+" in raw or "joinchat/" in raw or raw.startswith("+"):
        invite = raw.rstrip("/").split("/")[-1].lstrip("+")
        return f"invite:{invite}"
    return raw.lstrip("@").rstrip("/").split("/")[-1].lower()


def diagnose_sources_config(sources: list, accounts: list, buckets: dict,
                            store=None) -> list[str]:
    """Замечания по конфигу без сети: выключенное, дубли, без привязки, пропавшее из списка."""
    notes: list[str] = []
    disabled = [s.target for s in sources if not s.enabled]
    if disabled:
        notes.append(f"[i] выключено в конфиге (enabled: false) — радар их не читает: "
                     f"{', '.join(disabled)}")

    counts: dict[str, int] = {}
    for source in sources:
        key = source_chat_key(source)
        counts[key] = counts.get(key, 0) + 1
    duplicates = sorted(key for key, count in counts.items() if count > 1)
    if duplicates:
        notes.append(f"[!] чат указан дважды — читаться будет дважды, лимиты тоже: "
                     f"{', '.join(duplicates)}")

    if len(accounts) > 1:
        unassigned = [s.target for s in sources if not (s.account or "").strip()]
        if unassigned:
            notes.append(f"[i] без account: {len(unassigned)} шт. — достаются первому аккаунту "
                         f"«{accounts[0].name}»")

    for name, bucket in buckets.items():
        if not bucket:
            notes.append(f"[!] аккаунту «{name}» не назначен ни один чат — он простаивает")

    if store is not None:
        keys = {source_chat_key(s) for s in sources}
        stale = sorted(key for key in (store.scanned_by_chat() or {}) if key and key not in keys)
        if stale:
            notes.append(f"[i] читались раньше, но сейчас их нет в конфиге: {', '.join(stale)}")
    return notes


async def probe_source(client, source: "Source", paced) -> tuple[str, str]:
    """Один источник: (вердикт, подробность). Базу не трогает, ничего не пересылает."""
    try:
        entity = await call(lambda: client.get_entity(source.target), paced,
                            label=f"get_entity({source.target})")
    except Exception as exc:                                        # noqa: BLE001
        return classify_source_error(f"{type(exc).__name__}: {exc}", source.target)
    try:
        messages = await call(lambda: client.get_messages(entity, limit=1), paced,
                              label=f"get_messages({source.target})")
    except Exception as exc:                                        # noqa: BLE001
        return classify_source_error(f"{type(exc).__name__}: {exc}", source.target)
    if not messages:
        return "empty", ("чат доступен, но сообщений не видно: пустой чат либо история закрыта "
                         "для новых участников")
    stamp = getattr(messages[0], "date", None)
    return "ok", ("последнее сообщение " + stamp.strftime("%d.%m %H:%M") if stamp
                  else "сообщения есть")


async def check_sources(args, defaults: dict, sources: list, store, api_id: int,
                        api_hash: str) -> int:
    """Живая проверка источников: что читается, куда вступить, где радар «не смотрит».

    Открывает по клиенту на каждый аккаунт, поэтому допустима ТОЛЬКО когда радар остановлен
    (второй клиент на тот же .session = AuthKeyDuplicatedError). В облаке это единственный
    клиент — там проверку запускает эндпоинт /sources-check внутри контейнера с --force.
    """
    from bot_panel import BotPanel

    if not getattr(args, "force", False):
        busy = BotPanel.sessions_busy(store, minutes=3.0)
        if busy:
            print(f"[!] Отказ: аккаунт «{busy}» только что слал пульс — похоже, радар работает. "
                  f"Живая проверка откроет второй клиент на тот же .session, и Telegram отзовёт "
                  f"ключ (AuthKeyDuplicatedError). Останови радар (Ctrl+C в его окне), потом "
                  f"повтори: start.bat --check-sources", file=sys.stderr)
            return 1

    accounts = resolve_accounts(args, defaults)
    buckets = sources_for_account(sources, accounts)
    print("[i] проверка источников: подключаюсь к каждому аккаунту, базу не меняю")

    for note in diagnose_sources_config(sources, accounts, buckets, store):
        print(note)
    if not sources:
        print("[!] в конфиге нет ни одного источника — проверять нечего")
        return 1

    paced = Paced(getattr(args, "delay", 2.0))
    scanned = store.scanned_by_chat() or {}
    matched = store.matched_by_chat() or {}
    last_hit = store.last_hit_at_by_chat() or {}
    verdicts: dict[str, int] = {}
    code = 0

    for acc in accounts:
        own = buckets.get(acc.name, [])
        session_file = Path(f"{acc.session}.session")
        print(f"\n── {acc.name} · {session_file.name} · чатов {len(own)} ──")
        if not session_file.exists():
            print(f"    [!] файла {session_file.name} нет: "
                  f"start.bat --login-qr --session {acc.session}")
            code = 1
            continue
        client = make_client(acc.session, api_id, api_hash, delay=args.delay,
                             proxy=acc.proxy or args.proxy)
        try:
            await client.connect()
            me = await client.get_me() if await client.is_user_authorized() else None
            if me is None:
                print(f"    [!] сессия не авторизована: "
                      f"start.bat --login-qr --session {acc.session}")
                code = 1
                continue
            print(f"    [+] {display_name(me)} (id={me.id})")
            for source in own:
                verdict, detail = await probe_source(client, source, paced)
                verdicts[verdict] = verdicts.get(verdict, 0) + 1
                key = source_chat_key(source)
                read = int(scanned.get(key, 0))
                found = int(matched.get(key, 0))
                hit = last_hit.get(key, "")
                stats = (f" · прочитано {read}, найдено {found}"
                         + (f", последняя находка {hit[:16].replace('T', ' ')}" if hit else ""))
                label = VERDICT_LABEL.get(verdict, verdict)
                print(f"    {source.target[:42]:42} {label:16} {detail}{stats if verdict == 'ok' else ''}")
                if verdict == "ok" and read == 0:
                    verdicts["quiet"] = verdicts.get("quiet", 0) + 1
                    print(f"    {'':42} └─ сюда радар не смотрел: чат новый, либо не тот аккаунт, "
                          f"либо отсекается профилем «{source.profile}» с порогом {source.min_score}")
                elif verdict != "ok":
                    code = 1
        except Exception as exc:                                    # noqa: BLE001
            print(f"    [!] {type(exc).__name__}: {exc}", file=sys.stderr)
            store.log_error(type(exc).__name__, str(exc)[:300], account=acc.name)
            code = 1
        finally:
            try:
                await client.disconnect()
            except Exception:                                       # noqa: BLE001
                pass

    if verdicts:
        parts = [f"{VERDICT_LABEL.get(v, v).lower()} — {n}" for v, n in sorted(verdicts.items())]
        print("\nИТОГ: " + ", ".join(parts))
    todo = verdicts.get("join", 0) + verdicts.get("missing", 0) + verdicts.get("expired", 0)
    if todo:
        print(f"[i] нужно вступить или исправить: {todo} из {sum(verdicts.values())} источников")
    if verdicts.get("quiet"):
        print(f"[i] читаются, но по ним 0 прочитанных сообщений: {verdicts['quiet']} — "
              f"проверь account: у источника и порог min_score")
    return code


class ServiceNotify:
    """Служебные сообщения ботом в TG_NOTIFY_CHAT: как радар поработал, что упало.

    Находки сюда НЕ приходят: они уходят пересылкой самого аккаунта получателю из
    sources.yaml (forward.to — обычно @parcel_transfer_bot). Здесь — другое: сводка
    прохода (прочитано/найдено/переслано), ошибки, падение прохода, подозрительно
    долгая пауза между проходами. Иначе «работает ли радар» видно только по логам.

    Антиспам: время последней отправки лежит в bot_state внутри базы, а база — в R2.
    Контейнер в облаке живёт один проход, поэтому счётчик обязан переживать перезапуск,
    иначе сводка приходила бы каждые 10 минут.
    """

    def __init__(self, store, mode: str = "auto", every_hours: float = 6.0):
        self.store = store
        self.every = max(float(every_hours or 0), 0.1)
        self.token = (os.getenv("TG_BOT_TOKEN") or "").strip()
        self.chat = (os.getenv("TG_NOTIFY_CHAT") or "").strip()
        if mode == "auto":
            self.enabled = bool(self.token and self.chat)
        else:
            self.enabled = (mode == "bot") and bool(self.token and self.chat)
        self.sent: int = 0
        self.last_error: str = ""

    def why_disabled(self) -> str:
        if self.enabled:
            return ""
        if not self.token and not self.chat:
            return "не заданы TG_BOT_TOKEN и TG_NOTIFY_CHAT"
        if not self.token:
            return "не задан TG_BOT_TOKEN"
        if not self.chat:
            return "не задан TG_NOTIFY_CHAT"
        return "служебные сообщения выключены (--service-notify none)"

    def _due(self, key: str) -> bool:
        """Пора ли слать: с прошлого раза прошло больше every часов (или не было ни разу)."""
        raw = self.store.bot_state_get(f"service:{key}")
        if not raw:
            return True
        try:
            last = datetime.fromisoformat(raw)
        except ValueError:
            return True
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - last).total_seconds() >= self.every * 3600.0

    async def send(self, text: str, key: str = "summary", force: bool = False) -> bool:
        """Отправить служебное сообщение. False — молча пропустили (антиспам) или не ушло."""
        if not self.enabled or not text.strip():
            return False
        if not force and not self._due(key):
            return False
        from core_telegram import bot_send_text

        error, fatal = await bot_send_text(self.token, self.chat, text)
        if error:
            self.last_error = error
            print(f"[!] служебное сообщение не ушло: {error}", file=sys.stderr)
            if fatal:
                self.enabled = False      # 401/403: не повторяем до конца прогона
            return False
        self.store.bot_state_set(
            f"service:{key}", datetime.now(timezone.utc).isoformat(timespec="seconds"))
        self.sent += 1
        return True

    @staticmethod
    def pass_summary(now: datetime, seconds: float, read: int, found: int, forwarded: int,
                     per_account: list, errors: int, db_total: str,
                     gap_minutes: float | None = None, expected_minutes: float = 0.0,
                     leftover: list | None = None, seen_before: int | None = None) -> str:
        """Текст сводки прохода. Коротко: чтобы читалось с телефона за пару секунд."""
        lines = [f"Проход {now.strftime('%d.%m %H:%M')} UTC, {seconds:.0f} с",
                 f"• новых сообщений {read}, найдено {found}, переслано {forwarded}"]
        if seen_before:
            lines.append(f"• уже видели {seen_before} — повторно не смотрим")
        for name, acc_read, acc_found in per_account:
            lines.append(f"• {name}: новых {acc_read}, найдено {acc_found}")
        lines.append(f"• ошибок за сутки: {errors}")
        lines.append(f"• в базе: {db_total}")
        if gap_minutes is not None and expected_minutes and gap_minutes > expected_minutes * 2:
            lines.append(f"• предыдущий проход был {gap_minutes:.0f} мин назад "
                         f"(обычно {expected_minutes:.0f}) — радар простаивал")
        if leftover:
            lines.append(f"• не успел прочитать {len(leftover)} чатов: время прохода вышло — "
                         f"дойдут в следующий раз (порядок чатов случайный)")
        return "\n".join(lines)

    @staticmethod
    def failure_text(exc: BaseException) -> str:
        return f"Проход упал: {type(exc).__name__}: {exc}"[:400]


def rotate_runners(runners: list, store) -> list:
    """Порядок аккаунтов на этот проход: первым идёт тот, кто в прошлый раз был последним.

    Аккаунты читаются по очереди, а бюджет прохода обычно кончается на первом из них —
    без ротации второй аккаунт не читался бы никогда. Порядок помним в базе (она в R2),
    поэтому перезапуск контейнера, который в схеме B происходит каждый проход, его не
    сбрасывает: иначе «ротация» давала бы один и тот же порядок.
    """
    if len(runners) < 2:
        return runners
    last = store.bot_state_get("pass:last_account") or ""
    names = [acc.name for acc, _client, _monitor in runners]
    # Начинает тот, кто в прошлый раз шёл последним: именно он не успел почитать, потому
    # что бюджет кончился. Сдвиг на «следующий после последнего» оставил бы порядок тем же.
    start = names.index(last) if last in names else 0
    rotated = runners[start:] + runners[:start]
    store.bot_state_set("pass:last_account", rotated[-1][0].name)
    return rotated


async def answer_pending_commands(store, accounts, stats_file: str = "stats.txt",
                                  transport=None) -> int:
    """Раз в проход забираем команды хозяина из Telegram и отвечаем на них.

    В схеме B отдельной панели нет — контейнер живёт ровно один проход, — поэтому
    команды разбираются в конце прохода: отвечаем на всё, что накопилось с прошлого раза.
    Задержка — до интервала cron (10 мин), зато без отдельного процесса, без второго
    клиента на ту же сессию и без лишних денег: это 2-3 вызова Bot API за проход.

    Обработчики команд уже написаны в bot_panel (16 штук: /status, /last, /sources,
    /limits, /errors, /cost…), поэтому здесь только доставка апдейтов до них.
    """
    from bot_panel import BotPanel

    panel = BotPanel(store, accounts=accounts, mode="B", panel_only=True,
                     stats_file=stats_file, transport=transport)
    ok, why = panel.ready()
    if not ok:
        print(f"[i] команды из Telegram недоступны: {why}", file=sys.stderr)
        return 0
    # меню бота: без setMyCommands команды работают, но в Telegram их не видно
    try:
        menu_ok, menu_why = await panel.register_commands()
        if menu_ok:
            print("[i] меню команд бота обновлено", file=sys.stderr)
        elif menu_why:
            print(f"[!] меню команд не обновилось: {menu_why}", file=sys.stderr)
    except Exception as exc:                                      # не роняем проход из-за меню
        print(f"[!] меню команд не обновилось: {type(exc).__name__}: {exc}", file=sys.stderr)
    if panel.offset <= 0:
        # первый раз: сбрасываем накопившееся, чтобы не отвечать на старые команды
        try:
            stale = await panel.transport.get_updates(-1, timeout=1)
            if stale:
                panel.offset = int(stale[-1].get("update_id", 0)) + 1
                panel._save_offset()
        except Exception as exc:                                  # noqa: BLE001
            print(f"[!] старые апдейты не сбросились: {exc}", file=sys.stderr)
    updates = await panel.transport.get_updates(panel.offset, timeout=0)
    answers = await panel.handle_updates(updates or [])
    return len(answers)


def env_flag(name: str, default: bool = True) -> bool:
    """Выключатель из переменных воркера: 1 — включено, 0 — выключено.

    Переменные правятся в дашборде (Workers & Pages → бот → Settings → Variables),
    деплой для этого не нужен: контейнер читает их при старте, поэтому после смены
    значения его надо остановить (GET /restart). Всё, что не 0 и не 1 — пусто,
    опечатка, «yes» — считаем как default: радар работает, пока его явно не выключили.
    """
    raw = (os.getenv(name) or "").strip().lower()
    if raw in ("1", "on", "true", "yes", "да", "вкл"):
        return True
    if raw in ("0", "off", "false", "no", "нет", "выкл"):
        return False
    return default


async def handle_tg_commands(args, store, accounts, label: str = "") -> int:
    """Разбор команд из Telegram с понятным логом; ошибка бота проход не роняет."""
    if not getattr(args, "once", False):
        return 0
    if not tg_commands_enabled(args):
        # молчание выглядит как поломка: пишем причину, а не просто ничего
        if label != "в конце прохода":
            print(f"[i] команды из Telegram выключены: {tg_commands_reason(args)}", file=sys.stderr)
        return 0
    try:
        answered = await answer_pending_commands(store, accounts, stats_file=args.stats_file)
        if answered:
            print(f"[i] ответили на команд из Telegram ({label}): {answered}", file=sys.stderr)
        return answered
    except Exception as exc:                                  # не роняем проход из-за бота
        print(f"[!] команды из Telegram не обработаны: {type(exc).__name__}: {exc}",
              file=sys.stderr)
        store.log_error("TgCommands", f"{type(exc).__name__}: {exc}"[:300])
        return 0


def tg_commands_enabled(args) -> bool:
    """Нужно ли разбирать команды из Telegram в конце прохода.

    Три уровня: флаг --tg-commands (явный), переменная воркера TG_COMMANDS (0/1)
    и старое правило «есть токен и чат — значит включено».
    """
    mode = getattr(args, "tg_commands", "auto") or "auto"
    if mode == "off":
        return False
    if mode == "on":
        return True
    if not env_flag("TG_COMMANDS", True):
        return False
    return bool(os.getenv("TG_BOT_TOKEN") and os.getenv("TG_NOTIFY_CHAT"))


def tg_commands_reason(args) -> str:
    """Почему команды из Telegram выключены.

    Молчание выглядит как поломка: эту строку печатаем в лог, чтобы по /status
    было видно, что радар их не отвечает по причине, а не «потому что сломался».
    """
    mode = getattr(args, "tg_commands", "auto") or "auto"
    if mode == "off":
        return "сняты флагом --tg-commands off"
    if not env_flag("TG_COMMANDS", True):
        return "переменная TG_COMMANDS=0 — включить: Variables → TG_COMMANDS=1 и /restart"
    missing = [name for name in ("TG_BOT_TOKEN", "TG_NOTIFY_CHAT") if not os.getenv(name)]
    if missing:
        return (f"не заданы {', '.join(missing)} — нужны секреты воркера "
                f"(npx wrangler secret put {missing[0]})")
    return "не задан --tg-commands, а переменные найдены"


def build_marker() -> str:
    """Метка сборки: 8 символов sha256 по исходнику monitor.py.

    DEPLOY.md: после деплоя контейнер может ещё долго крутить старый образ —
    приложение обновлено, а живой инстанс нет. По этой метке в хвосте лога
    (/status, /log) видно, какой код реально работает в облаке. Считается от
    файла, поэтому править её при каждом релизе не нужно.
    """
    import hashlib
    try:
        return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:8]
    except Exception:                                             # noqa: BLE001
        return "?"


def resolve_mode(args) -> str:
    """Схема работы для отчётов панели: A — постоянный слушатель, B — проходы по расписанию.

    --mode задаёт схему явно; иначе она следует из способа запуска: --once (проход из
    планировщика) — это B, живой режим — A (ТЗ §16.1).
    """
    explicit = getattr(args, "mode", None)
    if explicit:
        return str(explicit).upper()
    return "B" if getattr(args, "once", False) else "A"


def build_panel(args, store: "HitStore", accounts: list, buckets: dict, sources: list[Source],
                mode: str, heartbeat_minutes: float, paced: Paced | None = None):
    """Бот-панель для живого режима (ТЗ §14.4, режим 1). None — если запускать нечего."""
    from bot_panel import AccountView, BotPanel

    views = [AccountView.from_config(acc, len(buckets.get(acc.name, []))) for acc in accounts]
    panel = BotPanel(store, accounts=views, titles=titles_by_key(sources), mode=mode,
                     heartbeat_minutes=heartbeat_minutes, alert_silent_minutes=args.alert_silent,
                     digest=(None if args.digest is None else args.digest == "on"),
                     stats_file=args.stats_file, stats_days=args.stats_days, db_path=args.db,
                     api_calls_counter=paced)          # счётчик API-вызовов для /usage
    ok, why = panel.ready()
    if not ok:
        print(f"[!] бот-панель не запущена: {why}", file=sys.stderr)
        return None
    print(f"[+] бот-панель включена: команды принимает chat_id={panel.owner_chat} "
          f"(команды: /help, /status, /accounts, /usage)", file=sys.stderr)
    return panel


async def async_main(args) -> None:
    if args.doctor:
        sys.exit(doctor())

    if args.panel_only:
        # панель отдельным процессом: ни Telethon, ни ключей аккаунта не нужно (ТЗ §14.4, режим 2)
        from bot_panel import panel_only_main

        sys.exit(await panel_only_main(args))

    defaults, sources = load_config(args.config)

    if args.channels:                      # быстрый запуск без конфига
        for chunk in args.channels.split(","):
            target = chunk.strip()
            if target:
                sources.append(Source(target=target, profile=args.profile,
                                      min_score=args.min_score, catchup=args.catchup))
    for source in sources:                 # CLI перекрывает конфиг
        if args.min_score is not None:
            source.min_score = args.min_score
        if args.catchup is not None:
            source.catchup = args.catchup
        if args.profile:
            source.profile = args.profile

    order_mode = args.order or defaults.get("order", "random")
    sources = order_sources(sources, order_mode)

    if not sources:
        sys.exit("Нет источников: заполни sources.yaml или передай --channels @chat1,@chat2")

    notifier = build_notifier(args.notify, args.notify_file, explain=args.explain)

    if args.reset_db:
        path = Path(args.db)
        if path.exists():
            path.unlink()
            print(f"[+] База {args.db} удалена: при следующем запуске всё соберётся заново "
                  "(правила и категории применятся текущие). Сессия и .env не тронуты.")
        else:
            print(f"[i] Файла {args.db} и так нет.")
        return

    store = HitStore(args.db)

    # FloodWait из core_telegram.call падает в базу: панель покажет «ограничение до 22:10» (ТЗ §7.3)
    def flood_to_db(label: str, seconds: float, wait: float) -> None:
        until = (datetime.now(timezone.utc) + timedelta(seconds=wait)).astimezone().strftime("%H:%M")
        try:
            store.log_heartbeat("flood", detail=f"{label}: до {until}")
            store.log_error("FloodWaitError", f"{label}: Telegram просит {int(seconds)} с, ждём до {until}")
        except Exception as exc:  # noqa: BLE001
            print(f"[!] не записал FloodWait в базу: {type(exc).__name__}", file=sys.stderr)

    add_flood_hook(flood_to_db)

    # ретеншн: heartbeats/metrics/errors старше 30 дней не храним, чтобы база не пухла (ТЗ §7.2)
    removed = store.retention_cleanup(days=30)
    if any(removed.values()):
        print(f"[i] ретеншн: чищены записи старше 30 дней — "
              + ", ".join(f"{table} {count}" for table, count in removed.items() if count),
              file=sys.stderr)

    if args.show_stats or args.stats_only:
        report = store.stats_report(days=args.stats_days, titles_from_config=titles_by_key(sources))
        print(report)
        if args.stats_file:
            Path(args.stats_file).write_text(report, encoding="utf-8")
            store.stats_csv(Path(args.stats_file).with_suffix(".csv").name)
            print(f"[+] статистика записана в {args.stats_file}")
        return

    if args.export:
        if args.export.lower().endswith(".txt"):
            count = store.export_txt(args.export, hours=args.export_hours)
            print(f"[+] {count} совпадений -> {args.export} (обычный текст, открывается в Блокноте)")
        else:
            count = store.export_csv(args.export)
            print(f"[+] {count} строк -> {args.export} (CSV для Excel)")
        return

    if args.list_topics:
        load_dotenv()
        api_id = args.api_id or (int(os.getenv("TG_API_ID")) if os.getenv("TG_API_ID") else None)
        api_hash = args.api_hash or os.getenv("TG_API_HASH")
        if not api_id or not api_hash:
            sys.exit("Нужны TG_API_ID и TG_API_HASH (или --api-id/--api-hash). "
                 "Проще всего: start.bat --login — мастер спросит ключи и запишет их в .env. "
                 "Ключи приложения берутся на https://my.telegram.org/auth?to=apps (название любое)")
        paced = Paced(args.delay)
        accounts = resolve_accounts(args, defaults)
        buckets = sources_for_account(sources, accounts)
        found_any = False
        for acc in accounts:
            acc_sources = buckets[acc.name]
            if not acc_sources:
                continue
            client = make_client(acc.session, api_id, api_hash, delay=args.delay,
                                 proxy=acc.proxy or args.proxy)
            await connect_client(client, acc, args)
            me = await client.get_me()
            who = f"[{acc.name}] " if len(accounts) > 1 else ""
            print(f"[+] {who}вошли как {display_name(me)} (id={me.id})", file=sys.stderr)
            resolved = await resolve_targets(client, [x.target for x in acc_sources], paced,
                                            auto_join=args.auto_join)
            for source in acc_sources:
                entity = resolved.get(source.target)
                if entity is None:
                    print(f"[!] {source.target}: недоступен для аккаунта «{acc.name}» "
                          f"(нет подписки?)", file=sys.stderr)
                    continue
                if await print_topics_of_source(client, acc, source, entity, paced, args):
                    found_any = True
            await client.disconnect()
        if not found_any:
            print("\n[i] Тем нигде не нашлось: скорее всего среди источников нет форум-чатов "
                  "(или хвост слишком короткий — попробуй --topics-depth 1000)")
        return

    if args.resend_pending is not None:
        if args.notify in ("bot", "both"):
            from core_telegram import bot_get_me
            token = os.getenv("TG_BOT_TOKEN")
            chat = os.getenv("TG_NOTIFY_CHAT")
            if not (token and chat):
                print("[!] Не заданы TG_BOT_TOKEN / TG_NOTIFY_CHAT — догонять нечем.", file=sys.stderr)
                print(f"    {BOT_TOKEN_HINT}", file=sys.stderr)
                sys.exit(1)
            ok, info = await bot_get_me(token)
            print(f"[{'i' if ok else '!'}] проверка токена: {info}", file=sys.stderr)
            if not ok:
                print(f"    {BOT_TOKEN_HINT}", file=sys.stderr)
                sys.exit(1)
        left = store.pending_count()
        if not left:
            print("[i] неотправленных уведомлений нет — догонять нечего")
            return
        print(f"[i] догоняю уведомления: всего неотправленных {left}, беру до {args.resend_pending}")
        sent, remaining = await resend_pending(store, notifier, args.resend_pending)
        print(f"[+] отправлено {sent}, осталось неотправленных {remaining}"
              + (f" (запусти снова, чтобы продолжить)" if remaining else ""))
        return

    if args.test_notify:
        if args.notify in ("bot", "both"):
            from core_telegram import bot_get_me
            token = os.getenv("TG_BOT_TOKEN")
            if token:
                ok, info = await bot_get_me(token)
                print(f"[{'i' if ok else '!'}] проверка токена: {info}", file=sys.stderr)
                if not ok:
                    print(f"    {BOT_TOKEN_HINT}", file=sys.stderr)
                    sys.exit(1)
            else:
                print("[!] TG_BOT_TOKEN не задан — боту отправлять нечем.", file=sys.stderr)
                print(f"    {BOT_TOKEN_HINT}", file=sys.stderr)
                sys.exit(1)
        sample = {
            "chat_title": args.channels or "@test_chat", "msg_id": 12345,
            "date": datetime.now(timezone.utc).isoformat(),
            "category": "parcel", "intent": "offer", "direction": "BY->PL", "score": 12,
            "text": "Возьму посылку из Минска в Варшаву, еду 20.09, есть 2 места в машине",
            "link": "https://t.me/travelersminsk/91532", "hits": ["посылка", "возьму", "есть место"],
        }
        delivered = await notifier(sample)
        if delivered is False:
            print("[!] Тестовое уведомление НЕ ушло — смотри причину выше.", file=sys.stderr)
            print(f"    {BOT_TOKEN_HINT}", file=sys.stderr)
            sys.exit(1)
        print("[+] тестовое уведомление отправлено (проверь выбранный канал)")
        print(f"[i] уведомления ботом: {os.getenv('TG_NOTIFY_CHAT') or '—'} | "
              f"пересылка находок работает независимо от этого канала")
        return

    api_id = args.api_id or (int(os.getenv("TG_API_ID")) if os.getenv("TG_API_ID") else None)
    api_hash = args.api_hash or os.getenv("TG_API_HASH")
    if not api_id or not api_hash:
        sys.exit("Нужны TG_API_ID и TG_API_HASH (или --api-id/--api-hash). "
                 "Проще всего: start.bat --login — мастер спросит ключи и запишет их в .env. "
                 "Ключи приложения берутся на https://my.telegram.org/auth?to=apps (название любое)")

    if args.check_sessions:
        sys.exit(await check_sessions(args, defaults, store, api_id, api_hash))

    if args.check_sources:
        sys.exit(await check_sources(args, defaults, sources, store, api_id, api_hash))

    paced = Paced(args.delay)
    accounts = resolve_accounts(args, defaults)
    buckets = sources_for_account(sources, accounts)
    multi = len(accounts) > 1
    panel_mode = resolve_mode(args)

    if multi:
        print(f"[i] аккаунтов в работе: {len(accounts)}", file=sys.stderr)
        for acc in accounts:
            limit = acc.forward.get("max_per_day", 100)
            print(f"    • {acc.name}: сессия {acc.session}.session, чатов {len(buckets[acc.name])}, "
                  f"лимит пересылок {limit if limit else '∞'}/сутки", file=sys.stderr)

    keep_hidden = args.keep_hidden or not defaults.get("skip_hidden", True)
    if not keep_hidden:
        print("[i] авторы со скрытым профилем («hidden by user») пропускаются: написать им нельзя "
              "(оставить их: --keep-hidden или skip_hidden: false в конфиге)", file=sys.stderr)

    pending_before = store.pending_count()
    if pending_before and args.notify != "none":
        print(f"[i] в базе {pending_before} неотправленных уведомлений (прошлые сбои). "
              f"Догнать: start.bat --resend-pending 50 --notify {args.notify}", file=sys.stderr)

    heartbeat_minutes = (args.heartbeat if args.heartbeat is not None
                         else float(defaults.get("heartbeat", 0) or 0))
    if args.bot_panel and heartbeat_minutes <= 0:
        heartbeat_minutes = 15.0
        print("[i] для бот-панели включён пульс каждые 15 мин: без него панель не видит, жив ли "
              "аккаунт. Отключить: --heartbeat 0", file=sys.stderr)

    # Бюджет прохода: в облаке RADAR_TIMEOUT обрывает процесс снаружи, а оборванный проход
    # не успевает записать ни пульс, ни очередь. Поэтому радар сам следит за временем.
    budget = args.pass_budget
    if budget <= 0:
        env_timeout = float(os.getenv("RADAR_TIMEOUT") or 0)
        if env_timeout > 0:
            budget = env_timeout * 0.75          # четвёртая часть — на запись в R2 и выход
    deadline = time.monotonic() + budget if budget > 0 else None
    if args.once and budget > 0:
        print(f"[i] бюджет прохода: {budget:.0f} с (RADAR_TIMEOUT={os.getenv('RADAR_TIMEOUT', 'нет')}), "
              f"лимит ожидания FloodWait {min(args.flood_wait_limit or budget * 0.25, budget):.0f} с",
              file=sys.stderr)

    # выключатель радара: RADAR_ON=0 в Variables воркера — и проходы встают, без деплоя
    if not env_flag("RADAR_ON", True):
        print("[i] радар выключен переменной RADAR_ON=0 — проход пропущен. "
              "Включить: Variables → RADAR_ON=1", file=sys.stderr)
        return
    if args.once:
        print(f"[i] выключатели: RADAR_ON=1, TG_COMMANDS={int(tg_commands_enabled(args))} "
              f"(правятся в Variables воркера, 0 — выкл, 1 — вкл)", file=sys.stderr)

    # служебные сообщения — в TG_NOTIFY_CHAT (сводка прохода, падения). Находки идут
    # отдельно: пересылкой аккаунта получателю из sources.yaml (forward.to).
    service = ServiceNotify(store, mode=args.service_notify, every_hours=args.service_every)
    if not service.enabled:
        print(f"[i] служебные сообщения выключены: {service.why_disabled()}", file=sys.stderr)

    # Команды бота — ПЕРВЫМ делом: до входа в аккаунты и чтения чатов. Раньше их разбирали только
    # в конце прохода, и если проход обрывался или аккаунт не входил, бот молчал на /status
    # и /version часами. Так ответ приходит в течение одного интервала cron в любом случае.
    if args.once:
        await handle_tg_commands(args, store, accounts, label="в начале прохода")

    # Сессии, подгруженные облачной обёрткой по ссылке (Google Диск): запоминаем для /accounts
    # и сообщаем в бот, когда поставлена новая версия или ссылка перестала работать.
    synced = import_session_sync(store, accounts)
    for acc_name, entry in synced.items():
        if entry.get("status") == "imported":
            await service.send(
                f"🔄 Сессия аккаунта «{acc_name}» подгружена с Google Диска (версия "
                f"{entry.get('sha', '?')}) и уже стоит в работе. Если вход пройдёт — следующим "
                f"сообщением придёт подтверждение; если нет — причина в /accounts.",
                key=f"session_imported:{acc_name}:{entry.get('sha', '')}")
        elif entry.get("status") == "error":
            await service.send(
                f"⚠️ Ссылка на сессию «{acc_name}» не сработала: {entry.get('detail', 'без пояснений')}. "
                f"Радар работает с прежней копией сессии. Проверь, что файл на Диске открыт по "
                f"ссылке («Все, у кого есть ссылка») и это именно файл .session.",
                key=f"session_url_error:{acc_name}")

    # Авто-окна лимита (после ручного /boost из команд выше и до создания пересылки)
    if args.once:
        # Очередь старше суток устарела: объявление давно неактуально. Чистим здесь, а не только в
        # пересылке аккаунта, — иначе очередь аккаунта, который не вошёл, висела бы вечно.
        for acc in accounts:
            ttl = float((acc.forward or {}).get("queue_ttl_hours", 24) or 0)
            dropped = store.expire_queue(ttl, acc.name)
            if dropped:
                print(f"[i] [{acc.name}] очередь: убрано {dropped} устаревших позиций "
                      f"(висели дольше {ttl:g} ч)", file=sys.stderr)
        if accounts:        # старые записи без аккаунта тоже должны уходить
            store.expire_queue(float((accounts[0].forward or {}).get("queue_ttl_hours", 24) or 0))
        for acc_name in apply_limit_windows(store, accounts):
            await service.send(f"🔓 Аккаунт «{acc_name}»: лимит пересылок обнулён автоматически по расписанию "
                               f"— очередь разбирается дальше.", key=f"limit_window:{acc_name}", force=True)

    runners: list[tuple[AccountConfig, object, Monitor]] = []
    failed_accounts: list[str] = []
    for acc in accounts:
        acc_sources = buckets[acc.name]
        if not acc_sources:
            print(f"[i] у аккаунта «{acc.name}» нет чатов в конфиге — пропускаю "
                  f"(укажи account: {acc.name} у нужных источников)", file=sys.stderr)
            continue

        client = make_client(acc.session, api_id, api_hash, delay=args.delay,
                             proxy=acc.proxy or args.proxy)
        try:
            await connect_client(client, acc, args)
        except SystemExit:
            # Бот должен ВИДЕТЬ, что аккаунт не вошёл (/accounts, /status), даже если проход
            # дальше упадёт: пишем состояние до любых решений ниже.
            fail_kind, fail_text = CONNECT_FAILURES.get(acc.name, ("ConnectError", "не удалось войти"))
            record_login(store, acc.name, False, fail_kind, fail_text)
            record_account_chats(store, acc.name, acc_sources, no_login=True)
            # В одном проходе (облако) один сломанный аккаунт не должен останавливать остальные:
            # раньше из-за одной сессии second не читал и main. Живой режим и --test-forward — как раньше.
            if not args.once or args.test_forward or len(accounts) < 2:
                raise
            failed_accounts.append(acc.name)
            continue
        me = await client.get_me()
        prefix = f"[{acc.name}] " if multi else ""
        print(f"[+] {prefix}вошли как {display_name(me)} (id={me.id})", file=sys.stderr)
        record_account_me(store, acc.name, me)
        if record_login(store, acc.name, True):
            await service.send(f"✅ Аккаунт «{acc.name}» снова вошёл в Telegram "
                               f"({display_name(me)}, id {me.id}) — чтение его чатов возобновлено.",
                               key=f"session_restored:{acc.name}", force=True)

        if args.test_forward:
            code = await test_forward(client, store, acc_sources, paced, args, defaults, account=acc)
            await client.disconnect()
            sys.exit(code)

        forwarder = build_forwarder(client, acc, store, paced, args)
        if forwarder is not None and not await forwarder.prepare():
            forwarder = None

        monitor = Monitor(client, store, acc_sources, notifier, paced,
                          explain=args.explain, skip_out=not args.include_own,
                          dedup_window=args.dedup_window, dedup_scope=args.dedup_scope,
                          max_age=args.max_age,
                          only_categories=tuple(x.strip() for x in (args.category or "").split(",") if x.strip()),
                          forwarder=forwarder, auto_join=args.auto_join,
                          heartbeat_minutes=heartbeat_minutes,
                          keep_hidden=keep_hidden,
                          account=acc.name, show_account=multi,
                          only_intents=tuple(x.strip() for x in (args.only_intent or "").split(",") if x.strip()),
                          only_directions=tuple(x.strip() for x in (args.only_direction or "").split(",") if x.strip()),
                          deadline=deadline, flood_wait_limit=args.flood_wait_limit)
        runners.append((acc, client, monitor))

    if failed_accounts:
        for name in failed_accounts:
            kind, why = CONNECT_FAILURES.get(name, ("ConnectError", "не удалось войти"))
            sess = next((a.session for a in accounts if a.name == name), name)
            store.log_error(kind, f"аккаунт «{name}»: {why}"[:300], account=name)
            drive = synced.get(name)
            if drive and drive.get("status") in ("imported", "unchanged"):
                same = (" ВНИМАНИЕ: ключ в файле на Диске — тот же, что уже отозван Telegram (или этот "
                        "аккаунт ещё где-то запущен с той же сессией, например старый бот на Pyrogram): "
                        "такой файл не поможет, нужен ключ от НОВОГО входа. "
                        if drive.get("same_key") else " ")
                how = (f"Сессия берётся с Google Диска (текущая версия {drive.get('sha', '?')}) и она не "
                       f"подошла.{same}Что делать: 1) на своей машине start.bat --login-qr --session {sess}; "
                       f"2) замени файл {sess}.session на Диске новой версией (ссылка не меняется); "
                       f"3) через пару минут радар сам подхватит его — останавливать ничего не нужно.")
            else:
                how = (f"Что делать: 1) на своей машине start.bat --login-qr --session {sess}; 2) положи "
                       f"{sess}.session на Google Диск и задай секрет SESSION_URL_{sess.upper()} со ссылкой "
                       f"(радар заберёт файл сам) — либо загрузи его в R2 (sessions/{sess}.session) "
                       f"при RADAR_ON=0, затем RADAR_ON=1 и GET /restart (см. DEPLOY.md).")
            await service.send(
                f"⚠️ Аккаунт «{name}» вышел из Telegram / не вошёл: {why}\n"
                f"Радар работает без него: " + (", ".join(a.name for a, _c, _m in runners) or "никто не вошёл") + ".\n"
                f"{how} Одну и ту же сессию нельзя держать одновременно на компьютере и в облаке.",
                key=f"session_broken:{name}")
        print(f"[!] Не вошли аккаунты: {', '.join(failed_accounts)} — проход идёт без них "
              f"(причина и что делать — выше и в сообщении бота)", file=sys.stderr)

    if not runners:
        sys.exit("Нет чатов для работы: проверь список источников и поле account в sources.yaml"
                 if not failed_accounts else
                 "Ни один аккаунт не вошёл в Telegram — читать нечем (см. причины выше)")

    if order_mode == "random" and len(sources) > 1:
        print(f"[i] порядок обхода случайный: начинаем с {sources[0].target} "
              f"(отключить: --order config)", file=sys.stderr)

    collector = build_collector(args, store, accounts, panel_mode, paced)

    panel = None
    if args.bot_panel and not args.once:
        panel = build_panel(args, store, accounts, buckets, sources, panel_mode,
                            heartbeat_minutes, paced)
    elif args.bot_panel and args.once:
        print("[i] --bot-panel с --once не запускается: проход короткий, панель нужна в живом "
              "режиме. Для схемы B держи панель отдельным процессом: start.bat --panel-only",
              file=sys.stderr)

    if args.once and len(runners) > 1:
        previous_last = store.bot_state_get("pass:last_account") or ""
        runners = rotate_runners(runners, store)
        print(f"[i] порядок аккаунтов: первым идёт «{runners[0][0].name}» — в прошлый "
              f"проход последним был «{previous_last or 'никто'}»", file=sys.stderr)

    gap_before = store.heartbeat_age_minutes("pulse")      # сколько радар молчал до этого прохода
    pass_started = time.time()

    if args.once:
        try:
            for acc, client, monitor in runners:
                prefix = f"[{acc.name}] " if multi else ""
                chat_report: dict = {}
                resolved = await resolve_targets(client, [s_.target for s_ in monitor.sources], paced,
                                                 auto_join=args.auto_join, report=chat_report)
                record_account_chats(store, acc.name, monitor.sources, resolved, chat_report)
                monitor.entities = resolved
                monitor.meta = {s_.target: s_ for s_ in monitor.sources if s_.target in resolved}
                if multi:
                    print(f"[i] {prefix}чатов разрешено: {len(resolved)} из {len(monitor.sources)}",
                          file=sys.stderr)
            for acc, client, monitor in runners:
                await monitor.flush_deferred()        # сначала отдаём то, что не влезло в лимит раньше
                await monitor.catch_up(list(monitor.meta.values()))
            run_read = sum(int(getattr(monitor, "counter", {}).get("scanned", 0) or 0)
                           for _a, _c, monitor in runners)
            for acc, client, monitor in runners:
                monitor.flush_stats()                 # счётчики — в базу до среза метрик
            for acc, client, monitor in runners:
                # Пульс за проход. Без него панель (--panel-only) в схеме B не видела бы, жив ли
                # радар вообще: живость определяется пульсом, а не находками (ТЗ §7.3, §16).
                # Числа — накопительные суммы из базы по чатам этого аккаунта: контейнер в облаке
                # живёт один проход, а суммы переживают его перезапуск.
                keys = [monitor.chat_key(src) for src in monitor.sources]
                read_total, found_total = store.totals_for_chats(keys)
                monitor.log_pulse(read=read_total, found=found_total)
            if collector is not None:
                # схема B: проход короткий, поэтому срез метрик один — но именно он и показывает расход.
                collector.msgs_provider = lambda: (store.scanned_total(), run_read)
                print_metrics(collector.write())
            for acc, client, monitor in runners:
                await client.disconnect()
        except Exception as exc:                                    # noqa: BLE001
            await service.send(ServiceNotify.failure_text(exc), key="failure", force=True)
            store.log_error(type(exc).__name__, str(exc)[:300])
            raise
    else:
        tasks = [monitor.run() for _, _, monitor in runners]
        if panel is not None:
            tasks.append(panel.run())     # панель живёт в том же процессе, но своей задачей
        if collector is not None:
            tasks.append(collector.loop())   # срезы ресурсов раз в --metrics-interval минут
        await asyncio.gather(*tasks)
    for _acc, _client, monitor in runners:
        monitor.flush_stats()

    # служебная сводка прохода: пустыми проходами не спамим — пишем, если что-то нашлось,
    # либо пришло время плановой сводки (антиспам по времени в базе, а не в памяти процесса)
    if getattr(args, "once", False) and service.enabled:
        read = sum(int(m.counter.get("scanned", 0) or 0) for _a, _c, m in runners)
        found = sum(int(m.counter.get("matched", 0) or 0) for _a, _c, m in runners)
        forwarded = sum(int(m.counter.get("forwarded", 0) or 0) for _a, _c, m in runners)
        per_account = []
        for acc in accounts:
            own = [m for a2, _c2, m in runners if a2.name == acc.name]
            per_account.append((acc.name,
                                sum(int(m.counter.get("scanned", 0) or 0) for m in own),
                                sum(int(m.counter.get("matched", 0) or 0) for m in own)))
        seen_before = sum(int(m.counter.get("duplicates", 0) or 0) for _a, _c, m in runners)
        text = ServiceNotify.pass_summary(
            now=datetime.now(timezone.utc), seconds=time.time() - pass_started,
            read=read, found=found, forwarded=forwarded, per_account=per_account,
            seen_before=seen_before,
            errors=store.errors_count(since_hours=24.0), db_total=store.stats(),
            gap_minutes=gap_before, expected_minutes=args.service_gap,
            leftover=[t for _a, _c, m in runners for t in getattr(m, "catchup_left", [])])
        await service.send(text, key="summary", force=bool(found or forwarded or not read))

    # длительность прохода пригодится для /cost: стоимость считаем от фактического расхода
    store.bot_state_set("pass:last_seconds", f"{time.time() - pass_started:.1f}")

    # метка сборки: по ней видно, какой код реально работает в облаке (см. DEPLOY.md)
    print(f"[i] код: monitor.py {build_marker()}", file=sys.stderr)
    try:
        import release as release_module
        print(f"[i] {release_module.short_line()}", file=sys.stderr)
    except Exception:                                             # noqa: BLE001
        pass

    # команды из Telegram: /status, /cost, /last, /sources… ещё раз в конце прохода (то, что
    # пришло, пока он шёл); основной разбор — в начале, см. ниже
    await handle_tg_commands(args, store, accounts, label="в конце прохода")

    # файл статистики: откуда и сколько сообщений идёт
    if args.stats_file:
        report = store.stats_report(days=args.stats_days, titles_from_config=titles_by_key(sources))
        Path(args.stats_file).write_text(report, encoding="utf-8")
        csv_path = Path(args.stats_file).with_suffix(".csv")
        store.stats_csv(str(csv_path))
        print(f"[i] статистика -> {args.stats_file} и {csv_path.name}", file=sys.stderr)

    for acc, _client, monitor in runners:
        prefix = f"[{acc.name}] " if multi else ""
        print(f"[i] {prefix}итоги прогона: {monitor.counter}", file=sys.stderr)
        forwarder = monitor.forwarder
        if forwarder is not None:
            queue_now = store.deferred_count(acc.name)
            print(f"[i] {prefix}пересылка: отправлено за сегодня {forwarder.sent_today}"
                  + (f" (лимит {forwarder.max_per_day})" if forwarder.max_per_day else "")
                  + (f", в очереди на добор {queue_now}" if queue_now else "")
                  + (f", остановлено: {forwarder.stopped_reason}" if forwarder.stopped_reason else ""),
                  file=sys.stderr)
    print(f"[i] в базе: {store.stats()} -> {args.db}", file=sys.stderr)


async def test_forward(client, store, sources: list[Source], paced, args, defaults: dict,
                       account: AccountConfig | None = None) -> int:
    """Проверка пересылки на живом Telegram: берёт последнее сообщение из первого источника
    и пересылает его получателю (боту). Показывает, работает ли именно forward."""
    from forwarder import Forwarder

    fwd = resolve_forward_settings(args, defaults) if account is None else dict(account.forward)
    if account is not None:
        if args.forward_to:
            fwd["to"] = args.forward_to
        if args.forward_mode:
            fwd["mode"] = args.forward_mode
        if args.forward_fallback:
            fwd["fallback"] = args.forward_fallback
    if not fwd.get("to"):
        print("[!] Не задан получатель. Укажи: --test-forward --forward-to @parcel_transfer_bot "
              "(или forward.to в sources.yaml)", file=sys.stderr)
        return 1

    forwarder = Forwarder(client, fwd["to"], store, paced, mode=fwd.get("mode", "forward"),
                          max_per_day=0, fallback=fwd.get("fallback", "link"),
                          dry_run=args.forward_dry_run,
                          account=account.name if account else "")
    if not await forwarder.prepare():
        print("[!] Получатель недоступен: проверь username бота и то, что диалог с ним начат.",
              file=sys.stderr)
        return 1

    resolved = await resolve_targets(client, [sources[0].target], paced)
    entity = resolved.get(sources[0].target)
    if entity is None:
        print(f"[!] Источник {sources[0].target} недоступен — проверь подписку.", file=sys.stderr)
        return 1

    messages = await call(lambda: client.get_messages(entity, limit=1), paced, label="test_forward(history)")
    sample = (messages or [None])[0]
    if sample is None:
        print("[i] В источнике нет сообщений для проверки — отправляю тестовый текст.")
        hit = {"chat_key": sources[0].target.lstrip("@"), "chat_title": getattr(entity, "title", ""),
               "msg_id": 0, "text": "Тестовая проверка пересылки радара. Если видишь это — всё настроено.",
               "link": "", "category": "parcel", "intent": "offer", "direction": "?"}
        if forwarder.dry_run:
            print("[i] dry-run: реальная отправка пропущена")
        else:
            await client.send_message(forwarder.target, forwarder._text_with_link(hit))
        print("[+] Тестовое сообщение отправлено.")
        return 0

    status = await forwarder.send(sample, {"chat_key": sources[0].target.lstrip("@"),
                                           "msg_id": sample.id, "text": getattr(sample, "text", "") or "",
                                           "chat_title": getattr(entity, "title", ""),
                                           "link": message_link(getattr(entity, "username", None),
                                                                getattr(entity, "id", None), sample.id)},
                                  mode_override="test")
    verdict = {
        "forwarded": "[+] Пересылка работает: сообщение ушло боту как forward (видно источник).",
        "copied": "[i] Пересылка в этом чате запрещена — ушёл текст со ссылкой (fallback).",
        "dry-run": "[i] dry-run: отправка не выполнялась, путь проверен.",
        "duplicate": "[i] Это сообщение уже пересылалось ранее — повторно не уходит (это правильно).",
        "failed": "[!] Отправка не удалась — смотри причину выше.",
        "limit": "[!] Сработал дневной лимит (в проверке он отключён — такого быть не должно).",
        "skipped": "[i] Пересылка запрещена в источнике, а fallback=skip — сообщение пропущено.",
    }.get(status, f"[?] Неизвестный статус: {status}")
    print(verdict)
    if status in ("forwarded", "copied"):
        print("[i] Это была проверка: она не съедает дневной лимит и не портит статистику отправок,")
        print("    но само сообщение помечено как пересланное — повторно боту оно не уйдёт.")
        print("[i] Дальше: start.bat --once --catchup 20 --category parcel,mixed")
    return 0 if status in ("forwarded", "copied", "dry-run", "duplicate", "skipped") else 1


def build_parser() -> argparse.ArgumentParser:
    """Все флаги командной строки.

    Вынесено отдельно, чтобы тесты могли проверить значения по умолчанию: раньше
    у флагов пересылки стояли «настоящие» значения (100, forward, link), и они молча
    перекрывали sources.yaml.
    """
    ap = argparse.ArgumentParser(description="Мониторинг Telegram: посылки/передачи/попутчики")
    ap.add_argument("--config", default="sources.yaml", help="файл со списком чатов/каналов")
    ap.add_argument("--channels", help="быстрый запуск: @chat1,@chat2 (в обход конфига)")
    ap.add_argument("--db", default="hits.sqlite3")
    ap.add_argument("--export", help="выгрузить найденное в CSV или TXT и выйти (формат по расширению)")
    ap.add_argument("--export-hours", type=float, default=0.0,
                    help="для TXT: только совпадения за последние N часов (0 — все)")
    ap.add_argument("--max-age", type=float, default=24.0,
                    help="не уведомлять о сообщениях старше N часов (по умолчанию 24; 0 — без ограничения)")
    ap.add_argument("--forward-to", help="пересылать совпадения этому получателю, напр. @parcel_transfer_bot")
    # ВАЖНО: default=None — иначе флаг перекрывал бы sources.yaml даже когда его не указывали
    ap.add_argument("--forward-mode", choices=["forward", "copy"], default=None,
                    help="forward — честная пересылка, copy — текст со ссылкой "
                         "(по умолчанию берётся из sources.yaml, иначе forward)")
    ap.add_argument("--forward-max-per-day", type=int, default=None,
                    help="лимит пересылок в сутки, 0 — без лимита "
                         "(по умолчанию из sources.yaml, иначе 100)")
    ap.add_argument("--forward-fallback", choices=["link", "skip"], default=None,
                    help="если в чате запрещена пересылка: link — текст со ссылкой, skip — пропустить "
                         "(по умолчанию из sources.yaml, иначе link)")
    ap.add_argument("--test-forward", action="store_true",
                    help="проверить пересылку: переслать последнее сообщение источника боту и выйти")
    ap.add_argument("--forward-dry-run", action="store_true",
                    help="проверить пересылку без реальной отправки (в базу пишется dry-run)")
    ap.add_argument("--stats-file", default="stats.txt", help="файл статистики (пусто — не писать)")
    ap.add_argument("--stats-days", type=int, default=7, help="сколько дней показывать в статистике")
    ap.add_argument("--show-stats", action="store_true", help="показать статистику и выйти")
    ap.add_argument("--stats-only", action="store_true", help="то же, что --show-stats")
    ap.add_argument("--test-notify", action="store_true", help="отправить тестовое уведомление и выйти")
    ap.add_argument("--keep-hidden", action="store_true",
                    help="не отсекать объявления от авторов со скрытым профилем («hidden by user»); "
                         "по умолчанию такие не пересылаются, потому что автору нельзя написать")
    ap.add_argument("--account", help="запустить только этот аккаунт из секции accounts "
                                      "(по умолчанию — все)")
    ap.add_argument("--heartbeat", type=float, default=None, metavar="MIN",
                    help="в живом режиме печатать пульс раз в N минут (0 — выключить); "
                         "показывает, что радар жив, и сколько прочитано с прошлого пульса")
    ap.add_argument("--bot-panel", action="store_true",
                    help="бот-панель внутри радара: команды /status /accounts /stats /usage и алерты "
                         "приходят в Telegram от бота (нужны TG_BOT_TOKEN и числовой TG_NOTIFY_CHAT)")
    ap.add_argument("--panel-only", action="store_true",
                    help="только бот-панель, без радара и без Telethon: читает базу и отвечает на "
                         "команды (когда радар крутится на другом сервере/в контейнере)")
    ap.add_argument("--alert-silent", type=float, default=30.0, metavar="MIN",
                    help="через сколько минут без пульса слать алерт «аккаунт молчит» "
                         "(по умолчанию 30; 0 — не слать)")
    ap.add_argument("--digest", choices=["on", "off"], default=None,
                    help="утренний дайджест в 09:00 местного времени (то же, что /digest у бота)")
    ap.add_argument("--pass-budget", type=float, default=0.0, metavar="СЕК",
                    help="сколько секунд отвести на проход: радар сам перестанет начинать "
                         "новые чаты и закончит чисто, вместо того чтобы быть убитым по "
                         "таймауту (0 — из RADAR_TIMEOUT минус четверть, нет его — без лимита)")
    ap.add_argument("--flood-wait-limit", type=float, default=0.0, metavar="СЕК",
                    help="не ждать FloodWait дольше этого: чат пропускается и дойдёт в "
                         "следующий проход (0 — не больше четверти бюджета)")
    ap.add_argument("--tg-commands", choices=("auto", "on", "off"), default="auto",
                    help="разбирать команды из Telegram в конце прохода (/status, /cost, "
                         "/last, /sources, /limits…). Ответ приходит со следующим проходом, "
                         "зато без отдельной панели. auto — включено, если заданы "
                         "TG_BOT_TOKEN и TG_NOTIFY_CHAT")
    ap.add_argument("--service-notify", choices=("auto", "bot", "none"), default="auto",
                    help="служебные сообщения ботом в TG_NOTIFY_CHAT: сводка прохода "
                         "(прочитано/найдено/переслано), падения, долгие паузы. Находки сюда "
                         "НЕ идут — они уходят пересылкой аккаунта получателю из sources.yaml "
                         "(forward.to). auto — слать, если заданы TG_BOT_TOKEN и TG_NOTIFY_CHAT")
    ap.add_argument("--service-every", type=float, default=6.0, metavar="ЧАСОВ",
                    help="как часто слать плановую сводку, когда находок нет (по умолчанию 6)")
    ap.add_argument("--service-gap", type=float, default=10.0, metavar="МИН",
                    help="ожидаемая пауза между проходами: если простой вдвое больше, радар "
                         "напишет об этом в сводке (по умолчанию 10 — как cron */10)")
    ap.add_argument("--check-sessions", action="store_true",
                    help="живая проверка сессий (get_me по каждому аккаунту) — ТОЛЬКО когда радар "
                         "остановлен: второй клиент на тот же .session отзывает ключ")
    ap.add_argument("--check-sources", action="store_true",
                    help="живая проверка чатов из конфига: что читается, куда надо вступить, где "
                         "радар «не смотрит» (0 прочитанных). Базу не меняет; ТОЛЬКО при "
                         "остановленном радаре (в облаке: GET /sources-check)")
    ap.add_argument("--force", action="store_true",
                    help="для --check-sources: проверить, даже если в базе виден живой пульс "
                         "(использует сам контейнер: он и есть единственный клиент)")
    ap.add_argument("--metrics-interval", type=float, default=15.0, metavar="MIN",
                    help="как часто писать срез ресурсов (память, CPU, нагрузка) в базу и раз в час "
                         "в metrics.csv; 0 — выключить метрики (по умолчанию 15)")
    ap.add_argument("--metrics-csv", default="metrics.csv",
                    help="файл срезов ресурсов для Excel (по умолчанию metrics.csv рядом с проектом)")
    ap.add_argument("--mode", choices=["A", "B"], default=None,
                    help="схема работы для отчётов панели: A — постоянный слушатель, B — проходы по "
                         "расписанию (по умолчанию A, а с --once — B)")
    ap.add_argument("--topics-depth", type=int, default=300, metavar="N",
                    help="сколько последних сообщений просмотреть при поиске тем (--list-topics)")
    ap.add_argument("--list-topics", action="store_true",
                    help="показать темы (форумы/супергруппы с темами) и их id, потом выйти")
    ap.add_argument("--order", choices=["random", "config"], default=None,
                    help="порядок обхода чатов: random (по умолчанию) — каждый запуск со случайного, "
                         "config — как в sources.yaml")
    ap.add_argument("--resend-pending", nargs="?", type=int, const=50, default=None, metavar="N",
                    help="догнать уведомления, которые не ушли раньше (по умолчанию до 50 штук)")
    ap.add_argument("--reset-db", action="store_true",
                    help="удалить базу hits.sqlite3 и выйти (пересобрать найденное заново)")
    ap.add_argument("--doctor", action="store_true", help="проверить окружение: зависимости, ключи, конфиг, сессию")
    ap.add_argument("--catchup", type=int, help="сколько последних сообщений прочитать при старте")
    ap.add_argument("--once", action="store_true", help="разовый проход (catch-up) и выход")
    ap.add_argument("--min-score", type=int, help="порог совпадения (по умолчанию 4)")
    ap.add_argument("--profile", choices=["chat", "news"], help="chat — чат с объявлениями, news — новостной канал")
    ap.add_argument("--notify", choices=["console", "file", "bot", "both", "none"], default="console")
    ap.add_argument("--notify-file", default="hits.log")
    ap.add_argument("--explain", action="store_true", help="показывать, какие правила сработали")
    ap.add_argument("--include-own", action="store_true", help="не пропускать свои сообщения")
    ap.add_argument("--dedup-window", type=float, default=24.0,
                    help="окно (часы), в котором одинаковые тексты считаются дублем; 0 — выключить")
    ap.add_argument("--dedup-scope", choices=["global", "chat"], default="global",
                    help="global — дубли ловятся между чатами (по умолчанию), chat — только внутри одного")
    ap.add_argument("--category", help="категории: parcel (посылки/передачи), mixed (посылки+попутчики), ride (только люди)")
    ap.add_argument("--only-intent", help="показывать только: offer,request (через запятую)")
    ap.add_argument("--only-direction", help="показывать только направления: BY->PL,PL->BY,?->PL (через запятую)")
    ap.add_argument("--delay", type=float, default=2.0, help="пауза между вызовами API, сек")
    ap.add_argument("--auto-join", action="store_true",
                    help="автоматически вступать в чаты по ссылкам-приглашениям (t.me/+...)")
    ap.add_argument("--proxy", help="socks5://user:pass@host:port")
    ap.add_argument("--session", default=os.getenv("TG_SESSION", "monitor_session"))
    ap.add_argument("--api-id", type=int)
    ap.add_argument("--api-hash")
    return ap


def main() -> None:
    load_dotenv()          # ключи из .env — тогда работает и прямой запуск python monitor.py

    ap = build_parser()
    args, _unknown = ap.parse_known_args()   # --no-pause нужен только для .bat — молча игнорируем

    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        print("\n[!] остановлено (база и сессия сохранены)", file=sys.stderr)


if __name__ == "__main__":
    main()
