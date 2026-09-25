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
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core_telegram import (BOT_TOKEN_HINT, Paced, add_flood_hook, build_notifier, call,
                           collect_topics, display_name, format_hit, hidden_author_reason,
                           load_dotenv, make_client, message_link, peer_id, resolve_targets,
                           topic_of, topic_title_of)
from matcher import analyze

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

    def stats_report(self, days: int = 7, titles_from_config: dict | None = None) -> str:
        """Текстовый отчёт: откуда сколько сообщений идёт, по дням и по чатам.

        titles_from_config — chat_key -> название из sources.yaml: подставляет имена чатам,
        по которым ещё не было находок (иначе такие строки были бы без названия).
        """
        titles = {row[0]: (row[1] or row[0]) for row in self.conn.execute(
            "SELECT chat_key, MAX(chat_title) FROM hits GROUP BY chat_key").fetchall()}
        for key, title in (titles_from_config or {}).items():
            if title and not titles.get(key):
                titles[key] = title
        lines = [
            "Статистика радара: откуда и сколько сообщений",
            f"Сформирована: {datetime.now().astimezone().strftime('%d.%m.%Y %H:%M')} (местное время)",
            "=" * 78,
        ]

        totals = self.conn.execute(
            """SELECT chat_key, SUM(scanned), SUM(matched), SUM(saved), SUM(forwarded),
                      SUM(forward_skipped), SUM(forward_failed), SUM(filtered), SUM(too_old),
                      SUM(deferred), SUM(hidden)
               FROM stats GROUP BY chat_key ORDER BY SUM(scanned) DESC"""
        ).fetchall()
        lines += ["", "ИТОГО ПО ИСТОЧНИКАМ (за всё время)", "-" * 78,
                  f"{'источник':20} {'название':31} {'аккаунт':8} {'прочит':>7} {'найдено':>8} "
                  f"{'новых':>6} {'переслано':>10} {'фильтр':>7} {'старше':>7}"]
        accounts_by_chat = {row[0]: (row[1] or "") for row in self.conn.execute(
            "SELECT chat_key, MAX(account) FROM stats GROUP BY chat_key").fetchall()}
        for (chat, scanned, matched, saved, forwarded, skipped, failed, filtered, too_old,
             _deferred, _hidden) in totals:
            label = (titles.get(chat) or "")[:31]
            who = (accounts_by_chat.get(chat) or "")[:8]
            lines.append(f"{self._chat_ref(chat):20} {label:31} {who:8} {scanned or 0:>7} "
                         f"{matched or 0:>8} {saved or 0:>6} {forwarded or 0:>10} "
                         f"{filtered or 0:>7} {too_old or 0:>7}")
        hidden_total = sum(row[10] or 0 for row in totals)
        deferred_total = sum(row[9] or 0 for row in totals)
        lines.append(f"пропущено пересылок (лимит/нельзя переслать): {sum(row[5] or 0 for row in totals)}, "
                     f"ошибок отправки: {sum(row[6] or 0 for row in totals)}")
        lines.append(f"отложено на добор (следующий прогон): {deferred_total}, "
                     f"сейчас в очереди: {self.deferred_count()}")
        if hidden_total:
            lines.append(f"пропущено из-за скрытых авторов («hidden by user»): {hidden_total}")

        # сводка по аккаунтам: видно, кто сколько прочитал, нашёл и отправил
        per_account = self.conn.execute(
            """SELECT COALESCE(account, '—') AS acc, SUM(scanned), SUM(matched), SUM(saved),
                      SUM(forwarded), SUM(hidden)
               FROM stats GROUP BY acc ORDER BY SUM(scanned) DESC"""
        ).fetchall()
        if len(per_account) > 1:
            lines += ["", "ПО АККАУНТАМ", "-" * 78,
                      f"{'аккаунт':14} {'прочит':>7} {'найдено':>8} {'новых':>6} {'переслано':>10} "
                      f"{'скрытых':>8} {'сегодня':>8}"]
            for acc, scanned, matched, saved, forwarded, hidden in per_account:
                today = self.forwarded_today(None if acc == "—" else acc)
                lines.append(f"{acc[:14]:14} {scanned or 0:>7} {matched or 0:>8} {saved or 0:>6} "
                             f"{forwarded or 0:>10} {hidden or 0:>8} {today:>8}")

        lines += ["", f"ПО ДНЯМ (последние {days})", "-" * 78,
                  f"{'дата':12} {'источник':20} {'название':32} {'прочит':>7} {'найдено':>8} {'переслано':>10}"]
        rows = self.conn.execute(
            "SELECT day, chat_key, scanned, matched, saved, forwarded FROM stats "
            "ORDER BY day DESC LIMIT ?", (days * 20,)
        ).fetchall()
        seen_days: list[str] = []
        for day, chat, scanned, matched, saved, forwarded in rows:
            if day not in seen_days:
                seen_days.append(day)
            if seen_days.index(day) >= days:
                continue
            label = (titles.get(chat) or "")[:31]
            lines.append(f"{day:12} {self._chat_ref(chat):20} {label:32} "
                         f"{scanned:>7} {matched:>8} {forwarded:>10}")

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
                 account: str = "", show_account: bool = False):
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
        """Достаёт исходное сообщение для добора из очереди (None — если его больше нет)."""
        mapping = self.entity_by_key()
        entity = mapping.get(chat_key)
        if entity is None:
            target = next((s.target for s in self.sources if self.chat_key(s) == chat_key), None)
            if target is None:
                return None
            try:
                entity = await call(lambda t=target: self.client.get_entity(t), self.paced,
                                    label=f"get_entity({target})")
            except Exception:  # noqa: BLE001
                return None
        try:
            found = await call(lambda: self.client.get_messages(entity, ids=msg_id), self.paced,
                               label="get_messages(queued)")
        except Exception as exc:  # noqa: BLE001
            print(f"[!] добор {chat_key}/{msg_id}: {type(exc).__name__} {exc}", file=sys.stderr)
            return None
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
        result = await self.forwarder.flush_deferred(self.fetch_queued)
        if result.get("sent"):
            self.counter["forwarded"] = self.counter.get("forwarded", 0) + result["sent"]
        return result

    # --- служебное

    def chat_key(self, source: Source) -> str:
        raw = source.target.strip()
        if "t.me/+" in raw or "joinchat/" in raw or raw.startswith("+"):
            invite = raw.rstrip("/").split("/")[-1].lstrip("+")
            return f"invite:{invite}"          # например invite:CmQyl50rf-NlODFi
        return raw.lstrip("@").rstrip("/").split("/")[-1].lower()

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
        # фильтры: категория, намерение, направление
        if self.only_categories and match.category not in self.only_categories:
            self.counter["filtered"] += 1
            self._bump(key, filtered=1)
            return None
        if self.only_intents and match.intent not in self.only_intents:
            self.counter["filtered"] += 1
            self._bump(key, filtered=1)
            return None
        if self.only_directions and match.direction not in self.only_directions:
            self.counter["filtered"] += 1
            self._bump(key, filtered=1)
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
            elif status in ("limit", "skipped"):
                self._bump(key, forward_skipped=1)
            elif status.startswith("failed"):
                self._bump(key, forward_failed=1)
        return hit

    # --- старт

    async def catch_up(self, sources: list[Source]) -> None:
        for source in sources:
            entity = self.entities.get(source.target)
            if entity is None or source.catchup <= 0:
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
            found = 0
            for message in sorted(messages or [], key=lambda m: m.id):   # старые -> новые
                if await self.process_message(message, source):
                    found += 1
            too_old = self.counter["too_old"]
            note = f", старше {self.max_age_hours:g} ч пропущено {too_old}" if self.max_age_hours > 0 else ""
            print(f"[i] catch-up {source.target}: прочитано {len(messages or [])}, "
                  f"совпадений {found}{note}", file=sys.stderr)

    # --- пульс: чтобы долгая тишина не выглядела как зависание

    def heartbeat_text(self) -> str:
        """Одна строка о состоянии: сколько прочитано/найдено с прошлого пульса, что с пересылкой."""
        now = datetime.now().astimezone().strftime("%H:%M")
        previous = self.last_pulse or {}
        delta = {k: self.counter.get(k, 0) - previous.get(k, 0) for k in
                 ("events", "scanned", "matched", "duplicates", "filtered", "too_old")}
        self.last_pulse = dict(self.counter)

        forwarded_today = self.store.forwarded_today()
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
            self.forwarder.sent_today = self.store.forwarded_today()   # сразу после полуночи это 0
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

    def log_pulse(self) -> None:
        """Пишет пульс в базу: по нему бот-панель понимает, жив ли аккаунт (ТЗ §7.3).

        Важно: живость определяется именно пульсом, а не находками — иначе тихий чат
        ночью выглядел бы как «аккаунт сломался» (ТЗ §14.1).
        """
        try:
            self.store.log_heartbeat(
                "pulse", account=self.account or None,
                detail=f"прочитано {self.counter.get('scanned', 0)}, "
                       f"найдено {self.counter.get('saved', 0)}",
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

        resolved = await resolve_targets(self.client, [s.target for s in self.sources], self.paced,
                                         auto_join=self.auto_join)
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
        report("FAIL", "TG_API_ID не задан", "впиши в .env или выполни: set TG_API_ID=1234567 (cmd) / $env:TG_API_ID=\"1234567\" (PowerShell)")
        fails.append("TG_API_ID")
    elif not api_id.strip().isdigit() or len(api_id.strip()) < 5:
        report("FAIL", f"TG_API_ID выглядит обрезанным: {api_id!r}", "нужно полное число из my.telegram.org, например 1234567")
        fails.append("TG_API_ID")
    else:
        report("PASS", f"TG_API_ID задан ({api_id.strip()})")

    if not api_hash:
        report("FAIL", "TG_API_HASH не задан", "впиши в .env или выполни: set TG_API_HASH=... (cmd) / $env:TG_API_HASH=\"...\" (PowerShell)")
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
        # здесь проверяется только наличие переменных, без сети: токен может быть неверным (401).
        # По-настоящему это проверяет --test-notify — там запрос getMe к Telegram.
        report("PASS", "TG_BOT_TOKEN и TG_NOTIFY_CHAT заданы (это проверка наличия, не отправки)",
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
                     account=account.name)


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
        print(f"[!] Аккаунт «{account.name}» ({account.session}.session): {message}", file=sys.stderr)
        if name in ("SessionPasswordNeededError", "PasswordHashInvalidError", "PhoneCodeInvalidError"):
            print("    Надёжный способ войти без кода и SMS:  start.bat --login-qr", file=sys.stderr)
        elif name == "AuthKeyDuplicatedError":
            print(f"    Удали файл {account.session}.session и войди заново: "
                  f"start.bat --login-qr --session {account.session}", file=sys.stderr)
        sys.exit(1)


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
            sys.exit("Нужны TG_API_ID и TG_API_HASH (или --api-id/--api-hash). Получить: my.telegram.org")
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
        sys.exit("Нужны TG_API_ID и TG_API_HASH (или --api-id/--api-hash). Получить: my.telegram.org")

    if args.check_sessions:
        sys.exit(await check_sessions(args, defaults, store, api_id, api_hash))

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

    runners: list[tuple[AccountConfig, object, Monitor]] = []
    for acc in accounts:
        acc_sources = buckets[acc.name]
        if not acc_sources:
            print(f"[i] у аккаунта «{acc.name}» нет чатов в конфиге — пропускаю "
                  f"(укажи account: {acc.name} у нужных источников)", file=sys.stderr)
            continue

        client = make_client(acc.session, api_id, api_hash, delay=args.delay,
                             proxy=acc.proxy or args.proxy)
        await connect_client(client, acc, args)
        me = await client.get_me()
        prefix = f"[{acc.name}] " if multi else ""
        print(f"[+] {prefix}вошли как {display_name(me)} (id={me.id})", file=sys.stderr)

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
                          only_directions=tuple(x.strip() for x in (args.only_direction or "").split(",") if x.strip()))
        runners.append((acc, client, monitor))

    if not runners:
        sys.exit("Нет чатов для работы: проверь список источников и поле account в sources.yaml")

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

    if args.once:
        for acc, client, monitor in runners:
            prefix = f"[{acc.name}] " if multi else ""
            resolved = await resolve_targets(client, [s_.target for s_ in monitor.sources], paced,
                                             auto_join=args.auto_join)
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
        if collector is not None:
            # схема B: проход короткий, поэтому срез один — но именно он и показывает расход.
            # «За час» здесь честно означает «за этот проход»: pulses в разовом режиме не пишутся.
            collector.msgs_provider = lambda: (store.scanned_total(), run_read)
            print_metrics(collector.write())
        for acc, client, monitor in runners:
            await client.disconnect()
    else:
        tasks = [monitor.run() for _, _, monitor in runners]
        if panel is not None:
            tasks.append(panel.run())     # панель живёт в том же процессе, но своей задачей
        if collector is not None:
            tasks.append(collector.loop())   # срезы ресурсов раз в --metrics-interval минут
        await asyncio.gather(*tasks)
    for _acc, _client, monitor in runners:
        monitor.flush_stats()

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
    ap.add_argument("--check-sessions", action="store_true",
                    help="живая проверка сессий (get_me по каждому аккаунту) — ТОЛЬКО когда радар "
                         "остановлен: второй клиент на тот же .session отзывает ключ")
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
