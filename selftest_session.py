#!/usr/bin/env python3
"""
Офлайн-тесты конвертера сессий (session_convert.py): сеть, Telegram и аккаунт НЕ нужны.

Проверяется то, что дороже всего стоит вживую — работа с auth_key:
  * распознавание формата по схеме (Pyrogram / Telethon / мусор / tdata / отсутствующий файл);
  * перенос ключа: побайтово тот же auth_key, правильные dc_id/server_address/port,
    версия схемы Telethon'а; файл Telethon читается самим TelegramClient;
  * auth_key НЕ утекает: ни байта ключа в выводе (hex, base64, сырые байты);
  * исходный файл Pyrogram остаётся цел и остаётся файлом Pyrogram;
  * ничего не перезаписывается молча: ни «на месте», ни поверх чужой рабочей сессии;
  * понятные отказы вместо трейсбеков: test_mode, короткий ключ, неизвестный DC,
    пустая таблица sessions, не-SQLite;
  * обвязка: start.bat ведёт --convert-session в конвертер и не требует ключей заранее,
    *.session вырезан из гита и из образа.

Запуск:  python3 selftest_session.py
"""
from __future__ import annotations

import base64
import contextlib
import io
import os
import shutil
import sqlite3
from pathlib import Path

import session_convert as conv

WORKDIR = Path(f"tests/_tmp_session_{os.getpid()}")

# Схема в точности как у Pyrogram (version + peers + sessions) — проверка должна ловить
# именно такой файл, а не «что-то, где есть dc_id».
PYRO_SCHEMA = [
    "CREATE TABLE version (number INTEGER PRIMARY KEY)",
    "INSERT INTO version VALUES (3)",
    "CREATE TABLE peers (id INTEGER PRIMARY KEY, access_hash INTEGER, type INTEGER NOT NULL, "
    "username TEXT, phone_number TEXT, last_update_on INTEGER NOT NULL DEFAULT "
    "(CAST(STRFTIME('%s', 'now') AS INTEGER)))",
    "CREATE TABLE sessions (dc_id INTEGER PRIMARY KEY, test_mode INTEGER, auth_key BLOB, "
    "date INTEGER NOT NULL, user_id INTEGER, is_bot INTEGER)",
]

checks: list[tuple[str, bool, str]] = []


def section(title: str) -> None:
    print(f"\n── {title} " + "─" * max(0, 66 - len(title)))


def check(name: str, ok: bool, detail: str = "") -> None:
    checks.append((name, bool(ok), detail))


def make_pyrogram(path: str, dc: int = 2, key: bytes | None = None, test_mode: int = 0,
                  rows: int = 1, user_id: int = 123456789, date: int = 1700000000) -> bytes:
    """Пишит файл сессии формата Pyrogram. Возвращает auth_key (нужен для сверки)."""
    key = key if key is not None else os.urandom(conv.AUTH_KEY_LEN)
    conn = sqlite3.connect(path)
    for statement in PYRO_SCHEMA:
        conn.execute(statement)
    for index in range(rows):
        # в каждой строке свой ключ: проверяем, что берётся свежая (по date)
        conn.execute(
            "INSERT OR REPLACE INTO sessions VALUES (?,?,?,?,?,?)",
            (dc + index if rows > 1 else dc, test_mode,
             key if index == 0 else os.urandom(conv.AUTH_KEY_LEN),
             date + index * 100, user_id, 0),
        )
    conn.commit()
    conn.close()
    return key


def main() -> int:
    WORKDIR.mkdir(parents=True, exist_ok=True)
    saved_environ = dict(os.environ)
    try:
        run_checks()
    finally:
        os.environ.clear()
        os.environ.update(saved_environ)
        shutil.rmtree(WORKDIR, ignore_errors=True)

    section("Итог")
    for name, ok, detail in checks:
        print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail else ""))
    failed = [name for name, ok, _ in checks if not ok]
    print("\nИТОГ:", "всё ок" if not failed else f"провалено: {failed}")
    return 1 if failed else 0


def run_checks() -> None:
    # ── распознавание формата ───────────────────────────────────────────
    section("Распознавание формата")
    pyro = str(WORKDIR / "pyro.session")
    key = make_pyrogram(pyro)
    check("файл Pyrogram опознан как pyrogram", conv.sniff_session(pyro) == "pyrogram",
          conv.sniff_session(pyro))

    junk = WORKDIR / "junk.session"
    junk.write_bytes(b"tdata-like binary garbage" * 8)
    check("мусор опознан как unknown", conv.sniff_session(str(junk)) == "unknown", "")

    tdata = WORKDIR / "tdata"
    tdata.mkdir()
    check("каталог tdata опознан как unknown", conv.sniff_session(str(tdata)) == "unknown", "")

    try:
        conv.sniff_session(str(WORKDIR / "нет.session"))
        check("отсутствующий файл: понятная ошибка", False, "исключения не было")
    except conv.SessionConvertError as exc:
        check("отсутствующий файл: понятная ошибка", "файла нет" in str(exc), str(exc)[:60])

    # ── перенос ключа ──────────────────────────────────────────────────
    section("Перенос ключа")
    target_name = WORKDIR / "second_session"
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        target = conv.convert(pyro, out=str(target_name))
    printed = out.getvalue()
    check("результат — файл .session", target.endswith(".session") and Path(target).exists(), target)
    check("результат опознан как telethon", conv.sniff_session(target) == "telethon", "")

    from telethon.sessions import SQLiteSession
    from telethon.sessions.sqlite import CURRENT_VERSION
    from telethon import TelegramClient

    session = SQLiteSession(str(target_name))
    check("auth_key перенесён побайтово", session.auth_key is not None
          and session.auth_key.key == key, f"{len(key)} байт")
    check("dc_id сохранён", session.dc_id == 2, str(session.dc_id))
    check("server_address соответствует DC", session.server_address == conv.DC_IPV4[2],
          str(session.server_address))
    check("порт 443", int(session.port or 0) == 443, str(session.port))

    version = sqlite3.connect(target).execute("select version from version").fetchone()[0]
    check(f"версия схемы Telethon ({CURRENT_VERSION})", version == CURRENT_VERSION, str(version))

    client = TelegramClient(str(target_name), 1234567, "0" * 32)
    check("TelegramClient видит тот же ключ (без сети)",
          client.session.auth_key is not None and client.session.auth_key.key == key, "")

    # ── auth_key не утекает в вывод ─────────────────────────────────────
    section("Ключ не утекает")
    check("в выводе нет ключа в hex", key.hex() not in printed and key.hex()[:40] not in printed, "")
    check("в выводе нет ключа в base64", base64.b64encode(key).decode() not in printed
          and base64.b64encode(key).decode()[:32] not in printed, "")
    check("в выводе нет сырых байт ключа", key not in printed.encode("utf-8", "replace"), "")
    check("ключ не попал в переменные окружения",
          not any(key.hex() in value for value in os.environ.values()), "")
    check("в выводе сказано, что ключ совпал", "совпал" in printed, "")

    # ── исходный файл не тронут ────────────────────────────────────────
    section("Исходный файл цел")
    check("исходник остался файлом Pyrogram", conv.sniff_session(pyro) == "pyrogram", "")
    check("ключ в исходнике тот же", conv.read_pyrogram(pyro)["auth_key"] == key, "")
    check("рядом нет временных копий",
          sorted(p.name for p in WORKDIR.iterdir() if p.is_file())
          == ["junk.session", "pyro.session", "second_session.session"],
          ", ".join(sorted(p.name for p in WORKDIR.iterdir() if p.is_file())))

    # ── защита от перезаписи ───────────────────────────────────────────
    section("Ничего не перезаписываем молча")
    try:
        conv.convert(pyro, out=str(target_name))
        check("поверх готового результата не пишем", False, "перезаписал без --force")
    except conv.SessionConvertError as exc:
        check("поверх готового результата не пишем", "--force" in str(exc) and "--out" in str(exc),
              str(exc)[:60])

    try:
        conv.convert(pyro)
        check("конвертация «на месте» не затирает исходник", False, "перезаписал исходник")
    except conv.SessionConvertError as exc:
        check("конвертация «на месте» не затирает исходник",
              "на месте" in str(exc) and "--out" in str(exc), str(exc)[:70])

    other_key = make_pyrogram(str(WORKDIR / "other.session"), dc=4)
    with contextlib.redirect_stdout(io.StringIO()):
        conv.convert(str(WORKDIR / "other.session"), out=str(target_name), force=True)
    check("--force перезаписывает", SQLiteSession(str(target_name)).auth_key.key == other_key, "")
    check("после --force файл валиден для Telethon",
          conv.sniff_session(str(WORKDIR / "second_session.session")) == "telethon", "")

    # ── понятные отказы ────────────────────────────────────────────────
    section("Понятные отказы")
    test_mode = str(WORKDIR / "test_mode.session")
    make_pyrogram(test_mode, test_mode=1)
    short = str(WORKDIR / "short.session")
    make_pyrogram(short, key=b"123")
    bad_dc = str(WORKDIR / "bad_dc.session")
    conn = sqlite3.connect(bad_dc)
    for statement in PYRO_SCHEMA:
        conn.execute(statement)
    conn.execute("INSERT INTO sessions VALUES (?,?,?,?,?,?)",
                 (99, 0, os.urandom(conv.AUTH_KEY_LEN), 1700000000, 1, 0))
    conn.commit()
    conn.close()
    empty = str(WORKDIR / "empty.session")
    conn = sqlite3.connect(empty)
    for statement in PYRO_SCHEMA:
        conn.execute(statement)
    conn.commit()
    conn.close()

    cases = [
        (test_mode, "тестового дата-центра", "test_mode=1"),
        (short, "256 байт", "короткий ключ"),
        (bad_dc, "такого дата-центра нет", "неизвестный dc_id"),
        (empty, "ключ", "пустая таблица sessions"),
        (str(junk), "не похоже на сессию Pyrogram", "не-SQLite"),
        (str(WORKDIR / "second_session.session"), "это уже сессия Telethon", "вход уже Telethon"),
        (str(WORKDIR / "нет.session"), "файла нет", "файла нет"),
    ]
    for path, expect, label in cases:
        try:
            conv.convert(path, out=str(WORKDIR / f"out_{len(checks)}"))
            check(f"отказ: {label}", False, "исключения не было")
        except conv.SessionConvertError as exc:
            check(f"отказ: {label}", expect in str(exc), str(exc)[:70])

    # ── несколько строк в sessions ─────────────────────────────────────
    section("Несколько строк в sessions")
    multi = str(WORKDIR / "multi.session")
    make_pyrogram(multi, rows=2)
    data = conv.read_pyrogram(multi)
    check("взята свежая строка (date больше)", data["rows"] == 2 and data["dc_id"] == 3,
          f"строк={data['rows']}, dc_id={data['dc_id']}")
    check("user_id прочитан", data["user_id"] == 123456789, str(data["user_id"]))
    check("длина ключа проверяется", len(data["auth_key"]) == conv.AUTH_KEY_LEN, "")

    # ── имя результата ─────────────────────────────────────────────────
    section("Имя результата")
    check(".session добавляется", conv.out_path("pyro.session", "second") == "second.session", "")
    check("двойное расширение не дублируется",
          conv.out_path("pyro.session", "second.session") == "second.session", "")
    check("путь с каталогом сохраняется",
          conv.out_path("pyro.session", "sessions/second") == "sessions/second.session", "")
    check("без --out имя берётся из исходника",
          conv.out_path("pyro.session") == "pyro.session", "")
    check("пустое --out = имя исходника", conv.out_path("pyro.session", "   ") == "pyro.session", "")
    try:
        conv.out_path("", "")
        check("совсем без имени: понятная ошибка", False, "принял пустое имя")
    except conv.SessionConvertError:
        check("совсем без имени: понятная ошибка", True, "")

    # ── командная строка ───────────────────────────────────────────────
    section("Командная строка")
    first = str(WORKDIR / "acc1.session")
    second = str(WORKDIR / "acc2.session")
    make_pyrogram(first, dc=1)
    make_pyrogram(second, dc=5)
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = conv.main([first, second, "--out", "несколько"])
    check("--out с несколькими файлами отклоняется", code == 2, str(code))

    out_dir = str(WORKDIR / "out")
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = conv.main([first, second, "--out-dir", out_dir])
    printed = out.getvalue()
    check("пачка файлов: код 0", code == 0, str(code))
    check("пачка файлов: оба результата в --out-dir",
          Path(out_dir, "acc1.session").exists() and Path(out_dir, "acc2.session").exists(), "")
    check("--out-dir не трогает исходники",
          conv.sniff_session(first) == "pyrogram" and conv.sniff_session(second) == "pyrogram", "")
    check("подсказка: имена для TG_SESSION через запятую",
          "TG_SESSION" in printed and "acc1,acc2" in printed, "")
    check("подсказка: команда загрузки в R2", "r2 object put" in printed and "--remote" in printed, "")
    check("ключи не попали в подсказки",
          conv.read_pyrogram(first)["auth_key"].hex() not in printed, "")

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = conv.main([first, second, "--out", "одно-имя"])
    check("--out с пачкой файлов отклоняется с кодом 2", code == 2, str(code))

    out = io.StringIO()
    err = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = conv.main([first])
    check("один файл без --out: «на месте» отклоняется", code == 1, str(code))
    check("отказ подсказывает --out-dir", "--out-dir" in err.getvalue(), err.getvalue()[-70:])

    out = io.StringIO()
    err = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = conv.main([str(junk)])
    check("битый файл: код 1, ошибка в stderr", code == 1 and "не похоже" in err.getvalue(), str(code))

    # ── обвязка: ключи заранее не нужны, секреты не в git и не в образ ──
    section("Обвязка")
    start_bat = Path("start.bat").read_text(encoding="utf-8", errors="replace")
    check("start.bat: --convert-session идёт в session_convert.py",
          "--convert-session" in start_bat and "session_convert.py" in start_bat, "")
    check("start.bat: для конвертации ключи не спрашиваем",
          'if "%~1"=="--convert-session" set "NEEDS_KEYS=0"' in start_bat.replace("'", '"'), "")
    check("start.bat: аргументы передаются целиком (tokens=1,*)", "tokens=1,*" in start_bat, "")

    gitignore = Path(".gitignore").read_text(encoding="utf-8", errors="replace")
    dockerignore = Path(".dockerignore").read_text(encoding="utf-8", errors="replace")
    check("*.session в .gitignore", "*.session" in gitignore, "")
    check("*.session в .dockerignore", "*.session" in dockerignore, "")

    # ── переводы строк в .bat: cmd.exe рвёт LF-файлы на куски ───────────
    section("Переводы строк (Windows)")
    bat_files = sorted(str(p) for p in Path(".").rglob("*.bat") if ".git" not in p.parts)
    bat_files += sorted(str(p) for p in Path(".").rglob("*.cmd") if ".git" not in p.parts)
    check(".bat/.cmd в проекте найдены", len(bat_files) >= 2, ", ".join(bat_files))

    not_crlf, lone_lf = [], []
    for name in bat_files:
        raw = Path(name).read_bytes()
        if raw.count(b"\r\n") == 0 or raw.count(b"\n") > raw.count(b"\r\n"):
            not_crlf.append(name)
        if raw[:3] == b"\xef\xbb\xbf":
            lone_lf.append(name + " (BOM)")
    check("все .bat/.cmd с переводами CRLF (иначе cmd.exe ломает файл)", not not_crlf,
          ", ".join(not_crlf) or "ok")
    check("в .bat/.cmd нет BOM", not lone_lf, ", ".join(lone_lf) or "ok")

    attrs = Path(".gitattributes").read_text(encoding="utf-8") if Path(".gitattributes").exists() else ""
    check(".gitattributes: .bat без нормализации переводов (-text)", "*.bat -text" in attrs,
          attrs[:60] or "файла нет")
    check(".gitattributes: .sh с LF", "*.sh text eol=lf" in attrs, "")
    start_raw = Path("start.bat").read_bytes()
    check("start.bat начинается с @echo off", start_raw.startswith(b"@echo off"), start_raw[:12])

    doc = Path("session_convert.py").read_text(encoding="utf-8", errors="replace")
    # печатать можно что угодно, кроме самого ключа: dc_id, user_id, длина — можно
    leaking = [line.strip() for line in doc.splitlines()
               if "print" in line and ('data["auth_key"]' in line
                                       or "auth_key.key" in line
                                       or "session.auth_key" in line)]
    check("в конвертере ключ не печатается", not leaking, "; ".join(leaking[:2]))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\n[!] прервано")
