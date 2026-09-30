"""Конвертер сессий Pyrogram → Telethon (без повторного входа в Telegram).

Зачем. Радар работает на Telethon: core_telegram.make_client() создаёт
TelegramClient(session, api_id, api_hash), а Telethon читает только СВОЙ файл .session —
SQLite со схемой

    version (version integer primary key)                                    -- сейчас 8
    sessions (dc_id, server_address, port, auth_key, takeout_id, tmp_auth_key)

Pyrogram хранит иначе:

    version (number integer primary key)
    peers   (id, access_hash, type, username, phone_number, last_update_on)
    sessions (dc_id, test_mode, auth_key, date, user_id, is_bot)

Переносится при этом одно и то же: auth_key — постоянный ключ авторизации на дата-центр,
256 байт. Он не привязан ни к формату файла, ни к api_id приложения, поэтому телефон, код
из SMS и QR-код не нужны: берём ключ из файла Pyrogram и записываем его в файл Telethon
средствами самого Telethon (SQLiteSession), чтобы схема и версия были заведомо правильными.

Что так перенести НЕЛЬЗЯ:
  * tdata — хранилище Telegram Desktop (папка с зашифрованными файлами и локальным ключом),
    MTProto-ключ оттуда без самого десктопа не достаётся;
  * JSON с «ключами» — не формат Telethon и не формат Pyrogram;
  * StringSession (base64-строка) — другой контейнер того же ключа, здесь не разбирается.
Проще всего в этих случаях войти по QR: start.bat --login-qr --session <имя>.

Безопасность. auth_key — это полный доступ к аккаунту. Скрипт его не печатает (ни целиком,
ни кусками), не пишет в лог, не кладёт рядом временных копий и НЕ перезаписывает готовый
.session без --force (иначе легко затереть живую сессию). Файлы .session не должны попадать
ни в git (.gitignore), ни в образ (.dockerignore) — только в R2.

Запуск:
    python session_convert.py pyrogram_session.session                  # -> pyrogram_session.session (Telethon)
    python session_convert.py pyro.session --out second_session         # -> second_session.session
    python session_convert.py a.session b.session --out-dir sessions   # пачкой, имена сохраняются
    start.bat --convert-session pyro.session --out monitor_session      # то же в Windows
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

AUTH_KEY_LEN = 256          # длина постоянного MTProto-ключа, байт
DC_PORT = 443

# Боевые дата-центры Telegram (публичные адреса, видны на my.telegram.org/apps →
# «Available MTProto servers» → Production configuration). Telethon при подключении всё
# равно уточняет адреса через help.getConfig, но оставить поле пустым нельзя: клиент
# идёт в тот DC, на который выпущен auth_key.
DC_IPV4 = {
    1: "149.154.175.50",
    2: "149.154.167.50",
    3: "149.154.175.100",
    4: "149.154.167.91",
    5: "91.108.56.130",
}

# колонки таблицы sessions — по ним отличаем форматы (см. sniff_session)
PYROGRAM_COLUMNS = {"dc_id", "test_mode", "auth_key", "date", "user_id", "is_bot"}
TELETHON_COLUMNS = {"dc_id", "server_address", "port", "auth_key"}


class SessionConvertError(RuntimeError):
    """Понятная ошибка конвертации: текст показываем пользователю вместо трейсбека."""


def _is_sqlite(path: str) -> bool:
    """Файл начинается с заголовка SQLite? (tdata и JSON так не выглядят)."""
    try:
        with open(path, "rb") as handle:
            return handle.read(16) == b"SQLite format 3\x00"
    except OSError:
        return False


def _columns(path: str) -> set[str]:
    """Колонки таблицы sessions; пустой набор, если таблицы нет."""
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        raise SessionConvertError(f"не удалось открыть {path}: {exc}") from exc
    try:
        rows = conn.execute("pragma table_info(sessions)").fetchall()
    except sqlite3.Error as exc:
        raise SessionConvertError(f"{path}: не читается как база SQLite ({exc})") from exc
    finally:
        conn.close()
    return {str(row[1]) for row in rows}


def sniff_session(path: str) -> str:
    """Определяет формат файла сессии: "pyrogram" | "telethon" | "unknown".

    Формат узнаётся по колонкам таблицы sessions, а не по имени файла: имена одинаковые
    (<что-то>.session), а содержимое разное, и ошибка здесь стоит аккаунта.
    """
    if not os.path.exists(path):
        raise SessionConvertError(f"файла нет: {path}")
    if not _is_sqlite(path):
        return "unknown"
    columns = _columns(path)
    if TELETHON_COLUMNS <= columns:
        return "telethon"
    if PYROGRAM_COLUMNS <= columns:
        return "pyrogram"
    return "unknown"


def read_pyrogram(path: str) -> dict:
    """Достаёт из сессии Pyrogram то, что нужно Telethon'у.

    Возвращает {"dc_id", "auth_key", "user_id", "is_bot", "test_mode", "date", "rows"}.
    auth_key наружу не печатается — только длина.
    """
    kind = sniff_session(path)
    if kind == "telethon":
        raise SessionConvertError(
            f"{path}: это уже сессия Telethon — конвертировать не нужно, её можно грузить в R2 "
            f"как есть (npx wrangler r2 object put …)")
    if kind == "unknown":
        raise SessionConvertError(
            f"{path}: не похоже на сессию Pyrogram (нет таблицы sessions с колонками "
            f"{', '.join(sorted(PYROGRAM_COLUMNS))}). tdata и JSON так не конвертируются — "
            f"нужен вход по QR: start.bat --login-qr --session <имя>")

    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        rows = conn.execute("select dc_id, test_mode, auth_key, date, user_id, is_bot "
                            "from sessions").fetchall()
    finally:
        conn.close()

    rows = [row for row in rows if row and row[2]]
    if not rows:
        raise SessionConvertError(f"{path}: в таблице sessions пусто — ключа авторизации нет, "
                                  f"входить заново: start.bat --login-qr --session <имя>")
    if len(rows) > 1:
        # бывает у старых Pyrogram-сессий: берём самую свежую по date
        rows.sort(key=lambda row: int(row[3] or 0), reverse=True)
    dc_id, test_mode, auth_key, date, user_id, is_bot = rows[0]

    if not isinstance(auth_key, (bytes, bytearray)) or len(auth_key) != AUTH_KEY_LEN:
        got = len(auth_key) if isinstance(auth_key, (bytes, bytearray)) else "не байты"
        raise SessionConvertError(f"{path}: auth_key должен быть {AUTH_KEY_LEN} байт, а там {got} "
                                  f"— файл повреждён или это не сессия Pyrogram")
    if int(test_mode or 0) == 1:
        raise SessionConvertError(f"{path}: сессия тестового дата-центра (test_mode=1). Радар "
                                  f"работает с боевым Telegram: войди заново по QR "
                                  f"(start.bat --login-qr --session <имя>)")
    dc_id = int(dc_id or 0)
    if dc_id not in DC_IPV4:
        raise SessionConvertError(f"{path}: dc_id={dc_id} — такого дата-центра нет "
                                  f"(известны {', '.join(str(x) for x in sorted(DC_IPV4))})")

    return {"dc_id": dc_id, "auth_key": bytes(auth_key), "user_id": user_id,
            "is_bot": int(is_bot or 0), "test_mode": int(test_mode or 0),
            "date": int(date or 0), "rows": len(rows)}


def out_path(source: str, out: str = "", out_dir: str = "") -> str:
    """Куда писать результат.

    Приоритет: --out <имя> → исходное имя в --out-dir → имя входного файла.
    .session добавляется сам (Telethon делает так же); путь с каталогом сохраняется.
    """
    name = (out or "").strip() or (os.path.basename(source) if out_dir else source)
    if not name.strip():
        raise SessionConvertError("не задано имя результата")
    if not name.endswith(".session"):
        name = f"{name}.session"
    return os.path.join(out_dir, os.path.basename(name)) if out_dir else name


def convert(source: str, out: str = "", out_dir: str = "", force: bool = False,
            print_fn=print) -> str:
    """Переносит ключ из сессии Pyrogram в файл Telethon. Возвращает путь к результату."""
    data = read_pyrogram(source)
    target = out_path(source, out, out_dir)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    # Конвертация «на месте» (то же имя) затрёт исходный файл Pyrogram — исходник может
    # понадобиться другим скриптам, поэтому без --force не делаем.
    in_place = os.path.abspath(target) == os.path.abspath(source)
    if os.path.exists(target) and not force:
        raise SessionConvertError(
            (f"{target} — это исходный файл Pyrogram: конвертация на месте затрёт его. "
             f"Задай имя результата: --out <имя>. Для пачки файлов — каталог: --out-dir sessions. "
             f"Исходник точно не нужен: --force"
             if in_place else
             f"{target} уже существует — не перезаписываю (это может быть рабочая сессия). "
             f"Другое имя: --out <имя>; перезаписать: --force"))

    # пишем средствами Telethon: схема, версия и порядок колонок будут заведомо верными
    from telethon.crypto import AuthKey
    from telethon.sessions import SQLiteSession

    session = SQLiteSession(target[:-len(".session")] if target.endswith(".session") else target)
    # set_dc() — единственный штатный способ заполнить адрес сессии: у dc_id/server_address/port
    # нет сеттеров, они заполняются только этим вызовом.
    session.set_dc(data["dc_id"], DC_IPV4[data["dc_id"]], DC_PORT)
    session.auth_key = AuthKey(data["auth_key"])
    session.save()

    # проверка: перечитываем результат и сверяем ключ побайтово (без сети)
    check = SQLiteSession(target[:-len(".session")] if target.endswith(".session") else target)
    if not check.auth_key or check.auth_key.key != data["auth_key"] or check.dc_id != data["dc_id"]:
        raise SessionConvertError(f"{target}: записался, но перечитался не тем ключом — "
                                  f"не используй файл, войди по QR заново")
    if check.server_address != DC_IPV4[data["dc_id"]] or int(check.port or 0) != DC_PORT:
        raise SessionConvertError(f"{target}: адрес дата-центра записался неверно")

    print_fn(f"[+] {source}: Pyrogram, DC {data['dc_id']}, user_id {data['user_id'] or '—'}, "
             f"auth_key {AUTH_KEY_LEN} байт"
             + (f" (строк в sessions: {data['rows']}, взята свежая)" if data["rows"] > 1 else ""))
    print_fn(f"[+] записано: {target} — Telethon, server_address {DC_IPV4[data['dc_id']]}:{DC_PORT}")
    print_fn(f"[i] ключ перечитан и совпал побайтово; вход в Telegram не выполнялся")
    return target


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(
        description="Конвертер сессий Pyrogram → Telethon (auth_key переносится, вход не нужен)")
    parser.add_argument("sources", nargs="+", metavar="СЕССИЯ",
                        help="файл(ы) сессии Pyrogram (*.session)")
    parser.add_argument("--out", default="", metavar="ИМЯ",
                        help="имя результата без .session (только для одного входного файла)")
    parser.add_argument("--out-dir", default="", metavar="КАТАЛОГ",
                        help="куда сложить результаты (для пачки файлов; имена сохраняются)")
    parser.add_argument("--force", action="store_true",
                        help="перезаписать существующий .session (по умолчанию не перезаписываем)")
    args = parser.parse_args(argv)

    if args.out and len(args.sources) > 1:
        print("[!] --out задаёт одно имя: для пачки файлов используй --out-dir <каталог>",
              file=sys.stderr)
        return 2

    print("[i] auth_key — полный доступ к аккаунту: он не печатается и не копируется никуда, "
          "кроме файла результата")
    made, failed = [], []
    for source in args.sources:
        try:
            made.append(convert(source, out=args.out, out_dir=args.out_dir, force=args.force))
        except SessionConvertError as exc:
            failed.append(source)
            print(f"[!] {exc}", file=sys.stderr)

    if made:
        print("")
        print("[i] дальше — загрузить в R2 и перечислить имена сессий:")
        for target in made:
            name = os.path.basename(target)[:-len(".session")]
            print(f"    npx wrangler r2 object put radar-state/sessions/{os.path.basename(target)} "
                  f"--file {os.path.basename(target)} --remote")
        if len(made) > 1:
            names = ",".join(os.path.basename(t)[:-len(".session")] for t in made)
            print(f"    wrangler.jsonc → vars.TG_SESSION: \"{names}\" (и accounts: в sources.yaml)")
        else:
            print(f"    wrangler.jsonc → vars.TG_SESSION: \"{name}\"")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
