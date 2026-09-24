#!/usr/bin/env python3
"""
Вход в Telegram по QR-коду — без SMS и без кода из сообщения.
Плюс корректная обработка облачного пароля (2FA): подсказки вместо трейсбеков.

Запуск:
    start.bat --login-qr        (Windows)
    ./start.sh --login-qr       (mac/Linux)
    python login_qr.py

Порядок: на телефоне Telegram → Настройки → Устройства → Подключить устройство,
навести камеру на QR в терминале (окно должно быть широким). Если включён облачный
пароль — скрипт спросит его здесь же.

QR-токен живёт недолго (обычно 30–60 секунд). Если не успеть подтвердить вход,
Telegram отвечает 400 AUTH_TOKEN_EXPIRED — скрипт сам выдаёт новый QR и продолжает
ждать, ничего перезапускать не нужно. Флаг --ascii-qr рисует код символами '##'
(если в консоли QR «рябит» или не отображается).

Важно про 2FA: если пароль введён неверно, Telegram присылает на телефон уведомление
«код введён верно, но правильный пароль не указан». Много попыток подряд делать нельзя —
Telegram ограничивает ввод пароля. Если пароль забыт, его надо сбросить в настройках
Telegram (Настройки → Конфиденциальность → Облачный пароль → Забыли пароль?), см. START-HERE.md.
"""
from __future__ import annotations

import asyncio
import datetime
import getpass
import os
import subprocess
import sys
from pathlib import Path

from core_telegram import display_name, load_dotenv, make_client


def remaining_seconds(qr) -> int:
    """Сколько секунд живёт QR. В Telethon это datetime (UTC), но на всякий случай
    поддерживаем timedelta и отсутствие значения."""
    expires = getattr(qr, "expires", None)
    try:
        if isinstance(expires, datetime.datetime):
            now = datetime.datetime.now(tz=expires.tzinfo or datetime.timezone.utc)
            return max(5, int((expires - now).total_seconds()))
        if isinstance(expires, datetime.timedelta):
            return max(5, int(expires.total_seconds()))
    except Exception:  # noqa: BLE001
        pass
    return 60


def render_qr(url: str, force_ascii: bool = False) -> bool:
    """Рисует QR в терминале. Если консоль не умеет юникод-блоки (старый cmd) —
    автоматически переходит на ASCII-режим. False — если библиотеки нет."""
    try:
        import qrcode
    except ImportError:
        print("[i] ставлю qrcode для отрисовки QR (один раз)...", file=sys.stderr)
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "qrcode"])
            import qrcode
        except Exception:  # noqa: BLE001
            return False

    qr = qrcode.QRCode(border=1)
    qr.add_data(url)
    qr.make(fit=True)

    if not force_ascii:
        try:
            "█▀▄".encode(sys.stdout.encoding or "utf-8")   # потянет ли консоль такие символы
            qr.print_ascii(invert=True)
            return True
        except (UnicodeEncodeError, LookupError, AttributeError):
            print("[i] Консоль не отображает юникод-блоки — рисую QR в ASCII-режиме.", file=sys.stderr)
        except Exception:  # noqa: BLE001
            pass

    for row in qr.get_matrix():                            # ASCII-фолбэк: '##' = тёмный модуль
        print("".join("##" if cell else "  " for cell in row))
    return True


async def ask_cloud_password(client, attempts: int = 3) -> bool:
    """Спрашивает облачный пароль (2FA). Возвращает True при успехе.
    Не спамит попытками: их всего три, дальше — понятная инструкция по сбросу."""
    from telethon.errors import FloodWaitError, PasswordHashInvalidError

    for attempt in range(1, attempts + 1):
        try:
            password = getpass.getpass(f"Облачный пароль (2FA), попытка {attempt}/{attempts}: ")
        except (KeyboardInterrupt, EOFError):
            return False
        if not password:
            print("[!] Пустой пароль — не подойдёт. Проверь раскладку клавиатуры (пароль чувствителен к языку).")
            continue
        try:
            await client.sign_in(password=password)
            return True
        except PasswordHashInvalidError:
            print(f"[!] Пароль неверный ({attempt}/{attempts}).")
            print("    Частая причина — включённая русская раскладка: пароль чувствителен к языку и регистру.")
        except FloodWaitError as exc:
            print(f"[!] Telegram просит подождать {exc.seconds} с перед следующей попыткой.")
            await asyncio.sleep(exc.seconds + 5)
    return False


PASSWORD_HELP = """
────────────────────────────────────────────────────────────────────────
Что делать с облачным паролем (2FA):

1. Если пароль известен — просто запусти снова:  start.bat --login-qr
   Вводи его внимательно: он скрыт при вводе, чувствителен к регистру
   и к раскладке клавиатуры (частая ошибка — пароль набран в русской раскладке).

2. Если пароль забыт — сбросить его можно только через Telegram:
   Настройки → Конфиденциальность и безопасность → Облачный пароль →
   «Забыли пароль?» → восстановить по e-mail (если он привязан) либо
   сбросить с ожиданием 7 дней (Telegram так защищает аккаунт).
   После сброса вернись сюда и снова запусти: start.bat --login-qr

3. Чего делать НЕ надо: подбирать пароль перебором. Telegram ограничивает
   попытки ввода пароля (может включиться FloodWait на часы), а при включённой
   опции «самоуничтожение» аккаунт вообще сбрасывается.
────────────────────────────────────────────────────────────────────────
"""


def force_ascii_qr() -> bool:
    return "--ascii-qr" in sys.argv


def session_from_argv() -> str:
    """Имя файла сессии: --session second_session (или TG_SESSION из .env).

    Так второй аккаунт входит своей сессией, не трогая первый:
        start.bat --login-qr --session second_session
    """
    argv = sys.argv[1:]
    for i, arg in enumerate(argv):
        if arg == "--session" and i + 1 < len(argv):
            return argv[i + 1]
        if arg.startswith("--session="):
            return arg.split("=", 1)[1]
    return os.getenv("TG_SESSION", "monitor_session")


async def main() -> int:
    load_dotenv()
    api_id, api_hash = os.getenv("TG_API_ID"), os.getenv("TG_API_HASH")
    if not api_id or not api_hash:
        print("[!] Нет TG_API_ID / TG_API_HASH: заполни .env (см. START-HERE.md, шаг 4).", file=sys.stderr)
        return 1

    from telethon.errors import (AuthKeyDuplicatedError, AuthTokenExpiredError,
                                 AuthTokenInvalid2Error, AuthTokenInvalidError, FloodWaitError,
                                 PhoneNumberBannedError, RPCError, SessionPasswordNeededError)

    session = session_from_argv()
    client = make_client(session, int(api_id), api_hash, delay=1.0)

    try:
        await client.connect()
        authorized = await client.is_user_authorized()
    except AuthKeyDuplicatedError:
        print(f"[!] Сессия {session}.session повреждена (ключ использован с другого IP/устройства).")
        print(f"    Удали файл {session}.session и запусти снова: start.bat --login-qr")
        return 1
    except RPCError as exc:
        print(f"[!] Не удалось подключиться к Telegram: {type(exc).__name__} {exc}")
        print("    Проверь интернет/VPN и попробуй ещё раз.")
        return 1

    if authorized:
        me = await client.get_me()
        print(f"[+] Сессия {session}.session активна: {display_name(me)} (id={me.id}). Вход не нужен.")
        await client.disconnect()
        return 0

    print(f"[i] Ключи и сеть в порядке. Входим по QR в сессию {session}.session.\n")
    try:
        qr = await client.qr_login()
    except PhoneNumberBannedError:
        print("[!] Этот номер заблокирован Telegram для входа через API. Нужен другой номер.")
        return 1

    signed_in = False
    need_password = False
    scan_count = 0
    try:
        print("Готовься отсканировать QR сразу после его появления: код живёт недолго,")
        print("а на телефоне после сканирования нужно ещё успеть нажать «Подтвердить».\n")
        while True:
            scan_count += 1
            render_qr(qr.url, force_ascii=force_ascii_qr())
            print("\n>>> На телефоне: Telegram → Настройки → Устройства → Подключить устройство,")
            print(">>> затем сканируй QR выше и нажми «Подтвердить».")
            print(">>> Либо открой на авторизованном телефоне ссылку ниже (можно отправить её")
            print(">>> себе в «Избранное» и тапнуть по ней):")
            print(f">>> {qr.url}\n")
            print(f"Жду подтверждения... (QR №{scan_count}, Ctrl+C — отменить)")
            try:
                await qr.wait(timeout=remaining_seconds(qr))
            except asyncio.TimeoutError:
                print(f"\n[i] QR №{scan_count} истёк по времени — выдаю новый. Это нормально: "
                      "сканируй тот, что появится ниже.")
                qr = await qr.recreate()
                continue
            except (AuthTokenExpiredError, AuthTokenInvalidError, AuthTokenInvalid2Error):
                print(f"\n[i] Telegram ответил AUTH_TOKEN_EXPIRED: QR №{scan_count} устарел "
                      "(сканирование или подтверждение заняло слишком много времени).")
                print("    Выдаю новый QR — просто отсканируй его и сразу подтверди.")
                qr = await qr.recreate()
                continue
            break
        signed_in = True
    except SessionPasswordNeededError:
        print("\n[i] QR подтверждён, но включён облачный пароль (2FA).")
        need_password = True
        signed_in = await ask_cloud_password(client)
    except PhoneNumberBannedError:
        print("[!] Номер заблокирован Telegram для API-входа. Нужен другой номер.")
    except FloodWaitError as exc:
        print(f"[!] Слишком много попыток входа: Telegram просит подождать {exc.seconds} с.")
        print("    Это не ошибка настроек — просто подожди и запусти снова.")
    except KeyboardInterrupt:
        print("\n[!] Отменено пользователем.")
    except RPCError as exc:
        print(f"[!] Ошибка Telegram: {type(exc).__name__} {exc}")

    if not signed_in:
        await client.disconnect()
        if need_password:
            print(PASSWORD_HELP)
        print("[i] Вход не завершён. Запусти снова: start.bat --login-qr")
        return 1

    me = await client.get_me()
    print(f"\n[+] Вход выполнен: {display_name(me)} (id={me.id}).")
    print(f"[+] Сессия сохранена: {session}.session — код и пароль больше не понадобятся.")
    print("\nДальше:")
    print("    start.bat --once --catchup 20      прочитать чаты и показать совпадения")
    print("    start.bat --notify bot             работать и уведомлять в Telegram")
    await client.disconnect()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        print("\n[!] Отменено. Запусти снова: start.bat --login-qr")
