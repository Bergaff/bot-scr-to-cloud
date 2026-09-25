#!/usr/bin/env python3
"""Мастер авторизации: от пустой папки до рабочей сессии и настроенного бота.

Запуск:
    start.bat --login           (Windows)
    ./start.sh --login          (mac/Linux)
    python login_wizard.py

Что делает по шагам (каждый можно пропустить, нажав Enter):
  1. проверяет `.env`: если файла нет — создаёт из `.env.example`;
  2. спрашивает `api_id` и `api_hash` и объясняет, где их взять (одна минута, бесплатно);
     проверяет формат, пишет значения в `.env`;
  3. спрашивает имя сессии (`monitor_session`; для второго аккаунта — `second_session`);
  4. входит по QR-коду (тот же код, что `start.bat --login-qr`), включая облачный пароль 2FA;
  5. показывает, кто вошёл (имя, @username, id), — это и есть проверка сессии;
  6. предлагает настроить бота: токен от @BotFather → `chat_id` мастер определит сам
     (попросит нажать «Старт» в боте) и запишет `TG_BOT_TOKEN` / `TG_NOTIFY_CHAT` в `.env`;
  7. печатает следующую команду.

Флаги:
    --session NAME     имя сессии (по умолчанию monitor_session или TG_SESSION из .env)
    --ascii-qr         рисовать QR символами '##' (если консоль рябит)
    --keys-only        только ключи и .env, без входа (нужно для деплоя в облако)
    --no-bot           не спрашивать токен бота и chat_id
    --no-save          ничего не писать в .env (ключи живут только в этом окне)

Про api_id/api_hash: это ключи ПРИЛОЖЕНИЯ, а не пароль от аккаунта — по ним Telethon
(и Pyrogram) говорят Telegram, какое приложение стучится. Регистрируй приложение на свой
аккаунт: «чужие» готовые ключи из интернета делят трафик со всеми, кто их использует,
и аккаунт с них слетает заметно чаще. Здесь таких ключей нет и не будет.
"""
from __future__ import annotations

import asyncio
import os
import re
import sys
from pathlib import Path

from core_telegram import load_dotenv

ENV_FILE = ".env"
ENV_EXAMPLE = ".env.example"
APPS_URL = "https://my.telegram.org/auth?to=apps"
DEFAULT_SESSION = "monitor_session"
HASH_LENGTH = 32
ID_MIN_DIGITS = 5


# --------------------------------------------------------------------------- проверка ключей

def validate_api_id(raw: str) -> tuple[bool, str]:
    """(годится, значение или причина). api_id — целое число, обычно 6–8 цифр."""
    value = (raw or "").strip().strip('"').strip("'")
    if not value:
        return False, "пусто"
    if not value.isdigit():
        return False, f"это не число: {value!r} (в my.telegram.org «App api_id» — только цифры)"
    if len(value) < ID_MIN_DIGITS:
        return False, f"похоже на обрезанное значение {value!r}: обычно {ID_MIN_DIGITS}+ цифр"
    return True, value


def validate_api_hash(raw: str) -> tuple[bool, str]:
    """(годится, значение или причина). api_hash — ровно 32 символа, только 0-9 и a-f."""
    value = (raw or "").strip().strip('"').strip("'")
    if not value:
        return False, "пусто"
    if len(value) != HASH_LENGTH:
        return False, (f"в строке {len(value)} символов вместо {HASH_LENGTH}: "
                       "скопируй «App api_hash» целиком, без пробелов")
    if not re.fullmatch(r"[0-9a-fA-F]+", value):
        return False, "встречаются символы не из 0-9/a-f: похоже, скопировалось с лишним текстом"
    return True, value.lower()


def mask_secret(value: str, keep: int = 4) -> str:
    """Показывает ключ так, чтобы его нельзя было сфотографировать с экрана целиком."""
    value = (value or "").strip()
    if not value:
        return "—"
    if len(value) <= keep + 2:
        return "…" * min(len(value), 6)
    return f"{value[:keep]}…{value[-2:]}"


def split_pasted_pair(raw: str) -> tuple[str, str | None]:
    """Одна строка вместо двух: «1234567 b221…» или «api_id: 1234567, api_hash: b221…».

    Возвращает (первое значение, второе или None). Люди копируют из my.telegram.org обе строки
    сразу, поэтому ругаться на это глупо — проще разобрать.
    """
    text = (raw or "").replace(",", " ").replace("=", " ")
    tokens = [t for t in re.split(r"\s+", text.strip()) if t and not t.lower().startswith(("api", "app", "hash", "id"))]
    if len(tokens) >= 2:
        return tokens[0], tokens[1]
    return (tokens[0] if tokens else ""), None


# --------------------------------------------------------------------------- файл .env

def env_newline(raw: bytes) -> str:
    """Перевод строки файла: .env.example в репозитории с CRLF, и .env надо оставить таким же."""
    return "\r\n" if b"\r\n" in raw else "\n"


def ensure_env_file(env_path: str = ENV_FILE, example_path: str = ENV_EXAMPLE) -> tuple[bool, str]:
    """Создаёт .env из .env.example, если файла ещё нет. Возвращает (создан ли, путь)."""
    path = Path(env_path)
    if path.exists():
        return False, str(path)
    example = Path(example_path)
    if example.exists():
        path.write_bytes(example.read_bytes())
        return True, str(path)
    path.open("w", encoding="utf-8", newline="\n").write(
        "# Ключи радара: api_id/api_hash — https://my.telegram.org/auth?to=apps\n"
        "TG_API_ID=\nTG_API_HASH=\nTG_SESSION=monitor_session\n"
    )
    return True, str(path)


def read_env_file(env_path: str = ENV_FILE) -> dict[str, str]:
    """Значения из .env (комментарии и пустые строки пропускаются)."""
    values: dict[str, str] = {}
    path = Path(env_path)
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def upsert_env(values: dict[str, str], env_path: str = ENV_FILE) -> list[str]:
    """Пишет KEY=VALUE в .env: существующие строки заменяет на месте, новые дописывает в конец.

    Комментарии, порядок и переводы строк файла сохраняются — правим только нужные ключи.
    Возвращает список фактически записанных ключей.
    """
    path = Path(env_path)
    raw = path.read_bytes() if path.exists() else b""
    newline = env_newline(raw)
    text = raw.decode("utf-8-sig") if raw else ""
    lines = text.splitlines()
    left = {key: str(value) for key, value in values.items() if value is not None}
    changed: list[str] = []
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key = stripped.partition("=")[0].strip()
        if key in left:
            lines[index] = f"{key}={left.pop(key)}"
            changed.append(key)
    for key, value in left.items():
        lines.append(f"{key}={value}")
        changed.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        handle.write(newline.join(lines) + newline)
    return changed


# --------------------------------------------------------------------------- подсказки

def api_instruction() -> str:
    """Текст «где взять api_id и api_hash» — его же цитируют START-HERE.md и README.md."""
    return "\n".join([
        "Где взять api_id и api_hash (один раз, бесплатно, около минуты):",
        f"  1. Открой {APPS_URL}",
        "  2. Войди номером телефона: код придёт в Telegram. Лучше тем аккаунтом,",
        "     который и будет работать в радаре.",
        "  3. Название приложения — ЛЮБОЕ (например «my radar»), короткое имя — латиницей.",
        "     Platform: Desktop, URL и Description можно оставить пустыми.",
        "  4. Сохрани App api_id (число) и App api_hash (32 символа).",
        "",
        "Это ключи приложения, а не пароль от аккаунта: по ним Telethon (и Pyrogram) говорят",
        "Telegram, какое приложение стучится. api_hash не публикуй; если он утечёт — приложение",
        "в my.telegram.org можно удалить и создать новое.",
        "Приложение регистрируй на СВОЙ аккаунт: «чужие» готовые ключи из интернета делят трафик",
        "со всеми, кто их использует, и аккаунт с них слетает заметно чаще.",
    ])


def next_steps(bot_ready: bool) -> str:
    """Что запускать дальше — текст печатается в конце мастера."""
    lines = [
        "Дальше:",
        "    start.bat --doctor                       проверить окружение и сессию",
        "    start.bat --once --catchup 20            разовый проход: прочитать чаты",
        "    start.bat --bot-panel --notify none      радар + бот-панель в одном окне",
    ]
    if not bot_ready:
        lines.append("    start.bat --login --keys-only            дописать токен бота и chat_id")
    return "\n".join(lines)


# --------------------------------------------------------------------------- опрос

def ask_keys(env_path: str = ENV_FILE, input_fn=input, secret_fn=None, print_fn=print,
             save: bool = True) -> tuple[str, str] | None:
    """Спрашивает api_id/api_hash, пока значения не пройдут проверку.

    Текущие значения из .env показывает маской и предлагает оставить. Пустой ввод при
    вопросе о замене = «оставить как есть». Возвращает (api_id, api_hash) или None, если
    пользователь прервал ввод.
    """
    secret_fn = secret_fn or _hidden_input
    current = read_env_file(env_path)
    have_id, have_hash = current.get("TG_API_ID", ""), current.get("TG_API_HASH", "")
    id_ok, _ = validate_api_id(have_id)
    hash_ok, _ = validate_api_hash(have_hash)

    if id_ok and hash_ok:
        print_fn(f"[i] ключи уже заданы: api_id={have_id}, api_hash={mask_secret(have_hash)}")
        answer = (input_fn("    Заменить их? [y/N] ") or "").strip().lower()
        if answer not in ("y", "yes", "да", "д"):
            _export_keys(have_id, have_hash)
            return have_id, have_hash
        print_fn("")

    if have_id and not id_ok:
        print_fn(f"[i] TG_API_ID в {env_path} не годится: {validate_api_id(have_id)[1]}")
    if have_hash and not hash_ok:
        print_fn(f"[i] TG_API_HASH в {env_path} не годится: {validate_api_hash(have_hash)[1]}")
        print_fn("    Если там пример значения (xxxx…) — нужны ключи ТВОЕГО приложения.")
    print_fn("")
    print_fn(api_instruction())
    print_fn("")
    api_id, api_hash = "", ""
    while True:
        raw = input_fn("App api_id (число из my.telegram.org): ")
        if raw is None:
            return None
        first, second = split_pasted_pair(raw)
        ok, why = validate_api_id(first)
        if not ok:
            print_fn(f"[!] api_id: {why}. Попробуй ещё раз (Ctrl+C — отменить).")
            continue
        api_id = why
        if second:
            ok_hash, hash_why = validate_api_hash(second)
            if ok_hash:
                api_hash = hash_why
                print_fn(f"[i] вижу обе строки в одной: api_hash={mask_secret(api_hash)} принят")
                break
            print_fn(f"[!] вторая строка не похожа на api_hash ({hash_why}) — спрошу отдельно")
        break

    while not api_hash:
        raw = secret_fn("App api_hash (32 символа; ввод не отображается): ")
        if raw is None:
            return None
        ok, why = validate_api_hash(raw)
        if not ok:
            print_fn(f"[!] api_hash: {why}. Попробуй ещё раз (Ctrl+C — отменить).")
            continue
        api_hash = why

    print_fn(f"[+] принято: api_id={api_id}, api_hash={mask_secret(api_hash)}")
    if save:
        changed = upsert_env({"TG_API_ID": api_id, "TG_API_HASH": api_hash}, env_path)
        print_fn(f"[+] записано в {env_path}: {', '.join(changed)}")
    else:
        print_fn("[i] в .env не пишу (--no-save): ключи живут только в этом окне")
    _export_keys(api_id, api_hash)
    return api_id, api_hash


def _hidden_input(prompt: str) -> str:
    """Ввод без эха (api_hash не должен светиться на экране/стриме). Fallback — обычный input."""
    import getpass

    try:
        return getpass.getpass(prompt)
    except Exception:                                    # noqa: BLE001 — нет терминала, Windows-чудеса
        return input(prompt)


def _export_keys(api_id: str, api_hash: str) -> None:
    """Ключи — в окружение этого процесса: вход по QR идёт сразу, без перезапуска."""
    os.environ["TG_API_ID"] = api_id
    os.environ["TG_API_HASH"] = api_hash


def ask_session(input_fn=input, print_fn=print, save: bool = True,
                env_path: str = ENV_FILE) -> str:
    """Имя файла сессии. Enter — оставить текущее (или monitor_session)."""
    current = os.getenv("TG_SESSION") or read_env_file(env_path).get("TG_SESSION") or DEFAULT_SESSION
    print_fn(f"[i] сессия — это файл {current}.session: ключ аккаунта, вход по нему больше не нужен.")
    print_fn("    Второй аккаунт? Впиши другое имя, например second_session.")
    answer = (input_fn(f"Имя сессии [{current}]: ") or "").strip()
    name = answer or current
    name = re.sub(r"[^\w.-]+", "_", name).strip("_") or DEFAULT_SESSION
    os.environ["TG_SESSION"] = name
    if save and name != current:
        upsert_env({"TG_SESSION": name}, env_path)
        print_fn(f"[+] записано в {env_path}: TG_SESSION")
    return name


# --------------------------------------------------------------------------- бот и chat_id

def chat_id_from_updates(updates: list[dict]) -> str | None:
    """chat_id владельца из getUpdates: первое личное сообщение от человека (не канал, не бот)."""
    for update in updates or []:
        payload = update or {}
        message = payload.get("message") or payload.get("edited_message") or {}
        chat = message.get("chat") or {}
        if chat.get("type") != "private":
            continue
        sender = message.get("from") or {}
        if sender.get("is_bot"):
            continue
        if chat.get("id") is None:
            continue
        return str(chat["id"])
    return None


async def detect_chat_id(token: str, print_fn=print, transport=None, rounds: int = 3,
                         timeout: int = 25) -> str | None:
    """Определяет твой chat_id сам: просит нажать «Старт» в боте и читает getUpdates.

    Так не нужен @userinfobot и не приходится гадать, какой из id вписывать в TG_NOTIFY_CHAT.
    """
    from core_telegram import bot_get_me

    ok, info = await bot_get_me(token)
    if not ok:
        print_fn(f"[!] токен бота не принят: {info}")
        print_fn("    Токен выдаёт @BotFather командой /newbot; скопируй его целиком.")
        return None
    print_fn(f"[+] бот на связи: {info}")

    if transport is None:
        from bot_panel import HttpTransport

        transport = HttpTransport(token)
    print_fn("    Открой бота в Telegram и нажми «Старт» (или напиши ему любое сообщение).")
    print_fn(f"    Жду сообщение… (до {rounds * timeout} с, Ctrl+C — пропустить)")
    offset = 0
    for _ in range(max(1, rounds)):
        try:
            updates = await transport.get_updates(offset, timeout=timeout)
        except Exception as exc:                          # noqa: BLE001
            print_fn(f"[!] не удалось прочитать апдейты: {type(exc).__name__} {exc}")
            return None
        for update in updates or []:
            offset = max(offset, int((update or {}).get("update_id", 0)) + 1)
        chat_id = chat_id_from_updates(updates)
        if chat_id:
            print_fn(f"[+] твой chat_id: {chat_id}")
            return chat_id
    print_fn("[!] сообщение от тебя так и не пришло — впиши chat_id позже вручную "
             "(например, через @userinfobot).")
    return None


async def ask_bot(env_path: str = ENV_FILE, input_fn=input, secret_fn=None, print_fn=print,
                  save: bool = True, detect=detect_chat_id) -> bool:
    """Токен бота + chat_id. Возвращает True, если уведомления ботом после этого работают."""
    secret_fn = secret_fn or _hidden_input
    current = read_env_file(env_path)
    token = current.get("TG_BOT_TOKEN", "")
    chat = current.get("TG_NOTIFY_CHAT", "")

    if token and chat:
        print_fn(f"[i] бот уже настроен: токен {mask_secret(token, 6)}, chat_id {chat}")
        answer = (input_fn("    Перенастроить? [y/N] ") or "").strip().lower()
        if answer not in ("y", "yes", "да", "д"):
            os.environ.setdefault("TG_BOT_TOKEN", token)
            os.environ.setdefault("TG_NOTIFY_CHAT", chat)
            return True

    print_fn("")
    print_fn("Уведомления в Telegram — по желанию, но с ними удобнее: находки приходят")
    print_fn("сообщением от бота, там же работает панель команд (/status, /accounts, /usage).")
    print_fn("Токен выдаёт @BotFather: /newbot → имя → username → токен вида 1234567890:AAH…")
    if not token:
        raw = secret_fn("Токен бота (Enter — пропустить): ") or ""
        token = raw.strip().strip('"').strip("'")
    if not token:
        print_fn("[i] пропускаю: уведомления пойдут в консоль и в hits.log (--notify both)")
        return False
    if not re.fullmatch(r"\d{6,}:[\w-]{20,}", token):
        print_fn(f"[!] токен выглядит неполным ({mask_secret(token, 6)}): скопируй строку от "
                 "@BotFather целиком. Пропускаю шаг.")
        return False

    chat_id = await detect(token, print_fn=print_fn)
    values = {"TG_BOT_TOKEN": token}
    if chat_id:
        values["TG_NOTIFY_CHAT"] = chat_id
    else:
        print_fn("[i] chat_id не определён: впиши TG_NOTIFY_CHAT в .env позже")
    os.environ["TG_BOT_TOKEN"] = token
    if chat_id:
        os.environ["TG_NOTIFY_CHAT"] = chat_id
    if save:
        changed = upsert_env(values, env_path)
        print_fn(f"[+] записано в {env_path}: {', '.join(changed)}")
    return bool(chat_id)


# --------------------------------------------------------------------------- сборка всего

def parse_flags(argv: list[str] | None = None) -> dict:
    """Флаги мастера. Всё необязательное, чтобы «start.bat --login» просто работал."""
    argv = list(sys.argv[1:] if argv is None else argv)
    flags = {
        "keys_only": "--keys-only" in argv,
        "no_bot": "--no-bot" in argv,
        "save": "--no-save" not in argv,
        "ascii_qr": "--ascii-qr" in argv,
        "session": "",
    }
    for index, arg in enumerate(argv):
        if arg == "--session" and index + 1 < len(argv):
            flags["session"] = argv[index + 1]
        elif arg.startswith("--session="):
            flags["session"] = arg.split("=", 1)[1]
    return flags


async def wizard(argv: list[str] | None = None, input_fn=input, secret_fn=None, print_fn=print,
                 login_fn=None, env_path: str = ENV_FILE, example_path: str = ENV_EXAMPLE,
                 detect=detect_chat_id) -> int:
    """Весь путь: .env → ключи → сессия → вход по QR → бот. Возвращает код выхода."""
    flags = parse_flags(argv)
    secret_fn = secret_fn or _hidden_input
    load_dotenv(env_path)

    print_fn("=" * 68)
    print_fn("  Мастер авторизации радара: ключи приложения, вход в аккаунт, бот")
    print_fn("=" * 68)
    print_fn("")

    if flags["save"]:
        created, path = ensure_env_file(env_path, example_path)
        if created:
            print_fn(f"[+] создан {path} (образец .env.example): ключи будут храниться там")
        else:
            print_fn(f"[i] использую {path}")
    else:
        print_fn(f"[i] --no-save: {env_path} не создаю и не правлю — ключи живут в этом окне")

    keys = ask_keys(env_path=env_path, input_fn=input_fn, secret_fn=secret_fn,
                    print_fn=print_fn, save=flags["save"])
    if keys is None:
        print_fn("[!] ввод прерван: запусти снова start.bat --login", file=sys.stderr)
        return 1

    session = flags["session"] or ask_session(input_fn=input_fn, print_fn=print_fn,
                                              save=flags["save"], env_path=env_path)
    os.environ["TG_SESSION"] = session

    if flags["keys_only"]:
        print_fn("")
        print_fn(f"[+] ключи готовы (--keys-only: вход не выполняю). Сессия: {session}.session")
        print_fn("    Для облака дальше — DEPLOY.md, шаг 3: войти локально и залить сессию в R2.")
        return 0

    print_fn("")
    print_fn(f"[i] ключи и .env готовы. Вхожу по QR в сессию {session}.session…")
    if flags["ascii_qr"] and "--ascii-qr" not in sys.argv:
        sys.argv.append("--ascii-qr")       # login_qr.py читает флаг из argv

    if login_fn is None:
        import login_qr

        login_fn = login_qr.main
    code = await login_fn()
    if code != 0:
        print_fn("")
        print_fn("[!] вход не завершён — сообщение выше объясняет причину. "
                 "Повторить: start.bat --login", file=sys.stderr)
        return int(code or 1)

    bot_ready = False
    if not flags["no_bot"]:
        bot_ready = await ask_bot(env_path=env_path, input_fn=input_fn, secret_fn=secret_fn,
                                  print_fn=print_fn, save=flags["save"], detect=detect)

    print_fn("")
    print_fn(next_steps(bot_ready))
    return 0


def main() -> int:
    try:
        return asyncio.run(wizard())
    except KeyboardInterrupt:
        print("\n[!] отменено. Запусти снова: start.bat --login", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
