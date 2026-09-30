#!/usr/bin/env python3
"""
Офлайн-тесты мастера авторизации (login_wizard.py): сеть, Telegram и аккаунт НЕ нужны.

Проверяется то, что ломается чаще всего и что нельзя проверить «на живом» Telegram:
  * валидация api_id/api_hash — обрезанные ключи самая частая причина «не работает»;
  * запись в .env: существующие строки заменяются на месте, комментарии и CRLF не ломаются;
  * определение chat_id из getUpdates (личное сообщение от человека, не канал и не бот);
  * опрос с повтором на неверном вводе и без записи в .env при --no-save;
  * весь мастер целиком на заглушках: --keys-only, --no-bot, проваленный вход;
  * обвязка: start.bat/start.sh ведут --login в мастер и не требуют ключей заранее;
  * чужих api_id/api_hash в проекте нет — приложение пользователь регистрирует своё.

Запуск:  python3 selftest_login.py
"""
from __future__ import annotations

import asyncio
import os
import shutil
from pathlib import Path

import core_telegram
import login_wizard as wizard

WORKDIR = Path(f"tests/_tmp_login_{os.getpid()}")
API_ID = "1234567"
API_HASH = "0123456789abcdef0123456789abcdef"      # 32 символа, выдуманное значение
TOKEN = "1234567890:AAH-test-token-abcdefghij"
CHAT = "999888777"
FOREIGN_ID = "14369082"                                  # «чужие» ключи из интернета
FOREIGN_HASH = "b221b4f79223104634a800ecd4a9c3e5"        # в проекте их быть не должно

checks: list[tuple[str, bool, str]] = []


def section(title: str) -> None:
    print(f"\n── {title} " + "─" * max(0, 66 - len(title)))


def check(name: str, ok: bool, detail: str = "") -> None:
    checks.append((name, bool(ok), detail))


class FakeInput:
    """input()/getpass() по списку ответов. Список кончился — возвращает '' (как голый Enter)."""

    def __init__(self, answers=None):
        self.answers = list(answers or [])
        self.prompts: list[str] = []

    def __call__(self, prompt: str = "") -> str:
        self.prompts.append(prompt)
        return self.answers.pop(0) if self.answers else ""


class FakeTransport:
    """getUpdates без сети: на каждый круг опроса — своя порция апдейтов."""

    def __init__(self, batches=None, fail: bool = False):
        self.batches = [list(batch) for batch in (batches or [])]
        self.fail = fail
        self.offsets: list[int] = []

    async def get_updates(self, offset: int, timeout: int | None = None) -> list[dict]:
        self.offsets.append(offset)
        if self.fail:
            raise RuntimeError("401 Unauthorized")
        return self.batches.pop(0) if self.batches else []


def private_update(update_id: int, chat_id: str = CHAT, is_bot: bool = False,
                   chat_type: str = "private", edited: bool = False) -> dict:
    key = "edited_message" if edited else "message"
    return {"update_id": update_id,
            key: {"message_id": update_id,
                  "chat": {"id": int(chat_id), "type": chat_type},
                  "from": {"id": int(chat_id), "is_bot": is_bot, "first_name": "Владелец"},
                  "text": "/start"}}


def silent(*_args, **_kwargs) -> None:
    """Вывод мастера в тестах не нужен: проверяем значения, а не текст."""


def env_file(name: str = ".env", content: str | None = None) -> str:
    path = WORKDIR / name
    if content is not None:
        path.write_text(content, encoding="utf-8", newline="")
    return str(path)


def make_env(content: str | None = None, name: str = ".env") -> str:
    WORKDIR.mkdir(parents=True, exist_ok=True)
    return env_file(name, content if content is not None else
                    f"# ключи\nTG_API_ID={API_ID}\nTG_API_HASH={API_HASH}\nTG_SESSION=monitor_session\n")


async def main() -> int:
    saved_environ = dict(os.environ)
    shutil.rmtree(WORKDIR, ignore_errors=True)
    WORKDIR.mkdir(parents=True)
    original_get_me = core_telegram.bot_get_me
    try:
        # ------------------------------------------------- 1. проверка ключей
        section("1. api_id и api_hash: валидация")
        ok, value = wizard.validate_api_id(f" {API_ID} ")
        check("api_id принимается с пробелами по краям", ok and value == API_ID, value)
        ok, value = wizard.validate_api_id(f'"{API_ID}"')
        check("api_id принимается в кавычках (копипаст из PowerShell)", ok and value == API_ID, value)
        ok, why = wizard.validate_api_id("")
        check("пустой api_id отклонён", not ok and why == "пусто", why)
        ok, why = wizard.validate_api_id("myradar")
        check("api_id не число — отклонён с понятной причиной", not ok and "не число" in why, why)
        ok, why = wizard.validate_api_id("123")
        check("обрезанный api_id отклонён (частая ошибка копипаста)", not ok and "обрезан" in why, why)
        ok, value = wizard.validate_api_id("14369082")
        check("8 цифр — валидный api_id (формат не привязан к длине 7)", ok and value == "14369082", value)

        ok, value = wizard.validate_api_hash(API_HASH.upper())
        check("api_hash приводится к нижнему регистру", ok and value == API_HASH, value)
        ok, why = wizard.validate_api_hash(API_HASH[:-1])
        check(f"api_hash из 31 символа отклонён с напоминанием про {wizard.HASH_LENGTH}",
              not ok and "31" in why and "32" in why, why)
        ok, why = wizard.validate_api_hash("z" * 32)
        check("api_hash с символами не из 0-9/a-f отклонён", not ok and "0-9/a-f" in why, why)
        ok, why = wizard.validate_api_hash(f"{API_HASH}  ")
        check("api_hash с хвостовыми пробелами принимается", ok and why == API_HASH, why)

        check("маска скрывает середину хеша", wizard.mask_secret(API_HASH) == "0123…ef",
              wizard.mask_secret(API_HASH))
        check("короткое значение маскируется целиком (нечего показывать)",
                "…" in wizard.mask_secret("abc") and "abc" not in wizard.mask_secret("abc"),
                wizard.mask_secret("abc"))
        check("пустое значение — прочерк, а не пустая строка", wizard.mask_secret("") == "—", "")

        first, second = wizard.split_pasted_pair(f"{API_ID} {API_HASH}")
        check("обе строки из my.telegram.org можно вставить одним куском",
              first == API_ID and second == API_HASH, f"{first} / {second}")
        first, second = wizard.split_pasted_pair(f"App api_id: {API_ID}, App api_hash: {API_HASH}")
        check("подписи «App api_id:» не мешают разбору", first == API_ID and second == API_HASH,
              f"{first} / {second}")
        first, second = wizard.split_pasted_pair(API_ID)
        check("одно значение вторым не дополняется", first == API_ID and second is None, str(second))

        # ------------------------------------------------- 2. файл .env
        section("2. .env: создание и правка на месте")
        path = env_file(".env", None)
        Path(path).unlink(missing_ok=True)
        created, _ = wizard.ensure_env_file(path, str(Path(".env.example")))
        text = Path(path).read_text(encoding="utf-8")
        check(".env создаётся из .env.example, если файла нет",
              created and "TG_API_ID" in text and "TG_API_HASH" in text, text.splitlines()[0][:60])
        check("созданный .env не переписывается повторно",
              wizard.ensure_env_file(path, str(Path(".env.example")))[0] is False, "")
        check("в .env.example есть все ключи, которые спрашивает мастер",
              all(key in text for key in ("TG_API_ID", "TG_API_HASH", "TG_BOT_TOKEN",
                                          "TG_NOTIFY_CHAT", "TG_SESSION")), "")

        missing = env_file(".env-none", None)
        Path(missing).unlink(missing_ok=True)
        wizard.ensure_env_file(missing, str(WORKDIR / "нет-такого-файла"))
        text = Path(missing).read_text(encoding="utf-8")
        check("без .env.example создаётся минимальный .env с теми же ключами",
              all(key in text for key in ("TG_API_ID", "TG_API_HASH", "TG_SESSION")), "")

        path = make_env("# мой комментарий\nTG_API_ID=old\nTG_API_HASH=old\nTG_SESSION=main\n"
                        "TG_BOT_TOKEN=\n")
        changed = wizard.upsert_env({"TG_API_ID": API_ID, "TG_NOTIFY_CHAT": CHAT}, path)
        text = Path(path).read_text(encoding="utf-8")
        check("существующий ключ заменяется на месте, новый дописывается в конец",
              text.count("TG_API_ID") == 1 and f"TG_API_ID={API_ID}" in text
              and text.rstrip().endswith(f"TG_NOTIFY_CHAT={CHAT}"), text.replace("\n", " | ")[:120])
        check("комментарии и соседние строки не пострадали",
              "# мой комментарий" in text and "TG_SESSION=main" in text
              and "TG_BOT_TOKEN=" in text and sorted(changed) == ["TG_API_ID", "TG_NOTIFY_CHAT"],
              str(changed))
        check("старое значение заменённого ключа не осталось в файле",
              "TG_API_ID=old" not in text and "TG_API_HASH=old" in text,
              "правим только названные ключи")

        crlf = env_file(".env-crlf", "# c\r\nTG_API_ID=1\r\nTG_API_HASH=2\r\n")
        wizard.upsert_env({"TG_API_ID": API_ID}, crlf)
        raw = Path(crlf).read_bytes()
        check("перевод строк CRLF сохраняется (иначе Блокнот покажет «всё в одну строку»)",
              b"\r\n" in raw and b"\n\n" not in raw and raw.count(b"\r\n") == 3, repr(raw[-24:]))

        values = wizard.read_env_file(make_env('# коммент\n\nTG_API_ID="1234567"\nTG_EMPTY=\n'))
        check("read_env_file пропускает комментарии и пустые строки, снимает кавычки",
              values == {"TG_API_ID": "1234567", "TG_EMPTY": ""}, str(values))

        # ------------------------------------------------- 3. chat_id из getUpdates
        section("3. chat_id из getUpdates")
        check("личное сообщение от человека даёт chat_id",
              wizard.chat_id_from_updates([private_update(1)]) == CHAT, "")
        check("пост из канала/группы за chat_id не принимается",
              wizard.chat_id_from_updates([private_update(1, chat_type="channel")]) is None, "")
        check("сообщение от бота в личке не принимается (иначе панель ответит сама себе)",
              wizard.chat_id_from_updates([private_update(1, is_bot=True)]) is None, "")
        check("edited_message тоже считается",
              wizard.chat_id_from_updates([private_update(1, edited=True)]) == CHAT, "")
        check("пустой список — None, а не исключение", wizard.chat_id_from_updates([]) is None, "")
        check("первым идёт канал, затем личка — берётся личка",
              wizard.chat_id_from_updates([private_update(1, chat_type="supergroup"),
                                           private_update(2, chat_id="555444333")]) == "555444333", "")

        # ------------------------------------------------- 4. detect_chat_id
        section("4. Определение chat_id через бота")

        async def get_me_ok(token: str) -> tuple[bool, str]:
            return True, "@radar_bot"

        async def get_me_bad(token: str) -> tuple[bool, str]:
            return False, "401 Unauthorized"

        lines: list[str] = []
        core_telegram.bot_get_me = get_me_ok
        transport = FakeTransport([[private_update(7)]])
        found = await wizard.detect_chat_id(TOKEN, print_fn=lines.append, transport=transport,
                                            rounds=1, timeout=1)
        check("chat_id найден: мастер сам определяет, кому слать уведомления", found == CHAT, str(found))
        check("перед поиском проверяется токен (getMe)",
              any("бот на связи" in line for line in lines), str(lines)[:80])

        transport = FakeTransport([[private_update(1, chat_type="channel")],
                                         [private_update(2)]])
        found = await wizard.detect_chat_id(TOKEN, print_fn=silent, transport=transport,
                                            rounds=2, timeout=1)
        check("второй круг опроса находит сообщение, пропущенное первым", found == CHAT, str(found))
        check("offset сдвигается, чтобы не читать одно и то же", transport.offsets == [0, 2],
              str(transport.offsets))

        transport = FakeTransport([[private_update(1, chat_type="channel")]])
        found = await wizard.detect_chat_id(TOKEN, print_fn=silent, transport=transport,
                                            rounds=1, timeout=1)
        check("нет личного сообщения — None и подсказка про @userinfobot", found is None, "")

        core_telegram.bot_get_me = get_me_bad
        lines = []
        found = await wizard.detect_chat_id(TOKEN, print_fn=lines.append,
                                            transport=FakeTransport([[private_update(1)]]),
                                            rounds=1, timeout=1)
        check("неверный токен: понятная ошибка и @BotFather в подсказке",
              found is None and any("BotFather" in line for line in lines), str(lines)[:80])

        core_telegram.bot_get_me = get_me_ok
        lines = []
        found = await wizard.detect_chat_id(TOKEN, print_fn=lines.append,
                                            transport=FakeTransport(fail=True), rounds=1, timeout=1)
        check("сбой сети при getUpdates не роняет мастера", found is None
              and any("не удалось прочитать" in line for line in lines), str(lines)[:80])

        # ------------------------------------------------- 5. опрос пользователя
        section("5. Опрос: повторы, сохранение, отказ")
        path = make_env()
        answers = FakeInput(["n"])
        result = wizard.ask_keys(env_path=path, input_fn=answers, secret_fn=answers,
                                 print_fn=silent)
        check("годные ключи в .env: Enter оставляет их как есть",
              result == (API_ID, API_HASH), str(result))
        check("ключи попали в окружение процесса — вход пойдёт сразу, без перезапуска",
              os.environ.get("TG_API_ID") == API_ID and os.environ.get("TG_API_HASH") == API_HASH, "")
        check("при согласии ничего не переспрашивается", len(answers.prompts) == 1,
              str(answers.prompts))

        path = make_env()
        answers = FakeInput(["y", API_ID, API_HASH])
        result = wizard.ask_keys(env_path=path, input_fn=answers, secret_fn=answers, print_fn=silent)
        text = Path(path).read_text(encoding="utf-8")
        check("«заменить» — спрашивает оба ключа и пишет их в .env",
              result == (API_ID, API_HASH) and f"TG_API_HASH={API_HASH}" in text, "")

        path = make_env("TG_API_ID=\nTG_API_HASH=\n")
        # один и тот же фейк отвечает и за input(), и за getpass(): неверный api_id дважды,
        # затем верный; неверный api_hash дважды, затем верный
        answers = FakeInput(["abc", "12", API_ID, "short", "z" * 32, API_HASH])
        result = wizard.ask_keys(env_path=path, input_fn=answers, secret_fn=answers, print_fn=silent)
        check("неверный ввод повторяется, а не роняет мастера (api_id и api_hash)",
              result == (API_ID, API_HASH), str(result))
        check("api_hash спрашивается скрытым вводом (не светится на экране)",
              any("не отображается" in prompt for prompt in answers.prompts), "")

        path = make_env("TG_API_ID=\nTG_API_HASH=\n")
        answers = FakeInput([f"App api_id: {API_ID}, App api_hash: {API_HASH}"])
        result = wizard.ask_keys(env_path=path, input_fn=answers, secret_fn=answers, print_fn=silent)
        check("вставка обоих значений одной строкой принимается",
              result == (API_ID, API_HASH) and f"TG_API_ID={API_ID}" in Path(path).read_text(encoding="utf-8"),
              str(result))

        path = make_env("TG_API_ID=\nTG_API_HASH=\n")
        before = Path(path).read_text(encoding="utf-8")
        answers = FakeInput([API_ID, API_HASH])
        wizard.ask_keys(env_path=path, input_fn=answers, secret_fn=answers, print_fn=silent,
                        save=False)
        check("--no-save: .env не тронут, ключи только в окружении",
              Path(path).read_text(encoding="utf-8") == before and os.environ["TG_API_ID"] == API_ID, "")

        path = make_env()
        os.environ.pop("TG_SESSION", None)          # окружение не должно решать за .env
        answers = FakeInput([""])
        name = wizard.ask_session(input_fn=answers, print_fn=silent, env_path=path)
        check("Enter оставляет текущее имя сессии", name == "monitor_session", name)
        answers = FakeInput(["second session!"])
        name = wizard.ask_session(input_fn=answers, print_fn=silent, env_path=path)
        check("имя сессии приводится к безопасному для файла",
              name == "second_session_" or name == "second_session", name)
        check("новое имя сессии записано в .env",
              f"TG_SESSION={name}" in Path(path).read_text(encoding="utf-8"), "")

        path = make_env(f"TG_API_ID={API_ID}\nTG_BOT_TOKEN={TOKEN}\nTG_NOTIFY_CHAT={CHAT}\n")
        answers = FakeInput(["n"])
        ready = await wizard.ask_bot(env_path=path, input_fn=answers, secret_fn=answers,
                                     print_fn=silent)
        check("бот уже настроен: Enter ничего не переспрашивает", ready is True, "")

        path = make_env(f"TG_API_ID={API_ID}\nTG_BOT_TOKEN=\nTG_NOTIFY_CHAT=\n")
        answers = FakeInput([TOKEN])
        lines = []
        ready = await wizard.ask_bot(env_path=path, input_fn=answers, secret_fn=answers,
                                     print_fn=lines.append,
                                     detect=lambda token, print_fn=None: _async_value(CHAT))
        text = Path(path).read_text(encoding="utf-8")
        check("токен + определённый chat_id записываются в .env",
              ready is True and f"TG_BOT_TOKEN={TOKEN}" in text and f"TG_NOTIFY_CHAT={CHAT}" in text, "")

        path = make_env(f"TG_API_ID={API_ID}\nTG_BOT_TOKEN=\n")
        answers = FakeInput([TOKEN])
        ready = await wizard.ask_bot(env_path=path, input_fn=answers, secret_fn=answers,
                                     print_fn=silent,
                                     detect=lambda token, print_fn=None: _async_value(None))
        text = Path(path).read_text(encoding="utf-8")
        check("chat_id не определился — токен всё равно сохранён, шаг не провален",
              ready is False and f"TG_BOT_TOKEN={TOKEN}" in text and "TG_NOTIFY_CHAT" not in text, "")

        path = make_env(f"TG_API_ID={API_ID}\nTG_BOT_TOKEN=\n")
        answers = FakeInput(["123:обрывок"])
        calls: list[str] = []
        ready = await wizard.ask_bot(env_path=path, input_fn=answers, secret_fn=answers,
                                     print_fn=silent,
                                     detect=lambda token, print_fn=None: calls.append(token)
                                     or _async_value(CHAT))
        check("обрывок токена не уходит в Telegram: шаг пропущен с подсказкой",
              ready is False and not calls, str(calls))

        answers = FakeInput([""])
        ready = await wizard.ask_bot(env_path=path, input_fn=answers, secret_fn=answers,
                                     print_fn=silent)
        check("Enter вместо токена — уведомления остаются в консоли, мастер не падает",
              ready is False, "")

        # ------------------------------------------------- 6. мастер целиком
        section("6. Мастер целиком (на заглушках)")
        calls: list[str] = []

        async def login_ok() -> int:
            calls.append("login")
            return 0

        async def login_fail() -> int:
            calls.append("login")
            return 1

        async def detect_ok(token: str, print_fn=None, **kwargs) -> str:
            calls.append("detect")
            return CHAT

        path = make_env()
        answers = FakeInput(["n", "", TOKEN, "n"])
        code = await wizard.wizard(argv=[], input_fn=answers, secret_fn=answers, print_fn=silent,
                                   login_fn=login_ok, env_path=path, detect=detect_ok)
        check("полный путь .env → ключи → сессия → вход → бот заканчивается нулём",
              code == 0 and calls == ["login", "detect"], f"code={code}, вызовы={calls}")

        calls.clear()
        path = make_env()
        answers = FakeInput(["n", ""])
        code = await wizard.wizard(argv=["--no-bot"], input_fn=answers, secret_fn=answers,
                                   print_fn=silent, login_fn=login_ok, env_path=path,
                                   detect=detect_ok)
        check("--no-bot: вход есть, бота не спрашиваем", code == 0 and calls == ["login"], str(calls))

        calls.clear()
        path = make_env()
        answers = FakeInput(["n", ""])
        code = await wizard.wizard(argv=["--keys-only"], input_fn=answers, secret_fn=answers,
                                   print_fn=silent, login_fn=login_ok, env_path=path,
                                   detect=detect_ok)
        check("--keys-only: ключи готовы, вход не запускается (нужно для облака)",
              code == 0 and calls == [], str(calls))

        calls.clear()
        path = make_env()
        answers = FakeInput(["n", ""])
        code = await wizard.wizard(argv=["--session", "second_session"], input_fn=answers,
                                   secret_fn=answers, print_fn=silent, login_fn=login_ok,
                                   env_path=path, detect=detect_ok)
        check("--session передаётся входу: второй аккаунт не трогает первый",
              code == 0 and os.environ.get("TG_SESSION") == "second_session", os.environ.get("TG_SESSION", ""))

        calls.clear()
        path = make_env()
        answers = FakeInput(["n", ""])
        code = await wizard.wizard(argv=[], input_fn=answers, secret_fn=answers, print_fn=silent,
                                   login_fn=login_fail, env_path=path, detect=detect_ok)
        check("проваленный вход: ненулевой код и без опроса бота",
              code == 1 and calls == ["login"], f"code={code}, вызовы={calls}")

        path = env_file(".env-empty", None)
        Path(path).unlink(missing_ok=True)
        answers = FakeInput([API_ID, API_HASH, "", ""])
        code = await wizard.wizard(argv=["--keys-only"], input_fn=answers, secret_fn=answers,
                                   print_fn=silent, login_fn=login_ok, env_path=path,
                                   example_path=".env.example", detect=detect_ok)
        text = Path(path).read_text(encoding="utf-8")
        check("пустая папка: мастер сам создаёт .env и вписывает ключи",
              code == 0 and f"TG_API_ID={API_ID}" in text and f"TG_API_HASH={API_HASH}" in text, "")

        path = env_file(".env-nosave", None)
        Path(path).unlink(missing_ok=True)
        answers = FakeInput([API_ID, API_HASH])
        code = await wizard.wizard(argv=["--keys-only", "--no-save"], input_fn=answers,
                                   secret_fn=answers, print_fn=silent, login_fn=login_ok,
                                   env_path=path, detect=detect_ok)
        check("--no-save: .env не создаётся вовсе (ничего не пишем на диск)",
              code == 0 and not Path(path).exists(), "")

        flags = wizard.parse_flags(["--keys-only", "--ascii-qr", "--session", "third"])
        check("флаги мастера разбираются", flags["keys_only"] and flags["ascii_qr"]
              and flags["session"] == "third" and flags["save"] is True, str(flags))
        flags = wizard.parse_flags(["--no-save", "--session=fourth"])
        check("--session=fourth и --no-save тоже понимаются",
              flags["session"] == "fourth" and flags["save"] is False, str(flags))

        # ------------------------------------------------- 7. обвязка и документы
        section("7. Обвязка, документы и чужие ключи")
        bat = Path("start.bat").read_text(encoding="utf-8", errors="replace")
        sh = Path("start.sh").read_text(encoding="utf-8", errors="replace")
        check("start.bat ведёт --login в мастер", "login_wizard.py" in bat
              and '"%~1"=="--login"' in bat, "")
        check("start.bat не требует ключей для --login (мастер их и спрашивает)",
              'if "%~1"=="--login" set "NEEDS_KEYS=0"' in bat, "")
        check("start.bat --login не путается с --login-qr (сравнение строки целиком)",
              bat.index('"%~1"=="--login"') < bat.index('"%~1"=="--login-qr"'), "")
        check("код возврата мастера доходит до cmd (!errorlevel!, а не %errorlevel%)",
              "exit /b !errorlevel!" in bat and "exit /b %errorlevel%" not in bat, "")
        check("start.sh ведёт --login в мастер до проверки ключей",
              "login_wizard.py" in sh and sh.index("--login") < sh.index("needs_keys=1"), "")

        monitor_src = Path("monitor.py").read_text(encoding="utf-8")
        login_src = Path("login_qr.py").read_text(encoding="utf-8")
        check("без ключей радар советует мастер, а не только «заполни .env»",
              "start.bat --login" in monitor_src and "start.bat --login" in login_src, "")

        docs = {name: Path(name).read_text(encoding="utf-8") for name in
                ("START-HERE.md", "README.md", "COMMANDS.md", "DEPLOY.md")}
        check("инструкция с прямой ссылкой на создание приложения есть в START-HERE и README",
              all(wizard.APPS_URL in text for text in (docs["START-HERE.md"], docs["README.md"])),
              wizard.APPS_URL)
        check("мастер --login описан в START-HERE, README, COMMANDS и DEPLOY",
              all("--login" in text for text in docs.values()), "")
        check("в доках сказано, что название приложения может быть любым",
              "любое" in docs["START-HERE.md"].lower() and "любое" in docs["README.md"].lower(), "")
        check("док объясняет, что api_id/api_hash — ключи приложения, а не пароль",
              "ключи приложения" in docs["START-HERE.md"].lower(), "")

        instruction = wizard.api_instruction()
        check("подсказка мастера содержит прямую ссылку и «название любое»",
              wizard.APPS_URL in instruction and "ЛЮБОЕ" in instruction, "")
        check("подсказка предупреждает про чужие ключи, не приводя их",
              "чужие" in instruction.lower() and FOREIGN_ID not in instruction
              and FOREIGN_HASH not in instruction, "")

        foreign: list[str] = []
        skip_dirs = {".git", "__pycache__", "node_modules", ".wrangler", "out", "dist"}
        for path_ in Path(".").rglob("*"):
            if not path_.is_file() or any(part in skip_dirs for part in path_.parts):
                continue
            if path_.suffix not in {".py", ".md", ".bat", ".sh", ".txt", ".json", ".jsonc",
                                    ".yaml", ".yml", ".example", ""}:
                continue
            if path_.name in {".env.example", "selftest_login.py"}:
                continue                     # в самом тесте значения — как образец того, чего нет
            try:
                text = path_.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            if FOREIGN_ID in text or FOREIGN_HASH in text:
                foreign.append(str(path_))
        check("чужих api_id/api_hash в проекте нет: пользователь регистрирует своё приложение",
              not foreign, ", ".join(foreign) or "чисто")

        env_example = Path(".env.example").read_text(encoding="utf-8", errors="replace")
        check("в .env.example нет готовых значений ключей (только заглушки)",
              FOREIGN_ID not in env_example and FOREIGN_HASH not in env_example
              and "TG_API_ID=1234567" in env_example, "")

    finally:
        core_telegram.bot_get_me = original_get_me
        os.environ.clear()
        os.environ.update(saved_environ)
        shutil.rmtree(WORKDIR, ignore_errors=True)

    section("Итог")
    for name, ok, detail in checks:
        print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail else ""))
    failed = [name for name, ok, _ in checks if not ok]
    print("\nИТОГ:", "всё ок" if not failed else f"провалено: {failed}")
    return 1 if failed else 0


async def _async_value(value):
    """Заглушка detect_chat_id: возвращает значение, как настоящая coroutine."""
    return value


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        print("\n[!] прервано")
