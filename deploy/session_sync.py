"""
Подгрузка файлов сессий Telegram по ссылке (Google Диск или любой https) в облачный радар.

Зачем. Раньше новую сессию (после «выхода из аккаунта» и т. п.) надо было класть в R2
руками через wrangler, а ещё останавливать радар, чтобы проход не затёр её старой копией.
Теперь для каждой сессии можно задать ссылку-источник: радар в начале КАЖДОГО прохода
скачивает файл и, если на Диске лежит новая версия, сам ставит её вместо рабочей, сразу
кладёт в R2 и продолжает проход. Чтобы заменить сессию, достаточно заменить файл на Диске.

Как решаем «новая или нет». Сравнивать файл на Диске с локальной сессией нельзя: Telethon сам
меняет .session в ходе работы. Поэтому помним хеш (sha256) последней ИМПОРТИРОВАННОЙ версии
в R2 (state/session_sources.json) и сравниваем только его:

    файла сессии нет на диске             -> ставим версию с Диска
    хеш на Диске не равен импортированному -> ставим (пользователь заменил файл)
    хеш тот же                             -> ничего не трогаем (рабочая копия новее)

Откуда берутся ссылки (секреты Worker'а — в ссылке есть id файла):
    SESSION_URL_<ИМЯ_СЕССИИ>   например SESSION_URL_SECOND_SESSION  (регистр и - / _ не важны)
    SESSION_URLS               «имя=ссылка» через запятую или перевод строки

Скачанное проверяется: заголовок SQLite, таблица sessions, непустой auth_key. HTML-страница
Google («нет доступа», «войдите в аккаунт») файлом сессии не считается и ничего не ломает.
Ссылки в лог не пишутся: только имя сессии и 8 символов хеша.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

KEY_SESSION_META = "state/session_sources.json"
META_NAME = "session_sources.json"          # локальная копия мета-файла
REPORT_NAME = "session_sync.json"           # отчёт для монитора (bot_state -> бот)
MAX_BYTES = 5 * 1024 * 1024                 # .session — десятки килобайт; больше — не сессия
SQLITE_MAGIC = b"SQLite format 3\x00"
DRIVE_HOSTS = ("drive.google.com", "docs.google.com", "drive.usercontent.google.com")
ID_RE = re.compile(r"^[A-Za-z0-9_-]{15,}$")


def norm_name(name: str) -> str:
    """second_session / SECOND-SESSION / second_session.session -> SECOND_SESSION."""
    clean = str(name or "").strip()
    if clean.lower().endswith(".session"):
        clean = clean[: -len(".session")]
    return re.sub(r"[^A-Za-z0-9]+", "_", clean).strip("_").upper()


def sources_from_env(env: dict, sessions: tuple[str, ...]) -> dict[str, str]:
    """{имя сессии -> ссылка} из SESSION_URL_<ИМЯ> и SESSION_URLS. Пустые значения игнорируются."""
    wanted = {norm_name(name): name for name in sessions}
    found: dict[str, str] = {}
    for key, value in env.items():
        if key.startswith("SESSION_URL_") and str(value or "").strip():
            real = wanted.get(norm_name(key[len("SESSION_URL_"):]))
            if real:
                found[real] = str(value).strip()
    for part in re.split(r"[,\n;]+", str(env.get("SESSION_URLS") or "")):
        if "=" not in part:
            continue
        name, _, link = part.partition("=")
        real = wanted.get(norm_name(name))
        if real and link.strip():
            found[real] = link.strip()
    return found


def direct_url(link: str) -> str:
    """Ссылка Google Диска любого вида -> прямая ссылка на скачивание. Прочие https — как есть.

    Понимает: /file/d/<id>/view, open?id=<id>, uc?id=<id>, голый id файла.
    """
    text = str(link or "").strip()
    if not text:
        raise ValueError("пустая ссылка")
    if "://" not in text:
        if ID_RE.match(text):
            return f"https://drive.google.com/uc?export=download&id={text}"
        raise ValueError("не похоже ни на ссылку, ни на id файла")
    parsed = urlparse(text)
    if parsed.scheme != "https":
        raise ValueError("нужна https-ссылка")
    if parsed.hostname in DRIVE_HOSTS:
        match = re.search(r"/file/d/([A-Za-z0-9_-]+)", parsed.path)
        file_id = match.group(1) if match else (parse_qs(parsed.query).get("id") or [""])[0]
        if not file_id:
            raise ValueError("в ссылке Google Диска не нашёл id файла "
                             "(нужна ссылка на файл, а не на папку)")
        if "/folders/" in parsed.path:
            raise ValueError("это ссылка на папку: открой сам файл .session и скопируй ссылку на него")
        return f"https://drive.google.com/uc?export=download&id={file_id}"
    return text


def validate_session(data: bytes) -> tuple[bool, str, dict]:
    """Похоже ли это на рабочий файл сессии Telethon. (да/нет, причина, сведения)."""
    if not data:
        return False, "файл пустой", {}
    if len(data) > MAX_BYTES:
        return False, f"файл слишком большой ({len(data) // 1024} КБ) — это не сессия", {}
    if not data.startswith(SQLITE_MAGIC):
        head = data[:200].lstrip().lower()
        if head.startswith((b"<!doctype", b"<html")):
            return False, ("вместо файла пришла веб-страница Google: открой доступ "
                           "«Все, у кого есть ссылка» (читатель) или проверь ссылку"), {}
        return False, "это не файл сессии Telethon (нет заголовка SQLite)", {}
    handle = tempfile.NamedTemporaryFile(suffix=".session", delete=False)
    try:
        handle.write(data)
        handle.close()
        conn = sqlite3.connect(f"file:{handle.name}?mode=ro", uri=True)
        try:
            row = conn.execute("SELECT dc_id, auth_key FROM sessions LIMIT 1").fetchone()
        finally:
            conn.close()
    except sqlite3.Error as exc:
        return False, f"файл не открывается как сессия Telethon ({type(exc).__name__})", {}
    finally:
        try:
            os.unlink(handle.name)
        except OSError:
            pass
    if not row or not row[1]:
        return False, "в файле нет ключа авторизации (вход в аккаунт не выполнен)", {}
    return True, "ok", {"dc_id": row[0]}


def fetch(url: str, timeout: float = 25.0) -> bytes:
    """Скачать файл (не больше MAX_BYTES). Ошибки сети — исключение."""
    request = Request(url, headers={"User-Agent": "Mozilla/5.0 (radar-session-sync)"})
    with urlopen(request, timeout=timeout) as response:          # noqa: S310 - https проверен
        return response.read(MAX_BYTES + 1)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _load_meta(workdir: Path) -> dict:
    try:
        data = json.loads((workdir / META_NAME).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _install(path: Path, data: bytes) -> None:
    """Подменить файл сессии целиком: старые журналы sqlite рядом только мешают."""
    path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("-journal", "-wal", "-shm"):
        stale = path.with_name(path.name + suffix)
        if stale.exists():
            stale.unlink()
    temp = path.with_name(path.name + ".new")
    temp.write_bytes(data)
    os.replace(temp, path)


def sync_sessions(client, workdir: str | Path, sessions: tuple[str, ...], urls: dict[str, str],
                  *, fetcher=fetch, log=print) -> dict:
    """Один раунд подгрузки. Возвращает {сессия: {status, sha, ...}}.

    status: imported (поставили новую версию) · unchanged (версия уже была) · skipped (ссылки нет)
            · error (не скачалось или не сессия — рабочая копия не тронута).
    Ничего не бросает наружу: сбой Диска не должен останавливать радар на рабочих сессиях.
    """
    root = Path(workdir)
    report: dict[str, dict] = {}
    if not urls:
        return report
    if client is not None:
        try:
            client.download(KEY_SESSION_META, root / META_NAME)
        except Exception as exc:                       # noqa: BLE001
            log(f"[!] мета подгрузки сессий не прочиталась из R2: {type(exc).__name__}")
    meta = _load_meta(root)
    changed = False
    from r2_state import session_key        # локальный импорт: модуль лежит рядом

    for name in sessions:
        link = urls.get(name)
        if not link:
            report[name] = {"status": "skipped"}
            continue
        known = meta.get(name) if isinstance(meta.get(name), dict) else {}
        path = root / f"{name}.session"
        try:
            data = fetcher(direct_url(link))
        except ValueError as exc:
            report[name] = {"status": "error", "detail": f"ссылка не подходит: {exc}"}
            log(f"[!] сессия {name}: ссылка не подходит ({exc})")
            continue
        except Exception as exc:                       # noqa: BLE001
            report[name] = {"status": "error",
                            "detail": f"не скачалось: {type(exc).__name__}",
                            **_known(known)}
            log(f"[!] сессия {name}: с ссылки не скачалось ({type(exc).__name__}), "
                f"остаётся рабочая копия")
            continue
        ok, why, info = validate_session(data)
        if not ok:
            report[name] = {"status": "error", "detail": why, **_known(known)}
            log(f"[!] сессия {name}: по ссылке не сессия — {why}")
            continue
        sha = hashlib.sha256(data).hexdigest()
        if path.exists() and known.get("sha") == sha:
            report[name] = {"status": "unchanged", **_known(known)}
            log(f"[i] сессия {name}: на Диске та же версия {sha[:8]} — не трогаю")
            continue
        _install(path, data)
        meta[name] = {"sha": sha, "at": _now(), "dc_id": info.get("dc_id")}
        changed = True
        saved = "—"
        if client is not None:
            try:        # сразу в R2, иначе обрыв прохода оставит там старую сессию
                saved = client.upload(path, session_key(name), skip_unchanged=False)
            except Exception as exc:                   # noqa: BLE001
                saved = f"не сохранилась ({type(exc).__name__})"
        report[name] = {"status": "imported", **_known(meta[name]), "r2": saved}
        log(f"[+] сессия {name}: поставил новую версию с Диска {sha[:8]} (R2: {saved})")

    if changed:
        (root / META_NAME).write_text(json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")
        if client is not None:
            try:
                client.upload(root / META_NAME, KEY_SESSION_META, skip_unchanged=False)
            except Exception as exc:                   # noqa: BLE001
                log(f"[!] мета подгрузки сессий не сохранилась в R2: {type(exc).__name__}")
    return report


def _known(entry: dict) -> dict:
    """Что помним про последнюю импортированную версию (для бота)."""
    out = {}
    if entry.get("sha"):
        out["sha"] = str(entry["sha"])[:8]
    if entry.get("at"):
        out["at"] = entry["at"]
    return out


def write_report(workdir: str | Path, report: dict) -> None:
    """Отчёт для монитора: он положит его в bot_state, а бот покажет в /accounts."""
    root = Path(workdir)
    payload = {"ts": _now(), "sessions": report}
    try:
        (root / REPORT_NAME).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except OSError:
        pass
