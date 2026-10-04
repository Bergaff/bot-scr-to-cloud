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
  Способ 1 (проще): одна ссылка на ПАПКУ на Диске + имена файлов
    SESSION_DRIVE_URL          ссылка на папку Диска («Все, у кого есть ссылка»)
    SESSION_FILE_<ИМЯ>         имя файла в папке, ИМЯ — имя аккаунта (MAIN, SECOND) или сессии;
                               не задано — ищется файл <имя_сессии>.session
    GOOGLE_API_KEY             необязательно: ключ Drive API, если просмотр папки без ключа не открылся
  Способ 2: ссылка на каждый файл
    SESSION_URL_<ИМЯ_СЕССИИ>   например SESSION_URL_SECOND_SESSION  (регистр и - / _ не важны)
    SESSION_URLS               «имя=ссылка» через запятую или перевод строки
  Ссылка на конкретный файл (способ 2) главнее папки (способ 1).

Формат файла. Радар работает на Telethon. Файл Pyrogram (таблица sessions с колонками
test_mode/user_id/is_bot) конвертируется автоматически тем же конвертером, что и
session_convert.py: переносится auth_key, повторный вход не нужен. Важно: если этот ключ уже
отозван Telegram (AuthKeyDuplicatedError) или та же сессия ещё работает где-то ещё
(старый бот на Pyrogram), аккаунт сломается снова — нужен ключ от НОВОГО входа.

Скачанное проверяется: заголовок SQLite, таблица sessions, непустой auth_key. HTML-страница
Google («нет доступа», «войдите в аккаунт») файлом сессии не считается и ничего не ломает.
Ссылки в лог не пишутся: только имя сессии и 8 символов хеша.
"""
from __future__ import annotations

import hashlib
import html as html_lib
import json
import os
import re
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse
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


def folder_id(link: str) -> str:
    """Ссылка на папку Google Диска (или голый id) -> id папки. ValueError — если это не папка."""
    text = str(link or "").strip()
    if not text:
        raise ValueError("пустая ссылка на папку")
    if "://" not in text:
        if ID_RE.match(text):
            return text
        raise ValueError("не похоже ни на ссылку, ни на id папки")
    parsed = urlparse(text)
    if parsed.scheme != "https" or parsed.hostname not in DRIVE_HOSTS:
        raise ValueError("нужна https-ссылка на папку Google Диска")
    match = re.search(r"/folders/([A-Za-z0-9_-]+)", parsed.path)
    found = match.group(1) if match else (parse_qs(parsed.query).get("id") or [""])[0]
    if not found:
        raise ValueError("в ссылке не нашёл id папки (нужна ссылка вида drive.google.com/drive/folders/…)")
    return found


def parse_folder_html(page: str) -> dict[str, str]:
    """Страница embeddedfolderview -> {имя файла: id}. Формат Google, держится на двух якорях."""
    found: dict[str, str] = {}
    for chunk in page.split('id="entry-')[1:]:
        file_id = re.match(r"([A-Za-z0-9_-]+)", chunk)
        title = re.search(r'flip-entry-title">([^<]+)<', chunk)
        if file_id and title:
            found[html_lib.unescape(title.group(1)).strip()] = file_id.group(1)
    return found


def list_folder(folder: str, api_key: str = "", fetcher=None) -> dict[str, str]:
    """Файлы папки Диска: {имя: id}. С GOOGLE_API_KEY — через Drive API, иначе — страница просмотра папки.

    Ошибка сети — исключение; пустая папка/закрытый доступ — пустой словарь."""
    fetcher = fetcher or fetch
    if api_key:
        query = quote(f"'{folder}' in parents and trashed=false")
        url = (f"https://www.googleapis.com/drive/v3/files?q={query}&pageSize=1000"
               f"&fields=files(id,name)&key={quote(api_key)}")
        data = json.loads(fetcher(url).decode("utf-8", "replace"))
        return {str(x.get("name")): str(x.get("id")) for x in data.get("files", []) if x.get("id")}
    page = fetcher(f"https://drive.google.com/embeddedfolderview?id={folder}#list")
    return parse_folder_html(page.decode("utf-8", "replace"))


def file_names_from_env(env: dict) -> dict[str, str]:
    """{НОРМ_ИМЯ -> имя файла в папке} из SESSION_FILE_<ИМЯ>."""
    out = {}
    for key, value in env.items():
        if key.startswith("SESSION_FILE_") and str(value or "").strip():
            out[norm_name(key[len("SESSION_FILE_"):])] = str(value).strip()
    return out


def account_sessions(workdir: str | Path) -> dict[str, str]:
    """{имя аккаунта: имя сессии} из sources.yaml (из R2 или репозитория), чтобы MAIN = monitor_session."""
    for path in (Path(workdir) / "sources.yaml", Path(__file__).resolve().parent.parent / "sources.yaml"):
        if not path.exists():
            continue
        try:
            import yaml
            accounts = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("accounts") or {}
        except Exception:                                  # noqa: BLE001
            continue
        if isinstance(accounts, dict):
            return {str(name): Path(str((cfg or {}).get("session") or "")).name.removesuffix(".session")
                    for name, cfg in accounts.items() if isinstance(cfg, dict)}
        if isinstance(accounts, list):
            return {str(a.get("name")): Path(str(a.get("session") or "")).name.removesuffix(".session")
                    for a in accounts if isinstance(a, dict) and a.get("name")}
    return {}


def pick_file(listing: dict[str, str], wanted: str) -> tuple[str, str] | None:
    """Найти в папке файл по имени без учёта регистра; «second» найдёт и «second.session»."""
    low = {name.lower(): (name, fid) for name, fid in listing.items()}
    for candidate in (wanted, wanted + ".session"):
        if candidate.lower() in low:
            return low[candidate.lower()]
    return None


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


def _key_of(data: bytes) -> bytes:
    """auth_key из байтов файла сессии (любого формата) — только для сравнения, наружу не отдаётся."""
    handle = tempfile.NamedTemporaryFile(suffix=".session", delete=False)
    try:
        handle.write(data)
        handle.close()
        conn = sqlite3.connect(f"file:{handle.name}?mode=ro", uri=True)
        try:
            row = conn.execute("SELECT auth_key FROM sessions LIMIT 1").fetchone()
        finally:
            conn.close()
        return bytes(row[0]) if row and row[0] else b""
    except sqlite3.Error:
        return b""
    finally:
        try:
            os.unlink(handle.name)
        except OSError:
            pass


def prepare_session(data: bytes) -> tuple[bool, str, dict, bytes]:
    """Проверить скачанное и привести к формату Telethon. (да/нет, причина, сведения, байты для установки).

    Сведения: dc_id, format (telethon | pyrogram). Pyrogram-файл конвертируется (ключ переносится)."""
    ok, why, info = validate_session(data)
    if not ok:
        return False, why, info, data
    parent = str(Path(__file__).resolve().parent.parent)
    if parent not in sys.path:
        sys.path.insert(0, parent)
    try:
        import session_convert
    except Exception as exc:                               # noqa: BLE001
        return False, f"нет конвертера сессий ({type(exc).__name__})", info, data
    workdir = tempfile.mkdtemp(prefix="sess_")
    try:
        source = os.path.join(workdir, "in.session")
        Path(source).write_bytes(data)
        try:
            kind = session_convert.sniff_session(source)
        except session_convert.SessionConvertError as exc:
            return False, str(exc), info, data
        info = dict(info, format=kind)
        if kind == "telethon":
            return True, "ok", info, data
        if kind != "pyrogram":
            return False, "неизвестный формат файла сессии (не Telethon и не Pyrogram)", info, data
        try:
            target = session_convert.convert(source, out=os.path.join(workdir, "out"), force=True,
                                             print_fn=lambda *a, **k: None)
        except session_convert.SessionConvertError as exc:
            return False, f"сессия Pyrogram не конвертируется: {exc}".replace(source, "файл"), info, data
        except Exception as exc:                           # noqa: BLE001
            return False, f"сессия Pyrogram не конвертируется ({type(exc).__name__})", info, data
        return True, "ok", info, Path(target).read_bytes()
    finally:
        import shutil
        shutil.rmtree(workdir, ignore_errors=True)


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
                  *, fetcher=fetch, log=print, folder: str = "", files: dict | None = None,
                  api_key: str = "") -> dict:
    """Один раунд подгрузки. Возвращает {сессия: {status, sha, ...}}.

    status: imported (поставили новую версию) · unchanged (версия уже была) · skipped (ссылки нет)
            · error (не скачалось или не сессия — рабочая копия не тронута).
    Ничего не бросает наружу: сбой Диска не должен останавливать радар на рабочих сессиях.
    """
    root = Path(workdir)
    report: dict[str, dict] = {}
    if not urls and not folder:
        return report
    listing: dict[str, str] | None = None
    listing_error = ""
    if folder and any(name not in urls for name in sessions):
        try:
            listing = list_folder(folder_id(folder), api_key, fetcher=fetcher)
            if not listing:
                listing_error = ("папка пустая или закрыта: открой доступ «Все, у кого есть ссылка» "
                                 "(либо задай GOOGLE_API_KEY)")
        except ValueError as exc:
            listing_error = f"ссылка на папку не подходит: {exc}"
        except Exception as exc:                           # noqa: BLE001
            listing_error = f"папка не открылась: {type(exc).__name__}"
        if listing_error:
            log(f"[!] папка с сессиями: {listing_error}")
    accounts = account_sessions(root) if folder else {}
    by_session = {}                                        # сессия -> желаемое имя файла
    for acc_name, sess in accounts.items():
        if files and norm_name(acc_name) in files:
            by_session[sess] = files[norm_name(acc_name)]
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
        if not link and folder:
            wanted = (by_session.get(name) or (files or {}).get(norm_name(name))
                      or f"{name}.session")
            if listing:
                hit = pick_file(listing, wanted)
                if hit:
                    link = hit[1]
                else:
                    shown = ", ".join(sorted(listing)[:8]) or "—"
                    report[name] = {"status": "error",
                                    "detail": f"в папке нет файла «{wanted}» (там: {shown})"}
                    log(f"[!] сессия {name}: в папке нет файла «{wanted}»")
                    continue
            elif listing_error:
                report[name] = {"status": "error", "detail": listing_error}
                continue
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
        ok, why, info, final = prepare_session(data)
        if not ok:
            report[name] = {"status": "error", "detail": why, **_known(known)}
            log(f"[!] сессия {name}: по ссылке не сессия — {why}")
            continue
        sha = hashlib.sha256(data).hexdigest()
        if path.exists() and known.get("sha") == sha:
            report[name] = {"status": "unchanged", **_known(known)}
            log(f"[i] сессия {name}: на Диске та же версия {sha[:8]} — не трогаю")
            continue
        new_key = _key_of(final)
        old_key = _key_of(path.read_bytes()) if path.exists() else b""
        same_key = bool(new_key and new_key == old_key)
        _install(path, final)
        meta[name] = {"sha": sha, "at": _now(), "dc_id": info.get("dc_id"),
                      "format": info.get("format", "telethon"), "same_key": same_key}
        changed = True
        saved = "—"
        if client is not None:
            try:        # сразу в R2, иначе обрыв прохода оставит там старую сессию
                saved = client.upload(path, session_key(name), skip_unchanged=False)
            except Exception as exc:                   # noqa: BLE001
                saved = f"не сохранилась ({type(exc).__name__})"
        report[name] = {"status": "imported", **_known(meta[name]), "r2": saved}
        log(f"[+] сессия {name}: поставил новую версию с Диска {sha[:8]} "
            f"(формат {info.get('format', 'telethon')}, R2: {saved})"
            + (" — ключ тот же, что был в работе" if same_key else ""))

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
    if entry.get("format"):
        out["format"] = entry["format"]
    if entry.get("same_key"):
        out["same_key"] = True
    return out


def write_report(workdir: str | Path, report: dict) -> None:
    """Отчёт для монитора: он положит его в bot_state, а бот покажет в /accounts."""
    root = Path(workdir)
    payload = {"ts": _now(), "sessions": report}
    try:
        (root / REPORT_NAME).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except OSError:
        pass
