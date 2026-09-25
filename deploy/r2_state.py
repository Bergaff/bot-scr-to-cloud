#!/usr/bin/env python3
"""
Состояние радара в R2: S3-клиент на стандартной библиотеке + сохранение/восстановление.

Зачем это нужно. Диск контейнера Cloudflare **эфемерен**: после сна файловая система чистая.
Значит `.session` (ключ аккаунта Telegram) и `hits.sqlite3` (находки, дедупликация, лимиты
пересылок) нельзя держать только в контейнере — их надо забирать при старте и возвращать
после каждого прохода. Иначе после первого же сна радар потеряет аккаунт и начнёт дублить.

Почему без boto3. Это ~50 МБ в образе и зависимость, которую надо сопровождать. Подпись
AWS Signature V4 — hmac и hashlib из стандартной библиотеки: функция `signed_headers()`
покрыта офлайн-тестом на официальном векторе AWS (`deploy/selftest_cloud.py`).

Раскладка в бакете (ключи — только латиница, цифры, точка, подчёркивание, дефис и слэш):

    sessions/<имя>.session    ключ аккаунта Telegram — СЕКРЕТ, в git не попадает никогда
    db/hits.sqlite3           база: находки, дедупликация, статистика, heartbeats, metrics
    metrics/metrics.csv       срезы расхода (ТЗ §15.2)
    logs/last-run.log         лог последнего прохода — смотреть, когда что-то пошло не так
    config/sources.yaml       необязательно: поменять список чатов без пересборки образа
"""
from __future__ import annotations

import hashlib
import hmac
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

ALGORITHM = "AWS4-HMAC-SHA256"
R2_REGION = "auto"          # у Cloudflare R2 регион всегда «auto»
SERVICE = "s3"
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
KEY_PATTERN = re.compile(r"^[A-Za-z0-9._~/-]+$")

DEFAULT_DB = "hits.sqlite3"
DEFAULT_METRICS = "metrics.csv"
KEY_DB = "db/hits.sqlite3"
KEY_METRICS = "metrics/metrics.csv"
KEY_LOG = "logs/last-run.log"
KEY_CONFIG = "config/sources.yaml"


def session_key(name: str) -> str:
    """Ключ R2 для файла сессии аккаунта: main_session -> sessions/main_session.session."""
    clean = str(name or "").strip()
    if clean.endswith(".session"):
        clean = clean[: -len(".session")]
    if not KEY_PATTERN.match(clean or ""):
        raise ValueError(f"имя сессии «{name}» не подходит для ключа R2 (только латиница, "
                         f"цифры, . _ ~ - /)")
    return f"sessions/{clean}.session"


def uri_encode(value: str, encode_slash: bool = True) -> str:
    """Кодирование по правилам AWS: незакодированными остаются A-Z a-z 0-9 - . _ ~."""
    return quote(str(value), safe="" if encode_slash else "/")


def _hmac(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def signing_key(secret_access_key: str, datestamp: str, region: str = R2_REGION,
                service: str = SERVICE) -> bytes:
    """Ключ подписи: «AWS4»+secret -> дата -> регион -> сервис -> aws4_request."""
    key = ("AWS4" + secret_access_key).encode("utf-8")
    for part in (datestamp, region, service, "aws4_request"):
        key = _hmac(key, part)
    return key


def canonical_request(method: str, url: str, headers: dict, payload_hash: str) -> str:
    """Канонический запрос — то, что реально подписывается (порядок строк важен)."""
    parsed = urlparse(url)
    canonical_uri = uri_encode(parsed.path or "/", encode_slash=False)
    query = ""
    if parsed.query:
        pairs = []
        for chunk in parsed.query.split("&"):
            if not chunk:
                continue
            name, _sep, value = chunk.partition("=")
            pairs.append((name, value))
        query = "&".join(f"{name}={value}" for name, value in sorted(pairs))
    names = sorted(headers)
    canonical_headers = "".join(f"{name}:{headers[name]}\n" for name in names)
    return "\n".join([method.upper(), canonical_uri, query, canonical_headers,
                      ";".join(names), payload_hash])


def signed_headers(method: str, url: str, headers: dict, payload_hash: str,
                   access_key_id: str, secret_access_key: str, region: str = R2_REGION,
                   service: str = SERVICE, amzdate: str | None = None,
                   sign_content_hash: bool = True) -> dict:
    """Заголовки запроса с подписью AWS Signature V4.

    Чистая функция: ничего не отправляет, поэтому её можно проверить на известном векторе.
    `sign_content_hash=False` нужен только для теста эталонного примера AWS, где заголовок
    `x-amz-content-sha256` не подписывается; R2 требует его всегда, поэтому по умолчанию True.
    """
    parsed = urlparse(url)
    stamp = amzdate or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    datestamp = stamp[:8]
    to_sign = {str(name).lower().strip(): " ".join(str(value).split())
               for name, value in (headers or {}).items()}
    to_sign.setdefault("host", parsed.netloc)
    to_sign["x-amz-date"] = stamp
    if sign_content_hash:
        to_sign["x-amz-content-sha256"] = payload_hash
    scope = f"{datestamp}/{region}/{service}/aws4_request"
    request = canonical_request(method, url, to_sign, payload_hash)
    string_to_sign = "\n".join([ALGORITHM, stamp, scope,
                                hashlib.sha256(request.encode("utf-8")).hexdigest()])
    signature = hmac.new(signing_key(secret_access_key, datestamp, region, service),
                         string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    out = {k: v for k, v in to_sign.items() if k not in ("host", "x-amz-date")}
    out["x-amz-date"] = stamp
    out["Authorization"] = (f"{ALGORITHM} Credential={access_key_id}/{scope}, "
                            f"SignedHeaders={';'.join(sorted(to_sign))}, Signature={signature}")
    return out


class R2Error(RuntimeError):
    """R2 ответил ошибкой (нет доступа, нет бакета, неверные ключи)."""


class R2Client:
    """Минимальный S3-клиент для R2: положить, взять, проверить наличие."""

    def __init__(self, bucket: str, access_key_id: str, secret_access_key: str,
                 account_id: str = "", endpoint: str = "", region: str = R2_REGION,
                 timeout: float = 120.0, opener=urlopen):
        self.bucket = (bucket or "").strip()
        self.access_key_id = access_key_id or ""
        self.secret_access_key = secret_access_key or ""
        self.endpoint = (endpoint or (f"https://{account_id}.r2.cloudflarestorage.com"
                                      if account_id else "")).rstrip("/")
        self.region = region or R2_REGION
        self.timeout = timeout
        self._opener = opener
        if not (self.bucket and self.access_key_id and self.secret_access_key and self.endpoint):
            raise ValueError("для R2 нужны бакет, access key id, secret access key и "
                             "endpoint (или R2_ACCOUNT_ID)")

    @classmethod
    def from_env(cls, env: dict | None = None) -> "R2Client | None":
        """Клиент из переменных окружения. None — если R2 не настроен (радар работает без него)."""
        env = os.environ if env is None else env
        try:
            return cls(bucket=env.get("R2_BUCKET", ""),
                       access_key_id=env.get("R2_ACCESS_KEY_ID", ""),
                       secret_access_key=env.get("R2_SECRET_ACCESS_KEY", ""),
                       account_id=env.get("R2_ACCOUNT_ID", ""),
                       endpoint=env.get("R2_ENDPOINT", ""))
        except ValueError:
            return None

    def url(self, key: str) -> str:
        if not KEY_PATTERN.match(key or ""):
            raise ValueError(f"ключ R2 «{key}» содержит недопустимые символы")
        return f"{self.endpoint}/{self.bucket}/{key}"

    def _call(self, method: str, key: str, data: bytes | None = None,
              extra_headers: dict | None = None) -> tuple[int, dict, bytes]:
        payload = data or b""
        payload_hash = hashlib.sha256(payload).hexdigest()
        headers = dict(extra_headers or {})
        if data is not None:
            headers.setdefault("content-type", "application/octet-stream")
            headers["content-length"] = str(len(payload))
        headers.update(signed_headers(method, self.url(key), headers, payload_hash,
                                      self.access_key_id, self.secret_access_key, self.region))
        request = Request(self.url(key), data=(payload if data is not None else None),
                          headers=headers, method=method.upper())
        try:
            with self._opener(request, timeout=self.timeout) as response:
                return response.status, dict(response.headers), response.read()
        except HTTPError as exc:
            body = exc.read() if hasattr(exc, "read") else b""
            return exc.code, dict(exc.headers or {}), body

    def put_object(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> bool:
        status, _headers, body = self._call("PUT", key, data, {"content-type": content_type})
        if status not in (200, 201):
            raise R2Error(f"PUT {key}: HTTP {status} {_explain(status, body)}")
        return True

    def get_object(self, key: str) -> bytes | None:
        """Содержимое объекта или None, если его нет (404 — это не ошибка)."""
        status, _headers, body = self._call("GET", key)
        if status == 404:
            return None
        if status != 200:
            raise R2Error(f"GET {key}: HTTP {status} {_explain(status, body)}")
        return body

    def head_object(self, key: str) -> dict | None:
        status, headers, body = self._call("HEAD", key)
        if status == 404:
            return None
        if status != 200:
            raise R2Error(f"HEAD {key}: HTTP {status} {_explain(status, body)}")
        return {k.lower(): v for k, v in headers.items()}

    def etag(self, key: str) -> str:
        meta = self.head_object(key)
        return str((meta or {}).get("etag", "")).strip('"') if meta else ""

    def upload(self, path: str | Path, key: str, *, skip_unchanged: bool = True) -> str:
        """Файл -> объект R2. Возвращает «ok», «skipped» (не изменился) или «missing»."""
        source = Path(path)
        if not source.exists():
            return "missing"
        data = source.read_bytes()
        digest = hashlib.md5(data).hexdigest()
        if skip_unchanged:
            try:
                if self.etag(key) == digest:
                    return "skipped"
            except (R2Error, URLError, OSError):
                pass                      # не смогли проверить — просто загружаем
        self.put_object(key, data)
        return "ok"

    def download(self, key: str, path: str | Path) -> str:
        """Объект R2 -> файл. Возвращает «ok» или «missing»; ошибки сети — исключение."""
        data = self.get_object(key)
        if data is None:
            return "missing"
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return "ok"


def _explain(status: int, body: bytes) -> str:
    """Человеческая подсказка по коду ответа R2 (в логе контейнера должно быть понятно)."""
    hints = {
        403: "нет доступа: проверь R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY и права токена "
             "(Object Read & Write на этот бакет)",
        404: "бакет или ключ не найден: проверь R2_BUCKET и R2_ACCOUNT_ID",
        400: "неверный запрос",
        411: "не хватает content-length",
    }
    if status in hints:
        return hints[status]
    text = body.decode("utf-8", "replace").strip()
    return text[:300] if text else "без пояснений"


def checkpoint_db(db_path: str | Path) -> bool:
    """Схлопывает WAL в основной файл базы: в R2 надо нести один файл, а не три."""
    path = Path(db_path)
    if not path.exists():
        return False
    connection = sqlite3.connect(str(path))
    try:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        connection.commit()
    except sqlite3.Error:
        return False
    finally:
        connection.close()
    for suffix in ("-wal", "-shm"):
        stale = Path(str(path) + suffix)
        if stale.exists():
            try:
                stale.unlink()
            except OSError:
                pass
    return True


def restore_state(client: R2Client | None, workdir: str | Path = ".", *,
                  sessions: tuple[str, ...] = ("monitor_session",), db_name: str = DEFAULT_DB,
                  metrics_name: str = DEFAULT_METRICS, config_name: str = "sources.yaml",
                  log=print) -> dict:
    """Тянет состояние из R2 в рабочую папку контейнера. Возвращает отчёт по каждому файлу.

    Отсутствие файла — не ошибка (первый запуск: базы ещё нет), а вот отсутствие сессии
    критично: без неё радар не войдёт в Telegram. Это видно в отчёте по ключу «sessions».
    """
    report: dict[str, object] = {"sessions": {}, "db": "missing", "metrics": "missing",
                                 "config": "missing"}
    if client is None:
        log("[i] R2 не настроен: состояние не восстанавливается (каждый запуск с нуля)")
        return report
    root = Path(workdir)
    root.mkdir(parents=True, exist_ok=True)
    for name in sessions:
        key = session_key(name)
        status = client.download(key, root / f"{name}.session")
        report["sessions"][name] = status            # type: ignore[index]
        log(f"[i] сессия {name}: {status}" + ("" if status == "ok" else f" (в R2 ключ {key})"))
    report["db"] = client.download(KEY_DB, root / db_name)
    for suffix in ("-wal", "-shm"):                  # от прошлой жизни контейнера не должно остаться
        stale = root / f"{db_name}{suffix}"
        if stale.exists():
            stale.unlink()
    report["metrics"] = client.download(KEY_METRICS, root / metrics_name)
    report["config"] = client.download(KEY_CONFIG, root / config_name)
    log(f"[i] состояние из R2: база {report['db']}, metrics.csv {report['metrics']}, "
        f"sources.yaml {report['config']}")
    return report


def save_state(client: R2Client | None, workdir: str | Path = ".", *,
               sessions: tuple[str, ...] = ("monitor_session",), db_name: str = DEFAULT_DB,
               metrics_name: str = DEFAULT_METRICS, log_file: str | None = None,
               log=print) -> dict:
    """Возвращает состояние в R2 после прохода. Не изменившиеся файлы не перезагружает.

    Обязательно перед загрузкой базы: WAL-файл схлопывается в основной (иначе в R2 уедет
    база без последних находок — они останутся в -wal, которого там никто не прочитает).
    """
    report: dict[str, object] = {"sessions": {}, "db": "missing", "metrics": "missing",
                                 "log": "missing"}
    if client is None:
        log("[!] R2 не настроен: состояние НЕ сохранено — после сна контейнера всё пропадёт")
        return report
    root = Path(workdir)
    checkpoint_db(root / db_name)
    report["db"] = client.upload(root / db_name, KEY_DB)
    for name in sessions:
        path = root / f"{name}.session"
        report["sessions"][name] = client.upload(path, session_key(name))   # type: ignore[index]
    report["metrics"] = client.upload(root / metrics_name, KEY_METRICS)
    if log_file:
        report["log"] = client.upload(log_file, KEY_LOG)
    log(f"[i] состояние в R2: база {report['db']}, metrics.csv {report['metrics']}, "
        f"лог {report['log']}, сессии "
        f"{', '.join(f'{n}:{v}' for n, v in report['sessions'].items()) or '—'}")
    return report


def missing_sessions(report: dict) -> list[str]:
    """Имена аккаунтов, чья сессия не нашлась в R2 — с ними радар работать не сможет."""
    sessions = report.get("sessions") or {}
    return sorted(name for name, status in sessions.items() if status != "ok")
