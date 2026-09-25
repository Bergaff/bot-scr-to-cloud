#!/usr/bin/env python3
"""
Офлайн-тесты облачного слоя: подпись R2, сохранение состояния и контейнерная обёртка.

Сеть не нужна: R2 подменяется локальным HTTP-сервером, запуск радара — подставной функцией.
Проверяется то, что в облаке молча ломается и стоит денег/аккаунта:

  * подпись AWS Signature V4 — на официальном векторе AWS (иначе R2 отвечает 403);
  * что именно подписывается: host, дата, хэш тела; «подписал одно — отправил другое»;
  * восстановление/сохранение состояния: .session, база (после WAL-чекпоинта!), metrics.csv;
  * проход НЕ запускается без файла сессии (иначе Telethon начал бы спрашивать телефон
    и код, а ввода в контейнере нет — проход завис бы до таймаута);
  * эндпоинты контейнера: /healthz без пароля, остальное только с RADAR_TOKEN;
  * конфиг деплоя: wrangler.jsonc, Dockerfile и .dockerignore (секреты не в образе).

Запуск:  python3 deploy/selftest_cloud.py
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import types
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(ROOT))

import cloud_entry                                        # noqa: E402
from r2_state import (EMPTY_SHA256, KEY_DB, KEY_LOG, KEY_METRICS, R2Client, R2Error,  # noqa: E402
                      canonical_request, checkpoint_db, missing_sessions, restore_state,
                      save_state, session_key, signed_headers, signing_key, uri_encode)

WORKDIR = ROOT / "tests" / f"_tmp_cloud_{os.getpid()}"


def repo_text(*parts: str) -> str:
    """Текст файла репозитория; пустая строка, если файла нет.

    Эта самопроверка запускается не только локально, но и при сборке образа (RUN в Dockerfile),
    где часть файлов вырезана .dockerignore. Пустая строка вместо FileNotFoundError означает,
    что пропавший файл даёт внятный FAIL конкретной проверки, а не трейсбек и падение сборки.
    """
    path = ROOT.joinpath(*parts)
    return path.read_text(encoding="utf-8") if path.exists() else ""
ACCESS_KEY = "test-access-key-id"
SECRET_KEY = "test-secret-access-key"
BUCKET = "radar-state"
SESSION_BYTES = "ключ аккаунта".encode("utf-8")   # байты сессии (в литерале b"" кириллица запрещена)


def section(title: str) -> None:
    print(f"\n── {title} " + "─" * max(0, 66 - len(title)))


# --------------------------------------------------------------------- подставной R2

OBJECTS: dict[str, bytes] = {}
REQUESTS: list[dict] = []


class FakeS3(BaseHTTPRequestHandler):
    """Мини-R2: хранит объекты в памяти и ПРОВЕРЯЕТ подпись так же, как это делает R2."""

    def log_message(self, *_args) -> None:
        pass

    def _key(self) -> str:
        return urllib.parse.unquote(self.path).split(f"/{BUCKET}/", 1)[-1]

    def _record(self, body: bytes = b"") -> None:
        auth = self.headers.get("Authorization", "")
        signed = re.search(r"SignedHeaders=([^,]+)", auth)
        REQUESTS.append({
            "method": self.command, "key": self._key(), "auth": auth,
            "signed_headers": (signed.group(1) if signed else "").split(";"),
            "amz_date": self.headers.get("x-amz-date", ""),
            "content_sha256": self.headers.get("x-amz-content-sha256", ""),
            "body_sha256": hashlib.sha256(body).hexdigest(),
            "length": len(body),
        })

    def _verify(self, body: bytes) -> str | None:
        """Пересчитывает подпись из полученных заголовков. Возвращает текст ошибки или None."""
        auth = self.headers.get("Authorization", "")
        match = re.search(r"Signature=([0-9a-f]{64})", auth)
        if not match:
            return "нет подписи"
        signed_names = (re.search(r"SignedHeaders=([^,]+)", auth).group(1)).split(";")
        headers = {name: self.headers.get(name) for name in signed_names if name != "host"}
        headers["host"] = self.headers.get("Host", "")
        stamp = self.headers.get("x-amz-date", "")
        expected = signed_headers(self.command, f"http://{self.headers.get('Host')}{self.path}",
                                 headers, self.headers.get("x-amz-content-sha256", ""),
                                 ACCESS_KEY, SECRET_KEY, amzdate=stamp)
        if expected["Authorization"] != auth:
            return "подпись не совпала"
        return None

    def _send(self, status: int, body: bytes = b"", extra: dict | None = None) -> None:
        self.send_response(status)
        for name, value in (extra or {}).items():
            self.send_header(name, value)
        if status != 204:
            self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body and status != 204:
            self.wfile.write(body)

    def do_PUT(self) -> None:                              # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        self._record(body)
        broken = self._verify(body)
        if broken:
            self._send(403, f"<Error><Code>SignatureDoesNotMatch</Code><Message>{broken}"
                            f"</Message></Error>".encode())
            return
        if self._key().startswith("forbidden/"):
            self._send(403, b"<Error><Code>AccessDenied</Code></Error>")
            return
        OBJECTS[self._key()] = body
        self._send(200, b"", {"ETag": f'"{hashlib.md5(body).hexdigest()}"'})

    def do_GET(self) -> None:                              # noqa: N802
        self._record()
        broken = self._verify(b"")
        if broken:
            self._send(403, b"<Error><Code>SignatureDoesNotMatch</Code></Error>")
            return
        if self._key() not in OBJECTS:
            self._send(404, b"<Error><Code>NoSuchKey</Code></Error>")
            return
        body = OBJECTS[self._key()]
        self._send(200, body, {"ETag": f'"{hashlib.md5(body).hexdigest()}"'})

    def do_HEAD(self) -> None:                             # noqa: N802
        self._record()
        broken = self._verify(b"")        # настоящий R2 проверяет подпись и на HEAD
        if broken:
            self._send(403, b"<Error><Code>SignatureDoesNotMatch</Code></Error>")
            return
        if self._key() not in OBJECTS:
            self._send(404)
            return
        body = OBJECTS[self._key()]
        self._send(200, b"", {"ETag": f'"{hashlib.md5(body).hexdigest()}"',
                              "Content-Length": str(len(body))})


def start_fake_r2() -> tuple[ThreadingHTTPServer, str]:
    OBJECTS.clear()
    REQUESTS.clear()
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), FakeS3)
    endpoint = f"http://127.0.0.1:{httpd.server_address[1]}"
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, endpoint


def make_client(endpoint: str) -> R2Client:
    return R2Client(BUCKET, ACCESS_KEY, SECRET_KEY, endpoint=endpoint)


def http_call(url: str, *, method: str = "GET", token: str = "", body: bytes = b"") -> tuple[int, str]:
    headers = {"x-radar-token": token} if token else {}
    request = urllib.request.Request(url, method=method, headers=headers,
                                     data=(body if body else None))
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")


# --------------------------------------------------------------------- тесты

async def main() -> None:
    checks: list[tuple[str, bool, str]] = []
    shutil.rmtree(WORKDIR, ignore_errors=True)
    WORKDIR.mkdir(parents=True, exist_ok=True)
    httpd, endpoint = start_fake_r2()
    client = make_client(endpoint)

    # -------------------------------------------- 1. подпись SigV4 (эталонный вектор AWS)
    section("1. Подпись AWS Signature V4")
    vector_url = "https://example.amazonaws.com/"
    vector_headers = {"host": "example.amazonaws.com", "x-amz-date": "20150830T123600Z"}
    request_text = canonical_request("GET", vector_url, vector_headers, EMPTY_SHA256)
    digest = hashlib.sha256(request_text.encode("utf-8")).hexdigest()
    checks.append(("канонический запрос совпадает с эталоном AWS (get-vanilla)",
                   digest == "bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63",
                   digest[:32] + "…"))
    signed = signed_headers("GET", vector_url, {"host": "example.amazonaws.com"}, EMPTY_SHA256,
                            "AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                            region="us-east-1", service="service",
                            amzdate="20150830T123600Z", sign_content_hash=False)
    checks.append(("итоговая подпись совпадает с эталоном AWS",
                   signed["Authorization"].endswith(
                       "Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"),
                   signed["Authorization"][-70:]))
    checks.append(("в подписи правильный scope и список заголовков",
                   "Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request" in signed["Authorization"]
                   and "SignedHeaders=host;x-amz-date" in signed["Authorization"],
                   signed["Authorization"][:80]))
    r2_signed = signed_headers("GET", f"{endpoint}/{BUCKET}/{KEY_DB}", {}, EMPTY_SHA256,
                               ACCESS_KEY, SECRET_KEY, amzdate="20260925T120000Z")
    checks.append(("для R2 регион «auto», сервис s3, подписан хэш тела",
                   "/20260925/auto/s3/aws4_request" in r2_signed["Authorization"]
                   and "x-amz-content-sha256" in r2_signed["Authorization"],
                   r2_signed["Authorization"][:90]))
    checks.append(("ключ подписи детерминирован и зависит от даты",
                   signing_key(SECRET_KEY, "20260925") != signing_key(SECRET_KEY, "20260926")
                   and signing_key(SECRET_KEY, "20260925") == signing_key(SECRET_KEY, "20260925"),
                   "ok"))
    checks.append(("uri_encode не трогает незакодированные символы AWS и кодирует пробел",
                   uri_encode("a-b_c.d~e") == "a-b_c.d~e" and uri_encode("a b") == "a%20b"
                   and uri_encode("a/b", encode_slash=False) == "a/b", uri_encode("a b/ч")))

    # -------------------------------------------- 2. клиент R2 на подставном сервере
    section("2. Клиент R2")
    payload = "находка: посылка Варшава → Минск".encode("utf-8")
    client.put_object("db/probe.bin", payload)
    checks.append(("PUT/GET: байты дошли без изменений (utf-8 включительно)",
                   client.get_object("db/probe.bin") == payload, f"{len(payload)} байт"))
    checks.append(("R2 принял подпись: сервер пересчитал её и не вернул 403",
                   all(request["auth"].startswith("AWS4-HMAC-SHA256") for request in REQUESTS)
                   and not any("SignatureDoesNotMatch" in str(request) for request in REQUESTS),
                   f"{len(REQUESTS)} запросов"))
    last = REQUESTS[-1]
    checks.append(("подписаны host, дата и хэш тела, а хэш тела совпадает с отправленным",
                   {"host", "x-amz-content-sha256", "x-amz-date"} <= set(last["signed_headers"])
                   and last["content_sha256"] == last["body_sha256"],
                   ",".join(last["signed_headers"])))
    checks.append(("нет объекта -> None (404 это не ошибка, а «ещё не сохранено»)",
                   client.get_object("db/missing-object") is None, "None"))
    checks.append(("head_object отдаёт ETag, по нему клиент пропускает неизменённые файлы",
                   client.etag("db/probe.bin") == hashlib.md5(payload).hexdigest(),
                   client.etag("db/probe.bin")[:16]))
    try:
        client.put_object("forbidden/x", b"1")
        denied = "исключения не было"
    except R2Error as exc:
        denied = str(exc)
    checks.append(("403 от R2 превращается в понятную ошибку про права токена",
                   "нет доступа" in denied and "R2_ACCESS_KEY_ID" in denied, denied[:90]))
    try:
        client.url("bad key with spaces")
        rejected = False
    except ValueError:
        rejected = True
    checks.append(("ключ с пробелами отклоняется (иначе подпись разъедется с запросом)",
                   rejected, "ValueError"))

    # -------------------------------------------- 3. файлы <-> объекты
    section("3. Файлы и объекты")
    source = WORKDIR / "probe.txt"
    source.write_text("первая версия", encoding="utf-8")
    first = client.upload(source, "db/probe.txt")
    second = client.upload(source, "db/probe.txt")
    source.write_text("вторая версия", encoding="utf-8")
    third = client.upload(source, "db/probe.txt")
    checks.append(("загрузка: первый раз «ok», без изменений «skipped», после правки снова «ok»",
                   (first, second, third) == ("ok", "skipped", "ok"), f"{first}/{second}/{third}"))
    target = WORKDIR / "downloads" / "probe.txt"
    status = client.download("db/probe.txt", target)
    checks.append(("скачивание создаёт папки и возвращает содержимое",
                   status == "ok" and target.read_text(encoding="utf-8") == "вторая версия",
                   target.read_text(encoding="utf-8")[:20]))
    checks.append(("скачивание отсутствующего объекта — «missing», без исключения",
                   client.download("db/missing-object", WORKDIR / "nope.txt") == "missing", "missing"))
    checks.append(("upload отсутствующего файла — «missing» (не роняет проход)",
                   client.upload(WORKDIR / "нет-такого.txt", "db/x") == "missing", "missing"))

    # -------------------------------------------- 4. база и WAL
    section("4. База и WAL-чекпоинт")
    from monitor import HitStore                     # noqa: PLC0415 - тяжёлый импорт не в начале
    db_path = WORKDIR / "hits.sqlite3"
    probe_store = HitStore(str(db_path))             # настоящая база радара: WAL включён в ней же
    probe_store.bump_stats("@granica_polska", scanned=412, matched=2, saved=1, account="main")
    wal_exists = (WORKDIR / "hits.sqlite3-wal").exists()
    checkpoint_db(db_path)
    probe_store.conn.close()
    checks.append(("до чекпоинта рядом с базой лежит -wal, после — только один файл",
                   wal_exists and not (WORKDIR / "hits.sqlite3-wal").exists()
                   and not (WORKDIR / "hits.sqlite3-shm").exists(),
                   f"wal был: {wal_exists}"))
    saved = client.upload(db_path, KEY_DB, skip_unchanged=False)
    pulled = client.get_object(KEY_DB) or b""
    probe = WORKDIR / "pulled.sqlite3"
    probe.write_bytes(pulled)
    reopened = HitStore(str(probe))
    total = reopened.scanned_total()
    reopened.conn.close()
    checks.append(("в R2 уезжает база С данными из WAL (иначе находки терялись бы)",
                   saved == "ok" and total == 412, f"прочитано в скачанной базе: {total}"))
    checks.append(("чекпоинт несуществующей базы — просто False, без исключения",
                   checkpoint_db(WORKDIR / "нет-базы.sqlite3") is False, "False"))

    # -------------------------------------------- 5. состояние целиком
    section("5. Восстановление и сохранение состояния")
    clean = WORKDIR / "clean"
    clean.mkdir(parents=True, exist_ok=True)
    (clean / "monitor_session.session").write_bytes(SESSION_BYTES)
    shutil.copy(db_path, clean / "hits.sqlite3")
    checkpoint_db(clean / "hits.sqlite3")
    (clean / "metrics.csv").write_text("ts,rss_mb\n", encoding="utf-8")
    report = save_state(client, clean, sessions=("monitor_session",), log=quiet)
    checks.append(("save_state кладёт в R2 базу, сессию и metrics.csv",
                   report["db"] in ("ok", "skipped") and report["sessions"]["monitor_session"] == "ok"
                   and report["metrics"] == "ok"
                   and KEY_DB in OBJECTS and "sessions/monitor_session.session" in OBJECTS,
                   json.dumps(report, ensure_ascii=False)[:110]))

    fresh = WORKDIR / "fresh"
    fresh.mkdir(parents=True, exist_ok=True)
    restored = restore_state(client, fresh, sessions=("monitor_session",), log=quiet)
    checks.append(("restore_state возвращает сессию и базу в пустую папку (как после сна)",
                   restored["sessions"]["monitor_session"] == "ok" and restored["db"] == "ok"
                   and (fresh / "monitor_session.session").read_bytes() == SESSION_BYTES,
                   json.dumps(restored, ensure_ascii=False)[:110]))
    empty = WORKDIR / "empty"
    empty.mkdir(parents=True, exist_ok=True)
    missing_report = restore_state(client, empty, sessions=("second_session",), log=quiet)
    checks.append(("нет сессии в R2 — это видно в отчёте, а не падение",
                   missing_sessions(missing_report) == ["second_session"]
                   and missing_report["db"] == "ok", str(missing_report["sessions"])))
    no_client = restore_state(None, empty, log=quiet)
    checks.append(("без R2 (client=None) радар не падает, но честно пишет, что состояния нет",
                   no_client["db"] == "missing" and no_client["sessions"] == {},
                   json.dumps(no_client, ensure_ascii=False)[:60]))
    checks.append(("session_key нормализует имя и не пускает мусор",
                   session_key("main_session") == "sessions/main_session.session"
                   and session_key("main_session.session") == "sessions/main_session.session",
                   session_key("main_session")))
    checks.append(("ключи R2 фиксированы: база, метрики и лог лежат там, где ищет Worker",
                   (KEY_DB, KEY_METRICS, KEY_LOG) == ("db/hits.sqlite3", "metrics/metrics.csv",
                                                      "logs/last-run.log"), KEY_DB))

    # -------------------------------------------- 6. проход радара
    section("6. Проход радара в контейнере")
    calls: list[dict] = []

    def fake_run(command, **kwargs):
        calls.append({"command": command, "cwd": kwargs.get("cwd"), "timeout": kwargs.get("timeout")})
        return types.SimpleNamespace(returncode=0,
                                     stdout="[i] прочитано 412\n[+] найдено 2\nИТОГ: ок\n",
                                     stderr="")

    bare = WORKDIR / "bare"
    bare.mkdir(parents=True, exist_ok=True)
    runner = cloud_entry.RadarRunner(workdir=bare, client=client, runner=fake_run,
                                     args="--once --catchup 0 --notify bot --mode B",
                                     sessions=("monitor_session",), log=quiet)
    OBJECTS.pop("sessions/monitor_session.session", None)   # сессии нет ни в R2, ни на диске
    result = runner.run_pass()
    checks.append(("без файла сессии проход НЕ запускается (иначе Telethon спросил бы код)",
                   result["ok"] is False and "нет файла сессии" in str(result.get("error"))
                   and not calls, str(result.get("error"))[:60]))
    checks.append(("в подсказке — готовая команда загрузки сессии в R2",
                   "wrangler r2 object put" in str(result.get("hint", ""))
                   and "sessions/monitor_session.session" in str(result.get("hint", "")),
                   str(result.get("hint"))[:80]))

    (bare / "monitor_session.session").write_bytes(SESSION_BYTES)
    calls.clear()
    result = runner.run_pass()
    checks.append(("с сессией на месте проход запускается и заканчивается ok",
                   result["ok"] is True and result["exit_code"] == 0 and len(calls) == 1,
                   json.dumps({k: v for k, v in result.items() if k in ("ok", "exit_code", "seconds")},
                              ensure_ascii=False)))
    checks.append(("команда прохода — monitor.py с аргументами схемы B",
                   calls[0]["command"][1:] == ["monitor.py", "--once", "--catchup", "0",
                                               "--notify", "bot", "--mode", "B"],
                   " ".join(calls[0]["command"][1:])))
    checks.append(("проход ограничен по времени (иначе cron наложится на следующий)",
                   calls[0]["timeout"] == cloud_entry.DEFAULT_TIMEOUT, str(calls[0]["timeout"])))
    checks.append(("выжимка из лога содержит только осмысленные строки",
                   result["summary"] == ["[i] прочитано 412", "[+] найдено 2", "ИТОГ: ок"],
                   str(result["summary"])))
    checks.append(("лог прохода записан в файл и уехал в R2",
                   runner.log_path.exists() and KEY_LOG in OBJECTS
                   and "ИТОГ" in OBJECTS[KEY_LOG].decode("utf-8"), str(runner.log_path.name)))
    checks.append(("после прохода состояние сохранено (база и сессия)",
                   result["saved"]["db"] in ("ok", "skipped")
                   and result["saved"]["sessions"]["monitor_session"] in ("ok", "skipped"),
                   json.dumps(result["saved"], ensure_ascii=False)[:90]))

    runner.lock.acquire()
    busy = runner.run_pass()
    runner.lock.release()
    checks.append(("второй проход во время первого — отказ, а не два радара на одну сессию",
                   busy.get("busy") is True and "уже идёт" in str(busy.get("error")),
                   str(busy.get("error"))[:40]))

    def timeout_run(command, **kwargs):
        raise subprocess.TimeoutExpired(command, kwargs.get("timeout", 1),
                                        output="[i] прочитано 12\n", stderr="")

    slow = cloud_entry.RadarRunner(workdir=bare, client=client, runner=timeout_run,
                                   timeout=3, sessions=("monitor_session",), log=quiet)
    slow_result = slow.run_pass()
    checks.append(("таймаут прохода — понятная ошибка, а не зависший контейнер",
                   slow_result["ok"] is False and "таймаут" in str(slow_result.get("error"))
                   and slow_result.get("log_tail"), str(slow_result.get("error"))[:50]))

    def broken_run(command, **kwargs):
        return types.SimpleNamespace(returncode=2, stdout="", stderr="[!] ошибка сессии: AUTH_KEY_UNREGISTERED\n")

    broken = cloud_entry.RadarRunner(workdir=bare, client=client, runner=broken_run,
                                     sessions=("monitor_session",), log=quiet)
    broken_result = broken.run_pass()
    checks.append(("ненулевой код возврата радара виден в результате прохода",
                   broken_result["ok"] is False and broken_result["exit_code"] == 2
                   and "AUTH_KEY_UNREGISTERED" in broken_result["log_tail"],
                   str(broken_result.get("exit_code"))))
    checks.append(("исключение внутри прохода не роняет сервер контейнера",
                   isinstance(cloud_entry.RadarRunner(workdir=bare, client=client,
                                                      runner=lambda *a, **k: (_ for _ in ()).throw(OSError("диск отвалился")),
                                                      sessions=("monitor_session",),
                                                      log=quiet).run_pass().get("error"), str),
                   "OSError пойман"))

    # -------------------------------------------- 7. HTTP-эндпоинты контейнера
    section("7. Эндпоинты контейнера")
    cloud_entry.Handler.log_message = lambda self, *_a: None    # меньше шума в выводе теста
    served = cloud_entry.RadarRunner(workdir=bare, client=client, runner=fake_run,
                                     sessions=("monitor_session",), log=quiet)
    served_token = "secret-token-123"
    server = cloud_entry.serve(served, token=served_token, port=0, bootstrap=False)
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        status, body = http_call(f"{base}/healthz")
        checks.append(("/healthz отвечает 204 без пароля (сюда смотрит pingEndpoint)",
                       status == 204 and body == "", str(status)))
        status, body = http_call(f"{base}/")
        checks.append(("/ без пароля отдаёт справку и не показывает данные",
                       status == 200 and "схема B" in body and "token" in body, str(status)))
        status, body = http_call(f"{base}/status")
        checks.append(("/status без пароля — 403 (наружу данные не отдаём)",
                       status == 403 and "x-radar-token" in body, str(status)))
        # заголовки HTTP живут в latin-1: токен обязан быть ASCII (это же верно и в бою)
        status, body = http_call(f"{base}/status", token="wrong-token")
        checks.append(("чужой токен — тоже 403", status == 403, str(status)))
        status, body = http_call(f"{base}/status", token=served_token)
        payload = json.loads(body) if status == 200 else {}
        checks.append(("/status со своим токеном — JSON с итогом прохода и состоянием R2",
                       status == 200 and payload.get("r2") == "настроен"
                       and "last_run" in payload and payload.get("db_mb") is not None,
                       str(list(payload)[:5])))
        status, body = http_call(f"{base}/run", method="POST", token=served_token)
        checks.append(("POST /run запускает проход и возвращает итог",
                       status == 200 and json.loads(body).get("ok") is True, str(status)))
        status, body = http_call(f"{base}/usage", token=served_token)
        checks.append(("/usage отдаёт текст расхода (тот же, что бот показывает в Telegram)",
                       status == 200 and ("Расход" in body or "базы hits.sqlite3" in body),
                       body.splitlines()[0][:60] if body else "(пусто)"))
        status, body = http_call(f"{base}/log", token=served_token)
        checks.append(("/log отдаёт лог последнего прохода",
                       status == 200 and "ИТОГ" in body, str(status)))
        status, body = http_call(f"{base}/metrics.csv", token=served_token)
        checks.append(("/metrics.csv — либо файл, либо честное 404 «нужен хотя бы один проход»",
                       status in (200, 404) and (status == 404 or "ts,rss_mb" in body), str(status)))
        status, body = http_call(f"{base}/check")
        checks.append(("/check без пароля — 403 (как и остальные данные)", status == 403, str(status)))
        status, body = http_call(f"{base}/check", token=served_token)
        report = json.loads(body) if body.startswith("{") else {}
        checks.append(("/check отвечает JSON-отчётом: 200 — готов, 409 — есть что исправить",
                       status in (200, 409) and "problems" in report and "r2" in report
                       and "sessions" in report, f"HTTP {status}"))
        status, body = http_call(f"{base}/{urllib.parse.quote('нет-такого')}", token=served_token)
        checks.append(("неизвестный путь — 404 со справкой", status == 404 and "нет такого пути" in body,
                       str(status)))
    finally:
        server.shutdown()
        server.server_close()

    checks.append(("про не-ASCII токен есть предупреждение: заголовки HTTP живут в latin-1",
                   cloud_entry.token_warning("секрет-токен").startswith("RADAR_TOKEN")
                   and cloud_entry.token_warning("abc-123_XY") == "",
                   "предупреждение для не-ASCII, тишина для ASCII"))

    cyr = cloud_entry.serve(served, token="секрет-токен", port=0, bootstrap=False)
    cyr_base = f"http://127.0.0.1:{cyr.server_address[1]}"
    try:
        # токен с кириллицей приходит через ?token= — заголовком его передать нельзя вовсе
        status, _body = http_call(f"{cyr_base}/status?token={urllib.parse.quote('секрет-токен')}")
        checks.append(("токен с кириллицей сравнивается по байтам и не роняет запрос",
                       status == 200, str(status)))
        status, _body = http_call(f"{cyr_base}/status?token={urllib.parse.quote('неверно')}")
        checks.append(("чужой кириллический токен — 403", status == 403, str(status)))
    finally:
        cyr.shutdown()
        cyr.server_close()

    open_server = cloud_entry.serve(cloud_entry.RadarRunner(workdir=bare, log=quiet),
                                    token="", port=0, bootstrap=False)
    open_base = f"http://127.0.0.1:{open_server.server_address[1]}"
    try:
        status, _body = http_call(f"{open_base}/status")
        checks.append(("без RADAR_TOKEN эндпоинты открыты (вариант для отладки) — и это видно в логе",
                       status == 200, str(status)))
    finally:
        open_server.shutdown()
        open_server.server_close()

    # ---------- готовность без прохода: первый деплой упирается именно в эти три вещи
    good_env = {"TG_API_ID": "1234567", "TG_API_HASH": "0" * 32,
                "TG_BOT_TOKEN": "1234567890:AAH-test-token-abcdefghij",
                "TG_NOTIFY_CHAT": "999888777"}
    ready = cloud_entry.RadarRunner(workdir=bare, client=client, runner=fake_run,
                                    sessions=("monitor_session",), log=quiet)
    report = ready.preflight(env=good_env)
    checks.append(("preflight: всё настроено — ok, проблем нет, следующий шаг POST /run",
                   report["ok"] is True and report["problems"] == []
                   and report["next"] == "POST /run", str(report["problems"])[:110]))
    checks.append(("preflight видит сессию и бакет в R2 (и размер, чтобы отличить пустой файл)",
                   str(report["r2"]["objects"].get("sessions/monitor_session.session", ""))
                   .startswith("есть (") and report["r2"]["bucket"] == BUCKET,
                   str(report["r2"]["objects"])[:110]))

    ghost = cloud_entry.RadarRunner(workdir=bare, client=client, runner=fake_run,
                                    sessions=("ghost_session",), log=quiet)
    problem = " ".join(ghost.preflight(env=good_env)["problems"])
    checks.append(("preflight: нет сессии в R2 — готова команда загрузки (не «где-то ошибка»)",
                  "wrangler r2 object put" in problem and "sessions/ghost_session.session" in problem,
                  problem[:110]))

    wrong = R2Client(BUCKET, "WRONG-ACCESS-KEY", "WRONG-SECRET-KEY", endpoint=endpoint)
    broken = cloud_entry.RadarRunner(workdir=bare, client=wrong, runner=fake_run,
                                     sessions=("monitor_session",), log=quiet)
    problem = " ".join(broken.preflight(env=good_env)["problems"])
    checks.append(("preflight: неверные ключи R2 видны за секунду, а не после прохода",
                  "R2 отказал" in problem and "R2_ACCESS_KEY_ID" in problem, problem[:110]))

    no_r2 = cloud_entry.RadarRunner(workdir=bare, client=None, runner=fake_run,
                                    sessions=("monitor_session",), log=quiet)
    problem = " ".join(no_r2.preflight(env=good_env)["problems"])
    checks.append(("preflight: R2 не настроен — перечислены нужные переменные",
                  "R2_ACCOUNT_ID" in problem and "R2_ACCESS_KEY_ID" in problem, problem[:110]))

    problem = " ".join(ready.preflight(env=dict(good_env, TG_API_ID="", TG_API_HASH=""))["problems"])
    checks.append(("preflight: нет ключей приложения — подсказка wrangler secret put",
                  "wrangler secret put TG_API_ID" in problem
                  and "wrangler secret put TG_API_HASH" in problem, problem[:110]))

    problem = " ".join(ready.preflight(env=dict(good_env, TG_BOT_TOKEN="", TG_NOTIFY_CHAT=""))
                       ["problems"])
    checks.append(("preflight: «--notify bot» без токена бота виден заранее",
                  "TG_BOT_TOKEN" in problem and "TG_NOTIFY_CHAT" in problem, problem[:110]))
    console_runner = cloud_entry.RadarRunner(workdir=bare, client=client, runner=fake_run,
                                             args="--once --catchup 0 --notify console --mode B",
                                             sessions=("monitor_session",), log=quiet)
    report = console_runner.preflight(env=dict(good_env, TG_BOT_TOKEN="", TG_NOTIFY_CHAT=""))
    checks.append(("preflight: без «--notify bot» токен бота не требуется (ложных тревог нет)",
                  report["ok"] is True, str(report["problems"])[:110]))

    before = len(REQUESTS)
    ready.preflight(env=good_env)
    added = REQUESTS[before:]
    checks.append(("preflight делает только HEAD: ни прохода, ни скачивания состояния",
                  bool(added) and all(request["method"] == "HEAD" for request in added),
                  ",".join(sorted({request["method"] for request in added})) or "запросов нет"))
    checks.append(("/check заявлен в справке контейнера и в списке эндпоинтов /status",
                  "/check" in cloud_entry.INDEX_TEXT
                  and "/check" in ready.status()["endpoints"], ""))

    # -------------------------------------------- 8. конфиг деплоя
    section("8. Конфиг деплоя")

    # Самопроверка читает файлы репозитория, а запускается ещё и внутри сборки образа, где
    # .dockerignore часть из них вырезает. На этом уже падал реальный деплой: .gitignore в образ
    # не попадал, и проверка «файл с секретами в .gitignore» роняла сборку с FileNotFoundError.
    # Список ниже — явный договор: всё перечисленное обязано дойти до контейнера.
    read_by_selftest = [".dockerignore", ".gitignore", "DEPLOY.md", "Dockerfile", "wrangler.jsonc",
                        "src/index.js", "cloud_panel.bat", "cloud_panel.sh",
                        "deploy/cloud_entry.py", "deploy/r2_state.py", "deploy/secrets.example.env"]
    absent = [name for name in read_by_selftest if not (ROOT / name).exists()]
    checks.append(("всё, что читает самопроверка, дошло до контекста сборки (не вырезано)",
                   not absent,
                   (", ".join(absent) + " — вырезан .dockerignore, нужно исключение !имя") if absent else "ok"))

    config_text = repo_text("wrangler.jsonc")
    plain = re.sub(r"^\s*//.*$", "", config_text, flags=re.MULTILINE)
    config = json.loads(plain) if plain.strip() else {}
    checks.append(("wrangler.jsonc парсится (комментарии не ломают JSON)",
                   config.get("name") == "bot-scr-to-cloud" and config.get("main") == "src/index.js",
                   config.get("name", "?")))
    container = (config.get("containers") or [{}])[0]
    checks.append(("контейнер: класс, образ из Dockerfile, lite и ОДИН экземпляр",
                   container.get("class_name") == "RadarContainer" and container.get("image") == "./Dockerfile"
                   and container.get("instance_type") == "lite" and container.get("max_instances") == 1,
                   json.dumps(container, ensure_ascii=False)[:90]))
    checks.append(("максимум один экземпляр: две копии на одну сессию = AuthKeyDuplicatedError",
                   container.get("max_instances") == 1, str(container.get("max_instances"))))
    bindings = (config.get("durable_objects") or {}).get("bindings") or []
    migrations = config.get("migrations") or []
    checks.append(("привязка Durable Object и миграция new_sqlite_classes на месте",
                   bindings and bindings[0]["name"] == "RADAR"
                   and bindings[0]["class_name"] == "RadarContainer"
                   and migrations and migrations[0]["new_sqlite_classes"] == ["RadarContainer"],
                   str(migrations[0] if migrations else None)))
    checks.append(("cron раз в 10 минут (схема B)",
                   (config.get("triggers") or {}).get("crons") == ["*/10 * * * *"],
                   str((config.get("triggers") or {}).get("crons"))))
    secrets = ("TG_API_HASH", "TG_BOT_TOKEN", "R2_SECRET_ACCESS_KEY", "RADAR_TOKEN")
    checks.append(("секретов в wrangler.jsonc нет — только несекретные vars",
                   not any(f'"{name}"' in plain for name in secrets), ", ".join(secrets)))

    worker = repo_text("src", "index.js")
    checks.append(("Worker поднимает контейнер, ждёт порт и передаёт переменные",
                   "startAndWaitForPorts" in worker and "envVars" in worker
                   and "containerFetch" in worker and "getContainer" in worker, "ok"))
    checks.append(("Worker будит контейнер по cron и дожидается прохода",
                   "async scheduled" in worker and "runPass(env)" in worker, "ok"))
    checks.append(("контейнеру разрешён выход в интернет (без него нет ни Telegram, ни R2)",
                   "enableInternet = true" in worker, "ok"))
    checks.append(("sleepAfter больше длительности прохода: фон-работа таймер простоя не сбрасывает",
                   int(re.search(r"sleepAfter = '(\d+)m'", worker).group(1)) * 60
                   > cloud_entry.DEFAULT_TIMEOUT,
                   re.search(r"sleepAfter = '(\d+)m'", worker).group(0)))
    checks.append(("эндпоинт входа в Worker совпадает с точкой входа контейнера",
                   "deploy/cloud_entry.py" in worker, "ok"))
    checks.append(("/restart есть: после смены секретов контейнер надо пересоздать",
                   "'/restart'" in worker and "container.stop()" in worker, "ok"))
    checks.append(("/check проксируется Worker'ом и закрыт токеном (как остальные данные)",
                  "'/check'" in worker and "'/check': 'application/json" in worker, "ok"))

    # ---------- шаблон секретов для `wrangler secret bulk`: не должен разъехаться с Worker'ом
    template_text = repo_text("deploy", "secrets.example.env")
    doc_text = repo_text("DEPLOY.md")
    template = {}
    for line in template_text.splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            template[key.strip()] = value.strip()
    passed = set(re.findall(r"^\t\t([A-Z][A-Z0-9_]+):", worker, flags=re.MULTILINE))
    non_secret = set((config.get("vars") or {}).keys())
    expected = sorted(passed - non_secret)
    checks.append(("шаблон секретов = ровно то, что Worker передаёт в контейнер, минус vars",
                   sorted(template) == expected and len(expected) == 8,
                   f"в шаблоне {sorted(template)}; ждём {expected}"))
    checks.append(("в шаблоне нет имён из vars: var и secret с одним именем роняют деплой",
                   not (set(template) & non_secret), str(sorted(set(template) & non_secret))))
    placeholder = re.compile(r"^(?:[x0]+|1234567|123456789|1234567890:AAHx+|change-me-[\w-]+)$")
    checks.append(("в шаблоне только заглушки: настоящие значения в git попасть не могут",
                   all(placeholder.match(value) for value in template.values()),
                   ", ".join(k for k, v in template.items() if not placeholder.match(v)) or "ok"))
    gitignore = repo_text(".gitignore").splitlines()
    checks.append(("заполненная копия deploy/secrets.env в .gitignore (и не только она)",
                   "deploy/secrets.env" in gitignore and "secrets.json" in gitignore
                   and "secrets.txt" in gitignore,
                   ".gitignore не прочитан: файла нет в контексте" if not gitignore
                   else "нет строк " + ", ".join(name for name in ("deploy/secrets.env", "secrets.json",
                                                                    "secrets.txt")
                                                 if name not in gitignore)))
    checks.append(("шаблон объясняет, почему нельзя грузить общий .env проекта",
                   "TG_SESSION" in template_text and ".env проекта" in template_text, "ok"))
    checks.append(("DEPLOY.md даёт загрузку одним файлом и команду удалить его",
                  "wrangler secret bulk" in doc_text and "del deploy" in doc_text
                  and "rm deploy/secrets.env" in doc_text, "ok"))

    dockerfile = repo_text("Dockerfile")
    checks.append(("Dockerfile ставит зависимости из requirements.txt и запускает cloud_entry",
                   "COPY requirements.txt" in dockerfile and "pip install" in dockerfile
                   and 'CMD ["python", "-u", "deploy/cloud_entry.py"]' in dockerfile, "ok"))
    checks.append(("в образе проверяются импорты и офлайн-тесты ещё до деплоя",
                   "import monitor, bot_panel, metrics" in dockerfile
                   and "deploy/selftest_cloud.py" in dockerfile, "ok"))
    ignore = repo_text(".dockerignore")
    ignore_lines = ignore.splitlines()
    checks.append((".dockerignore не пускает в образ .env и *.session (иначе секрет уедет в реестр)",
                   ".env" in ignore_lines and "*.session" in ignore_lines
                   and "*.sqlite3" in ignore_lines, "ok"))
    # заполненная копия шаблона: восемь настоящих значений. Слои образа скачиваемы, поэтому
    # «забыл удалить deploy/secrets.env» не должно означать «секреты уехали в реестр».
    checks.append(("заполненный deploy/secrets.env вырезан и из образа, не только из git",
                   "deploy/secrets.env" in ignore_lines and "secrets.json" in ignore_lines
                   and "secrets.txt" in ignore_lines, "ok"))
    checks.append(("шаблон deploy/secrets.example.env в образе остаётся: его читает проверка",
                   (ROOT / "deploy" / "secrets.example.env").exists()
                   and "deploy/secrets.example.env" not in ignore_lines, "ok"))
    # эта самопроверка запускается и при сборке образа (RUN в Dockerfile), поэтому всё,
    # что она читает, обязано в образ попасть: *.md выкинут, DEPLOY.md возвращён исключением
    excluded_without_exception = sorted(name for name in ("DEPLOY.md", ".gitignore")
                                         if f"!{name}" not in ignore_lines)
    checks.append(("DEPLOY.md и .gitignore попадают в образ: самопроверка читает их при сборке",
                   not excluded_without_exception,
                   ", ".join(f"{n} вырезан, исключения !{n} нет" for n in excluded_without_exception)
                   or "ok"))
    for needed in ("cloud_panel.bat", "cloud_panel.sh", "DEPLOY.md", "deploy/cloud_entry.py"):
        checks.append((f"{needed} не выкинут из образа (.dockerignore)",
                       needed not in ignore_lines
                       and not any(line == "*.bat" or line == "*.sh" for line in ignore_lines),
                       "ok"))

    # панель на облачной базе: снимок из R2 + --panel-only (ключи аккаунта не нужны)
    for script in ("cloud_panel.bat", "cloud_panel.sh"):
        text = repo_text(script)
        checks.append((f"{script}: качает базу из R2 с --remote и запускает панель на снимке",
                       "db/hits.sqlite3" in text and "--remote" in text
                       and "--panel-only" in text and "radar-state" in text, "ok"))

    # инструкция не должна расходиться с кодом: иначе деплой встанет на ровном месте
    deploy_doc = repo_text("DEPLOY.md")
    entry_src = repo_text("deploy", "cloud_entry.py")
    r2_src = repo_text("deploy", "r2_state.py")
    names = set(re.findall(r'(?:env\.get|os\.getenv)\(\s*"([A-Z][A-Z0-9_]+)"', entry_src + r2_src))
    names |= set(re.findall(r"env\.([A-Z][A-Z0-9_]+)", worker))
    names |= set((config.get("vars") or {}).keys())
    undocumented = sorted(name for name in names if name not in deploy_doc)
    checks.append((f"каждая переменная окружения описана в DEPLOY.md ({len(names)} шт.)",
                   not undocumented, ", ".join(undocumented) or "ok"))
    for secret in ("TG_API_ID", "TG_API_HASH", "TG_BOT_TOKEN", "TG_NOTIFY_CHAT", "R2_ACCOUNT_ID",
                   "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "RADAR_TOKEN"):
        checks.append((f"DEPLOY.md говорит ввести секрет {secret}",
                       f"wrangler secret put {secret}" in deploy_doc, "ok"))
    checks.append(("DEPLOY.md: бакет, загрузка сессии, production-ветка и проверка после деплоя",
                   all(step in deploy_doc for step in
                       ("Create bucket", "wrangler r2 object put radar-state/sessions/",
                        "Production branch", "POST \"$URL/run", "npx wrangler tail")), "ok"))
    checks.append(("DEPLOY.md предупреждает: токен только ASCII (заголовки HTTP живут в latin-1)",
                   "ASCII" in deploy_doc and "latin-1" in deploy_doc, "ok"))
    checks.append(("DEPLOY.md предупреждает: нельзя держать радар на ПК и в облаке одновременно",
                   "AuthKeyDuplicatedError" in deploy_doc, "ok"))

    # -------------------------------------------- итог
    section("Итог")
    for name, ok, detail in checks:
        print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail else ""))
    failed = [name for name, ok, _ in checks if not ok]
    print("\nИТОГ:", "всё ок" if not failed else f"провалено: {failed}")
    httpd.shutdown()
    httpd.server_close()
    shutil.rmtree(WORKDIR, ignore_errors=True)
    if failed:
        raise SystemExit(1)


def quiet(*_args, **_kwargs) -> None:
    """Заглушка логов: в тесте важны проверки, а не текст в консоли."""


if __name__ == "__main__":
    import asyncio

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] прервано")
