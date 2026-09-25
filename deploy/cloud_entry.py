#!/usr/bin/env python3
"""
Точка входа радара в Cloudflare Containers (схема B: проход по расписанию).

Зачем этот файл. Контейнер Cloudflare — не обычный сервер: он просыпается по запросу
Worker'а, диск у него эфемерный, а входящих TCP-портов из интернета нет вовсе. Поэтому
радар здесь живёт так:

    cron в Worker'е (каждые 10 минут)
        -> поднять/разбудить контейнер
        -> POST /run
             -> восстановить .session и hits.sqlite3 из R2
             -> monitor.py --once --catchup 0 --notify bot --mode B
             -> вернуть базу, сессии, metrics.csv и лог в R2
        -> контейнер засыпает через sleepAfter

Сам радар при этом не переписан: это обёртка, которая запускает `monitor.py` подпроцессом
и отвечает на несколько HTTP-запросов (их дёргает Worker, наружу они не торчат).

Эндпоинты:
    GET  /healthz, /ping  готовность (без пароля — сюда смотрит pingEndpoint при старте)
    POST /run             один проход радара (восстановить -> пройти -> сохранить)
    GET  /status          итог последнего прохода в JSON
    GET  /usage           расход и вердикт «A или B» текстом (как /usage у бота)
    GET  /metrics.csv     файл срезов расхода
    GET  /log             лог последнего прохода
    GET  /                короткая справка (без пароля, без данных)

Пароль: если задан RADAR_TOKEN, все эндпоинты кроме /healthz, /ping и / требуют заголовок
`x-radar-token` (или `?token=`). Worker передаёт тот же токен, что лежит у него в секретах.
"""
from __future__ import annotations

import hmac
import json
import os
import shlex
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from r2_state import (R2Client, checkpoint_db, missing_sessions, restore_state,  # noqa: E402
                      save_state)

DEFAULT_ARGS = "--once --catchup 0 --notify bot --mode B"
DEFAULT_PORT = 8080
DEFAULT_TIMEOUT = 540.0          # 9 минут: проход должен успеть до следующего cron (10 минут)
LOG_NAME = "last-run.log"
SUMMARY_MARKERS = ("ИТОГ", "переслано", "найдено", "прочитано", "[+]", "[!]", "FloodWait")


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on", "да")


class RadarRunner:
    """Один проход радара: состояние из R2 -> monitor.py -> состояние обратно в R2."""

    def __init__(self, workdir: str | Path = ".", client: R2Client | None = None,
                 args: str = DEFAULT_ARGS, python: str | None = None,
                 timeout: float = DEFAULT_TIMEOUT, sessions: tuple[str, ...] = ("monitor_session",),
                 db_name: str = "hits.sqlite3", metrics_name: str = "metrics.csv",
                 runner=subprocess.run, log=print):
        self.workdir = Path(workdir).resolve()
        self.client = client
        self.args = str(args or DEFAULT_ARGS)
        self.python = python or sys.executable or "python3"
        self.timeout = float(timeout or DEFAULT_TIMEOUT)
        self.sessions = tuple(sessions or ("monitor_session",))
        self.db_name = db_name
        self.metrics_name = metrics_name
        self._runner = runner
        self._log = log
        self.lock = threading.Lock()
        self.log_path = self.workdir / "logs" / LOG_NAME
        self.last: dict = {"ok": None, "finished_at": None, "note": "проходов ещё не было"}
        self.started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # --- состояние

    def restore(self) -> dict:
        """Тянет .session, базу, metrics.csv и (если есть) sources.yaml из R2."""
        return restore_state(self.client, self.workdir, sessions=self.sessions,
                             db_name=self.db_name, metrics_name=self.metrics_name,
                             log=self._log)

    def save(self, with_log: bool = True) -> dict:
        """Возвращает в R2 базу (после WAL-чекпоинта), сессии, metrics.csv и лог."""
        return save_state(self.client, self.workdir, sessions=self.sessions,
                          db_name=self.db_name, metrics_name=self.metrics_name,
                          log_file=str(self.log_path) if with_log and self.log_path.exists() else None,
                          log=self._log)

    def missing_session_files(self) -> list[str]:
        """Каких файлов сессии нет на диске — с ними запускать радар нельзя."""
        return [name for name in self.sessions
                if not (self.workdir / f"{name}.session").exists()]

    # --- проход

    def command(self) -> list[str]:
        return [self.python, "monitor.py", *shlex.split(self.args)]

    def run_pass(self) -> dict:
        """Полный цикл прохода. Повторный вызов во время прохода — отказ (409 снаружи)."""
        if not self.lock.acquire(blocking=False):
            return dict(self.last, ok=False, busy=True,
                        error="проход уже идёт — дождись окончания")
        started = time.time()
        try:
            result = self._run_pass_locked(started)
        except Exception as exc:                       # noqa: BLE001 - контейнер должен жить
            result = {
                "ok": False, "error": f"{type(exc).__name__}: {exc}", "started_at": self.started_at,
                "seconds": round(time.time() - started, 1),
                "hint": "смотри /log и логи контейнера в дашборде Cloudflare",
            }
        finally:
            self.lock.release()      # без этого второй проход в том же контейнере не запустился бы
        result["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.last = result
        return result

    def _run_pass_locked(self, started: float) -> dict:
        restored = self.restore()
        missing = self.missing_session_files()
        if missing:
            names = ", ".join(missing)
            return {
                "ok": False, "restored": restored, "seconds": round(time.time() - started, 1),
                "error": f"нет файла сессии: {names}",
                "hint": ("Войди в Telegram на своей машине (start.bat --login-qr) и загрузи файл "
                         "в R2, например:\n  npx wrangler r2 object put "
                         f"<бакет>/sessions/{missing[0]}.session --file {missing[0]}.session "
                         "--remote\nБез сессии радар начал бы спрашивать телефон и код, а ввода "
                         "в контейнере нет."),
            }
        command = self.command()
        self._log(f"[i] запуск: {' '.join(command)}")
        try:
            completed = self._runner(command, cwd=str(self.workdir), capture_output=True,
                                     text=True, timeout=self.timeout, env=self.environment())
        except subprocess.TimeoutExpired as exc:
            text = ((exc.stdout or "") + (exc.stderr or "")) if isinstance(exc.stdout, str) else ""
            self._write_log(text or f"проход не завершился за {self.timeout:g} с")
            return {"ok": False, "error": f"таймаут прохода ({self.timeout:g} с)",
                    "seconds": round(time.time() - started, 1), "restored": restored,
                    "hint": "уменьши число чатов или увеличь RADAR_TIMEOUT; лог сохранён",
                    "log_tail": self.read_log(2000)}
        output = (completed.stdout or "") + (completed.stderr or "")
        self._write_log(output)
        saved = self.save()
        return {
            "ok": completed.returncode == 0,
            "exit_code": completed.returncode,
            "seconds": round(time.time() - started, 1),
            "command": " ".join(command),
            "restored": restored, "saved": saved,
            "summary": summarize(output),
            "log_tail": output[-3000:],
        }

    def environment(self) -> dict:
        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"
        env.setdefault("TZ", "UTC")
        return env

    def _write_log(self, text: str) -> None:
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
            self.log_path.write_text(f"# {stamp}\n{text}", encoding="utf-8")
        except OSError as exc:
            self._log(f"[!] лог прохода не записан: {exc}")

    def read_log(self, tail: int = 20000) -> str:
        try:
            return self.log_path.read_text(encoding="utf-8")[-int(tail):]
        except OSError:
            return ""

    # --- отчёты

    def usage_text(self) -> str:
        """Тот же текст, что бот показывает на /usage (берётся из восстановленной базы)."""
        db_path = self.workdir / self.db_name
        if not db_path.exists():
            return ("Расход: базы hits.sqlite3 пока нет — ни одного прохода не сохранено.\n"
                    "Сделай POST /run, затем зайди сюда снова.")
        try:
            if str(self.workdir) not in sys.path:
                sys.path.insert(0, str(self.workdir))
            from bot_panel import BotPanel                     # noqa: PLC0415 - ленивый импорт
            from monitor import HitStore                       # noqa: PLC0415
            store = HitStore(str(db_path))
            try:
                panel = BotPanel(store, token="", chat_id="", accounts=[], mode="B",
                                 db_path=str(db_path))
                return panel.usage_text()
            finally:
                getattr(store.conn, "close", lambda: None)()   # контейнер живой: соединения не копим
        except Exception as exc:                               # noqa: BLE001
            return f"Не смог собрать /usage из базы: {type(exc).__name__}: {exc}"

    def status(self) -> dict:
        db_path = self.workdir / self.db_name
        size_mb = round(db_path.stat().st_size / 1048576.0, 2) if db_path.exists() else None
        return {
            "service": "telegram-radar (схема B, Cloudflare Containers)",
            "container_started_at": self.started_at,
            "r2": "настроен" if self.client is not None else "НЕ настроен (состояние не сохранится!)",
            "db_mb": size_mb,
            "args": self.args,
            "last_run": self.last,
            "endpoints": ["/run (POST)", "/status", "/usage", "/metrics.csv", "/log", "/healthz"],
        }


def summarize(text: str, limit: int = 14) -> list[str]:
    """Выжимка из лога прохода: только строки, которые человек реально читает."""
    picked: list[str] = []
    for line in (text or "").splitlines():
        stripped = line.strip()
        if stripped and any(marker in stripped for marker in SUMMARY_MARKERS):
            picked.append(stripped)
    return picked[-limit:]


class Handler(BaseHTTPRequestHandler):
    """HTTP-обвязка контейнера. Отвечает Worker'у, наружу не торчит."""

    runner: RadarRunner = None            # type: ignore[assignment]
    token: str = ""
    server_version = "radar-container/2"
    protocol_version = "HTTP/1.1"
    open_paths = ("/healthz", "/ping", "/")

    def log_message(self, fmt: str, *args) -> None:      # свой лог короче и без мусора
        sys.stderr.write(f"[http] {self.address_string()} {fmt % args}\n")

    # --- служебное

    def _send(self, status: int, body: str | bytes = b"", content_type: str = "text/plain; charset=utf-8") -> None:
        payload = body.encode("utf-8") if isinstance(body, str) else (body or b"")
        self.send_response(status)
        if status != 204:                       # у 204 тела нет, и Content-Length не положен
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if payload and status != 204:
            self.wfile.write(payload)

    def _authorized(self) -> bool:
        if not self.token:
            return True
        given = self.headers.get("x-radar-token") or ""
        if not given:
            from urllib.parse import parse_qs, urlparse
            given = (parse_qs(urlparse(self.path).query).get("token") or [""])[0]
        # compare_digest для строк не умеет не-ASCII, поэтому сравниваем байты:
        # иначе кириллический токен ронял бы любой запрос с TypeError
        return bool(given) and hmac.compare_digest(given.encode("utf-8"),
                                                   self.token.encode("utf-8"))

    def _path(self) -> str:
        from urllib.parse import urlparse
        return urlparse(self.path).path.rstrip("/") or "/"

    def _guard(self) -> bool:
        """Проверяет пароль; при отказе сама отвечает 403 и возвращает False."""
        if self._authorized():
            return True
        self._send(403, "нужен токен: заголовок x-radar-token или ?token=...\n"
                        "(секрет RADAR_TOKEN задаётся в дашборде Worker'а)\n")
        return False

    # --- маршруты

    def do_GET(self) -> None:                              # noqa: N802 - имя из stdlib
        path = self._path()
        if path in ("/healthz", "/ping"):
            self._send(204)
            return
        if path == "/":
            self._send(200, INDEX_TEXT)
            return
        if not self._guard():
            return
        if path == "/status":
            self._send(200, json.dumps(self.runner.status(), ensure_ascii=False, indent=2),
                       "application/json; charset=utf-8")
        elif path == "/usage":
            self._send(200, self.runner.usage_text() + "\n")
        elif path == "/metrics.csv":
            target = self.runner.workdir / self.runner.metrics_name
            if target.exists():
                self._send(200, target.read_bytes(), "text/csv; charset=utf-8")
            else:
                self._send(404, "metrics.csv ещё нет: нужен хотя бы один проход (/run)\n")
        elif path == "/log":
            text = self.runner.read_log()
            self._send(200 if text else 404, text or "лога ещё нет\n")
        else:
            self._send(404, f"нет такого пути: {path}\n{INDEX_TEXT}")

    def do_POST(self) -> None:                             # noqa: N802 - имя из stdlib
        path = self._path()
        if not self._guard():
            return
        if path != "/run":
            self._send(404, f"POST бывает только на /run (получено {path})\n")
            return
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            self.rfile.read(length)                        # тело не используем, но прочитать надо
        if self.runner.lock.locked():
            self._send(409, json.dumps({"ok": False, "error": "проход уже идёт"},
                                       ensure_ascii=False), "application/json; charset=utf-8")
            return
        result = self.runner.run_pass()
        self._send(200 if result.get("ok") else 500,
                   json.dumps(result, ensure_ascii=False, indent=2),
                   "application/json; charset=utf-8")


INDEX_TEXT = """Telegram-радар в Cloudflare Containers (схема B: проход по расписанию).

Контейнер поднимается по cron Worker'а, делает один проход по чатам и засыпает.
Состояние (.session и hits.sqlite3) живёт в R2: диск контейнера эфемерен.

Проверка и управление (нужен ?token=<RADAR_TOKEN>):
  POST /run         — проход вне очереди
  GET  /status      — итог последнего прохода
  GET  /usage       — расход и вердикт «A подходит / рекомендую B»
  GET  /metrics.csv — срезы расхода (открывается в Excel)
  GET  /log         — лог последнего прохода
Без пароля отвечает только /healthz.
"""


def token_warning(token: str) -> str:
    """Предупреждение про токен: HTTP-заголовки живут в latin-1, кириллица их сломает.

    Сам контейнер токен с кириллицей сравнит (мы сравниваем байты), а вот Worker передаёт
    его заголовком `x-radar-token` — и там не-ASCII не пройдёт. Поэтому предупреждаем заранее.
    """
    if not token:
        return ""
    try:
        str(token).encode("latin-1")
    except UnicodeEncodeError:
        return ("RADAR_TOKEN содержит не-ASCII символы: Worker передаёт токен HTTP-заголовком, "
                "а заголовки живут в latin-1 — запросы до контейнера не дойдут. Поставь токен "
                "из ASCII (буквы, цифры, дефис, подчёркивание).")
    return ""


def build_runner(env: dict | None = None) -> RadarRunner:
    """Сборка раннера из переменных окружения (их передаёт Worker при старте контейнера)."""
    env = os.environ if env is None else env
    client = None
    try:
        client = R2Client(bucket=env.get("R2_BUCKET", ""),
                          access_key_id=env.get("R2_ACCESS_KEY_ID", ""),
                          secret_access_key=env.get("R2_SECRET_ACCESS_KEY", ""),
                          account_id=env.get("R2_ACCOUNT_ID", ""),
                          endpoint=env.get("R2_ENDPOINT", ""))
    except ValueError:
        client = None
    sessions = tuple(part.strip() for part in
                     (env.get("RADAR_SESSIONS") or env.get("TG_SESSION") or "monitor_session"
                      ).split(",") if part.strip())
    return RadarRunner(
        workdir=env.get("RADAR_WORKDIR", "."), client=client,
        args=env.get("RADAR_ARGS", DEFAULT_ARGS), python=env.get("RADAR_PYTHON") or None,
        timeout=float(env.get("RADAR_TIMEOUT") or DEFAULT_TIMEOUT), sessions=sessions,
        db_name=env.get("RADAR_DB", "hits.sqlite3"),
        metrics_name=env.get("RADAR_METRICS_CSV", "metrics.csv"),
    )


def serve(runner: RadarRunner, token: str = "", port: int = DEFAULT_PORT,
          host: str = "0.0.0.0", bootstrap: bool = True,
          background: bool = True) -> ThreadingHTTPServer:
    """Поднимает HTTP-сервер.

    background=True — сервер крутится в потоке (так удобнее тестам);
    background=False — блокирующий serve_forever в текущем потоке (так работает контейнер).
    """
    Handler.runner = runner
    Handler.token = token or ""
    httpd = ThreadingHTTPServer((host, port), Handler)

    def stop(signum, _frame) -> None:
        runner._log(f"[i] сигнал {signum}: останавливаю контейнер")   # noqa: SLF001
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    for signal_name in ("SIGTERM", "SIGINT"):
        signal_number = getattr(signal, signal_name, None)
        if signal_number is not None:
            try:
                signal.signal(signal_number, stop)
            except (ValueError, OSError):
                pass                                    # не в главном потоке — пропускаем

    if bootstrap and runner.client is not None:
        # Состояние тянем в фоне: сервер должен ответить /healthz как можно раньше,
        # иначе Worker не дождётся готовности контейнера.
        threading.Thread(target=lambda: runner.restore(), daemon=True,
                         name="r2-bootstrap").start()
    if background:
        threading.Thread(target=httpd.serve_forever, daemon=True, name="http").start()
        return httpd
    try:
        httpd.serve_forever(poll_interval=0.5)
    finally:
        httpd.server_close()
    return httpd


def main() -> int:
    runner = build_runner()
    port = int(os.getenv("RADAR_PORT") or DEFAULT_PORT)
    token = os.getenv("RADAR_TOKEN", "")
    print(f"[i] радар-контейнер: порт {port}, аргументы прохода «{runner.args}», "
          f"R2 {'настроен' if runner.client else 'НЕ настроен'}", file=sys.stderr)
    if runner.client is None:
        print("[!] R2 не настроен: после сна контейнера .session и база пропадут. "
              "Задай R2_BUCKET, R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY.",
              file=sys.stderr)
    if not token:
        print("[!] RADAR_TOKEN пустой: эндпоинты контейнера открыты для Worker'а без пароля "
              "(наружу они не торчат, но лучше задать токен)", file=sys.stderr)
    warning = token_warning(token)
    if warning:
        print(f"[!] {warning}", file=sys.stderr)
    try:
        serve(runner, token=token, port=port, background=False)
    except KeyboardInterrupt:
        print("[i] остановлено", file=sys.stderr)
    finally:
        if runner.client is not None:
            checkpoint_db(runner.workdir / runner.db_name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
