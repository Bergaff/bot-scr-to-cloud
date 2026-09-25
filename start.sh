#!/usr/bin/env bash
# Запуск мониторинга: активирует .venv, подхватывает ключи из .env (если файл есть).
# Примеры:
#   ./start.sh                              # живьём, уведомления в консоль + hits.log
#   ./start.sh --notify bot                 # живьём, уведомления ботом
#   ./start.sh --once --catchup 30          # разовый проход (для cron)
#   ./start.sh --login                      # мастер: ключи приложения -> .env -> вход по QR -> бот
#   ./start.sh --login-qr                   # вход по QR-коду (без SMS)
#   ./start.sh --bot-panel                  # живой режим + бот-панель (статус в Telegram)
#   ./start.sh --panel-only                 # только панель: ключи аккаунта не нужны
set -euo pipefail
cd "$(dirname "$0")"

if [ -f .env ]; then
  set -a; . ./.env; set +a
fi
if [ -d .venv ]; then
  # shellcheck disable=SC1091
  . .venv/bin/activate
fi

# зависимости: ставим при первом запуске (и если окружение пересоздали)
# QR-вход идёт своим скриптом
if [ "${1:-}" = "--login-qr" ]; then
  exec python login_qr.py "$@"
fi

if ! python -c "import telethon, yaml, qrcode" >/dev/null 2>&1; then
  echo "[i] ставлю зависимости — это один раз, займёт минуту..." >&2
  python -m pip install --quiet -r requirements.txt || {
    echo "[!] не удалось: выполни вручную: python -m pip install -r requirements.txt" >&2; exit 1; }
fi
# мастер авторизации: сам спросит api_id/api_hash и запишет их в .env
if [ "${1:-}" = "--login" ]; then
  exec python login_wizard.py "$@"
fi

# ключи не нужны для справки, экспорта и проверки уведомлений
needs_keys=1
for arg in "$@"; do
  case "$arg" in -h|--help|--export|--test-notify|--doctor|--show-stats|--stats-only|--panel-only|--export=*|--test-notify=*) needs_keys=0 ;; esac
done
if [ "$needs_keys" = 1 ] && { [ -z "${TG_API_ID:-}" ] || [ -z "${TG_API_HASH:-}" ]; }; then
  echo "[!] TG_API_ID / TG_API_HASH не заданы: запусти мастер ./start.sh --login (или заполни .env, см. START-HERE.md, шаг 2)" >&2
  exit 1
fi
if [ "$#" -eq 0 ]; then
  exec python monitor.py --notify both --catchup 30
else
  exec python monitor.py "$@"
fi
