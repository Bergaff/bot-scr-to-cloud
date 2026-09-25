#!/usr/bin/env bash
# Бот-панель на ОБЛАЧНОЙ базе (схема B: радар работает в Cloudflare).
# Скачивает hits.sqlite3 из R2 и запускает панель на этом снимке.
# Ключи аккаунта Telegram не нужны (панель их не открывает), .env — нужен.
#
#   ./cloud_panel.sh                 # скачать базу и запустить панель
#   ./cloud_panel.sh --pull-only     # только скачать базу (cloud_hits.sqlite3)
#
# Имя бакета — из переменной RADAR_R2_BUCKET, по умолчанию radar-state
# (должно совпадать с R2_BUCKET в wrangler.jsonc).
set -euo pipefail
cd "$(dirname "$0")"

BUCKET="${RADAR_R2_BUCKET:-radar-state}"
KEY="db/hits.sqlite3"
LOCAL="cloud_hits.sqlite3"

echo "[i] скачиваю базу радара из R2: ${BUCKET}/${KEY}" >&2
if ! npx wrangler r2 object get "${BUCKET}/${KEY}" --file "${LOCAL}" --remote; then
  echo "[!] не удалось скачать базу. Проверь по порядку:" >&2
  echo "      npx wrangler login" >&2
  echo "      npx wrangler r2 bucket list" >&2
  echo "      имя бакета = R2_BUCKET в wrangler.jsonc (по умолчанию radar-state)" >&2
  echo "      радар уже отработал хотя бы один проход (POST /run)" >&2
  exit 1
fi

if [ "${1:-}" = "--pull-only" ]; then
  echo "[+] база сохранена: ${LOCAL}" >&2
  exit 0
fi

echo "[i] панель работает на снимке базы: цифры — на момент скачивания." >&2
echo "    за свежими данными запусти ./cloud_panel.sh ещё раз" >&2
exec ./start.sh --panel-only --db "${LOCAL}"
