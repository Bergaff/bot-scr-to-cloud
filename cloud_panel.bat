@echo off
setlocal enabledelayedexpansion
rem =====================================================================
rem  Бот-панель на ОБЛАЧНОЙ базе (схема B: радар работает в Cloudflare)
rem  Скачивает hits.sqlite3 из R2 и запускает панель на этом снимке.
rem  Ключи аккаунта Telegram не нужны (панель их не открывает), .env — нужен.
rem
rem    cloud_panel.bat                 скачать базу и запустить панель
rem    cloud_panel.bat --pull-only     только скачать базу (cloud_hits.sqlite3)
rem
rem  Имя бакета берётся из переменной RADAR_R2_BUCKET, по умолчанию radar-state
rem  (должно совпадать с R2_BUCKET в wrangler.jsonc).
rem =====================================================================
chcp 65001 >nul 2>&1
cd /d "%~dp0"

set "BUCKET=radar-state"
if not "%RADAR_R2_BUCKET%"=="" set "BUCKET=%RADAR_R2_BUCKET%"
set "KEY=db/hits.sqlite3"
set "LOCAL=cloud_hits.sqlite3"

echo [i] Скачиваю базу радара из R2: %BUCKET%/%KEY%
call npx wrangler r2 object get %BUCKET%/%KEY% --file %LOCAL% --remote
if errorlevel 1 (
  echo.
  echo [!] Не удалось скачать базу. Проверь по порядку:
  echo     npx wrangler login
  echo     npx wrangler r2 bucket list
  echo     имя бакета = R2_BUCKET в wrangler.jsonc ^(по умолчанию radar-state^)
  echo     радар уже отработал хотя бы один проход ^(POST /run^)
  pause
  exit /b 1
)

if "%~1"=="--pull-only" (
  echo [+] База сохранена: %LOCAL%
  pause
  exit /b 0
)

echo [i] Панель работает на снимке базы: цифры — на момент скачивания.
echo     За свежими данными запусти cloud_panel.bat ещё раз.
call start.bat --panel-only --db %LOCAL%
