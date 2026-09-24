@echo off
setlocal enabledelayedexpansion
rem =====================================================================
rem  Telegram radar launcher (Windows)
rem  Loads keys from .env, installs deps on first run, starts monitor.py
rem
rem  Examples:
rem    start.bat                                    live mode, console + hits.log
rem    start.bat --login-qr                         sign in by QR code (no SMS needed)
rem    start.bat --doctor                           check environment
rem    start.bat --once --catchup 20                one pass, then exit
rem    start.bat --once --notify bot --no-pause     for Task Scheduler
rem =====================================================================
chcp 65001 >nul 2>&1
cd /d "%~dp0"

set "PY=python"
if exist ".venv\Scripts\python.exe" set "PY=.venv\Scripts\python.exe"

rem --- keys from .env (skip comments and empty lines) -----------------
if exist ".env" (
  for /f "usebackq tokens=1,* delims==" %%a in (".env") do (
    set "key=%%a"
    set "val=%%b"
    if not "!key!"=="" if not "!key:~0,1!"=="#" set "!key!=!val!"
  )
)

rem --- dependencies: install once, then quick check ------------------
%PY% -c "import telethon, yaml, qrcode" >nul 2>&1
if errorlevel 1 (
  echo [i] Installing dependencies, one time, about a minute...
  %PY% -m pip install --quiet -r requirements.txt
  %PY% -c "import telethon, yaml, qrcode" >nul 2>&1
  if errorlevel 1 (
    echo [!] pip install failed. Run manually:
    echo     .venv\Scripts\activate
    echo     python -m pip install -r requirements.txt
    pause
    exit /b 1
  )
)

rem --- keys required for this run? -----------------------------------
set "NEEDS_KEYS=1"
if "%~1"=="--doctor" set "NEEDS_KEYS=0"
if "%~1"=="--test-notify" set "NEEDS_KEYS=0"
if "%~1"=="--export" set "NEEDS_KEYS=0"
if "%~1"=="--show-stats" set "NEEDS_KEYS=0"
if "%~1"=="--stats-only" set "NEEDS_KEYS=0"
if "%~1"=="--help" set "NEEDS_KEYS=0"

if "%NEEDS_KEYS%"=="1" (
  if "%TG_API_ID%"=="" (
    echo [!] TG_API_ID is empty: copy .env.example to .env and put your keys there.
    echo     Then check with:  start.bat --doctor
    pause
    exit /b 1
  )
  if "%TG_API_HASH%"=="" (
    echo [!] TG_API_HASH is empty: see .env / START-HERE.md step 4
    pause
    exit /b 1
  )
)

rem --- QR login (no SMS/code needed) -----------------------------------
if "%~1"=="--login-qr" (
  %PY% login_qr.py %*
  pause
  exit /b %errorlevel%
)

rem --- run ------------------------------------------------------------
if "%~1"=="" (
  %PY% monitor.py --notify both --catchup 30
) else (
  %PY% monitor.py %*
)

rem --- pause unless --no-pause (Task Scheduler must skip it) ---------
set "NOPAUSE="
for %%a in (%*) do if "%%a"=="--no-pause" set "NOPAUSE=1"
if not "%NOPAUSE%"=="1" pause
