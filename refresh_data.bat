@echo off
setlocal enabledelayedexpansion

set "REPO=C:\Users\zhenyuyong\Documents\marketboard-lwc"
set "LOGDIR=%REPO%\logs"
set "PYTHON=python"

:: Get timestamp
for /f %%I in ('powershell -command "Get-Date -Format yyyyMMdd_HHmmss"') do set "DT=%%I"
set "LOGFILE=%LOGDIR%\refresh_data_%DT%.log"

if not exist "%LOGDIR%" mkdir "%LOGDIR%"

call :main >> "%LOGFILE%" 2>&1
exit /b %ERRORLEVEL%

:main
echo === refresh_data started at %DATE% %TIME% ===

cd /d "%REPO%"

:: Load .env
if exist ".env" (
    for /f "usebackq tokens=1,* delims==" %%A in (".env") do (
        if not "%%A"=="" if not "%%B"=="" (
            for /f "tokens=* delims= " %%C in ("%%B") do set "%%A=%%C"
        )
    )
    echo [OK] Loaded .env
) else (
    echo [WARN] No .env file found
)

:: Pull latest
echo.
echo --- git pull ---
git pull --rebase origin main
if errorlevel 1 (
    echo [ERROR] git pull failed
    exit /b 1
)

set ERRORS=0

echo.
echo --- build_data.py ---
%PYTHON% scripts/build_data.py --out-dir data
if errorlevel 1 (
    echo [ERROR] build_data.py failed
    set /a ERRORS+=1
)

echo.
echo --- build_fedwatch.py ---
%PYTHON% scripts/build_fedwatch.py
if errorlevel 1 (
    echo [ERROR] build_fedwatch.py failed
    set /a ERRORS+=1
)

echo.
echo --- build_news.py ---
%PYTHON% scripts/build_news.py --out-dir data
if errorlevel 1 (
    echo [ERROR] build_news.py failed
    set /a ERRORS+=1
)

echo.
echo --- generate_briefing.py ---
%PYTHON% scripts/generate_briefing.py --out-dir data
if errorlevel 1 (
    echo [ERROR] generate_briefing.py failed
    set /a ERRORS+=1
)

echo.
echo --- git commit + push ---
git add data/
git diff --staged --quiet
if errorlevel 1 (
    git commit -m "chore: refresh dashboard data [automated]"
    git push origin main
    if errorlevel 1 (
        echo [ERROR] git push failed
        set /a ERRORS+=1
    ) else (
        echo [OK] Pushed new data
    )
) else (
    echo [SKIP] No data changes to commit
)

echo.
echo === refresh_data finished at %DATE% %TIME% ===
echo Errors: !ERRORS!
exit /b !ERRORS!
