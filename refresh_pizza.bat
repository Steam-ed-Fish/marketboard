@echo off
setlocal enabledelayedexpansion

set "REPO=C:\Users\zhenyuyong\Documents\marketboard-lwc"
set "LOGDIR=%REPO%\logs"
set "PYTHON=python"

:: Get timestamp
for /f %%I in ('powershell -command "Get-Date -Format yyyyMMdd_HHmmss"') do set "DT=%%I"
set "LOGFILE=%LOGDIR%\refresh_pizza_%DT%.log"

if not exist "%LOGDIR%" mkdir "%LOGDIR%"

call :main >> "%LOGFILE%" 2>&1
exit /b %ERRORLEVEL%

:main
echo === refresh_pizza started at %DATE% %TIME% ===

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
echo --- build_polymarket.py ---
%PYTHON% scripts/build_polymarket.py --out-dir data
if errorlevel 1 (
    echo [ERROR] build_polymarket.py failed
    set /a ERRORS+=1
)

echo.
echo --- build_pizza_index.py ---
%PYTHON% scripts/build_pizza_index.py --out-dir data
if errorlevel 1 (
    echo [ERROR] build_pizza_index.py failed
    set /a ERRORS+=1
)

echo.
echo --- git commit + push ---
git add data/pizza_index.json data/polymarket.json data/polymarket_history.json
git diff --staged --quiet
if errorlevel 1 (
    git commit -m "chore: refresh pizza index [automated]"
    git push origin main
    if errorlevel 1 (
        echo [ERROR] git push failed
        set /a ERRORS+=1
    ) else (
        echo [OK] Pushed pizza data
    )
) else (
    echo [SKIP] No data changes to commit
)

echo.
echo === refresh_pizza finished at %DATE% %TIME% ===
echo Errors: !ERRORS!
exit /b !ERRORS!
