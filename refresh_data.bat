@echo off
setlocal enabledelayedexpansion

set "REPO=C:\Users\zhenyuyong\Documents\marketboard-lwc"
set "LOGDIR=%REPO%\logs"
:: All market data now fetched via futu-cli (network-only internal SDK), not the
:: OpenD daemon. futu-cli requires protobuf 4.x, which conflicts with futu_api's
:: protobuf 3.x — so the whole build runs in a dedicated Python 3.12 venv where
:: protobuf 4.25.9 uses its native C-ext (no pure-Python fallback needed, unlike
:: the old 3.14 setup). Venv has futu-cli + repo deps from requirements.txt.
set "PYTHON=%REPO%\.venv312\Scripts\python.exe"

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

:: Pull latest (non-fatal — build and deploy don't depend on GitHub)
echo.
echo --- git pull ---
git pull --rebase origin main
if errorlevel 1 (
    echo [WARN] git pull failed (continuing anyway)
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
echo --- rescue_finnhub.py (auto-patch stale OHLC via Finnhub) ---
%PYTHON% scripts/rescue_finnhub.py
if errorlevel 1 (
    echo [WARN] rescue_finnhub.py failed (non-fatal)
)

echo.
echo --- fetch_spx500_constituents.py ---
%PYTHON% scripts/fetch_spx500_constituents.py --out-dir data
if errorlevel 1 (
    echo [WARN] fetch_spx500_constituents.py failed (non-fatal — cached list used)
)

echo.
echo --- build_spx500_breadth.py ---
%PYTHON% scripts/build_spx500_breadth.py --out-dir data
if errorlevel 1 (
    echo [ERROR] build_spx500_breadth.py failed
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
echo --- build_polymarket.py ---
%PYTHON% scripts/build_polymarket.py --out-dir data
if errorlevel 1 (
    echo [WARN] build_polymarket.py failed (non-critical)
)

echo.
echo --- build_futures.py ---
%PYTHON% scripts/build_futures.py --out-dir data
if errorlevel 1 (
    echo [WARN] build_futures.py failed (non-critical)
)

echo.
echo --- generate_briefing.py ---
%PYTHON% scripts/generate_briefing.py --out-dir data
if errorlevel 1 (
    echo [ERROR] generate_briefing.py failed
    set /a ERRORS+=1
)

echo.
echo --- push_briefing_lark.py ---
%PYTHON% scripts/push_briefing_lark.py --out-dir data
if errorlevel 1 (
    echo [WARN] push_briefing_lark.py failed (non-critical)
)

echo.
echo --- deploy to Cloudflare Pages ---
:: Assemble deploy directory
if exist "%REPO%\_deploy" rmdir /s /q "%REPO%\_deploy"
mkdir "%REPO%\_deploy"
xcopy "%REPO%\index.html" "%REPO%\_deploy\" /Y >nul
xcopy "%REPO%\CNAME" "%REPO%\_deploy\" /Y >nul
:: Exclude parallel_test/ and backups/ from the deploy (internal scratch, not served)
echo parallel_test\ > "%REPO%\_deploy_exclude.txt"
echo backups\ >> "%REPO%\_deploy_exclude.txt"
xcopy "%REPO%\data" "%REPO%\_deploy\data\" /E /Y /I /EXCLUDE:"%REPO%\_deploy_exclude.txt" >nul
del "%REPO%\_deploy_exclude.txt"

:: Deploy via wrangler (use `call` so cmd.exe returns to this bat after npx.cmd exits)
call npx wrangler pages deploy "%REPO%\_deploy" --project-name marketboard --branch main
if errorlevel 1 (
    echo [ERROR] Cloudflare Pages deploy failed
    set /a ERRORS+=1
) else (
    echo [OK] Deployed to Cloudflare Pages
)

:: Cleanup deploy directory
rmdir /s /q "%REPO%\_deploy"

echo.
echo --- git backup (code only) ---
git add data/
git diff --staged --quiet
if errorlevel 1 (
    git commit -m "chore: refresh dashboard data [automated]"
    git push origin main
    if errorlevel 1 (
        echo [WARN] git push failed (non-critical, deploy already done)
    ) else (
        echo [OK] Pushed backup to GitHub
    )
) else (
    echo [SKIP] No data changes to commit
)

echo.
echo === refresh_data finished at %DATE% %TIME% ===
echo Errors: !ERRORS!
exit /b !ERRORS!
