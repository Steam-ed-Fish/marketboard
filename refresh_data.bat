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
echo --- deploy to Cloudflare Pages ---
:: Assemble deploy directory
if exist "%REPO%\_deploy" rmdir /s /q "%REPO%\_deploy"
mkdir "%REPO%\_deploy"
xcopy "%REPO%\index.html" "%REPO%\_deploy\" /Y >nul
xcopy "%REPO%\CNAME" "%REPO%\_deploy\" /Y >nul
xcopy "%REPO%\data" "%REPO%\_deploy\data\" /E /Y /I >nul

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
