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

:: futu-cli talks to an internal corp endpoint (internal-service-api.futuoa.com,
:: private IP 10.254.3.175). If a system/VPN proxy is on (e.g. Hysteria2 on
:: 127.0.0.1:10808), httpx routes the internal request through the VPN tunnel,
:: which can't reach the private IP -> WinError 10054 and the whole fetch fails
:: (0/240 tickers, current_price=None everywhere). Bypass the proxy for the
:: internal host so futu-cli works whether or not the VPN is up.
set "NO_PROXY=internal-service-api.futuoa.com,10.254.3.175,.futuoa.com,127.0.0.1,localhost"
set "no_proxy=%NO_PROXY%"

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
echo --- check_futu_token.py (gateway token preflight) ---
:: One tiny gateway call up front. If the token is rejected (HTTP 401 — the futu
:: team rotates/invalidates tokens, e.g. 2026-08-07), it posts a loud Feishu alert
:: so the token gets refreshed same-day instead of the whole build silently
:: fetching stale OHLC/futures. WARN-level: the build continues (OHLC degrades to
:: cache) and the alert is what matters — a dead token shouldn't freeze the deploy.
%PYTHON% scripts/check_futu_token.py
if errorlevel 1 (
    echo [WARN] futu gateway token check failed — alert sent; OHLC/futures may be stale this run
)

echo.
echo --- ensure_opend.py (OpenD daemon preflight) ---
:: RefreshData fires at 06:00, but "moomoo OpenD" is a LOGON-triggered task — on
:: mornings nobody logs in before 6 AM the daemon is down and the bridge below
:: degrades (options/AUM/expected-move/S&P weights fall back to yfinance, which
:: then rate-limits; heatmap weight column goes null). Launch it if it isn't up.
:: Bounded wait; WARN-level (exit 1 just lets the bridge fall back — never hangs).
%PYTHON% scripts/ensure_opend.py --timeout 120
if errorlevel 1 (
    echo [WARN] OpenD daemon not up — opend_bridge will fall back this run
)

echo.
echo --- opend_bridge.py (OpenD: options greeks + ETF AUM) ---
:: OpenD runs under system Python 3.14 (futu + protobuf 3.20.3). futu-cli's
:: protobuf 4.x lives in the .venv312 — the two SDKs cannot coexist in one
:: process, so the OpenD fetch runs as a separate 3.14 step that writes JSON
:: (data/opend_options.json + data/opend_aum.json) for build_data to consume.
:: protobuf 3.20.3's C-ext is broken on 3.14, so use the pure-Python fallback
:: for THIS step only, then clear it before the .venv312 steps run.
set "PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python"
py -3.14 scripts/opend_bridge.py --out-dir data
if errorlevel 1 (
    echo [WARN] opend_bridge.py failed (non-fatal — options/etf_flow fall back)
)
set "PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION="

echo.
echo --- build_data.py ---
%PYTHON% scripts/build_data.py --out-dir data
if errorlevel 1 (
    echo [ERROR] build_data.py failed
    set /a ERRORS+=1
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
echo --- build_heatmap.py (quant heatmap table) ---
:: Runs after snapshot + breadth + ohlc + etf_flow are all present; reuses
:: build_spx500_breadth.build_universe for sector/index member breadth.
%PYTHON% scripts/build_heatmap.py --out-dir data
if errorlevel 1 (
    echo [WARN] build_heatmap.py failed (non-fatal — heatmap tab shows no data)
)

echo.
echo --- build_qqq_internals.py (QQQ Structure pane in heatmap modal) ---
:: One Invesco dng-api call for the full ~100-name QQQ holdings (yfinance caps at
:: 10). Degrades to the top-10 QQQ.json cache if dng-api is down. Non-fatal.
%PYTHON% scripts/build_qqq_internals.py --out-dir data
if errorlevel 1 (
    echo [WARN] build_qqq_internals.py failed (non-fatal — QQQ Structure pane shows no data)
)

:: rescue_finnhub runs LAST among the fetch steps — after build_data AND the SPX
:: breadth futu-cli prefetch — so it only patches OHLC that ALL futu-cli fetches
:: genuinely missed. It used to run right after build_data, before the ~500 SPX
:: constituents were fetched, so it crawled ~369 "stale" constituents one-by-one
:: via Finnhub (1.1s/call = ~8-10 min) right before build_spx500_breadth re-fetched
:: the same names fresh via futu-cli batch. Now they're already fresh when it runs,
:: so it has almost nothing to crawl. (Trade-off: on a full futu-cli outage the
:: breadth build would be stale, since rescue's patches land after breadth computes
:: — acceptable now that the VPN-proxy outage cause is fixed via NO_PROXY.)
echo.
echo --- rescue_finnhub.py (auto-patch stale OHLC via Finnhub) ---
%PYTHON% scripts/rescue_finnhub.py
if errorlevel 1 (
    echo [WARN] rescue_finnhub.py failed (non-fatal)
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

:: On Saturdays, ALSO push a weekly review after the daily briefing. The first
:: generate_briefing call above produced the daily EOD recap (no --weekly flag =
:: daily mode). Now force a second pass in weekly mode so the group gets BOTH
:: the daily and the week-in-review. DOW==6 is Saturday in .NET DayOfWeek.
:: A 90s gap lets the futu gateway's rate-limit window recover before the second
:: generate_briefing hits it (the daily just spent its calls there). Without this,
:: a 429'd daily would cascade into a 429'd weekly.
:: Guard with `goto` instead of a big `if (...)` block: unescaped parens in the
:: echo strings below (e.g. "(Saturday weekly recap)") broke cmd's parse of the
:: parenthesized block on EVERY run — even weekdays when it's skipped — with
:: "--- was unexpected at this time", which killed the deploy step that follows.
:: Deploy was silently dead ~2026-08-29 -> 2026-09-15 because of this. Parens in
:: echoes are now [brackets]; the goto avoids the fragile outer block entirely.
for /f "usebackq" %%D in (`powershell -command "[int](Get-Date).DayOfWeek"`) do set "DOW=%%D"
if not "!DOW!"=="6" goto :skip_weekly
echo.
echo --- 90s cooldown before weekly recap [futu gateway rate-limit recovery] ---
powershell -command "Start-Sleep -Seconds 90"
echo --- generate_briefing.py --weekly [Saturday weekly recap] ---
%PYTHON% scripts/generate_briefing.py --out-dir data --weekly
if errorlevel 1 (
    echo [ERROR] weekly generate_briefing.py failed
    set /a ERRORS+=1
) else (
    echo --- push_briefing_lark.py weekly recap ---
    %PYTHON% scripts/push_briefing_lark.py --out-dir data
    if errorlevel 1 echo [WARN] weekly push_briefing_lark.py failed [non-critical]
)
:skip_weekly

echo.
echo --- deploy to Cloudflare Pages ---
:: Assemble deploy directory
if exist "%REPO%\_deploy" rmdir /s /q "%REPO%\_deploy"
mkdir "%REPO%\_deploy"
copy /Y "%REPO%\index.html" "%REPO%\_deploy\" >nul
copy /Y "%REPO%\CNAME" "%REPO%\_deploy\" >nul
:: Copy the data tree with robocopy, excluding internal scratch dirs inline via /XD.
:: (The old xcopy /EXCLUDE:tempfile approach silently skipped ALL data whenever the
:: temp exclude file was unreadable — "Can't read file _deploy_exclude.txt" — which
:: shipped an empty deploy and froze the live site on a stale build. robocopy /XD
:: needs no temp file. robocopy exit codes <8 = success, 8+ = real failure.)
:: /XF excludes internal build-inputs from the public deploy: the OpenD bridge
:: outputs (opend_options/aum/em) are consumed by build_data into snapshot.json and
:: don't need serving; opend_unknown is a cache; reddit_keyword_research isn't on the
:: dashboard. Frontend never links them — this just avoids publicly serving ~155KB of
:: internal option data.
robocopy "%REPO%\data" "%REPO%\_deploy\data" /E /XD "%REPO%\data\parallel_test" "%REPO%\data\backups" /XF opend_options.json opend_aum.json opend_em.json opend_spx_weights.json opend_unknown.json reddit_keyword_research.json .push_marker_*.txt /NFL /NDL /NJH /NJS /NP >nul
if errorlevel 8 (
    echo [ERROR] robocopy data -^> _deploy failed
    set /a ERRORS+=1
)
:: Reset errorlevel (robocopy returns 1 on success-with-copies, which would false-trip
:: later `if errorlevel 1` checks).
cmd /c "exit /b 0"

:: Guard: never deploy an empty/partial data dir. If snapshot.json didn't make it in,
:: abort the deploy so Cloudflare keeps serving the last good build instead of blank data.
if not exist "%REPO%\_deploy\data\snapshot.json" (
    echo [ERROR] _deploy\data\snapshot.json missing — aborting deploy ^(would ship empty data^)
    set /a ERRORS+=1
    goto :after_deploy
)

:: Deploy via wrangler (use `call` so cmd.exe returns to this bat after npx.cmd exits)
call npx wrangler pages deploy "%REPO%\_deploy" --project-name marketboard --branch main
if errorlevel 1 (
    echo [ERROR] Cloudflare Pages deploy failed
    set /a ERRORS+=1
) else (
    echo [OK] Deployed to Cloudflare Pages
)

:after_deploy
:: Cleanup deploy directory
if exist "%REPO%\_deploy" rmdir /s /q "%REPO%\_deploy"

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
