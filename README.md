# Market Dashboard

Live at **https://dashboard.flyflyfly.win**

Static market dashboard with 273 tickers, sector heatmaps, FedWatch, AI briefing, and pizza index. Auto-refreshes daily via local Windows Task Scheduler, hosted on Cloudflare Pages.

## Architecture

```
Task Scheduler (Windows, local machine)
  │
  ├─ 6:00 AM Tue-Sat ─→ refresh_data.bat
  │                        ├─ python scripts (build_data, fedwatch, news, briefing)
  │                        ├─ assemble _deploy/ directory (index.html + CNAME + data/)
  │                        ├─ wrangler pages deploy _deploy → Cloudflare Pages (DIRECT UPLOAD)
  │                        ├─ cleanup _deploy/
  │                        └─ git add + commit + push → GitHub (backup only, does NOT trigger deploy)
  │
  └─ 7:05 AM daily ───→ refresh_pizza.bat
                           ├─ python scripts (polymarket, pizza index)
                           ├─ assemble _deploy/ → wrangler deploy → Cloudflare Pages
                           └─ git push → GitHub (backup only)

Deploy target: dashboard.flyflyfly.win (via Cloudflare Pages)
GitHub repo: Steam-ed-Fish/marketboard (code backup — auto-deploy DISABLED)
```

### How deployment works

1. **Task Scheduler** wakes the machine and runs the `.bat` file
2. The bat file runs Python scripts that fetch market data and write to `data/`
3. A temporary `_deploy/` folder is assembled with only the deployable files: `index.html`, `CNAME`, and `data/`
4. **`npx wrangler pages deploy`** uploads `_deploy/` directly to Cloudflare Pages (no build step on Cloudflare's side — it's a static upload)
5. `_deploy/` is deleted after upload
6. The data changes are also committed and pushed to GitHub as a code backup, but GitHub **does not** trigger a Cloudflare deploy (automatic deployments are disabled in Cloudflare settings)

### Why direct upload instead of GitHub-triggered deploy?

- **Faster**: wrangler uploads directly (~6 seconds) vs git push → GitHub webhook → Cloudflare build
- **Cleaner git history**: no more noisy "chore: refresh data" commits triggering builds
- **More reliable**: one fewer link in the chain (GitHub webhook can delay or fail)

## Scheduled Tasks

| Task | Schedule | Script | What it does |
|------|----------|--------|--------------|
| RefreshData | Tue-Sat 6:00 AM HKT | `refresh_data.bat` | build_data, fedwatch, news, briefing |
| RefreshPizza | Tue-Sat 7:05 AM HKT | `refresh_pizza.bat` | polymarket, pizza index |

Both tasks are configured with "Wake computer to run" and "Run as soon as possible after missed start."

## API Keys

Stored in `.env` (gitignored). Required keys:

| Key | Used by |
|-----|---------|
| `FRED_API_KEY` | build_data.py (economic data) |
| `FINNHUB_API_KEY` | build_data.py (market data) |
| `PERPLEXITY_API_KEY` | generate_briefing.py (AI briefing) + news context |
| `TAVILY_API_KEY` | build_news.py (news search) |
| `ANTHROPIC_API_KEY` | Not currently used (geo-blocked from China) |
| `CLOUDFLARE_API_TOKEN` | wrangler deploy (Cloudflare Pages direct upload) |
| `CLOUDFLARE_ACCOUNT_ID` | wrangler deploy (Cloudflare account identifier) |

## Troubleshooting

### 1. Dashboard not updating

**Check the logs first:**
```
logs/refresh_data_YYYYMMDD_HHMMSS.log
logs/refresh_pizza_YYYYMMDD_HHMMSS.log
```

If no log file exists for today, the batch script didn't run. See #2.

If a log exists, open it and look for `[ERROR]` lines. Common errors:

| Log message | Cause | Fix |
|-------------|-------|-----|
| `[ERROR] git pull failed` | Uncommitted local changes or merge conflict | `cd` to repo, run `git stash && git pull --rebase origin main && git stash pop` |
| `[ERROR] git push failed` | Remote has newer commits | Run `git pull --rebase origin main` then `git push` |
| `[ERROR] build_data.py failed` | API key missing/invalid, network issue | Check `.env`, try running the script manually |
| `Briefing generation failed` | Perplexity API down or key invalid | Check key at https://perplexity.ai, briefing is non-critical |
| `[SKIP] No data changes to commit` | Data didn't change (weekend/holiday) | Normal, not an error |
| `Detected N stale tickers` | yfinance returned cached data for some tickers | Auto-retried; check if `Stale recovery` line shows all recovered |
| `WARNING: N tickers still stale after retry` | Re-fetch didn't fix stale data | Likely heavy rate-limiting; try running again later or reduce batch size |

### 2. No log file at all (script didn't run)

**Task Scheduler didn't fire.** Check:

1. Open Task Scheduler > Task Scheduler Library
2. Find `RefreshData` or `RefreshPizza`
3. Check "Last Run Time" and "Last Run Result":
   - `0x0` = success
   - `0x1` = script errored (check log)
   - `0x41301` = task is currently running
   - `0x80070005` = access denied (run as admin, see #5)
4. Check the trigger is correct (Tue-Sat, correct time, enabled)
5. Check "Settings" tab: "Wake the computer" and "Run as soon as possible after missed start" should be checked

**Quick manual test:** Right-click the task > Run. Check `logs/` for a new file.

### 3. Batch script flashes and disappears (no log created)

Run it from a command prompt to see the error:
```cmd
cmd /c "C:\Users\zhenyuyong\Documents\marketboard-lwc\refresh_data.bat"
```

Common causes:
- Python not in PATH: install Python and ensure "Add to PATH" was checked
- PowerShell blocked: the script uses `powershell -command "Get-Date"` for timestamps

### 4. Dashboard not updating even though scripts ran successfully

The deploy now goes **directly** via wrangler, not through GitHub.

1. Check the log for `[OK] Deployed to Cloudflare Pages` or `[ERROR] Cloudflare Pages deploy failed`
2. If deploy failed: verify `.env` has `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID`
3. Verify wrangler is installed: `npx wrangler --version`
4. Manual deploy test:
   ```cmd
   cd C:\Users\zhenyuyong\Documents\marketboard-lwc
   mkdir _deploy && xcopy index.html _deploy\ /Y && xcopy CNAME _deploy\ /Y && xcopy data _deploy\data\ /E /Y /I
   npx wrangler pages deploy _deploy --project-name marketboard --branch main
   rmdir /s /q _deploy
   ```
5. Check Cloudflare dashboard > Workers & Pages > marketboard > Deployments for recent deploys

### 5. Task Scheduler permission issues

If tasks fail with access denied:
1. Open Task Scheduler as Administrator
2. Right-click the task > Properties
3. Check "Run with highest privileges"
4. Under "Security options", select your user account and click "Change User or Group" if needed

### 6. Specific script failures

**build_data.py** — fetches 273 tickers from Yahoo Finance. A few tickers failing is normal (some delist or have temporary API issues). Check log for the count: `Processed 273 tickers, X failed`. The script also detects **stale data** — when yfinance silently returns cached prices from a previous trading day (common when rate-limited). It compares each ticker's data date against SPY's reference date and automatically re-fetches mismatched tickers. Look for `Detected N stale tickers` in the log.

**build_fedwatch.py** — fetches CME FedWatch + BLS data. BLS (unemployment/CPI) timeouts are common and non-critical. FedWatch still works via CME.

**build_news.py** — uses Tavily API for news search. Free tier allows ~1000 searches/month (~10 per build).

**generate_briefing.py** — uses Perplexity API (sonar-pro) to write the AI briefing. If this fails, everything else still works; you just get no briefing card.

**build_polymarket.py / build_pizza_index.py** — fetches prediction market data. No API key needed.

## Manual Build

If you need to refresh data manually, **run all four scripts** — not just `build_data.py`. Each script populates a different part of the dashboard:

| Script | What it populates |
|--------|-------------------|
| `build_data.py` | Ticker prices, charts, sector heatmaps, options, breadth, RS, Fear & Greed |
| `build_fedwatch.py` | Fed Watch section (FOMC probabilities, unemployment, CPI, speeches) |
| `build_news.py` | Market News section (headlines, mover/sector news) |
| `generate_briefing.py` | Quick Recap briefing (BOTTOM LINE, INDICES, VOLUME, SECTORS, etc.) |

Running only `build_data.py` will update prices but leave the briefing, news, and macro sections empty or stale.

```cmd
cd C:\Users\zhenyuyong\Documents\marketboard-lwc

:: Load API keys
for /f "usebackq tokens=1,* delims==" %A in (".env") do set "%A=%B"

:: Run ALL four scripts (order matters — briefing uses data from the others)
python scripts/build_data.py --out-dir data
python scripts/build_fedwatch.py
python scripts/build_news.py --out-dir data
python scripts/generate_briefing.py --out-dir data

:: Deploy to Cloudflare Pages
mkdir _deploy
xcopy index.html _deploy\ /Y
xcopy CNAME _deploy\ /Y
xcopy data _deploy\data\ /E /Y /I
npx wrangler pages deploy _deploy --project-name marketboard --branch main
rmdir /s /q _deploy

:: Optional: push to GitHub as backup
git add data/
git commit -m "chore: manual data refresh"
git push origin main
```

## Project Structure

```
marketboard-lwc/
├── index.html                  # Dashboard frontend (single-page)
├── refresh_data.bat            # Daily market data build script
├── refresh_pizza.bat           # Daily pizza/polymarket build script
├── .env                        # API keys (gitignored)
├── scripts/
│   ├── build_data.py           # 273 tickers, sector data, charts
│   ├── build_fedwatch.py       # CME FedWatch + BLS economic data
│   ├── build_news.py           # Tavily news search
│   ├── generate_briefing.py    # AI briefing via Perplexity
│   ├── build_polymarket.py     # Prediction market data
│   └── build_pizza_index.py    # Pizza index calculation
├── data/                       # Generated output (committed)
│   ├── snapshot.json           # All ticker data
│   ├── events.json             # Briefing + calendar events
│   ├── meta.json               # Sector/ticker metadata
│   ├── fedwatch.json           # Fed data
│   ├── news.json               # News headlines
│   ├── pizza_index.json        # Pizza index
│   ├── polymarket.json         # Prediction markets
│   ├── charts/                 # Ticker chart PNGs
│   └── holdings/               # ETF holdings JSON
├── logs/                       # Build logs (gitignored)
└── requirements.txt
```
