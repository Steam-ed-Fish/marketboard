# Market Dashboard

Live at **https://dashboard.flyflyfly.win**

Static market dashboard with 273 tickers, sector heatmaps, FedWatch, AI briefing, and pizza index. Auto-refreshes daily via local Windows Task Scheduler, hosted on Cloudflare Pages.

## Architecture

```
Task Scheduler (4:30 AM / 7:05 AM HKT)
  -> refresh_data.bat / refresh_pizza.bat
    -> python scripts (fetch data, generate briefing)
    -> git commit + push
      -> Cloudflare Pages auto-deploys on push
        -> dashboard.flyflyfly.win updated
```

## Scheduled Tasks

| Task | Schedule | Script | What it does |
|------|----------|--------|--------------|
| RefreshData | Tue-Sat 4:30 AM HKT | `refresh_data.bat` | build_data, fedwatch, news, briefing |
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

### 4. Dashboard updated on GitHub but not on the website

Cloudflare Pages should auto-deploy within 1-2 minutes of a push.

1. Go to Cloudflare dashboard > Workers & Pages > `marketboard-olm`
2. Check "Deployments" tab for recent deploys
3. If no deploy after a push: check that the GitHub connection is still active (Settings > Builds & deployments)
4. Manual redeploy: click "Retry deployment" on the latest entry

### 5. Task Scheduler permission issues

If tasks fail with access denied:
1. Open Task Scheduler as Administrator
2. Right-click the task > Properties
3. Check "Run with highest privileges"
4. Under "Security options", select your user account and click "Change User or Group" if needed

### 6. Specific script failures

**build_data.py** — fetches 273 tickers from Yahoo Finance. A few tickers failing is normal (some delist or have temporary API issues). Check log for the count: `Processed 273 tickers, X failed`.

**build_fedwatch.py** — fetches CME FedWatch + BLS data. BLS (unemployment/CPI) timeouts are common and non-critical. FedWatch still works via CME.

**build_news.py** — uses Tavily API for news search. Free tier allows ~1000 searches/month (~10 per build).

**generate_briefing.py** — uses Perplexity API (sonar-pro) to write the AI briefing. If this fails, everything else still works; you just get no briefing card.

**build_polymarket.py / build_pizza_index.py** — fetches prediction market data. No API key needed.

## Manual Build

If you need to refresh data manually:

```cmd
cd C:\Users\zhenyuyong\Documents\marketboard-lwc

:: Load API keys
for /f "usebackq tokens=1,* delims==" %A in (".env") do set "%A=%B"

:: Run whichever scripts you need
python scripts/build_data.py --out-dir data
python scripts/build_fedwatch.py
python scripts/build_news.py --out-dir data
python scripts/generate_briefing.py --out-dir data

:: Commit and push
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
