# Market Dashboard

Static stock dashboard with daily auto-refresh via GitHub Actions (Yahoo Finance), hosted on GitHub Pages.

## Build data locally

```bash
cd market-dashboard
pip install -r requirements.txt
python scripts/build_data.py --out-dir data
```

This generates: `data/snapshot.json`, `data/events.json`, `data/meta.json`, `data/charts/*.png`, and `data/holdings/*.json` (top holdings per fund ticker).

- **Holdings only:** `python scripts/build_data.py --holdings` (writes `data/holdings/` using current `STOCK_GROUPS`).

To preview locally: open `index.html` in a browser, or serve the project root with a static server (e.g. `python -m http.server 8000`) and visit `http://localhost:8000`.

## Deploy to GitHub Pages

1. Create a new GitHub repository and push this directory’s contents to it (or push as the repo root).
2. **Before first deploy** you need initial data. Either:
   - **Recommended:** In the repo go to **Actions** → “Refresh dashboard data” → **Run workflow**. When it finishes, it will commit `data/` to the repo.
   - Or run locally: `python scripts/build_data.py --out-dir data`, then `git add data/`, commit and push.
3. In the repo **Settings → Pages**:
   - Set Source to **GitHub Actions** (or “Deploy from a branch”).
   - If using a branch: choose branch `main` and folder `/ (root)`.
4. The workflow runs daily at 16:30 US Eastern to refresh data; you can also run it manually from **Actions**.

Site URL: `https://<your-username>.github.io/<repo-name>/`

## Project structure

```
market-dashboard/
├── .github/workflows/refresh_data.yml   # Daily data refresh
├── scripts/build_data.py                # Fetches data, outputs JSON + charts
├── data/                                # Generated (commit for Pages)
│   ├── snapshot.json
│   ├── events.json
│   ├── meta.json
│   ├── charts/*.png
│   └── holdings/*.json
├── index.html                            # Static frontend
├── requirements.txt
└── README.md
```

Data: Yahoo Finance (yfinance), economic calendar (investpy). Charts: TradingView embed.

## Reddit keyword research (PRAW)

Searches Reddit for a fixed keyword list, counts which subreddits appear in the first N search results (default 200), exports the **top 10** subreddits per keyword to JSON, then prints **old.reddit.com** search URLs and post titles for each top subreddit.

```powershell
# PowerShell — set credentials (use a Reddit "script" or installed app from reddit.com/prefs/apps)
$env:REDDIT_CLIENT_ID="your_client_id"
$env:REDDIT_CLIENT_SECRET="your_client_secret"
$env:REDDIT_USER_AGENT="your_app_name_by_username"

python -m pip install -r requirements.txt
python scripts/reddit_keyword_research.py --limit 200 --top 10
```

Output: `data/reddit_keyword_research.json`. **Do not commit API secrets**; prefer env vars or a local `.env` (not tracked by git).
