#!/usr/bin/env python3
"""
build_pizza_index.py  —  Pentagon Pizza Index + Freddie's Bar Index
Scrapes live Google Maps busyness for venues near the Pentagon,
appends to rolling history, computes 20D MA, writes data/pizza_index.json.

Designed to run at ~10pm ET (2am UTC) weekdays.

Usage:
    python3 scripts/build_pizza_index.py --out-dir data
    python3 scripts/build_pizza_index.py --out-dir data --dry-run
"""

import sys
import re
import json
import time
import argparse
import os
from datetime import datetime, timezone, timedelta

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ── Venues ────────────────────────────────────────────────────────────────────
VENUES = [
    {
        "name": "Pizzato Pizza",
        "type": "pizza",
        "maps_query": "Pizzato Pizza 2626 N Pershing Dr Arlington VA 22201",
    },
    {
        "name": "Papa John's Crystal City",
        "type": "pizza",
        "maps_query": "Papa Johns Pizza Crystal City Arlington VA 22202",
    },
    {
        "name": "Domino's Pentagon City",
        "type": "pizza",
        "maps_query": "Dominos Pizza Pentagon City Arlington VA",
    },
    {
        "name": "MOD Pizza Pentagon City",
        "type": "pizza",
        "maps_query": "MOD Pizza Pentagon City Arlington VA",
    },
    {
        "name": "Pizza Hut Pentagon City",
        "type": "pizza",
        "maps_query": "Pizza Hut Pentagon City Arlington VA",
    },
    {
        "name": "Freddie's Beach Bar",
        "type": "bar",
        "maps_query": "Freddie's Beach Bar 555 23rd St S Arlington VA 22202",
    },
]

MA_WINDOW = 20  # days for rolling average


# ── Scraping ──────────────────────────────────────────────────────────────────

def extract_busyness(content: str):
    """
    Extract live current busyness from Google Maps page HTML.
    Returns int 1–100, or None if no live reading available.
    Only matches the explicit 'Currently X% busy' aria-label — the most
    reliable signal. Avoids false matches from histogram bars.
    """
    live_patterns = [
        r'aria-label="Currently (\d+)% busy',
        r'Currently\s+(\d+)%\s+busy',
        r'(\d+)%\s+busy\s+right\s+now',
        r'Live[:\s]+(\d+)%\s+busy',
        r'"current_popularity"\s*:\s*(\d+)',
    ]
    for pat in live_patterns:
        m = re.search(pat, content, re.IGNORECASE)
        if m:
            val = int(m.group(1))
            return val if val > 0 else None
    return None


def extract_label(content: str):
    """Extract Google's qualitative busyness label."""
    labels = [
        ("as busy as it gets", "As Busy As It Gets"),
        ("busier than usual",  "Busier Than Usual"),
        ("a little busy",      "A Little Busy"),
        ("not too busy",       "Not Too Busy"),
        ("usually not busy",   "Usually Not Busy"),
    ]
    lower = content.lower()
    for phrase, label in labels:
        if phrase in lower:
            return label
    return None


def scrape_venue(page, venue: dict) -> dict:
    """Navigate Google Maps for a venue and return busyness reading."""
    result = {
        "name": venue["name"],
        "type": venue["type"],
        "busyness": None,
        "label": None,
    }
    try:
        query = venue["maps_query"].replace(" ", "+")
        url = f"https://www.google.com/maps/search/{query}/?hl=en"
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(4_000)

        content = page.content()
        busyness = extract_busyness(content)
        label = extract_label(content)

        # If on a list page, click into the first place result
        if busyness is None:
            try:
                first = page.locator('a[href*="/maps/place/"]').first
                if first.count():
                    first.click()
                    page.wait_for_timeout(4_000)
                    content = page.content()
                    busyness = extract_busyness(content)
                    label = extract_label(content)
            except Exception:
                pass

        result["busyness"] = busyness
        result["label"] = label
    except Exception as e:
        print(f"  [warn] {venue['name']}: {e}", file=sys.stderr)

    return result


def scrape_all(verbose=True) -> list:
    from playwright.sync_api import sync_playwright

    readings = []
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx = browser.new_context(
            viewport={"width": 1280, "height": 800},
            locale="en-US",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = ctx.new_page()

        for venue in VENUES:
            if verbose:
                print(f"  {venue['name']}...", end="", flush=True)
            r = scrape_venue(page, venue)
            readings.append(r)
            if verbose:
                if r["busyness"] is not None:
                    print(f" {r['busyness']}% ({r['label'] or 'live'})")
                else:
                    print(" — no live data")
            time.sleep(1.5)

        browser.close()
    return readings


# ── History + MA ──────────────────────────────────────────────────────────────

def load_history(path: str) -> list:
    if os.path.exists(path):
        with open(path) as f:
            data = json.load(f)
        return data.get("history", [])
    return []


def compute_score(readings: list, venue_type: str) -> int | None:
    """Average busyness across venues of a given type that returned data."""
    vals = [r["busyness"] for r in readings if r["type"] == venue_type and r["busyness"] is not None]
    return round(sum(vals) / len(vals)) if vals else None


def compute_ma(history: list, field: str, window: int) -> float | None:
    """Rolling mean of the last `window` non-None values for a field."""
    vals = [e[field] for e in history[-window:] if e.get(field) is not None]
    return round(sum(vals) / len(vals), 1) if vals else None


def pct_vs_ma(current, ma) -> str | None:
    if current is None or ma is None or ma == 0:
        return None
    diff = round((current - ma) / ma * 100)
    return f"{'+' if diff >= 0 else ''}{diff}%"


def alert_level(pizza_score, bar_score, pizza_ma, bar_ma) -> str:
    """
    Dual-signal classification:
      DUAL_SIGNAL  — pizza elevated + bar empty
      PIZZA_ALERT  — pizza elevated only
      BAR_ALERT    — bar empty only (military staff called in)
      NORMAL       — nothing unusual
      INSUFFICIENT — not enough data

    When MA is available: compare current vs MA with threshold.
    When MA is not yet available (< 20 days): use absolute fallback thresholds
    so we don't false-alarm on day 1.
    """
    if pizza_ma is not None:
        pizza_elevated = pizza_score is not None and pizza_score > pizza_ma * 1.3
    else:
        # No baseline yet — require absolute threshold (>= 80% is unusual for late night)
        pizza_elevated = pizza_score is not None and pizza_score >= 80

    if bar_ma is not None:
        bar_empty = bar_score is not None and bar_score < bar_ma * 0.5
    else:
        # No baseline yet — require clearly empty bar (< 20%)
        bar_empty = bar_score is not None and bar_score < 20

    if pizza_elevated and bar_empty:
        return "DUAL_SIGNAL"
    if pizza_elevated:
        return "PIZZA_ALERT"
    if bar_empty:
        return "BAR_ALERT"
    if pizza_score is None and bar_score is None:
        return "INSUFFICIENT"
    return "NORMAL"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="data", help="Output directory")
    parser.add_argument("--dry-run", action="store_true", help="Print results but don't write files")
    args = parser.parse_args()

    # ET timestamp (UTC-4 in summer, UTC-5 in winter — use UTC and note it)
    now_utc = datetime.now(timezone.utc)
    et_offset = timedelta(hours=-4)  # EDT; change to -5 in Nov
    now_et = now_utc + et_offset
    date_str = now_et.strftime("%Y-%m-%d")
    time_str = now_et.strftime("%H:%M")

    out_path = os.path.join(args.out_dir, "pizza_index.json")
    history = load_history(out_path)

    print(f"Pentagon Pizza + Bar Index  [{date_str} {time_str} ET]")

    try:
        from playwright.sync_api import sync_playwright  # noqa
    except ImportError:
        print("ERROR: playwright not installed. Run: pip3 install playwright && playwright install chromium --with-deps")
        sys.exit(1)

    readings = scrape_all(verbose=True)

    pizza_score = compute_score(readings, "pizza")
    bar_score   = compute_score(readings, "bar")

    # Append today's reading to history (one entry per day; overwrite if same date)
    entry = {
        "date":   date_str,
        "time":   time_str,
        "pizza":  pizza_score,
        "bar":    bar_score,
        "venues": {r["name"]: {"pct": r["busyness"], "label": r["label"]} for r in readings},
    }
    if history and history[-1]["date"] == date_str:
        history[-1] = entry   # overwrite same-day entry
    else:
        history.append(entry)

    # Keep up to 2 years of history
    history = history[-500:]

    # Compute MAs from history (excluding today for cleaner trend line)
    past = history[:-1]
    pizza_ma = compute_ma(past, "pizza", MA_WINDOW)
    bar_ma   = compute_ma(past, "bar",   MA_WINDOW)

    alert = alert_level(pizza_score, bar_score, pizza_ma, bar_ma)

    output = {
        "as_of": f"{date_str} {time_str} ET",
        "latest": {
            "date":         date_str,
            "time":         time_str,
            "pizza":        pizza_score,
            "bar":          bar_score,
            "pizza_ma20":   pizza_ma,
            "bar_ma20":     bar_ma,
            "pizza_vs_ma":  pct_vs_ma(pizza_score, pizza_ma),
            "bar_vs_ma":    pct_vs_ma(bar_score, bar_ma),
            "alert":        alert,
            "venues":       entry["venues"],
        },
        "history": history,
    }

    # Print summary
    print()
    print("─── SUMMARY ─────────────────────────────────────────────────")
    if pizza_score is not None:
        bar_vis = "█" * (pizza_score // 10) + "░" * (10 - pizza_score // 10)
        ma_str = f"  (20D MA: {pizza_ma}%  vs {output['latest']['pizza_vs_ma']})" if pizza_ma else ""
        print(f"🍕 Pizza  {pizza_score:3d}%  [{bar_vis}]{ma_str}")
    else:
        print("🍕 Pizza  —  no live data")

    if bar_score is not None:
        bar_vis = "█" * (bar_score // 10) + "░" * (10 - bar_score // 10)
        ma_str = f"  (20D MA: {bar_ma}%  vs {output['latest']['bar_vs_ma']})" if bar_ma else ""
        print(f"🏳️‍🌈 Bar    {bar_score:3d}%  [{bar_vis}]{ma_str}  ← low = alert")
    else:
        print("🏳️‍🌈 Bar    —  no live data")

    alert_labels = {
        "DUAL_SIGNAL":  "🚨 DUAL SIGNAL — pizza elevated + bar empty",
        "PIZZA_ALERT":  "⚡ PIZZA ELEVATED",
        "BAR_ALERT":    "⚡ BAR EMPTY — staff possibly called in",
        "NORMAL":       "✅ NORMAL",
        "INSUFFICIENT": "⚠  INSUFFICIENT DATA",
    }
    print(f"\nAlert: {alert_labels.get(alert, alert)}")
    print(f"History: {len(history)} days stored")
    print("──────────────────────────────────────────────────────────────")

    if not args.dry_run:
        os.makedirs(args.out_dir, exist_ok=True)
        with open(out_path, "w") as f:
            json.dump(output, f, separators=(",", ":"))
        print(f"Written: {out_path}")
    else:
        print("[dry-run] not writing files")


if __name__ == "__main__":
    main()
