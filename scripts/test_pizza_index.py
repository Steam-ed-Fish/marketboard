#!/usr/bin/env python3
"""
test_pizza_index.py  —  Pentagon Pizza Index + Freddie's Bar Index
Scrapes live Google Maps busyness for venues near the Pentagon.

Run this first to confirm data is reachable before wiring into the cron.
Best run at 8pm–midnight ET when the signal is meaningful.

Usage:
    python3 scripts/test_pizza_index.py
    python3 scripts/test_pizza_index.py --json     # JSON output only
    python3 scripts/test_pizza_index.py --install  # install deps then run
"""

import sys
import re
import json
import time
import argparse
import subprocess
from datetime import datetime

# ── Venues to track ───────────────────────────────────────────────────────────
# type "pizza" → high busyness = alert signal
# type "bar"   → LOW busyness = alert signal (inverted)
VENUES = [
    {
        "name": "Pizzato Pizza",
        "type": "pizza",
        "address": "2626 N Pershing Dr, Arlington, VA 22201",
        "maps_query": "Pizzato Pizza 2626 N Pershing Dr Arlington VA",
    },
    {
        "name": "Papa John's Crystal City",
        "type": "pizza",
        "address": "Crystal City, Arlington, VA 22202",
        "maps_query": "Papa Johns Pizza Crystal City Arlington VA 22202",
    },
    {
        "name": "Domino's Pentagon City",
        "type": "pizza",
        "address": "Pentagon City, Arlington, VA",
        "maps_query": "Dominos Pizza Pentagon City Arlington VA",
    },
    {
        "name": "MOD Pizza Pentagon City",
        "type": "pizza",
        "address": "Pentagon City, Arlington, VA 22202",
        "maps_query": "MOD Pizza Pentagon City Arlington VA",
    },
    {
        "name": "Pizza Hut Pentagon City",
        "type": "pizza",
        "address": "Pentagon City, Arlington, VA 22202",
        "maps_query": "Pizza Hut Pentagon City Arlington VA",
    },
    {
        "name": "Freddie's Beach Bar",
        "type": "bar",
        "address": "555 23rd St S, Arlington, VA 22202",
        "maps_query": "Freddie's Beach Bar 555 23rd St S Arlington VA",
    },
]

def ensure_deps():
    """Install playwright and chromium if not present."""
    try:
        from playwright.sync_api import sync_playwright  # noqa
        print("[deps] playwright already installed.")
    except ImportError:
        print("[deps] Installing playwright...")
        subprocess.run([sys.executable, "-m", "pip", "install", "playwright"], check=True)
    # Check if chromium is installed
    result = subprocess.run(
        [sys.executable, "-m", "playwright", "install", "--dry-run", "chromium"],
        capture_output=True, text=True
    )
    if "chromium" in (result.stdout + result.stderr).lower() and "already" not in (result.stdout + result.stderr).lower():
        print("[deps] Installing Chromium (one-time, ~120MB)...")
        subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"],
            check=True
        )
    else:
        # just run install anyway — it's a no-op if already there
        subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            check=True, capture_output=True
        )


def extract_busyness(content: str) -> int | None:
    """
    Parse Google Maps page HTML/text for LIVE current busyness percentage.
    Only returns a value when Google is showing real-time data (place is open).
    Returns 1–100 or None if no live reading available.

    Google shows live data as:
      - aria-label="Currently 72% busy"
      - JS data: current_popularity:72
      - Text: "72% busy right now" / "Live: 72%"
    Historical histogram bars also contain "X% busy" but we must NOT match those —
    they cause false 0% readings when the place is closed.
    """
    # Most specific first — these only appear with live data
    live_patterns = [
        r'aria-label="Currently (\d+)% busy"',
        r'Currently\s+(\d+)%\s+busy',
        r'(\d+)%\s+busy\s+right\s+now',
        r'Live[:\s]+(\d+)%',
        r'"current_popularity"\s*:\s*(\d+)',
        r'currentPopularity["\s:]+(\d+)',
    ]
    for pat in live_patterns:
        m = re.search(pat, content, re.IGNORECASE)
        if m:
            val = int(m.group(1))
            return val if val > 0 else None  # 0 means closed / no live data
    return None


def extract_popular_times_label(content: str) -> str | None:
    """Extract qualitative busyness label if present."""
    labels = [
        "as busy as it gets", "busier than usual", "usually not too busy",
        "not too busy", "a little busy", "live"
    ]
    lower = content.lower()
    for lbl in labels:
        if lbl in lower:
            return lbl.title()
    return None


def scrape_venue(page, venue: dict) -> dict:
    """Open Google Maps for venue and extract busyness."""
    query = venue["maps_query"].replace(" ", "+")
    url = f"https://www.google.com/maps/search/{query}/?hl=en"

    result = {
        "name": venue["name"],
        "type": venue["type"],
        "busyness": None,
        "label": None,
        "status": "no_data",
        "ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        # Wait a bit for dynamic content to load
        page.wait_for_timeout(4_000)

        content = page.content()
        busyness = extract_busyness(content)
        label = extract_popular_times_label(content)

        # Try clicking the first result if we're on a list page
        if busyness is None:
            try:
                first = page.locator('a[href*="/maps/place/"]').first
                if first.count():
                    first.click()
                    page.wait_for_timeout(4_000)
                    content = page.content()
                    busyness = extract_busyness(content)
                    label = extract_popular_times_label(content)
            except Exception:
                pass

        result["busyness"] = busyness
        result["label"] = label
        result["status"] = "ok" if busyness is not None else "no_live_data"

    except Exception as e:
        result["status"] = f"error: {e}"

    return result


def run(json_only=False, debug=False):
    from playwright.sync_api import sync_playwright

    results = []
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
            if not json_only:
                print(f"  Checking {venue['name']}...", end="", flush=True)
            r = scrape_venue(page, venue)
            if debug:
                # Save raw HTML for inspection
                fname = f"/tmp/debug_{venue['name'].replace(' ','_')}.html"
                try:
                    page.goto(
                        f"https://www.google.com/maps/search/{venue['maps_query'].replace(' ','+')}/?hl=en",
                        wait_until="domcontentloaded", timeout=20_000
                    )
                    page.wait_for_timeout(3000)
                    with open(fname, "w") as fh:
                        fh.write(page.content())
                    print(f"\n    [debug] saved {fname}")
                except Exception as e:
                    print(f"\n    [debug] failed to save: {e}")
            results.append(r)
            if not json_only:
                if r["busyness"] is not None:
                    print(f" {r['busyness']}% busy  ({r['label'] or 'live data'})")
                else:
                    print(f" — no live data ({r['status']})")
            time.sleep(1.5)  # polite delay between requests

        browser.close()

    return results


def summarise(results):
    pizza = [r["busyness"] for r in results if r["type"] == "pizza" and r["busyness"] is not None]
    bar   = [r["busyness"] for r in results if r["type"] == "bar"   and r["busyness"] is not None]

    pizza_score = round(sum(pizza) / len(pizza)) if pizza else None
    bar_score   = bar[0] if bar else None

    print("\n─── SUMMARY ───────────────────────────────────────────")
    if pizza_score is not None:
        bar_filled = "█" * (pizza_score // 10) + "░" * (10 - pizza_score // 10)
        alert = "⚠  ELEVATED" if pizza_score > 75 else ("  NORMAL" if pizza_score > 40 else "  QUIET")
        print(f"🍕 Pizza Index  : {pizza_score:3d}%  [{bar_filled}]  {alert}")
    else:
        print("🍕 Pizza Index  : no live data  (try again after 8pm ET)")

    if bar_score is not None:
        bar_filled = "█" * (bar_score // 10) + "░" * (10 - bar_score // 10)
        # Inverted: empty bar = alert
        alert = "⚠  EMPTY (ALERT)" if bar_score < 25 else ("  NORMAL" if bar_score > 50 else "  QUIET")
        print(f"🏳️‍🌈 Bar Index    : {bar_score:3d}%  [{bar_filled}]  {alert}  (low = military activity)")
    else:
        print("🏳️‍🌈 Bar Index    : no live data  (Freddie's opens at 4pm ET)")

    if pizza_score is not None and bar_score is not None:
        if pizza_score > 75 and bar_score < 25:
            print("\n🚨 DUAL SIGNAL — pizza high + bar empty. Historically strong geopolitical indicator.")
        elif pizza_score > 75:
            print("\n⚡ Pizza elevated but bar data unclear.")
        elif bar_score < 25:
            print("\n⚡ Bar empty but pizza data unclear.")
    print("────────────────────────────────────────────────────────")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json",    action="store_true", help="JSON output only")
    parser.add_argument("--install", action="store_true", help="Install deps before running")
    parser.add_argument("--debug",   action="store_true", help="Save raw HTML to /tmp for inspection")
    args = parser.parse_args()

    if args.install:
        ensure_deps()
    else:
        # Try importing — if it fails, tell the user
        try:
            from playwright.sync_api import sync_playwright  # noqa
        except ImportError:
            print("playwright not found. Run with --install first:\n")
            print("  python3 scripts/test_pizza_index.py --install\n")
            sys.exit(1)

    if not args.json:
        print(f"\nPentagon Pizza + Gay Bar Index  [{datetime.now().strftime('%Y-%m-%d %H:%M ET')}]")
        print("Note: live data only appears when places are currently open & busy.\n")

    results = run(json_only=args.json, debug=args.debug)

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        summarise(results)


if __name__ == "__main__":
    main()
