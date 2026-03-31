#!/usr/bin/env python3
"""Fetch financially relevant Polymarket prediction markets and output polymarket.json."""

import argparse
import json
import os
import re
import requests
from datetime import datetime, timezone

CATEGORIES = {
    "Fed / Rates": [
        "fed", "fomc", "federal reserve", "rate cut", "rate hike", "basis point",
        "powell", "interest rate", "fed funds", "25bp", "50bp", "monetary policy",
    ],
    "Macro": [
        "recession", "gdp", "cpi", "inflation", "unemployment", "jobs report",
        "nonfarm", "pce", "consumer price", "producer price", "soft landing",
        "hard landing", "stagflation", "yield curve", "payroll", "retail sales",
        "economic", "economy", "debt", "deficit", "treasury", "10-year", "10 year",
        "housing", "mortgage", "default", "bankruptcy",
    ],
    "Markets": [
        "s&p", "sp500", "nasdaq", "dow jones", "russell", "stock market",
        "bitcoin", "btc", "crypto", "ipo", "earnings", "correction", "bear market",
        "bull market", "vix", "volatility",
    ],
    "Geopolitical": [
        "tariff", "trade war", "china", "russia", "ukraine", "taiwan", "sanctions",
        "trump", "election", "congress", "senate", "house", "debt ceiling",
        "shutdown", "g7", "g20", "opec", "oil",
    ],
}

API_URL = "https://gamma-api.polymarket.com/markets"
FETCH_LIMIT = 500


def fetch_markets():
    params = {
        "active": "true",
        "closed": "false",
        "limit": FETCH_LIMIT,
        "order": "volume24hr",
        "ascending": "false",
    }
    resp = requests.get(API_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def categorize(question):
    q = question.lower()
    for cat, keywords in CATEGORIES.items():
        if any(kw in q for kw in keywords):
            return cat
    return None


def parse_market(m):
    question = m.get("question", "")
    outcomes = json.loads(m.get("outcomes", '["Yes","No"]'))
    prices = json.loads(m.get("outcomePrices", '["0.5","0.5"]'))

    # Build outcome→probability map
    probs = {}
    for i, outcome in enumerate(outcomes):
        try:
            probs[outcome] = round(float(prices[i]) * 100, 1)
        except (IndexError, ValueError):
            probs[outcome] = None

    # For Yes/No markets show just the Yes probability
    yes_prob = probs.get("Yes")

    end_date = m.get("endDateIso") or (m.get("endDate", "")[:10] if m.get("endDate") else None)

    return {
        "question": question,
        "yes_prob": yes_prob,
        "probs": probs,
        "volume24hr": round(m.get("volume24hr", 0)),
        "volume": round(m.get("volumeNum", 0)),
        "liquidity": round(m.get("liquidityNum", 0)),
        "end_date": end_date,
        "outcomes": outcomes,
        "is_binary": set(outcomes) == {"Yes", "No"},
    }


def build_polymarket(out_dir):
    print("Fetching Polymarket data...")
    try:
        markets = fetch_markets()
    except Exception as e:
        print(f"  Polymarket fetch error: {e}")
        return

    grouped = {cat: [] for cat in CATEGORIES}
    skipped = 0

    today = datetime.now(timezone.utc).date()
    for m in markets:
        # Skip already-ended markets
        end_iso = m.get("endDateIso") or (m.get("endDate", "")[:10] if m.get("endDate") else None)
        if end_iso:
            try:
                if datetime.fromisoformat(end_iso).date() <= today:
                    skipped += 1
                    continue
            except ValueError:
                pass

        cat = categorize(m.get("question", ""))
        if cat is None:
            skipped += 1
            continue
        grouped[cat].append(parse_market(m))

    # Sort each category by 24hr volume
    for cat in grouped:
        grouped[cat].sort(key=lambda x: x["volume24hr"], reverse=True)

    total = sum(len(v) for v in grouped.values())
    print(f"  {total} relevant markets across {len(CATEGORIES)} categories ({skipped} skipped)")
    for cat, items in grouped.items():
        print(f"    {cat}: {len(items)}")

    output = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "categories": grouped,
    }

    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "polymarket.json")
    with open(path, "w") as f:
        json.dump(output, f, separators=(",", ":"))
    print(f"  Written to {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="data")
    args = parser.parse_args()
    build_polymarket(args.out_dir)
