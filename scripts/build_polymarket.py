#!/usr/bin/env python3
"""Fetch financially relevant Polymarket prediction markets and output polymarket.json."""

import argparse
import hashlib
import json
import os
import requests
from datetime import datetime, timezone, timedelta

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
        "trump", "congress", "senate", "debt ceiling", "shutdown", "g7", "g20", "opec", "oil",
    ],
}

# Patterns to cluster duplicate markets by underlying topic
# Each entry: (regex pattern, topic key)
TOPIC_PATTERNS = [
    (r"bitcoin|btc", "bitcoin"),
    (r"crude oil|\(cl\)", "crude_oil"),
    (r"fed.*april|april.*fed|fomc.*april|april.*fomc", "fed_april"),
    (r"fed.*may|may.*fed|fomc.*may|may.*fomc", "fed_may"),
    (r"fed.*june|june.*fed|fomc.*june|june.*fomc", "fed_june"),
    (r"fed.*july|july.*fed|fomc.*july|july.*fomc", "fed_july"),
    (r"fed.*2026|2026.*fed rate|federal funds rate.*2026", "fed_2026"),
    (r"2028.*president|president.*2028|2028.*election", "us_2028_election"),
    (r"2026.*midterm|midterm.*2026", "us_2026_midterm"),
    (r"russia.*ukraine|ukraine.*russia|ceasefire", "ukraine_russia"),
    (r"china.*taiwan|taiwan.*china", "taiwan_china"),
    (r"s&p 500|sp500|spx", "sp500"),
    (r"nasdaq|qqq", "nasdaq"),
]

# Max markets to show per topic cluster
MAX_PER_TOPIC = 2
# Max markets per category
MAX_PER_CATEGORY = 8

API_URL = "https://gamma-api.polymarket.com/markets"
FETCH_LIMIT = 500
HISTORY_DAYS = 7


def q_hash(question):
    return hashlib.md5(question.encode()).hexdigest()[:12]


def get_topic_key(question):
    q = question.lower()
    for pattern, key in TOPIC_PATTERNS:
        if __import__("re").search(pattern, q):
            return key
    return q_hash(question)


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

    probs = {}
    for i, outcome in enumerate(outcomes):
        try:
            probs[outcome] = round(float(prices[i]) * 100, 1)
        except (IndexError, ValueError):
            probs[outcome] = None

    end_date = m.get("endDateIso") or (m.get("endDate", "")[:10] if m.get("endDate") else None)

    return {
        "id": q_hash(question),
        "question": question,
        "yes_prob": probs.get("Yes"),
        "probs": probs,
        "volume24hr": round(m.get("volume24hr", 0)),
        "volume": round(m.get("volumeNum", 0)),
        "liquidity": round(m.get("liquidityNum", 0)),
        "end_date": end_date,
        "outcomes": outcomes,
        "is_binary": set(outcomes) == {"Yes", "No"},
    }


def deduplicate(items):
    """Group by topic, keep MAX_PER_TOPIC highest-volume per topic, then cap total."""
    seen_topics = {}
    result = []
    for m in items:
        key = get_topic_key(m["question"])
        count = seen_topics.get(key, 0)
        if count < MAX_PER_TOPIC:
            seen_topics[key] = count + 1
            result.append(m)
        if len(result) >= MAX_PER_CATEGORY:
            break
    return result


def load_history(out_dir):
    path = os.path.join(out_dir, "polymarket_history.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def save_history(history, today_probs, out_dir):
    today_str = datetime.now(timezone.utc).date().isoformat()
    history[today_str] = today_probs

    # Keep last HISTORY_DAYS
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=HISTORY_DAYS)).isoformat()
    history = {k: v for k, v in history.items() if k >= cutoff}

    path = os.path.join(out_dir, "polymarket_history.json")
    with open(path, "w") as f:
        json.dump(history, f, separators=(",", ":"))
    return history


def get_5d_prob(history, market_id):
    today = datetime.now(timezone.utc).date()
    target = (today - timedelta(days=5)).isoformat()
    # Look for closest date at or before target
    candidates = sorted([d for d in history if d <= target], reverse=True)
    if not candidates:
        return None
    return history[candidates[0]].get(market_id)


def build_polymarket(out_dir):
    print("Fetching Polymarket data...")
    try:
        markets = fetch_markets()
    except Exception as e:
        print(f"  Polymarket fetch error: {e}")
        return

    os.makedirs(out_dir, exist_ok=True)
    history = load_history(out_dir)

    grouped = {cat: [] for cat in CATEGORIES}
    skipped = 0
    today = datetime.now(timezone.utc).date()

    for m in markets:
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

    # Sort by 24hr volume
    for cat in grouped:
        grouped[cat].sort(key=lambda x: x["volume24hr"], reverse=True)

    # Deduplicate
    for cat in grouped:
        grouped[cat] = deduplicate(grouped[cat])

    # Save today's probs to history
    today_probs = {}
    for items in grouped.values():
        for m in items:
            if m["yes_prob"] is not None:
                today_probs[m["id"]] = m["yes_prob"]
    history = save_history(history, today_probs, out_dir)

    # Attach 5-day delta
    for items in grouped.values():
        for m in items:
            prob_5d = get_5d_prob(history, m["id"])
            m["prob_5d"] = prob_5d
            if prob_5d is not None and m["yes_prob"] is not None:
                m["delta_5d"] = round(m["yes_prob"] - prob_5d, 1)
            else:
                m["delta_5d"] = None

    total = sum(len(v) for v in grouped.values())
    print(f"  {total} markets after dedup ({skipped} skipped)")
    for cat, items in grouped.items():
        print(f"    {cat}: {len(items)}")

    output = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "categories": grouped,
    }

    path = os.path.join(out_dir, "polymarket.json")
    with open(path, "w") as f:
        json.dump(output, f, separators=(",", ":"))
    print(f"  Written to {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="data")
    args = parser.parse_args()
    build_polymarket(args.out_dir)
