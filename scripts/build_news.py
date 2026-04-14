"""
Fetch market news via Tavily API and write data/news.json.
Designed for Tavily free tier (~10 searches per build, 1000/month).

Usage: python scripts/build_news.py --out-dir data
Requires: TAVILY_API_KEY env var
"""
from __future__ import print_function
import argparse
import json
import os
from datetime import datetime, timezone

from tavily import TavilyClient


# Friendly names for sector ETFs
SECTOR_NAMES = {
    "XLK": "Technology", "XLI": "Industrials", "XLC": "Communication Services",
    "XLF": "Financials", "XLU": "Utilities", "XLY": "Consumer Discretionary",
    "XLRE": "Real Estate", "XLP": "Consumer Staples", "XLB": "Materials",
    "XLE": "Energy", "XLV": "Healthcare",
}


def tavily_search(client, query, max_results=3):
    """Run a single Tavily basic search. Returns list of {title, url, snippet, source}."""
    try:
        resp = client.search(query, max_results=max_results, search_depth="basic")
        results = []
        for r in resp.get("results", []):
            snippet = (r.get("content") or "")[:300]
            url = r.get("url", "")
            # Extract source domain
            source = ""
            if url:
                try:
                    from urllib.parse import urlparse
                    source = urlparse(url).netloc.replace("www.", "")
                except Exception:
                    pass
            results.append({
                "title": r.get("title", ""),
                "url": url,
                "snippet": snippet,
                "source": source,
            })
        return results
    except Exception as e:
        print("  Tavily search failed for '{}': {}".format(query, e))
        return []


def pick_top_movers(snapshot, n=3):
    """Return top N tickers by absolute daily % change across all groups."""
    movers = []
    for group_name, rows in (snapshot.get("groups") or {}).items():
        if group_name in ("Industries", "Countries", "EW Sectors", "S&P Style ETFs"):
            continue
        for r in rows:
            ticker = r.get("ticker", "")
            daily = r.get("daily")
            if ticker and daily is not None:
                movers.append((abs(daily), daily, ticker))
    movers.sort(reverse=True)
    seen = set()
    result = []
    for _, daily, ticker in movers:
        if ticker not in seen:
            seen.add(ticker)
            result.append((ticker, daily))
            if len(result) >= n:
                break
    return result


def pick_hot_sectors(snapshot, n=2):
    """Return top N sectors by absolute daily % change."""
    sectors = []
    for r in snapshot.get("groups", {}).get("Sel Sectors", []):
        ticker = r.get("ticker", "")
        daily = r.get("daily")
        if ticker and daily is not None:
            sectors.append((abs(daily), daily, ticker))
    sectors.sort(reverse=True)
    return [(t, d) for _, d, t in sectors[:n]]


def pick_volume_spike(snapshot):
    """Return the ticker with highest vol_ratio > 3x, if any."""
    best = (0, "")
    for group_name, rows in (snapshot.get("groups") or {}).items():
        if group_name in ("Industries", "Countries"):
            continue
        for r in rows:
            vr = r.get("vol_ratio") or 0
            if vr > 3.0 and vr > best[0]:
                best = (vr, r.get("ticker", ""))
    return best[1] if best[1] else None


def build_news(snapshot, client):
    """Run ~10 Tavily searches and return structured news data."""
    news = {"market": [], "movers": {}, "sectors": {}}
    search_count = 0

    # --- 3 broad market searches ---
    broad_queries = [
        "US stock market today",
        "S&P 500 news today",
        "Federal Reserve policy news",
    ]
    for q in broad_queries:
        print("  Searching: '{}'".format(q))
        results = tavily_search(client, q)
        search_count += 1
        for r in results:
            # Deduplicate by title
            if not any(existing["title"] == r["title"] for existing in news["market"]):
                news["market"].append(r)

    # --- Top 3 movers ---
    top_movers = pick_top_movers(snapshot, n=3)
    for ticker, daily in top_movers:
        q = "{} stock news today".format(ticker)
        print("  Searching mover: '{}' ({:+.2f}%)".format(q, daily))
        results = tavily_search(client, q)
        search_count += 1
        if results:
            news["movers"][ticker] = results

    # --- Top 2 hot sectors ---
    hot_sectors = pick_hot_sectors(snapshot, n=2)
    for ticker, daily in hot_sectors:
        name = SECTOR_NAMES.get(ticker, ticker)
        q = "{} {} sector news today".format(ticker, name)
        print("  Searching sector: '{}' ({:+.2f}%)".format(q, daily))
        results = tavily_search(client, q)
        search_count += 1
        if results:
            news["sectors"][ticker] = results

    # --- 1 geopolitical ---
    print("  Searching: 'geopolitical risk financial markets'")
    geo_results = tavily_search(client, "geopolitical risk financial markets")
    search_count += 1
    for r in geo_results:
        if not any(existing["title"] == r["title"] for existing in news["market"]):
            r["category"] = "geopolitical"
            news["market"].append(r)

    # --- 1 volume spike ---
    vol_ticker = pick_volume_spike(snapshot)
    if vol_ticker:
        q = "{} unusual volume stock news".format(vol_ticker)
        print("  Searching volume spike: '{}'".format(q))
        results = tavily_search(client, q)
        search_count += 1
        if results:
            news["movers"].setdefault(vol_ticker, []).extend(results)
    else:
        print("  No volume spike >3x found, skipping search")

    print("Total Tavily searches: {}".format(search_count))
    return news


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="data")
    args = parser.parse_args()

    api_key = os.environ.get("TAVILY_API_KEY", "")
    if not api_key:
        print("No TAVILY_API_KEY set — writing empty news.json")
        empty = {
            "built_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "market": [], "movers": {}, "sectors": {},
        }
        out_path = os.path.join(args.out_dir, "news.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(empty, f, ensure_ascii=False, indent=2)
        return

    snapshot_path = os.path.join(args.out_dir, "snapshot.json")
    if not os.path.exists(snapshot_path):
        print("Snapshot not found at {} — cannot determine movers".format(snapshot_path))
        return

    with open(snapshot_path, encoding="utf-8") as f:
        snapshot = json.load(f)

    client = TavilyClient(api_key=api_key)
    print("Fetching market news via Tavily...")
    news = build_news(snapshot, client)
    news["built_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    out_path = os.path.join(args.out_dir, "news.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(news, f, ensure_ascii=False, indent=2)

    print("News written to {} ({} market headlines, {} movers, {} sectors)".format(
        out_path, len(news["market"]),
        len(news["movers"]), len(news["sectors"])))


if __name__ == "__main__":
    main()
