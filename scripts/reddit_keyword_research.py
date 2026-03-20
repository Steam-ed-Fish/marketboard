#!/usr/bin/env python3
"""
Reddit keyword research via PRAW.

- For each keyword: search r/all (limit 200), count posts per subreddit.
- Export top-10 subreddits per keyword to JSON.
- For each keyword, search the top-10 subreddits and print post titles + old.reddit search URLs.

Credentials (do not commit real values):
  REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

# Cleaner titles on Windows terminals
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
import urllib.parse
from collections import Counter
from pathlib import Path

try:
    import praw
except ImportError:
    print("Install PRAW: pip install praw", file=sys.stderr)
    raise SystemExit(1)

KEYWORDS = [
    "Insta360 Link",
    "OBSBOT Tiny",
    "AI webcam",
    "4K webcam",
    "Streaming camera",
    "DJI mic",
    "wireless microphone",
    "bluetooth mic",
    "wireless lavalier microphone",
]

DEFAULT_SEARCH_LIMIT = 200
TOP_N = 10
SUBREDDIT_SEARCH_LIMIT = 25  # titles to show per (keyword, subreddit)


def build_reddit() -> praw.Reddit:
    cid = os.environ.get("REDDIT_CLIENT_ID")
    secret = os.environ.get("REDDIT_CLIENT_SECRET")
    ua = os.environ.get("REDDIT_USER_AGENT")
    if not all([cid, secret, ua]):
        print(
            "Set environment variables: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT",
            file=sys.stderr,
        )
        raise SystemExit(1)
    return praw.Reddit(client_id=cid, client_secret=secret, user_agent=ua)


def old_reddit_search_url(subreddit: str, query: str) -> str:
    q = urllib.parse.quote_plus(query)
    sr = urllib.parse.quote(subreddit, safe="")
    return (
        f"https://old.reddit.com/r/{sr}/search?q={q}&restrict_sr=1&sort=relevance"
    )


def collect_subreddit_counts(
    reddit: praw.Reddit, keyword: str, limit: int
) -> Counter[str]:
    counts: Counter[str] = Counter()
    try:
        for submission in reddit.subreddit("all").search(keyword, limit=limit):
            try:
                name = submission.subreddit.display_name
            except Exception:
                continue
            counts[name] += 1
    except Exception as e:
        print(f"  [warn] search failed for {keyword!r}: {e}", file=sys.stderr)
    return counts


def top_n_counts(counter: Counter[str], n: int) -> dict[str, int]:
    return dict(counter.most_common(n))


def print_subreddit_hits(
    reddit: praw.Reddit, keyword: str, subreddits: list[str]
) -> None:
    for sr in subreddits:
        url = old_reddit_search_url(sr, keyword)
        print(f"\n  r/{sr}")
        print(f"  Old Reddit: {url}")
        try:
            sub = reddit.subreddit(sr)
            found = 0
            for submission in sub.search(keyword, limit=SUBREDDIT_SEARCH_LIMIT):
                title = (submission.title or "").replace("\n", " ").strip()
                print(f"    - {title}")
                found += 1
            if found == 0:
                print("    (no results in this subreddit via API — try the URL above)")
        except Exception as e:
            print(f"    [error] {e}")


def main() -> None:
    parser = argparse.ArgumentParser(description="PRAW subreddit keyword research")
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_SEARCH_LIMIT,
        help="Max posts to fetch per keyword from r/all search (default 200)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=TOP_N,
        help="Top subreddits to keep per keyword (default 10)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data") / "reddit_keyword_research.json",
        help="Output JSON path",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=1.0,
        help="Seconds to sleep between r/all searches (rate limits)",
    )
    args = parser.parse_args()

    reddit = build_reddit()
    export: dict[str, dict[str, int]] = {}

    print("=== r/all search: counting subreddits (per keyword, limit=%d) ===\n" % args.limit)
    for kw in KEYWORDS:
        print(f"Keyword: {kw!r}")
        counts = collect_subreddit_counts(reddit, kw, args.limit)
        top = top_n_counts(counts, args.top)
        export[kw] = {f"r/{name}": c for name, c in top.items()}
        print(f"  Unique subreddits in sample: {len(counts)} | Top {args.top}:")
        for name, c in counts.most_common(args.top):
            print(f"    r/{name}: {c}")
        time.sleep(max(0.0, args.sleep))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(export, f, indent=2, ensure_ascii=False)
    print(f"\n=== Wrote {args.out} ===\n")

    print("=== Per-keyword: top subreddits + post titles (PRAW) + old.reddit URL ===\n")
    for kw in KEYWORDS:
        # export keys are "r/subreddit"
        top_names = [k[2:] if k.startswith("r/") else k for k in export[kw]]
        print(f"--- {kw!r} ---")
        print_subreddit_hits(reddit, kw, top_names)
        time.sleep(max(0.0, args.sleep))


if __name__ == "__main__":
    main()
