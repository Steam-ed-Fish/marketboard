"""
Daily Intelligence Briefing — reads data/snapshot.json, optionally calls Perplexity
for a market news summary, then calls Claude Sonnet to write the briefing.
Writes result as first entry in data/events.json.

Usage: python scripts/generate_briefing.py --out-dir data
Requires: ANTHROPIC_API_KEY env var
Optional: PERPLEXITY_API_KEY env var (adds market context)
"""
from __future__ import print_function
import argparse
import json
import os
import requests
from datetime import datetime, timezone

import anthropic


# ---------------------------------------------------------------------------
# Perplexity — fetch brief market-focused news context
# ---------------------------------------------------------------------------

CROSS_ASSET_NAMES = {
    "TLT": "20Y+ Treasuries", "IEF": "7-10Y Treasuries", "SHY": "1-3Y Treasuries",
    "HYG": "High Yield Credit", "LQD": "Investment Grade Credit",
    "UUP": "US Dollar Index", "GLD": "Gold", "SLV": "Silver",
    "USO": "WTI Oil", "UNG": "Natural Gas", "VIXY": "VIX Futures",
}

def fetch_perplexity_context(api_key):
    """Call Perplexity Sonar to get a brief market-focused news summary."""
    today = datetime.now(timezone.utc).strftime("%B %d, %Y")
    prompt = (
        "Today is {}. In exactly 3 concise sentences, summarize what drove US equity markets today. "
        "Focus ONLY on: which sectors or asset classes moved and the immediate market catalyst behind each move. "
        "Be specific — name the sectors, ETFs, or assets and the direction. "
        "Do not discuss political opinions. Do not speculate beyond what actually moved."
    ).format(today)
    try:
        resp = requests.post(
            "https://api.perplexity.ai/chat/completions",
            headers={"Authorization": "Bearer {}".format(api_key), "Content-Type": "application/json"},
            json={"model": "sonar", "messages": [{"role": "user", "content": prompt}]},
            timeout=20,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print("Perplexity call failed: {}".format(e))
        return None


# ---------------------------------------------------------------------------
# Context builder
# ---------------------------------------------------------------------------

def build_context(snapshot, news_context=None):
    lines = []
    built_at = snapshot.get("built_at", "unknown")
    lines.append("DATA TIMESTAMP: {}\n".format(built_at))

    # News context from Perplexity (market-focused, 3 sentences)
    if news_context:
        lines.append("TODAY'S MARKET CONTEXT (use to explain WHY sectors moved, keep brief):")
        lines.append("  {}".format(news_context))
        lines.append("")

    # Fear & Greed
    fg = snapshot.get("fear_greed") or {}
    if fg:
        lines.append("FEAR & GREED: {}/100 — {}".format(fg.get("score", "?"), fg.get("sentiment", "?")))
        vix_detail = (fg.get("components") or {}).get("volatility", {}).get("detail", "")
        if vix_detail:
            lines.append("  VIX: {}".format(vix_detail))
        lines.append("")

    # FRED Macro
    mf = snapshot.get("macro_fred") or {}
    if mf and mf.get("series"):
        lines.append("MACRO: {}".format((mf.get("dominant_signal") or "neutral").upper()))
        for sid, sd in mf.get("series", {}).items():
            val_str = "{:.2f}{}".format(sd["value"], sd.get("unit", "")) if sd.get("value") is not None else "N/A"
            lines.append("  {}: {} [{}] chg={}".format(
                sd.get("label", sid), val_str, sd.get("signal", ""), sd.get("change", "")))
        lines.append("")

    groups = snapshot.get("groups") or {}

    # Indices — ranked by daily
    idx_rows = sorted(groups.get("Indices") or [], key=lambda r: r.get("daily") or 0, reverse=True)
    idx_lookup = {r.get("ticker"): r for r in idx_rows}
    if idx_rows:
        lines.append("INDICES (ranked by 1d):")
        for r in idx_rows:
            lines.append("  {}: 1d={:+.2f}%  5d={:+.2f}%  20d={:+.2f}%  ytd={:+.2f}%".format(
                r.get("ticker", "?"),
                r.get("daily") or 0, r.get("5d") or 0,
                r.get("20d") or 0, r.get("ytd") or 0))
        spy_20d = (idx_lookup.get("SPY") or {}).get("20d") or 0
        rsp_20d = (idx_lookup.get("RSP") or {}).get("20d") or 0
        diff = rsp_20d - spy_20d
        lines.append("  BREADTH: RSP {:+.2f}% vs SPY {:+.2f}% (20d) — RSP {} SPY by {:.2f}pp".format(
            rsp_20d, spy_20d, "OUTPERFORMING" if diff > 0 else "UNDERPERFORMING", abs(diff)))
        lines.append("")

    # Cross-asset — daily moves
    ca = snapshot.get("cross_asset") or {}
    if ca:
        ca_sorted = sorted(ca.items(), key=lambda x: x[1].get("daily") or 0, reverse=True)
        lines.append("CROSS-ASSET (ranked by 1d):")
        for t, v in ca_sorted:
            name = CROSS_ASSET_NAMES.get(t, t)
            lines.append("  {} ({}): 1d={:+.2f}%  20d={:+.2f}%".format(
                t, name, v.get("daily") or 0, v.get("20d") or 0))
        lines.append("")

    # Sel Sectors — ranked by daily
    sel_rows = sorted(groups.get("Sel Sectors") or [], key=lambda r: r.get("daily") or 0, reverse=True)
    if sel_rows:
        lines.append("SECTORS (ranked by 1d):")
        for r in sel_rows:
            lines.append("  {}: 1d={:+.2f}%  5d={:+.2f}%  20d={:+.2f}%".format(
                r.get("ticker", "?"), r.get("daily") or 0,
                r.get("5d") or 0, r.get("20d") or 0))
        lines.append("")

    # Top/bottom Industries ETFs by daily
    ind_rows = sorted(groups.get("Industries") or [], key=lambda r: r.get("daily") or 0, reverse=True)
    if ind_rows:
        top3 = ind_rows[:4]
        bot3 = ind_rows[-4:]
        lines.append("INDUSTRIES — top 4 today: " + "  ".join(
            "{} {:+.2f}%".format(r.get("ticker",""), r.get("daily") or 0) for r in top3))
        lines.append("INDUSTRIES — bot 4 today: " + "  ".join(
            "{} {:+.2f}%".format(r.get("ticker",""), r.get("daily") or 0) for r in bot3))
        lines.append("")

    # The 7s — ranked by daily
    glance_rows = [r for r in (groups.get("The 7s at a Glance") or []) if not r.get("is_rrg_row")]
    if glance_rows:
        g_sorted = sorted(glance_rows, key=lambda r: r.get("daily") or 0, reverse=True)
        lines.append("THE 7s BASKETS (ranked by 1d):")
        for r in g_sorted:
            lines.append("  {}: 1d={:+.2f}%  20d={:+.2f}%  ytd={:+.2f}%".format(
                r.get("ticker", "?"), r.get("daily") or 0,
                r.get("20d") or 0, r.get("ytd") or 0))
        lines.append("")

    # Volume spikes
    vol_spikes = []
    for gname, rows in groups.items():
        if gname in {"The 7s at a Glance"}:
            continue
        for r in rows:
            vr = r.get("vol_ratio") or 0
            if vr > 2.0:
                vol_spikes.append((vr, r.get("ticker", ""), r.get("daily") or 0))
    vol_spikes.sort(reverse=True)
    if vol_spikes:
        lines.append("VOL SPIKES (>2x avg):")
        for vr, t, d in vol_spikes[:8]:
            lines.append("  {}: {:.1f}x  1d={:+.2f}%".format(t, vr, d))
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Claude prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "You are writing a daily end-of-day market recap for a professional trader. "
    "Tone: direct, specific, zero filler. Every sentence must cite a number or a name.\n\n"
    "Write exactly four sections:\n\n"
    "INDICES — VIX level and move. Rank SPY/QQQ/IWM/DIA by today's return (best to worst) with exact %s. "
    "State whether small caps outperformed or underperformed large caps. "
    "One sentence on bonds/credit/dollar from cross-asset data if notable.\n\n"
    "SECTORS — Rank all 11 sectors by today's return, best to worst, with exact %s. "
    "Then one sentence on top 2 and bottom 2 Industries ETFs today with %s.\n\n"
    "THE 7s — Rank all baskets by today's return, best to worst, with exact %s. "
    "Name the strongest individual ticker in the top basket and weakest in the bottom basket.\n\n"
    "SIGNALS — Fear & Greed score. Top vol spikes. "
    "20d trend leaders and laggards (best and worst sector + basket). "
    "If news context is provided, use one sentence to tie the biggest sector move to its catalyst — "
    "keep it market-focused, no political commentary.\n\n"
    "Total: strictly under 250 words. Plain text. Section labels ALL CAPS + em-dash. No markdown."
)


def generate_briefing(snapshot, api_key, news_context=None):
    context = build_context(snapshot, news_context)
    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=900,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": context}],
    )
    return message.content[0].text.strip()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="data")
    args = parser.parse_args()

    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not anthropic_key:
        print("No ANTHROPIC_API_KEY set — skipping briefing generation")
        return

    perplexity_key = os.environ.get("PERPLEXITY_API_KEY", "")

    snapshot_path = os.path.join(args.out_dir, "snapshot.json")
    events_path   = os.path.join(args.out_dir, "events.json")

    if not os.path.exists(snapshot_path):
        print("Snapshot not found at {} — skipping briefing".format(snapshot_path))
        return

    with open(snapshot_path, encoding="utf-8") as f:
        snapshot = json.load(f)

    # Optional: fetch market context from Perplexity
    news_context = None
    if perplexity_key:
        print("Fetching market context from Perplexity...")
        news_context = fetch_perplexity_context(perplexity_key)
        if news_context:
            print("  Context: {}".format(news_context[:120]))

    print("Generating intelligence briefing via Claude API...")
    try:
        text = generate_briefing(snapshot, anthropic_key, news_context)
    except Exception as e:
        print("Briefing generation failed: {}".format(e))
        return

    # Load existing events.json, drop old briefing, prepend new one
    events = []
    if os.path.exists(events_path):
        try:
            with open(events_path, encoding="utf-8") as f:
                events = json.load(f)
        except Exception:
            events = []

    events = [e for e in events if e.get("type") != "briefing"]

    now = datetime.now(timezone.utc)
    events.insert(0, {
        "type": "briefing",
        "date": now.strftime("%d/%m/%Y"),
        "time": now.strftime("%H:%M"),
        "event": "Daily Intelligence Briefing",
        "text": text,
    })

    with open(events_path, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2)

    print("Briefing written to {}".format(events_path))
    print("--- PREVIEW ---")
    print(text[:400])


if __name__ == "__main__":
    main()
