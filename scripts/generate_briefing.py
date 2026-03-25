"""
Daily Intelligence Briefing — reads data/snapshot.json, calls Claude Sonnet API,
writes briefing as the first entry in data/events.json.

Usage: python scripts/generate_briefing.py --out-dir data
Requires: ANTHROPIC_API_KEY env var
"""
from __future__ import print_function
import argparse
import json
import os
from datetime import datetime, timezone

import anthropic


# ---------------------------------------------------------------------------
# Context builder — extract the most signal-rich data from snapshot
# ---------------------------------------------------------------------------

def build_context(snapshot):
    lines = []
    built_at = snapshot.get("built_at", "unknown")
    lines.append("DATA TIMESTAMP: {}\n".format(built_at))

    # Fear & Greed
    fg = snapshot.get("fear_greed") or {}
    if fg:
        lines.append("FEAR & GREED INDEX: {}/100 — {}".format(
            fg.get("score", "?"), fg.get("sentiment", "?")))
        for k, v in (fg.get("components") or {}).items():
            lines.append("  {}: {:.0f}/100 — {}".format(
                v.get("label", k), v.get("score", 0), v.get("detail", "")))
        lines.append("")

    # FRED Macro
    mf = snapshot.get("macro_fred") or {}
    if mf:
        lines.append("MACRO REGIME: {}".format(
            (mf.get("dominant_signal") or "unknown").upper()))
        lines.append("  {}".format(mf.get("narrative") or ""))
        for sid, sd in (mf.get("series") or {}).items():
            lines.append("  {}: {} [{}]  chg={}".format(
                sd.get("label", sid),
                sd.get("formatted_value", ""),
                sd.get("signal", ""),
                sd.get("change", "")))
        lines.append("")

    groups = snapshot.get("groups") or {}

    # Indices
    idx_rows = groups.get("Indices") or []
    if idx_rows:
        lines.append("INDICES:")
        for r in idx_rows:
            lines.append("  {}: 1d={:+.1f}%  5d={:+.1f}%  20d={:+.1f}%  ytd={:+.1f}%  vol={:.1f}x  rs={:.0f}".format(
                r.get("ticker", "?"),
                r.get("daily") or 0, r.get("5d") or 0,
                r.get("20d") or 0, r.get("ytd") or 0,
                r.get("vol_ratio") or 1.0, r.get("rs") or 50))
        lines.append("")

    # Sel Sectors ranked by 20d
    sel_rows = sorted(groups.get("Sel Sectors") or [], key=lambda r: r.get("20d") or 0, reverse=True)
    if sel_rows:
        lines.append("SELECT SECTORS (by 20d return):")
        for r in sel_rows:
            lines.append("  {}: 1d={:+.1f}%  5d={:+.1f}%  20d={:+.1f}%  rs={:.0f}".format(
                r.get("ticker", "?"),
                r.get("daily") or 0, r.get("5d") or 0,
                r.get("20d") or 0, r.get("rs") or 50))
        lines.append("")

    # The 7s at a Glance
    glance_rows = [r for r in (groups.get("The 7s at a Glance") or []) if not r.get("is_rrg_row")]
    if glance_rows:
        g_sorted = sorted(glance_rows, key=lambda r: r.get("20d") or 0, reverse=True)
        lines.append("THE 7s BASKETS (by 20d return):")
        for r in g_sorted:
            lines.append("  {}: 1d={:+.1f}%  5d={:+.1f}%  20d={:+.1f}%  ytd={:+.1f}%  rs={:.0f}".format(
                r.get("ticker", "?"),
                r.get("daily") or 0, r.get("5d") or 0,
                r.get("20d") or 0, r.get("ytd") or 0,
                r.get("rs") or 50))
        lines.append("")

    # High volume spikes — most actionable signal
    vol_spikes = []
    skip_groups = {"The 7s at a Glance"}
    for gname, rows in groups.items():
        if gname in skip_groups:
            continue
        for r in rows:
            vr = r.get("vol_ratio") or 0
            if vr > 1.8:
                vol_spikes.append((vr, r.get("ticker", ""), gname, r.get("daily") or 0))
    vol_spikes.sort(reverse=True)
    if vol_spikes:
        lines.append("VOLUME SPIKES (>1.8x average):")
        for vr, t, g, d in vol_spikes[:12]:
            lines.append("  {} [{}]: {:.1f}x vol, 1d={:+.1f}%".format(t, g, vr, d))
        lines.append("")

    # Top and bottom movers across all The X 7 groups (individual tickers)
    all_movers = []
    for gname, rows in groups.items():
        if not (gname.startswith("The ") and gname.endswith(" 7")):
            continue
        for r in rows:
            if r.get("20d") is not None:
                all_movers.append((r.get("20d", 0), r.get("ticker", ""), gname))
    if all_movers:
        all_movers.sort(reverse=True)
        leaders = all_movers[:8]
        laggards = all_movers[-8:]
        lines.append("TOP INDIVIDUAL PERFORMERS (20d):")
        for ret, t, g in leaders:
            lines.append("  {} [{}]: {:+.1f}%".format(t, g, ret))
        lines.append("")
        lines.append("WEAKEST INDIVIDUAL PERFORMERS (20d):")
        for ret, t, g in reversed(laggards):
            lines.append("  {} [{}]: {:+.1f}%".format(t, g, ret))
        lines.append("")

    # AI Themes
    themes = sorted(snapshot.get("themes") or [], key=lambda x: x.get("20d") or 0, reverse=True)
    if themes:
        lines.append("AI THEMES (by 20d):")
        for th in themes:
            lines.append("  {}: 1d={:+.1f}%  20d={:+.1f}%  rs={:.0f}".format(
                th.get("name", "?"),
                th.get("daily") or 0, th.get("20d") or 0, th.get("rs") or 50))
        lines.append("")

    # Countries — sort by 20d
    cty_rows = sorted(groups.get("Countries") or [], key=lambda r: r.get("20d") or 0, reverse=True)
    if cty_rows:
        top3 = cty_rows[:4]
        bot3 = cty_rows[-4:]
        lines.append("COUNTRIES — top 4 (20d): " + "  ".join(
            "{} {:+.1f}%".format(r.get("ticker",""), r.get("20d") or 0) for r in top3))
        lines.append("COUNTRIES — bot 4 (20d): " + "  ".join(
            "{} {:+.1f}%".format(r.get("ticker",""), r.get("20d") or 0) for r in bot3))
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Claude prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "You are a veteran macro/equity trader writing a daily end-of-day intelligence briefing "
    "for a professional audience. Your tone is sharp, direct, and specific — no filler, no "
    "platitudes. Use exact numbers from the data. Call out what is working, what is breaking, "
    "and what to watch. The briefing must have exactly these five labeled sections:\n\n"
    "MARKET PULSE — 2–3 sentences on the overall tape (cite SPY/QQQ/IWM returns, F&G score, regime)\n"
    "MACRO BACKDROP — Fed stance, CPI/PCE trajectory, yield curve, key FRED signals\n"
    "LEADERS & LAGGARDS — which groups/sectors/names are leading vs dragging; cite specific %s\n"
    "RISK SIGNALS — vol spikes, credit conditions, safe-haven flows, rotation tells\n"
    "WHAT TO WATCH — 3–5 specific tickers, levels, or catalysts to monitor tomorrow\n\n"
    "Total length: under 420 words. Write in plain text with the section labels in ALL CAPS "
    "followed by an em-dash. No markdown, no bullet symbols beyond hyphens."
)


def generate_briefing(snapshot, api_key):
    context = build_context(snapshot)
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

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print("No ANTHROPIC_API_KEY set — skipping briefing generation")
        return

    snapshot_path = os.path.join(args.out_dir, "snapshot.json")
    events_path = os.path.join(args.out_dir, "events.json")

    if not os.path.exists(snapshot_path):
        print("Snapshot not found at {} — skipping briefing".format(snapshot_path))
        return

    with open(snapshot_path, encoding="utf-8") as f:
        snapshot = json.load(f)

    print("Generating intelligence briefing via Claude API...")
    try:
        text = generate_briefing(snapshot, api_key)
    except Exception as e:
        print("Briefing generation failed: {}".format(e))
        return

    # Load existing events.json (preserve calendar entries, drop old briefing)
    events = []
    if os.path.exists(events_path):
        try:
            with open(events_path, encoding="utf-8") as f:
                events = json.load(f)
        except Exception:
            events = []

    events = [e for e in events if e.get("type") != "briefing"]

    now = datetime.now(timezone.utc)
    briefing_entry = {
        "type": "briefing",
        "date": now.strftime("%d/%m/%Y"),
        "time": now.strftime("%H:%M"),
        "event": "Daily Intelligence Briefing",
        "text": text,
    }
    events.insert(0, briefing_entry)

    with open(events_path, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2)

    print("Briefing written to {}".format(events_path))
    print("--- PREVIEW (first 300 chars) ---")
    print(text[:300])


if __name__ == "__main__":
    main()
