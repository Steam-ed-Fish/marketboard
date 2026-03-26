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
                "{:.2f}{}".format(sd["value"], sd.get("unit", "")) if sd.get("value") is not None else "N/A",
                sd.get("signal", ""),
                sd.get("change", "")))
        lines.append("")

    groups = snapshot.get("groups") or {}

    # Indices
    idx_rows = groups.get("Indices") or []
    idx_lookup = {r.get("ticker"): r for r in idx_rows}
    if idx_rows:
        lines.append("INDICES:")
        for r in idx_rows:
            lines.append("  {}: 1d={:+.2f}%  5d={:+.2f}%  20d={:+.2f}%  ytd={:+.2f}%  vol={:.2f}x  rs={:.0f}".format(
                r.get("ticker", "?"),
                r.get("daily") or 0, r.get("5d") or 0,
                r.get("20d") or 0, r.get("ytd") or 0,
                r.get("vol_ratio") or 1.0, r.get("rs") or 50))
        # Explicit breadth note so Claude doesn't infer wrong direction
        spy_20d = (idx_lookup.get("SPY") or {}).get("20d") or 0
        rsp_20d = (idx_lookup.get("RSP") or {}).get("20d") or 0
        diff = rsp_20d - spy_20d
        direction = "OUTPERFORMING" if diff > 0 else "UNDERPERFORMING"
        lines.append("  NOTE: RSP {:+.2f}% vs SPY {:+.2f}% over 20d — RSP is {} SPY by {:.2f}pp (breadth {})".format(
            rsp_20d, spy_20d, direction, abs(diff),
            "expanding" if diff > 0 else "contracting"))
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
    "You are writing a concise daily market recap for a professional trader. "
    "Be direct and specific — state what happened, rank the movers, cite exact numbers. "
    "No filler phrases, no generic observations. Every sentence must contain a number or a name.\n\n"
    "Structure the briefing with these four sections in order:\n\n"
    "INDICES — VIX level and direction, then SPY/QQQ/IWM/DIA daily returns ranked best to worst. "
    "Note if small caps outperformed large caps or vice versa. One sentence on bond yields if notable.\n\n"
    "SECTORS — Rank ALL 11 select sectors (XLE, XLK, XLC, XLF, XLU, XLY, XLRE, XLP, XLB, XLI, XLV) "
    "by daily return from best to worst. State each ticker and its exact daily %. "
    "Then name the top 3 and bottom 3 Industries ETFs by daily return with exact %s.\n\n"
    "THE 7s — Rank all The X 7 baskets by daily return, best to worst, with exact %s. "
    "Call out the standout performers and worst laggards within the best and worst baskets.\n\n"
    "SIGNALS — Fear & Greed score and what it implies. Any vol spikes (>2x avg volume). "
    "20-day trend: which sectors and baskets have the strongest and weakest 20d momentum. "
    "RSP vs SPY breadth — state the direction explicitly using the NOTE provided in the data.\n\n"
    "Total length: 300–450 words. Plain text, section labels in ALL CAPS followed by em-dash. "
    "No markdown. Use exact numbers from the data — do not round aggressively or invent values."
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
