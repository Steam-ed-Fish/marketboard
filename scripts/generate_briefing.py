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

def build_context(snapshot, news_context=None, fedwatch=None, tavily_news=None):
    lines = []
    built_at = snapshot.get("built_at", "unknown")
    lines.append("DATA TIMESTAMP: {}\n".format(built_at))

    # News context from Perplexity (market-focused, 3 sentences)
    if news_context:
        lines.append("TODAY'S MARKET CONTEXT (use to explain WHY sectors moved, keep brief):")
        lines.append("  {}".format(news_context))
        lines.append("")

    # Tavily news headlines (grounded search results)
    if tavily_news:
        market_headlines = tavily_news.get("market") or []
        movers_headlines = tavily_news.get("movers") or {}
        sectors_headlines = tavily_news.get("sectors") or {}
        if market_headlines or movers_headlines or sectors_headlines:
            lines.append("NEWS HEADLINES (use to explain movers and add context):")
            for h in market_headlines[:5]:
                lines.append("  MARKET: {} — {} ({})".format(
                    h.get("title", ""), h.get("snippet", "")[:150], h.get("source", "")))
            for ticker, articles in movers_headlines.items():
                for h in articles[:2]:
                    lines.append("  MOVER {}: {} — {}".format(
                        ticker, h.get("title", ""), h.get("snippet", "")[:120]))
            for ticker, articles in sectors_headlines.items():
                for h in articles[:2]:
                    lines.append("  SECTOR {}: {} — {}".format(
                        ticker, h.get("title", ""), h.get("snippet", "")[:120]))
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

    # FedWatch — FOMC, rate probabilities, hawk/dove balance
    if fedwatch:
        mkt = fedwatch.get("market") or {}
        fomc_label = mkt.get("next_fomc_label", "?")
        fomc_date = mkt.get("next_fomc_date", "")
        rate = mkt.get("current_rate", "?")
        hold = mkt.get("rate_hold_pct")
        cut = mkt.get("rate_cut_pct")
        hike = mkt.get("rate_hike_pct")
        cpi = mkt.get("cpi")
        cpi_month = mkt.get("cpi_month", "")
        unemp = mkt.get("unemployment")
        unemp_month = mkt.get("unemployment_month", "")

        lines.append("FED POLICY:")
        lines.append("  FFR target: {}".format(rate))
        lines.append("  Next FOMC: {}".format(fomc_label))
        if hold is not None:
            lines.append("  CME probabilities: hold={:.0f}% cut={:.0f}% hike={:.0f}%".format(
                hold or 0, cut or 0, hike or 0))
        if cpi is not None:
            lines.append("  CPI YoY: {:.1f}% ({})".format(cpi, cpi_month))
        if unemp is not None:
            lines.append("  Unemployment: {:.1f}% ({})".format(unemp, unemp_month))

        # Hawk/dove tally
        stances = {}
        for m in fedwatch.get("members") or []:
            s = m.get("stance", "Neutral")
            stances.setdefault(s, []).append(m["name"].split()[-1])
        if stances:
            parts = []
            for label in ["Hawk", "Neutral", "Dove"]:
                names = stances.get(label, [])
                if names:
                    parts.append("{}({})={}".format(label, len(names), ",".join(names)))
            lines.append("  Stance tally: {}".format("  ".join(parts)))
        lines.append("")

    # USD Liquidity Stress
    liq = (snapshot.get("macro") or {}).get("usd_liquidity") or {}
    if liq.get("components"):
        score = liq.get("score", "?")
        label = liq.get("score_label", "?")
        lines.append("USD LIQUIDITY STRESS: {}/100 ({})".format(score, label))
        for c in liq["components"]:
            lines.append("  {}: {:.2f}{} ({}th percentile, weight={:.0f}%)".format(
                c.get("label", c.get("id", "?")),
                c.get("value") or 0,
                c.get("unit", ""),
                c.get("percentile", "?"),
                (c.get("weight") or 0) * 100))
        raw = liq.get("raw") or {}
        if raw.get("tedrate") is not None:
            lines.append("  TED Spread: {:.2f}%".format(raw["tedrate"]))
        if raw.get("rrp") is not None:
            lines.append("  ON-RRP: ${:.1f}B".format(raw["rrp"]))
        if raw.get("tga") is not None:
            lines.append("  TGA: ${:.0f}M (chg={})".format(raw["tga"], raw.get("tga_chg", "?")))
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
        # Style divergence — small vs large cap daily
        small = max((idx_lookup.get("IWM") or {}).get("daily") or 0,
                    (idx_lookup.get("IJR") or {}).get("daily") or 0)
        large = max((idx_lookup.get("QQQ") or {}).get("daily") or 0,
                    (idx_lookup.get("SPY") or {}).get("daily") or 0)
        style_spread = small - large
        style_label = "SMALL > LARGE" if style_spread > 0 else "LARGE > SMALL"
        lines.append("  STYLE: {} by {:.2f}pp (small={:+.2f}% large={:+.2f}%)".format(
            style_label, abs(style_spread), small, large))
        # 200-day MA positioning
        sma_notes = []
        for t in ["SPY", "QQQ"]:
            row = idx_lookup.get(t) or {}
            if row.get("above_sma200") is False:
                sma_notes.append("{} BELOW 200d MA".format(t))
        if sma_notes:
            lines.append("  SMA200: {}".format(", ".join(sma_notes)))
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

    # Volume spikes — enhanced with group name
    vol_spikes = []
    for gname, rows in groups.items():
        if gname in {"The 7s at a Glance"}:
            continue
        for r in rows:
            vr = r.get("vol_ratio") or 0
            if vr > 2.0:
                vol_spikes.append((vr, r.get("ticker", ""), r.get("daily") or 0, gname))
    vol_spikes.sort(reverse=True)
    if vol_spikes:
        lines.append("VOL SPIKES (>2x avg):")
        for vr, t, d, g in vol_spikes[:8]:
            lines.append("  {}: {:.1f}x  1d={:+.2f}%  [{}]".format(t, vr, d, g))
        lines.append("")

    # Volume breadth — avg vol_ratio per group, top/bottom tickers
    for gname in ["Indices", "Sel Sectors", "Industries"]:
        rows = groups.get(gname) or []
        vr_rows = [(r.get("ticker", "?"), r.get("vol_ratio") or 0, r.get("daily") or 0)
                    for r in rows if r.get("vol_ratio")]
        if not vr_rows:
            continue
        avg_vr = sum(vr for _, vr, _ in vr_rows) / len(vr_rows)
        vr_sorted = sorted(vr_rows, key=lambda x: x[1], reverse=True)
        top3 = vr_sorted[:3]
        bot3 = vr_sorted[-3:]
        if gname == "Indices" and not any(True for _ in lines if "VOLUME BREADTH" in _):
            lines.append("VOLUME BREADTH:")
        lines.append("  {} (avg {:.2f}x):".format(gname, avg_vr))
        lines.append("    gaining: " + "  ".join(
            "{} {:.1f}x".format(t, vr) for t, vr, _ in top3))
        lines.append("    losing:  " + "  ".join(
            "{} {:.1f}x".format(t, vr) for t, vr, _ in bot3))
    lines.append("")

    # Options intel
    opts = snapshot.get("options_intel") or {}
    if opts:
        lines.append("OPTIONS INTEL:")
        for ticker in ["SPY", "QQQ", "IWM", "GLD", "SLV"]:
            o = opts.get(ticker)
            if not o:
                continue
            pcr = o.get("pcr") or {}
            gex = o.get("gex") or {}
            skew = o.get("iv_skew") or {}
            mp = o.get("max_pain") or {}
            lines.append("  {}: ATM_IV={:.1f}% PCR_vol={:.2f} skew={:+.1f}% GEX_flip={} maxpain={}".format(
                ticker,
                o.get("atm_iv") or 0,
                pcr.get("vol") or 0,
                skew.get("skew") or 0,
                gex.get("gamma_flip") or "?",
                mp.get("strike") or "?"))
        lines.append("")

    # Volatility signals
    vol_sigs = snapshot.get("vol_signals") or {}
    if vol_sigs:
        lines.append("VOL SIGNALS:")
        for category in ["Equities", "Rates", "Commodities"]:
            for sig in vol_sigs.get(category) or []:
                lines.append("  {} ({}): {:.1f} (ma20={:.1f}, 52w {:.1f}-{:.1f})".format(
                    sig.get("name", "?"), sig.get("desc", ""),
                    sig.get("current") or 0, sig.get("ma20") or 0,
                    sig.get("lo52") or 0, sig.get("hi52") or 0))
        lines.append("")

    # Factor regime
    factors = snapshot.get("factor_regime") or {}
    if factors:
        lines.append("FACTOR REGIME:")
        for fid, fd in factors.items():
            lines.append("  {} ({}): {} zscore={:+.2f} days={}".format(
                fid, fd.get("name", ""), fd.get("regime", "?"),
                fd.get("zscore") or 0, fd.get("days_in_regime") or 0))
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Claude prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "You are writing a daily end-of-day market recap for a professional trader. "
    "Tone: direct, specific, zero filler. Every sentence must cite a number or a name.\n\n"
    "Write exactly ten sections in this order:\n\n"
    "BOTTOM LINE — Open with one punchy sentence (max 10 words) capturing the tape character "
    "(e.g. 'Risk-on, small caps led, energy faded, breadth expanding.'). "
    "Then 2-3 sentences synthesizing the entire day into one takeaway — "
    "weave together price action, positioning, flows, and catalysts into one coherent conclusion. "
    "This is your verdict on what today meant, not a summary of the sections below.\n\n"
    "INDICES — VIX level and direction. Rank SPY/QQQ/IWM/DIA by today's return best to worst with exact %s. "
    "State small vs large cap outcome using the STYLE line (e.g. 'Small > large by 0.95pp'). "
    "Name the most notable cross-asset move (bonds, dollar, gold, oil) with exact %. "
    "If SPY or QQQ is below the 200-day MA, flag it — this is a CTA sell trigger.\n\n"
    "VOLUME — From VOLUME BREADTH data: which indices or sectors are seeing elevated volume (gaining attention) "
    "vs declining volume (being ignored). Interpret direction: vol up + price up = accumulation, "
    "vol up + price down = distribution, vol down + price up = low-conviction rally. 2-3 sentences.\n\n"
    "SECTORS — Rank all 11 sectors best to worst with exact %s. "
    "MANDATORY format — ticker then name then %: "
    "'XLY Discretionary +2.36%, XLI Industrials +1.87%, XLK Technology +1.53%, "
    "XLRE Real Estate +1.53%, XLV Healthcare +1.49%, XLP Staples +1.26%, "
    "XLF Financials +0.77%, XLB Materials +0.25%, XLC Comms +0.23%, "
    "XLU Utilities -0.41%, XLE Energy -2.76%'. "
    "One sentence naming top 2 and bottom 2 Industries ETFs with %s.\n\n"
    "THE 7s — List only the top 5 and bottom 5 baskets by today's return with exact %s (skip the middle). "
    "Drop 'The' and '7' — write 'Energy +0.08%' not 'The Energy 7 +0.08%'. "
    "Name strongest AND weakest ticker in both the top and bottom basket. "
    "If a basket shows clear internal divergence (e.g. storage split), call it out.\n\n"
    "FED & LIQUIDITY — If Fed policy data is provided: next FOMC date, CME rate probabilities (hold/cut/hike %), "
    "and hawk/dove/neutral count in one sentence. If USD liquidity stress data is provided: composite score, "
    "label, and name the single most-stressed component (highest percentile) with its percentile. "
    "If TED spread or ON-RRP is notable, mention it. Keep to 2-3 sentences max. "
    "Skip this section entirely if no Fed or liquidity data is available.\n\n"
    "POSITIONING — Options market read from OPTIONS INTEL data. "
    "SPY/QQQ put-call ratio: elevated (>1.0) = protection demand, low (<0.7) = complacency. "
    "IV skew: positive = put premium = fear. GEX flip level vs spot — if price is near or below gamma flip, "
    "dealer hedging amplifies moves. VIX vs its 20-day MA: above = elevated fear, below = complacency. "
    "If MOVE index is notably elevated, mention bond vol stress. 2-3 sentences max. "
    "Skip this section if no options data is available.\n\n"
    "CROSS-ASSET — Name the 1-2 most notable divergences or transmission chains across asset classes today. "
    "Examples: 'Oil +5% dragging airlines -4%', 'Dollar strength pressuring gold and EM', "
    "'Gold falling despite risk-off — unusual'. Connect the WHY. 1-2 sentences. "
    "Skip if nothing notable.\n\n"
    "SIGNALS — Fear & Greed score in one phrase. "
    "Top 3 individual movers across all groups today (name, %, group). "
    "20d momentum: best and worst sector + best and worst basket. "
    "If any vol spike tickers exist, name the top 1-2 with their group and interpret "
    "(accumulation if vol+price up, distribution if vol+price down). "
    "If news context provided, one sentence tying the biggest move to its catalyst.\n\n"
    "WATCH — 2-3 tickers worth closer attention tomorrow. For each: name, today's %, and one specific reason "
    "from the data (vol spike, trend divergence, cross-asset signal, momentum break, gamma flip proximity). "
    "Flag what deserves a closer look — don't just repeat what already moved.\n\n"
    "CRITICAL: Total output MUST be under 400 words. Be extremely concise — no redundancy, no citations like [1][2]. "
    "Each section: 1-3 sentences max. Plain text. Section labels ALL CAPS + em-dash. No markdown."
)


def generate_briefing(snapshot, api_key, news_context=None, fedwatch=None, tavily_news=None):
    context = build_context(snapshot, news_context, fedwatch, tavily_news)
    resp = requests.post(
        "https://api.perplexity.ai/chat/completions",
        headers={"Authorization": "Bearer {}".format(api_key), "Content-Type": "application/json"},
        json={
            "model": "sonar-pro",
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": context},
            ],
            "max_tokens": 2000,
        },
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="data")
    args = parser.parse_args()

    perplexity_key = os.environ.get("PERPLEXITY_API_KEY", "")
    if not perplexity_key:
        print("No PERPLEXITY_API_KEY set — skipping briefing generation")
        return

    snapshot_path  = os.path.join(args.out_dir, "snapshot.json")
    events_path    = os.path.join(args.out_dir, "events.json")
    fedwatch_path  = os.path.join(args.out_dir, "fedwatch.json")

    if not os.path.exists(snapshot_path):
        print("Snapshot not found at {} — skipping briefing".format(snapshot_path))
        return

    with open(snapshot_path, encoding="utf-8") as f:
        snapshot = json.load(f)

    # Load FedWatch data if available
    fedwatch = None
    if os.path.exists(fedwatch_path):
        try:
            with open(fedwatch_path, encoding="utf-8") as f:
                fedwatch = json.load(f)
            print("Loaded FedWatch data ({} members)".format(len(fedwatch.get("members", []))))
        except Exception as e:
            print("FedWatch load failed: {}".format(e))

    # Optional: fetch market context from Perplexity
    news_context = None
    if perplexity_key:
        print("Fetching market context from Perplexity...")
        news_context = fetch_perplexity_context(perplexity_key)
        if news_context:
            print("  Context: {}".format(news_context[:120]))

    # Load Tavily news context if available
    tavily_news = None
    news_path = os.path.join(args.out_dir, "news.json")
    if os.path.exists(news_path):
        try:
            with open(news_path, encoding="utf-8") as f:
                tavily_news = json.load(f)
            mkt_count = len(tavily_news.get("market") or [])
            mov_count = len(tavily_news.get("movers") or {})
            print("Loaded Tavily news ({} headlines, {} movers)".format(mkt_count, mov_count))
        except Exception as e:
            print("Tavily news load failed: {}".format(e))

    print("Generating intelligence briefing via Perplexity API...")
    try:
        text = generate_briefing(snapshot, perplexity_key, news_context, fedwatch, tavily_news)
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
