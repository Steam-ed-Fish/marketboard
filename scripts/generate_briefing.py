"""
Daily Intelligence Briefing — reads data/snapshot.json, fetches a live-grounded
market-context summary (Perplexity if its key works, else DeepSeek's native web
search via the Responses API), then composes the briefing with DeepSeek v4-flash
(chat completions, thinking disabled). Writes result into data/events.json.

Usage: python scripts/generate_briefing.py --out-dir data
Requires: DEEPSEEK_API_KEY env var (generation + web-search grounding)
Optional: PERPLEXITY_API_KEY (preferred grounding when live), TAVILY news.json
"""
from __future__ import print_function
import argparse
import json
import os
import time
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
# DeepSeek web search — live-grounded market context (Perplexity replacement)
# ---------------------------------------------------------------------------

import re as _re

_MD_LINK_RE = _re.compile(r"\[([^\]]+)\]\((https?://[^)]+)\)")


def _strip_md_links(text):
    """Convert '[AP](https://...)' -> 'AP' so the context is clean plain text for
    the Feishu push (the composer is instructed no markdown / no citations)."""
    return _MD_LINK_RE.sub(r"\1", text or "").strip()


def fetch_deepseek_search_context(api_key, snapshot=None, timeout=180):
    """Use DeepSeek v4-flash's NATIVE web search (Responses API, server-side
    web_search tool) to fetch a live-grounded summary of what drove the latest
    US session. This restores the live-web grounding lost when the Perplexity
    key died — DeepSeek runs the searches itself and returns cited prose.

    Returns clean plain text (markdown links flattened to source names), or None
    on any failure so the build falls back to the Tavily-headlines-only path.
    """
    # Anchor the search to the session the snapshot actually covers (the build
    # runs ~6 AM China = the prior US close), not a vague "today".
    session_date = ""
    if snapshot:
        ba = str(snapshot.get("built_at") or "")
        session_date = ba[:10]  # YYYY-MM-DD
    date_txt = "around {}".format(session_date) if session_date else "in the most recent session"
    prompt = (
        "Summarize what drove US equity markets in the most recent completed trading session "
        "({}). In 3-4 factual sentences: name the sectors/ETFs/stocks that moved and the SPECIFIC "
        "catalyst behind each (earnings, data, Fed, oil, geopolitics). Focus on the WHY behind moves, "
        "not restating index levels. Only state facts you find in search results; if a driver is "
        "unclear, say so rather than guessing. Do not give political opinions."
    ).format(date_txt)
    try:
        resp = requests.post(
            "https://api.deepseek.com/responses",
            headers={"Authorization": "Bearer {}".format(api_key), "Content-Type": "application/json"},
            json={
                "model": "deepseek-v4-flash",
                "input": prompt,
                "tools": [{"type": "web_search"}],
                "tool_choice": "auto",
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "completed":
            print("  [ds-search] non-completed status: {}".format(data.get("status")))
            return None
        # Final answer = concatenated output_text from message items.
        parts, n_search = [], 0
        for o in data.get("output", []):
            if o.get("type") == "web_search_call":
                n_search += 1
            if o.get("type") == "message":
                for c in o.get("content", []):
                    if c.get("type") == "output_text" and c.get("text"):
                        parts.append(c["text"])
        text = _strip_md_links(" ".join(parts).strip())
        if not text:
            print("  [ds-search] no output_text in response")
            return None
        print("  [ds-search] grounded context via {} web searches, {} chars".format(n_search, len(text)))
        return text
    except Exception as e:
        print("  [ds-search] failed ({}) — falling back to Tavily-only grounding".format(str(e)[:140]))
        return None


# ---------------------------------------------------------------------------
# Context builder
# ---------------------------------------------------------------------------

def build_context(snapshot, news_context=None, fedwatch=None, tavily_news=None, breadth=None, polymarket=None, weekly=False, econ_calendar=None):
    lines = []
    built_at = snapshot.get("built_at", "unknown")
    lines.append("DATA TIMESTAMP: {}\n".format(built_at))
    if weekly:
        lines.append("SCOPE: WEEKLY REVIEW — this is the Saturday wrap-up of the full trading week "
                     "(Mon–Fri). Treat WTD (week-to-date) as the primary move; 1d is just Friday's "
                     "session. Recap the WEEK, not a single day.\n")

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
            for ticker, articles in sectors_headlines.items():
                for h in articles[:2]:
                    lines.append("  SECTOR {}: {} — {}".format(
                        ticker, h.get("title", ""), h.get("snippet", "")[:120]))
            lines.append("")

        # TOP MOVERS — the individual stocks we fetched news for (from build_news's
        # pick_top_movers) paired with their EXACT 1d % from the snapshot and their
        # single best real headline (or an explicit "no catalyst"). This is the ONE
        # aligned movers source: it stops the model from (a) naming random tickers
        # as "movers" when the real biggest movers are elsewhere in the data, and
        # (b) confabulating a catalyst — each mover here carries its true headline
        # or a stated absence, so there is nothing to invent.
        if movers_headlines:
            # ticker -> 1d % across every group (movers can be single stocks in
            # theme baskets, not just index/sector rows).
            daily_by_ticker = {}
            for _rows in (snapshot.get("groups") or {}).values():
                for _r in (_rows or []):
                    _t = _r.get("ticker")
                    if _t and _t not in daily_by_ticker and _r.get("daily") is not None:
                        daily_by_ticker[_t] = _r.get("daily")
            # order movers by |1d| so the biggest is first
            ordered = sorted(movers_headlines.keys(),
                             key=lambda t: abs(daily_by_ticker.get(t) or 0), reverse=True)
            lines.append("TOP MOVERS TODAY (biggest 1d movers — these are THE movers to name in "
                         "SIGNALS & WATCH; attribute a catalyst ONLY from the headline shown, and "
                         "if it says '(no catalyst in feed)' give a data-based reason, never invent one):")
            for ticker in ordered:
                d = daily_by_ticker.get(ticker)
                dtxt = "{:+.2f}%".format(d) if d is not None else "n/a"
                arts = movers_headlines.get(ticker) or []
                if arts:
                    h = arts[0]
                    lines.append('  {} {} — "{}" ({})'.format(
                        ticker, dtxt, h.get("title", ""), h.get("source", "")))
                    if h.get("snippet"):
                        lines.append("      {}".format(h.get("snippet", "")[:160]))
                else:
                    lines.append("  {} {} — (no catalyst in feed)".format(ticker, dtxt))
            lines.append("")

    # Fear & Greed
    fg = snapshot.get("fear_greed") or {}
    if fg:
        lines.append("FEAR & GREED: {}/100 — {}".format(fg.get("score", "?"), fg.get("sentiment", "?")))
        vix_detail = (fg.get("components") or {}).get("volatility", {}).get("detail", "")
        if vix_detail:
            lines.append("  VIX: {}".format(vix_detail))
        lines.append("")

    # FRED Macro — only include series with a fresh last_date (within ~3 weeks).
    # Stale macro figures (e.g. CPI from May when today is July) must NOT be fed
    # to the LLM: an undated "CPI YoY 4.27% chg=-0.17" line caused DeepSeek to
    # hallucinate that today's market move was driven by a CPI release that
    # never happened — a no-fabrication violation. If the data is stale or
    # undated, omit the block entirely rather than risk a fabricated narrative.
    from datetime import date as _date, timedelta as _td
    try:
        _today = _date.today()
        _cutoff = _today - _td(days=21)
    except Exception:
        _cutoff = None
    mf = snapshot.get("macro_fred") or {}
    fresh_series = []
    if mf and mf.get("series"):
        for sid, sd in mf.get("series", {}).items():
            ld = sd.get("last_date")
            try:
                ld_date = _date.fromisoformat(str(ld)[:10]) if ld else None
            except Exception:
                ld_date = None
            if ld_date is None or _cutoff is None:
                continue  # undated → skip (can't verify freshness)
            if ld_date >= _cutoff:
                fresh_series.append((sid, sd, ld_date))
    if fresh_series:
        lines.append("MACRO: {}".format((mf.get("dominant_signal") or "neutral").upper()))
        for sid, sd, ld_date in fresh_series:
            val_str = "{:.2f}{}".format(sd["value"], sd.get("unit", "")) if sd.get("value") is not None else "N/A"
            # Stamp the release date so the LLM knows this is a prior release, NOT today's print.
            lines.append("  {}: {} [{}] chg={} (release {})".format(
                sd.get("label", sid), val_str, sd.get("signal", ""), sd.get("change", ""), ld_date.isoformat()))
        lines.append("")
    else:
        lines.append("MACRO: no fresh macro data available (stale or undated FRED series omitted)")
        lines.append("")

    # Fed / rate-path calendar (from FedWatch) — dated, so the LLM cites facts
    # instead of inventing an FOMC. build_context previously ignored `fedwatch`,
    # which let the model hallucinate "Fed held rates on <date>" with no source.
    fw = (fedwatch or {}).get("market") or {}
    if fw:
        rate = str(fw.get("current_rate") or "")
        for bad in ("–", "—", "�", "\x96", "�C"):
            rate = rate.replace(bad, "-")
        rate = "".join(ch for ch in rate if ch.isprintable())
        lines.append("FED / RATES (verified calendar — do NOT contradict or invent around this):")
        if rate:
            lines.append("  Current target range: {} — a STANDING level set at the LAST FOMC, not a this-week event.".format(rate))
        if fw.get("next_fomc_date"):
            src = fw.get("rate_prob_source") or ""
            reliable = src.startswith("CME FedWatch")  # investing.com CME table, not the ZQ approx
            odds = ""
            if reliable and fw.get("rate_hold_pct") is not None:
                odds = " Market-implied odds (CME FedWatch): hold {}%, cut {}%, hike {}%.".format(
                    fw.get("rate_hold_pct"), fw.get("rate_cut_pct"), fw.get("rate_hike_pct"))
            else:
                odds = " Rate-path odds unavailable — defer to NEWS HEADLINES; do not state a specific probability."
            lines.append("  NEXT FOMC decision: {} — UPCOMING/future unless that date is on or before the DATA "
                         "TIMESTAMP above.{}".format(fw.get("next_fomc_label") or fw.get("next_fomc_date"), odds))
        if fw.get("cpi") is not None:
            lines.append("  Last CPI: {}% ({}, released {}).".format(
                fw.get("cpi"), fw.get("cpi_month"), fw.get("bls_cpi_updated")))
        if fw.get("unemployment") is not None:
            lines.append("  Last unemployment: {}% ({}).".format(
                fw.get("unemployment"), fw.get("unemployment_month")))
        lines.append("  RULE: Do NOT say the Fed met / held / cut / raised, or cite an FOMC as having happened, "
                     "unless the NEXT FOMC date above is on/before the data timestamp. Otherwise the week had NO Fed decision.")
        lines.append("")

    # Economic calendar (futu-cli) — released prints w/ actuals + upcoming w/
    # forecast. This is the ONLY calendar source for the briefing; cite releases
    # from here, not from memory. Sorted: released (most recent first), then
    # upcoming (soonest first). Star 3 = high-impact (CPI/PPI/NFP/Fed/claims).
    econ = econ_calendar or []
    released = [e for e in econ if e.get("released") and e.get("star", 0) >= 3]
    upcoming = [e for e in econ if not e.get("released") and e.get("star", 0) >= 3]
    if released or upcoming:
        lines.append("ECONOMIC CALENDAR (verified — futu-sourced; cite releases from here only, never invent a date/print):")
        for e in released[-12:]:  # last ~12 high-impact released prints
            line = "  RELEASED {}: {} — actual {} (fc {}, prev {})".format(
                e.get("date", ""), (e.get("event") or "")[:70],
                e.get("actual") or "-", e.get("forecast") or "-", e.get("previous") or "-")
            lines.append(line)
        for e in upcoming[:10]:   # next ~10 high-impact upcoming
            line = "  UPCOMING {}: {} — fc {} (prev {})".format(
                e.get("date", ""), (e.get("event") or "")[:70],
                e.get("forecast") or "-", e.get("previous") or "-")
            lines.append(line)
        lines.append("  RULE: Released events already happened (use the ACTUAL value). Upcoming are forward-looking "
                     "(use the FORECAST, or state uncertainty if blank). A release with no actual value listed did NOT print.")
        lines.append("")

    # Prediction-market rate-path odds (Polymarket "Fed Path"). The CME FedWatch feed is
    # often unavailable, so these live market-implied odds are the fallback source for the
    # rate beat. Each is a real, tradable market — cite it as "Polymarket", with the 5-day move.
    pm_fed = (polymarket or {}).get("categories", {}).get("Fed Path") or []
    pm_rows = [m for m in pm_fed if isinstance(m, dict) and m.get("yes_prob") is not None]
    if pm_rows:
        lines.append("PREDICTION-MARKET RATE ODDS (Polymarket 'Fed Path' — live market-implied, cite as 'Polymarket'):")
        for m in pm_rows:
            d5 = m.get("delta_5d")
            d5s = "  (5d {:+.0f}pp)".format(d5) if isinstance(d5, (int, float)) else ""
            lines.append("  {}: {:.0f}% YES{}  [ends {}, 24h vol ${:,.0f}]".format(
                (m.get("question") or "").rstrip("?"), m.get("yes_prob") or 0, d5s,
                m.get("end_date") or "?", m.get("volume24hr") or 0))
        lines.append("")

    groups = snapshot.get("groups") or {}

    # Indices — ranked by WTD (weekly) or daily
    _pk = "wtd" if weekly else "daily"
    idx_rows = sorted(groups.get("Indices") or [], key=lambda r: r.get(_pk) or 0, reverse=True)
    idx_lookup = {r.get("ticker"): r for r in idx_rows}
    if idx_rows:
        lines.append("INDICES (ranked by {}):".format("WTD" if weekly else "1d"))
        for r in idx_rows:
            if weekly:
                lines.append("  {}: WTD={:+.2f}%  1d={:+.2f}%  20d={:+.2f}%  ytd={:+.2f}%".format(
                    r.get("ticker", "?"), r.get("wtd") or 0, r.get("daily") or 0,
                    r.get("20d") or 0, r.get("ytd") or 0))
            else:
                lines.append("  {}: 1d={:+.2f}%  5d={:+.2f}%  20d={:+.2f}%  ytd={:+.2f}%".format(
                    r.get("ticker", "?"),
                    r.get("daily") or 0, r.get("5d") or 0,
                    r.get("20d") or 0, r.get("ytd") or 0))
        # Style divergence — small vs large cap (WTD if weekly, else daily)
        small = max((idx_lookup.get("IWM") or {}).get(_pk) or 0,
                    (idx_lookup.get("IJR") or {}).get(_pk) or 0)
        large = max((idx_lookup.get("QQQ") or {}).get(_pk) or 0,
                    (idx_lookup.get("SPY") or {}).get(_pk) or 0)
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

    # Sel Sectors — ranked by WTD (weekly) or daily
    sel_rows = sorted(groups.get("Sel Sectors") or [], key=lambda r: r.get(_pk) or 0, reverse=True)
    if sel_rows:
        lines.append("SECTORS (ranked by {}):".format("WTD" if weekly else "1d"))
        for r in sel_rows:
            if weekly:
                lines.append("  {}: WTD={:+.2f}%  1d={:+.2f}%  20d={:+.2f}%".format(
                    r.get("ticker", "?"), r.get("wtd") or 0,
                    r.get("daily") or 0, r.get("20d") or 0))
            else:
                lines.append("  {}: 1d={:+.2f}%  5d={:+.2f}%  20d={:+.2f}%".format(
                    r.get("ticker", "?"), r.get("daily") or 0,
                    r.get("5d") or 0, r.get("20d") or 0))
        lines.append("")

    # Top/bottom Industries ETFs by WTD (weekly) or daily
    _wlabel = "this week" if weekly else "today"
    ind_rows = sorted(groups.get("Industries") or [], key=lambda r: r.get(_pk) or 0, reverse=True)
    if ind_rows:
        top3 = ind_rows[:4]
        bot3 = ind_rows[-4:]
        lines.append("INDUSTRIES — top 4 {}: ".format(_wlabel) + "  ".join(
            "{} {:+.2f}%".format(r.get("ticker",""), r.get(_pk) or 0) for r in top3))
        lines.append("INDUSTRIES — bot 4 {}: ".format(_wlabel) + "  ".join(
            "{} {:+.2f}%".format(r.get("ticker",""), r.get(_pk) or 0) for r in bot3))
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

    # S&P 500 participation breadth (from breadth.json — distinct from the volume-ratio
    # "VOLUME BREADTH" block above). Tells the model if the move was broad or narrow.
    if breadth:
        part = breadth.get("participation") or {}
        sig = breadth.get("signals") or {}
        if part:
            lines.append("BREADTH (S&P 500, {} names):".format(breadth.get("universe_size", "?")))
            lines.append("  adv/dec: {}/{} ({:.0f}% up)  %>50DMA: {:.0f}  %>200DMA: {:.0f}".format(
                part.get("adv", 0), part.get("dec", 0), part.get("pct_up") or 0,
                part.get("pct_above_sma50") or 0, part.get("pct_above_sma200") or 0))
            lines.append("  up/down vol ratio: {:.2f}  new 60d highs/lows: {}/{}  narrow rally: {}".format(
                part.get("uvol_dvol_ratio") or 0, sig.get("new_60d_highs", "?"),
                sig.get("new_60d_lows", "?"), sig.get("narrow_rally")))
            lines.append("")

    # Sector / ETF net flows (5-day, $mm). +in / -out, with 52w percentile. Split the
    # 11 GICS sector SPDRs (what "sector flows" means) from broad-index/thematic ETFs so
    # the huge SPY/QQQ index flows don't crowd out the sector-rotation signal.
    SECTOR_ETFS = {"XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY"}
    flow_tickers = (snapshot.get("etf_flow") or {}).get("tickers") or {}
    ranked = sorted(
        ((t, d.get("flow_5d"), d.get("flow_52w_pct"))
         for t, d in flow_tickers.items() if d.get("flow_5d") is not None),
        key=lambda x: x[1], reverse=True)

    def _flow_line(rows):
        return "  ".join("{} {:+.0f} ({:.0f}p)".format(t, f, p or 0) for t, f, p in rows)

    sector_ranked = [r for r in ranked if r[0] in SECTOR_ETFS]
    other_ranked = [r for r in ranked if r[0] not in SECTOR_ETFS]
    if sector_ranked:
        lines.append("SECTOR FLOWS (11 GICS SPDRs, 5d net $mm; +in/-out, 52w pctile):")
        lines.append("  inflows:  " + _flow_line(sector_ranked[:3]))
        outs = [r for r in reversed(sector_ranked[-3:]) if r[1] < 0]
        if outs:
            lines.append("  outflows: " + _flow_line(outs))
        lines.append("")
    if other_ranked:
        outs = [r for r in reversed(other_ranked[-3:]) if r[1] < 0]
        lines.append("BROAD/THEMATIC FLOWS (index & industry ETFs):")
        lines.append("  inflows:  " + _flow_line(other_ranked[:3]))
        if outs:
            lines.append("  outflows: " + _flow_line(outs))
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 晨会素材 SPEECH BRIEFING — pure-data element pack (no LLM, no fabrication)
# ---------------------------------------------------------------------------
# Pulls nearly everything from the already-built snapshot (groups/themes/
# cross_asset/vol_signals/mega_caps/etf_flow). Only newly-fetches what the
# snapshot lacks: US10Y/US30Y yields (US10Y.BD/US30Y.BD), BTC (BTC.CC), IBIT
# (IBIT.US) — all via the same futu-cli gateway as the main fetch. Appended to
# the LLM briefing text as a final section so the morning speech has every
# element pre-organized (the user composes the wording).

_SECTOR_NAMES = {
    "XLB": "材料", "XLC": "通讯", "XLE": "能源", "XLF": "金融", "XLI": "工业",
    "XLK": "科技", "XLP": "必需消费", "XLRE": "地产", "XLU": "公用", "XLV": "医疗",
    "XLY": "可选消费",
}


def _futu_snapshot(codes):
    """Fetch latest + prior close for a few extra futu codes (yields/BTC/IBIT)
    the snapshot doesn't already carry. Returns {code: {last, prev, chg_pct}}.
    NOTE: BatchSimpleKline rejects mixed-asset-type batches (yields .BD, equities
    .US, crypto .CC can't share a call) → fetch each code individually."""
    out = {}
    try:
        import fetch_futucli
        for c in codes:
            res = fetch_futucli._fetch_klines([c], item_count=3, exright_type=0)
            bars = (res.get(c) or {}).get("bars") or []
            if len(bars) >= 2:
                last, prev = bars[-1].get("close"), bars[-2].get("close")
                if last and prev:
                    out[c] = {"last": last, "prev": prev,
                              "chg_pct": round((last / prev - 1) * 100, 2)}
            elif bars:
                out[c] = {"last": bars[-1].get("close"), "prev": None, "chg_pct": None}
    except Exception as e:
        print("[speech] futu extra-fetch failed: {}".format(e))
    return out


def build_speech_briefing(snapshot):
    """晨会素材 — element pack for the morning speech. Pure data, no LLM.
    Mirrors the structure the user uses in their hand-written speeches."""
    lines = ["", "━" * 6 + " 晨会素材 SPEECH BRIEFING " + "━" * 6, ""]
    none_str = "—"

    # ---- extra futu fetches (yields / BTC / IBIT) ----
    extra = _futu_snapshot(["US10Y.BD", "US30Y.BD", "US2Y.BD", "BTC.CC", "IBIT.US"])
    y10 = extra.get("US10Y.BD", {})
    y30 = extra.get("US30Y.BD", {})
    y2 = extra.get("US2Y.BD", {})
    btc = extra.get("BTC.CC", {})
    ibit = extra.get("IBIT.US", {})

    # ---- ① 波动率 VIX/VXN/VIXEQ/SKEW (from vol_signals) ----
    vol = snapshot.get("vol_signals") or {}
    eq = vol.get("Equities") or []
    vmap = {it.get("name"): it for it in eq if isinstance(it, dict)}
    lines.append("① 波动率")
    for nm in ["VIX", "VXN", "VIXEQ", "VVIX"]:
        v = vmap.get(nm) or {}
        cur = v.get("current")
        vs = v.get("vs_ma")
        s = "  • {}: {}{}".format(nm, cur if cur is not None else none_str,
                                  ("  (vs20MA {:+.1f}%)".format(vs)) if vs is not None else "")
        lines.append(s)
    # SKEW — not in vol_signals; pull from the extra snapshot if we fetch it
    skew = _futu_snapshot([".SKEW.US"]).get(".SKEW.US", {})
    if skew:
        chg = skew.get("chg_pct")
        lines.append("  • SKEW: {}{}".format(skew.get("last"),
                     ("  (1d {:+.1f}%)".format(chg)) if chg is not None else ""))
    lines.append("")

    # ---- ② 指数相对强弱 (from groups.Indices) ----
    idx = {r.get("ticker"): r for r in (snapshot.get("groups", {}).get("Indices") or [])
            if isinstance(r, dict) and r.get("daily") is not None}
    order = ["SPY", "QQQ", "IWM", "DIA", "RSP", "IJH", "IJR", "SOXX"]
    idx_rows = sorted([(t, idx[t]) for t in order if t in idx],
                      key=lambda x: x[1].get("daily") or 0, reverse=True)
    lines.append("② 指数相对强弱")
    if idx_rows:
        top = idx_rows[0]
        bot = idx_rows[-1]
        lines.append("  • 最强: {} {:+.2f}%".format(top[0], top[1].get("daily")))
        lines.append("  • 最弱: {} {:+.2f}%".format(bot[0], bot[1].get("daily")))
        spy = idx.get("SPY", {}).get("daily")
        rsp = idx.get("RSP", {}).get("daily")
        if spy is not None and rsp is not None:
            lines.append("  • 大盘vs平权: SPY {:+.2f}% vs RSP {:+.2f}% → {}{:.2f}pp".format(
                spy, rsp, "大盘领先" if spy > rsp else "平权领先", abs(spy - rsp)))
        for t in ["SPY", "QQQ"]:
            r = idx.get(t)
            if r and r.get("vol_ratio") is not None:
                lines.append("  • 成交量 {}: {:.2f}x".format(t, r.get("vol_ratio")))
    lines.append("")

    # ---- ③ Mega Cap (>$1T) 按当日涨跌排序，全列 ----
    themes = snapshot.get("themes") or []
    mc_theme = next((t for t in themes if t.get("name") == "Mega Cap"), None)
    lines.append("③ Mega Cap (>$1T)")
    if mc_theme and mc_theme.get("mega_caps"):
        # daily from constituent_daily; mktcap for display
        cd = mc_theme.get("constituent_daily") or {}
        caps = list(mc_theme.get("mega_caps") or [])
        ranked = sorted(caps, key=lambda m: (cd.get(m["ticker"]) if cd.get(m["ticker"]) is not None else 0), reverse=True)
        up = [m for m in ranked if (cd.get(m["ticker"]) or 0) > 0]
        dn = [m for m in ranked if (cd.get(m["ticker"]) or 0) < 0]
        if up:
            lines.append("  • 涨: " + ", ".join("{} {:+.2f}%".format(m["ticker"], cd[m["ticker"]])
                        for m in up if cd.get(m["ticker"]) is not None))
        if dn:
            lines.append("  • 跌: " + ", ".join("{} {:+.2f}%".format(m["ticker"], cd[m["ticker"]])
                        for m in dn if cd.get(m["ticker"]) is not None))
        lines.append("  • 整体: {}/{}收涨".format(len(up), len(ranked)))
    lines.append("")

    # ---- ④ 利率水位 ----
    # (Mag7 section removed 2026-09-24 — its 7 names are all inside the Mega Cap
    # >$1T basket (③), so it was fully redundant. Mega Cap's 涨/跌 split covers it.)
    lines.append("④ 利率水位")
    if y10.get("last") is not None:
        lines.append("  • US2Y: {:.2f}%  US10Y: {:.2f}%  US30Y: {:.2f}%".format(
            y2.get("last") or 0, y10.get("last") or 0, y30.get("last") or 0))
        spread = (y10.get("last") or 0) - (y2.get("last") or 0)
        lines.append("  • 10y-2y利差: {:+.2f}".format(spread))
        if y30.get("last") and y30.get("last") >= 5.2:
            lines.append("  • 定性: 30y站上5.2% (关键高位)")
        elif y30.get("last"):
            lines.append("  • 定性: 30y {:.2f}%".format(y30.get("last")))
    lines.append("")

    # ---- ⑤ 科技与动量板块 (DRAM/LITE/MU etc) ----
    # Most come from snapshot.Industries; single-name momentum stocks the dashboard
    # doesn't track (LITE/MU/AAOI/FOTO) are fetched fresh via futu so the speech
    # still has them — these are the names the user flags in their morning speech.
    ind = {r.get("ticker"): r for r in (snapshot.get("groups", {}).get("Industries") or [])
           if isinstance(r, dict)}
    # futu-fetch the momentum singles NOT already in the snapshot
    _momentum_singles = ["LITE", "MU", "AAOI", "FOTO"]
    _missing = [t for t in _momentum_singles if t not in ind]
    _sing_extra = _futu_snapshot([t + ".US" for t in _missing]) if _missing else {}
    def _daily_of(t):
        if t in ind and ind[t].get("daily") is not None:
            return ind[t].get("daily")
        e = _sing_extra.get(t + ".US") or {}
        return e.get("chg_pct")  # fallback: fresh 1d change from futu
    lines.append("⑤ 科技与动量")
    for label, tks in [("半导体", ["SOXX", "SMH"]), ("存储", ["DRAM", "MU"]),
                       ("光通信", ["LITE", "AAOI", "FOTO"]), ("软件/AI", ["IGV", "AIQ", "CIBR"])]:
        found = [(t, _daily_of(t)) for t in tks if _daily_of(t) is not None]
        if found:
            lines.append("  • {}: ".format(label) + ", ".join("{} {:+.2f}%".format(t, d)
                        for t, d in found))
    lines.append("")

    # ---- ⑥ 板块主题群 (Sel Sectors 按涨跌聚类) ----
    secs = [(r.get("ticker"), r) for r in (snapshot.get("groups", {}).get("Sel Sectors") or [])
            if isinstance(r, dict) and r.get("daily") is not None]
    secs.sort(key=lambda x: x[1].get("daily") or 0, reverse=True)
    lines.append("⑥ 板块主题群")
    if secs:
        lines.append("  • 领涨: " + ", ".join("{}({}) {:+.2f}%".format(t, _SECTOR_NAMES.get(t, ""), r.get("daily"))
                    for t, r in secs[:3]))
        lines.append("  • 领跌: " + ", ".join("{}({}) {:+.2f}%".format(t, _SECTOR_NAMES.get(t, ""), r.get("daily"))
                    for t, r in secs[-3:]))
    lines.append("")

    # ---- ⑦ 个股催化 (from news.json movers — but we don't have it here reliably) ----
    # news.json is loaded in main() not passed in; skip movers-with-catalyst here,
    # the LLM briefing already covers it. Keep a stub line.
    lines.append("⑦ 个股催化")
    lines.append("  (见上方简报 SIGNALS & WATCH 区的movers)")
    lines.append("")

    # ---- ⑧ 资金面 (etf_flow creation/redemption = 机构净买卖) ----
    ef = (snapshot.get("etf_flow") or {}).get("tickers") or {}
    flows = sorted([(t, d) for t, d in ef.items() if d.get("flow_5d") is not None],
                   key=lambda x: x[1].get("flow_5d") or 0, reverse=True)
    lines.append("⑧ 资金面 (5日净流入/流出 = 机构净买卖)")
    if flows:
        inflow = [(t, d) for t, d in flows if (d.get("flow_5d") or 0) > 0][:3]
        outflow = [(t, d) for t, d in reversed(flows) if (d.get("flow_5d") or 0) < 0][:3]
        if inflow:
            lines.append("  • 净流入: " + ", ".join("{} {:+.0f}M".format(t, d.get("flow_5d")) for t, d in inflow))
        if outflow:
            lines.append("  • 净流出: " + ", ".join("{} {:+.0f}M".format(t, d.get("flow_5d")) for t, d in outflow))
    lines.append("")

    # ---- ⑩ 金银/BTC/IBIT (cross_asset + extra fetch) ----
    ca = snapshot.get("cross_asset") or {}
    def _ca_chg(t):
        r = ca.get(t) or {}
        d = r.get("daily")
        return d if isinstance(d, (int, float)) else None
    lines.append("⑨ 金银/比特币")
    gld = _ca_chg("GLD")
    slv = _ca_chg("SLV")
    if gld is not None or slv is not None:
        lines.append("  • 金(GLD) {}  银(SLV) {}".format(
            "{:+.2f}%".format(gld) if gld is not None else none_str,
            "{:+.2f}%".format(slv) if slv is not None else none_str))
    if btc.get("last"):
        chg = btc.get("chg_pct")
        lines.append("  • BTC: ${:,.0f}{}".format(btc.get("last"),
                     "  (1d {:+.2f}%)".format(chg) if chg is not None else ""))
    if ibit.get("last"):
        chg = ibit.get("chg_pct")
        lines.append("  • IBIT: ${:.2f}{}".format(ibit.get("last"),
                     "  (1d {:+.2f}%)".format(chg) if chg is not None else ""))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Claude prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "You are writing a daily end-of-day market recap for a professional trader. "
    "Tone: direct, specific, zero filler. Every sentence must cite a number or a name.\n\n"
    "Write exactly seven sections in this order:\n\n"
    "BOTTOM LINE — Open with one punchy sentence (max 10 words) capturing the tape character "
    "(e.g. 'Risk-on, small caps led, energy faded.'). "
    "Then 2-3 sentences synthesizing the entire day into one takeaway — "
    "weave together price action, positioning, flows, and catalysts into one coherent conclusion. "
    "This is your verdict on what today meant, not a summary of the sections below.\n\n"
    "INDICES — VIX level and direction. Rank SPY/QQQ/IWM/DIA/^SOX by today's return best to worst with exact %s. "
    "State small vs large cap outcome using the STYLE line (e.g. 'Small > large by 0.95pp'). "
    "Name the most notable cross-asset move (bonds, dollar, gold, oil) with exact %. "
    "If SPY or QQQ is below the 200-day MA, flag it — this is a CTA sell trigger.\n\n"
    "VOLUME & BREADTH — From VOLUME BREADTH data: which indices or sectors are seeing elevated volume (gaining attention) "
    "vs declining volume (being ignored). ALWAYS mention SPY and QQQ vol_ratio explicitly. "
    "Interpret direction: vol up + price up = accumulation, "
    "vol up + price down = distribution, vol down + price up = low-conviction rally. "
    "Then ONE breadth sentence from the BREADTH block: % of S&P 500 above the 50/200-DMA, advance/decline, "
    "up/down volume ratio, and new-60d-highs vs lows — state whether participation confirms or diverges from the index move "
    "(narrow mega-cap tape vs broad). 3-4 sentences total.\n\n"
    "SECTORS & ROTATION — Open with the sector ranking line. "
    "MANDATORY format — ticker then name then %, all 11 sectors best to worst: "
    "'XLY Discretionary +2.36%, XLI Industrials +1.87%, XLK Technology +1.53%, "
    "XLRE Real Estate +1.53%, XLV Healthcare +1.49%, XLP Staples +1.26%, "
    "XLF Financials +0.77%, XLB Materials +0.25%, XLC Comms +0.23%, "
    "XLU Utilities -0.41%, XLE Energy -2.76%'. "
    "Then one sentence naming top 2 and bottom 2 Industries ETFs with %s. "
    "Then ONE flows sentence from the SECTOR FLOWS block (the 11 GICS SPDRs): name the 1-2 sectors with the biggest 5-day net INflows "
    "and the 1-2 with the biggest OUTflows (with the $mm figure and 52w percentile), and say whether flows confirm or "
    "contradict the price ranking (e.g. leaders also seeing inflows = real rotation; leaders bleeding flows = short-covering, not conviction). "
    "Then 1-2 sentences making the explicit money-in / money-out rotation call — "
    "name the 1-2 strongest groups and 1-2 weakest, and end with one rotation-state label chosen from: "
    "'AI hardware leading', 'AI hardware fading / high-to-low rotation', 'software catching up', "
    "'broad risk-on', 'defensive risk-off', 'narrow mega-cap tape', 'broadening participation', "
    "'rangebound chop'.\n\n"
    "POSITIONING — Options market read from OPTIONS INTEL data. "
    "SPY/QQQ put-call ratio: elevated (>1.0) = protection demand, low (<0.7) = complacency. "
    "IV skew: positive = put premium = fear. GEX flip level vs spot — if price is near or below gamma flip, "
    "dealer hedging amplifies moves. VIX vs its 20-day MA: above = elevated fear, below = complacency. "
    "If MOVE index is notably elevated, mention bond vol stress. "
    "From VOL SIGNALS, add one dispersion read: compare VIXEQ (S&P 500 constituent vol) to VIX (index vol) — "
    "a wide VIXEQ-over-VIX gap = high single-stock dispersion / low implied correlation = a stock-pickers'/rotation tape; "
    "a narrow gap = correlated, macro-driven tape. 3 sentences max. "
    "Skip this section if no options data is available.\n\n"
    "CROSS-ASSET — Name the 1-2 most notable divergences or transmission chains across asset classes today. "
    "Examples: 'Oil +5% dragging airlines -4%', 'Dollar strength pressuring gold and EM', "
    "'Gold falling despite risk-off — unusual'. Connect the WHY. "
    "ALWAYS end with one rates beat: name the NEXT FOMC date, then state the rate-path odds using this source priority — "
    "(1) if FED/RATES shows 'Market-implied odds (CME FedWatch)', cite those hold/cut/hike %s; "
    "(2) ELSE if a PREDICTION-MARKET RATE ODDS (Polymarket) block is present, cite those YES probabilities and attribute them to 'Polymarket' "
    "(e.g. 'Polymarket pricing 54% for a 25bp hike / 45% no change at the Sep 16 FOMC'), and flag the 5-day move if it's a big swing "
    "(e.g. 'hike odds +23pp in 5 days'); "
    "(3) ELSE give the FOMC date with NO probability. Cite ONLY figures shown in those blocks; never invent an odds number. "
    "2-3 sentences.\n\n"
    "SIGNALS & WATCH — Open with Fear & Greed score in one phrase, then name the movers from the TOP MOVERS TODAY "
    "block (use those exact tickers and %s — they are the real biggest movers; do NOT substitute smaller sector/ETF "
    "moves as 'top movers'), plus 20d-momentum extremes (best/worst sector + best/worst basket). "
    "If any vol spike tickers exist, name the top 1-2 with their group and interpret "
    "(accumulation if vol+price up, distribution if vol+price down). "
    "Tie a mover to a news catalyst ONLY from the exact headline shown next to it in TOP MOVERS TODAY. "
    "If a mover shows '(no catalyst in feed)', give ONLY a data-derived reason (its %, a vol spike, gamma proximity) "
    "or state the move with no 'why'. "
    "NEVER invent an analyst/price target, an upgrade or downgrade, a firm name, or an earnings/revenue result — "
    "e.g. do NOT write 'on Piper Sandler $225 target' unless that exact fact appears in the headline shown. "
    "Then close with 2-3 tickers worth closer attention tomorrow — for each: name, today's %, and one specific reason "
    "from the DATA ONLY (vol spike, trend divergence, cross-asset signal, momentum break, gamma flip proximity) — "
    "never an invented news event. "
    "The 'watch' picks should flag what deserves a closer look, not just repeat the biggest movers already named.\n\n"
    "CRITICAL: Total output MUST be under 450 words — count words and self-edit if over. "
    "Each section: 1-2 sentences max where unspecified. SECTORS & ROTATION caps at 5 sentences after the sector ranking line; SIGNALS & WATCH caps at 5 sentences total. "
    "Do not repeat numbers across sections — if a ticker or metric appears in INDICES, do not restate it later in SECTORS & ROTATION or SIGNALS & WATCH. "
    "Do not self-correct mid-sentence (e.g. 'X? No, Y'); decide first, then write the final answer. "
    "If a flag is false, do not invent a contradicting hedge. "
    "ECONOMIC CALENDAR IS PROVIDED: the ECONOMIC CALENDAR block lists verified released prints (with ACTUAL values) "
    "and upcoming releases (with FORECASTS) — use it. You MAY reference an upcoming release as a catalyst (e.g. "
    "'into tomorrow's jobless claims') ONLY if that exact event+date appears in the block. Released prints: cite the "
    "ACTUAL value and you may note beat/miss vs the forecast shown. If a release is not in the block, do NOT invent "
    "it, do NOT guess a date — frame forward-looking notes around the DATA (positioning, momentum, vol regime, levels) "
    "instead. This calendar rule supersedes the older 'no calendar' rule. "
    "No redundancy, no citations like [1][2]. Plain text. Section labels ALL CAPS + em-dash. No markdown."
)

WEEKLY_SYSTEM_PROMPT = (
    "You are writing an end-of-WEEK market review for a professional trader — the Saturday wrap-up "
    "of the full trading week (Monday–Friday). Use the week-to-date (WTD) figures as the primary lens; "
    "1d/20d/ytd are secondary context. Tone: direct, specific, zero filler. Every sentence cites a number or a name.\n\n"
    "Write exactly five sections in this order:\n\n"
    "BOTTOM LINE — One punchy sentence (max 12 words) capturing the week's character "
    "(e.g. 'Risk-off week; semis unwound, defensives and real assets bid.'). "
    "Then 2-3 sentences synthesizing the whole WEEK into one takeaway — the arc of the week, "
    "what led and what lagged, and the dominant driver. This is your verdict on the week, not a list.\n\n"
    "INDICES — Rank SPY/QQQ/IWM/DIA by WTD return best to worst with exact %s. State the week's small-vs-large "
    "outcome from the STYLE line. Give VIX level and whether it rose/fell on the week. Name the most notable "
    "cross-asset move of the week (bonds, dollar, gold, oil) with %. Flag any index below its 200-day MA.\n\n"
    "SECTORS & ROTATION — MANDATORY: rank all 11 sectors by WTD, ticker then name then WTD%, best to worst. "
    "Then one sentence on the top 2 and bottom 2 Industries ETFs for the week with %s. "
    "Then 1-2 sentences on the week's money-in / money-out rotation, ending with one rotation-state label: "
    "'AI hardware leading', 'AI hardware fading / high-to-low rotation', 'software catching up', 'broad risk-on', "
    "'defensive risk-off', 'narrow mega-cap tape', 'broadening participation', 'rangebound chop'.\n\n"
    "DRIVERS — Describe the week's OBSERVABLE market drivers from the data: which sectors/assets led and lagged, "
    "the rotation, the vol move, positioning — this is all verifiable and IS 'what happened' (e.g. 'Semis led the "
    "decline, SOXX -4.40%; energy and silver absorbed the flows'). "
    "CATALYST RULE (critical, no-fabrication): only attribute a move to an external news EVENT if a SPECIFIC, "
    "substantive headline for it appears in NEWS HEADLINES. The provided headlines are often generic index/quote-page "
    "titles with no news value — if so, do NOT infer, guess, or invent a catalyst, and do NOT invent a date. When no "
    "real catalyst is available, describe the move mechanically without a 'why'. "
    "FED RULE: do NOT say the Fed met/held/cut/raised or cite an FOMC as having occurred unless the NEXT FOMC date in "
    "FED/RATES is on/before the data timestamp; if future, it's upcoming (put it in WHAT'S NEXT). Never invent a date "
    "or an event. 2-4 sentences.\n\n"
    "WHAT'S NEXT — 2-3 things to watch next week grounded in the data (momentum extremes, gamma/positioning, "
    "vol regime, a group at an inflection). Frame constructively — where the opportunity or risk sets up going "
    "forward, not a victory lap on what already moved. 2-3 sentences.\n\n"
    "CRITICAL: Total output MUST be under 480 words — count and self-edit if over. "
    "Do not repeat a number across sections. Decide first, then write (no 'X? No, Y' self-corrections). "
    "NO FABRICATION: never invent a date, an economic release, an FOMC meeting, an earnings result, or a news "
    "catalyst. Every event you name must be explicitly in the provided data. Numbers come only from the data. "
    "If you cannot source a 'why', state the 'what' (the price/rotation/vol move) without a cause. "
    "No citations like [1][2]. Plain text. Section labels ALL CAPS + em-dash. No markdown."
)


def _try_perplexity(api_key, context, system_prompt=SYSTEM_PROMPT):
    resp = requests.post(
        "https://api.perplexity.ai/chat/completions",
        headers={"Authorization": "Bearer {}".format(api_key), "Content-Type": "application/json"},
        json={
            "model": "sonar-pro",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": context},
            ],
            "max_tokens": 6000,
        },
        timeout=90,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()


def _try_deepseek(api_key, context, system_prompt=SYSTEM_PROMPT):
    # Retry transient failures (429/5xx and the occasional 400 seen during
    # DeepSeek's model migration to v4-flash — the same payload succeeds on retry).
    # DeepSeek is the sole generator whenever Perplexity's key is down, so a single
    # hiccup here otherwise silently kills the whole briefing + Feishu push.
    last_exc = None
    for attempt in range(3):
        try:
            resp = requests.post(
                "https://api.deepseek.com/chat/completions",
                headers={"Authorization": "Bearer {}".format(api_key), "Content-Type": "application/json"},
                json={
                    # deepseek-chat/-reasoner deprecated 2026-07-24 → use v4-flash explicitly.
                    "model": "deepseek-v4-flash",
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": context},
                    ],
                    # DISABLE THINKING. v4-flash became a thinking-heavy hybrid; on the
                    # complex briefing prompt its hidden reasoning runs away UNBOUNDED —
                    # it consumed the entire max_tokens budget as reasoning_tokens and
                    # emitted 0 words (finish_reason=length), killing the 2026-08-04 push.
                    # Raising max_tokens doesn't help (16000 → still 0 words / all reasoning).
                    # A briefing needs no chain-of-thought — grounding comes from the
                    # Tavily/snapshot context, not model reasoning — so turn thinking off.
                    # With reasoning off it finishes cleanly in ~7s at ~310–330 words.
                    "reasoning_effort": "none",
                    "max_tokens": 6000,
                },
                timeout=120,
            )
            if resp.status_code in (400, 429, 500, 502, 503, 504) and attempt < 2:
                print("  [deepseek] {} on attempt {} — retrying (body: {})".format(
                    resp.status_code, attempt + 1, resp.text[:160]))
                time.sleep(3 * (attempt + 1))
                continue
            resp.raise_for_status()
            choice = resp.json()["choices"][0]
            content = (choice.get("message", {}).get("content") or "").strip()
            finish = choice.get("finish_reason")
            # Retry truncated (finish_reason=length) or implausibly short responses —
            # both mean the briefing was cut off mid-section.
            if (finish == "length" or not content or len(content.split()) < 120) and attempt < 2:
                print("  [deepseek] incomplete (finish={}, words={}) on attempt {} — retrying".format(
                    finish, len(content.split()), attempt + 1))
                time.sleep(3 * (attempt + 1))
                continue
            if not content:
                raise RuntimeError("DeepSeek returned empty content after retries")
            if finish == "length":
                raise RuntimeError("DeepSeek response still truncated after retries (finish_reason=length)")
            return content
        except requests.exceptions.RequestException as e:
            last_exc = e
            if attempt < 2:
                print("  [deepseek] request error on attempt {} — retrying: {}".format(attempt + 1, str(e)[:120]))
                time.sleep(3 * (attempt + 1))
            else:
                raise
    if last_exc:
        raise last_exc


def generate_briefing(snapshot, perplexity_key=None, news_context=None, fedwatch=None, tavily_news=None, breadth=None, polymarket=None, deepseek_key=None, weekly=False, econ_calendar=None):
    """Compose the briefing. Prefers Perplexity sonar-pro when its quota is
    available; falls back to DeepSeek (composition-only, no web grounding)
    if Perplexity fails. Tavily headlines in `tavily_news` already carry
    fresh news facts, so losing sonar's live search doesn't leave the
    briefing without ground truth. When weekly=True (Saturday), produces a
    full week-in-review using WTD moves instead of the daily EOD recap.
    `econ_calendar` = futu-sourced calendar entries (released prints +
    upcoming) from events.json — the only permitted calendar source."""
    context = build_context(snapshot, news_context, fedwatch, tavily_news, breadth, polymarket, weekly=weekly, econ_calendar=econ_calendar)
    prompt = WEEKLY_SYSTEM_PROMPT if weekly else SYSTEM_PROMPT
    if perplexity_key:
        try:
            return _try_perplexity(perplexity_key, context, prompt)
        except Exception as e:
            print("Perplexity failed ({}), falling back to DeepSeek...".format(str(e)[:120]))
    if deepseek_key:
        return _try_deepseek(deepseek_key, context, prompt)
    raise RuntimeError("Neither Perplexity nor DeepSeek key available for briefing generation")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="data")
    parser.add_argument("--weekly", action="store_true",
                        help="Force weekly-review mode (overrides the Saturday auto-detect). "
                             "Use when calling a second time on Saturdays to push the weekly "
                             "recap after the daily has already been sent.")
    args = parser.parse_args()

    perplexity_key = os.environ.get("PERPLEXITY_API_KEY", "")
    deepseek_key   = os.environ.get("DEEPSEEK_API_KEY", "")
    if not perplexity_key and not deepseek_key:
        print("Neither PERPLEXITY_API_KEY nor DEEPSEEK_API_KEY set — skipping briefing generation")
        return

    snapshot_path  = os.path.join(args.out_dir, "snapshot.json")
    events_path    = os.path.join(args.out_dir, "events.json")
    fedwatch_path  = os.path.join(args.out_dir, "fedwatch.json")
    breadth_path   = os.path.join(args.out_dir, "breadth.json")

    # Economic-calendar entries (futu-sourced, written by build_data) — fed to
    # the prompt as the ONLY permitted calendar source (see ECONOMIC CALENDAR
    # block in build_context). Cal entries are dicts w/ 'date'; the briefing
    # entry itself is the dict w/ 'type': 'briefing' and is excluded here.
    econ_calendar_entries = []
    if os.path.exists(events_path):
        try:
            with open(events_path, encoding="utf-8") as f:
                econ_calendar_entries = [e for e in (json.load(f) or [])
                                         if isinstance(e, dict) and e.get("date") and not e.get("type")]
        except Exception as e:
            print("events.json unreadable for calendar context ({}) — proceeding without".format(e))
            econ_calendar_entries = []

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

    # Load breadth data if available
    breadth = None
    if os.path.exists(breadth_path):
        try:
            with open(breadth_path, encoding="utf-8") as f:
                breadth = json.load(f)
            print("Loaded breadth data (universe={})".format(breadth.get("universe_size", "?")))
        except Exception as e:
            print("Breadth load failed: {}".format(e))

    # Load Polymarket data if available (prediction-market rate-path odds — the fallback
    # rate-odds source now that CME FedWatch often returns unavailable).
    polymarket = None
    polymarket_path = os.path.join(args.out_dir, "polymarket.json")
    if os.path.exists(polymarket_path):
        try:
            with open(polymarket_path, encoding="utf-8") as f:
                polymarket = json.load(f)
            _fed = (polymarket.get("categories") or {}).get("Fed Path") or []
            print("Loaded Polymarket data ({} Fed Path markets)".format(len(_fed)))
        except Exception as e:
            print("Polymarket load failed: {}".format(e))

    # Fetch live-grounded market context. Prefer Perplexity when its key works;
    # otherwise use DeepSeek's native web search (Responses API) — this restores
    # the live-web grounding that was lost when the Perplexity key died. Either
    # way it's optional: on failure news_context stays None and the briefing is
    # grounded by the Tavily headlines + snapshot data.
    news_context = None
    if perplexity_key:
        print("Fetching market context from Perplexity...")
        news_context = fetch_perplexity_context(perplexity_key)
        if news_context:
            print("  Context: {}".format(news_context[:120]))
    if not news_context and deepseek_key:
        print("Fetching live market context via DeepSeek web search...")
        news_context = fetch_deepseek_search_context(deepseek_key, snapshot)
        if news_context:
            print("  Context: {}".format(news_context[:160]))

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

    # Weekly mode is opt-in via --weekly. The Saturday "send both daily and weekly"
    # behavior is orchestrated by refresh_data.bat (it calls this script twice on
    # Saturdays: once without --weekly for the daily recap, once with --weekly for
    # the week-in-review). Auto-detect on weekday was removed because it made the
    # Saturday daily call silently produce a weekly instead.
    is_weekly = args.weekly
    mode = "WEEKLY review" if is_weekly else "daily briefing"
    print("Generating intelligence {} (Perplexity primary, DeepSeek fallback)...".format(mode))
    t0 = datetime.now(timezone.utc)
    try:
        text = generate_briefing(
            snapshot,
            perplexity_key=perplexity_key or None,
            news_context=news_context,
            fedwatch=fedwatch,
            tavily_news=tavily_news,
            breadth=breadth,
            polymarket=polymarket,
            deepseek_key=deepseek_key or None,
            weekly=is_weekly,
            econ_calendar=econ_calendar_entries,
        )
    except Exception as e:
        print("Briefing generation failed: {}".format(e))
        return
    duration_s = (datetime.now(timezone.utc) - t0).total_seconds()

    # Append 晨会素材 SPEECH BRIEFING (pure-data element pack, no LLM) to the
    # generated briefing text so the morning speech has every element pre-organized.
    try:
        speech = build_speech_briefing(snapshot)
        if speech:
            text = (text or "") + speech
    except Exception as e:
        print("speech-brief append failed: {}".format(e))

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
        "event": "Weekly Market Review" if is_weekly else "Daily Intelligence Briefing",
        "title": "Weekly Market Review" if is_weekly else "Daily Market Briefing",
        "text": text,
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_s": round(duration_s, 1),
    })

    with open(events_path, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2)

    print("Briefing written to {}".format(events_path))
    print("--- PREVIEW ---")
    print(text[:400])


if __name__ == "__main__":
    main()
