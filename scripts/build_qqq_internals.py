#!/usr/bin/env python3
"""build_qqq_internals.py — QQQ Structure pane for the Quant Heatmap modal.

Breaks QQQ into (a) the 11 GICS sectors (the XLK/XLC/XLY-style view) and
(b) code-defined cross-sector baskets (MAG-7 / Semis / AI Build / …) that are
allowed to overlap — the dashboard shows them as horizontal bars, not a donut,
precisely because NVDA legitimately sits in MAG-7 *and* Semis *and* AI Build.

Emits data/qqq_internals.json:
  as_of, built_at, qqq{daily,wtd,ytd},
  gics_sectors[ {sector, weight, daily, daily_contrib, members[5]} ],
  baskets[     {name, color, weight, daily, daily_contrib, members[]} ],
  all_members[ {t, name, w, daily, sector, baskets[]} ]

Data sources (no yfinance — it caps QQQ holdings at 10):
  1. Invesco dng-api holdings feed (the issuer's own published full list,
     ~100 names with % of net assets). ONE http call per build, no key.
  2. data/ohlc/*.json (futu-cli cache, via build_spx500_breadth.load_ohlc)
     for per-member daily moves — same pattern build_heatmap uses.
  3. data/snapshot.json for the QQQ header row (daily/wtd/ytd).

Fallback: if dng-api is unreachable, degrade to the top-10 data/holdings/QQQ.json
cache (weights only; partial=True in output) and log loudly. Never crash the
build — refresh_data.bat runs this non-fatal.
"""
import argparse
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from build_spx500_breadth import load_ohlc, sanitize_for_json  # noqa: E402
from build_heatmap import _split_adjust  # noqa: E402

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

DNG_URL = ("https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/QQQ/"
           "holdings/fund?idType=ticker&interval=monthly&productType=ETF")
DNG_REFERER = "https://www.invesco.com/qqq-etf/en/about.html"

# Ticker → GICS sector for the QQQ (Nasdaq-100) constituent set. Hand-maintained:
# Invesco publishes ICB "Technology/Telecommunications" buckets, not GICS, so we
# map per-name to the same GICS sectors the SPDR ETFs (XLK/XLC/XLY/…) track.
# Reflects the 2023 GICS reclassification (PYPL→Financials, WMT/COST→Staples,
# PLTR/SHOP/APP→IT, transaction & payment processing→Financials).
SECTOR_LABELS = {
    "IT":   "Information Technology",
    "CS":   "Communication Services",
    "CD":   "Consumer Discretionary",
    "CSAP": "Consumer Staples",
    "HC":   "Health Care",
    "FIN":  "Financials",
    "IND":  "Industrials",
    "UT":   "Utilities",
    "EN":   "Energy",
    "MAT":  "Materials",
    "RE":   "Real Estate",
}

GICS_SECTOR_MAP = {
    # Information Technology (43)
    "NVDA": "IT", "AAPL": "IT", "MSFT": "IT", "MU": "IT", "AMD": "IT", "INTC": "IT",
    "AVGO": "IT", "CSCO": "IT", "LRCX": "IT", "AMAT": "IT", "PANW": "IT", "SNDK": "IT",
    "CRWD": "IT", "TXN": "IT", "KLAC": "IT", "MRVL": "IT", "QCOM": "IT", "STX": "IT",
    "ADI": "IT", "WDC": "IT", "ASML": "IT", "ARM": "IT", "FTNT": "IT", "APP": "IT",
    "ADBE": "IT", "LITE": "IT", "CDNS": "IT", "DDOG": "IT", "INTU": "IT", "SNPS": "IT",
    "MPWR": "IT", "ALAB": "IT", "TER": "IT", "NXPI": "IT", "NBIS": "IT", "ADSK": "IT",
    "MCHP": "IT", "CRWV": "IT", "TTWO": "IT", "WDAY": "IT", "ROP": "IT", "CPRT": "IT",
    "MSTR": "IT", "PLTR": "IT", "SHOP": "IT",
    # Communication Services (8)
    "META": "CS", "GOOGL": "CS", "GOOG": "CS", "NFLX": "CS", "TMUS": "CS",
    "CMCSA": "CS", "WBD": "CS",
    # Consumer Discretionary (11)
    "AMZN": "CD", "TSLA": "CD", "BKNG": "CD", "SBUX": "CD", "MELI": "CD", "MAR": "CD",
    "ABNB": "CD", "DASH": "CD", "ORLY": "CD", "ROST": "CD", "PDD": "CD",
    # Consumer Staples (7)
    "WMT": "CSAP", "COST": "CSAP", "PEP": "CSAP", "MDLZ": "CSAP", "CCEP": "CSAP",
    "KDP": "CSAP", "MNST": "CSAP",
    # Health Care (9)
    "AMGN": "HC", "GILD": "HC", "ISRG": "HC", "VRTX": "HC", "REGN": "HC", "IDXX": "HC",
    "DXCM": "HC", "ALNY": "HC", "GEHC": "HC",
    # Financials (1)
    "PYPL": "FIN",
    # Industrials (15)
    "HON": "IND", "HONA": "IND", "FER": "IND", "RKLB": "IND", "CTAS": "IND",
    "FAST": "IND", "CSX": "IND", "PCAR": "IND", "ODFL": "IND", "ADP": "IND",
    "PAYX": "IND", "AXON": "IND", "TRI": "IND", "SPCX": "IND",
    # Utilities (4)
    "CEG": "UT", "AEP": "UT", "XEL": "UT", "EXC": "UT",
    # Energy (2)
    "FANG": "EN", "BKR": "EN",
    # Materials (1)
    "LIN": "MAT",
}

# Cross-sector baskets (overlap intentional — NVDA belongs in MAG-7, Semis and
# AI Build simultaneously). Members missing from the current QQQ list are skipped
# silently; a basket whose members all vanish is omitted from the output.
BASKETS = [
    ("MAG-7",           "#a855f7", ["NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "GOOG", "TSLA"]),
    ("Semis",           "#3b82f6", ["NVDA", "MU", "AMD", "AVGO", "INTC", "LRCX", "AMAT", "TXN",
                                    "KLAC", "MRVL", "QCOM", "ADI", "ASML", "ARM", "NXPI", "MPWR",
                                    "ALAB", "TER", "MCHP", "SNDK", "STX", "WDC", "LITE"]),
    ("AI Build",        "#10b981", ["NVDA", "MU", "AMD", "AVGO", "CRWV", "NBIS", "ARM", "MRVL", "ALAB"]),
    ("Software/SaaS",   "#f59e0b", ["MSFT", "PLTR", "CRWD", "PANW", "FTNT", "ADBE", "DDOG",
                                    "INTU", "SNPS", "CDNS", "ADSK", "WDAY", "ROP", "MSTR"]),
    ("Internet/Ad",     "#eab308", ["META", "GOOGL", "GOOG", "NFLX"]),
    ("E-comm & Autos",  "#ef4444", ["AMZN", "TSLA", "PDD", "BKNG", "MELI", "DASH", "ABNB"]),
    ("Mega-defensive",  "#94a3b8", ["WMT", "COST", "PEP", "MDLZ"]),
]

# Non-equity lines in the Invesco feed (cash, collateral, index-future hedges).
_SKIP_TOKENS = ("FUTURE", "CONTRA", "CASH", "COLLATERAL", "USDPDV")


def fetch_dng_holdings():
    """Full QQQ holdings from Invesco dng-api. Returns (list, effective_date) or (None, None)."""
    req = urllib.request.Request(DNG_URL, headers={
        "User-Agent": "Mozilla/5.0 (marketboard build)",
        "Referer": DNG_REFERER,
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            d = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"[qqq-int] dng-api fetch failed: {e}")
        return None, None
    out = []
    for x in d.get("holdings") or []:
        t = x.get("ticker")
        w = x.get("percentageOfTotalNetAssets")
        name = (x.get("issuerName") or "").upper()
        if not t or t in ("USDPDV", "USD", "NQZ6"):
            continue
        if any(tok in name for tok in _SKIP_TOKENS):
            continue
        if not isinstance(w, (int, float)):
            continue
        out.append({"t": t, "name": x.get("issuerName") or t, "w": round(float(w), 4)})
    if not out:
        print("[qqq-int] dng-api returned no usable holdings")
        return None, None
    return out, d.get("effectiveDate")


def load_top10_fallback(out_dir):
    """Degraded source: the top-10 cache build_data.fetch_etf_holdings wrote."""
    p = os.path.join(out_dir, "holdings", "QQQ.json")
    try:
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
    except Exception as e:
        print(f"[qqq-int] top-10 fallback unreadable: {e}")
        return []
    return [{"t": h.get("symbol"), "name": h.get("symbol"), "w": round(100.0 * h["weight"], 4)}
            for h in (d.get("holdings") or [])
            if h.get("symbol") and isinstance(h.get("weight"), (int, float))]


def backfill_ohlc(out_dir, tickers):
    """Fetch 3 newest daily bars via futu-cli for members with no OHLC cache and
    write data/ohlc/<ticker>.json so load_ohlc (this build AND heatmap/breadth)
    picks them up. QQQ members aren't in build_data's STOCK_GROUPS universe, so
    new NDX adds (IPOs, ADRs like ARM/MELI/PDD) never get prefetched — this
    closes that gap locally without rewiring the main pipeline. Returns the set
    of tickers still missing after the attempt (genuinely unavailable)."""
    import re as _re
    from fetch_futucli import _fetch_klines, to_futucli_code

    missing = []
    for t in tickers:
        bars = load_ohlc(out_dir, t)
        if not bars or len(bars) < 2:
            missing.append(t)
    if not missing:
        return set()

    # Only attempt when a gateway token exists — else this degrades every run
    # into a slow guaranteed-fail batch (30s timeout each).
    try:
        import futu_gateway
        if not futu_gateway.get_token():
            print("[qqq-int] no futu gateway token — skipping OHLC backfill")
            return set(missing)
    except Exception as e:
        print(f"[qqq-int] gateway check failed ({e}) — skipping OHLC backfill")
        return set(missing)

    print(f"[qqq-int] backfilling OHLC for {len(missing)} member(s): {', '.join(missing)}")
    codes = {to_futucli_code(t): t for t in missing}
    try:
        res = _fetch_klines(list(codes), item_count=3, exright_type=0, verbose=False)
    except Exception as e:
        print(f"[qqq-int] backfill fetch failed: {e}")
        return set(missing)

    still_missing = set()
    for code, t in codes.items():
        entry = res.get(code)
        bars = (entry or {}).get("bars") or []
        usable = [b for b in bars if b.get("close")]
        if len(usable) < 2:
            still_missing.add(t)
            continue
        ohlc = [{
            "t": f"{b['date']//10000:04d}-{b['date']//100%100:02d}-{b['date']%100:02d}",
            "o": round(b["open"], 4), "h": round(b["high"], 4),
            "l": round(b["low"], 4), "c": round(b["close"], 4),
            "v": b.get("volume", 0),
        } for b in usable if b.get("date")]
        if len(ohlc) < 2:
            still_missing.add(t)
            continue
        safe = _re.sub(r"[^A-Za-z0-9._-]", "_", t)
        p = os.path.join(out_dir, "ohlc", f"{safe}.json")
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump({"ticker": t, "name": t, "ohlc": ohlc}, f,
                      allow_nan=False, ensure_ascii=False)
    if still_missing:
        print(f"[qqq-int] still no OHLC after backfill: {', '.join(sorted(still_missing))}")
    return still_missing


def member_daily(out_dir, ticker):
    """Split-adjusted last-vs-prev close (%) from the futu-cli OHLC cache. The
    cache is unadjusted (exright_type=0), so a raw last/prev across a split
    would show a fake ±50-100% "daily" — back-adjust exactly as build_heatmap
    does for its 52W metrics. None if insufficient bars."""
    bars = load_ohlc(out_dir, ticker)
    if not bars or len(bars) < 2:
        return None
    ab = [b for b in bars if isinstance(b.get("c"), (int, float)) and b.get("c") > 0]
    if len(ab) < 2:
        return None
    closes = [b["c"] for b in ab]
    highs = [b["h"] if isinstance(b.get("h"), (int, float)) and b["h"] > 0 else b["c"] for b in ab]
    adj_c, _ = _split_adjust(closes, highs)
    prev, last = adj_c[-2], adj_c[-1]
    if not prev:
        return None
    return round(100.0 * (last / prev - 1), 2)


def aggregate(members):
    """(weight%, weighted-avg daily%, contribution in percentage-points) for a
    member list. Contribution = Σ(weight% × daily%)/100 → units of pp; for a
    99.8%-covered universe this sums to ≈ QQQ's own daily move. avg is None
    when no member has a usable daily."""
    w_sum = sum(m["w"] for m in members)
    contrib = sum(m["w"] * m["daily"] for m in members if m.get("daily") is not None)
    w_daily = sum(m["w"] for m in members if m.get("daily") is not None)
    avg = round(contrib / w_daily, 2) if w_daily else None
    return round(w_sum, 2), avg, round(contrib / 100, 4)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data")
    args = ap.parse_args()
    out_dir = os.path.abspath(args.out_dir)

    holdings, as_of = fetch_dng_holdings()
    partial = False
    if not holdings:
        print("[qqq-int] falling back to top-10 QQQ.json cache (PARTIAL data)")
        holdings = load_top10_fallback(out_dir)
        partial = True
        as_of = None
    if not holdings:
        print("[qqq-int] no holdings source available — writing nothing")
        return 1

    # Backfill OHLC for members the main pipeline never prefetched (new NDX adds,
    # ADRs). Writes data/ohlc/*.json so heatmap/breadth also benefit. Track what
    # stays missing — surfaced in the UI like the GICS-unmapped banner.
    no_ohlc = sorted(backfill_ohlc(out_dir, [m["t"] for m in holdings]))

    # Enrich daily moves; keep only names we can also sector-map for the GICS view
    for m in holdings:
        m["daily"] = member_daily(out_dir, m["t"])
        m["sector"] = GICS_SECTOR_MAP.get(m["t"])

    by_ticker = {m["t"]: m for m in holdings}

    # QQQ header row from snapshot.json (stop scanning once found)
    qqq_row = {}
    try:
        with open(os.path.join(out_dir, "snapshot.json"), encoding="utf-8") as f:
            for rows in (json.load(f).get("groups") or {}).values():
                if not isinstance(rows, list) or qqq_row:
                    continue
                for r in rows:
                    if r.get("ticker") == "QQQ":
                        qqq_row = r
                        break
    except Exception as e:
        print(f"[qqq-int] snapshot.json unreadable ({e}) — QQQ header blank")

    # Loud guard: names Invesco added that the hand-maintained GICS map doesn't
    # cover. They stay in the table/baskets but silently drop out of the GICS
    # view otherwise — surface them so the map gets updated at the next rebalance.
    unmapped = sorted(m["t"] for m in holdings if not m["sector"])
    if unmapped:
        print(f"[qqq-int] WARNING — {len(unmapped)} holdings missing from GICS_SECTOR_MAP "
              f"(excluded from GICS view): {', '.join(unmapped)}")

    # GICS sector view (partition of the mapped universe; unmapped names excluded)
    sec_members = {}
    for m in holdings:
        if m["sector"]:
            sec_members.setdefault(m["sector"], []).append(m)
    gics_out = []
    for code in sorted(sec_members, key=lambda c: -sum(x["w"] for x in sec_members[c])):
        mem = sorted(sec_members[code], key=lambda x: -x["w"])
        weight, avg, contrib = aggregate(mem)
        gics_out.append({
            "sector": SECTOR_LABELS.get(code, code),
            "weight": weight,
            "daily": avg,
            "daily_contrib": contrib,
            "members": [{"t": x["t"], "w": x["w"], "daily": x["daily"]} for x in mem[:5]],
        })

    # Custom overlapping baskets. Tag order for all_members follows the BASKETS
    # declaration order (deterministic), independent of the weight-sorted output.
    baskets_out = []
    ticker_baskets = {}
    for name, color, tickers in BASKETS:
        mem = [by_ticker[t] for t in tickers if t in by_ticker]
        if not mem:
            continue
        for t in tickers:
            if t in by_ticker:
                ticker_baskets.setdefault(t, []).append(name)
        weight, avg, contrib = aggregate(mem)
        baskets_out.append({
            "name": name,
            "color": color,
            "weight": weight,
            "daily": avg,
            "daily_contrib": contrib,
            "members": [{"t": x["t"], "w": x["w"], "daily": x["daily"]}
                        for x in sorted(mem, key=lambda z: -z["w"])],
        })
    baskets_out.sort(key=lambda b: -b["weight"])

    # Ranked full-member table (every name, incl. GICS-unmapped ones)
    all_members = [{
        "t": m["t"],
        "name": m["name"],
        "w": m["w"],
        "daily": m["daily"],
        "sector": SECTOR_LABELS.get(m["sector"]) if m["sector"] else None,
        "baskets": ticker_baskets.get(m["t"], []),
    } for m in sorted(holdings, key=lambda z: -z["w"])]

    built_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    covered = round(sum(m["w"] for m in holdings), 2)
    payload = {
        "as_of": as_of,
        "built_at": built_at,
        "partial": partial,
        "qqq": {k: qqq_row.get(k) for k in ("daily", "wtd", "ytd")},
        "coverage": covered,
        "gics_sectors": gics_out,
        "baskets": baskets_out,
        "all_members": all_members,
        "unmapped": unmapped,
        "no_ohlc": no_ohlc,
    }
    payload = sanitize_for_json(payload)

    out_path = os.path.join(out_dir, "qqq_internals.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, allow_nan=False, ensure_ascii=False, separators=(",", ":"))

    print(f"[qqq-int] wrote {out_path}: as_of={as_of or '?'} partial={partial} "
          f"names={len(holdings)} weight-covered={covered:.2f}%")
    if no_ohlc:
        w_no = sum(m["w"] for m in holdings if m["t"] in no_ohlc)
        print(f"[qqq-int] WARNING — {len(no_ohlc)} member(s) without OHLC daily "
              f"({w_no:.2f}% of QQQ), excluded from contribution math: {', '.join(no_ohlc)}")
    for b in baskets_out:
        print(f"  [basket] {b['name']:16s} w={b['weight']:6.2f}%  daily={b['daily']}  "
              f"contrib={b['daily_contrib']}")
    for s in gics_out:
        print(f"  [gics ] {s['sector']:26s} w={s['weight']:6.2f}%  daily={s['daily']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
