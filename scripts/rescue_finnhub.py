#!/usr/bin/env python3
"""Finnhub fallback: patch stale OHLC files with today's EOD quote.

Uses /quote (free tier, 60/min). No volume -> v=0 for appended bar.
Priorities Indices + 'The XX 7' theme groups first.

TARGET is auto-detected as the freshest last-bar date across all OHLC files —
so if Yahoo partially worked (e.g. indices updated but stocks didn't), this
patches the stragglers to match the fresh date. If Yahoo totally failed or
everything is already fresh, the script is a no-op.
"""
import json, os, re, sys, time, urllib.request
from collections import Counter
from glob import glob

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try: sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

TOKEN = "d7aasqhr01qn9i7k8v30d7aasqhr01qn9i7k8v3g"
OUT = "data/ohlc"
SNAP = "data/snapshot.json"
SLEEP = 1.1  # ~55 calls/min, under 60/min free limit

# Tickers in frozen groups (build_data skips them too). Don't patch — they're
# intentionally allowed to go stale.
FROZEN_TICKERS = set()


def recompute_snapshot_fields(bars):
    """Recompute price-derived snapshot fields purely from OHLC bars.

    These are the fields that go stale when build_data's Yahoo fetch fails:
    snapshot keeps yesterday's row but OHLC gets patched fresh by rescue.
    Returns dict of field updates (only fields we can derive from OHLC alone).
    """
    import datetime as dt
    if not bars or len(bars) < 2:
        return {}
    closes = [b.get("c") for b in bars]
    opens  = [b.get("o") for b in bars]
    vols   = [b.get("v") or 0 for b in bars]
    last_c, prev_c, last_o = closes[-1], closes[-2], opens[-1]
    out = {}
    if prev_c:
        out["daily"] = round((last_c / prev_c - 1) * 100, 2)
    if last_o:
        out["intra"] = round((last_c / last_o - 1) * 100, 2)
    if len(closes) >= 6 and closes[-6]:
        out["5d"] = round((last_c / closes[-6] - 1) * 100, 2)
    if len(closes) >= 21 and closes[-21]:
        out["20d"] = round((last_c / closes[-21] - 1) * 100, 2)
    # WTD: prior Friday close (last bar before this Monday)
    try:
        last_d = dt.date.fromisoformat(bars[-1]["t"])
        monday = last_d - dt.timedelta(days=last_d.weekday())
        baseline = None
        for b in reversed(bars[:-1]):
            if dt.date.fromisoformat(b["t"]) < monday:
                baseline = b.get("c")
                break
        if baseline:
            out["wtd"] = round((last_c / baseline - 1) * 100, 2)
    except Exception:
        pass
    # YTD: last close of prior year
    try:
        last_d = dt.date.fromisoformat(bars[-1]["t"])
        prior_close = None
        for b in reversed(bars[:-1]):
            if dt.date.fromisoformat(b["t"]).year < last_d.year:
                prior_close = b.get("c")
                break
        if prior_close:
            out["ytd"] = round((last_c / prior_close - 1) * 100, 2)
    except Exception:
        pass
    # vol_ratio: today / mean(prior 20)
    if len(vols) >= 21:
        prior_20 = vols[-21:-1]
        avg20 = sum(prior_20) / 20 if prior_20 else 0
        if avg20 > 0 and vols[-1] > 0:
            out["vol_ratio"] = round(vols[-1] / avg20, 2)
    # SMAs
    def _sma(period):
        if len(closes) < period:
            return None
        seg = closes[-period:]
        return sum(seg) / period if all(c is not None for c in seg) else None
    sma20, sma50, sma200 = _sma(20), _sma(50), _sma(200)
    if sma20:  out["above_sma20"]  = bool(last_c > sma20)
    if sma50:  out["above_sma50"]  = bool(last_c > sma50)
    if sma200: out["above_sma200"] = bool(last_c > sma200)
    return out


AI_THEMES = {
    "Mag 7":         ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA"],
    "Memory":        ["MU", "WDC", "SNDK", "STX", "000660.KS", "005930.KS"],
    "Optical Comms": ["COHR", "LITE", "AAOI", "VIAV", "TSEM", "AXTI", "GLW"],
    "Neocloud":      ["CRWV", "NBIS", "APLD", "IREN", "WULF", "HUT", "CIFR"],
    "Data Center":   ["VRT", "EQIX", "DLR", "IRM", "SMCI", "NVT", "MOD"],
    "Power Grid":    ["VST", "CEG", "NRG", "GEV", "ETN", "PWR", "TLN"],
}
THEME_ETF_PROXY = {"Memory": "DRAM", "Optical Comms": "FOTO"}
AGG_FIELDS = ("daily", "intra", "wtd", "5d", "20d", "ytd", "vol_ratio",
              "atr_pct", "dist_sma50_atr", "rs")


def find_row_in_snap(snap, ticker):
    for rows in (snap.get("groups") or {}).values():
        for r in rows:
            if isinstance(r, dict) and r.get("ticker") == ticker:
                return r
    return None


def _safe_ticker(t):
    return re.sub(r"[^a-zA-Z0-9]", "_", t)


def find_or_derive_row(snap, ticker):
    """Like find_row_in_snap, but for theme-only tickers (not in any STOCK_GROUPS),
    fall back to deriving fields directly from the on-disk OHLC file. This keeps
    AI_THEMES aggregates valid for tickers that exist only as theme constituents
    (Optical Comms: COHR/LITE/AAOI/VIAV/TSEM/AXTI/GLW; most of Memory)."""
    row = find_row_in_snap(snap, ticker)
    if row:
        return row
    path = f"{OUT}/{_safe_ticker(ticker)}.json"
    if not os.path.exists(path):
        return None
    try:
        d = json.load(open(path, encoding="utf-8"))
    except Exception:
        return None
    bars = d.get("ohlc") or []
    fields = recompute_snapshot_fields(bars)
    if not fields:
        return None
    fields["ticker"] = ticker
    return fields


def rebuild_aggregates(snap):
    """Recompute snapshot.themes and 'The 7s at a Glance' rows from the
    (now-updated) per-ticker rows. Build_data computed these BEFORE rescue ran,
    so they reflect Yahoo's rate-limited fallback data — refresh them here."""
    def _avg(rows, key, ndec=2):
        vals = [r.get(key) for r in rows if r.get(key) is not None]
        return round(sum(vals) / len(vals), ndec) if vals else None

    # 1. AI Themes — fall back to on-disk OHLC for theme-only tickers (Optical
    # Comms / most of Memory aren't in any STOCK_GROUPS, so find_row_in_snap
    # alone would blank them).
    themes_out = []
    for theme_name, tickers in AI_THEMES.items():
        t_rows_map = {t: find_or_derive_row(snap, t) for t in tickers}
        t_rows = [r for r in t_rows_map.values() if r]
        proxy_t = THEME_ETF_PROXY.get(theme_name)
        proxy = find_or_derive_row(snap, proxy_t) if proxy_t else None
        def _val(key, ndec=2):
            if proxy and proxy.get(key) is not None:
                return round(proxy[key], ndec)
            return _avg(t_rows, key, ndec)
        themes_out.append({
            "name": theme_name,
            "tickers": tickers,
            "etf": proxy_t if proxy else None,
            **{k: _val(k, 1 if k == "atr_pct" else 2) for k in AGG_FIELDS},
            "vol_chart": None,
            "constituent_daily": {
                t: r.get("daily") for t, r in t_rows_map.items() if r
            },
        })
    # Preserve any existing vol_chart paths if present
    for new_t, old_t in zip(themes_out, snap.get("themes") or []):
        if isinstance(old_t, dict) and old_t.get("vol_chart"):
            new_t["vol_chart"] = old_t["vol_chart"]
    snap["themes"] = themes_out

    # 2. The 7s at a Glance — one row per "The X 7" group
    glance = snap.get("groups", {}).get("The 7s at a Glance") or []
    seven_groups = {k: v for k, v in (snap.get("groups") or {}).items()
                    if k.startswith("The ") and k.endswith(" 7")}
    for gr in glance:
        if not isinstance(gr, dict) or gr.get("is_rrg_row"):
            continue
        gname = gr.get("ticker")  # rows are keyed by group label
        rows = seven_groups.get(gname) or []
        for k in AGG_FIELDS:
            v = _avg(rows, k, 1 if k == "atr_pct" else 2)
            if v is not None:
                gr[k] = v


def update_snapshot_for(tickers):
    """For each ticker, read its OHLC and overwrite price-derived fields in
    every group row referencing it. Then rebuild theme + glance aggregates so
    they reflect the patched per-row data instead of build_data's stale view."""
    if not tickers:
        return 0
    try:
        snap = json.load(open(SNAP, encoding="utf-8"))
    except Exception as e:
        print(f"[finnhub] snapshot load failed: {e}", flush=True)
        return 0
    targets = set(tickers)
    bars_cache = {}
    for sym in targets:
        try:
            d = json.load(open(f"{OUT}/{sym}.json", encoding="utf-8"))
            bars_cache[sym] = d.get("ohlc") or []
        except Exception:
            bars_cache[sym] = []
    updated = 0
    for gname, rows in (snap.get("groups") or {}).items():
        for r in rows:
            if not isinstance(r, dict):
                continue
            t = r.get("ticker")
            if t in targets:
                fields = recompute_snapshot_fields(bars_cache.get(t, []))
                if fields:
                    r.update(fields)
                    updated += 1
    if updated:
        rebuild_aggregates(snap)
        with open(SNAP, "w", encoding="utf-8") as f:
            json.dump(snap, f, separators=(",", ":"), default=str)
    return updated


def cache_majority():
    """Most common last-bar date across all OHLC files."""
    c = Counter()
    for f in glob(f"{OUT}/*.json"):
        try:
            d = json.load(open(f, encoding="utf-8"))
            bars = d.get("ohlc") or []
            if bars:
                c[bars[-1].get("t")] += 1
        except Exception:
            pass
    return max(c) if c else None


def finnhub_truth(sym="SPY"):
    """Probe Finnhub for SPY's latest trading date as ground truth.

    Guard: Finnhub's /quote `t` field returns the *current* intraday timestamp
    during market hours, not the last EOD bar's date. If we naively used that,
    we'd target a date that has no completed EOD anywhere yet, and every cached
    ticker would be flagged stale against an impossible target. So reject any
    Finnhub date that equals today's US/Eastern date *before* market close
    (4:30pm ET — gives ~30 min for EOD bars to settle).
    """
    try:
        import datetime as dt
        url = f"https://finnhub.io/api/v1/quote?symbol={sym}&token={TOKEN}"
        r = json.loads(urllib.request.urlopen(url, timeout=10).read())
        if not r.get("t"):
            return None
        probe_date = dt.datetime.fromtimestamp(r["t"], dt.timezone.utc).date()
        # US/Eastern "now". Use a fixed -5/-4 offset is wrong w/ DST, but datetime's
        # zoneinfo isn't always installed on Windows Python; approximate via UTC-4
        # which is correct for June (EDT). Build_data runs daily during EDT/EST
        # transitions so an hour of slack on the cutoff is acceptable.
        now_et = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=4)
        today_et = now_et.date()
        market_closed = now_et.time() >= dt.time(16, 30)
        if probe_date >= today_et and not market_closed:
            # Intraday — Finnhub's "today" hasn't produced an EOD bar yet.
            return None
        return probe_date.isoformat()
    except Exception as e:
        print(f"[finnhub] truth probe failed: {e}", flush=True)
        return None


def detect_target():
    """Target = max(cache majority, Finnhub ground truth).

    Cache majority handles the common case (Yahoo partially worked).
    Finnhub probe handles the failure case (Yahoo totally failed → cache is
    uniformly stale and we'd otherwise think it was fresh).
    """
    cm = cache_majority()
    ft = finnhub_truth()
    if cm and ft:
        return max(cm, ft)
    return cm or ft


TARGET = detect_target()

def load_priority():
    try:
        snap = json.load(open("data/snapshot.json", encoding="utf-8"))
    except Exception:
        return set()
    pri = set()
    for gname, rows in (snap.get("groups") or {}).items():
        if gname == "Indices" or (gname.startswith("The ") and "7" in gname):
            for r in rows:
                t = r.get("ticker") if isinstance(r, dict) else None
                if t: pri.add(t)
    return pri

def list_stale():
    """Stale = ticker present in snapshot/OHLC dir but last bar older than TARGET.

    Also flags tickers in snapshot that have no OHLC file at all — these are
    persistent Yahoo failures (e.g. DRAM) that would otherwise stay invisible
    to rescue forever, and silently leak stale prior-snapshot daily into theme
    aggregates.
    """
    stale = []
    seen = set()
    for f in glob(f"{OUT}/*.json"):
        try:
            d = json.load(open(f, encoding="utf-8"))
            t = d.get("ticker")
            if not t or t in FROZEN_TICKERS:
                continue
            seen.add(t)
            bars = d.get("ohlc") or []
            last = bars[-1].get("t") if bars else None
            if last and last < TARGET:
                stale.append((t, f, last))
            elif not bars:
                stale.append((t, f, None))
        except Exception:
            pass
    # Catch tickers in snapshot that never got an OHLC file written.
    try:
        snap = json.load(open(SNAP, encoding="utf-8"))
        for gname, rows in (snap.get("groups") or {}).items():
            for r in rows:
                if not isinstance(r, dict):
                    continue
                t = r.get("ticker")
                if not t or t in FROZEN_TICKERS or t in seen:
                    continue
                if r.get("is_rrg_row"):
                    continue
                stale.append((t, f"{OUT}/{t}.json", None))
                seen.add(t)
    except Exception:
        pass
    return stale

def fetch_quote(sym):
    url = f"https://finnhub.io/api/v1/quote?symbol={sym}&token={TOKEN}"
    r = json.loads(urllib.request.urlopen(url, timeout=10).read())
    import datetime as dt
    tdate = dt.datetime.fromtimestamp(r["t"], dt.timezone.utc).date().isoformat()
    return tdate, r


def fetch_stooq_volume(sym):
    """Stooq's free light-quote endpoint returns last EOD volume.

    Returns int volume or 0 if unavailable. Foreign tickers (e.g. .KS) skipped.
    """
    if "." in sym and not sym.endswith(".US"):
        # Skip foreign tickers — Stooq US universe only
        return 0
    stooq_sym = sym.replace(".", "-").lower() + ".us"  # BRK.B -> brk-b.us
    url = f"https://stooq.com/q/l/?s={stooq_sym}&f=sv&h&e=csv"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        body = urllib.request.urlopen(req, timeout=10).read().decode()
        lines = body.strip().split("\n")
        if len(lines) < 2:
            return 0
        parts = lines[1].split(",")
        if len(parts) < 2 or parts[1] in ("N/D", ""):
            return 0
        return int(parts[1])
    except Exception:
        return 0

def patch(path, payload, target_date, sym):
    if os.path.exists(path):
        d = json.load(open(path, encoding="utf-8"))
    else:
        # Brand-new ticker (no prior OHLC). Seed with prev-close from Finnhub
        # so recompute_snapshot_fields can derive 'daily' from 2 bars.
        import datetime as dt
        prev_d = (dt.date.fromisoformat(target_date) - dt.timedelta(days=1)).isoformat()
        pc = payload.get("pc")
        seed = []
        if pc:
            seed.append({"t": prev_d, "o": pc, "h": pc, "l": pc, "c": pc, "v": 0})
        d = {"ticker": sym, "ohlc": seed}
    bars = d.get("ohlc") or []
    # Dedup if same date already present
    bars = [b for b in bars if b.get("t") != target_date]
    vol = fetch_stooq_volume(sym)
    new_bar = {
        "t": target_date,
        "o": round(float(payload["o"]), 4),
        "h": round(float(payload["h"]), 4),
        "l": round(float(payload["l"]), 4),
        "c": round(float(payload["c"]), 4),
        "v": vol,
    }
    bars.append(new_bar)
    # Sort chronologically so a stale appendee can't end up at the tail and
    # poison downstream `tail(N)` consumers (see fetch_opend._bars_to_yf_df).
    bars.sort(key=lambda b: b.get("t") or "")
    d["ohlc"] = bars[-252:]
    d.setdefault("ticker", sym)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(d, f, separators=(",", ":"))
    return vol

def main():
    if not TARGET:
        print("[finnhub] No OHLC files found, nothing to do.", flush=True)
        return
    print(f"[finnhub] target date = {TARGET} (freshest last bar across cache)", flush=True)
    priority = load_priority()
    stale = list_stale()
    if not stale:
        print(f"[finnhub] All files already at {TARGET}. No-op.", flush=True)
        return
    stale.sort(key=lambda x: (0 if x[0] in priority else 1, x[0]))
    print(f"[finnhub] priority universe: {len(priority)}  stale: {len(stale)}", flush=True)
    pri_count = sum(1 for s in stale if s[0] in priority)
    print(f"[finnhub] stale-priority: {pri_count}  stale-other: {len(stale)-pri_count}", flush=True)

    ok, mismatch, failed = [], [], []
    for i, (sym, path, last) in enumerate(stale, 1):
        tag = "P" if sym in priority else " "
        try:
            tdate, q = fetch_quote(sym)
            if tdate != TARGET:
                mismatch.append((sym, tdate))
                print(f"  [{i:3}/{len(stale)}] {tag} {sym}  returned {tdate} (want {TARGET})", flush=True)
            elif not q.get("c") or q["c"] <= 0:
                failed.append((sym, "empty quote"))
                print(f"  [{i:3}/{len(stale)}] {tag} {sym}  EMPTY quote", flush=True)
            else:
                vol = patch(path, q, TARGET, sym)
                ok.append(sym)
                vol_tag = f" v={vol/1e6:.1f}M" if vol else " v=0"
                print(f"  [{i:3}/{len(stale)}] {tag} {sym}  -> {TARGET} c={q['c']:.2f}{vol_tag} OK", flush=True)
        except Exception as e:
            failed.append((sym, str(e)[:80]))
            print(f"  [{i:3}/{len(stale)}] {tag} {sym}  FAIL: {str(e)[:80]}", flush=True)
        time.sleep(SLEEP)

    # Snapshot has stale daily/vol_ratio/above_sma* for tickers Yahoo dropped.
    # Recompute those fields from the freshly-patched OHLC so the dashboard
    # and breadth.json see correct values, not yesterday's row.
    snap_updated = update_snapshot_for(ok)

    print()
    print("─── FINNHUB RESCUE SUMMARY ───")
    print(f"  Patched to {TARGET}: {len(ok)}")
    print(f"  Wrong date:         {len(mismatch)}")
    print(f"  Failed:             {len(failed)}")
    print(f"  Snapshot rows updated: {snap_updated}")
    if mismatch: print(f"  Wrong-date sample: {mismatch[:10]}")
    if failed:   print(f"  Failed sample:     {failed[:10]}")

if __name__ == "__main__":
    main()
