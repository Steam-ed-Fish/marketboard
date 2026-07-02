#!/usr/bin/env python3
"""OpenD pre-pass: populate build_data._BATCH_CACHE for OpenD-eligible tickers.

OpenD covers US stocks/ETFs, HK, and CN A-shares with no rate-limit. US indices
(^VIX/^SPX/etc.), Korean (.KS), and Japanese (.T) tickers stay on yfinance.

Failure mode: if the daemon is unreachable or returns errors, this returns an
empty set — callers fall back to the existing yfinance prefetch with no
regression.
"""
import os
import json
import re
import sys
import time
from datetime import date, timedelta

import pandas as pd

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

OHLC_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "ohlc")
UNKNOWN_CACHE = os.path.join(os.path.dirname(__file__), "..", "data", "opend_unknown.json")
HOST = "127.0.0.1"
PORT = 11111

# OpenD QPS limits (observed, per Futu docs):
#   get_market_snapshot:    60 calls / 30s, max 200 codes per call
#   request_history_kline:  60 calls / 30s
SNAPSHOT_CHUNK = 200
SNAPSHOT_INTERCALL_S = 0.55  # ~1.8/sec, well under 60/30s
HISTORY_INTERCALL_S = 0.55


def _load_unknown():
    try:
        with open(UNKNOWN_CACHE, encoding="utf-8") as f:
            return set(json.load(f))
    except Exception:
        return set()


def _save_unknown(unknown):
    try:
        os.makedirs(os.path.dirname(UNKNOWN_CACHE), exist_ok=True)
        with open(UNKNOWN_CACHE, "w", encoding="utf-8") as f:
            json.dump(sorted(unknown), f, indent=2)
    except Exception:
        pass


FORCE_YAHOO_TICKERS = set()  # Was {"QQQ","DIA","SPY","IWM"}; route everything through OpenD now and leave blank if OpenD lacks coverage.

def is_opend_eligible(ticker: str, unknown=None) -> bool:
    """OpenD covers US stocks/ETFs, HK, and CN A-shares only."""
    if not ticker or ticker.startswith("^"):
        return False
    if ticker in FORCE_YAHOO_TICKERS:
        return False
    if ticker.endswith(".KS") or ticker.endswith(".KQ"):
        return False
    if ticker.endswith(".T") or ticker.endswith(".TO"):
        return False
    if unknown and ticker in unknown:
        return False
    return True


def to_opend_code(ticker: str) -> str:
    """Yahoo ticker → OpenD code. Caller guarantees is_opend_eligible(ticker)."""
    if ticker.endswith(".HK"):
        return f"HK.{ticker[:-3].zfill(5)}"
    if ticker.endswith(".SS"):
        return f"SH.{ticker[:-3]}"
    if ticker.endswith(".SZ"):
        return f"SZ.{ticker[:-3]}"
    return f"US.{ticker}"


def _safe_filename(ticker: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "_", ticker)


def _load_cached_ohlc(ticker: str):
    path = os.path.join(OHLC_DIR, f"{_safe_filename(ticker)}.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        return d.get("ohlc") or []
    except Exception:
        return []


def _bars_to_yf_df(bars):
    """Convert list of OHLC bar dicts to yfinance-shaped DataFrame.

    yfinance: columns Open/High/Low/Close/Volume, DatetimeIndex.
    Defensive: dedupe by date (keep last) and sort chronologically — otherwise
    a corrupt cache with out-of-order bars (e.g. a Finnhub rescue that appended
    a stale date at the tail) would make tail(N) return the wrong "last" bar.
    """
    if not bars:
        return None
    by_date = {}
    for b in bars:
        try:
            t = b["t"]
        except (KeyError, TypeError):
            continue
        by_date[t] = b
    clean = sorted(by_date.values(), key=lambda x: x["t"])
    if not clean:
        return None
    return pd.DataFrame(
        {
            "Open":   [float(b["o"]) for b in clean],
            "High":   [float(b["h"]) for b in clean],
            "Low":    [float(b["l"]) for b in clean],
            "Close":  [float(b["c"]) for b in clean],
            "Volume": [int(b.get("v") or 0) for b in clean],
        },
        index=pd.DatetimeIndex([pd.Timestamp(b["t"]) for b in clean]),
    )


def _snapshot_to_bar(row, default_date, cached_bars=None):
    """Build a single OHLC bar dict from one OpenD snapshot row.

    Rules:
      1. bar_date = update_time[:10] if valid, else default_date.
      2. Never rewrite history: if bar_date < cached[-1].t, drop.
      3. Never write a future bar: if bar_date > today_ET, drop.
      4. If bar_date == today_ET AND now < 16:00 ET (market open/closed status),
         drop — session not complete, snapshot OHLC is either pre-market carry
         or in-progress. Prevents duplicating yesterday's bar with today's date.
    """
    import datetime as _dt
    upd = row.get("update_time") or ""
    bar_date = upd[:10] if upd and upd[:10] >= "2000-01-01" else default_date

    # Compute US/Eastern "now" (EDT approx UTC-4 for now; safe slack on the cutoff).
    try:
        now_et = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(hours=4)
        today_et = now_et.date().isoformat()
        market_closed = now_et.time() >= _dt.time(16, 30)
    except Exception:
        today_et = None
        market_closed = True

    # Rule 3: future date beyond today ET → drop (OpenD update_time rollover artifact).
    if today_et and bar_date > today_et:
        return None
    # Rule 4: today's date but market not yet closed → drop (stale pre-market snapshot).
    if today_et and bar_date == today_et and not market_closed:
        return None

    # Rule 2: older than most recent cached bar → drop.
    if cached_bars:
        try:
            last_t = cached_bars[-1].get("t")
            if last_t and bar_date < last_t:
                return None
        except Exception:
            pass

    try:
        return {
            "t": bar_date,
            "o": round(float(row["open_price"]),  4),
            "h": round(float(row["high_price"]),  4),
            "l": round(float(row["low_price"]),   4),
            "c": round(float(row["last_price"]),  4),
            "v": int(row.get("volume") or 0),
        }
    except (KeyError, ValueError, TypeError):
        return None


_UNKNOWN_RE = re.compile(r"Unknown stock\s+(\S+)", re.IGNORECASE)
_QPS_RE = re.compile(r"too frequent|too many requests", re.IGNORECASE)


def _snapshot_chunk(ctx, codes, RET_OK, *, max_unknown_drops=10, verbose=True):
    """Snapshot one chunk of ≤200 codes, dropping unknown codes one-by-one.

    Sleeps SNAPSHOT_INTERCALL_S between calls. Bails on QPS errors instead of
    looping. Returns (rows_dict, newly_discovered_unknown_codes).
    """
    rows = {}
    new_unknowns = set()
    work = list(codes)
    drops = 0
    while drops <= max_unknown_drops:
        if not work:
            break
        time.sleep(SNAPSHOT_INTERCALL_S)
        ret, df = ctx.get_market_snapshot(work)
        if ret == RET_OK:
            for _, row in df.iterrows():
                rows[row["code"]] = row
            return rows, new_unknowns
        msg = str(df) if df is not None else ""
        if _QPS_RE.search(msg):
            if verbose:
                print(f"  [opend] snapshot QPS hit ({len(work)} codes pending) — stopping chunk")
            return rows, new_unknowns
        m = _UNKNOWN_RE.search(msg)
        if m:
            bad = m.group(1)
            # OpenD error reports bare symbol (e.g. "MMC"); our list holds prefixed codes ("US.MMC").
            # Match suffix to find the offender.
            removed = [c for c in work if c == bad or c.endswith("." + bad)]
            if not removed:
                if verbose:
                    print(f"  [opend] cannot locate unknown '{bad}' in work list — aborting chunk")
                return rows, new_unknowns
            for c in removed:
                new_unknowns.add(c)
            work = [c for c in work if c not in removed]
            drops += 1
            if verbose:
                print(f"  [opend] dropped unknown {removed[0]} ({drops})")
            continue
        if verbose:
            print(f"  [opend] snapshot error: {msg[:100]} — skipping chunk")
        return rows, new_unknowns
    if verbose:
        print(f"  [opend] hit max unknown drops ({max_unknown_drops}); accepting partial")
    return rows, new_unknowns


def _resilient_snapshot(ctx, codes, RET_OK, *, verbose=True):
    """Snapshot codes in chunks of SNAPSHOT_CHUNK. Returns (rows, newly_unknown)."""
    rows = {}
    new_unknowns = set()
    for i in range(0, len(codes), SNAPSHOT_CHUNK):
        chunk = codes[i:i + SNAPSHOT_CHUNK]
        chunk_rows, chunk_unknown = _snapshot_chunk(ctx, chunk, RET_OK, verbose=verbose)
        rows.update(chunk_rows)
        new_unknowns.update(chunk_unknown)
    return rows, new_unknowns


def _check_daemon(ctx):
    """Return True if quote-logged-in, False otherwise."""
    try:
        ret, data = ctx.get_global_state()
        if ret != 0:
            print(f"[opend] global_state error: {data}")
            return False
        if not data.get("qot_logined"):
            print("[opend] daemon not qot_logined; skipping")
            return False
        return True
    except Exception as e:
        print(f"[opend] daemon health check failed: {e}")
        return False


def populate_batch_cache(batch_cache: dict, all_symbols, *, verbose=True) -> set:
    """Fill batch_cache with yfinance-shaped DataFrames for OpenD-eligible tickers.

    Returns the set of tickers successfully cached. Empty set on any failure.
    """
    try:
        from futu import OpenQuoteContext, KLType, AuType, RET_OK
    except Exception as e:
        if verbose:
            print(f"[opend] futu SDK not importable: {e}")
        return set()

    unknown_cache = _load_unknown()
    eligible = sorted({t for t in all_symbols if is_opend_eligible(t, unknown_cache)})
    if not eligible:
        if verbose:
            print("[opend] no eligible tickers in universe")
        return set()
    if verbose and unknown_cache:
        print(f"[opend] {len(unknown_cache)} cached-unknown tickers pre-filtered")

    if verbose:
        print(f"[opend] {len(eligible)} eligible tickers; connecting to {HOST}:{PORT}...")

    try:
        ctx = OpenQuoteContext(host=HOST, port=PORT)
    except Exception as e:
        print(f"[opend] connect failed: {e}")
        return set()

    if not _check_daemon(ctx):
        try:
            ctx.close()
        except Exception:
            pass
        return set()

    cached_set = set()
    today_iso = date.today().isoformat()

    try:
        codes = [to_opend_code(t) for t in eligible]
        code_to_ticker = dict(zip(codes, eligible))
        snap_rows, new_unknowns = _resilient_snapshot(ctx, codes, RET_OK, verbose=verbose)
        if new_unknowns:
            added = 0
            for bad_code in new_unknowns:
                t = code_to_ticker.get(bad_code)
                if t and t not in unknown_cache:
                    unknown_cache.add(t)
                    added += 1
            if added:
                _save_unknown(unknown_cache)
            if verbose:
                print(f"[opend] persisted {added} newly-unknown tickers to cache")
        if verbose:
            print(f"[opend] snapshot: {len(snap_rows)}/{len(codes)} rows")

        history_fallback = []
        for ticker in eligible:
            code = to_opend_code(ticker)
            row = snap_rows.get(code)
            cached_bars = _load_cached_ohlc(ticker)

            if row is not None and len(cached_bars) >= 200:
                today_bar = _snapshot_to_bar(row, today_iso, cached_bars=cached_bars)
                if not today_bar:
                    history_fallback.append((ticker, code))
                    continue
                merged = [b for b in cached_bars if b.get("t") != today_bar["t"]]
                merged.append(today_bar)
                df = _bars_to_yf_df(merged)
                if df is not None and len(df) >= 50:
                    batch_cache[ticker] = df
                    cached_set.add(ticker)
                else:
                    history_fallback.append((ticker, code))
            else:
                history_fallback.append((ticker, code))

        if history_fallback:
            if verbose:
                print(f"[opend] history fallback for {len(history_fallback)} ticker(s) (throttled)")
            start = (date.today() - timedelta(days=400)).isoformat()
            end = today_iso
            for ticker, code in history_fallback:
                time.sleep(HISTORY_INTERCALL_S)
                try:
                    ret, kdf, _page = ctx.request_history_kline(
                        code, start=start, end=end,
                        ktype=KLType.K_DAY, autype=AuType.NONE,
                        max_count=1000,
                    )
                    if ret != RET_OK:
                        if verbose:
                            print(f"  [opend] {ticker}: history error: {kdf}")
                        continue
                    if kdf is None or len(kdf) < 50:
                        continue
                    df = pd.DataFrame(
                        {
                            "Open":   kdf["open"].astype(float).values,
                            "High":   kdf["high"].astype(float).values,
                            "Low":    kdf["low"].astype(float).values,
                            "Close":  kdf["close"].astype(float).values,
                            "Volume": kdf["volume"].astype("int64", errors="ignore").values,
                        },
                        index=pd.DatetimeIndex([str(t)[:10] for t in kdf["time_key"].values]),
                    )
                    batch_cache[ticker] = df
                    cached_set.add(ticker)
                except Exception as e:
                    if verbose:
                        print(f"  [opend] {ticker}: history exception: {e}")

        if verbose:
            print(f"[opend] populated batch cache for {len(cached_set)}/{len(eligible)} tickers")
    finally:
        try:
            ctx.close()
        except Exception:
            pass

    return cached_set


def fetch_etf_aum_snapshot(tickers, *, verbose=True):
    """Snapshot trust_netAssetValue × trust_outstanding_units for ETF tickers.

    Returns {ticker: {"date": iso, "nav": float, "units": float, "aum": float}}.
    Tickers that don't expose the trust_* block (regular stocks, or non-ETF
    products) are silently dropped — no error, just absent from the result.
    """
    try:
        from futu import OpenQuoteContext, RET_OK
    except Exception as e:
        if verbose:
            print(f"[opend-aum] futu SDK not importable: {e}")
        return {}

    eligible = sorted({t for t in tickers if is_opend_eligible(t)})
    if not eligible:
        return {}

    try:
        ctx = OpenQuoteContext(host=HOST, port=PORT)
    except Exception as e:
        if verbose:
            print(f"[opend-aum] connect failed: {e}")
        return {}

    out = {}
    today_iso = date.today().isoformat()
    try:
        if not _check_daemon(ctx):
            return {}
        codes = [to_opend_code(t) for t in eligible]
        code_to_ticker = dict(zip(codes, eligible))
        rows, _ = _resilient_snapshot(ctx, codes, RET_OK, verbose=verbose)
        for code, row in rows.items():
            ticker = code_to_ticker.get(code)
            if not ticker:
                continue
            try:
                nav = row.get("trust_netAssetValue")
                units = row.get("trust_outstanding_units")
                aum = row.get("trust_aum")
                nav_f = float(nav) if nav not in (None, "", 0, "0") else None
                units_f = float(units) if units not in (None, "", 0, "0") else None
                if not nav_f or not units_f:
                    continue
                aum_f = float(aum) if aum not in (None, "", 0, "0") else nav_f * units_f
                upd = row.get("update_time") or ""
                bar_date = upd[:10] if upd[:10] >= "2000-01-01" else today_iso
                out[ticker] = {
                    "date":  bar_date,
                    "nav":   round(nav_f, 4),
                    "units": round(units_f, 0),
                    "aum":   round(aum_f, 4),
                }
            except (TypeError, ValueError):
                continue
    finally:
        try:
            ctx.close()
        except Exception:
            pass

    if verbose:
        print(f"[opend-aum] {len(out)}/{len(eligible)} tickers returned trust_* data")
    return out


def fetch_options_intel_opend(tickers, spot_lookup=None, *,
                              moneyness_band=0.15, days_lo=3, days_hi=14,
                              verbose=True):
    """Fetch raw option-chain data + per-contract greeks/IV/OI from OpenD.

    Returns a dict {ticker: {spot, expiry, days, contracts: [...]} } where each
    contract row carries strike, type ('CALL'/'PUT'), iv (decimal), delta, gamma,
    open_interest, volume, last_price. Only contracts within ±moneyness_band of
    spot are kept (to bound snapshot quota).

    spot_lookup: optional callable(ticker) -> float for the underlying spot.
    Falls back to underlying snapshot if spot_lookup returns None.
    """
    try:
        from futu import OpenQuoteContext, RET_OK
    except Exception as e:
        if verbose:
            print(f"[opend-opts] futu SDK not importable: {e}")
        return {}

    try:
        ctx = OpenQuoteContext(host=HOST, port=PORT)
    except Exception as e:
        if verbose:
            print(f"[opend-opts] connect failed: {e}")
        return {}

    out = {}
    today = date.today()
    try:
        for ticker in tickers:
            try:
                code = to_opend_code(ticker)
                # 1) spot
                spot = None
                if spot_lookup is not None:
                    try:
                        spot = spot_lookup(ticker)
                    except Exception:
                        spot = None
                if not spot:
                    time.sleep(SNAPSHOT_INTERCALL_S)
                    ret_u, snap_u = ctx.get_market_snapshot([code])
                    if ret_u == RET_OK and len(snap_u) > 0:
                        spot = float(snap_u.iloc[0].get("last_price") or 0)
                if not spot or spot <= 0:
                    if verbose:
                        print(f"  [opend-opts] {ticker}: no spot, skip")
                    continue

                # 2) expiries → pick 3-14 day window, fall back to nearest
                time.sleep(SNAPSHOT_INTERCALL_S)
                ret_e, exps = ctx.get_option_expiration_date(code)
                if ret_e != RET_OK or exps is None or len(exps) == 0:
                    if verbose:
                        print(f"  [opend-opts] {ticker}: no expiries")
                    continue
                exp_rows = []
                for _, r in exps.iterrows():
                    try:
                        d = date.fromisoformat(str(r["strike_time"])[:10])
                        if d >= today:
                            exp_rows.append((d, str(r["strike_time"])[:10]))
                    except Exception:
                        continue
                if not exp_rows:
                    continue
                exp_rows.sort()
                preferred = [(d, s) for d, s in exp_rows if days_lo <= (d - today).days <= days_hi]
                pick = preferred[0] if preferred else exp_rows[0]
                exp_d, exp_str = pick
                days = max((exp_d - today).days, 1)

                # 3) chain for that expiry
                time.sleep(SNAPSHOT_INTERCALL_S)
                ret_c, chain = ctx.get_option_chain(code, start=exp_str, end=exp_str)
                if ret_c != RET_OK or chain is None or len(chain) == 0:
                    if verbose:
                        print(f"  [opend-opts] {ticker}: chain empty for {exp_str}")
                    continue

                # 4) filter strikes to ±band of spot
                lo, hi = spot * (1 - moneyness_band), spot * (1 + moneyness_band)
                chain_f = chain[(chain["strike_price"] >= lo) & (chain["strike_price"] <= hi)]
                contract_codes = chain_f["code"].tolist()
                if not contract_codes:
                    continue

                # 5) batch snapshot the contracts
                snap_rows, _ = _resilient_snapshot(ctx, contract_codes, RET_OK, verbose=False)

                contracts = []
                for ccode in contract_codes:
                    row = snap_rows.get(ccode)
                    if row is None:
                        continue
                    try:
                        iv_pct = row.get("option_implied_volatility")
                        if iv_pct in (None, "", "N/A"):
                            continue
                        iv = float(iv_pct) / 100.0  # OpenD returns IV as a percentage
                        if iv <= 0:
                            continue
                        contracts.append({
                            "strike": float(row.get("option_strike_price")),
                            "type":   str(row.get("option_type")),  # 'CALL' / 'PUT'
                            "iv":     iv,
                            "delta":  float(row.get("option_delta")  or 0),
                            "gamma":  float(row.get("option_gamma")  or 0),
                            "oi":     int(float(row.get("option_open_interest") or 0)),
                            "volume": int(float(row.get("volume") or 0)),
                            "last_price": float(row.get("last_price") or 0),
                        })
                    except (ValueError, TypeError):
                        continue

                if not contracts:
                    if verbose:
                        print(f"  [opend-opts] {ticker}: no usable contracts after parse")
                    continue

                out[ticker] = {
                    "spot": float(spot),
                    "expiry": exp_str,
                    "days": days,
                    "contracts": contracts,
                }
                if verbose:
                    print(f"  [opend-opts] {ticker}: {len(contracts)} contracts @ {exp_str} ({days}d)")
            except Exception as e:
                if verbose:
                    print(f"  [opend-opts] {ticker}: error: {e}")
                continue
    finally:
        try:
            ctx.close()
        except Exception:
            pass

    return out


def fetch_expected_move_opend(tickers, spot_lookup=None, *, verbose=True):
    """Compute ATM-straddle expected move via OpenD using nearest Friday expiry.

    Returns {ticker: {em_pct, em_days}}. Missing/failed stays None so callers
    can fall back to yfinance per-ticker.
    """
    try:
        from futu import OpenQuoteContext, RET_OK
    except Exception as e:
        if verbose:
            print(f"[opend-em] futu SDK not importable: {e}")
        return {}
    try:
        ctx = OpenQuoteContext(host=HOST, port=PORT)
    except Exception as e:
        if verbose:
            print(f"[opend-em] connect failed: {e}")
        return {}

    today = date.today()
    out = {}
    try:
        for ticker in tickers:
            res = {"em_pct": None, "em_days": None}
            try:
                code = to_opend_code(ticker)
                # spot
                spot = None
                if spot_lookup is not None:
                    try:
                        spot = spot_lookup(ticker)
                    except Exception:
                        spot = None
                if not spot:
                    time.sleep(SNAPSHOT_INTERCALL_S)
                    ret_u, snap_u = ctx.get_market_snapshot([code])
                    if ret_u == RET_OK and len(snap_u) > 0:
                        spot = float(snap_u.iloc[0].get("last_price") or 0)
                if not spot or spot <= 0:
                    if verbose:
                        print(f"  [opend-em] {ticker}: no spot, skip")
                    out[ticker] = res
                    continue

                # expiries
                time.sleep(SNAPSHOT_INTERCALL_S)
                ret_e, exps = ctx.get_option_expiration_date(code)
                if ret_e != RET_OK or exps is None or len(exps) == 0:
                    out[ticker] = res
                    continue
                exp_rows = []
                for _, r in exps.iterrows():
                    try:
                        d = date.fromisoformat(str(r["strike_time"])[:10])
                        if d > today:
                            exp_rows.append((d, str(r["strike_time"])[:10]))
                    except Exception:
                        continue
                if not exp_rows:
                    out[ticker] = res
                    continue
                exp_rows.sort()

                fridays = [(d, s) for d, s in exp_rows if d.weekday() == 4]
                pick_weekly = fridays[0] if fridays else None

                def _straddle_em(exp_d, exp_s):
                    days = max((exp_d - today).days, 1)
                    time.sleep(SNAPSHOT_INTERCALL_S)
                    ret_c, chain = ctx.get_option_chain(code, start=exp_s, end=exp_s)
                    if ret_c != RET_OK or chain is None or len(chain) == 0:
                        return None, None
                    # narrow to ±5% of spot to keep snapshot cheap
                    lo, hi = spot * 0.95, spot * 1.05
                    chain_n = chain[(chain["strike_price"] >= lo) & (chain["strike_price"] <= hi)]
                    if chain_n.empty:
                        chain_n = chain
                    contract_codes = chain_n["code"].tolist()
                    snap_rows, _ = _resilient_snapshot(ctx, contract_codes, RET_OK, verbose=False)
                    # Build per-strike call/put mid
                    def _mid(row):
                        bid = float(row.get("bid_price") or 0)
                        ask = float(row.get("ask_price") or 0)
                        if bid > 0 or ask > 0:
                            return (bid + ask) / 2 if (bid > 0 and ask > 0) else (bid or ask)
                        return float(row.get("last_price") or 0)
                    calls_mid = {}
                    puts_mid  = {}
                    for ccode in contract_codes:
                        row = snap_rows.get(ccode)
                        if row is None: continue
                        try:
                            K = float(row.get("option_strike_price"))
                            t = str(row.get("option_type") or "")
                            m = _mid(row)
                            if m <= 0 or K <= 0: continue
                            (calls_mid if t == "CALL" else puts_mid)[K] = m
                        except (ValueError, TypeError):
                            continue
                    common = sorted(set(calls_mid) & set(puts_mid))
                    if not common:
                        return None, None
                    atm = min(common, key=lambda k: abs(k - spot))
                    straddle = calls_mid[atm] + puts_mid[atm]
                    if straddle <= 0:
                        return None, None
                    return round(straddle / spot * 100, 2), days

                if pick_weekly:
                    em, dn = _straddle_em(*pick_weekly)
                    res["em_pct"], res["em_days"] = em, dn
                if verbose:
                    print(f"  [opend-em] {ticker}: ±{res['em_pct']}% ({res['em_days']}d)")
            except Exception as e:
                if verbose:
                    print(f"  [opend-em] {ticker}: error: {e}")
            out[ticker] = res
    finally:
        try:
            ctx.close()
        except Exception:
            pass
    return out


def _probe():
    """Standalone smoke test — invoked via `python scripts/fetch_opend.py`."""
    samples = ["SPY", "QQQ", "NVDA", "BRK.B", "0700.HK", "600519.SS", "000001.SZ", "^VIX", "000660.KS"]
    print("ELIGIBILITY:")
    for t in samples:
        ok = is_opend_eligible(t)
        code = to_opend_code(t) if ok else "—"
        print(f"  {t:12s}  eligible={ok}  code={code}")

    print("\nLIVE PROBE:")
    cache = {}
    cached = populate_batch_cache(cache, samples)
    print(f"\nCached tickers ({len(cached)}): {sorted(cached)}")
    for t in sorted(cached):
        df = cache[t]
        last = df["Close"].iloc[-1]
        prev = df["Close"].iloc[-2] if len(df) >= 2 else None
        daily = (last / prev - 1) * 100 if prev else None
        print(f"  {t:12s}  bars={len(df):3d}  last={last:.4f}  daily%={daily:+.2f}" if daily is not None else f"  {t:12s}  bars={len(df):3d}  last={last:.4f}")


if __name__ == "__main__":
    _probe()
