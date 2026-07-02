"""
Build dashboard data for static GitHub Pages deployment.
Run from repo root: python scripts/build_data.py [--out-dir data]
Outputs: data/snapshot.json, data/events.json, data/meta.json, data/charts/*.png
"""
from __future__ import print_function
import argparse
import json
import os
import re
import time
import requests

import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from io import BytesIO
from scipy.stats import rankdata
from scipy.signal import find_peaks

try:
    import investpy
except ImportError:
    investpy = None

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import math

# ── Batch-download cache + rate-limit circuit breaker ────────────────────────
# Pre-fills a dict of ticker -> DataFrame using yf.download() in chunks of 50,
# collapsing ~300 individual HTTP calls into ~6 bulk requests. get_stock_data()
# reads from this cache first, falling back to yf.Ticker() only when missing.
# Circuit breaker: if we see 3 consecutive 429s from yfinance, stop calling it
# for the rest of this run (previous-snapshot fallback takes over).
_BATCH_CACHE = {}
_RATE_LIMIT_HITS = 0
_CIRCUIT_OPEN = False
_CIRCUIT_THRESHOLD = 3

def _is_rate_limit_error(exc):
    s = str(exc).lower()
    return "429" in s or "too many" in s or "rate limit" in s

def _note_yf_result(error=None):
    global _RATE_LIMIT_HITS, _CIRCUIT_OPEN
    if error is None:
        _RATE_LIMIT_HITS = 0
        return
    if _is_rate_limit_error(error):
        _RATE_LIMIT_HITS += 1
        if _RATE_LIMIT_HITS >= _CIRCUIT_THRESHOLD and not _CIRCUIT_OPEN:
            _CIRCUIT_OPEN = True
            print(f"[CIRCUIT-BREAKER] Opened after {_RATE_LIMIT_HITS} consecutive 429s — remaining fetches will fall back to previous snapshot.")

def prefetch_histories(symbols, chunk_size=50, inter_chunk_sleep=6):
    """Batch-download 1y histories for all symbols; populate _BATCH_CACHE.
    Treats any chunk-level 429 as partial failure — moves on so the circuit
    breaker kicks in once enough individual retries also 429."""
    global _BATCH_CACHE
    symbols = [s for s in symbols if s]
    print(f"[prefetch] {len(symbols)} tickers in chunks of {chunk_size} (inter-chunk sleep {inter_chunk_sleep}s)")
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i+chunk_size]
        try:
            df = yf.download(chunk, period="1y", group_by="ticker",
                             progress=False, threads=True, auto_adjust=False)
            got = 0
            if hasattr(df.columns, "levels"):  # multi-index (>1 ticker)
                for sym in chunk:
                    try:
                        sub = df[sym].dropna(how="all")
                        if len(sub) >= 50:
                            _BATCH_CACHE[sym] = sub
                            got += 1
                    except Exception:
                        pass
            else:  # single-ticker batch returns flat columns
                sub = df.dropna(how="all")
                if len(sub) >= 50:
                    _BATCH_CACHE[chunk[0]] = sub
                    got += 1
            print(f"  [prefetch {i//chunk_size + 1}/{(len(symbols)+chunk_size-1)//chunk_size}] {got}/{len(chunk)} cached")
            _note_yf_result(error=None)
        except Exception as e:
            print(f"  [prefetch chunk {i//chunk_size + 1}] FAILED: {str(e)[:120]}")
            _note_yf_result(error=e)
        time.sleep(inter_chunk_sleep)
    print(f"[prefetch] cache filled: {len(_BATCH_CACHE)}/{len(symbols)} tickers")


def sanitize_for_json(obj):
    """Recursively replace NaN/Infinity with None so json.dump produces valid JSON."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [sanitize_for_json(v) for v in obj]
    return obj

# --- Config: no Liquid Stocks ---
KEY_EVENTS = [
    "Fed", "Federal Reserve", "Interest Rate", "FOMC",
    "ISM Manufacturing", "ISM Non-Manufacturing", "ISM Services", "ISM",
    "CPI", "Consumer Price Index", "Nonfarm Payrolls", "NFP", "Employment",
    "PPI", "Producer Price Index", "PCE", "Core PCE", "Personal Consumption",
    "Retail Sales", "GDP", "Gross Domestic Product", "Unemployment", "Jobless Claims", "Initial Claims",
    "Housing Starts", "Building Permits", "Durable Goods", "Factory Orders",
    "Consumer Confidence", "Michigan Consumer", "Trade Balance", "Trade Deficit",
    "Beige Book", "Fed Minutes", "JOLTS", "Job Openings"
]

STOCK_GROUPS = {
    "Indices": ["QQQ", "DIA", "SPY", "RSP", "IWM", "IJH", "IJR", "SOXX", "GLD", "SLV"],
    "S&P Style ETFs": ["IJS", "IJR", "IJT", "IJJ", "IJH", "IJK", "IVE", "IVV", "IVW"],
    "The Mag 7": ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA"],
    "The Cloud 7": ["MSFT", "AMZN", "GOOGL", "ORCL", "IBM", "META", "NET"],
    "The Software 7": ["ADBE", "PANW", "PLTR", "INTU", "CRM", "NOW", "WDAY"],
    "The New Staples 7": ["AAPL", "AMZN", "META", "NFLX", "GOOGL", "SPOT", "UBER"],
    "The Old Staples 7": ["PG", "KO", "PEP", "PM", "CL", "MDLZ", "WMT"],
    "The Toll Road 7": ["V", "MA", "PYPL", "CME", "ICE", "SPGI", "MSCI"],
    "The Energy 7": ["XOM", "CVX", "COP", "EOG", "FANG", "DVN", "SLB"],
    "The Defense 7": ["LMT", "RTX", "NOC", "GD", "BA", "LHX", "HII"],
    "The Industrial 7": ["CAT", "DE", "HON", "PH", "EMR", "GE", "TT"],
    "The Semi 7": ["NVDA", "AVGO", "AMD", "QCOM", "INTC", "TXN", "MRVL"],
    "The Fab 7": ["ASML", "AMAT", "LRCX", "KLAC", "TSM", "MU", "TER"],
    "The Buildout 7": ["ANET", "VRT", "APH", "PWR", "EME", "GEV", "ETN"],
    "The Housing 7": ["DHI", "LEN", "VMC", "MLM", "SHW", "HD", "LOW"],
    "The Old Metals 7": ["NEM", "GOLD", "AEM", "WPM", "FNV", "RGLD", "AGI"],
    "The New Metals 7": ["FCX", "SCCO", "CCJ", "ALB", "SQM", "MP", "PAAS"],
    "The Bank 7": ["JPM", "GS", "MS", "BAC", "WFC", "BLK", "C"],
    "The Health 7": ["UNH", "LLY", "ABBV", "JNJ", "PFE", "MRK", "GILD"],
    "The Medtech 7": ["ISRG", "TMO", "ABT", "MDT", "SYK", "BSX", "DHR"],
    "The Freight 7": ["UNP", "CSX", "FDX", "UPS", "ODFL", "JBHT", "NSC"],
    "The Insurance 7": ["PGR", "CB", "AON", "AIG", "MET", "AFL", "TRV"],
    "The Power 7": ["NEE", "SO", "DUK", "CEG", "VST", "AEP", "SRE"],
    "Industries": [
        "TAN", "QQQE", "JETS", "IBB", "SMH", "CIBR", "UTES", "IGV", "ITA", "PAVE", "AIQ", "FDN", "KBE",
        "UNG", "KWEB", "KRE", "IBIT", "XRT", "IHI", "MSOS", "XLU", "XME", "GLD", "GXC", "SCHH", "MAGS", "SOXX", "DRAM",
        "GDX", "IWM", "XOP", "VNQ", "FXI", "DBA", "ICLN", "SILJ", "SLV", "XHB", "USO", "DBC", "FCG", "XBI",
        "OIH", "FNGS", "URA", "WGMI"
    ],
    "Sel Sectors": ["XLK", "XLI", "XLC", "XLF", "XLU", "XLY", "XLRE", "XLP", "XLB", "XLE", "XLV"],
}

FROZEN_GROUPS = set()

AI_THEMES = {
    "Mag 7":         ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA"],
    "Memory":        ["MU", "WDC", "SNDK", "STX"],
    "Optical Comms": ["COHR", "LITE", "AAOI", "VIAV", "TSEM", "AXTI", "GLW"],
    "Neocloud":      ["CRWV", "NBIS", "APLD", "IREN", "WULF", "HUT", "CIFR"],
    "Data Center":   ["VRT", "EQIX", "DLR", "IRM", "SMCI", "NVT", "MOD"],
    "Power Grid":    ["VST", "CEG", "NRG", "GEV", "ETN", "PWR", "TLN"],
}

# ETF proxy for a theme — use the ETF's actual returns instead of equal-weighted avg
THEME_ETF_PROXY = {
    "Memory": "DRAM",
    "Optical Comms": "FOTO",
}

LEVERAGED_ETFS = {
    "QQQ": {"long": ["TQQQ"], "short": ["SQQQ"]},
    "MDY": {"long": ["MIDU"], "short": []},
    "IWM": {"long": ["TNA"], "short": ["TZA"]},
    "TLT": {"long": ["TMF"], "short": ["TMV"]},
    "SPY": {"long": ["SPXL", "UPRO"], "short": ["SPXS", "SH"]},
    "ETHA": {"long": ["ETHU"], "short": []},
    "XLK": {"long": ["TECL"], "short": ["TECS"]},
    "XLI": {"long": ["DUSL"], "short": []},
    "XLC": {"long": ["LTL"], "short": []},
    "XLF": {"long": ["FAS"], "short": ["FAZ"]},
    "XLU": {"long": ["UTSL"], "short": []},
    "XLY": {"long": ["WANT"], "short": ["SCC"]},
    "XLRE": {"long": ["DRN"], "short": ["DRV"]},
    "XLP": {"long": ["UGE"], "short": ["SZK"]},
    "XLB": {"long": ["UYM"], "short": ["SMN"]},
    "XLE": {"long": ["ERX"], "short": ["ERY"]},
    "XLV": {"long": ["CURE"], "short": []},
    "SMH": {"long": ["SOXL"], "short": ["SOXS"]},
    "ARKK": {"long": ["TARK"], "short": ["SARK"]},
    "XTN": {"long": ["TPOR"], "short": []},
    "KWEB": {"long": ["CWEB"], "short": []},
    "XRT": {"long": ["RETL"], "short": []},
    "KRE": {"long": ["DPST"], "short": []},
    "DRIV": {"long": ["EVAV"], "short": []},
    "XBI": {"long": ["LABU"], "short": ["LABD"]},
    "ROBO": {"long": ["UBOT"], "short": []},
    "XHB": {"long": ["NAIL"], "short": []},
    "FNGS": {"long": ["FNGB"], "short": ["FNGD"]},
    "WCLD": {"long": ["CLDL"], "short": []},
    "XOP": {"long": ["GUSH"], "short": ["DRIP"]},
    "FDN": {"long": ["WEBL"], "short": ["WEBS"]},
    "FXI": {"long": ["YINN"], "short": ["YANG"]},
    "PEJ": {"long": ["OOTO"], "short": []},
    "USO": {"long": ["UCO"], "short": ["SCO"]},
    "PPH": {"long": ["PILL"], "short": []},
    "ITA": {"long": ["DFEN"], "short": []},
    "SLV": {"long": ["AGQ"], "short": ["ZSL"]},
    "GLD": {"long": ["UGL"], "short": ["GLL"]},
    "UNG": {"long": ["BOIL"], "short": ["KOLD"]},
    "GDX": {"long": ["NUGT", "GDXU"], "short": ["JDST", "GDXD"]},
    "IBIT": {"long": ["BITX", "BITU"], "short": ["SBIT", "BITI"]},
    "MSOS": {"long": ["MSOX"], "short": []},
    "REMX": {"long": [], "short": []},
    "EWY": {"long": ["KORU"], "short": []},
    "IEV": {"long": ["EURL"], "short": []},
    "EWJ": {"long": ["EZJ"], "short": []},
    "EWW": {"long": ["MEXX"], "short": []},
    "ASHR": {"long": ["CHAU"], "short": []},
    "INDA": {"long": ["INDL"], "short": []},
    "EEM": {"long": ["EDC"], "short": ["EDZ"]},
    "EWZ": {"long": ["BRZU"], "short": []}
}

SECTOR_COLORS = {
    "Information Technology": "#3f51b5", "Industrials": "#333", "Emerging Markets": "#00bcd4",
    "Consumer Discretionary": "#4caf50", "Health Care": "#e91e63", "Financials": "#ff5722",
    "Energy": "#795548", "Communication Services": "#9c27b0", "Real Estate": "#673ab7",
    "Commodities": "#8b6914", "Materials": "#ff9800", "Utilities": "#009688",
    "Consumer Staples": "#8bc34a", "Broad Market": "#9e9e9e",
}

Industries_COLORS = {
    "SMH": "#3f51b5", "ARKK": "#3f51b5", "XTN": "#333", "KWEB": "#00bcd4", "XRT": "#4caf50", "KRE": "#ff5722",
    "ARKF": "#3f51b5", "ARKG": "#e91e63", "BOAT": "#333", "DRIV": "#4caf50", "KBE": "#ff5722", "XES": "#795548",
    "XBI": "#e91e63", "OIH": "#795548", "SOCL": "#9c27b0", "ROBO": "#333", "AIQ": "#3f51b5", "XHB": "#4caf50",
    "FNGS": "#9e9e9e", "BLOK": "#3f51b5", "LIT": "#ff9800", "WCLD": "#3f51b5", "XOP": "#795548", "FDN": "#4caf50",
    "TAN": "#795548", "IBB": "#e91e63", "PAVE": "#333", "PEJ": "#4caf50", "KCE": "#ff5722", "XHE": "#e91e63",
    "IBUY": "#4caf50", "MSOS": "#4caf50", "FCG": "#795548", "JETS": "#4caf50", "IPAY": "#ff5722", "SLX": "#ff9800",
    "IGV": "#3f51b5", "CIBR": "#3f51b5", "PPH": "#e91e63", "IHI": "#e91e63", "UTES": "#009688",
    "ICLN": "#795548", "XME": "#ff9800", "IYZ": "#9c27b0", "URA": "#795548", "ITA": "#333", "VNQ": "#673ab7",
    "SCHH": "#673ab7", "KIE": "#ff5722", "REZ": "#673ab7", "CPER": "#8b6914", "PBJ": "#8bc34a", "SLV": "#8b6914",
    "GLD": "#8b6914", "SILJ": "#ff9800", "GDX": "#ff9800", "FXI": "#00bcd4", "GXC": "#00bcd4", "USO": "#8b6914",
    "DBA": "#8b6914", "UNG": "#8b6914", "DBC": "#8b6914", "WGMI": "#3f51b5", "REMX": "#ff9800",
}


def get_ticker_to_sector_mapping():
    color_to_sector = {c: s for s, c in SECTOR_COLORS.items()}
    return {t: color_to_sector.get(c, "Broad Market") for t, c in Industries_COLORS.items()}


TICKER_TO_SECTOR = get_ticker_to_sector_mapping()


def get_leveraged_etfs(ticker):
    if ticker in LEVERAGED_ETFS:
        return LEVERAGED_ETFS[ticker].get("long", []), LEVERAGED_ETFS[ticker].get("short", [])
    return [], []


def get_upcoming_key_events(days_ahead=7):
    if investpy is None:
        return []
    today = datetime.today()
    end_date = today + timedelta(days=days_ahead)
    from_date = today.strftime('%d/%m/%Y')
    to_date = end_date.strftime('%d/%m/%Y')
    try:
        calendar = investpy.news.economic_calendar(
            time_zone=None, time_filter='time_only', countries=['united states'],
            importances=['high'], categories=None, from_date=from_date, to_date=to_date
        )
        if calendar.empty:
            return []
        pattern = '|'.join(KEY_EVENTS)
        filtered = calendar[
            (calendar['event'].str.contains(pattern, case=False, na=False)) &
            (calendar['importance'].str.lower() == 'high')
        ]
        if filtered.empty:
            return []
        filtered = filtered.sort_values(['date', 'time'])
        return filtered[['date', 'time', 'event']].to_dict('records')
    except Exception as e:
        print("Economic calendar error:", e)
        return []


def calculate_atr(hist_data, period=14):
    try:
        hl = hist_data['High'] - hist_data['Low']
        hc = (hist_data['High'] - hist_data['Close'].shift()).abs()
        lc = (hist_data['Low'] - hist_data['Close'].shift()).abs()
        tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        return tr.ewm(alpha=1/period, adjust=False).mean().iloc[-1]
    except Exception:
        return None


def get_expected_move(ticker_sym, weekly=False):
    """Fetch ATM straddle EM% via yfinance.
    weekly=False (indices): nearest next trading day expiry (1-day EM).
    weekly=True  (others):  nearest Friday expiry (week-end EM).
    Returns (em_pct, days_to_expiry) or (None, None) on failure.
    """
    try:
        t = yf.Ticker(ticker_sym)
        expirations = t.options
        if not expirations:
            return None, None
        # Use US Eastern date (market timezone) to avoid UTC date-ahead issues
        from datetime import timezone as _tz, timedelta as _td
        _et = _tz(_td(hours=-4))  # EDT; close enough for date calc
        today = datetime.now(_tz.utc).astimezone(_et).date()
        exp_dates = [datetime.strptime(e, '%Y-%m-%d').date() for e in expirations]
        future = [d for d in exp_dates if d > today]
        if not future:
            return None, None

        if weekly:
            # Pick nearest Friday expiry
            fridays = [d for d in future if d.weekday() == 4]
            nearest_date = fridays[0] if fridays else future[0]
        else:
            # Pick nearest next-day expiry
            nearest_date = future[0]

        nearest = nearest_date.strftime('%Y-%m-%d')
        days = max((nearest_date - today).days, 1)
        chain = t.option_chain(nearest)
        calls, puts = chain.calls, chain.puts
        if calls.empty or puts.empty:
            return None, None
        # Use actual last close from history — fast_info.previousClose is stale (prior day)
        hist = t.history(period='5d')
        price = hist['Close'].iloc[-1] if not hist.empty else None
        if not price:
            price = t.fast_info.get('last_price') or t.fast_info.get('previousClose')
        if not price:
            return None, None
        # Find ATM from strikes common to both chains
        common_strikes = np.intersect1d(calls['strike'].values, puts['strike'].values)
        if len(common_strikes) == 0:
            return None, None
        atm = common_strikes[np.argmin(np.abs(common_strikes - price))]
        call_row = calls[calls['strike'] == atm]
        put_row  = puts[puts['strike']  == atm]
        if call_row.empty or put_row.empty:
            return None, None
        def mid(row):
            b, a = row['bid'].values[0], row['ask'].values[0]
            return (b + a) / 2 if (b > 0 or a > 0) else row['lastPrice'].values[0]
        straddle = mid(call_row) + mid(put_row)
        return round(straddle / price * 100, 2), days
    except Exception:
        return None, None


# ── Options Intelligence helpers ─────────────────────────────────────────────
OPTIONS_INTEL_TICKERS = ["SPY", "QQQ", "IWM", "DIA", "RSP", "IJH", "IJR", "GLD", "SLV"]


def _find_atm_strike(calls, puts, spot):
    """Find ATM strike from intersection of call/put strike grids."""
    common = np.intersect1d(calls['strike'].values, puts['strike'].values)
    if len(common) == 0:
        return None
    return common[np.argmin(np.abs(common - spot))]


def _bs_call_price(S, K, T, r, sigma):
    """Black-Scholes call price."""
    from scipy.stats import norm
    d1 = (math.log(S / K) + (r + sigma ** 2 / 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)


def _bs_put_price(S, K, T, r, sigma):
    """Black-Scholes put price."""
    from scipy.stats import norm
    d1 = (math.log(S / K) + (r + sigma ** 2 / 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


def _implied_vol(price, S, K, T, r, is_call=True):
    """Solve for IV from option price using bisection. Returns decimal (e.g. 0.18)."""
    if price <= 0 or T <= 0 or S <= 0 or K <= 0:
        return None
    bs_fn = _bs_call_price if is_call else _bs_put_price
    lo, hi = 0.01, 5.0
    for _ in range(60):
        mid = (lo + hi) / 2
        try:
            model_price = bs_fn(S, K, T, r, mid)
        except (ValueError, ZeroDivisionError):
            return None
        if model_price > price:
            hi = mid
        else:
            lo = mid
        if hi - lo < 0.0001:
            break
    result = (lo + hi) / 2
    return result if 0.01 < result < 4.99 else None


def _compute_atm_iv(calls, puts, spot, days_to_expiry):
    """Return ATM implied volatility as a percentage (e.g. 14.2).
    Tries yfinance IV first; falls back to computing from lastPrice.
    """
    atm = _find_atm_strike(calls, puts, spot)
    if atm is None:
        return None
    cr = calls[calls['strike'] == atm]
    pr = puts[puts['strike'] == atm]

    T = max(days_to_expiry, 1) / 365.0
    r = 0.05

    # Prefer computing from lastPrice (more reliable after hours)
    for row, is_call in [(cr, True), (pr, False)]:
        if not row.empty:
            lp = row['lastPrice'].values[0]
            if lp and lp > 0:
                iv = _implied_vol(lp, spot, atm, T, r, is_call)
                if iv and iv > 0.02:
                    return round(iv * 100, 1)

    # Fall back to yfinance IV (only if clearly real, not placeholder)
    for row in [cr, pr]:
        if not row.empty:
            iv = row['impliedVolatility'].values[0]
            if iv and iv > 0.05:
                return round(iv * 100, 1)
    return None


def _compute_pcr(calls, puts):
    """Compute put-call ratios by OI and volume."""
    call_oi = calls['openInterest'].fillna(0).sum()
    put_oi = puts['openInterest'].fillna(0).sum()
    call_vol = calls['volume'].fillna(0).sum()
    put_vol = puts['volume'].fillna(0).sum()
    return {
        "oi": round(put_oi / call_oi, 2) if call_oi > 0 else None,
        "vol": round(put_vol / call_vol, 2) if call_vol > 0 else None,
        "call_oi": int(call_oi), "put_oi": int(put_oi),
        "call_vol": int(call_vol), "put_vol": int(put_vol),
    }


def _compute_max_pain(calls, puts, spot):
    """Find max pain strike (minimizes total option payout).
    Uses OI if available; falls back to volume as a proxy.
    """
    # Decide which weight column to use
    call_oi_sum = calls['openInterest'].fillna(0).sum()
    put_oi_sum = puts['openInterest'].fillna(0).sum()
    weight_col = 'openInterest' if (call_oi_sum + put_oi_sum) > 0 else 'volume'

    call_data = calls[['strike', weight_col]].copy()
    put_data = puts[['strike', weight_col]].copy()
    call_data[weight_col] = call_data[weight_col].fillna(0)
    put_data[weight_col] = put_data[weight_col].fillna(0)

    # Filter to strikes within 10% of spot (avoid junk far-OTM)
    lo, hi = spot * 0.90, spot * 1.10
    call_data = call_data[(call_data['strike'] >= lo) & (call_data['strike'] <= hi)]
    put_data = put_data[(put_data['strike'] >= lo) & (put_data['strike'] <= hi)]

    all_strikes = sorted(set(call_data['strike'].values) | set(put_data['strike'].values))
    if not all_strikes:
        return None

    call_w = dict(zip(call_data['strike'], call_data[weight_col]))
    put_w = dict(zip(put_data['strike'], put_data[weight_col]))

    min_pain = float('inf')
    mp_strike = all_strikes[0]
    for K in all_strikes:
        total = 0
        for s, w in call_w.items():
            if K > s:
                total += w * (K - s) * 100
        for s, w in put_w.items():
            if K < s:
                total += w * (s - K) * 100
        if total < min_pain:
            min_pain = total
            mp_strike = K
    return {
        "strike": mp_strike,
        "dist_pct": round((mp_strike - spot) / spot * 100, 2),
        "source": "OI" if weight_col == 'openInterest' else "volume",
    }


def _compute_gex(calls, puts, spot, days_to_expiry):
    """Estimate dealer Gamma Exposure per strike using Black-Scholes gamma.
    Uses OI for weighting; falls back to volume if OI is all zeros.
    Computes IV from lastPrice if yfinance IV is stale.
    """
    T = max(days_to_expiry, 1) / 365.0
    r = 0.05
    sqrt_T = math.sqrt(T)
    two_pi_sqrt = math.sqrt(2 * math.pi)

    # Decide weight column
    total_oi = calls['openInterest'].fillna(0).sum() + puts['openInterest'].fillna(0).sum()
    weight_col = 'openInterest' if total_oi > 0 else 'volume'

    gex_by_strike = {}
    for side, df, sign in [('call', calls, 1), ('put', puts, -1)]:
        is_call = (side == 'call')
        for _, row in df.iterrows():
            K = row['strike']
            w = row.get(weight_col) or 0
            if w <= 0 or K <= 0:
                continue
            # Filter to ±15% of spot
            if K < spot * 0.85 or K > spot * 1.15:
                continue
            # Compute IV from lastPrice (more reliable after hours)
            iv = 0
            lp = row.get('lastPrice') or 0
            if lp > 0:
                iv = _implied_vol(lp, spot, K, T, r, is_call) or 0
            if iv < 0.02:
                iv = row.get('impliedVolatility') or 0
            if iv < 0.01:
                continue
            try:
                d1 = (math.log(spot / K) + (r + iv * iv / 2) * T) / (iv * sqrt_T)
                gamma = math.exp(-d1 * d1 / 2) / (two_pi_sqrt * spot * iv * sqrt_T)
                gex_val = sign * gamma * w * 100 * spot / 1e7
                gex_by_strike[K] = gex_by_strike.get(K, 0) + gex_val
            except (ValueError, ZeroDivisionError):
                continue

    if not gex_by_strike:
        return None

    # Filter NaN values
    gex_by_strike = {k: v for k, v in gex_by_strike.items() if not math.isnan(v)}
    if not gex_by_strike:
        return None

    net_gex = round(sum(gex_by_strike.values()), 2)

    # Max gamma strike
    max_k = max(gex_by_strike, key=lambda k: abs(gex_by_strike[k]))

    # Gamma flip: where cumulative GEX crosses zero (scan from low to high strike)
    sorted_strikes = sorted(gex_by_strike.keys())
    cum = 0
    gamma_flip = None
    for k in sorted_strikes:
        prev_cum = cum
        cum += gex_by_strike[k]
        if prev_cum * cum < 0:  # sign changed
            gamma_flip = k
            break

    return {
        "net_gex": net_gex,
        "max_gamma_strike": max_k,
        "gamma_flip": gamma_flip,
        "gamma_flip_dist_pct": round((gamma_flip - spot) / spot * 100, 2) if gamma_flip else None,
    }


def _compute_iv_skew(calls, puts, spot, days_to_expiry):
    """Compare IV of 5% OTM puts vs 5% OTM calls.
    Computes IV from lastPrice if yfinance IV is stale.
    """
    put_target = spot * 0.95
    call_target = spot * 1.05
    T = max(days_to_expiry, 1) / 365.0
    r = 0.05

    def get_iv_at_strike(df, target_strike, is_call):
        if df.empty:
            return None
        idx = (df['strike'] - target_strike).abs().idxmin()
        row = df.loc[idx]
        # Always try computing from lastPrice first (more reliable after hours)
        lp = row.get('lastPrice') or 0
        K = row['strike']
        if lp > 0 and K > 0:
            computed = _implied_vol(lp, spot, K, T, r, is_call)
            if computed and computed > 0.02:
                return computed * 100
        # Fall back to yfinance IV
        iv = row.get('impliedVolatility') or 0
        if iv > 0.05:
            return iv * 100
        return None

    put_iv = get_iv_at_strike(puts, put_target, False)
    call_iv = get_iv_at_strike(calls, call_target, True)
    if put_iv is None or call_iv is None:
        return None

    return {
        "skew": round(put_iv - call_iv, 1),
        "put_iv": round(put_iv, 1),
        "call_iv": round(call_iv, 1),
    }


def build_options_intel_opend(tickers, ticker_data=None):
    """OpenD-sourced options intel. Same schema as build_options_intel() plus:
      - dex (net dollar delta exposure)
      - gex.gex_by_strike[]   — per-strike GEX bars for chart
      - iv_skew.iv_curve[]    — per-strike IV (call & put) for smile chart

    Greeks (delta/gamma) and IV come straight from the broker — no Black-Scholes
    recomputation needed. Returns {} on any total failure so caller can fall
    back to yfinance build_options_intel().
    """
    try:
        from fetch_opend import fetch_options_intel_opend
    except Exception as e:
        print(f"  [opend-opts] import failed: {e}")
        return {}

    # Spot from already-fetched in-memory cache (no extra fetch)
    def spot_lookup(t):
        row = (ticker_data or {}).get(t) or {}
        return row.get("last_close")

    raw = fetch_options_intel_opend(tickers, spot_lookup=spot_lookup, verbose=True)
    if not raw:
        return {}

    result = {}
    for sym, payload in raw.items():
        try:
            spot = payload["spot"]
            days = payload["days"]
            contracts = payload["contracts"]
            calls = [c for c in contracts if c["type"] == "CALL"]
            puts  = [c for c in contracts if c["type"] == "PUT"]
            if not calls or not puts:
                continue

            # ATM strike = call closest to spot among calls (calls and puts share strike grid)
            strikes = sorted({c["strike"] for c in contracts})
            atm = min(strikes, key=lambda k: abs(k - spot))
            atm_cs = [c for c in calls if c["strike"] == atm]
            atm_ps = [p for p in puts  if p["strike"] == atm]
            atm_iv_vals = [c["iv"] for c in (atm_cs + atm_ps) if c["iv"] > 0]
            atm_iv = round(sum(atm_iv_vals) / len(atm_iv_vals) * 100, 1) if atm_iv_vals else None

            # PCR
            call_oi  = sum(c["oi"] for c in calls)
            put_oi   = sum(p["oi"] for p in puts)
            call_vol = sum(c["volume"] for c in calls)
            put_vol  = sum(p["volume"] for p in puts)
            pcr = {
                "oi":  round(put_oi / call_oi, 2) if call_oi > 0 else None,
                "vol": round(put_vol / call_vol, 2) if call_vol > 0 else None,
                "call_oi": int(call_oi), "put_oi": int(put_oi),
                "call_vol": int(call_vol), "put_vol": int(put_vol),
            }

            # Max pain (OI weighting; only strikes ±10% of spot)
            mp_lo, mp_hi = spot * 0.90, spot * 1.10
            mp_strikes = [k for k in strikes if mp_lo <= k <= mp_hi]
            max_pain = None
            if mp_strikes:
                call_w = {c["strike"]: c["oi"] for c in calls}
                put_w  = {p["strike"]: p["oi"] for p in puts}
                weight_total = sum(call_w.values()) + sum(put_w.values())
                if weight_total <= 0:
                    call_w = {c["strike"]: c["volume"] for c in calls}
                    put_w  = {p["strike"]: p["volume"] for p in puts}
                    src = "volume"
                else:
                    src = "OI"
                min_pain, mp_k = float("inf"), mp_strikes[0]
                for K in mp_strikes:
                    total = 0
                    for s, w in call_w.items():
                        if K > s: total += w * (K - s) * 100
                    for s, w in put_w.items():
                        if K < s: total += w * (s - K) * 100
                    if total < min_pain:
                        min_pain = total; mp_k = K
                max_pain = {
                    "strike": mp_k,
                    "dist_pct": round((mp_k - spot) / spot * 100, 2),
                    "source": src,
                }

            # GEX per strike (dealers short customer positions; sign convention:
            # calls long-gamma to dealer means positive GEX, puts negative)
            # Using contract gamma from broker × OI × 100 × spot² scaled to $-millions / 1pt move.
            gex_by_strike_dict = {}
            dex_total = 0.0
            for c in calls:
                if c["oi"] <= 0 or c["gamma"] <= 0: continue
                gex_by_strike_dict[c["strike"]] = gex_by_strike_dict.get(c["strike"], 0) + (
                    c["gamma"] * c["oi"] * 100 * spot * spot / 1e9
                )
                dex_total += c["delta"] * c["oi"] * 100 * spot / 1e9
            for p in puts:
                if p["oi"] <= 0 or p["gamma"] <= 0: continue
                gex_by_strike_dict[p["strike"]] = gex_by_strike_dict.get(p["strike"], 0) - (
                    p["gamma"] * p["oi"] * 100 * spot * spot / 1e9
                )
                dex_total += p["delta"] * p["oi"] * 100 * spot / 1e9   # puts already have negative delta

            gex_by_strike = sorted(gex_by_strike_dict.items())
            gex = None
            if gex_by_strike:
                net_gex = round(sum(v for _, v in gex_by_strike), 2)
                max_k = max(gex_by_strike_dict, key=lambda k: abs(gex_by_strike_dict[k]))
                # Gamma flip: cumulative GEX zero-crossing scanning low→high
                cum = 0; gamma_flip = None
                for k, v in gex_by_strike:
                    prev = cum; cum += v
                    if prev * cum < 0:
                        gamma_flip = k; break
                gex = {
                    "net_gex": net_gex,
                    "max_gamma_strike": max_k,
                    "gamma_flip": gamma_flip,
                    "gamma_flip_dist_pct": round((gamma_flip - spot) / spot * 100, 2) if gamma_flip else None,
                    "gex_by_strike": [{"k": round(k, 2), "g": round(v, 3)} for k, v in gex_by_strike],
                }

            # IV skew: 5% OTM put vs 5% OTM call
            put_target = spot * 0.95
            call_target = spot * 1.05
            def closest(side_list, target):
                if not side_list: return None
                return min(side_list, key=lambda c: abs(c["strike"] - target))
            p_pick = closest(puts, put_target)
            c_pick = closest(calls, call_target)
            iv_skew = None
            iv_curve = []
            # Build IV curve: per strike, take call IV and put IV side-by-side
            calls_by_k = {c["strike"]: c for c in calls}
            puts_by_k  = {p["strike"]: p for p in puts}
            for k in strikes:
                ck = calls_by_k.get(k); pk = puts_by_k.get(k)
                iv_curve.append({
                    "k": round(k, 2),
                    "iv_call": round(ck["iv"] * 100, 2) if ck and ck["iv"] > 0 else None,
                    "iv_put":  round(pk["iv"] * 100, 2) if pk and pk["iv"] > 0 else None,
                })
            if p_pick and c_pick and p_pick["iv"] > 0 and c_pick["iv"] > 0:
                put_iv = p_pick["iv"] * 100
                call_iv = c_pick["iv"] * 100
                iv_skew = {
                    "skew": round(put_iv - call_iv, 1),
                    "put_iv": round(put_iv, 1),
                    "call_iv": round(call_iv, 1),
                    "iv_curve": iv_curve,
                }

            # DEX summary
            dex = {
                "net_dex": round(dex_total, 2),  # in $-billions per 1pt spot move (decomposed)
            }

            result[sym] = {
                "spot": round(float(spot), 2),
                "expiry_used": payload["expiry"],
                "days_to_expiry": days,
                "atm_iv": atm_iv,
                "pcr": pcr,
                "max_pain": max_pain,
                "gex": gex,
                "dex": dex,
                "iv_skew": iv_skew,
                "source": "opend",
            }
            print(f"  {sym}: IV={atm_iv}%, PCR(OI)={pcr['oi']}, "
                  f"GEX={gex['net_gex'] if gex else 'n/a'}, "
                  f"DEX={dex['net_dex']}, "
                  f"Skew={iv_skew['skew'] if iv_skew else 'n/a'}")
        except Exception as e:
            print(f"  {sym}: opend aggregate failed: {e}")
            continue

    return result


def build_options_intel(tickers=None):
    """Compute options intelligence metrics for index ETFs."""
    if tickers is None:
        tickers = OPTIONS_INTEL_TICKERS

    from datetime import timezone as _tz, timedelta as _td
    _et = _tz(_td(hours=-4))
    today = datetime.now(_tz.utc).astimezone(_et).date()

    result = {}
    for sym in tickers:
        try:
            t = yf.Ticker(sym)
            expirations = t.options
            if not expirations:
                print(f"  {sym}: no options expirations")
                continue

            exp_dates = [datetime.strptime(e, '%Y-%m-%d').date() for e in expirations]
            future = [d for d in exp_dates if d > today]
            if not future:
                continue

            # Prefer an expiry 3-14 days out for better OI/IV data
            # Fall back to nearest if nothing in that range
            preferred = [d for d in future if 3 <= (d - today).days <= 14]
            nearest_date = preferred[0] if preferred else future[0]
            nearest = nearest_date.strftime('%Y-%m-%d')
            days = max((nearest_date - today).days, 1)

            chain = t.option_chain(nearest)
            calls, puts = chain.calls, chain.puts
            if calls.empty or puts.empty:
                continue

            # Spot price from history (same fix as get_expected_move)
            hist = t.history(period='5d')
            spot = hist['Close'].iloc[-1] if not hist.empty else None
            if not spot:
                spot = t.fast_info.get('last_price') or t.fast_info.get('previousClose')
            if not spot:
                continue

            atm_iv = _compute_atm_iv(calls, puts, spot, days)
            pcr = _compute_pcr(calls, puts)
            max_pain = _compute_max_pain(calls, puts, spot)
            gex = _compute_gex(calls, puts, spot, days)
            iv_skew = _compute_iv_skew(calls, puts, spot, days)

            result[sym] = {
                "spot": round(float(spot), 2),
                "expiry_used": nearest,
                "days_to_expiry": days,
                "atm_iv": atm_iv,
                "pcr": pcr,
                "max_pain": max_pain,
                "gex": gex,
                "iv_skew": iv_skew,
            }
            iv_str = f"IV={atm_iv}%" if atm_iv else "IV=n/a"
            pcr_str = f"PCR(OI)={pcr.get('oi','n/a')}" if pcr else "PCR=n/a"
            mp_str = f"MaxPain=${max_pain['strike']}" if max_pain else "MaxPain=n/a"
            print(f"  {sym}: {iv_str}, {pcr_str}, {mp_str}")
        except Exception as e:
            print(f"  {sym}: options intel failed: {e}")
        time.sleep(0.3)

    return result


def calculate_rrs(stock_data, spy_data, atr_length=14, length_rolling=50, length_sma=20, atr_multiplier=1.0):
    try:
        merged = pd.merge(
            stock_data[['High', 'Low', 'Close']], spy_data[['High', 'Low', 'Close']],
            left_index=True, right_index=True, suffixes=('_stock', '_spy'), how='inner'
        )
        if len(merged) < atr_length + 1:
            return None
        for prefix in ['stock', 'spy']:
            h, l, c = merged[f'High_{prefix}'], merged[f'Low_{prefix}'], merged[f'Close_{prefix}']
            tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
            merged[f'atr_{prefix}'] = tr.ewm(alpha=1/atr_length, adjust=False).mean()
        sc = merged['Close_stock'] - merged['Close_stock'].shift(1)
        spy_c = merged['Close_spy'] - merged['Close_spy'].shift(1)
        spy_pi = spy_c / merged['atr_spy']
        expected = spy_pi * merged['atr_stock'] * atr_multiplier
        rrs = (sc - expected) / merged['atr_stock']
        rolling_rrs = rrs.rolling(window=length_rolling, min_periods=1).mean()
        rrs_sma = rolling_rrs.rolling(window=length_sma, min_periods=1).mean()
        return pd.DataFrame({'RRS': rrs, 'rollingRRS': rolling_rrs, 'RRS_SMA': rrs_sma}, index=merged.index)
    except Exception:
        return None


def calculate_sma(hist_data, period=50):
    try:
        return hist_data['Close'].rolling(window=period).mean().iloc[-1]
    except Exception:
        return None


def calculate_ema(hist_data, period=10):
    try:
        return hist_data['Close'].ewm(span=period, adjust=False).mean().iloc[-1]
    except Exception:
        return None


def calculate_sr_levels(hist_data, current_price):
    """Detect S/R levels via swing high/low peak detection, merge nearby levels, score by touch count."""
    try:
        highs = hist_data['High'].values.astype(float)
        lows = hist_data['Low'].values.astype(float)
        closes = hist_data['Close'].values.astype(float)
        if len(highs) < 30 or current_price <= 0:
            return None

        PROX_PCT = 0.015   # merge levels within 1.5% of each other
        TOUCH_PCT = 0.005  # count a touch if any OHLC value is within 0.5%

        peaks, _ = find_peaks(highs, distance=5)
        troughs, _ = find_peaks(-lows, distance=5)
        raw = sorted([float(highs[i]) for i in peaks] + [float(lows[i]) for i in troughs])
        if not raw:
            return None

        # Merge nearby levels into clusters
        merged = []
        for lvl in raw:
            if merged and abs(lvl - merged[-1]['p']) / merged[-1]['p'] < PROX_PCT:
                total = merged[-1]['p'] * merged[-1]['c'] + lvl
                merged[-1]['c'] += 1
                merged[-1]['p'] = total / merged[-1]['c']
            else:
                merged.append({'p': lvl, 'c': 1})

        # Score each level by how many OHLC values touched it
        all_vals = np.concatenate([highs, lows, closes])
        for m in merged:
            m['score'] = int(np.sum(np.abs(all_vals - m['p']) / m['p'] < TOUCH_PCT))

        # Split support (below) / resistance (above), top 3 by score each, nearest-first
        sup = sorted([m for m in merged if m['p'] < current_price * 0.998], key=lambda x: -x['score'])[:3]
        res = sorted([m for m in merged if m['p'] > current_price * 1.002], key=lambda x: -x['score'])[:3]
        sup.sort(key=lambda x: -x['p'])   # nearest support first
        res.sort(key=lambda x: x['p'])    # nearest resistance first

        def fmt(m):
            return {'price': round(m['p'], 2), 'score': m['score'],
                    'dist_pct': round((m['p'] - current_price) / current_price * 100, 1)}

        result = {'support': [fmt(m) for m in sup], 'resistance': [fmt(m) for m in res]}

        # Near alert: within 2% of a level
        near = None
        for m in res:
            if (m['p'] - current_price) / current_price <= 0.02:
                near = {'type': 'resistance', 'price': round(m['p'], 2),
                        'dist_pct': round((m['p'] - current_price) / current_price * 100, 1)}
                break
        if not near:
            for m in sup:
                if (current_price - m['p']) / current_price <= 0.02:
                    near = {'type': 'support', 'price': round(m['p'], 2),
                            'dist_pct': round((m['p'] - current_price) / current_price * 100, 1)}
                    break
        if near:
            result['near'] = near
        return result
    except Exception:
        return None


def calculate_abc_rating(hist_data):
    try:
        ema10 = calculate_ema(hist_data, 10)
        ema20 = calculate_ema(hist_data, 20)
        sma50 = calculate_sma(hist_data, 50)
        if ema10 is None or ema20 is None or sma50 is None:
            return None
        if ema10 > ema20 and ema20 > sma50:
            return "A"
        if (ema10 > ema20 and ema20 < sma50) or (ema10 < ema20 and ema20 > sma50):
            return "B"
        if ema10 < ema20 and ema20 < sma50:
            return "C"
    except Exception:
        pass
    return None


def calculate_stage(hist_data):
    """Weinstein Stage: 1=Basing, 2=Advancing, 3=Topping, 4=Declining.
    Based on price vs 30-week MA (SMA150) and MA slope."""
    try:
        close = hist_data['Close']
        if len(close) < 150:
            return None
        sma150 = close.rolling(150).mean()
        cur = close.iloc[-1]
        ma = sma150.iloc[-1]
        ma_prev = sma150.iloc[-5]  # MA slope over ~1 week
        ma_slope = (ma - ma_prev) / ma_prev if ma_prev else 0

        above = cur > ma
        ma_rising = ma_slope > 0.001
        ma_falling = ma_slope < -0.001

        if above and ma_rising:      return 2  # Advancing
        if above and not ma_rising:   return 3  # Topping
        if not above and ma_falling:  return 4  # Declining
        return 1                                # Basing
    except Exception:
        return None


def create_vol_chart_png(vol_history, ticker, charts_dir):
    try:
        if not vol_history or len(vol_history) == 0:
            return None
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(8, 2))
        fig.patch.set_facecolor('#1a1a1a')
        ax.set_facecolor('#1a1a1a')
        bar_colors = ['#ef4444' if v >= 2.0 else '#b0b0b0' for v in vol_history]
        ax.bar(range(len(vol_history)), vol_history, color=bar_colors, width=0.8, edgecolor='none')
        ax.axhline(y=1.0, color='#808080', linestyle='--', linewidth=1)
        ax.set_xticks([])
        ax.set_yticks([])
        for s in ax.spines.values():
            s.set_visible(False)
        fig.tight_layout(pad=0)
        safe = re.sub(r'[^a-zA-Z0-9]', '_', ticker)
        path = os.path.join(charts_dir, f"vol_{safe}.png")
        fig.savefig(path, format='png', dpi=80, bbox_inches='tight', facecolor='#1a1a1a')
        plt.close(fig)
        return f"data/charts/vol_{safe}.png"
    except Exception as e:
        print("Vol chart error", ticker, e)
        return None

def create_rs_chart_png(rrs_data, ticker, charts_dir):
    try:
        recent = rrs_data.tail(20)
        if len(recent) == 0:
            return None
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(8, 2))
        fig.patch.set_facecolor('#1a1a1a')
        ax.set_facecolor('#1a1a1a')
        rolling_rrs = recent['rollingRRS'].values
        rrs_sma = recent['RRS_SMA'].values
        max_idx = rolling_rrs.argmax()
        bar_colors = ['#4ade80' if i == max_idx else '#b0b0b0' for i in range(len(rolling_rrs))]
        ax.bar(range(len(rolling_rrs)), rolling_rrs, color=bar_colors, width=0.8, edgecolor='none')
        ax.plot(range(len(rrs_sma)), rrs_sma, color='yellow', lw=2)
        ax.axhline(y=0, color='#808080', linestyle='--', linewidth=1)
        mn = min(rolling_rrs.min(), rrs_sma.min() if len(rrs_sma) else 0)
        mx = max(rolling_rrs.max(), rrs_sma.max() if len(rrs_sma) else 0)
        pad = 0.1 if mn == mx else (mx - mn) * 0.2
        ax.set_ylim(mn - pad, mx + pad)
        ax.set_xticks([])
        ax.set_yticks([])
        for s in ax.spines.values():
            s.set_visible(False)
        fig.tight_layout(pad=0)
        safe = re.sub(r'[^a-zA-Z0-9]', '_', ticker)
        path = os.path.join(charts_dir, f"{safe}.png")
        fig.savefig(path, format='png', dpi=80, bbox_inches='tight', facecolor='#1a1a1a')
        plt.close(fig)
        return f"data/charts/{safe}.png"
    except Exception as e:
        print("Chart error", ticker, e)
        return None


# Distinct colors for RRG groups (visible on dark background); trails use same color per group
RRG_COLORS = [
    "#06b6d4", "#22c55e", "#eab308", "#f97316", "#ef4444", "#a855f7", "#ec4899",
    "#14b8a6", "#84cc16", "#f59e0b", "#f43f5e", "#8b5cf6", "#d946ef", "#0ea5e9", "#2dd4bf",
    "#a3e635", "#fb923c", "#f87171", "#c084fc", "#f472b6",
]

def create_rrg_chart_png(rrg_points, charts_dir, trails=None):
    """Create a Relative Rotation Graph scatter plot (RS-Ratio vs RS-Momentum) for the 7s groups.
    rrg_points: list of dicts with keys name, rs_ratio_norm, rs_momentum_norm (0-100 scale).
    trails: optional list of (name, [(x1,y1), (x2,y2), ...]) for each group, oldest to newest; draws lines showing path."""
    try:
        if not rrg_points or len(rrg_points) == 0:
            return None
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(10, 8))
        fig.patch.set_facecolor('#1a1a1a')
        ax.set_facecolor('#1a1a1a')

        # Subtle quadrant shading (RRG convention)
        # Top-right: Leading, Top-left: Weakening, Bottom-left: Lagging, Bottom-right: Improving
        ax.axvspan(50, 100, ymin=0.5, ymax=1.0, facecolor="#22c55e", alpha=0.06, zorder=0)  # Leading
        ax.axvspan(0, 50, ymin=0.5, ymax=1.0, facecolor="#eab308", alpha=0.06, zorder=0)   # Weakening
        ax.axvspan(0, 50, ymin=0.0, ymax=0.5, facecolor="#ef4444", alpha=0.05, zorder=0)   # Lagging
        ax.axvspan(50, 100, ymin=0.0, ymax=0.5, facecolor="#06b6d4", alpha=0.05, zorder=0) # Improving

        # Data-driven axis limits so points spread out instead of cramming into 0-100
        all_x = [p["rs_ratio_norm"] for p in rrg_points]
        all_y = [p["rs_momentum_norm"] for p in rrg_points]
        if trails:
            for _name, path in trails:
                for (px, py) in path:
                    all_x.append(px)
                    all_y.append(py)
        x_min, x_max = min(all_x), max(all_x)
        y_min, y_max = min(all_y), max(all_y)
        x_pad = max((x_max - x_min) * 0.12, 5)
        y_pad = max((y_max - y_min) * 0.12, 5)
        x_lo = max(0, x_min - x_pad)
        x_hi = min(100, x_max + x_pad)
        y_lo = max(0, y_min - y_pad)
        y_hi = min(100, y_max + y_pad)
        if x_hi - x_lo < 10:
            x_lo, x_hi = max(0, (x_lo + x_hi) / 2 - 15), min(100, (x_lo + x_hi) / 2 + 15)
        if y_hi - y_lo < 10:
            y_lo, y_hi = max(0, (y_lo + y_hi) / 2 - 15), min(100, (y_lo + y_hi) / 2 + 15)
        x_mid = (x_lo + x_hi) / 2
        y_mid = (y_lo + y_hi) / 2

        # Quadrant shading and dividers at view center (so quadrants stay meaningful when zoomed)
        ax.set_xlim(x_lo, x_hi)
        ax.set_ylim(y_lo, y_hi)
        ax.axvspan(x_mid, x_hi, (y_mid - y_lo) / (y_hi - y_lo) if y_hi != y_lo else 0.5, 1.0, facecolor="#22c55e", alpha=0.06, zorder=0)
        ax.axvspan(x_lo, x_mid, (y_mid - y_lo) / (y_hi - y_lo) if y_hi != y_lo else 0.5, 1.0, facecolor="#eab308", alpha=0.06, zorder=0)
        ax.axvspan(x_lo, x_mid, 0.0, (y_mid - y_lo) / (y_hi - y_lo) if y_hi != y_lo else 0.5, facecolor="#ef4444", alpha=0.05, zorder=0)
        ax.axvspan(x_mid, x_hi, 0.0, (y_mid - y_lo) / (y_hi - y_lo) if y_hi != y_lo else 0.5, facecolor="#06b6d4", alpha=0.05, zorder=0)
        ax.axvline(x=x_mid, color="#4a5568", linestyle="--", linewidth=1)
        ax.axhline(y=y_mid, color="#4a5568", linestyle="--", linewidth=1)
        ax.text((x_mid + x_hi) / 2, (y_mid + y_hi) / 2, "Leading", color="#e0e0e0", alpha=0.5, fontsize=11, fontweight="bold", ha="center", va="center", zorder=0)
        ax.text((x_lo + x_mid) / 2, (y_mid + y_hi) / 2, "Weakening", color="#e0e0e0", alpha=0.5, fontsize=11, fontweight="bold", ha="center", va="center", zorder=0)
        ax.text((x_lo + x_mid) / 2, (y_lo + y_mid) / 2, "Lagging", color="#e0e0e0", alpha=0.5, fontsize=11, fontweight="bold", ha="center", va="center", zorder=0)
        ax.text((x_mid + x_hi) / 2, (y_lo + y_mid) / 2, "Improving", color="#e0e0e0", alpha=0.5, fontsize=11, fontweight="bold", ha="center", va="center", zorder=0)

        # Build name -> index for consistent colors between trails and dots
        name_to_idx = {p["name"]: i for i, p in enumerate(rrg_points)}

        # Draw trail lines: different color per group; old segments more transparent, recent more vivid
        if trails:
            for name, path in trails:
                if not path or len(path) < 2:
                    continue
                idx = name_to_idx.get(name, 0)
                color = RRG_COLORS[idx % len(RRG_COLORS)]
                n = len(path)
                for j in range(n - 1):
                    # Alpha increases along trail: old (start) ~0.15, recent (end) ~0.9
                    alpha = 0.15 + 0.75 * (j + 1) / n
                    ax.plot([path[j][0], path[j + 1][0]], [path[j][1], path[j + 1][1]],
                            color=color, alpha=alpha, linewidth=1.2, zorder=1)
                ax.scatter([path[0][0]], [path[0][1]], c=color, s=18, alpha=0.25, zorder=2, edgecolors="none")
                # Arrowhead on most recent segment (shows direction "where it's going")
                try:
                    x0, y0 = path[-2]
                    x1, y1 = path[-1]
                    ax.annotate(
                        "",
                        xy=(x1, y1),
                        xytext=(x0, y0),
                        arrowprops=dict(arrowstyle="-|>", color=color, lw=2.2, alpha=0.95, shrinkA=0, shrinkB=2),
                        zorder=4,
                    )
                except Exception:
                    pass

        xs = [p["rs_ratio_norm"] for p in rrg_points]
        ys = [p["rs_momentum_norm"] for p in rrg_points]
        names = [p["name"] for p in rrg_points]
        colors = [RRG_COLORS[name_to_idx.get(n, 0) % len(RRG_COLORS)] for n in names]
        ax.scatter(xs, ys, c=colors, s=85, edgecolors="#e0e0e0", linewidths=1.2, zorder=3, alpha=0.95)
        for i, label in enumerate(names):
            short = label.replace("The ", "").replace(" 7", "") if label.startswith("The ") and label.endswith(" 7") else label[:12]
            ax.annotate(short, (xs[i], ys[i]), xytext=(6, 6), textcoords="offset points", fontsize=8, color="#e0e0e0")
        ax.set_xlabel("RS-Ratio (3M vs SPY)", color="#9ca3af", fontsize=10)
        ax.set_ylabel("RS-Momentum (1M vs SPY)", color="#9ca3af", fontsize=10)
        ax.tick_params(colors="#9ca3af")
        for s in ax.spines.values():
            s.set_color("#4a5568")
        ax.set_title("7s Rotation Graph (3M / 1M vs SPY) — lines show path over last ~30 days", color="#e0e0e0", fontsize=12)
        fig.tight_layout()
        path = os.path.join(charts_dir, "seven_rrg.png")
        fig.savefig(path, format="png", dpi=100, bbox_inches="tight", facecolor="#1a1a1a")
        plt.close(fig)
        return "data/charts/seven_rrg.png"
    except Exception as e:
        print("RRG chart error", e)
        return None


def compute_trendlines(highs, lows, closes, dates, lookback=90):
    """Find 1 rising support + 1 falling resistance trendline from pivot points."""
    n = min(lookback, len(highs))
    if n < 15:
        return None
    h = np.array(highs[-n:], dtype=float)
    l = np.array(lows[-n:], dtype=float)
    c = np.array(closes[-n:], dtype=float)
    d = list(dates[-n:])

    # Find pivot lows (local minima with 2-bar window for more candidates)
    pivot_lows = []
    for i in range(2, n - 2):
        if l[i] <= min(l[i-2:i]) and l[i] <= min(l[i+1:i+3]):
            pivot_lows.append((i, l[i]))

    # Find pivot highs (local maxima with 2-bar window)
    pivot_highs = []
    for i in range(2, n - 2):
        if h[i] >= max(h[i-2:i]) and h[i] >= max(h[i+1:i+3]):
            pivot_highs.append((i, h[i]))

    result = {}

    # Support: best line through pivot lows (any slope)
    best_support = None
    best_support_score = -1
    for a in range(len(pivot_lows)):
        for b in range(a + 1, len(pivot_lows)):
            i1, p1 = pivot_lows[a]
            i2, p2 = pivot_lows[b]
            if i2 - i1 < 3:
                continue
            slope = (p2 - p1) / (i2 - i1)
            # Allow violations — count how many bars close below line
            violations = 0
            touches = 0
            for k in range(i1, n):
                line_val = p1 + slope * (k - i1)
                if line_val <= 0:
                    continue
                if c[k] < line_val * 0.99:
                    violations += 1
                if abs(l[k] - line_val) / line_val < 0.015:
                    touches += 1
            # Allow up to 15% of bars as violations
            max_violations = max(2, int((n - i1) * 0.15))
            if violations <= max_violations and touches >= 2:
                score = touches * 2 - violations + (i2 / n) + (i2 - i1) / n
                if score > best_support_score:
                    best_support_score = score
                    p_end = p1 + slope * (n - 1 - i1)
                    best_support = {"t1": d[i1], "p1": round(p1, 2), "t2": d[n-1], "p2": round(p_end, 2)}

    # Resistance: best line through pivot highs (any slope)
    best_resist = None
    best_resist_score = -1
    for a in range(len(pivot_highs)):
        for b in range(a + 1, len(pivot_highs)):
            i1, p1 = pivot_highs[a]
            i2, p2 = pivot_highs[b]
            if i2 - i1 < 3:
                continue
            slope = (p2 - p1) / (i2 - i1)
            violations = 0
            touches = 0
            for k in range(i1, n):
                line_val = p1 + slope * (k - i1)
                if line_val <= 0:
                    continue
                if c[k] > line_val * 1.01:
                    violations += 1
                if abs(h[k] - line_val) / line_val < 0.015:
                    touches += 1
            max_violations = max(2, int((n - i1) * 0.15))
            if violations <= max_violations and touches >= 2:
                score = touches * 2 - violations + (i2 / n) + (i2 - i1) / n
                if score > best_resist_score:
                    best_resist_score = score
                    p_end = p1 + slope * (n - 1 - i1)
                    best_resist = {"t1": d[i1], "p1": round(p1, 2), "t2": d[n-1], "p2": round(p_end, 2)}

    if best_support:
        result["support"] = best_support
    if best_resist:
        result["resistance"] = best_resist
    return result if result else None


def detect_consolidation(highs, lows, closes, lookback=50):
    """Detect if ticker is currently in a low-volatility consolidation (squeeze)."""
    n = min(lookback, len(highs))
    if n < 30:
        return None
    h = np.array(highs[-n:], dtype=float)
    l = np.array(lows[-n:], dtype=float)
    c = np.array(closes[-n:], dtype=float)

    # Compute ATR
    tr = np.maximum(h[1:] - l[1:], np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))
    if len(tr) < 20:
        return None
    atr_20 = np.mean(tr[-20:])
    atr_50 = np.mean(tr) if len(tr) >= 40 else np.mean(tr)

    # Squeeze: current ATR < 60% of longer-term average
    if atr_50 == 0 or atr_20 / atr_50 >= 0.60:
        return None

    # Box bounds: last 20 bars
    box_high = round(float(np.max(h[-20:])), 2)
    box_low = round(float(np.min(l[-20:])), 2)
    return {"high": box_high, "low": box_low}


def get_stock_data(ticker_symbol, charts_dir, spy_hist=None, ohlc_dir=None):
    try:
        stock = None
        all_hist = None
        # Prefer the batch cache — avoids an individual HTTP hit per ticker
        if ticker_symbol in _BATCH_CACHE:
            all_hist = _BATCH_CACHE[ticker_symbol]
        elif _CIRCUIT_OPEN:
            # Skip live fetch — previous-snapshot fallback takes over upstream
            return None
        else:
            try:
                stock = yf.Ticker(ticker_symbol)
                all_hist = stock.history(period="1y")
                _note_yf_result(error=None)
            except Exception as _e:
                _note_yf_result(error=_e)
                raise
        hist = all_hist.tail(21)
        daily = all_hist.tail(60)
        yearly = all_hist

        if len(hist) < 2 or len(daily) < 50:
            return None

        if ohlc_dir:
            try:
                safe = re.sub(r'[^a-zA-Z0-9]', '_', ticker_symbol)
                ohlc_rows = [
                    {"t": idx.date().isoformat(),
                     "o": round(float(r['Open']), 4), "h": round(float(r['High']), 4),
                     "l": round(float(r['Low']), 4),  "c": round(float(r['Close']), 4),
                     "v": int(r['Volume'])}
                    for idx, r in all_hist.tail(260).iterrows()
                ]
                ticker_name = ""
                # Skip .info lookup when we came from batch cache — it's a separate HTTP
                # call and Yahoo rate-limits it heavily. Preserve any existing cached name.
                if stock is not None:
                    try:
                        ticker_name = stock.info.get("shortName", "") or stock.info.get("longName", "")
                    except Exception:
                        pass
                else:
                    try:
                        existing = json.load(open(os.path.join(ohlc_dir, f"{safe}.json"), encoding="utf-8"))
                        ticker_name = existing.get("name", "") or ""
                    except Exception:
                        pass
                ohlc_payload = {"ticker": ticker_symbol, "name": ticker_name, "ohlc": ohlc_rows}
                with open(os.path.join(ohlc_dir, f"{safe}.json"), 'w') as _f:
                    json.dump(ohlc_payload, _f, separators=(',', ':'))
            except Exception as _e:
                print(f"OHLC write error {ticker_symbol}: {_e}")

        daily_change = (hist['Close'].iloc[-1] / hist['Close'].iloc[-2] - 1) * 100
        intraday_change = (hist['Close'].iloc[-1] / hist['Open'].iloc[-1] - 1) * 100
        five_day_change = (hist['Close'].iloc[-1] / hist['Close'].iloc[-6] - 1) * 100 if len(hist) >= 6 else None
        twenty_day_change = (hist['Close'].iloc[-1] / hist['Close'].iloc[-21] - 1) * 100 if len(hist) >= 21 else None

        today = daily.index[-1].date()
        days_since_monday = today.weekday()
        wtd_change = None
        # WTD: always from last Friday's close
        # WTD: always from last Friday's close (skip today if today is Friday)
        for i in range(2, len(daily)+1):
            day = daily.index[-i].date()
            if day.weekday() == 4:  # 4 = Friday
                wtd_change = (daily['Close'].iloc[-1] / daily['Close'].iloc[-i] - 1) * 100
                break

        ytd_change = None
        current_year = today.year
        if yearly is not None and len(yearly) > 0:
            yearly_filtered = yearly[yearly.index.year == current_year]
            if len(yearly_filtered) >= 2:
                ytd_change = (yearly_filtered['Close'].iloc[-1] / yearly_filtered['Close'].iloc[0] - 1) * 100

        one_month_return = None
        three_month_return = None
        if len(daily) >= 22:
            one_month_return = round((daily['Close'].iloc[-1] / daily['Close'].iloc[-22] - 1) * 100, 2)
        if yearly is not None and len(yearly) >= 64:
            three_month_return = round((yearly['Close'].iloc[-1] / yearly['Close'].iloc[-64] - 1) * 100, 2)

        six_month_return = None
        twelve_month_return = None
        if yearly is not None and len(yearly) >= 126:
            six_month_return = round((yearly['Close'].iloc[-1] / yearly['Close'].iloc[-126] - 1) * 100, 2)
        if yearly is not None and len(yearly) >= 252:
            twelve_month_return = round((yearly['Close'].iloc[-1] / yearly['Close'].iloc[-252] - 1) * 100, 2)

        sma50 = calculate_sma(daily)
        sma20 = calculate_sma(daily, 20)
        sma200 = calculate_sma(all_hist, 200) if len(all_hist) >= 200 else None
        atr = calculate_atr(daily)
        current_close = daily['Close'].iloc[-1]
        atr_pct = (atr / current_close) * 100 if atr and current_close else None
        dist_sma50_atr = (100 * (current_close / sma50 - 1) / atr_pct) if (sma50 and atr_pct and atr_pct != 0) else None
        abc_rating = calculate_abc_rating(daily)
        stage = calculate_stage(all_hist)
        sr_levels = calculate_sr_levels(all_hist, current_close)
        # Volume calculations
        vol_ratio = None
        vol_history = None
        try:
            avg_vol_20 = daily['Volume'].iloc[-21:-1].mean()
            today_vol = daily['Volume'].iloc[-1]
            if avg_vol_20 and avg_vol_20 > 0:
                vol_ratio = round(today_vol / avg_vol_20, 2)
            # Last 10 days of volume as ratio vs 20D avg
            vol_history = []
            for j in range(20, 0, -1):
                v = daily['Volume'].iloc[-j]
                ratio = round(v / avg_vol_20, 2) if avg_vol_20 and avg_vol_20 > 0 else 0
                vol_history.append(ratio)
        except Exception:
            pass

        rs_sts = None
        rrs_data = None
        end_date = datetime.now()
        start_date = end_date - timedelta(days=120)
        try:
            if all_hist is not None and len(all_hist) > 0:
                idx = all_hist.index
                start_ts = pd.Timestamp(start_date, tz=idx.tz) if idx.tz is not None else pd.Timestamp(start_date)
                stock_history = all_hist.loc[idx >= start_ts]
            elif stock is not None:
                stock_history = stock.history(start=start_date, end=end_date)
            else:
                stock_history = None
            spy_history = spy_hist.loc[spy_hist.index >= pd.Timestamp(start_date, tz=spy_hist.index.tz)] if spy_hist is not None and len(spy_hist) > 0 else yf.Ticker("SPY").history(start=start_date, end=end_date)
            if stock_history is not None and spy_history is not None:
                rrs_data = calculate_rrs(stock_history, spy_history, atr_length=14, length_rolling=50, length_sma=20, atr_multiplier=1.0)
                if rrs_data is not None and len(rrs_data) >= 21:
                    recent_21 = rrs_data['rollingRRS'].iloc[-21:]
                    ranks = rankdata(recent_21, method='average')
                    rs_sts = ((ranks[-1] - 1) / (len(recent_21) - 1)) * 100
        except Exception as e:
            print("RRS error", ticker_symbol, e)

        rs_chart_path = create_rs_chart_png(rrs_data, ticker_symbol, charts_dir) if rrs_data is not None and len(rrs_data) > 0 else None
        vol_chart_path = create_vol_chart_png(vol_history, ticker_symbol, charts_dir) if vol_history else None
        long_etfs, short_etfs = get_leveraged_etfs(ticker_symbol)

        # Gap detection: full candle gap in last 5 trading days [day1=most recent, day5=oldest]
        # 'up'   = entire current candle above previous candle (curr_low > prev_high)
        # 'down' = entire current candle below previous candle (curr_high < prev_low)
        gaps = []
        try:
            for i in range(1, 6):
                if len(hist) < i + 1:
                    gaps.append(None)
                    continue
                curr_low  = float(hist['Low'].iloc[-i])
                curr_high = float(hist['High'].iloc[-i])
                prev_low  = float(hist['Low'].iloc[-(i + 1)])
                prev_high = float(hist['High'].iloc[-(i + 1)])
                if curr_low > prev_high:
                    gaps.append('up')
                elif curr_high < prev_low:
                    gaps.append('down')
                else:
                    gaps.append(None)
        except Exception:
            pass
        while len(gaps) < 5:
            gaps.append(None)
        # Trendlines + consolidation detection
        _tl_dates = [idx.date().isoformat() for idx in all_hist.index]
        _tl_highs = all_hist['High'].values
        _tl_lows = all_hist['Low'].values
        _tl_closes = all_hist['Close'].values
        trendlines = compute_trendlines(_tl_highs, _tl_lows, _tl_closes, _tl_dates)
        consolidation = detect_consolidation(_tl_highs, _tl_lows, _tl_closes)

        # Keep last 20 rolling RRS for equal-weighted summary charts (The 7s at a Glance)
        rolling_rrs = None
        if rrs_data is not None and len(rrs_data) >= 20:
            rolling_rrs = rrs_data['rollingRRS'].iloc[-20:].tolist()

        return {
            "ticker": ticker_symbol,
            "data_date": today.isoformat(),
            "daily": round(daily_change, 2) if daily_change is not None else None,
            "intra": round(intraday_change, 2) if intraday_change is not None else None,
            "5d": round(five_day_change, 2) if five_day_change is not None else None,
            "20d": round(twenty_day_change, 2) if twenty_day_change is not None else None,
            "wtd": round(wtd_change, 2) if wtd_change is not None else None,
            "ytd": round(ytd_change, 2) if ytd_change is not None else None,
            "1m_return": one_month_return,
            "3m_return": three_month_return,
            "6m_return": six_month_return,
            "12m_return": twelve_month_return,
            "stage": stage,
            "vol_ratio": vol_ratio,
            "vol_chart": vol_chart_path,
            "vol_history": vol_history if vol_history else None,
            "rolling_rrs": rolling_rrs,
            "atr_pct": round(atr_pct, 1) if atr_pct is not None else None,
            "dist_sma50_atr": round(dist_sma50_atr, 2) if dist_sma50_atr is not None else None,
            "rs": round(rs_sts, 0) if rs_sts is not None else None,
            "rs_chart": rs_chart_path,
            "long": long_etfs,
            "short": short_etfs,
            "abc": abc_rating,
            "gaps": gaps,
            "sr": sr_levels,
            "above_sma20": bool(current_close > sma20) if sma20 else None,
            "above_sma50": bool(current_close > sma50) if sma50 else None,
            "above_sma200": bool(current_close > sma200) if sma200 else None,
            "trendlines": trendlines,
            "consolidation": consolidation,
        }
    except Exception as e:
        print("Error", ticker_symbol, e)
        return None


def get_all_etfs_for_holdings():
    """Unique tickers across all groups (ETFs + stocks; holdings JSON only for funds).
    Frozen groups skipped — their holdings already fetched from prior runs."""
    etfs = set()
    for _group, tickers in STOCK_GROUPS.items():
        if _group in FROZEN_GROUPS:
            continue
        etfs.update(tickers)
    return sorted(etfs)


def refresh_holdings_daily_from_cache(out_dir, ticker_data):
    """Re-enrich every data/holdings/*.json `daily` field from already-fetched
    ticker_data (OpenD-sourced). Runs after fetch_etf_holdings so that:
      - When yfinance rate-limits funds_data and the holdings file isn't
        rewritten, its `daily` values still reflect today's market action.
      - When fetch_etf_holdings did rewrite but enrich_holdings_daily failed
        (yf.download rate-limited), stale or missing daily values get filled.
    Symbols absent from ticker_data are left untouched."""
    if not ticker_data:
        return
    holdings_dir = os.path.join(out_dir, "holdings")
    if not os.path.isdir(holdings_dir):
        return
    refreshed = 0
    for fname in os.listdir(holdings_dir):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(holdings_dir, fname)
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        hs = data.get("holdings") or []
        if not hs:
            continue
        changed = False
        for h in hs:
            sym = h.get("symbol")
            if not sym:
                continue
            row = ticker_data.get(sym)
            if row is None:
                continue
            new_daily = row.get("daily")
            if new_daily is not None and h.get("daily") != new_daily:
                h["daily"] = new_daily
                changed = True
        if changed:
            try:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                refreshed += 1
            except Exception as e:
                print(f"  holdings refresh write error {fname}: {e}")
    if refreshed:
        print(f"Refreshed `daily` in {refreshed} holdings file(s) from in-memory ticker cache")


def enrich_holdings_daily(holdings):
    """Fetch previous-day return for each holding symbol and add 'daily' field."""
    if not holdings:
        return
    symbols = [h["symbol"] for h in holdings if h.get("symbol")]
    if not symbols:
        return
    try:
        hist = yf.download(symbols, period="5d", interval="1d", auto_adjust=True, progress=False)
        if hist.empty:
            return
        if len(symbols) == 1:
            close = hist["Close"].dropna()
            if len(close) >= 2:
                prev, curr = float(close.iloc[-2]), float(close.iloc[-1])
                daily = round((curr - prev) / prev * 100, 2) if prev != 0 else None
                for h in holdings:
                    if h.get("symbol") == symbols[0]:
                        h["daily"] = daily
        else:
            close = hist["Close"] if "Close" in hist.columns.get_level_values(0) else hist
            for h in holdings:
                sym = h.get("symbol", "")
                if sym in close.columns:
                    col = close[sym].dropna()
                    if len(col) >= 2:
                        prev, curr = float(col.iloc[-2]), float(col.iloc[-1])
                        h["daily"] = round((curr - prev) / prev * 100, 2) if prev != 0 else None
    except Exception:
        pass


def fetch_etf_holdings(etf_list, out_dir):
    """Top 10 holdings per symbol via yfinance; writes data/holdings/{SYM}.json when fund data exists."""
    holdings_dir = os.path.join(out_dir, "holdings")
    os.makedirs(holdings_dir, exist_ok=True)
    print("\nFetching ETF/fund holdings for {} symbols...".format(len(etf_list)))
    for i, etf_symbol in enumerate(etf_list):
        try:
            ticker = yf.Ticker(etf_symbol)
            holdings_data = None
            try:
                fd = getattr(ticker, "funds_data", None)
                if fd is not None:
                    holdings_data = getattr(fd, "top_holdings", None)
            except Exception:
                holdings_data = None
            has_holdings = holdings_data is not None and len(holdings_data) > 0
            if has_holdings:
                holdings = []
                for idx, item in holdings_data.head(10).iterrows():
                    holding_symbol = str(idx).strip() if idx is not None else ""
                    if not holding_symbol or holding_symbol == "nan":
                        try:
                            holding_symbol = str(item.get("Symbol", item.get("name", "")) or "")
                        except Exception:
                            holding_symbol = ""
                    weight = None
                    try:
                        weight = item.get("Holding Percent", item.get("weight"))
                        if weight is not None:
                            weight = float(weight)
                    except (ValueError, TypeError):
                        weight = None
                    if holding_symbol:
                        holdings.append({"symbol": holding_symbol, "weight": weight})
                if holdings:
                    enrich_holdings_daily(holdings)
                    path = os.path.join(holdings_dir, "{}.json".format(etf_symbol))
                    with open(path, "w", encoding="utf-8") as f:
                        json.dump({"symbol": etf_symbol, "holdings": holdings}, f, ensure_ascii=False, indent=2)
                    print("  [{}/{}] {}: {} holdings".format(i + 1, len(etf_list), etf_symbol, len(holdings)))
                else:
                    print("  [{}/{}] {}: (empty)".format(i + 1, len(etf_list), etf_symbol))
            else:
                info = {}
                try:
                    info = ticker.info or {}
                except Exception:
                    pass
                top_holdings = info.get("topHoldings") or []
                if top_holdings:
                    holdings = []
                    for h in top_holdings[:10]:
                        sym = h.get("symbol") or h.get("ticker") or ""
                        w = h.get("holdingPercent") or h.get("weight")
                        try:
                            w = float(w) if w is not None else None
                        except (ValueError, TypeError):
                            w = None
                        if sym:
                            holdings.append({"symbol": sym, "weight": w})
                    if holdings:
                        enrich_holdings_daily(holdings)
                        path = os.path.join(holdings_dir, "{}.json".format(etf_symbol))
                        with open(path, "w", encoding="utf-8") as f:
                            json.dump({"symbol": etf_symbol, "holdings": holdings}, f, ensure_ascii=False, indent=2)
                        print("  [{}/{}] {}: {} (info)".format(i + 1, len(etf_list), etf_symbol, len(holdings)))
                    else:
                        print("  [{}/{}] {}: no holdings".format(i + 1, len(etf_list), etf_symbol))
                else:
                    print("  [{}/{}] {}: no fund holdings".format(i + 1, len(etf_list), etf_symbol))
            time.sleep(0.25)
        except Exception as ex:
            print("  [{}/{}] {}: {}".format(i + 1, len(etf_list), etf_symbol, ex))


FOMC_DATES = [
    "2026-05-06", "2026-06-17", "2026-07-29", "2026-09-16",
    "2026-10-28", "2026-12-09", "2027-01-27", "2027-03-17",
    "2027-05-05", "2027-06-16", "2027-07-28", "2027-09-15",
]

def next_fomc():
    today = datetime.utcnow().date()
    for d in FOMC_DATES:
        dt = datetime.strptime(d, "%Y-%m-%d").date()
        if dt >= today:
            return d, (dt - today).days
    return None, None


def build_usd_liquidity(fred_api_key):
    """Build USD Liquidity Pressure metrics from FRED.
    Uses 1-year percentile ranks for a calibrated 5-component stress score."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from scipy.stats import percentileofscore

    ALL_SERIES = [
        "SOFR", "SOFR1", "SOFR99", "DTB3", "RRPONTSYD", "WTREGEN",
        "EFFR", "DCPF3M", "BAMLH0A0HYM2", "BAMLC0A0CM", "WRESBAL", "TEDRATE",
    ]

    # Fetch ~1 year of history concurrently
    raw = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(fetch_fred_series, fred_api_key, sid, 260): sid
                for sid in ALL_SERIES}
        for f in as_completed(futs):
            raw[futs[f]] = f.result()

    def latest(sid):
        d = raw.get(sid, [])
        return float(d[-1][1]) if d else None

    def prev_val(sid):
        d = raw.get(sid, [])
        return float(d[-2][1]) if len(d) >= 2 else None

    def series_vals(sid):
        return [float(p[1]) for p in raw.get(sid, [])]

    def series_dates(sid):
        return [p[0] for p in raw.get(sid, [])]

    # ── Derived spread series ───────────────────────────────────────────
    def aligned_spread(sid_a, sid_b):
        """Compute spread between two series, aligning by date."""
        da = {p[0]: float(p[1]) for p in raw.get(sid_a, [])}
        db = {p[0]: float(p[1]) for p in raw.get(sid_b, [])}
        dates = sorted(set(da.keys()) & set(db.keys()))
        return [(d, round(da[d] - db[d], 4)) for d in dates]

    sofr_disp_ts   = aligned_spread("SOFR99", "SOFR1")
    sofr_tbill_ts  = aligned_spread("SOFR", "DTB3")
    cp_tbill_ts    = aligned_spread("DCPF3M", "DTB3")

    # HY OAS is a direct series (not a spread)
    hy_oas_ts = [(p[0], float(p[1])) for p in raw.get("BAMLH0A0HYM2", [])]

    # Reserve scarcity: use WRESBAL directly (inverted for percentile)
    wresbal_ts = [(p[0], float(p[1])) for p in raw.get("WRESBAL", [])]

    # ── Component definitions ───────────────────────────────────────────
    COMPONENTS = [
        {
            "id": "sofr_dispersion", "label": "SOFR Dispersion",
            "formula": "SOFR99 \u2212 SOFR1", "unit": "%",
            "weight": 0.25, "fred_ids": ["SOFR99", "SOFR1"],
            "ts": sofr_disp_ts, "invert": False,
        },
        {
            "id": "sofr_tbill", "label": "SOFR vs T-Bill",
            "formula": "SOFR \u2212 DTB3", "unit": "%",
            "weight": 0.20, "fred_ids": ["SOFR", "DTB3"],
            "ts": sofr_tbill_ts, "invert": False,
        },
        {
            "id": "cp_tbill", "label": "CP\u2212Tbill Spread",
            "formula": "DCPF3M \u2212 DTB3", "unit": "%",
            "weight": 0.20, "fred_ids": ["DCPF3M", "DTB3"],
            "ts": cp_tbill_ts, "invert": False,
        },
        {
            "id": "hy_oas", "label": "HY Credit OAS",
            "formula": "BAMLH0A0HYM2", "unit": "%",
            "weight": 0.20, "fred_ids": ["BAMLH0A0HYM2"],
            "ts": hy_oas_ts, "invert": False,
        },
        {
            "id": "reserve_scarcity", "label": "Reserve Scarcity",
            "formula": "\u2212WRESBAL (inverted)", "unit": "B",
            "weight": 0.15, "fred_ids": ["WRESBAL"],
            "ts": wresbal_ts, "invert": True,
        },
    ]

    # ── Build component output ──────────────────────────────────────────
    comp_out = []
    total_weight = 0
    weighted_sum = 0

    for c in COMPONENTS:
        ts = c["ts"]
        vals = [v for _, v in ts]
        if len(vals) < 20:
            continue  # too few observations for meaningful percentile

        current = vals[-1]
        prev = vals[-2] if len(vals) >= 2 else current
        mn, mx = min(vals), max(vals)

        if c["invert"]:
            pctile = round(100 - percentileofscore(vals, current, kind='rank'))
        else:
            pctile = round(percentileofscore(vals, current, kind='rank'))

        direction = "up" if current > prev else ("down" if current < prev else "flat")
        contribution = round(c["weight"] * pctile, 1)
        total_weight += c["weight"]
        weighted_sum += c["weight"] * pctile

        # Last 90 data points for sparkline
        hist_slice = ts[-90:]

        comp_out.append({
            "id": c["id"],
            "label": c["label"],
            "formula": c["formula"],
            "value": round(current, 4) if c["unit"] == "%" else round(current, 1),
            "unit": c["unit"],
            "percentile": pctile,
            "weight": c["weight"],
            "contribution": contribution,
            "direction": direction,
            "prev_value": round(prev, 4) if c["unit"] == "%" else round(prev, 1),
            "min_1y": round(mn, 4) if c["unit"] == "%" else round(mn, 1),
            "max_1y": round(mx, 4) if c["unit"] == "%" else round(mx, 1),
            "fred_ids": c["fred_ids"],
            "history": [{"t": d, "v": round(v, 4)} for d, v in hist_slice],
        })

    # Composite score (renormalize if any components were skipped)
    score = round(weighted_sum / total_weight) if total_weight > 0 else None
    if score is not None:
        score_label = (
            "Calm" if score <= 25 else
            "Normal" if score <= 50 else
            "Elevated" if score <= 75 else
            "Stressed"
        )
    else:
        score_label = None

    # ── Raw values for metric cards ─────────────────────────────────────
    rrp    = latest("RRPONTSYD")
    rrp_p  = prev_val("RRPONTSYD")
    tga    = latest("WTREGEN")
    tga_p  = prev_val("WTREGEN")

    raw_out = {
        "sofr": latest("SOFR"), "sofr1": latest("SOFR1"), "sofr99": latest("SOFR99"),
        "dtb3": latest("DTB3"), "effr": latest("EFFR"), "dcpf3m": latest("DCPF3M"),
        "rrp": rrp,
        "rrp_chg": round(rrp - rrp_p, 1) if rrp is not None and rrp_p is not None else None,
        "tga": tga,
        "tga_chg": round(tga - tga_p, 1) if tga is not None and tga_p is not None else None,
        "wresbal": latest("WRESBAL"), "tedrate": latest("TEDRATE"),
        "hy_oas": latest("BAMLH0A0HYM2"), "ig_oas": latest("BAMLC0A0CM"),
    }

    fred_links = {sid: f"https://fred.stlouisfed.org/series/{sid}" for sid in ALL_SERIES}

    # as_of = max latest_date across all series we actually consumed. Used by
    # the dashboard "data as of" label so the UI shows the underlying FRED
    # release date, not the build timestamp.
    _series_latest = [raw[sid][-1][0] for sid in ALL_SERIES if raw.get(sid)]
    as_of = max(_series_latest) if _series_latest else None

    return {
        "score": score,
        "score_label": score_label,
        "as_of": as_of,
        "components": comp_out,
        "raw": raw_out,
        "fred_links": fred_links,
    }


def fetch_gscpi(out_dir):
    """NY Fed Global Supply Chain Pressure Index. Monthly XLSX, no auth.
    Caches to data/_cache/gscpi.json; re-fetches only if cache > 7 days old."""
    cache_dir = os.path.join(out_dir, "_cache")
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, "gscpi.json")

    if os.path.exists(cache_path):
        try:
            mtime = os.path.getmtime(cache_path)
            if (time.time() - mtime) < 7 * 86400:
                with open(cache_path) as f:
                    return json.load(f)
        except Exception:
            pass

    try:
        url = "https://www.newyorkfed.org/medialibrary/research/interactives/gscpi/downloads/gscpi_data.xlsx"
        import io
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        df = pd.read_excel(io.BytesIO(resp.content), sheet_name="GSCPI Monthly Data", header=5)
        df = df.dropna(subset=[df.columns[0]])
        rows = []
        for _, r in df.iterrows():
            try:
                d = pd.to_datetime(r.iloc[0]).strftime("%Y-%m-%d")
                v = float(r.iloc[1])
                rows.append({"t": d, "v": v})
            except Exception:
                continue
        if not rows:
            return None
        out = {"history": rows[-60:], "latest": rows[-1]}
        with open(cache_path, "w") as f:
            json.dump(out, f, separators=(",", ":"))
        return out
    except Exception as e:
        print(f"[gscpi] fetch failed: {e}")
        if os.path.exists(cache_path):
            try:
                with open(cache_path) as f:
                    return json.load(f)
            except Exception:
                pass
        return None


def build_inflation_pillar(fred_api_key, out_dir):
    """Real yields + breakeven inflation + core PCE + GSCPI. All free FRED + NY Fed."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    SERIES = ["T5YIE", "T10YIE", "DFII5", "DFII10", "T5YIFR", "PCEPILFE"]

    raw = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(fetch_fred_series, fred_api_key, sid, 260): sid for sid in SERIES}
        for f in as_completed(futs):
            raw[futs[f]] = f.result()

    def metric(series_id):
        d = raw.get(series_id) or []
        if not d:
            return None
        latest_date, latest_val = d[-1]
        # 1m delta: ~21 trading days back
        prev = d[-22][1] if len(d) >= 22 else (d[0][1] if d else None)
        change_1m = (latest_val - prev) if prev is not None else None
        vals = [p[1] for p in d]
        if len(vals) >= 5:
            below = sum(1 for v in vals if v < latest_val)
            pct_1y = round((below / len(vals)) * 100)
        else:
            pct_1y = None
        return {
            "value": latest_val,
            "as_of": latest_date,
            "change_1m": change_1m,
            "percentile_1y": pct_1y,
        }

    # Core PCE: published as index level — convert to YoY %
    core_pce_yoy = None
    pce_obs = raw.get("PCEPILFE") or []
    if len(pce_obs) >= 13:
        latest_date, latest_val = pce_obs[-1]
        # 12 months prior — series is monthly, so go back ~12 entries
        ya_val = pce_obs[-13][1] if len(pce_obs) >= 13 else None
        if ya_val:
            yoy = ((latest_val / ya_val) - 1.0) * 100
            # 3m trend: compare yoy vs 3-months-ago yoy
            trend_3m = None
            if len(pce_obs) >= 16:
                ya3_val = pce_obs[-16][1]
                ya3_minus12 = pce_obs[-28][1] if len(pce_obs) >= 28 else None
                if ya3_val and ya3_minus12:
                    yoy3m = ((ya3_val / ya3_minus12) - 1.0) * 100
                    trend_3m = yoy - yoy3m
            core_pce_yoy = {
                "value": yoy,
                "release_date": latest_date,
                "trend_3m": trend_3m,
            }

    # GSCPI from NY Fed
    gscpi_block = None
    gscpi = fetch_gscpi(out_dir)
    if gscpi and gscpi.get("latest"):
        latest = gscpi["latest"]
        hist_vals = [r["v"] for r in gscpi.get("history") or []]
        z = None
        if len(hist_vals) >= 12:
            mu = sum(hist_vals) / len(hist_vals)
            var = sum((v - mu) ** 2 for v in hist_vals) / len(hist_vals)
            sd = math.sqrt(var) if var > 0 else 0
            if sd > 0:
                z = (latest["v"] - mu) / sd
        gscpi_block = {
            "value": latest["v"],
            "release_date": latest["t"],
            "z_score": z,
        }

    return {
        "real_yields": {
            "us_5y": metric("DFII5"),
            "us_10y": metric("DFII10"),
        },
        "breakeven": {
            "5y": metric("T5YIE"),
            "10y": metric("T10YIE"),
            "5y5y_forward": metric("T5YIFR"),
        },
        "core_pce_yoy": core_pce_yoy,
        "supply_chain_gscpi": gscpi_block,
        "fred_links": {
            "DFII5":  "https://fred.stlouisfed.org/series/DFII5",
            "DFII10": "https://fred.stlouisfed.org/series/DFII10",
            "T5YIE":  "https://fred.stlouisfed.org/series/T5YIE",
            "T10YIE": "https://fred.stlouisfed.org/series/T10YIE",
            "T5YIFR": "https://fred.stlouisfed.org/series/T5YIFR",
            "PCEPILFE": "https://fred.stlouisfed.org/series/PCEPILFE",
        },
        "ny_fed_link": "https://www.newyorkfed.org/research/policy/gscpi",
    }


def _rsi_series(closes, period=14):
    """Wilder's RSI series. Returns list aligned with closes[period:]."""
    if len(closes) < period + 1:
        return []
    deltas = [closes[i+1] - closes[i] for i in range(len(closes)-1)]
    gains = [d if d > 0 else 0.0 for d in deltas]
    losses = [-d if d < 0 else 0.0 for d in deltas]
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    out = []
    rs = (avg_g / avg_l) if avg_l > 0 else float('inf')
    out.append(100.0 - 100.0/(1.0+rs) if rs != float('inf') else 100.0)
    for i in range(period, len(deltas)):
        avg_g = (avg_g*(period-1) + gains[i]) / period
        avg_l = (avg_l*(period-1) + losses[i]) / period
        rs = (avg_g / avg_l) if avg_l > 0 else float('inf')
        out.append(100.0 - 100.0/(1.0+rs) if rs != float('inf') else 100.0)
    return out


def _pct_rank(window, latest):
    """Percentile rank (0-100) of `latest` within `window` (count strictly below)."""
    if not window:
        return None
    below = sum(1 for v in window if v < latest)
    return round(100.0 * below / len(window), 1)


def _load_ohlc_closes(ohlc_dir, ticker):
    """Load (dates_iso, closes) chronologically. Returns ([], []) if not found."""
    safe = re.sub(r"[^a-zA-Z0-9]", "_", ticker)
    path = os.path.join(ohlc_dir, f"{safe}.json")
    if not os.path.exists(path):
        return [], []
    try:
        with open(path, encoding="utf-8") as f:
            bars = json.load(f).get("ohlc") or []
    except Exception:
        return [], []
    dates, closes = [], []
    for b in bars:
        c = b.get("c")
        t = b.get("t")
        if c is None or t is None:
            continue
        try:
            closes.append(float(c))
            dates.append(t[:10])
        except (TypeError, ValueError):
            continue
    return dates, closes


def build_etf_flow(out_dir, ohlc_dir):
    """Compose ETF fund-flow metrics for the Sector/Industry rotation bubble chart.

    Reads the Bloomberg-derived weekly baseline at data/baseline/etf_flow_weekly.json
    and a rolling daily NAV/units snapshot at data/baseline/etf_flow_daily.json.
    Calls OpenD once to refresh today's trust_* block per baseline ticker, appends
    to the daily file (60-day rolling, idempotent same-date), and returns metrics:
      - wtd_return        % move from prior Friday close to latest close (cached OHLC)
      - flow_5d           latest 5-day flow, USD millions (Bloomberg current_5d)
      - flow_52w_pct      rank of flow_5d within trailing 52 weekly flows
      - flow_change_vs_1m flow_5d minus mean of last 4 weekly flows
      - stretch_score     0-100 composite "expensiveness": avg of RSI(14) 52w pct,
                          4w cumulative-flow 52w pct, and RS-vs-SPY 52w pct.
                          100 = stretched/crowded/expensive; 0 = washed out/cheap.
      - stretch_components  per-component breakdown (rsi_pct, flow_z_pct, rs_pct)
    """
    base_path = os.path.join(out_dir, "baseline", "etf_flow_weekly.json")
    daily_path = os.path.join(out_dir, "baseline", "etf_flow_daily.json")
    if not os.path.exists(base_path):
        print(f"  etf_flow: baseline missing at {base_path}")
        return None
    try:
        with open(base_path, encoding="utf-8") as f:
            baseline = json.load(f)
    except Exception as e:
        print(f"  etf_flow: baseline load failed: {e}")
        return None

    base_tickers = baseline.get("tickers") or {}
    if not base_tickers:
        return None

    # Refresh daily AUM snapshot from OpenD (best-effort; baseline metrics still work without it).
    daily = {}
    if os.path.exists(daily_path):
        try:
            with open(daily_path, encoding="utf-8") as f:
                daily = json.load(f).get("snapshots", {}) or {}
        except Exception:
            daily = {}

    aum_today = {}
    try:
        from fetch_opend import fetch_etf_aum_snapshot
        aum_today = fetch_etf_aum_snapshot(list(base_tickers.keys()))
    except Exception as e:
        print(f"  etf_flow: OpenD aum snapshot failed: {e}")

    today_iso = datetime.now().date().isoformat()
    for tk, snap in aum_today.items():
        rows = daily.setdefault(tk, [])
        rows[:] = [r for r in rows if r.get("date") != snap["date"]]
        rows.append(snap)
        rows.sort(key=lambda r: r.get("date") or "")
        if len(rows) > 60:
            del rows[:-60]

    if aum_today:
        try:
            os.makedirs(os.path.dirname(daily_path), exist_ok=True)
            with open(daily_path, "w", encoding="utf-8") as f:
                json.dump({"as_of": today_iso, "snapshots": daily},
                          f, separators=(",", ":"))
        except Exception as e:
            print(f"  etf_flow: daily snapshot write failed: {e}")

    # Load SPY closes once (used by RS-vs-SPY component for every ticker).
    spy_dates, spy_closes = _load_ohlc_closes(ohlc_dir, "SPY")
    spy_by_date = dict(zip(spy_dates, spy_closes))

    out_tickers = {}
    for tk, info in base_tickers.items():
        history = info.get("history") or []
        flows = [h["flow"] for h in history if h.get("flow") is not None]
        latest_5d = info.get("current_5d")

        flow_52w_pct = None
        if latest_5d is not None and len(flows) >= 13:
            window = flows[-52:] if len(flows) >= 52 else flows
            below = sum(1 for v in window if v < latest_5d)
            flow_52w_pct = round(100.0 * below / len(window), 1)

        flow_change_vs_1m = None
        if latest_5d is not None and len(flows) >= 4:
            ref = sum(flows[-4:]) / 4.0
            flow_change_vs_1m = round(latest_5d - ref, 2)

        wtd_return = None
        tk_dates, tk_closes = _load_ohlc_closes(ohlc_dir, tk)
        if len(tk_closes) >= 6:
            # Walk back to the most recent Friday strictly before the latest bar.
            target_close = None
            for i in range(len(tk_dates)-2, -1, -1):
                try:
                    if datetime.fromisoformat(tk_dates[i]).weekday() == 4:
                        target_close = tk_closes[i]
                        break
                except ValueError:
                    continue
            if target_close and tk_closes[-1]:
                wtd_return = round(100.0 * (tk_closes[-1] / target_close - 1), 2)

        # ── Stretch score: 0 = cheap/washed-out, 100 = expensive/crowded ───────
        # Three orthogonal lenses, averaged over whichever are computable today.
        rsi_pct = None
        if len(tk_closes) >= 28:  # need 14 + 14 to start RSI then have headroom
            rsi = _rsi_series(tk_closes, 14)
            if rsi:
                window = rsi[-252:] if len(rsi) >= 252 else rsi
                rsi_pct = _pct_rank(window, rsi[-1])

        flow_z_pct = None
        if len(flows) >= 8:
            cum4 = [sum(flows[i-3:i+1]) for i in range(3, len(flows))]
            if cum4:
                window = cum4[-52:] if len(cum4) >= 52 else cum4
                flow_z_pct = _pct_rank(window, cum4[-1])

        rs_pct = None
        if tk != "SPY" and len(tk_closes) >= 60 and spy_by_date:
            ratios = []
            for d, c in zip(tk_dates, tk_closes):
                sc = spy_by_date.get(d)
                if sc and c:
                    ratios.append(c / sc)
            if len(ratios) >= 60:
                window = ratios[-252:] if len(ratios) >= 252 else ratios
                rs_pct = _pct_rank(window, ratios[-1])

        components = [v for v in (rsi_pct, flow_z_pct, rs_pct) if v is not None]
        stretch_score = round(sum(components) / len(components), 1) if components else None

        out_tickers[tk] = {
            "name": info.get("name"),
            "wtd_return": wtd_return,
            "flow_5d": latest_5d,
            "flow_52w_pct": flow_52w_pct,
            "flow_change_vs_1m": flow_change_vs_1m,
            "stretch_score": stretch_score,
            "stretch_components": {
                "rsi_pct": rsi_pct,
                "flow_z_pct": flow_z_pct,
                "rs_pct": rs_pct,
            },
        }

    return {
        "as_of": today_iso,
        "baseline_as_of": baseline.get("as_of"),
        "source": "Bloomberg weekly baseline + OpenD daily NAV/units",
        "units": "USD millions",
        "daily_snapshot_count": sum(len(v) for v in daily.values()),
        "tickers": out_tickers,
    }


def build_fed_watch(fred_api_key, perplexity_api_key=None):
    """Build Fed Watch: current rates, next FOMC, hawk/dove from Perplexity."""
    fomc_date, fomc_days = next_fomc()

    raw_dff   = fetch_fred_series(fred_api_key, "DFF",      5) if fred_api_key else []
    raw_lower = fetch_fred_series(fred_api_key, "DFEDTARL", 5) if fred_api_key else []
    raw_upper = fetch_fred_series(fred_api_key, "DFEDTARU", 5) if fred_api_key else []

    result = {
        "ffr":            float(raw_dff[-1][1])   if raw_dff   else None,
        "target_lower":   float(raw_lower[-1][1]) if raw_lower else None,
        "target_upper":   float(raw_upper[-1][1]) if raw_upper else None,
        "next_fomc_date": fomc_date,
        "next_fomc_days": fomc_days,
        "speeches":       [],
        "hawk_dove_score":      None,
        "market_implied_cuts":  None,
    }

    if perplexity_api_key:
        try:
            import requests as _req
            today_str = datetime.utcnow().strftime('%Y-%m-%d')
            payload = {
                "model": "sonar",
                "messages": [
                    {"role": "system", "content": "Return only valid JSON, no markdown."},
                    {"role": "user", "content": (
                        f"Today is {today_str}. Return a JSON object with:\n"
                        '{"speeches":[last 5 Fed official speeches past 2 weeks, each: '
                        '"name","role","voter"(bool),"date"(YYYY-MM-DD),"summary"(1 sentence),'
                        '"score"(int -2 to +2 hawk/dove)],'
                        '"market_implied_cuts":(int, 25bp cuts CME FedWatch prices for rest of 2026),'
                        '"hawk_dove_score":(int -100 to +100 overall Fed tone)}'
                    )},
                ],
            }
            resp = _req.post(
                "https://api.perplexity.ai/chat/completions",
                json=payload,
                headers={"Authorization": f"Bearer {perplexity_api_key}", "Content-Type": "application/json"},
                timeout=25,
            )
            if resp.ok:
                content = resp.json()["choices"][0]["message"]["content"].strip()
                if "```" in content:
                    content = content.split("```")[1]
                    if content.startswith("json"):
                        content = content[4:]
                parsed = json.loads(content)
                result["speeches"]            = parsed.get("speeches", [])
                result["market_implied_cuts"] = parsed.get("market_implied_cuts")
                result["hawk_dove_score"]     = parsed.get("hawk_dove_score")
        except Exception as e:
            print(f"  Fed Watch fetch error: {e}")

    return result


def compute_fear_greed(all_ticker_data):
    """Compute a 0–100 Fear & Greed composite from 6 market signals."""
    def _g(ticker, field):
        return all_ticker_data.get(ticker, {}).get(field) or 0

    components = {}

    # 1. VIX Volatility (15%) — high VIX = fear = low score
    try:
        cboe_df = _fetch_cboe_vix_history("^VIX")
        if cboe_df is not None and not cboe_df.empty:
            vix_hist = cboe_df.tail(252)
        else:
            vix_hist = yf.Ticker("^VIX").history(period="1y")
        vix_now = float(vix_hist["Close"].iloc[-1])
        vix_min = float(vix_hist["Close"].min())
        vix_max = float(vix_hist["Close"].max())
        s = max(0.0, min(100.0, (1 - (vix_now - vix_min) / max(vix_max - vix_min, 0.01)) * 100))
        components["volatility"] = {"score": round(s, 1), "label": "VIX Volatility",
                                     "detail": "VIX {:.1f} (52wk: {:.1f}–{:.1f})".format(vix_now, vix_min, vix_max)}
    except Exception:
        components["volatility"] = {"score": 50.0, "label": "VIX Volatility", "detail": "N/A"}

    # 2. SPY Trend (20%) — SMA50 dist + 20d return
    dist = _g("SPY", "dist_sma50_atr")
    r20  = _g("SPY", "20d")
    dist_s = max(0.0, min(100.0, (dist + 5) / 10 * 100))
    r20_s  = max(0.0, min(100.0, (r20  + 15) / 30 * 100))
    components["trend"] = {"score": round((dist_s + r20_s) / 2, 1), "label": "SPY Trend",
                            "detail": "SMA50 {:+.1f}ATR, 20d {:+.1f}%".format(dist, r20)}

    # 3. Sector Momentum (15%) — % Sel Sectors positive 20d
    sel = ["XLK","XLI","XLC","XLF","XLU","XLY","XLRE","XLP","XLB","XLE","XLV"]
    avail = [t for t in sel if t in all_ticker_data]
    pos   = sum(1 for t in avail if (_g(t, "20d") > 0))
    s = (pos / max(len(avail), 1)) * 100
    components["momentum"] = {"score": round(s, 1), "label": "Sector Momentum",
                               "detail": "{}/{} sectors positive 20d".format(pos, len(avail))}

    # 4. HYG/TLT Credit (20%) — HYG outperforms TLT = less fear
    hyg_20d = _g("HYG", "20d")
    tlt_20d = _g("TLT", "20d")
    diff = hyg_20d - tlt_20d
    s = max(0.0, min(100.0, (diff + 10) / 20 * 100))
    components["credit"] = {"score": round(s, 1), "label": "HYG/TLT Credit",
                             "detail": "HYG {:+.1f}%, TLT {:+.1f}%".format(hyg_20d, tlt_20d)}

    # 5. RSP/SPY Breadth (15%) — RSP outperforms SPY = broader participation = less fear
    rsp_20d = _g("RSP", "20d")
    spy_20d = _g("SPY", "20d")
    diff = rsp_20d - spy_20d
    s = max(0.0, min(100.0, (diff + 5) / 10 * 100))
    components["breadth"] = {"score": round(s, 1), "label": "RSP/SPY Breadth",
                              "detail": "RSP {:+.1f}%, SPY {:+.1f}%".format(rsp_20d, spy_20d)}

    # 6. GLD/TLT Cross-Asset (15%) — safe-haven demand = fear = low score (inverted)
    gld_20d = _g("GLD", "20d")
    safe_haven = gld_20d + tlt_20d
    s = max(0.0, min(100.0, (1 - (safe_haven + 15) / 30) * 100))
    components["cross_asset"] = {"score": round(s, 1), "label": "GLD/TLT Cross-Asset",
                                  "detail": "GLD {:+.1f}%, TLT {:+.1f}%".format(gld_20d, tlt_20d)}

    weights = {"volatility": 0.15, "trend": 0.20, "momentum": 0.15,
               "credit": 0.20, "breadth": 0.15, "cross_asset": 0.15}
    total = sum(weights[k] * components[k]["score"] for k in weights)

    if   total <= 20: sentiment = "Extreme Fear"
    elif total <= 40: sentiment = "Fear"
    elif total <= 60: sentiment = "Neutral"
    elif total <= 80: sentiment = "Greed"
    else:             sentiment = "Extreme Greed"

    return {"score": round(total, 1), "sentiment": sentiment,
            "components": components, "weights": weights}


_CBOE_VIX_CACHE = {}

def _fetch_cboe_vix_history(sym):
    """CBOE direct sources — authoritative VIX/VIX9D/VIX3M/VIX6M/VVIX/VXN/RVX.

    Pulls the daily-history CSV for the 252-bar series, then patches the latest
    bar from CBOE's delayed-quote JSON. The CSV is rewritten only ~9:51 PM ET
    each evening, so a build that runs at 6 PM ET would otherwise miss that
    day's settled close. The JSON endpoint exposes the settled close as soon as
    it prints (~4:15 PM ET) and is also the only path for the latest VVIX/VXN/
    RVX value (CSVs cover them too, but their JSON updates land first).

    Returns DataFrame indexed by date with Close column, or None on failure.
    """
    cboe_map = {
        "^VIX":   "VIX",   "^VIX9D": "VIX9D", "^VIX3M": "VIX3M", "^VIX6M": "VIX6M",
        "^VVIX":  "VVIX",  "^VXN":   "VXN",   "^RVX":   "RVX",
    }
    cboe_name = cboe_map.get(sym)
    if not cboe_name:
        return None
    if cboe_name in _CBOE_VIX_CACHE:
        return _CBOE_VIX_CACHE[cboe_name]
    df = None
    try:
        url = f"https://cdn.cboe.com/api/global/us_indices/daily_prices/{cboe_name}_History.csv"
        r = requests.get(url, timeout=10)
        if r.status_code == 200 and len(r.text) > 1000:
            from io import StringIO
            df = pd.read_csv(StringIO(r.text))
            df["DATE"] = pd.to_datetime(df["DATE"], format="%m/%d/%Y", errors="coerce")
            df = df.dropna(subset=["DATE"]).set_index("DATE")
            # VIX/VXN/RVX/VIX9D/VIX3M/VIX6M use OHLC columns ("CLOSE"); VVIX is single-column
            # ("VVIX"). Pick whichever close-column actually exists.
            close_col = next((c for c in ("CLOSE", "Close", cboe_name) if c in df.columns), None)
            if close_col and close_col != "Close":
                df = df.rename(columns={close_col: "Close"})
            df = df[["Close"]] if "Close" in df.columns else None
    except Exception as e:
        print(f"  CBOE CSV fetch {cboe_name}: {e}")

    # Patch latest close from delayed-quote JSON if newer than CSV last bar.
    try:
        jurl = f"https://cdn.cboe.com/api/global/delayed_quotes/quotes/_{cboe_name}.json"
        jr = requests.get(jurl, timeout=10)
        if jr.status_code == 200:
            jd = jr.json().get("data") or {}
            last_close = jd.get("close")
            ltt = jd.get("last_trade_time") or ""
            ltt_date = pd.to_datetime(ltt[:10], errors="coerce")
            if last_close is not None and pd.notna(ltt_date) and float(last_close) > 0:
                if df is None:
                    df = pd.DataFrame({"Close": [float(last_close)]}, index=[ltt_date])
                elif ltt_date > df.index[-1]:
                    df.loc[ltt_date, "Close"] = float(last_close)
                    print(f"  CBOE JSON {cboe_name}: appended {ltt_date.date()} close={last_close}")
    except Exception as e:
        print(f"  CBOE JSON fetch {cboe_name}: {e}")

    _CBOE_VIX_CACHE[cboe_name] = df if df is not None and not df.empty else None
    return _CBOE_VIX_CACHE[cboe_name]


def fetch_vol_signals():
    """Fetch implied volatility indices across asset classes."""
    VOL_TICKERS = {
        "Equities": [
            {"sym": "^VIX",  "name": "VIX",  "desc": "S&P 500"},
            {"sym": "^VXN",  "name": "VXN",  "desc": "Nasdaq 100"},
            {"sym": "^RVX",  "name": "RVX",  "desc": "Russell 2000"},
            {"sym": "^VVIX", "name": "VVIX", "desc": "VIX of VIX"},
        ],
        "Rates": [
            {"sym": "^MOVE", "name": "MOVE", "desc": "Treasury Bonds"},
        ],
        "Commodities": [
            {"sym": "^OVX", "name": "OVX", "desc": "Crude Oil"},
            {"sym": "^GVZ", "name": "GVZ", "desc": "Gold"},
        ],
    }
    result = {}
    for category, items in VOL_TICKERS.items():
        result[category] = []
        for item in items:
            try:
                cboe_df = _fetch_cboe_vix_history(item["sym"])
                if cboe_df is not None and not cboe_df.empty:
                    hist = cboe_df.tail(252)
                else:
                    hist = yf.Ticker(item["sym"]).history(period="1y")
                if hist.empty:
                    raise ValueError("empty")
                current = float(hist["Close"].iloc[-1])
                ma20 = float(hist["Close"].tail(20).mean())
                hi52 = float(hist["Close"].max())
                lo52 = float(hist["Close"].min())
                pct52 = round((current - lo52) / max(hi52 - lo52, 0.01) * 100, 1)
                vs_ma = round((current - ma20) / max(ma20, 0.01) * 100, 1)
                hist90 = hist.tail(90)
                history = [
                    {"t": str(d.date()), "v": round(float(v), 2)}
                    for d, v in zip(hist90.index, hist90["Close"])
                    if not (v != v)  # skip NaN
                ]
                # Rolling 20D MA series aligned to history
                ma_series = hist["Close"].rolling(20).mean().tail(90)
                ma_history = [
                    {"t": str(d.date()), "v": round(float(v), 2)}
                    for d, v in zip(ma_series.index, ma_series)
                    if not (v != v)
                ]
                result[category].append({
                    "name": item["name"], "desc": item["desc"],
                    "current": round(current, 2), "ma20": round(ma20, 2),
                    "vs_ma": vs_ma, "hi52": round(hi52, 2),
                    "lo52": round(lo52, 2), "pct52": pct52,
                    "history": history, "ma_history": ma_history,
                })
            except Exception as e:
                print(f"  vol_signals {item['name']}: {e}")
                result[category].append({
                    "name": item["name"], "desc": item["desc"],
                    "current": None, "ma20": None,
                    "vs_ma": None, "hi52": None,
                    "lo52": None, "pct52": None,
                })
            time.sleep(0.5)
    return result


def build_vix_term_structure():
    """VIX futures term-structure proxy via constant-maturity VIX indices.

    VIX (30d) − VIX3M (93d) approximates VX1!−VX2! with ~0.95 correlation,
    same sign convention: negative = contango/calm, positive = backwardation/fear.
    More robust than scraping CBOE settlement CSV (rolling expirations).
    """
    SYMS = ["^VIX9D", "^VIX", "^VIX3M", "^VIX6M"]
    points = {}
    for sym in SYMS:
        try:
            cboe_df = _fetch_cboe_vix_history(sym)
            if cboe_df is not None and not cboe_df.empty:
                hist = cboe_df.tail(252)
            else:
                hist = yf.Ticker(sym).history(period="6mo")
            if hist.empty:
                continue
            tail = hist.tail(90)
            points[sym] = {
                "current": round(float(hist["Close"].iloc[-1]), 2),
                "history": [
                    {"t": str(d.date()), "v": round(float(v), 2)}
                    for d, v in zip(tail.index, tail["Close"])
                    if not (v != v)
                ],
            }
            time.sleep(0.4)
        except Exception as e:
            print(f"  vix_term {sym}: {e}")

    vix = points.get("^VIX", {}).get("current")
    vix3m = points.get("^VIX3M", {}).get("current")
    if vix is None or vix3m is None:
        return None

    spread = round(vix - vix3m, 2)
    ratio = round(vix / vix3m, 4) if vix3m else None

    if spread <= -2.0:
        regime, label_en, label_zh = "contango_deep", "Deep Contango", "深度Contango · 极度平静"
    elif spread <= -0.5:
        regime, label_en, label_zh = "contango", "Contango", "Contango · 正常"
    elif spread < 0.5:
        regime, label_en, label_zh = "flat", "Flat", "Flat · 警戒"
    elif spread < 2.0:
        regime, label_en, label_zh = "backwardation", "Backwardation", "Backwardation · 紧张"
    else:
        regime, label_en, label_zh = "backwardation_deep", "Deep Backwardation", "深度Backwardation · 恐慌"

    # Walk back through VIX/VIX3M history to find last regime flip into backwardation
    last_back = None
    vix_hist = points.get("^VIX", {}).get("history") or []
    v3m_hist = points.get("^VIX3M", {}).get("history") or []
    if vix_hist and v3m_hist:
        v3m_by_date = {h["t"]: h["v"] for h in v3m_hist}
        for h in reversed(vix_hist[:-1]):
            v3 = v3m_by_date.get(h["t"])
            if v3 is None:
                continue
            if h["v"] - v3 >= 0.5:
                last_back = h["t"]
                break

    spread_history = []
    if vix_hist and v3m_hist:
        v3m_by_date = {h["t"]: h["v"] for h in v3m_hist}
        for h in vix_hist:
            v3 = v3m_by_date.get(h["t"])
            if v3 is not None:
                spread_history.append({"t": h["t"], "v": round(h["v"] - v3, 2)})

    return {
        "vix": vix,
        "vix9d": points.get("^VIX9D", {}).get("current"),
        "vix3m": vix3m,
        "vix6m": points.get("^VIX6M", {}).get("current"),
        "spread_1m_3m": spread,
        "ratio_1m_3m": ratio,
        "regime": regime,
        "regime_label_en": label_en,
        "regime_label_zh": label_zh,
        "last_backwardation": last_back,
        "spread_history": spread_history,
        "curve": [
            {"label": "9D",  "value": points.get("^VIX9D", {}).get("current")},
            {"label": "1M",  "value": vix},
            {"label": "3M",  "value": vix3m},
            {"label": "6M",  "value": points.get("^VIX6M", {}).get("current")},
        ],
    }


# ── FRED Macro Monitor ─────────────────────────────────────────────────────────

FRED_SERIES_CONFIG = [
    # (series_id,      label,            unit,  transform,  category)
    # _MOM suffix = virtual ID; fetches from the base series, applies MoM % transform
    ("CPIAUCSL",       "CPI YoY",        "%",   "yoy",      "Inflation"),
    ("CPIAUCSL_MOM",   "CPI MoM",        "%",   "mom_pct",  "Inflation"),
    ("CPILFESL",       "Core CPI YoY",   "%",   "yoy",      "Inflation"),
    ("CPILFESL_MOM",   "Core CPI MoM",   "%",   "mom_pct",  "Inflation"),
    ("PCEPI",          "PCE YoY",        "%",   "yoy",      "Inflation"),
    ("PCEPILFE",       "Core PCE YoY",   "%",   "yoy",      "Inflation"),
    ("PCEPILFE_MOM",   "Core PCE MoM",   "%",   "mom_pct",  "Inflation"),
    ("UNRATE",         "Unemployment",   "%",   "level",    "Employment"),
    ("PAYEMS",         "NFP MoM",        "k",   "mom_k",    "Employment"),
    ("UMCSENT",        "Consumer Sent.", "",    "level",    "Employment"),
    ("DFF",            "Fed Funds",      "%",   "level",    "Interest Rates"),
    ("T10Y2Y",         "Yield Curve",    "%",   "level",    "Interest Rates"),
]

FRED_CATEGORIES = ["Inflation", "Employment", "Interest Rates"]

def _fred_signal(series_id, value):
    """Classify a macro value into a policy signal."""
    if series_id == "DFF":
        return "tightening" if value > 4 else ("dovish" if value < 2 else "neutral")
    if series_id in ("CPIAUCSL", "CPILFESL", "CPIAUCSL_MOM", "CPILFESL_MOM"):
        return "hawkish" if value > 3 else ("dovish" if value < 2 else "neutral")
    if series_id in ("PCEPI", "PCEPILFE", "PCEPILFE_MOM"):
        return "hawkish" if value > 2.5 else ("dovish" if value < 2 else "neutral")
    if series_id == "UNRATE":
        return "dovish" if value > 5 else ("hawkish" if value < 4 else "neutral")
    if series_id == "PAYEMS":
        return "hawkish" if value > 200 else ("dovish" if value < 0 else "neutral")
    if series_id == "UMCSENT":
        return "hawkish" if value > 80 else ("dovish" if value < 60 else "neutral")
    if series_id == "T10Y2Y":
        return "mixed" if value < 0 else ("hawkish" if value > 0.5 else "neutral")
    if series_id == "DCOILBRENTEU":
        return "hawkish" if value > 90 else ("dovish" if value < 60 else "neutral")
    return "neutral"

def fetch_fred_series(api_key, series_id, limit=40):
    """Fetch the last `limit` observations from FRED."""
    try:
        resp = requests.get(
            "https://api.stlouisfed.org/fred/series/observations",
            params={"series_id": series_id, "api_key": api_key,
                    "file_type": "json", "sort_order": "desc", "limit": limit},
            timeout=12)
        resp.raise_for_status()
        pairs = []
        for obs in reversed(resp.json().get("observations", [])):
            try:
                pairs.append((obs["date"], float(obs["value"])))
            except (ValueError, KeyError):
                pass
        return pairs
    except Exception as e:
        print(f"FRED fetch error {series_id}: {e}")
        return []

def fetch_investing_calendar_data():
    """Scrape US economic calendar from investing.com (actual, forecast, dates).
    Returns dict keyed by FRED series_id.
    """
    from bs4 import BeautifulSoup

    # investing.com event name (lowercase) -> FRED series_id
    EVENT_MAP = {
        'cpi y/y':                           'CPIAUCSL',
        'core cpi y/y':                      'CPILFESL',
        'pce price index y/y':               'PCEPI',
        'core pce price index y/y':          'PCEPILFE',
        'core pce price index m/m':          'PCEPILFE',
        'non-farm payrolls':                 'PAYEMS',
        'unemployment rate':                 'UNRATE',
        'michigan consumer sentiment':       'UMCSENT',
        'michigan consumer sentiment prel.': 'UMCSENT',
        'u. of mich. consumer sentiment':    'UMCSENT',
        'u.of mich. consumer sentiment':     'UMCSENT',
    }

    today = datetime.utcnow()
    from_date = (today - timedelta(days=60)).strftime('%Y-%m-%d')
    to_date   = (today + timedelta(days=60)).strftime('%Y-%m-%d')

    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        ),
        'X-Requested-With': 'XMLHttpRequest',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.investing.com/economic-calendar/',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://www.investing.com',
    }
    payload = (
        'country%5B%5D=5'          # US
        '&importance%5B%5D=3'      # High importance only
        f'&dateFrom={from_date}'
        f'&dateTo={to_date}'
        '&timeZone=8'
        '&timeFilter=timeOnly'
        '&currentTab=custom'
        '&submitFilters=1'
        '&limit_from=0'
    )

    resp = requests.post(
        'https://www.investing.com/economic-calendar/Service/getCalendarFilteredData',
        headers=headers, data=payload, timeout=25)
    resp.raise_for_status()
    html_content = resp.json().get('data', '')
    soup = BeautifulSoup(html_content, 'html.parser')

    def parse_num(text):
        t = text.strip().replace('%', '').replace('K', '').replace(',', '').strip()
        try:
            return float(t)
        except Exception:
            return None

    result = {}
    current_date = None
    today_str = today.strftime('%Y-%m-%d')

    for row in soup.find_all('tr'):
        classes = row.get('class', [])

        # Date header rows
        if 'theDay' in classes:
            raw = row.get_text(strip=True)
            for fmt in ('%A, %B %d, %Y', '%B %d, %Y'):
                try:
                    current_date = datetime.strptime(raw, fmt).strftime('%Y-%m-%d')
                    break
                except ValueError:
                    pass
            continue

        row_id = row.get('id', '')
        if not row_id.startswith('eventRowId_') or current_date is None:
            continue

        cells = row.find_all('td')
        if len(cells) < 6:
            continue

        event_name = cells[3].get_text(strip=True).lower()
        series_id  = EVENT_MAP.get(event_name)
        if not series_id:
            continue

        actual_txt   = cells[4].get_text(strip=True) if len(cells) > 4 else ''
        forecast_txt = cells[5].get_text(strip=True) if len(cells) > 5 else ''
        actual   = parse_num(actual_txt)   if actual_txt   else None
        forecast = parse_num(forecast_txt) if forecast_txt else None

        entry = result.setdefault(series_id, {})

        if current_date <= today_str:
            # Keep the most recent past release
            if not entry.get('date') or current_date > entry['date']:
                entry['date']     = current_date
                entry['actual']   = actual
                entry['forecast'] = forecast
        else:
            # Keep the nearest upcoming release
            if not entry.get('next_date') or current_date < entry['next_date']:
                entry['next_date']     = current_date
                entry['next_forecast'] = forecast

    return result


def fetch_finnhub_calendar(finnhub_api_key):
    """Fetch US economic calendar from Finnhub (actual + consensus estimate + dates)."""
    # Maps fragments of Finnhub event names (lowercase) to FRED series IDs
    EVENT_MAP = [
        # MoM variants first — must match before the general 'core cpi' / 'cpi' fragments
        ('core cpi mom',        'CPILFESL_MOM'),
        ('core cpi m/m',        'CPILFESL_MOM'),
        ('cpi mom',             'CPIAUCSL_MOM'),
        ('cpi m/m',             'CPIAUCSL_MOM'),
        # YoY / general
        ('core cpi yoy',        'CPILFESL'),
        ('core cpi y/y',        'CPILFESL'),
        ('core cpi',            'CPILFESL'),
        ('cpi yoy',             'CPIAUCSL'),
        ('cpi y/y',             'CPIAUCSL'),
        ('core pce mom',        'PCEPILFE_MOM'),
        ('core pce m/m',        'PCEPILFE_MOM'),
        ('core pce',            'PCEPILFE'),
        ('pce price index',     'PCEPI'),
        ('nonfarm payroll',     'PAYEMS'),
        ('non farm payroll',    'PAYEMS'),
        ('non-farm payroll',    'PAYEMS'),
        ('unemployment rate',   'UNRATE'),
        ('michigan consumer',   'UMCSENT'),
        ('u. of mich',          'UMCSENT'),
        ('consumer sentiment',  'UMCSENT'),
    ]

    today = datetime.utcnow()
    from_date = (today - timedelta(days=60)).strftime('%Y-%m-%d')
    to_date   = (today + timedelta(days=60)).strftime('%Y-%m-%d')

    resp = requests.get(
        'https://finnhub.io/api/v1/calendar/economic',
        params={'from': from_date, 'to': to_date, 'token': finnhub_api_key},
        timeout=15)
    resp.raise_for_status()
    events = [e for e in resp.json().get('economicCalendar', [])
              if e.get('country', '').upper() == 'US']
    print(f"  Finnhub: {len(events)} US events fetched")

    result = {}
    today_str = today.strftime('%Y-%m-%d')

    for event in events:
        name = event.get('event', '').lower()
        series_id = next((sid for frag, sid in EVENT_MAP if frag in name), None)
        if not series_id:
            continue

        time_str = event.get('time', '')
        try:
            event_date = time_str[:10]
            datetime.strptime(event_date, '%Y-%m-%d')  # validate
        except Exception:
            continue

        actual   = event.get('actual')
        estimate = event.get('estimate')
        entry = result.setdefault(series_id, {})

        if event_date <= today_str:
            if not entry.get('date') or event_date > entry['date']:
                entry['date']     = event_date
                entry['forecast'] = estimate   # consensus estimate for that release
        else:
            if not entry.get('next_date') or event_date < entry['next_date']:
                entry['next_date']     = event_date
                entry['next_forecast'] = estimate

    print(f"  Finnhub: matched {len(result)} series")
    return result


def fetch_economic_forecasts_perplexity(perplexity_api_key):
    """Fetch consensus forecasts for key US indicators via Perplexity."""
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    prompt = (
        f"Today is {today_str}. Return a JSON object (and NOTHING else — no prose, no markdown) "
        "with the most recent release data and next scheduled release for each US economic indicator. "
        "Use exactly these keys:\n"
        '{"CPIAUCSL":{"forecast":2.9,"date":"2025-03-12","next_date":"2025-04-10","next_forecast":2.6},'
        '"CPIAUCSL_MOM":{"forecast":0.3,"date":"2025-03-12","next_date":"2025-04-10","next_forecast":0.2},'
        '"CPILFESL":{"forecast":3.2,"date":"2025-03-12","next_date":"2025-04-10","next_forecast":3.0},'
        '"CPILFESL_MOM":{"forecast":0.3,"date":"2025-03-12","next_date":"2025-04-10","next_forecast":0.3},'
        '"PCEPI":{"forecast":2.6,"date":"2025-02-28","next_date":"2025-03-28","next_forecast":2.4},'
        '"PCEPILFE":{"forecast":2.9,"date":"2025-02-28","next_date":"2025-03-28","next_forecast":2.7},'
        '"PCEPILFE_MOM":{"forecast":0.3,"date":"2025-02-28","next_date":"2025-03-28","next_forecast":0.3},'
        '"UNRATE":{"forecast":4.1,"date":"2025-03-07","next_date":"2025-04-04","next_forecast":4.1},'
        '"PAYEMS":{"forecast":160,"date":"2025-03-07","next_date":"2025-04-04","next_forecast":140},'
        '"UMCSENT":{"forecast":65.0,"date":"2025-03-14","next_date":"2025-04-11","next_forecast":63.0}}'
        "\nUnits: forecast/next_forecast are YoY% for CPIAUCSL/CPILFESL/PCEPI/PCEPILFE, "
        "MoM% for CPIAUCSL_MOM/CPILFESL_MOM, level% for UNRATE, "
        "thousands (integer) for PAYEMS NFP, level for UMCSENT. "
        "date = most recent release date YYYY-MM-DD. next_date = next scheduled release YYYY-MM-DD."
    )
    try:
        resp = requests.post(
            "https://api.perplexity.ai/chat/completions",
            json={
                "model": "sonar",
                "messages": [
                    {"role": "system", "content": "You output only raw JSON with no markdown, no code fences, no explanation."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0,
            },
            headers={"Authorization": f"Bearer {perplexity_api_key}", "Content-Type": "application/json"},
            timeout=35,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"].strip()
        print(f"  Perplexity raw response (first 300 chars): {content[:300]}")
        # Extract first JSON object from response regardless of surrounding text
        m = re.search(r'\{[\s\S]*\}', content)
        if not m:
            raise ValueError(f"No JSON object found in response")
        result = json.loads(m.group())
        print(f"  Parsed forecast keys: {list(result.keys())}")
        return result
    except Exception as e:
        print(f"  Perplexity forecast error: {e}")
        return {}

def fetch_fred_release_id(api_key, series_id):
    """Return the FRED release ID for a series."""
    try:
        resp = requests.get(
            "https://api.stlouisfed.org/fred/series/release",
            params={"series_id": series_id, "api_key": api_key, "file_type": "json"},
            timeout=10)
        resp.raise_for_status()
        releases = resp.json().get("releases", [])
        return releases[0]["id"] if releases else None
    except Exception:
        return None

def fetch_fred_release_dates(api_key, release_id):
    """Return (most_recent_past_date, next_date) for a FRED release."""
    today = datetime.utcnow().date().isoformat()
    past_date = next_date = None
    try:
        r = requests.get(
            "https://api.stlouisfed.org/fred/release/dates",
            params={"release_id": release_id, "api_key": api_key, "file_type": "json",
                    "realtime_end": today, "sort_order": "desc", "limit": 2},
            timeout=10)
        r.raise_for_status()
        dates = r.json().get("release_dates", [])
        past_date = dates[0]["date"] if dates else None
    except Exception:
        pass
    try:
        r = requests.get(
            "https://api.stlouisfed.org/fred/release/dates",
            params={"release_id": release_id, "api_key": api_key, "file_type": "json",
                    "realtime_start": today, "sort_order": "asc", "limit": 3,
                    "include_release_dates_with_no_data": "true"},
            timeout=10)
        r.raise_for_status()
        dates = r.json().get("release_dates", [])
        next_date = dates[0]["date"] if dates else None
    except Exception:
        pass
    return past_date, next_date

# Keep old name as thin wrapper for any existing callers
def fetch_fred_next_release_date(api_key, release_id):
    _, next_date = fetch_fred_release_dates(api_key, release_id)
    return next_date

def create_fred_sparkline(values, series_id, charts_dir, color="#38bdf8"):
    """Thin line + fill sparkline for a FRED series."""
    try:
        if not values or len(values) < 2:
            return None
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(8, 2))
        fig.patch.set_facecolor('#060d1c')
        ax.set_facecolor('#060d1c')
        xs = range(len(values))
        ax.plot(xs, values, color=color, lw=1.8, solid_capstyle='round')
        ax.fill_between(xs, values, min(values), alpha=0.18, color=color)
        ax.set_xticks([]); ax.set_yticks([])
        for s in ax.spines.values(): s.set_visible(False)
        fig.tight_layout(pad=0)
        safe = re.sub(r'[^a-zA-Z0-9]', '_', series_id)
        path = os.path.join(charts_dir, f"fred_{safe}.png")
        fig.savefig(path, format='png', dpi=80, bbox_inches='tight', facecolor='#060d1c')
        plt.close(fig)
        return f"data/charts/fred_{safe}.png"
    except Exception as e:
        print(f"FRED sparkline error {series_id}: {e}")
        return None

def build_macro_fred(api_key, charts_dir, finnhub_api_key=None, perplexity_api_key=None):
    """Fetch FRED series, transform, classify signals, generate sparklines."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    SIGNAL_COLORS = {"hawkish": "#ef4444", "dovish": "#10b981",
                     "tightening": "#f59e0b", "neutral": "#38bdf8", "mixed": "#8b5cf6"}
    # _MOM virtual IDs re-use the base series data — deduplicate FRED fetches
    def _real_sid(sid):
        return sid[:-4] if sid.endswith('_MOM') else sid

    # Ordered dedup so fetch order is deterministic
    seen_real = set()
    real_sids_list = []
    for sid, *_ in FRED_SERIES_CONFIG:
        real = _real_sid(sid)
        if real not in seen_real:
            seen_real.add(real)
            real_sids_list.append(real)

    raw = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(fetch_fred_series, api_key, rsid, 40): rsid
                for rsid in real_sids_list}
        for f in as_completed(futs):
            raw[futs[f]] = f.result()

    # Retry any series that came back empty (transient FRED API failures)
    failed = [rsid for rsid in real_sids_list if not raw.get(rsid)]
    if failed:
        print(f"  Retrying {len(failed)} empty FRED series: {failed}")
        time.sleep(2)
        for rsid in failed:
            raw[rsid] = fetch_fred_series(api_key, rsid, 40)

    series_out = {}
    signal_counts = {"hawkish": 0, "dovish": 0, "tightening": 0, "neutral": 0, "mixed": 0}

    for series_id, label, unit, transform, category in FRED_SERIES_CONFIG:
        pairs = raw.get(_real_sid(series_id), [])
        if not pairs:
            continue
        values = [v for _, v in pairs]

        if transform == "yoy":
            if len(values) < 13:
                continue
            transformed = [(values[i] / values[i-12] - 1) * 100
                           for i in range(12, len(values)) if values[i-12] != 0]
            spark = transformed[-24:]
        elif transform == "mom_pct":
            if len(values) < 2:
                continue
            transformed = [(values[i] / values[i-1] - 1) * 100
                           for i in range(1, len(values)) if values[i-1] != 0]
            spark = transformed[-24:]
        elif transform == "mom_k":
            if len(values) < 2:
                continue
            transformed = [(values[i] - values[i-1]) for i in range(1, len(values))]
            spark = transformed[-24:]
        else:
            transformed = values
            spark = values[-24:]

        if not transformed:
            continue
        current = round(transformed[-1], 2)
        prev    = round(transformed[-2], 2) if len(transformed) >= 2 else None
        signal  = _fred_signal(series_id, current)
        signal_counts[signal] = signal_counts.get(signal, 0) + 1
        chart = create_fred_sparkline(spark, series_id, charts_dir, SIGNAL_COLORS.get(signal, "#38bdf8"))
        series_out[series_id] = {
            "label": label, "value": current, "prev": prev,
            "change": round(current - prev, 2) if prev is not None else None,
            "unit": unit, "signal": signal, "chart": chart,
            "category": category, "last_date": pairs[-1][0] if pairs else None,
        }

    # Fetch release IDs, then both past and next release dates from FRED
    from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed
    release_ids = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        # Use real FRED series ID for release lookup (_MOM virtual IDs share the same release)
        futs2 = {ex.submit(fetch_fred_release_id, api_key, _real_sid(sid)): sid
                 for sid in series_out}
        for f in _as_completed(futs2):
            release_ids[futs2[f]] = f.result()

    unique_rids = set(v for v in release_ids.values() if v)
    fred_dates = {}   # rid -> (past_date, next_date)
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs3 = {ex.submit(fetch_fred_release_dates, api_key, rid): rid
                 for rid in unique_rids}
        for f in _as_completed(futs3):
            fred_dates[futs3[f]] = f.result()

    for sid, d in series_out.items():
        rid = release_ids.get(sid)
        past_date, next_date = fred_dates.get(rid, (None, None))
        d["release_date"] = past_date or d.get("last_date")
        d["next_release"]  = next_date

    # Fetch consensus forecasts — Finnhub for dates + estimates, Perplexity fills any gaps
    fc_data = {}
    if finnhub_api_key:
        print("  Fetching calendar from Finnhub...")
        try:
            fc_data = fetch_finnhub_calendar(finnhub_api_key)
        except Exception as e:
            print(f"  Finnhub failed ({e})")

    # Always try Perplexity for forecasts if any series is missing an estimate
    missing_forecasts = not fc_data or not any(
        v.get("forecast") is not None or v.get("next_forecast") is not None
        for v in fc_data.values()
    )
    if missing_forecasts and perplexity_api_key:
        print("  Fetching consensus forecasts via Perplexity (Finnhub estimates missing)...")
        perp_data = fetch_economic_forecasts_perplexity(perplexity_api_key)
        print(f"  Perplexity: got forecast data for {len(perp_data)} series")
        for sid, pd in perp_data.items():
            entry = fc_data.setdefault(sid, {})
            if entry.get("forecast") is None:
                entry["forecast"] = pd.get("forecast")
            if entry.get("next_forecast") is None:
                entry["next_forecast"] = pd.get("next_forecast")
            # Fill next_date from Perplexity if Finnhub didn't have it
            if not entry.get("next_date") and pd.get("next_date"):
                entry["next_date"] = pd["next_date"]

    for sid, d in series_out.items():
        fc = fc_data.get(sid, {})
        d["forecast"]      = fc.get("forecast")
        d["next_forecast"] = fc.get("next_forecast")
        # Prefer Finnhub next_date — it has the exact day (e.g. Apr 10).
        # FRED only returns a month-start placeholder (e.g. 2026-05-01) for future dates.
        if fc.get("next_date"):
            d["next_release"] = fc.get("next_date")
        # FRED date is the fallback when Finnhub has nothing

    # NFP YTD comparison — uses PAYEMS absolute levels already in raw[]
    payems_ytd = {}
    payems_pairs = raw.get('PAYEMS', [])
    if payems_pairs:
        by_ym = {(p[0][:4], p[0][5:7]): p[1] for p in payems_pairs}
        cur_date   = payems_pairs[-1][0]
        cur_year   = cur_date[:4]
        cur_month  = cur_date[5:7]
        prev_year  = str(int(cur_year) - 1)
        prev2_year = str(int(cur_year) - 2)

        cur_level       = by_ym.get((cur_year, cur_month))
        dec_prev        = by_ym.get((prev_year, '12'))
        prev_same_month = by_ym.get((prev_year, cur_month))
        dec_prev2       = by_ym.get((prev2_year, '12'))

        if cur_level is not None and dec_prev is not None:
            payems_ytd['ytd_current']  = round(cur_level - dec_prev)
            payems_ytd['ytd_year']     = cur_year
            payems_ytd['ytd_month']    = cur_month
        if prev_same_month is not None and dec_prev2 is not None:
            payems_ytd['ytd_prev']      = round(prev_same_month - dec_prev2)
            payems_ytd['ytd_prev_year'] = prev_year
        if payems_ytd.get('ytd_current') is not None and payems_ytd.get('ytd_prev') is not None:
            payems_ytd['ytd_diff'] = payems_ytd['ytd_current'] - payems_ytd['ytd_prev']

    # Narrative
    dominant = max(signal_counts, key=signal_counts.get) if signal_counts else "neutral"
    parts = []
    if "CPIAUCSL" in series_out:
        parts.append(f"CPI at {series_out['CPIAUCSL']['value']:.1f}%")
    if "DFF" in series_out:
        parts.append(f"Fed Funds at {series_out['DFF']['value']:.2f}%")
    if "UNRATE" in series_out:
        parts.append(f"unemployment at {series_out['UNRATE']['value']:.1f}%")
    narrative = f"Leaning {dominant.capitalize()}"
    if parts:
        narrative += ": " + ", ".join(parts) + "."

    return {
        "series": series_out,
        "signal_counts": signal_counts,
        "dominant_signal": dominant,
        "narrative": narrative,
        "series_order": [s[0] for s in FRED_SERIES_CONFIG if s[0] in series_out],
        "categories": FRED_CATEGORIES,
        "payems_ytd": payems_ytd,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="data", help="Output directory (default: data)")
    args = parser.parse_args()
    out_dir = args.out_dir
    charts_dir = os.path.join(out_dir, "charts")
    ohlc_dir = os.path.join(out_dir, "ohlc")
    os.makedirs(charts_dir, exist_ok=True)
    os.makedirs(ohlc_dir, exist_ok=True)

    print("Fetching economic events...")
    events = get_upcoming_key_events()

    # SPY baseline is loaded AFTER OpenD prefetch (further down), so it reads
    # from the freshly-written data/ohlc/SPY.json instead of going through Yahoo.
    # Yahoo's stale responses for SPY were poisoning the stale-data detector
    # and triggering bad Finnhub rescues that overwrote OpenD's correct OHLC.
    _spy_cache = None
    spy_ref_date = None

    def _blank_row(ticker, prev_row=None):
        """Stub row used when both primary OpenD + Yahoo fallback fail.
        Preserves identity (ticker, name) but zeroes all price/change fields,
        so the frontend renders a visibly-blank cell instead of yesterday's stale price."""
        return {
            "ticker": ticker,
            "name": (prev_row or {}).get("name") or ticker,
            "data_date": None,
            "current_price": None,
            "change": None,
            "change_pct": None,
            "wtd_pct": None,
            "mtd_pct": None,
            "ytd_pct": None,
            "volume_avg": None,
            "blank_reason": "no fresh data — stale fallback disabled",
        }

    # Load previous snapshot — used for vol_signals/options_intel/factor_regime
    # block-level fallback only. Per-ticker rows do NOT fall back to stale data.
    prev_ticker_data = {}
    prev_snap = {}
    snapshot_path = os.path.join(out_dir, "snapshot.json")
    if os.path.exists(snapshot_path):
        try:
            with open(snapshot_path, encoding="utf-8") as f:
                prev_snap = json.load(f)
            for grp in prev_snap.get("groups", {}).values():
                for row in grp:
                    if row.get("ticker"):
                        prev_ticker_data[row["ticker"]] = row
            print(f"Loaded previous snapshot: {len(prev_ticker_data)} tickers available as fallback")
        except Exception as e:
            print(f"Could not load previous snapshot: {e}")

    # ── Early fetches (low call count, run before main loop while rate limit is fresh) ──

    print("Fetching volatility signals...")
    vol_signals = fetch_vol_signals()
    _vs_ok = sum(1 for cat in vol_signals.values() for item in cat if item.get('current') is not None)
    _vs_total = sum(len(cat) for cat in vol_signals.values())
    if _vs_ok == 0 and _vs_total > 0:
        prev_vs = prev_snap.get("vol_signals")
        if prev_vs:
            vol_signals = prev_vs
            print(f"  vol_signals: all {_vs_total} failed — using previous snapshot data")
        else:
            print(f"  vol_signals: all {_vs_total} failed — no fallback available")
    else:
        for cat, items in vol_signals.items():
            for item in items:
                if item['current'] is not None:
                    print(f"  {item['name']}: {item['current']:.2f} (vs MA20: {item['vs_ma']:+.1f}%)")
                else:
                    print(f"  {item['name']}: no data")

    time.sleep(2)
    print("Computing options intelligence (OpenD primary)...")
    options_intel = build_options_intel_opend(OPTIONS_INTEL_TICKERS, prev_ticker_data)
    if not options_intel or len(options_intel) < len(OPTIONS_INTEL_TICKERS) // 2:
        # OpenD totally failed or covered less than half — try yfinance for the rest
        print("  options_intel: OpenD partial/empty, falling back to yfinance...")
        missing = [t for t in OPTIONS_INTEL_TICKERS if t not in (options_intel or {})]
        yf_oi = build_options_intel(missing) if missing else {}
        options_intel = {**(options_intel or {}), **yf_oi}
    if not options_intel:
        prev_oi = prev_snap.get("options_intel")
        if prev_oi:
            options_intel = prev_oi
            print("  options_intel: all failed — using previous snapshot data")
        else:
            print("  options_intel: all failed — no fallback available")

    time.sleep(2)
    FACTOR_ETFS = {"VLUE": "Value", "MTUM": "Momentum", "QUAL": "Quality", "SIZE": "Size", "IWF": "Growth"}
    print("Computing factor regime...")
    factor_regime = {}
    try:
        spy_2y = yf.Ticker("SPY").history(period="2y")
        time.sleep(0.5)
        spy_2y_returns = spy_2y['Close'].pct_change().dropna()
        for fticker, fname in FACTOR_ETFS.items():
            try:
                fhist = yf.Ticker(fticker).history(period="2y")
                time.sleep(0.5)
                if len(fhist) < 200:
                    print(f"  {fticker}: insufficient data ({len(fhist)} bars)")
                    continue
                freturns = fhist['Close'].pct_change().dropna()
                common = freturns.index.intersection(spy_2y_returns.index)
                if len(common) < 200:
                    continue
                active = freturns[common] - spy_2y_returns[common]
                ewma = active.ewm(halflife=90).mean()
                exp_mean = ewma.expanding().mean()
                exp_std = ewma.expanding().std()
                zscore = ((ewma - exp_mean) / exp_std).ewm(halflife=30).mean()
                latest_z = float(zscore.iloc[-1])
                regime = "BULL" if latest_z >= 0 else "BEAR"
                signs = (zscore >= 0).astype(int)
                changes = signs.diff().abs()
                last_change_idx = changes[changes == 1].index
                last_change = last_change_idx[-1] if len(last_change_idx) > 0 else zscore.index[0]
                days_in = len(zscore[zscore.index >= last_change])
                factor_regime[fticker] = {
                    "name": fname, "regime": regime,
                    "zscore": round(latest_z, 2), "days_in_regime": days_in,
                }
                print(f"  {fticker} ({fname}): {regime} z={latest_z:.2f} ({days_in}d)")
            except Exception as e:
                print(f"  Factor {fticker} error: {e}")
    except Exception as e:
        print(f"  SPY 2y fetch error: {e}")
    if not factor_regime:
        prev_fr = prev_snap.get("factor_regime")
        if prev_fr:
            factor_regime = prev_fr
            print("  factor_regime: all failed — using previous snapshot data")
        else:
            print("  factor_regime: all failed — no fallback available")

    # ── Cooldown before main ticker loop ──
    time.sleep(5)

    # ── Batch prefetch: collapse ~300 individual requests into ~6 bulk requests ──
    all_syms = []
    _seen = set()
    # Frozen groups (e.g. Countries) reuse last snapshot — exclude their unique
    # tickers from the prefetch to save Yahoo bandwidth.
    _frozen_only = set()
    for _g, _ts in STOCK_GROUPS.items():
        if _g in FROZEN_GROUPS:
            _frozen_only.update(_ts)
    _shared_with_active = set()
    for _g, _ts in STOCK_GROUPS.items():
        if _g not in FROZEN_GROUPS:
            _shared_with_active.update(_ts)
    _frozen_only -= _shared_with_active
    for _g, _ts in STOCK_GROUPS.items():
        for _t in _ts:
            if _t in _frozen_only:
                continue
            if _t not in _seen:
                _seen.add(_t)
                all_syms.append(_t)
    # Also include theme + cross-asset tickers the post-loop pass fetches
    try:
        _extra = set(t for tickers in AI_THEMES.values() for t in tickers) | set(THEME_ETF_PROXY.values()) | {"HYG", "TLT", "VIXY", "USO", "UNG", "UUP", "LQD", "IEF", "SHY"}
        for _t in sorted(_extra - _seen):
            all_syms.append(_t)
    except NameError:
        pass
    try:
        from fetch_opend import populate_batch_cache
        opend_cached = populate_batch_cache(_BATCH_CACHE, all_syms)
    except Exception as e:
        print(f"[opend] pre-pass failed: {e}")
        opend_cached = set()
    yahoo_syms = [s for s in all_syms if s not in opend_cached]
    print(f"[opend] {len(opend_cached)} via OpenD; {len(yahoo_syms)} residual via Yahoo")
    prefetch_histories(yahoo_syms, chunk_size=50, inter_chunk_sleep=6)

    # SPY baseline: prefer OpenD's fresh data/ohlc/SPY.json (just written above).
    # Yahoo fallback only if OpenD failed for SPY.
    print("Loading SPY history baseline...")
    spy_json_path = os.path.join(ohlc_dir, "SPY.json")
    if os.path.exists(spy_json_path):
        try:
            _spy_d = json.load(open(spy_json_path, encoding="utf-8"))
            _spy_bars = _spy_d.get("ohlc") or []
            if len(_spy_bars) >= 50:
                _spy_df = pd.DataFrame(_spy_bars)
                _spy_df["Date"] = pd.to_datetime(_spy_df["t"]).dt.tz_localize("America/New_York")
                _spy_df = (_spy_df.set_index("Date")
                                  .rename(columns={"o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume"})
                                  [["Open", "High", "Low", "Close", "Volume"]]
                                  .sort_index())
                _spy_cache = _spy_df
                print(f"  [OK] SPY baseline from OpenD: {len(_spy_cache)} bars, last={_spy_cache.index[-1].date()}")
        except Exception as e:
            print(f"  [WARN] Failed to load SPY baseline from OpenD: {e}")
    if _spy_cache is None or len(_spy_cache) < 50:
        print("  [fallback] Loading SPY baseline from yfinance...")
        try:
            _spy_cache = yf.Ticker("SPY").history(period="400d")
        except Exception as e:
            print(f"  [WARN] yfinance SPY fallback failed: {e}")
            _spy_cache = None
    if _spy_cache is not None and len(_spy_cache) >= 1:
        spy_ref_date = _spy_cache.index[-1].date()
        print(f"SPY reference date: {spy_ref_date}")

    # Write OHLC files for theme-proxy ETFs. They were fetched into _BATCH_CACHE
    # above, but no group loop calls get_stock_data() on them, so without this
    # explicit write their data/ohlc/<sym>.json never lands on disk — and
    # rescue_finnhub's find_or_derive_row() then falls back to constituent
    # averages, silently mislabeling the theme with etf=<proxy> while not
    # actually using proxy prices.
    for _proxy in set(THEME_ETF_PROXY.values()):
        _hist = _BATCH_CACHE.get(_proxy)
        if _hist is None or len(_hist) < 2:
            continue
        try:
            _safe = re.sub(r'[^a-zA-Z0-9]', '_', _proxy)
            _path = os.path.join(ohlc_dir, f"{_safe}.json")
            _rows = [
                {"t": idx.date().isoformat(),
                 "o": round(float(r['Open']), 4), "h": round(float(r['High']), 4),
                 "l": round(float(r['Low']), 4),  "c": round(float(r['Close']), 4),
                 "v": int(r['Volume'])}
                for idx, r in _hist.tail(260).iterrows()
            ]
            _name = ""
            try:
                _name = json.load(open(_path, encoding="utf-8")).get("name", "") or ""
            except Exception:
                pass
            with open(_path, "w") as _f:
                json.dump({"ticker": _proxy, "name": _name, "ohlc": _rows}, _f, separators=(",", ":"))
            print(f"  [proxy] wrote {_proxy}.json ({len(_rows)} bars, last={_rows[-1]['t']})")
        except Exception as _e:
            print(f"  [proxy] write failed for {_proxy}: {_e}")

    print("Fetching stock data (no Liquid Stocks)...")
    groups_data = {}
    all_ticker_data = {}
    failed_tickers = []
    fallback_tickers = []
    _fetch_count = 0
    def _rate_sleep():
        nonlocal _fetch_count
        _fetch_count += 1
        time.sleep(0.6)
        # Longer pause every 25 tickers to avoid Yahoo rate-limit
        if _fetch_count % 25 == 0:
            print(f"    [rate-limit pause after {_fetch_count} fetches]")
            time.sleep(6)

    for group_name, tickers in STOCK_GROUPS.items():
        rows = []
        if group_name in FROZEN_GROUPS:
            # Frozen group — reuse last snapshot's rows verbatim, skip fetch.
            kept = 0
            for ticker in tickers:
                prev = prev_ticker_data.get(ticker)
                if prev:
                    rows.append(prev)
                    all_ticker_data[ticker] = prev
                    kept += 1
            print(f"  [{group_name}] FROZEN — reused {kept}/{len(tickers)} prior rows (no fetch)")
            groups_data[group_name] = rows
            continue
        for i, ticker in enumerate(tickers):
            if ticker in all_ticker_data:
                # already fetched from a previous group — reuse cached data
                rows.append(all_ticker_data[ticker])
                continue
            print(f"  [{group_name}] {i+1}/{len(tickers)} {ticker}")
            row = get_stock_data(ticker, charts_dir, spy_hist=_spy_cache, ohlc_dir=ohlc_dir)
            if row:
                rows.append(row)
                all_ticker_data[ticker] = row
            else:
                # Reserve a blank slot so the retry pass can update it in place;
                # never substitute yesterday's prices (user explicitly disabled stale fallback).
                blank = _blank_row(ticker, prev_ticker_data.get(ticker))
                rows.append(blank)
                all_ticker_data[ticker] = blank
                failed_tickers.append(ticker)
                print(f"    -> blank placeholder for {ticker} (will retry)")
            _rate_sleep()
        groups_data[group_name] = rows

    if fallback_tickers:
        print(f"Used previous data for {len(fallback_tickers)} rate-limited tickers")

    # Multi-pass retry with exponential cooldowns — bias for completeness over speed.
    # Each pass re-attempts everything that's still missing or on fallback data.
    retry_cooldowns = [30, 90, 180]
    for attempt, cooldown in enumerate(retry_cooldowns, start=1):
        retry_candidates = list(set(fallback_tickers + failed_tickers))
        if not retry_candidates:
            break
        print(f"\n[Retry pass {attempt}/{len(retry_cooldowns)}] {len(retry_candidates)} tickers — cooling {cooldown}s then retrying at 1.0s/ticker...")
        time.sleep(cooldown)
        retried_ok = 0
        for ticker in retry_candidates:
            row = get_stock_data(ticker, charts_dir, spy_hist=_spy_cache, ohlc_dir=ohlc_dir)
            if row:
                all_ticker_data[ticker] = row
                for gname, grow in groups_data.items():
                    for j, r in enumerate(grow):
                        if r.get("ticker") == ticker:
                            grow[j] = row
                if ticker in fallback_tickers:
                    fallback_tickers.remove(ticker)
                if ticker in failed_tickers:
                    failed_tickers.remove(ticker)
                retried_ok += 1
            # Slower pace on retry — we already know Yahoo was pushing back
            time.sleep(1.0)
        print(f"[Retry pass {attempt}] recovered {retried_ok}/{len(retry_candidates)} tickers")
        if retried_ok == 0:
            # No progress this pass — try the next cooldown, but if that's the last one, stop early
            if attempt < len(retry_cooldowns):
                print(f"[Retry pass {attempt}] no progress — will try again after {retry_cooldowns[attempt]}s")
            continue

    # Fetch any AI_THEMES tickers + Fear & Greed tickers + cross-asset tickers not already fetched
    theme_ticker_set = set(t for tickers in AI_THEMES.values() for t in tickers) | set(THEME_ETF_PROXY.values()) | {"HYG", "TLT", "VIXY", "USO", "UNG", "UUP", "LQD", "IEF", "SHY"}
    for ticker in sorted(theme_ticker_set - set(all_ticker_data.keys())):
        print(f"  [AI Themes] {ticker}")
        row = get_stock_data(ticker, charts_dir, spy_hist=_spy_cache, ohlc_dir=ohlc_dir)
        if row:
            all_ticker_data[ticker] = row
        else:
            all_ticker_data[ticker] = _blank_row(ticker, prev_ticker_data.get(ticker))
            failed_tickers.append(ticker)
            print(f"    -> blank placeholder for {ticker}")
        _rate_sleep()

    # Detect stale tickers by comparing data_date against SPY's reference date
    stale_tickers = []
    if spy_ref_date:
        for ticker, row in all_ticker_data.items():
            ticker_date = row.get("data_date")
            if ticker_date and ticker_date != spy_ref_date.isoformat():
                stale_tickers.append(ticker)
        if stale_tickers:
            print(f"\nDetected {len(stale_tickers)} stale tickers (date != {spy_ref_date}):")
            for t in stale_tickers:
                print(f"  {t}: data_date={all_ticker_data[t].get('data_date')}")
            print(f"Re-fetching {len(stale_tickers)} stale tickers — 2 passes with cooldowns...")
            stale_recovered = 0
            remaining_stale = list(stale_tickers)
            for sattempt, scooldown in enumerate([30, 90], start=1):
                if not remaining_stale:
                    break
                print(f"  [Stale pass {sattempt}] {len(remaining_stale)} tickers — cooling {scooldown}s...")
                time.sleep(scooldown)
                recovered_this = []
                for ticker in remaining_stale:
                    row = get_stock_data(ticker, charts_dir, spy_hist=_spy_cache, ohlc_dir=ohlc_dir)
                    if row and row.get("data_date") == spy_ref_date.isoformat():
                        all_ticker_data[ticker] = row
                        for gname, grow in groups_data.items():
                            for j, r in enumerate(grow):
                                if r.get("ticker") == ticker:
                                    grow[j] = row
                        recovered_this.append(ticker)
                        stale_recovered += 1
                    time.sleep(1.0)
                remaining_stale = [t for t in remaining_stale if t not in recovered_this]
                print(f"  [Stale pass {sattempt}] recovered {len(recovered_this)} / still stale: {len(remaining_stale)}")
            print(f"Stale recovery: {stale_recovered}/{len(stale_tickers)} tickers updated")
            still_stale = [t for t in stale_tickers
                           if all_ticker_data[t].get("data_date") != spy_ref_date.isoformat()]
            if still_stale:
                print(f"WARNING: {len(still_stale)} tickers still stale after retry: {still_stale}")
        else:
            print("No stale tickers detected — all dates match SPY reference date")

    # Build AI themes summary (equal-weighted averages)
    themes_data = []
    for theme_name, tickers in AI_THEMES.items():
        t_rows = [all_ticker_data[t] for t in tickers if t in all_ticker_data]
        def _theme_avg(key, ndec=2, rows=t_rows):
            vals = [r.get(key) for r in rows if r.get(key) is not None]
            return round(sum(vals) / len(vals), ndec) if vals else None
        vol_histories = [r.get("vol_history") for r in t_rows if r.get("vol_history") and len(r.get("vol_history")) == 20]
        vol_chart_path = None
        if vol_histories:
            avg_vol = np.mean(vol_histories, axis=0).tolist()
            safe_name = re.sub(r'[^a-zA-Z0-9]', '_', theme_name)
            vol_chart_path = create_vol_chart_png(avg_vol, "Theme_" + safe_name, charts_dir)
        # Use ETF proxy data if available (e.g. DRAM for Memory)
        proxy_ticker = THEME_ETF_PROXY.get(theme_name)
        proxy = all_ticker_data.get(proxy_ticker) if proxy_ticker else None
        def _val(key, ndec=2):
            if proxy and proxy.get(key) is not None:
                return round(proxy[key], ndec)
            return _theme_avg(key, ndec)

        themes_data.append({
            "name": theme_name,
            "tickers": tickers,
            "etf": proxy_ticker if proxy else None,
            "daily": _val("daily"),
            "intra": _val("intra"),
            "wtd": _val("wtd"),
            "5d": _val("5d"),
            "20d": _val("20d"),
            "ytd": _val("ytd"),
            "vol_ratio": _val("vol_ratio"),
            "atr_pct": _val("atr_pct", 1),
            "dist_sma50_atr": _val("dist_sma50_atr"),
            "rs": _val("rs"),
            "vol_chart": vol_chart_path,
            "constituent_daily": {t: all_ticker_data[t].get("daily") for t in tickers if t in all_ticker_data},
        })

    # Build "The 7s at a Glance" – one row per "The X 7" group with aggregate metrics (equal-weighted)
    seven_group_names = [k for k in STOCK_GROUPS if k.startswith("The ") and k.endswith(" 7")]
    summary_rows = []
    for gname in seven_group_names:
        rows = groups_data.get(gname, [])
        if not rows:
            continue
        def _avg(key):
            vals = [r.get(key) for r in rows if r.get(key) is not None]
            return sum(vals) / len(vals) if vals else None
        def _avg_round(key, ndec=2):
            v = _avg(key)
            return round(v, ndec) if v is not None else None
        abc_vals = [r.get("abc") for r in rows if r.get("abc")]
        abc_majority = max(set(abc_vals), key=abc_vals.count) if abc_vals else None
        stage_vals = [r.get("stage") for r in rows if r.get("stage") is not None]
        stage_majority = max(set(stage_vals), key=stage_vals.count) if stage_vals else None

        # Equal-weighted Vol Chart: average vol_history (list of 20) across the 7 tickers
        vol_histories = [r.get("vol_history") for r in rows if r.get("vol_history") and len(r.get("vol_history")) == 20]
        vol_chart_path = None
        if vol_histories:
            avg_vol = np.mean(vol_histories, axis=0).tolist()
            safe_gname = re.sub(r'[^a-zA-Z0-9]', '_', gname)
            vol_chart_path = create_vol_chart_png(avg_vol, "Glance_" + safe_gname, charts_dir)

        # Equal-weighted VARS (RS) chart: average last 20 rollingRRS across the 7 tickers
        rolling_list = [r.get("rolling_rrs") for r in rows if r.get("rolling_rrs") and len(r.get("rolling_rrs")) == 20]
        rs_chart_path = None
        if rolling_list:
            avg_rr = np.mean(rolling_list, axis=0)
            rr_series = pd.Series(avg_rr)
            rs_sma = rr_series.rolling(5, min_periods=1).mean().values
            rs_df = pd.DataFrame({"rollingRRS": avg_rr, "RRS_SMA": rs_sma})
            safe_gname = re.sub(r'[^a-zA-Z0-9]', '_', gname)
            rs_chart_path = create_rs_chart_png(rs_df, "Glance_" + safe_gname, charts_dir)

        above_20_count = sum(1 for r in rows if r.get("above_sma20") is True)
        above_50_count = sum(1 for r in rows if r.get("above_sma50") is True)
        above_200_count = sum(1 for r in rows if r.get("above_sma200") is True)

        summary_rows.append({
            "ticker": gname,
            "daily": _avg_round("daily"),
            "intra": _avg_round("intra"),
            "5d": _avg_round("5d"),
            "20d": _avg_round("20d"),
            "wtd": _avg_round("wtd"),
            "ytd": _avg_round("ytd"),
            "vol_ratio": _avg_round("vol_ratio"),
            "vol_chart": vol_chart_path,
            "atr_pct": _avg_round("atr_pct", 1),
            "dist_sma50_atr": _avg_round("dist_sma50_atr"),
            "rs": round(_avg("rs"), 0) if _avg("rs") is not None else None,
            "rs_chart": rs_chart_path,
            "long": [],
            "short": [],
            "abc": abc_majority,
            "stage": stage_majority,
            "breadth": {
                "above_20": above_20_count,
                "above_50": above_50_count,
                "above_200": above_200_count,
                "total": len(rows),
            },
        })

    spy_3m = None
    spy_1m = None
    try:
        spy_hist = _spy_cache
        if spy_hist is not None and len(spy_hist) >= 64:
            spy_3m = (spy_hist["Close"].iloc[-1] / spy_hist["Close"].iloc[-64] - 1) * 100
        if spy_hist is not None and len(spy_hist) >= 22:
            spy_1m = (spy_hist["Close"].iloc[-1] / spy_hist["Close"].iloc[-22] - 1) * 100
    except Exception as e:
        print("SPY RRG calc error:", e)

    rrg_points = []
    if spy_3m is not None and spy_1m is not None:
        for gname in seven_group_names:
            rows = groups_data.get(gname, [])
            if not rows:
                continue
            vals_3m = [r.get("3m_return") for r in rows if r.get("3m_return") is not None]
            vals_1m = [r.get("1m_return") for r in rows if r.get("1m_return") is not None]
            avg_3m = sum(vals_3m) / len(vals_3m) if vals_3m else None
            avg_1m = sum(vals_1m) / len(vals_1m) if vals_1m else None
            if avg_3m is not None and avg_1m is not None:
                rs_ratio_raw = avg_3m - spy_3m
                rs_momentum_raw = avg_1m - spy_1m
                rrg_points.append({"name": gname, "rs_ratio_raw": rs_ratio_raw, "rs_momentum_raw": rs_momentum_raw})

    trails = None
    group_series_raw = None
    if rrg_points:
        # Fetch long history for trail: RRG at 30, 20, 10, 0 trading days ago (shorter path = less clutter)
        lookbacks = [30, 20, 10, 0]
        seven_tickers = []
        for gname in seven_group_names:
            for r in groups_data.get(gname, []):
                t = r.get("ticker")
                if t and t not in seven_tickers:
                    seven_tickers.append(t)
        long_hist = {}
        try:
            time.sleep(5)  # cooldown before RRG history batch
            print("Fetching long history for RRG trails...")
            spy_long = _spy_cache
            if spy_long is not None and len(spy_long) >= 320:
                long_hist["SPY"] = spy_long
            for i, t in enumerate(seven_tickers):
                try:
                    df = yf.Ticker(t).history(period="400d")
                    if df is not None and len(df) >= 320:
                        long_hist[t] = df
                except Exception:
                    pass
                if (i + 1) % 20 == 0:
                    print("  RRG history {}/{}".format(i + 1, len(seven_tickers)))
                time.sleep(0.35)
                if (i + 1) % 40 == 0:
                    time.sleep(3)
        except Exception as e:
            print("RRG long history:", e)

        spy_long = long_hist.get("SPY")
        if spy_long is not None and len(spy_long) >= 320 and len(long_hist) > 1:
            spy_close = spy_long["Close"]
            group_series_raw = {}
            for gname in seven_group_names:
                rows = groups_data.get(gname, [])
                tickers_in_g = [r["ticker"] for r in rows if r.get("ticker") in long_hist]
                if not tickers_in_g:
                    continue
                series = []
                for lb in lookbacks:
                    end_idx = -1 - lb
                    need_len = -end_idx + 63
                    if len(spy_close) < need_len:
                        continue
                    spy_3m_lb = (spy_close.iloc[end_idx] / spy_close.iloc[end_idx - 63] - 1) * 100
                    spy_1m_lb = (spy_close.iloc[end_idx] / spy_close.iloc[end_idx - 21] - 1) * 100
                    group_3m = []
                    group_1m = []
                    for t in tickers_in_g:
                        c = long_hist[t]["Close"]
                        if len(c) < need_len:
                            continue
                        group_3m.append((c.iloc[end_idx] / c.iloc[end_idx - 63] - 1) * 100)
                        group_1m.append((c.iloc[end_idx] / c.iloc[end_idx - 21] - 1) * 100)
                    if group_3m and group_1m:
                        avg_3m_lb = sum(group_3m) / len(group_3m)
                        avg_1m_lb = sum(group_1m) / len(group_1m)
                        rr = avg_3m_lb - spy_3m_lb
                        rm = avg_1m_lb - spy_1m_lb
                        series.append((rr, rm))
                if len(series) == len(lookbacks):
                    group_series_raw[gname] = series

    if rrg_points:
        r_vals = [p["rs_ratio_raw"] for p in rrg_points]
        m_vals = [p["rs_momentum_raw"] for p in rrg_points]
        r_min, r_max = min(r_vals), max(r_vals)
        m_min, m_max = min(m_vals), max(m_vals)
        r_span = (r_max - r_min) if r_max != r_min else 1
        m_span = (m_max - m_min) if m_max != m_min else 1
        for p in rrg_points:
            p["rs_ratio_norm"] = 50 + 50 * (p["rs_ratio_raw"] - (r_min + r_max) / 2) / (r_span / 2) if r_span else 50
            p["rs_momentum_norm"] = 50 + 50 * (p["rs_momentum_raw"] - (m_min + m_max) / 2) / (m_span / 2) if m_span else 50
            p["rs_ratio_norm"] = max(0, min(100, p["rs_ratio_norm"]))
            p["rs_momentum_norm"] = max(0, min(100, p["rs_momentum_norm"]))
        if group_series_raw and r_span and m_span:
            trails = []
            name_to_xy = {p["name"]: (p["rs_ratio_norm"], p["rs_momentum_norm"]) for p in rrg_points}
            for gname, series in group_series_raw.items():
                if len(series) < 2:
                    continue
                smoothed = [series[0]]
                for i in range(1, len(series)):
                    smoothed.append((
                        (series[i - 1][0] + series[i][0]) / 2,
                        (series[i - 1][1] + series[i][1]) / 2,
                    ))
                path = []
                for rr, rm in smoothed[:-1]:
                    x = 50 + 50 * (rr - (r_min + r_max) / 2) / (r_span / 2)
                    y = 50 + 50 * (rm - (m_min + m_max) / 2) / (m_span / 2)
                    x = max(0, min(100, x))
                    y = max(0, min(100, y))
                    path.append((x, y))
                if gname in name_to_xy:
                    path.append(name_to_xy[gname])
                trails.append((gname, path))
        rrg_chart_path = create_rrg_chart_png(rrg_points, charts_dir, trails=trails)
    else:
        rrg_chart_path = None

    rrg_row = {
        "ticker": "7s Rotation Graph",
        "daily": None,
        "intra": None,
        "5d": None,
        "20d": None,
        "wtd": None,
        "ytd": None,
        "vol_ratio": None,
        "vol_chart": None,
        "atr_pct": None,
        "dist_sma50_atr": None,
        "rs": None,
        "rs_chart": None,
        "long": [],
        "short": [],
        "abc": None,
        "rrg_chart": rrg_chart_path,
        "is_rrg_row": True,
    }
    summary_rows.insert(0, rrg_row)
    groups_data["The 7s at a Glance"] = summary_rows
    # Explicit section order
    seven_groups = [k for k in STOCK_GROUPS.keys() if k.startswith("The ") and k.endswith(" 7")]
    desired_order = ["Indices", "The 7s at a Glance", "Sel Sectors", "S&P Style ETFs", "EW Sectors", "Industries", "Countries"] + seven_groups
    remaining = [k for k in groups_data.keys() if k not in desired_order]
    new_order = [k for k in desired_order if k in groups_data] + remaining
    groups_data = {k: groups_data[k] for k in new_order}

    # Sort Industries rows by sector order and tag each row with its sector
    if "Industries" in groups_data:
        _sector_order = list(SECTOR_COLORS.keys())
        for row in groups_data["Industries"]:
            row["sector"] = TICKER_TO_SECTOR.get(row.get("ticker", ""), "Broad Market")
        groups_data["Industries"].sort(
            key=lambda r: _sector_order.index(r["sector"]) if r["sector"] in _sector_order else len(_sector_order)
        )

    # Generate equal-weighted vol + RS chart PNGs per Industries sector (must run before histories are popped)
    industries_sector_charts = {}
    industries_sector_rs_charts = {}
    if "Industries" in groups_data:
        from collections import defaultdict
        _sec_vol_hists = defaultdict(list)
        _sec_rrs_hists = defaultdict(list)
        for _row in groups_data["Industries"]:
            _vh = _row.get("vol_history")
            if _vh and len(_vh) == 20:
                _sec_vol_hists[_row["sector"]].append(_vh)
            _rh = _row.get("rolling_rrs")
            if _rh and len(_rh) == 20:
                _sec_rrs_hists[_row["sector"]].append(_rh)
        for _sec_name in set(list(_sec_vol_hists.keys()) + list(_sec_rrs_hists.keys())):
            _safe_sec = re.sub(r'[^a-zA-Z0-9]', '_', _sec_name)
            if _sec_vol_hists[_sec_name]:
                _avg_vol = np.mean(_sec_vol_hists[_sec_name], axis=0).tolist()
                _path = create_vol_chart_png(_avg_vol, "IndSec_" + _safe_sec, charts_dir)
                if _path:
                    industries_sector_charts[_sec_name] = _path
            if _sec_rrs_hists[_sec_name]:
                _avg_rr = np.mean(_sec_rrs_hists[_sec_name], axis=0)
                _rs_sma = pd.Series(_avg_rr).rolling(5, min_periods=1).mean().values
                _rs_df = pd.DataFrame({"rollingRRS": _avg_rr, "RRS_SMA": _rs_sma})
                _rpath = create_rs_chart_png(_rs_df, "IndSecRS_" + _safe_sec, charts_dir)
                if _rpath:
                    industries_sector_rs_charts[_sec_name] = _rpath

    # ── Expected Move (ATM straddle) ─────────────────────────────────────────────
    # All tickers use nearest Friday expiry (GS methodology).
    time.sleep(5)  # cooldown before expected move batch
    em_groups = ['Indices', 'Sel Sectors', 'S&P Style ETFs']
    for gname in groups_data:
        if gname.startswith('The ') and gname.endswith(' 7'):
            em_groups.append(gname)
    print(f"Fetching options expected move for {len(em_groups)} groups...")
    em_tickers = []
    seen = set()
    for g in em_groups:
        for r in groups_data.get(g, []):
            t = r.get('ticker')
            if t and t not in seen:
                em_tickers.append(t)
                seen.add(t)
    em_data = {}

    # OpenD primary pre-pass — covers all US ETFs/stocks in one connection
    try:
        from fetch_opend import fetch_expected_move_opend, is_opend_eligible
        opend_em_tickers = [t for t in em_tickers if is_opend_eligible(t)]
        if opend_em_tickers:
            def _spot(t):
                return all_ticker_data.get(t, {}).get("last_close")
            print(f"  [opend-em] pre-pass for {len(opend_em_tickers)} tickers...")
            opend_em = fetch_expected_move_opend(opend_em_tickers, spot_lookup=_spot, verbose=False)
            for sym, res in opend_em.items():
                if res.get("em_pct") is not None:
                    em_data[sym] = {
                        "em_pct":  res.get("em_pct"),
                        "em_days": res.get("em_days"),
                    }
            print(f"  [opend-em] covered {len(em_data)}/{len(opend_em_tickers)} via OpenD")
    except Exception as e:
        print(f"  [opend-em] pre-pass failed, falling through to yfinance: {e}")

    # yfinance fallback — only for tickers OpenD couldn't fill
    yf_remaining = [t for t in em_tickers if t not in em_data or em_data[t].get("em_pct") is None]
    if yf_remaining:
        print(f"  yfinance fallback for {len(yf_remaining)} ticker(s)...")
    for sym in yf_remaining:
        em_pct, em_days = get_expected_move(sym, weekly=True)
        em_data.setdefault(sym, {}).update({'em_pct': em_pct, 'em_days': em_days})
        if em_pct is not None:
            print(f"  {sym}: ±{em_pct}% ({em_days}d) [fri/yf]")
        time.sleep(0.35)
    for gname, rows in groups_data.items():
        for r in rows:
            ed = em_data.get(r.get('ticker'), {})
            r['em_pct']  = ed.get('em_pct')
            r['em_days'] = ed.get('em_days')

    # Backfill averaged EM into 7s summary rows
    for sr in groups_data.get("The 7s at a Glance", []):
        gname = sr.get("ticker")
        if gname and gname in groups_data:
            constituent_rows = groups_data[gname]
            em_vals = [r.get("em_pct") for r in constituent_rows if r.get("em_pct") is not None]
            sr["em_pct"] = round(sum(em_vals) / len(em_vals), 2) if em_vals else None

    # ── RS Composite Ranking ──────────────────────────────────────────────────
    print("Computing RS composite ranking...")
    spy_data = all_ticker_data.get("SPY", {})
    spy_1m_ret = spy_data.get("1m_return") or 0
    spy_3m_ret = spy_data.get("3m_return") or 0
    spy_6m_ret = spy_data.get("6m_return") or 0
    spy_12m_ret = spy_data.get("12m_return") or 0

    rs_composites = {}
    for _gn, rows in groups_data.items():
        for r in rows:
            ticker = r.get("ticker", "")
            if not ticker:
                continue
            active_1m = (r.get("1m_return") or 0) - spy_1m_ret
            active_3m = (r.get("3m_return") or 0) - spy_3m_ret
            active_6m = (r.get("6m_return") or 0) - spy_6m_ret
            active_12m = (r.get("12m_return") or 0) - spy_12m_ret
            composite = 0.2 * active_1m + 0.3 * active_3m + 0.3 * active_6m + 0.2 * active_12m
            rs_composites[ticker] = composite

    # Convert to percentile ranks
    if rs_composites:
        sorted_tickers = sorted(rs_composites.keys(), key=lambda t: rs_composites[t])
        n = len(sorted_tickers)
        for rank_i, ticker in enumerate(sorted_tickers):
            pct = round(rank_i / max(n - 1, 1) * 100, 0)
            rs_composites[ticker] = pct

        # Assign back to rows
        for _gn, rows in groups_data.items():
            for r in rows:
                ticker = r.get("ticker", "")
                if ticker in rs_composites:
                    r["rs_composite"] = rs_composites[ticker]
    print(f"  RS composite: ranked {len(rs_composites)} tickers")

    # Remove temporary series from rows so they are not written to snapshot.json
    for _gn, rows in groups_data.items():
        for r in rows:
            r.pop("vol_history", None)
            r.pop("rolling_rrs", None)
            r.pop("1m_return", None)
            r.pop("3m_return", None)
            r.pop("6m_return", None)
            r.pop("12m_return", None)
            r.pop("data_date", None)

    print("Computing column ranges...")
    column_ranges = {}
    for group_name, rows in groups_data.items():
        daily_v = [r["daily"] for r in rows if r.get("daily") is not None]
        intra_v = [r["intra"] for r in rows if r.get("intra") is not None]
        five_v = [r["5d"] for r in rows if r.get("5d") is not None]
        twenty_v = [r["20d"] for r in rows if r.get("20d") is not None]
        column_ranges[group_name] = {
            "daily": (min(daily_v) if daily_v else -10, max(daily_v) if daily_v else 10),
            "intra": (min(intra_v) if intra_v else -10, max(intra_v) if intra_v else 10),
            "5d": (min(five_v) if five_v else -20, max(five_v) if five_v else 20),
            "20d": (min(twenty_v) if twenty_v else -30, max(twenty_v) if twenty_v else 30),
        }

    fear_greed = compute_fear_greed(all_ticker_data)

    fred_api_key = os.environ.get("FRED_API_KEY", "")
    perplexity_api_key = os.environ.get("PERPLEXITY_API_KEY", "")

    macro_data = {}
    if fred_api_key:
        print("Fetching USD Liquidity data...")
        macro_data["usd_liquidity"] = build_usd_liquidity(fred_api_key)
        print("Fetching Inflation Pillar data...")
        try:
            macro_data["inflation_pillar"] = build_inflation_pillar(fred_api_key, out_dir)
        except Exception as e:
            print(f"  inflation_pillar failed: {e}")

    print("Building ETF flow metrics...")
    etf_flow = None
    try:
        etf_flow = build_etf_flow(out_dir, ohlc_dir)
        if etf_flow:
            n = len(etf_flow.get("tickers") or {})
            print(f"  etf_flow: {n} tickers, daily_snapshot_count={etf_flow.get('daily_snapshot_count')}")
    except Exception as e:
        print(f"  etf_flow failed: {e}")

    print("Fetching Fed Watch data...")
    macro_data["fed_watch"] = build_fed_watch(
        fred_api_key if fred_api_key else None,
        perplexity_api_key if perplexity_api_key else None,
    )

    print("Fetching VIX term structure...")
    try:
        vt = build_vix_term_structure()
        if vt:
            macro_data["vix_term"] = vt
            print(f"  vix_term: spread={vt['spread_1m_3m']} regime={vt['regime']}")
    except Exception as e:
        print(f"  vix_term failed: {e}")

    macro_fred = {}
    if fred_api_key:
        print("Fetching FRED macro data...")
        finnhub_api_key = os.environ.get("FINNHUB_API_KEY", "")
        macro_fred = build_macro_fred(fred_api_key, charts_dir,
                                      finnhub_api_key or None,
                                      perplexity_api_key or None)
        print(f"  FRED: {len(macro_fred.get('series', {}))} series, dominant={macro_fred.get('dominant_signal')}")
    else:
        print("No FRED_API_KEY set — skipping macro data")

    # Cross-asset snapshot for briefing agent
    _ca_tickers = ["TLT", "HYG", "LQD", "IEF", "SHY", "UUP", "GLD", "SLV", "USO", "UNG", "VIXY"]
    cross_asset = {}
    for _t in _ca_tickers:
        _r = all_ticker_data.get(_t)
        if _r:
            cross_asset[_t] = {
                "daily": _r.get("daily"),
                "5d": _r.get("5d"),
                "20d": _r.get("20d"),
                "vol_ratio": _r.get("vol_ratio"),
            }

    # ── Correlation Matrix ────────────────────────────────────────────────────
    CORR_TICKERS = ["SPY", "QQQ", "IWM", "TLT", "GLD", "USO", "UUP", "HYG", "VIXY"]
    print("Computing correlation matrix...")
    corr_data_map = {}
    for _ct in CORR_TICKERS:
        safe_ct = re.sub(r'[^a-zA-Z0-9]', '_', _ct)
        ohlc_path = os.path.join(ohlc_dir, safe_ct + ".json")
        if os.path.exists(ohlc_path):
            try:
                with open(ohlc_path) as f:
                    ohlc = json.load(f)
                closes = [bar["c"] for bar in ohlc.get("ohlc", [])]
                if len(closes) >= 60:
                    returns = [(closes[i] / closes[i - 1] - 1) for i in range(1, len(closes))]
                    corr_data_map[_ct] = returns[-60:]
            except Exception:
                pass

    correlation = None
    corr_tickers_sorted = sorted(corr_data_map.keys())
    if len(corr_tickers_sorted) >= 4:
        n_corr = len(corr_tickers_sorted)
        corr_matrix = []
        for i in range(n_corr):
            row = []
            for j in range(n_corr):
                ri, rj = corr_data_map[corr_tickers_sorted[i]], corr_data_map[corr_tickers_sorted[j]]
                min_len = min(len(ri), len(rj))
                if min_len >= 20:
                    c = float(np.corrcoef(ri[-min_len:], rj[-min_len:])[0, 1])
                    row.append(round(c, 2))
                else:
                    row.append(None)
            corr_matrix.append(row)
        correlation = {"tickers": corr_tickers_sorted, "matrix": corr_matrix, "window": 60}
        print(f"  Correlation: {n_corr}x{n_corr} matrix")
    else:
        print("  Correlation: insufficient OHLC data")

    snapshot = {
        "built_at": datetime.utcnow().isoformat() + "Z",
        "groups": groups_data,
        "column_ranges": column_ranges,
        "themes": themes_data,
        "fear_greed": fear_greed,
        "macro": macro_data,
        "vol_signals": vol_signals,
        "macro_fred": macro_fred,
        "cross_asset": cross_asset,
        "correlation": correlation,
        "factor_regime": factor_regime if factor_regime else None,
        "options_intel": options_intel if options_intel else None,
        "industries_sector_charts": industries_sector_charts,
        "industries_sector_rs_charts": industries_sector_rs_charts,
        "etf_flow": etf_flow,
    }
    meta = {
        "SECTOR_COLORS": SECTOR_COLORS,
        "TICKER_TO_SECTOR": TICKER_TO_SECTOR,
        "Industries_COLORS": Industries_COLORS,
        "SECTOR_ORDER": list(SECTOR_COLORS.keys()),
        "default_symbol": STOCK_GROUPS["Indices"][0] if STOCK_GROUPS["Indices"] else "SPY",
    }

    # ── Rotation history snapshot ────────────────────────────────────────────────
    ROTATION_GROUPS = ["Indices", "The 7s at a Glance", "Industries", "Sel Sectors", "Countries", "S&P Style ETFs"]
    for gname in groups_data:
        if gname.startswith('The ') and gname.endswith(' 7') and gname not in ROTATION_GROUPS:
            ROTATION_GROUPS.append(gname)
    rot_path = os.path.join(out_dir, "rotation_history.json")
    rot_history = []
    if os.path.exists(rot_path):
        try:
            with open(rot_path) as f:
                rot_history = json.load(f).get("snapshots", [])
        except Exception:
            pass
    today_str = datetime.utcnow().date().isoformat()
    rot_entry = {"date": today_str, "groups": {}}
    for gname in ROTATION_GROUPS:
        rows = groups_data.get(gname, [])
        if not rows:
            continue
        rot_entry["groups"][gname] = {
            r["ticker"]: {"20d": r.get("20d"), "5d": r.get("5d"), "daily": r.get("daily"), "rs": r.get("rs"), "em_pct": r.get("em_pct")}
            for r in rows if r.get("ticker")
        }
    rot_history = [h for h in rot_history if h.get("date") != today_str]
    rot_history.append(rot_entry)
    rot_history = rot_history[-60:]
    with open(rot_path, "w") as f:
        json.dump({"snapshots": rot_history}, f, separators=(",", ":"))
    print(f"Rotation snapshot saved ({today_str}, {len(rot_history)} entries total)")

    # ── USD Liquidity score history snapshot ────────────────────────────────────
    liq = macro_data.get("usd_liquidity") or {}
    if liq.get("score") is not None:
        liq_path = os.path.join(out_dir, "liquidity_history.json")
        liq_history = []
        if os.path.exists(liq_path):
            try:
                with open(liq_path) as f:
                    liq_history = json.load(f).get("snapshots", [])
            except Exception:
                liq_history = []
        liq_entry = {
            "date": today_str,
            "score": liq.get("score"),
            "score_label": liq.get("score_label"),
            "components": {c["id"]: c.get("percentile") for c in liq.get("components") or [] if c.get("id")},
        }
        liq_history = [h for h in liq_history if h.get("date") != today_str]
        liq_history.append(liq_entry)
        liq_history = liq_history[-90:]
        with open(liq_path, "w") as f:
            json.dump({"snapshots": liq_history}, f, separators=(",", ":"))
        print(f"Liquidity snapshot saved ({today_str}, {len(liq_history)} entries total)")

    snapshot_path = os.path.join(out_dir, "snapshot.json")
    events_path = os.path.join(out_dir, "events.json")
    meta_path = os.path.join(out_dir, "meta.json")

    with open(snapshot_path, "w", encoding="utf-8") as f:
        json.dump(sanitize_for_json(snapshot), f, ensure_ascii=False, indent=2)
    with open(events_path, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2)
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print("Wrote", snapshot_path, events_path, meta_path, "and charts in", charts_dir)
    if failed_tickers:
        print(f"FAILED ({len(failed_tickers)}): {', '.join(failed_tickers)}")
    fetch_etf_holdings(get_all_etfs_for_holdings(), out_dir)
    refresh_holdings_daily_from_cache(out_dir, all_ticker_data)
    print("Done.")


if __name__ == "__main__":
    import sys
    if "--holdings" in sys.argv:
        p = argparse.ArgumentParser()
        p.add_argument("--out-dir", default="data")
        p.add_argument("--holdings", action="store_true")
        args = p.parse_args()
        fetch_etf_holdings(get_all_etfs_for_holdings(), args.out_dir)
    else:
        main()
