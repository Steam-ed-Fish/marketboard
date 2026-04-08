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
    "Indices": ["QQQ", "DIA", "SPY", "RSP", "IWM", "IJH", "IJR", "GLD", "SLV"],
    "S&P Style ETFs": ["IJS", "IJR", "IJT", "IJJ", "IJH", "IJK", "IVE", "IVV", "IVW"],
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
    "The Insurance 7": ["PGR", "CB", "MMC", "AON", "AIG", "MET", "AFL"],
    "The Power 7": ["NEE", "SO", "DUK", "CEG", "VST", "AEP", "SRE"],
    "Industries": [
        "TAN", "KCE", "IBUY", "QQQE", "JETS", "IBB", "SMH", "CIBR", "UTES", "ROBO", "IGV", "WCLD", "ITA", "PAVE", "BLOK", "AIQ", "IYZ", "PEJ", "FDN", "KBE",
        "UNG", "BOAT", "KWEB", "KRE", "IBIT", "XRT", "IHI", "DRIV", "MSOS", "SOCL", "XLU", "ARKF", "SLX", "ARKK", "XTN", "XME", "KIE", "GLD", "GXC", "SCHH",
        "GDX", "IPAY", "IWM", "XOP", "VNQ", "EATZ", "FXI", "DBA", "ICLN", "SILJ", "REZ", "LIT", "SLV", "XHB", "XHE", "PBJ", "USO", "DBC", "FCG", "XBI",
        "ARKG", "CPER", "XES", "OIH", "PPH", "FNGS", "URA", "WGMI", "REMX"
    ],
    "Sel Sectors": ["XLK", "XLI", "XLC", "XLF", "XLU", "XLY", "XLRE", "XLP", "XLB", "XLE", "XLV"],
    "EW Sectors": ["RSPT", "RSPC", "RSPN", "RSPF", "RSP", "RSPD", "RSPU", "RSPR", "RSPH", "RSPM", "RSPS", "RSPG"],
    "Countries": [
        "EZA", "ARGT", "EWA", "THD", "EIDO", "EWC", "GREK", "EWP", "EWG", "EWL", "EUFN", "EWY", "IEUR", "EFA", "ACWI",
        "IEV", "EWQ", "EWI", "EWJ", "EWW", "ECH", "EWD", "ASHR", "EWS", "KSA", "INDA", "EEM", "EWZ", "TUR", "EWH", "EWT", "MCHI"
    ],
}

AI_THEMES = {
    "Mag 7":        ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA"],
    "Memory":       ["MU", "WDC", "SNDK", "STX", "000660.KS", "005930.KS"],
    "Optical Comms": ["COHR", "LITE", "AAOI", "VIAV", "TSEM", "AXTI", "GLW"],
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
    "IGV": "#3f51b5", "CIBR": "#3f51b5", "EATZ": "#4caf50", "PPH": "#e91e63", "IHI": "#e91e63", "UTES": "#009688",
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


def get_expected_move(ticker_sym):
    """Fetch ATM straddle EM% from nearest next-day options expiry via yfinance.
    Skips today's expiry (0DTE at close = nearly worthless).
    Returns (em_pct, days_to_expiry) or (None, None) on failure.
    """
    try:
        t = yf.Ticker(ticker_sym)
        expirations = t.options
        if not expirations:
            return None, None
        today = datetime.utcnow().date()
        # Skip same-day expiry — take first expiry strictly after today
        nearest = next(
            (e for e in expirations if datetime.strptime(e, '%Y-%m-%d').date() > today),
            expirations[0]  # fallback if all are today (e.g. weekend run)
        )
        days = max((datetime.strptime(nearest, '%Y-%m-%d').date() - today).days, 1)
        chain = t.option_chain(nearest)
        calls, puts = chain.calls, chain.puts
        if calls.empty or puts.empty:
            return None, None
        price = (t.fast_info.get('last_price') or t.fast_info.get('previousClose'))
        if not price:
            return None, None
        strikes = calls['strike'].values
        atm = strikes[np.argmin(np.abs(strikes - price))]
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


def get_stock_data(ticker_symbol, charts_dir, spy_hist=None, ohlc_dir=None):
    try:
        stock = yf.Ticker(ticker_symbol)
        all_hist = stock.history(period="1y")
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
                    for idx, r in all_hist.tail(60).iterrows()
                ]
                with open(os.path.join(ohlc_dir, f"{safe}.json"), 'w') as _f:
                    json.dump({"ticker": ticker_symbol, "ohlc": ohlc_rows}, _f, separators=(',', ':'))
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

        sma50 = calculate_sma(daily)
        atr = calculate_atr(daily)
        current_close = daily['Close'].iloc[-1]
        atr_pct = (atr / current_close) * 100 if atr and current_close else None
        dist_sma50_atr = (100 * (current_close / sma50 - 1) / atr_pct) if (sma50 and atr_pct and atr_pct != 0) else None
        abc_rating = calculate_abc_rating(daily)
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
            stock_history = stock.history(start=start_date, end=end_date)
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
        # Keep last 20 rolling RRS for equal-weighted summary charts (The 7s at a Glance)
        rolling_rrs = None
        if rrs_data is not None and len(rrs_data) >= 20:
            rolling_rrs = rrs_data['rollingRRS'].iloc[-20:].tolist()

        return {
            "ticker": ticker_symbol,
            "daily": round(daily_change, 2) if daily_change is not None else None,
            "intra": round(intraday_change, 2) if intraday_change is not None else None,
            "5d": round(five_day_change, 2) if five_day_change is not None else None,
            "20d": round(twenty_day_change, 2) if twenty_day_change is not None else None,
            "wtd": round(wtd_change, 2) if wtd_change is not None else None,
            "ytd": round(ytd_change, 2) if ytd_change is not None else None,
            "1m_return": one_month_return,
            "3m_return": three_month_return,
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
        }
    except Exception as e:
        print("Error", ticker_symbol, e)
        return None


def get_all_etfs_for_holdings():
    """Unique tickers across all groups (ETFs + stocks; holdings JSON only for funds)."""
    etfs = set()
    for _group, tickers in STOCK_GROUPS.items():
        etfs.update(tickers)
    return sorted(etfs)


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

    return {
        "score": score,
        "score_label": score_label,
        "components": comp_out,
        "raw": raw_out,
        "fred_links": fred_links,
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
            except Exception:
                result[category].append({
                    "name": item["name"], "desc": item["desc"],
                    "current": None, "ma20": None,
                    "vs_ma": None, "hi52": None,
                    "lo52": None, "pct52": None,
                })
    return result


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

    print("Fetching SPY history (cached for full run)...")
    try:
        _spy_cache = yf.Ticker("SPY").history(period="400d")
    except Exception as e:
        print("SPY cache fetch failed:", e)
        _spy_cache = None

    print("Fetching stock data (no Liquid Stocks)...")
    groups_data = {}
    all_ticker_data = {}
    failed_tickers = []
    for group_name, tickers in STOCK_GROUPS.items():
        rows = []
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
                failed_tickers.append(ticker)
            time.sleep(0.15)
        groups_data[group_name] = rows

    # Fetch any AI_THEMES tickers + Fear & Greed tickers + cross-asset tickers not already fetched
    theme_ticker_set = set(t for tickers in AI_THEMES.values() for t in tickers) | {"HYG", "TLT", "VIXY", "USO", "UNG", "UUP", "LQD", "IEF", "SHY"}
    for ticker in sorted(theme_ticker_set - set(all_ticker_data.keys())):
        print(f"  [AI Themes] {ticker}")
        row = get_stock_data(ticker, charts_dir, spy_hist=_spy_cache, ohlc_dir=ohlc_dir)
        if row:
            all_ticker_data[ticker] = row
        else:
            failed_tickers.append(ticker)
        time.sleep(0.15)

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
        themes_data.append({
            "name": theme_name,
            "tickers": tickers,
            "daily": _theme_avg("daily"),
            "intra": _theme_avg("intra"),
            "wtd": _theme_avg("wtd"),
            "5d": _theme_avg("5d"),
            "20d": _theme_avg("20d"),
            "ytd": _theme_avg("ytd"),
            "vol_ratio": _theme_avg("vol_ratio"),
            "atr_pct": _theme_avg("atr_pct", 1),
            "dist_sma50_atr": _theme_avg("dist_sma50_atr"),
            "rs": _theme_avg("rs"),
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
                time.sleep(0.08)
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

    # ── Expected Move (ATM straddle) for Indices group ───────────────────────────
    print("Fetching options expected move for Indices, Sel Sectors, S&P Style ETFs...")
    em_groups = ['Indices', 'Sel Sectors', 'S&P Style ETFs']
    em_tickers = []
    seen = set()
    for g in em_groups:
        for r in groups_data.get(g, []):
            t = r.get('ticker')
            if t and t not in seen:
                em_tickers.append(t)
                seen.add(t)
    em_data = {}
    for sym in em_tickers:
        em_pct, em_days = get_expected_move(sym)
        em_data[sym] = {'em_pct': em_pct, 'em_days': em_days}
        if em_pct is not None:
            print(f"  {sym}: ±{em_pct}% ({em_days}d)")
        else:
            print(f"  {sym}: no options data")
        time.sleep(0.3)
    for gname, rows in groups_data.items():
        for r in rows:
            ed = em_data.get(r.get('ticker'), {})
            r['em_pct']  = ed.get('em_pct')
            r['em_days'] = ed.get('em_days')

    # Remove temporary series from rows so they are not written to snapshot.json
    for _gn, rows in groups_data.items():
        for r in rows:
            r.pop("vol_history", None)
            r.pop("rolling_rrs", None)
            r.pop("1m_return", None)
            r.pop("3m_return", None)

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
    print("Fetching Fed Watch data...")
    macro_data["fed_watch"] = build_fed_watch(
        fred_api_key if fred_api_key else None,
        perplexity_api_key if perplexity_api_key else None,
    )

    print("Fetching volatility signals...")
    vol_signals = fetch_vol_signals()
    for cat, items in vol_signals.items():
        for item in items:
            if item['current'] is not None:
                print(f"  {item['name']}: {item['current']:.2f} (vs MA20: {item['vs_ma']:+.1f}%)")
            else:
                print(f"  {item['name']}: no data")
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
        "industries_sector_charts": industries_sector_charts,
        "industries_sector_rs_charts": industries_sector_rs_charts,
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

    snapshot_path = os.path.join(out_dir, "snapshot.json")
    events_path = os.path.join(out_dir, "events.json")
    meta_path = os.path.join(out_dir, "meta.json")

    with open(snapshot_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)
    with open(events_path, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2)
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print("Wrote", snapshot_path, events_path, meta_path, "and charts in", charts_dir)
    if failed_tickers:
        print(f"FAILED ({len(failed_tickers)}): {', '.join(failed_tickers)}")
    fetch_etf_holdings(get_all_etfs_for_holdings(), out_dir)
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
