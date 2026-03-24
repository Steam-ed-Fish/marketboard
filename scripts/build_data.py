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

import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from io import BytesIO
from scipy.stats import rankdata

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
    "Sel Sectors": ["XLK", "XLI", "XLC", "XLF", "XLU", "XLY", "XLRE", "XLP", "XLB", "XLE", "XLV"],
    "EW Sectors": ["RSPT", "RSPC", "RSPN", "RSPF", "RSP", "RSPD", "RSPU", "RSPR", "RSPH", "RSPM", "RSPS", "RSPG"],
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
    "Countries": [
        "EZA", "ARGT", "EWA", "THD", "EIDO", "EWC", "GREK", "EWP", "EWG", "EWL", "EUFN", "EWY", "IEUR", "EFA", "ACWI",
        "IEV", "EWQ", "EWI", "EWJ", "EWW", "ECH", "EWD", "ASHR", "EWS", "KSA", "INDA", "EEM", "EWZ", "TUR", "EWH", "EWT", "MCHI"
    ],
}

AI_THEMES = {
    "Mag 7":        ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA"],
    "Memory":       ["MU", "WDC", "SNDK", "STX"],
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


def get_stock_data(ticker_symbol, charts_dir):
    try:
        stock = yf.Ticker(ticker_symbol)
        hist = stock.history(period="21d")
        daily = stock.history(period="60d")
        yearly = stock.history(period="1y")
        
        if len(hist) < 2 or len(daily) < 50:
            return None

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
            spy_history = yf.Ticker("SPY").history(start=start_date, end=end_date)
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

        # Gap detection: all gaps in last 5 trading days [day1=most recent, day5=oldest]
        gaps = []
        GAP_THRESHOLD = 0.5
        try:
            for i in range(1, 6):
                if len(hist) < i + 1:
                    gaps.append(None)
                    continue
                open_price = float(hist['Open'].iloc[-i])
                prev_close = float(hist['Close'].iloc[-(i + 1)])
                gap_pct = (open_price - prev_close) / prev_close * 100
                if gap_pct >= GAP_THRESHOLD:
                    gaps.append('up')
                elif gap_pct <= -GAP_THRESHOLD:
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
            "gaps": gaps
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="data", help="Output directory (default: data)")
    args = parser.parse_args()
    out_dir = args.out_dir
    charts_dir = os.path.join(out_dir, "charts")
    os.makedirs(charts_dir, exist_ok=True)

    print("Fetching economic events...")
    events = get_upcoming_key_events()

    print("Fetching stock data (no Liquid Stocks)...")
    groups_data = {}
    all_ticker_data = {}
    for group_name, tickers in STOCK_GROUPS.items():
        rows = []
        for i, ticker in enumerate(tickers):
            print(f"  [{group_name}] {i+1}/{len(tickers)} {ticker}")
            row = get_stock_data(ticker, charts_dir)
            if row:
                rows.append(row)
                all_ticker_data[ticker] = row
            time.sleep(0.15)
        groups_data[group_name] = rows

    # Fetch any AI_THEMES tickers not already fetched via STOCK_GROUPS
    theme_ticker_set = set(t for tickers in AI_THEMES.values() for t in tickers)
    for ticker in sorted(theme_ticker_set - set(all_ticker_data.keys())):
        print(f"  [AI Themes] {ticker}")
        row = get_stock_data(ticker, charts_dir)
        if row:
            all_ticker_data[ticker] = row
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
        spy = yf.Ticker("SPY")
        spy_hist = spy.history(period="1y")
        if spy_hist is not None and len(spy_hist) >= 64:
            spy_3m = (spy_hist["Close"].iloc[-1] / spy_hist["Close"].iloc[-64] - 1) * 100
        if spy_hist is not None and len(spy_hist) >= 22:
            spy_1m = (spy_hist["Close"].iloc[-1] / spy_hist["Close"].iloc[-22] - 1) * 100
    except Exception as e:
        print("SPY fetch for RRG:", e)

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
            spy_long = yf.Ticker("SPY").history(period="400d")
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
    # Place summary section right after EW Sectors (before The Cloud 7, etc.)
    order = list(groups_data.keys())
    if "EW Sectors" in order:
        idx = order.index("EW Sectors") + 1
        new_order = order[:idx] + ["The 7s at a Glance"] + [k for k in order[idx:] if k != "The 7s at a Glance"]
    else:
        new_order = order
    groups_data = {k: groups_data[k] for k in new_order}

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

    snapshot = {
        "built_at": datetime.utcnow().isoformat() + "Z",
        "groups": groups_data,
        "column_ranges": column_ranges,
        "themes": themes_data,
    }
    meta = {
        "SECTOR_COLORS": SECTOR_COLORS,
        "TICKER_TO_SECTOR": TICKER_TO_SECTOR,
        "Industries_COLORS": Industries_COLORS,
        "SECTOR_ORDER": list(SECTOR_COLORS.keys()),
        "default_symbol": STOCK_GROUPS["Indices"][0] if STOCK_GROUPS["Indices"] else "SPY",
    }

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
