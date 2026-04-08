#!/usr/bin/env python3
"""
FedWatch Daily Data Updater
Updates data/fedwatch.json with:
  - BLS unemployment + CPI (free, no key)
  - Fed speeches via official RSS feed
  - CME rate probabilities via yfinance ZQ futures (pyfedwatch methodology)
"""

import json
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, date
import os
import sys

DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'fedwatch.json')


def load_existing():
    with open(DATA_PATH, encoding='utf-8') as f:
        return json.load(f)


def fetch_url(url, timeout=15):
    req = urllib.request.Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0 FedWatch/1.0')
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode('utf-8', errors='replace')


# ─── BLS Unemployment ────────────────────────────────────────
def fetch_unemployment():
    try:
        raw = fetch_url(
            "https://api.bls.gov/publicAPI/v1/timeseries/data/LNS14000000?latest=true"
        )
        series = json.loads(raw)['Results']['series'][0]['data']
        latest = series[0]
        rate   = float(latest['value'])
        period = latest['periodName'] + ' ' + latest['year']
        print(f"  → Unemployment: {rate}% ({period})")
        return rate, period
    except Exception as e:
        print(f"  [WARN] BLS unemployment: {e}")
        return None, None


# ─── BLS CPI YoY ─────────────────────────────────────────────
def fetch_cpi():
    try:
        # Request 13 months so we can do a proper YoY
        bls_url = (
            "https://api.bls.gov/publicAPI/v2/timeseries/data/"
            "CUUR0000SA0?latest=false&startYear={}&endYear={}"
        ).format(date.today().year - 1, date.today().year)
        raw  = fetch_url(bls_url)
        data = json.loads(raw)['Results']['series'][0]['data']
        # Sort descending by year/period
        data.sort(key=lambda x: (x['year'], x['period']), reverse=True)
        if len(data) >= 13:
            latest   = float(data[0]['value'])
            year_ago = float(data[12]['value'])
            yoy = round((latest - year_ago) / year_ago * 100, 1)
        else:
            yoy = None
        period = data[0]['periodName'] + ' ' + data[0]['year']
        print(f"  → CPI YoY: {yoy}% ({period})")
        return yoy, period
    except Exception as e:
        print(f"  [WARN] BLS CPI: {e}")
        return None, None


# ─── Fed Speeches RSS ─────────────────────────────────────────
def fetch_recent_speeches(top_n=30):
    try:
        raw  = fetch_url('https://www.federalreserve.gov/feeds/speeches.xml', timeout=20)
        root = ET.fromstring(raw)
        items = root.find('channel').findall('item')[:top_n]
        out = []
        for item in items:
            title = (item.findtext('title') or '').strip()
            pub   = (item.findtext('pubDate') or '')
            link  = (item.findtext('link') or '').strip()
            desc  = (item.findtext('description') or '').strip()
            try:
                dt = datetime.strptime(pub[:16].strip(), '%a, %d %b %Y')
                date_str = dt.strftime('%Y-%m-%d')
            except Exception:
                date_str = pub[:10]
            out.append({'raw_title': title, 'date': date_str, 'link': link, 'description': desc})
        print(f"  → {len(out)} speeches from RSS")
        return out
    except Exception as e:
        print(f"  [WARN] Fed RSS: {e}")
        return []


def match_speeches(members, new_speeches):
    name_map = {m['name'].split()[-1].lower(): m for m in members}
    added = 0
    updated = set()  # member names that received a new speech
    for speech in new_speeches:
        title = speech['raw_title'].lower()
        for last, member in name_map.items():
            if last in title:
                existing = {s['date'] for s in member.get('speeches', [])}
                if speech['date'] not in existing:
                    member.setdefault('speeches', []).insert(0, {
                        'date':             speech['date'],
                        'event':            speech['raw_title'],
                        'url':              speech['link'],
                        'brief_summary':    (speech['description'] or 'See full speech.')[:250],
                        'detailed_summary': '',
                    })
                    member['speeches'] = member['speeches'][:5]
                    added += 1
                    updated.add(member['name'])
                break
    print(f"  → {added} new speeches matched")
    return members, updated


# ─── Stance reassessment via Claude ───────────────────────────
def reassess_stance(member, api_key):
    """Ask Claude to classify stance as Hawk/Neutral/Dove based on recent speeches."""
    try:
        import anthropic
        speeches = member.get('speeches', [])[:3]
        if not speeches:
            return member.get('stance', 'Neutral')
        speech_text = '\n\n'.join(
            f"Date: {s['date']}\nEvent: {s['event']}\nSummary: {s['brief_summary']}"
            for s in speeches
        )
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model='claude-haiku-4-5-20251001',
            max_tokens=5,
            messages=[{
                'role': 'user',
                'content': (
                    f"Based on these recent speeches by Federal Reserve official {member['name']}, "
                    f"classify their current monetary policy stance as exactly one word.\n\n"
                    f"{speech_text}\n\n"
                    f"Respond with only one word: Hawk, Neutral, or Dove."
                )
            }]
        )
        result = msg.content[0].text.strip().capitalize()
        if result in ('Hawk', 'Neutral', 'Dove'):
            print(f"  → Stance reassessed: {member['name']} → {result} (was {member.get('stance')})")
            return result
        return member.get('stance', 'Neutral')
    except Exception as e:
        print(f"  [WARN] Stance reassessment for {member['name']}: {e}")
        return member.get('stance', 'Neutral')


# ─── CME Rate Probabilities via yfinance ZQ futures ──────────
def fetch_cme_probabilities(next_fomc_date_str):
    """
    Compute hold/cut/hike probabilities from 30-Day Fed Funds futures (ZQ).
    Uses CME FedWatch simplified methodology:
      implied_rate = 100 - futures_price
      prob_cut  = max(0, current_rate - implied_rate) / 0.25
      prob_hike = max(0, implied_rate - current_rate) / 0.25
      prob_hold = 1 - prob_cut - prob_hike
    Returns dict or None on failure.
    """
    try:
        import yfinance as yf
        from datetime import datetime

        fomc = datetime.strptime(next_fomc_date_str, '%Y-%m-%d')
        # Month codes: F=Jan G=Feb H=Mar J=Apr K=May M=Jun N=Jul Q=Aug U=Sep V=Oct X=Nov Z=Dec
        month_codes = {1:'F',2:'G',3:'H',4:'J',5:'K',6:'M',7:'N',8:'Q',9:'U',10:'V',11:'X',12:'Z'}
        code = month_codes[fomc.month]
        yr   = str(fomc.year)[-2:]
        ticker = f"ZQ{code}{yr}.CBT"

        t    = yf.Ticker(ticker)
        hist = t.history(period='1d')
        if hist.empty:
            # Try next month contract as fallback
            next_month = fomc.month % 12 + 1
            next_yr    = yr if next_month > 1 else str(fomc.year + 1)[-2:]
            ticker2    = f"ZQ{month_codes[next_month]}{next_yr}.CBT"
            hist = yf.Ticker(ticker2).history(period='1d')
            if hist.empty:
                return None

        price        = float(hist['Close'].iloc[-1])
        implied_rate = 100.0 - price          # e.g. 100 - 96.25 = 3.75%

        # Current effective Fed Funds rate (mid of target range)
        # Pull from FRED daily series via the cached snapshot if available
        # Fallback: use 3.625 (midpoint of 3.50-3.75%)
        try:
            import requests
            r = requests.get(
                'https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFF',
                timeout=10
            )
            lines = r.text.strip().split('\n')
            current_rate = float(lines[-1].split(',')[1])
        except Exception:
            current_rate = 3.625

        step = 0.25
        diff = implied_rate - current_rate

        if diff <= -step / 2:
            prob_cut  = min(1.0, abs(diff) / step)
            prob_hike = 0.0
        elif diff >= step / 2:
            prob_hike = min(1.0, diff / step)
            prob_cut  = 0.0
        else:
            prob_cut  = 0.0
            prob_hike = 0.0

        prob_hold = max(0.0, 1.0 - prob_cut - prob_hike)

        result = {
            'rate_cut_pct':  round(prob_cut  * 100, 1),
            'rate_hold_pct': round(prob_hold * 100, 1),
            'rate_hike_pct': round(prob_hike * 100, 1),
        }
        print(f"  → CME via ZQ: hold={result['rate_hold_pct']}% cut={result['rate_cut_pct']}% hike={result['rate_hike_pct']}%")
        return result
    except Exception as e:
        print(f"  [WARN] CME futures: {e}")
        return None


# ─── FOMC Schedule ────────────────────────────────────────────
FOMC_2026 = [
    ('2026-01-28', 'Jan 28, 2026'),
    ('2026-03-18', 'Mar 18, 2026'),
    ('2026-04-29', 'Apr 29, 2026'),
    ('2026-06-17', 'Jun 17, 2026'),
    ('2026-07-29', 'Jul 29, 2026'),
    ('2026-09-16', 'Sep 16, 2026'),
    ('2026-10-28', 'Oct 28, 2026'),
    ('2026-12-09', 'Dec 9, 2026'),
]

def next_fomc():
    today = date.today().isoformat()
    for d, label in FOMC_2026:
        if d >= today:
            return d, label
    return FOMC_2026[-1]


# ─── Main ─────────────────────────────────────────────────────
def main():
    print(f"[FedWatch] Updating at {datetime.now().isoformat()}")
    data = load_existing()

    # 1. Unemployment
    print("  Fetching unemployment (BLS)...")
    unemp, unemp_period = fetch_unemployment()
    if unemp is not None:
        data['market']['unemployment']       = unemp
        data['market']['unemployment_month'] = unemp_period

    # 2. CPI
    print("  Fetching CPI (BLS)...")
    cpi, cpi_period = fetch_cpi()
    if cpi is not None:
        data['market']['cpi']       = cpi
        data['market']['cpi_month'] = cpi_period

    # 3. Next FOMC date
    fomc_date, fomc_label = next_fomc()
    data['market']['next_fomc_date']  = fomc_date
    data['market']['next_fomc_label'] = fomc_label
    print(f"  → Next FOMC: {fomc_label}")

    # 4. CME probabilities
    print("  Computing CME probabilities from ZQ futures...")
    probs = fetch_cme_probabilities(fomc_date)
    if probs:
        data['market'].update(probs)

    # 5. Fed speeches RSS
    print("  Fetching Fed speeches RSS...")
    recent = fetch_recent_speeches(30)
    data['members'], updated_members = match_speeches(data['members'], recent)

    # 6. Reassess stance for members who received a new speech
    anthropic_key = os.environ.get('ANTHROPIC_API_KEY')
    if updated_members and anthropic_key:
        print(f"  Reassessing stance for: {', '.join(updated_members)}")
        for m in data['members']:
            if m['name'] in updated_members:
                m['stance'] = reassess_stance(m, anthropic_key)
    elif updated_members:
        print("  [INFO] ANTHROPIC_API_KEY not set — skipping stance reassessment")

    # 7. Timestamp
    data['last_updated'] = date.today().isoformat()

    with open(DATA_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"[FedWatch] Written to {DATA_PATH}")


if __name__ == '__main__':
    main()
