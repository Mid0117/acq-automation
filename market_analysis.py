"""
market_analysis.py — pulls free real-estate market data, computes a
"where to point marketing" score per state (and skeleton for city/ZIP),
writes site/markets.json.

DECISION DRIVER: where to point cold-call / SMS marketing.
Higher score = more attractive to send marketing to.

Free data sources (no API keys needed):
  - Realtor.com Research:  inventory + hotness by state, county, metro, ZIP
    https://econdata.s3-us-west-2.amazonaws.com/Reports/...
  - Zillow Research:       ZHVI home values by state and ZIP (CSVs)
    https://files.zillowstatic.com/research/public_csvs/...
  - Hardcoded:             landlord/tenant friendliness tier

Scoring (state level):
  +30  landlord-friendly tier 1
  +20  landlord-friendly tier 2
   0   neutral
  -20  tenant-friendly (avoid)

  +0..15  days-on-market (longer = more distressed sellers, easier wholesaling)
  +0..15  price-reduced share (more reductions = motivated sellers)
  +0..10  active listing density (more inventory = more deal flow)
  -0..10  median home value too high (>$500k = wholesaling math harder)

Run weekly via .github/workflows/markets.yml.
"""
import csv, io, json, sys, time
from collections import defaultdict
from pathlib import Path
import requests

ROOT = Path(__file__).parent
SITE = ROOT / 'site'

# --- Hardcoded legal-environment tier --------------------------------
# Sources: NOLO landlord-tenant guide, Avail blog, RentRedi rankings,
# Steadily insurance landlord-friendly state index 2024-2025.
LANDLORD_TIER_1 = {  # most landlord-friendly, fastest evictions, no rent control
    'TX', 'FL', 'AL', 'GA', 'IN', 'OH', 'NC', 'SC', 'TN', 'AZ', 'MO', 'AR',
}
LANDLORD_TIER_2 = {
    'KS', 'OK', 'KY', 'WV', 'IA', 'NE', 'NV', 'MS', 'LA', 'AK', 'ID', 'WY',
    'UT', 'PA', 'NM', 'MI', 'WI', 'SD', 'ND', 'MT',
}
TENANT_FRIENDLY = {  # avoid: rent control, slow eviction, strong tenant rights
    'CA', 'NY', 'NJ', 'MA', 'OR', 'WA', 'MD', 'IL', 'CT', 'VT', 'NH', 'MN',
    'HI', 'RI', 'DC', 'CO', 'ME', 'DE',
}

STATE_NAMES = {
    'AL':'Alabama','AK':'Alaska','AZ':'Arizona','AR':'Arkansas','CA':'California',
    'CO':'Colorado','CT':'Connecticut','DE':'Delaware','DC':'District of Columbia',
    'FL':'Florida','GA':'Georgia','HI':'Hawaii','ID':'Idaho','IL':'Illinois',
    'IN':'Indiana','IA':'Iowa','KS':'Kansas','KY':'Kentucky','LA':'Louisiana',
    'ME':'Maine','MD':'Maryland','MA':'Massachusetts','MI':'Michigan','MN':'Minnesota',
    'MS':'Mississippi','MO':'Missouri','MT':'Montana','NE':'Nebraska','NV':'Nevada',
    'NH':'New Hampshire','NJ':'New Jersey','NM':'New Mexico','NY':'New York',
    'NC':'North Carolina','ND':'North Dakota','OH':'Ohio','OK':'Oklahoma',
    'OR':'Oregon','PA':'Pennsylvania','RI':'Rhode Island','SC':'South Carolina',
    'SD':'South Dakota','TN':'Tennessee','TX':'Texas','UT':'Utah','VT':'Vermont',
    'VA':'Virginia','WA':'Washington','WV':'West Virginia','WI':'Wisconsin','WY':'Wyoming',
}

REALTOR_STATE_URL = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_State.csv'
ZILLOW_ZHVI_STATE_URL = 'https://files.zillowstatic.com/research/public_csvs/zhvi/State_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv'


def landlord_score(state_abbr):
    if state_abbr in LANDLORD_TIER_1:   return 30
    if state_abbr in LANDLORD_TIER_2:   return 20
    if state_abbr in TENANT_FRIENDLY:   return -20
    return 0


def landlord_label(state_abbr):
    if state_abbr in LANDLORD_TIER_1:   return 'Landlord-friendly · Tier 1'
    if state_abbr in LANDLORD_TIER_2:   return 'Landlord-friendly · Tier 2'
    if state_abbr in TENANT_FRIENDLY:   return 'Tenant-friendly · AVOID'
    return 'Neutral'


def fetch_csv(url, label):
    print(f'fetching {label}…', file=sys.stderr)
    r = requests.get(url, timeout=60, headers={'User-Agent': 'Mozilla/5.0 APG-market-analysis'})
    r.raise_for_status()
    return list(csv.DictReader(io.StringIO(r.text)))


def latest_month_rows(rows, date_col_prefix='month'):
    """Realtor.com publishes monthly snapshots — keep the most recent month."""
    if not rows:
        return []
    if 'month_date_yyyymm' in rows[0]:
        max_m = max(r['month_date_yyyymm'] for r in rows if r.get('month_date_yyyymm'))
        return [r for r in rows if r.get('month_date_yyyymm') == max_m]
    return rows


def num(v):
    if v is None or v == '': return None
    try: return float(v)
    except (ValueError, TypeError): return None


def fetch_realtor_state():
    rows = fetch_csv(REALTOR_STATE_URL, 'Realtor.com state inventory')
    rows = latest_month_rows(rows)
    by_state = {}
    for r in rows:
        # State abbreviation column varies — try common names
        st = (r.get('state_id') or r.get('state') or r.get('STATEABBR') or '').upper()[:2]
        if not st or st not in STATE_NAMES:
            continue
        by_state[st] = {
            'days_on_market': num(r.get('median_days_on_market')),
            'price_reduced_share': num(r.get('price_reduced_share') or r.get('price_reduced_count_yy')),
            'active_listing_count': num(r.get('active_listing_count')),
            'median_listing_price': num(r.get('median_listing_price')),
            'pending_ratio': num(r.get('pending_ratio')),
            'month': r.get('month_date_yyyymm'),
        }
    print(f'Realtor: {len(by_state)} states', file=sys.stderr)
    return by_state


def fetch_zillow_state_zhvi():
    rows = fetch_csv(ZILLOW_ZHVI_STATE_URL, 'Zillow ZHVI state')
    if not rows:
        return {}
    # last column is the most recent monthly value
    date_cols = [c for c in rows[0].keys() if c[:4].isdigit() and '-' in c[:7]]
    latest = max(date_cols) if date_cols else None
    out = {}
    for r in rows:
        st = (r.get('StateName') or r.get('RegionName') or '').strip()
        # The state CSV uses full state names. Convert to abbr.
        abbr = next((a for a, n in STATE_NAMES.items() if n.lower() == st.lower()), None)
        if not abbr:
            continue
        v = num(r.get(latest)) if latest else None
        out[abbr] = {'zhvi': v, 'as_of': latest}
    print(f'Zillow ZHVI: {len(out)} states', file=sys.stderr)
    return out


def normalize(d, lo=0, hi=15, invert=False):
    """Take a dict {key:value} and produce {key:0..hi} based on rank."""
    vals = [(k, v) for k, v in d.items() if v is not None]
    if not vals:
        return {}
    vals.sort(key=lambda kv: kv[1], reverse=not invert)
    out = {}
    for i, (k, _) in enumerate(vals):
        out[k] = round(hi - (i * hi / max(len(vals)-1, 1)), 1)
    return out


def main():
    realtor = fetch_realtor_state()
    zillow  = fetch_zillow_state_zhvi()

    states = {}
    for abbr, name in STATE_NAMES.items():
        r = realtor.get(abbr, {})
        z = zillow.get(abbr, {})
        states[abbr] = {
            'abbr':  abbr,
            'name':  name,
            'tier':  landlord_label(abbr),
            'tier_score': landlord_score(abbr),
            'days_on_market':       r.get('days_on_market'),
            'price_reduced_share':  r.get('price_reduced_share'),
            'active_listing_count': r.get('active_listing_count'),
            'median_listing_price': r.get('median_listing_price'),
            'pending_ratio':        r.get('pending_ratio'),
            'zhvi':                 z.get('zhvi'),
        }

    # --- Component scoring (free metrics that proxy "easy to wholesale") ---
    dom_score = normalize({s: d['days_on_market']        for s, d in states.items()}, hi=15, invert=False)
    pr_score  = normalize({s: d['price_reduced_share']   for s, d in states.items()}, hi=15, invert=False)
    al_score  = normalize({s: d['active_listing_count']  for s, d in states.items()}, hi=10, invert=False)
    # ZHVI: penalize states with median home > $500k (wholesale spread harder)
    zh_score  = {}
    for s, d in states.items():
        v = d.get('zhvi')
        if v is None:                zh_score[s] = 0
        elif v <= 250000:            zh_score[s] = 10
        elif v <= 400000:            zh_score[s] = 5
        elif v <= 500000:            zh_score[s] = 0
        elif v <= 700000:            zh_score[s] = -5
        else:                        zh_score[s] = -10

    for s, d in states.items():
        d['score_landlord_tier']   = d['tier_score']
        d['score_days_on_market']  = dom_score.get(s, 0)
        d['score_price_reduced']   = pr_score.get(s, 0)
        d['score_active_listings'] = al_score.get(s, 0)
        d['score_home_value']      = zh_score.get(s, 0)
        d['composite_score'] = round(
            d['score_landlord_tier'] + d['score_days_on_market']
            + d['score_price_reduced'] + d['score_active_listings']
            + d['score_home_value'], 1)

    ranked = sorted(states.values(), key=lambda x: x['composite_score'], reverse=True)
    for i, s in enumerate(ranked, 1):
        s['rank'] = i

    out = {
        'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'methodology': {
            'decision_driver': 'where to point cold-call / SMS marketing',
            'inputs': [
                'landlord/tenant friendliness tier (hardcoded; ±30)',
                'days on market (Realtor.com; +0..15, more = better)',
                'price-reduced share (Realtor.com; +0..15)',
                'active listing density (Realtor.com; +0..10)',
                'median home value (Zillow ZHVI; -10..+10, $250k sweet spot)',
            ],
            'sources': [
                'Realtor.com Research (free)',
                'Zillow Research ZHVI (free)',
                'Hardcoded landlord-tenant tier (NOLO + Steadily 2024-2025 indices)',
            ],
        },
        'states': ranked,
        'cities':  [],   # TODO v2
        'zips':    [],   # TODO v3
    }
    SITE.mkdir(exist_ok=True)
    (SITE / 'markets.json').write_text(json.dumps(out, indent=2), encoding='utf-8')
    print(f'Wrote markets.json — top 5: ' + ', '.join(s['abbr'] for s in ranked[:5]))


if __name__ == '__main__':
    main()
