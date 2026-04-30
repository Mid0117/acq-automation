"""
Deals Dashboard — generates site/deals.html with one card per active deal
in ACQ pipeline stages 2-4 (LAO, Due Diligence, MAO).

Differs from the SMS Follow-Up dashboard (index.html) in that this is a
property-and-deal view: address, beds/baths/sqft, asking/ARV/MAO/spread,
last-call rating + summary, links to the rehab report and the GHL contact.

Pulls all data from:
  - GHL opportunities/search (server-side per-stage filter)
  - contacts_cache.json (written by sms_followup.py earlier in the workflow)
  - GHL contact custom fields
  - GHL opportunity custom fields
  - Latest "APG Lead Summary" note (regex-parsed for call rating + summary)
"""
import json, os, re, time, requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from html import escape

ET = ZoneInfo('America/New_York')

GHL_TOKEN      = os.environ['GHL_TOKEN']
GHL_LOCATION   = 'RCkiUmWqXX4BYQ39JXmm'
PIPELINE_ID    = 'O8wzIa6E3SgD8HLg6gh9'
CONTACTS_CACHE = 'contacts_cache.json'
OUT_DIR        = 'site'
OUT_FILE       = os.path.join(OUT_DIR, 'deals.html')

GHL_H = {'Authorization': f'Bearer {GHL_TOKEN}',
         'Content-Type': 'application/json', 'Version': '2021-07-28'}

HTTP_TIMEOUT = 30

def http(method, url, **kw):
    kw.setdefault('timeout', HTTP_TIMEOUT)
    for attempt in range(2):
        try:
            r = requests.request(method, url, **kw)
            if r.status_code >= 500 and attempt == 0:
                time.sleep(1.0); continue
            return r
        except (requests.Timeout, requests.ConnectionError):
            if attempt == 0:
                time.sleep(1.0); continue
            raise


# Stages = "real deals" (advanced past initial qualification)
STAGE_LAO       = 'd43fddd8-3a17-46b2-a193-cf18619f654f'
STAGE_RR        = '23a159ad-ba39-4c74-9d07-c1beb219d9f2'
STAGE_MAO       = '43589167-14f0-4e09-ba2a-8b9bd3296a4a'
STAGE_QUALIFIED = 'a17517be-8d1a-49fd-bd53-b9128a66e242'

DEAL_STAGES = {
    STAGE_LAO: '2. LAO',
    STAGE_RR:  '3. Due Diligence',
    STAGE_MAO: '4. MAO',
}
# Show Qualified too, but in its own grouping at the bottom
INCLUDE_QUALIFIED = True

# Contact custom fields
CF_BED         = 'xXEm77wvbxEbiqsw3lAz'
CF_BATH        = 'EtKof5yT7KAWmoaNQqJZ'
CF_SQFT        = '8kqwjqtJyTTeQ8SIaLQz'
CF_PROP_TYPE   = '7xsc1QHTleEFjRJChOgA'
CF_CONDITION   = '1Q4MENz9a1PsCF4jEtOU'
CF_TIMELINE    = 'v47I1Mi63RBpCD5N5RrH'
CF_MOTIVATION  = 'rbYZAdhvuvX1NQgexhxy'
CF_REASON_SELL = 'cJdRGRoox0RZCytRAVSI'
CF_ASK_PRICE   = '6q7syt4puxfP7E03Xxhd'
CF_DEAL_TYPE   = 'xzdGu36ZWBTQBNLuCuG7'
CF_REPAIRS     = 'dbYoYFVTiCbqoJxC9HkR'
CF_ARV         = 'nCWzIGfZHki0dv84gUem'
CF_70_ARV      = 'R7QUzOdOnJXgoGRPwxdF'
CF_ZILLOW      = '48pr9cc9hDFas111fDpF'
CF_ASSIGN_FEE  = '4IJPj2UebvkrYJ0rK06l'

# Opportunity custom fields
OF_REHAB       = 'cPCQEuwOJNMtoWR8CrLR'
OF_DEAL_TYPE   = 'CfbtlEDb6zapBZrhwkM4'
OF_EXIT_STRAT  = 'MT83ArwttTUiH17oo9l0'
OF_NUM_UNITS   = 'w9OeqjXnlK5jjnm4IMFp'

GHL_CONTACT_BASE = 'https://app.gohighlevel.com/v2/location/RCkiUmWqXX4BYQ39JXmm/contacts/detail'


def write_status(success, summary='', error=''):
    try:
        with open('last_run_deals.json', 'w') as f:
            json.dump({'success': success,
                       'timestamp': datetime.now(timezone.utc).isoformat(),
                       'summary': summary,
                       'error': error[:500]}, f, indent=2)
    except Exception:
        pass


def load_contacts_cache():
    if not os.path.exists(CONTACTS_CACHE):
        return {}
    try:
        return (json.load(open(CONTACTS_CACHE)) or {}).get('contacts', {}) or {}
    except Exception:
        return {}


_CONTACTS_LOOKUP = {}


def get_contact(cid):
    cached = _CONTACTS_LOOKUP.get(cid)
    if cached:
        return cached
    r = http('GET', f'https://services.leadconnectorhq.com/contacts/{cid}', headers=GHL_H)
    if r.status_code != 200:
        return {}
    return r.json().get('contact', {})


def get_opp_fields(oid):
    r = http('GET', f'https://services.leadconnectorhq.com/opportunities/{oid}', headers=GHL_H)
    if r.status_code != 200:
        return {}, ''
    opp = r.json().get('opportunity', {})
    cf  = {f['id']: (f.get('fieldValue') or f.get('field_value') or '')
           for f in (opp.get('customFields') or [])}
    return cf, opp.get('updatedAt', '')


def fetch_deals():
    out = []
    stage_set = dict(DEAL_STAGES)
    if INCLUDE_QUALIFIED:
        stage_set[STAGE_QUALIFIED] = '1. Qualified'
    for stage_id, label in stage_set.items():
        page = 1
        while True:
            r = http('GET', 'https://services.leadconnectorhq.com/opportunities/search',
                     headers=GHL_H,
                     params={'location_id': GHL_LOCATION, 'pipeline_id': PIPELINE_ID,
                             'pipeline_stage_id': stage_id,
                             'limit': 100, 'page': page})
            if r.status_code != 200: break
            opps = r.json().get('opportunities', [])
            if not opps: break
            for o in opps:
                c = o.get('contact') or {}
                if 'agent' in c.get('tags', []) or not o.get('contactId'):
                    continue
                out.append({
                    'cid':         o['contactId'],
                    'oid':         o['id'],
                    'stage':       stage_id,
                    'stage_label': label,
                    'updated':     o.get('updatedAt', ''),
                    'tags':        c.get('tags', []),
                })
            if len(opps) < 100: break
            page += 1
            time.sleep(0.1)
    return out


SUMMARY_RE_RATING  = re.compile(r'Rating:\s*(\d+)\s*/\s*10', re.IGNORECASE)
SUMMARY_RE_SUMMARY = re.compile(r'Summary:\s*(.+?)(?=\n\n|\nWhat we could improve|\nAction items|\nRed flags|\nRehab|$)',
                                re.IGNORECASE | re.DOTALL)
SUMMARY_RE_TEMP    = re.compile(r'Lead Temp:\s*([A-Za-z]+)', re.IGNORECASE)


def fetch_summary_note(cid):
    """Pull the latest 'APG Lead Summary' note. Returns dict with rating, summary, raw."""
    try:
        r = http('GET', f'https://services.leadconnectorhq.com/contacts/{cid}/notes', headers=GHL_H)
        if r.status_code != 200:
            return {}
        for n in r.json().get('notes', []):
            body = n.get('body') or ''
            if body.startswith('APG Lead Summary'):
                rating = None
                summary = ''
                temp = ''
                m = SUMMARY_RE_RATING.search(body)
                if m: rating = int(m.group(1))
                m = SUMMARY_RE_SUMMARY.search(body)
                if m: summary = m.group(1).strip().replace('\n', ' ')[:400]
                m = SUMMARY_RE_TEMP.search(body)
                if m: temp = m.group(1).strip()
                return {'rating': rating, 'summary': summary, 'temp': temp,
                        'updated_at': n.get('dateAdded') or n.get('createdAt') or ''}
        return {}
    except Exception:
        return {}


def fmt_money(v):
    try:
        v = int(v)
        if v >= 1_000_000:
            return f'${v/1_000_000:.2f}M'
        if v >= 1_000:
            return f'${v/1_000:.0f}k'
        return f'${v:,}'
    except Exception:
        return str(v) if v else '—'


def days_ago(iso):
    if not iso: return ''
    try:
        dt = datetime.fromisoformat(str(iso).replace('Z', '+00:00'))
        delta = datetime.now(timezone.utc) - dt
        d = delta.days
        if d == 0: return 'today'
        if d == 1: return '1d ago'
        return f'{d}d ago'
    except Exception:
        return ''


def to_et(iso):
    if not iso: return ''
    try:
        dt = datetime.fromisoformat(str(iso).replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(ET).strftime('%b %d, %I:%M %p ET')
    except Exception:
        return ''


def temp_class(temp):
    t = (temp or '').lower()
    if t == 'hot':  return 'temp hot'
    if t == 'warm': return 'temp warm'
    if t == 'cold': return 'temp cold'
    return 'temp gray'


def stage_class(stage_id):
    if stage_id == STAGE_LAO:       return 'stage-lao'
    if stage_id == STAGE_RR:        return 'stage-rr'
    if stage_id == STAGE_MAO:       return 'stage-mao'
    if stage_id == STAGE_QUALIFIED: return 'stage-qualified'
    return ''


def rating_class(r):
    if r is None: return 'rating gray'
    if r >= 8:    return 'rating green'
    if r >= 5:    return 'rating warm'
    return 'rating hot'


HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>APG ACQ — Deals Dashboard</title>
<style>
:root {
  --bg: #0b0d12;
  --panel: rgba(255,255,255,0.04);
  --panel-border: rgba(255,255,255,0.08);
  --text: #e6e8ee;
  --text-dim: #8b93a7;
  --accent: #6366f1;
  --accent2: #06b6d4;
  --hot: #ef4444;
  --warm: #f59e0b;
  --green: #10b981;
  --gray: #475569;
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 0;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  background: radial-gradient(1200px 800px at 20% -10%, rgba(99,102,241,.15), transparent 60%),
              radial-gradient(1000px 600px at 100% 100%, rgba(6,182,212,.12), transparent 60%),
              var(--bg);
  color: var(--text);
  min-height: 100vh;
  -webkit-font-smoothing: antialiased;
}
.container { max-width: 1500px; margin: 0 auto; padding: 32px 24px 80px; }
header {
  display: flex; justify-content: space-between; align-items: end;
  margin-bottom: 24px; flex-wrap: wrap; gap: 12px;
}
h1 {
  font-size: 28px; font-weight: 700; letter-spacing: -0.02em;
  margin: 0; background: linear-gradient(90deg, #fff, #94a3b8);
  -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}
.subtitle { color: var(--text-dim); font-size: 13px; margin-top: 6px; }
.nav { display: flex; gap: 8px; margin-bottom: 24px; }
.nav a {
  padding: 10px 18px; border-radius: 10px; background: var(--panel);
  border: 1px solid var(--panel-border); color: var(--text-dim);
  font-size: 13px; font-weight: 500; text-decoration: none; transition: all .15s;
}
.nav a:hover { color: var(--text); background: rgba(255,255,255,0.06); }
.nav a.active {
  color: #fff; background: linear-gradient(135deg, var(--accent), var(--accent2));
  border-color: transparent;
}
.kpi-grid {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 16px; margin-bottom: 24px;
}
.kpi {
  background: var(--panel); border: 1px solid var(--panel-border);
  backdrop-filter: blur(20px); border-radius: 16px; padding: 20px;
}
.kpi .label { font-size: 12px; color: var(--text-dim); text-transform: uppercase;
              letter-spacing: 0.08em; margin-bottom: 8px; }
.kpi .value { font-size: 28px; font-weight: 700; line-height: 1; }
.kpi .sub { color: var(--text-dim); font-size: 12px; margin-top: 6px; }
.kpi.green .value { color: var(--green); }
.kpi.warm .value { color: var(--warm); }
.kpi.hot .value { color: var(--hot); }

.filter-bar {
  display: flex; gap: 8px; flex-wrap: wrap; align-items: center;
  margin-bottom: 20px;
  padding: 12px 16px; background: var(--panel); border: 1px solid var(--panel-border);
  border-radius: 12px;
}
.filter-bar input[type=text] {
  flex: 1; min-width: 220px;
  padding: 8px 12px; background: rgba(255,255,255,0.04);
  border: 1px solid var(--panel-border); border-radius: 8px;
  color: var(--text); font-size: 13px;
}
.filter-bar input[type=text]:focus { outline: none; border-color: var(--accent); }
.filter-chip {
  padding: 6px 12px; border-radius: 999px; background: rgba(255,255,255,0.04);
  border: 1px solid var(--panel-border); color: var(--text-dim);
  font-size: 12px; font-weight: 500; cursor: pointer; user-select: none;
}
.filter-chip.active { background: var(--accent); color: #fff; border-color: var(--accent); }

.grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
  gap: 16px;
}
.card {
  background: var(--panel); border: 1px solid var(--panel-border);
  border-radius: 16px; padding: 20px; display: flex; flex-direction: column;
  position: relative; overflow: hidden;
}
.card .top { display: flex; justify-content: space-between; align-items: start; gap: 12px; margin-bottom: 12px; }
.card .name { font-size: 13px; color: var(--text-dim); font-weight: 500; }
.card .addr { font-size: 16px; font-weight: 600; line-height: 1.3; margin: 4px 0 6px; }
.card .place { font-size: 12px; color: var(--text-dim); }
.card .stage-pill {
  font-size: 11px; padding: 4px 10px; border-radius: 999px; font-weight: 600;
  white-space: nowrap; letter-spacing: 0.04em;
}
.stage-qualified { background: rgba(99,102,241,0.15); color: #a5b4fc; }
.stage-lao       { background: rgba(6,182,212,0.15); color: #67e8f9; }
.stage-rr        { background: rgba(245,158,11,0.15); color: #fcd34d; }
.stage-mao       { background: rgba(16,185,129,0.15); color: #6ee7b7; }

.card .pillrow { display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 12px; }
.pill {
  font-size: 11px; padding: 3px 8px; border-radius: 6px;
  background: rgba(255,255,255,0.06); color: var(--text-dim);
}
.pill.temp.hot   { background: rgba(239,68,68,0.18); color: #fca5a5; box-shadow: 0 0 8px rgba(239,68,68,0.3); }
.pill.temp.warm  { background: rgba(245,158,11,0.18); color: #fcd34d; }
.pill.temp.cold  { background: rgba(99,102,241,0.18); color: #a5b4fc; }

.card .specs { display: flex; gap: 14px; font-size: 13px; color: var(--text-dim); margin-bottom: 14px; }
.card .specs span { display: inline-flex; align-items: center; gap: 4px; }
.card .specs strong { color: var(--text); font-weight: 600; }

.fin-grid {
  display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px;
  margin-bottom: 14px; padding: 12px;
  background: rgba(0,0,0,0.2); border-radius: 10px;
}
.fin-grid.four { grid-template-columns: repeat(4, 1fr); gap: 8px; padding: 10px; }
.fin-grid .fin .lab {
  font-size: 10px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.08em;
}
.fin-grid .fin .val { font-size: 16px; font-weight: 700; }
.fin-grid.four .fin .val { font-size: 15px; }
.fin-grid .fin .val.positive { color: var(--green); }
.fin-grid .fin .val.negative { color: var(--hot); }
.fin-grid .fin.spread .val.positive { color: var(--green); }
.fin-grid .fin.spread .val.negative { color: var(--hot); }

.rating-row {
  display: flex; align-items: center; gap: 10px; margin-bottom: 10px;
}
.rating {
  font-size: 14px; font-weight: 700; padding: 4px 10px; border-radius: 8px;
}
.rating.green { background: rgba(16,185,129,0.15); color: #6ee7b7; }
.rating.warm  { background: rgba(245,158,11,0.15); color: #fcd34d; }
.rating.hot   { background: rgba(239,68,68,0.15); color: #fca5a5; }
.rating.gray  { background: rgba(71,85,105,0.18); color: #94a3b8; }
.rating-row .summary { font-size: 12px; color: var(--text-dim); flex: 1; line-height: 1.4; }
.rating-row .label   { font-size: 10px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.08em; }

.deal-meta { font-size: 12px; color: var(--text-dim); margin-bottom: 12px; line-height: 1.5; }
.deal-meta strong { color: var(--text); font-weight: 500; }

.card .actions { display: flex; gap: 8px; margin-top: auto; }
.btn {
  padding: 8px 14px; border-radius: 8px; font-size: 12px; font-weight: 600;
  text-decoration: none; transition: all .15s; flex: 1; text-align: center;
}
.btn.primary {
  background: linear-gradient(135deg, var(--accent), var(--accent2));
  color: #fff; border: 1px solid transparent;
}
.btn.primary:hover { transform: translateY(-1px); box-shadow: 0 6px 16px rgba(99,102,241,0.4); }
.btn.secondary { background: rgba(255,255,255,0.04); border: 1px solid var(--panel-border); color: var(--text); }
.btn.secondary:hover { background: rgba(255,255,255,0.08); }
.btn.ghost { background: transparent; color: var(--text-dim); border: 1px dashed var(--panel-border); }
.section { margin-bottom: 32px; }
.section h2 { font-size: 18px; font-weight: 600; margin: 0 0 14px; display: flex; align-items: center; gap: 10px; }
.section .count { font-size: 13px; color: var(--text-dim); font-weight: 400; }
.empty { text-align: center; color: var(--text-dim); padding: 40px; background: var(--panel);
         border: 1px solid var(--panel-border); border-radius: 16px; }
footer { color: var(--text-dim); font-size: 12px; text-align: center; margin-top: 40px; }
</style>
</head>
<body>
<div class="container">
  <header>
    <div>
      <h1>APG ACQ — Deals Dashboard</h1>
      <div class="subtitle">Active properties in stages 1-4 · Last updated __TIMESTAMP__</div>
    </div>
  </header>

  <div class="nav">
    <a href="index.html">Follow-Ups</a>
    <a href="deals.html" class="active">Deals</a>
  </div>

  <div class="kpi-grid">
    <div class="kpi"><div class="label">Total Deals</div><div class="value">__TOTAL__</div><div class="sub">stages 1-4 (active)</div></div>
    <div class="kpi green"><div class="label">Total Assignment Fee</div><div class="value">__ASSIGN_FEE_TOTAL__</div><div class="sub">across __WITH_FEE__ deals with fee set</div></div>
    <div class="kpi"><div class="label">Avg Call Rating</div><div class="value">__AVG_RATING__/10</div><div class="sub">across __RATED__ rated calls</div></div>
    <div class="kpi warm"><div class="label">With ARV calculated</div><div class="value">__WITH_ARV__</div><div class="sub">/ __TOTAL__ deals</div></div>
    <div class="kpi hot"><div class="label">Hot Leads</div><div class="value">__HOT__</div><div class="sub">flagged by call AI</div></div>
  </div>

  <div class="filter-bar">
    <input type="text" id="searchBox" placeholder="Search by name, address, state, deal type, motivation...">
    <span class="filter-chip active" data-filter="all">All</span>
    <span class="filter-chip" data-filter="1. Qualified">Qualified</span>
    <span class="filter-chip" data-filter="2. LAO">LAO</span>
    <span class="filter-chip" data-filter="3. Due Diligence">DD</span>
    <span class="filter-chip" data-filter="4. MAO">MAO</span>
    <span class="filter-chip" data-filter="hot">🔥 Hot</span>
    <span class="filter-chip" data-filter="has-arv">Has ARV</span>
  </div>

  __SECTIONS__

  <footer>Auto-refreshed every 30 minutes by the GitHub Actions cron · <a href="index.html" style="color:var(--accent2)">Back to follow-ups</a></footer>
</div>

<script>
const cards = Array.from(document.querySelectorAll('.card'));
const search = document.getElementById('searchBox');
const chips  = Array.from(document.querySelectorAll('.filter-chip'));
let activeFilter = 'all';

function applyFilters() {
  const q = (search.value || '').toLowerCase();
  cards.forEach(c => {
    const text = c.dataset.search || '';
    const stage = c.dataset.stage || '';
    const flags = (c.dataset.flags || '').split(' ');
    let visible = true;
    if (q && !text.includes(q)) visible = false;
    if (visible && activeFilter !== 'all') {
      if (activeFilter.startsWith('1.') || activeFilter.startsWith('2.') ||
          activeFilter.startsWith('3.') || activeFilter.startsWith('4.')) {
        if (stage !== activeFilter) visible = false;
      } else if (!flags.includes(activeFilter)) {
        visible = false;
      }
    }
    c.style.display = visible ? '' : 'none';
  });
  // Hide empty sections
  document.querySelectorAll('.section').forEach(s => {
    const visibleCards = s.querySelectorAll('.card:not([style*="display: none"])');
    s.style.display = visibleCards.length ? '' : 'none';
  });
}

search.addEventListener('input', applyFilters);
chips.forEach(c => c.addEventListener('click', e => {
  chips.forEach(x => x.classList.remove('active'));
  c.classList.add('active');
  activeFilter = c.dataset.filter;
  applyFilters();
}));
</script>
</body>
</html>
"""


def render_card(d):
    c = d['contact']
    cf = {f['id']: (f.get('value') or '') for f in (c.get('customFields') or [])}
    of = d['opp_cf']

    name = f"{c.get('firstName','')} {c.get('lastName','')}".strip() or '(no name)'
    addr1 = (c.get('address1') or '').strip()
    city  = (c.get('city') or '').strip()
    state = (c.get('state') or '').strip()
    zipc  = (c.get('postalCode') or '').strip().split('-')[0]
    place = ', '.join(p for p in (city, state, zipc) if p)

    beds  = cf.get(CF_BED) or '?'
    baths = cf.get(CF_BATH) or '?'
    sqft  = cf.get(CF_SQFT) or '?'
    cond  = cf.get(CF_CONDITION) or ''
    ptype = cf.get(CF_PROP_TYPE) or ''

    asking_raw = cf.get(CF_ASK_PRICE) or ''
    arv_raw    = cf.get(CF_ARV) or ''
    arv70_raw  = cf.get(CF_70_ARV) or ''

    try:
        asking_v = int(asking_raw) if asking_raw else None
    except Exception:
        asking_v = None
    try:
        arv_v = int(arv_raw) if arv_raw else None
    except Exception:
        arv_v = None
    try:
        arv70_v = int(arv70_raw) if arv70_raw else (int(arv_v * 0.7) if arv_v else None)
    except Exception:
        arv70_v = None

    # Spread = MAO - Asking (positive means deal makes sense)
    spread_v = None
    spread_class = ''
    if arv70_v and asking_v:
        spread_v = arv70_v - asking_v
        spread_class = 'positive' if spread_v >= 0 else 'negative'

    motiv  = cf.get(CF_MOTIVATION) or ''
    timeln = cf.get(CF_TIMELINE) or ''
    reason = cf.get(CF_REASON_SELL) or ''
    deal_t = cf.get(CF_DEAL_TYPE) or of.get(OF_DEAL_TYPE) or ''
    repairs = cf.get(CF_REPAIRS) or ''
    rehab_url = of.get(OF_REHAB) or ''
    zillow = cf.get(CF_ZILLOW) or ''
    assign_fee_raw = cf.get(CF_ASSIGN_FEE) or ''
    try:
        assign_fee_v = int(float(assign_fee_raw)) if assign_fee_raw else None
    except Exception:
        assign_fee_v = None

    note = d.get('note') or {}
    rating = note.get('rating')
    summary = note.get('summary') or ''
    temp = (note.get('temp') or '').strip()

    last_updated = d.get('updated', '')

    flags = []
    if temp.lower() == 'hot':
        flags.append('hot')
    if arv_v:
        flags.append('has-arv')

    ghl_link = f'{GHL_CONTACT_BASE}/{d["cid"]}'

    # Build search blob (lowercase) for client-side filter
    search_blob = ' '.join([
        name, addr1, city, state, place,
        deal_t, motiv, reason, timeln, ptype, cond, temp,
        d['stage_label']
    ]).lower()

    rating_html = ''
    if rating is not None:
        rating_html = f'<span class="{rating_class(rating)}">{rating}/10</span>'
    else:
        rating_html = '<span class="rating gray">—</span>'

    # Compose
    pieces = []
    pieces.append(f'<div class="card" data-stage="{escape(d["stage_label"])}" '
                  f'data-flags="{escape(" ".join(flags))}" '
                  f'data-search="{escape(search_blob)}">')

    pieces.append('<div class="top">')
    pieces.append(f'<div><div class="name">{escape(name)}</div>'
                  f'<div class="addr">{escape(addr1 or "(no address on file)")}</div>'
                  f'<div class="place">{escape(place)}</div></div>')
    pieces.append(f'<div class="stage-pill {stage_class(d["stage"])}">{escape(d["stage_label"])}</div>')
    pieces.append('</div>')

    # Pill row: temp + days
    pill_pieces = []
    if temp:
        pill_pieces.append(f'<span class="pill {temp_class(temp)}">🌡 {escape(temp)}</span>')
    if last_updated:
        pill_pieces.append(f'<span class="pill">⏱ Updated {escape(days_ago(last_updated))}</span>')
    if deal_t:
        pill_pieces.append(f'<span class="pill">{escape(deal_t)}</span>')
    if pill_pieces:
        pieces.append(f'<div class="pillrow">{"".join(pill_pieces)}</div>')

    # Specs row
    spec_pieces = []
    if beds != '?' or baths != '?':
        spec_pieces.append(f'<span><strong>{escape(str(beds))}</strong>bd / <strong>{escape(str(baths))}</strong>ba</span>')
    if sqft != '?':
        try:
            spec_pieces.append(f'<span><strong>{int(sqft):,}</strong> sqft</span>')
        except Exception:
            spec_pieces.append(f'<span><strong>{escape(str(sqft))}</strong> sqft</span>')
    if cond:
        spec_pieces.append(f'<span>Cond: <strong>{escape(cond)}</strong></span>')
    if spec_pieces:
        pieces.append(f'<div class="specs">{"".join(spec_pieces)}</div>')

    # Financials — 4 tiles: Asking / ARV / 70% MAO / Assignment Fee
    pieces.append('<div class="fin-grid four">')
    pieces.append(f'<div class="fin"><div class="lab">Asking</div><div class="val">{fmt_money(asking_v) if asking_v else "—"}</div></div>')
    pieces.append(f'<div class="fin"><div class="lab">ARV</div><div class="val">{fmt_money(arv_v) if arv_v else "—"}</div></div>')
    pieces.append(f'<div class="fin"><div class="lab">70% MAO</div><div class="val">{fmt_money(arv70_v) if arv70_v else "—"}</div></div>')
    fee_class = 'val'
    if assign_fee_v is not None:
        fee_class = 'val positive'
    pieces.append(f'<div class="fin"><div class="lab">Assign Fee</div>'
                  f'<div class="{fee_class}">{fmt_money(assign_fee_v) if assign_fee_v else "—"}</div></div>')
    pieces.append('</div>')

    # Call rating + summary
    pieces.append('<div class="rating-row">')
    pieces.append(f'<div><div class="label">Call</div>{rating_html}</div>')
    if summary:
        pieces.append(f'<div class="summary">{escape(summary[:240])}</div>')
    else:
        pieces.append('<div class="summary" style="font-style:italic">No call analysis yet.</div>')
    pieces.append('</div>')

    # Deal meta
    meta_lines = []
    if motiv:   meta_lines.append(f'<strong>Motivation:</strong> {escape(motiv)}')
    if timeln:  meta_lines.append(f'<strong>Timeline:</strong> {escape(timeln)}')
    if reason:  meta_lines.append(f'<strong>Reason:</strong> {escape(reason[:180])}')
    if repairs: meta_lines.append(f'<strong>Repairs:</strong> {escape(repairs[:180])}')
    if meta_lines:
        pieces.append(f'<div class="deal-meta">{"<br>".join(meta_lines)}</div>')

    # Actions
    pieces.append('<div class="actions">')
    pieces.append(f'<a class="btn primary" href="{escape(ghl_link)}" target="_blank">Open in GHL</a>')
    if rehab_url:
        pieces.append(f'<a class="btn secondary" href="{escape(rehab_url)}" target="_blank">Rehab Report</a>')
    else:
        pieces.append('<span class="btn ghost">No rehab report yet</span>')
    if zillow:
        pieces.append(f'<a class="btn secondary" href="{escape(zillow)}" target="_blank">Zillow</a>')
    pieces.append('</div>')

    pieces.append('</div>')
    return ''.join(pieces)


def main():
    try:
        _main_inner()
        write_status(True, 'deals dashboard rendered')
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f'!! deals dashboard failed: {e}\n{tb}')
        write_status(False, '', f'{e}: {tb[-300:]}')
        raise


def _main_inner():
    os.makedirs(OUT_DIR, exist_ok=True)
    global _CONTACTS_LOOKUP
    _CONTACTS_LOOKUP = load_contacts_cache()
    print(f'Contacts cache: {len(_CONTACTS_LOOKUP)} entries')

    deals = fetch_deals()
    print(f'Found {len(deals)} active deals across stages 1-4')

    # Enrich each deal
    enriched = []
    for d in deals:
        d['contact'] = get_contact(d['cid'])
        d['opp_cf'], _ = get_opp_fields(d['oid'])
        d['note'] = fetch_summary_note(d['cid'])
        enriched.append(d)
        time.sleep(0.1)

    # Aggregates
    assign_fee_total = 0
    with_fee = 0
    rated = []
    hot = 0
    with_arv = 0
    for d in enriched:
        cf = {f['id']: (f.get('value') or '') for f in (d['contact'].get('customFields') or [])}
        try:
            if int(cf.get(CF_ARV) or 0) > 0:
                with_arv += 1
        except Exception:
            pass
        try:
            fee = int(float(cf.get(CF_ASSIGN_FEE) or 0))
            if fee > 0:
                assign_fee_total += fee
                with_fee += 1
        except Exception:
            pass
        if d['note'].get('rating') is not None:
            rated.append(d['note']['rating'])
        if (d['note'].get('temp') or '').lower() == 'hot':
            hot += 1

    avg_rating = (sum(rated) / len(rated)) if rated else 0

    # Group by stage in display order: MAO → RR → LAO → Qualified
    order = [STAGE_MAO, STAGE_RR, STAGE_LAO, STAGE_QUALIFIED]
    sections_html = []
    for stage_id in order:
        in_stage = [d for d in enriched if d['stage'] == stage_id]
        if not in_stage:
            continue
        # Sort within section: rated first (by rating desc), then by updated desc
        in_stage.sort(key=lambda x: (
            -1 if x['note'].get('rating') is None else -x['note']['rating'],
            x.get('updated', '')
        ))
        label = in_stage[0]['stage_label']
        sections_html.append(
            f'<section class="section">'
            f'<h2>{escape(label)} <span class="count">({len(in_stage)})</span></h2>'
            f'<div class="grid">{"".join(render_card(d) for d in in_stage)}</div>'
            f'</section>'
        )
    if not sections_html:
        sections_html = ['<div class="empty">No deals in stages 1-4 right now.</div>']

    html = HTML
    html = html.replace('__TIMESTAMP__', datetime.now(ET).strftime('%b %d, %Y %I:%M %p ET'))
    html = html.replace('__TOTAL__', str(len(enriched)))
    html = html.replace('__ASSIGN_FEE_TOTAL__', fmt_money(assign_fee_total) if assign_fee_total else '—')
    html = html.replace('__WITH_FEE__', str(with_fee))
    html = html.replace('__AVG_RATING__', f'{avg_rating:.1f}' if rated else '—')
    html = html.replace('__RATED__', str(len(rated)))
    html = html.replace('__WITH_ARV__', str(with_arv))
    html = html.replace('__HOT__', str(hot))
    html = html.replace('__SECTIONS__', ''.join(sections_html))

    with open(OUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'Wrote {OUT_FILE} ({len(html):,} bytes)')


if __name__ == '__main__':
    main()
