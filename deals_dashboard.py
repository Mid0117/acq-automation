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


def zillow_search_url(addr1, city, state, zipc):
    """Build a Zillow search URL that works for any property with an address.
    Used as a fallback when we don't have the actual /homedetails/ URL from Apify.
    Format: https://www.zillow.com/homes/<slug>_rb/"""
    parts = []
    for p in (addr1, city, state, zipc):
        if p:
            slug = re.sub(r'[^a-zA-Z0-9 ]', '', str(p)).strip().replace(' ', '-')
            if slug:
                parts.append(slug)
    if not parts:
        return ''
    return f'https://www.zillow.com/homes/{"-".join(parts)}_rb/'


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
  --bg: #FBF8F0;
  --paper: #FFFCF4;
  --ink: #1A2840;
  --ink-soft: #455066;
  --ink-mute: #6B7591;
  --gold: #FFC72C;
  --gold-deep: #C99500;
  --gold-soft: #FFF6CC;
  --rule: rgba(26,40,64,0.12);
  --rule-strong: rgba(26,40,64,0.22);
  --green: #2F7D5B;
  --hot:   #C5443A;
  --warm:  #B57A1A;
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 0;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
  background: var(--bg);
  color: var(--ink);
  line-height: 1.55;
  -webkit-font-smoothing: antialiased;
}
.container { max-width: 1300px; margin: 0 auto; padding: 28px 36px 80px; }

/* ── Top meta ────────────────────────────── */
.meta-bar {
  display: flex; justify-content: space-between; align-items: center;
  border-top: 4px solid var(--ink); padding: 14px 0 0;
  font-size: 11px; font-weight: 700;
  text-transform: uppercase; letter-spacing: 0.14em; color: var(--ink-soft);
}

/* ── Logo + nav row ──────────────────────── */
.logo-row {
  display: flex; align-items: center; justify-content: space-between;
  gap: 16px; margin: 24px 0 12px; flex-wrap: wrap;
}
.logo-svg { width: 200px; height: auto; }
.logo-svg .atom-orbit { stroke: var(--gold-deep); stroke-width: 2.6; fill: none; }
.logo-svg .atom-core  { fill: var(--gold-deep); }
.logo-svg .brand-main { fill: var(--ink); }
.logo-svg .brand-sub  { fill: var(--ink-soft); letter-spacing: 4px; }
.nav { display: flex; gap: 6px; }
.nav a {
  padding: 6px 12px; border-radius: 3px;
  background: transparent; border: 1px solid var(--rule);
  color: var(--ink-soft); font-size: 11px; font-weight: 700;
  letter-spacing: 0.06em; text-transform: uppercase; text-decoration: none;
  transition: all .12s;
}
.nav a:hover { color: var(--ink); border-color: var(--ink); }
.nav a.active { background: var(--ink); color: var(--gold); border-color: var(--ink); }

/* ── Document header ─────────────────────── */
.doc-header { padding: 28px 0 24px; }
.doc-header h1 {
  font-family: "Iowan Old Style", "Palatino Linotype", Palatino, Georgia, serif;
  font-weight: 600;
  font-size: 48px; line-height: 1.05; letter-spacing: -0.01em;
  margin: 0 0 12px; color: var(--ink);
}
.doc-header h1 .accent { font-style: italic; color: var(--gold-deep); }
.doc-header .lede {
  font-family: "Iowan Old Style", Georgia, serif; font-style: italic;
  font-size: 16px; color: var(--ink-soft); max-width: 640px; margin: 0;
}
.doc-header hr { border: 0; border-top: 1px solid var(--rule); margin: 28px 0 0; }

/* ── Numbered section ─────────────────────── */
.sec { margin: 36px 0 18px; }
.sec .tag-row { display: flex; align-items: center; gap: 12px; margin-bottom: 8px; flex-wrap: wrap; }
.sec .num {
  display: inline-block; background: var(--gold); color: var(--ink);
  font-weight: 800; font-size: 12px; letter-spacing: 0.04em;
  padding: 3px 8px; border-radius: 3px;
  font-family: ui-monospace, "SF Mono", monospace;
}
.sec h2 {
  font-family: "Iowan Old Style", Georgia, serif; font-weight: 600;
  font-size: 24px; letter-spacing: -0.005em; margin: 0; color: var(--ink);
}
.sec .count { font-size: 13px; color: var(--ink-mute); font-weight: 500; }
.sec hr { border: 0; border-top: 1px solid var(--rule); margin: 0 0 16px; }

/* ── Stat row (KPIs) ──────────────────────── */
.stat-row {
  display: grid; gap: 10px;
  grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
  margin-top: 14px;
}
.stat {
  background: var(--paper); border: 1px solid var(--rule);
  border-top: 3px solid var(--gold);
  border-radius: 3px; padding: 16px 18px;
}
.stat .lab {
  font-size: 10px; font-weight: 800; letter-spacing: 0.12em;
  text-transform: uppercase; color: var(--ink-mute); margin-bottom: 8px;
}
.stat .v {
  font-family: "Iowan Old Style", Georgia, serif;
  font-size: 30px; font-weight: 600; color: var(--ink); line-height: 1.05;
}
.stat .sub { font-size: 12px; color: var(--ink-soft); margin-top: 6px; line-height: 1.4; }
.stat.green { border-top-color: var(--green); }
.stat.green .v { color: var(--green); }
.stat.hot { border-top-color: var(--hot); }
.stat.hot .v { color: var(--hot); }
.stat.warm { border-top-color: var(--warm); }
.stat.warm .v { color: var(--warm); }

/* ── Filter bar ─────────────────────────── */
.filter-bar {
  display: flex; gap: 8px; flex-wrap: wrap; align-items: center;
  margin-bottom: 16px; padding: 10px 14px;
  background: var(--paper); border: 1px solid var(--rule); border-radius: 4px;
}
.filter-bar input[type=text] {
  flex: 1; min-width: 220px;
  padding: 8px 12px; background: var(--bg);
  border: 1px solid var(--rule); border-radius: 3px;
  color: var(--ink); font-size: 13px;
}
.filter-bar input[type=text]:focus { outline: none; border-color: var(--gold-deep); }
.filter-chip {
  padding: 6px 12px; border-radius: 3px;
  background: transparent; border: 1px solid var(--rule);
  color: var(--ink-soft); font-size: 11px; font-weight: 700;
  letter-spacing: 0.06em; text-transform: uppercase;
  cursor: pointer; user-select: none; transition: all .12s;
}
.filter-chip:hover { color: var(--ink); border-color: var(--ink); }
.filter-chip.active { background: var(--ink); color: var(--gold); border-color: var(--ink); }

/* ── Card grid ──────────────────────────── */
.grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
  gap: 14px;
}
.card {
  background: var(--paper); border: 1px solid var(--rule);
  border-top: 3px solid var(--gold);
  border-radius: 3px; padding: 18px;
  display: flex; flex-direction: column;
}
.card.stage-mao        { border-top-color: var(--green); }
.card.stage-rr         { border-top-color: var(--gold-deep); }
.card.stage-lao        { border-top-color: var(--warm); }
.card.stage-qualified  { border-top-color: var(--ink-soft); }

.card .top { display: flex; justify-content: space-between; align-items: start; gap: 12px; margin-bottom: 12px; }
.card .name {
  font-size: 11px; font-weight: 700; letter-spacing: 0.06em;
  text-transform: uppercase; color: var(--ink-mute);
}
.card .addr {
  font-family: "Iowan Old Style", Georgia, serif;
  font-size: 17px; font-weight: 600; line-height: 1.25; margin: 4px 0 6px;
  color: var(--ink);
}
.card .place { font-size: 12px; color: var(--ink-soft); }
.card .stage-pill {
  font-size: 10px; padding: 3px 8px; border-radius: 3px; font-weight: 800;
  letter-spacing: 0.06em; text-transform: uppercase; white-space: nowrap;
  font-family: ui-monospace, monospace;
}
.stage-pill.stage-qualified { background: var(--gold-soft); color: var(--ink); }
.stage-pill.stage-lao       { background: rgba(181,122,26,0.15); color: var(--warm); }
.stage-pill.stage-rr        { background: rgba(201,165,42,0.18); color: var(--gold-deep); }
.stage-pill.stage-mao       { background: rgba(47,125,91,0.15); color: var(--green); }

.card .pillrow { display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 12px; }
.pill {
  font-size: 10px; padding: 2px 7px; border-radius: 3px;
  background: var(--gold-soft); color: var(--ink);
  font-weight: 700; letter-spacing: 0.04em;
}
.pill.temp.hot   { background: rgba(197,68,58,0.18); color: var(--hot); }
.pill.temp.warm  { background: rgba(181,122,26,0.18); color: var(--warm); }
.pill.temp.cold  { background: rgba(26,40,64,0.10);  color: var(--ink-soft); }

.card .specs {
  display: flex; gap: 14px; font-size: 13px;
  color: var(--ink-soft); margin-bottom: 12px;
}
.card .specs span { display: inline-flex; align-items: center; gap: 4px; }
.card .specs strong { color: var(--ink); font-weight: 700; }

/* Financial strip */
.fin-grid {
  display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px;
  margin-bottom: 14px; padding: 10px 12px;
  background: var(--bg); border: 1px solid var(--rule); border-radius: 3px;
}
.fin-grid .fin .lab {
  font-size: 9px; color: var(--ink-mute); text-transform: uppercase;
  letter-spacing: 0.10em; font-weight: 800;
}
.fin-grid .fin .val {
  font-family: "Iowan Old Style", Georgia, serif;
  font-size: 17px; font-weight: 600; color: var(--ink); line-height: 1.1;
}
.fin-grid .fin .val.positive { color: var(--green); }
.fin-grid .fin .val.negative { color: var(--hot); }

/* Call rating row */
.rating-row { display: flex; align-items: center; gap: 12px; margin-bottom: 10px; }
.rating-row > div:first-child { display: flex; flex-direction: column; gap: 4px; }
.rating-row .label {
  font-size: 9px; color: var(--ink-mute); text-transform: uppercase;
  letter-spacing: 0.10em; font-weight: 800;
}
.rating {
  font-family: "Iowan Old Style", Georgia, serif;
  font-size: 16px; font-weight: 700; padding: 3px 10px; border-radius: 3px;
  display: inline-block;
}
.rating.green { background: rgba(47,125,91,0.15); color: var(--green); }
.rating.warm  { background: rgba(181,122,26,0.15); color: var(--warm); }
.rating.hot   { background: rgba(197,68,58,0.15); color: var(--hot); }
.rating.gray  { background: rgba(26,40,64,0.08);  color: var(--ink-mute); }
.rating-row .summary {
  font-family: "Iowan Old Style", Georgia, serif; font-style: italic;
  font-size: 13px; color: var(--ink-soft); flex: 1; line-height: 1.5;
  border-left: 2px solid var(--rule); padding-left: 10px;
}

.deal-meta { font-size: 12px; color: var(--ink-soft); margin-bottom: 12px; line-height: 1.6; }
.deal-meta strong { color: var(--ink); font-weight: 700; }

.card .actions { display: flex; gap: 6px; margin-top: auto; flex-wrap: wrap; }
.btn {
  padding: 7px 12px; border-radius: 3px;
  font-size: 10px; font-weight: 800;
  letter-spacing: 0.06em; text-transform: uppercase;
  text-decoration: none; transition: all .15s; flex: 1; text-align: center;
  white-space: nowrap;
}
.btn.primary {
  background: var(--ink); color: var(--gold); border: 1px solid var(--ink);
}
.btn.primary:hover { background: var(--gold-deep); color: var(--ink); }
.btn.secondary {
  background: transparent; border: 1px solid var(--rule); color: var(--ink-soft);
}
.btn.secondary:hover { border-color: var(--ink); color: var(--ink); }
.btn.ghost {
  background: transparent; color: var(--ink-mute);
  border: 1px dashed var(--rule); cursor: not-allowed;
}

.empty {
  text-align: center; color: var(--ink-mute); padding: 40px;
  background: var(--paper); border: 1px solid var(--rule);
  border-radius: 4px; font-style: italic;
}

footer {
  color: var(--ink-mute); font-size: 11px; text-align: center;
  margin-top: 64px; padding-top: 18px;
  border-top: 1px solid var(--rule); letter-spacing: 0.04em;
}
a { color: var(--gold-deep); }
a:hover { color: var(--ink); }

/* ── Animations & micro-interactions ─────────────────────── */
@keyframes fade-in-up {
  from { opacity: 0; transform: translateY(10px); }
  to   { opacity: 1; transform: translateY(0); }
}
@keyframes fade-in {
  from { opacity: 0; }
  to   { opacity: 1; }
}
.fade-in-up { animation: fade-in-up 0.4s cubic-bezier(0.22, 1, 0.36, 1) backwards; }
.fade-in    { animation: fade-in 0.4s ease-out backwards; }

/* Smooth filter transitions */
.card, .lead-line, .stat, .bucket, .chart-card, .roadmap .item, .cron-card {
  transition: opacity 0.25s ease, transform 0.25s cubic-bezier(0.22, 1, 0.36, 1);
}
.card.filtered-out, .lead-line.filtered-out {
  opacity: 0; transform: scale(0.96) translateY(-4px); pointer-events: none;
}

/* Hover lift for clickable cards */
.card { will-change: transform; }
.card:hover {
  transform: translateY(-3px);
  box-shadow: 0 8px 24px rgba(26,40,64,0.08);
}
.bucket .lead-line { transition: background 0.15s ease, transform 0.15s ease; }
.bucket .lead-line:hover { transform: translateX(2px); }

/* Filter chip + button micro-interactions */
.filter-chip, .nav a, .btn, .ghl-link {
  transition: all 0.18s cubic-bezier(0.22, 1, 0.36, 1);
}
.filter-chip:active { transform: scale(0.96); }
.btn:active, .ghl-link:active { transform: scale(0.97); }

/* Stat KPI value entrance */
.stat .v { transition: color 0.3s ease; }

/* Smooth section reveal (set via JS-applied stagger) */
.bucket, .stat, .card { animation-fill-mode: backwards; }

</style>
</head>
<body>
<div class="container">

  <div class="meta-bar">
    <div>APG · ACQ Operating Layer · Deals</div>
    <div>Last updated __TIMESTAMP__</div>
  </div>

  <div class="logo-row">
    <img class="logo-svg" src="logo.png"
         onerror="this.onerror=null; this.src='logo.svg';"
         alt="Atom Property Group" />
    <div class="nav">
      <a href="index.html">Follow-Ups</a>
      <a href="deals.html" class="active">Deals</a>
      <a href="weekly.html">Weekly</a>
      <a href="about.html">About</a>
    </div>
  </div>

  <header class="doc-header">
    <h1>Active <span class="accent">Deals</span></h1>
    <p class="lede">Every property currently in stages 1-4 of the ACQ pipeline. Sorted by call rating within each stage so the strongest deals surface first. Use the filter bar to narrow.</p>
    <hr>
  </header>

  <section class="sec">
    <div class="tag-row"><span class="num">01</span><h2>At a Glance</h2></div>
    <hr>
    <div class="stat-row">
      <div class="stat"><div class="lab">Total Deals</div><div class="v">__TOTAL__</div><div class="sub">stages 1-4 (active)</div></div>
      <div class="stat green"><div class="lab">Total Assignment Fee</div><div class="v">__ASSIGN_FEE_TOTAL__</div><div class="sub">across __WITH_FEE__ deals with fee set</div></div>
      <div class="stat"><div class="lab">Avg Call Rating</div><div class="v">__AVG_RATING__/10</div><div class="sub">across __RATED__ rated calls</div></div>
      <div class="stat warm"><div class="lab">With ARV calculated</div><div class="v">__WITH_ARV__</div><div class="sub">/ __TOTAL__ deals</div></div>
      <div class="stat hot"><div class="lab">Hot Leads</div><div class="v">__HOT__</div><div class="sub">flagged by call AI</div></div>
    </div>
  </section>

  <section class="sec">
    <div class="tag-row"><span class="num">02</span><h2>Filter</h2></div>
    <hr>
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
  </section>

  __SECTIONS__

  <footer>Auto-refreshed every 30 minutes by the GitHub Actions cron · APG ACQ Operating Layer</footer>
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
    // Animated hide/show — toggle class for the fade transition, then commit
    // display after the transition so layout reflows cleanly.
    if (visible) {
      if (c.style.display === 'none') c.style.display = '';
      void c.offsetHeight;  // force reflow so transition runs
      c.classList.remove('filtered-out');
    } else {
      c.classList.add('filtered-out');
      setTimeout(() => {
        if (c.classList.contains('filtered-out')) c.style.display = 'none';
      }, 250);
    }
  });
  // Hide empty sections (after the fade settles)
  setTimeout(() => {
    document.querySelectorAll('.deals-section').forEach(s => {
      const visibleCards = s.querySelectorAll('.card:not(.filtered-out):not([style*="display: none"])');
      s.style.display = visibleCards.length ? '' : 'none';
    });
  }, 260);
}

search.addEventListener('input', applyFilters);
chips.forEach(c => c.addEventListener('click', e => {
  chips.forEach(x => x.classList.remove('active'));
  c.classList.add('active');
  activeFilter = c.dataset.filter;
  applyFilters();
}));

// ── Animation runtime (count-up + stagger) ──────────────────
function animateNumber(el, durationMs) {
  durationMs = durationMs || 700;
  const original = el.textContent.trim();
  const m = original.match(/-?\d+(?:\.\d+)?/);
  if (!m) return;
  const target = parseFloat(m[0]);
  const decimals = (m[0].split('.')[1] || '').length;
  if (target === 0) return;  // nothing to animate
  const start = performance.now();
  function tick(now) {
    const p = Math.min(1, (now - start) / durationMs);
    const eased = 1 - Math.pow(1 - p, 3);
    const current = (target * eased).toFixed(decimals);
    el.textContent = original.replace(/-?\d+(?:\.\d+)?/, current);
    if (p < 1) requestAnimationFrame(tick);
    else el.textContent = original;
  }
  requestAnimationFrame(tick);
}

function staggerIn(selector, baseDelay) {
  baseDelay = baseDelay || 0;
  document.querySelectorAll(selector).forEach((el, i) => {
    el.style.animationDelay = (baseDelay + i * 35) + 'ms';
    el.classList.add('fade-in-up');
  });
}

function runEntranceAnimations(root) {
  root = root || document;
  root.querySelectorAll('.stat .v').forEach(el => animateNumber(el));
  // Stagger top-level grid children within key containers
  ['.stat-row', '.charts-grid', '.kpi-row'].forEach(sel => {
    root.querySelectorAll(sel).forEach(parent => {
      Array.from(parent.children).forEach((el, i) => {
        el.style.animationDelay = (i * 50) + 'ms';
        el.classList.add('fade-in-up');
      });
    });
  });
  ['.bucket', '.cron-card', '.roadmap .item', '.grid > .card'].forEach(sel => {
    root.querySelectorAll(sel).forEach((el, i) => {
      el.style.animationDelay = (i * 30) + 'ms';
      el.classList.add('fade-in-up');
    });
  });
}

// Run once on initial load. For pages that re-render content (weekly,
// follow-ups), they should call this again after replacing the DOM.
document.addEventListener('DOMContentLoaded', () => runEntranceAnimations());
window.runEntranceAnimations = runEntranceAnimations;  // expose for re-render

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

    motiv  = cf.get(CF_MOTIVATION) or ''
    timeln = cf.get(CF_TIMELINE) or ''
    reason = cf.get(CF_REASON_SELL) or ''
    deal_t = cf.get(CF_DEAL_TYPE) or of.get(OF_DEAL_TYPE) or ''
    repairs = cf.get(CF_REPAIRS) or ''
    rehab_url = of.get(OF_REHAB) or ''
    # Always have a Zillow link: prefer the saved /homedetails/ URL from Apify,
    # fall back to a search URL built from the address parts.
    zillow = cf.get(CF_ZILLOW) or zillow_search_url(addr1, city, state, zipc)
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

    pieces = []
    pieces.append(f'<div class="card {stage_class(d["stage"])}" data-stage="{escape(d["stage_label"])}" '
                  f'data-flags="{escape(" ".join(flags))}" '
                  f'data-search="{escape(search_blob)}">')

    pieces.append('<div class="top">')
    pieces.append(f'<div><div class="name">{escape(name)}</div>'
                  f'<div class="addr">{escape(addr1 or "(no address on file)")}</div>'
                  f'<div class="place">{escape(place)}</div></div>')
    pieces.append(f'<div class="stage-pill {stage_class(d["stage"])}">{escape(d["stage_label"])}</div>')
    pieces.append('</div>')

    pill_pieces = []
    if temp:
        pill_pieces.append(f'<span class="pill {temp_class(temp)}">🌡 {escape(temp)}</span>')
    if last_updated:
        pill_pieces.append(f'<span class="pill">⏱ Updated {escape(days_ago(last_updated))}</span>')
    if deal_t:
        pill_pieces.append(f'<span class="pill">{escape(deal_t)}</span>')
    if pill_pieces:
        pieces.append(f'<div class="pillrow">{"".join(pill_pieces)}</div>')

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
    pieces.append('<div class="fin-grid">')
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
    pieces.append(f'<div><span class="label">Call</span>{rating_html}</div>')
    if summary:
        pieces.append(f'<div class="summary">{escape(summary[:240])}</div>')
    else:
        pieces.append('<div class="summary">No call analysis yet.</div>')
    pieces.append('</div>')

    meta_lines = []
    if motiv:   meta_lines.append(f'<strong>Motivation:</strong> {escape(motiv)}')
    if timeln:  meta_lines.append(f'<strong>Timeline:</strong> {escape(timeln)}')
    if reason:  meta_lines.append(f'<strong>Reason:</strong> {escape(reason[:180])}')
    if repairs: meta_lines.append(f'<strong>Repairs:</strong> {escape(repairs[:180])}')
    if meta_lines:
        pieces.append(f'<div class="deal-meta">{"<br>".join(meta_lines)}</div>')

    pieces.append('<div class="actions">')
    pieces.append(f'<a class="btn primary" href="{escape(ghl_link)}" target="_blank">Open in GHL</a>')
    if rehab_url:
        pieces.append(f'<a class="btn secondary" href="{escape(rehab_url)}" target="_blank">Rehab Report</a>')
    else:
        pieces.append('<span class="btn ghost">No rehab yet</span>')
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
    section_nums = {STAGE_MAO: '03', STAGE_RR: '04', STAGE_LAO: '05', STAGE_QUALIFIED: '06'}
    sections_html = []
    for stage_id in order:
        in_stage = [d for d in enriched if d['stage'] == stage_id]
        if not in_stage:
            continue
        in_stage.sort(key=lambda x: (
            -1 if x['note'].get('rating') is None else -x['note']['rating'],
            x.get('updated', '')
        ))
        label = in_stage[0]['stage_label']
        sec_num = section_nums.get(stage_id, '·')
        sections_html.append(
            f'<section class="sec deals-section">'
            f'<div class="tag-row"><span class="num">{sec_num}</span>'
            f'<h2>{escape(label)} <span class="count">({len(in_stage)})</span></h2></div>'
            f'<hr>'
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
