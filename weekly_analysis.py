"""
Weekly Analysis — runs every Friday 10 AM ET via .github/workflows/weekly.yml.

For every lead in the ACQ pipeline, snapshots the current state and compares
against last Friday's snapshot. Categorizes each lead into action buckets:

  advanced          — moved forward (e.g. Qualified → LAO → DD → MAO → Contract)
  demoted           — moved backward (e.g. routed to Unqualified, Dead Deals)
  new               — first appearance in the pipeline this week
  stagnant_active   — same stage as last week but had SMS / reply / call activity
  stagnant_inactive — same stage, no activity at all (the worst bucket)
  ready_contract    — in MAO with Hot temp + ARV + positive 70%-MAO spread
  ready_mao         — in DD with Hot temp + ARV calculated
  drop_suggest      — Cold/Nurture sitting in active stages > 30 days

Outputs:
  weekly/{YYYY-W##}.json   — full analysis for the week
  weekly/_state.json       — current snapshot for next week's diff
  weekly/index.json        — list of available weeks (newest first)

Display: site/weekly.html (static page) loads these JSON files via fetch().
"""
import json, os, re, time, requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo('America/New_York')

GHL_TOKEN    = os.environ['GHL_TOKEN']
GHL_LOCATION = 'RCkiUmWqXX4BYQ39JXmm'
PIPELINE_ID  = 'O8wzIa6E3SgD8HLg6gh9'
WEEKLY_DIR   = 'weekly'

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


# Stage position map: higher = further along the deal lifecycle.
# Reactivation stages get low positions so transitions into them count as
# "demoted." Dead Deals is lowest. Agents is excluded.
STAGES = {
    'c1d23905-7096-439c-9a31-f8db5b2b53d0': ('0. Unqualified Leads',           0),
    'a17517be-8d1a-49fd-bd53-b9128a66e242': ('1. Qualified Leads (Warm/Hot)',  1),
    'd43fddd8-3a17-46b2-a193-cf18619f654f': ('2. Prequalified Offer (LAO)',    2),
    '23a159ad-ba39-4c74-9d07-c1beb219d9f2': ('3. Due Diligence (RR)',          3),
    '43589167-14f0-4e09-ba2a-8b9bd3296a4a': ('4. Negotiate (MAO)',             4),
    '53eb29e2-92d9-439e-8865-a875a46a6fd8': ('5. Contract Sent',               5),
    'e377ba40-6d3b-4981-86cb-d31e7ef0c9c1': ('6. Executed PSA',                6),
    'aefeb703-5ab9-403c-b2eb-47fe550d62ee': ('7. Disposition',                 7),
    '4aa78ab3-85dc-46d1-a683-d97b0c7a23ee': ('Follow Up (1.5 month)',          0.5),
    '571c115e-2603-4f3f-8546-d716f44ba8ef': ('Follow Up (3 months)',           0.3),
    'b9b560b0-30cb-47fc-a4ca-1e55ca2531e2': ('Dead Deals',                    -2),
}
STAGE_LAO  = 'd43fddd8-3a17-46b2-a193-cf18619f654f'
STAGE_RR   = '23a159ad-ba39-4c74-9d07-c1beb219d9f2'
STAGE_MAO  = '43589167-14f0-4e09-ba2a-8b9bd3296a4a'
STAGE_QUAL = 'a17517be-8d1a-49fd-bd53-b9128a66e242'
ACTIVE_STAGES = {STAGE_QUAL, STAGE_LAO, STAGE_RR, STAGE_MAO}

# Custom field IDs (mirror acq_automation.py)
CF_BED         = 'xXEm77wvbxEbiqsw3lAz'
CF_BATH        = 'EtKof5yT7KAWmoaNQqJZ'
CF_SQFT        = '8kqwjqtJyTTeQ8SIaLQz'
CF_ASK_PRICE   = '6q7syt4puxfP7E03Xxhd'
CF_ARV         = 'nCWzIGfZHki0dv84gUem'
CF_70_ARV      = 'R7QUzOdOnJXgoGRPwxdF'
CF_MOTIVATION  = 'rbYZAdhvuvX1NQgexhxy'
CF_TIMELINE    = 'v47I1Mi63RBpCD5N5RrH'
CF_VA_NOTES    = 'ctNVXVw8VY1PD4B1oqXj'  # Last-call summary written by acq cron

NOTE_RATING_RE = re.compile(r'Rating:\s*(\d+)\s*/\s*10', re.IGNORECASE)
NOTE_TEMP_RE   = re.compile(r'Lead Temp:\s*([A-Za-z]+)', re.IGNORECASE)


def week_id(dt):
    """ISO week id like '2026-W18'."""
    y, w, _ = dt.isocalendar()
    return f'{y}-W{w:02d}'


def now_et():
    return datetime.now(ET)


def now_utc():
    return datetime.now(timezone.utc)


def days_between(iso_a, iso_b=None):
    if not iso_a: return None
    try:
        a = datetime.fromisoformat(str(iso_a).replace('Z', '+00:00'))
        b = datetime.fromisoformat(str(iso_b).replace('Z', '+00:00')) if iso_b else now_utc()
        return (b - a).days
    except Exception:
        return None


def fetch_all_leads():
    """Pull every opportunity in the pipeline that we care about (positions ≥ -2)."""
    out = []
    for stage_id in STAGES:
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
                    'cid':       o['contactId'],
                    'oid':       o['id'],
                    'stage_id':  stage_id,
                    'updated':   o.get('updatedAt', ''),
                    'embedded_contact': c,
                })
            if len(opps) < 100: break
            page += 1
            time.sleep(0.1)
    return out


def get_contact(cid):
    r = http('GET', f'https://services.leadconnectorhq.com/contacts/{cid}', headers=GHL_H)
    if r.status_code != 200:
        return {}
    return r.json().get('contact', {})


def fetch_summary_note(cid):
    """Pull the 'APG Lead Summary' note and parse rating + temp."""
    try:
        r = http('GET', f'https://services.leadconnectorhq.com/contacts/{cid}/notes', headers=GHL_H)
        if r.status_code != 200:
            return {}
        for n in r.json().get('notes', []):
            body = n.get('body') or ''
            if body.startswith('APG Lead Summary'):
                rating = None; temp = ''
                m = NOTE_RATING_RE.search(body)
                if m: rating = int(m.group(1))
                m = NOTE_TEMP_RE.search(body)
                if m: temp = m.group(1).strip().lower()
                return {'rating': rating, 'temp': temp,
                        'updated_at': n.get('dateAdded') or n.get('createdAt') or ''}
    except Exception:
        pass
    return {}


SLACK_NOTE_HEAD_RE      = re.compile(r'#(\S+)\s+by\s+<@([^>]+)>\s+—\s+(.+?)(?:\n|$)')
SLACK_PERMALINK_RE      = re.compile(r'Slack:\s*(https?://\S+)')
SLACK_ORIGINAL_RE       = re.compile(r'Original:\s*"(.+?)"', re.DOTALL)
SLACK_SUMMARY_RE        = re.compile(r'Summary:\s*(.+?)(?:\n\n|\nFields auto-updated|$)', re.DOTALL)


def parse_slack_note(body):
    """Parse a 'Slack mention' note body. Returns dict with channel, user, ts,
    permalink, original, summary."""
    out = {'channel': '', 'user': '', 'ts_text': '',
           'permalink': '', 'original': '', 'summary': ''}
    m = SLACK_NOTE_HEAD_RE.search(body)
    if m:
        out['channel'] = m.group(1)
        out['user']    = m.group(2)
        out['ts_text'] = m.group(3).strip()
    m = SLACK_PERMALINK_RE.search(body)
    if m: out['permalink'] = m.group(1).strip()
    m = SLACK_ORIGINAL_RE.search(body)
    if m: out['original'] = m.group(1).strip().replace('\n', ' ')[:280]
    m = SLACK_SUMMARY_RE.search(body)
    if m: out['summary'] = m.group(1).strip().replace('\n', ' ')[:280]
    return out


def fetch_slack_mentions(cid):
    """Return list of Slack mention notes added to this contact, sorted newest first.
    Each entry: {note_id, added_at, channel, user, permalink, original, summary}."""
    out = []
    try:
        r = http('GET', f'https://services.leadconnectorhq.com/contacts/{cid}/notes', headers=GHL_H)
        if r.status_code != 200:
            return out
        for n in r.json().get('notes', []):
            body = n.get('body') or ''
            if not body.startswith('Slack mention'):
                continue
            parsed = parse_slack_note(body)
            parsed['note_id']  = n.get('id', '')
            parsed['added_at'] = n.get('dateAdded') or n.get('createdAt') or ''
            out.append(parsed)
    except Exception:
        pass
    out.sort(key=lambda x: x.get('added_at',''), reverse=True)
    return out


def load_state():
    path = os.path.join(WEEKLY_DIR, '_state.json')
    if not os.path.exists(path):
        return {}
    try:
        return json.load(open(path)) or {}
    except Exception:
        return {}


def load_sms_state():
    if not os.path.exists('sms_state.json'):
        return {}
    try:
        return json.load(open('sms_state.json'))
    except Exception:
        return {}


def save_json(path, data):
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, sort_keys=False)


def to_int(v):
    try:    return int(v)
    except Exception:
        try: return int(float(v))
        except Exception: return None


def build_lead_record(lead, sms_state):
    """Compose the per-lead snapshot dict."""
    c = get_contact(lead['cid']) or lead['embedded_contact']
    cf = {f['id']: (f.get('value') or '') for f in (c.get('customFields') or [])}
    note = fetch_summary_note(lead['cid'])
    sms = sms_state.get(lead['cid'], {}) or {}

    stage_label, stage_pos = STAGES.get(lead['stage_id'], ('Unknown', -99))

    name = f"{c.get('firstName','')} {c.get('lastName','')}".strip() or '(no name)'
    addr = (c.get('address1') or '').strip()
    place_parts = [c.get('city',''), c.get('state',''), (c.get('postalCode') or '').split('-')[0]]
    place = ', '.join(p for p in place_parts if p)

    asking = to_int(cf.get(CF_ASK_PRICE))
    arv    = to_int(cf.get(CF_ARV))
    arv70  = to_int(cf.get(CF_70_ARV))
    if not arv70 and arv:
        arv70 = int(arv * 0.7)
    spread = (arv70 - asking) if (arv70 is not None and asking is not None) else None

    # Phone — strip to digits then format as +1 (XXX) XXX-XXXX for display
    raw_phone = (c.get('phone') or '').strip()
    digits = re.sub(r'\D', '', raw_phone)
    if len(digits) == 10:
        phone_display = f'({digits[0:3]}) {digits[3:6]}-{digits[6:]}'
        phone_e164    = f'+1{digits}'
    elif len(digits) == 11 and digits.startswith('1'):
        phone_display = f'+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}'
        phone_e164    = f'+{digits}'
    else:
        phone_display = raw_phone
        phone_e164    = raw_phone

    return {
        'cid':         lead['cid'],
        'oid':         lead['oid'],
        'name':        name,
        'addr':        addr,
        'place':       place,
        'state':       (c.get('state') or '').strip().upper(),
        'stage_id':    lead['stage_id'],
        'stage_label': stage_label,
        'stage_pos':   stage_pos,
        'asking':      asking,
        'arv':         arv,
        'mao':         arv70,
        'spread':      spread,
        'rating':      note.get('rating'),
        'temp':        (note.get('temp') or '').lower(),
        'motivation':  cf.get(CF_MOTIVATION) or '',
        'timeline':    cf.get(CF_TIMELINE) or '',
        'last_sms_at': sms.get('last_sms_at') or '',
        'sms_count':   sms.get('sms_count') or 0,
        'replied':     bool(sms.get('replied')),
        'replied_at':  sms.get('replied_at') or '',
        'reply_text':  (sms.get('reply_text') or '')[:400],
        'reply_class': sms.get('reply_class') or '',
        'last_from_number': sms.get('last_from_number') or '',
        'dormant':     bool(sms.get('dormant')),
        'dnd':         bool(sms.get('dnd')),
        'phone':       phone_display,
        'phone_e164':  phone_e164,
        'last_call_summary': (cf.get(CF_VA_NOTES) or '')[:500],
        'last_updated':       lead.get('updated', ''),
        'contact_updated_at': c.get('dateUpdated') or c.get('updatedAt') or '',
    }


def count_recent_notes(cid, since_iso):
    """Count HUMAN-added notes (manual entries by Adam/Jeff/Mike post-call)
    in the past 7 days. Excludes auto-generated 'APG Lead Summary' upserts
    and 'Slack mention' bodies — those are written by the cron, not by a
    person paying attention to the lead."""
    try:
        r = http('GET', f'https://services.leadconnectorhq.com/contacts/{cid}/notes', headers=GHL_H)
        if r.status_code != 200:
            return 0
        n = 0
        for note in r.json().get('notes', []):
            ts = note.get('dateAdded') or note.get('createdAt') or ''
            if not ts or ts < since_iso:
                continue
            body = (note.get('body') or '')[:60]
            # Skip auto-generated note types
            if body.startswith('APG Lead Summary'):
                continue
            if body.startswith('Slack mention'):
                continue
            n += 1
        return n
    except Exception:
        return 0


def categorize(curr, prev_map, week_start_iso, slack_mention_count, recent_notes_count):
    """Bucket a single lead based on this-week vs last-week diff.

    Weekly view focuses on what's CHANGED or had ACTIVITY this week.
    Activity = SMS sent / reply received / Slack mention captured / contact
    or opp updated in GHL / new note added (any note, including manual
    notes Adam or Jeff add after a meeting or call).
    """
    cid = curr['cid']
    prev = prev_map.get(cid)

    # Activity = HUMAN signals only. opp/contact updatedAt are excluded
    # from had_activity because our own backfill scripts (ARV, rehab,
    # auto-routing) touch every lead's fields, which would mark the whole
    # pipeline 'active'. Notes are filtered to exclude auto-generated
    # APG Lead Summary / Slack mention bodies (see count_recent_notes).
    last_sms_at  = curr.get('last_sms_at') or ''
    replied_at   = curr.get('replied_at') or ''
    sms_this_week    = bool(last_sms_at  and last_sms_at  >= week_start_iso)
    reply_this_week  = bool(replied_at   and replied_at   >= week_start_iso)
    slack_this_week  = slack_mention_count > 0
    notes_this_week  = recent_notes_count > 0
    # Kept for inline-detail display but NOT counted in had_activity
    last_updated = curr.get('last_updated') or ''
    contact_updated = curr.get('contact_updated_at') or ''
    opp_update_week  = bool(last_updated and last_updated >= week_start_iso)
    contact_update_week = bool(contact_updated and contact_updated >= week_start_iso)
    had_activity = sms_this_week or reply_this_week or slack_this_week or notes_this_week

    # Movement bucket
    if not prev:
        movement = 'new'
        movement_meta = {}
    elif curr['stage_pos'] > prev.get('stage_pos', -99):
        movement = 'advanced'
        movement_meta = {'from': prev.get('stage_label',''), 'to': curr['stage_label']}
    elif curr['stage_pos'] < prev.get('stage_pos', -99):
        movement = 'demoted'
        movement_meta = {'from': prev.get('stage_label',''), 'to': curr['stage_label']}
    else:
        # Same stage — split by activity.
        movement = 'active_no_move' if had_activity else 'quiet'
        movement_meta = {'stage': curr['stage_label'],
                         'days_in_stage': days_between(prev.get('first_seen_at') or week_start_iso)}

    # Action recommendations (overlay; a lead can be in movement bucket AND action bucket)
    action_tags = []
    if (curr['stage_id'] == STAGE_MAO and curr['temp'] == 'hot'
            and curr['arv'] and curr['spread'] is not None and curr['spread'] > 0):
        action_tags.append('ready_contract')
    if (curr['stage_id'] == STAGE_RR and curr['temp'] == 'hot' and curr['arv']):
        action_tags.append('ready_mao')
    if (curr['stage_id'] in ACTIVE_STAGES
            and curr['temp'] in ('cold', 'nurture')
            and movement == 'quiet'):
        action_tags.append('drop_suggest')

    # Activity flags exposed for the inline-expand detail
    curr['_activity'] = {
        'sms_this_week':       sms_this_week,
        'reply_this_week':     reply_this_week,
        'slack_this_week':     slack_this_week,
        'slack_count':         slack_mention_count,
        'opp_update_week':     opp_update_week,
        'contact_update_week': contact_update_week,
        'notes_this_week':     recent_notes_count,
        'had_activity':        had_activity,
    }

    return movement, movement_meta, action_tags


def write_html_shell():
    """The static weekly.html page that consumes the JSON outputs.
    Idempotent — only write once unless the structure changes."""
    # Always rewrite so the latest layout always wins.
    html = WEEKLY_HTML
    out = os.path.join('site', 'weekly.html')
    os.makedirs('site', exist_ok=True)
    with open(out, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'  Wrote {out}')


WEEKLY_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Atom Property Group — Weekly Docket</title>
<style>
:root {
  --ink: #0A1F44;
  --ink-deep: #061331;
  --ink-soft: #1A3A7A;
  --ink-mist: #2E4A82;
  --gold: #F5C518;
  --gold-soft: #FFE58A;
  --gold-wash: #FFF6D0;
  --cream: #FAF7EC;
  --cream-deep: #F3EED8;
  --paper: #FFFFFF;
  --rule: #C9C2A8;
  --rule-soft: #E5E0C8;
  --muted: #5A6786;
  --muted-soft: #8A93AA;
  --text: #101827;
  --s-uc:   #B91C1C;
  --s-live: #10B981;
  --s-warm: #EA580C;
  --s-hold: #EAB308;
  --s-dead: #6B625A;
}
* { box-sizing: border-box; }
html, body {
  margin: 0; padding: 0; background: var(--cream); color: var(--text);
  font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
  font-size: 16px; line-height: 1.6; -webkit-font-smoothing: antialiased;
  scroll-behavior: smooth;
}
.shell {
  max-width: 1240px; margin: 0 auto;
  padding: 40px 64px 120px;
  background: var(--paper); min-height: 100vh;
}
@media (max-width: 820px) { .shell { padding: 32px 24px 80px; } }

/* ── MASTHEAD ──────────────────────────────────────── */
.masthead {
  border-top: 5px solid var(--ink);
  border-bottom: 1px solid var(--rule);
  padding: 28px 0 24px; margin-bottom: 24px; position: relative;
}
.masthead::before {
  content: ""; position: absolute; left: 0; top: 0;
  width: 160px; height: 5px; background: var(--gold);
}
.brandrow {
  display: flex; justify-content: space-between; align-items: baseline;
  font-size: 11px; letter-spacing: 0.22em; text-transform: uppercase;
  color: var(--muted); margin-bottom: 16px; flex-wrap: wrap; gap: 8px;
}
.brand { color: var(--ink); font-weight: 700; }
h1 {
  font-family: Georgia, "Times New Roman", serif;
  font-size: 54px; line-height: 1.04; letter-spacing: -0.015em;
  margin: 0 0 14px; color: var(--ink); font-weight: 700;
}
h1 .accent { color: var(--gold); font-style: italic; }
.dek {
  font-family: Georgia, serif; font-style: italic; font-size: 18px;
  line-height: 1.5; color: var(--ink-soft); max-width: 780px; margin: 10px 0 0;
}

/* ── NAV BAR (sticky) ──────────────────────────────── */
.topnav {
  position: sticky; top: 0; z-index: 50;
  background: rgba(250, 247, 236, 0.96); backdrop-filter: blur(6px);
  border-bottom: 1px solid var(--rule);
  margin: 0 -64px 28px; padding: 10px 64px;
  font-size: 11px; letter-spacing: 0.14em; text-transform: uppercase;
  color: var(--muted);
  display: flex; gap: 18px; overflow-x: auto; white-space: nowrap;
  align-items: center;
}
@media (max-width: 820px) { .topnav { margin: 0 -24px 24px; padding: 10px 24px; } }
.topnav a {
  color: var(--ink-soft); text-decoration: none; font-weight: 700; padding: 4px 0;
  border-bottom: 2px solid transparent; transition: border-color .15s, color .15s;
}
.topnav a:hover, .topnav a.active { color: var(--ink); border-bottom: 2px solid var(--gold); }

.topnav .spacer { flex: 1; }
.topnav .week-pick { display: inline-flex; align-items: center; gap: 8px; }
.topnav .week-pick select {
  padding: 4px 10px; border: 1px solid var(--rule); background: var(--paper);
  color: var(--ink); font-size: 11px; font-weight: 700; letter-spacing: 0.08em;
  text-transform: uppercase; font-family: inherit; cursor: pointer;
}
.topnav .refresh-btn {
  padding: 5px 12px; background: var(--ink); color: var(--gold);
  border: 1px solid var(--ink); cursor: pointer;
  font-size: 11px; font-weight: 700; letter-spacing: 0.10em;
  text-transform: uppercase; transition: all 0.12s;
  display: inline-flex; align-items: center; gap: 6px;
}
.topnav .refresh-btn:hover { background: var(--gold); color: var(--ink); }
.topnav .refresh-btn:disabled { opacity: 0.5; cursor: not-allowed; }
.topnav .refresh-btn .spin {
  display: inline-block; width: 10px; height: 10px;
  border: 2px solid currentColor; border-right-color: transparent;
  border-radius: 50%; animation: spin 0.7s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }

.refresh-status { font-size: 11px; color: var(--muted); margin-left: 8px; }

/* ── SECTION HEADINGS ──────────────────────────────── */
section { margin: 48px 0; }
h2 {
  font-family: Georgia, serif; font-size: 26px; color: var(--ink);
  margin: 0 0 16px; padding-bottom: 10px; border-bottom: 2px solid var(--ink);
  letter-spacing: -0.01em; display: flex; align-items: center; gap: 14px;
  flex-wrap: wrap;
}
h2 .num {
  display: inline-block; min-width: 34px; padding: 4px 10px;
  background: var(--gold); color: var(--ink);
  font-family: "Helvetica Neue", Arial, sans-serif;
  font-size: 12px; font-weight: 700; text-align: center; letter-spacing: 0.04em;
}
h2 .sec-count {
  margin-left: auto;
  font-family: "Helvetica Neue", Arial, sans-serif;
  font-size: 11px; letter-spacing: 0.18em; text-transform: uppercase;
  color: var(--muted); font-weight: 700;
}

.lede {
  font-family: Georgia, serif; font-size: 19px; line-height: 1.6;
  color: #1a2540; border-left: 4px solid var(--gold); padding: 6px 0 6px 22px;
  margin: 14px 0 4px; max-width: 880px;
}

/* ── KPI CRIT-GRID ─────────────────────────────────── */
.crit-grid {
  display: grid; gap: 14px; grid-template-columns: repeat(6, 1fr);
  margin-top: 24px;
}
@media (max-width: 1100px) { .crit-grid { grid-template-columns: repeat(3, 1fr); } }
@media (max-width: 600px)  { .crit-grid { grid-template-columns: repeat(2, 1fr); } }
.crit {
  background: var(--cream); border-top: 4px solid var(--gold);
  border-bottom: 1px solid var(--rule); padding: 18px 20px 22px;
}
.crit .kicker {
  font-size: 10px; letter-spacing: 0.22em; text-transform: uppercase;
  color: var(--muted); margin-bottom: 6px; font-weight: 700;
}
.crit .num-big {
  font-family: Georgia, serif; font-size: 38px; font-weight: 700;
  color: var(--ink); line-height: 1; margin: 0;
}
.crit .sub {
  font-size: 11px; color: var(--muted); margin-top: 4px; line-height: 1.4;
}
.crit.uc   { border-top-color: var(--s-uc); }
.crit.uc   .num-big { color: var(--s-uc); }
.crit.live { border-top-color: var(--s-live); }
.crit.live .num-big { color: var(--s-live); }
.crit.warm { border-top-color: var(--s-warm); }
.crit.warm .num-big { color: var(--s-warm); }
.crit.dead { border-top-color: var(--muted); }
.crit.dead .num-big { color: var(--muted); }

/* ── DEAL CARDS (inline-expand) ────────────────────── */
.deal-grid { display: grid; gap: 14px; margin-top: 18px; }
.deal {
  background: var(--paper); border: 1px solid var(--rule);
  border-top: 5px solid var(--gold); transition: box-shadow .2s ease;
}
.deal:hover { box-shadow: 0 8px 28px -18px rgba(10,31,68,0.25); }
.deal.uc   { border-top-color: var(--s-uc); }
.deal.live { border-top-color: var(--s-live); }
.deal.warm { border-top-color: var(--s-warm); }
.deal.hold { border-top-color: var(--s-hold); }
.deal.dead { border-top-color: var(--muted); }

.deal-head {
  padding: 16px 22px; display: grid; grid-template-columns: auto 1fr auto;
  gap: 14px; align-items: center; cursor: pointer; user-select: none;
}
@media (max-width: 820px) {
  .deal-head { grid-template-columns: auto 1fr; }
  .deal-head .deal-status { grid-column: 1/-1; }
}
.deal-num {
  font-family: Georgia, serif; font-size: 16px; font-weight: 700;
  color: var(--ink); background: var(--gold); padding: 6px 11px;
  letter-spacing: 0.02em; line-height: 1;
}
.deal.uc   .deal-num { background: var(--s-uc);   color: #fff; }
.deal.live .deal-num { background: var(--s-live); color: #fff; }
.deal.warm .deal-num { background: var(--s-warm); color: #fff; }

.deal-title h3 {
  font-family: Georgia, serif; font-size: 19px; margin: 0 0 4px; color: var(--ink);
  letter-spacing: -0.005em;
}
.deal-title p.tagline {
  font-family: "Helvetica Neue", Arial, sans-serif;
  font-size: 11px; color: var(--muted); margin: 0; line-height: 1.4;
  letter-spacing: 0.06em; text-transform: uppercase; font-weight: 700;
}
.deal-title p.tagline .signal-set { display: inline-flex; gap: 6px; margin-left: 8px; }
.signal-set .sg {
  font-size: 10px; padding: 2px 7px; background: var(--cream-deep);
  color: var(--ink); border-radius: 2px; letter-spacing: 0.06em;
  text-transform: uppercase; font-weight: 700;
}
.signal-set .sg.green { background: rgba(16,185,129,0.18); color: #065F46; }
.signal-set .sg.red   { background: rgba(185,28,28,0.16);  color: #7F1D1D; }
.signal-set .sg.warm  { background: rgba(234,88,12,0.18);  color: #7C2D12; }
.signal-set .sg.hold  { background: rgba(234,179,8,0.20);  color: #713F12; }
.signal-set .sg.gray  { background: rgba(91,103,134,0.18); color: #334155; }

.deal-status {
  background: var(--ink); color: var(--paper); padding: 8px 12px;
  font-size: 11px; line-height: 1.3; border-left: 3px solid var(--gold);
  white-space: nowrap; letter-spacing: 0.08em; text-transform: uppercase;
  font-weight: 700;
}
.deal-status .lbl {
  display: block; font-size: 9.5px; letter-spacing: 0.16em;
  color: var(--gold); margin-bottom: 3px; font-weight: 700;
}
.deal-status.uc { background: var(--s-uc); }
.deal-status.live { background: var(--s-live); }
.deal-status.warm { background: var(--s-warm); }
.deal-status.hold { background: var(--s-hold); color: var(--ink); }
.deal-status.hold .lbl { color: var(--ink); opacity: 0.75; }
.deal-status.dead { background: var(--muted); }

.deal-toggle {
  grid-column: 1/-1; display: flex; justify-content: flex-end; margin-top: 4px;
  font-size: 10px; letter-spacing: 0.18em; color: var(--muted); font-weight: 700;
  text-transform: uppercase;
}
.deal-toggle::after { content: "▸ TAP TO EXPAND"; }
.deal.open .deal-toggle::after { content: "▾ TAP TO COLLAPSE"; color: var(--ink); }

.deal-body {
  padding: 0; max-height: 0; overflow: hidden;
  transition: max-height .45s ease, padding .45s ease;
  border-top: 0;
}
.deal.open .deal-body {
  padding: 22px 26px 28px;
  max-height: 3000px;
  border-top: 1px solid var(--rule);
}
.deal-body-inner {
  display: grid; grid-template-columns: 1fr 1fr; gap: 22px;
}
@media (max-width: 820px) { .deal-body-inner { grid-template-columns: 1fr; } }
.field h4 {
  font-size: 10.5px; letter-spacing: 0.2em; text-transform: uppercase;
  color: var(--ink-soft); margin: 0 0 8px; font-weight: 700;
}
.field p { margin: 0 0 10px; color: #2b3856; font-size: 14px; line-height: 1.55; }
.field p:last-child { margin-bottom: 0; }
.field.full { grid-column: 1/-1; }

.contact {
  background: var(--cream); border: 1px solid var(--rule);
  padding: 10px 14px; margin-top: 8px;
  font-family: "Helvetica Neue", Arial, sans-serif;
  font-size: 13px; line-height: 1.7; color: var(--ink);
}
.contact .c-lbl {
  display: inline-block; width: 18px; color: var(--gold); font-weight: 700;
}
.contact a { color: var(--ink-soft); text-decoration: none; font-weight: 700; }
.contact a:hover { color: var(--ink); text-decoration: underline; }

.num-table {
  display: grid; grid-template-columns: auto 1fr; gap: 6px 16px; font-size: 13px;
}
.num-table .k {
  font-size: 10.5px; letter-spacing: 0.16em; text-transform: uppercase;
  color: var(--muted); font-weight: 700; padding: 6px 0;
  border-top: 1px dashed var(--rule);
}
.num-table .v {
  color: var(--ink); font-weight: 700; padding: 6px 0;
  border-top: 1px dashed var(--rule); font-variant-numeric: tabular-nums;
}
.num-table .v.green { color: var(--s-live); }
.num-table .v.red   { color: var(--s-uc); }
.num-table .k:first-of-type, .num-table .v:first-of-type { border-top: none; }

.callout {
  border-left: 3px solid var(--gold); background: var(--gold-wash);
  padding: 12px 16px; font-size: 13px; color: #24314e; line-height: 1.5;
  margin-top: 10px;
}
.callout.decision { border-left-color: var(--s-uc);  background: #FFF0F0; color: #450A0A; }
.callout.appt     { border-left-color: var(--s-live); background: #ECFDF5; color: #064E3B; }
.callout.revise   { border-left-color: var(--ink);   background: var(--cream-deep); color: var(--ink); }
.callout strong {
  display: block; color: var(--ink); font-size: 10.5px;
  text-transform: uppercase; letter-spacing: 0.18em; margin-bottom: 4px;
  font-weight: 700;
}
.callout.decision strong { color: #7F1D1D; }
.callout.appt strong     { color: #065F46; }
.callout.revise strong   { color: var(--ink); }

/* ── STAGE FILTER CHIPS ────────────────────────────── */
.stage-filter {
  display: flex; gap: 6px; flex-wrap: wrap;
  margin: 14px 0 0; padding: 10px 14px;
  background: var(--cream); border: 1px solid var(--rule);
}
.filter-chip {
  padding: 6px 12px; background: transparent; border: 1px solid var(--rule);
  color: var(--muted); font-size: 10.5px; font-weight: 700;
  letter-spacing: 0.10em; text-transform: uppercase;
  cursor: pointer; user-select: none; transition: all .12s;
}
.filter-chip:hover { color: var(--ink); border-color: var(--ink); }
.filter-chip.active { background: var(--ink); color: var(--gold); border-color: var(--ink); }

/* ── SLACK MENTIONS ────────────────────────────────── */
.slack-card {
  background: var(--gold-wash); border: 1px solid var(--gold-soft);
  border-left: 3px solid var(--gold); padding: 12px 16px; margin-bottom: 8px;
}
.slack-card .slack-meta {
  font-size: 10px; color: var(--muted); margin-bottom: 6px;
  letter-spacing: 0.10em; text-transform: uppercase; font-weight: 700;
  font-family: ui-monospace, monospace;
}
.slack-card .lead-link {
  font-family: Georgia, serif; font-size: 14px; font-weight: 700;
  color: var(--ink); text-decoration: none; margin-right: 8px;
}
.slack-card .lead-link:hover { color: var(--gold); text-decoration: underline; }
.slack-card .original {
  font-family: Georgia, serif; font-style: italic; font-size: 13px;
  color: #2b3856; padding: 4px 0 4px 12px; border-left: 2px solid var(--rule);
  margin: 6px 0;
}
.slack-card .summary { font-size: 12px; color: #475569; }
.slack-card .actions { margin-top: 6px; display: flex; gap: 8px; }
.slack-card .actions a {
  font-size: 10px; padding: 3px 8px; background: var(--ink); color: var(--gold);
  text-decoration: none; font-weight: 700; letter-spacing: 0.08em;
  text-transform: uppercase;
}
.slack-card .actions a:hover { background: var(--gold); color: var(--ink); }

.empty {
  text-align: center; color: var(--muted); padding: 36px;
  font-style: italic; background: var(--cream); border: 1px dashed var(--rule);
}
.loading { text-align: center; color: var(--muted); padding: 60px; font-style: italic; }

a { color: var(--ink-soft); }
a:hover { color: var(--ink); }

.footer {
  margin-top: 56px; padding-top: 20px;
  border-top: 3px double var(--ink);
  display: flex; justify-content: space-between;
  font-size: 11px; letter-spacing: 0.18em; text-transform: uppercase;
  color: var(--muted); font-weight: 700; flex-wrap: wrap; gap: 12px;
}
.footer .gold-stamp {
  display: inline-block; padding: 4px 10px;
  background: var(--gold); color: var(--ink); letter-spacing: 0.14em;
}
</style>
</head>
<body>
<div class="shell">

  <header class="masthead">
    <div class="brandrow">
      <span class="brand">Atom Property Group · ACQ Operations</span>
      <span id="metaWeek">Loading…</span>
    </div>
    <h1>The Weekly <span class="accent">Docket.</span></h1>
    <p class="dek">A snapshot of every lead the team touched this week — what moved, what's ready for action, what went quiet. Click any deal to expand the full context.</p>
  </header>

  <nav class="topnav">
    <a href="index.html">Follow-Ups</a>
    <a href="deals.html">Deals</a>
    <a href="weekly.html" class="active">Weekly</a>
    <a href="about.html">About</a>
    <span class="spacer"></span>
    <span class="week-pick">
      <label for="weekSel">Week:</label>
      <select id="weekSel"><option>Loading…</option></select>
    </span>
    <button class="refresh-btn" id="refreshBtn">
      <span id="refreshIcon">↻</span><span id="refreshLabel">Refresh</span>
    </button>
    <span class="refresh-status" id="refreshStatus"></span>
  </nav>

  <div id="content"><div class="loading">Loading analysis…</div></div>

  <div class="footer">
    <span>Auto-generated each Friday 10 AM ET · APG ACQ Operating Layer</span>
    <span class="gold-stamp">Mido · Ops · Live</span>
  </div>

</div>

<script>
const GHL_BASE = 'https://app.gohighlevel.com/v2/location/RCkiUmWqXX4BYQ39JXmm/contacts/detail';
const ROOT = 'weekly/';
const REPO = 'Mid0117/acq-automation';
const PAT_KEY = 'apg_gh_pat_v1';

let stageFilter = 'all';
let _currentWeekData = null;

function escapeHtml(s) {
  return (s || '').replace(/[&<>"']/g, ch => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  }[ch]));
}

function fmtMoney(v) {
  if (v == null || isNaN(v)) return '—';
  v = Math.round(v);
  if (Math.abs(v) >= 1_000_000) return '$' + (v/1_000_000).toFixed(2) + 'M';
  if (Math.abs(v) >= 1_000)     return '$' + (v/1_000).toFixed(0) + 'k';
  return '$' + v.toLocaleString();
}

function leadMatchesStage(l) {
  if (stageFilter === 'all') return true;
  return (l.stage_label || '').trim() === stageFilter;
}

// ── Bucket definitions: which goes where, what color, what's the heading ──
const BUCKET_DEFS = [
  {key:'ready_contract', title:'Ready for Contract',  status:'uc',   num:'01', desc:'In MAO, Hot, ARV calculated, positive 70%-MAO spread. Send the contract.'},
  {key:'ready_mao',      title:'Ready for MAO Offer', status:'live', num:'02', desc:'In Due Diligence with Hot temp + ARV calculated. Time to put a number on it.'},
  {key:'advanced',       title:'Moved Forward',       status:'live', num:'03', desc:'Stage advanced this week.', showMove: true},
  {key:'new',            title:'New This Week',       status:'warm', num:'04', desc:'First appearance in the pipeline.'},
  {key:'active_no_move', title:'Active — No Stage Move', status:'warm', num:'05', desc:'Same stage, but had SMS / reply / Slack / note activity this week. Keep them warm.'},
  {key:'demoted',        title:'Moved Backward',      status:'dead', num:'06', desc:'Auto-routed to Unqualified or downgraded.', showMove: true},
  {key:'drop_suggest',   title:'Drop Suggestions',    status:'hold', num:'07', desc:'Cold/Nurture sitting in active stages > 30 days.'},
];

function statusForBucket(key) {
  const d = BUCKET_DEFS.find(b => b.key === key);
  return d ? d.status : 'gray';
}

function renderSignalSet(l) {
  const a = l._activity || {};
  const out = [];
  if (a.sms_this_week)   out.push('<span class="sg green">SMS this wk</span>');
  if (a.reply_this_week) out.push('<span class="sg green">Reply</span>');
  if (a.slack_count > 0) out.push('<span class="sg warm">' + a.slack_count + ' Slack</span>');
  if (a.notes_this_week) out.push('<span class="sg hold">' + a.notes_this_week + ' Notes</span>');
  if (l.replied) out.push('<span class="sg green">Replied</span>');
  if (l.dnd)     out.push('<span class="sg gray">DND</span>');
  if (l.dormant && !l.dnd) out.push('<span class="sg hold">Dormant</span>');
  if ((l.temp || '').toLowerCase() === 'hot') out.push('<span class="sg red">Hot</span>');
  if (l.rating != null) {
    const cls = (l.rating >= 8 ? 'green' : l.rating >= 5 ? 'warm' : 'red');
    out.push('<span class="sg ' + cls + '">★' + l.rating + '/10</span>');
  }
  return out.length ? '<span class="signal-set">' + out.join('') + '</span>' : '';
}

function statusBadge(l, def) {
  const s = def.status;
  let label = def.title;
  if (def.key === 'advanced' && l.movement_meta && l.movement_meta.to)
    label = '→ ' + l.movement_meta.to;
  else if (def.key === 'demoted' && l.movement_meta && l.movement_meta.to)
    label = '↘ ' + l.movement_meta.to;
  return '<div class="deal-status ' + s + '"><span class="lbl">Status</span>' + escapeHtml(label) + '</div>';
}

function renderDealHead(l, def, idx) {
  const stagePill = '<span class="sg ' + (def.status==='uc'?'red':def.status==='live'?'green':def.status==='warm'?'warm':def.status==='hold'?'hold':'gray') + '">' + escapeHtml(l.stage_label || '?') + '</span>';
  return '<div class="deal-head" onclick="toggleDeal(this)">' +
    '<div class="deal-num">' + String(idx).padStart(2,'0') + '</div>' +
    '<div class="deal-title">' +
      '<h3>' + escapeHtml(l.name || '(no name)') + (l.addr ? ' · <span style="font-weight:400;color:var(--muted)">' + escapeHtml(l.addr) + '</span>' : '') + '</h3>' +
      '<p class="tagline">' + (l.place ? escapeHtml(l.place) + ' · ' : '') + stagePill + ' ' + renderSignalSet(l) + '</p>' +
    '</div>' +
    statusBadge(l, def) +
    '<div class="deal-toggle"></div>' +
    '</div>';
}

function renderDealBody(l) {
  const ghl = GHL_BASE + '/' + l.cid;
  let html = '<div class="deal-body"><div class="deal-body-inner">';

  // Contact + Phone field
  html += '<div class="field"><h4>Contact</h4>';
  html += '<div class="contact">';
  if (l.phone)        html += '<div><span class="c-lbl">☎</span><a href="tel:' + escapeHtml(l.phone_e164 || l.phone) + '">' + escapeHtml(l.phone) + '</a> · <a href="sms:' + escapeHtml(l.phone_e164 || l.phone) + '">Text</a></div>';
  if (l.last_from_number) html += '<div><span class="c-lbl">↳</span>Last SMS from: ' + escapeHtml(l.last_from_number) + '</div>';
  html += '</div></div>';

  // Numbers field
  if (l.asking || l.arv || l.mao || l.spread != null) {
    html += '<div class="field"><h4>Deal Numbers</h4><div class="num-table">';
    if (l.asking) html += '<div class="k">Asking</div><div class="v">' + fmtMoney(l.asking) + '</div>';
    if (l.arv)    html += '<div class="k">ARV</div><div class="v">' + fmtMoney(l.arv) + '</div>';
    if (l.mao)    html += '<div class="k">70% MAO</div><div class="v">' + fmtMoney(l.mao) + '</div>';
    if (l.spread != null) {
      const cls = l.spread >= 0 ? 'green' : 'red';
      const sign = l.spread >= 0 ? '+' : '';
      html += '<div class="k">Spread</div><div class="v ' + cls + '">' + sign + fmtMoney(l.spread) + '</div>';
    }
    html += '</div></div>';
  }

  // Last call
  if (l.last_call_summary) {
    html += '<div class="field full"><h4>Last Call</h4>';
    html += '<p>' + escapeHtml(l.last_call_summary) + '</p>';
    if (l.rating != null) html += '<p style="font-size:12px;color:var(--muted)">Rating: ' + l.rating + '/10' + (l.temp ? ' · Temp: ' + escapeHtml(l.temp) : '') + '</p>';
    html += '</div>';
  }

  // SMS
  if (l.sms_count || l.last_sms_at || l.replied) {
    html += '<div class="field"><h4>SMS</h4>';
    html += '<p><strong>' + (l.sms_count || 0) + '</strong> sent';
    if (l.last_sms_at) {
      const d = new Date(l.last_sms_at);
      html += ' · Last: ' + d.toLocaleString('en-US', {timeZone:'America/New_York', month:'short', day:'numeric', hour:'numeric', minute:'2-digit'}) + ' ET';
    }
    html += '</p>';
    if (l.replied && l.reply_text) {
      html += '<div class="callout"><strong>Last Reply' + (l.reply_class ? ' · ' + escapeHtml(l.reply_class) : '') + '</strong>"' + escapeHtml(l.reply_text) + '"</div>';
    }
    html += '</div>';
  }

  // Motivation/timeline
  if (l.motivation || l.timeline) {
    html += '<div class="field"><h4>Context</h4><p>';
    if (l.motivation) html += '<strong>Motivation:</strong> ' + escapeHtml(l.motivation) + '<br>';
    if (l.timeline)   html += '<strong>Timeline:</strong> ' + escapeHtml(l.timeline);
    html += '</p></div>';
  }

  // Slack mentions for this lead
  if (l.slack_this_week && l.slack_this_week.length) {
    html += '<div class="field full"><h4>Slack This Week</h4>';
    for (const sm of l.slack_this_week) {
      html += '<div class="slack-card">';
      html += '<div class="slack-meta">#' + escapeHtml(sm.channel || '?') + ' · ' + escapeHtml(sm.ts_text || '') + '</div>';
      if (sm.original) html += '<div class="original">"' + escapeHtml(sm.original) + '"</div>';
      if (sm.summary)  html += '<div class="summary"><strong>AI summary:</strong> ' + escapeHtml(sm.summary) + '</div>';
      if (sm.permalink) html += '<div class="actions"><a href="' + escapeHtml(sm.permalink) + '" target="_blank">Open in Slack ↗</a></div>';
      html += '</div>';
    }
    html += '</div>';
  }

  // Action buttons
  html += '<div class="field full" style="display:flex;gap:8px;flex-wrap:wrap">';
  html += '<a class="deal-status" style="text-decoration:none;cursor:pointer;background:var(--ink)" href="' + ghl + '" target="_blank">Open in GHL ↗</a>';
  if (l.phone_e164) html += '<a class="deal-status" style="text-decoration:none;cursor:pointer;background:var(--s-live)" href="tel:' + escapeHtml(l.phone_e164) + '">Call</a>';
  html += '</div>';

  html += '</div></div>';
  return html;
}

function renderDeal(l, def, idx) {
  return '<article class="deal ' + def.status + '" data-stage="' + escapeHtml(l.stage_label || '') + '">' +
    renderDealHead(l, def, idx) + renderDealBody(l) +
    '</article>';
}

function toggleDeal(head) {
  head.parentElement.classList.toggle('open');
}

function renderSlackSection(week) {
  const items = (week.slack_mentions || []).filter(s => stageFilter === 'all' || s.lead_stage === stageFilter);
  if (!items.length) return '';
  let html = '<section><h2><span class="num">📡</span>Slack Mentions This Week<span class="sec-count">' + items.length + ' mention' + (items.length===1?'':'s') + '</span></h2>';
  html += '<p class="lede">Lead mentions captured from APG Slack channels in the last 7 days. Click "Open in Slack" to jump to the original message.</p>';
  html += '<div class="deal-grid">';
  for (const s of items) {
    const ghl = GHL_BASE + '/' + s.cid;
    html += '<div class="slack-card">';
    html += '<div class="slack-meta">#' + escapeHtml(s.channel || '?') + ' · ' + escapeHtml(s.ts_text || '') + ' · ' + escapeHtml(s.lead_stage || '') + '</div>';
    html += '<a class="lead-link" href="' + ghl + '" target="_blank">' + escapeHtml(s.lead_name || '(no name)') + '</a>';
    if (s.lead_addr) html += '<span style="color:var(--muted);font-size:12px"> · ' + escapeHtml(s.lead_addr) + '</span>';
    if (s.original)  html += '<div class="original">"' + escapeHtml(s.original) + '"</div>';
    if (s.summary)   html += '<div class="summary"><strong>AI:</strong> ' + escapeHtml(s.summary) + '</div>';
    html += '<div class="actions">';
    if (s.permalink) html += '<a href="' + escapeHtml(s.permalink) + '" target="_blank">Open in Slack ↗</a>';
    html += '<a href="' + ghl + '" target="_blank">Open in GHL ↗</a>';
    html += '</div>';
    html += '</div>';
  }
  html += '</div></section>';
  return html;
}

function render(week) {
  const c = document.getElementById('content');
  if (!week) { c.innerHTML = '<div class="empty">No analysis for this week.</div>'; return; }
  const totals = week.totals || {};
  const buckets = week.buckets || {};
  let html = '';

  // Snapshot section
  html += '<section><h2><span class="num">00</span>This Week</h2>';
  html += '<p class="lede">' + (totals.active_this_week || 0) + ' lead' + (totals.active_this_week===1?'':'s') + ' had activity this week. ' +
          (totals.advanced || 0) + ' moved forward, ' + (totals.demoted || 0) + ' moved back. ' +
          (totals.new || 0) + ' new in pipeline. ' +
          (totals.ready_contract || 0) + ' ready to contract, ' + (totals.ready_mao || 0) + ' ready to MAO offer.</p>';
  html += '<div class="crit-grid">';
  html += '<div class="crit live"><div class="kicker">Active This Week</div><p class="num-big">' + (totals.active_this_week||0) + '</p><div class="sub">SMS / reply / Slack / note</div></div>';
  html += '<div class="crit live"><div class="kicker">Stage Moves</div><p class="num-big">' + ((totals.advanced||0) + (totals.demoted||0)) + '</p><div class="sub">' + (totals.advanced||0) + ' fwd · ' + (totals.demoted||0) + ' back</div></div>';
  html += '<div class="crit warm"><div class="kicker">New This Week</div><p class="num-big">' + (totals.new||0) + '</p><div class="sub">added to pipeline</div></div>';
  html += '<div class="crit"><div class="kicker">SMS Sent / Replies</div><p class="num-big">' + (totals.sms_sent_week||0) + '<span style="font-size:18px;color:var(--muted)"> / ' + (totals.replies_week||0) + '</span></p><div class="sub">past 7 days</div></div>';
  html += '<div class="crit"><div class="kicker">Slack Mentions</div><p class="num-big">' + (totals.slack_mentions||0) + '</p><div class="sub">captured this week</div></div>';
  html += '<div class="crit uc"><div class="kicker">Ready Contract</div><p class="num-big">' + (totals.ready_contract||0) + '</p><div class="sub">send it</div></div>';
  html += '</div>';
  html += '<div style="margin-top:12px;font-size:12px;color:var(--muted);letter-spacing:0.06em">Pipeline total: <strong style="color:var(--ink)">' + (totals.total_pipeline||0) + '</strong> · Quiet (no activity): <strong style="color:var(--muted)">' + (totals.quiet||0) + '</strong></div>';

  // Stage filter chips
  html += '<div class="stage-filter">';
  const chipDefs = [
    ['all','All Stages'],
    ['0. Unqualified Leads','Unqualified'],
    ['1. Qualified Leads (Warm/Hot)','Qualified'],
    ['2. Prequalified Offer (LAO)','LAO'],
    ['3. Due Diligence (RR)','DD'],
    ['4. Negotiate (MAO)','MAO'],
    ['5. Contract Sent','Contract'],
    ['6. Executed PSA','PSA'],
    ['7. Disposition','Dispo'],
    ['Follow Up (1.5 month)','FU 1.5mo'],
    ['Follow Up (3 months)','FU 3mo'],
    ['Dead Deals','Dead'],
  ];
  for (const [val,lab] of chipDefs) {
    html += '<span class="filter-chip' + (stageFilter===val?' active':'') + '" data-stage="' + escapeHtml(val) + '">' + escapeHtml(lab) + '</span>';
  }
  html += '</div></section>';

  // Slack section
  html += renderSlackSection(week);

  // Buckets
  let dealCounter = 1;
  for (const def of BUCKET_DEFS) {
    const all = buckets[def.key] || [];
    const items = all.filter(leadMatchesStage);
    if (!items.length) continue;
    const filterNote = (stageFilter !== 'all' && all.length !== items.length) ?
      ' <span style="color:var(--muted);font-weight:500">of ' + all.length + '</span>' : '';
    html += '<section><h2><span class="num">' + def.num + '</span>' + def.title + '<span class="sec-count">' + items.length + ' lead' + (items.length===1?'':'s') + filterNote + '</span></h2>';
    if (def.desc) html += '<p class="lede">' + def.desc + '</p>';
    html += '<div class="deal-grid">';
    for (const l of items) {
      html += renderDeal(l, def, dealCounter++);
    }
    html += '</div></section>';
  }

  // Quiet bucket — collapsed
  const quietAll = buckets['quiet'] || [];
  const quietItems = quietAll.filter(leadMatchesStage);
  if (quietItems.length) {
    html += '<section><h2><span class="num">∅</span>Quiet — No Activity This Week<span class="sec-count">' + quietItems.length + ' lead' + (quietItems.length===1?'':'s') + '</span></h2>';
    html += '<p class="lede">Leads in pipeline but no SMS / reply / Slack / note this week. Click any to expand details.</p>';
    html += '<div class="deal-grid">';
    const quietDef = {key:'quiet', title:'Quiet', status:'dead', num:'∅'};
    for (const l of quietItems) {
      html += renderDeal(l, quietDef, dealCounter++);
    }
    html += '</div></section>';
  }

  if (!html.includes('<article')) html += '<div class="empty">No leads match this filter for this week.</div>';

  c.innerHTML = html;
  // Wire stage-filter chip clicks
  document.querySelectorAll('.filter-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      stageFilter = chip.dataset.stage;
      render(_currentWeekData);
    });
  });
}

// ── Refresh button (PAT → workflow_dispatch) ──
async function dispatchWorkflow(workflowFile, ref='main', inputs={}) {
  let pat = localStorage.getItem(PAT_KEY);
  if (!pat) {
    pat = prompt(
      'Refreshing requires a GitHub fine-grained personal access token (one-time setup).\\n\\n' +
      '1. https://github.com/settings/tokens?type=beta\\n' +
      '2. Repo: Mid0117/acq-automation · Permissions: Actions = Read+Write\\n' +
      '3. Paste token here. Stored only in your browser.'
    );
    if (!pat) throw new Error('No token provided');
    localStorage.setItem(PAT_KEY, pat.trim());
  }
  const url = 'https://api.github.com/repos/' + REPO + '/actions/workflows/' + workflowFile + '/dispatches';
  const r = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': 'Bearer ' + pat,
      'Accept': 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ref, inputs}),
  });
  if (r.status === 204) return true;
  if (r.status === 401 || r.status === 403) {
    localStorage.removeItem(PAT_KEY);
    throw new Error('Token rejected — please re-enter');
  }
  const txt = await r.text();
  throw new Error('Dispatch failed: ' + r.status + ' ' + txt.slice(0, 120));
}

async function pollLatestRun(workflowName, sinceMs, maxMs=240000) {
  const pat = localStorage.getItem(PAT_KEY);
  const url = 'https://api.github.com/repos/' + REPO + '/actions/runs?per_page=10';
  const start = Date.now();
  while (Date.now() - start < maxMs) {
    const r = await fetch(url, {
      headers: {'Authorization': 'Bearer ' + pat, 'Accept': 'application/vnd.github+json'}
    });
    if (r.ok) {
      const js = await r.json();
      const run = (js.workflow_runs || []).find(rr =>
        rr.name === workflowName && new Date(rr.created_at).getTime() > sinceMs - 5000
      );
      if (run && run.status === 'completed') return run;
    }
    await new Promise(res => setTimeout(res, 4000));
  }
  return null;
}

document.getElementById('refreshBtn').addEventListener('click', async () => {
  const btn = document.getElementById('refreshBtn');
  const icon = document.getElementById('refreshIcon');
  const label = document.getElementById('refreshLabel');
  const stat = document.getElementById('refreshStatus');
  btn.disabled = true;
  icon.outerHTML = '<span class="spin" id="refreshIcon"></span>';
  label.textContent = 'Triggering…';
  stat.textContent = '';
  const startedAt = Date.now();
  try {
    await dispatchWorkflow('weekly.yml');
    label.textContent = 'Running…';
    stat.textContent = '~1 min';
    const run = await pollLatestRun('Weekly Analysis', startedAt);
    if (run && run.conclusion === 'success') {
      label.textContent = 'Done — reloading';
      stat.textContent = 'Fresh snapshot ready.';
      setTimeout(() => location.reload(), 800);
    } else {
      label.textContent = 'Refresh';
      stat.textContent = 'Still running. Refresh page in a minute.';
    }
  } catch (e) {
    label.textContent = 'Refresh';
    stat.textContent = '⚠ ' + (e.message || e);
  } finally {
    btn.disabled = false;
    const newIcon = document.getElementById('refreshIcon');
    if (newIcon && newIcon.classList.contains('spin')) {
      newIcon.outerHTML = '<span id="refreshIcon">↻</span>';
    }
  }
});

async function main() {
  let idx;
  try {
    idx = await fetch(ROOT + 'index.json', {cache:'no-store'}).then(r => r.json());
  } catch (e) {
    document.getElementById('content').innerHTML = '<div class="empty">No weekly analysis has run yet. Click Refresh above to fire the first one.</div>';
    document.getElementById('weekSel').innerHTML = '<option>—</option>';
    return;
  }
  const weeks = idx.weeks || [];
  const sel = document.getElementById('weekSel');
  sel.innerHTML = weeks.map(w => '<option value="' + w + '">' + w + '</option>').join('');

  async function loadWeek(w) {
    document.getElementById('content').innerHTML = '<div class="loading">Loading ' + w + '…</div>';
    try {
      const data = await fetch(ROOT + w + '.json', {cache:'no-store'}).then(r => r.json());
      _currentWeekData = data;
      document.getElementById('metaWeek').textContent = w + ' · ' + (data.range_label || '');
      render(data);
    } catch (e) {
      _currentWeekData = null;
      document.getElementById('content').innerHTML = '<div class="empty">Failed to load this week\\'s data.</div>';
    }
  }
  sel.addEventListener('change', e => loadWeek(e.target.value));
  if (weeks.length) loadWeek(weeks[0]);
  else document.getElementById('content').innerHTML = '<div class="empty">No weeks available yet.</div>';
}

main();
</script>
</body>
</html>
"""


def main():
    print(f'[Weekly Analysis] {now_et().strftime("%Y-%m-%d %I:%M %p ET")}')
    os.makedirs(WEEKLY_DIR, exist_ok=True)

    prev = load_state()
    sms_state = load_sms_state()
    leads = fetch_all_leads()
    print(f'  Found {len(leads)} leads in pipeline')

    et_now = now_et()
    week = week_id(et_now)
    week_start = et_now - timedelta(days=7)
    week_start_iso = week_start.astimezone(timezone.utc).isoformat()

    current = {}
    slack_mentions_week = []   # [{lead_name, lead_addr, cid, channel, user, permalink, original, summary, added_at}]
    slack_count_by_cid = {}    # for activity scoring + modal display
    for lead in leads:
        rec = build_lead_record(lead, sms_state)
        # carry over first_seen_at from prev if we had it
        if lead['cid'] in prev:
            rec['first_seen_at'] = prev[lead['cid']].get('first_seen_at') or prev[lead['cid']].get('snapshot_at')
        else:
            rec['first_seen_at'] = now_utc().isoformat()
        rec['snapshot_at'] = now_utc().isoformat()

        # Slack mentions captured this week for this lead
        per_lead_slacks = []
        for sm in fetch_slack_mentions(lead['cid']):
            if sm.get('added_at') and sm['added_at'] >= week_start_iso:
                entry = {
                    'cid':       lead['cid'],
                    'lead_name': rec['name'],
                    'lead_addr': rec['addr'],
                    'lead_place': rec['place'],
                    'lead_stage': rec['stage_label'],
                    **sm,
                }
                slack_mentions_week.append(entry)
                per_lead_slacks.append(entry)
        rec['slack_this_week'] = per_lead_slacks
        slack_count_by_cid[lead['cid']] = len(per_lead_slacks)
        # Total notes added this week (any kind — manual notes, summary
        # updates, Slack mentions all count as "the team paid attention")
        rec['notes_added_this_week'] = count_recent_notes(lead['cid'], week_start_iso)

        current[lead['cid']] = rec
        time.sleep(0.05)

    slack_mentions_week.sort(key=lambda x: x.get('added_at',''), reverse=True)

    # Categorize. Buckets focus on weekly activity:
    #   advanced / demoted / new       — stage moves and additions
    #   active_no_move                  — same stage but had SMS / reply / Slack this week
    #   ready_contract / ready_mao      — overlay tags for action items
    #   drop_suggest                    — overlay for cold/nurture too long
    #   quiet                           — same stage, no activity (shown collapsed at bottom)
    buckets = {k: [] for k in (
        'ready_contract','ready_mao','advanced','demoted','new',
        'active_no_move','drop_suggest','quiet')}

    for cid, c in current.items():
        movement, meta, action_tags = categorize(c, prev, week_start_iso,
                                                  slack_count_by_cid.get(cid, 0),
                                                  c.get('notes_added_this_week', 0))
        c_out = dict(c)
        c_out['movement'] = movement
        c_out['movement_meta'] = meta
        c_out['action_tags'] = action_tags
        buckets[movement].append(c_out)
        for tag in action_tags:
            buckets[tag].append(c_out)

    # Sort within buckets — most-relevant first
    for k in buckets:
        if k in ('ready_contract', 'ready_mao'):
            buckets[k].sort(key=lambda x: -(x.get('rating') or 0))
        elif k in ('advanced', 'new', 'active_no_move'):
            buckets[k].sort(key=lambda x: x.get('snapshot_at',''), reverse=True)
        else:
            buckets[k].sort(key=lambda x: -(x.get('rating') or 0))

    # Totals oriented around WEEKLY activity, not pipeline state
    sms_sent_this_week = sum(1 for c in current.values() if c['_activity']['sms_this_week'])
    replies_this_week  = sum(1 for c in current.values() if c['_activity']['reply_this_week'])
    active_this_week_total = sum(1 for c in current.values() if c['_activity']['had_activity'])

    totals = {
        'total_pipeline':     len(current),
        'active_this_week':   active_this_week_total,
        'sms_sent_week':      sms_sent_this_week,
        'replies_week':       replies_this_week,
        'slack_mentions':     len(slack_mentions_week),
        'advanced':           len(buckets['advanced']),
        'demoted':            len(buckets['demoted']),
        'new':                len(buckets['new']),
        'active_no_move':     len(buckets['active_no_move']),
        'ready_contract':     len(buckets['ready_contract']),
        'ready_mao':          len(buckets['ready_mao']),
        'drop_suggest':       len(buckets['drop_suggest']),
        'quiet':              len(buckets['quiet']),
    }

    # Range label
    range_label = f'{week_start.strftime("%b %d")} → {et_now.strftime("%b %d, %Y")}'

    output = {
        'week_id':              week,
        'range_label':          range_label,
        'generated_at':         now_utc().isoformat(),
        'generated_at_et':      et_now.strftime('%b %d, %Y %I:%M %p ET'),
        'totals':               totals,
        'buckets':              buckets,
        'slack_mentions':       slack_mentions_week,
    }

    save_json(os.path.join(WEEKLY_DIR, f'{week}.json'), output)
    save_json(os.path.join(WEEKLY_DIR, '_state.json'), current)

    # Update index
    idx_path = os.path.join(WEEKLY_DIR, 'index.json')
    idx = {'weeks': []}
    if os.path.exists(idx_path):
        try: idx = json.load(open(idx_path))
        except Exception: pass
    if week in idx.get('weeks', []):
        idx['weeks'] = [w for w in idx['weeks'] if w != week]
    idx['weeks'] = [week] + idx.get('weeks', [])
    save_json(idx_path, idx)

    write_html_shell()

    print(f'\n[Week {week}] {range_label}')
    print(f'  Total pipeline: {totals["total_pipeline"]}  |  Active this week: {totals["active_this_week"]}')
    print(f'  Advanced: {totals["advanced"]} | Demoted: {totals["demoted"]} | New: {totals["new"]}')
    print(f'  Active no stage move: {totals["active_no_move"]} | Quiet: {totals["quiet"]}')
    print(f'  Ready contract: {totals["ready_contract"]} | Ready MAO: {totals["ready_mao"]} | Drop suggest: {totals["drop_suggest"]}')


if __name__ == '__main__':
    main()
