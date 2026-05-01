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
        'last_updated': lead.get('updated', ''),
    }


def categorize(curr, prev_map, week_start_iso, slack_mention_count):
    """Bucket a single lead based on this-week vs last-week diff.

    Weekly view focuses on what's CHANGED or had ACTIVITY this week.
    Stagnant-no-activity leads are still categorized but rendered separately
    (collapsed by default) so the team isn't drowning in pipeline noise.
    """
    cid = curr['cid']
    prev = prev_map.get(cid)

    # Did anything happen for this lead in the last 7 days?
    last_sms_at  = curr.get('last_sms_at') or ''
    replied_at   = curr.get('replied_at') or ''
    last_updated = curr.get('last_updated') or ''
    sms_this_week    = bool(last_sms_at  and last_sms_at  >= week_start_iso)
    reply_this_week  = bool(replied_at   and replied_at   >= week_start_iso)
    update_this_week = bool(last_updated and last_updated >= week_start_iso)
    slack_this_week  = slack_mention_count > 0
    had_activity = sms_this_week or reply_this_week or slack_this_week

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

    # Activity flags exposed for the modal
    curr['_activity'] = {
        'sms_this_week':    sms_this_week,
        'reply_this_week':  reply_this_week,
        'slack_this_week':  slack_this_week,
        'slack_count':      slack_mention_count,
        'had_activity':     had_activity,
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
<title>APG ACQ — Weekly Analysis</title>
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
  background: var(--bg); color: var(--ink); line-height: 1.55;
  -webkit-font-smoothing: antialiased;
}
.container { max-width: 1200px; margin: 0 auto; padding: 28px 36px 80px; }

.meta-bar {
  display: flex; justify-content: space-between; align-items: center;
  border-top: 4px solid var(--ink); padding: 14px 0 0;
  font-size: 11px; font-weight: 700;
  text-transform: uppercase; letter-spacing: 0.14em; color: var(--ink-soft);
}

.logo-row {
  display: flex; align-items: center; justify-content: space-between;
  gap: 16px; margin: 24px 0 12px; flex-wrap: wrap;
}
.logo-svg { width: 200px; height: auto; }
.logo-svg .atom-orbit { stroke: var(--gold-deep); stroke-width: 2.6; fill: none; }
.logo-svg .atom-core  { fill: var(--gold-deep); }
.logo-svg .brand-main { fill: var(--ink); }
.logo-svg .brand-sub  { fill: var(--ink-soft); letter-spacing: 4px; }
.nav { display: flex; gap: 6px; flex-wrap: wrap; }
.nav a {
  padding: 6px 12px; border-radius: 3px;
  background: transparent; border: 1px solid var(--rule);
  color: var(--ink-soft); font-size: 11px; font-weight: 700;
  letter-spacing: 0.06em; text-transform: uppercase; text-decoration: none;
  transition: all .12s;
}
.nav a:hover { color: var(--ink); border-color: var(--ink); }
.nav a.active { background: var(--ink); color: var(--gold); border-color: var(--ink); }

.doc-header { padding: 28px 0 18px; }
.doc-header h1 {
  font-family: "Iowan Old Style", Palatino, Georgia, serif;
  font-weight: 600; font-size: 48px; line-height: 1.05;
  margin: 0 0 12px; color: var(--ink); letter-spacing: -0.01em;
}
.doc-header h1 .accent { font-style: italic; color: var(--gold-deep); }
.doc-header .lede {
  font-family: "Iowan Old Style", Georgia, serif; font-style: italic;
  font-size: 16px; color: var(--ink-soft); max-width: 680px; margin: 0;
}

.week-picker {
  display: flex; align-items: center; gap: 12px; flex-wrap: wrap;
  margin-top: 20px; padding: 12px 16px;
  background: var(--paper); border: 1px solid var(--rule); border-radius: 4px;
}
.week-picker label {
  font-size: 11px; font-weight: 800; letter-spacing: 0.1em;
  text-transform: uppercase; color: var(--ink-mute);
}
.week-picker select {
  padding: 8px 12px; border: 1px solid var(--rule);
  background: var(--bg); color: var(--ink); border-radius: 3px;
  font-size: 13px; font-weight: 700; cursor: pointer;
  font-family: ui-monospace, monospace;
}
.week-picker .meta { color: var(--ink-soft); font-size: 12px; margin-left: auto; }

.stage-filter {
  display: flex; gap: 6px; flex-wrap: wrap;
  margin-top: 14px; padding: 10px 14px;
  background: var(--paper); border: 1px solid var(--rule); border-radius: 4px;
}
.filter-chip {
  padding: 6px 12px; border-radius: 3px;
  background: transparent; border: 1px solid var(--rule);
  color: var(--ink-soft); font-size: 11px; font-weight: 700;
  letter-spacing: 0.06em; text-transform: uppercase;
  cursor: pointer; user-select: none; transition: all .12s;
}
.filter-chip:hover { color: var(--ink); border-color: var(--ink); }
.filter-chip.active { background: var(--ink); color: var(--gold); border-color: var(--ink); }

.slack-list .lead-line { grid-template-columns: 1fr auto auto; }
.slack-list .lead-line .who .nm a { color: var(--ink); text-decoration: none; }
.slack-list .lead-line .who .nm a:hover { color: var(--gold-deep); text-decoration: underline; }
.slack-list .lead-line .excerpt {
  font-style: italic; color: var(--ink-soft); margin-top: 4px;
  font-size: 12px; line-height: 1.5;
  border-left: 2px solid var(--rule); padding-left: 10px; max-width: 600px;
}
.slack-list .lead-line .channel-tag {
  font-size: 10px; padding: 3px 8px; border-radius: 3px;
  background: rgba(255,199,44,0.20); color: var(--ink);
  font-weight: 700; letter-spacing: 0.04em;
  font-family: ui-monospace, monospace;
}

.lead-line.clickable { cursor: pointer; }
.lead-line.clickable:hover .nm { color: var(--gold-deep); }

/* Description rows inside buckets */
.lead-line.desc-row {
  background: rgba(0,0,0,0.02); font-style: italic;
  color: var(--ink-soft); grid-template-columns: 1fr;
}

/* Quiet bucket — collapsible details */
details.quiet-block { background: var(--paper); border: 1px solid var(--rule); border-radius: 4px; margin-bottom: 14px; overflow: hidden; }
details.quiet-block > summary {
  list-style: none; cursor: pointer;
  padding: 14px 18px;
  background: rgba(26,40,64,0.06);
  display: flex; align-items: center; gap: 12px;
}
details.quiet-block > summary::-webkit-details-marker { display: none; }
details.quiet-block > summary::after {
  content: '▼'; margin-left: auto; color: var(--ink-mute);
  transition: transform 0.2s ease;
}
details[open].quiet-block > summary::after { transform: rotate(180deg); }
.quiet-summary-text { font-size: 14px; color: var(--ink); }
.quiet-summary-text .ct { font-size: 12px; color: var(--ink-mute); margin-left: 6px; }

/* In-page refresh button */
.refresh-bar {
  display: flex; align-items: center; gap: 12px; flex-wrap: wrap;
  margin: 12px 0 0;
}
.refresh-btn {
  padding: 8px 14px; border-radius: 3px;
  background: var(--ink); color: var(--gold);
  border: 1px solid var(--ink); cursor: pointer;
  font-size: 11px; font-weight: 800; letter-spacing: 0.06em;
  text-transform: uppercase; transition: all 0.15s;
  display: inline-flex; align-items: center; gap: 8px;
}
.refresh-btn:hover { background: var(--gold-deep); color: var(--ink); }
.refresh-btn:disabled { opacity: 0.6; cursor: not-allowed; }
.refresh-btn .spin {
  display: inline-block; width: 12px; height: 12px;
  border: 2px solid currentColor; border-right-color: transparent;
  border-radius: 50%; animation: spin 0.7s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }
.refresh-status { font-size: 12px; color: var(--ink-soft); }

/* Lead Detail Modal */
.modal-overlay {
  position: fixed; inset: 0; background: rgba(26,40,64,0.55);
  backdrop-filter: blur(4px);
  display: none; align-items: flex-start; justify-content: center;
  z-index: 1000; padding: 60px 20px 20px;
  overflow-y: auto;
  opacity: 0; transition: opacity 0.2s ease;
}
.modal-overlay.open {
  display: flex; opacity: 1; animation: modal-fade-in 0.2s ease forwards;
}
@keyframes modal-fade-in { from { opacity: 0; } to { opacity: 1; } }
.modal {
  background: var(--paper); border-radius: 8px;
  border: 1px solid var(--rule);
  width: 100%; max-width: 720px;
  box-shadow: 0 20px 60px rgba(26,40,64,0.30);
  transform: translateY(20px) scale(0.96);
  opacity: 0;
  transition: transform 0.25s cubic-bezier(0.22, 1, 0.36, 1), opacity 0.25s ease;
}
.modal-overlay.open .modal { transform: translateY(0) scale(1); opacity: 1; }
.modal-close {
  position: absolute; top: 14px; right: 18px;
  background: transparent; border: 0; font-size: 22px;
  color: var(--ink-mute); cursor: pointer; line-height: 1;
  padding: 4px 8px;
}
.modal-close:hover { color: var(--ink); }
.modal-body { padding: 28px 32px 24px; position: relative; }
.modal-header {
  display: flex; justify-content: space-between; align-items: start; gap: 12px;
  padding-bottom: 16px; margin-bottom: 14px;
  border-bottom: 1px solid var(--rule);
}
.modal-header h2 {
  font-family: "Iowan Old Style", Georgia, serif;
  margin: 0 0 4px; font-size: 24px; font-weight: 600; color: var(--ink);
  letter-spacing: -0.005em;
}
.modal-sub { font-size: 13px; color: var(--ink-soft); }
.modal-row {
  display: grid; grid-template-columns: 110px 1fr; gap: 16px;
  padding: 12px 0; border-bottom: 1px solid var(--rule);
}
.modal-row:last-of-type { border-bottom: 0; }
.modal-label {
  font-size: 10px; font-weight: 800; letter-spacing: 0.12em;
  text-transform: uppercase; color: var(--ink-mute); padding-top: 4px;
}
.modal-value { font-size: 14px; color: var(--ink); line-height: 1.5; }
.modal-value strong { font-weight: 700; }
.modal-value .quote {
  font-family: "Iowan Old Style", Georgia, serif; font-style: italic;
  border-left: 2px solid var(--gold); padding: 2px 12px;
  color: var(--ink-soft); margin: 4px 0;
}
.phone-link {
  font-family: ui-monospace, monospace; font-size: 14px; font-weight: 700;
  color: var(--gold-deep); text-decoration: none;
}
.phone-link:hover { color: var(--ink); text-decoration: underline; }
.slack-card {
  background: rgba(255,199,44,0.08); border: 1px solid rgba(255,199,44,0.25);
  border-radius: 4px; padding: 10px 12px; margin-bottom: 8px;
}
.slack-card .slack-meta {
  font-size: 11px; color: var(--ink-mute); margin-bottom: 4px;
  font-family: ui-monospace, monospace;
}
.modal-actions {
  display: flex; gap: 8px; justify-content: flex-end; flex-wrap: wrap;
  margin-top: 16px; padding-top: 16px;
  border-top: 1px solid var(--rule);
}
.modal-actions .btn {
  padding: 8px 16px; border-radius: 3px;
  font-size: 11px; font-weight: 800; letter-spacing: 0.06em;
  text-transform: uppercase; text-decoration: none; cursor: pointer;
  border: 1px solid transparent; transition: all 0.15s;
}
.modal-actions .btn.primary  { background: var(--ink); color: var(--gold); border-color: var(--ink); }
.modal-actions .btn.primary:hover { background: var(--gold-deep); color: var(--ink); }
.modal-actions .btn.secondary{ background: transparent; color: var(--ink); border-color: var(--rule-strong); }
.modal-actions .btn.secondary:hover { border-color: var(--ink); }
.modal-actions .btn.ghost    { background: transparent; color: var(--ink-mute); border-color: var(--rule); }
.modal-actions .btn.ghost:hover { color: var(--ink); }

hr.head { border: 0; border-top: 1px solid var(--rule); margin: 24px 0 0; }

.sec { margin: 36px 0 18px; }
.sec .tag-row { display: flex; align-items: center; gap: 12px; margin-bottom: 8px; flex-wrap: wrap; }
.sec .num {
  display: inline-block; background: var(--gold); color: var(--ink);
  font-weight: 800; font-size: 12px; letter-spacing: 0.04em;
  padding: 3px 8px; border-radius: 3px;
  font-family: ui-monospace, monospace;
}
.sec h2 {
  font-family: "Iowan Old Style", Georgia, serif; font-weight: 600;
  font-size: 24px; letter-spacing: -0.005em; margin: 0; color: var(--ink);
}
.sec .count { font-size: 13px; color: var(--ink-mute); font-weight: 500; }
.sec hr { border: 0; border-top: 1px solid var(--rule); margin: 0 0 16px; }

.stat-row {
  display: grid; gap: 10px;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
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

.bucket {
  background: var(--paper); border: 1px solid var(--rule);
  border-radius: 4px; margin-bottom: 14px; overflow: hidden;
}
.bucket .head {
  padding: 14px 18px; border-bottom: 1px solid var(--rule);
  background: var(--gold-soft); display: flex; align-items: center;
  gap: 12px; flex-wrap: wrap;
}
.bucket .head .num-big {
  font-family: ui-monospace, monospace; font-size: 18px;
  color: var(--gold-deep); font-weight: 800;
}
.bucket .head h3 {
  margin: 0; font-family: "Iowan Old Style", Georgia, serif;
  font-weight: 600; font-size: 20px; color: var(--ink);
  flex: 1; min-width: 0;
}
.bucket .head .ct {
  font-size: 12px; color: var(--ink-mute); font-weight: 700;
  font-family: ui-monospace, monospace;
}
.bucket.green .head { background: rgba(47,125,91,0.10); }
.bucket.green .head .num-big { color: var(--green); }
.bucket.hot   .head { background: rgba(197,68,58,0.10); }
.bucket.hot   .head .num-big { color: var(--hot); }
.bucket.warm  .head { background: rgba(181,122,26,0.12); }
.bucket.warm  .head .num-big { color: var(--warm); }
.bucket.gray  .head { background: rgba(26,40,64,0.06); }
.bucket.gray  .head .num-big { color: var(--ink-soft); }
.bucket .body { padding: 0; }
.bucket .lead-line {
  padding: 14px 18px; border-bottom: 1px solid var(--rule);
  display: grid; grid-template-columns: 1fr auto auto auto auto; gap: 12px;
  align-items: center; font-size: 13px;
}
.bucket .lead-line:last-child { border-bottom: none; }
.bucket .lead-line:hover { background: rgba(232,197,71,0.05); }
@media (max-width: 760px) {
  .bucket .lead-line { grid-template-columns: 1fr; gap: 4px; padding: 12px 14px; }
}
.bucket .lead-line .who {
  display: flex; flex-direction: column; gap: 2px; min-width: 0;
}
.bucket .lead-line .who .nm {
  font-family: "Iowan Old Style", Georgia, serif; font-weight: 600;
  font-size: 15px; color: var(--ink);
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.bucket .lead-line .who .pl { font-size: 11px; color: var(--ink-mute); }
.bucket .lead-line .stage-pill {
  font-size: 10px; padding: 3px 8px; border-radius: 3px;
  font-weight: 800; letter-spacing: 0.06em; text-transform: uppercase;
  background: var(--gold-soft); color: var(--ink); white-space: nowrap;
  font-family: ui-monospace, monospace;
}
.bucket .lead-line .move-arrow {
  font-size: 11px; color: var(--ink-soft);
  font-family: ui-monospace, monospace;
}
.bucket .lead-line .move-arrow .arrow { color: var(--gold-deep); margin: 0 4px; }
.bucket .lead-line .move-arrow.demoted .arrow { color: var(--hot); }
.bucket .lead-line .signal {
  font-size: 11px; color: var(--ink-soft);
  display: flex; gap: 6px; flex-wrap: wrap;
}
.bucket .lead-line .signal .tag {
  display: inline-block; padding: 2px 6px; font-size: 10px; font-weight: 700;
  border-radius: 3px; background: var(--gold-soft); color: var(--ink);
  letter-spacing: 0.04em;
}
.bucket .lead-line .signal .tag.hot   { background: rgba(197,68,58,0.15); color: var(--hot); }
.bucket .lead-line .signal .tag.warm  { background: rgba(181,122,26,0.15); color: var(--warm); }
.bucket .lead-line .signal .tag.green { background: rgba(47,125,91,0.15);  color: var(--green); }
.bucket .lead-line .ghl-link {
  font-size: 10px; padding: 5px 10px; border-radius: 3px;
  background: var(--ink); color: var(--gold);
  text-decoration: none; font-weight: 800; letter-spacing: 0.06em;
  text-transform: uppercase; white-space: nowrap;
}
.bucket .lead-line .ghl-link:hover { background: var(--gold-deep); color: var(--ink); }

.empty { text-align: center; color: var(--ink-mute); padding: 30px; font-style: italic; }
.loading { text-align: center; color: var(--ink-mute); padding: 60px; font-style: italic; }

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
    <div>APG · ACQ Operating Layer · Weekly Analysis</div>
    <div id="metaWeek">Loading…</div>
  </div>

  <div class="logo-row">
    <img class="logo-svg" src="logo.png"
         onerror="this.onerror=null; this.src='logo.svg';"
         alt="Atom Property Group" />
    <div class="nav">
      <a href="index.html">Follow-Ups</a>
      <a href="deals.html">Deals</a>
      <a href="weekly.html" class="active">Weekly</a>
      <a href="about.html">About</a>
    </div>
  </div>

  <header class="doc-header">
    <h1>Weekly <span class="accent">Analysis</span></h1>
    <p class="lede">Every Friday at 10 AM ET, the system snapshots all leads in the pipeline and compares against the previous Friday. Use the dropdown below to view past weeks. Lead movement, stagnation, and recommended actions are computed from real GHL data.</p>
    <hr class="head">
  </header>

  <div class="week-picker">
    <label for="weekSel">Week</label>
    <select id="weekSel"><option>Loading…</option></select>
    <span class="meta" id="weekMeta"></span>
  </div>

  <div class="refresh-bar">
    <button class="refresh-btn" id="refreshBtn">
      <span id="refreshIcon">↻</span>
      <span id="refreshLabel">Refresh Now</span>
    </button>
    <span class="refresh-status" id="refreshStatus"></span>
  </div>

  <div class="stage-filter" id="stageFilter">
    <span class="filter-chip active" data-stage="all">All Stages</span>
    <span class="filter-chip" data-stage="0. Unqualified Leads">Unqualified</span>
    <span class="filter-chip" data-stage="1. Qualified Leads (Warm/Hot)">Qualified</span>
    <span class="filter-chip" data-stage="2. Prequalified Offer (LAO)">LAO</span>
    <span class="filter-chip" data-stage="3. Due Diligence (RR)">DD</span>
    <span class="filter-chip" data-stage="4. Negotiate (MAO)">MAO</span>
    <span class="filter-chip" data-stage="5. Contract Sent">Contract</span>
    <span class="filter-chip" data-stage="Follow Up (1.5 month)">FU 1.5mo</span>
    <span class="filter-chip" data-stage="Follow Up (3 months)">FU 3mo</span>
    <span class="filter-chip" data-stage="Dead Deals">Dead</span>
  </div>

  <div id="content"><div class="loading">Loading analysis…</div></div>

  <footer>Auto-generated each Friday 10 AM ET · APG ACQ Operating Layer</footer>
</div>

<!-- Lead detail modal — populated by openLeadModal(cid) -->
<div class="modal-overlay" id="leadModal">
  <div class="modal" role="dialog" aria-modal="true">
    <button class="modal-close" onclick="closeLeadModal()" aria-label="Close">×</button>
    <div class="modal-body" id="leadModalBody"></div>
  </div>
</div>

<script>
const GHL_BASE = 'https://app.gohighlevel.com/v2/location/RCkiUmWqXX4BYQ39JXmm/contacts/detail';
const ROOT     = 'weekly/';

function fmtMoney(v) {
  if (v == null || isNaN(v)) return '—';
  v = Math.round(v);
  if (Math.abs(v) >= 1_000_000) return '$' + (v/1_000_000).toFixed(2) + 'M';
  if (Math.abs(v) >= 1_000)     return '$' + (v/1_000).toFixed(0) + 'k';
  return '$' + v.toLocaleString();
}

function tempTag(t) {
  if (!t) return '';
  const cls = ({hot:'hot',warm:'warm',cold:'',nurture:''})[t] || '';
  return '<span class="tag ' + cls + '">🌡 ' + t + '</span>';
}

function renderLead(l, opts={}) {
  opts = opts || {};
  const ghl = GHL_BASE + '/' + l.cid;
  let move = '';
  if (opts.showMove && l.movement_meta && l.movement_meta.from && l.movement_meta.to) {
    const cls = (l.movement === 'demoted') ? 'demoted' : '';
    move = '<div class="move-arrow ' + cls + '">' +
           (l.movement_meta.from || '') + '<span class="arrow">→</span>' +
           (l.movement_meta.to || '') + '</div>';
  }
  const signals = [];
  if (l.temp)    signals.push(tempTag(l.temp));
  if (l.rating != null) {
    const cls = (l.rating >= 8 ? 'green' : l.rating >= 5 ? 'warm' : 'hot');
    signals.push('<span class="tag ' + cls + '">★ ' + l.rating + '/10</span>');
  }
  if (l.spread != null && l.arv) {
    const cls = l.spread >= 0 ? 'green' : 'hot';
    signals.push('<span class="tag ' + cls + '">spread ' + (l.spread>=0?'+':'') + fmtMoney(l.spread) + '</span>');
  }
  if (l.replied) signals.push('<span class="tag green">replied</span>');
  if (l.dormant && !l.dnd) signals.push('<span class="tag warm">dormant</span>');
  if (l.dnd)     signals.push('<span class="tag gray">DND</span>');
  // Activity flags drawn from the lead's _activity object
  const a = l._activity || {};
  if (a.sms_this_week)   signals.push('<span class="tag green">📱 SMS this wk</span>');
  if (a.reply_this_week) signals.push('<span class="tag green">💬 reply this wk</span>');
  if (a.slack_count > 0) signals.push('<span class="tag warm">📡 ' + a.slack_count + ' Slack</span>');

  return '<div class="lead-line clickable" data-cid="' + escapeHtml(l.cid) + '" tabindex="0">' +
    '<div class="who"><div class="nm">' + (l.name || '(no name)') + '</div>' +
    '<div class="pl">' + (l.addr ? l.addr + ' · ' : '') + (l.place || '') + '</div></div>' +
    '<span class="stage-pill">' + (l.stage_label || '?') + '</span>' +
    move +
    '<div class="signal">' + signals.join('') + '</div>' +
    '<a class="ghl-link" href="' + ghl + '" target="_blank" onclick="event.stopPropagation()">Open</a>' +
    '</div>';
}

// ─────────────── Lead Detail Modal ───────────────
function findLeadByCid(cid) {
  if (!_currentWeekData) return null;
  const buckets = _currentWeekData.buckets || {};
  for (const k of Object.keys(buckets)) {
    const found = (buckets[k] || []).find(l => l && l.cid === cid);
    if (found) return found;
  }
  return null;
}

function openLeadModal(cid) {
  const l = findLeadByCid(cid);
  if (!l) return;
  const ghl = GHL_BASE + '/' + l.cid;
  const modal = document.getElementById('leadModal');
  const body  = document.getElementById('leadModalBody');

  let html = '';
  // Header
  html += '<div class="modal-header">';
  html += '<div><h2>' + escapeHtml(l.name || '(no name)') + '</h2>';
  html += '<div class="modal-sub">' + escapeHtml((l.addr ? l.addr + ' · ' : '') + (l.place || '')) + '</div></div>';
  html += '<span class="stage-pill">' + escapeHtml(l.stage_label || '?') + '</span>';
  html += '</div>';

  // Phone + quick contact
  if (l.phone) {
    html += '<div class="modal-row">';
    html += '<div class="modal-label">Phone</div>';
    html += '<div class="modal-value"><a href="tel:' + escapeHtml(l.phone_e164 || l.phone) + '" class="phone-link">📞 ' + escapeHtml(l.phone) + '</a>';
    if (l.phone_e164) html += ' <a href="sms:' + escapeHtml(l.phone_e164) + '" class="phone-link" style="margin-left:8px">💬 Text</a>';
    html += '</div></div>';
  }

  // Financials
  if (l.asking || l.arv || l.mao) {
    html += '<div class="modal-row">';
    html += '<div class="modal-label">Numbers</div>';
    html += '<div class="modal-value">';
    if (l.asking) html += '<strong>Asking:</strong> ' + fmtMoney(l.asking) + '  ·  ';
    if (l.arv)    html += '<strong>ARV:</strong> ' + fmtMoney(l.arv) + '  ·  ';
    if (l.mao)    html += '<strong>70% MAO:</strong> ' + fmtMoney(l.mao);
    if (l.spread != null) {
      const sign = l.spread >= 0 ? '+' : '';
      html += '  ·  <strong>Spread:</strong> <span style="color:' + (l.spread>=0?'var(--green)':'var(--hot)') + '">' + sign + fmtMoney(l.spread) + '</span>';
    }
    html += '</div></div>';
  }

  // Last call summary (from acq_automation Claude analysis)
  if (l.last_call_summary) {
    html += '<div class="modal-row">';
    html += '<div class="modal-label">Last call</div>';
    html += '<div class="modal-value"><div class="quote">' + escapeHtml(l.last_call_summary) + '</div>';
    if (l.rating != null) html += '<div style="margin-top:6px;font-size:12px"><strong>Rating:</strong> ' + l.rating + '/10' + (l.temp ? ' · <strong>Temp:</strong> ' + l.temp : '') + '</div>';
    html += '</div></div>';
  }

  // SMS history summary
  if (l.sms_count || l.last_sms_at) {
    html += '<div class="modal-row">';
    html += '<div class="modal-label">SMS</div>';
    html += '<div class="modal-value">';
    html += '<strong>' + (l.sms_count || 0) + '</strong> sent';
    if (l.last_sms_at) html += '  ·  Last: ' + escapeHtml(new Date(l.last_sms_at).toLocaleString('en-US', {timeZone:'America/New_York', month:'short', day:'numeric', hour:'numeric', minute:'2-digit'}) + ' ET');
    if (l.last_from_number) html += '  ·  From ' + escapeHtml(l.last_from_number);
    html += '</div></div>';
  }

  // Last reply text (if there was one)
  if (l.replied && l.reply_text) {
    html += '<div class="modal-row">';
    html += '<div class="modal-label">Last reply</div>';
    html += '<div class="modal-value">';
    if (l.reply_class) html += '<span class="tag ' + (l.reply_class==='POSITIVE'?'green':l.reply_class==='NEUTRAL'?'warm':'hot') + '">' + escapeHtml(l.reply_class) + '</span> ';
    html += '<div class="quote">"' + escapeHtml(l.reply_text) + '"</div>';
    html += '</div></div>';
  }

  // Slack mentions for this lead this week
  if (l.slack_this_week && l.slack_this_week.length) {
    html += '<div class="modal-row">';
    html += '<div class="modal-label">Slack this week</div>';
    html += '<div class="modal-value">';
    for (const sm of l.slack_this_week) {
      html += '<div class="slack-card">';
      html += '<div class="slack-meta">#' + escapeHtml(sm.channel || '?') + ' · ' + escapeHtml(sm.ts_text || '');
      if (sm.permalink) html += ' · <a href="' + escapeHtml(sm.permalink) + '" target="_blank">Open in Slack ↗</a>';
      html += '</div>';
      if (sm.original) html += '<div class="quote">"' + escapeHtml(sm.original) + '"</div>';
      if (sm.summary)  html += '<div style="font-size:12px;color:var(--ink-soft);margin-top:4px"><strong>AI summary:</strong> ' + escapeHtml(sm.summary) + '</div>';
      html += '</div>';
    }
    html += '</div></div>';
  }

  // Motivation, timeline, deal context
  if (l.motivation || l.timeline) {
    html += '<div class="modal-row">';
    html += '<div class="modal-label">Context</div>';
    html += '<div class="modal-value">';
    if (l.motivation) html += '<strong>Motivation:</strong> ' + escapeHtml(l.motivation) + '<br>';
    if (l.timeline)   html += '<strong>Timeline:</strong> ' + escapeHtml(l.timeline);
    html += '</div></div>';
  }

  // Footer actions
  html += '<div class="modal-actions">';
  html += '<a class="btn primary" href="' + ghl + '" target="_blank">Open in GHL ↗</a>';
  if (l.phone_e164) html += '<a class="btn secondary" href="tel:' + escapeHtml(l.phone_e164) + '">Call</a>';
  html += '<button class="btn ghost" onclick="closeLeadModal()">Close</button>';
  html += '</div>';

  body.innerHTML = html;
  modal.classList.add('open');
  document.body.style.overflow = 'hidden';
}

function closeLeadModal() {
  const m = document.getElementById('leadModal');
  m.classList.remove('open');
  document.body.style.overflow = '';
}

// Click anywhere on a lead row → open modal (anchors inside row stop propagation)
document.addEventListener('click', e => {
  const row = e.target.closest('.lead-line.clickable');
  if (row && row.dataset.cid) openLeadModal(row.dataset.cid);
  if (e.target.matches('.modal-overlay')) closeLeadModal();
});
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') closeLeadModal();
});

// Buckets sorted by what mattered THIS WEEK. Quiet (no activity, no movement)
// is at the bottom inside a collapsed details element so it doesn't bury the signal.
const BUCKET_DEFS = [
  {key:'ready_contract', title:'Ready for Contract',  tone:'green', num:'01', desc:'In MAO + Hot + ARV + positive spread. Send the contract.'},
  {key:'ready_mao',      title:'Ready for MAO Offer', tone:'green', num:'02', desc:'In Due Diligence + Hot + ARV. Time to put a number on it.'},
  {key:'advanced',       title:'Moved Forward',       tone:'green', num:'03', desc:'Stage advanced this week.', showMove: true},
  {key:'new',            title:'New This Week',       tone:'warm',  num:'04', desc:'First appearance in the pipeline.'},
  {key:'active_no_move', title:'Active — No Stage Move', tone:'warm', num:'05', desc:'Same stage, but had SMS / reply / Slack activity this week. Keep warm.'},
  {key:'demoted',        title:'Moved Backward',      tone:'gray',  num:'06', desc:'Auto-routed to Unqualified or downgraded.', showMove: true},
  {key:'drop_suggest',   title:'Drop Suggestions',    tone:'gray',  num:'07', desc:'Cold/Nurture sitting in active stages > 30 days.'},
];
// Quiet is rendered separately, inside a <details> collapsed by default
const QUIET_DEF = {key:'quiet', title:'Quiet — No Activity This Week', tone:'gray', num:'∅',
                   desc:'Leads in the pipeline but no SMS, reply, Slack mention, or stage move in the past 7 days. Click to expand.'};

// Active stage filter (set by chip clicks). 'all' = no filter.
let stageFilter = 'all';

function leadMatchesStage(l) {
  if (stageFilter === 'all') return true;
  return (l.stage_label || '').trim() === stageFilter;
}

function escapeHtml(s) {
  return (s || '').replace(/[&<>"']/g, ch => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  }[ch]));
}

function renderSlack(week) {
  const items = week.slack_mentions || [];
  if (!items.length) return '';
  const filtered = items.filter(s => stageFilter === 'all' || s.lead_stage === stageFilter);
  if (!filtered.length) return '';
  let html = '<div class="bucket warm slack-list">';
  html += '<div class="head"><span class="num-big">📡</span><h3>Slack Mentions This Week</h3>';
  html += '<span class="ct">' + filtered.length + ' mention' + (filtered.length===1?'':'s') + '</span></div>';
  html += '<div class="body">';
  html += '<div class="lead-line" style="background:rgba(0,0,0,0.02);font-style:italic;color:var(--ink-soft);grid-template-columns:1fr;"><div>Lead mentions captured from APG Slack channels this week. Each card links to the GHL contact and to the original Slack message.</div></div>';
  for (const s of filtered) {
    const ghl = GHL_BASE + '/' + s.cid;
    const slackBtn = s.permalink
      ? '<a class="ghl-link" href="' + escapeHtml(s.permalink) + '" target="_blank">Open in Slack</a>'
      : '<span class="channel-tag">#' + escapeHtml(s.channel || '?') + '</span>';
    const ghlBtn = '<a class="ghl-link" href="' + ghl + '" target="_blank">Open GHL</a>';
    html += '<div class="lead-line">' +
      '<div class="who">' +
      '<div class="nm"><a href="' + ghl + '" target="_blank">' + escapeHtml(s.lead_name || '(no name)') + '</a></div>' +
      '<div class="pl">' + escapeHtml((s.lead_addr ? s.lead_addr + ' · ' : '') + (s.lead_place || '')) +
        ' · <span class="channel-tag">#' + escapeHtml(s.channel || '?') + '</span></div>' +
      (s.original ? '<div class="excerpt">"' + escapeHtml(s.original) + '"</div>' : '') +
      (s.summary  ? '<div class="excerpt" style="border-left-color:var(--gold-deep)">' + escapeHtml(s.summary) + '</div>' : '') +
      '</div>' +
      slackBtn + ghlBtn + '</div>';
  }
  html += '</div></div>';
  return html;
}

function render(week) {
  const c = document.getElementById('content');
  if (!week) {
    c.innerHTML = '<div class="empty">No analysis for this week.</div>';
    return;
  }
  const totals = week.totals || {};
  const buckets = week.buckets || {};

  let html = '';

  // KPIs — focus on WEEKLY ACTIVITY, not pipeline totals
  html += '<section class="sec">';
  html += '<div class="tag-row"><span class="num">00</span><h2>This Week</h2></div><hr>';
  html += '<div class="stat-row">';
  html += '<div class="stat green"><div class="lab">Active This Week</div><div class="v">' + (totals.active_this_week||0) + '</div><div class="sub">had SMS / reply / Slack</div></div>';
  html += '<div class="stat green"><div class="lab">Stage Moves</div><div class="v">' + ((totals.advanced||0) + (totals.demoted||0)) + '</div><div class="sub">' + (totals.advanced||0) + ' forward · ' + (totals.demoted||0) + ' back</div></div>';
  html += '<div class="stat warm"><div class="lab">New This Week</div><div class="v">' + (totals.new||0) + '</div><div class="sub">added to pipeline</div></div>';
  html += '<div class="stat"><div class="lab">SMS Sent</div><div class="v">' + (totals.sms_sent_week||0) + '</div><div class="sub">' + (totals.replies_week||0) + ' reply' + (totals.replies_week===1?'':'ies') + ' received</div></div>';
  html += '<div class="stat"><div class="lab">Slack Mentions</div><div class="v">' + (totals.slack_mentions||0) + '</div><div class="sub">captured this week</div></div>';
  html += '<div class="stat green"><div class="lab">Ready Contract</div><div class="v">' + (totals.ready_contract||0) + '</div><div class="sub">send it</div></div>';
  html += '</div>';
  html += '<div style="margin-top:10px;font-size:12px;color:var(--ink-mute)">Pipeline total this snapshot: <strong>' + (totals.total_pipeline||0) + '</strong> · Quiet (no activity): <strong>' + (totals.quiet||0) + '</strong></div>';
  html += '</section>';

  // Slack mentions section first (high signal — what the team talked about)
  html += renderSlack(week);

  // Active buckets — apply stage filter to each bucket's items
  for (const def of BUCKET_DEFS) {
    const all = buckets[def.key] || [];
    const items = all.filter(leadMatchesStage);
    if (!items.length) continue;
    html += '<div class="bucket ' + def.tone + '">';
    html += '<div class="head">';
    html += '<span class="num-big">' + def.num + '</span>';
    html += '<h3>' + def.title + '</h3>';
    const filterNote = (stageFilter !== 'all' && all.length !== items.length) ?
        ' <span style="color:var(--ink-mute);font-weight:500">of ' + all.length + '</span>' : '';
    html += '<span class="ct">' + items.length + ' lead' + (items.length===1?'':'s') + filterNote + '</span>';
    html += '</div>';
    html += '<div class="body">';
    if (def.desc) html += '<div class="lead-line desc-row"><div>' + def.desc + '</div></div>';
    for (const l of items) html += renderLead(l, {showMove: def.showMove});
    html += '</div></div>';
  }

  // Quiet bucket — collapsed details so it's findable but not in the way
  const quietAll = buckets['quiet'] || [];
  const quietItems = quietAll.filter(leadMatchesStage);
  if (quietItems.length) {
    html += '<details class="bucket gray quiet-block"><summary>';
    html += '<span class="num-big">∅</span>';
    html += '<span class="quiet-summary-text"><strong>' + QUIET_DEF.title + '</strong>';
    html += ' <span class="ct">' + quietItems.length + ' lead' + (quietItems.length===1?'':'s') + '</span></span>';
    html += '</summary><div class="body">';
    html += '<div class="lead-line desc-row"><div>' + QUIET_DEF.desc + '</div></div>';
    for (const l of quietItems) html += renderLead(l, {});
    html += '</div></details>';
  }

  if (!html.includes('bucket')) html += '<div class="empty">No weekly activity to show. The pipeline is quiet.</div>';

  c.innerHTML = html;
  // Re-run entrance animations on the freshly rendered DOM
  if (window.runEntranceAnimations) window.runEntranceAnimations(c);
}

let _currentWeekData = null;

function applyStageFilter(stage) {
  stageFilter = stage;
  document.querySelectorAll('.stage-filter .filter-chip').forEach(c => {
    c.classList.toggle('active', c.dataset.stage === stage);
  });
  if (_currentWeekData) render(_currentWeekData);
}

document.querySelectorAll('.stage-filter .filter-chip').forEach(chip => {
  chip.addEventListener('click', e => applyStageFilter(chip.dataset.stage));
});

// ─────────────── In-page refresh (workflow_dispatch via PAT) ───────────────
const REPO = 'Mid0117/acq-automation';
const PAT_KEY = 'apg_gh_pat_v1';

async function dispatchWorkflow(workflowFile, ref='main', inputs={}) {
  let pat = localStorage.getItem(PAT_KEY);
  if (!pat) {
    pat = prompt(
      'Refreshing requires a GitHub personal access token (one-time setup).\\n\\n' +
      '1. Open: https://github.com/settings/tokens?type=beta\\n' +
      '2. Create a fine-grained token with "Actions: Read & Write" on Mid0117/acq-automation\\n' +
      '3. Paste it here. Stored only in your browser.'
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

async function pollLatestRun(workflowName, sinceMs, maxMs=180000) {
  const pat = localStorage.getItem(PAT_KEY);
  const url = 'https://api.github.com/repos/' + REPO + '/actions/runs?per_page=10';
  const start = Date.now();
  while (Date.now() - start < maxMs) {
    const r = await fetch(url, {
      headers: {
        'Authorization': 'Bearer ' + pat,
        'Accept': 'application/vnd.github+json',
      }
    });
    if (r.ok) {
      const js = await r.json();
      const run = (js.workflow_runs || []).find(rr =>
        rr.name === workflowName && new Date(rr.created_at).getTime() > sinceMs - 5000
      );
      if (run) {
        if (run.status === 'completed') return run;
      }
    }
    await new Promise(res => setTimeout(res, 4000));
  }
  return null;
}

document.getElementById('refreshBtn').addEventListener('click', async () => {
  const btn   = document.getElementById('refreshBtn');
  const icon  = document.getElementById('refreshIcon');
  const label = document.getElementById('refreshLabel');
  const stat  = document.getElementById('refreshStatus');
  btn.disabled = true;
  icon.outerHTML = '<span class="spin" id="refreshIcon"></span>';
  label.textContent = 'Triggering…';
  stat.textContent  = '';
  const startedAt = Date.now();
  try {
    await dispatchWorkflow('weekly.yml');
    label.textContent = 'Running…';
    stat.textContent  = 'Snapshot in progress (typically ~1 min)';
    const run = await pollLatestRun('Weekly Analysis', startedAt);
    if (run && run.conclusion === 'success') {
      label.textContent = 'Done — reloading';
      stat.textContent  = 'Fresh snapshot ready.';
      setTimeout(() => location.reload(), 800);
    } else {
      label.textContent = 'Refresh';
      stat.textContent  = 'Workflow may still be running. Refresh manually in a minute.';
    }
  } catch (e) {
    label.textContent = 'Refresh';
    stat.textContent = '⚠ ' + (e.message || e);
  } finally {
    document.getElementById('refreshBtn').disabled = false;
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
    document.getElementById('content').innerHTML = '<div class="empty">No weekly analysis has run yet. The first one fires Friday 10 AM ET.</div>';
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
      document.getElementById('weekMeta').textContent = 'Generated ' + (data.generated_at_et || '');
      render(data);
    } catch(e) {
      _currentWeekData = null;
      document.getElementById('content').innerHTML = '<div class="empty">Failed to load this week\\'s data.</div>';
    }
  }

  sel.addEventListener('change', e => loadWeek(e.target.value));
  if (weeks.length) loadWeek(weeks[0]);
  else document.getElementById('content').innerHTML = '<div class="empty">No weeks available yet.</div>';
}

main();

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
        movement, meta, action_tags = categorize(c, prev, week_start_iso, slack_count_by_cid.get(cid, 0))
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
    print(f'  Total: {totals["total"]}')
    print(f'  Advanced: {totals["advanced"]} | Demoted: {totals["demoted"]} | New: {totals["new"]}')
    print(f'  Stagnant active: {totals["stagnant_active"]} | Stagnant cold: {totals["stagnant_inactive"]}')
    print(f'  Ready contract: {totals["ready_contract"]} | Ready MAO: {totals["ready_mao"]} | Drop suggest: {totals["drop_suggest"]}')


if __name__ == '__main__':
    main()
