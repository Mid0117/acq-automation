"""
Slack scraper — pulls messages from configured APG channels, matches each
against the GHL contact list, and uses Claude to extract lead info that's not
yet captured. Adds a 'Slack mention' note on the contact, and (when Claude is
highly confident) auto-updates select custom fields.

Runs on its own cron, separate from the SMS cron.

Channels monitored: base0-hot-warm-nurture-fu, base1-sms-leadgen, base4-dispo,
construction-services. Bot must be invited to each.

State file: slack_state.json — tracks last processed message ts per channel.
"""
import os, json, re, requests, time
from datetime import datetime, timezone

# Default 30s timeout for any bare requests.* call that didn't specify one.
_orig_session_request = requests.Session.request
def _request_with_default_timeout(self, method, url, **kwargs):
    kwargs.setdefault('timeout', 30)
    return _orig_session_request(self, method, url, **kwargs)
requests.Session.request = _request_with_default_timeout


def _write_status(success, summary='', error=''):
    try:
        with open('last_run_slack.json', 'w') as f:
            json.dump({'success': success,
                       'timestamp': datetime.now(timezone.utc).isoformat(),
                       'summary': summary,
                       'error': error[:500]}, f, indent=2)
    except Exception:
        pass

GHL_TOKEN     = os.environ['GHL_TOKEN']
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
SLACK_TOKEN   = os.environ.get('SLACK_BOT_TOKEN', '')
LOC           = 'RCkiUmWqXX4BYQ39JXmm'
STATE_FILE    = 'slack_state.json'

# Pipelines we care about (any active deal across these)
PIPELINES = ['O8wzIa6E3SgD8HLg6gh9', 'OewETJqBPEEJHhrCBXAh',
             'rHGqnAGIt5OC5CQzgSNm', 'tuOWYmvCgjxfYAwNqq0v']

CHANNELS = ['base0-hot-warm-nurture-fu', 'base1-sms-leadgen',
            'base4-dispo', 'construction-services']

# How far back to look on first run (in days). After that, only messages since last run.
BACKFILL_DAYS = 30

GHL_H = {'Authorization': f'Bearer {GHL_TOKEN}',
         'Content-Type': 'application/json', 'Version': '2021-07-28'}
SLACK_AUTH = {'Authorization': f'Bearer {SLACK_TOKEN}'}
SLACK_H_JSON = {'Authorization': f'Bearer {SLACK_TOKEN}', 'Content-Type': 'application/json'}

# Custom field IDs that Claude is allowed to auto-update from Slack mentions
CF_ASK_PRICE   = '6q7syt4puxfP7E03Xxhd'  # Asking Price
CF_TIMELINE    = 'v47I1Mi63RBpCD5N5RrH'  # Timeline to Sell
CF_MOTIVATION  = 'rbYZAdhvuvX1NQgexhxy'  # Motivation
CF_REASON_SELL = 'cJdRGRoox0RZCytRAVSI'  # Reason for selling
CF_VA_NOTES    = 'ctNVXVw8VY1PD4B1oqXj'

ALLOWED_FIELDS = {
    'asking_price':       CF_ASK_PRICE,
    'timeline':           CF_TIMELINE,
    'motivation':         CF_MOTIVATION,
    'reason_for_selling': CF_REASON_SELL,
}


def load_state():
    if os.path.exists(STATE_FILE):
        return json.load(open(STATE_FILE))
    return {}


def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2, sort_keys=True)


def slack_call(method, params=None, json_body=None):
    if not SLACK_TOKEN:
        return None
    url = f'https://slack.com/api/{method}'
    if json_body is not None:
        r = requests.post(url, headers=SLACK_H_JSON, json=json_body, timeout=20)
    else:
        # GET — no Content-Type or Slack may mishandle the request
        r = requests.get(url, headers=SLACK_AUTH, params=params or {}, timeout=20)
    if r.status_code != 200:
        return None
    j = r.json()
    if not j.get('ok'):
        print(f'  Slack {method} error: {j.get("error")}')
        return None
    return j


def list_channel_ids():
    """Return a {name: id} map for all public+private channels the bot can see."""
    ids = {}
    for ctype in ('public_channel', 'private_channel'):
        cursor = ''
        while True:
            params = {'types': ctype, 'limit': 1000}
            if cursor:
                params['cursor'] = cursor
            j = slack_call('conversations.list', params=params)
            if not j: break
            for c in j.get('channels', []):
                ids[c['name']] = c['id']
            cursor = (j.get('response_metadata') or {}).get('next_cursor', '')
            if not cursor:
                break
    return ids


def fetch_messages(channel_id, oldest_ts):
    """Pull messages newer than oldest_ts (Slack timestamp string).

    Slack's May 2025 change: non-Marketplace apps get capped at limit=15 and
    severely throttled on conversations.history. We use limit=15 + sleep
    between calls. A single channel may take a while during backfill but
    the cron only runs the delta each hour going forward.
    """
    msgs, cursor = [], ''
    pages = 0
    while True:
        params = {'channel': channel_id, 'limit': 15}
        if oldest_ts:
            params['oldest'] = oldest_ts
        if cursor:
            params['cursor'] = cursor
        j = slack_call('conversations.history', params=params)
        if not j:
            break
        for m in j.get('messages', []):
            if m.get('subtype') in ('channel_join', 'channel_leave', 'bot_message'):
                continue
            if not m.get('text'):
                continue
            msgs.append({'ts': m['ts'], 'user': m.get('user', ''),
                         'text': m['text']})
        cursor = (j.get('response_metadata') or {}).get('next_cursor', '')
        pages += 1
        if not cursor:
            break
        # Throttle — Slack limits to ~1 req/min on non-Marketplace apps
        time.sleep(2)
        # Safety cap so a single channel can't run the cron over budget
        if pages >= 200:
            break
    return msgs


def fetch_active_contacts():
    """Lightweight index — just cid + display name + phone from the opportunity
    search response. No per-contact GET (which times out at scale). We enrich
    only the candidates that actually matched a Slack message."""
    contacts = {}
    for pipe in PIPELINES:
        page = 1
        while True:
            try:
                r = requests.get('https://services.leadconnectorhq.com/opportunities/search',
                                 headers=GHL_H,
                                 params={'location_id': LOC, 'pipeline_id': pipe,
                                         'limit': 100, 'page': page}, timeout=30)
            except requests.exceptions.Timeout:
                break
            if r.status_code != 200: break
            opps = r.json().get('opportunities', [])
            if not opps: break
            for o in opps:
                cid = o.get('contactId')
                c   = o.get('contact') or {}
                if not cid or cid in contacts:
                    continue
                full = (c.get('name') or '').strip()
                parts = full.split(' ', 1)
                contacts[cid] = {
                    'cid':       cid,
                    'name':      full,
                    'firstName': parts[0] if parts else '',
                    'lastName':  parts[1] if len(parts) > 1 else '',
                    'phone':     re.sub(r'\D','', c.get('phone','') or ''),
                }
            if len(opps) < 100: break
            page += 1
            time.sleep(0.1)
    return list(contacts.values())


def enrich_contact(c):
    """Fetch full contact details only when needed (after a candidate match)."""
    cid = c['cid']
    for attempt in range(2):
        try:
            r = requests.get(f'https://services.leadconnectorhq.com/contacts/{cid}',
                             headers=GHL_H, timeout=20)
            if r.status_code == 200:
                cc = r.json().get('contact', {})
                c['firstName'] = cc.get('firstName','') or c.get('firstName','')
                c['lastName']  = cc.get('lastName','')  or c.get('lastName','')
                c['address1']  = cc.get('address1','')
                c['city']      = cc.get('city','')
                c['state']     = cc.get('state','')
                c['phone']     = re.sub(r'\D','', cc.get('phone','') or '') or c['phone']
            return c
        except requests.exceptions.RequestException:
            if attempt == 0:
                time.sleep(1)
                continue
    return c


def candidates_for_message(text, contacts):
    """Local keyword match using the lightweight index. Address checks happen
    after enrichment in the calling code, only on hits."""
    t = text.lower()
    digits = re.sub(r'\D','', text)
    cands = []
    for c in contacts:
        first = (c.get('firstName') or '').lower()
        last  = (c.get('lastName') or '').lower()
        full  = (c.get('name') or '').lower()
        phone = (c.get('phone') or '')
        score = 0
        if full and len(full) >= 5 and full in t: score += 3
        if first and len(first) >= 3 and first in t: score += 1
        if last  and len(last)  >= 3 and last  in t: score += 1
        if first and last and f'{first} {last}' in t: score += 2
        if phone and len(phone) >= 7 and phone[-7:] in digits: score += 3
        if score >= 2:
            cands.append((score, c))
    cands.sort(key=lambda x: -x[0])
    return [c for _, c in cands[:5]]


CLAUDE_SYSTEM = """You analyze a Slack message from a real estate investor's team chat
to determine whether it provides actionable info about a specific lead in their CRM.

You'll receive: the Slack message, plus 1-5 candidate leads it might reference.

Return ONLY valid JSON:
{
  "match_cid": <the contactId of the lead this message is about, or null if no clear match>,
  "match_confidence": "high" | "medium" | "low" | "none",
  "summary": <one short sentence summarizing what the message says about the lead>,
  "field_updates": {
    "asking_price": <int or null>,
    "timeline": <"ASAP"|"30 days"|"60 days"|"90 days"|"6+ months"|"No rush"|null>,
    "motivation": <short string or null>,
    "reason_for_selling": <one-sentence specific reason or null>
  }
}

Only set match_confidence to "high" when the message clearly identifies one specific lead by
name, address, or phone AND contains substantive info worth recording. Otherwise use lower
confidence levels. Set field_updates values only when you're highly confident the message
states a NEW value for that field — never guess."""


def analyze_with_claude(msg_text, slack_user, channel, candidates):
    if not (ANTHROPIC_KEY and candidates):
        return None
    cand_summary = '\n'.join(
        f'- cid={c["cid"]} | name={c.get("firstName","")} {c.get("lastName","")} | '
        f'addr={c.get("address1","")} | phone={c.get("phone","")[-7:] if c.get("phone") else ""}'
        for c in candidates
    )
    user_msg = f"""SLACK MESSAGE (from #{channel} by {slack_user}):
\"\"\"
{msg_text}
\"\"\"

CANDIDATE LEADS (one of these MAY be the subject):
{cand_summary}

Return JSON per system instructions."""
    body = {'model': 'claude-sonnet-4-5', 'max_tokens': 800,
            'system': CLAUDE_SYSTEM,
            'messages': [{'role':'user','content':user_msg}]}
    headers = {'x-api-key': ANTHROPIC_KEY, 'anthropic-version':'2023-06-01',
               'content-type':'application/json'}
    try:
        r = requests.post('https://api.anthropic.com/v1/messages', headers=headers, json=body, timeout=60)
        if r.status_code != 200:
            print(f'  ⚠ Claude API {r.status_code}: {r.text[:300]}')
            return None
        text = r.json()['content'][0]['text'].strip()
        if text.startswith('```'):
            text = text.split('```',2)[1]
            if text.startswith('json'): text = text[4:]
            text = text.strip()
        return json.loads(text)
    except Exception as e:
        print(f'  ⚠ Claude exception: {e}')
        return None


def slack_permalink(channel_id, ts):
    """Get a real Slack permalink for a message via chat.getPermalink. Returns
    empty string if anything fails (the note is still useful without it)."""
    try:
        j = slack_call('chat.getPermalink', params={'channel': channel_id, 'message_ts': ts})
        if j and j.get('permalink'):
            return j['permalink']
    except Exception:
        pass
    return ''


def add_note(cid, body):
    try:
        requests.post(f'https://services.leadconnectorhq.com/contacts/{cid}/notes',
                      headers=GHL_H, json={'body': body}, timeout=15)
    except Exception:
        pass


def update_fields(cid, field_updates):
    """Apply allowed field updates to a contact. Returns dict of changes made."""
    changed = {}
    payload = []
    for key, value in (field_updates or {}).items():
        if value in (None, '', 'null'):
            continue
        if key not in ALLOWED_FIELDS:
            continue
        payload.append({'id': ALLOWED_FIELDS[key], 'field_value': str(value)})
        changed[key] = value
    if payload:
        try:
            requests.put(f'https://services.leadconnectorhq.com/contacts/{cid}',
                         headers=GHL_H, json={'customFields': payload}, timeout=15)
        except Exception:
            pass
    return changed


def fmt_slack_ts(ts):
    try:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        return dt.strftime('%b %d, %Y %I:%M %p UTC')
    except Exception:
        return ts


def main():
    try:
        summary = _main_inner()
        _write_status(True, summary or '')
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f'!! Slack scrape failed: {e}\n{tb}')
        _write_status(False, '', f'{e}: {tb[-300:]}')
        raise


def _main_inner():
    if not SLACK_TOKEN:
        print('SLACK_BOT_TOKEN not set — exiting.')
        return 'SLACK_BOT_TOKEN missing'
    if not ANTHROPIC_KEY:
        print('ANTHROPIC_API_KEY not set — exiting.')
        return 'ANTHROPIC_API_KEY missing'

    state = load_state()
    print('Resolving channel IDs...')
    chan_ids = list_channel_ids()
    print(f'  Visible channels: {len(chan_ids)}')
    missing = [c for c in CHANNELS if c not in chan_ids]
    if missing:
        print(f'  ⚠ Bot not in or cannot see: {", ".join(missing)}')
        print('  Make sure the bot is invited via /invite @APG Automations in each channel.')

    print('\nLoading contacts from GHL pipelines...')
    contacts = fetch_active_contacts()
    print(f'  {len(contacts)} active contacts indexed')

    matches = updates_made = scanned = 0
    backfill_oldest = str(time.time() - BACKFILL_DAYS * 86400)

    for ch_name in CHANNELS:
        if ch_name not in chan_ids:
            continue
        ch_id = chan_ids[ch_name]
        oldest = state.get(ch_name) or backfill_oldest
        print(f'\n#{ch_name}: pulling messages since {fmt_slack_ts(oldest)}...')
        msgs = fetch_messages(ch_id, oldest)
        print(f'  {len(msgs)} new messages')
        if not msgs:
            continue

        latest_ts = state.get(ch_name) or oldest
        for m in msgs:
            scanned += 1
            cands = candidates_for_message(m['text'], contacts)
            if not cands:
                if float(m['ts']) > float(latest_ts): latest_ts = m['ts']
                continue
            # Enrich candidates with full contact details (only here — not for all 2400)
            enriched = [enrich_contact(dict(c)) for c in cands]
            result = analyze_with_claude(m['text'], m.get('user',''), ch_name, enriched)
            if result and result.get('match_cid') and result.get('match_confidence') in ('high','medium'):
                cid = result['match_cid']
                summary = result.get('summary','')
                changed = {}
                if result.get('match_confidence') == 'high':
                    changed = update_fields(cid, result.get('field_updates') or {})
                # Note with audit trail — include real Slack permalink so the
                # weekly dashboard can link straight to the original message.
                permalink = slack_permalink(ch_id, m['ts'])
                permalink_line = f'\nSlack: {permalink}' if permalink else ''
                note_body = (
                    f'Slack mention\n'
                    f'#{ch_name} by <@{m.get("user","")}> — {fmt_slack_ts(m["ts"])}'
                    f'{permalink_line}\n'
                    f'Confidence: {result.get("match_confidence")}\n\n'
                    f'Original: "{m["text"][:600]}"\n\n'
                    f'Summary: {summary}'
                )
                if changed:
                    note_body += f'\n\nFields auto-updated: {", ".join(f"{k}={v}" for k,v in changed.items())}'
                add_note(cid, note_body)
                matches += 1
                if changed: updates_made += 1
                print(f'  ✓ Matched cid={cid[-6:]} | {result.get("match_confidence")} | changes={list(changed.keys())}')
            if float(m['ts']) > float(latest_ts): latest_ts = m['ts']
            time.sleep(0.2)
        state[ch_name] = latest_ts

    save_state(state)
    msg = f'scanned {scanned} | matched {matches} | updates {updates_made}'
    print(f'\nDONE — {msg}')
    return msg


if __name__ == '__main__':
    main()
