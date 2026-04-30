"""
ACQ Pipeline SMS Follow-Up Automation
Runs every 30 min via GitHub Actions cron.

For each contact in ACQ pipeline stages 1-4:
- Tracks per-contact SMS state in sms_state.json
- Sends next SMS in sequence (7-day intervals, 6 touches over 6 weeks)
- Routes from-number based on contact's state (primary for 1-3, secondary for 4-6)
- Polls for replies; on reply: stops sequence, tags, creates Jeff task + Mike review task
- After 6 SMS no reply: marks dormant, creates manual-call task
"""
import json, os, re, requests, time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo('America/New_York')

GHL_TOKEN     = os.environ['GHL_TOKEN']
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
GHL_LOCATION  = 'RCkiUmWqXX4BYQ39JXmm'
PIPELINE_ID   = 'O8wzIa6E3SgD8HLg6gh9'
STATE_FILE    = 'sms_state.json'
CONTACTS_CACHE = 'contacts_cache.json'
STATUS_FILE   = 'last_run_sms.json'
SHEET_ID      = os.environ.get('DASHBOARD_SHEET_ID', '')
SLACK_WEBHOOK = os.environ.get('SLACK_WEBHOOK_URL', '')

GHL_H = {'Authorization': f'Bearer {GHL_TOKEN}',
         'Content-Type': 'application/json', 'Version': '2021-07-28'}

# Network defaults — every request gets a timeout + one retry on transient errors
HTTP_TIMEOUT = 30


def http(method, url, **kw):
    """Wrapped requests with timeout + one retry on connection/5xx errors."""
    kw.setdefault('timeout', HTTP_TIMEOUT)
    for attempt in range(2):
        try:
            r = requests.request(method, url, **kw)
            if r.status_code >= 500 and attempt == 0:
                time.sleep(1.0)
                continue
            return r
        except (requests.Timeout, requests.ConnectionError):
            if attempt == 0:
                time.sleep(1.0)
                continue
            raise

# Active deal stages (high-engagement 7-day cadence, 6 touches)
STAGE_QUALIFIED = 'a17517be-8d1a-49fd-bd53-b9128a66e242'
STAGE_LAO       = 'd43fddd8-3a17-46b2-a193-cf18619f654f'
STAGE_RR        = '23a159ad-ba39-4c74-9d07-c1beb219d9f2'
STAGE_MAO       = '43589167-14f0-4e09-ba2a-8b9bd3296a4a'
# Re-engagement stages (slow cadence, low-pressure tone)
STAGE_FU_15MO   = '4aa78ab3-85dc-46d1-a683-d97b0c7a23ee'  # Follow Up (1.5 month)
STAGE_FU_3MO    = '571c115e-2603-4f3f-8546-d716f44ba8ef'  # Follow Up (3 months)
STAGE_DEAD      = 'b9b560b0-30cb-47fc-a4ca-1e55ca2531e2'  # Dead Deals

STAGE_NAMES = {
    STAGE_QUALIFIED: 'qualified',
    STAGE_LAO:       'lao',
    STAGE_RR:        'rr',
    STAGE_MAO:       'mao',
    STAGE_FU_15MO:   'fu15mo',
    STAGE_FU_3MO:    'fu3mo',
    STAGE_DEAD:      'dead',
}
ACTIVE_STAGES = set(STAGE_NAMES.keys())

# Per-stage cadence / behavior
STAGE_CONFIG = {
    'qualified': {'interval_days': 7,   'max_touches': 6, 'secondary_after': 3, 'dormant_wait': 3},
    'lao':       {'interval_days': 7,   'max_touches': 6, 'secondary_after': 3, 'dormant_wait': 3},
    'rr':        {'interval_days': 7,   'max_touches': 6, 'secondary_after': 3, 'dormant_wait': 3},
    'mao':       {'interval_days': 7,   'max_touches': 6, 'secondary_after': 3, 'dormant_wait': 3},
    'fu15mo':    {'interval_days': 30,  'max_touches': 3, 'secondary_after': 999, 'dormant_wait': 14},
    'fu3mo':     {'interval_days': 60,  'max_touches': 3, 'secondary_after': 999, 'dormant_wait': 14},
    'dead':      {'interval_days': 180, 'max_touches': 3, 'secondary_after': 999, 'dormant_wait': 30},
}

# GHL user IDs
USER_JEFF = 'vDKOqPSkA8nLkia5skd0'
USER_MIKE = 'Vj4WwH1ovxGN5Hv5Kq17'

# Phone routing
JEFF_NJ        = '+16094388996'
NJ_SECONDARY   = '+12676197270'  # PA Market
STATE_PRIMARY = {
    'AL': '+12568006289', 'GA': '+14707508168',
    'IN': '+12603193698', 'OH': '+14406169376',
    'PA': '+12676197270', 'SC': '+18037843538',
    'TN': '+19013138258', 'WI': '+14143489182',
}
STATE_SECONDARY = {
    'AL': '+19013138258',  # TN
    'GA': '+18037843538',  # SC
    'IN': '+14406169376',  # OH
    'OH': '+12603193698',  # IN
    'PA': '+16094388996',  # Jeff NJ
    'SC': '+14707508168',  # GA
    'TN': '+12568006289',  # AL
    'WI': '+12603193698',  # IN
}

# SMS templates: 6 per stage. Index 0-2 = primary number, 3-5 = secondary.
TEMPLATES = {
    'qualified': [
        "Hey {first_name}, this is Jeff with APG — circling back on {address1}. Still thinking about selling, or did things shift on your end?",
        "Hey {first_name}, checking back on {address1}. Anything you wanted to think over before we kept the conversation going?",
        "{first_name} — last one on this. If {address1} is still something you'd sell, reply Y. If not, no problem and I'll stop reaching out.",
        "Hey {first_name}, Jeff again from APG. Switched numbers in case the last one wasn't reaching you. You still considering selling {address1}?",
        "{first_name}, just wanted to check one more time — any update on {address1}? Quick yes-or-no works for me.",
        "{first_name} — final attempt. Reply Y if still on the table for {address1}, or I'll mark this closed on our end. Either way is fine.",
    ],
    'lao': [
        "Hey {first_name}, Jeff at APG. Just making sure our offer on {address1} made it to you. Any thoughts?",
        "{first_name}, did the number we sent for {address1} work for what you had in mind? Happy to hear your feedback.",
        "{first_name} — final check on {address1}. Reply Y to revisit, N to pass. No hard feelings either way.",
        "Hey {first_name}, switched numbers — wanted to make sure our offer on {address1} got through. Any thoughts?",
        "{first_name}, just one more nudge on the {address1} offer. Y or N works for me.",
        "{first_name} — last attempt. If the {address1} offer is worth revisiting, reply Y. Otherwise I'll close it on our end.",
    ],
    'rr': [
        "Hey {first_name}, Jeff at APG. Wrapping up our review on {address1} this week. Anything we should know on your end?",
        "{first_name} — any new info from your end on {address1}? Want to make sure we have the full picture.",
        "{first_name}, let me know if you've heard from anyone else on {address1} — just keeping us aligned.",
        "Hey {first_name}, Jeff here. Switched numbers — we're closing in on review for {address1}. Quick update?",
        "{first_name}, anything I should know before we finalize on {address1}?",
        "{first_name} — last check before we close out review on {address1}. All good on your end?",
    ],
    'mao': [
        "Hey {first_name}, Jeff with APG. Final number on {address1} is in your court. Want to grab a quick call to walk through it?",
        "{first_name}, anything I can answer on the {address1} offer? Happy to adjust if there's something specific.",
        "{first_name} — last check on {address1}. Reply Y to keep moving, N to pass. All good either way.",
        "Hey {first_name}, Jeff. Different number — wanted to make sure our final number on {address1} got to you.",
        "{first_name}, any final thoughts on the {address1} number? Either way works for me.",
        "{first_name} — last attempt on {address1}. Y to move forward, N to pass. No hard feelings.",
    ],
    # Re-engagement: low-pressure, calm, "checking in" tone
    'fu15mo': [
        "Hey {first_name}, Jeff with APG. Wanted to circle back on {address1} — anything new on your end?",
        "{first_name}, just checking in on {address1}. Door's still open whenever you're ready to talk.",
        "{first_name} — last touch on {address1}. If anything's changed, just shoot me a text. Otherwise no worries.",
    ],
    'fu3mo': [
        "Hey {first_name}, Jeff with APG. It's been a few months — anything change with {address1}?",
        "{first_name}, just keeping in touch on {address1}. Always here if anything shifts.",
        "{first_name} — quick check on {address1}. Reply if there's anything to revisit, otherwise all good.",
    ],
    'dead': [
        "Hey {first_name} — Jeff with APG. It's been a while. If anything's ever changed with {address1} and you'd consider selling, just let me know. No pressure either way.",
        "{first_name}, Jeff again. Just a quick check on {address1} — sometimes life shifts. If you're ever curious about a number, I'm here.",
        "{first_name} — last reach-out on {address1}. If anything's in the air, you know where to find me.",
    ],
}


def load_state():
    if os.path.exists(STATE_FILE):
        return json.load(open(STATE_FILE))
    return {}


def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2, sort_keys=True)


def now_utc():
    return datetime.now(timezone.utc)


def parse_iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    except Exception:
        return None


def days_since(iso):
    dt = parse_iso(iso)
    if not dt:
        return None
    return (now_utc() - dt).total_seconds() / 86400.0


def from_number_for(state_code, sms_index, stage_name):
    """Pick from-number based on contact's state and where in the sequence we are."""
    s = (state_code or '').strip().upper()
    cfg = STAGE_CONFIG.get(stage_name, STAGE_CONFIG['qualified'])
    if sms_index < cfg['secondary_after']:
        return STATE_PRIMARY.get(s, JEFF_NJ)
    return STATE_SECONDARY.get(s, NJ_SECONDARY)


def fetch_active_leads():
    """Query each active stage server-side. GHL's pagination 'total' field is unreliable
    (returns 0 even when there are 400+ opps), so we filter by stage at fetch time."""
    entries = []
    for stage_id, stage_name in STAGE_NAMES.items():
        page = 1
        while True:
            r = http('GET', 'https://services.leadconnectorhq.com/opportunities/search',
                     headers=GHL_H,
                     params={'location_id': GHL_LOCATION, 'pipeline_id': PIPELINE_ID,
                             'pipeline_stage_id': stage_id,
                             'limit': 100, 'page': page})
            if r.status_code != 200:
                break
            opps = r.json().get('opportunities', [])
            if not opps:
                break
            for o in opps:
                c = o.get('contact') or {}
                if 'agent' not in c.get('tags', []) and o.get('contactId'):
                    entries.append({'cid': o['contactId'], 'oid': o['id'],
                                    'stage': stage_id, 'stage_name': stage_name})
            if len(opps) < 100:
                break
            page += 1
            time.sleep(0.15)
    return entries


def get_contact(cid):
    r = http('GET', f'https://services.leadconnectorhq.com/contacts/{cid}', headers=GHL_H)
    if r.status_code != 200:
        return None
    return r.json().get('contact')


def has_inbound_since(contact_id, after_iso):
    """Look for any inbound message from contact after the given timestamp.

    Returns (replied: bool, when_iso: str|None, text: str|None) so the caller
    can classify the reply (negative/positive/wrong-number) before flagging Jeff.
    """
    after = parse_iso(after_iso)
    if not after:
        return False, None, None
    r = http('GET', 'https://services.leadconnectorhq.com/conversations/search',
             headers=GHL_H,
             params={'locationId': GHL_LOCATION, 'contactId': contact_id, 'limit': 5})
    if r.status_code != 200:
        return False, None, None
    convs = r.json().get('conversations', [])
    for conv in convs:
        cid = conv.get('id')
        if not cid:
            continue
        rm = http('GET', f'https://services.leadconnectorhq.com/conversations/{cid}/messages',
                  headers=GHL_H, params={'limit': 50})
        if rm.status_code != 200:
            continue
        msgs = (rm.json().get('messages') or {}).get('messages', [])
        for m in msgs:
            if m.get('direction') == 'inbound':
                msg_dt = parse_iso(m.get('dateAdded', ''))
                if msg_dt and msg_dt > after:
                    return True, msg_dt.isoformat(), (m.get('body') or m.get('message') or '').strip()
    return False, None, None


# ── Reply classifier ─────────────────────────────────────────────────────────
# Hard-stop keywords trigger DND immediately without an LLM call (cheap + fast).
HARD_STOP_RE = re.compile(
    r'\b(stop|stopall|unsubscribe|cancel|end|quit|remove\s*me|opt\s*out|leave\s*me\s*alone)\b',
    re.IGNORECASE)
WRONG_NUMBER_RE = re.compile(
    r'\b(wrong\s*number|not\s*me|no\s*such\s*person|never\s*owned|don.?t\s*own)\b',
    re.IGNORECASE)
HARD_NEG_RE = re.compile(
    r'(f[\*u]+ck|piss\s*off|go\s*to\s*hell|don.?t\s*(text|message|contact|call)\s*me|'
    r'do\s*not\s*(text|message|contact|call)\s*me|harass|sue\s*you|lawyer|attorney|tcpa)',
    re.IGNORECASE)


CLASSIFY_SYSTEM = """You classify a one-line SMS reply from a homeowner to a real estate investor's outreach.

Return ONLY one of these tokens, nothing else:
- NEGATIVE   — they're declining, not interested, annoyed, but not legally hostile (e.g. "no thanks", "not selling", "leave me alone")
- WRONG      — wrong number / not the owner / never owned this property
- POSITIVE   — interested, wants to talk, asks about offer, gives info
- NEUTRAL    — ambiguous, asking who you are, requesting more info before deciding
- HOSTILE    — threatens legal action, profanity directed at sender, demands stop

Be strict on POSITIVE — only if there's clear interest. Default ambiguity to NEUTRAL."""


def classify_reply(text):
    """Returns one of: HARD_STOP, WRONG, HOSTILE, NEGATIVE, POSITIVE, NEUTRAL.
    Free regex check first; falls back to Claude only when ambiguous."""
    if not text:
        return 'NEUTRAL'
    if HARD_STOP_RE.search(text):
        return 'HARD_STOP'
    if HARD_NEG_RE.search(text):
        return 'HOSTILE'
    if WRONG_NUMBER_RE.search(text):
        return 'WRONG'
    if not ANTHROPIC_KEY:
        # No LLM — be conservative: treat as POSITIVE so Jeff sees it
        return 'POSITIVE'
    try:
        r = http('POST', 'https://api.anthropic.com/v1/messages',
                 headers={'x-api-key': ANTHROPIC_KEY,
                          'anthropic-version': '2023-06-01',
                          'content-type': 'application/json'},
                 json={'model': 'claude-haiku-4-5-20251001',
                       'max_tokens': 8,
                       'system': CLASSIFY_SYSTEM,
                       'messages': [{'role': 'user', 'content': text[:500]}]},
                 timeout=20)
        if r.status_code != 200:
            return 'POSITIVE'  # fail-safe to flag for human
        token = r.json()['content'][0]['text'].strip().upper().split()[0]
        if token in ('NEGATIVE', 'WRONG', 'POSITIVE', 'NEUTRAL', 'HOSTILE'):
            return token
        return 'POSITIVE'
    except Exception:
        return 'POSITIVE'


def set_dnd(contact_id, reason):
    """Set GHL DND flags so we never SMS this contact again."""
    payload = {
        'dnd': True,
        'dndSettings': {
            'SMS':   {'status': 'active', 'message': f'auto: {reason}', 'code': 'opt_out'},
            'Call':  {'status': 'active', 'message': f'auto: {reason}', 'code': 'opt_out'},
            'Email': {'status': 'active', 'message': f'auto: {reason}', 'code': 'opt_out'},
        },
    }
    try:
        http('PUT', f'https://services.leadconnectorhq.com/contacts/{contact_id}',
             headers=GHL_H, json=payload)
    except Exception as e:
        print(f'  set_dnd failed: {e}')


# Append TCPA opt-out language on touches 1, 4, and 6 (every 3rd touch) so we
# stay compliant without making every message look like spam.
def with_tcpa(message, touch_index_zero_based, max_touches):
    one_based = touch_index_zero_based + 1
    if one_based == 1 or one_based == 4 or one_based == max_touches:
        return f'{message}\n\nReply STOP to opt out.'
    return message


def send_sms(contact_id, message, from_number):
    body = {
        'type': 'SMS',
        'contactId': contact_id,
        'message': message,
        'fromNumber': from_number,
    }
    r = http('POST', 'https://services.leadconnectorhq.com/conversations/messages',
             headers=GHL_H, json=body)
    if r.status_code in (200, 201):
        try:
            return True, r.json().get('messageId', '')
        except Exception:
            return True, ''
    return False, f'{r.status_code} {r.text[:200]}'


def add_tag(contact_id, tag):
    try:
        http('POST', f'https://services.leadconnectorhq.com/contacts/{contact_id}/tags',
             headers=GHL_H, json={'tags': [tag]})
    except Exception as e:
        print(f'  tag add failed: {e}')


def slack_post(text):
    if not SLACK_WEBHOOK:
        return
    try:
        requests.post(SLACK_WEBHOOK, json={'text': text}, timeout=10)
    except Exception:
        pass


def create_task(contact_id, user_id, title, body, due_in_days=0):
    due = (now_utc() + timedelta(days=due_in_days)).isoformat()
    try:
        r = http('POST', f'https://services.leadconnectorhq.com/contacts/{contact_id}/tasks',
                 headers=GHL_H,
                 json={'title': title, 'body': body, 'dueDate': due,
                       'completed': False, 'assignedTo': user_id})
        return r.status_code in (200, 201)
    except Exception as e:
        print(f'  task create failed: {e}')
        return False


def process_lead(entry, contact, state):
    cid = entry['cid']
    stage_name = entry['stage_name']

    # state for this contact (init or pull)
    cs = state.setdefault(cid, {})

    # If stage changed since last run, reset SMS sequence for the new stage
    if cs.get('stage_name') != stage_name:
        cs.update({
            'stage_name':       stage_name,
            'stage_entered_at': now_utc().isoformat(),
            'sms_count':        0,
            'last_sms_at':      None,
            'last_from_number': None,
            'replied':          False,
            'replied_at':       None,
            'dormant':          False,
        })

    # Skip if already replied or dormant
    if cs.get('replied') or cs.get('dormant'):
        return 'skipped'

    # Respect DND — don't text people who opted out
    dnd_settings = contact.get('dndSettings') or {}
    sms_dnd = (dnd_settings.get('SMS') or {}).get('status') == 'active'
    if contact.get('dnd') or sms_dnd:
        cs['dormant'] = True   # treat DND as terminal — no point retrying
        cs['dnd'] = True
        add_tag(cid, 'dormant-sms-dnd')
        return 'dnd'

    # Don't SMS to a contact without a phone number
    if not (contact.get('phone') or '').strip():
        return 'no-phone'

    name  = f"{contact.get('firstName','')} {contact.get('lastName','')}".strip()
    addr1 = (contact.get('address1') or '').strip()

    # Reply detection — only if at least one SMS sent
    if cs.get('sms_count', 0) > 0:
        anchor = cs.get('last_sms_at') or cs.get('stage_entered_at')
        replied, when, reply_text = has_inbound_since(cid, anchor)
        if replied:
            cs['replied']      = True
            cs['replied_at']   = when
            cs['reply_text']   = (reply_text or '')[:500]
            verdict = classify_reply(reply_text or '')
            cs['reply_class']  = verdict

            if verdict in ('HARD_STOP', 'HOSTILE'):
                # Legal-protection path: stop forever, no Jeff task, no Mike review
                set_dnd(cid, verdict.lower())
                add_tag(cid, 'dnd-opt-out')
                add_tag(cid, f'replied-{verdict.lower()}-{stage_name}')
                slack_post(f'🚫 *{name}* opted out ({verdict}) — DND set, no callback. {addr1}')
                return f'replied-{verdict.lower()}'

            if verdict == 'WRONG':
                set_dnd(cid, 'wrong-number')
                add_tag(cid, 'wrong-number')
                add_tag(cid, f'replied-wrong-{stage_name}')
                slack_post(f'☎️ *{name}* — wrong number, DND set. {addr1}')
                return 'replied-wrong'

            if verdict == 'NEGATIVE':
                # Polite no — don't waste Jeff's time, but no DND (still allowed to outreach later)
                add_tag(cid, 'not-interested')
                add_tag(cid, f'replied-negative-{stage_name}')
                slack_post(f'👎 *{name}* — declined politely, no callback task. {addr1}')
                return 'replied-negative'

            # POSITIVE or NEUTRAL → real lead, Jeff handles
            add_tag(cid, f'replied-stage-{stage_name}')
            create_task(cid, USER_JEFF,
                        f'Call back: {name} ({addr1})',
                        f'Seller replied to {stage_name.upper()} SMS. Reply: "{(reply_text or "")[:200]}". Call back today.',
                        due_in_days=0)
            create_task(cid, USER_MIKE,
                        f'REVIEW: Did Jeff call {name} back?',
                        f'Verify Jeff completed the callback. Reply text: "{(reply_text or "")[:200]}"',
                        due_in_days=1)
            slack_post(f'💬 *{name}* replied ({verdict}) to {stage_name.upper()} — {addr1}. Jeff has callback task.')
            return f'replied-{verdict.lower()}'

    sms_count = cs.get('sms_count', 0)
    cfg = STAGE_CONFIG[stage_name]

    # All max touches sent — wait dormant_wait, then mark dormant
    if sms_count >= cfg['max_touches']:
        d = days_since(cs.get('last_sms_at'))
        if d is not None and d >= cfg['dormant_wait']:
            cs['dormant'] = True
            add_tag(cid, 'dormant-sms')
            # Active stages get a follow-up task; reactivation stages just get tagged dormant
            if stage_name in ('qualified', 'lao', 'rr', 'mao'):
                create_task(cid, USER_JEFF,
                            f'Manual call attempt: {name} ({addr1})',
                            f'Seller never replied to {cfg["max_touches"]} SMS in stage {stage_name.upper()}. Try a manual call.',
                            due_in_days=0)
                create_task(cid, USER_MIKE,
                            f'REVIEW: Did Jeff call {name}?',
                            'Verify Jeff made the manual call.',
                            due_in_days=2)
                slack_post(f'📞 *{name}* went dormant in {stage_name.upper()} — no replies after {cfg["max_touches"]} SMS. Jeff to call manually. {addr1}')
            return 'dormant'
        return 'wait-dormant'

    # Should we send next SMS?
    interval = cfg['interval_days']
    if cs.get('last_sms_at'):
        d = days_since(cs['last_sms_at'])
        if d is None or d < interval:
            return 'wait'
    else:
        d = days_since(cs.get('stage_entered_at'))
        if d is None or d < interval:
            return 'wait'

    # Compose & send
    if not addr1:
        addr1 = (contact.get('city') or 'your property').strip() or 'your property'
    first = (contact.get('firstName') or 'there').strip() or 'there'
    template = TEMPLATES[stage_name][sms_count]
    message  = template.format(first_name=first, address1=addr1)
    # TCPA: append "Reply STOP to opt out." on touches 1, 4, and the final touch
    message  = with_tcpa(message, sms_count, cfg['max_touches'])
    state_code = (contact.get('state') or '').strip().upper()
    from_num   = from_number_for(state_code, sms_count, stage_name)

    ok, info = send_sms(cid, message, from_num)
    if ok:
        cs['sms_count']        = sms_count + 1
        cs['last_sms_at']      = now_utc().isoformat()
        cs['last_from_number'] = from_num
        add_tag(cid, f'stage-{stage_name}-sms{sms_count + 1}')
        return f'sent#{sms_count + 1}'
    return f'fail:{info[:60]}'


def in_business_hours_et():
    """9 AM - 8 PM Eastern. Handles EST/EDT correctly via zoneinfo."""
    h = datetime.now(ET).hour
    return 9 <= h < 20


def read_sheet_config():
    """Read kill switch and live templates from Google Sheet.
    Returns (kill_switch_on: bool, templates: dict).
    Falls back to hardcoded TEMPLATES if anything fails."""
    if not SHEET_ID:
        return True, TEMPLATES
    token_json = os.environ.get('GOOGLE_TOKEN_JSON', '')
    if not token_json:
        return True, TEMPLATES
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
        SCOPES = ['https://www.googleapis.com/auth/drive',
                  'https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
        if not creds.valid and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        svc = build('sheets', 'v4', credentials=creds)

        # Kill switch — Settings!B2
        kill_on = True
        try:
            r = svc.spreadsheets().values().get(
                spreadsheetId=SHEET_ID, range="Settings!B2"
            ).execute()
            val = (r.get('values') or [[]])[0]
            if val and str(val[0]).strip().upper() in ('OFF', 'FALSE', 'NO', 'DISABLED'):
                kill_on = False
        except Exception:
            pass

        # Templates — Templates!A2:C200
        templates = dict(TEMPLATES)
        try:
            r = svc.spreadsheets().values().get(
                spreadsheetId=SHEET_ID, range="Templates!A2:C200"
            ).execute()
            rows = r.get('values', [])
            built = {}
            for row in rows:
                if len(row) < 3:
                    continue
                stage = (row[0] or '').strip().lower()
                try:
                    idx = int(row[1]) - 1
                except Exception:
                    continue
                msg = row[2]
                if not msg or not stage:
                    continue
                built.setdefault(stage, [])
                while len(built[stage]) <= idx:
                    built[stage].append('')
                built[stage][idx] = msg
            # Merge: only override stages where the sheet has all required slots
            for stage, msgs in built.items():
                expected = len(TEMPLATES.get(stage, []))
                if expected and len(msgs) >= expected and all(msgs[:expected]):
                    templates[stage] = msgs[:expected]
        except Exception:
            pass

        return kill_on, templates
    except Exception as e:
        print(f'  Sheet config read failed: {e}; using defaults.')
        return True, TEMPLATES


def process_call_needed_cadence(state):
    """For contacts tagged 'from-call-needed', create a Jeff+Mike task pair every 48h
    until they reply OR 6 days have elapsed. After 6 days, remove the tag so the
    standard SMS sequence takes over. Dedups against existing open tasks."""
    # Search GHL by tag
    page = 1
    processed = 0
    transitioned = 0
    while True:
        r = http('GET', 'https://services.leadconnectorhq.com/contacts/search',
                 headers=GHL_H,
                 json={'locationId': GHL_LOCATION,
                       'query': '',
                       'pageLimit': 100,
                       'page': page,
                       'filters': [{'field': 'tags', 'operator': 'contains', 'value': 'from-call-needed'}]})
        # Some GHL versions need GET — fall back if POST is rejected
        if r.status_code in (404, 405):
            r = http('GET', f'https://services.leadconnectorhq.com/contacts/?locationId={GHL_LOCATION}&query=',
                     headers=GHL_H)
        if r.status_code != 200:
            break
        contacts = r.json().get('contacts', []) or []
        if not contacts:
            break
        for c in contacts:
            tags = c.get('tags', []) or []
            if 'from-call-needed' not in tags:
                continue
            cid = c.get('id')
            if not cid: continue
            cs = state.setdefault(cid, {})

            # Has the seller replied? (any inbound message in last 6 days)
            anchor = cs.get('cn_started') or cs.get('stage_entered_at') or now_utc().isoformat()
            replied, _, _ = has_inbound_since(cid, anchor)
            if replied:
                # Stop the cadence — standard reply handler in main loop will process tasks
                http('DELETE', f'https://services.leadconnectorhq.com/contacts/{cid}/tags',
                     headers=GHL_H, json={'tags': ['from-call-needed']})
                cs['cn_done'] = True
                continue

            # Init cadence start tracker
            if 'cn_started' not in cs:
                cs['cn_started'] = now_utc().isoformat()
                cs['cn_attempts'] = 0
                cs['cn_last_at'] = None

            elapsed = days_since(cs['cn_started']) or 0
            if elapsed >= 6:
                # Transition to standard SMS sequence
                http('DELETE', f'https://services.leadconnectorhq.com/contacts/{cid}/tags',
                     headers=GHL_H, json={'tags': ['from-call-needed']})
                cs['cn_done'] = True
                transitioned += 1
                continue

            # Time for the next task pair? Every 48h.
            last = cs.get('cn_last_at')
            if last:
                d = days_since(last) or 0
                if d < 2.0:
                    continue

            name  = f"{c.get('firstName','')} {c.get('lastName','')}".strip() or '(no name)'
            addr  = (c.get('address1') or c.get('city') or '').strip()
            jeff_title = f'CALL: {name} ({addr})'
            jeff_body  = 'Lead has not been reached yet. Try again.'
            mike_title = f'REVIEW: Did Jeff call {name}? ({addr})'
            mike_body  = 'Verify Jeff attempted the call. Mark complete after confirming.'

            j_made = create_task(cid, USER_JEFF, jeff_title, jeff_body, due_in_days=0)
            m_made = create_task(cid, USER_MIKE, mike_title, mike_body, due_in_days=1)
            if j_made or m_made:
                cs['cn_attempts'] = (cs.get('cn_attempts') or 0) + 1
                cs['cn_last_at'] = now_utc().isoformat()
                processed += 1
                slack_post(f'📞 Manual call retry queued: *{name}* ({addr}) — attempt {cs["cn_attempts"]}/3')
        if len(contacts) < 100:
            break
        page += 1
        time.sleep(0.2)
    return processed, transitioned


def write_status(success, summary='', error=''):
    """Write last-run status so dashboards can surface failures."""
    try:
        with open(STATUS_FILE, 'w') as f:
            json.dump({
                'success':   success,
                'timestamp': now_utc().isoformat(),
                'summary':   summary,
                'error':     error[:500],
            }, f, indent=2)
    except Exception:
        pass


def main():
    et_now = datetime.now(ET)
    print(f'[{et_now.strftime("%Y-%m-%d %I:%M %p ET")}] SMS Follow-Up starting...')
    counts = {}

    try:
        # Kill switch + live templates from Google Sheet
        kill_on, live_templates = read_sheet_config()
        if not kill_on:
            print('!! KILL SWITCH IS OFF — Settings!B2 in dashboard sheet says OFF. Skipping all SMS sends.')
            print('   (Dashboard will still update.)')
            write_status(True, 'kill-switch off; no sends')
            return
        global TEMPLATES
        TEMPLATES = live_templates
        print(f'SMS Automation: ON  |  Templates loaded for stages: {list(TEMPLATES.keys())}')

        if not in_business_hours_et():
            print(f'Outside business hours (9 AM - 8 PM ET); current ET hour: {et_now.hour}. Skipping sends.')
            write_status(True, f'outside business hours (hour={et_now.hour} ET)')
            return

        state    = load_state()
        entries  = fetch_active_leads()
        print(f'Active leads in stages 1-7: {len(entries)}')

        # Build/refresh shared contacts cache for the dashboards (avoids each one
        # re-fetching every contact).
        contacts_cache = {}
        for e in entries:
            contact = get_contact(e['cid'])
            if not contact:
                continue
            contacts_cache[e['cid']] = contact
            result = process_lead(e, contact, state)
            counts[result] = counts.get(result, 0) + 1
            time.sleep(0.3)

        try:
            with open(CONTACTS_CACHE, 'w') as f:
                json.dump({'fetched_at': now_utc().isoformat(),
                           'contacts': contacts_cache}, f)
        except Exception as e:
            print(f'  contacts cache write failed: {e}')

        cn_processed, cn_transitioned = process_call_needed_cadence(state)
        if cn_processed or cn_transitioned:
            print(f'Call-needed cadence: {cn_processed} retry tasks created, {cn_transitioned} graduated to SMS')

        save_state(state)
        print('\nSummary:', json.dumps(counts, indent=2))
        write_status(True, json.dumps(counts))
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f'\n!! SMS run failed: {e}\n{tb}')
        write_status(False, json.dumps(counts), f'{e}: {tb[-300:]}')
        raise


if __name__ == '__main__':
    main()
