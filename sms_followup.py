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
import json, os, requests, time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo('America/New_York')

GHL_TOKEN    = os.environ['GHL_TOKEN']
GHL_LOCATION = 'RCkiUmWqXX4BYQ39JXmm'
PIPELINE_ID  = 'O8wzIa6E3SgD8HLg6gh9'
STATE_FILE   = 'sms_state.json'

GHL_H = {'Authorization': f'Bearer {GHL_TOKEN}',
         'Content-Type': 'application/json', 'Version': '2021-07-28'}

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
            r = requests.get('https://services.leadconnectorhq.com/opportunities/search',
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
    r = requests.get(f'https://services.leadconnectorhq.com/contacts/{cid}', headers=GHL_H)
    if r.status_code != 200:
        return None
    return r.json().get('contact')


def has_inbound_since(contact_id, after_iso):
    """Look for any inbound message from contact after the given timestamp."""
    after = parse_iso(after_iso)
    if not after:
        return False, None
    r = requests.get('https://services.leadconnectorhq.com/conversations/search',
                     headers=GHL_H,
                     params={'locationId': GHL_LOCATION, 'contactId': contact_id, 'limit': 5})
    if r.status_code != 200:
        return False, None
    convs = r.json().get('conversations', [])
    for conv in convs:
        cid = conv.get('id')
        if not cid:
            continue
        rm = requests.get(f'https://services.leadconnectorhq.com/conversations/{cid}/messages',
                          headers=GHL_H, params={'limit': 50})
        if rm.status_code != 200:
            continue
        msgs = (rm.json().get('messages') or {}).get('messages', [])
        for m in msgs:
            if m.get('direction') == 'inbound':
                msg_dt = parse_iso(m.get('dateAdded', ''))
                if msg_dt and msg_dt > after:
                    return True, msg_dt.isoformat()
    return False, None


def send_sms(contact_id, message, from_number):
    body = {
        'type': 'SMS',
        'contactId': contact_id,
        'message': message,
        'fromNumber': from_number,
    }
    r = requests.post('https://services.leadconnectorhq.com/conversations/messages',
                      headers=GHL_H, json=body)
    if r.status_code in (200, 201):
        try:
            return True, r.json().get('messageId', '')
        except Exception:
            return True, ''
    return False, f'{r.status_code} {r.text[:200]}'


def add_tag(contact_id, tag):
    try:
        requests.post(f'https://services.leadconnectorhq.com/contacts/{contact_id}/tags',
                      headers=GHL_H, json={'tags': [tag]})
    except Exception as e:
        print(f'  tag add failed: {e}')


def create_task(contact_id, user_id, title, body, due_in_days=0):
    due = (now_utc() + timedelta(days=due_in_days)).isoformat()
    try:
        r = requests.post(f'https://services.leadconnectorhq.com/contacts/{contact_id}/tasks',
                          headers=GHL_H,
                          json={'title': title, 'body': body, 'dueDate': due, 'assignedTo': user_id})
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
        replied, when = has_inbound_since(cid, anchor)
        if replied:
            cs['replied']    = True
            cs['replied_at'] = when
            tag = f'replied-stage-{stage_name}'
            add_tag(cid, tag)
            create_task(cid, USER_JEFF,
                        f'Call back: {name} ({addr1})',
                        f'Seller replied to follow-up SMS in stage {stage_name.upper()}. Call back today.',
                        due_in_days=0)
            create_task(cid, USER_MIKE,
                        f'REVIEW: Did Jeff call {name} back?',
                        'Verify Jeff completed the callback.',
                        due_in_days=1)
            return 'replied'

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


def main():
    et_now = datetime.now(ET)
    print(f'[{et_now.strftime("%Y-%m-%d %I:%M %p ET")}] SMS Follow-Up starting...')

    if not in_business_hours_et():
        print(f'Outside business hours (9 AM - 8 PM ET); current ET hour: {et_now.hour}. Skipping sends.')
        return

    state    = load_state()
    entries  = fetch_active_leads()
    print(f'Active leads in stages 1-4: {len(entries)}')

    counts = {}
    for e in entries:
        contact = get_contact(e['cid'])
        if not contact:
            continue
        result = process_lead(e, contact, state)
        counts[result] = counts.get(result, 0) + 1
        time.sleep(0.3)

    save_state(state)
    print('\nSummary:', json.dumps(counts, indent=2))


if __name__ == '__main__':
    main()
