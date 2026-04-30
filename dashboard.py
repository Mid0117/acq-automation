"""
SMS Follow-Up Dashboard — writes a snapshot to Google Sheets every run.

Two tabs:
  Summary — top-level counts and stats
  Leads   — one row per active contact with full status

If env var DASHBOARD_SHEET_ID is set, writes to that sheet.
Otherwise creates a new sheet, prints the URL, you set the env var.
"""
import json, os, time, requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo('America/New_York')

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


def now_utc():
    return datetime.now(timezone.utc)


def write_status(success, summary='', error=''):
    try:
        with open('last_run_dashboard.json', 'w') as f:
            json.dump({'success': success,
                       'timestamp': now_utc().isoformat(),
                       'summary': summary,
                       'error': error[:500]}, f, indent=2)
    except Exception:
        pass


def load_contacts_cache():
    if not os.path.exists('contacts_cache.json'):
        return {}
    try:
        return (json.load(open('contacts_cache.json')) or {}).get('contacts', {}) or {}
    except Exception:
        return {}


def to_et_str(iso_or_str):
    """Format ISO timestamp as readable ET. Empty if missing/invalid."""
    if not iso_or_str:
        return ''
    try:
        dt = datetime.fromisoformat(str(iso_or_str).replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(ET).strftime('%b %d, %Y %I:%M %p ET')
    except Exception:
        return str(iso_or_str)

GHL_TOKEN    = os.environ['GHL_TOKEN']
GHL_LOCATION = 'RCkiUmWqXX4BYQ39JXmm'
PIPELINE_ID  = 'O8wzIa6E3SgD8HLg6gh9'
STATE_FILE   = 'sms_state.json'
SHEET_ID     = os.environ.get('DASHBOARD_SHEET_ID', '')

GHL_H = {'Authorization': f'Bearer {GHL_TOKEN}',
         'Content-Type': 'application/json', 'Version': '2021-07-28'}

STAGE_NAMES = {
    'a17517be-8d1a-49fd-bd53-b9128a66e242': '1. Qualified',
    'd43fddd8-3a17-46b2-a193-cf18619f654f': '2. LAO',
    '23a159ad-ba39-4c74-9d07-c1beb219d9f2': '3. Due Diligence',
    '43589167-14f0-4e09-ba2a-8b9bd3296a4a': '4. MAO',
    '4aa78ab3-85dc-46d1-a683-d97b0c7a23ee': 'Follow Up 1.5mo',
    '571c115e-2603-4f3f-8546-d716f44ba8ef': 'Follow Up 3mo',
    'b9b560b0-30cb-47fc-a4ca-1e55ca2531e2': 'Dead Deals',
}
ACTIVE_STAGES = set(STAGE_NAMES.keys())


def get_google_services():
    token_json = os.environ.get('GOOGLE_TOKEN_JSON', '')
    if not token_json:
        return None
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    SCOPES = ['https://www.googleapis.com/auth/drive',
              'https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
    if not creds.valid and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return {
        'drive':  build('drive', 'v3', credentials=creds),
        'sheets': build('sheets', 'v4', credentials=creds),
    }


def fetch_active_leads():
    """Query each stage server-side (GHL 'total' field is unreliable)."""
    entries = []
    for stage_id, stage_label in STAGE_NAMES.items():
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
                                    'stage': stage_label, 'name': c.get('name', ''),
                                    'tags': c.get('tags', [])})
            if len(opps) < 100:
                break
            page += 1
    return entries


_CONTACTS_LOOKUP = {}


def get_contact(cid):
    cached = _CONTACTS_LOOKUP.get(cid)
    if cached:
        return cached
    r = http('GET', f'https://services.leadconnectorhq.com/contacts/{cid}', headers=GHL_H)
    if r.status_code != 200:
        return {}
    return r.json().get('contact', {})


def ensure_sheet(svc):
    """If SHEET_ID env var set, return it. Else create a new sheet."""
    global SHEET_ID
    if SHEET_ID:
        return SHEET_ID
    sheet = svc['sheets'].spreadsheets().create(
        body={'properties': {'title': 'ACQ SMS Follow-Up Dashboard'}}
    ).execute()
    sid = sheet['spreadsheetId']
    print(f'Created sheet: https://docs.google.com/spreadsheets/d/{sid}/edit')
    print(f'>>> SET GitHub Secret DASHBOARD_SHEET_ID = {sid}')
    # Share with team
    for email in ('atompropertygroup@gmail.com', 'jeff@atompropertygroup.org', 'mike@atompropertygroup.org'):
        try:
            svc['drive'].permissions().create(
                fileId=sid, body={'type':'user','role':'writer','emailAddress':email},
                sendNotificationEmail=False
            ).execute()
        except Exception:
            pass
    SHEET_ID = sid
    return sid


def ensure_tabs(svc, sheet_id):
    meta = svc['sheets'].spreadsheets().get(spreadsheetId=sheet_id).execute()
    existing = {s['properties']['title'] for s in meta.get('sheets', [])}
    reqs = []
    for name in ('Summary', 'Leads', 'Settings', 'Templates'):
        if name not in existing:
            reqs.append({'addSheet': {'properties': {'title': name}}})
    if reqs:
        svc['sheets'].spreadsheets().batchUpdate(
            spreadsheetId=sheet_id, body={'requests': reqs}
        ).execute()


# Default templates — what gets seeded the first time Templates tab is empty.
DEFAULT_TEMPLATES = {
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


def ensure_settings(svc, sheet_id):
    """Seed Settings tab with kill switch ON if empty. Don't overwrite user changes."""
    try:
        r = svc['sheets'].spreadsheets().values().get(
            spreadsheetId=sheet_id, range="Settings!A1:B5"
        ).execute()
        if r.get('values'):
            return  # already populated
    except Exception:
        pass
    rows = [
        ['Setting', 'Value'],
        ['SMS Automation', 'ON'],
        ['', ''],
        ['Notes', 'Set "SMS Automation" to OFF to halt all SMS sends. Other automations continue.'],
    ]
    svc['sheets'].spreadsheets().values().update(
        spreadsheetId=sheet_id, range="Settings!A1",
        valueInputOption='USER_ENTERED', body={'values': rows}
    ).execute()


def ensure_templates(svc, sheet_id):
    """Seed Templates tab with defaults if empty. Don't overwrite user edits."""
    try:
        r = svc['sheets'].spreadsheets().values().get(
            spreadsheetId=sheet_id, range="Templates!A1:C5"
        ).execute()
        if r.get('values'):
            return  # already populated
    except Exception:
        pass
    rows = [['Stage', '#', 'Message']]
    for stage in ('qualified', 'lao', 'rr', 'mao', 'fu15mo', 'fu3mo', 'dead'):
        for i, msg in enumerate(DEFAULT_TEMPLATES[stage], 1):
            rows.append([stage, i, msg])
    svc['sheets'].spreadsheets().values().update(
        spreadsheetId=sheet_id, range="Templates!A1",
        valueInputOption='USER_ENTERED', body={'values': rows}
    ).execute()


def write_tab(svc, sheet_id, tab, rows):
    svc['sheets'].spreadsheets().values().clear(
        spreadsheetId=sheet_id, range=f"'{tab}'!A:Z"
    ).execute()
    if rows:
        svc['sheets'].spreadsheets().values().update(
            spreadsheetId=sheet_id, range=f"'{tab}'!A1",
            valueInputOption='USER_ENTERED', body={'values': rows}
        ).execute()


def main():
    try:
        _main_inner()
        write_status(True, 'dashboard updated')
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f'!! dashboard run failed: {e}\n{tb}')
        write_status(False, '', f'{e}: {tb[-300:]}')
        raise


def _main_inner():
    svc = get_google_services()
    if not svc:
        print('Google not configured; skipping dashboard.')
        return

    # Hydrate contacts cache from sms_followup → no per-lead GHL GETs
    global _CONTACTS_LOOKUP
    _CONTACTS_LOOKUP = load_contacts_cache()
    if _CONTACTS_LOOKUP:
        print(f'Contacts cache: {len(_CONTACTS_LOOKUP)} entries (skipping per-lead GETs)')

    sms_state = json.load(open(STATE_FILE)) if os.path.exists(STATE_FILE) else {}
    leads     = fetch_active_leads()
    print(f'Building dashboard for {len(leads)} active leads')

    sheet_id = ensure_sheet(svc)
    ensure_tabs(svc, sheet_id)
    ensure_settings(svc, sheet_id)
    ensure_templates(svc, sheet_id)

    leads_rows = [[
        'Name', 'Address', 'City', 'State', 'Stage',
        'SMS Sent', 'Last SMS At', 'Last From Number',
        'Replied?', 'Replied At', 'Dormant?', 'Tags', 'Contact ID',
    ]]
    by_stage   = {s: 0 for s in STAGE_NAMES.values()}
    by_state   = {}
    sms_counts = [0] * 7
    replied = dormant = active = 0

    for e in leads:
        c = get_contact(e['cid'])
        st = sms_state.get(e['cid'], {})
        leads_rows.append([
            f"{c.get('firstName','')} {c.get('lastName','')}".strip(),
            c.get('address1', ''),
            c.get('city', ''),
            c.get('state', ''),
            e['stage'],
            st.get('sms_count', 0),
            to_et_str(st.get('last_sms_at', '')),
            st.get('last_from_number', ''),
            'YES' if st.get('replied') else '',
            to_et_str(st.get('replied_at', '')),
            'YES' if st.get('dormant') else '',
            ', '.join(e.get('tags', [])),
            e['cid'],
        ])
        by_stage[e['stage']] = by_stage.get(e['stage'], 0) + 1
        s = (c.get('state') or 'Unknown').strip().upper() or 'Unknown'
        by_state[s] = by_state.get(s, 0) + 1
        n = st.get('sms_count', 0)
        if 0 <= n <= 6:
            sms_counts[n] += 1
        if st.get('replied'):    replied += 1
        elif st.get('dormant'):  dormant += 1
        else:                    active  += 1

    write_tab(svc, sheet_id, 'Leads', leads_rows)

    now_str = datetime.now(ET).strftime('%b %d, %Y %I:%M %p ET')
    summary = [
        ['ACQ SMS Follow-Up Dashboard'],
        [f'Snapshot: {now_str}'],
        [],
        ['HEADLINE'],
        ['Total active leads', len(leads)],
        ['Active in sequence', active],
        ['Replied (need callback)', replied],
        ['Dormant (need manual call)', dormant],
        ['Reply rate', f'{(replied / max(1, replied + dormant + active)) * 100:.1f}%'],
        [],
        ['BY STAGE'],
        *[[k, v] for k, v in sorted(by_stage.items())],
        [],
        ['SMS PROGRESS'],
        ['SMS sent', 'Contacts'],
        *[[i, sms_counts[i]] for i in range(7)],
        [],
        ['BY STATE'],
        *[[k, v] for k, v in sorted(by_state.items(), key=lambda x: -x[1])],
    ]
    write_tab(svc, sheet_id, 'Summary', summary)

    print(f'Dashboard updated: https://docs.google.com/spreadsheets/d/{sheet_id}/edit')


if __name__ == '__main__':
    main()
