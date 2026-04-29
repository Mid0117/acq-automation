"""
SMS Follow-Up Dashboard — writes a snapshot to Google Sheets every run.

Two tabs:
  Summary — top-level counts and stats
  Leads   — one row per active contact with full status

If env var DASHBOARD_SHEET_ID is set, writes to that sheet.
Otherwise creates a new sheet, prints the URL, you set the env var.
"""
import json, os, requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo('America/New_York')


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
                                    'stage': stage_label, 'name': c.get('name', ''),
                                    'tags': c.get('tags', [])})
            if len(opps) < 100:
                break
            page += 1
    return entries


def get_contact(cid):
    r = requests.get(f'https://services.leadconnectorhq.com/contacts/{cid}', headers=GHL_H)
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
    for name in ('Summary', 'Leads'):
        if name not in existing:
            reqs.append({'addSheet': {'properties': {'title': name}}})
    if reqs:
        svc['sheets'].spreadsheets().batchUpdate(
            spreadsheetId=sheet_id, body={'requests': reqs}
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
    svc = get_google_services()
    if not svc:
        print('Google not configured; skipping dashboard.')
        return

    sms_state = json.load(open(STATE_FILE)) if os.path.exists(STATE_FILE) else {}
    leads     = fetch_active_leads()
    print(f'Building dashboard for {len(leads)} active leads')

    sheet_id = ensure_sheet(svc)
    ensure_tabs(svc, sheet_id)

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
