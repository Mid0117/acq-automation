"""
add_note.py — append a feedback note to a GHL contact.

Triggered by the Priority Deals page when the team types in the
"Adam's Feedback" textbox and clicks Save. Workflow_dispatch passes
cid + who + body. We POST a new note to that contact.
"""
import os, sys, requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo('America/New_York')

GHL_TOKEN = os.environ['GHL_TOKEN']
CID  = os.environ.get('CID', '').strip()
WHO  = os.environ.get('WHO', 'Team').strip() or 'Team'
BODY = os.environ.get('BODY', '').strip()


def main():
    if not CID or not all(ch.isalnum() or ch in '-_' for ch in CID):
        print(f'::error::Invalid CID: {CID!r}')
        sys.exit(1)
    if not BODY:
        print('::error::Empty body')
        sys.exit(1)

    ts = datetime.now(ET).strftime('%b %d, %Y %I:%M %p ET')
    note = f'[Priority Deals · {ts}]\n{WHO} feedback:\n\n{BODY[:2000]}'

    print(f'Adding note to contact ...{CID[-6:]} from {WHO}')
    print(f'Body length: {len(BODY)} chars')

    r = requests.post(
        f'https://services.leadconnectorhq.com/contacts/{CID}/notes',
        headers={
            'Authorization': f'Bearer {GHL_TOKEN}',
            'Content-Type':  'application/json',
            'Version':       '2021-07-28',
        },
        json={'body': note},
        timeout=30,
    )
    if r.status_code in (200, 201):
        print(f'OK: {r.status_code}')
        return
    print(f'::error::GHL API failed: {r.status_code} {r.text[:300]}')
    sys.exit(1)


if __name__ == '__main__':
    main()
