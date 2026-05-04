"""
update_lead.py — one-shot manual update for a specific lead.

Triggered by .github/workflows/update_lead.yml with workflow_dispatch inputs:
  - search:         text to search GHL contacts (name or address fragment)
  - field_updates:  JSON object of {key: value} field updates
  - stage:          optional pipeline stage to move the opportunity to
                    (one of: unqualified, qualified, lao, dd, mao, contract,
                    psa, dispo, follow_15, follow_3, dead)

Used to apply meeting-brief style updates the team agrees on (assignment fees,
stage moves to Disposition, manual note adds, etc.) without leaving the
dashboard / from this workflow's UI.
"""
import json, os, sys, requests

GHL_TOKEN = os.environ['GHL_TOKEN']
SEARCH    = os.environ.get('SEARCH', '').strip()
FIELD_UPDATES_JSON = os.environ.get('FIELD_UPDATES', '{}')
STAGE_NAME = (os.environ.get('STAGE', '') or '').strip().lower()

LOC = 'RCkiUmWqXX4BYQ39JXmm'
PIPELINE = 'O8wzIa6E3SgD8HLg6gh9'

H = {'Authorization': f'Bearer {GHL_TOKEN}',
     'Content-Type': 'application/json',
     'Version': '2021-07-28'}

# Friendly name → GHL contact custom field IDs
FIELD_MAP = {
    'asking_price':       '6q7syt4puxfP7E03Xxhd',
    'timeline':           'v47I1Mi63RBpCD5N5RrH',
    'motivation':         'rbYZAdhvuvX1NQgexhxy',
    'reason_for_selling': 'cJdRGRoox0RZCytRAVSI',
    'arv':                'nCWzIGfZHki0dv84gUem',
    '70_arv':             'R7QUzOdOnJXgoGRPwxdF',
    'mao':                'zNcoeZfYp1CpVXjV5YhG',
    'assignment_fee':     '4IJPj2UebvkrYJ0rK06l',
    'lao':                'Vmga1E7W5QIQMJW0DWZm',
    'rehab_value':        'zgXw88mb4R98Un7NRDQP',
    'lead_type':          'nqErDKRO1IdhmmoDos15',
}

# Friendly stage tag → pipeline stage id
STAGE_MAP = {
    'unqualified': 'c1d23905-7096-439c-9a31-f8db5b2b53d0',
    'qualified':   'a17517be-8d1a-49fd-bd53-b9128a66e242',
    'lao':         'd43fddd8-3a17-46b2-a193-cf18619f654f',
    'dd':          '23a159ad-ba39-4c74-9d07-c1beb219d9f2',
    'rr':          '23a159ad-ba39-4c74-9d07-c1beb219d9f2',
    'mao':         '43589167-14f0-4e09-ba2a-8b9bd3296a4a',
    'contract':    '53eb29e2-92d9-439e-8865-a875a46a6fd8',
    'psa':         'e377ba40-6d3b-4981-86cb-d31e7ef0c9c1',
    'dispo':       'aefeb703-5ab9-403c-b2eb-47fe550d62ee',
    'disposition': 'aefeb703-5ab9-403c-b2eb-47fe550d62ee',
    'follow_15':   '4aa78ab3-85dc-46d1-a683-d97b0c7a23ee',
    'follow_3':    '571c115e-2603-4f3f-8546-d716f44ba8ef',
    'dead':        'b9b560b0-30cb-47fc-a4ca-1e55ca2531e2',
}


def search_contacts(query):
    """Return list of (cid, name, address1) tuples matching the query."""
    out = []
    # Try the contacts search endpoint
    r = requests.post(
        'https://services.leadconnectorhq.com/contacts/search',
        headers=H,
        json={'locationId': LOC, 'query': query, 'pageLimit': 20, 'page': 1},
        timeout=30,
    )
    if r.status_code != 200:
        # Fall back to GET-based search
        r = requests.get(
            'https://services.leadconnectorhq.com/contacts/',
            headers=H,
            params={'locationId': LOC, 'query': query, 'limit': 20},
            timeout=30,
        )
    if r.status_code != 200:
        return []
    contacts = r.json().get('contacts', []) or []
    for c in contacts:
        out.append((c.get('id'),
                    f"{c.get('firstName','')} {c.get('lastName','')}".strip(),
                    c.get('address1', ''),
                    c.get('city', ''),
                    c.get('state', '')))
    return out


def find_opportunity(cid):
    """Return the first ACQ-pipeline opportunity for a contact, or None."""
    r = requests.get(
        'https://services.leadconnectorhq.com/opportunities/search',
        headers=H,
        params={'location_id': LOC, 'pipeline_id': PIPELINE,
                'contact_id': cid, 'limit': 5},
        timeout=30,
    )
    if r.status_code != 200:
        return None
    opps = r.json().get('opportunities', [])
    return opps[0] if opps else None


def main():
    if not SEARCH:
        print('::error::SEARCH input is required')
        sys.exit(1)
    try:
        updates = json.loads(FIELD_UPDATES_JSON) if FIELD_UPDATES_JSON.strip() else {}
    except Exception as e:
        print(f'::error::Invalid field_updates JSON: {e}')
        sys.exit(1)

    print(f'Searching GHL for: {SEARCH!r}')
    matches = search_contacts(SEARCH)
    if not matches:
        print(f'::error::No contacts matched {SEARCH!r}')
        sys.exit(1)
    if len(matches) > 1:
        print(f'⚠ Multiple matches ({len(matches)}). Using the first:')
        for m in matches[:5]:
            print(f'   - {m[1]:30s} | {m[2]:30s} | {m[3]}, {m[4]} | cid=...{m[0][-6:]}')
    cid, name, addr, city, state = matches[0]
    print(f'\nUsing: {name}  |  {addr}, {city} {state}  |  cid=...{cid[-6:]}')

    # 1. Apply contact custom-field updates
    if updates:
        cf = []
        for key, value in updates.items():
            if key not in FIELD_MAP:
                print(f'  ⚠ Unknown key {key!r} — skipping')
                continue
            if value in (None, '', 'null'):
                continue
            cf.append({'id': FIELD_MAP[key], 'field_value': str(value)})
        if cf:
            print(f'  Applying {len(cf)} contact field update(s)...')
            r = requests.put(f'https://services.leadconnectorhq.com/contacts/{cid}',
                             headers=H, json={'customFields': cf}, timeout=30)
            if r.status_code in (200, 201):
                for f in cf:
                    print(f'    ✓ {f["id"]} = {f["field_value"]!r}')
            else:
                print(f'    ::error::Contact PUT failed: {r.status_code} {r.text[:200]}')
                sys.exit(1)

    # 2. Optional stage move
    if STAGE_NAME:
        stage_id = STAGE_MAP.get(STAGE_NAME)
        if not stage_id:
            print(f'::warning::Unknown stage {STAGE_NAME!r}; valid options: {", ".join(STAGE_MAP.keys())}')
        else:
            opp = find_opportunity(cid)
            if not opp:
                print(f'::warning::No ACQ opportunity for cid=...{cid[-6:]}; skipping stage move.')
            else:
                oid = opp['id']
                print(f'  Moving opportunity {oid[-6:]} → stage {STAGE_NAME}')
                r = requests.put(
                    f'https://services.leadconnectorhq.com/opportunities/{oid}',
                    headers=H,
                    json={'pipelineStageId': stage_id},
                    timeout=30,
                )
                if r.status_code in (200, 201):
                    print(f'    ✓ Stage updated')
                else:
                    print(f'    ::error::Opp PUT failed: {r.status_code} {r.text[:200]}')
                    sys.exit(1)

    print('\nDone.')


if __name__ == '__main__':
    main()
