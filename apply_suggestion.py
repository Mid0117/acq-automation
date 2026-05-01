"""
Apply suggested field updates to a GHL contact.

Triggered by the dashboard's 'Apply to GHL' button via workflow_dispatch.
Reads CID + FIELD_UPDATES JSON from env (passed in by the workflow inputs),
maps the suggestion keys to real GHL custom field IDs, and PUTs the change.

The GHL_TOKEN secret stays server-side — never reaches the browser.
"""
import json, os, sys, requests

GHL_TOKEN          = os.environ['GHL_TOKEN']
CID                = os.environ.get('CID', '').strip()
FIELD_UPDATES_JSON = os.environ.get('FIELD_UPDATES', '{}')

# Friendly-name → real custom field IDs. Mirrors the SUGGEST_FIELD_IDS in the
# dashboard JS. Add to both maps when extending.
FIELD_MAP = {
    'asking_price':       '6q7syt4puxfP7E03Xxhd',
    'timeline':           'v47I1Mi63RBpCD5N5RrH',
    'motivation':         'rbYZAdhvuvX1NQgexhxy',
    'reason_for_selling': 'cJdRGRoox0RZCytRAVSI',
}


def main():
    if not CID:
        print('::error::No CID provided')
        sys.exit(1)
    # Validate cid contains only safe chars (avoid URL injection)
    if not all(ch.isalnum() or ch in '-_' for ch in CID):
        print(f'::error::Invalid CID: {CID!r}')
        sys.exit(1)

    try:
        updates = json.loads(FIELD_UPDATES_JSON)
    except Exception as e:
        print(f'::error::Invalid field_updates JSON: {e}')
        sys.exit(1)
    if not isinstance(updates, dict) or not updates:
        print('::error::field_updates must be a non-empty JSON object')
        sys.exit(1)

    custom_fields = []
    for key, value in updates.items():
        if key not in FIELD_MAP:
            print(f'::warning::Unknown key {key!r}, skipping')
            continue
        if value in (None, '', 'null'):
            continue
        custom_fields.append({'id': FIELD_MAP[key], 'field_value': str(value)})

    if not custom_fields:
        print('::error::No valid field updates to apply (after filtering)')
        sys.exit(1)

    print(f'Applying {len(custom_fields)} update(s) to contact ...{CID[-6:]}')
    for f in custom_fields:
        print(f'  - {f["id"]} = {f["field_value"]!r}')

    r = requests.put(
        f'https://services.leadconnectorhq.com/contacts/{CID}',
        headers={
            'Authorization': f'Bearer {GHL_TOKEN}',
            'Content-Type':  'application/json',
            'Version':       '2021-07-28',
        },
        json={'customFields': custom_fields},
        timeout=30,
    )
    if r.status_code in (200, 201):
        print(f'OK: GHL response {r.status_code}')
        return
    print(f'::error::GHL API failed: {r.status_code} {r.text[:300]}')
    sys.exit(1)


if __name__ == '__main__':
    main()
