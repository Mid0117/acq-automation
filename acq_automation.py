"""
ACQ Pipeline Automation — GitHub Actions edition
Single-run script: finds new call recordings, transcribes (Deepgram),
analyzes with Claude, updates GHL contact + opportunity, creates/fills Rehab Report.
Scheduled every 15 min via GitHub Actions cron.
"""
import json, os, re, time, requests, warnings
from urllib.parse import urlparse
from datetime import datetime
warnings.filterwarnings('ignore')

GHL_TOKEN      = os.environ['GHL_TOKEN']
DG_KEY         = os.environ['DG_KEY']
ANTHROPIC_KEY  = os.environ.get('ANTHROPIC_API_KEY', '')
GHL_LOCATION   = 'RCkiUmWqXX4BYQ39JXmm'
PIPELINE_ID    = 'O8wzIa6E3SgD8HLg6gh9'
TEMPLATE_ID    = '1xYeKGmcbxJqCykxGXo1mEHBDEPQfbEHqXuS3cKrIliM'
PROCESSED_FILE = 'processed_contacts.json'

GHL_H = {'Authorization': f'Bearer {GHL_TOKEN}',
         'Content-Type': 'application/json', 'Version': '2021-07-28'}

# Contact custom fields
CF_CALL_REC      = 'swEkGAoiPVsNF9gAwA2g'
CF_AI_TX         = 'CmX7LZ66JFFlo0ACFFoM'
CF_VA_NOTES      = 'ctNVXVw8VY1PD4B1oqXj'
CF_BED           = 'xXEm77wvbxEbiqsw3lAz'
CF_BATH          = 'EtKof5yT7KAWmoaNQqJZ'
CF_SQFT          = '8kqwjqtJyTTeQ8SIaLQz'
CF_PROP_TYPE     = '7xsc1QHTleEFjRJChOgA'
CF_CONDITION     = '1Q4MENz9a1PsCF4jEtOU'
CF_TIMELINE      = 'v47I1Mi63RBpCD5N5RrH'
CF_MOTIVATION    = 'rbYZAdhvuvX1NQgexhxy'
CF_REASON_SELL   = 'cJdRGRoox0RZCytRAVSI'
CF_ASK_PRICE     = '6q7syt4puxfP7E03Xxhd'
CF_DEAL_TYPE     = 'xzdGu36ZWBTQBNLuCuG7'
CF_REPAIRS       = 'dbYoYFVTiCbqoJxC9HkR'
CF_ARV           = 'nCWzIGfZHki0dv84gUem'
CF_70_ARV        = 'R7QUzOdOnJXgoGRPwxdF'

# Opportunity custom fields
OF_BED        = 'NdjIxlmD8KGBJH7xQ0rv'
OF_BATH       = 'zl4RaWqAip1kmWax7YwI'
OF_SQFT       = 'PeHYon7Z5yv89Z9JtOws'
OF_PROP_TYPE  = 'VgMGTqo5Em7aHT6u9z7E'
OF_CONDITION  = 'pHig12D8t68DIU4M4lfG'
OF_TIMELINE   = 'NRYctFmTV6vckzbjrbi3'
OF_VA_NOTES   = 'RSi8RVZHqkdR7rC7hpLi'
OF_NUM_UNITS  = 'w9OeqjXnlK5jjnm4IMFp'
OF_REHAB      = 'cPCQEuwOJNMtoWR8CrLR'
OF_DEAL_TYPE  = 'CfbtlEDb6zapBZrhwkM4'
OF_EXIT_STRAT = 'MT83ArwttTUiH17oo9l0'
OF_NOTES      = 'KCGvjhEQg8drMv5w7SiL'

# contact field -> opportunity field
SYNC_MAP = {
    CF_BED:        OF_BED,
    CF_BATH:       OF_BATH,
    CF_SQFT:       OF_SQFT,
    CF_PROP_TYPE:  OF_PROP_TYPE,
    CF_CONDITION:  OF_CONDITION,
    CF_TIMELINE:   OF_TIMELINE,
    CF_VA_NOTES:   OF_VA_NOTES,
    CF_DEAL_TYPE:  OF_DEAL_TYPE,
}


def load_processed():
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE) as f:
            data = json.load(f)
            if isinstance(data, list):
                return {cid: '' for cid in data}
            return data
    return {}

def save_processed(processed):
    with open(PROCESSED_FILE, 'w') as f:
        json.dump(processed, f, indent=2)


def get_google_services():
    token_json = os.environ.get('GOOGLE_TOKEN_JSON', '')
    creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON', '')
    if not token_json or not creds_json:
        return None
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build

        SCOPES = ['https://www.googleapis.com/auth/drive',
                  'https://www.googleapis.com/auth/documents']
        creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
        if not creds.valid and creds.expired and creds.refresh_token:
            creds.refresh(Request())

        return {
            'drive': build('drive', 'v3', credentials=creds),
            'docs':  build('docs', 'v1', credentials=creds),
        }
    except Exception as e:
        print(f'Google auth error: {e}')
        return None


# ── Claude analysis ──────────────────────────────────────────────────────────
CLAUDE_SYSTEM = """You are a real estate acquisition analyst. You analyze call transcripts between a homeowner/seller and a real estate investor's VA, and extract structured deal data.

Return ONLY valid JSON — no prose, no code fences. Use null for any field not clearly mentioned in the transcript.

{
  "beds": <integer or null>,
  "baths": <number or null>,
  "sqft": <integer or null>,
  "property_type": "Single Family" | "Multi-Family" | "Condo" | "Townhouse" | "Land" | "Mobile Home" | null,
  "condition": "Excellent" | "Good" | "Fair" | "Poor" | "Needs Major Work" | null,
  "timeline": "ASAP" | "30 days" | "60 days" | "90 days" | "6+ months" | "No rush" | null,
  "motivation": <short string e.g. "Relocating", "Inherited", "Tired landlord", "Behind on payments", "Divorce", "Downsizing"> or null,
  "reason_for_selling": <one-sentence specific reason or null>,
  "asking_price": <integer dollars or null>,
  "estimated_arv": <integer dollars — your estimate of After Repair Value if any market context was given, otherwise null>,
  "deal_type": "Cash" | "Owner Finance" | "Subject-To" | "Wholesale" | "Lease Option" | "Hybrid" | "Unknown",
  "exit_strategy": "Flip" | "BRRRR" | "Wholesale" | "Buy & Hold" | "Owner Finance" | "Unknown",
  "repairs_needed": <short string listing major repairs mentioned, or null>,
  "lead_temp": "Hot" | "Warm" | "Cold",
  "va_notes_summary": <3-4 sentence professional briefing — what the seller said, condition, motivation, timeline, asking price, and how the call ended>,
  "red_flags": <array of short strings — title issues, occupancy, unrealistic price, missed payments, etc., or empty array>,
  "next_steps": <one sentence describing what was agreed at end of call, or null>
}"""


def analyze_with_claude(transcript):
    if not transcript or not ANTHROPIC_KEY:
        return None
    body = {
        'model': 'claude-sonnet-4-5',
        'max_tokens': 1500,
        'system': CLAUDE_SYSTEM,
        'messages': [{'role': 'user', 'content': transcript[:15000]}],
    }
    headers = {
        'x-api-key': ANTHROPIC_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
    }
    try:
        r = requests.post('https://api.anthropic.com/v1/messages',
                          headers=headers, json=body, timeout=90)
        if r.status_code != 200:
            print(f'  Claude API {r.status_code}: {r.text[:200]}')
            return None
        text = r.json()['content'][0]['text'].strip()
        if text.startswith('```'):
            text = text.split('```', 2)[1]
            if text.startswith('json'):
                text = text[4:]
            text = text.strip()
        return json.loads(text)
    except Exception as e:
        print(f'  Claude analysis failed: {e}')
        return None


def extract_fields_regex(text):
    """Fallback if Claude is unavailable."""
    if not text:
        return {}
    t = text.lower()
    d = {}
    bb = re.search(r'(\d+)\s*/\s*(\d+)', t)
    if bb:
        d['beds'], d['baths'] = bb.group(1), bb.group(2)
    else:
        bm  = re.search(r'(\d+)\s*bed', t)
        btm = re.search(r'(\d+)\s*bath', t)
        if bm:  d['beds']  = bm.group(1)
        if btm: d['baths'] = btm.group(1)
    sq = re.search(r'([\d,]+)\s*(?:square\s*f(?:eet|t)|sq\.?\s*ft)', t)
    if sq: d['sqft'] = sq.group(1).replace(',', '')
    for kw in ['excellent','great','good','fair','poor','needs work','fixer','renovated']:
        if kw in t:
            d['condition'] = kw.title()
            break
    pr = re.search(r'\$\s*([\d,]+(?:\.\d+)?)\s*([kKmM])?', text)
    if pr:
        val  = pr.group(1).replace(',', '')
        mult = (pr.group(2) or '').upper()
        if mult == 'K': val = str(int(float(val) * 1000))
        elif mult == 'M': val = str(int(float(val) * 1000000))
        try:
            if float(val) >= 10000:
                d['asking_price'] = int(float(val))
        except: pass
    return d


# ── Deepgram ─────────────────────────────────────────────────────────────────
def transcribe(audio_bytes):
    r = requests.post(
        'https://api.deepgram.com/v1/listen?model=nova-2&smart_format=true&language=en-US',
        headers={'Authorization': f'Token {DG_KEY}', 'Content-Type': 'audio/wav'},
        data=audio_bytes, timeout=60
    )
    if r.status_code == 200:
        return r.json()['results']['channels'][0]['alternatives'][0]['transcript']
    return None


# ── Rehab Report ─────────────────────────────────────────────────────────────
def create_rehab_doc(svc, address, data, transcript):
    try:
        title = f'Rehab Report - {address}'
        copy = svc['drive'].files().copy(
            fileId=TEMPLATE_ID, body={'name': title}
        ).execute()
        doc_id = copy['id']

        beds  = data.get('beds') or ''
        baths = data.get('baths') or ''
        sqft  = data.get('sqft') or ''

        reqs = [
            {'replaceAllText': {'containsText': {'text': 'Property Address:  ', 'matchCase': False},
                                'replaceText': f'Property Address: {address}'}},
            {'replaceAllText': {'containsText': {'text': 'Property Address: \n', 'matchCase': False},
                                'replaceText': f'Property Address: {address}\n'}},
            {'replaceAllText': {'containsText': {'text': 'Bed/Bath:\n', 'matchCase': False},
                                'replaceText': f'Bed/Bath: {beds}/{baths}\n'}},
            {'replaceAllText': {'containsText': {'text': 'SQFT: \n', 'matchCase': False},
                                'replaceText': f'SQFT: {sqft}\n'}},
        ]

        notes = f'\n\n─────────────────────────────\nCall Briefing  ({datetime.now().strftime("%Y-%m-%d")})\n─────────────────────────────\n'
        if data.get('va_notes_summary'):
            notes += f'{data["va_notes_summary"]}\n\n'
        if data.get('lead_temp'):           notes += f'Lead Temperature: {data["lead_temp"]}\n'
        if data.get('asking_price'):        notes += f'Asking Price: ${data["asking_price"]:,}\n'
        if data.get('estimated_arv'):       notes += f'Estimated ARV: ${data["estimated_arv"]:,}\n'
        if data.get('condition'):           notes += f'Condition: {data["condition"]}\n'
        if data.get('repairs_needed'):      notes += f'Repairs Needed: {data["repairs_needed"]}\n'
        if data.get('timeline'):            notes += f'Timeline: {data["timeline"]}\n'
        if data.get('motivation'):          notes += f'Motivation: {data["motivation"]}\n'
        if data.get('deal_type'):           notes += f'Deal Type: {data["deal_type"]}\n'
        if data.get('exit_strategy'):       notes += f'Exit Strategy: {data["exit_strategy"]}\n'
        if data.get('red_flags'):
            notes += f'Red Flags: {", ".join(data["red_flags"])}\n'
        if data.get('next_steps'):          notes += f'Next Steps: {data["next_steps"]}\n'
        if transcript:
            notes += f'\n─────────────────────────────\nCall Transcript\n─────────────────────────────\n{transcript[:3000]}\n'

        reqs.append({'insertText': {'endOfSegmentLocation': {'segmentId': ''}, 'text': notes}})

        svc['docs'].documents().batchUpdate(documentId=doc_id, body={'requests': reqs}).execute()
        return f'https://docs.google.com/document/d/{doc_id}/edit'
    except Exception as e:
        print(f'  Rehab doc error: {e}')
        return None


# ── GHL ──────────────────────────────────────────────────────────────────────
def fetch_acq_entries():
    entries, page = [], 1
    while True:
        r = requests.get('https://services.leadconnectorhq.com/opportunities/search',
                         headers=GHL_H,
                         params={'location_id': GHL_LOCATION, 'pipeline_id': PIPELINE_ID,
                                 'limit': 100, 'page': page})
        if r.status_code != 200: break
        data = r.json()
        opps = data.get('opportunities', [])
        if not opps: break
        for o in opps:
            c = o.get('contact') or {}
            if 'agent' not in c.get('tags', []) and o.get('contactId'):
                entries.append({'cid': o['contactId'], 'oid': o['id']})
        if page * 100 >= data.get('total', 0): break
        page += 1
        time.sleep(0.2)
    return entries


def get_opp_fields(oid):
    r = requests.get(f'https://services.leadconnectorhq.com/opportunities/{oid}', headers=GHL_H)
    if r.status_code != 200:
        return {}
    return {f['id']: (f.get('fieldValue') or '')
            for f in r.json().get('opportunity', {}).get('customFields', [])}


def sync_contact_to_opp(oid, contact_cf, opp_cf):
    """Mirror non-empty contact fields → empty opportunity fields."""
    updates = {}
    for cf_id, of_id in SYNC_MAP.items():
        v = contact_cf.get(cf_id, '')
        if v and not opp_cf.get(of_id, ''):
            updates[of_id] = v
    if updates:
        requests.put(f'https://services.leadconnectorhq.com/opportunities/{oid}',
                     headers=GHL_H,
                     json={'customFields': [{'id': k, 'field_value': str(v)} for k, v in updates.items()]})
    return updates


def process_contact(cid, oid, google_svc):
    r = requests.get(f'https://services.leadconnectorhq.com/contacts/{cid}', headers=GHL_H)
    if r.status_code != 200:
        return 'error'
    contact = r.json().get('contact', {})
    cfields = {f['id']: (f.get('value') or '') for f in contact.get('customFields', [])}

    rec_url = cfields.get(CF_CALL_REC, '')
    if not rec_url:
        return 'no_rec'

    if 'misc-media-ct.s3.amazonaws.com' in rec_url:
        path = urlparse(rec_url).path
        rec_url = f'https://d3njiazx9u20q.cloudfront.net{path}'

    try:
        resp = requests.get(rec_url, timeout=45)
        if resp.status_code != 200:
            return 'fail'
    except:
        return 'fail'

    transcript = transcribe(resp.content)
    if not transcript:
        return 'fail'

    # Claude first; regex fallback
    data = analyze_with_claude(transcript) or extract_fields_regex(transcript)

    # Append transcript so multiple calls preserved
    existing_tx = cfields.get(CF_AI_TX, '')
    if existing_tx:
        date_stamp = datetime.now().strftime('%Y-%m-%d %H:%M')
        combined = f"{existing_tx}\n\n--- New Call {date_stamp} ---\n{transcript}"
    else:
        combined = transcript

    cu = {CF_AI_TX: combined[:5000]}
    if 'misc-media-ct.s3.amazonaws.com' in (cfields.get(CF_CALL_REC) or ''):
        cu[CF_CALL_REC] = rec_url
    ou = {}

    def set_if_new(cf, of, val):
        if val in (None, '', 'Unknown'):
            return
        v = str(val)
        if not cfields.get(cf):
            cu[cf] = v
        if of:
            ou[of] = v

    set_if_new(CF_BED,         OF_BED,         data.get('beds'))
    set_if_new(CF_BATH,        OF_BATH,        data.get('baths'))
    set_if_new(CF_SQFT,        OF_SQFT,        data.get('sqft'))
    set_if_new(CF_PROP_TYPE,   OF_PROP_TYPE,   data.get('property_type'))
    set_if_new(CF_CONDITION,   OF_CONDITION,   data.get('condition'))
    set_if_new(CF_TIMELINE,    OF_TIMELINE,    data.get('timeline'))
    set_if_new(CF_MOTIVATION,  None,           data.get('motivation'))
    set_if_new(CF_REASON_SELL, None,           data.get('reason_for_selling'))
    set_if_new(CF_DEAL_TYPE,   OF_DEAL_TYPE,   data.get('deal_type'))
    set_if_new(CF_REPAIRS,     None,           data.get('repairs_needed'))
    set_if_new(None,           OF_EXIT_STRAT,  data.get('exit_strategy'))

    if data.get('asking_price') and not cfields.get(CF_ASK_PRICE):
        cu[CF_ASK_PRICE] = str(data['asking_price'])
    if data.get('estimated_arv') and not cfields.get(CF_ARV):
        cu[CF_ARV] = str(data['estimated_arv'])
        if not cfields.get(CF_70_ARV):
            cu[CF_70_ARV] = str(int(data['estimated_arv'] * 0.7))

    # VA Notes summary — overwrite each call so it stays fresh
    summary = data.get('va_notes_summary') or transcript[:800]
    cu[CF_VA_NOTES] = summary
    ou[OF_VA_NOTES] = summary

    # Strip None keys (set_if_new(None, …) leftovers)
    cu = {k: v for k, v in cu.items() if k}
    ou = {k: v for k, v in ou.items() if k}

    if cu:
        requests.put(f'https://services.leadconnectorhq.com/contacts/{cid}',
                     headers=GHL_H,
                     json={'customFields': [{'id': k, 'field_value': str(v)} for k, v in cu.items()]})
        time.sleep(0.1)
    if ou:
        requests.put(f'https://services.leadconnectorhq.com/opportunities/{oid}',
                     headers=GHL_H,
                     json={'customFields': [{'id': k, 'field_value': str(v)} for k, v in ou.items()]})
        time.sleep(0.1)

    # Rehab Report — only create if not already present
    opp_cf = get_opp_fields(oid)
    if google_svc and not opp_cf.get(OF_REHAB):
        addr_parts = [contact.get('address1',''), contact.get('city',''), contact.get('state','')]
        address = ', '.join(p for p in addr_parts if p) or \
                  f"{contact.get('firstName','')} {contact.get('lastName','')}".strip()
        doc_url = create_rehab_doc(google_svc, address, data, transcript)
        if doc_url:
            requests.put(f'https://services.leadconnectorhq.com/opportunities/{oid}',
                         headers=GHL_H, json={'customFields': [{'id': OF_REHAB, 'field_value': doc_url}]})
            print(f'  Rehab: {doc_url}')

    name = f"{contact.get('firstName','')} {contact.get('lastName','')}".strip()
    print(f'  OK: {name} | {data.get("beds")}/{data.get("baths")} | {data.get("deal_type")} | {data.get("lead_temp")}')
    return 'ok'


def main():
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] ACQ Automation starting...')
    print(f'Claude: {"ready" if ANTHROPIC_KEY else "NOT configured (will fall back to regex)"}')

    processed   = load_processed()
    google_svc  = get_google_services()
    print(f'Google Drive: {"ready" if google_svc else "not configured"}')
    print(f'Processed cache: {len(processed)} contacts')

    entries = fetch_acq_entries()
    print(f'Total in pipeline: {len(entries)}')

    ok = fail = skipped = synced = 0
    for e in entries:
        cid, oid = e['cid'], e['oid']

        r = requests.get(f'https://services.leadconnectorhq.com/contacts/{cid}', headers=GHL_H)
        if r.status_code != 200:
            time.sleep(0.05)
            continue
        contact = r.json().get('contact', {})
        cfields = {f['id']: (f.get('value') or '') for f in contact.get('customFields', [])}
        rec_url = cfields.get(CF_CALL_REC, '')

        # Field sync runs every time even with no new recording
        opp_cf = get_opp_fields(oid)
        s = sync_contact_to_opp(oid, cfields, opp_cf)
        if s:
            synced += 1
        time.sleep(0.05)

        if not rec_url:
            processed[cid] = ''
            continue

        # Normalize URL
        if 's3.amazonaws.com' in rec_url:
            rec_url_normalized = f'https://d3njiazx9u20q.cloudfront.net{urlparse(rec_url).path}'
        else:
            rec_url_normalized = rec_url.split('?')[0]

        if processed.get(cid) == rec_url_normalized:
            skipped += 1
            continue

        print(f'New recording for {contact.get("firstName","")} {contact.get("lastName","")}')
        result = process_contact(cid, oid, google_svc)
        time.sleep(0.1)

        if result == 'ok':
            processed[cid] = rec_url_normalized
            ok += 1
        elif result in ('has_transcript', 'no_rec'):
            processed[cid] = rec_url_normalized
            skipped += 1
        else:
            fail += 1

    save_processed(processed)
    print(f'\nDone: {ok} new transcripts | {synced} field-synced | {skipped} skipped | {fail} errors')


if __name__ == '__main__':
    main()
