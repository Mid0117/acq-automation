"""
ACQ Pipeline Automation — GitHub Actions edition
Single-run script: finds new call recordings, transcribes, updates GHL, creates Rehab Reports.
Scheduled every 15 min via GitHub Actions cron.
"""
import json, os, re, time, requests, warnings
from urllib.parse import urlparse
from datetime import datetime
warnings.filterwarnings('ignore')

GHL_TOKEN    = os.environ['GHL_TOKEN']
DG_KEY       = os.environ['DG_KEY']
GHL_LOCATION = 'RCkiUmWqXX4BYQ39JXmm'
PIPELINE_ID  = 'O8wzIa6E3SgD8HLg6gh9'
TEMPLATE_ID  = '1xYeKGmcbxJqCykxGXo1mEHBDEPQfbEHqXuS3cKrIliM'
PROCESSED_FILE = 'processed_contacts.json'

GHL_H = {'Authorization': f'Bearer {GHL_TOKEN}',
         'Content-Type': 'application/json', 'Version': '2021-07-28'}

CF_CALL_REC  = 'swEkGAoiPVsNF9gAwA2g'
CF_AI_TX     = 'CmX7LZ66JFFlo0ACFFoM'
CF_VA_NOTES  = 'ctNVXVw8VY1PD4B1oqXj'
CF_BED       = 'xXEm77wvbxEbiqsw3lAz'
CF_BATH      = 'EtKof5yT7KAWmoaNQqJZ'
CF_SQFT      = '8kqwjqtJyTTeQ8SIaLQz'
CF_PROP_TYPE = '7xsc1QHTleEFjRJChOgA'
CF_CONDITION = '1Q4MENz9a1PsCF4jEtOU'
CF_TIMELINE  = 'v47I1Mi63RBpCD5N5RrH'
CF_MOTIVATION= 'rbYZAdhvuvX1NQgexhxy'
CF_ASK_PRICE = '6q7syt4puxfP7E03Xxhd'

OF_BED       = 'NdjIxlmD8KGBJH7xQ0rv'
OF_BATH      = 'zl4RaWqAip1kmWax7YwI'
OF_SQFT      = 'PeHYon7Z5yv89Z9JtOws'
OF_PROP_TYPE = 'VgMGTqo5Em7aHT6u9z7E'
OF_CONDITION = 'pHig12D8t68DIU4M4lfG'
OF_TIMELINE  = 'NRYctFmTV6vckzbjrbi3'
OF_VA_NOTES  = 'RSi8RVZHqkdR7rC7hpLi'
OF_NUM_UNITS = 'w9OeqjXnlK5jjnm4IMFp'
OF_REHAB     = 'cPCQEuwOJNMtoWR8CrLR'


def load_processed():
    # Returns dict: {contactId: last_processed_recording_url}
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE) as f:
            data = json.load(f)
            # Handle old format (list of IDs) → upgrade to dict
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
        import tempfile

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write(token_json)
            token_path = f.name

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


def create_rehab_doc(svc, address, data, transcript):
    try:
        title = f'Rehab Report - {address}'
        copy = svc['drive'].files().copy(
            fileId=TEMPLATE_ID, body={'name': title}
        ).execute()
        doc_id = copy['id']

        today = datetime.now().strftime('%B %d, %Y')
        replacements = {
            '{{ADDRESS}}': address,      '[ADDRESS]': address,
            '{{DATE}}': today,            '[DATE]': today,
            '{{BEDS}}': data.get('beds','N/A'),
            '{{BATHS}}': data.get('baths','N/A'),
            '{{SQFT}}': data.get('sqft','N/A'),
            '{{CONDITION}}': data.get('condition','N/A'),
            '{{TIMELINE}}': data.get('timeline','N/A'),
            '{{ASKING_PRICE}}': data.get('asking_price','N/A'),
            '{{TRANSCRIPT}}': transcript[:3000] if transcript else '',
            '[TRANSCRIPT]': transcript[:3000] if transcript else '',
        }
        reqs = [
            {'replaceAllText': {
                'containsText': {'text': ph, 'matchCase': False},
                'replaceText': str(val)
            }}
            for ph, val in replacements.items() if val and val != 'N/A'
        ]
        if reqs:
            svc['docs'].documents().batchUpdate(documentId=doc_id, body={'requests': reqs}).execute()
        return f'https://docs.google.com/document/d/{doc_id}/edit'
    except Exception as e:
        print(f'  Rehab doc error: {e}')
        return None


def transcribe(audio_bytes):
    r = requests.post(
        'https://api.deepgram.com/v1/listen?model=nova-2&smart_format=true&language=en-US',
        headers={'Authorization': f'Token {DG_KEY}', 'Content-Type': 'audio/wav'},
        data=audio_bytes, timeout=60
    )
    if r.status_code == 200:
        return r.json()['results']['channels'][0]['alternatives'][0]['transcript']
    return None


def extract_fields(text):
    if not text:
        return {}
    t = text.lower()
    d = {}
    bb = re.search(r'(\d+)\s*/\s*(\d+)', t)
    if bb:
        d['beds'], d['baths'] = bb.group(1), bb.group(2)
    else:
        bm = re.search(r'(\d+)\s*bed', t)
        btm = re.search(r'(\d+)\s*bath', t)
        if bm: d['beds'] = bm.group(1)
        if btm: d['baths'] = btm.group(1)
    sq = re.search(r'([\d,]+)\s*(?:square\s*f(?:eet|t)|sq\.?\s*ft)', t)
    if sq: d['sqft'] = sq.group(1).replace(',', '')
    tl = re.search(r'(?:timeline|time ?frame|looking to sell)[^\n]{0,40}?'
                   r'(\d+\s*(?:day|week|month|year)s?|asap|immediately|soon)', t)
    if tl: d['timeline'] = tl.group(1)
    for kw in ['excellent','great','good','fair','poor','needs work','fixer','renovated']:
        if kw in t:
            d['condition'] = kw
            break
    pr = re.search(r'\$\s*([\d,]+(?:\.\d+)?)\s*([kKmM])?', text)
    if pr:
        val = pr.group(1).replace(',', '')
        mult = (pr.group(2) or '').upper()
        if mult == 'K': val = str(int(float(val) * 1000))
        elif mult == 'M': val = str(int(float(val) * 1000000))
        try:
            if float(val) >= 10000:
                d['asking_price'] = val
        except: pass
    for kw in ['retire','divorce','relocat','moving','death','inherit','foreclos']:
        if kw in t:
            d['motivation'] = kw
            break
    return d


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

    data = extract_fields(transcript)

    # Append to existing transcript so multiple calls are all preserved
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

    def set_if_empty(cf, of, key):
        val = data.get(key)
        if val:
            if not cfields.get(cf): cu[cf] = val
            if of: ou[of] = val

    set_if_empty(CF_BED, OF_BED, 'beds')
    set_if_empty(CF_BATH, OF_BATH, 'baths')
    set_if_empty(CF_SQFT, OF_SQFT, 'sqft')
    set_if_empty(CF_PROP_TYPE, OF_PROP_TYPE, 'property_type')
    set_if_empty(CF_CONDITION, OF_CONDITION, 'condition')
    set_if_empty(CF_TIMELINE, OF_TIMELINE, 'timeline')
    set_if_empty(CF_MOTIVATION, None, 'motivation')
    if data.get('asking_price') and not cfields.get(CF_ASK_PRICE):
        cu[CF_ASK_PRICE] = data['asking_price']
    if not cfields.get(CF_VA_NOTES):
        notes = transcript[:800]
        cu[CF_VA_NOTES] = notes
        ou[OF_VA_NOTES] = notes

    requests.put(f'https://services.leadconnectorhq.com/contacts/{cid}',
                 headers=GHL_H, json={'customFields': [{'id': k, 'field_value': str(v)} for k, v in cu.items()]})
    time.sleep(0.1)
    if ou:
        requests.put(f'https://services.leadconnectorhq.com/opportunities/{oid}',
                     headers=GHL_H, json={'customFields': [{'id': k, 'field_value': str(v)} for k, v in ou.items()]})
        time.sleep(0.1)

    # Rehab Report
    if google_svc:
        addr_parts = [contact.get('address1',''), contact.get('city',''), contact.get('state','')]
        address = ', '.join(p for p in addr_parts if p) or \
                  f"{contact.get('firstName','')} {contact.get('lastName','')}".strip()
        doc_url = create_rehab_doc(google_svc, address, data, transcript)
        if doc_url:
            requests.put(f'https://services.leadconnectorhq.com/opportunities/{oid}',
                         headers=GHL_H, json={'customFields': [{'id': OF_REHAB, 'field_value': doc_url}]})
            print(f'  Rehab: {doc_url}')

    name = f"{contact.get('firstName','')} {contact.get('lastName','')}".strip()
    print(f'  OK: {name} | beds={data.get("beds")} baths={data.get("baths")} cond={data.get("condition")}')
    return 'ok'


def main():
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] ACQ Automation starting...')

    processed = load_processed()  # {cid: last_processed_url}
    google_svc = get_google_services()
    print(f'Google Drive: {"ready" if google_svc else "not configured"}')
    print(f'Processed cache: {len(processed)} contacts')

    entries = fetch_acq_entries()
    print(f'Total in pipeline: {len(entries)}')

    ok = fail = skipped = 0
    for e in entries:
        cid, oid = e['cid'], e['oid']

        # Quick fetch to get recording URL before doing full processing
        r = requests.get(f'https://services.leadconnectorhq.com/contacts/{cid}', headers=GHL_H)
        if r.status_code != 200:
            time.sleep(0.05)
            continue
        contact = r.json().get('contact', {})
        cfields = {f['id']: (f.get('value') or '') for f in contact.get('customFields', [])}
        rec_url = cfields.get(CF_CALL_REC, '')

        if not rec_url:
            processed[cid] = ''
            time.sleep(0.05)
            continue

        # Normalize URL for comparison (strip query params, convert S3→CF)
        if 's3.amazonaws.com' in rec_url:
            path = urlparse(rec_url).path
            rec_url_normalized = f'https://d3njiazx9u20q.cloudfront.net{path}'
        else:
            rec_url_normalized = rec_url.split('?')[0]

        # Skip only if we already processed THIS exact recording URL
        if processed.get(cid) == rec_url_normalized:
            skipped += 1
            continue

        # New or updated recording — process it
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
    print(f'\nDone: {ok} new transcripts | {skipped} skipped | {fail} errors')


if __name__ == '__main__':
    main()
