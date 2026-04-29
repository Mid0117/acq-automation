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
APIFY_TOKEN    = os.environ.get('APIFY_TOKEN', '')
SLACK_WEBHOOK  = os.environ.get('SLACK_WEBHOOK_URL', '')
GHL_LOCATION   = 'RCkiUmWqXX4BYQ39JXmm'
PIPELINE_ID    = 'O8wzIa6E3SgD8HLg6gh9'
TEMPLATE_ID    = '1xYeKGmcbxJqCykxGXo1mEHBDEPQfbEHqXuS3cKrIliM'
PROCESSED_FILE = 'processed_contacts.json'

# Only process opportunities in active deal stages (1-4). Skip Unqualified, Agents,
# Contract Sent and beyond (already advanced), Follow Up, Dead Deals.
STAGE_QUALIFIED = 'a17517be-8d1a-49fd-bd53-b9128a66e242'  # 1. Qualified Leads (Warm/Hot)
STAGE_LAO       = 'd43fddd8-3a17-46b2-a193-cf18619f654f'  # 2. Prequalified Offer (LAO)
STAGE_RR        = '23a159ad-ba39-4c74-9d07-c1beb219d9f2'  # 3. Due Diligence (RR)
STAGE_MAO       = '43589167-14f0-4e09-ba2a-8b9bd3296a4a'  # 4. Negotiate (MAO)
ACTIVE_STAGES = {STAGE_QUALIFIED, STAGE_LAO, STAGE_RR, STAGE_MAO}

# GHL user IDs for task assignment
USER_JEFF = 'vDKOqPSkA8nLkia5skd0'
USER_MIKE = 'Vj4WwH1ovxGN5Hv5Kq17'
USER_ADAM = 'vCjuvuuQ7p7K5GUODujQ'


def slack_post(blocks_or_text, fallback=''):
    """Post to APG Slack via incoming webhook. Silent no-op if SLACK_WEBHOOK is unset."""
    if not SLACK_WEBHOOK:
        return
    try:
        if isinstance(blocks_or_text, str):
            payload = {'text': blocks_or_text}
        else:
            payload = {'text': fallback or 'APG Automation', 'blocks': blocks_or_text}
        requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
    except Exception as e:
        print(f'    Slack post failed: {e}')

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
CF_ZILLOW        = '48pr9cc9hDFas111fDpF'

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
  "estimated_arv": <integer dollars — ONLY if seller or VA explicitly mentioned an after-repair value, comp price, or recently-sold neighbor — otherwise null. Do NOT guess.>,
  "deal_type": "Cash" | "Owner Finance" | "Subject-To" | "Wholesale" | "Lease Option" | "Hybrid" | "Unknown",
  "exit_strategy": "Flip" | "BRRRR" | "Wholesale" | "Buy & Hold" | "Owner Finance" | "Unknown",
  "repairs_needed": <short string listing major repairs mentioned, or null>,
  "lead_temp": "Hot" | "Warm" | "Cold",
  "va_notes_summary": <3-4 sentence professional briefing — what the seller said, condition, motivation, timeline, asking price, and how the call ended>,
  "red_flags": <array of short strings — title issues, occupancy, unrealistic price, missed payments, etc., or empty array>,
  "next_steps": <one sentence describing what was agreed at end of call, or null>,
  "call_rating": <integer 1-10 — how effective the call was: did the VA build rapport, ask qualifying questions, extract usable data, set clear next steps>,
  "could_improve": <array of 2-4 short strings — what the VA could have done better. Be specific and constructive. e.g. "Did not ask seller's bottom-line price", "Skipped occupancy/tenant question", "Should have set firm callback time">,
  "action_items": <array of 2-5 short strings — concrete next steps the team should take to advance this deal>
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


# ── Apify (Zillow property lookup) ───────────────────────────────────────────
HOME_TYPE_MAP = {
    'SINGLE_FAMILY': 'Single Family', 'CONDO': 'Condo', 'TOWNHOUSE': 'Townhouse',
    'MULTI_FAMILY': 'Multi-Family',   'APARTMENT': 'Multi-Family',
    'MANUFACTURED': 'Mobile Home',    'LOT': 'Land',
}


_APIFY_BUDGET_OK = None

def apify_has_budget():
    """Cache budget check across calls; skip if over monthly cap."""
    global _APIFY_BUDGET_OK
    if _APIFY_BUDGET_OK is not None:
        return _APIFY_BUDGET_OK
    if not APIFY_TOKEN:
        _APIFY_BUDGET_OK = False
        return False
    try:
        r = requests.get(f'https://api.apify.com/v2/users/me/limits?token={APIFY_TOKEN}', timeout=15)
        d = r.json().get('data', {})
        used = (d.get('current') or {}).get('monthlyUsageUsd', 0)
        cap  = (d.get('limits')  or {}).get('maxMonthlyUsageUsd', 29)
        _APIFY_BUDGET_OK = (cap - used) > 1.0   # need $1+ headroom for a call
        if not _APIFY_BUDGET_OK:
            print(f'  Apify budget exhausted: ${used:.2f}/${cap}; skipping property + comps lookups.')
        return _APIFY_BUDGET_OK
    except Exception:
        _APIFY_BUDGET_OK = True
        return True


def lookup_property(addr1, city, state):
    if not (APIFY_TOKEN and addr1 and city and state):
        return None
    if not apify_has_budget():
        return None
    s = addr1.replace(',', '').strip().replace(' ', '-')
    c = city.replace(',', '').strip().replace(' ', '-')
    url = f'https://www.zillow.com/homes/{s},-{c}-{state.strip()}_rb/'
    try:
        r = requests.post(
            f'https://api.apify.com/v2/acts/quiet_bark~zillow-scraper/run-sync-get-dataset-items?token={APIFY_TOKEN}',
            json={'mode': 'DETAIL', 'startUrls': [{'url': url}]},
            timeout=240
        )
        if r.status_code not in (200, 201):
            print(f'  Apify HTTP {r.status_code}: {r.text[:120]}')
            return None
        items = r.json()
        if not items:
            return None
        i = items[0]
        rf = i.get('resoFacts') or {}
        return {
            'beds':       i.get('bedrooms')  or rf.get('bedrooms'),
            'baths':      i.get('bathrooms') or rf.get('bathrooms'),
            'sqft':       i.get('livingAreaValue') or rf.get('livingArea'),
            'year_built': i.get('yearBuilt'),
            'lot_size':   i.get('lotAreaValue'),
            'lot_unit':   i.get('lotAreaUnitsShort') or 'acres',
            'home_type':  HOME_TYPE_MAP.get(i.get('homeType',''), i.get('homeType')),
            'zestimate':  i.get('zestimate') or i.get('price'),
            'rent_zest':  i.get('rentZestimate'),
            'zpid':       i.get('zpid'),
            'zillow_url': f"https://www.zillow.com{i.get('hdpUrl','')}" if i.get('hdpUrl') else url,
        }
    except Exception as e:
        print(f'  Apify error: {e}')
        return None


def fetch_sold_comps(city, state, zipcode, max_items=30):
    """Pull recently-sold listings near the property. Apify returns SOLD when given a /sold/ URL as a search query."""
    if not (APIFY_TOKEN and city and state):
        return []
    if not apify_has_budget():
        return []
    c = city.replace(' ', '-').strip().lower()
    s = state.strip().lower()
    z = (zipcode or '').strip().split('-')[0]  # ZIP+4 -> 5-digit
    url = f'https://www.zillow.com/{c}-{s}-{z}/sold/' if z else f'https://www.zillow.com/{c}-{s}/sold/'
    try:
        r = requests.post(
            f'https://api.apify.com/v2/acts/quiet_bark~zillow-scraper/run-sync-get-dataset-items?token={APIFY_TOKEN}',
            json={'mode': 'SEARCH', 'searchQueries': [url], 'maxItems': max_items},
            timeout=240
        )
        if r.status_code not in (200, 201):
            return []
        items = r.json() or []
        comps = []
        for it in items:
            if it.get('statusType') != 'SOLD':
                continue
            comps.append({
                'address': it.get('address') or '',
                'beds':    it.get('beds'),
                'baths':   it.get('baths'),
                'sqft':    it.get('area'),
                'price':   it.get('unformattedPrice'),
                'zpid':    it.get('zpid'),
                'url':     it.get('detailUrl') or '',
            })
        return comps[:max_items]
    except Exception as e:
        print(f'  Comps fetch error: {e}')
        return []


COMPS_SYSTEM = """You are a real estate appraiser computing ARV (After Repair Value) from sold comparables.

Given the subject property and a list of nearby SOLD properties, you must:
1. Pick the 5 most comparable properties (similar size ±25%, similar beds ±1, same property type if possible).
2. Compute median price-per-sqft of selected comps.
3. ARV = median $/sqft × subject sqft.
4. Discard outliers (huge/tiny size mismatches, condos vs SFH mismatches, etc).

Return ONLY valid JSON:
{
  "arv": <integer dollars>,
  "rationale": "<one short sentence on how comps were chosen>",
  "selected_comps": [
    {"address":"...","beds":N,"baths":N,"sqft":N,"sold_price":N,"price_per_sqft":N,"url":"..."},
    ...
  ]
}

If fewer than 3 valid comps exist, set arv to null."""


def estimate_arv_from_comps(subject, comps):
    if not (ANTHROPIC_KEY and comps and subject.get('sqft')):
        return None
    user_msg = f"""SUBJECT PROPERTY:
Address: {subject.get('address','')}
Beds: {subject.get('beds')} | Baths: {subject.get('baths')} | Sqft: {subject.get('sqft')}
Property Type: {subject.get('home_type','')}
Year Built: {subject.get('year_built','')}

NEARBY SOLD PROPERTIES (candidates):
{json.dumps(comps, indent=2)}"""
    body = {
        'model': 'claude-sonnet-4-5', 'max_tokens': 2000,
        'system': COMPS_SYSTEM,
        'messages': [{'role':'user','content':user_msg}],
    }
    headers = {'x-api-key': ANTHROPIC_KEY, 'anthropic-version':'2023-06-01', 'content-type':'application/json'}
    try:
        r = requests.post('https://api.anthropic.com/v1/messages', headers=headers, json=body, timeout=90)
        if r.status_code != 200:
            print(f'  Comps Claude {r.status_code}: {r.text[:200]}')
            return None
        text = r.json()['content'][0]['text'].strip()
        if text.startswith('```'):
            text = text.split('```',2)[1]
            if text.startswith('json'): text = text[4:]
            text = text.strip()
        return json.loads(text)
    except Exception as e:
        print(f'  Comps analysis failed: {e}')
        return None


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

        notes = ''
        ap = data.get('_apify') or {}
        if ap:
            notes += f'\n\n─────────────────────────────\nProperty Data (Zillow)\n─────────────────────────────\n'
            if ap.get('beds') or ap.get('baths'):  notes += f'Beds/Baths:    {ap.get("beds") or "?"}/{ap.get("baths") or "?"}\n'
            if ap.get('sqft'):                     notes += f'Living Area:   {ap["sqft"]:,} sqft\n'
            if ap.get('year_built'):               notes += f'Year Built:    {ap["year_built"]}\n'
            if ap.get('lot_size'):                 notes += f'Lot Size:      {ap["lot_size"]} {ap.get("lot_unit") or ""}\n'
            if ap.get('home_type'):                notes += f'Property Type: {ap["home_type"]}\n'
            if ap.get('zestimate'):                notes += f'Current Value (Zestimate): ${int(ap["zestimate"]):,}  (as-is, NOT ARV)\n'
            if ap.get('rent_zest'):                notes += f'Rent Estimate: ${int(ap["rent_zest"]):,}/mo\n'
            if ap.get('zillow_url'):               notes += f'Zillow Link:   {ap["zillow_url"]}\n'

        # COMPS USING CLAUDE
        comps = data.get('_comps') or {}
        if comps.get('selected_comps'):
            notes += f'\n\n─────────────────────────────\nCOMPS USING CLAUDE\n─────────────────────────────\n'
            if comps.get('arv'):
                notes += f'Estimated ARV: ${int(comps["arv"]):,}\n'
                notes += f'70% ARV (MAO): ${int(int(comps["arv"]) * 0.7):,}\n'
            if comps.get('rationale'):
                notes += f'Rationale: {comps["rationale"]}\n'
            notes += '\nSelected Comparables:\n'
            for c in comps['selected_comps']:
                notes += f'  • {c.get("address","?")}\n'
                notes += f'    {c.get("beds","?")}bd / {c.get("baths","?")}ba | {c.get("sqft","?")} sqft | Sold ${int(c.get("sold_price",0)):,} | ${c.get("price_per_sqft","?")}/sqft\n'
                if c.get('url'): notes += f'    {c["url"]}\n'

        notes += f'\n\n─────────────────────────────\nCall Briefing  ({datetime.now().strftime("%Y-%m-%d")})\n─────────────────────────────\n'
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
    """Query each active stage server-side (GHL 'total' field is unreliable)."""
    entries = []
    for stage_id in ACTIVE_STAGES:
        page = 1
        while True:
            r = requests.get('https://services.leadconnectorhq.com/opportunities/search',
                             headers=GHL_H,
                             params={'location_id': GHL_LOCATION, 'pipeline_id': PIPELINE_ID,
                                     'pipeline_stage_id': stage_id,
                                     'limit': 100, 'page': page})
            if r.status_code != 200: break
            opps = r.json().get('opportunities', [])
            if not opps: break
            for o in opps:
                c = o.get('contact') or {}
                if 'agent' not in c.get('tags', []) and o.get('contactId'):
                    entries.append({'cid': o['contactId'], 'oid': o['id']})
            if len(opps) < 100: break
            page += 1
            time.sleep(0.15)
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

    # Apify property + comps lookup. Stage filter (only stages 1-4) already
    # gates this — if we reached here, the lead is qualified.
    addr1 = contact.get('address1','').strip()
    city  = contact.get('city','').strip()
    state = contact.get('state','').strip()
    prop = lookup_property(addr1, city, state) if (addr1 and city and state) else None
    if prop:
        if prop.get('beds')        and not data.get('beds'):           data['beds']        = prop['beds']
        if prop.get('baths')       and not data.get('baths'):          data['baths']       = prop['baths']
        if prop.get('sqft'):                                            data['sqft']        = prop['sqft']
        if prop.get('home_type')   and not data.get('property_type'):  data['property_type']= prop['home_type']
        data['_apify'] = prop

        # Real ARV from sold comps + Claude
        zipc = (contact.get('postalCode') or '').strip()
        comps = fetch_sold_comps(city, state, zipc, max_items=30)
        if comps and prop.get('sqft'):
            comps_result = estimate_arv_from_comps({
                'address':    f"{addr1}, {city}, {state}",
                'beds':       data.get('beds'),
                'baths':      data.get('baths'),
                'sqft':       prop.get('sqft'),
                'home_type':  prop.get('home_type'),
                'year_built': prop.get('year_built'),
            }, comps)
            if comps_result:
                if comps_result.get('arv'):
                    data['estimated_arv'] = comps_result['arv']
                data['_comps'] = comps_result

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
    if data.get('estimated_arv'):
        cu[CF_ARV]    = str(int(data['estimated_arv']))
        cu[CF_70_ARV] = str(int(int(data['estimated_arv']) * 0.7))
    if data.get('_apify', {}).get('zillow_url') and not cfields.get(CF_ZILLOW):
        cu[CF_ZILLOW] = data['_apify']['zillow_url']

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

    # Auto-stage move: HOT lead in stage 1 -> stage 2 (LAO) so the team can present an offer.
    # Only Hot triggers; Warm/Cold stay in stage 1.
    rr2 = requests.get(f'https://services.leadconnectorhq.com/opportunities/{oid}', headers=GHL_H)
    if rr2.status_code == 200:
        opp = rr2.json().get('opportunity', {})
        current_stage = opp.get('pipelineStageId', '')
        if (data.get('lead_temp','').lower() == 'hot' and current_stage == STAGE_QUALIFIED):
            mv = requests.put(f'https://services.leadconnectorhq.com/opportunities/{oid}',
                              headers=GHL_H,
                              json={'pipelineStageId': STAGE_LAO})
            if mv.status_code == 200:
                print(f'  ➜ Auto-moved to LAO (Hot lead)')
                # Notify the team via GHL tasks
                lead_name = f"{contact.get('firstName','')} {contact.get('lastName','')}".strip() or '(no name)'
                addr = contact.get('address1','') or contact.get('city','') or '(no address)'
                # Build a rich task body with what Claude extracted
                body_lines = [
                    f'🔥 HOT lead from call analysis. Auto-moved Qualified → LAO.',
                    '',
                    f'Address: {addr}, {contact.get("city","")} {contact.get("state","")}',
                ]
                if data.get('asking_price'):    body_lines.append(f'Asking: ${int(data["asking_price"]):,}')
                if data.get('estimated_arv'):   body_lines.append(f'ARV (Claude): ${int(data["estimated_arv"]):,}')
                if data.get('deal_type'):       body_lines.append(f'Deal type: {data["deal_type"]}')
                if data.get('motivation'):      body_lines.append(f'Motivation: {data["motivation"]}')
                if data.get('timeline'):        body_lines.append(f'Timeline: {data["timeline"]}')
                if data.get('condition'):       body_lines.append(f'Condition: {data["condition"]}')
                if data.get('next_steps'):
                    body_lines.append('')
                    body_lines.append(f'Next steps from call: {data["next_steps"]}')
                body_lines.append('')
                body_lines.append('Action: present LAO offer ASAP.')
                task_body = '\n'.join(body_lines)
                # Create one task per team member
                from datetime import timezone as _tz
                due = datetime.now(_tz.utc).isoformat()
                for uid, who in [(USER_JEFF, 'Jeff'), (USER_ADAM, 'Adam'), (USER_MIKE, 'Mike')]:
                    try:
                        requests.post(
                            f'https://services.leadconnectorhq.com/contacts/{cid}/tasks',
                            headers=GHL_H,
                            json={
                                'title': f'🔥 HOT → LAO: {lead_name} ({addr})',
                                'body':  task_body,
                                'dueDate': due,
                                'completed': False,
                                'assignedTo': uid,
                            }, timeout=15
                        )
                    except Exception as e:
                        print(f'    Task for {who} failed: {e}')
                print(f'  ➜ Notification tasks created for Jeff + Adam + Mike')
                # Slack ping (only if SLACK_WEBHOOK_URL is set)
                slack_blocks = [
                    {'type':'header', 'text':{'type':'plain_text','text':f'🔥 Hot Lead → LAO: {lead_name}'}},
                    {'type':'section','text':{'type':'mrkdwn','text':task_body.replace('\n','\n')}},
                    {'type':'context','elements':[{'type':'mrkdwn','text':f'_Auto-moved from Qualified. Tasks created for Jeff, Adam, Mike._'}]},
                ]
                slack_post(slack_blocks, fallback=f'🔥 Hot Lead {lead_name} moved to LAO')

    # Write a "Claude Call Rating + Action Items" note on the contact
    if data.get('call_rating') or data.get('could_improve') or data.get('action_items'):
        note_lines = ['Claude Call Rating + Action Items', '=' * 38]
        if data.get('call_rating') is not None:
            note_lines.append(f'Rating: {data["call_rating"]}/10')
        if data.get('lead_temp'):
            note_lines.append(f'Lead Temperature: {data["lead_temp"]}')
        note_lines.append('')
        if data.get('could_improve'):
            note_lines.append('What could have been done better:')
            for item in data['could_improve']:
                note_lines.append(f'  • {item}')
            note_lines.append('')
        if data.get('action_items'):
            note_lines.append('Action items:')
            for item in data['action_items']:
                note_lines.append(f'  • {item}')
            note_lines.append('')
        if data.get('next_steps'):
            note_lines.append(f'Next steps from call: {data["next_steps"]}')
        if data.get('red_flags'):
            note_lines.append('')
            note_lines.append(f'Red flags: {", ".join(data["red_flags"])}')
        note_body = '\n'.join(note_lines)
        try:
            requests.post(f'https://services.leadconnectorhq.com/contacts/{cid}/notes',
                          headers=GHL_H, json={'body': note_body, 'userId': USER_MIKE},
                          timeout=15)
        except Exception as e:
            print(f'    Note write failed: {e}')

    name = f"{contact.get('firstName','')} {contact.get('lastName','')}".strip()
    print(f'  OK: {name} | {data.get("beds")}/{data.get("baths")} | {data.get("deal_type")} | {data.get("lead_temp")} | rating={data.get("call_rating")}')
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
