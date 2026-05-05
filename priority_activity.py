"""
priority_activity.py — build a per-deal activity feed for the Priority Deals page.

For each deal in site/priorities.json, we resolve to a GHL contact
(via search query against site/weekly/_state.json) and pull:

  - Recent GHL notes (last 10) — includes VA call summaries (auto-posted
    by acq_automation), Adam's feedback (from add_note workflow), and
    any team notes.
  - Latest call recording URL (from contact's CF_CALL_REC custom field).
  - Slack mentions for that contact this week (joined from latest weekly
    snapshot).

Output: site/priority_activity.json — read by priorities.html, no token
in browser. Refreshed each cron run.
"""
import json, os, re, sys, time
from pathlib import Path
import requests

ROOT = Path(__file__).parent
SITE = ROOT / 'site'

GHL_TOKEN  = os.environ['GHL_TOKEN']
GHL_H = {
    'Authorization': f'Bearer {GHL_TOKEN}',
    'Version': '2021-07-28',
    'Accept': 'application/json',
}
CF_CALL_REC = 'swEkGAoiPVsNF9gAwA2g'


def find_contact_in_state(state, query):
    q = (query or '').lower().strip()
    if not q:
        return None
    for cid, lead in state.items():
        blob = ' '.join([lead.get('name', ''), lead.get('addr', ''), lead.get('place', '')]).lower()
        if q in blob:
            return lead
    return None


def fetch_notes(cid, limit=10):
    try:
        r = requests.get(
            f'https://services.leadconnectorhq.com/contacts/{cid}/notes',
            headers=GHL_H, timeout=15,
        )
        if r.status_code != 200:
            print(f'  notes fetch {cid}: {r.status_code}', file=sys.stderr)
            return []
        notes = r.json().get('notes', []) or []
        notes.sort(key=lambda n: n.get('createdAt') or n.get('dateAdded') or '', reverse=True)
        out = []
        for n in notes[:limit]:
            body = (n.get('body') or '').strip()
            if not body:
                continue
            out.append({
                'ts':   n.get('createdAt') or n.get('dateAdded') or '',
                'body': body[:1500],
                'by':   classify_note_author(body),
            })
        return out
    except Exception as e:
        print(f'  notes fetch {cid} err: {e}', file=sys.stderr)
        return []


def classify_note_author(body):
    """Best-effort label so the UI can color-code who wrote what."""
    head = body[:120].lower()
    if 'priority deals · ' in head or 'priority deals ·' in head:
        m = re.search(r'(adam|mido|jeff|mike)\s+feedback', body[:200].lower())
        if m:
            return m.group(1).capitalize()
        return 'Team'
    if 'apg lead summary' in head or 'va call summary' in head or 'call summary' in head:
        return 'Call summary'
    if 'rehab report' in head:
        return 'Rehab report'
    return ''


def fetch_call_recording(cid):
    try:
        r = requests.get(
            f'https://services.leadconnectorhq.com/contacts/{cid}',
            headers=GHL_H, timeout=15,
        )
        if r.status_code != 200:
            return ''
        cf = r.json().get('contact', {}).get('customFields', []) or []
        for f in cf:
            if f.get('id') == CF_CALL_REC:
                v = (f.get('value') or '').strip()
                if v:
                    return v
        return ''
    except Exception as e:
        print(f'  call rec fetch {cid} err: {e}', file=sys.stderr)
        return ''


def load_slack_for_cid(cid):
    """Pull slack mentions for a CID from the latest weekly snapshot."""
    weekly = SITE / 'weekly'
    if not weekly.exists():
        return []
    try:
        idx = json.loads((weekly / 'index.json').read_text(encoding='utf-8'))
    except Exception:
        return []
    weeks = idx.get('weeks') or []
    if not weeks:
        return []
    try:
        snap = json.loads((weekly / f'{weeks[0]}.json').read_text(encoding='utf-8'))
    except Exception:
        return []
    out = []
    for sm in snap.get('slack_mentions') or []:
        if sm.get('cid') == cid:
            out.append({
                'ts':        sm.get('ts') or sm.get('ts_text') or '',
                'ts_text':   sm.get('ts_text') or '',
                'channel':   sm.get('channel') or '',
                'user':      sm.get('user') or '',
                'text':      (sm.get('original') or sm.get('text') or '')[:1500],
                'permalink': sm.get('permalink') or '',
            })
    return out


def main():
    if not (SITE / 'priorities.json').exists():
        print('no priorities.json — skipping')
        return
    if not (SITE / 'weekly' / '_state.json').exists():
        print('no _state.json — skipping (run weekly first)')
        return

    priorities = json.loads((SITE / 'priorities.json').read_text(encoding='utf-8'))
    state      = json.loads((SITE / 'weekly' / '_state.json').read_text(encoding='utf-8'))

    out = {
        'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'contacts': {},
    }

    for deal in priorities.get('deals', []):
        lead = find_contact_in_state(state, deal.get('search_query'))
        if not lead or not lead.get('cid'):
            print(f'{deal["id"]} {deal["address"]}: no GHL match')
            continue
        cid = lead['cid']
        print(f'{deal["id"]} {deal["address"]} → {cid}')

        notes = fetch_notes(cid)
        rec   = fetch_call_recording(cid)
        slack = load_slack_for_cid(cid)

        out['contacts'][cid] = {
            'notes':         notes,
            'call_recording': rec,
            'slack':         slack,
        }
        print(f'  {len(notes)} notes, {"rec✓" if rec else "no rec"}, {len(slack)} slack')
        time.sleep(0.1)

    (SITE / 'priority_activity.json').write_text(
        json.dumps(out, indent=2, ensure_ascii=False),
        encoding='utf-8',
    )
    print(f'Wrote priority_activity.json — {len(out["contacts"])} contacts')


if __name__ == '__main__':
    main()
