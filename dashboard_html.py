"""
Generate a beautiful HTML dashboard from sms_state.json + GHL data.
Output: site/index.html — pushed to gh-pages branch by the workflow.
"""
import json, os, time, requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from html import escape

GHL_TOKEN      = os.environ['GHL_TOKEN']
GHL_LOCATION   = 'RCkiUmWqXX4BYQ39JXmm'
PIPELINE_ID    = 'O8wzIa6E3SgD8HLg6gh9'
STATE_FILE     = 'sms_state.json'
CONTACTS_CACHE = 'contacts_cache.json'
SHEET_ID       = os.environ.get('DASHBOARD_SHEET_ID', '')
OUT_DIR        = 'site'
OUT_FILE       = os.path.join(OUT_DIR, 'index.html')

ET = ZoneInfo('America/New_York')

GHL_H = {'Authorization': f'Bearer {GHL_TOKEN}',
         'Content-Type': 'application/json', 'Version': '2021-07-28'}

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


def load_contacts_cache():
    """Loaded by sms_followup.py earlier in the workflow. Falls back to {} so
    we can still degrade to per-contact GETs."""
    if not os.path.exists(CONTACTS_CACHE):
        return {}
    try:
        d = json.load(open(CONTACTS_CACHE))
        return d.get('contacts', {}) or {}
    except Exception:
        return {}


def collect_run_status():
    """Returns last-run status for each cron workflow.

    Two sources:
    - GitHub Actions API → cross-workflow visibility (catches sms/acq/slack equally
      since they run in separate workflows)
    - Local last_run_*.json → catches script-level failures inside this workflow
      that didn't fail the whole job (e.g., partial errors).
    """
    out = []
    # 1. GitHub Actions API — most authoritative for "did the cron run + succeed"
    gh_token = os.environ.get('GITHUB_TOKEN', '')
    repo     = os.environ.get('GITHUB_REPOSITORY', 'Mid0117/acq-automation')
    if gh_token:
        try:
            r = http('GET', f'https://api.github.com/repos/{repo}/actions/runs',
                     headers={'Authorization': f'Bearer {gh_token}',
                              'Accept': 'application/vnd.github+json',
                              'X-GitHub-Api-Version': '2022-11-28'},
                     params={'per_page': 30})
            if r.status_code == 200:
                seen = {}
                for run in r.json().get('workflow_runs', []):
                    name = run.get('name', '')
                    if run.get('event') not in ('schedule', 'workflow_dispatch'):
                        continue
                    # Skip in-progress / queued runs — we only want the last
                    # COMPLETED run per workflow. The dashboard render itself
                    # runs inside one of these workflows, so its current run
                    # always has status='in_progress' / conclusion=null at this
                    # moment in time and we'd flag it as failed otherwise.
                    if run.get('status') != 'completed':
                        continue
                    if name in seen:
                        continue
                    success = run.get('conclusion') == 'success'
                    seen[name] = {
                        'success':   success,
                        'timestamp': run.get('updated_at') or run.get('created_at') or '',
                        'summary':   run.get('conclusion') or run.get('status') or '',
                        'error':     '' if success else f"{run.get('conclusion','')} — see logs",
                        'url':       run.get('html_url', ''),
                    }
                for name, st in seen.items():
                    out.append((name, st))
        except Exception as e:
            print(f'  GitHub status fetch failed: {e}')
    # 2. Local status files (within this workflow run — script-level granularity)
    for fname in sorted(os.listdir('.')):
        if fname.startswith('last_run_') and fname.endswith('.json'):
            try:
                out.append((fname[len('last_run_'):-len('.json')] + ' (script)',
                            json.load(open(fname))))
            except Exception:
                pass
    out.sort(key=lambda x: (1 if x[1].get('success') else 0, x[0]))
    return out

STAGE_NAMES = {
    'a17517be-8d1a-49fd-bd53-b9128a66e242': '1. Qualified',
    'd43fddd8-3a17-46b2-a193-cf18619f654f': '2. LAO',
    '23a159ad-ba39-4c74-9d07-c1beb219d9f2': '3. Due Diligence',
    '43589167-14f0-4e09-ba2a-8b9bd3296a4a': '4. MAO',
    '4aa78ab3-85dc-46d1-a683-d97b0c7a23ee': 'Follow Up 1.5mo',
    '571c115e-2603-4f3f-8546-d716f44ba8ef': 'Follow Up 3mo',
    'b9b560b0-30cb-47fc-a4ca-1e55ca2531e2': 'Dead Deals',
}


def to_et(iso):
    if not iso:
        return ''
    try:
        dt = datetime.fromisoformat(str(iso).replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(ET).strftime('%b %d, %I:%M %p')
    except Exception:
        return ''


def read_sheet_kill_and_templates():
    """Returns (kill_on: bool, templates_rows: list, templates_tab_gid: str)."""
    if not SHEET_ID:
        return True, [], ''
    token_json = os.environ.get('GOOGLE_TOKEN_JSON', '')
    if not token_json:
        return True, [], ''
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
        SCOPES = ['https://www.googleapis.com/auth/drive',
                  'https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
        if not creds.valid and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        svc = build('sheets', 'v4', credentials=creds)

        kill_on = True
        try:
            r = svc.spreadsheets().values().get(
                spreadsheetId=SHEET_ID, range="Settings!B2"
            ).execute()
            val = (r.get('values') or [[]])[0]
            if val and str(val[0]).strip().upper() in ('OFF', 'FALSE', 'NO', 'DISABLED'):
                kill_on = False
        except Exception:
            pass

        rows = []
        try:
            r = svc.spreadsheets().values().get(
                spreadsheetId=SHEET_ID, range="Templates!A2:C200"
            ).execute()
            for row in r.get('values', []):
                if len(row) >= 3 and row[0] and row[2]:
                    rows.append({'stage': row[0], 'idx': row[1], 'msg': row[2]})
        except Exception:
            pass

        # Find the gid for the Templates tab so we can embed it directly
        templates_gid = ''
        try:
            meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
            for s in meta.get('sheets', []):
                if s['properties']['title'] == 'Templates':
                    templates_gid = str(s['properties']['sheetId'])
                    break
        except Exception:
            pass

        return kill_on, rows, templates_gid
    except Exception:
        return True, [], ''


def fetch_active():
    """Query each stage server-side (GHL 'total' field is unreliable)."""
    out = []
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
                    out.append({'cid': o['contactId'], 'oid': o['id'],
                                'stage': stage_label, 'tags': c.get('tags', [])})
            if len(opps) < 100:
                break
            page += 1
    return out


# Cache filled by load_contacts_cache(); we look there before hitting GHL.
_CONTACTS_LOOKUP = {}


def get_contact(cid):
    cached = _CONTACTS_LOOKUP.get(cid)
    if cached:
        return cached
    r = http('GET', f'https://services.leadconnectorhq.com/contacts/{cid}', headers=GHL_H)
    if r.status_code != 200:
        return {}
    return r.json().get('contact', {})


# GHL user IDs -> friendly name
USERS = {
    'vDKOqPSkA8nLkia5skd0': 'Jeff',
    'Vj4WwH1ovxGN5Hv5Kq17': 'Mike',
    'vCjuvuuQ7p7K5GUODujQ': 'Adam',
    'duREBRmN19R12ixPfrvS': 'Wendy',
    '0tPk7tYJTs8r5vjeuAfL': 'Muhammad',
    '1X0bfFpMocO5hRewdjV0': 'John',
    'YCynATh5GncHo3kcA5KZ': 'Aaron',
    'vDYwvbLYnBziSZFoDLce': 'Jonathan',
}


def get_open_tasks(cid):
    """Get all open (not completed) tasks for a contact."""
    try:
        r = http('GET', f'https://services.leadconnectorhq.com/contacts/{cid}/tasks', headers=GHL_H)
        if r.status_code != 200:
            return []
        return [t for t in r.json().get('tasks', []) if not t.get('completed')]
    except Exception:
        return []


HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>APG ACQ — Follow-Ups Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
:root {
  --ink: #0A1F44;
  --ink-deep: #061331;
  --ink-soft: #1A3A7A;
  --gold: #F5C518;
  --gold-soft: #FFE58A;
  --gold-wash: #FFF6D0;
  --cream: #FAF7EC;
  --cream-deep: #F3EED8;
  --paper: #FFFFFF;
  --rule: #C9C2A8;
  --rule-soft: #E5E0C8;
  --muted: #5A6786;
  --muted-soft: #8A93AA;
  --text: #101827;
  --s-uc:   #B91C1C;
  --s-live: #10B981;
  --s-warm: #EA580C;
  --s-hold: #EAB308;
  --s-dead: #6B625A;
  /* legacy aliases — keep older selectors working */
  --bg: var(--cream);
  --paper-border: var(--rule);
  --panel: var(--paper);
  --panel-border: var(--rule);
  --ink-mute: var(--muted);
  --gold-deep: var(--ink);
  --rule-strong: var(--ink);
  --green: var(--s-live);
  --hot:   var(--s-uc);
  --warm:  var(--s-warm);
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 0;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
  background: var(--bg);
  color: var(--ink);
  line-height: 1.55;
  -webkit-font-smoothing: antialiased;
}
.container { max-width: 1200px; margin: 0 auto; padding: 28px 36px 80px; }

/* ── Top meta ─────────────────────────────────── */
.meta-bar {
  display: flex; justify-content: space-between; align-items: center;
  border-top: 4px solid var(--ink); padding: 14px 0 0;
  font-size: 11px; font-weight: 700;
  text-transform: uppercase; letter-spacing: 0.14em; color: var(--ink-soft);
}

/* ── Logo + nav row ───────────────────────────── */
.logo-row {
  display: flex; align-items: center; justify-content: space-between;
  gap: 16px; margin: 24px 0 12px; flex-wrap: wrap;
}
.logo-svg { width: 200px; height: auto; }
.logo-svg .atom-orbit { stroke: var(--gold-deep); stroke-width: 2.6; fill: none; }
.logo-svg .atom-core  { fill: var(--gold-deep); }
.logo-svg .brand-main { fill: var(--ink); }
.logo-svg .brand-sub  { fill: var(--ink-soft); letter-spacing: 4px; }
.nav { display: flex; gap: 6px; }
.nav a {
  padding: 6px 12px; border-radius: 3px;
  background: transparent; border: 1px solid var(--rule);
  color: var(--ink-soft); font-size: 11px; font-weight: 700;
  letter-spacing: 0.06em; text-transform: uppercase; text-decoration: none;
  transition: all .12s;
}
.nav a:hover { color: var(--ink); border-color: var(--ink); }
.nav a.active { background: var(--ink); color: var(--gold); border-color: var(--ink); }

/* ── Document header ──────────────────────────── */
.doc-header { padding: 28px 0 24px; }
.doc-header h1 {
  font-family: Georgia, "Times New Roman", serif;
  font-weight: 600;
  font-size: 48px; line-height: 1.05; letter-spacing: -0.01em;
  margin: 0 0 12px; color: var(--ink);
}
.doc-header h1 .accent { font-style: italic; color: var(--gold-deep); }
.doc-header .lede {
  font-family: Georgia, "Times New Roman", serif; font-style: italic;
  font-size: 16px; color: var(--ink-soft); max-width: 640px; margin: 0;
}
.doc-header hr { border: 0; border-top: 1px solid var(--rule); margin: 28px 0 0; }

/* ── Numbered section headers ─────────────────── */
.sec { margin: 44px 0 18px; }
.sec .tag-row { display: flex; align-items: center; gap: 12px; margin-bottom: 8px; flex-wrap: wrap; }
.sec .num {
  display: inline-block; background: var(--gold); color: var(--ink);
  font-weight: 800; font-size: 12px; letter-spacing: 0.04em;
  padding: 3px 8px; border-radius: 3px;
  font-family: ui-monospace, "SF Mono", monospace;
}
.sec h2 {
  font-family: Georgia, "Times New Roman", serif; font-weight: 600;
  font-size: 24px; letter-spacing: -0.005em; margin: 0; color: var(--ink);
}
.sec hr { border: 0; border-top: 1px solid var(--rule); margin: 0 0 16px; }

/* ── Status banners (kill switch + failure) ───── */
.status-banner {
  display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap;
  padding: 14px 18px; border-radius: 4px; margin-bottom: 12px;
  background: var(--paper); border: 1px solid var(--rule); gap: 12px;
}
.status-banner.on  { border-left: 3px solid var(--green); }
.status-banner.off { border-left: 3px solid var(--hot); background: rgba(197,68,58,0.05); }
.status-banner.failure {
  border: 1px solid rgba(197,68,58,0.40); border-left: 3px solid var(--hot);
  background: rgba(197,68,58,0.06);
}
.status-banner .status-text {
  font-size: 13px; font-weight: 700; display: flex; align-items: center; gap: 10px;
  color: var(--ink);
}
.status-banner.failure .status-text { color: var(--hot); }
.status-banner .pulse {
  width: 8px; height: 8px; border-radius: 50%;
  background: var(--green); animation: pulse 2s infinite;
}
.status-banner.off .pulse { background: var(--hot); animation: none; }
@keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }

.btn {
  display: inline-block; padding: 7px 14px; border-radius: 3px;
  font-size: 11px; font-weight: 800; letter-spacing: 0.06em;
  text-transform: uppercase; text-decoration: none; transition: all 0.15s;
}
.btn-kill { background: var(--hot); color: var(--paper); }
.btn-kill:hover { background: #A93730; }
.btn-edit { background: var(--ink); color: var(--gold); border: 1px solid var(--ink); }
.btn-edit:hover { background: var(--gold-deep); color: var(--ink); }

/* ── Run status grid ──────────────────────────── */
.run-grid {
  display: flex; flex-direction: column; gap: 6px;
  padding: 12px 16px; background: var(--paper);
  border: 1px solid var(--rule); border-radius: 4px;
  margin-bottom: 12px; font-size: 12px;
}
.run-grid-collapsed summary {
  cursor: pointer; padding: 10px 16px; color: var(--ink-soft);
  background: var(--paper); border: 1px solid var(--rule);
  border-radius: 4px; font-size: 12px; margin-bottom: 12px;
  font-weight: 700; letter-spacing: 0.04em;
}
.run-row { display: grid; grid-template-columns: 80px 160px 160px 1fr; gap: 12px; align-items: center; }
.run-row .run-name { color: var(--ink); font-weight: 700; }
.run-row .run-ts { color: var(--ink-mute); font-size: 11px; }
.run-row .run-detail { color: var(--ink-mute); font-family: monospace; font-size: 11px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

/* ── KPI / Stat blocks ────────────────────────── */
.stat-row {
  display: grid; gap: 10px;
  grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
  margin-top: 14px;
}
.stat {
  background: var(--paper); border: 1px solid var(--rule);
  border-top: 3px solid var(--gold);
  border-radius: 3px; padding: 16px 18px;
}
.stat .lab {
  font-size: 10px; font-weight: 800; letter-spacing: 0.12em;
  text-transform: uppercase; color: var(--ink-mute); margin-bottom: 8px;
}
.stat .v {
  font-family: Georgia, "Times New Roman", serif;
  font-size: 32px; font-weight: 600; color: var(--ink); line-height: 1.05;
}
.stat .sub { font-size: 12px; color: var(--ink-soft); margin-top: 6px; line-height: 1.4; }
.stat.green { border-top-color: var(--green); }
.stat.green .v { color: var(--green); }
.stat.hot { border-top-color: var(--hot); }
.stat.hot .v { color: var(--hot); }
.stat.warm { border-top-color: var(--warm); }
.stat.warm .v { color: var(--warm); }

/* ── Charts ───────────────────────────────────── */
.charts-grid {
  display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-top: 14px;
}
@media (max-width: 900px) { .charts-grid { grid-template-columns: 1fr; } }
.chart-card {
  background: var(--paper); border: 1px solid var(--rule);
  border-radius: 4px; padding: 18px; height: 320px;
}
.chart-card h3 {
  margin: 0 0 14px; font-size: 11px; font-weight: 800;
  letter-spacing: 0.12em; color: var(--ink-mute); text-transform: uppercase;
}
.chart-card canvas { max-height: 240px; }

/* ── Tables ───────────────────────────────────── */
.lead-table {
  background: var(--paper); border: 1px solid var(--rule);
  border-radius: 4px; overflow: hidden;
}
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: left; padding: 12px 16px;
     background: var(--gold-soft); color: var(--ink);
     font-size: 11px; text-transform: uppercase;
     letter-spacing: 0.06em; font-weight: 800; }
td { padding: 12px 16px; border-top: 1px solid var(--rule); color: var(--ink); }
tr:hover td { background: rgba(232,197,71,0.05); }
.tag {
  display: inline-block; padding: 2px 7px; font-size: 11px; font-weight: 700;
  border-radius: 3px; background: var(--gold-soft); color: var(--ink);
  letter-spacing: 0.04em;
}
.tag.hot   { background: rgba(197,68,58,0.15); color: var(--hot); }
.tag.warm  { background: rgba(181,122,26,0.15); color: var(--warm); }
.tag.green { background: rgba(47,125,91,0.15);  color: var(--green); }
.tag.gray  { background: rgba(26,40,64,0.08);   color: var(--ink-mute); }

.search {
  width: 100%; padding: 10px 14px; background: var(--paper);
  border: 1px solid var(--rule); border-radius: 4px;
  color: var(--ink); font-size: 14px; margin-bottom: 12px;
}
.search:focus { outline: none; border-color: var(--gold-deep); }
.empty { text-align: center; color: var(--ink-mute); padding: 40px; font-style: italic; }

/* ── Section dot indicators ───────────────────── */
.dot { width: 9px; height: 9px; border-radius: 50%; display: inline-block; margin-right: 4px; }
.dot.hot   { background: var(--hot); }
.dot.warm  { background: var(--warm); }
.dot.green { background: var(--green); }
.dot.gray  { background: var(--ink-mute); }

/* ── Templates editor ─────────────────────────── */
.template-grid {
  background: var(--paper); border: 1px solid var(--rule);
  border-radius: 4px; padding: 4px 16px;
}
.template-row {
  display: grid; grid-template-columns: 110px 40px 1fr; gap: 12px;
  padding: 10px 0; border-bottom: 1px solid var(--rule);
  align-items: start; font-size: 13px;
}
.template-row:last-child { border-bottom: none; }
.template-row .stage {
  color: var(--gold-deep); font-weight: 700;
  text-transform: uppercase; font-size: 11px;
  letter-spacing: 0.06em; padding-top: 2px;
}
.template-row .num { color: var(--ink-mute); text-align: center; padding-top: 2px; font-family: ui-monospace, monospace; }
.template-row .msg { color: var(--ink); line-height: 1.5; }

.help-line { color: var(--ink-soft); font-size: 13px; margin-bottom: 12px; }
.help-line strong { color: var(--ink); font-weight: 700; }

footer {
  color: var(--ink-mute); font-size: 11px; text-align: center;
  margin-top: 64px; padding-top: 18px;
  border-top: 1px solid var(--rule);
  letter-spacing: 0.04em;
}
a { color: var(--gold-deep); }
a:hover { color: var(--ink); }

/* ── Animations & micro-interactions ─────────────────────── */
@keyframes fade-in-up {
  from { opacity: 0; transform: translateY(10px); }
  to   { opacity: 1; transform: translateY(0); }
}
@keyframes fade-in {
  from { opacity: 0; }
  to   { opacity: 1; }
}
.fade-in-up { animation: fade-in-up 0.4s cubic-bezier(0.22, 1, 0.36, 1) backwards; }
.fade-in    { animation: fade-in 0.4s ease-out backwards; }

/* Smooth filter transitions */
.card, .lead-line, .stat, .bucket, .chart-card, .roadmap .item, .cron-card {
  transition: opacity 0.25s ease, transform 0.25s cubic-bezier(0.22, 1, 0.36, 1);
}
.card.filtered-out, .lead-line.filtered-out {
  opacity: 0; transform: scale(0.96) translateY(-4px); pointer-events: none;
}

/* Hover lift for clickable cards */
.card { will-change: transform; }
.card:hover {
  transform: translateY(-3px);
  box-shadow: 0 8px 24px rgba(26,40,64,0.08);
}
.bucket .lead-line { transition: background 0.15s ease, transform 0.15s ease; }
.bucket .lead-line:hover { transform: translateX(2px); }

/* Filter chip + button micro-interactions */
.filter-chip, .nav a, .btn, .ghl-link {
  transition: all 0.18s cubic-bezier(0.22, 1, 0.36, 1);
}
.filter-chip:active { transform: scale(0.96); }
.btn:active, .ghl-link:active { transform: scale(0.97); }

/* Stat KPI value entrance */
.stat .v { transition: color 0.3s ease; }

/* Smooth section reveal (set via JS-applied stagger) */
.bucket, .stat, .card { animation-fill-mode: backwards; }


/* ── MEETING-BRIEF MASTHEAD ────────────────────────── */
body { background: var(--cream); color: var(--text); font-family: "Helvetica Neue", Helvetica, Arial, sans-serif; }
.container, .shell { max-width: 1240px; margin: 0 auto; padding: 40px 64px 120px; background: var(--paper); min-height: 100vh; }
@media (max-width: 820px) { .container, .shell { padding: 32px 24px 80px; } }
.masthead {
  border-top: 5px solid var(--ink);
  border-bottom: 1px solid var(--rule);
  padding: 28px 0 24px; margin-bottom: 24px; position: relative;
}
.masthead::before {
  content: ""; position: absolute; left: 0; top: 0;
  width: 160px; height: 5px; background: var(--gold);
}
.brandrow {
  display: flex; justify-content: space-between; align-items: baseline;
  font-size: 11px; letter-spacing: 0.22em; text-transform: uppercase;
  color: var(--muted); margin-bottom: 16px; flex-wrap: wrap; gap: 8px;
}
.brand { color: var(--ink); font-weight: 700; }
.masthead h1 {
  font-family: Georgia, "Times New Roman", serif;
  font-size: 54px; line-height: 1.04; letter-spacing: -0.015em;
  margin: 0 0 14px; color: var(--ink); font-weight: 700;
}
.masthead h1 .accent { color: var(--gold); font-style: italic; }
.masthead .dek {
  font-family: Georgia, serif; font-style: italic; font-size: 18px;
  line-height: 1.5; color: var(--ink-soft); max-width: 780px; margin: 10px 0 0;
}
.topnav {
  position: sticky; top: 0; z-index: 50;
  background: rgba(250, 247, 236, 0.96); backdrop-filter: blur(6px);
  border-bottom: 1px solid var(--rule);
  margin: 0 -64px 28px; padding: 10px 64px;
  font-size: 11px; letter-spacing: 0.14em; text-transform: uppercase;
  color: var(--muted);
  display: flex; gap: 18px; overflow-x: auto; white-space: nowrap;
  align-items: center;
}
@media (max-width: 820px) { .topnav { margin: 0 -24px 24px; padding: 10px 24px; } }
.topnav a {
  color: var(--ink-soft); text-decoration: none; font-weight: 700; padding: 4px 0;
  border-bottom: 2px solid transparent;
}
.topnav a:hover, .topnav a.active { color: var(--ink); border-bottom: 2px solid var(--gold); }
section h2, h2 {
  font-family: Georgia, serif; font-size: 26px; color: var(--ink);
  border-bottom: 2px solid var(--ink); padding-bottom: 10px;
  display: flex; align-items: center; gap: 14px; flex-wrap: wrap;
  letter-spacing: -0.01em;
}

</style>
</head>
<body>
<div class="container">

  <header class="masthead">
    <div class="brandrow">
      <span class="brand">Atom Property Group · ACQ Operations</span>
      <span>Last updated __TIMESTAMP__ ET</span>
    </div>
    <h1>SMS <span class="accent">Follow-Ups.</span></h1>
    <p class="dek">Action queue for Jeff and Mike — replies to call back, leads gone dormant, and everything still cycling through the SMS sequence.</p>
  </header>

  <nav class="topnav">
    <a href="index.html" class="active">Follow-Ups</a>
    <a href="deals.html">Deals</a>
    <a href="weekly.html">Weekly</a>
    <a href="about.html">About</a>
  </nav>

  __STATUS_BANNER__

  <section class="sec">
    <div class="tag-row"><span class="num">01</span><h2>At a Glance</h2></div>
    <hr>
    <div class="stat-row">
      <div class="stat"><div class="lab">Total Leads</div><div class="v">__TOTAL__</div><div class="sub">across all stages</div></div>
      <div class="stat"><div class="lab">In Sequence</div><div class="v">__ACTIVE__</div><div class="sub">SMS scheduled</div></div>
      <div class="stat hot"><div class="lab">Replied — Call Now</div><div class="v">__REPLIED__</div><div class="sub">Jeff has tasks</div></div>
      <div class="stat warm"><div class="lab">Dormant — Manual Call</div><div class="v">__DORMANT__</div><div class="sub">no reply after sequence</div></div>
      <div class="stat green"><div class="lab">Reply Rate</div><div class="v">__RATE__%</div><div class="sub">replied / total touched</div></div>
    </div>
  </section>

  <section class="sec">
    <div class="tag-row"><span class="num">02</span><h2>Visual Breakdown</h2></div>
    <hr>
    <div class="charts-grid">
      <div class="chart-card"><h3>Leads by Stage</h3><canvas id="stageChart"></canvas></div>
      <div class="chart-card"><h3>Leads by State</h3><canvas id="stateChart"></canvas></div>
    </div>
    <div class="charts-grid">
      <div class="chart-card"><h3>SMS Progress</h3><canvas id="progressChart"></canvas></div>
      <div class="chart-card"><h3>From Numbers — Sends</h3><canvas id="numberChart"></canvas></div>
    </div>
  </section>

  <section class="sec">
    <div class="tag-row"><span class="num">03</span><h2><span class="dot hot"></span>Replied — Action Needed</h2></div>
    <hr>
    <div class="lead-table">__REPLIED_TABLE__</div>
  </section>

  <section class="sec">
    <div class="tag-row"><span class="num">04</span><h2><span class="dot warm"></span>Dormant — Manual Call Needed</h2></div>
    <hr>
    <div class="lead-table">__DORMANT_TABLE__</div>
  </section>

  <section class="sec">
    <div class="tag-row"><span class="num">05</span><h2><span class="dot green"></span>Active in Sequence</h2></div>
    <hr>
    <input type="text" class="search" id="searchActive" placeholder="Search by name, address, state...">
    <div class="lead-table">__ACTIVE_TABLE__</div>
  </section>

  <section class="sec">
    <div class="tag-row"><span class="num">06</span><h2><span class="dot hot"></span>Open Tasks</h2></div>
    <hr>
    <div class="lead-table">__TASKS_TABLE__</div>
  </section>

  <section class="sec">
    <div class="tag-row"><span class="num">07</span><h2><span class="dot gray"></span>SMS Templates — Edit Live</h2></div>
    <hr>
    <p class="help-line">
      Click any cell below to edit. Changes save instantly to the sheet and apply on the next 30-min cron run.
      <strong>You must be signed in to a Google account that has access</strong>
      (mike@atompropertygroup.org / atompropertygroup@gmail.com / jeff@atompropertygroup.org).
      <a class="btn btn-edit" style="float:right;" href="__TEMPLATES_EDIT_URL__" target="_blank">Open in Sheets ↗</a>
    </p>
    <iframe src="__TEMPLATES_EDIT_URL__"
            style="width:100%;height:600px;border:1px solid var(--rule);border-radius:4px;background:white;"
            allow="clipboard-read; clipboard-write"></iframe>
    <details style="margin-top:14px;">
      <summary style="cursor:pointer;color:var(--ink-soft);font-size:13px;font-weight:700;letter-spacing:0.04em;">Show templates as plain list (read-only)</summary>
      <div class="template-grid" style="margin-top:10px;">__TEMPLATES_BLOCK__</div>
    </details>
  </section>

  <footer>Auto-refreshed every 30 minutes by the GitHub Actions cron · APG ACQ Operating Layer</footer>
</div>

<script>
Chart.defaults.color = '#6B7591';
Chart.defaults.borderColor = 'rgba(26,40,64,0.08)';
Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif';

// Editorial cream palette: gold/navy/green/copper — readable on cream bg
const palette = ['#C9A52A','#1A2840','#2F7D5B','#B57A1A','#C5443A','#6B7591','#8B6914','#3F5275'];

new Chart(document.getElementById('stageChart'), {
  type: 'doughnut',
  data: { labels: __STAGE_LABELS__, datasets: [{ data: __STAGE_DATA__, backgroundColor: palette, borderWidth: 2, borderColor: '#FFFCF4' }] },
  options: { plugins: { legend: { position: 'bottom', labels: { padding: 12, usePointStyle: true, color: '#455066' } } }, cutout: '65%' }
});

new Chart(document.getElementById('stateChart'), {
  type: 'bar',
  data: { labels: __STATE_LABELS__, datasets: [{ data: __STATE_DATA__, backgroundColor: '#C9A52A', borderRadius: 4 }] },
  options: { plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, ticks: { precision: 0, color: '#6B7591' }, grid: { color: 'rgba(26,40,64,0.06)' } }, x: { ticks: { color: '#455066' }, grid: { display: false } } } }
});

new Chart(document.getElementById('progressChart'), {
  type: 'bar',
  data: { labels: ['0','1','2','3','4','5','6'], datasets: [{ label: 'Contacts', data: __PROGRESS_DATA__, backgroundColor: '#1A2840', borderRadius: 4 }] },
  options: { plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, ticks: { precision: 0, color: '#6B7591' }, grid: { color: 'rgba(26,40,64,0.06)' } }, x: { ticks: { color: '#455066' }, grid: { display: false } } } }
});

new Chart(document.getElementById('numberChart'), {
  type: 'bar',
  data: { labels: __NUMBER_LABELS__, datasets: [{ data: __NUMBER_DATA__, backgroundColor: palette, borderRadius: 4 }] },
  options: { indexAxis: 'y', plugins: { legend: { display: false } }, scales: { x: { beginAtZero: true, ticks: { precision: 0, color: '#6B7591' }, grid: { color: 'rgba(26,40,64,0.06)' } }, y: { ticks: { color: '#455066' }, grid: { display: false } } } }
});

// Search filter
document.getElementById('searchActive')?.addEventListener('input', e => {
  const q = e.target.value.toLowerCase();
  document.querySelectorAll('#activeTable tbody tr').forEach(r => {
    r.style.display = r.textContent.toLowerCase().includes(q) ? '' : 'none';
  });
});

// ── Animation runtime (count-up + stagger) ──────────────────
function animateNumber(el, durationMs) {
  durationMs = durationMs || 700;
  const original = el.textContent.trim();
  const m = original.match(/-?\d+(?:\.\d+)?/);
  if (!m) return;
  const target = parseFloat(m[0]);
  const decimals = (m[0].split('.')[1] || '').length;
  if (target === 0) return;  // nothing to animate
  const start = performance.now();
  function tick(now) {
    const p = Math.min(1, (now - start) / durationMs);
    const eased = 1 - Math.pow(1 - p, 3);
    const current = (target * eased).toFixed(decimals);
    el.textContent = original.replace(/-?\d+(?:\.\d+)?/, current);
    if (p < 1) requestAnimationFrame(tick);
    else el.textContent = original;
  }
  requestAnimationFrame(tick);
}

function staggerIn(selector, baseDelay) {
  baseDelay = baseDelay || 0;
  document.querySelectorAll(selector).forEach((el, i) => {
    el.style.animationDelay = (baseDelay + i * 35) + 'ms';
    el.classList.add('fade-in-up');
  });
}

function runEntranceAnimations(root) {
  root = root || document;
  root.querySelectorAll('.stat .v').forEach(el => animateNumber(el));
  // Stagger top-level grid children within key containers
  ['.stat-row', '.charts-grid', '.kpi-row'].forEach(sel => {
    root.querySelectorAll(sel).forEach(parent => {
      Array.from(parent.children).forEach((el, i) => {
        el.style.animationDelay = (i * 50) + 'ms';
        el.classList.add('fade-in-up');
      });
    });
  });
  ['.bucket', '.cron-card', '.roadmap .item', '.grid > .card'].forEach(sel => {
    root.querySelectorAll(sel).forEach((el, i) => {
      el.style.animationDelay = (i * 30) + 'ms';
      el.classList.add('fade-in-up');
    });
  });
}

// Run once on initial load. For pages that re-render content (weekly,
// follow-ups), they should call this again after replacing the DOM.
document.addEventListener('DOMContentLoaded', () => runEntranceAnimations());
window.runEntranceAnimations = runEntranceAnimations;  // expose for re-render

</script>
</body>
</html>
"""


PHONE_NICKNAMES = {
    '+12568006289': 'AL Market',  '+14707508168': 'GA Market',
    '+12603193698': 'IN Market',  '+14406169376': 'OH Market',
    '+12676197270': 'PA Market',  '+18037843538': 'SC Market',
    '+19013138258': 'TN Market',  '+14143489182': 'WI Market',
    '+16094388996': 'Jeff (NJ)',
}


def render_table(rows, kind):
    if not rows:
        return f'<div class="empty">No leads in this group right now.</div>'
    if kind == 'replied':
        head = '<thead><tr><th>Contact</th><th>Address</th><th>Stage</th><th>Replied</th><th>Last Number</th></tr></thead>'
        body = '\n'.join(
            f'<tr><td><strong>{escape(r["name"])}</strong></td>'
            f'<td>{escape(r["addr"])}</td>'
            f'<td><span class="tag">{escape(r["stage"])}</span></td>'
            f'<td><span class="tag hot">{escape(r["replied_at"])}</span></td>'
            f'<td>{escape(PHONE_NICKNAMES.get(r["from_num"], r["from_num"]))}</td></tr>'
            for r in rows
        )
    elif kind == 'dormant':
        head = '<thead><tr><th>Contact</th><th>Address</th><th>Stage</th><th>Reason</th><th>SMS Sent</th><th>Last Sent</th></tr></thead>'
        def _reason_tag(r):
            # Dormant-because-DND (sequence never started): GHL DND was on
            # before our first send, so we marked dormant with sms_count=0.
            # No manual call needed — they don't want to be contacted.
            if r.get('dnd') or r.get('reply_class') in ('HARD_STOP','HOSTILE','WRONG'):
                return '<span class="tag gray">DND — do not contact</span>'
            if r.get('sms_count', 0) == 0:
                return '<span class="tag gray">DND — sequence never started</span>'
            # Sequence-exhausted: 6 SMS sent, no reply → manual call needed
            return '<span class="tag warm">6 SMS no reply — call manually</span>'
        body = '\n'.join(
            f'<tr><td><strong>{escape(r["name"])}</strong></td>'
            f'<td>{escape(r["addr"])}</td>'
            f'<td><span class="tag">{escape(r["stage"])}</span></td>'
            f'<td>{_reason_tag(r)}</td>'
            f'<td><span class="tag warm">{r["sms_count"]}/6</span></td>'
            f'<td>{escape(r["last_sms"])}</td></tr>'
            for r in rows
        )
    else:  # active
        head = '<thead><tr><th>Contact</th><th>Address</th><th>State</th><th>Stage</th><th>SMS Sent</th><th>Last Sent</th><th>From Number</th></tr></thead>'
        body = '\n'.join(
            f'<tr><td><strong>{escape(r["name"])}</strong></td>'
            f'<td>{escape(r["addr"])}</td>'
            f'<td>{escape(r["state"])}</td>'
            f'<td><span class="tag">{escape(r["stage"])}</span></td>'
            f'<td><span class="tag green">{r["sms_count"]}</span></td>'
            f'<td>{escape(r["last_sms"])}</td>'
            f'<td>{escape(PHONE_NICKNAMES.get(r["from_num"], r["from_num"] or "—"))}</td></tr>'
            for r in rows
        )
    table_id = ' id="activeTable"' if kind == 'active' else ''
    return f'<table{table_id}>{head}<tbody>{body}</tbody></table>'


def write_status(success, summary='', error=''):
    try:
        with open('last_run_html.json', 'w') as f:
            json.dump({'success': success,
                       'timestamp': datetime.now(timezone.utc).isoformat(),
                       'summary': summary,
                       'error': error[:500]}, f, indent=2)
    except Exception:
        pass


def main():
    try:
        _main_inner()
        write_status(True, 'html dashboard rendered')
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f'!! html dashboard failed: {e}\n{tb}')
        write_status(False, '', f'{e}: {tb[-300:]}')
        raise


def _main_inner():
    os.makedirs(OUT_DIR, exist_ok=True)
    sms_state = json.load(open(STATE_FILE)) if os.path.exists(STATE_FILE) else {}
    # Hydrate contacts from sms_followup's cache → avoids per-lead GETs
    global _CONTACTS_LOOKUP
    _CONTACTS_LOOKUP = load_contacts_cache()
    if _CONTACTS_LOOKUP:
        print(f'Contacts cache: {len(_CONTACTS_LOOKUP)} entries (skipping per-lead GETs)')
    run_status = collect_run_status()
    leads = fetch_active()
    print(f'HTML dashboard: {len(leads)} leads')

    kill_on, template_rows, templates_gid = read_sheet_kill_and_templates()
    sheet_url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit' if SHEET_ID else '#'
    templates_edit_url = (f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit?usp=sharing&widget=true&headers=false&rm=embedded#gid={templates_gid}'
                          if SHEET_ID and templates_gid else sheet_url)

    by_stage   = {}
    by_state   = {}
    sms_progress = [0] * 7
    by_number  = {}
    replied = dormant = active = 0
    replied_rows = []
    dormant_rows = []
    active_rows  = []
    open_tasks   = []  # [{name, title, assignee, due, contact_url}]

    for e in leads:
        c = get_contact(e['cid'])
        st = sms_state.get(e['cid'], {})
        n  = st.get('sms_count', 0)
        if 0 <= n <= 6:
            sms_progress[n] += 1
        by_stage[e['stage']] = by_stage.get(e['stage'], 0) + 1
        s = (c.get('state') or 'Unknown').upper().strip() or 'Unknown'
        by_state[s] = by_state.get(s, 0) + 1
        fn = st.get('last_from_number', '')
        if fn:
            by_number[fn] = by_number.get(fn, 0) + 1

        row = {
            'name':       f"{c.get('firstName','')} {c.get('lastName','')}".strip() or '(no name)',
            'addr':       c.get('address1', '') or c.get('city', ''),
            'state':      c.get('state', '') or '',
            'stage':      e['stage'],
            'sms_count':  n,
            'last_sms':   to_et(st.get('last_sms_at')),
            'replied_at': to_et(st.get('replied_at')),
            'from_num':   st.get('last_from_number', '') or '',
            'dnd':        bool(st.get('dnd')),
            'reply_class': st.get('reply_class', ''),
        }

        if st.get('replied'):
            replied_rows.append(row); replied += 1
        elif st.get('dormant'):
            dormant_rows.append(row); dormant += 1
        else:
            active_rows.append(row); active += 1

        # Open tasks for this lead
        for t in get_open_tasks(e['cid']):
            open_tasks.append({
                'name': row['name'],
                'addr': row['addr'],
                'title': t.get('title', ''),
                'assignee': USERS.get(t.get('assignedTo', ''), 'Unassigned'),
                'due': to_et(t.get('dueDate', '')),
            })

    total = len(leads)
    rate = (replied / max(1, replied + dormant + active)) * 100

    # Top 10 states for chart
    top_states = sorted(by_state.items(), key=lambda x: -x[1])[:10]
    top_numbers = sorted(by_number.items(), key=lambda x: -x[1])[:10]

    # Status banner — kill switch + workflow-failure visibility
    if kill_on:
        banner = (
            '<div class="status-banner on">'
            '<div class="status-text"><span class="pulse"></span>SMS Automation: <span style="color:var(--green);font-weight:800">ACTIVE</span></div>'
            f'<a class="btn btn-kill" href="{sheet_url}#gid=0" target="_blank">EMERGENCY KILL SWITCH</a>'
            '</div>'
        )
    else:
        banner = (
            '<div class="status-banner off">'
            '<div class="status-text"><span class="pulse"></span>SMS Automation: <span style="color:var(--hot);font-weight:800">HALTED</span> — no messages will be sent</div>'
            f'<a class="btn btn-edit" href="{sheet_url}#gid=0" target="_blank">RE-ENABLE</a>'
            '</div>'
        )

    # Workflow failure visibility — prepend a red banner if any cron's last run failed.
    # Always show a small block listing each cron's status so silent staleness is obvious.
    if run_status:
        failed = [(n, s) for n, s in run_status if not s.get('success')]
        ok     = [(n, s) for n, s in run_status if s.get('success')]
        rows = ''
        for name, st in run_status:
            ts = to_et(st.get('timestamp'))
            tone = 'green' if st.get('success') else 'hot'
            label = 'OK' if st.get('success') else 'FAILED'
            detail = escape((st.get('error') or st.get('summary') or '')[:200])
            rows += (
                f'<div class="run-row">'
                f'<span class="tag {tone}">{label}</span>'
                f'<span class="run-name">{escape(name)}</span>'
                f'<span class="run-ts">{escape(ts)}</span>'
                f'<span class="run-detail">{detail}</span>'
                f'</div>'
            )
        if failed:
            failure_banner = (
                '<div class="status-banner failure">'
                f'<div class="status-text">⚠ <strong>{len(failed)} cron job(s) failed on their last run</strong> — automation is partially down.</div>'
                '<a class="btn btn-edit" href="https://github.com/Mid0117/acq-automation/actions" target="_blank">VIEW LOGS</a>'
                '</div>'
                f'<div class="run-grid">{rows}</div>'
            )
        else:
            failure_banner = (
                f'<details class="run-grid-collapsed"><summary>All {len(ok)} cron jobs OK on last run — click for timestamps</summary>'
                f'<div class="run-grid">{rows}</div></details>'
            )
        banner = failure_banner + banner

    # Templates block
    if template_rows:
        tmpl_html = ''.join(
            f'<div class="template-row">'
            f'<div class="stage">{escape(r["stage"])}</div>'
            f'<div class="num">#{escape(str(r["idx"]))}</div>'
            f'<div class="msg">{escape(r["msg"])}</div>'
            f'</div>'
            for r in template_rows
        )
    else:
        tmpl_html = '<div class="empty">Templates not yet seeded. Will populate on next cron run.</div>'

    html = HTML_TEMPLATE
    html = html.replace('__TIMESTAMP__', datetime.now(ET).strftime('%b %d, %Y %I:%M %p'))
    html = html.replace('__TOTAL__', str(total))
    html = html.replace('__ACTIVE__', str(active))
    html = html.replace('__REPLIED__', str(replied))
    html = html.replace('__DORMANT__', str(dormant))
    html = html.replace('__RATE__', f'{rate:.1f}')
    html = html.replace('__STAGE_LABELS__', json.dumps(list(by_stage.keys())))
    html = html.replace('__STAGE_DATA__', json.dumps(list(by_stage.values())))
    html = html.replace('__STATE_LABELS__', json.dumps([s for s, _ in top_states]))
    html = html.replace('__STATE_DATA__', json.dumps([n for _, n in top_states]))
    html = html.replace('__PROGRESS_DATA__', json.dumps(sms_progress))
    html = html.replace('__NUMBER_LABELS__', json.dumps([PHONE_NICKNAMES.get(p, p) for p, _ in top_numbers]))
    html = html.replace('__NUMBER_DATA__', json.dumps([n for _, n in top_numbers]))
    html = html.replace('__REPLIED_TABLE__', render_table(replied_rows, 'replied'))
    html = html.replace('__DORMANT_TABLE__', render_table(dormant_rows, 'dormant'))
    html = html.replace('__ACTIVE_TABLE__', render_table(active_rows, 'active'))
    html = html.replace('__STATUS_BANNER__', banner)
    html = html.replace('__TEMPLATES_BLOCK__', tmpl_html)
    html = html.replace('__SHEET_URL__', sheet_url)
    html = html.replace('__TEMPLATES_EDIT_URL__', templates_edit_url)

    # Render tasks table
    if open_tasks:
        head = '<thead><tr><th>Contact</th><th>Address</th><th>Task</th><th>Assignee</th><th>Due</th></tr></thead>'
        body = '\n'.join(
            f'<tr><td><strong>{escape(t["name"])}</strong></td>'
            f'<td>{escape(t["addr"])}</td>'
            f'<td>{escape(t["title"])}</td>'
            f'<td><span class="tag">{escape(t["assignee"])}</span></td>'
            f'<td>{escape(t["due"])}</td></tr>'
            for t in open_tasks
        )
        tasks_table = f'<table>{head}<tbody>{body}</tbody></table>'
    else:
        tasks_table = '<div class="empty">No open tasks on active leads. All caught up.</div>'
    html = html.replace('__TASKS_TABLE__', tasks_table)

    with open(OUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'Wrote {OUT_FILE} ({len(html)} bytes)')


if __name__ == '__main__':
    main()
