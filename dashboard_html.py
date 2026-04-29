"""
Generate a beautiful HTML dashboard from sms_state.json + GHL data.
Output: site/index.html — pushed to gh-pages branch by the workflow.
"""
import json, os, requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from html import escape

GHL_TOKEN    = os.environ['GHL_TOKEN']
GHL_LOCATION = 'RCkiUmWqXX4BYQ39JXmm'
PIPELINE_ID  = 'O8wzIa6E3SgD8HLg6gh9'
STATE_FILE   = 'sms_state.json'
SHEET_ID     = os.environ.get('DASHBOARD_SHEET_ID', '')
OUT_DIR      = 'site'
OUT_FILE     = os.path.join(OUT_DIR, 'index.html')

ET = ZoneInfo('America/New_York')

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
    """Returns (kill_on: bool, templates_rows: list of {stage, idx, msg})."""
    if not SHEET_ID:
        return True, []
    token_json = os.environ.get('GOOGLE_TOKEN_JSON', '')
    if not token_json:
        return True, []
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

        return kill_on, rows
    except Exception:
        return True, []


def fetch_active():
    """Query each stage server-side (GHL 'total' field is unreliable)."""
    out = []
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
                    out.append({'cid': o['contactId'], 'oid': o['id'],
                                'stage': stage_label, 'tags': c.get('tags', [])})
            if len(opps) < 100:
                break
            page += 1
    return out


def get_contact(cid):
    r = requests.get(f'https://services.leadconnectorhq.com/contacts/{cid}', headers=GHL_H)
    if r.status_code != 200:
        return {}
    return r.json().get('contact', {})


HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>APG ACQ Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
:root {
  --bg: #0b0d12;
  --panel: rgba(255,255,255,0.04);
  --panel-border: rgba(255,255,255,0.08);
  --text: #e6e8ee;
  --text-dim: #8b93a7;
  --accent: #6366f1;
  --accent2: #06b6d4;
  --hot: #ef4444;
  --warm: #f59e0b;
  --green: #10b981;
  --gray: #475569;
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 0;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  background: radial-gradient(1200px 800px at 20% -10%, rgba(99,102,241,.15), transparent 60%),
              radial-gradient(1000px 600px at 100% 100%, rgba(6,182,212,.12), transparent 60%),
              var(--bg);
  color: var(--text);
  min-height: 100vh;
  -webkit-font-smoothing: antialiased;
}
.container { max-width: 1400px; margin: 0 auto; padding: 32px 24px 80px; }
header {
  display: flex; justify-content: space-between; align-items: end;
  margin-bottom: 32px; flex-wrap: wrap; gap: 12px;
}
h1 {
  font-size: 28px; font-weight: 700; letter-spacing: -0.02em;
  margin: 0; background: linear-gradient(90deg, #fff, #94a3b8);
  -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}
.subtitle { color: var(--text-dim); font-size: 13px; margin-top: 6px; }
.kpi-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 16px;
  margin-bottom: 32px;
}
.kpi {
  background: var(--panel);
  border: 1px solid var(--panel-border);
  backdrop-filter: blur(20px);
  border-radius: 16px;
  padding: 20px;
  position: relative;
  overflow: hidden;
}
.kpi::before {
  content: ''; position: absolute; inset: 0;
  background: linear-gradient(135deg, rgba(255,255,255,0.04), transparent 60%);
  pointer-events: none;
}
.kpi .label {
  font-size: 12px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.08em;
  margin-bottom: 8px;
}
.kpi .value { font-size: 32px; font-weight: 700; line-height: 1; }
.kpi .sub { color: var(--text-dim); font-size: 12px; margin-top: 6px; }
.kpi.hot .value { color: var(--hot); }
.kpi.warm .value { color: var(--warm); }
.kpi.green .value { color: var(--green); }
.charts-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  margin-bottom: 32px;
}
@media (max-width: 900px) { .charts-grid { grid-template-columns: 1fr; } }
.chart-card {
  background: var(--panel);
  border: 1px solid var(--panel-border);
  border-radius: 16px;
  padding: 24px;
  height: 320px;
}
.chart-card h3 {
  margin: 0 0 16px; font-size: 14px; font-weight: 600;
  text-transform: uppercase; letter-spacing: 0.08em; color: var(--text-dim);
}
.chart-card canvas { max-height: 240px; }
section { margin-bottom: 32px; }
.section-title {
  font-size: 18px; font-weight: 600; margin: 0 0 16px;
  display: flex; align-items: center; gap: 10px;
}
.dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }
.dot.hot { background: var(--hot); box-shadow: 0 0 12px var(--hot); }
.dot.warm { background: var(--warm); }
.dot.green { background: var(--green); }
.dot.gray { background: var(--gray); }
.lead-table {
  background: var(--panel);
  border: 1px solid var(--panel-border);
  border-radius: 16px;
  overflow: hidden;
}
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: left; padding: 14px 16px; background: rgba(255,255,255,0.02);
     font-size: 11px; text-transform: uppercase; letter-spacing: 0.08em;
     color: var(--text-dim); font-weight: 600; }
td { padding: 14px 16px; border-top: 1px solid var(--panel-border); }
tr:hover td { background: rgba(255,255,255,0.02); }
.tag {
  display: inline-block; padding: 3px 8px; font-size: 11px; border-radius: 4px;
  background: rgba(255,255,255,0.06); color: var(--text-dim); margin-right: 4px;
}
.tag.hot { background: rgba(239,68,68,0.15); color: var(--hot); }
.tag.warm { background: rgba(245,158,11,0.15); color: var(--warm); }
.tag.green { background: rgba(16,185,129,0.15); color: var(--green); }
.tag.gray { background: rgba(71,85,105,0.2); color: #94a3b8; }
.search { width: 100%; padding: 10px 14px; background: rgba(255,255,255,0.04);
          border: 1px solid var(--panel-border); border-radius: 10px;
          color: var(--text); font-size: 14px; margin-bottom: 16px; }
.search:focus { outline: none; border-color: var(--accent); }
.empty { text-align: center; color: var(--text-dim); padding: 40px; }
.status-banner {
  display: flex; align-items: center; justify-content: space-between;
  padding: 16px 20px; border-radius: 12px; margin-bottom: 24px;
  background: var(--panel); border: 1px solid var(--panel-border);
}
.status-banner.on  { border-color: rgba(16,185,129,0.4); }
.status-banner.off { border-color: rgba(239,68,68,0.6); background: rgba(239,68,68,0.08); }
.status-banner .status-text { font-size: 14px; font-weight: 600; display: flex; align-items: center; gap: 10px; }
.status-banner .pulse { width: 10px; height: 10px; border-radius: 50%; background: var(--green); box-shadow: 0 0 12px var(--green); animation: pulse 2s infinite; }
.status-banner.off .pulse { background: var(--hot); box-shadow: 0 0 12px var(--hot); animation: none; }
@keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }
.btn { display: inline-block; padding: 8px 16px; border-radius: 8px; font-size: 13px; font-weight: 600; text-decoration: none; transition: all 0.15s; }
.btn-kill { background: var(--hot); color: white; }
.btn-kill:hover { background: #dc2626; }
.btn-edit { background: rgba(99,102,241,0.2); color: #a5b4fc; border: 1px solid rgba(99,102,241,0.3); }
.btn-edit:hover { background: rgba(99,102,241,0.3); }
.template-grid {
  background: var(--panel); border: 1px solid var(--panel-border);
  border-radius: 12px; padding: 4px 16px;
}
.template-row {
  display: grid; grid-template-columns: 110px 40px 1fr; gap: 12px;
  padding: 10px 0; border-bottom: 1px solid var(--panel-border);
  align-items: start; font-size: 13px;
}
.template-row:last-child { border-bottom: none; }
.template-row .stage { color: var(--accent); font-weight: 600; text-transform: uppercase; font-size: 11px; letter-spacing: 0.06em; padding-top: 2px; }
.template-row .num { color: var(--text-dim); text-align: center; padding-top: 2px; }
.template-row .msg { color: var(--text); line-height: 1.5; }
footer { color: var(--text-dim); font-size: 12px; text-align: center; margin-top: 40px; }
</style>
</head>
<body>
<div class="container">
  <header>
    <div>
      <h1>APG ACQ — SMS Follow-Up Dashboard</h1>
      <div class="subtitle">Last updated: __TIMESTAMP__ (Eastern Time)</div>
    </div>
  </header>

  __STATUS_BANNER__

  <div class="kpi-grid">
    <div class="kpi"><div class="label">Total Leads</div><div class="value">__TOTAL__</div><div class="sub">across all stages</div></div>
    <div class="kpi"><div class="label">In Sequence</div><div class="value">__ACTIVE__</div><div class="sub">SMS scheduled</div></div>
    <div class="kpi hot"><div class="label">Replied — Call Now</div><div class="value">__REPLIED__</div><div class="sub">Jeff has tasks</div></div>
    <div class="kpi warm"><div class="label">Dormant — Manual Call</div><div class="value">__DORMANT__</div><div class="sub">no reply after sequence</div></div>
    <div class="kpi green"><div class="label">Reply Rate</div><div class="value">__RATE__%</div><div class="sub">replied / total touched</div></div>
  </div>

  <div class="charts-grid">
    <div class="chart-card">
      <h3>Leads by Stage</h3>
      <canvas id="stageChart"></canvas>
    </div>
    <div class="chart-card">
      <h3>Leads by State</h3>
      <canvas id="stateChart"></canvas>
    </div>
  </div>

  <div class="charts-grid">
    <div class="chart-card">
      <h3>SMS Progress</h3>
      <canvas id="progressChart"></canvas>
    </div>
    <div class="chart-card">
      <h3>From Numbers — Sends</h3>
      <canvas id="numberChart"></canvas>
    </div>
  </div>

  <section>
    <h2 class="section-title"><span class="dot hot"></span> Replied (Action Needed)</h2>
    <div class="lead-table">__REPLIED_TABLE__</div>
  </section>

  <section>
    <h2 class="section-title"><span class="dot warm"></span> Dormant (Manual Call Needed)</h2>
    <div class="lead-table">__DORMANT_TABLE__</div>
  </section>

  <section>
    <h2 class="section-title"><span class="dot green"></span> Active in Sequence</h2>
    <input type="text" class="search" id="searchActive" placeholder="Search by name, address, state...">
    <div class="lead-table">__ACTIVE_TABLE__</div>
  </section>

  <section>
    <h2 class="section-title"><span class="dot gray"></span> Active SMS Templates</h2>
    <div style="margin-bottom:12px;color:var(--text-dim);font-size:13px;">
      These are the messages currently being sent. Edit in the Templates tab of the
      <a href="__SHEET_URL__#gid=0" target="_blank" style="color:#a5b4fc;text-decoration:none;">Google Sheet</a>
      — changes apply on the next cron run.
    </div>
    <div class="template-grid">__TEMPLATES_BLOCK__</div>
  </section>

  <footer>Auto-refreshed every 30 minutes by the GitHub Actions cron</footer>
</div>

<script>
Chart.defaults.color = '#8b93a7';
Chart.defaults.borderColor = 'rgba(255,255,255,0.06)';
Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';

const palette = ['#6366f1','#06b6d4','#10b981','#f59e0b','#ef4444','#a855f7','#ec4899','#14b8a6'];

new Chart(document.getElementById('stageChart'), {
  type: 'doughnut',
  data: { labels: __STAGE_LABELS__, datasets: [{ data: __STAGE_DATA__, backgroundColor: palette, borderWidth: 0 }] },
  options: { plugins: { legend: { position: 'bottom', labels: { padding: 12, usePointStyle: true } } }, cutout: '65%' }
});

new Chart(document.getElementById('stateChart'), {
  type: 'bar',
  data: { labels: __STATE_LABELS__, datasets: [{ data: __STATE_DATA__, backgroundColor: '#06b6d4', borderRadius: 6 }] },
  options: { plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, ticks: { precision: 0 } } } }
});

new Chart(document.getElementById('progressChart'), {
  type: 'bar',
  data: { labels: ['0','1','2','3','4','5','6'], datasets: [{ label: 'Contacts', data: __PROGRESS_DATA__, backgroundColor: '#6366f1', borderRadius: 6 }] },
  options: { plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, ticks: { precision: 0 } } } }
});

new Chart(document.getElementById('numberChart'), {
  type: 'bar',
  data: { labels: __NUMBER_LABELS__, datasets: [{ data: __NUMBER_DATA__, backgroundColor: palette, borderRadius: 6 }] },
  options: { indexAxis: 'y', plugins: { legend: { display: false } }, scales: { x: { beginAtZero: true, ticks: { precision: 0 } } } }
});

// Search filter
document.getElementById('searchActive')?.addEventListener('input', e => {
  const q = e.target.value.toLowerCase();
  document.querySelectorAll('#activeTable tbody tr').forEach(r => {
    r.style.display = r.textContent.toLowerCase().includes(q) ? '' : 'none';
  });
});
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
        head = '<thead><tr><th>Contact</th><th>Address</th><th>Stage</th><th>SMS Sent</th><th>Last Sent</th></tr></thead>'
        body = '\n'.join(
            f'<tr><td><strong>{escape(r["name"])}</strong></td>'
            f'<td>{escape(r["addr"])}</td>'
            f'<td><span class="tag">{escape(r["stage"])}</span></td>'
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


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    sms_state = json.load(open(STATE_FILE)) if os.path.exists(STATE_FILE) else {}
    leads = fetch_active()
    print(f'HTML dashboard: {len(leads)} leads')

    kill_on, template_rows = read_sheet_kill_and_templates()
    sheet_url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit' if SHEET_ID else '#'

    by_stage   = {}
    by_state   = {}
    sms_progress = [0] * 7
    by_number  = {}
    replied = dormant = active = 0
    replied_rows = []
    dormant_rows = []
    active_rows  = []

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
            'name':      f"{c.get('firstName','')} {c.get('lastName','')}".strip() or '(no name)',
            'addr':      c.get('address1', '') or c.get('city', ''),
            'state':     c.get('state', '') or '',
            'stage':     e['stage'],
            'sms_count': n,
            'last_sms':  to_et(st.get('last_sms_at')),
            'replied_at': to_et(st.get('replied_at')),
            'from_num':  st.get('last_from_number', '') or '',
        }

        if st.get('replied'):
            replied_rows.append(row); replied += 1
        elif st.get('dormant'):
            dormant_rows.append(row); dormant += 1
        else:
            active_rows.append(row); active += 1

    total = len(leads)
    rate = (replied / max(1, replied + dormant + active)) * 100

    # Top 10 states for chart
    top_states = sorted(by_state.items(), key=lambda x: -x[1])[:10]
    top_numbers = sorted(by_number.items(), key=lambda x: -x[1])[:10]

    # Status banner
    if kill_on:
        banner = (
            '<div class="status-banner on">'
            '<div class="status-text"><span class="pulse"></span>SMS Automation: <span style="color:var(--green)">ACTIVE</span></div>'
            f'<a class="btn btn-kill" href="{sheet_url}#gid=0" target="_blank">EMERGENCY KILL SWITCH</a>'
            '</div>'
        )
    else:
        banner = (
            '<div class="status-banner off">'
            '<div class="status-text"><span class="pulse"></span>SMS Automation: <span style="color:var(--hot)">HALTED</span> — no messages will be sent</div>'
            f'<a class="btn btn-edit" href="{sheet_url}#gid=0" target="_blank">RE-ENABLE</a>'
            '</div>'
        )

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

    with open(OUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'Wrote {OUT_FILE} ({len(html)} bytes)')


if __name__ == '__main__':
    main()
