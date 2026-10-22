/** The control panel: one self-contained page (no build step, no external requests). */
export const PANEL_HTML = /* html */ `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>sluice control panel</title>
<style>
:root {
  --bg: #f7f7f5; --panel: #ffffff; --ink: #1d1d1b; --muted: #6b6b66; --line: #e3e3de;
  --accent: #2f6fdb; --ok: #2e8b57; --warn: #c98a00; --bad: #c0392b; --chip: #efefea;
  --s1: #2f6fdb; --s2: #2e8b57; --s3: #c98a00;
}
@media (prefers-color-scheme: dark) {
  :root { --bg: #141413; --panel: #1d1d1b; --ink: #ececea; --muted: #9a9a94; --line: #2e2e2b;
    --accent: #6f9cf0; --ok: #5cbf86; --warn: #e0b040; --bad: #e57368; --chip: #2a2a27;
    --s1: #6f9cf0; --s2: #5cbf86; --s3: #e0b040; }
}
* { box-sizing: border-box; }
body { margin: 0; font: 14px/1.45 system-ui, -apple-system, Segoe UI, sans-serif; background: var(--bg); color: var(--ink); }
header { display: flex; align-items: center; gap: 12px; padding: 12px 16px; border-bottom: 1px solid var(--line); background: var(--panel); position: sticky; top: 0; z-index: 2; flex-wrap: wrap; }
header h1 { font-size: 16px; margin: 0; letter-spacing: .2px; }
header .meta { color: var(--muted); font-size: 12px; }
header .spacer { flex: 1; }
input, button, select { font: inherit; color: inherit; }
input { background: var(--bg); border: 1px solid var(--line); border-radius: 6px; padding: 6px 8px; min-width: 0; }
button { background: var(--chip); border: 1px solid var(--line); border-radius: 6px; padding: 6px 10px; cursor: pointer; }
button.primary { background: var(--accent); border-color: var(--accent); color: #fff; }
button.danger { color: var(--bad); }
button:disabled { opacity: .5; cursor: not-allowed; }
main { display: grid; grid-template-columns: minmax(280px, 380px) 1fr; gap: 16px; padding: 16px; max-width: 1400px; margin: 0 auto; }
@media (max-width: 900px) { main { grid-template-columns: 1fr; } }
.card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 14px; min-width: 0; }
.card h2 { font-size: 13px; text-transform: uppercase; letter-spacing: .6px; color: var(--muted); margin: 0 0 10px; font-weight: 600; }
.pool { padding: 10px; border-radius: 8px; cursor: pointer; border: 1px solid transparent; }
.pool:hover { background: var(--bg); }
.pool.active { border-color: var(--accent); background: var(--bg); }
.pool .row { display: flex; justify-content: space-between; gap: 8px; align-items: baseline; }
.pool .name { font-weight: 600; overflow-wrap: anywhere; }
.pool .ns { color: var(--muted); font-size: 12px; }
.bar { height: 6px; background: var(--chip); border-radius: 3px; overflow: hidden; margin-top: 6px; }
.bar > span { display: block; height: 100%; background: var(--accent); }
.badge { font-size: 11px; padding: 2px 7px; border-radius: 999px; background: var(--chip); white-space: nowrap; }
.badge.ok { color: var(--ok); } .badge.warn { color: var(--warn); } .badge.bad { color: var(--bad); }
.stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(110px, 1fr)); gap: 10px; margin-bottom: 14px; }
.stat { background: var(--bg); border-radius: 8px; padding: 10px; }
.stat .v { font-size: 22px; font-weight: 600; font-variant-numeric: tabular-nums; }
.stat .k { color: var(--muted); font-size: 12px; }
table { width: 100%; border-collapse: collapse; font-variant-numeric: tabular-nums; }
th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid var(--line); white-space: nowrap; }
th { color: var(--muted); font-weight: 500; font-size: 12px; }
.scroll { overflow-x: auto; }
.legend { display: flex; gap: 14px; color: var(--muted); font-size: 12px; margin: 6px 0 0; flex-wrap: wrap; }
.legend i { display: inline-block; width: 10px; height: 3px; vertical-align: middle; margin-right: 5px; border-radius: 2px; }
svg.chart { width: 100%; height: 180px; display: block; }
.grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
@media (max-width: 1100px) { .grid2 { grid-template-columns: 1fr; } }
.actions { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
.muted { color: var(--muted); }
.empty { color: var(--muted); padding: 24px 8px; text-align: center; }
.log { font: 12px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace; max-height: 260px; overflow: auto; }
.log div { padding: 2px 0; border-bottom: 1px dashed var(--line); overflow-wrap: anywhere; }
.err { color: var(--bad); font-size: 12px; min-height: 1em; }
</style>
</head>
<body>
<header>
  <h1>sluice</h1>
  <span class="meta" id="server">connecting…</span>
  <span class="spacer"></span>
  <input id="token" type="password" placeholder="admin token" aria-label="Admin token" autocomplete="off" size="16">
  <button id="saveToken">Save</button>
</header>
<main>
  <section class="card" aria-label="Targets">
    <h2>Targets</h2>
    <input id="filter" placeholder="Filter targets" aria-label="Filter targets" style="width:100%;margin-bottom:8px">
    <div id="pools"><div class="empty">No targets yet. Pools appear when an engine joins.</div></div>
  </section>
  <section id="detail" class="card"><div class="empty">Select a target to see its pool, callers and instances.</div></section>
</main>
<script>
"use strict";
const $ = (s) => document.querySelector(s);
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
let state = { pools: [], sessions: 0 };
let selected = null;
let actionsEnabled = false;
const tokenKey = "sluice.adminToken";
const getToken = () => { try { return localStorage.getItem(tokenKey) || ""; } catch { return ""; } };
$("#token").value = getToken();
$("#saveToken").onclick = () => { try { localStorage.setItem(tokenKey, $("#token").value); } catch {} render(); };
$("#filter").oninput = () => renderPools();

function health(p) {
  if (p.override === "0" || (p.override !== false && Number(p.override) === 0)) return ["paused", "bad"];
  if (p.waiters > 0) return ["contended", "warn"];
  if (p.instances.length === 0) return ["idle", ""];
  return ["ok", "ok"];
}

function renderPools() {
  const f = $("#filter").value.trim().toLowerCase();
  const list = state.pools.filter((p) => !f || p.pool.toLowerCase().includes(f));
  if (list.length === 0) { $("#pools").innerHTML = '<div class="empty">No matching targets.</div>'; return; }
  $("#pools").innerHTML = list.map((p) => {
    const [label, cls] = health(p);
    const pct = p.cap > 0 ? Math.min(100, Math.round((p.held / p.cap) * 100)) : 0;
    return '<div class="pool' + (p.pool === selected ? " active" : "") + '" data-pool="' + esc(p.pool) + '" tabindex="0" role="button">' +
      '<div class="row"><span class="name">' + esc(p.target) + '</span><span class="badge ' + cls + '">' + esc(label) + "</span></div>" +
      '<div class="row ns"><span>' + esc(p.namespace) + "</span><span>" + p.held + " / " + p.cap + " held · " + p.backlog + " queued</span></div>" +
      '<div class="bar"><span style="width:' + pct + '%"></span></div></div>';
  }).join("");
  for (const el of document.querySelectorAll(".pool")) {
    const open = () => { selected = el.dataset.pool; render(); loadDetail(); };
    el.onclick = open;
    el.onkeydown = (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(); } };
  }
}

function chart(points) {
  if (!points || points.length < 2) return '<div class="empty">Collecting history…</div>';
  const W = 600, H = 170, P = 24;
  const max = Math.max(1, ...points.map((p) => Math.max(p.cap, p.held, p.inFlight, p.backlog)));
  const t0 = points[0].t, t1 = points[points.length - 1].t || t0 + 1;
  const x = (t) => P + ((t - t0) / Math.max(1, t1 - t0)) * (W - 2 * P);
  const y = (v) => H - P + 4 - (v / max) * (H - 2 * P);
  const line = (key, color, dash) => '<polyline fill="none" stroke="' + color + '" stroke-width="2"' + (dash ? ' stroke-dasharray="5 4"' : "") +
    ' points="' + points.map((p) => x(p.t).toFixed(1) + "," + y(p[key]).toFixed(1)).join(" ") + '"/>';
  return '<svg class="chart" viewBox="0 0 ' + W + " " + H + '" role="img" aria-label="Capacity, held tokens and backlog over time">' +
    '<line x1="' + P + '" x2="' + (W - P) + '" y1="' + y(0) + '" y2="' + y(0) + '" stroke="var(--line)"/>' +
    '<text x="' + P + '" y="12" fill="var(--muted)" font-size="11">' + max + "</text>" +
    line("cap", "var(--s1)", true) + line("held", "var(--s2)") + line("backlog", "var(--s3)") + "</svg>" +
    '<div class="legend"><span><i style="background:var(--s1)"></i>capacity N</span><span><i style="background:var(--s2)"></i>tokens held</span><span><i style="background:var(--s3)"></i>queued</span></div>';
}

let detailCache = { history: [], log: [] };
async function loadDetail() {
  if (!selected) return;
  const enc = encodeURIComponent(selected);
  try {
    const [history, log] = await Promise.all([api("/api/pools/" + enc + "/history"), api("/api/pools/" + enc + "/log")]);
    detailCache = { history, log };
  } catch {}
  renderDetail();
}

function renderDetail() {
  const p = state.pools.find((x) => x.pool === selected);
  if (!p) { $("#detail").innerHTML = '<div class="empty">Select a target to see its pool, callers and instances.</div>'; return; }
  const callers = {};
  for (const i of p.instances) {
    const c = callers[i.caller] || (callers[i.caller] = { held: 0, share: 0, backlog: 0, n: 0, weight: i.weight });
    c.held += i.held; c.share += i.share; c.backlog += i.backlog; c.n++;
  }
  const [label, cls] = health(p);
  const canAct = actionsEnabled && getToken();
  $("#detail").innerHTML =
    '<div class="row" style="display:flex;justify-content:space-between;align-items:baseline;gap:8px;flex-wrap:wrap">' +
      '<h2 style="margin:0">' + esc(p.namespace) + " / " + esc(p.target) + '</h2><span class="badge ' + cls + '">' + esc(label) + "</span></div>" +
    '<div class="stats" style="margin-top:12px">' +
      stat("Capacity N", p.cap + (p.override !== false ? " 📌" : "")) + stat("Held", p.held) + stat("Free", p.free) +
      stat("Queued", p.backlog) + stat("Waiting instances", p.waiters) + stat("Debt", p.debt) + "</div>" +
    '<div class="grid2"><div><h2>History</h2>' + chart(detailCache.history) + "</div>" +
    "<div><h2>Callers</h2>" + (Object.keys(callers).length === 0 ? '<div class="empty">No callers connected.</div>' :
      '<div class="scroll"><table><thead><tr><th>Caller</th><th>Weight</th><th>Instances</th><th>Held</th><th>Fair share</th><th>Queued</th></tr></thead><tbody>' +
      Object.entries(callers).map(([name, c]) => "<tr><td>" + esc(name) + "</td><td>" + c.weight + "</td><td>" + c.n + "</td><td>" + c.held + "</td><td>" + c.share + "</td><td>" + c.backlog + "</td></tr>").join("") +
      "</tbody></table></div>") + "</div></div>" +
    '<h2 style="margin-top:16px">Instances</h2><div class="scroll"><table><thead><tr><th>Instance</th><th>Caller</th><th>Held</th><th>Share</th><th>In flight</th><th>Queued</th><th>Waiting</th><th>Lease</th><th>Capacity owner</th></tr></thead><tbody>' +
      (p.instances.map((i) => "<tr><td>" + esc(i.id) + "</td><td>" + esc(i.caller) + "</td><td>" + i.held + "</td><td>" + i.share + "</td><td>" + (i.stats ? i.stats.inFlight : "–") + "</td><td>" + i.backlog + "</td><td>" + (i.waiting ? "yes" : "") + "</td><td>" + Math.max(0, Math.round(i.leaseMs / 100) / 10) + "s</td><td>" + (p.owner === i.id ? "✓" : "") + "</td></tr>").join("") ||
      '<tr><td colspan="9" class="muted">No instances.</td></tr>') + "</tbody></table></div>" +
    '<div class="grid2" style="margin-top:16px"><div><h2>Actions</h2>' +
      (actionsEnabled ? "" : '<p class="muted">Actions are disabled on this server (set SLUICE_ADMIN_TOKEN).</p>') +
      '<div class="actions"><input id="cap" type="number" min="0" step="1" placeholder="capacity" aria-label="New capacity" style="width:110px"' + (canAct ? "" : " disabled") + '>' +
      '<button class="primary" id="setCap"' + (canAct ? "" : " disabled") + ">Pin capacity</button>" +
      '<button id="clearCap"' + (canAct && p.override !== false ? "" : " disabled") + ">Unpin</button>" +
      '<button class="danger" id="pause"' + (canAct ? "" : " disabled") + ">Pause</button>" +
      '<button id="resume"' + (canAct ? "" : " disabled") + ">Resume</button></div>" +
      '<p class="muted" style="font-size:12px">Pinning overrides the capacity source until unpinned. Pause sets N to 0; requests in flight finish, new ones wait.</p>' +
      '<div class="err" id="actErr" role="alert"></div></div>' +
    "<div><h2>Event log</h2><div class=\\"log\\">" + (detailCache.log.map((e) => "<div>" + esc(new Date(Number(e.at)).toLocaleTimeString()) + " · " + esc(e.type) + " " +
      esc(Object.entries(e).filter(([k]) => !["id", "at", "type"].includes(k)).map(([k, v]) => k + "=" + v).join(" ")) + "</div>").join("") || '<div class="muted">No events yet.</div>') +
    "</div></div></div>";
  const act = async (path, method, body) => {
    $("#actErr").textContent = "";
    try { await api("/api/pools/" + encodeURIComponent(p.pool) + path, method, body); await refresh(); await loadDetail(); }
    catch (e) { $("#actErr").textContent = e.message; }
  };
  if (canAct) {
    $("#setCap").onclick = () => { const v = Number($("#cap").value); if (!Number.isInteger(v) || v < 0) { $("#actErr").textContent = "Enter a whole number ≥ 0."; return; } act("/capacity", "POST", { capacity: v }); };
    $("#clearCap").onclick = () => act("/capacity", "DELETE");
    $("#pause").onclick = () => { if (confirm("Pause " + p.target + "? New requests will wait until resumed.")) act("/pause", "POST"); };
    $("#resume").onclick = () => act("/resume", "POST");
  }
}
const stat = (k, v) => '<div class="stat"><div class="v">' + esc(v) + '</div><div class="k">' + esc(k) + "</div></div>";

async function api(path, method = "GET", body) {
  const headers = { "content-type": "application/json" };
  const t = getToken();
  if (t) headers.authorization = "Bearer " + t;
  const res = await fetch(path, { method, headers, body: body ? JSON.stringify(body) : undefined });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || "HTTP " + res.status);
  return data;
}

function render() { renderPools(); renderDetail(); }

async function refresh() {
  try {
    const o = await api("/api/overview");
    state = { pools: o.pools, sessions: o.sessions.length };
    actionsEnabled = o.server.actions;
    $("#server").textContent = "server " + o.server.id + " · " + o.sessions.length + " instances connected here · " + o.pools.length + " targets";
    render();
  } catch (e) { $("#server").textContent = "error: " + e.message; }
}

function stream() {
  const t = getToken();
  const es = new EventSource("/api/events" + (t ? "?token=" + encodeURIComponent(t) : ""));
  es.onmessage = (ev) => {
    const d = JSON.parse(ev.data);
    state.pools = d.pools;
    render();
  };
  es.onerror = () => { es.close(); setTimeout(stream, 3000); };
}
refresh().then(stream);
setInterval(() => { if (selected) loadDetail(); }, 4000);
</script>
</body>
</html>`;
