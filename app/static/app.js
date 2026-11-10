'use strict';

// Everything that comes from the server (run names, metric names) is user-controlled text.
// It is only ever placed with textContent / setAttribute, never as HTML.

const KEY_STORAGE = 'apiKey';

function getKey() {
  try { return sessionStorage.getItem(KEY_STORAGE); } catch { return null; }
}
function setKey(key) {
  try { key ? sessionStorage.setItem(KEY_STORAGE, key) : sessionStorage.removeItem(KEY_STORAGE); } catch { /* storage blocked */ }
}
function api(path, options = {}) {
  return fetch(path, {
    ...options,
    cache: 'no-store',
    // Never wait forever: on a dead network a request may get no answer and no error.
    signal: AbortSignal.timeout(options.timeoutMs || 10000),
    headers: { ...(options.headers || {}), Authorization: 'Bearer ' + (getKey() || '') },
  });
}
function $(id) { return document.getElementById(id); }
function show(node, text) {
  if (text !== undefined) node.textContent = text;
  node.hidden = false;
}
function cell(text, className) {
  const td = document.createElement('td');
  td.textContent = text;
  if (className) td.className = className;
  return td;
}

// --- run list page ---------------------------------------------------------------------------

function initIndex() {
  const loginSection = $('login-section');
  const runsSection = $('runs-section');
  // After login, return to the run page that sent us here. Only same-site run pages are accepted.
  const next = new URLSearchParams(location.search).get('next');
  const safeNext = next && /^\/runs\/[0-9a-f-]{36}\/view$/i.test(next) ? next : null;
  let timer = null;

  function showLogin(message) {
    clearInterval(timer);
    runsSection.hidden = true;
    $('logout').hidden = true;
    loginSection.hidden = false;
    $('login-error').hidden = true;
    if (message) show($('login-error'), message);
  }

  async function loadRuns() {
    let response;
    try {
      response = await api('/runs');
    } catch {
      show($('runs-error'), 'Cannot reach the server. Retrying…');
      return;
    }
    if (response.status === 401) {
      setKey(null);
      showLogin('That key was not accepted.');
      return;
    }
    if (!response.ok) {
      show($('runs-error'), 'The server returned an error. Retrying…');
      return;
    }
    $('runs-error').hidden = true;
    const { runs } = await response.json();
    const rows = runs.map((run) => {
      const tr = document.createElement('tr');
      const link = document.createElement('a');
      link.href = '/runs/' + encodeURIComponent(run.id) + '/view';
      link.textContent = run.name;
      const nameCell = document.createElement('td');
      nameCell.append(link);
      tr.append(nameCell, cell(new Date(run.created_at).toLocaleString()), cell(String(run.points), 'num'), cell(String(run.last_seq), 'num'));
      tr.dataset.runId = run.id;
      return tr;
    });
    $('runs-body').replaceChildren(...rows);
    $('runs-empty').hidden = runs.length > 0;
    $('runs-table').hidden = runs.length === 0;
  }

  async function showRuns() {
    if (safeNext) { location.replace(safeNext); return; }
    loginSection.hidden = true;
    runsSection.hidden = false;
    $('logout').hidden = false;
    await loadRuns();
    clearInterval(timer);
    timer = setInterval(loadRuns, 5000);
  }

  $('login-form').addEventListener('submit', (event) => {
    event.preventDefault();
    setKey($('key-input').value.trim());
    $('key-input').value = '';
    showRuns();
  });
  $('logout').addEventListener('click', () => { setKey(null); showLogin(); });
  $('new-run-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    const input = $('new-run-name');
    const response = await api('/runs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: input.value }),
    }).catch(() => null);
    if (response && response.ok) {
      input.value = '';
      $('runs-error').hidden = true;
    } else {
      show($('runs-error'), response && response.status === 422 ? 'That run name is not valid.' : 'Could not create the run.');
    }
    loadRuns();
  });

  getKey() ? showRuns() : showLogin();
}

// --- run page --------------------------------------------------------------------------------

function initRun() {
  const runId = location.pathname.split('/')[2];
  const SVG = 'http://www.w3.org/2000/svg';

  // The page's whole truth. The DOM is drawn from it and never read back.
  const metrics = new Map(); // name -> Map(step -> point)
  let total = 0;
  let lastSeq = 0;
  let status = 'connecting';
  let reconnects = 0;

  // Read by the E2E tests: exactly what this page believes the run contains.
  Object.defineProperty(window, '__state', {
    get() {
      const points = [];
      for (const steps of metrics.values()) {
        for (const p of steps.values()) points.push({ name: p.name, step: p.step, value: p.value, seq: p.seq });
      }
      points.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : a.step - b.step));
      return { run_id: runId, status, last_seq: lastSeq, total, reconnects, points };
    },
  });

  // --- rendering ---
  const sections = new Map(); // name -> { root, count, line, body }
  const dirty = new Set();
  let renderTimer = null;

  function scheduleRender() {
    if (renderTimer === null) renderTimer = setTimeout(render, 50);
  }

  function sectionFor(name) {
    let section = sections.get(name);
    if (section) return section;
    const root = document.createElement('section');
    root.className = 'metric';
    root.dataset.metric = name;
    const head = document.createElement('div');
    head.className = 'metric-head';
    const title = document.createElement('h3');
    title.textContent = name;
    const count = document.createElement('span');
    count.className = 'count';
    head.append(title, count);

    const svg = document.createElementNS(SVG, 'svg');
    svg.setAttribute('class', 'spark');
    svg.setAttribute('viewBox', '0 0 300 60');
    svg.setAttribute('preserveAspectRatio', 'none');
    svg.setAttribute('role', 'img');
    svg.setAttribute('aria-label', 'Values by step');
    const line = document.createElementNS(SVG, 'polyline');
    svg.append(line);

    const wrap = document.createElement('div');
    wrap.className = 'table-wrap';
    const table = document.createElement('table');
    const thead = document.createElement('thead');
    const headRow = document.createElement('tr');
    for (const [label, cls] of [['Step', 'num'], ['Value', 'num']]) {
      const th = document.createElement('th');
      th.textContent = label;
      th.className = cls;
      headRow.append(th);
    }
    thead.append(headRow);
    const body = document.createElement('tbody');
    table.append(thead, body);
    wrap.append(table);
    root.append(head, svg, wrap);

    // Keep sections in alphabetical order.
    const names = [...sections.keys(), name].sort();
    const after = names[names.indexOf(name) + 1];
    $('metrics').insertBefore(root, after ? sections.get(after).root : null);

    section = { root, count, line, body };
    sections.set(name, section);
    return section;
  }

  function render() {
    renderTimer = null;
    $('last-seq').textContent = String(lastSeq);
    $('total').textContent = String(total);
    $('metric-count').textContent = String(metrics.size);
    $('reconnects').textContent = String(reconnects);
    $('empty').hidden = total > 0;
    const badge = $('status');
    badge.dataset.status = status;
    badge.textContent = status.replace('_', ' ');

    for (const name of dirty) {
      const points = [...metrics.get(name).values()].sort((a, b) => a.step - b.step);
      const section = sectionFor(name);
      section.count.textContent = points.length + (points.length === 1 ? ' point' : ' points');
      section.body.replaceChildren(...points.map((p) => {
        const tr = document.createElement('tr');
        tr.dataset.step = String(p.step);
        tr.append(cell(String(p.step), 'num'), cell(String(p.value), 'num'));
        return tr;
      }));

      let low = Infinity;
      let high = -Infinity;
      for (const p of points) { low = Math.min(low, p.value); high = Math.max(high, p.value); }
      const spanX = Math.max(1, points[points.length - 1].step - points[0].step);
      const spanY = high - low || 1;
      section.line.setAttribute('points', points.map((p) => {
        const x = ((p.step - points[0].step) / spanX) * 300;
        const y = 56 - ((p.value - low) / spanY) * 52;
        return x.toFixed(1) + ',' + y.toFixed(1);
      }).join(' '));
    }
    dirty.clear();
  }

  function setStatus(next) {
    if (status === 'connected' && next === 'reconnecting') reconnects += 1;
    status = next;
    scheduleRender();
  }

  // --- applying events ---
  function apply(point) {
    // Events arrive in seq order and a reconnect resumes after lastSeq, so anything at or below
    // it is a replay and is ignored. Keying by (name, step) makes a replay harmless even so.
    if (point.seq <= lastSeq) return;
    lastSeq = point.seq;
    let steps = metrics.get(point.name);
    if (!steps) { steps = new Map(); metrics.set(point.name, steps); }
    if (!steps.has(point.step)) total += 1;
    steps.set(point.step, point);
    dirty.add(point.name);
    scheduleRender();
  }

  // --- connection ---
  let source = null;
  let stopped = false;
  let attempt = 0;
  let retryTimer = null;
  let watchdog = null;
  let staleTimer = null;
  let staleAfterMs = 45000; // replaced by 3 x the server's ping interval once it says hello

  // A connection can die without any error reaching the page (half-open TCP, a proxy that
  // stopped forwarding). The server pings regularly; if nothing at all arrives for three ping
  // intervals, assume the connection is dead and start a new one.
  function heardFromServer() {
    clearTimeout(staleTimer);
    staleTimer = setTimeout(() => {
      if (stopped) return;
      setStatus('reconnecting');
      connect();
    }, staleAfterMs);
  }

  function stop(finalStatus, message) {
    stopped = true;
    clearTimeout(retryTimer);
    clearTimeout(watchdog);
    clearTimeout(staleTimer);
    if (source) source.close();
    setStatus(finalStatus);
    show($('message'), message);
  }

  function retryLater() {
    setStatus('reconnecting');
    attempt += 1;
    clearTimeout(retryTimer);
    retryTimer = setTimeout(connect, Math.min(5000, 250 * 2 ** Math.min(attempt, 5)));
  }

  async function connect() {
    if (stopped) return;
    clearTimeout(retryTimer);
    clearTimeout(watchdog);
    clearTimeout(staleTimer);
    if (source) { source.close(); source = null; }

    // EventSource cannot send the Authorization header, so trade the key for a 60 s stream token.
    let response;
    try {
      response = await api('/runs/' + runId + '/stream-token', { method: 'POST', timeoutMs: 5000 });
    } catch {
      return retryLater();
    }
    if (response.status === 401) return stop('unauthorized', 'This API key is no longer valid. Go back and enter a current key.');
    if (response.status === 404) return stop('not_found', 'Run not found.');
    if (!response.ok) return retryLater();
    const { token } = await response.json();
    if (stopped) return;

    const es = new EventSource('/runs/' + runId + '/stream?token=' + encodeURIComponent(token) + '&since_seq=' + lastSeq);
    source = es;
    es.onopen = () => {
      if (source !== es) return;
      attempt = 0;
      clearTimeout(watchdog);
      setStatus('connected');
      heardFromServer();
    };
    es.addEventListener('hello', (event) => {
      if (source !== es) return;
      staleAfterMs = Math.max(3000, JSON.parse(event.data).ping_seconds * 3000);
      heardFromServer();
    });
    es.addEventListener('ping', () => { if (source === es) heardFromServer(); });
    es.addEventListener('metric', (event) => {
      if (source !== es) return;
      heardFromServer();
      apply(JSON.parse(event.data));
    });
    es.addEventListener('revoked', () => stop('unauthorized', 'This API key was revoked. Go back and enter a current key.'));
    es.onerror = () => {
      if (stopped || source !== es) return;
      clearTimeout(staleTimer);
      setStatus('reconnecting');
      if (es.readyState === EventSource.CLOSED) {
        // The browser gave up (e.g. the token expired and the retry got 401): start over with a
        // fresh token, resuming from lastSeq.
        retryLater();
      } else {
        // The browser is retrying by itself and will send Last-Event-ID. If that has not
        // worked within a few seconds, take over.
        clearTimeout(watchdog);
        watchdog = setTimeout(() => {
          if (source === es && es.readyState !== EventSource.OPEN) connect();
        }, 8000);
      }
    };
  }

  async function start() {
    if (!getKey()) {
      location.replace('/?next=' + encodeURIComponent(location.pathname));
      return;
    }
    render();
    const response = await api('/runs/' + runId).catch(() => null);
    if (response && response.status === 401) return stop('unauthorized', 'This API key is not valid. Go back and enter a current key.');
    if (response && response.status === 404) return stop('not_found', 'Run not found.');
    if (response && response.ok) {
      const run = await response.json();
      $('run-name').textContent = run.name;
      document.title = run.name + ' · Experiment Tracker';
    }
    connect();
  }

  start();
}

if (document.body.dataset.page === 'index') initIndex();
if (document.body.dataset.page === 'run') initRun();
