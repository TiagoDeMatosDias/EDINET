/**
 * Security Analysis screen logic.
 */

import { log } from '../common/console.js';
import { fetchJson } from '../common/utils.js';

const state = {
  initDone: false, company: null, formulas: null, history: null,
  activeTable: null, viewMode: 'table', searchFilter: '',
  hiddenMetrics: {}, millions: false,
  chartInstances: [], loading: false, error: null,
  searchResults: [], searchIdx: -1, searchTimer: null, eventBound: false,
};

export function init() {
  try {
    const s = sessionStorage.getItem('sa.state');
    if (s) {
      const p = JSON.parse(s);
      // hiddenMetrics is serialized as { tableName: [field1, field2, ...] }
      if (p.hiddenMetrics) {
        const hm = {};
        for (const [key, val] of Object.entries(p.hiddenMetrics)) {
          hm[key] = new Set(Array.isArray(val) ? val : []);
        }
        p.hiddenMetrics = hm;
      }
      Object.assign(state, p);
    }
  } catch (e) {}
}

export function markReady() { state.initDone = true; }

function persist() {
  try {
    const hm = {};
    for (const [key, s] of Object.entries(state.hiddenMetrics)) {
      hm[key] = s instanceof Set ? [...s] : [];
    }
    sessionStorage.setItem('sa.state', JSON.stringify({
      activeTable: state.activeTable, viewMode: state.viewMode,
      millions: state.millions, hiddenMetrics: hm,
    }));
  } catch (e) {}
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

export function render() { renderToolbar(); renderContent(); }

function renderContent() {
  const empty = H.$('sa-empty'), hdr = H.$('sa-header'), banner = H.$('sa-banner');
  const tabbar = H.$('sa-tabbar'), tv = H.$('sa-table-view');
  const et = H.$('sa-empty-title'), es = H.$('sa-empty-sub');

  if (!state.initDone) {
    hideAll(hdr, banner, tabbar, tv); show(empty);
    et.textContent = 'Initializing…'; if (es) es.textContent = 'Connecting…'; return;
  }
  if (state.error) {
    hideAll(hdr, banner, tabbar, tv); show(empty);
    et.textContent = 'Error'; if (es) es.textContent = state.error; return;
  }
  if (state.loading) {
    hdr.classList.add('is-visible'); hdr.innerHTML = '<div class="sa-loading">Loading…</div>';
    hideAll(banner, tabbar, tv); empty.classList.add('hidden'); return;
  }
  if (state.company) {
    renderCompanyHeader(); renderBanner(); renderHistorySection();
    empty.classList.add('hidden');
  } else {
    hideAll(hdr, banner, tabbar, tv); show(empty);
    et.textContent = 'Search for a company to begin';
    if (es) es.textContent = 'Type a ticker, EDINET code, or company name above.';
  }
}

function hideAll(...els) { els.forEach(e => { if (e) { e.classList.remove('is-visible'); e.classList.remove('is-active'); } }); }
function show(el) { if (el) el.classList.remove('hidden'); }

// ---------------------------------------------------------------------------
// Toolbar / Search
// ---------------------------------------------------------------------------

function renderToolbar() {
  const inp = H.$('sa-search'), status = H.$('sa-status');
  inp.disabled = !state.initDone;
  if (state.loading) { status.textContent = 'Loading…'; status.className = 'sa-status sa-status-loading'; }
  else if (state.company) { status.textContent = state.company.company?.company_name || ''; status.className = 'sa-status'; }
  else if (state.error) { status.textContent = 'Error'; status.className = 'sa-status sa-status-error'; }
  else if (state.initDone) { status.textContent = 'Ready'; status.className = 'sa-status sa-status-ok'; }
  else { status.textContent = 'Initializing…'; status.className = 'sa-status sa-status-loading'; }

  if (state.eventBound) return;
  state.eventBound = true;
  inp.addEventListener('input', onSearchInput);
  inp.addEventListener('keydown', onSearchKeydown);
  inp.addEventListener('focus', onSearchFocus);
  document.addEventListener('click', onClickOutside);
}

function onClickOutside(e) {
  const w = document.querySelector('.sa-search-wrap');
  if (w && !w.contains(e.target)) closeDropdown();
}

function onSearchInput() {
  const q = H.$('sa-search').value.trim();
  clearTimeout(state.searchTimer);
  if (!q) { closeDropdown(); return; }
  state.searchTimer = setTimeout(() => doSearch(q), 300);
}

async function doSearch(q) {
  try {
    const d = await fetchJson(`/api/security/search?q=${encodeURIComponent(q)}&limit=25`);
    state.searchResults = d.results || []; state.searchIdx = d.results.length > 0 ? 0 : -1;
    renderDropdown();
    if (state.searchResults.length) H.$('sa-search-dropdown').classList.add('is-open');
  } catch (e) { log('error', `Search: ${e.message}`); }
}

function onSearchKeydown(e) {
  const dd = H.$('sa-search-dropdown'), open = dd.classList.contains('is-open');
  if (!open && e.key === 'Enter') {
    e.preventDefault(); const q = H.$('sa-search').value.trim(); if (!q) return;
    clearTimeout(state.searchTimer);
    doSearch(q).then(() => { if (state.searchResults.length) { closeDropdown(); selectCompany(state.searchResults[0].company_code); } });
    return;
  }
  if (!open) return;
  if (e.key === 'ArrowDown') { e.preventDefault(); state.searchIdx = Math.min(state.searchIdx + 1, state.searchResults.length - 1); renderDropdown(); }
  else if (e.key === 'ArrowUp') { e.preventDefault(); state.searchIdx = Math.max(state.searchIdx - 1, 0); renderDropdown(); }
  else if (e.key === 'Enter') { e.preventDefault(); if (state.searchIdx >= 0) { const r = state.searchResults[state.searchIdx]; closeDropdown(); selectCompany(r.company_code); } }
  else if (e.key === 'Escape') { closeDropdown(); }
}

function onSearchFocus() { if (state.searchResults.length) H.$('sa-search-dropdown').classList.add('is-open'); }

function renderDropdown() {
  const dd = H.$('sa-search-dropdown'); dd.innerHTML = '';
  if (!state.searchResults.length) { dd.appendChild(H.el('div', { class: 'sa-search-empty', text: 'No results' })); return; }
  state.searchResults.forEach((r, i) => {
    dd.appendChild(H.el('div', {
      class: `sa-search-item${i === state.searchIdx ? ' is-active' : ''}`,
      onmousedown(e) { e.preventDefault(); closeDropdown(); selectCompany(r.company_code); },
    }, H.el('span', { class: 'sa-search-item-code', text: r.ticker || '-' }),
       H.el('span', { class: 'sa-search-item-name', text: r.company_name || r.company_code }),
       r.latest_price != null ? H.el('span', { class: 'sa-search-item-price', text: `¥${Number(r.latest_price).toLocaleString()}` }) : null));
  });
}

function closeDropdown() { H.$('sa-search-dropdown').classList.remove('is-open'); state.searchResults = []; state.searchIdx = -1; }

// ---------------------------------------------------------------------------
// Company selection
// ---------------------------------------------------------------------------

export async function selectCompany(code) {
  code = String(code).trim(); if (!code) return;
  state.loading = true; state.error = null; state.company = null; render();
  try {
    const [overview, formulas, history] = await Promise.all([
      fetchJson(`/api/security/overview?company_code=${encodeURIComponent(code)}`),
      fetchJson('/api/security/formulas'),
      fetchJson(`/api/security/history?company_code=${encodeURIComponent(code)}&periods=20`),
    ]);
    state.company = overview; state.formulas = formulas.formulas || []; state.history = history;
    sessionStorage.setItem('sa.lastCompanyCode', code);
    if (!state.activeTable || !history.tables?.[state.activeTable])
      state.activeTable = Object.keys(history.tables || {})[0] || null;
    state.loading = false; persist(); render();
    log('info', `Loaded: ${overview.company?.company_name || code}`);
  } catch (e) {
    state.loading = false; state.company = null; state.error = `Failed: ${e.message}`;
    log('error', state.error); render();
  }
}

// ---------------------------------------------------------------------------
// Company header
// ---------------------------------------------------------------------------

function renderCompanyHeader() {
  const hdr = H.$('sa-header'); hdr.classList.add('is-visible'); hdr.innerHTML = '';
  const c = state.company; if (!c?.company) return;
  const co = c.company, mkt = c.market || {}, metrics = c.metrics || {};

  // Identity
  const id = H.el('div', { class: 'sa-company-identity' },
    H.el('div', { class: 'sa-company-name', text: co.company_name || co.company_code }),
    H.el('div', { class: 'sa-company-meta' },
      ...[co.ticker, co.company_code, co.industry, co.market].filter(Boolean).map(v => H.el('span', { text: v }))));
  hdr.appendChild(id);

  // Metric tiles — price tile first, then formula-driven
  const tiles = H.el('div', { class: 'sa-metrics-grid' });
  const price = mkt.latest_price, chg = mkt.change_pct_1d, pd = mkt.latest_price_date;
  const pt = buildTile('Latest Price',
    price != null ? `¥${Number(price).toLocaleString()}` : '—',
    chg != null ? (chg >= 0 ? 'up' : 'down') : '',
    chg != null ? ` ${chg >= 0 ? '▲' : '▼'}${(Math.abs(chg) * 100).toFixed(1)}%` : '',
    pd || '—');
  if (co.ticker) {
    pt.appendChild(H.el('button', { class: 'sa-update-price-btn', text: 'Update Price',
      onclick() { const b = this; b.disabled = true; b.textContent = 'Updating…';
        fetchJson('/api/security/update-price', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker: co.ticker }) })
          .then(r => { log('info', r.message || 'Updated'); selectCompany(co.company_code); })
          .catch(e => { log('error', e.message); b.disabled = false; b.textContent = 'Update Price'; }); }}));
  }
  tiles.appendChild(pt);

  for (const f of (state.formulas || [])) {
    if (f.id === 'LatestPrice') continue;
    const val = metrics[f.id];
    let disp = '—';
    if (val != null) {
      const n = Number(val);
      if (f.format === 'percent') disp = `${(n * 100).toFixed(1)}%`;
      else if (f.format === 'currency') disp = fmtCur(n);
      else disp = n.toFixed(2);
    }
    tiles.appendChild(buildTile(f.name, disp));
  }
  hdr.appendChild(tiles);

  // 52-week range
  if (mkt.range_52w_low != null && mkt.range_52w_high != null && price != null) {
    const pct = (price - mkt.range_52w_low) / (mkt.range_52w_high - mkt.range_52w_low);
    hdr.appendChild(H.el('div', { class: 'sa-range-bar-wrap' },
      H.el('span', { text: `¥${Number(mkt.range_52w_low).toLocaleString()}` }),
      H.el('div', { class: 'sa-range-bar' }, H.el('div', { class: 'sa-range-dot', style: { left: `${Math.min(100, Math.max(0, pct * 100))}%` } })),
      H.el('span', { text: `¥${Number(mkt.range_52w_high).toLocaleString()}` })));
  }

  // Description
  const desc = co.description_summary || co.description || '';
  if (desc.length > 10) {
    const dd = H.el('div', { class: 'sa-description' });
    dd.textContent = desc.length > 300 ? desc.slice(0, 300) : desc;
    if (desc.length > 300) {
      dd.classList.remove('expanded');
      const tog = H.el('button', { class: 'sa-description-toggle', text: ' …more', onclick() { const x = dd.classList.toggle('expanded'); tog.textContent = x ? ' …less' : ' …more'; dd.textContent = x ? desc : desc.slice(0, 300); } });
      dd.appendChild(tog);
    }
    hdr.appendChild(dd);
  }

  const md = c.metadata || {};
  if (md.last_financial_period_end || md.last_price_date) {
    hdr.appendChild(H.el('div', { class: 'sa-company-meta', style: { marginTop: '6px' } },
      ...[md.last_financial_period_end && H.el('span', { text: `Financials: ${md.last_financial_period_end}` }),
         md.last_price_date && H.el('span', { text: `Price: ${md.last_price_date}` })].filter(Boolean)));
  }
}

function buildTile(label, value, extraClass, extraText, sub) {
  const t = H.el('div', { class: `sa-metric-tile${label === 'Latest Price' ? ' price-tile' : ''}` },
    H.el('div', { class: 'sa-metric-label', text: label }));
  const v = H.el('div', { class: `sa-metric-value${extraClass ? ' ' + extraClass : ''}` });
  v.textContent = value; if (extraText) v.textContent += extraText;
  t.appendChild(v);
  if (sub) t.appendChild(H.el('div', { class: 'sa-metric-sub', text: sub }));
  return t;
}

function fmtCur(v) {
  const n = Number(v);
  if (Math.abs(n) >= 1e12) return `¥${(n / 1e12).toFixed(1)}T`;
  if (Math.abs(n) >= 1e9) return `¥${(n / 1e9).toFixed(1)}B`;
  if (Math.abs(n) >= 1e6) return `¥${(n / 1e6).toFixed(1)}M`;
  return `¥${n.toLocaleString()}`;
}

function renderBanner() {
  const b = H.$('sa-banner');
  const flags = state.company?.metadata?.data_quality_flags || [];
  if (!flags.length) { b.style.display = 'none'; return; }
  b.style.display = 'flex';
  b.textContent = '⚠ ' + flags.map(f => f === 'missing_latest_price' ? 'No price data.' : 'No financials.').join(' ');
}

// ---------------------------------------------------------------------------
// History section
// ---------------------------------------------------------------------------

function renderHistorySection() {
  const tabbar = H.$('sa-tabbar'); tabbar.classList.add('is-visible');
  const tv = H.$('sa-table-view'); tv.classList.add('is-active');
  const tables = state.history?.tables || {}, periods = state.history?.periods || [];
  const keys = Object.keys(tables);
  if (!keys.length) { tv.innerHTML = '<div class="sa-empty"><div class="sa-empty-title">No historical data</div></div>'; return; }

  if (!state.activeTable || !tables[state.activeTable]) state.activeTable = keys[0];

  tabbar.innerHTML = '';
  for (const key of keys) {
    const t = tables[key];
    tabbar.appendChild(H.el('button', { class: `tab sa-tab${key === state.activeTable ? ' is-active' : ''}`, text: t.display_name,
      onclick() { state.activeTable = key; persist(); renderHistorySection(); } }));
  }

  tv.innerHTML = '';
  const ctrl = H.el('div', { class: 'sa-history-controls' },
    H.el('button', { class: `scr-btn-soft${state.viewMode === 'table' ? ' scr-btn-active' : ''}`, text: 'Table',
      onclick() { state.viewMode = 'table'; persist(); renderHistoryBody(); } }),
    H.el('button', { class: `scr-btn-soft${state.viewMode === 'chart' ? ' scr-btn-active' : ''}`, text: 'Chart',
      onclick() { state.viewMode = 'chart'; persist(); renderHistoryBody(); } }),
    H.el('button', { class: 'scr-btn-soft', text: 'Hide All',
      onclick() { const h = new Set(); (tables[state.activeTable]?.metrics || []).forEach(m => h.add(m.field)); state.hiddenMetrics[state.activeTable] = h; persist(); renderHistoryBody(); } }),
    H.el('button', { class: 'scr-btn-soft', text: 'Show All',
      onclick() { state.hiddenMetrics[state.activeTable] = new Set(); persist(); renderHistoryBody(); } }),
    H.el('button', { class: 'scr-btn-soft', text: 'Hide Empty',
      onclick() { const h = state.hiddenMetrics[state.activeTable] || new Set(); (tables[state.activeTable]?.metrics || []).forEach(m => { if (!(m.values || []).some(v => v != null && !isNaN(v))) h.add(m.field); }); state.hiddenMetrics[state.activeTable] = h; persist(); renderHistoryBody(); } }),
    H.el('input', { class: 'sa-col-search', placeholder: 'Filter metrics…', value: state.searchFilter,
      oninput() { state.searchFilter = this.value; renderHistoryBody(); } }),
    H.el('label', { class: 'scr-toggle', style: { marginLeft: 'auto' } },
      H.el('input', { type: 'checkbox', checked: state.millions, onchange() { state.millions = this.checked; persist(); renderHistoryBody(); } }), 'Millions'));
  tv.appendChild(ctrl);
  tv.appendChild(H.el('div', { id: 'sa-history-body' }));
  renderHistoryBody();
}

function renderHistoryBody() {
  // Update toggle button states
  const tb = document.querySelector('#sa-table-view button:nth-child(1)');
  const cb = document.querySelector('#sa-table-view button:nth-child(2)');
  if (tb) tb.classList.toggle('scr-btn-active', state.viewMode === 'table');
  if (cb) cb.classList.toggle('scr-btn-active', state.viewMode === 'chart');

  const body = H.$('sa-history-body'); body.innerHTML = '';
  const tables = state.history?.tables || {}, periods = state.history?.periods || [];
  const table = tables[state.activeTable];
  if (!table?.metrics?.length) { body.textContent = 'No data.'; return; }

  const hidden = state.hiddenMetrics[state.activeTable] || new Set();
  let metrics = table.metrics || [];
  if (state.searchFilter) { const q = state.searchFilter.toLowerCase(); metrics = metrics.filter(m => (m.display_name || m.field || '').toLowerCase().includes(q)); }

  if (state.viewMode === 'chart') {
    const visible = metrics.filter(m => !hidden.has(m.field));
    if (!visible.length) { body.textContent = 'All hidden.'; return; }
    destroyCharts();
    const wrap = H.el('div', { class: 'sa-chart-canvas-wrap', style: { height: '420px' } });
    const canvas = H.el('canvas'); wrap.appendChild(canvas); body.appendChild(wrap);
    if (typeof Chart === 'undefined') { body.textContent = 'Chart.js N/A.'; return; }
    const labels = periods.map(p => String(p).slice(0, 7));
    const colors = ['#58a6ff','#44d17b','#e0af4f','#ff6b6b','#b794f4','#56d4dd','#f48fb1','#a5d6a7','#90caf9','#aed581','#ffcc80','#ef9a9a'];
    const ds = visible.map((m, i) => ({ label: m.display_name, data: m.values || [], borderColor: colors[i % colors.length], backgroundColor: colors[i % colors.length] + '40', borderWidth: 2, tension: 0.1, pointRadius: metrics.length > 8 ? 0 : 3, spanGaps: true }));
    state.chartInstances.push(new Chart(canvas.getContext('2d'), {
      type: 'line', data: { labels, datasets: ds },
      options: { responsive: true, maintainAspectRatio: false, interaction: { intersect: false, mode: 'index' },
        plugins: { title: { display: true, text: table.display_name, color: '#8ea0b8', font: { size: 12 } }, legend: { position: 'top', labels: { color: '#8ea0b8', font: { size: 10 }, boxWidth: 10, padding: 8 } }, tooltip: { backgroundColor: '#111c2a', borderColor: '#243244', borderWidth: 1 } },
        scales: { x: { ticks: { color: '#8ea0b8', font: { size: 10 }, maxTicksLimit: 12 }, grid: { color: 'rgba(48,63,82,0.4)' } }, y: { ticks: { color: '#8ea0b8', font: { size: 10 }, callback: v => typeof v === 'number' ? fmtY(v) : v }, grid: { color: 'rgba(48,63,82,0.3)' } } } } }));
    return;
  }

  // Table view
  const wrap = H.el('div', { class: 'sa-table-wrap' });
  const tbl = H.el('table', { class: 'sa-table sa-history-table' });
  const thead = H.el('thead', {}, H.el('tr', {}, H.el('th', { class: 'sa-metric-col', text: 'Metric' }), ...periods.map(p => H.el('th', { text: p.slice(0, 7) })), H.el('th', { class: 'sa-act-col', text: '' })));
  tbl.appendChild(thead);

  const tbody = H.el('tbody');
  for (const m of metrics) {
    const isHidden = hidden.has(m.field);
    if (isHidden) continue;
    const tr = H.el('tr');
    tr.appendChild(H.el('td', { class: 'sa-metric-col' },
      H.el('input', { type: 'checkbox', checked: true, onchange() { const h = state.hiddenMetrics[state.activeTable] || new Set(); this.checked ? h.delete(m.field) : h.add(m.field); state.hiddenMetrics[state.activeTable] = h; persist(); renderHistoryBody(); } }),
      H.el('span', { text: m.display_name, style: { marginLeft: '6px' } })));
    for (const v of (m.values || [])) tr.appendChild(H.el('td', { text: fmtVal(v, state.millions) }));
    tr.appendChild(H.el('td', { class: 'sa-act-col' }, H.el('button', { class: 'sa-row-rm', text: '×', title: 'Hide',
      onclick() { const h = state.hiddenMetrics[state.activeTable] || new Set(); h.add(m.field); state.hiddenMetrics[state.activeTable] = h; persist(); renderHistoryBody(); } })));
    tbody.appendChild(tr);
  }
  tbl.appendChild(tbody);

  // Hidden section
  const hiddenMetrics = metrics.filter(m => hidden.has(m.field));
  if (hiddenMetrics.length) {
    const ht = H.el('tbody');
    ht.appendChild(H.el('tr', {}, H.el('td', { colSpan: String(periods.length + 2), class: 'sa-hidden-header', style: { padding: '8px 10px' } }, H.el('span', { class: 'sa-hidden-title', text: `Hidden (${hiddenMetrics.length})` }))));
    for (const m of hiddenMetrics) {
      ht.appendChild(H.el('tr', { class: 'sa-row-hidden' },
        H.el('td', { class: 'sa-metric-col', colSpan: String(periods.length + 2) },
          H.el('button', { class: 'sa-restore-btn', text: '↩ ' + m.display_name,
            onclick() { const h = state.hiddenMetrics[state.activeTable] || new Set(); h.delete(m.field); state.hiddenMetrics[state.activeTable] = h; persist(); renderHistoryBody(); } }))));
    }
    tbl.appendChild(ht);
  }
  wrap.appendChild(tbl); body.appendChild(wrap);
}

function fmtVal(v, millions) {
  if (v == null || (typeof v === 'number' && isNaN(v))) return '—';
  if (typeof v === 'number') {
    if (millions) return `${(v / 1e6).toFixed(1)}M`;
    if (Math.abs(v) >= 1e12) return `${(v / 1e12).toFixed(2)}T`;
    if (Math.abs(v) >= 1e9) return `${(v / 1e9).toFixed(2)}B`;
    if (Math.abs(v) >= 1e6) return `${(v / 1e6).toFixed(2)}M`;
    return v.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
  return String(v);
}

function fmtY(v) {
  if (Math.abs(v) >= 1e12) return (v / 1e12).toFixed(1) + 'T';
  if (Math.abs(v) >= 1e9) return (v / 1e9).toFixed(1) + 'B';
  if (Math.abs(v) >= 1e6) return (v / 1e6).toFixed(1) + 'M';
  if (Math.abs(v) >= 1e3) return (v / 1e3).toFixed(1) + 'K';
  return Number(v).toFixed(1);
}

function destroyCharts() { state.chartInstances.forEach(c => { try { c.destroy(); } catch (e) {} }); state.chartInstances = []; }

// ---------------------------------------------------------------------------
// DOM helpers
// ---------------------------------------------------------------------------

const H = {
  $(id) { return document.getElementById(id); },
  el(tag, attrs = {}, ...children) {
    const el = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (k === 'class' || k === 'className') el.className = v;
      else if (k === 'text') el.textContent = v;
      else if (k === 'html') el.innerHTML = v;
      else if (k === 'style') { if (v && typeof v === 'object' && !Array.isArray(v)) Object.assign(el.style, v); else if (v) el.style.cssText = String(v); }
      else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2), v);
      else if (v !== undefined && v !== null && k !== 'selected') el.setAttribute(k, v);
      if (k === 'selected' && v) el.setAttribute(k, '');
    }
    for (const c of children.flat()) { if (c != null && c !== false) el.append(c.nodeType ? c : document.createTextNode(String(c))); }
    return el;
  }
};
