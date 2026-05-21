/**
 * Portfolio Management — four-tab interface.
 *
 * Features:
 *   - Multi-file upload with progress queue
 *   - Summary stats bar (always visible)
 *   - Holdings with value chart + allocation pie
 *   - Transactions with filters + scrollable table
 *   - Performance auto-compute on tab switch
 *   - Scrolling enabled on all tabs
 */

import { $, el, fetchJson } from '../common/utils.js';

// ---------------------------------------------------------------------------
// Ticker mapping: IBKR format (5984.T) → db2 format (59840)
// ---------------------------------------------------------------------------
function normalizeTicker(symbol) {
  // Strip CASH prefix
  if (symbol.startsWith('CASH:')) return symbol;
  // Options — pass through unchanged
  if (symbol.match(/\s\d{6}[CP]\d{8}$/)) return symbol;
  // Japanese tickers: 5984.T → 59840, 4-digit.T → 5-digit0
  const m = symbol.match(/^(\d{4})\.T$/);
  if (m) return m[1] + '0';
  return symbol;
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
const state = {
  holdings: [],
  transactions: [],
  activitySummary: {},
  performance: null,
  charts: {},
  uploadedFiles: [],
  sortColumn: null,      // current sort column key
  sortAsc: true,          // sort direction
};

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
async function init() {
  wireTabs();
  wireUpload();
  await refreshSummary();
  await refreshSymbols();
  await loadHoldings();
  await loadTransactions();
  wireTransactionsFilters();
  wirePerformance();
  wireRebuild();
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function $$(sel, root) { return Array.from((root || document).querySelectorAll(sel)); }

function formatMoney(v) {
  if (v == null || isNaN(v)) return '—';
  if (Math.abs(v) >= 1e6) return (v / 1e6).toFixed(2) + 'M';
  if (Math.abs(v) >= 1e3) return (v / 1e3).toFixed(1) + 'K';
  return v.toFixed(2);
}

function formatPct(v) { return v != null ? (v * 100).toFixed(2) + '%' : '—'; }

function formatNum(v, d) { return v != null ? v.toFixed(d || 4) : '—'; }

function badgeClass(type) {
  const map = {
    TRADE: 'bg-accent', DIVIDEND: 'bg-success', WITHHOLDING_TAX: 'bg-danger',
    DEPOSIT_WITHDRAWAL: 'bg-muted', BROKER_INTEREST: 'bg-muted',
    SPINOFF: 'bg-warning', PIL_DIVIDEND: 'bg-success',
    OTHER_FEE: 'bg-muted', COMMISSION_ADJ: 'bg-muted',
  };
  return map[type] || '';
}

// ---------------------------------------------------------------------------
// Summary bar
// ---------------------------------------------------------------------------
async function refreshSummary() {
  try {
    const [counts, dateRange, daily] = await Promise.all([
      fetchJson('/api/portfolio/activity-summary'),
      fetchJson('/api/portfolio/date-range'),
      fetchJson('/api/portfolio/holdings/history?end_date=' + new Date().toISOString().slice(0, 10)),
    ]);
    state.activitySummary = counts.by_activity || {};

    const totalTxns = Object.values(state.activitySummary).reduce((a, b) => a + b, 0);
    $('#pf-stat-txn').textContent = totalTxns.toLocaleString();
    $('#pf-stat-dates').textContent =
      dateRange.min_date ? `${dateRange.min_date} → ${dateRange.max_date}` : '—';

    if (daily.length > 0) {
      const last = daily[daily.length - 1];
      $('#pf-stat-value').textContent = formatMoney(last.total_value) + ' EUR';
    }

    // Holdings count
    try {
      const h = await fetchJson('/api/portfolio/holdings');
      $('#pf-stat-holdings').textContent = h.length;
    } catch (_) { /* empty */ }

    renderActivityBreakdown();
  } catch (_) { /* server may not have data yet */ }
}

function renderActivityBreakdown() {
  const div = $('#pf-activity-breakdown');
  const sa = state.activitySummary;
  const entries = Object.entries(sa);
  if (!entries.length) {
    div.innerHTML = '<span class="muted">No data yet. Upload transactions above.</span>';
    return;
  }
  div.innerHTML = entries.map(([type, count]) =>
    `<div class="metric-tile" style="min-width:120px;">
      <div class="metric-label">${type.replace('_', ' ')}</div>
      <div class="metric-value" style="font-size:1.1rem;">${count}</div>
    </div>`
  ).join('');
}

// ---------------------------------------------------------------------------
// Tabs — with auto-render on switch
// ---------------------------------------------------------------------------
function wireTabs() {
  $('#pf-tabs').addEventListener('click', (e) => {
    const btn = e.target.closest('.tab-btn');
    if (!btn) return;
    $$('.tab-btn').forEach(b => b.classList.remove('is-active'));
    btn.classList.add('is-active');
    const tab = btn.dataset.tab;
    $$('.tab-panel').forEach(p => p.classList.remove('is-active'));
    $(`[data-panel="${tab}"]`).classList.add('is-active');
    // Auto-render on switch
    if (tab === 'holdings') renderHoldingsTab();
    if (tab === 'transactions') loadTransactions();
    if (tab === 'performance') renderPerformanceTab();
  });
}

// ================================================================
// UPLOAD — multi-file with queue
// ================================================================
function wireUpload() {
  const dropZone = $('#pf-drop-zone');
  const fileInput = $('#pf-file-input');
  const queue = $('#pf-upload-queue');
  const results = $('#pf-upload-results');

  dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('drag-over'); });
  dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag-over'));

  dropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    dropZone.classList.remove('drag-over');
    if (e.dataTransfer.files.length) {
      uploadFiles(Array.from(e.dataTransfer.files));
    }
  });

  fileInput.addEventListener('change', () => {
    if (fileInput.files.length) {
      uploadFiles(Array.from(fileInput.files));
      fileInput.value = ''; // allow re-upload of same files
    }
  });

  dropZone.addEventListener('click', (e) => {
    if (e.target !== fileInput) fileInput.click();
  });

  async function uploadFiles(files) {
    state.uploadedFiles = state.uploadedFiles || [];

    // Filter non-xml
    const xmlFiles = files.filter(f => f.name.toLowerCase().endsWith('.xml'));
    if (!xmlFiles.length) {
      queue.style.display = 'block';
      queue.innerHTML = '<div class="status-text error">No .xml files selected.</div>';
      return;
    }

    // Show queue
    queue.style.display = 'block';
    queue.innerHTML = xmlFiles.map((f, i) =>
      `<div class="status-text info" data-queue="${i}">⏳ Queued: ${f.name}</div>`
    ).join('');

    const allResults = [];
    for (let i = 0; i < xmlFiles.length; i++) {
      const file = xmlFiles[i];
      const status = $(`[data-queue="${i}"]`);
      if (status) status.textContent = `⏳ Uploading: ${file.name}…`;

      try {
        const form = new FormData();
        form.append('file', file);
        const resp = await fetch('/api/portfolio/upload', { method: 'POST', body: form });
        if (!resp.ok) {
          const err = await resp.json();
          throw new Error(err.detail || `Upload failed (${resp.status})`);
        }
        const data = await resp.json();
        if (status) {
          status.textContent = `✓ ${file.name}: ${data.inserted} new, ${data.skipped} skipped`;
          status.className = 'status-text success';
        }
        allResults.push(data);
        state.uploadedFiles.push(file.name);
      } catch (err) {
        if (status) {
          status.textContent = `✗ ${file.name}: ${err.message}`;
          status.className = 'status-text error';
        }
      }
    }

    // Show aggregate results
    const totalInserted = allResults.reduce((s, r) => s + (r.inserted || 0), 0);
    const totalSkipped = allResults.reduce((s, r) => s + (r.skipped || 0), 0);
    results.style.display = 'block';
    results.innerHTML = `
      <div class="status-text success">
        ✓ Upload complete: ${totalInserted} inserted, ${totalSkipped} skipped across ${xmlFiles.length} file(s)
      </div>`;

    // Refresh everything
    await refreshSummary();
    await refreshSymbols();
    await loadTransactions();
    await loadHoldings();
  }
}

// ================================================================
// HOLDINGS
// ================================================================
async function loadHoldings() {
  try {
    // Fetch enhanced holdings with performance data
    const data = await fetchJson('/api/portfolio/holdings/performance');
    state.holdings = data;
    $('#pf-stat-holdings').textContent = data.filter(h => h.asset_category !== 'CASH' && !h.symbol.startsWith('CASH')).length;
    renderHoldingsTab();
  } catch (e) {
    // Fallback to simple holdings
    try {
      state.holdings = await fetchJson('/api/portfolio/holdings');
      $('#pf-stat-holdings').textContent = state.holdings.length;
      renderHoldingsTab();
    } catch (e2) {
      $('#pf-holdings-table').innerHTML = `<span class="status-text error">${e2.message}</span>`;
    }
  }
}

function renderHoldingsTab() {
  renderHoldingsTable();
  renderValueChart();
  renderAllocationChart();
}

function renderHoldingsTable() {
  const div = $('#pf-holdings-table');
  const hh = state.holdings;
  if (!hh.length) {
    div.innerHTML = '<span class="muted">No holdings. Upload transactions and rebuild.</span>';
    return;
  }
  const stocks = hh.filter(h => h.asset_category !== 'CASH' && !h.symbol.startsWith('CASH'));
  $('#pf-holdings-count').textContent = `${stocks.length} positions`;

  let totalBase = 0;
  for (const h of hh) totalBase += h.market_value || 0;

  // ── Column definitions ──
  const cols = [
    { key: 'symbol',     label: 'Symbol',       get: h => h.symbol },
    { key: 'quantity',   label: 'Qty',          get: h => h.quantity, num: true },
    { key: 'avg_cost',   label: 'Avg Cost',     get: h => h.avg_cost, num: true },
    { key: 'price',      label: 'Price',        get: h => h.market_price, num: true },
    { key: 'val_native', label: 'Value (Nat.)', get: h => h.market_value_native, num: true },
    { key: 'val_eur',    label: 'Value (EUR)',  get: h => h.market_value, num: true },
    { key: 'pnl',        label: 'P&L',          get: h => h.performance?.unrealized_pnl, num: true },
    { key: 'dividends',  label: 'Div',          get: h => h.performance?.dividend_income, num: true },
    { key: 'pct_ret',    label: '% Return',     get: h => h.performance?.total_return, num: true },
    { key: 'ann_ret',    label: 'Ann. Ret',     get: h => h.performance?.annualized_return, num: true },
    { key: 'weight',     label: 'Wt%',          get: h => totalBase ? (Math.abs(h.market_value || 0) / Math.abs(totalBase)) * 100 : 0, num: true },
  ];

  // ── Sort ──
  let sorted = [...hh];
  if (state.sortColumn) {
    const colDef = cols.find(c => c.key === state.sortColumn);
    if (colDef) {
      sorted.sort((a, b) => {
        const va = colDef.get(a) ?? (colDef.num ? -Infinity : '');
        const vb = colDef.get(b) ?? (colDef.num ? -Infinity : '');
        if (colDef.num) {
          return state.sortAsc ? va - vb : vb - va;
        }
        const sa = String(va).toLowerCase();
        const sb = String(vb).toLowerCase();
        return state.sortAsc ? sa.localeCompare(sb) : sb.localeCompare(sa);
      });
    }
  }

  // ── Render header ──
  let html = '<table class="data-table" id="pf-holdings-tbl"><thead><tr>';
  for (const col of cols) {
    const arrow = state.sortColumn === col.key ? (state.sortAsc ? ' ▲' : ' ▼') : '';
    html += `<th class="pf-sort-th" data-sort="${col.key}" style="cursor:pointer;user-select:none;white-space:nowrap;">${col.label}${arrow}</th>`;
  }
  html += '</tr></thead><tbody>';

  // ── Render rows ──
  for (const h of sorted) {
    const isCash = h.asset_category === 'CASH' || h.symbol.startsWith('CASH');
    const wt = totalBase ? ((Math.abs(h.market_value || 0) / Math.abs(totalBase)) * 100).toFixed(1) : '—';
    const mvNative = h.market_value_native != null ? formatMoney(h.market_value_native) : '—';
    const mvBase = h.market_value != null ? formatMoney(h.market_value) : '—';
    const cashStyle = isCash ? (h.market_value < 0 ? 'color:var(--danger);' : 'color:var(--success);') : '';
    const rowClick = isCash ? '' : `data-holding="${encodeURIComponent(h.symbol)}"`;

    let capGain = '—', dividends = '—', pctRet = '—', annRet = '—';
    let capColor = '', retColor = '';
    if (!isCash && h.performance) {
      const p = h.performance;
      capGain = formatMoney(p.unrealized_pnl || 0);
      capColor = (p.unrealized_pnl || 0) >= 0 ? 'color:var(--success);' : 'color:var(--danger);';
      dividends = formatMoney(p.dividend_income || 0);
      pctRet = p.total_return != null ? (p.total_return * 100).toFixed(1) + '%' : '—';
      retColor = (p.total_return || 0) >= 0 ? 'color:var(--success);' : 'color:var(--danger);';
      annRet = p.annualized_return != null ? (p.annualized_return * 100).toFixed(1) + '%' : '—';
    }

    html += `<tr class="pf-holding-row" ${rowClick} data-symbol="${encodeURIComponent(h.symbol)}" style="cursor:${isCash ? 'default' : 'pointer'};">
      <td><a class="pf-sym-link" href="/security?symbol=${encodeURIComponent(normalizeTicker(h.symbol))}" onclick="event.stopPropagation();"><strong style="${cashStyle}">${h.symbol}</strong></a></td>
      <td>${isCash ? '' : h.quantity}</td>
      <td>${h.avg_cost != null ? formatMoney(h.avg_cost) : '—'}</td>
      <td>${h.market_price != null ? formatMoney(h.market_price) : '—'}</td>
      <td>${mvNative} ${isCash ? '' : h.currency}</td>
      <td style="${cashStyle}">${mvBase}</td>
      <td style="${capColor}">${capGain}</td>
      <td style="${dividends !== '—' ? 'color:var(--success);' : ''}">${dividends}</td>
      <td style="${retColor}">${pctRet}</td>
      <td style="${retColor}">${annRet}</td>
      <td>${wt}%</td>
    </tr>`;
  }
  html += '</tbody></table>';
  div.innerHTML = html;

  // Wire sort clicks
  $$('.pf-sort-th').forEach(th => {
    th.addEventListener('click', () => {
      const key = th.dataset.sort;
      if (state.sortColumn === key) {
        state.sortAsc = !state.sortAsc;
      } else {
        state.sortColumn = key;
        state.sortAsc = true;
      }
      renderHoldingsTable();
      // Re-wire row clicks after re-render
      $$('.pf-holding-row[data-holding]').forEach(row => {
        row.addEventListener('click', () => toggleHoldingDetail(row));
      });
    });
  });

  // Wire row clicks for inline detail
  $$('.pf-holding-row[data-holding]').forEach(row => {
    row.addEventListener('click', () => toggleHoldingDetail(row));
  });
}

function toggleHoldingDetail(row) {
  const symbol = decodeURIComponent(row.dataset.symbol || row.dataset.holding);
  const existing = row.nextElementSibling;
  // If detail row already open, close it
  if (existing && existing.classList.contains('pf-holding-detail-row')) {
    existing.remove();
    return;
  }
  // Close any other open detail rows
  $$('.pf-holding-detail-row').forEach(r => r.remove());

  // Find performance data from state
  const h = state.holdings.find(x => x.symbol === symbol);
  if (!h || !h.performance) {
    // Fetch on demand
    fetchJson(`/api/portfolio/holdings/${encodeURIComponent(symbol)}/performance`)
      .then(p => insertDetailRow(row, symbol, p))
      .catch(() => {});
    return;
  }
  insertDetailRow(row, symbol, h.performance);
}

function insertDetailRow(row, symbol, p) {
  const cols = row.querySelectorAll('td').length;
  const pct = v => v != null ? (v * 100).toFixed(2) + '%' : '—';
  const pnlColor = (p.unrealized_pnl || 0) >= 0 ? 'var(--success)' : 'var(--danger)';
  const retColor = (p.total_return || 0) >= 0 ? 'var(--success)' : 'var(--danger)';
  const detailTr = document.createElement('tr');
  detailTr.className = 'pf-holding-detail-row';
  const chartId = 'pf-detail-chart-' + symbol.replace(/[^a-zA-Z0-9]/g, '_');
  detailTr.innerHTML = `<td colspan="${cols}" style="padding:12px 16px;background:rgba(88,166,255,0.04);border-left:3px solid var(--accent);">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
      <strong>${p.symbol} — Performance Details</strong>
      <button class="btn-ghost btn-sm" onclick="this.closest('tr').remove();">✕</button>
    </div>
    <div class="metric-grid" style="gap:8px;margin-bottom:12px;">
      <div class="metric-tile"><div class="metric-label">Total Return</div><div class="metric-value" style="color:${retColor};">${pct(p.total_return)}</div></div>
      <div class="metric-tile"><div class="metric-label">Ann. Return</div><div class="metric-value" style="color:${retColor};">${pct(p.annualized_return)}</div></div>
      <div class="metric-tile"><div class="metric-label">Volatility</div><div class="metric-value">${pct(p.volatility)}</div></div>
      <div class="metric-tile"><div class="metric-label">Unrealized P&L</div><div class="metric-value" style="color:${pnlColor};">${formatMoney(p.unrealized_pnl)}</div></div>
      <div class="metric-tile"><div class="metric-label">Dividend Income</div><div class="metric-value" style="color:var(--success);">${formatMoney(p.dividend_income)}</div></div>
      <div class="metric-tile"><div class="metric-label">Dividend Yield</div><div class="metric-value">${pct(p.dividend_yield)}</div></div>
      <div class="metric-tile"><div class="metric-label">Avg Cost</div><div class="metric-value">${formatMoney(p.avg_cost)}</div></div>
      <div class="metric-tile"><div class="metric-label">Current Price</div><div class="metric-value">${formatMoney(p.current_price)}</div></div>
      <div class="metric-tile"><div class="metric-label">First Purchase</div><div class="metric-value">${p.first_purchase || '—'}</div></div>
      <div class="metric-tile"><div class="metric-label">Last Purchase</div><div class="metric-value">${p.last_purchase || '—'}</div></div>
      <div class="metric-tile"><div class="metric-label"># Buys / # Sells</div><div class="metric-value">${p.num_buys} / ${p.num_sells}</div></div>
      <div class="metric-tile"><div class="metric-label">Current Value</div><div class="metric-value">${formatMoney(p.current_value)} (${formatMoney(p.current_value_native)} ${p.currency})</div></div>
      <div class="metric-tile"><div class="metric-label">Div Gross</div><div class="metric-value" style="color:var(--success);">${formatMoney(p.dividend_gross)}</div></div>
      <div class="metric-tile"><div class="metric-label">Div Tax</div><div class="metric-value" style="color:var(--danger);">${formatMoney(p.dividend_tax)}</div></div>
    </div>
    <div style="height:220px;margin-bottom:8px;">
      <canvas id="${chartId}"></canvas>
    </div>
    <div style="margin-top:4px;">
      <a href="/security?symbol=${encodeURIComponent(normalizeTicker(p.symbol))}" class="btn-ghost btn-sm">Open Security Analysis →</a>
    </div>
  </td>`;
  row.after(detailTr);

  // Fetch history and render chart
  destroyDetailChart();
  fetchJson(`/api/portfolio/holdings/${encodeURIComponent(symbol)}/history`)
    .then(history => {
      const ctx = document.getElementById(chartId);
      if (!ctx || !history || !history.length) return;
      const filtered = history.filter(h => (h.market_value || 0) > 0);
      if (!filtered.length) return;
      _detailChart = new Chart(ctx, {
        type: 'line',
        data: {
          labels: filtered.map(h => h.date),
          datasets: [{
            label: 'Value (EUR)',
            data: filtered.map(h => h.market_value),
            borderColor: '#58a6ff',
            backgroundColor: 'rgba(88,166,255,0.08)',
            fill: true, tension: 0.2, pointRadius: 0,
            yAxisID: 'y',
          }, {
            label: 'Price',
            data: filtered.map(h => h.market_price),
            borderColor: '#e0af4f',
            borderDash: [4, 2],
            tension: 0.2, pointRadius: 0,
            yAxisID: 'y1',
          }],
        },
        options: {
          responsive: true, maintainAspectRatio: false,
          interaction: { intersect: false, mode: 'index' },
          scales: {
            y: { position: 'left', ticks: { color: '#8ea0b8', callback: v => formatMoney(v) }, title: { display: true, text: 'Value', color: '#8ea0b8' } },
            y1: { position: 'right', ticks: { color: '#8ea0b8', callback: v => formatMoney(v) }, grid: { display: false }, title: { display: true, text: 'Price', color: '#8ea0b8' } },
            x: { ticks: { color: '#8ea0b8', maxTicksLimit: 8 } },
          },
          plugins: { legend: { labels: { color: '#d9e2f2', usePointStyle: true } } },
        },
      });
    })
    .catch(() => {});
}

function renderValueChart() {
  fetchJson('/api/portfolio/holdings/history').then(daily => {
    if (!daily.length) return;
    const ctx = $('#pf-value-chart');
    destroyChart('value');
    state.charts.value = new Chart(ctx, {
      type: 'line',
      data: {
        labels: daily.map(d => d.date),
        datasets: [{
          label: 'Total Value', data: daily.map(d => d.total_value),
          borderColor: '#58a6ff', backgroundColor: 'rgba(88,166,255,0.1)',
          fill: true, tension: 0.2, pointRadius: 0,
        }, {
          label: 'Cash', data: daily.map(d => d.cash_balance),
          borderColor: '#44d17b', borderDash: [5, 3],
          tension: 0.2, pointRadius: 0,
        }],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { intersect: false, mode: 'index' },
        scales: {
          y: { ticks: { color: '#8ea0b8', callback: v => formatMoney(v) } },
          x: { ticks: { color: '#8ea0b8', maxTicksLimit: 10 } },
        },
        plugins: { legend: { labels: { color: '#d9e2f2', usePointStyle: true } } },
      },
    });
  });
}

function renderAllocationChart() {
  const hh = state.holdings.filter(h => !h.is_option);
  if (!hh.length) return;
  const ctx = $('#pf-allocation-chart');
  destroyChart('allocation');
  const labels = hh.map(h => h.symbol);
  const data = hh.map(h => Math.abs(h.market_value || 0));
  state.charts.allocation = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels,
      datasets: [{
        data,
        backgroundColor: [
          '#58a6ff','#44d17b','#e0af4f','#ff6b6b','#8ea0b8',
          '#58a6ff88','#44d17b88','#e0af4f88','#ff6b6b88','#8ea0b888',
        ],
        borderColor: 'transparent',
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { position: 'right', labels: { color: '#d9e2f2', usePointStyle: true, padding: 12 } },
        tooltip: { callbacks: { label: ctx => `${ctx.label}: ${formatMoney(ctx.raw)}` } },
      },
    },
  });
}

// ================================================================
// TRANSACTIONS — scrollable, with filter summary
// ================================================================
async function loadTransactions(params = {}) {
  try {
    const qs = new URLSearchParams();
    if (params.symbol) qs.set('symbol', params.symbol);
    if (params.activity_type) qs.set('activity_type', params.activity_type);
    if (params.start_date) qs.set('start_date', params.start_date);
    if (params.end_date) qs.set('end_date', params.end_date);
    qs.set('limit', '2000');
    const data = await fetchJson('/api/portfolio/transactions?' + qs.toString());
    state.transactions = data;
    $('#pf-txn-count').textContent = `${data.length} transactions`;
    renderTransactionsTable();
  } catch (e) {
    $('#pf-transactions-table').innerHTML = `<span class="status-text error">${e.message}</span>`;
  }
}

function renderTransactionsTable() {
  const div = $('#pf-transactions-table');
  const txns = state.transactions;
  if (!txns.length) {
    div.innerHTML = '<span class="muted">No transactions match filters.</span>';
    return;
  }
  let html = '<table class="data-table"><thead><tr>' +
    '<th>Date</th><th>Type</th><th>Symbol</th><th>Qty</th>' +
    '<th>Price</th><th>Amount</th><th>Cur</th><th>B/S</th></tr></thead><tbody>';
  for (const t of txns) {
    const amt = t.activity_type === 'TRADE'
      ? (t.net_cash != null ? t.net_cash : (t.amount || 0))
      : (t.amount || 0);
    const isNeg = (typeof amt === 'number' && amt < 0);
    html += `<tr>
      <td style="white-space:nowrap;">${t.trade_date || '—'}</td>
      <td><span class="badge ${badgeClass(t.activity_type)}" style="font-size:10px;">${t.activity_type.replace('_',' ')}</span></td>
      <td><strong>${t.symbol || '—'}</strong></td>
      <td>${t.quantity != null ? t.quantity : '—'}</td>
      <td>${t.trade_price != null ? formatMoney(t.trade_price) : '—'}</td>
      <td style="color:${isNeg ? 'var(--danger)' : 'var(--success)'};">${formatMoney(amt)}</td>
      <td>${t.currency || ''}</td>
      <td>${t.buy_sell || ''}</td>
    </tr>`;
  }
  html += '</tbody></table>';
  div.innerHTML = html;
}

function wireTransactionsFilters() {
  const typeF = $('#pf-txn-type-filter');
  const symF = $('#pf-txn-symbol-filter');
  const startD = $('#pf-txn-start-date');
  const endD = $('#pf-txn-end-date');

  const apply = () => loadTransactions({
    activity_type: typeF.value,
    symbol: symF.value,
    start_date: startD.value,
    end_date: endD.value,
  });

  [typeF, symF, startD, endD].forEach(el => el.addEventListener('change', apply));
  $('#pf-txn-clear-filters').addEventListener('click', () => {
    typeF.value = symF.value = startD.value = endD.value = '';
    loadTransactions();
  });
}

async function refreshSymbols() {
  try {
    const data = await fetchJson('/api/portfolio/symbols');
    const sel = $('#pf-txn-symbol-filter');
    sel.innerHTML = '<option value="">All Symbols</option>';
    for (const s of data) {
      if (s.symbol) {
        sel.innerHTML += `<option value="${s.symbol}">${s.symbol}</option>`;
      }
    }
  } catch (_) { /* empty */ }
}

// ================================================================
// PERFORMANCE
// ================================================================
function wirePerformance() {
  $('#pf-perf-compute').addEventListener('click', computePerformance);
  $('#pf-perf-detect-rf').addEventListener('click', async () => {
    const curr = $('#pf-perf-currency').value;
    try {
      const d = await fetchJson(`/api/portfolio/risk-free-rate?base_currency=${curr}`);
      $('#pf-perf-rf').value = d.risk_free_rate.toFixed(4);
    } catch (_) { /* */ }
  });
  // Benchmark quick-select buttons
  $$('#pf-bench-shortcuts .btn-ghost').forEach(btn => {
    btn.addEventListener('click', () => {
      $('#pf-perf-benchmark').value = btn.dataset.bench;
      computePerformance();
    });
  });
}

async function computePerformance() {
  const perfBtn = $('#pf-perf-compute');
  perfBtn.disabled = true;
  perfBtn.textContent = 'Computing…';
  try {
    const qs = new URLSearchParams({ base_currency: $('#pf-perf-currency').value });
    const bench = $('#pf-perf-benchmark').value.trim();
    const rf = $('#pf-perf-rf').value;
    if (bench) qs.set('benchmark_ticker', bench);
    if (rf) qs.set('risk_free_rate', rf);
    state.performance = await fetchJson('/api/portfolio/performance?' + qs.toString());
  } catch (e) {
    $('#pf-metrics-grid').innerHTML = `<span class="status-text error">${e.message}</span>`;
  } finally {
    perfBtn.disabled = false;
    perfBtn.textContent = 'Compute';
  }
  renderPerformanceTab();
}

function renderPerformanceTab() {
  // Auto-compute if no cached data
  if (!state.performance || (!state.performance.sharpe_ratio && !state.performance.total_return)) {
    computePerformance().catch(() => {});
  } else {
    renderMetrics();
    renderEquityChart();
  }
}

function renderMetrics() {
  const div = $('#pf-metrics-grid');
  const p = state.performance;
  if (!p) return;

  div.innerHTML = `<div class="metric-grid">
    <div class="metric-tile"><div class="metric-label">Total Return</div><div class="metric-value">${formatPct(p.total_return)}</div></div>
    <div class="metric-tile"><div class="metric-label">Ann. Return</div><div class="metric-value">${formatPct(p.annualized_return)}</div></div>
    <div class="metric-tile"><div class="metric-label">Volatility</div><div class="metric-value">${formatPct(p.volatility)}</div></div>
    <div class="metric-tile"><div class="metric-label">Sharpe</div><div class="metric-value">${formatNum(p.sharpe_ratio)}</div></div>
    <div class="metric-tile"><div class="metric-label">Sortino</div><div class="metric-value">${formatNum(p.sortino_ratio)}</div></div>
    <div class="metric-tile"><div class="metric-label">Max DD</div><div class="metric-value" style="color:var(--danger);">${formatPct(p.max_drawdown)}</div></div>
    <div class="metric-tile"><div class="metric-label">Calmar</div><div class="metric-value">${formatNum(p.calmar_ratio)}</div></div>
    <div class="metric-tile"><div class="metric-label">Win Rate</div><div class="metric-value">${formatPct(p.win_rate)}</div></div>
    <div class="metric-tile"><div class="metric-label">Profit Factor</div><div class="metric-value">${p.profit_factor === 999 ? '∞' : formatNum(p.profit_factor)}</div></div>
    <div class="metric-tile"><div class="metric-label">VaR 95%</div><div class="metric-value" style="color:var(--danger);">${formatPct(p.var_95)}</div></div>
    <div class="metric-tile"><div class="metric-label">CVaR 95%</div><div class="metric-value" style="color:var(--danger);">${formatPct(p.cvar_95)}</div></div>
    <div class="metric-tile"><div class="metric-label">Dividends</div><div class="metric-value" style="color:var(--success);">€${formatMoney(p.total_dividend_income)}</div></div>
    <div class="metric-tile"><div class="metric-label">Risk-Free</div><div class="metric-value">${formatPct(p.risk_free_rate)}</div></div>
    ${p.dividend_breakdown ? `
    <div class="metric-tile"><div class="metric-label">Div Gross</div><div class="metric-value" style="color:var(--success);">€${formatMoney(p.dividend_breakdown.total_gross)}</div></div>
    <div class="metric-tile"><div class="metric-label">Div Tax</div><div class="metric-value" style="color:var(--danger);">€${formatMoney(p.dividend_breakdown.total_tax)}</div></div>
    ` : ''}
    ${p.benchmark && p.benchmark.ticker ? `
    <div class="metric-tile" style="grid-column:1 / -1;background:rgba(88,166,255,0.06);border-left:3px solid var(--accent);">
      <div class="metric-label">Benchmark Comparison vs ${p.benchmark.ticker}</div>
    </div>
    <div class="metric-tile"><div class="metric-label">${p.benchmark.ticker} Return</div><div class="metric-value">${formatPct(p.benchmark.total_return)}</div></div>
    <div class="metric-tile"><div class="metric-label">Excess Return</div><div class="metric-value" style="color:${(p.benchmark.excess_return || 0) >= 0 ? 'var(--success)' : 'var(--danger)'};">${formatPct(p.benchmark.excess_return)}</div></div>
    <div class="metric-tile"><div class="metric-label">Beta</div><div class="metric-value">${formatNum(p.benchmark.beta, 2)}</div></div>
    <div class="metric-tile"><div class="metric-label">Alpha</div><div class="metric-value" style="color:${(p.benchmark.alpha || 0) >= 0 ? 'var(--success)' : 'var(--danger)'};">${formatPct(p.benchmark.alpha)}</div></div>
    <div class="metric-tile"><div class="metric-label">Info Ratio</div><div class="metric-value">${formatNum(p.benchmark.information_ratio, 4)}</div></div>
    <div class="metric-tile"><div class="metric-label">Tracking Error</div><div class="metric-value">${formatPct(p.benchmark.tracking_error)}</div></div>
    ` : ''}
    ${p.return_distribution ? `
    <div class="metric-tile" style="grid-column:1 / -1;background:rgba(224,175,79,0.06);border-left:3px solid var(--warning);">
      <div class="metric-label">Daily Return Distribution</div>
    </div>
    <div class="metric-tile"><div class="metric-label">Min</div><div class="metric-value" style="color:var(--danger);">${formatPct(p.return_distribution.min)}</div></div>
    <div class="metric-tile"><div class="metric-label">25th %ile</div><div class="metric-value">${formatPct(p.return_distribution.p25)}</div></div>
    <div class="metric-tile"><div class="metric-label">Median</div><div class="metric-value">${formatPct(p.return_distribution.median)}</div></div>
    <div class="metric-tile"><div class="metric-label">75th %ile</div><div class="metric-value">${formatPct(p.return_distribution.p75)}</div></div>
    <div class="metric-tile"><div class="metric-label">Max</div><div class="metric-value" style="color:var(--success);">${formatPct(p.return_distribution.max)}</div></div>
    <div class="metric-tile"><div class="metric-label">Skewness</div><div class="metric-value">${formatNum(p.return_distribution.skewness, 2)}</div></div>
    <div class="metric-tile"><div class="metric-label">Kurtosis</div><div class="metric-value">${formatNum(p.return_distribution.kurtosis, 2)}</div></div>
    <div class="metric-tile"><div class="metric-label">+ Days</div><div class="metric-value" style="color:var(--success);">${p.return_distribution.positive_days}</div></div>
    <div class="metric-tile"><div class="metric-label">− Days</div><div class="metric-value" style="color:var(--danger);">${p.return_distribution.negative_days}</div></div>
    <div class="metric-tile"><div class="metric-label">0 Days</div><div class="metric-value">${p.return_distribution.zero_days}</div></div>
    ` : ''}
    ${p.return_attribution ? `
    <div class="metric-tile" style="grid-column:1 / -1;background:rgba(68,209,123,0.06);border-left:3px solid var(--success);">
      <div class="metric-label">Return Attribution</div>
    </div>
    <div class="metric-tile"><div class="metric-label">Total Return</div><div class="metric-value">${formatPct(p.return_attribution.total_return)}</div></div>
    <div class="metric-tile"><div class="metric-label">Real Return</div><div class="metric-value" style="color:${(p.return_attribution.real_return || 0) >= 0 ? 'var(--success)' : 'var(--danger)'};">${formatPct(p.return_attribution.real_return)}</div></div>
    <div class="metric-tile"><div class="metric-label">Dividend Yield</div><div class="metric-value" style="color:var(--success);">${formatPct(p.return_attribution.dividend_yield)}</div></div>
    <div class="metric-tile"><div class="metric-label">Capital Apprec.</div><div class="metric-value">${formatPct(p.return_attribution.capital_appreciation)}</div></div>
    <div class="metric-tile"><div class="metric-label">Inflation</div><div class="metric-value" style="color:var(--danger);">${formatPct(p.return_attribution.inflation_total)}</div></div>
    ` : ''}
  </div>`;
}

function renderEquityChart() {
  const p = state.performance;
  Promise.all([
    fetchJson('/api/portfolio/holdings/history'),
  ]).then(([daily]) => {
    if (!daily.length) return;
    const ctx = $('#pf-equity-chart');
    destroyChart('equity');

    const datasets = [{
      label: 'Portfolio',
      data: daily.map(d => ((d.cumulative_return || 0)) * 100),
      borderColor: '#58a6ff',
      backgroundColor: 'rgba(88,166,255,0.08)',
      fill: true, tension: 0.2, pointRadius: 0, yAxisID: 'y',
      borderWidth: 2,
    }];

    // Add benchmark line from performance data
    if (p && p.benchmark && p.benchmark.series && p.benchmark.series.length) {
      // Build a map of date → benchmark cumulative return
      const benchMap = {};
      for (const pt of p.benchmark.series) {
        benchMap[pt.date] = pt.cumulative_return;
      }
      const benchData = daily.map(d => {
        const v = benchMap[d.date];
        return v != null ? v * 100 : null;
      });
      datasets.push({
        label: p.benchmark.ticker || 'Benchmark',
        data: benchData,
        borderColor: '#e0af4f',
        borderDash: [2, 2],
        tension: 0.2, pointRadius: 0, yAxisID: 'y',
        borderWidth: 1.5,
        spanGaps: true,
      });
    }

    // Add inflation line
    if (p && p.inflation_series && p.inflation_series.length) {
      const infMap = {};
      for (const pt of p.inflation_series) {
        infMap[pt.date] = pt.cumulative;
      }
      const infData = daily.map(d => {
        const v = infMap[d.date];
        return v != null ? v * 100 : null;
      });
      datasets.push({
        label: 'Inflation (' + (p.base_currency || 'EUR') + ')',
        data: infData,
        borderColor: '#ff6b6b',
        borderDash: [8, 4],
        tension: 0.2, pointRadius: 0, yAxisID: 'y',
        borderWidth: 1,
        spanGaps: true,
      });
    }

    state.charts.equity = new Chart(ctx, {
      type: 'line',
      data: {
        labels: daily.map(d => d.date),
        datasets,
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { intersect: false, mode: 'index' },
        scales: {
          y: { position: 'left', ticks: { color: '#8ea0b8', callback: v => (v).toFixed(1) + '%' }, title: { display: true, text: 'Cumulative Return', color: '#8ea0b8' } },
          x: { ticks: { color: '#8ea0b8', maxTicksLimit: 10 } },
        },
        plugins: { legend: { labels: { color: '#d9e2f2', usePointStyle: true } } },
      },
    });
  });
}

// ================================================================
// Rebuild
// ================================================================
function wireRebuild() {
  $('#pf-rebuild-btn').addEventListener('click', async () => {
    const btn = $('#pf-rebuild-btn');
    btn.disabled = true;
    btn.textContent = '⏳ Rebuilding…';
    try {
      const resp = await fetch('/api/portfolio/rebuild', { method: 'POST' });
      const data = await resp.json();
      btn.textContent = `✓ ${data.daily_rows} days, ${data.holdings_count} holdings`;
    } catch (e) {
      btn.textContent = `✗ Failed`;
    } finally {
      setTimeout(() => { btn.disabled = false; btn.textContent = 'Rebuild State'; }, 3000);
      await refreshSummary();
      await loadHoldings();
      await loadTransactions();
    }
  });
}

// ================================================================
// Chart lifecycle
// ================================================================
function destroyChart(key) {
  if (state.charts[key]) { state.charts[key].destroy(); state.charts[key] = null; }
}

// Per-holding mini-charts
let _detailChart = null;
function destroyDetailChart() {
  if (_detailChart) { _detailChart.destroy(); _detailChart = null; }
}

// ================================================================
// Boot
// ================================================================
init().catch(err => console.error('Portfolio boot error:', err));
