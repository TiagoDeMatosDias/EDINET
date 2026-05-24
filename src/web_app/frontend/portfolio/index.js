/**
 * Portfolio Management — five-tab interface.
 *
 * Features:
 *   - Multi-file upload with progress queue
 *   - Summary stats bar (always visible)
 *   - Holdings table with inline detail panels
 *   - Transactions with filters + scrollable table
 *   - Charts: cumulative returns, portfolio value, dividends, allocation,
 *     dividend growth YoY, returns by company, currency split, closed positions
 *   - Performance Metrics auto-compute on tab switch
 */

import { $, el, fetchJson } from '../common/utils.js';
import { log } from '../common/console.js';

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
  chartSettings: { currency: 'EUR', benchmark: '', inflation: true },
  chartData: null,
  chartRawData: {},       // cached raw API responses per chart key
  chartViewMode: {},      // 'chart' | 'table' per chart key
  charts: {},
  uploadedFiles: [],
  sortColumn: null,      // current sort column key
  sortAsc: true,          // sort direction
  displayCurrency: 'EUR', // current display currency for holdings
  displayCurrencies: [],  // available display currencies
  visibleColumns: null,   // null = show all; Set of visible column keys once toggled
  pinnedColumns: new Set(['symbol', 'name', 'asset_type', 'industry', 'open_pos', 'native_ccy', 'quantity']), // sticky columns
  columnOrder: null,      // lazy-init from HOLDINGS_COLUMNS; drag-reorderable
  columnFilters: {},      // {colKey: {type:'text'|'num', values:Set, min, max, active:bool}}
  _colMap: null,          // lazy-built Map<key, colDef>
};

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
async function init() {
  wireTabs();
  wireUpload();
  await refreshSummary();
  await loadDisplayCurrencies();
  wireDisplayCurrency();
  wireColumnVisibility();
  await loadHoldings();
  await loadTransactions();
  wirePerformance();
  wireChartControls();
  wireChartExpand();
  wireTableViewToggles();
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
    const [counts, dateRange] = await Promise.all([
      fetchJson('/api/portfolio/activity-summary'),
      fetchJson('/api/portfolio/date-range'),
    ]);
    state.activitySummary = counts.by_activity || {};
    const totalTxns = Object.values(state.activitySummary).reduce((a, b) => a + b, 0);
    $('#pf-stat-txn').textContent = totalTxns.toLocaleString();
    $('#pf-stat-dates').textContent =
      dateRange.min_date ? `${dateRange.min_date} → ${dateRange.max_date}` : '—';
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
    if (tab === 'charts') renderChartsTab();
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

// ---------------------------------------------------------------------------
// View toggle (Current / Closed / All)
// ---------------------------------------------------------------------------
// ================================================================
// HOLDINGS
// ================================================================
async function loadHoldings() {
  const div = $('#pf-holdings-table');
  div.innerHTML = '<div class="pf-table-loading"><div class="pf-chart-loading-spinner"></div><div class="pf-chart-loading-text">Loading holdings…</div></div>';
  const minDelay = new Promise(resolve => setTimeout(resolve, 300));
  try {
    const dc = state.displayCurrency || 'EUR';
    const fetchPromise = fetchJson(`/api/portfolio/holdings/performance?display_currency=${dc}&include_closed=true`);
    state.holdings = await Promise.all([fetchPromise, minDelay]).then(r => r[0]);
    // Default: filter to show only open positions, hide closed by default
    if (!state.columnFilters['open_pos']) {
      state.columnFilters['open_pos'] = { type: 'text', values: new Set(['Open']), includeNulls: false };
    }
    renderHoldingsTab();
  } catch (e) {
    // Fallback to simple holdings
    try {
      state.holdings = await fetchJson('/api/portfolio/holdings');
      renderHoldingsTab();
    } catch (e2) {
      $('#pf-holdings-table').innerHTML = `<span class="status-text error">${e2.message}</span>`;
    }
  }
}

async function loadDisplayCurrencies() {
  try {
    state.displayCurrencies = await fetchJson('/api/portfolio/display-currencies');
    const sel = $('#pf-display-currency');
    if (!sel) return;
    sel.innerHTML = '';
    for (const c of state.displayCurrencies) {
      const opt = document.createElement('option');
      opt.value = c.code;
      opt.textContent = c.code;
      if (c.code === state.displayCurrency) opt.selected = true;
      sel.appendChild(opt);
    }
  } catch (_) {
    // Keep default options
  }
}

function wireDisplayCurrency() {
  const sel = $('#pf-display-currency');
  if (!sel) return;
  sel.addEventListener('change', async () => {
    state.displayCurrency = sel.value;
    await loadHoldings();
  });
}

// ================================================================
// Chart loading indicator
// ================================================================
function showChartLoading(canvasId) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  hideChartLoading(canvasId);
  const overlay = document.createElement('div');
  overlay.className = 'pf-chart-loading';
  overlay.id = canvasId + '-loading';
  overlay.innerHTML = '<div class="pf-chart-loading-spinner"></div><div class="pf-chart-loading-text">Loading…</div>';
  canvas.parentNode.appendChild(overlay);
}

function hideChartLoading(canvasId) {
  const overlay = document.getElementById(canvasId + '-loading');
  if (overlay) overlay.remove();
}

// ================================================================
// ================================================================
function wireColumnVisibility() {
  const btn = $('#pf-columns-btn');
  const popover = $('#pf-columns-popover');
  const list = $('#pf-columns-list');
  const search = $('#pf-columns-search');
  const selAll = $('#pf-cols-select-all');
  const selNone = $('#pf-cols-select-none');
  if (!btn || !popover || !list) return;

  const allCols = _orderedCols();

  // Render the checkbox list filtered by search term
  function renderColList(filter) {
    const term = (filter || '').toLowerCase();
    const filtered = allCols.filter(col => col.label.toLowerCase().includes(term));
    list.innerHTML = filtered.map(col => {
      const visible = !state.visibleColumns || state.visibleColumns.has(col.key);
      const checked = visible ? 'checked' : '';
      const pinned = state.pinnedColumns.has(col.key);
      const pinIcon = pinned ? '▣' : '□';
      const pinTitle = pinned ? 'Unpin column' : 'Pin column';
      const pinClass = pinned ? 'pf-pin-btn pf-pinned' : 'pf-pin-btn';
      return `<label>
        <input type="checkbox" data-col="${col.key}" ${checked} />
        <span>${col.label}</span>
        <button class="${pinClass}" data-pin="${col.key}" title="${pinTitle}" style="background:none;border:none;cursor:pointer;padding:0;font-size:10px;line-height:1;">${pinIcon}</button>
      </label>`;
    }).join('');
    // Re-wire checkboxes
    list.querySelectorAll('input[type=checkbox]').forEach(cb => {
      cb.addEventListener('change', () => {
        const key = cb.dataset.col;
        if (!state.visibleColumns) {
          state.visibleColumns = new Set(allCols.map(c => c.key));
        }
        if (cb.checked) {
          state.visibleColumns.add(key);
        } else {
          state.visibleColumns.delete(key);
        }
        applyColumnVisibility();
      });
    });
    // Re-wire pin buttons
    list.querySelectorAll('.pf-pin-btn').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        e.preventDefault();
        const key = btn.dataset.pin;
        if (state.pinnedColumns.has(key)) {
          state.pinnedColumns.delete(key);
        } else {
          state.pinnedColumns.add(key);
        }
        applyColumnVisibility();
        renderColList(search ? search.value : '');
      });
    });
  }

  btn.addEventListener('click', (e) => {
    e.stopPropagation();
    if (popover.style.display === 'block') {
      popover.style.display = 'none';
      return;
    }
    popover.style.display = 'block';
    if (search) search.value = '';
    renderColList('');
  });

  // Search filter
  if (search) {
    search.addEventListener('input', () => renderColList(search.value));
  }

  // Select All
  if (selAll) {
    selAll.addEventListener('click', () => {
      state.visibleColumns = new Set(allCols.map(c => c.key));
      applyColumnVisibility();
      renderColList(search ? search.value : '');
    });
  }

  // Select None
  if (selNone) {
    selNone.addEventListener('click', () => {
      state.visibleColumns = new Set(['symbol', 'name', 'native_ccy']);
      applyColumnVisibility();
      renderColList(search ? search.value : '');
    });
  }

  // Close popover on outside click
  document.addEventListener('click', (e) => {
    if (!popover.contains(e.target) && e.target !== btn) {
      popover.style.display = 'none';
    }
  });
}

function applyColumnVisibility() {
  const visible = state.visibleColumns;
  const pinned = state.pinnedColumns;
  const allCols = _orderedCols();

  // Calculate left offsets for pinned columns
  let leftOffset = 0;
  const colOffsets = {};
  for (const col of allCols) {
    if (pinned.has(col.key) && (!visible || visible.has(col.key))) {
      colOffsets[col.key] = leftOffset;
      // Estimate column width: th width (approx, refined by browser after render)
    }
    leftOffset += 0; // Will be set by browser after DOM update
  }

  // Update th visibility and pinning
  let lastPinnedKey = null;
  for (const col of allCols) {
    const th = document.querySelector(`.pf-col-th-${col.key}`);
    if (!th) continue;
    const show = !visible || visible.has(col.key);
    const isPinned = show && pinned.has(col.key);
    th.classList.toggle('pf-col-hidden', !show);
    th.classList.toggle('pf-sticky-col', isPinned);
    th.classList.remove('pf-sticky-col-last');
    if (isPinned) lastPinnedKey = col.key;
  }
  // Mark the LAST pinned column for shadow
  if (lastPinnedKey) {
    const lastTh = document.querySelector(`.pf-col-th-${lastPinnedKey}`);
    if (lastTh) lastTh.classList.add('pf-sticky-col-last');
  }

  // Update td visibility and pinning (tbody + tfoot)
  const rows = document.querySelectorAll('#pf-holdings-tbl tbody tr, #pf-holdings-tbl tfoot tr');
  for (const row of rows) {
    let lastTdPinnedKey = null;
    for (const col of allCols) {
      const td = row.querySelector(`.pf-col-td-${col.key}`);
      if (!td) continue;
      const show = !visible || visible.has(col.key);
      const isPinned = show && pinned.has(col.key);
      td.classList.toggle('pf-col-hidden', !show);
      td.classList.toggle('pf-sticky-col', isPinned);
      td.classList.remove('pf-sticky-col-last');
      if (isPinned) lastTdPinnedKey = col.key;
    }
    if (lastTdPinnedKey) {
      const lastTd = row.querySelector(`.pf-col-td-${lastTdPinnedKey}`);
      if (lastTd) lastTd.classList.add('pf-sticky-col-last');
    }
  }

  // Recalculate sticky left offsets based on actual rendered widths
  requestAnimationFrame(() => {
    let offset = 0;
    let lastKey = null;
    for (const col of allCols) {
      const th = document.querySelector(`.pf-col-th-${col.key}`);
      if (!th || th.classList.contains('pf-col-hidden')) continue;
      if (!pinned.has(col.key)) continue;
      th.style.left = offset + 'px';
      lastKey = col.key;
      offset += th.offsetWidth;
    }
    // Apply same offsets to tds (tbody + tfoot)
    for (const row of document.querySelectorAll('#pf-holdings-tbl tbody tr, #pf-holdings-tbl tfoot tr')) {
      let tdOffset = 0;
      for (const col of allCols) {
        const td = row.querySelector(`.pf-col-td-${col.key}`);
        if (!td || td.classList.contains('pf-col-hidden')) continue;
        if (!pinned.has(col.key)) continue;
        td.style.left = tdOffset + 'px';
        tdOffset += td.offsetWidth;
      }
    }
  });
}

const HOLDINGS_COLUMNS = [
  { key: 'symbol',      label: 'Symbol',               get: h => h.symbol },
  { key: 'name',        label: 'Name',                 get: h => h.performance?.name ?? '' },
  { key: 'asset_type',  label: 'Type',                 get: h => h.asset_category },
  { key: 'industry',    label: 'Industry',             get: h => h.performance?.industry ?? '' },
  { key: 'open_pos',    label: 'Open position',         get: h => h.is_open ? 'Open' : 'Closed' },
  { key: 'native_ccy',  label: 'Native Currency',      get: h => h.currency },
  { key: 'longest_hold',label: 'Longest Hold (Days)',  get: h => h.performance?.longest_holding_days ?? 0, num: true },
  { key: 'latest_hold', label: 'Latest Hold (Days)',   get: h => h.performance?.latest_holding_days ?? 0, num: true },
  { key: 'num_holds',   label: '# Holding Periods',    get: h => h.performance?.num_holding_periods ?? 0, num: true },
  { key: 'quantity',    label: 'Qty',                  get: h => h.quantity, num: true },
  { key: 'avg_cost',    label: 'Avg Cost (Nat.)',      get: h => h.performance?.avg_cost ?? h.avg_cost, num: true },
  { key: 'price',       label: 'Price',                get: h => h.market_price, num: true },
  { key: 'val_native',  label: 'Value (Nat.)',         get: h => h.market_value_native, num: true, native: true },
  { key: 'val_display', label: 'Value (Disp.)',        get: h => h.performance?.current_value_display ?? h.market_value, num: true },
  { key: 'cost_native', label: 'Cost Basis (Nat.)',    get: h => h.performance?.cost_basis_native, num: true, native: true },
  { key: 'cost_display',label: 'Cost Basis (Disp.)',   get: h => h.performance?.cost_basis_display, num: true },
  { key: 'pnl_native',  label: 'P&L (Nat.)',           get: h => h.performance?.pnl_native, num: true, native: true },
  { key: 'pnl_display', label: 'P&L (Disp.)',          get: h => h.performance?.pnl_display, num: true },
  { key: 'div_native',  label: 'Div (Nat.)',           get: h => h.performance?.dividends_native, num: true, native: true },
  { key: 'div_display', label: 'Div (Disp.)',          get: h => h.performance?.dividends_display, num: true },
  { key: 'pct_ret_nat', label: '% Return (Nat.)',      get: h => h.performance?.total_return_native, num: true, native: true },
  { key: 'pct_ret',     label: '% Return (Disp.)',     get: h => h.performance?.total_return_display ?? h.performance?.total_return, num: true },
  { key: 'fx_effect',   label: 'FX Effect',            get: h => h.performance?.fx_return, num: true },
  { key: 'ann_ret_nat', label: 'Ann. Ret (Nat.)',      get: h => h.performance?.annualized_return_native, num: true, native: true },
  { key: 'ann_ret',     label: 'Ann. Ret (Disp.)',     get: h => h.performance?.annualized_return, num: true },
  { key: 'weight',      label: 'Wt%',                  get: h => {
    const totalBase = state._totalDisplayValue || 0;
    const dv = h.performance?.current_value_display ?? h.market_value ?? 0;
    return totalBase ? (Math.abs(dv) / Math.abs(totalBase)) * 100 : 0;
  }, num: true },
];

// ── Column helpers ──
function _col(key) {
  if (!state._colMap) state._colMap = new Map(HOLDINGS_COLUMNS.map(c => [c.key, c]));
  return state._colMap.get(key);
}
function _orderedCols() {
  if (!state.columnOrder) state.columnOrder = HOLDINGS_COLUMNS.map(c => c.key);
  return state.columnOrder.map(k => _col(k)).filter(Boolean);
}

function renderHoldingsTab() {
  renderHoldingsTable();
}

function renderHoldingsTable() {
  const div = $('#pf-holdings-table');
  const hh = state.holdings;
  const dc = state.displayCurrency || 'EUR';

  if (!hh.length) {
    div.innerHTML = '<span class="muted">No positions to show.</span>';
    return;
  }

  const stocks = hh.filter(h => h.asset_category !== 'CASH' && !h.symbol.startsWith('CASH'));
  const openCount = hh.filter(h => h.is_open !== false && h.asset_category !== 'CASH' && !h.symbol.startsWith('CASH')).length;
  const closedCount = hh.filter(h => h.is_open === false).length;
  $('#pf-holdings-title').textContent = 'Holdings';
  const countLabel = `${stocks.length} positions` + (closedCount > 0 ? ` (${closedCount} closed)` : '');
  $('#pf-holdings-count').textContent = countLabel;

  // Compute total display value for weight calculation
  let totalDisplay = 0;
  for (const h of hh) {
    totalDisplay += Math.abs(h.performance?.current_value_display ?? h.market_value ?? 0);
  }
  state._totalDisplayValue = totalDisplay;

  const cols = _orderedCols();

  // ── Sort ──
  let sorted = _applyFilters([...hh]);
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
    html += `<th class="pf-sort-th pf-col-th-${col.key}" data-sort="${col.key}" style="cursor:pointer;user-select:none;white-space:nowrap;">${col.label}${arrow}</th>`;
  }
  html += '</tr></thead><tbody>';

  // ── Render rows ──
  for (const h of sorted) {
    const isCash = h.asset_category === 'CASH' || h.symbol.startsWith('CASH');
    const isClosed = h.is_open === false;
    const isOption = h.asset_category === 'OPT';
    const cashStyle = isCash ? ((h.market_value || 0) < 0 ? 'color:var(--danger);' : 'color:var(--success);') : '';
    const closedStyle = isClosed ? 'opacity:0.55;' : '';
    const clickable = !isCash;
    const rowClick = clickable ? `data-holding="${encodeURIComponent(h.symbol)}"` : '';
    const p = h.performance || {};
    const ccy = h.currency || (p.currency || '');
    const dcDisp = state.displayCurrency || 'EUR';

    // Helper: format a numeric field with color based on sign
    function _fmt(val, colorNeg) {
      if (val == null || isNaN(val)) return '—';
      return formatMoney(val);
    }
    function _pct(val) {
      if (val == null || isNaN(val)) return '—';
      return (val * 100).toFixed(1) + '%';
    }
    function _color(val, pos, neg) {
      if (val == null) return '';
      return val >= 0 ? `color:${pos || 'var(--success)'};` : `color:${neg || 'var(--danger)'};`;
    }

    // Build cell values
    const normalizedSym = normalizeTicker(h.symbol);
    const canLink = !isOption && !isCash;
    const symLabel = isClosed
      ? (canLink
          ? `<a class="pf-sym-link" href="/security?symbol=${encodeURIComponent(normalizedSym)}" onclick="event.stopPropagation();"><span style="${closedStyle}">${h.symbol}</span></a> <span class="pf-closed-badge">CLOSED</span>`
          : `<span style="${closedStyle}">${h.symbol}</span> <span class="pf-closed-badge">CLOSED</span>`)
      : `<a class="pf-sym-link" href="/security?symbol=${encodeURIComponent(normalizedSym)}" onclick="event.stopPropagation();"><strong style="${cashStyle}">${h.symbol}</strong></a>`;

    const mvNative = isClosed ? '—' : (h.market_value_native != null ? formatMoney(h.market_value_native) + ' ' + (isCash ? '' : ccy) : '—');
    const mvDisplay = isClosed ? '—' : (p.current_value_display != null ? formatMoney(p.current_value_display) + ' ' + dcDisp : (h.market_value != null ? formatMoney(h.market_value) : '—'));
    const costNat = isClosed ? '—' : (p.cost_basis_native != null ? formatMoney(p.cost_basis_native) : '—');
    const costDisp = isClosed ? '—' : (p.cost_basis_display != null ? formatMoney(p.cost_basis_display) : '—');
    const pnlNat = isClosed ? '—' : (p.pnl_native != null ? formatMoney(p.pnl_native) : '—');
    const pnlDisp = isClosed ? (p.realized_pnl != null ? formatMoney(p.realized_pnl) : '—') : (p.pnl_display != null ? formatMoney(p.pnl_display) : '—');
    const divNat = isClosed ? '—' : (p.dividends_native != null ? formatMoney(p.dividends_native) : '—');
    const divDisp = isClosed ? '—' : (p.dividends_display != null ? formatMoney(p.dividends_display) : (p.dividend_income != null ? formatMoney(p.dividend_income) : '—'));
    // Closed positions: compute % return from realized_pnl / total_cost
    const retNat = isClosed ? '—' : (p.total_return_native != null ? _pct(p.total_return_native) : '—');
    const closedRet = isClosed && p.realized_pnl != null && p.total_cost ? (p.realized_pnl / p.total_cost) : null;
    const retDisp = isClosed ? (closedRet != null ? _pct(closedRet) : '—') : ((p.total_return_display ?? p.total_return) != null ? _pct(p.total_return_display ?? p.total_return) : '—');
    // FX effect: how much of the display-currency return comes from FX movement
    const fxEffect = isClosed ? '—' : (p.fx_return != null ? _pct(p.fx_return) : '—');
    const annNat = isClosed ? '—' : (p.annualized_return_native != null ? _pct(p.annualized_return_native) : '—');
    const annDisp = isClosed ? '—' : (p.annualized_return != null ? _pct(p.annualized_return) : '—');
    const wt = totalDisplay ? (Math.abs(p.current_value_display ?? h.market_value ?? 0) / Math.abs(totalDisplay) * 100).toFixed(1) + '%' : '—';

    // Deterministic P&L colors
    const pnlNatRaw = p.pnl_native;
    const pnlDispRaw = isClosed ? p.realized_pnl : p.pnl_display;
    const retNatRaw = p.total_return_native;
    const retDispRaw = p.total_return_display ?? p.total_return;
    const fxEffectRaw = p.fx_return;
    const annNatRaw = p.annualized_return_native;
    const annDispRaw = p.annualized_return;

    const nameStr = (p.name || h.description || '').length > 40 ? (p.name || h.description || '').substring(0, 37) + '…' : (p.name || h.description || '');
    const typeStr = isCash ? 'Cash' : isOption ? 'Option' : (h.asset_category === 'ETF' ? 'ETF' : 'Stock');
    const industryStr = p.industry || '';

    html += `<tr class="pf-holding-row" ${rowClick} data-symbol="${encodeURIComponent(h.symbol)}" style="cursor:${clickable ? 'pointer' : 'default'}; ${closedStyle}">
      <td class="pf-col-td-symbol">${symLabel}</td>
      <td class="pf-col-td-name" style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${(p.name || h.description || '').replace(/"/g, '&quot;')}">${nameStr || '—'}</td>
      <td class="pf-col-td-asset_type"><span class="badge ${h.asset_category === 'CASH' ? 'bg-muted' : h.asset_category === 'OPT' ? 'bg-warning' : 'bg-accent'}" style="font-size:10px;">${typeStr}</span></td>
      <td class="pf-col-td-industry" style="max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${(industryStr).replace(/"/g, '&quot;')}">${industryStr || '—'}</td>
      <td class="pf-col-td-open_pos"><span class="badge ${isClosed ? 'bg-muted' : 'bg-success'}" style="font-size:10px;">${isClosed ? 'Closed' : 'Open'}</span></td>
      <td class="pf-col-td-native_ccy"><span class="pf-ccy-badge" style="font-size:10px;color:var(--warning);">${ccy || '—'}</span></td>
      <td class="pf-col-td-longest_hold">${isCash ? '' : (p.longest_holding_days ?? 0)}</td>
      <td class="pf-col-td-latest_hold">${isCash ? '' : (p.latest_holding_days ?? 0)}</td>
      <td class="pf-col-td-num_holds">${isCash ? '' : (p.num_holding_periods ?? 0)}</td>
      <td class="pf-col-td-quantity">${isCash ? '' : h.quantity}</td>
      <td class="pf-col-td-avg_cost">${h.avg_cost != null ? formatMoney(h.avg_cost) : '—'}</td>
      <td class="pf-col-td-price">${isClosed ? '—' : (h.market_price != null ? formatMoney(h.market_price) : '—')}</td>
      <td class="pf-col-td-val_native">${mvNative}</td>
      <td class="pf-col-td-val_display" style="${cashStyle}${closedStyle}">${mvDisplay}</td>
      <td class="pf-col-td-cost_native">${costNat}</td>
      <td class="pf-col-td-cost_display">${costDisp}</td>
      <td class="pf-col-td-pnl_native" style="${_color(pnlNatRaw)}${closedStyle}">${pnlNat}</td>
      <td class="pf-col-td-pnl_display" style="${_color(pnlDispRaw)}${closedStyle}">${pnlDisp}</td>
      <td class="pf-col-td-div_native" style="${p.dividends_native !== null && p.dividends_native !== 0 ? _color(p.dividends_native) : ''}${closedStyle}">${divNat}</td>
      <td class="pf-col-td-div_display" style="${p.dividends_display !== null && p.dividends_display !== 0 ? _color(p.dividends_display) : (p.dividend_income !== null && p.dividend_income !== 0 ? 'color:var(--success);' : '')}${closedStyle}">${divDisp}</td>
      <td class="pf-col-td-pct_ret_nat" style="${_color(retNatRaw)}${closedStyle}">${retNat}</td>
      <td class="pf-col-td-pct_ret" style="${_color(retDispRaw)}${closedStyle}">${retDisp}</td>
      <td class="pf-col-td-fx_effect" style="${_color(fxEffectRaw)}${closedStyle}">${fxEffect}</td>
      <td class="pf-col-td-ann_ret_nat" style="${_color(annNatRaw)}${closedStyle}">${annNat}</td>
      <td class="pf-col-td-ann_ret" style="${_color(annDispRaw)}${closedStyle}">${annDisp}</td>
      <td class="pf-col-td-weight">${wt}</td>
    </tr>`;
  }

  // ── Compute summary row ──
  const summary = {};
  const sumCols = ['val_native','val_display','cost_native','cost_display','pnl_native','pnl_display','div_native','div_display'];
  const avgCols = ['pct_ret_nat','pct_ret','fx_effect','ann_ret_nat','ann_ret'];
  for (const k of sumCols) summary[k] = 0;
  for (const k of avgCols) { summary[k] = 0; summary[k + '_count'] = 0; }
  summary.weight = 0;
  for (const h of sorted) {
    const p = h.performance || {};
    for (const k of sumCols) {
      const col = cols.find(c => c.key === k);
      const v = col ? col.get(h) : 0;
      if (v != null && !isNaN(v)) summary[k] += v;
    }
    for (const k of avgCols) {
      const col = cols.find(c => c.key === k);
      const v = col ? col.get(h) : null;
      if (v != null && !isNaN(v)) { summary[k] += v; summary[k + '_count']++; }
    }
    const wcol = cols.find(c => c.key === 'weight');
    const w = wcol ? wcol.get(h) : 0;
    if (w != null && !isNaN(w)) summary.weight += w;
  }
  const sVals = {};
  for (const k of sumCols) sVals[k] = formatMoney(summary[k]);
  for (const k of avgCols) {
    const cnt = summary[k + '_count'];
    sVals[k] = cnt > 0 ? (summary[k] / cnt * 100).toFixed(1) + '%' : '—';
  }
  sVals['weight'] = summary.weight.toFixed(1) + '%';

  html += '<tfoot><tr class="pf-summary-row">';
  for (const col of cols) {
    const k = col.key;
    if (k === 'symbol') { html += '<td class="pf-col-td-symbol"><strong>Summary</strong></td>'; continue; }
    if (k === 'name' || k === 'asset_type' || k === 'industry' || k === 'longest_hold' || k === 'latest_hold' || k === 'num_holds' || k === 'quantity' || k === 'avg_cost' || k === 'price') {
      html += `<td class="pf-col-td-${k}"></td>`;
      continue;
    }
    const v = sVals[k] || '';
    let style = '';
    if (k === 'pnl_native' || k === 'pnl_display' || k === 'pct_ret_nat' || k === 'pct_ret' || k === 'fx_effect' || k === 'ann_ret_nat' || k === 'ann_ret') {
      const raw = summary[k];
      if (raw != null) style = raw >= 0 ? 'color:var(--success);' : 'color:var(--danger);';
    }
    if (k === 'div_native' || k === 'div_display') style = 'color:var(--success);';
    html += `<td class="pf-col-td-${k}" style="${style}">${v}</td>`;
  }
  html += '</tr></tfoot>';

  html += '</tbody></table>';
  div.innerHTML = html;

  // Apply column visibility after render
  applyColumnVisibility();
  // Apply filter indicators
  _updateFilterIndicators();

  // Wire sort clicks
  $$('.pf-sort-th').forEach(th => {
    th.addEventListener('click', (e) => {
      // Ctrl+click opens filter popup
      if (e.ctrlKey || e.metaKey) {
        e.preventDefault();
        openColumnFilter(th.dataset.sort, th);
        return;
      }
      const key = th.dataset.sort;
      if (state.sortColumn === key) {
        state.sortAsc = !state.sortAsc;
      } else {
        state.sortColumn = key;
        state.sortAsc = true;
      }
      renderHoldingsTable();
    });
    // Right-click for filter
    th.addEventListener('contextmenu', (e) => {
      e.preventDefault();
      openColumnFilter(th.dataset.sort, th);
    });
    // Drag-and-drop reorder
    th.draggable = true;
    th.addEventListener('dragstart', (e) => {
      e.dataTransfer.setData('text/plain', th.dataset.sort);
      e.dataTransfer.effectAllowed = 'move';
      th.style.opacity = '0.4';
    });
    th.addEventListener('dragend', (e) => {
      th.style.opacity = '1';
      $$('.pf-sort-th').forEach(t => t.classList.remove('pf-drag-over'));
    });
    th.addEventListener('dragover', (e) => {
      e.preventDefault();
      e.dataTransfer.dropEffect = 'move';
      th.classList.add('pf-drag-over');
    });
    th.addEventListener('dragleave', () => th.classList.remove('pf-drag-over'));
    th.addEventListener('drop', (e) => {
      e.preventDefault();
      th.classList.remove('pf-drag-over');
      const fromKey = e.dataTransfer.getData('text/plain');
      const toKey = th.dataset.sort;
      if (fromKey && toKey && fromKey !== toKey) {
        const order = state.columnOrder;
        const fromIdx = order.indexOf(fromKey);
        const toIdx = order.indexOf(toKey);
        if (fromIdx >= 0 && toIdx >= 0) {
          order.splice(fromIdx, 1);
          order.splice(toIdx, 0, fromKey);
          renderHoldingsTable();
        }
      }
    });
  });

  // Wire row clicks for inline detail
  $$('.pf-holding-row[data-holding]').forEach(row => {
    row.addEventListener('click', () => toggleHoldingDetail(row));
  });
}

// ── Column filter popup ──
function openColumnFilter(colKey, anchorEl) {
  closeFilterPopup();
  const col = _col(colKey);
  if (!col) return;
  const existing = state.columnFilters[colKey] || {};
  const popup = document.createElement('div');
  popup.className = 'pf-filter-popup';
  popup.id = 'pf-filter-popup';

  if (col.num) {
    // ── Numeric filter: expression-bar style with add/remove ──
    const conditions = (existing.conditions && existing.conditions.length) ? existing.conditions : [{ op: 'gte', val: null }];

    function renderNumRows() {
      var rows = popup.querySelector('#pf-filt-rows');
      if (!rows) return;
      var h = '';
      for (var i = 0; i < conditions.length; i++) {
        var c = conditions[i];
        h += '<div class="pf-filt-cond-row" style="display:flex;align-items:center;gap:4px;padding:2px 0;flex-shrink:0;">' +
          '<select class="pf-filt-cond-op" data-idx="' + i + '" style="padding:3px 4px;font-size:10px;background:var(--bg,#0b1018);color:var(--text);border:1px solid var(--line);border-radius:3px;">' +
            '<option value="eq"' + (c.op==='eq'?' selected':'') + '>=</option>' +
            '<option value="gte"' + (c.op==='gte'?' selected':'') + '>≥</option>' +
            '<option value="gt"' + (c.op==='gt'?' selected':'') + '>&gt;</option>' +
            '<option value="lte"' + (c.op==='lte'?' selected':'') + '>≤</option>' +
            '<option value="lt"' + (c.op==='lt'?' selected':'') + '>&lt;</option>' +
          '</select>' +
          '<input type="number" class="pf-filt-cond-val" data-idx="' + i + '" value="' + (c.val != null ? c.val : '') + '" placeholder="Value" step="any" style="flex:1;min-width:60px;padding:3px 6px;font-size:11px;background:var(--bg);color:var(--text);border:1px solid var(--line);border-radius:3px;" />' +
          '<button class="pf-filt-cond-rm" data-idx="' + i + '" style="background:none;border:none;cursor:pointer;color:var(--muted);font-size:14px;padding:0 4px;" title="Remove">×</button>' +
        '</div>';
      }
      rows.innerHTML = h;
    }

    var html = '<div style="font-weight:600;margin-bottom:6px;flex-shrink:0;">Filter: ' + col.label + '</div>' +
      '<div class="pf-filter-body">' +
      '<div id="pf-filt-rows" style="flex-shrink:0;"></div>' +
      '<button class="btn-ghost btn-sm" id="pf-filt-add" style="font-size:10px;align-self:flex-start;margin-top:4px;">+ Add condition</button>' +
      '</div>' +
      '<div class="pf-filter-actions">' +
        '<button class="btn-ghost btn-sm" id="pf-filt-apply" style="flex:1;">Apply</button>' +
        '<button class="btn-ghost btn-sm" id="pf-filt-clear" style="flex:1;">Clear</button>' +
      '</div>';

    popup.innerHTML = html;
    renderNumRows();
    document.body.appendChild(popup);
    _setupFilterResize(popup);
    var rect = anchorEl.getBoundingClientRect();
    popup.style.left = Math.min(rect.left, window.innerWidth - 330) + 'px';
    popup.style.top = (rect.bottom + 4) + 'px';

    popup.addEventListener('change', function(e) {
      if (e.target.classList.contains('pf-filt-cond-op')) {
        conditions[parseInt(e.target.dataset.idx)].op = e.target.value;
      }
    });
    popup.addEventListener('input', function(e) {
      if (e.target.classList.contains('pf-filt-cond-val')) {
        conditions[parseInt(e.target.dataset.idx)].val = e.target.value;
      }
    });
    popup.addEventListener('click', function(e) {
      if (e.target.classList.contains('pf-filt-cond-rm')) {
        e.stopPropagation();
        e.preventDefault();
        var idx = parseInt(e.target.dataset.idx);
        conditions.splice(idx, 1);
        renderNumRows();
      }
    });
    $('#pf-filt-add').addEventListener('click', function() {
      conditions.push({ op: 'gte', val: null });
      renderNumRows();
    });
    $('#pf-filt-apply').addEventListener('click', function() {
      // Read current values from DOM
      var rows = popup.querySelectorAll('.pf-filt-cond-row');
      for (var i = 0; i < rows.length; i++) {
        if (i < conditions.length) {
          conditions[i].op = rows[i].querySelector('.pf-filt-cond-op').value;
          conditions[i].val = rows[i].querySelector('.pf-filt-cond-val').value;
        }
      }
      var active = conditions.filter(function(c) { return c.val !== null && c.val !== ''; });
      if (active.length === 0) {
        delete state.columnFilters[colKey];
      } else {
        state.columnFilters[colKey] = { type: 'num', conditions: active };
      }
      renderHoldingsTable();
    });
    $('#pf-filt-clear').addEventListener('click', function() { delete state.columnFilters[colKey]; renderHoldingsTable(); });
    popup.addEventListener('keydown', function(e) { if (e.key === 'Enter') $('#pf-filt-apply').click(); });
  } else {
    // ── Text filter: search + values + wildcard ──
    const allVals = new Set();
    var hasNulls = false;
    const scope = state.holdings;
    for (const h of scope) {
      const v = col.get(h);
      if (v != null && v !== '') { allVals.add(String(v)); }
      else { hasNulls = true; }
    }
    const sorted = [...allVals].sort();
    const selected = existing.values || allVals;
    const includeNulls = existing.hasOwnProperty('includeNulls') ? existing.includeNulls : true;

    var nullsHtml = '';
    if (hasNulls) {
      nullsHtml = '<label class="pf-check-row" style="border-bottom:1px solid var(--line);margin-bottom:2px;padding-bottom:4px;"><input type="checkbox" id="pf-filt-nulls" ' + (includeNulls ? 'checked' : '') + ' /><span style="color:var(--muted);font-style:italic;">— (no data)</span></label>';
    }

    const html = '<div style="font-weight:600;margin-bottom:6px;flex-shrink:0;">Filter: ' + col.label + '</div>' +
      '<div class="pf-filter-body">' +
      '<input type="text" id="pf-filt-search" placeholder="Search…" style="width:100%;box-sizing:border-box;margin-bottom:4px;padding:4px 6px;font-size:11px;background:var(--bg,#0b1018);color:var(--text);border:1px solid var(--line);border-radius:3px;flex-shrink:0;" />' +
      '<div class="pf-filter-row" style="margin-bottom:4px;flex-shrink:0;">' +
        '<input type="text" id="pf-filt-wildcard" placeholder="Wildcard e.g. *.T" value="' + (existing.wildcard || '') + '" />' +
        '<select id="pf-filt-wc-mode" style="width:60px;"><option value="contains">has</option><option value="start">→</option><option value="end">←</option><option value="regex">~</option></select>' +
      '</div>' +
      '<div class="pf-filter-values" id="pf-filt-values">' +
        nullsHtml +
        sorted.map(function(v) { return '<label class="pf-check-row"><input type="checkbox" data-val="' + v.replace(/"/g,'&quot;') + '" ' + (selected.has(v)?'checked':'') + ' /><span>' + v + '</span></label>'; }).join('') +
      '</div>' +
      '</div>' +
      '<div class="pf-filter-actions">' +
        '<button class="btn-ghost btn-sm" id="pf-filt-apply" style="flex:1;">Apply</button>' +
        '<button class="btn-ghost btn-sm" id="pf-filt-all" style="flex:1;font-size:10px;">All</button>' +
        '<button class="btn-ghost btn-sm" id="pf-filt-none" style="flex:1;font-size:10px;">None</button>' +
      '</div>';
    popup.innerHTML = html;
    // Set saved wildcard mode
    const wcModeSel = popup.querySelector('#pf-filt-wc-mode');
    if (wcModeSel && existing.wcMode) wcModeSel.value = existing.wcMode;
    document.body.appendChild(popup);
    _setupFilterResize(popup);
    const rect = anchorEl.getBoundingClientRect();
    popup.style.left = Math.min(rect.left, window.innerWidth - 330) + 'px';
    popup.style.top = (rect.bottom + 4) + 'px';

    const applyText = () => {
      const sel = new Set();
      popup.querySelectorAll('#pf-filt-values input[type=checkbox]:checked').forEach(cb => { if (cb.id !== 'pf-filt-nulls') sel.add(cb.dataset.val); });
      const nullsCb = popup.querySelector('#pf-filt-nulls');
      const incNulls = nullsCb ? nullsCb.checked : true;
      const wc = $('#pf-filt-wildcard')?.value || '';
      const wcMode = $('#pf-filt-wc-mode')?.value || 'contains';
      if (sel.size === 0 && !wc && incNulls) {
        // Nothing checked but nulls included = only show nulls
        state.columnFilters[colKey] = { type:'text', values: sel, wildcard: null, wcMode, includeNulls: incNulls };
      } else if (sel.size === 0 && !wc && !incNulls) {
        // Nothing checked at all = filter everything out
        state.columnFilters[colKey] = { type:'text', values: sel, wildcard: null, wcMode, includeNulls: false };
      } else if (sel.size === allVals.size && !wc && incNulls) {
        delete state.columnFilters[colKey];
      } else {
        state.columnFilters[colKey] = { type:'text', values: sel, wildcard: wc || null, wcMode, includeNulls: incNulls };
      }
      renderHoldingsTable();
    };
    $('#pf-filt-apply').addEventListener('click', applyText);
    $('#pf-filt-all').addEventListener('click', () => { popup.querySelectorAll('#pf-filt-values input[type=checkbox]').forEach(cb => cb.checked = true); });
    $('#pf-filt-none').addEventListener('click', () => { popup.querySelectorAll('#pf-filt-values input[type=checkbox]').forEach(cb => cb.checked = false); });
    // Search filters the value list
    $('#pf-filt-search').addEventListener('input', () => {
      const q = $('#pf-filt-search').value.toLowerCase();
      popup.querySelectorAll('#pf-filt-values .pf-check-row').forEach(row => {
        const label = (row.querySelector('span')?.textContent || '').toLowerCase();
        row.style.display = label.includes(q) ? '' : 'none';
      });
    });
    popup.addEventListener('keydown', (e) => { if (e.key === 'Enter') $('#pf-filt-apply').click(); });
  }
}

function _setupFilterResize(popup) {
  var ro = new ResizeObserver(function(entries) {
    for (var i = 0; i < entries.length; i++) {
      var e = entries[i];
      var h = e.contentRect.height;
      var headerEl = e.target.querySelector(':scope > div:first-child');
      var actionsEl = e.target.querySelector('.pf-filter-actions');
      var searchEl = e.target.querySelector('#pf-filt-search');
      var wcRow = e.target.querySelector('#pf-filt-wildcard')?.closest('.pf-filter-row');
      var valuesEl = e.target.querySelector('.pf-filter-values');
      if (!valuesEl) return;
      var used = 20; // padding
      if (headerEl) used += headerEl.offsetHeight;
      if (searchEl) used += searchEl.offsetHeight + 4; // +margin
      if (wcRow) used += wcRow.offsetHeight;
      if (actionsEl) used += actionsEl.offsetHeight;
      var avail = h - used;
      if (avail < 20) avail = 20;
      valuesEl.style.maxHeight = avail + 'px';
    }
  });
  ro.observe(popup);
}

function closeFilterPopup() {
  const p = document.getElementById('pf-filter-popup');
  if (p) p.remove();
}

function _updateFilterIndicators() {
  $$('.pf-sort-th').forEach(th => {
    const key = th.dataset.sort;
    const hasFilter = !!state.columnFilters[key];
    // Add/remove filter icon
    let icon = th.querySelector('.pf-filter-indicator');
    if (hasFilter && !icon) {
      icon = document.createElement('span');
      icon.className = 'pf-filter-indicator';
      icon.textContent = ' ⏏';
      icon.style.cssText = 'color:var(--accent,#58a6ff);font-size:10px;';
      th.appendChild(icon);
    } else if (!hasFilter && icon) {
      icon.remove();
    }
  });
}

function _applyFilters(rows) {
  const filters = state.columnFilters;
  if (!Object.keys(filters).length) return rows;
  return rows.filter(h => {
    for (const [key, f] of Object.entries(filters)) {
      const col = _col(key);
      if (!col) continue;
      const raw = col.get(h);
      if (f.type === 'text') {
        const s = String(raw ?? '');
        // Empty/null values: check includeNulls flag (default true = show nulls)
        if ((raw == null || s === '' || s === 'null' || s === 'undefined')) {
          if (f.includeNulls === false) return false;
          continue; // skip value/wildcard checks for empty values if nulls included
        }
        // Explicit empty values set = exclude all
        if (f.values && f.values.size === 0 && !f.wildcard) return false;
        // Check exact values (skip if values not present or wildcard-only)
        if (f.values && f.values.size > 0 && !f.values.has(s)) return false;
        // Check wildcard
        if (f.wildcard) {
          const w = f.wildcard;
          const mode = f.wcMode || 'contains';
          let match = false;
          if (mode === 'contains') match = s.toLowerCase().includes(w.toLowerCase());
          else if (mode === 'start') match = s.toLowerCase().startsWith(w.toLowerCase());
          else if (mode === 'end') match = s.toLowerCase().endsWith(w.toLowerCase());
          else if (mode === 'regex') { try { match = new RegExp(w, 'i').test(s); } catch(_) { match = true; } }
          if (!match) return false;
        }
      } else if (f.type === 'num') {
        var n = parseFloat(raw);
        if (isNaN(n)) return false;
        // All conditions must be met (AND logic)
        var conds = f.conditions || [];
        for (var ci = 0; ci < conds.length; ci++) {
          var c = conds[ci];
          if (c.val === null || c.val === '') continue;
          var v = parseFloat(c.val);
          var op = c.op || 'gte';
          if (op === 'eq' && n !== v) return false;
          if (op === 'gte' && n < v) return false;
          if (op === 'gt' && n <= v) return false;
          if (op === 'lte' && n > v) return false;
          if (op === 'lt' && n >= v) return false;
        }
      }
    }
    return true;
  });
}

// Close filter on outside click
document.addEventListener('click', (e) => {
  const popup = document.getElementById('pf-filter-popup');
  if (popup && !popup.contains(e.target) && !e.target.closest('.pf-sort-th')) {
    popup.remove();
  }
});


// ═══════════════════════════════════════════════════════════════════════════
// Holding detail panel
// ═══════════════════════════════════════════════════════════════════════════
function toggleHoldingDetail(row) {
  const raw = row.dataset.symbol || row.dataset.holding;
  if (!raw) return;
  const symbol = decodeURIComponent(raw);
  const existing = row.nextElementSibling;
  if (existing && existing.classList.contains('pf-holding-detail-row')) {
    existing.remove();
    return;
  }
  $$('.pf-holding-detail-row').forEach(r => r.remove());

  // Find in current holdings first, then closed
  const h = state.holdings.find(x => x.symbol === symbol);
  if (h && h.performance) {
    insertDetailRow(row, symbol, h.performance, false);
    return;
  }
  // Check closed positions
  const cp = state.closedPositions.find(x => x.symbol === symbol);
  if (cp) {
    insertDetailRow(row, symbol, cp, true);
    return;
  }
  // Fetch on demand
  fetchJson(`/api/portfolio/holdings/${encodeURIComponent(symbol)}/performance`)
    .then(p => insertDetailRow(row, symbol, p, false))
    .catch(err => log('warn', `Detail error for ${symbol}: ` + (err && err.message || err)));
}

function insertDetailRow(row, symbol, p, isClosed) {
  const cols = row.querySelectorAll('td').length;
  const pct = v => v != null ? (v * 100).toFixed(2) + '%' : '—';
  const detailTr = document.createElement('tr');
  detailTr.className = 'pf-holding-detail-row';
  const chartId = 'pf-detail-chart-' + symbol.replace(/[^a-zA-Z0-9]/g, '_');

  if (isClosed) {
    // ── Closed position detail ──
    const rpnl = p.realized_pnl || 0;
    const pnlColor = rpnl >= 0 ? 'var(--success)' : 'var(--danger)';
    const cost = p.total_cost || 0;
    const pnlPct = cost > 0 ? ((rpnl / cost) * 100).toFixed(2) + '%' : '—';
    const retColor = rpnl >= 0 ? 'var(--success)' : 'var(--danger)';
    const normSym = normalizeTicker(p.symbol);
    const isStock = p.asset_category === 'STK';
    detailTr.innerHTML = `<td colspan="${cols}" style="padding:12px 16px;background:rgba(88,166,255,0.04);border-left:3px solid var(--warning);">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
      <strong>${p.symbol} — Closed Position  <span class="pf-closed-badge" style="margin-left:6px;">CLOSED</span></strong>
      <button class="btn-ghost btn-sm" onclick="this.closest('tr').remove();">✕</button>
    </div>
    <div class="metric-grid" style="gap:8px;">
      <div class="metric-tile"><div class="metric-label">Realized P&L</div><div class="metric-value" style="color:${pnlColor};">${formatMoney(rpnl)}</div></div>
      <div class="metric-tile"><div class="metric-label">Return %</div><div class="metric-value" style="color:${retColor};">${pnlPct}</div></div>
      <div class="metric-tile"><div class="metric-label">Total Cost</div><div class="metric-value">${formatMoney(p.total_cost)}</div></div>
      <div class="metric-tile"><div class="metric-label">Total Proceeds</div><div class="metric-value">${formatMoney(p.total_proceeds)}</div></div>
      <div class="metric-tile"><div class="metric-label">Total Bought</div><div class="metric-value">${p.total_bought || 0}</div></div>
      <div class="metric-tile"><div class="metric-label">Total Sold</div><div class="metric-value">${p.total_sold || 0}</div></div>
      <div class="metric-tile"><div class="metric-label">First Trade</div><div class="metric-value">${p.first_trade_date || '—'}</div></div>
      <div class="metric-tile"><div class="metric-label">Last Trade</div><div class="metric-value">${p.last_trade_date || '—'}</div></div>
      <div class="metric-tile"><div class="metric-label">Category</div><div class="metric-value">${p.asset_category || '—'}</div></div>
      ${isStock ? `<div class="metric-tile"><a href="/security?symbol=${encodeURIComponent(normSym)}" class="btn-ghost btn-sm">Open Security Analysis →</a></div>` : ''}
    </div>
  </td>`;
  } else {
    // ── Current holding detail ──
    const pnlColor2 = (p.unrealized_pnl || 0) >= 0 ? 'var(--success)' : 'var(--danger)';
    const retColor2 = (p.total_return || 0) >= 0 ? 'var(--success)' : 'var(--danger)';
    detailTr.innerHTML = `<td colspan="${cols}" style="padding:12px 16px;background:rgba(88,166,255,0.04);border-left:3px solid var(--accent);">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
      <strong>${p.symbol} — Performance Details</strong>
      <button class="btn-ghost btn-sm" onclick="this.closest('tr').remove();">✕</button>
    </div>
    <div class="metric-grid" style="gap:8px;margin-bottom:12px;">
      <div class="metric-tile"><div class="metric-label">Total Return</div><div class="metric-value" style="color:${retColor2};">${pct(p.total_return)}</div></div>
      <div class="metric-tile"><div class="metric-label">Ann. Return</div><div class="metric-value" style="color:${retColor2};">${pct(p.annualized_return)}</div></div>
      <div class="metric-tile"><div class="metric-label">Volatility</div><div class="metric-value">${pct(p.volatility)}</div></div>
      <div class="metric-tile"><div class="metric-label">Unrealized P&L</div><div class="metric-value" style="color:${pnlColor2};">${formatMoney(p.unrealized_pnl)}</div></div>
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
  }
  row.after(detailTr);

  // Fetch history and render chart (current holdings only)
  if (!isClosed) {
    destroyDetailChart();
    fetchJson(`/api/portfolio/holdings/${encodeURIComponent(symbol)}/history`)
    .then(history => {
      const ctx = document.getElementById(chartId);
      if (!ctx || !history || !history.length) return;
      const filtered = history.filter(h => h.market_value != null || h.market_price != null);
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
    .catch(err => log('warn', `Chart fetch error for ${symbol}: ${(err && err.message) || err}`));
  }  // end if (!isClosed)
}  // end insertDetailRow

function renderChartsTab() {
  // Pre-load chart data from performance endpoint (sets state.chartData for
  // the equity chart) then render all charts.
  refreshAllCharts();
}

// ---------------------------------------------------------------------------
// Zoom plugin registration & shared helpers
// ---------------------------------------------------------------------------
function _zoomableLineOptions({ yLabel, yCallback, stacked, legendPos } = {}) {
  return {
    responsive: true, maintainAspectRatio: false,
    interaction: { intersect: false, mode: 'index' },
    scales: {
      y: {
        stacked: stacked || false,
        ticks: { color: '#8ea0b8', callback: yCallback || (v => v) },
        title: { display: true, text: yLabel || '', color: '#8ea0b8' },
      },
      x: { ticks: { color: '#8ea0b8', maxTicksLimit: 12 } },
    },
    plugins: {
      legend: {
        position: legendPos || 'top',
        labels: { color: '#d9e2f2', usePointStyle: true, padding: 8, font: { size: 11 } },
      },
      zoom: {
        zoom: {
          wheel: { enabled: true, modifierKey: 'ctrl' },
          pinch: { enabled: true },
          drag: {
            enabled: true,
            backgroundColor: 'rgba(88,166,255,0.1)',
            borderColor: '#58a6ff',
            borderWidth: 1,
          },
          mode: 'x',
        },
        pan: {
          enabled: true,
          mode: 'x',
          modifierKey: 'ctrl',
        },
      },
    },
  };
}

function _destroyAndClear(key) {
  if (state.charts[key]) { state.charts[key].destroy(); state.charts[key] = null; }
}

function destroyChart(key) { _destroyAndClear(key); }

function renderValueChart() {
  const breakdown = $('#pf-value-breakdown')?.checked;
  if (breakdown) {
    renderValueBreakdown();
    return;
  }
  const ccy = state.chartSettings.currency;
  showChartLoading('pf-value-chart');
  fetchJson('/api/portfolio/holdings/history?base_currency=' + ccy).then(daily => {
    if (!daily.length) { hideChartLoading('pf-value-chart'); return; }
    state.chartRawData.value = daily;
    const ctx = $('#pf-value-chart');
    destroyChart('value');
    hideChartLoading('pf-value-chart');
    state.charts.value = new Chart(ctx, {
      type: 'line',
      data: {
        labels: daily.map(d => d.date),
        datasets: [{
          label: 'Total Value (' + ccy + ')',
          data: daily.map(d => d.total_value),
          borderColor: '#58a6ff', backgroundColor: 'rgba(88,166,255,0.1)',
          fill: true, tension: 0.2, pointRadius: 0,
        }, {
          label: 'Cash (' + ccy + ')',
          data: daily.map(d => d.cash_balance),
          borderColor: '#44d17b', borderDash: [5, 3],
          tension: 0.2, pointRadius: 0,
        }],
      },
      options: _zoomableLineOptions({
        yLabel: ccy,
        yCallback: v => formatMoney(v),
      }),
    });
  });
}

const _VALUE_COLORS = [
  '#58a6ff','#44d17b','#e0af4f','#ff6b6b','#8ea0b8',
  '#58a6ffcc','#44d17bcc','#e0af4fcc','#ff6b6bcc','#8ea0b8cc',
  '#58a6ff88','#44d17b88','#e0af4f88','#ff6b6b88','#8ea0b888',
];

function renderValueBreakdown() {
  const ccy = state.chartSettings.currency;
  showChartLoading('pf-value-chart');
  Promise.all([
    fetchJson('/api/portfolio/holdings/history/constituents?base_currency=' + ccy),
    fetchJson('/api/portfolio/holdings/history?base_currency=' + ccy),
  ]).then(([constituents, daily]) => {
    const ctx = $('#pf-value-chart');
    if (!constituents || !constituents.dates || !constituents.dates.length || !daily.length) { hideChartLoading('pf-value-chart'); return; }
    destroyChart('value');

    // Sort symbols by total contribution (largest last = on top of stacked area)
    const syms = Object.keys(constituents.series).sort((a, b) => {
      const sumA = (constituents.series[a] || []).reduce((s, v) => s + (v || 0), 0);
      const sumB = (constituents.series[b] || []).reduce((s, v) => s + (v || 0), 0);
      return sumA - sumB;  // smallest first → rendered last = on top
    });

    // Build cash map from daily
    const cashMap = {};
    for (const d of daily) cashMap[d.date] = d.cash_balance || 0;

    const datasets = [];
    // Stacked area for each symbol
    for (let i = 0; i < syms.length; i++) {
      const sym = syms[i];
      const vals = constituents.series[sym] || [];
      datasets.push({
        label: sym,
        data: vals,
        backgroundColor: _VALUE_COLORS[i % _VALUE_COLORS.length],
        fill: true, tension: 0.2, pointRadius: 0,
        borderWidth: 0,
        spanGaps: false,
      });
    }
    // Cash line on top (not stacked)
    datasets.push({
      label: 'Cash',
      data: constituents.dates.map(d => cashMap[d] || null),
      borderColor: '#44d17b',
      borderDash: [5, 3],
      tension: 0.2, pointRadius: 0,
      fill: false,
      borderWidth: 2,
    });

    hideChartLoading('pf-value-chart');
    state.charts.value = new Chart(ctx, {
      type: 'line',
      data: {
        labels: constituents.dates,
        datasets,
      },
      options: _zoomableLineOptions({
        yLabel: ccy,
        yCallback: v => formatMoney(v),
        stacked: true,
        legendPos: 'right',
      }),
    });
  });
}

function renderDividendsChart() {
  const period = $('#pf-div-period')?.value || 'monthly';
  const ccy = state.chartSettings.currency;
  showChartLoading('pf-dividends-chart');
  fetchJson(`/api/portfolio/dividends/history?period=${period}&base_currency=${ccy}`).then(data => {
    if (!data || !data.length) { hideChartLoading('pf-dividends-chart'); return; }
    state.chartRawData.dividends = data;
    const ctx = $('#pf-dividends-chart');
    destroyChart('dividends');
    hideChartLoading('pf-dividends-chart');
    state.charts.dividends = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: data.map(d => d.period),
        datasets: [
          {
            label: 'Gross',
            data: data.map(d => d.gross),
            backgroundColor: 'rgba(68,209,123,0.6)',
            borderColor: '#44d17b',
            borderWidth: 1,
            borderRadius: 2,
          },
          {
            label: 'Tax',
            data: data.map(d => -d.tax),
            backgroundColor: 'rgba(255,107,107,0.5)',
            borderColor: '#ff6b6b',
            borderWidth: 1,
            borderRadius: 2,
          },
          {
            label: 'Net',
            data: data.map(d => d.net),
            type: 'line',
            borderColor: '#58a6ff',
            backgroundColor: 'rgba(88,166,255,0.15)',
            borderWidth: 2.5,
            tension: 0.2, pointRadius: 3, pointBackgroundColor: '#58a6ff',
            fill: false,
          },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { intersect: false, mode: 'index' },
        scales: {
          y: { ticks: { color: '#8ea0b8', callback: v => formatMoney(v) }, title: { display: true, text: ccy, color: '#8ea0b8' } },
          x: { ticks: { color: '#8ea0b8', maxTicksLimit: 12 } },
        },
        plugins: {
          legend: { labels: { color: '#d9e2f2', usePointStyle: true } },
          tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${formatMoney(ctx.raw)}` } },
        },
      },
    });
  });
}

function renderAllocationChart() {
  const hh = state.holdings.filter(h => !h.is_option);
  if (!hh.length) return;
  state.chartRawData.allocation = { labels: hh.map(h => h.symbol), values: hh.map(h => Math.abs(h.market_value || 0)) };
  const ctx = $('#pf-allocation-chart');
  destroyChart('allocation');
  const labels = hh.map(h => h.symbol);
  const data = hh.map(h => Math.abs(h.market_value || 0));
  hideChartLoading('pf-allocation-chart');
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
// Chart 5: Dividend Growth Year over Year
// ================================================================
function renderDividendGrowthChart() {
  const ccy = state.chartSettings.currency;
  showChartLoading('pf-div-growth-chart');
  fetchJson('/api/portfolio/dividends/yoy?base_currency=' + ccy).then(data => {
    if (!data || !data.years || !data.years.length) { hideChartLoading('pf-div-growth-chart'); return; }
    state.chartRawData.divGrowth = data;
    const ctx = $('#pf-div-growth-chart');
    destroyChart('divGrowth');
    hideChartLoading('pf-div-growth-chart');
    state.charts.divGrowth = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: data.years.map(y => String(y)),
        datasets: [
          {
            label: 'Dividends (' + (data.currency || 'EUR') + ')',
            data: data.dividends,
            backgroundColor: 'rgba(68,209,123,0.55)',
            borderColor: '#44d17b',
            borderWidth: 1,
            borderRadius: 2,
            order: 2,
          },
          {
            label: 'YoY Growth %',
            data: data.yoy_growth,
            type: 'line',
            borderColor: '#58a6ff',
            backgroundColor: 'rgba(88,166,255,0.1)',
            borderWidth: 2.5,
            tension: 0.2,
            pointRadius: 4,
            pointBackgroundColor: '#58a6ff',
            yAxisID: 'y1',
            order: 1,
          },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { intersect: false, mode: 'index' },
        scales: {
          y: {
            position: 'left',
            ticks: { color: '#8ea0b8', callback: v => formatMoney(v) },
            title: { display: true, text: data.currency || 'EUR', color: '#8ea0b8' },
          },
          y1: {
            position: 'right',
            ticks: { color: '#58a6ff', callback: v => v.toFixed(1) + '%' },
            grid: { drawOnChartArea: false },
          },
          x: { ticks: { color: '#8ea0b8' } },
        },
        plugins: {
          legend: { labels: { color: '#d9e2f2', usePointStyle: true } },
          tooltip: {
            callbacks: {
              label: (ctx) => {
                if (ctx.dataset.label === 'YoY Growth %') {
                  return 'YoY Growth: ' + (ctx.raw != null ? ctx.raw.toFixed(1) + '%' : 'N/A');
                }
                return 'Dividends: ' + formatMoney(ctx.raw);
              },
            },
          },
        },
      },
    });
  });
}

// ================================================================
// Chart: DPS per Company Year over Year (toggled from Dividend Growth)
// ================================================================
let _divGrowthMode = 'agg';  // 'agg' | 'per-company'

function renderDpsPerCompanyChart() {
  showChartLoading('pf-div-growth-chart');
  fetchJson('/api/portfolio/dividends/yoy/per-company').then(data => {
    if (!data || !data.years || !data.years.length) { hideChartLoading('pf-div-growth-chart'); return; }
    state.chartRawData.divGrowth = data;
    const ctx = $('#pf-div-growth-chart');
    destroyChart('divGrowth');

    const companies = data.companies;
    const syms = Object.keys(companies).sort();
    if (!syms.length) return;

    const colors = [
      '#58a6ff','#44d17b','#e0af4f','#ff6b6b','#8ea0b8',
      '#58a6ffcc','#44d17bcc','#e0af4fcc','#ff6b6bcc','#8ea0b8cc',
    ];

    const datasets = syms.map((sym, i) => ({
      label: sym + ' (DPS)',
      data: companies[sym].dps,
      backgroundColor: colors[i % colors.length] + '99',
      borderColor: colors[i % colors.length],
      borderWidth: 1,
      borderRadius: 2,
      hidden: i >= 8,  // show first 8, rest toggleable
    }));

    hideChartLoading('pf-div-growth-chart');
    state.charts.divGrowth = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: data.years.map(y => String(y)),
        datasets,
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { intersect: false, mode: 'index' },
        scales: {
          y: {
            ticks: { color: '#8ea0b8', callback: v => formatMoney(v, 2) },
            title: { display: true, text: 'DPS', color: '#8ea0b8' },
          },
          x: { ticks: { color: '#8ea0b8' } },
        },
        plugins: {
          legend: { position: 'right', labels: { color: '#d9e2f2', usePointStyle: true, padding: 8, font: { size: 10 } } },
          tooltip: { callbacks: { label: ctx => {
            const sym = ctx.dataset.label.replace(' (DPS)', '');
            const val = ctx.raw;
            if (val == null) return sym + ': N/A';
            const growth = companies[sym]?.yoy_growth?.[ctx.dataIndex];
            const growthStr = growth != null ? ' (YoY ' + growth.toFixed(1) + '%)' : '';
            return sym + ': ' + formatMoney(val, 4) + growthStr;
          }}},
        },
      },
    });
  });
}

// ================================================================
// Chart 6: Returns by Company by Year
// ================================================================
function renderReturnsByCompanyChart() {
  showChartLoading('pf-returns-by-company-chart');
  fetchJson('/api/portfolio/returns/by-company').then(data => {
    if (!data || !data.years || !data.years.length) { hideChartLoading('pf-returns-by-company-chart'); return; }

    state.chartRawData.returnsByCompany = data;
    // Cache data so year-switch doesn't re-fetch
    _returnsByCompanyData = data;

    // Populate year selector if not already done
    const yearSel = $('#pf-returns-year');
    if (yearSel && yearSel.options.length <= 1) {
      yearSel.innerHTML = '<option value="all">All Years</option>';
      for (const y of data.years) {
        const opt = document.createElement('option');
        opt.value = String(y);
        opt.textContent = String(y);
        yearSel.appendChild(opt);
      }
    }

    _drawReturnsByCompanyChart(data);
  });
}

let _returnsByCompanyData = null;

function _drawReturnsByCompanyChart(data) {
  const ctx = $('#pf-returns-by-company-chart');
  destroyChart('returnsByCompany');

  const decompose = $('#pf-returns-decompose')?.checked;
  const companies = data.companies;
  const yearSel = $('#pf-returns-year');
  const selectedYear = yearSel?.value || 'all';
  const yearIdx = selectedYear === 'all' ? -1 : data.years.indexOf(parseInt(selectedYear));

  // Build list of [company, value, cap_val, div_val]
  let items = [];
  for (const sym of Object.keys(companies)) {
    const c = companies[sym];
    if (yearIdx >= 0) {
      const val = c.total_return[yearIdx];
      if (val == null) continue;
      items.push({
        sym,
        total: val,
        cap: c.capital_gain[yearIdx],
        div: c.dividend_return[yearIdx],
      });
    } else {
      // All years: use _total_all_years
      const val = c._total_all_years;
      if (val == null) continue;
      items.push({
        sym,
        total: val,
        cap: val,  // no decomposition for all-years view
        div: 0,
      });
    }
  }

  if (!items.length) return;

  // Sort by total value (ascending → horizontal bars render bottom-to-top)
  items.sort((a, b) => a.total - b.total);

  // Dynamic height so all companies are visible
  ctx.parentNode.style.height = Math.max(350, items.length * 26) + 'px';

  const labels = items.map(it => it.sym);

  // Color function: green for positive, red for negative, intensity by magnitude
  function pnlColor(val) {
    if (val == null || val === 0) return '#8ea0b8';
    const intensity = Math.min(Math.abs(val) / 50, 1);  // cap at 50% return
    if (val > 0) {
      const g = Math.round(180 + 75 * intensity);
      const r = Math.round(60 - 60 * intensity);
      return `rgba(${r},${g},80,0.85)`;
    } else {
      const r = Math.round(180 + 75 * intensity);
      const g = Math.round(60 - 60 * intensity);
      return `rgba(${r},${g},80,0.85)`;
    }
  }

  let datasets;
  if (decompose && selectedYear !== 'all') {
    datasets = [
      {
        label: 'Capital Gain',
        data: items.map(it => it.cap),
        backgroundColor: items.map(it => pnlColor(it.cap)),
        borderColor: 'transparent',
        borderWidth: 0,
      },
      {
        label: 'Dividends',
        data: items.map(it => it.div),
        backgroundColor: items.map(it => {
          const v = it.div || 0;
          return v >= 0 ? 'rgba(68,209,123,0.4)' : 'rgba(255,107,107,0.4)';
        }),
        borderColor: 'transparent',
        borderWidth: 0,
      },
    ];
  } else {
    datasets = [{
      label: selectedYear === 'all' ? 'Total Return (all yrs)' : 'Total Return ' + selectedYear,
      data: items.map(it => it.total),
      backgroundColor: items.map(it => pnlColor(it.total)),
      borderColor: items.map(it => it.total >= 0 ? '#44d17b' : '#ff6b6b'),
      borderWidth: 0.5,
      borderRadius: 2,
    }];
  }

  hideChartLoading('pf-returns-by-company-chart');
  state.charts.returnsByCompany = new Chart(ctx, {
    type: 'bar',
    data: { labels, datasets },
    options: {
      indexAxis: 'y',
      responsive: true, maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      scales: {
        x: {
          ticks: { color: '#8ea0b8', callback: v => v.toFixed(1) + '%' },
          title: { display: true, text: 'Return %', color: '#8ea0b8' },
        },
        y: {
          ticks: { color: '#d9e2f2', font: { size: 11 } },
        },
      },
      plugins: {
        legend: {
          display: decompose && selectedYear !== 'all',
          labels: { color: '#d9e2f2', usePointStyle: true },
        },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const it = items[ctx.dataIndex];
              if (!it) return '';
              if (decompose && ctx.dataset.label === 'Capital Gain') {
                return `Cap Gain: ${it.cap?.toFixed(1)}%`;
              }
              if (decompose && ctx.dataset.label === 'Dividends') {
                return `Dividends: ${it.div?.toFixed(1)}%`;
              }
              let s = `Total: ${it.total?.toFixed(1)}%`;
              if (selectedYear !== 'all' && it.div) {
                s += ` (Cap: ${it.cap?.toFixed(1)}%, Div: ${it.div?.toFixed(1)}%)`;
              }
              return s;
            },
          },
        },
      },
    },
  });
}

// ================================================================
// Chart 7: Asset Currency Split (Pie)
// ================================================================
function renderCurrencySplitChart() {
  const hh = state.holdings.filter(h => !h.is_option && h.currency);
  if (!hh.length) return;
  const ctx = $('#pf-currency-split-chart');
  destroyChart('currencySplit');

  const byCcy = {};
  for (const h of hh) {
    const ccy = h.currency || '???';
    const val = Math.abs(h.market_value_native || h.market_value || 0);
    byCcy[ccy] = (byCcy[ccy] || 0) + val;
  }

  const labels = Object.keys(byCcy).sort();
  const data = labels.map(c => byCcy[c]);
  const total = data.reduce((s, v) => s + v, 0);
  state.chartRawData.currencySplit = { labels, values: data, total };

  const currencyColors = {
    EUR: '#44d17b',
    USD: '#58a6ff',
    JPY: '#e0af4f',
    GBP: '#ff6b6b',
    CHF: '#8ea0b8',
    CAD: '#ff6b6bcc',
    HKD: '#44d17bcc',
    SEK: '#58a6ffcc',
    NOK: '#e0af4fcc',
    DKK: '#8ea0b8cc',
  };
  const bgColors = labels.map(c => currencyColors[c] || '#8ea0b8');

  hideChartLoading('pf-currency-split-chart');
  state.charts.currencySplit = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: labels.map(c => c + ' (' + (total > 0 ? (byCcy[c]/total*100).toFixed(1) + '%' : '0%') + ')'),
      datasets: [{
        data,
        backgroundColor: bgColors,
        borderColor: 'transparent',
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { position: 'right', labels: { color: '#d9e2f2', usePointStyle: true, padding: 12 } },
        tooltip: { callbacks: { label: ctx => {
          const ccy = labels[ctx.dataIndex];
          return ccy + ': ' + formatMoney(ctx.raw);
        }}},
      },
    },
  });
}

// ================================================================
// Chart 8: Returns by Closed Positions
// ================================================================
function renderClosedReturnsChart() {
  const ccy = state.chartSettings.currency;
  showChartLoading('pf-closed-returns-chart');
  fetchJson('/api/portfolio/holdings/closed?base_currency=' + ccy).then(closed => {
    state.chartRawData.closedReturns = closed || [];
    if (!closed || !closed.length) {
      const ctx = $('#pf-closed-returns-chart');
      ctx.parentNode.querySelector('canvas')?.remove();
      ctx.parentNode.innerHTML += '<div class="muted" style="text-align:center;padding-top:120px;">No closed positions</div>';
      return;
    }
    const ctx = $('#pf-closed-returns-chart');
    destroyChart('closedReturns');

    // Sort by absolute P&L
    closed.sort((a, b) => Math.abs(b.realized_pnl || 0) - Math.abs(a.realized_pnl || 0));

    const labels = closed.map(c => c.symbol);
    const pnl = closed.map(c => c.realized_pnl || 0);
    const bgColors = pnl.map(v => v >= 0 ? 'rgba(68,209,123,0.7)' : 'rgba(255,107,107,0.7)');
    const borderColors = pnl.map(v => v >= 0 ? '#44d17b' : '#ff6b6b');

    hideChartLoading('pf-closed-returns-chart');
    state.charts.closedReturns = new Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [{
          label: 'Realized P&L',
          data: pnl,
          backgroundColor: bgColors,
          borderColor: borderColors,
          borderWidth: 1,
          borderRadius: 3,
        }],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        scales: {
          y: {
            ticks: { color: '#8ea0b8', callback: v => formatMoney(v) },
            title: { display: true, text: ccy, color: '#8ea0b8' },
          },
          x: { ticks: { color: '#d9e2f2' } },
        },
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: ctx => 'P&L: ' + formatMoney(ctx.raw) } },
        },
      },
    });
  });
}

// ================================================================
// Chart 9: Money-Weighted Returns (Modified Dietz)
// ================================================================
let _mwReturnsData = null;

function renderMoneyWeightedReturnsChart() {
  showChartLoading('pf-mw-returns-chart');
  fetchJson('/api/portfolio/returns/money-weighted').then(data => {
    if (!data || !data.years || !data.years.length) { hideChartLoading('pf-mw-returns-chart'); return; }
    state.chartRawData.mwReturns = data;
    _mwReturnsData = data;

    const yearSel = $('#pf-mw-returns-year');
    if (yearSel && yearSel.options.length <= 1) {
      yearSel.innerHTML = '<option value="all">All Years</option>';
      for (const y of data.years) {
        const opt = document.createElement('option');
        opt.value = String(y); opt.textContent = String(y);
        yearSel.appendChild(opt);
      }
    }
    _drawMWReturnsChart(data);
  });
}

function _drawMWReturnsChart(data) {
  const ctx = $('#pf-mw-returns-chart');
  destroyChart('mwReturns');

  const companies = data.companies;
  const yearSel = $('#pf-mw-returns-year');
  const selectedYear = yearSel?.value || 'all';
  const yearIdx = selectedYear === 'all' ? -1 : data.years.indexOf(parseInt(selectedYear));

  let items = [];
  for (const sym of Object.keys(companies)) {
    const c = companies[sym];
    if (yearIdx >= 0) {
      const val = c.return_pct[yearIdx];
      if (val == null) continue;
      items.push({ sym, val });
    } else {
      // Use full-period total return computed by the backend
      if (c._total_return != null) {
        items.push({ sym, val: c._total_return });
      }
    }
  }
  if (!items.length) return;
  items.sort((a, b) => a.val - b.val);

  // Dynamic height so all companies are visible
  ctx.parentNode.style.height = Math.max(350, items.length * 26) + 'px';

  function mwColor(val) {
    if (val == null || val === 0) return '#8ea0b8';
    const intensity = Math.min(Math.abs(val) / 50, 1);
    if (val > 0) {
      return `rgba(${Math.round(60-60*intensity)},${Math.round(180+75*intensity)},80,0.85)`;
    } else {
      return `rgba(${Math.round(180+75*intensity)},${Math.round(60-60*intensity)},80,0.85)`;
    }
  }

  hideChartLoading('pf-mw-returns-chart');
  state.charts.mwReturns = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: items.map(it => it.sym),
      datasets: [{
        label: 'MW Return %',
        data: items.map(it => it.val),
        backgroundColor: items.map(it => mwColor(it.val)),
        borderColor: items.map(it => it.val >= 0 ? '#44d17b' : '#ff6b6b'),
        borderWidth: 0.5, borderRadius: 2,
      }],
    },
    options: {
      indexAxis: 'y',
      responsive: true, maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      scales: {
        x: {
          ticks: { color: '#8ea0b8', callback: v => v.toFixed(1) + '%' },
          title: { display: true, text: 'Return %', color: '#8ea0b8' },
        },
        y: { ticks: { color: '#d9e2f2', font: { size: 11 } } },
      },
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: ctx => 'MW Return: ' + ctx.raw?.toFixed(1) + '%' } },
      },
    },
  });
}

// ================================================================
// Chart 10: Contribution to Portfolio Return
// ================================================================
let _contribData = null;

function renderContributionChart() {
  const ccy = state.chartSettings.currency;
  showChartLoading('pf-contrib-chart');
  fetchJson('/api/portfolio/returns/contribution?base_currency=' + ccy).then(data => {
    if (!data || !data.years || !data.years.length) { hideChartLoading('pf-contrib-chart'); return; }
    state.chartRawData.contribution = data;
    _contribData = data;

    const yearSel = $('#pf-contrib-year');
    if (yearSel && yearSel.options.length <= 1) {
      yearSel.innerHTML = '<option value="all">All Years</option>';
      for (const y of data.years) {
        const opt = document.createElement('option');
        opt.value = String(y); opt.textContent = String(y);
        yearSel.appendChild(opt);
      }
    }
    _drawContributionChart(data);
  });
}

function _drawContributionChart(data) {
  const ctx = $('#pf-contrib-chart');
  destroyChart('contribution');

  const companies = data.companies;
  const yearSel = $('#pf-contrib-year');
  const selectedYear = yearSel?.value || 'all';
  const yearIdx = selectedYear === 'all' ? -1 : data.years.indexOf(parseInt(selectedYear));

  let items = [];
  for (const sym of Object.keys(companies)) {
    const c = companies[sym];
    if (yearIdx >= 0) {
      const val = c.contribution_eur[yearIdx];
      if (val == null) continue;
      items.push({ sym, val });
    } else {
      let total = 0, count = 0;
      for (const v of c.contribution_eur) { if (v != null) { total += v; count++; } }
      if (count > 0) items.push({ sym, val: total });
    }
  }
  if (!items.length) return;
  items.sort((a, b) => a.val - b.val);

  // Dynamic height so all companies are visible
  ctx.parentNode.style.height = Math.max(350, items.length * 26) + 'px';

  hideChartLoading('pf-contrib-chart');
  state.charts.contribution = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: items.map(it => it.sym),
      datasets: [{
        label: 'Contribution (' + ccy + ')',
        data: items.map(it => it.val),
        backgroundColor: items.map(it => it.val >= 0
          ? 'rgba(68,209,123,0.7)' : 'rgba(255,107,107,0.7)'),
        borderColor: items.map(it => it.val >= 0 ? '#44d17b' : '#ff6b6b'),
        borderWidth: 0.5, borderRadius: 2,
      }],
    },
    options: {
      indexAxis: 'y',
      responsive: true, maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      scales: {
        x: {
          ticks: { color: '#8ea0b8', callback: v => formatMoney(v) },
          title: { display: true, text: ccy, color: '#8ea0b8' },
        },
        y: { ticks: { color: '#d9e2f2', font: { size: 11 } } },
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const it = items[ctx.dataIndex];
              if (!it) return '';
              const c = companies[it.sym];
              const pct = yearIdx >= 0 ? c.contribution_pct[yearIdx] : null;
              let s = ccy + ' ' + formatMoney(it.val);
              if (pct != null) s += ' (' + pct.toFixed(1) + '% of portfolio)';
              return s;
            },
          },
        },
      },
    },
  });
}

// ================================================================
// ── Transaction column definitions for filtering ──
const TXN_COLUMNS = [
  { key: 'trade_date',  label: 'Date',        get: t => t.trade_date, date: true },
  { key: 'activity_type',label: 'Type',       get: t => t.activity_type },
  { key: 'symbol',      label: 'Symbol',      get: t => t.symbol },
  { key: 'quantity',    label: 'Qty',         get: t => t.quantity, num: true },
  { key: 'trade_price', label: 'Price',       get: t => t.trade_price, num: true },
  { key: 'amount',      label: 'Amount',      get: t => t.activity_type === 'TRADE' ? (t.net_cash ?? t.amount ?? 0) : (t.amount || 0), num: true },
  { key: 'currency',    label: 'Cur',         get: t => t.currency },
  { key: 'buy_sell',    label: 'B/S',         get: t => t.buy_sell },
  { key: 'commission',  label: 'Comm',        get: t => t.commission, num: true },
  { key: 'description', label: 'Description', get: t => t.description },
];
const TXN_COL_MAP = new Map(TXN_COLUMNS.map(c => [c.key, c]));
if (!state._txnFilters) state._txnFilters = {};

// TRANSACTIONS — scrollable, with filter summary
// ================================================================
async function loadTransactions() {
  try {
    const data = await fetchJson('/api/portfolio/transactions?limit=10000');
    state.transactions = data;
    $('#pf-txn-count').textContent = `${data.length} transactions`;
    renderTransactionsTable();
  } catch (e) {
    $('#pf-transactions-table').innerHTML = `<span class="status-text error">${e.message}</span>`;
  }
}

// ── Transaction column filter (client-side, runs after render) ──
function _applyTxnFilters(rows) {
  const f = state._txnFilters;
  if (!f || !Object.keys(f).length) return rows;
  return rows.filter(row => {
    for (const [key, filt] of Object.entries(f)) {
      const col = TXN_COL_MAP.get(key); if (!col) continue;
      const raw = col.get(row);
      if (filt.type === 'date') {
        const d = String(raw ?? '');
        if (filt.from && d < filt.from) return false;
        if (filt.to && d > filt.to) return false;
      } else if (filt.type === 'text') {
        const s = String(raw ?? '');
        if ((raw == null || s === '' || s === 'null' || s === 'undefined')) {
          if (filt.includeNulls === false) return false;
          continue;
        }
        if (filt.values && filt.values.size === 0 && !filt.wildcard) return false;
        if (filt.values && filt.values.size > 0 && !filt.values.has(s)) return false;
        if (filt.wildcard) {
          const w = filt.wildcard, m = filt.wcMode || 'contains';
          let ok = false;
          if (m === 'contains') ok = s.toLowerCase().includes(w.toLowerCase());
          else if (m === 'start') ok = s.toLowerCase().startsWith(w.toLowerCase());
          else if (m === 'end') ok = s.toLowerCase().endsWith(w.toLowerCase());
          else if (m === 'regex') { try { ok = new RegExp(w, 'i').test(s); } catch(_) { ok = true; } }
          if (!ok) return false;
        }
      } else if (filt.type === 'num') {
        const n = parseFloat(raw); if (isNaN(n)) return false;
        const conds = filt.conditions || [];
        for (let ci = 0; ci < conds.length; ci++) {
          const c = conds[ci];
          if (c.val === null || c.val === '') continue;
          const v = parseFloat(c.val), op = c.op || 'gte';
          if (op === 'eq' && n !== v) return false;
          if (op === 'gte' && n < v) return false;
          if (op === 'gt' && n <= v) return false;
          if (op === 'lte' && n > v) return false;
          if (op === 'lt' && n >= v) return false;
        }
      }
    }
    return true;
  });
}

function _openTxnFilter(colKey, anchorEl) {
  _closeTxnPopup();
  const col = TXN_COL_MAP.get(colKey); if (!col) return;
  const existing = state._txnFilters[colKey] || {};
  const popup = document.createElement('div');
  popup.className = 'pf-filter-popup';
  popup.id = 'pf-txn-filter-popup';

  if (col.date) {
    // ── Date-range filter ──
    popup.innerHTML = '<div style="font-weight:600;margin-bottom:6px;">Filter: ' + col.label + '</div>' +
      '<div class="pf-filter-body">' +
      '<div class="pf-filter-row" style="flex-shrink:0;"><label style="font-size:10px;color:var(--muted);width:30px;">From</label>' +
      '<input type="date" id="dt-filt-date-from" value="' + (existing.from || '') + '" style="flex:1;padding:3px 6px;font-size:11px;background:var(--bg);color:var(--text);border:1px solid var(--line);border-radius:3px;" /></div>' +
      '<div class="pf-filter-row" style="flex-shrink:0;"><label style="font-size:10px;color:var(--muted);width:30px;">To</label>' +
      '<input type="date" id="dt-filt-date-to" value="' + (existing.to || '') + '" style="flex:1;padding:3px 6px;font-size:11px;background:var(--bg);color:var(--text);border:1px solid var(--line);border-radius:3px;" /></div>' +
      '</div>' +
      '<div class="pf-filter-actions">' +
        '<button class="btn-ghost btn-sm" id="dt-filt-apply" style="flex:1;">Apply</button>' +
        '<button class="btn-ghost btn-sm" id="dt-filt-clear" style="flex:1;">Clear</button>' +
      '</div>';
    document.body.appendChild(popup);
    const pos = anchorEl.getBoundingClientRect();
    popup.style.left = Math.min(pos.left, window.innerWidth - 300) + 'px';
    popup.style.top = (pos.bottom + 4) + 'px';

    popup.querySelector('#dt-filt-apply').addEventListener('click', function() {
      const from = popup.querySelector('#dt-filt-date-from').value;
      const to = popup.querySelector('#dt-filt-date-to').value;
      if (!from && !to) { delete state._txnFilters[colKey]; }
      else { state._txnFilters[colKey] = { type: 'date', from: from || null, to: to || null }; }
      renderTransactionsTable();
    });
    popup.querySelector('#dt-filt-clear').addEventListener('click', function() {
      delete state._txnFilters[colKey]; renderTransactionsTable();
    });
  } else if (col.num) {
    const conds = (existing.conditions && existing.conditions.length) ? existing.conditions : [{ op: 'gte', val: null }];
    function renderRows() {
      const r = popup.querySelector('#dt-filt-rows'); if (!r) return;
      let h = '';
      for (let i = 0; i < conds.length; i++) {
        const c = conds[i];
        h += '<div class="pf-filt-cond-row" style="display:flex;align-items:center;gap:4px;padding:2px 0;">' +
          '<select class="pf-filt-cond-op" data-idx="' + i + '" style="padding:3px 4px;font-size:10px;background:var(--bg);color:var(--text);border:1px solid var(--line);border-radius:3px;">' +
            '<option value="eq"' + (c.op==='eq'?' selected':'') + '>=</option><option value="gte"' + (c.op==='gte'?' selected':'') + '>≥</option>' +
            '<option value="gt"' + (c.op==='gt'?' selected':'') + '>&gt;</option><option value="lte"' + (c.op==='lte'?' selected':'') + '>≤</option>' +
            '<option value="lt"' + (c.op==='lt'?' selected':'') + '>&lt;</option>' +
          '</select>' +
          '<input type="number" class="pf-filt-cond-val" data-idx="' + i + '" value="' + (c.val != null ? c.val : '') + '" placeholder="Value" step="any" style="flex:1;min-width:60px;padding:3px 6px;font-size:11px;background:var(--bg);color:var(--text);border:1px solid var(--line);border-radius:3px;" />' +
          '<button class="pf-filt-cond-rm" data-idx="' + i + '" style="background:none;border:none;cursor:pointer;color:var(--muted);font-size:14px;padding:0 4px;">×</button>' +
        '</div>';
      }
      r.innerHTML = h;
    }
    popup.innerHTML = '<div style="font-weight:600;margin-bottom:6px;">Filter: ' + col.label + '</div>' +
      '<div class="pf-filter-body"><div id="dt-filt-rows"></div>' +
      '<button class="btn-ghost btn-sm" id="dt-filt-add" style="font-size:10px;margin-top:4px;">+ Add condition</button></div>' +
      '<div class="pf-filter-actions"><button class="btn-ghost btn-sm" id="dt-filt-apply" style="flex:1;">Apply</button><button class="btn-ghost btn-sm" id="dt-filt-clear" style="flex:1;">Clear</button></div>';
    document.body.appendChild(popup);
    const pos = anchorEl.getBoundingClientRect();
    popup.style.left = Math.min(pos.left, window.innerWidth - 300) + 'px';
    popup.style.top = (pos.bottom + 4) + 'px';
    renderRows();

    popup.addEventListener('change', function(e) { if (e.target.classList.contains('pf-filt-cond-op')) conds[+e.target.dataset.idx].op = e.target.value; });
    popup.addEventListener('input', function(e) { if (e.target.classList.contains('pf-filt-cond-val')) conds[+e.target.dataset.idx].val = e.target.value; });
    popup.addEventListener('click', function(e) { if (e.target.classList.contains('pf-filt-cond-rm')) { e.stopPropagation(); conds.splice(+e.target.dataset.idx, 1); renderRows(); } });
    popup.querySelector('#dt-filt-add').addEventListener('click', function() { conds.push({ op: 'gte', val: null }); renderRows(); });
    popup.querySelector('#dt-filt-apply').addEventListener('click', function() {
      const rows = popup.querySelectorAll('.pf-filt-cond-row');
      for (let i = 0; i < rows.length; i++) { conds[i].op = rows[i].querySelector('.pf-filt-cond-op').value; conds[i].val = rows[i].querySelector('.pf-filt-cond-val').value; }
      const active = conds.filter(c => c.val !== null && c.val !== '');
      if (active.length === 0) delete state._txnFilters[colKey]; else state._txnFilters[colKey] = { type: 'num', conditions: active };
      renderTransactionsTable();
    });
    popup.querySelector('#dt-filt-clear').addEventListener('click', function() { delete state._txnFilters[colKey]; renderTransactionsTable(); });
  } else {
    const allVals = new Set(); let hasNulls = false;
    for (const t of state.transactions) { const v = col.get(t); if (v != null && v !== '') allVals.add(String(v)); else hasNulls = true; }
    const sorted = [...allVals].sort();
    const selected = existing.values || allVals;
    const includeNulls = existing.hasOwnProperty('includeNulls') ? existing.includeNulls : true;
    let nullsHtml = '';
    if (hasNulls) nullsHtml = '<label class="pf-check-row" style="border-bottom:1px solid var(--line);margin-bottom:2px;padding-bottom:4px;"><input type="checkbox" id="dt-filt-nulls" ' + (includeNulls ? 'checked' : '') + ' /><span style="color:var(--muted);font-style:italic;">— (no data)</span></label>';

    popup.innerHTML = '<div style="font-weight:600;margin-bottom:6px;">Filter: ' + col.label + '</div>' +
      '<div class="pf-filter-body">' +
      '<input type="text" id="dt-filt-search" placeholder="Search…" style="width:100%;box-sizing:border-box;margin-bottom:4px;padding:4px 6px;font-size:11px;background:var(--bg);color:var(--text);border:1px solid var(--line);border-radius:3px;" />' +
      '<div class="pf-filter-row" style="margin-bottom:4px;"><input type="text" id="dt-filt-wc" placeholder="Wildcard e.g. *.T" value="' + (existing.wildcard || '') + '" /><select id="dt-filt-wcm" style="width:60px;"><option value="contains">has</option><option value="start">→</option><option value="end">←</option><option value="regex">~</option></select></div>' +
      '<div class="pf-filter-values" id="dt-filt-vals">' + nullsHtml +
        sorted.map(function(v) { return '<label class="pf-check-row"><input type="checkbox" data-val="' + v.replace(/"/g,'&quot;') + '" ' + (selected.has(v)?'checked':'') + ' /><span>' + v + '</span></label>'; }).join('') +
      '</div></div>' +
      '<div class="pf-filter-actions"><button class="btn-ghost btn-sm" id="dt-filt-apply" style="flex:1;">Apply</button><button class="btn-ghost btn-sm" id="dt-filt-all" style="flex:1;font-size:10px;">All</button><button class="btn-ghost btn-sm" id="dt-filt-none" style="flex:1;font-size:10px;">None</button></div>';
    document.body.appendChild(popup);
    if (existing.wcMode) popup.querySelector('#dt-filt-wcm').value = existing.wcMode;
    const pos = anchorEl.getBoundingClientRect();
    popup.style.left = Math.min(pos.left, window.innerWidth - 300) + 'px';
    popup.style.top = (pos.bottom + 4) + 'px';

    const apply = function() {
      const sel = new Set();
      popup.querySelectorAll('#dt-filt-vals input[type=checkbox]:checked').forEach(cb => { if (cb.id !== 'dt-filt-nulls') sel.add(cb.dataset.val); });
      const ncb = popup.querySelector('#dt-filt-nulls'); const incNulls = ncb ? ncb.checked : true;
      const wc = popup.querySelector('#dt-filt-wc')?.value || ''; const wcm = popup.querySelector('#dt-filt-wcm')?.value || 'contains';
      if (sel.size === 0 && !wc && !incNulls) state._txnFilters[colKey] = { type:'text', values: sel, includeNulls: false };
      else if (sel.size === allVals.size && !wc && incNulls) delete state._txnFilters[colKey];
      else state._txnFilters[colKey] = { type:'text', values: sel, wildcard: wc || null, wcMode: wcm, includeNulls: incNulls };
      renderTransactionsTable();
    };
    popup.querySelector('#dt-filt-apply').addEventListener('click', apply);
    popup.querySelector('#dt-filt-all').addEventListener('click', function() { popup.querySelectorAll('#dt-filt-vals input[type=checkbox]').forEach(cb => cb.checked = true); });
    popup.querySelector('#dt-filt-none').addEventListener('click', function() { popup.querySelectorAll('#dt-filt-vals input[type=checkbox]').forEach(cb => cb.checked = false); });
    popup.querySelector('#dt-filt-search').addEventListener('input', function() {
      const q = popup.querySelector('#dt-filt-search').value.toLowerCase();
      popup.querySelectorAll('#dt-filt-vals .pf-check-row').forEach(row => { row.style.display = (row.querySelector('span')?.textContent || '').toLowerCase().includes(q) ? '' : 'none'; });
    });
  }
}

function _closeTxnPopup() {
  const p = document.getElementById('pf-txn-filter-popup');
  if (p) p.remove();
}
document.addEventListener('click', function(e) {
  const p = document.getElementById('pf-txn-filter-popup');
  if (p && !p.contains(e.target) && !e.target.closest('[data-txn-sort]')) p.remove();
});

function renderTransactionsTable() {
  const div = $('#pf-transactions-table');
  let txns = _applyTxnFilters(state.transactions);
  if (!txns.length) {
    div.innerHTML = '<span class="muted">No transactions match filters.</span>';
    return;
  }
  let h = '<table class="data-table" style="width:100%;" id="pf-txn-tbl"><thead><tr>' +
    '<th data-txn-sort="trade_date">Date</th><th data-txn-sort="activity_type">Type</th><th data-txn-sort="symbol">Symbol</th><th data-txn-sort="quantity">Qty</th>' +
    '<th data-txn-sort="trade_price">Price</th><th data-txn-sort="amount">Amount</th><th data-txn-sort="currency">Cur</th><th data-txn-sort="buy_sell">B/S</th>' +
    '<th data-txn-sort="commission">Comm</th><th data-txn-sort="description">Description</th></tr></thead><tbody>';
  for (const t of txns) {
    const amt = t.activity_type === 'TRADE'
      ? (t.net_cash != null ? t.net_cash : (t.amount || 0))
      : (t.amount || 0);
    const isNeg = (typeof amt === 'number' && amt < 0);
    const desc = (t.description || '—');
    const descShort = desc.length > 50 ? desc.substring(0, 47) + '…' : desc;
    h += '<tr>' +
      '<td style="white-space:nowrap;">' + (t.trade_date || '—') + '</td>' +
      '<td><span class="badge ' + badgeClass(t.activity_type) + '" style="font-size:10px;">' + (t.activity_type || '').replace('_', ' ') + '</span></td>' +
      '<td><strong>' + (t.symbol || '—') + '</strong></td>' +
      '<td>' + (t.quantity != null ? t.quantity : '—') + '</td>' +
      '<td>' + (t.trade_price != null ? formatMoney(t.trade_price) : '—') + '</td>' +
      '<td style="color:' + (isNeg ? 'var(--danger)' : 'var(--success)') + ';">' + formatMoney(amt) + '</td>' +
      '<td>' + (t.currency || '') + '</td>' +
      '<td>' + (t.buy_sell || '') + '</td>' +
      '<td>' + (t.commission != null ? formatMoney(t.commission) : '') + '</td>' +
      '<td title="' + desc.replace(/"/g, '&quot;') + '" style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + descShort + '</td>' +
    '</tr>';
  }
  h += '</tbody></table>';
  div.innerHTML = h;

  // Wire header right-clicks for filter + indicator
  const headers = div.querySelectorAll('th[data-txn-sort]');
  headers.forEach(th => {
    th.addEventListener('contextmenu', e => {
      e.preventDefault();
      _openTxnFilter(th.dataset.txnSort, th);
    });
    // Filter indicator
    const key = th.dataset.txnSort;
    let icon = th.querySelector('.pf-filter-indicator');
    if (state._txnFilters && state._txnFilters[key]) {
      if (!icon) {
        icon = document.createElement('span');
        icon.className = 'pf-filter-indicator';
        icon.textContent = ' ⏏';
        icon.style.cssText = 'color:var(--accent,#58a6ff);font-size:10px;';
        th.appendChild(icon);
      }
    } else if (icon) {
      icon.remove();
    }
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

// ================================================================
// CHART CONTROLS — unified settings applied to ALL charts
// ================================================================
function wireChartControls() {
  const updateBtn = $('#pf-chart-update');
  const benchInput = $('#pf-chart-benchmark');
  const currencySel = $('#pf-chart-currency');
  const inflationCb = $('#pf-chart-inflation');

  const applySettings = () => {
    state.chartSettings.currency = currencySel.value;
    state.chartSettings.benchmark = benchInput.value.trim();
    state.chartSettings.inflation = inflationCb.checked;
    refreshAllCharts();
  };

  updateBtn.addEventListener('click', applySettings);

  // Benchmark quick-select buttons
  $$('#pf-chart-bench-shortcuts .btn-ghost').forEach(btn => {
    btn.addEventListener('click', () => {
      benchInput.value = btn.dataset.bench;
      applySettings();
    });
  });

  // Currency change triggers full refresh
  currencySel.addEventListener('change', applySettings);

  // Inflation toggle triggers full refresh
  inflationCb.addEventListener('change', applySettings);

  // Portfolio Value breakdown toggle (no full refresh needed)
  const breakdownCb = $('#pf-value-breakdown');
  if (breakdownCb) {
    breakdownCb.addEventListener('change', () => renderValueChart());
  }

  // Dividends period selector
  const divPeriod = $('#pf-div-period');
  if (divPeriod) {
    divPeriod.addEventListener('change', () => renderDividendsChart());
  }

  // Dividend Growth YoY toggle: aggregate vs per-company
  const divGrowthBtn = $('#pf-div-growth-show-dps');
  if (divGrowthBtn) {
    divGrowthBtn.addEventListener('click', () => {
      _divGrowthMode = _divGrowthMode === 'per-company' ? 'agg' : 'per-company';
      divGrowthBtn.textContent = _divGrowthMode === 'per-company' ? 'Aggregate' : 'Per Company';
      if (_divGrowthMode === 'per-company') {
        renderDpsPerCompanyChart();
      } else {
        renderDividendGrowthChart();
      }
    });
  }

  // Returns by Company decompose toggle
  const decomposeCb = $('#pf-returns-decompose');
  if (decomposeCb) {
    decomposeCb.addEventListener('change', () => {
      if (_returnsByCompanyData) _drawReturnsByCompanyChart(_returnsByCompanyData);
    });
  }

  // Returns by Company year selector
  const yearSel = $('#pf-returns-year');
  if (yearSel) {
    yearSel.addEventListener('change', () => {
      if (_returnsByCompanyData) _drawReturnsByCompanyChart(_returnsByCompanyData);
    });
  }

  // Money-Weighted Returns year selector
  const mwYearSel = $('#pf-mw-returns-year');
  if (mwYearSel) {
    mwYearSel.addEventListener('change', () => {
      if (_mwReturnsData) _drawMWReturnsChart(_mwReturnsData);
    });
  }

  // Contribution year selector
  const contribYearSel = $('#pf-contrib-year');
  if (contribYearSel) {
    contribYearSel.addEventListener('change', () => {
      if (_contribData) _drawContributionChart(_contribData);
    });
  }
}

async function refreshAllCharts() {
  const updateBtn = $('#pf-chart-update');
  updateBtn.disabled = true;
  updateBtn.textContent = 'Applying…';

  // Show loading on all chart canvases
  const chartIds = [
    'pf-equity-chart','pf-value-chart','pf-dividends-chart','pf-allocation-chart',
    'pf-div-growth-chart','pf-currency-split-chart','pf-returns-by-company-chart',
    'pf-closed-returns-chart','pf-mw-returns-chart','pf-contrib-chart',
  ];
  chartIds.forEach(id => showChartLoading(id));

  try {
    const cp = state.chartSettings;
    const qs = new URLSearchParams({ base_currency: cp.currency });
    if (cp.benchmark) qs.set('benchmark_ticker', cp.benchmark);
    state.chartData = await fetchJson('/api/portfolio/performance?' + qs.toString());
  } catch (e) {
    log('warn', 'Chart data fetch error: ' + (e && e.message || e));
    state.chartData = null;
  } finally {
    updateBtn.disabled = false;
    updateBtn.textContent = 'Apply Settings';
  }
  // Re-render all charts with new settings
  renderEquityChart();
  renderValueChart();
  renderDividendsChart();
  renderAllocationChart();
  renderDividendGrowthChart();
  renderReturnsByCompanyChart();
  renderCurrencySplitChart();
  renderClosedReturnsChart();
  renderMoneyWeightedReturnsChart();
  renderContributionChart();
}

function renderEquityChart() {
  const cd = state.chartData;
  const cp = state.chartSettings;

  if (cd && cd.series) state.chartRawData.equity = cd;

  // Use portfolio series from performance endpoint (single source of truth
  // for portfolio, benchmark, and inflation — all in the same currency).
  if (!cd || !cd.series || !cd.series.length) {
    // Fallback: fetch from holdings/history if chartData not yet loaded
    fetchJson('/api/portfolio/holdings/history?base_currency=' + cp.currency).then(daily => {
      if (!daily.length) return;
      state.chartRawData.equity = { series: daily.map(d => ({ date: d.date, cumulative_return: d.cumulative_return || 0, total_value: d.total_value })) };
      _drawEquityChartFromDaily(daily, cd, cp);
    });
    return;
  }

  const ctx = $('#pf-equity-chart');
  destroyChart('equity');

  const labels = cd.series.map(pt => pt.date);
  const datasets = [{
    label: 'Portfolio',
    data: cd.series.map(pt => (pt.cumulative_return || 0) * 100),
    borderColor: '#58a6ff',
    backgroundColor: 'rgba(88,166,255,0.08)',
    fill: true, tension: 0.2, pointRadius: 0, yAxisID: 'y',
    borderWidth: 2,
  }];

  // Benchmark line — from same API response, same currency
  if (cd.benchmark && cd.benchmark.series && cd.benchmark.series.length) {
    const benchMap = {};
    for (const pt of cd.benchmark.series) benchMap[pt.date] = pt.cumulative_return;
    datasets.push({
      label: cd.benchmark.ticker || 'Benchmark',
      data: labels.map(d => { const v = benchMap[d]; return v != null ? v * 100 : null; }),
      borderColor: '#e0af4f',
      borderDash: [2, 2],
      tension: 0.2, pointRadius: 0, yAxisID: 'y',
      borderWidth: 1.5, spanGaps: true,
    });
  }

  // Inflation line — from same API response, same currency
  if (cp.inflation && cd.inflation_series && cd.inflation_series.length) {
    const infMap = {};
    for (const pt of cd.inflation_series) infMap[pt.date] = pt.cumulative;
    datasets.push({
      label: 'Inflation (' + cp.currency + ')',
      data: labels.map(d => { const v = infMap[d]; return v != null ? v * 100 : null; }),
      borderColor: '#ff6b6b',
      borderDash: [8, 4],
      tension: 0.2, pointRadius: 0, yAxisID: 'y',
      borderWidth: 1, spanGaps: true,
    });
  }

  hideChartLoading('pf-equity-chart');
  state.charts.equity = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets },
    options: _zoomableLineOptions({
      yLabel: 'Cumulative Return',
      yCallback: v => v.toFixed(1) + '%',
    }),
  });
}

// Fallback for when chartData hasn't been loaded yet (first tab visit)
function _drawEquityChartFromDaily(daily, cd, cp) {
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
  if (cd && cd.benchmark && cd.benchmark.series && cd.benchmark.series.length) {
    const benchMap = {};
    for (const pt of cd.benchmark.series) benchMap[pt.date] = pt.cumulative_return;
    datasets.push({
      label: cd.benchmark.ticker || 'Benchmark',
      data: daily.map(d => { const v = benchMap[d.date]; return v != null ? v * 100 : null; }),
      borderColor: '#e0af4f', borderDash: [2, 2],
      tension: 0.2, pointRadius: 0, yAxisID: 'y', borderWidth: 1.5, spanGaps: true,
    });
  }
  if (cp.inflation && cd && cd.inflation_series && cd.inflation_series.length) {
    const infMap = {};
    for (const pt of cd.inflation_series) infMap[pt.date] = pt.cumulative;
    datasets.push({
      label: 'Inflation (' + cp.currency + ')',
      data: daily.map(d => { const v = infMap[d.date]; return v != null ? v * 100 : null; }),
      borderColor: '#ff6b6b', borderDash: [8, 4],
      tension: 0.2, pointRadius: 0, yAxisID: 'y', borderWidth: 1, spanGaps: true,
    });
  }
  hideChartLoading('pf-equity-chart');
  state.charts.equity = new Chart(ctx, {
    type: 'line',
    data: { labels: daily.map(d => d.date), datasets },
    options: _zoomableLineOptions({
      yLabel: 'Cumulative Return',
      yCallback: v => v.toFixed(1) + '%',
    }),
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
// Chart maximize: click a chart body to expand to full width
// ================================================================
function wireChartExpand() {
  const chartsTab = document.querySelector('[data-panel="charts"]');
  if (!chartsTab) return;

  chartsTab.addEventListener('click', (e) => {
    const body = e.target.closest('.panel-body');
    if (!body) return;
    const panel = body.closest('.panel');
    if (!panel) return;

    // Ignore clicks on interactive elements inside panel-body
    if (e.target.closest('select, input, button, label, .btn-ghost, .btn-primary')) return;

    if (panel.classList.contains('chart-expanded')) {
      panel.classList.remove('chart-expanded');
    } else {
      const other = chartsTab.querySelector('.chart-expanded');
      if (other) other.classList.remove('chart-expanded');
      panel.classList.add('chart-expanded');
    }

    // Resize the chart after CSS layout settles.  Use two-phase
    // resize: once on next animation frame (fast path) and a
    // second time after a short delay to handle CSS transitions.
    const resizeCanvas = () => {
      const canvas = panel.querySelector('canvas');
      if (!canvas) return;
      for (const key of Object.keys(state.charts)) {
        const ch = state.charts[key];
        if (ch && ch.canvas === canvas) { ch.resize(); break; }
      }
    };
    requestAnimationFrame(resizeCanvas);
    setTimeout(resizeCanvas, 150);
  });

  // Escape key to close expanded chart
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      const expanded = chartsTab.querySelector('.chart-expanded');
      if (expanded) {
        expanded.classList.remove('chart-expanded');
        // Resize chart back to original dimensions
        const canvas = expanded.querySelector('canvas');
        if (canvas) {
          for (const key of Object.keys(state.charts)) {
            const ch = state.charts[key];
            if (ch && ch.canvas === canvas) {
              requestAnimationFrame(() => ch.resize());
              setTimeout(() => ch.resize(), 150);
              break;
            }
          }
        }
      }
    }
  });
}

// ================================================================
// Table view toggle — show raw chart data as a sortable table
// ================================================================
const _CHART_KEY_MAP = {
  'pf-equity-chart':     'equity',
  'pf-value-chart':      'value',
  'pf-dividends-chart':  'dividends',
  'pf-allocation-chart': 'allocation',
  'pf-div-growth-chart': 'divGrowth',
  'pf-currency-split-chart': 'currencySplit',
  'pf-returns-by-company-chart': 'returnsByCompany',
  'pf-closed-returns-chart': 'closedReturns',
  'pf-mw-returns-chart': 'mwReturns',
  'pf-contrib-chart':    'contribution',
};

function wireTableViewToggles() {
  const chartsTab = document.querySelector('[data-panel="charts"]');
  if (!chartsTab) return;

  // Inject toggle buttons into every chart panel's header
  const panels = chartsTab.querySelectorAll('.panel');
  for (const panel of panels) {
    const canvas = panel.querySelector('canvas');
    if (!canvas) continue;
    const chartKey = _CHART_KEY_MAP[canvas.id];
    if (!chartKey) continue;

    let head = panel.querySelector('.panel-head');
    if (!head) {
      head = document.createElement('div');
      head.className = 'panel-head';
      panel.insertBefore(head, panel.firstChild);
    }
    // Avoid duplicate buttons
    if (head.querySelector('.pf-tbl-toggle')) continue;

    const btn = document.createElement('button');
    btn.className = 'btn-ghost btn-sm pf-tbl-toggle';
    btn.textContent = '📋 Table';
    btn.title = 'Toggle table view';
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      _toggleTableView(panel, chartKey);
    });
    head.appendChild(btn);
  }
}

function _toggleTableView(panel, chartKey) {
  const currentMode = state.chartViewMode[chartKey] || 'chart';
  const canvas = panel.querySelector('canvas');
  const body = panel.querySelector('.panel-body');
  let tableWrap = panel.querySelector('.pf-table-wrap');

  if (currentMode === 'chart') {
    // Switch to table view
    state.chartViewMode[chartKey] = 'table';
    if (canvas) canvas.style.display = 'none';
    panel.querySelector('.pf-tbl-toggle').textContent = '📊 Chart';

    if (!tableWrap) {
      tableWrap = document.createElement('div');
      tableWrap.className = 'pf-table-wrap';
      tableWrap.style.cssText = 'overflow:auto;height:100%;';
      body.appendChild(tableWrap);
    }
    tableWrap.style.display = 'block';
    _renderTable(chartKey, tableWrap);
  } else {
    // Switch back to chart — remove table, restore canvas
    state.chartViewMode[chartKey] = 'chart';
    if (tableWrap) tableWrap.remove();
    if (canvas) {
      canvas.style.display = 'block';
      canvas.style.width = '';
      canvas.style.height = '';
    }
    panel.querySelector('.pf-tbl-toggle').textContent = '📋 Table';

    // Resize chart after restoring canvas to fill container
    for (const k of Object.keys(state.charts)) {
      const ch = state.charts[k];
      if (ch && ch.canvas === canvas) {
        requestAnimationFrame(() => ch.resize());
        setTimeout(() => ch.resize(), 100);
        break;
      }
    }
  }
}

function _renderTable(chartKey, container) {
  const raw = state.chartRawData[chartKey];
  if (!raw) {
    container.innerHTML = '<div class="muted" style="padding:20px;text-align:center;">No data loaded yet. Switch to chart view and ensure data is loaded.</div>';
    return;
  }

  let columns = [];
  let rows = [];

  switch (chartKey) {
    case 'equity': {
      // raw = { series: [{date, cumulative_return, total_value}], benchmark, inflation_series }
      const cd = state.chartData;
      columns = ['Date', 'Portfolio %', 'Total Value'];
      if (cd?.benchmark?.series?.length) columns.push('Benchmark %');
      if (state.chartSettings.inflation && cd?.inflation_series?.length) columns.push('Inflation %');

      const benchMap = {};
      if (cd?.benchmark?.series) for (const p of cd.benchmark.series) benchMap[p.date] = p.cumulative_return;
      const infMap = {};
      if (cd?.inflation_series) for (const p of cd.inflation_series) infMap[p.date] = p.cumulative;

      rows = raw.series.map(pt => {
        const r = [pt.date, (pt.cumulative_return * 100).toFixed(2), pt.total_value?.toFixed(2)];
        if (columns.includes('Benchmark %')) r.push(benchMap[pt.date] != null ? (benchMap[pt.date] * 100).toFixed(2) : '');
        if (columns.includes('Inflation %')) r.push(infMap[pt.date] != null ? (infMap[pt.date] * 100).toFixed(2) : '');
        return r;
      });
      break;
    }
    case 'value': {
      // raw = array of {date, total_value, cash_balance, ...}
      columns = ['Date', 'Total Value', 'Cash', 'Stock Value', 'Option Value', 'Dividend', 'Net Inflow', 'Daily Return %', 'Cum. Return %'];
      rows = raw.map(d => [
        d.date, d.total_value?.toFixed(2), d.cash_balance?.toFixed(2),
        d.stock_value?.toFixed(2), d.option_value?.toFixed(2),
        d.dividend_income?.toFixed(2), d.net_inflow?.toFixed(2),
        d.daily_return != null ? (d.daily_return * 100).toFixed(4) : '',
        d.cumulative_return != null ? (d.cumulative_return * 100).toFixed(2) : '',
      ]);
      break;
    }
    case 'dividends': {
      // raw = array of {period, gross, tax, net}
      columns = ['Period', 'Gross', 'Tax', 'Net'];
      rows = raw.map(d => [d.period, d.gross?.toFixed(2), d.tax?.toFixed(2), d.net?.toFixed(2)]);
      break;
    }
    case 'allocation': {
      // raw = { labels: [...], values: [...] }
      const total = raw.values.reduce((s, v) => s + v, 0);
      columns = ['Symbol', 'Market Value', '% of Portfolio'];
      rows = raw.labels.map((sym, i) => [sym, raw.values[i]?.toFixed(2), total > 0 ? ((raw.values[i] / total) * 100).toFixed(1) : '0.0']);
      break;
    }
    case 'divGrowth': {
      // raw = { years, dividends, yoy_growth, currency }  OR
      // raw = { years, companies: { sym: { dps, yoy_growth, currency } } }
      if (raw.companies) {
        // Per-company view
        columns = ['Company', 'Currency'];
        raw.years.forEach(y => columns.push('DPS ' + y, 'YoY ' + y));
        rows = Object.entries(raw.companies).map(([sym, c]) => {
          const r = [sym, c.currency || ''];
          raw.years.forEach((y, i) => {
            r.push(c.dps[i] != null ? c.dps[i].toFixed(4) : '—');
            r.push(c.yoy_growth[i] != null ? c.yoy_growth[i].toFixed(1) + '%' : '—');
          });
          return r;
        });
      } else {
        // Aggregate view
        columns = ['Year', 'Dividends (' + (raw.currency || 'EUR') + ')', 'YoY Growth %'];
        rows = raw.years.map((y, i) => [
          y, raw.dividends[i]?.toFixed(2),
          raw.yoy_growth[i] != null ? raw.yoy_growth[i].toFixed(1) : '—',
        ]);
      }
      break;
    }
    case 'currencySplit': {
      // raw = { labels: [...], values: [...], total }
      columns = ['Currency', 'Native Value', '% of Total'];
      rows = raw.labels.map((ccy, i) => [
        ccy, raw.values[i]?.toFixed(2), raw.total > 0 ? ((raw.values[i] / raw.total) * 100).toFixed(1) : '0.0',
      ]);
      break;
    }
    case 'returnsByCompany': {
      // raw = { years, companies: { sym: { total_return, capital_gain, dividend_return } } }
      const year = $('#pf-returns-year')?.value || 'all';
      columns = ['Symbol'];
      if (year === 'all') {
        columns.push('Cumulative Total %');
        rows = Object.entries(raw.companies).map(([sym, c]) => [sym, c._total_all_years?.toFixed(1)]);
      } else {
        columns.push('Total Return %', 'Capital Gain %', 'Dividend Return %');
        const yi = raw.years.indexOf(parseInt(year));
        rows = Object.entries(raw.companies)
          .filter(([, c]) => c.total_return[yi] != null)
          .map(([sym, c]) => [sym, c.total_return[yi]?.toFixed(1), c.capital_gain[yi]?.toFixed(1), c.dividend_return[yi]?.toFixed(1)]);
      }
      break;
    }
    case 'closedReturns': {
      // raw = array of {symbol, realized_pnl, total_cost, total_proceeds, currency, first_trade_date, last_trade_date}
      columns = ['Symbol', 'Realized P&L', 'Total Cost', 'Total Proceeds', 'Currency', 'First Trade', 'Last Trade'];
      rows = raw.map(c => [
        c.symbol, c.realized_pnl?.toFixed(2), c.total_cost?.toFixed(2),
        c.total_proceeds?.toFixed(2), c.currency || '', c.first_trade_date || '', c.last_trade_date || '',
      ]);
      break;
    }
    case 'mwReturns': {
      // raw = { years, companies: { sym: { return_pct, _total_return } } }
      const year = $('#pf-mw-returns-year')?.value || 'all';
      if (year === 'all') {
        columns = ['Symbol', 'Total Return % (full period)'];
        rows = Object.entries(raw.companies)
          .filter(([, c]) => c._total_return != null)
          .map(([sym, c]) => [sym, c._total_return?.toFixed(1)]);
      } else {
        const yi = raw.years.indexOf(parseInt(year));
        columns = ['Symbol', 'MW Return % (' + year + ')'];
        rows = Object.entries(raw.companies)
          .filter(([, c]) => c.return_pct[yi] != null)
          .map(([sym, c]) => [sym, c.return_pct[yi]?.toFixed(1)]);
      }
      break;
    }
    case 'contribution': {
      // raw = { years, companies: { sym: { contribution_eur, contribution_pct } }, portfolio_start }
      const year = $('#pf-contrib-year')?.value || 'all';
      columns = ['Symbol'];
      if (year === 'all') {
        columns.push('Total Contribution (EUR)', 'Total Contribution %');
      } else {
        columns.push('Contribution (' + year + ' EUR)', '% of Portfolio');
      }
      const yi = year === 'all' ? -1 : raw.years.indexOf(parseInt(year));
      rows = Object.entries(raw.companies)
        .filter(([, c]) => {
          if (yi >= 0) return c.contribution_eur[yi] != null;
          return c.contribution_eur.some(v => v != null);
        })
        .map(([sym, c]) => {
          if (yi >= 0) return [sym, c.contribution_eur[yi]?.toFixed(2), c.contribution_pct[yi]?.toFixed(1)];
          const tot = c.contribution_eur.reduce((s, v) => s + (v || 0), 0);
          const pct = c.contribution_pct.reduce((s, v) => s + (v || 0), 0);
          return [sym, tot.toFixed(2), pct.toFixed(1)];
        });
      break;
    }
    default:
      container.innerHTML = '<div class="muted" style="padding:20px;">Unknown chart type</div>';
      return;
  }

  // Build table
  let html = '<table class="data-table" style="font-size:11px;width:100%;"><thead><tr>';
  for (const col of columns) html += `<th>${col}</th>`;
  html += '</tr></thead><tbody>';
  for (const row of rows) {
    html += '<tr>';
    for (const cell of row) html += `<td>${cell ?? ''}</td>`;
    html += '</tr>';
  }
  html += '</tbody></table>';
  container.innerHTML = html;
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
