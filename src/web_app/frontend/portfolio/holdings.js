import { $, $$, fetchJson, state, normalizeTicker, formatMoney, formatPct, formatNum, badgeClass } from './common.js';

// HOLDINGS columns
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

function _col(key) {
  if (!state._colMap) state._colMap = new Map(HOLDINGS_COLUMNS.map(c => [c.key, c]));
  return state._colMap.get(key);
}
function _orderedCols() {
  if (!state.columnOrder) state.columnOrder = HOLDINGS_COLUMNS.map(c => c.key);
  return state.columnOrder.map(k => _col(k)).filter(Boolean);
}

export async function loadHoldings() {
  const div = $('#pf-holdings-table');
  if (div) div.innerHTML = '<div class="pf-table-loading"><div class="pf-chart-loading-spinner"></div><div class="pf-chart-loading-text">Loading holdings…</div></div>';
  const minDelay = new Promise(resolve => setTimeout(resolve, 300));
  try {
    const dc = state.displayCurrency || 'EUR';
    const fetchPromise = fetchJson(`/api/portfolio/holdings/performance?display_currency=${dc}&include_closed=true`);
    state.holdings = await Promise.all([fetchPromise, minDelay]).then(r => r[0]);
    // Set closed positions cache
    state.closedPositions = state.holdings.filter(h => h.is_open === false);
    if (!state.columnFilters['open_pos']) {
      state.columnFilters['open_pos'] = { type: 'text', values: new Set(['Open']), includeNulls: false };
    }
    renderHoldingsTab();
  } catch (e) {
    try {
      state.holdings = await fetchJson('/api/portfolio/holdings');
      state.closedPositions = state.holdings.filter(h => h.is_open === false);
      renderHoldingsTab();
    } catch (e2) {
      if ($('#pf-holdings-table')) $('#pf-holdings-table').innerHTML = `<span class="status-text error">${e2.message}</span>`;
    }
  }
}

export async function loadDisplayCurrencies() {
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
  } catch (_) { }
}

export function wireDisplayCurrency() {
  const sel = $('#pf-display-currency');
  if (!sel) return;
  sel.addEventListener('change', async () => {
    state.displayCurrency = sel.value;
    await loadHoldings();
  });
}

export function wireColumnVisibility() {
  const btn = $('#pf-columns-btn');
  const popover = $('#pf-columns-popover');
  const list = $('#pf-columns-list');
  const search = $('#pf-columns-search');
  const selAll = $('#pf-cols-select-all');
  const selNone = $('#pf-cols-select-none');
  if (!btn || !popover || !list) return;

  const allCols = _orderedCols();

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

    list.querySelectorAll('input[type=checkbox]').forEach(cb => {
      cb.addEventListener('change', () => {
        const key = cb.dataset.col;
        if (!state.visibleColumns) state.visibleColumns = new Set(allCols.map(c => c.key));
        if (cb.checked) state.visibleColumns.add(key); else state.visibleColumns.delete(key);
        applyColumnVisibility();
      });
    });

    list.querySelectorAll('.pf-pin-btn').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation(); e.preventDefault();
        const key = btn.dataset.pin;
        if (state.pinnedColumns.has(key)) state.pinnedColumns.delete(key); else state.pinnedColumns.add(key);
        applyColumnVisibility();
        renderColList(search ? search.value : '');
      });
    });
  }

  btn.addEventListener('click', (e) => {
    e.stopPropagation();
    if (popover.style.display === 'block') { popover.style.display = 'none'; return; }
    popover.style.display = 'block';
    if (search) search.value = '';
    renderColList('');
  });

  if (search) search.addEventListener('input', () => renderColList(search.value));

  if (selAll) {
    selAll.addEventListener('click', () => {
      state.visibleColumns = new Set(allCols.map(c => c.key));
      applyColumnVisibility();
      renderColList(search ? search.value : '');
    });
  }
  if (selNone) {
    selNone.addEventListener('click', () => {
      state.visibleColumns = new Set(['symbol', 'name', 'native_ccy']);
      applyColumnVisibility();
      renderColList(search ? search.value : '');
    });
  }

  document.addEventListener('click', (e) => {
    if (!popover.contains(e.target) && e.target !== btn) popover.style.display = 'none';
  });
}

export function applyColumnVisibility() {
  const visible = state.visibleColumns;
  const pinned = state.pinnedColumns;
  const allCols = _orderedCols();
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
  if (lastPinnedKey) {
    const lastTh = document.querySelector(`.pf-col-th-${lastPinnedKey}`);
    if (lastTh) lastTh.classList.add('pf-sticky-col-last');
  }

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

  requestAnimationFrame(() => {
    let offset = 0;
    for (const col of allCols) {
      const th = document.querySelector(`.pf-col-th-${col.key}`);
      if (!th || th.classList.contains('pf-col-hidden')) continue;
      if (!state.pinnedColumns.has(col.key)) continue;
      th.style.left = offset + 'px';
      offset += th.offsetWidth;
    }
    for (const row of document.querySelectorAll('#pf-holdings-tbl tbody tr, #pf-holdings-tbl tfoot tr')) {
      let tdOffset = 0;
      for (const col of allCols) {
        const td = row.querySelector(`.pf-col-td-${col.key}`);
        if (!td || td.classList.contains('pf-col-hidden')) continue;
        if (!state.pinnedColumns.has(col.key)) continue;
        td.style.left = tdOffset + 'px';
        tdOffset += td.offsetWidth;
      }
    }
  });
}

export function renderHoldingsTab() { renderHoldingsTable(); }

function renderHoldingsTable() {
  const div = $('#pf-holdings-table');
  const hh = state.holdings || [];
  const dc = state.displayCurrency || 'EUR';
  if (!hh.length) { if (div) div.innerHTML = '<span class="muted">No positions to show.</span>'; return; }

  const stocks = hh.filter(h => h.asset_category !== 'CASH' && !h.symbol.startsWith('CASH'));
  const openCount = hh.filter(h => h.is_open !== false && h.asset_category !== 'CASH' && !h.symbol.startsWith('CASH')).length;
  const closedCount = hh.filter(h => h.is_open === false).length;
  const titleEl = $('#pf-holdings-title'); if (titleEl) titleEl.textContent = 'Holdings';
  const countLabel = `${stocks.length} positions` + (closedCount > 0 ? ` (${closedCount} closed)` : '');
  const countEl = $('#pf-holdings-count'); if (countEl) countEl.textContent = countLabel;

  let totalDisplay = 0;
  for (const h of hh) { totalDisplay += Math.abs(h.performance?.current_value_display ?? h.market_value ?? 0); }
  state._totalDisplayValue = totalDisplay;

  const cols = _orderedCols();
  let sorted = _applyFilters([...hh]);
  if (state.sortColumn) {
    const colDef = cols.find(c => c.key === state.sortColumn);
    if (colDef) {
      sorted.sort((a, b) => {
        const va = colDef.get(a) ?? (colDef.num ? -Infinity : '');
        const vb = colDef.get(b) ?? (colDef.num ? -Infinity : '');
        if (colDef.num) return state.sortAsc ? va - vb : vb - va;
        const sa = String(va).toLowerCase(); const sb = String(vb).toLowerCase();
        return state.sortAsc ? sa.localeCompare(sb) : sb.localeCompare(sa);
      });
    }
  }

  let html = '<table class="data-table" id="pf-holdings-tbl"><thead><tr>';
  for (const col of cols) {
    const arrow = state.sortColumn === col.key ? (state.sortAsc ? ' ▲' : ' ▼') : '';
    html += `<th class="pf-sort-th pf-col-th-${col.key}" data-sort="${col.key}" style="cursor:pointer;user-select:none;white-space:nowrap;">${col.label}${arrow}</th>`;
  }
  html += '</tr></thead><tbody>';

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

    function _pct(v) { if (v == null || isNaN(v)) return '—'; return (v * 100).toFixed(1) + '%'; }
    function _color(val, pos, neg) { if (val == null) return ''; return val >= 0 ? `color:${pos || 'var(--success)'};` : `color:${neg || 'var(--danger)'};`; }

    const normalizedSym = normalizeTicker(h.symbol);
    const canLink = !isOption && !isCash;
    const symLabel = isClosed
      ? (canLink ? `<a class="pf-sym-link" href="/security?symbol=${encodeURIComponent(normalizedSym)}" onclick="event.stopPropagation();"><span style="${closedStyle}">${h.symbol}</span></a> <span class="pf-closed-badge">CLOSED</span>` : `<span style="${closedStyle}">${h.symbol}</span> <span class="pf-closed-badge">CLOSED</span>`)
      : `<a class="pf-sym-link" href="/security?symbol=${encodeURIComponent(normalizedSym)}" onclick="event.stopPropagation();"><strong style="${cashStyle}">${h.symbol}</strong></a>`;

    const mvNative = isClosed ? '—' : (h.market_value_native != null ? formatMoney(h.market_value_native) + ' ' + (isCash ? '' : ccy) : '—');
    const mvDisplay = isClosed ? '—' : (p.current_value_display != null ? formatMoney(p.current_value_display) + ' ' + dcDisp : (h.market_value != null ? formatMoney(h.market_value) : '—'));
    const costNat = isClosed ? '—' : (p.cost_basis_native != null ? formatMoney(p.cost_basis_native) : '—');
    const costDisp = isClosed ? '—' : (p.cost_basis_display != null ? formatMoney(p.cost_basis_display) : '—');
    const pnlNat = isClosed ? '—' : (p.pnl_native != null ? formatMoney(p.pnl_native) : '—');
    const pnlDisp = isClosed ? (p.realized_pnl != null ? formatMoney(p.realized_pnl) : '—') : (p.pnl_display != null ? formatMoney(p.pnl_display) : '—');
    const divNat = isClosed ? '—' : (p.dividends_native != null ? formatMoney(p.dividends_native) : '—');
    const divDisp = isClosed ? '—' : (p.dividends_display != null ? formatMoney(p.dividends_display) : (p.dividend_income != null ? formatMoney(p.dividend_income) : '—'));
    const retNat = isClosed ? '—' : (p.total_return_native != null ? _pct(p.total_return_native) : '—');
    const closedRet = isClosed && p.realized_pnl != null && p.total_cost ? (p.realized_pnl / p.total_cost) : null;
    const retDisp = isClosed ? (closedRet != null ? _pct(closedRet) : '—') : ((p.total_return_display ?? p.total_return) != null ? _pct(p.total_return_display ?? p.total_return) : '—');
    const fxEffect = isClosed ? '—' : (p.fx_return != null ? _pct(p.fx_return) : '—');
    const annNat = isClosed ? '—' : (p.annualized_return_native != null ? _pct(p.annualized_return_native) : '—');
    const annDisp = isClosed ? '—' : (p.annualized_return != null ? _pct(p.annualized_return) : '—');
    const wt = totalDisplay ? (Math.abs(p.current_value_display ?? h.market_value ?? 0) / Math.abs(totalDisplay) * 100).toFixed(1) + '%' : '—';

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

  // Summary row
  const summary = {};
  const sumCols = ['val_native','val_display','cost_native','cost_display','pnl_native','pnl_display','div_native','div_display'];
  const avgCols = ['pct_ret_nat','pct_ret','fx_effect','ann_ret_nat','ann_ret'];
  for (const k of sumCols) summary[k] = 0;
  for (const k of avgCols) { summary[k] = 0; summary[k + '_count'] = 0; }
  summary.weight = 0;
  for (const h of sorted) {
    const p = h.performance || {};
    for (const k of sumCols) { const col = cols.find(c => c.key === k); const v = col ? col.get(h) : 0; if (v != null && !isNaN(v)) summary[k] += v; }
    for (const k of avgCols) { const col = cols.find(c => c.key === k); const v = col ? col.get(h) : null; if (v != null && !isNaN(v)) { summary[k] += v; summary[k + '_count']++; } }
    const wcol = cols.find(c => c.key === 'weight'); const w = wcol ? wcol.get(h) : 0; if (w != null && !isNaN(w)) summary.weight += w;
  }
  const sVals = {};
  for (const k of sumCols) sVals[k] = formatMoney(summary[k]);
  for (const k of avgCols) { const cnt = summary[k + '_count']; sVals[k] = cnt > 0 ? (summary[k] / cnt * 100).toFixed(1) + '%' : '—'; }
  sVals['weight'] = summary.weight.toFixed(1) + '%';

  html += '<tfoot><tr class="pf-summary-row">';
  for (const col of cols) {
    const k = col.key;
    if (k === 'symbol') { html += '<td class="pf-col-td-symbol"><strong>Summary</strong></td>'; continue; }
    if (['name','asset_type','industry','longest_hold','latest_hold','num_holds','quantity','avg_cost','price'].includes(k)) { html += `<td class="pf-col-td-${k}"></td>`; continue; }
    const v = sVals[k] || '';
    let style = '';
    if (['pnl_native','pnl_display','pct_ret_nat','pct_ret','fx_effect','ann_ret_nat','ann_ret'].includes(k)) { const raw = summary[k]; if (raw != null) style = raw >= 0 ? 'color:var(--success);' : 'color:var(--danger);'; }
    if (k === 'div_native' || k === 'div_display') style = 'color:var(--success);';
    html += `<td class="pf-col-td-${k}" style="${style}">${v}</td>`;
  }
  html += '</tr></tfoot>';

  html += '</tbody></table>';
  if (div) div.innerHTML = html;

  applyColumnVisibility();
  _updateFilterIndicators();

  $$('.pf-sort-th').forEach(th => {
    th.addEventListener('click', (e) => {
      if (e.ctrlKey || e.metaKey) { e.preventDefault(); openColumnFilter(th.dataset.sort, th); return; }
      const key = th.dataset.sort;
      if (state.sortColumn === key) state.sortAsc = !state.sortAsc; else { state.sortColumn = key; state.sortAsc = true; }
      renderHoldingsTable();
    });
    th.addEventListener('contextmenu', (e) => { e.preventDefault(); openColumnFilter(th.dataset.sort, th); });
    th.draggable = true;
    th.addEventListener('dragstart', (e) => { e.dataTransfer.setData('text/plain', th.dataset.sort); e.dataTransfer.effectAllowed = 'move'; th.style.opacity = '0.4'; });
    th.addEventListener('dragend', (e) => { th.style.opacity = '1'; $$('.pf-sort-th').forEach(t => t.classList.remove('pf-drag-over')); });
    th.addEventListener('dragover', (e) => { e.preventDefault(); e.dataTransfer.dropEffect = 'move'; th.classList.add('pf-drag-over'); });
    th.addEventListener('dragleave', () => th.classList.remove('pf-drag-over'));
    th.addEventListener('drop', (e) => {
      e.preventDefault(); th.classList.remove('pf-drag-over'); const fromKey = e.dataTransfer.getData('text/plain'); const toKey = th.dataset.sort;
      if (fromKey && toKey && fromKey !== toKey) { const order = state.columnOrder; const fromIdx = order.indexOf(fromKey); const toIdx = order.indexOf(toKey); if (fromIdx >= 0 && toIdx >= 0) { order.splice(fromIdx, 1); order.splice(toIdx, 0, fromKey); renderHoldingsTable(); } }
    });
  });

  $$('.pf-holding-row[data-holding]').forEach(row => { row.addEventListener('click', () => toggleHoldingDetail(row)); });
}

function openColumnFilter(colKey, anchorEl) {
  closeFilterPopup();
  const col = _col(colKey); if (!col) return;
  const existing = state.columnFilters[colKey] || {};
  const popup = document.createElement('div'); popup.className = 'pf-filter-popup'; popup.id = 'pf-filter-popup';

  if (col.num) {
    const conditions = (existing.conditions && existing.conditions.length) ? existing.conditions : [{ op: 'gte', val: null }];
    function renderNumRows() { var rows = popup.querySelector('#pf-filt-rows'); if (!rows) return; var h = ''; for (var i = 0; i < conditions.length; i++) { var c = conditions[i]; h += '<div class="pf-filt-cond-row" style="display:flex;align-items:center;gap:4px;padding:2px 0;flex-shrink:0;">' + '<select class="pf-filt-cond-op" data-idx="' + i + '" style="padding:3px 4px;font-size:10px;background:var(--bg,#0b1018);color:var(--text);border:1px solid var(--line);border-radius:3px;">' + '<option value="eq"' + (c.op==='eq'?' selected':'') + '>=</option>' + '<option value="gte"' + (c.op==='gte'?' selected':'') + '>≥</option>' + '<option value="gt"' + (c.op==='gt'?' selected':'') + '>&gt;</option>' + '<option value="lte"' + (c.op==='lte'?' selected':'') + '>≤</option>' + '<option value="lt"' + (c.op==='lt'?' selected':'') + '>&lt;</option>' + '</select>' + '<input type="number" class="pf-filt-cond-val" data-idx="' + i + '" value="' + (c.val != null ? c.val : '') + '" placeholder="Value" step="any" style="flex:1;min-width:60px;padding:3px 6px;font-size:11px;background:var(--bg);color:var(--text);border:1px solid var(--line);border-radius:3px;" />' + '<button class="pf-filt-cond-rm" data-idx="' + i + '" style="background:none;border:none;cursor:pointer;color:var(--muted);font-size:14px;padding:0 4px;" title="Remove">×</button>' + '</div>'; } rows.innerHTML = h; }

    var html = '<div style="font-weight:600;margin-bottom:6px;flex-shrink:0;">Filter: ' + col.label + '</div>' + '<div class="pf-filter-body">' + '<div id="pf-filt-rows" style="flex-shrink:0;"></div>' + '<button class="btn-ghost btn-sm" id="pf-filt-add" style="font-size:10px;align-self:flex-start;margin-top:4px;">+ Add condition</button>' + '</div>' + '<div class="pf-filter-actions">' + '<button class="btn-ghost btn-sm" id="pf-filt-apply" style="flex:1;">Apply</button>' + '<button class="btn-ghost btn-sm" id="pf-filt-clear" style="flex:1;">Clear</button>' + '</div>';
    popup.innerHTML = html; renderNumRows(); document.body.appendChild(popup); _setupFilterResize(popup);
    var rect = anchorEl.getBoundingClientRect(); popup.style.left = Math.min(rect.left, window.innerWidth - 330) + 'px'; popup.style.top = (rect.bottom + 4) + 'px';

    popup.addEventListener('change', function(e) { if (e.target.classList.contains('pf-filt-cond-op')) conditions[parseInt(e.target.dataset.idx)].op = e.target.value; });
    popup.addEventListener('input', function(e) { if (e.target.classList.contains('pf-filt-cond-val')) conditions[parseInt(e.target.dataset.idx)].val = e.target.value; });
    popup.addEventListener('click', function(e) { if (e.target.classList.contains('pf-filt-cond-rm')) { e.stopPropagation(); e.preventDefault(); var idx = parseInt(e.target.dataset.idx); conditions.splice(idx, 1); renderNumRows(); } });
    $('#pf-filt-add').addEventListener('click', function() { conditions.push({ op: 'gte', val: null }); renderNumRows(); });
    $('#pf-filt-apply').addEventListener('click', function() { var rows = popup.querySelectorAll('.pf-filt-cond-row'); for (var i = 0; i < rows.length; i++) { if (i < conditions.length) { conditions[i].op = rows[i].querySelector('.pf-filt-cond-op').value; conditions[i].val = rows[i].querySelector('.pf-filt-cond-val').value; } } var active = conditions.filter(function(c) { return c.val !== null && c.val !== ''; }); if (active.length === 0) { delete state.columnFilters[colKey]; } else { state.columnFilters[colKey] = { type: 'num', conditions: active }; } renderHoldingsTable(); });
    $('#pf-filt-clear').addEventListener('click', function() { delete state.columnFilters[colKey]; renderHoldingsTable(); });
    popup.addEventListener('keydown', function(e) { if (e.key === 'Enter') $('#pf-filt-apply').click(); });
  } else {
    const allVals = new Set(); let hasNulls = false; const scope = state.holdings || [];
    for (const h of scope) { const v = col.get(h); if (v != null && v !== '') { allVals.add(String(v)); } else { hasNulls = true; } }
    const sorted = [...allVals].sort(); const selected = existing.values || allVals; const includeNulls = existing.hasOwnProperty('includeNulls') ? existing.includeNulls : true;
    var nullsHtml = ''; if (hasNulls) nullsHtml = '<label class="pf-check-row" style="border-bottom:1px solid var(--line);margin-bottom:2px;padding-bottom:4px;"><input type="checkbox" id="pf-filt-nulls" ' + (includeNulls ? 'checked' : '') + ' /><span style="color:var(--muted);font-style:italic;">— (no data)</span></label>';

    const html = '<div style="font-weight:600;margin-bottom:6px;flex-shrink:0;">Filter: ' + col.label + '</div>' + '<div class="pf-filter-body">' + '<input type="text" id="pf-filt-search" placeholder="Search…" style="width:100%;box-sizing:border-box;margin-bottom:4px;padding:4px 6px;font-size:11px;background:var(--bg,#0b1018);color:var(--text);border:1px solid var(--line);border-radius:3px;flex-shrink:0;" />' + '<div class="pf-filter-row" style="margin-bottom:4px;flex-shrink:0;">' + '<input type="text" id="pf-filt-wildcard" placeholder="Wildcard e.g. *.T" value="' + (existing.wildcard || '') + '" />' + '<select id="pf-filt-wc-mode" style="width:60px;"><option value="contains">has</option><option value="start">→</option><option value="end">←</option><option value="regex">~</option></select>' + '</div>' + '<div class="pf-filter-values" id="pf-filt-values">' + nullsHtml + sorted.map(function(v) { return '<label class="pf-check-row"><input type="checkbox" data-val="' + v.replace(/"/g,'&quot;') + '" ' + (selected.has(v)?'checked':'') + ' /><span>' + v + '</span></label>'; }).join('') + '</div>' + '</div>' + '<div class="pf-filter-actions">' + '<button class="btn-ghost btn-sm" id="pf-filt-apply" style="flex:1;">Apply</button>' + '<button class="btn-ghost btn-sm" id="pf-filt-all" style="flex:1;font-size:10px;">All</button>' + '<button class="btn-ghost btn-sm" id="pf-filt-none" style="flex:1;font-size:10px;">None</button>' + '</div>';
    popup.innerHTML = html; const wcModeSel = popup.querySelector('#pf-filt-wc-mode'); if (wcModeSel && existing.wcMode) wcModeSel.value = existing.wcMode; document.body.appendChild(popup); _setupFilterResize(popup); const rect = anchorEl.getBoundingClientRect(); popup.style.left = Math.min(rect.left, window.innerWidth - 330) + 'px'; popup.style.top = (rect.bottom + 4) + 'px';

    const applyText = () => {
      const sel = new Set(); popup.querySelectorAll('#pf-filt-values input[type=checkbox]:checked').forEach(cb => { if (cb.id !== 'pf-filt-nulls') sel.add(cb.dataset.val); });
      const nullsCb = popup.querySelector('#pf-filt-nulls'); const incNulls = nullsCb ? nullsCb.checked : true; const wc = $('#pf-filt-wildcard')?.value || ''; const wcMode = $('#pf-filt-wc-mode')?.value || 'contains';
      if (sel.size === 0 && !wc && incNulls) { state.columnFilters[colKey] = { type:'text', values: sel, wildcard: null, wcMode, includeNulls: incNulls }; }
      else if (sel.size === 0 && !wc && !incNulls) { state.columnFilters[colKey] = { type:'text', values: sel, wildcard: null, wcMode, includeNulls: false }; }
      else if (sel.size === allVals.size && !wc && incNulls) delete state.columnFilters[colKey]; else { state.columnFilters[colKey] = { type:'text', values: sel, wildcard: wc || null, wcMode, includeNulls: incNulls }; }
      renderHoldingsTable();
    };
    $('#pf-filt-apply').addEventListener('click', applyText);
    $('#pf-filt-all').addEventListener('click', () => { popup.querySelectorAll('#pf-filt-values input[type=checkbox]').forEach(cb => cb.checked = true); });
    $('#pf-filt-none').addEventListener('click', () => { popup.querySelectorAll('#pf-filt-values input[type=checkbox]').forEach(cb => cb.checked = false); });
    $('#pf-filt-search').addEventListener('input', () => { const q = $('#pf-filt-search').value.toLowerCase(); popup.querySelectorAll('#pf-filt-values .pf-check-row').forEach(row => { const label = (row.querySelector('span')?.textContent || '').toLowerCase(); row.style.display = label.includes(q) ? '' : 'none'; }); });
    popup.addEventListener('keydown', (e) => { if (e.key === 'Enter') $('#pf-filt-apply').click(); });
  }
}

function _setupFilterResize(popup) {
  var ro = new ResizeObserver(function(entries) {
    for (var i = 0; i < entries.length; i++) {
      var e = entries[i]; var h = e.contentRect.height; var headerEl = e.target.querySelector(':scope > div:first-child'); var actionsEl = e.target.querySelector('.pf-filter-actions'); var searchEl = e.target.querySelector('#pf-filt-search'); var wcRow = e.target.querySelector('#pf-filt-wildcard')?.closest('.pf-filter-row'); var valuesEl = e.target.querySelector('.pf-filter-values'); if (!valuesEl) return; var used = 20; if (headerEl) used += headerEl.offsetHeight; if (searchEl) used += searchEl.offsetHeight + 4; if (wcRow) used += wcRow.offsetHeight; if (actionsEl) used += actionsEl.offsetHeight; var avail = h - used; if (avail < 20) avail = 20; valuesEl.style.maxHeight = avail + 'px';
    }
  });
  ro.observe(popup);
}

function closeFilterPopup() { const p = document.getElementById('pf-filter-popup'); if (p) p.remove(); }

function _updateFilterIndicators() { $$('.pf-sort-th').forEach(th => { const key = th.dataset.sort; const hasFilter = !!state.columnFilters[key]; let icon = th.querySelector('.pf-filter-indicator'); if (hasFilter && !icon) { icon = document.createElement('span'); icon.className = 'pf-filter-indicator'; icon.textContent = ' ⏏'; icon.style.cssText = 'color:var(--accent,#58a6ff);font-size:10px;'; th.appendChild(icon); } else if (!hasFilter && icon) { icon.remove(); } }); }

function _applyFilters(rows) {
  const filters = state.columnFilters; if (!Object.keys(filters).length) return rows; return rows.filter(h => { for (const [key, f] of Object.entries(filters)) { const col = _col(key); if (!col) continue; const raw = col.get(h); if (f.type === 'text') { const s = String(raw ?? ''); if ((raw == null || s === '' || s === 'null' || s === 'undefined')) { if (f.includeNulls === false) return false; continue; } if (f.values && f.values.size === 0 && !f.wildcard) return false; if (f.values && f.values.size > 0 && !f.values.has(s)) return false; if (f.wildcard) { const w = f.wildcard; const mode = f.wcMode || 'contains'; let match = false; if (mode === 'contains') match = s.toLowerCase().includes(w.toLowerCase()); else if (mode === 'start') match = s.toLowerCase().startsWith(w.toLowerCase()); else if (mode === 'end') match = s.toLowerCase().endsWith(w.toLowerCase()); else if (mode === 'regex') { try { match = new RegExp(w, 'i').test(s); } catch(_) { match = true; } } if (!match) return false; } } else if (f.type === 'num') { var n = parseFloat(raw); if (isNaN(n)) return false; var conds = f.conditions || []; for (var ci = 0; ci < conds.length; ci++) { var c = conds[ci]; if (c.val === null || c.val === '') continue; var v = parseFloat(c.val); var op = c.op || 'gte'; if (op === 'eq' && n !== v) return false; if (op === 'gte' && n < v) return false; if (op === 'gt' && n <= v) return false; if (op === 'lte' && n > v) return false; if (op === 'lt' && n >= v) return false; } } } return true; }); }

// Holding detail
let _detailChart = null;
function destroyDetailChart() { if (_detailChart) { _detailChart.destroy(); _detailChart = null; } }

function toggleHoldingDetail(row) {
  const raw = row.dataset.symbol || row.dataset.holding; if (!raw) return; const symbol = decodeURIComponent(raw); const existing = row.nextElementSibling; if (existing && existing.classList.contains('pf-holding-detail-row')) { existing.remove(); return; } $$('.pf-holding-detail-row').forEach(r => r.remove());
  const h = state.holdings.find(x => x.symbol === symbol);
  if (h && h.performance) { insertDetailRow(row, symbol, h.performance, false); return; }
  const cp = state.closedPositions.find(x => x.symbol === symbol);
  if (cp) { insertDetailRow(row, symbol, cp, true); return; }
  fetchJson(`/api/portfolio/holdings/${encodeURIComponent(symbol)}/performance`).then(p => insertDetailRow(row, symbol, p, false)).catch(err => log('warn', `Detail error for ${symbol}: ` + (err && err.message || err)));
}

function insertDetailRow(row, symbol, p, isClosed) {
  const cols = row.querySelectorAll('td').length; const pct = v => v != null ? (v * 100).toFixed(2) + '%' : '—'; const detailTr = document.createElement('tr'); detailTr.className = 'pf-holding-detail-row'; const chartId = 'pf-detail-chart-' + symbol.replace(/[^a-zA-Z0-9]/g, '_');

  if (isClosed) {
    const rpnl = p.realized_pnl || 0; const pnlColor = rpnl >= 0 ? 'var(--success)' : 'var(--danger)'; const cost = p.total_cost || 0; const pnlPct = cost > 0 ? ((rpnl / cost) * 100).toFixed(2) + '%' : '—'; const retColor = rpnl >= 0 ? 'var(--success)' : 'var(--danger)'; const normSym = normalizeTicker(p.symbol); const isStock = p.asset_category === 'STK';
    detailTr.innerHTML = `<td colspan="${cols}" style="padding:12px 16px;background:rgba(88,166,255,0.04);border-left:3px solid var(--warning);">\n    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">\n      <strong>${p.symbol} — Closed Position  <span class="pf-closed-badge" style="margin-left:6px;">CLOSED</span></strong>\n      <button class="btn-ghost btn-sm" onclick="this.closest('tr').remove();">✕</button>\n    </div>\n    <div class="metric-grid" style="gap:8px;">\n      <div class="metric-tile"><div class="metric-label">Realized P&L</div><div class="metric-value" style="color:${pnlColor};">${formatMoney(rpnl)}</div></div>\n      <div class="metric-tile"><div class="metric-label">Return %</div><div class="metric-value" style="color:${retColor};">${pnlPct}</div></div>\n      <div class="metric-tile"><div class="metric-label">Total Cost</div><div class="metric-value">${formatMoney(p.total_cost)}</div></div>\n      <div class="metric-tile"><div class="metric-label">Total Proceeds</div><div class="metric-value">${formatMoney(p.total_proceeds)}</div></div>\n      <div class="metric-tile"><div class="metric-label">Total Bought</div><div class="metric-value">${p.total_bought || 0}</div></div>\n      <div class="metric-tile"><div class="metric-label">Total Sold</div><div class="metric-value">${p.total_sold || 0}</div></div>\n      <div class="metric-tile"><div class="metric-label">First Trade</div><div class="metric-value">${p.first_trade_date || '—'}</div></div>\n      <div class="metric-tile"><div class="metric-label">Last Trade</div><div class="metric-value">${p.last_trade_date || '—'}</div></div>\n      <div class="metric-tile"><div class="metric-label">Category</div><div class="metric-value">${p.asset_category || '—'}</div></div>\n      ${isStock ? `<div class="metric-tile"><a href="/security?symbol=${encodeURIComponent(normSym)}" class="btn-ghost btn-sm">Open Security Analysis →</a></div>` : ''}\n    </div>\n  </td>`;
  } else {
    const pnlColor2 = (p.unrealized_pnl || 0) >= 0 ? 'var(--success)' : 'var(--danger)'; const retColor2 = (p.total_return || 0) >= 0 ? 'var(--success)' : 'var(--danger)';
    detailTr.innerHTML = `<td colspan="${cols}" style="padding:12px 16px;background:rgba(88,166,255,0.04);border-left:3px solid var(--accent);">\n    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">\n      <strong>${p.symbol} — Performance Details</strong>\n      <button class="btn-ghost btn-sm" onclick="this.closest('tr').remove();">✕</button>\n    </div>\n    <div class="metric-grid" style="gap:8px;margin-bottom:12px;">\n      <div class="metric-tile"><div class="metric-label">Total Return</div><div class="metric-value" style="color:${retColor2};">${pct(p.total_return)}</div></div>\n      <div class="metric-tile"><div class="metric-label">Ann. Return</div><div class="metric-value" style="color:${retColor2};">${pct(p.annualized_return)}</div></div>\n      <div class="metric-tile"><div class="metric-label">Volatility</div><div class="metric-value">${pct(p.volatility)}</div></div>\n      <div class="metric-tile"><div class="metric-label">Unrealized P&L</div><div class="metric-value" style="color:${pnlColor2};">${formatMoney(p.unrealized_pnl)}</div></div>\n      <div class="metric-tile"><div class="metric-label">Dividend Income</div><div class="metric-value" style="color:var(--success);">${formatMoney(p.dividend_income)}</div></div>\n      <div class="metric-tile"><div class="metric-label">Dividend Yield</div><div class="metric-value">${pct(p.dividend_yield)}</div></div>\n      <div class="metric-tile"><div class="metric-label">Avg Cost</div><div class="metric-value">${formatMoney(p.avg_cost)}</div></div>\n      <div class="metric-tile"><div class="metric-label">Current Price</div><div class="metric-value">${formatMoney(p.current_price)}</div></div>\n      <div class="metric-tile"><div class="metric-label">First Purchase</div><div class="metric-value">${p.first_purchase || '—'}</div></div>\n      <div class="metric-tile"><div class="metric-label">Last Purchase</div><div class="metric-value">${p.last_purchase || '—'}</div></div>\n      <div class="metric-tile"><div class="metric-label"># Buys / # Sells</div><div class="metric-value">${p.num_buys} / ${p.num_sells}</div></div>\n      <div class="metric-tile"><div class="metric-label">Current Value</div><div class="metric-value">${formatMoney(p.current_value)} (${formatMoney(p.current_value_native)} ${p.currency})</div></div>\n      <div class="metric-tile"><div class="metric-label">Div Gross</div><div class="metric-value" style="color:var(--success);">${formatMoney(p.dividend_gross)}</div></div>\n      <div class="metric-tile"><div class="metric-label">Div Tax</div><div class="metric-value" style="color:var(--danger);">${formatMoney(p.dividend_tax)}</div></div>\n    </div>\n    <div style="height:220px;margin-bottom:8px;">\n      <canvas id="${chartId}"></canvas>\n    </div>\n    <div style="margin-top:4px;">\n      <a href="/security?symbol=${encodeURIComponent(normalizeTicker(p.symbol))}" class="btn-ghost btn-sm">Open Security Analysis →</a>\n    </div>\n  </td>`;
  }
  row.after(detailTr);

  if (!isClosed) {
    destroyDetailChart();
    fetchJson(`/api/portfolio/holdings/${encodeURIComponent(symbol)}/history`).then(history => {
      const ctx = document.getElementById(chartId); if (!ctx || !history || !history.length) return; const filtered = history.filter(h => h.market_value != null || h.market_price != null); if (!filtered.length) return;
      _detailChart = new Chart(ctx, {
        type: 'line',
        data: {
          labels: filtered.map(h => h.date),
          datasets: [{ label: 'Value (EUR)', data: filtered.map(h => h.market_value), borderColor: '#58a6ff', backgroundColor: 'rgba(88,166,255,0.08)', fill: true, tension: 0.2, pointRadius: 0, yAxisID: 'y', }, { label: 'Price', data: filtered.map(h => h.market_price), borderColor: '#e0af4f', borderDash: [4, 2], tension: 0.2, pointRadius: 0, yAxisID: 'y1', }],
        },
        options: {
          responsive: true, maintainAspectRatio: false,
          interaction: { intersect: false, mode: 'index' },
          scales: { y: { position: 'left', ticks: { color: '#8ea0b8', callback: v => formatMoney(v) }, title: { display: true, text: 'Value', color: '#8ea0b8' } }, y1: { position: 'right', ticks: { color: '#8ea0b8', callback: v => formatMoney(v) }, grid: { display: false }, title: { display: true, text: 'Price', color: '#8ea0b8' } }, x: { ticks: { color: '#8ea0b8', maxTicksLimit: 8 } }, },
          plugins: { legend: { labels: { color: '#d9e2f2', usePointStyle: true } } },
        },
      });
    }).catch(err => log('warn', `Chart fetch error for ${symbol}: ${(err && err.message) || err}`));
  }
}

export { loadHoldings as default };
