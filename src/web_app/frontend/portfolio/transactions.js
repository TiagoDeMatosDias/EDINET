import { $, $$, fetchJson, state, badgeClass, formatMoney } from './common.js';

export const TXN_COLUMNS = [
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

export async function loadTransactions() {
  try {
    const data = await fetchJson('/api/portfolio/transactions?limit=10000');
    state.transactions = data;
    const cnt = $('#pf-txn-count'); if (cnt) cnt.textContent = `${data.length} transactions`;
    renderTransactionsTable();
  } catch (e) {
    const div = $('#pf-transactions-table'); if (div) div.innerHTML = `<span class="status-text error">${e.message}</span>`;
  }
}

function _applyTxnFilters(rows) {
  const f = state._txnFilters || {};
  if (!f || !Object.keys(f).length) return rows;
  return rows.filter(row => {
    for (const [key, filt] of Object.entries(f)) {
      const col = TXN_COL_MAP.get(key); if (!col) continue;
      const raw = col.get(row);
      if (filt.type === 'date') {
        const d = String(raw ?? ''); if (filt.from && d < filt.from) return false; if (filt.to && d > filt.to) return false;
      } else if (filt.type === 'text') {
        const s = String(raw ?? ''); if ((raw == null || s === '' || s === 'null' || s === 'undefined')) { if (filt.includeNulls === false) return false; continue; }
        if (filt.values && filt.values.size === 0 && !filt.wildcard) return false;
        if (filt.values && filt.values.size > 0 && !filt.values.has(s)) return false;
        if (filt.wildcard) {
          const w = filt.wildcard, m = filt.wcMode || 'contains'; let ok = false;
          if (m === 'contains') ok = s.toLowerCase().includes(w.toLowerCase()); else if (m === 'start') ok = s.toLowerCase().startsWith(w.toLowerCase()); else if (m === 'end') ok = s.toLowerCase().endsWith(w.toLowerCase()); else if (m === 'regex') { try { ok = new RegExp(w, 'i').test(s); } catch(_) { ok = true; } }
          if (!ok) return false;
        }
      } else if (filt.type === 'num') {
        const n = parseFloat(raw); if (isNaN(n)) return false; const conds = filt.conditions || [];
        for (let ci = 0; ci < conds.length; ci++) {
          const c = conds[ci]; if (c.val === null || c.val === '') continue; const v = parseFloat(c.val), op = c.op || 'gte';
          if (op === 'eq' && n !== v) return false; if (op === 'gte' && n < v) return false; if (op === 'gt' && n <= v) return false; if (op === 'lte' && n > v) return false; if (op === 'lt' && n >= v) return false;
        }
      }
    }
    return true;
  });
}

function _openTxnFilter(colKey, anchorEl) {
  _closeTxnPopup(); const col = TXN_COL_MAP.get(colKey); if (!col) return; const existing = state._txnFilters && state._txnFilters[colKey] ? state._txnFilters[colKey] : {};
  const popup = document.createElement('div'); popup.className = 'pf-filter-popup'; popup.id = 'pf-txn-filter-popup';

  if (col.date) {
    popup.innerHTML = '<div style="font-weight:600;margin-bottom:6px;">Filter: ' + col.label + '</div>' + '<div class="pf-filter-body">' + '<div class="pf-filter-row" style="flex-shrink:0;"><label style="font-size:10px;color:var(--muted);width:30px;">From</label>' + '<input type="date" id="dt-filt-date-from" value="' + (existing.from || '') + '" style="flex:1;padding:3px 6px;font-size:11px;background:var(--bg);color:var(--text);border:1px solid var(--line);border-radius:3px;" /></div>' + '<div class="pf-filter-row" style="flex-shrink:0;"><label style="font-size:10px;color:var(--muted);width:30px;">To</label>' + '<input type="date" id="dt-filt-date-to" value="' + (existing.to || '') + '" style="flex:1;padding:3px 6px;font-size:11px;background:var(--bg);color:var(--text);border:1px solid var(--line);border-radius:3px;" /></div>' + '</div>' + '<div class="pf-filter-actions">' + '<button class="btn-ghost btn-sm" id="dt-filt-apply" style="flex:1;">Apply</button>' + '<button class="btn-ghost btn-sm" id="dt-filt-clear" style="flex:1;">Clear</button>' + '</div>';
    document.body.appendChild(popup); const pos = anchorEl.getBoundingClientRect(); popup.style.left = Math.min(pos.left, window.innerWidth - 300) + 'px'; popup.style.top = (pos.bottom + 4) + 'px';
    popup.querySelector('#dt-filt-apply').addEventListener('click', function() { const from = popup.querySelector('#dt-filt-date-from').value; const to = popup.querySelector('#dt-filt-date-to').value; if (!from && !to) { if (state._txnFilters) delete state._txnFilters[colKey]; } else { state._txnFilters = state._txnFilters || {}; state._txnFilters[colKey] = { type: 'date', from: from || null, to: to || null }; } renderTransactionsTable(); });
    popup.querySelector('#dt-filt-clear').addEventListener('click', function() { if (state._txnFilters) delete state._txnFilters[colKey]; renderTransactionsTable(); });
  } else if (col.num) {
    const conds = (existing.conditions && existing.conditions.length) ? existing.conditions : [{ op: 'gte', val: null }];
    function renderRows() { const r = popup.querySelector('#dt-filt-rows'); if (!r) return; let h = ''; for (let i = 0; i < conds.length; i++) { const c = conds[i]; h += '<div class="pf-filt-cond-row" style="display:flex;align-items:center;gap:4px;padding:2px 0;">' + '<select class="pf-filt-cond-op" data-idx="' + i + '" style="padding:3px 4px;font-size:10px;background:var(--bg);color:var(--text);border:1px solid var(--line);border-radius:3px;">' + '<option value="eq"' + (c.op==='eq'?' selected':'') + '>=</option><option value="gte"' + (c.op==='gte'?' selected':'') + '>≥</option>' + '<option value="gt"' + (c.op==='gt'?' selected':'') + '>&gt;</option><option value="lte"' + (c.op==='lte'?' selected':'') + '>≤</option>' + '<option value="lt"' + (c.op==='lt'?' selected':'') + '>&lt;</option>' + '</select>' + '<input type="number" class="pf-filt-cond-val" data-idx="' + i + '" value="' + (c.val != null ? c.val : '') + '" placeholder="Value" step="any" style="flex:1;min-width:60px;padding:3px 6px;font-size:11px;background:var(--bg);color:var(--text);border:1px solid var(--line);border-radius:3px;" />' + '<button class="pf-filt-cond-rm" data-idx="' + i + '" style="background:none;border:none;cursor:pointer;color:var(--muted);font-size:14px;padding:0 4px;">×</button>' + '</div>'; } r.innerHTML = h; }
    popup.innerHTML = '<div style="font-weight:600;margin-bottom:6px;">Filter: ' + col.label + '</div>' + '<div class="pf-filter-body"><div id="dt-filt-rows"></div>' + '<button class="btn-ghost btn-sm" id="dt-filt-add" style="font-size:10px;margin-top:4px;">+ Add condition</button></div>' + '<div class="pf-filter-actions"><button class="btn-ghost btn-sm" id="dt-filt-apply" style="flex:1;">Apply</button><button class="btn-ghost btn-sm" id="dt-filt-clear" style="flex:1;">Clear</button></div>';
    document.body.appendChild(popup); const pos = anchorEl.getBoundingClientRect(); popup.style.left = Math.min(pos.left, window.innerWidth - 300) + 'px'; popup.style.top = (pos.bottom + 4) + 'px'; renderRows();
    popup.addEventListener('change', function(e) { if (e.target.classList.contains('pf-filt-cond-op')) conds[+e.target.dataset.idx].op = e.target.value; });
    popup.addEventListener('input', function(e) { if (e.target.classList.contains('pf-filt-cond-val')) conds[+e.target.dataset.idx].val = e.target.value; });
    popup.addEventListener('click', function(e) { if (e.target.classList.contains('pf-filt-cond-rm')) { e.stopPropagation(); conds.splice(+e.target.dataset.idx, 1); renderRows(); } });
    popup.querySelector('#dt-filt-add').addEventListener('click', function() { conds.push({ op: 'gte', val: null }); renderRows(); });
    popup.querySelector('#dt-filt-apply').addEventListener('click', function() { const rows = popup.querySelectorAll('.pf-filt-cond-row'); for (let i = 0; i < rows.length; i++) { conds[i].op = rows[i].querySelector('.pf-filt-cond-op').value; conds[i].val = rows[i].querySelector('.pf-filt-cond-val').value; } const active = conds.filter(c => c.val !== null && c.val !== ''); if (active.length === 0) { if (state._txnFilters) delete state._txnFilters[colKey]; } else { state._txnFilters = state._txnFilters || {}; state._txnFilters[colKey] = { type: 'num', conditions: active }; } renderTransactionsTable(); });
    popup.querySelector('#dt-filt-clear').addEventListener('click', function() { if (state._txnFilters) delete state._txnFilters[colKey]; renderTransactionsTable(); });
  } else {
    const allVals = new Set(); let hasNulls = false; for (const t of state.transactions || []) { const v = col.get(t); if (v != null && v !== '') allVals.add(String(v)); else hasNulls = true; }
    const sorted = [...allVals].sort(); const selected = existing.values || allVals; const includeNulls = existing.hasOwnProperty('includeNulls') ? existing.includeNulls : true; let nullsHtml = ''; if (hasNulls) nullsHtml = '<label class="pf-check-row" style="border-bottom:1px solid var(--line);margin-bottom:2px;padding-bottom:4px;"><input type="checkbox" id="dt-filt-nulls" ' + (includeNulls ? 'checked' : '') + ' /><span style="color:var(--muted);font-style:italic;">— (no data)</span></label>';
    popup.innerHTML = '<div style="font-weight:600;margin-bottom:6px;">Filter: ' + col.label + '</div>' + '<div class="pf-filter-body">' + '<input type="text" id="dt-filt-search" placeholder="Search…" style="width:100%;box-sizing:border-box;margin-bottom:4px;padding:4px 6px;font-size:11px;background:var(--bg);color:var(--text);border:1px solid var(--line);border-radius:3px;" />' + '<div class="pf-filter-row" style="margin-bottom:4px;"><input type="text" id="dt-filt-wc" placeholder="Wildcard e.g. *.T" value="' + (existing.wildcard || '') + '" /><select id="dt-filt-wcm" style="width:60px;"><option value="contains">has</option><option value="start">→</option><option value="end">←</option><option value="regex">~</option></select></div>' + '<div class="pf-filter-values" id="dt-filt-vals">' + nullsHtml + sorted.map(function(v) { return '<label class="pf-check-row"><input type="checkbox" data-val="' + v.replace(/"/g,'&quot;') + '" ' + (selected.has(v)?'checked':'') + ' /><span>' + v + '</span></label>'; }).join('') + '</div></div>' + '<div class="pf-filter-actions"><button class="btn-ghost btn-sm" id="dt-filt-apply" style="flex:1;">Apply</button><button class="btn-ghost btn-sm" id="dt-filt-all" style="flex:1;font-size:10px;">All</button><button class="btn-ghost btn-sm" id="dt-filt-none" style="flex:1;font-size:10px;">None</button></div>';
    document.body.appendChild(popup); if (existing.wcMode) popup.querySelector('#dt-filt-wcm').value = existing.wcMode; const pos = anchorEl.getBoundingClientRect(); popup.style.left = Math.min(pos.left, window.innerWidth - 300) + 'px'; popup.style.top = (pos.bottom + 4) + 'px';
    const apply = function() { const sel = new Set(); popup.querySelectorAll('#dt-filt-vals input[type=checkbox]:checked').forEach(cb => { if (cb.id !== 'dt-filt-nulls') sel.add(cb.dataset.val); }); const ncb = popup.querySelector('#dt-filt-nulls'); const incNulls = ncb ? ncb.checked : true; const wc = popup.querySelector('#dt-filt-wc')?.value || ''; const wcm = popup.querySelector('#dt-filt-wcm')?.value || 'contains'; if (sel.size === 0 && !wc && !incNulls) state._txnFilters = state._txnFilters || {}, state._txnFilters[colKey] = { type:'text', values: sel, includeNulls: false }; else if (sel.size === allVals.size && !wc && incNulls) { if (state._txnFilters) delete state._txnFilters[colKey]; } else state._txnFilters = state._txnFilters || {}, state._txnFilters[colKey] = { type:'text', values: sel, wildcard: wc || null, wcMode: wcm, includeNulls: incNulls }; renderTransactionsTable(); };
    popup.querySelector('#dt-filt-apply').addEventListener('click', apply); popup.querySelector('#dt-filt-all').addEventListener('click', function() { popup.querySelectorAll('#dt-filt-vals input[type=checkbox]').forEach(cb => cb.checked = true); }); popup.querySelector('#dt-filt-none').addEventListener('click', function() { popup.querySelectorAll('#dt-filt-vals input[type=checkbox]').forEach(cb => cb.checked = false); }); popup.querySelector('#dt-filt-search').addEventListener('input', function() { const q = popup.querySelector('#dt-filt-search').value.toLowerCase(); popup.querySelectorAll('#dt-filt-vals .pf-check-row').forEach(row => { row.style.display = (row.querySelector('span')?.textContent || '').toLowerCase().includes(q) ? '' : 'none'; }); });
  }
}

function _closeTxnPopup() { const p = document.getElementById('pf-txn-filter-popup'); if (p) p.remove(); }
document.addEventListener('click', function(e) { const p = document.getElementById('pf-txn-filter-popup'); if (p && !p.contains(e.target) && !e.target.closest('[data-txn-sort]')) p.remove(); });

export function renderTransactionsTable() {
  const div = $('#pf-transactions-table');
  let txns = _applyTxnFilters(state.transactions || []);
  if (!txns.length) { if (div) div.innerHTML = '<span class="muted">No transactions match filters.</span>'; return; }
  let h = '<table class="data-table" style="width:100%;" id="pf-txn-tbl"><thead><tr>' + '<th data-txn-sort="trade_date">Date</th><th data-txn-sort="activity_type">Type</th><th data-txn-sort="symbol">Symbol</th><th data-txn-sort="quantity">Qty</th>' + '<th data-txn-sort="trade_price">Price</th><th data-txn-sort="amount">Amount</th><th data-txn-sort="currency">Cur</th><th data-txn-sort="buy_sell">B/S</th>' + '<th data-txn-sort="commission">Comm</th><th data-txn-sort="description">Description</th></tr></thead><tbody>';
  for (const t of txns) {
    const amt = t.activity_type === 'TRADE' ? (t.net_cash != null ? t.net_cash : (t.amount || 0)) : (t.amount || 0);
    const isNeg = (typeof amt === 'number' && amt < 0);
    const desc = (t.description || '—'); const descShort = desc.length > 50 ? desc.substring(0, 47) + '…' : desc;
    h += '<tr>' + '<td style="white-space:nowrap;">' + (t.trade_date || '—') + '</td>' + '<td><span class="badge ' + badgeClass(t.activity_type) + '" style="font-size:10px;">' + (t.activity_type || '').replace('_', ' ') + '</span></td>' + '<td><strong>' + (t.symbol || '—') + '</strong></td>' + '<td>' + (t.quantity != null ? t.quantity : '—') + '</td>' + '<td>' + (t.trade_price != null ? formatMoney(t.trade_price) : '—') + '</td>' + '<td style="color:' + (isNeg ? 'var(--danger)' : 'var(--success)') + ';">' + formatMoney(amt) + '</td>' + '<td>' + (t.currency || '') + '</td>' + '<td>' + (t.buy_sell || '') + '</td>' + '<td>' + (t.commission != null ? formatMoney(t.commission) : '') + '</td>' + '<td title="' + desc.replace(/"/g, '&quot;') + '" style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + descShort + '</td>' + '</tr>';
  }
  h += '</tbody></table>';
  if (div) div.innerHTML = h;

  const headers = div ? div.querySelectorAll('th[data-txn-sort]') : [];
  headers.forEach(th => {
    th.addEventListener('contextmenu', e => { e.preventDefault(); _openTxnFilter(th.dataset.txnSort, th); });
    const key = th.dataset.txnSort; let icon = th.querySelector('.pf-filter-indicator'); if (state._txnFilters && state._txnFilters[key]) { if (!icon) { icon = document.createElement('span'); icon.className = 'pf-filter-indicator'; icon.textContent = ' ⏏'; icon.style.cssText = 'color:var(--accent,#58a6ff);font-size:10px;'; th.appendChild(icon); } } else if (icon) { icon.remove(); }
  });
}

export async function refreshSymbols() {
  try {
    const data = await fetchJson('/api/portfolio/symbols');
    const sel = $('#pf-txn-symbol-filter');
    if (!sel) return;
    sel.innerHTML = '<option value="">All Symbols</option>';
    for (const s of data) { if (s.symbol) sel.innerHTML += `<option value="${s.symbol}">${s.symbol}</option>`; }
  } catch (_) { /* ignore */ }
}
