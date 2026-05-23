/**
 * Reusable DataTable component with column visibility, pinning, drag-reorder,
 * column filtering, sorting, and summary row.
 *
 * Usage:
 *   import { createDataTable } from '../common/datatable.js';
 *   const tbl = createDataTable({
 *     container: '#my-table-div',
 *     columns: [
 *       { key: 'symbol', label: 'Symbol', get: row => row.symbol },
 *       { key: 'price', label: 'Price', get: row => row.price, num: true },
 *     ],
 *     dataSource: async () => fetchJson('/api/my-data'),
 *     defaultPinned: ['symbol'],
 *     defaultSort: null
 *   });
 *   await tbl.load();
 */

import { $ } from './utils.js';

// ── Helpers ──
function $$(sel, root) { return Array.from((root || document).querySelectorAll(sel)); }

export function createDataTable(config) {
  const container = typeof config.container === 'string' ? $(config.container) : config.container;
  if (!container) throw new Error('DataTable: container not found');

  const columns = config.columns || [];
  const colMap = new Map(columns.map(c => [c.key, c]));
  function _col(key) { return colMap.get(key); }

  // ── State ──
  let data = config.initialData || [];
  let _renderRaf = 0; // pending requestAnimationFrame ID for chunked render
  let visibleColumns = config.defaultVisible || null; // null = show all
  let pinnedColumns = new Set(config.defaultPinned || []);
  let columnOrder = (config.defaultOrder || columns.map(c => c.key));
  let columnFilters = config.defaultFilters || {};
  let sortColumn = config.defaultSort || null;
  let sortAsc = true;
  let displayCurrency = config.defaultCurrency || 'EUR';
  let tableId = 'dt-' + (config.id || Math.random().toString(36).slice(2, 8));
  let loading = false;

  // ── Computed helpers ──
  function _orderedCols() { return columnOrder.map(k => _col(k)).filter(Boolean); }
  function _sumCols() { return columns.filter(c => c.summary === 'sum' || (c.num && c.summary !== 'avg')); }
  function _avgCols() { return columns.filter(c => c.summary === 'avg'); }

  // ── Loading ──
  function _showLoading() {
    loading = true;
    container.innerHTML = '<div class="pf-table-loading"><div class="pf-chart-loading-spinner"></div><div class="pf-chart-loading-text">Loading…</div></div>';
  }

  // ── Filters ──
  function _applyFilters(rows) {
    if (!Object.keys(columnFilters).length) return rows;
    return rows.filter(row => {
      for (const [key, f] of Object.entries(columnFilters)) {
        const col = _col(key); if (!col) continue;
        const raw = col.get(row);
        if (f.type === 'text') {
          const s = String(raw ?? '');
          if ((raw == null || s === '' || s === 'null' || s === 'undefined')) {
            if (f.includeNulls === false) return false;
            continue;
          }
          if (f.values && f.values.size === 0 && !f.wildcard) return false;
          if (f.values && f.values.size > 0 && !f.values.has(s)) return false;
          if (f.wildcard) {
            const w = f.wildcard, mode = f.wcMode || 'contains';
            let match = false;
            if (mode === 'contains') match = s.toLowerCase().includes(w.toLowerCase());
            else if (mode === 'start') match = s.toLowerCase().startsWith(w.toLowerCase());
            else if (mode === 'end') match = s.toLowerCase().endsWith(w.toLowerCase());
            else if (mode === 'regex') { try { match = new RegExp(w, 'i').test(s); } catch(_) { match = true; } }
            if (!match) return false;
          }
        } else if (f.type === 'num') {
          const n = parseFloat(raw); if (isNaN(n)) return false;
          const conds = f.conditions || [];
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

  // ── Render ──
  function render() {
    // Cancel any pending chunked render from a previous call
    if (_renderRaf) { cancelAnimationFrame(_renderRaf); _renderRaf = 0; }

    const cols = _orderedCols();
    let sorted = _applyFilters([...data]);
    if (sortColumn) {
      const sd = _col(sortColumn);
      if (sd) {
        sorted.sort((a, b) => {
          const va = sd.get(a) ?? (sd.num ? -Infinity : '');
          const vb = sd.get(b) ?? (sd.num ? -Infinity : '');
          if (sd.num) return sortAsc ? va - vb : vb - va;
          return sortAsc ? String(va).toLowerCase().localeCompare(String(vb).toLowerCase())
                         : String(vb).toLowerCase().localeCompare(String(va).toLowerCase());
        });
      }
    }

    // Summary
    const summary = {};
    const sumKeys = _sumCols().map(c => c.key);
    const avgKeys = _avgCols().map(c => c.key);
    for (const k of sumKeys) summary[k] = 0;
    for (const k of avgKeys) { summary[k] = 0; summary[k + '_c'] = 0; }
    for (const row of sorted) {
      for (const k of sumKeys) { const v = _col(k).get(row); if (v != null && !isNaN(v)) summary[k] += v; }
      for (const k of avgKeys) { const v = _col(k).get(row); if (v != null && !isNaN(v)) { summary[k] += v; summary[k + '_c']++; } }
    }

    // ── Build table shell (thead + empty tbody + tfoot) ──
    let html = '<table class="data-table" id="' + tableId + '"><thead><tr>';
    for (const col of cols) {
      const arrow = sortColumn === col.key ? (sortAsc ? ' ▲' : ' ▼') : '';
      html += '<th class="pf-sort-th pf-col-th-' + col.key + '" data-sort="' + col.key + '" draggable="true" style="cursor:pointer;user-select:none;white-space:nowrap;">' + col.label + arrow + '</th>';
    }
    html += '</tr></thead><tbody id="' + tableId + '-body"></tbody>';
    if (sumKeys.length || avgKeys.length) {
      html += '<tfoot><tr class="pf-summary-row">';
      for (const col of cols) {
        const k = col.key;
        if (sumKeys.includes(k)) {
          html += '<td class="pf-col-td-' + k + '" style="color:var(--text);">' + _fmtMoney(summary[k]) + '</td>';
        } else if (avgKeys.includes(k)) {
          html += '<td class="pf-col-td-' + k + '">' + (summary[k + '_c'] > 0 ? (summary[k] / summary[k + '_c'] * 100).toFixed(1) + '%' : '—') + '</td>';
        } else if (k === cols[0].key) {
          html += '<td class="pf-col-td-' + k + '"><strong>Summary</strong></td>';
        } else {
          html += '<td class="pf-col-td-' + k + '"></td>';
        }
      }
      html += '</tr></tfoot>';
    }
    html += '</table>';
    container.innerHTML = html;

    // Wire header events immediately
    _wireSortAndDrag();
    _updateFilterIndicators();

    // ── Build tbody HTML string (fast) and set via RAF (yields to browser) ──
    // innerHTML is 10-20× faster than createElement for large tables —
    // the browser's HTML parser is highly optimized C++ code.
    if (sorted.length > 0) {
      const tbody = document.getElementById(tableId + '-body');
      if (tbody) {
        _renderRaf = requestAnimationFrame(function() {
          let h = '';
          for (let i = 0; i < sorted.length; i++) {
            const row = sorted[i];
            h += '<tr class="pf-dt-row">';
            for (const col of cols) {
              const raw = col.get(row);
              h += '<td class="pf-col-td-' + col.key + '">' + (col.render ? col.render(raw, row) : (raw != null ? raw : '—')) + '</td>';
            }
            h += '</tr>';
          }
          tbody.innerHTML = h;
          _renderRaf = 0;
          _applyColumnVisibility();
        });
      }
    }
  }

  function _applyColumnVisibility() {
    const cols = _orderedCols();
    // th
    for (const col of cols) {
      const th = document.querySelector('#' + tableId + ' .pf-col-th-' + col.key);
      if (!th) continue;
      const show = !visibleColumns || visibleColumns.has(col.key);
      th.classList.toggle('pf-col-hidden', !show);
      const isPinned = show && pinnedColumns.has(col.key);
      th.classList.toggle('pf-sticky-col', isPinned);
    }
    // td
    const sel = '#' + tableId + ' tbody tr, #' + tableId + ' tfoot tr';
    for (const row of document.querySelectorAll(sel)) {
      for (const col of cols) {
        const td = row.querySelector('.pf-col-td-' + col.key);
        if (!td) continue;
        const show = !visibleColumns || visibleColumns.has(col.key);
        td.classList.toggle('pf-col-hidden', !show);
        const isPinned = show && pinnedColumns.has(col.key);
        td.classList.toggle('pf-sticky-col', isPinned);
      }
    }
    _recalcStickyOffsets();
  }

  function _recalcStickyOffsets() {
    const cols = _orderedCols();
    requestAnimationFrame(() => {
      let offset = 0;
      for (const col of cols) {
        const th = document.querySelector('#' + tableId + ' .pf-col-th-' + col.key);
        if (!th || th.classList.contains('pf-col-hidden') || !pinnedColumns.has(col.key)) continue;
        th.style.left = offset + 'px';
        offset += th.offsetWidth;
      }
      for (const row of document.querySelectorAll('#' + tableId + ' tbody tr, #' + tableId + ' tfoot tr')) {
        let tdOffset = 0;
        for (const col of cols) {
          const td = row.querySelector('.pf-col-td-' + col.key);
          if (!td || td.classList.contains('pf-col-hidden') || !pinnedColumns.has(col.key)) continue;
          td.style.left = tdOffset + 'px';
          tdOffset += td.offsetWidth;
        }
      }
    });
  }

  function _wireSortAndDrag() {
    $$('.pf-sort-th', container).forEach(th => {
      th.addEventListener('click', e => {
        if (e.ctrlKey || e.metaKey) { e.preventDefault(); _openColumnFilter(th.dataset.sort, th); return; }
        const key = th.dataset.sort;
        if (sortColumn === key) { sortAsc = !sortAsc; }
        else { sortColumn = key; sortAsc = true; }
        render();
      });
      th.addEventListener('contextmenu', e => { e.preventDefault(); _openColumnFilter(th.dataset.sort, th); });
      // Drag
      th.addEventListener('dragstart', e => { e.dataTransfer.setData('text/plain', th.dataset.sort); th.style.opacity = '0.4'; });
      th.addEventListener('dragend', e => { th.style.opacity = '1'; });
      th.addEventListener('dragover', e => { e.preventDefault(); th.classList.add('pf-drag-over'); });
      th.addEventListener('dragleave', e => th.classList.remove('pf-drag-over'));
      th.addEventListener('drop', e => {
        e.preventDefault(); th.classList.remove('pf-drag-over');
        const from = e.dataTransfer.getData('text/plain');
        const to = th.dataset.sort;
        if (from && to && from !== to) {
          const fi = columnOrder.indexOf(from), ti = columnOrder.indexOf(to);
          if (fi >= 0 && ti >= 0) { columnOrder.splice(fi, 1); columnOrder.splice(ti, 0, from); render(); }
        }
      });
    });
  }

  // ── Filter popup ──
  function _openColumnFilter(colKey, anchorEl) {
    _closeAllPopups();
    const col = _col(colKey); if (!col) return;
    const existing = columnFilters[colKey] || {};
    const popup = document.createElement('div');
    popup.className = 'pf-filter-popup';
    popup.id = tableId + '-filt';

    if (col.num) {
      const conds = (existing.conditions && existing.conditions.length) ? existing.conditions : [{ op: 'gte', val: null }];
      function renderNumRows() {
        const rows = popup.querySelector('#dt-filt-rows');
        if (!rows) return;
        let h = '';
        for (let i = 0; i < conds.length; i++) {
          const c = conds[i];
          h += '<div class="pf-filt-cond-row" style="display:flex;align-items:center;gap:4px;padding:2px 0;flex-shrink:0;">' +
            '<select class="pf-filt-cond-op" data-idx="' + i + '" style="padding:3px 4px;font-size:10px;background:var(--bg);color:var(--text);border:1px solid var(--line);border-radius:3px;">' +
              '<option value="eq"' + (c.op==='eq'?' selected':'') + '>=</option><option value="gte"' + (c.op==='gte'?' selected':'') + '>≥</option>' +
              '<option value="gt"' + (c.op==='gt'?' selected':'') + '>&gt;</option><option value="lte"' + (c.op==='lte'?' selected':'') + '>≤</option>' +
              '<option value="lt"' + (c.op==='lt'?' selected':'') + '>&lt;</option>' +
            '</select>' +
            '<input type="number" class="pf-filt-cond-val" data-idx="' + i + '" value="' + (c.val != null ? c.val : '') + '" placeholder="Value" step="any" style="flex:1;min-width:60px;padding:3px 6px;font-size:11px;background:var(--bg);color:var(--text);border:1px solid var(--line);border-radius:3px;" />' +
            '<button class="pf-filt-cond-rm" data-idx="' + i + '" style="background:none;border:none;cursor:pointer;color:var(--muted);font-size:14px;padding:0 4px;">×</button>' +
          '</div>';
        }
        rows.innerHTML = h;
      }
      popup.innerHTML = '<div style="font-weight:600;margin-bottom:6px;flex-shrink:0;">Filter: ' + col.label + '</div>' +
        '<div class="pf-filter-body"><div id="dt-filt-rows"></div>' +
        '<button class="btn-ghost btn-sm" id="dt-filt-add" style="font-size:10px;align-self:flex-start;margin-top:4px;">+ Add condition</button></div>' +
        '<div class="pf-filter-actions"><button class="btn-ghost btn-sm" id="dt-filt-apply" style="flex:1;">Apply</button><button class="btn-ghost btn-sm" id="dt-filt-clear" style="flex:1;">Clear</button></div>';
      document.body.appendChild(popup);
      _setupFilterResize(popup);
      renderNumRows();

      const pos = anchorEl.getBoundingClientRect();
      popup.style.left = Math.min(pos.left, window.innerWidth - 330) + 'px';
      popup.style.top = (pos.bottom + 4) + 'px';

      popup.addEventListener('change', function(e) { if (e.target.classList.contains('pf-filt-cond-op')) conds[+e.target.dataset.idx].op = e.target.value; });
      popup.addEventListener('input', function(e) { if (e.target.classList.contains('pf-filt-cond-val')) conds[+e.target.dataset.idx].val = e.target.value; });
      popup.addEventListener('click', function(e) {
        if (e.target.classList.contains('pf-filt-cond-rm')) { e.stopPropagation(); e.preventDefault(); conds.splice(+e.target.dataset.idx, 1); renderNumRows(); }
      });
      popup.querySelector('#dt-filt-add').addEventListener('click', function() { conds.push({ op: 'gte', val: null }); renderNumRows(); });
      popup.querySelector('#dt-filt-apply').addEventListener('click', function() {
        const rows = popup.querySelectorAll('.pf-filt-cond-row');
        for (let i = 0; i < rows.length; i++) { conds[i].op = rows[i].querySelector('.pf-filt-cond-op').value; conds[i].val = rows[i].querySelector('.pf-filt-cond-val').value; }
        const active = conds.filter(c => c.val !== null && c.val !== '');
        if (active.length === 0) delete columnFilters[colKey]; else columnFilters[colKey] = { type: 'num', conditions: active };
        render();
      });
      popup.querySelector('#dt-filt-clear').addEventListener('click', function() { delete columnFilters[colKey]; render(); });
      popup.addEventListener('keydown', function(e) { if (e.key === 'Enter') popup.querySelector('#dt-filt-apply').click(); });
    } else {
      // Text filter
      const allVals = new Set(); let hasNulls = false;
      for (const row of data) { const v = col.get(row); if (v != null && v !== '') allVals.add(String(v)); else hasNulls = true; }
      const sorted = [...allVals].sort();
      const selected = existing.values || allVals;
      const includeNulls = existing.hasOwnProperty('includeNulls') ? existing.includeNulls : true;
      let nullsHtml = '';
      if (hasNulls) nullsHtml = '<label class="pf-check-row" style="border-bottom:1px solid var(--line);margin-bottom:2px;padding-bottom:4px;"><input type="checkbox" id="dt-filt-nulls" ' + (includeNulls ? 'checked' : '') + ' /><span style="color:var(--muted);font-style:italic;">— (no data)</span></label>';

      popup.innerHTML = '<div style="font-weight:600;margin-bottom:6px;flex-shrink:0;">Filter: ' + col.label + '</div>' +
        '<div class="pf-filter-body">' +
        '<input type="text" id="dt-filt-search" placeholder="Search…" style="width:100%;box-sizing:border-box;margin-bottom:4px;padding:4px 6px;font-size:11px;background:var(--bg);color:var(--text);border:1px solid var(--line);border-radius:3px;flex-shrink:0;" />' +
        '<div class="pf-filter-row" style="margin-bottom:4px;flex-shrink:0;"><input type="text" id="dt-filt-wc" placeholder="Wildcard e.g. *.T" value="' + (existing.wildcard || '') + '" /><select id="dt-filt-wcm" style="width:60px;"><option value="contains">has</option><option value="start">→</option><option value="end">←</option><option value="regex">~</option></select></div>' +
        '<div class="pf-filter-values" id="dt-filt-vals">' + nullsHtml +
          sorted.map(function(v) { return '<label class="pf-check-row"><input type="checkbox" data-val="' + v.replace(/"/g,'&quot;') + '" ' + (selected.has(v)?'checked':'') + ' /><span>' + v + '</span></label>'; }).join('') +
        '</div></div>' +
        '<div class="pf-filter-actions"><button class="btn-ghost btn-sm" id="dt-filt-apply" style="flex:1;">Apply</button><button class="btn-ghost btn-sm" id="dt-filt-all" style="flex:1;font-size:10px;">All</button><button class="btn-ghost btn-sm" id="dt-filt-none" style="flex:1;font-size:10px;">None</button></div>';
      document.body.appendChild(popup);
      _setupFilterResize(popup);
      if (existing.wcMode) popup.querySelector('#dt-filt-wcm').value = existing.wcMode;
      const pos = anchorEl.getBoundingClientRect();
      popup.style.left = Math.min(pos.left, window.innerWidth - 330) + 'px';
      popup.style.top = (pos.bottom + 4) + 'px';

      const apply = () => {
        const sel = new Set();
        popup.querySelectorAll('#dt-filt-vals input[type=checkbox]:checked').forEach(cb => { if (cb.id !== 'dt-filt-nulls') sel.add(cb.dataset.val); });
        const ncb = popup.querySelector('#dt-filt-nulls'); const incNulls = ncb ? ncb.checked : true;
        const wc = popup.querySelector('#dt-filt-wc')?.value || ''; const wcm = popup.querySelector('#dt-filt-wcm')?.value || 'contains';
        if (sel.size === 0 && !wc && !incNulls) { columnFilters[colKey] = { type: 'text', values: sel, includeNulls: false }; }
        else if (sel.size === allVals.size && !wc && incNulls) { delete columnFilters[colKey]; }
        else { columnFilters[colKey] = { type: 'text', values: sel, wildcard: wc || null, wcMode: wcm, includeNulls: incNulls }; }
        render();
      };
      popup.querySelector('#dt-filt-apply').addEventListener('click', apply);
      popup.querySelector('#dt-filt-all').addEventListener('click', () => popup.querySelectorAll('#dt-filt-vals input[type=checkbox]').forEach(cb => cb.checked = true));
      popup.querySelector('#dt-filt-none').addEventListener('click', () => popup.querySelectorAll('#dt-filt-vals input[type=checkbox]').forEach(cb => cb.checked = false));
      popup.querySelector('#dt-filt-search').addEventListener('input', () => {
        const q = popup.querySelector('#dt-filt-search').value.toLowerCase();
        popup.querySelectorAll('#dt-filt-vals .pf-check-row').forEach(row => { row.style.display = (row.querySelector('span')?.textContent || '').toLowerCase().includes(q) ? '' : 'none'; });
      });
      popup.addEventListener('keydown', function(e) { if (e.key === 'Enter') popup.querySelector('#dt-filt-apply').click(); });
    }
  }

  function _setupFilterResize(popup) {
    var ro = new ResizeObserver(function(entries) {
      for (var i = 0; i < entries.length; i++) {
        var e = entries[i]; var h = e.contentRect.height;
        var vals = e.target.querySelector('.pf-filter-values');
        if (!vals) return;
        var header = e.target.querySelector(':scope > div:first-child');
        var actions = e.target.querySelector('.pf-filter-actions');
        var search = e.target.querySelector('input[type=text]');
        var wcRow = e.target.querySelector('.pf-filter-row');
        var used = 20;
        if (header) used += header.offsetHeight;
        if (search) used += search.offsetHeight + 4;
        if (wcRow && !wcRow.querySelector('.pf-filter-values')) used += wcRow.offsetHeight;
        if (actions) used += actions.offsetHeight;
        var avail = h - used; if (avail < 20) avail = 20;
        vals.style.maxHeight = avail + 'px';
      }
    });
    ro.observe(popup);
  }

  function _closeAllPopups() {
    const p = document.getElementById(tableId + '-filt');
    if (p) p.remove();
  }

  function _updateFilterIndicators() {
    $$('.pf-sort-th', container).forEach(th => {
      const key = th.dataset.sort;
      let icon = th.querySelector('.pf-filter-indicator');
      if (columnFilters[key] && !icon) {
        icon = document.createElement('span'); icon.className = 'pf-filter-indicator';
        icon.textContent = ' ⏏'; icon.style.cssText = 'color:var(--accent,#58a6ff);font-size:10px;';
        th.appendChild(icon);
      } else if (!columnFilters[key] && icon) { icon.remove(); }
    });
  }

  document.addEventListener('click', function(e) {
    const popup = document.getElementById(tableId + '-filt');
    if (popup && !popup.contains(e.target) && !e.target.closest('.pf-sort-th')) { popup.remove(); }
  });

  // ── Formatting ──
  function _fmtMoney(v) {
    if (v == null || isNaN(v)) return '—';
    if (Math.abs(v) >= 1e6) return (v / 1e6).toFixed(2) + 'M';
    if (Math.abs(v) >= 1e3) return (v / 1e3).toFixed(1) + 'K';
    return v.toFixed(2);
  }

  // ── Public API ──

  function setData(newData) {
    data = newData;
    render();
  }

  async function load() {
    _showLoading();
    const minDelay = new Promise(r => setTimeout(r, 300));
    try {
      if (config.dataSource) {
        const result = await Promise.all([config.dataSource(), minDelay]).then(r => r[0]);
        data = result;
      }
    } catch (e) {
      container.innerHTML = '<span class="status-text error">' + (e.message || 'Load failed') + '</span>';
      return;
    }
    render();
    if (config.onLoaded) config.onLoaded(data);
  }

  function getFilterState() { return { filters: { ...columnFilters }, sortColumn, sortAsc, visibleColumns: visibleColumns ? new Set(visibleColumns) : null, pinnedColumns: new Set(pinnedColumns) }; }
  function setFilterState(s) { if (s.filters) columnFilters = { ...s.filters }; if (s.sortColumn) sortColumn = s.sortColumn; if (s.sortAsc !== undefined) sortAsc = s.sortAsc; if (s.visibleColumns) visibleColumns = new Set(s.visibleColumns); if (s.pinnedColumns) pinnedColumns = new Set(s.pinnedColumns); render(); }
  function setColumnFilters(f) { columnFilters = f; render(); }
  function getData() { return data; }

  // Auto-render if initialData was provided
  if (config.initialData && config.initialData.length) render();

  return { load, render, setData, getData, getFilterState, setFilterState, setColumnFilters };
}
