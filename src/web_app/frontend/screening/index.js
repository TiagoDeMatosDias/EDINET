/**
 * Screening — expression-bar criteria builder.
 *
 * Matches the mockup: Run/Save/Load, Screening Date, Criteria, Columns.
 * DB auto-loads from default. No valuation, no period, no DB picker.
 *
 * Criteria rendered as bracketed expressions:
 *   [ [[Table].[Column]]  >  [[value]] ]
 *   [ [[Table].[Column]]  >  [[Table2].[Col] - [[0.05]]] ]
 *   [ [[Table].[Column]]  IN  [["A"], ["B"]] ]
 */

import { el, $, fetchJson } from '../common/utils.js';
import { log } from '../common/console.js';

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

const ST = {
  dbPath: '',
  availableMetrics: {},
  screeningDate: '',
  criteria: [],
  columns: [],        // unified: {id, kind:'col'|'comp', ref?, name?, ...}
  prebuiltFormulas: [],
  sortBy: '',
  sortOrder: 'DESC',
  formattedValues: true,
  results: null,
  resultsLoading: false,
  sqlDisplay: '',
  _nextId: 1,
};

function uid() { return String(ST._nextId++); }

// Helpers to extract col refs / computed specs from the unified columns array
function colRefs() { return ST.columns.filter(c => c.kind === 'col').map(c => c.ref); }

// Table alias mapping — must match _TABLE_ALIAS in screening.py
const _ALIAS = {
  FinancialStatements: 'f', CompanyInfo: 'c', Stock_Prices: 's_p',
  PerShare: 'ps', Valuation: 'v', Quality: 'q',
  IncomeStatement: 'i', BalanceSheet: 'b', CashflowStatement: 'cf',
  Pershare_Historical: 'psh', Valuation_Historical: 'vh', Quality_Historical: 'qh',
};

function compileTokensToSQL(tokens) {
  if (!tokens || !tokens.length) return '';
  const parts = [];
  for (const t of tokens) {
    if (t.type === 'column') {
      const alias = _ALIAS[t.table] || t.table;
      parts.push(`${alias}.[${(t.column || '').replace(/]/g, ']]')}]`);
    } else if (t.type === 'value') {
      parts.push(String(t.value ?? 0));
    } else if (t.type === 'op') {
      parts.push(t.op);
    }
  }
  return parts.join(' ');
}

function computedColSpecs() {
  return ST.columns.filter(c => c.kind === 'comp').map(cc => {
    // If the column has expression tokens, compile them to SQL
    if (cc._tokens && cc._tokens.length) {
      const sql = compileTokensToSQL(cc._tokens);
      return {
        name: cc.name,
        formula_type: 'custom',
        numerator_table: '', numerator_column: '',
        denominator_table: '', denominator_column: '',
        formula: sql || null,
      };
    }
    return {
      name: cc.name,
      formula_type: cc.formula_type || 'price_ratio',
      numerator_table: cc.numerator_table || '',
      numerator_column: cc.numerator_column || '',
      denominator_table: cc.denominator_table || '',
      denominator_column: cc.denominator_column || '',
      formula: cc.formula || null,
    };
  });
}

// ---------------------------------------------------------------------------
// API
// ---------------------------------------------------------------------------

async function loadMetrics(dbPath) {
  const d = await fetchJson(`/api/screening/metrics?db_path=${encodeURIComponent(dbPath)}`);
  ST.availableMetrics = d.tables || {};
}

async function defaultDbPath() {
  try { return (await fetchJson('/api/screening/db-path')).db_path || ''; } catch { return ''; }
}

async function apiSavedList() {
  try { return (await fetchJson('/api/screening/saved')).screenings || []; } catch { return []; }
}

// ---------------------------------------------------------------------------
// Shell
// ---------------------------------------------------------------------------

function buildShell() {
  const root = $('#screening-root');
  root.innerHTML = '';
  root.style.cssText = 'display:flex;flex-direction:column;height:100%;overflow:hidden;';

  // ── Config area ──
  const cfg = el('div', { id: 'scr-cfg', class: 'scr-cfg' });

  // Row 1: actions + details toggle
  cfg.append(
    el('div', { class: 'scr-row-actions' },
      el('button', { id: 'scr-btn-run', class: 'scr-btn-run', text: 'Run' }),
      el('button', { id: 'scr-btn-save', class: 'scr-btn-soft', text: 'Save' }),
      el('button', { id: 'scr-btn-load', class: 'scr-btn-soft', text: 'Load' }),
    ),
  );

  // Row 2: Collapsible screening details (date, criteria, computed columns, columns)
  const detailsEl = el('details', { id: 'scr-details', class: 'scr-details', open: 'open' },
    el('summary', { id: 'scr-details-toggle', class: 'scr-details-toggle', text: 'Screening Details ▾' }),
    el('div', { id: 'scr-details-body', class: 'scr-details-body' },
      // Date
      el('div', { class: 'scr-row-date' },
        el('label', { class: 'scr-lbl', text: 'Screening Date:' }),
        el('input', { id: 'scr-date', class: 'scr-input-date', type: 'date' }),
        el('span', { id: 'scr-status', class: 'scr-status' }),
      ),
      // Criteria
      el('div', { class: 'scr-section' },
        el('div', { class: 'scr-section-head' }, el('span', { text: 'Criteria:' })),
        el('div', { id: 'scr-criteria' }),
        el('div', { id: 'scr-crit-builder' }),
        el('button', { id: 'scr-add-crit', class: 'scr-btn-add', text: '+ Add Criteria' }),
      ),
      // Columns (unified: regular + computed)
      el('div', { class: 'scr-section' },
        el('div', { class: 'scr-section-head' }, el('span', { text: 'Columns:' })),
        el('div', { id: 'scr-columns' }),
        el('span', { class: 'scr-add-row' },
          el('button', { id: 'scr-add-col', class: 'scr-btn-add', text: '+ Add Column' }),
          el('button', { id: 'scr-add-comp', class: 'scr-btn-add', text: '+ Add Computed' }),
        ),
      ),
    ),
  );
  cfg.append(detailsEl);

  // Row 3: Results toolbar
  cfg.append(
    el('div', { class: 'scr-row-bottom' },
      el('button', { id: 'scr-btn-update-prices', class: 'scr-btn-soft', text: 'Update Prices' }),
      el('button', { id: 'scr-btn-export', class: 'scr-btn-soft', text: 'Export CSV' }),
      el('button', { id: 'scr-btn-export-bt', class: 'scr-btn-soft', text: 'Export Backtest' }),
      el('button', { id: 'scr-btn-backtest', class: 'scr-btn-soft', text: 'Backtest →' }),
      el('label', { class: 'scr-toggle' },
        el('input', { id: 'scr-fmt', type: 'checkbox', checked: 'checked' }),
        el('span', { text: 'Formatted' }),
      ),
      el('span', { id: 'scr-count' }),
    ),
  );

  // ── Results table ──
  const res = el('div', { class: 'scr-results' },
    el('div', { class: 'scr-table-wrap' },
      el('table', { id: 'scr-table', class: 'scr-table' },
        el('thead', { id: 'scr-thead' }),
        el('tbody', { id: 'scr-tbody' }),
      ),
    ),
    el('details', { id: 'scr-sql-section', class: 'scr-sql-section', style: 'display:none;' },
      el('summary', { class: 'scr-sql-toggle', text: 'SQL' }),
      el('pre', { id: 'scr-sql-text', class: 'scr-sql-text' }),
    ),
  );

  root.append(cfg, res);
  wireShell();
}

// ---------------------------------------------------------------------------
// Wire
// ---------------------------------------------------------------------------

function wireShell() {
  $('#scr-btn-run').addEventListener('click', run);
  $('#scr-btn-save').addEventListener('click', save);
  $('#scr-btn-load').addEventListener('click', load);
  $('#scr-btn-export').addEventListener('click', exportCSV);
  $('#scr-btn-export-bt').addEventListener('click', exportBacktest);
  $('#scr-btn-backtest').addEventListener('click', openInBacktesting);
  $('#scr-btn-update-prices').addEventListener('click', updatePrices);
  $('#scr-date').addEventListener('change', () => { ST.screeningDate = $('#scr-date').value; });
  $('#scr-fmt').addEventListener('change', () => { ST.formattedValues = $('#scr-fmt').checked; renderResults(); });
  $('#scr-add-crit').addEventListener('click', showCritBuilder);
  $('#scr-add-col').addEventListener('click', showColPicker);
  $('#scr-add-comp').addEventListener('click', showComputedColBuilder);

  // Update details summary text when toggled
  const detailsEl = $('#scr-details');
  if (detailsEl) {
    detailsEl.addEventListener('toggle', () => {
      const s = $('#scr-details-toggle');
      if (s) s.textContent = detailsEl.open ? 'Screening Details ▾' : 'Screening Details ▸';
    });
  }

  // Cache state on page unload (belt-and-suspenders)
  window.addEventListener('beforeunload', () => cacheState());
}

// ---------------------------------------------------------------------------
// Session cache — persists state across page navigations
// ---------------------------------------------------------------------------

const CACHE_KEY = 'screening_state';

function cacheState() {
  try {
    const data = {
      dbPath: ST.dbPath,
      availableMetrics: ST.availableMetrics,
      screeningDate: ST.screeningDate,
      criteria: JSON.parse(JSON.stringify(ST.criteria)),
      columns: ST.columns.map(c => c.kind === 'comp' ? {
        id: c.id, kind: 'comp', name: c.name, formula_type: c.formula_type,
        numerator_table: c.numerator_table, numerator_column: c.numerator_column,
        denominator_table: c.denominator_table, denominator_column: c.denominator_column,
        formula: c.formula, _hint: c._hint, _tokens: c._tokens,
      } : { id: c.id, kind: 'col', ref: c.ref }),
      sortBy: ST.sortBy,
      sortOrder: ST.sortOrder,
      formattedValues: ST.formattedValues,
      results: ST.results ? {
        columns: ST.results.columns,
        rows: ST.results.rows,
        row_count: ST.results.row_count,
      } : null,
      sqlDisplay: ST.sqlDisplay,
      _nextId: ST._nextId,
      prebuiltFormulas: ST.prebuiltFormulas,
    };
    sessionStorage.setItem(CACHE_KEY, JSON.stringify(data));
  } catch { /* quota exceeded — silently ignore */ }
}

function restoreCachedState() {
  try {
    const raw = sessionStorage.getItem(CACHE_KEY);
    if (!raw) return false;
    const data = JSON.parse(raw);
    if (!data || !data.dbPath) return false;

    ST.dbPath = data.dbPath;
    ST.availableMetrics = data.availableMetrics || {};
    ST.screeningDate = data.screeningDate || '';
    ST.criteria = (data.criteria || []).map(c => ({ id: uid(), ...c }));
    // Restore unified columns (handles both old format {id,ref} and new format {id,kind,...})
    ST.columns = (data.columns || []).map(c => {
      if (c.kind === 'comp') {
        return {
          id: c.id || uid(), kind: 'comp', name: c.name,
          formula_type: c.formula_type || 'price_ratio',
          numerator_table: c.numerator_table || '',
          numerator_column: c.numerator_column || '',
          denominator_table: c.denominator_table || '',
          denominator_column: c.denominator_column || '',
          formula: c.formula || null, _hint: c._hint || '',
        };
      }
      return { id: c.id || uid(), kind: 'col', ref: c.ref || '' };
    });
    ST.sortBy = data.sortBy || '';
    ST.sortOrder = data.sortOrder || 'DESC';
    ST.formattedValues = data.formattedValues !== false;
    ST.results = data.results || null;
    ST.sqlDisplay = data.sqlDisplay || '';
    ST._nextId = data._nextId || 1;
    ST.prebuiltFormulas = data.prebuiltFormulas || [];

    if (ST.screeningDate && $('#scr-date')) $('#scr-date').value = ST.screeningDate;
    if ($('#scr-fmt')) $('#scr-fmt').checked = ST.formattedValues;

    return true;
  } catch { return false; }
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

async function init() {
  // Try restoring cached state first (survives page navigation)
  const cached = restoreCachedState();

  if (cached && ST.results) {
    // We have cached results — show them immediately
    renderAll();
    const cnt = $('#scr-count');
    if (cnt) cnt.textContent = ST.results.row_count ? `${ST.results.row_count} companies` : 'No results';
    status('Restored from cache');
    log('info', `DB: ${ST.dbPath} (cached)`);

    // Refresh metrics in background for column picker freshness
    try {
      await loadMetrics(ST.dbPath);
      if (!ST.prebuiltFormulas.length) {
        const f = await fetchJson('/api/screening/formulas');
        ST.prebuiltFormulas = f.formulas || [];
      }
    } catch {}
    return;
  }

  status('Loading database…');
  const path = ST.dbPath || await defaultDbPath();
  if (!path) { status('No database found'); return; }

  ST.dbPath = path;
  try {
    await loadMetrics(path);
    if (ST.columns.length === 0) setDefaultColumns();

    // Load prebuilt formula definitions
    if (!ST.prebuiltFormulas.length) {
      try {
        const f = await fetchJson('/api/screening/formulas');
        ST.prebuiltFormulas = f.formulas || [];
      } catch {} // formulas are optional — silent fail
    }

    status(`${Object.keys(ST.availableMetrics).length} tables loaded`);
    log('info', `DB: ${path}`);
    renderAll();
  } catch (e) {
    status(`Error: ${e.message}`);
    log('error', `DB load: ${e.message}`);
  }
}

function setDefaultColumns() {
  const ci = ST.availableMetrics.CompanyInfo || [];
  const refs = [];
  const ec = ci.find(c => /edinetcode/i.test(c)) || (ci.includes('edinetCode') ? 'edinetCode' : null);
  if (ec) refs.push(`CompanyInfo.${ec}`);
  const tc = ci.find(c => /company_ticker/i.test(c)) || ci.find(c => c === 'Ticker');
  if (tc) refs.push(`CompanyInfo.${tc}`);
  const nc = ci.find(c => /name|submitter|filer/i.test(c));
  if (nc) refs.push(`CompanyInfo.${nc}`);
  const ic = ci.find(c => /industry|sector/i.test(c));
  if (ic) refs.push(`CompanyInfo.${ic}`);
  ST.columns = refs.map(r => ({ id: uid(), kind: 'col', ref: r }));
}

function status(msg) {
  const el = $('#scr-status'); if (el) el.textContent = msg;
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

function renderAll() {
  renderCriteria();
  renderColumns();
  renderResults();
}

// ---------------------------------------------------------------------------
// CRITERIA — expression bars
// ---------------------------------------------------------------------------

function renderCriteria() {
  const ctr = $('#scr-criteria'); if (!ctr) return;
  ctr.innerHTML = '';
  for (const c of ST.criteria) ctr.append(buildExprBar(c));
}

function buildExprBar(crit) {
  const bar = el('div', { class: 'scr-expr' });
  const mode = crit.comparison_mode || 'fixed';
  const op = crit.operator;

  // Opening bracket
  bar.append(el('span', { class: 'scr-brk', text: '[' }));

  if (mode === 'full_expression') {
    // Free-form expression on both sides — render tokens directly
    const leftTokens = crit.left_side || [];
    if (leftTokens.length) bar.append(renderTokenList(leftTokens, crit, 'left_side'));
    else bar.append(el('span', { class: 'scr-tok-col', text: '[[?]]' }));
    bar.append(tokenOp(crit));
    const rightTokens = crit.right_side || [];
    const isNullOp = crit.operator === 'IS' || crit.operator === 'IS NOT';
    if (rightTokens.length) bar.append(renderTokenList(rightTokens, crit, 'right_side'));
    else bar.append(el('span', { class: 'scr-tok-val', text: isNullOp ? 'NULL' : '[[?]]' }));
  } else {
    // Standard modes: left column + operator + mode-specific right side
    bar.append(tokenCol(crit, 'table', 'column'));
    bar.append(tokenOp(crit));

    if (mode === 'fixed') {
    if (op === 'BETWEEN') {
      bar.append(tokenVal(crit, 'value'));
      bar.append(el('span', { class: 'scr-word', text: 'AND' }));
      bar.append(tokenVal(crit, 'value2'));
    } else {
      bar.append(tokenVal(crit, 'value'));
    }
  } else if (mode === 'column') {
    bar.append(
      el('span', { class: 'scr-brk', text: '[' }),
      tokenColRef(crit, 'compare_table', 'compare_column'),
    );
    if (crit.offset != null) {
      bar.append(el('span', { class: 'scr-op', text: '-' }));
      bar.append(tokenVal(crit, 'offset'));
    }
    bar.append(el('span', { class: 'scr-brk', text: ']' }));
  } else if (mode === 'expression') {
    bar.append(el('span', { class: 'scr-brk', text: '[' }));
    const tokens = crit.right_side || [];
    for (const t of tokens) {
      if (t.type === 'value') bar.append(tokenValObj(t));
      else if (t.type === 'column') bar.append(tokenColRef(t, 'table', 'column'));
      else if (t.type === 'op') bar.append(el('span', { class: 'scr-op', text: t.op }));
    }
    bar.append(el('span', { class: 'scr-brk', text: ']' }));
  } else if (mode === 'stock_price') {
    // Show the left-side expression if present
    if (crit.left_expression) {
      const exprSpan = el('span', { class: 'scr-tok-val', text: `[[${crit.left_expression}]]` });
      exprSpan.addEventListener('click', () => {
        const inp = el('input', { class: 'scr-inp', type: 'text', value: crit.left_expression || '', placeholder: 'e.g. / 2' });
        exprSpan.innerHTML = ''; exprSpan.append(inp); inp.focus();
        inp.addEventListener('blur', () => { crit.left_expression = inp.value.trim(); renderCriteria(); });
        inp.addEventListener('keydown', (e) => { if (e.key === 'Enter') inp.blur(); });
      });
      bar.append(exprSpan);
    }
    // Right side: [Current Price] suffix
    bar.append(el('span', { class: 'scr-tok-col scr-tok-price', text: '[Current Price]' }));
  } else if (mode === 'in') {
    bar.append(el('span', { class: 'scr-brk', text: '[' }));
    const vals = crit.values || [];
    for (let i = 0; i < vals.length; i++) {
      if (i > 0) bar.append(el('span', { class: 'scr-punct', text: ',' }));
      bar.append(tokenInVal(crit, i));
    }
    const plus = el('button', { class: 'scr-plus', text: '+' });
    plus.addEventListener('click', () => { crit.values.push(''); renderCriteria(); });
    bar.append(plus);
    bar.append(el('span', { class: 'scr-brk', text: ']' }));
  } else if (mode === 'like') {
    bar.append(tokenLikeVal(crit));
  }
  }

  // Closing bracket
  bar.append(el('span', { class: 'scr-brk', text: ']' }));

  // Remove
  const rm = el('button', { class: 'scr-rm', text: '✕' });
  rm.addEventListener('click', (e) => { e.stopPropagation(); ST.criteria = ST.criteria.filter(c => c.id !== crit.id); renderCriteria(); });
  bar.append(rm);

  return bar;
}

// ── Token builders ──

function tokenCol(obj, tk, ck) {
  const t = obj[tk] || '?', c = obj[ck] || '?';
  const s = el('span', { class: 'scr-tok-col', text: `[[${t}].[${c}]]` });
  s.addEventListener('click', () => pickCol(obj, tk, ck, renderCriteria));
  return s;
}

function renderTokenList(tokens, owner, sideKey) {
  // Renders a list of expression tokens (columns, values, ops) as inline spans.
  // Each token is clickable to edit/change/remove it.
  const wrap = el('span', { class: 'scr-tok-list' });
  for (let i = 0; i < tokens.length; i++) {
    const t = tokens[i];
    if (t.type === 'column') {
      wrap.append(tokenCol(t, 'table', 'column'));
    } else if (t.type === 'value') {
      wrap.append(tokenValObj(t));
    } else if (t.type === 'op') {
      const opSpan = el('span', { class: 'scr-op', text: t.op, title: 'Click to change' });
      opSpan.addEventListener('click', () => showTokenOpMenu(opSpan, t, tokens, i, () => {
        owner[sideKey] = tokens; renderCriteria();
      }));
      wrap.append(opSpan);
    }
  }
  return wrap;
}

function tokenColRef(obj, tk, ck) {
  const t = obj[tk] || '?', c = obj[ck] || '?';
  const s = el('span', { class: 'scr-tok-col', text: `[[${t}].[${c}]]` });
  s.addEventListener('click', () => pickCol(obj, tk, ck, renderCriteria));
  return s;
}

function tokenVal(obj, key) {
  const v = obj[key];
  const ft = obj.field_type || 'num';
  const display = fmtTok(v, ft);
  const s = el('span', { class: 'scr-tok-val', text: `[[${display}]]` });
  s.addEventListener('click', () => editVal(obj, key, s, renderCriteria));
  return s;
}

function tokenValObj(tobj) {
  const display = fmtTok(tobj.value, 'num');
  const s = el('span', { class: 'scr-tok-val', text: `[[${display}]]` });
  s.addEventListener('click', () => {
    const inp = el('input', { class: 'scr-inp', type: 'text', value: tobj.value != null ? String(tobj.value) : '' });
    s.innerHTML = ''; s.append(inp); inp.focus();
    inp.addEventListener('blur', () => { const r = inp.value.trim(); tobj.value = r === '' ? null : (isNaN(Number(r)) ? r : Number(r)); renderCriteria(); });
    inp.addEventListener('keydown', (e) => { if (e.key === 'Enter') inp.blur(); });
  });
  return s;
}

function tokenInVal(crit, idx) {
  const vals = crit.values || [];
  const v = vals[idx] ?? '';
  const s = el('span', { class: 'scr-tok-val', text: `["${v}"]` });
  s.addEventListener('click', () => {
    const inp = el('input', { class: 'scr-inp', type: 'text', value: v });
    s.innerHTML = ''; s.append(inp); inp.focus();
    inp.addEventListener('blur', () => { vals[idx] = inp.value.trim(); renderCriteria(); });
    inp.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') inp.blur();
      if (e.key === 'Backspace' && inp.value === '' && vals.length > 1) { crit.values = vals.filter((_,i) => i !== idx); renderCriteria(); }
    });
  });
  return s;
}

function tokenLikeVal(crit) {
  const v = crit.value != null ? String(crit.value) : '';
  const s = el('span', { class: 'scr-tok-val', text: `[["${v}"]]` });
  s.addEventListener('click', () => {
    const inp = el('input', { class: 'scr-inp', type: 'text', value: v });
    s.innerHTML = ''; s.append(inp); inp.focus();
    inp.addEventListener('blur', () => { crit.value = inp.value.trim(); renderCriteria(); });
    inp.addEventListener('keydown', (e) => { if (e.key === 'Enter') inp.blur(); });
  });
  return s;
}

function tokenOp(crit) {
  const s = el('span', { class: 'scr-tok-op', text: crit.operator || '>' });
  s.addEventListener('click', () => showOpMenu(s, crit));
  return s;
}

function fmtTok(v, ft) {
  if (v === null || v === undefined) return '?';
  if (ft === 'text' || ft === 'industry') return String(v);
  if (ft === 'percent') return (Number(v) * 100).toFixed(1) + '%';
  if (typeof v === 'number') {
    if (Number.isInteger(v)) return String(v);
    return v.toFixed(4).replace(/0+$/, '').replace(/\.$/, '');
  }
  return String(v);
}

// ── Inline editing ──

function pickCol(obj, tk, ck, cb) {
  const ex = document.querySelector('.scr-pop'); if (ex) ex.remove();
  const pop = el('div', { class: 'scr-pop' });
  const tables = Object.keys(ST.availableMetrics);

  const tSel = el('select', { class: 'scr-pop-sel' });
  tSel.append(el('option', { value: '' }, '— table —'));
  for (const t of tables) tSel.append(el('option', { value: t, selected: obj[tk] === t ? '' : undefined }, t));

  const cSel = el('select', { class: 'scr-pop-sel' });
  cSel.append(el('option', { value: '' }, '— column —'));
  function fillCols() {
    cSel.innerHTML = '<option value="">— column —</option>';
    const cols = ST.availableMetrics[obj[tk]] || [];
    for (const c of cols) cSel.append(el('option', { value: c, selected: obj[ck] === c ? '' : undefined }, c));
  }
  fillCols();

  tSel.addEventListener('change', () => { obj[tk] = tSel.value; obj[ck] = ''; fillCols(); });
  cSel.addEventListener('change', () => { obj[ck] = cSel.value; pop.remove(); if (cb) cb(); });

  pop.append(tSel, cSel);
  document.body.append(pop);
  positionPop(pop);
  tSel.focus();
  autoClose(pop);
}

function showOpMenu(anchor, crit) {
  const ex = document.querySelector('.scr-pop'); if (ex) ex.remove();
  const pop = el('div', { class: 'scr-pop scr-pop-ops' });
  const restricted = crit.comparison_mode === 'stock_price' || crit.comparison_mode === 'full_expression';
  const allOps = ['>', '>=', '<', '<=', '=', '!=', 'BETWEEN', 'IN', 'LIKE', 'IS', 'IS NOT'];
  const ops = restricted
    ? ['>', '>=', '<', '<=', '=', '!=', 'IS', 'IS NOT']
    : allOps;
  for (const o of ops) {
    const btn = el('button', { class: 'scr-pop-op' + (o === crit.operator ? ' is-sel' : ''), text: o });
    btn.addEventListener('click', () => {
      crit.operator = o;
      if (o === 'IN') { crit.comparison_mode = 'in'; crit.values = crit.value != null ? [String(crit.value)] : ['']; }
      else if (o === 'LIKE') { crit.comparison_mode = 'like'; }
      else if (o === 'BETWEEN') { crit.comparison_mode = 'fixed'; }
      else if (o === 'IS' || o === 'IS NOT') { crit.right_side = []; }
      else if (crit.comparison_mode === 'in' || crit.comparison_mode === 'like') { crit.comparison_mode = 'fixed'; }
      pop.remove(); renderCriteria();
    });
    pop.append(btn);
  }
  document.body.append(pop);
  const r = anchor.getBoundingClientRect();
  pop.style.position = 'fixed'; pop.style.left = r.left + 'px'; pop.style.top = (r.bottom + 4) + 'px'; pop.style.zIndex = '1000';
  autoClose(pop);
}

function editVal(obj, key, span, cb) {
  const v = obj[key], ft = obj.field_type || 'num';
  const inp = el('input', { class: 'scr-inp', type: 'text', value: v != null ? String(v) : '', placeholder: ft === 'text' ? 'text' : 'number' });
  span.innerHTML = ''; span.append(inp); inp.focus();
  inp.addEventListener('blur', () => {
    const r = inp.value.trim();
    if (r === '') obj[key] = null;
    else if (ft === 'text' || ft === 'industry') obj[key] = r;
    else obj[key] = isNaN(Number(r)) ? r : Number(r);
    if (cb) cb();
  });
  inp.addEventListener('keydown', (e) => { if (e.key === 'Enter') inp.blur(); });
}

function positionPop(pop) {
  const r = (document.activeElement || {}).getBoundingClientRect?.() || { left: 100, bottom: 200 };
  pop.style.position = 'fixed';
  pop.style.left = Math.min(r.left, window.innerWidth - 300) + 'px';
  pop.style.top = Math.min(r.bottom + 4, window.innerHeight - 220) + 'px';
  pop.style.zIndex = '1000';
}

function autoClose(pop) {
  setTimeout(() => document.addEventListener('click', function f(e) {
    if (!e.composedPath().includes(pop)) { pop.remove(); document.removeEventListener('click', f); }
  }), 0);
}

function showTokenOpMenu(anchor, token, tokens, idx, cb) {
  const ex = document.querySelector('.scr-pop'); if (ex) ex.remove();
  const pop = el('div', { class: 'scr-pop scr-pop-ops' });
  for (const o of ['+', '-', '*', '/']) {
    const btn = el('button', { class: 'scr-pop-op' + (token.op === o ? ' is-sel' : ''), text: o });
    btn.addEventListener('click', () => { token.op = o; pop.remove(); if (cb) cb(); });
    pop.append(btn);
  }
  // Remove button
  const rmBtn = el('button', { class: 'scr-pop-op scr-pop-rm', text: '✕ Remove' });
  rmBtn.addEventListener('click', () => { tokens.splice(idx, 1); pop.remove(); if (cb) cb(); });
  pop.append(rmBtn);
  document.body.append(pop);
  const r = anchor.getBoundingClientRect();
  pop.style.position = 'fixed'; pop.style.left = r.left + 'px'; pop.style.top = (r.bottom + 4) + 'px'; pop.style.zIndex = '1000';
  autoClose(pop);
}

// ---------------------------------------------------------------------------
// Criteria builder (+ Add Criteria)
// ---------------------------------------------------------------------------

function showCritBuilder() {
  // Full-expression builder: freely build both left and right side token lists.
  // Tokens: column refs, numeric values, arithmetic operators (+, -, *, /).
  const ctr = $('#scr-crit-builder');
  if (!ctr) return;
  const tables = Object.keys(ST.availableMetrics);
  if (!tables.length) { status('No tables loaded'); return; }

  ctr.innerHTML = '';

  const leftTokens = [];
  const rightTokens = [];
  let cmpOp = '>';

  function renderBuilder() {
    ctr.innerHTML = '';

    // ── Left side ──
    const leftPane = el('div', { class: 'scr-bld-expr' });
    leftPane.append(el('div', { class: 'scr-bld-label', text: 'Left side:' }));
    const leftRow = el('div', { class: 'scr-bld-tokens' });
    renderTokenChips(leftTokens, leftRow);
    leftRow.append(buildTokenAddBtn(leftTokens, leftRow));
    leftPane.append(leftRow);

    // ── Operator ──
    const opPane = el('div', { class: 'scr-bld-op' });
    opPane.append(el('div', { class: 'scr-bld-label', text: 'Operator:' }));
    const opRow = el('select', { class: 'scr-bld-sel-scr-bld-op' });
    for (const o of ['>', '>=', '<', '<=', '=', '!=', 'IS', 'IS NOT']) {
      opRow.append(el('option', { value: o, selected: cmpOp === o ? '' : undefined }, o));
    }
    opRow.addEventListener('change', () => { cmpOp = opRow.value; });
    opPane.append(opRow);

    // ── Right side ──
    const rightPane = el('div', { class: 'scr-bld-expr' });
    rightPane.append(el('div', { class: 'scr-bld-label', text: 'Right side:' }));
    const rightRow = el('div', { class: 'scr-bld-tokens' });
    renderTokenChips(rightTokens, rightRow);
    rightRow.append(buildTokenAddBtn(rightTokens, rightRow));
    rightPane.append(rightRow);

    // ── Actions ──
    const cancelBtn = el('button', { class: 'scr-bld-cancel', text: 'Cancel' });
    cancelBtn.addEventListener('click', () => { ctr.innerHTML = ''; });

    const addBtn = el('button', { class: 'scr-bld-add', text: 'Add Criteria' });
    addBtn.addEventListener('click', () => {
      if (!leftTokens.length) return;
      // Right side optional for IS / IS NOT operators
      if (cmpOp !== 'IS' && cmpOp !== 'IS NOT' && !rightTokens.length) return;
      const crit = {
        id: uid(),
        comparison_mode: 'full_expression',
        operator: cmpOp,
        left_side: JSON.parse(JSON.stringify(leftTokens)),
        right_side: JSON.parse(JSON.stringify(rightTokens)),
      };
      ST.criteria.push(crit);
      ctr.innerHTML = '';
      renderCriteria();
    });

    const preview = el('div', { class: 'scr-bld-preview', text: previewText(leftTokens, cmpOp, rightTokens) });

    ctr.append(leftPane, opPane, rightPane, preview, el('div', { class: 'scr-bld-acts' }, cancelBtn, addBtn));
  }

  function renderTokenChips(tokens, row) {
    row.innerHTML = '';
    for (let i = 0; i < tokens.length; i++) {
      const t = tokens[i];
      let chip;
      if (t.type === 'column') {
        chip = el('span', { class: 'scr-chp scr-chp-col', text: `${t.table}.${t.column}` });
      } else if (t.type === 'value') {
        chip = el('span', { class: 'scr-chp scr-chp-val', text: String(t.value ?? '?') });
      } else if (t.type === 'op') {
        chip = el('span', { class: 'scr-chp scr-chp-op', text: t.op });
      }
      chip.addEventListener('click', () => editToken(tokens, i, renderBuilder));
      row.append(chip);
    }
  }

  function buildTokenAddBtn(tokens, row) {
    const btn = el('button', { class: 'scr-chp scr-chp-add', text: '+' });
    btn.addEventListener('click', (e) => {
      const ex = document.querySelector('.scr-pop'); if (ex) ex.remove();
      const pop = el('div', { class: 'scr-pop scr-pop-ops' });
      for (const item of [
        { label: 'Add Column…', action: () => pickTokenColumn(tokens, renderBuilder) },
        { label: 'Add Value…', action: () => pickTokenValue(tokens, renderBuilder) },
        { label: '+', action: () => { tokens.push({ type: 'op', op: '+' }); renderBuilder(); } },
        { label: '-', action: () => { tokens.push({ type: 'op', op: '-' }); renderBuilder(); } },
        { label: '*', action: () => { tokens.push({ type: 'op', op: '*' }); renderBuilder(); } },
        { label: '/', action: () => { tokens.push({ type: 'op', op: '/' }); renderBuilder(); } },
      ]) {
        const btn2 = el('button', { class: 'scr-pop-op', text: item.label });
        btn2.addEventListener('click', () => { pop.remove(); item.action(); });
        pop.append(btn2);
      }
      document.body.append(pop);
      const r = btn.getBoundingClientRect();
      pop.style.position = 'fixed'; pop.style.left = r.left + 'px'; pop.style.top = (r.bottom + 4) + 'px'; pop.style.zIndex = '1000';
      autoClose(pop);
    });
    return btn;
  }

  function editToken(tokens, idx, cb) {
    const t = tokens[idx];
    const ex = document.querySelector('.scr-pop'); if (ex) ex.remove();
    const pop = el('div', { class: 'scr-pop scr-pop-ops' });

    if (t.type === 'column') {
      const chg = el('button', { class: 'scr-pop-op', text: 'Change Column…' });
      chg.addEventListener('click', () => { pop.remove(); pickTokenColumnAt(tokens, idx, cb); });
      pop.append(chg);
    } else if (t.type === 'value') {
      const chg = el('button', { class: 'scr-pop-op', text: 'Edit Value…' });
      chg.addEventListener('click', () => { pop.remove(); pickTokenValueAt(tokens, idx, cb); });
      pop.append(chg);
    } else if (t.type === 'op') {
      for (const o of ['+', '-', '*', '/']) {
        const btn2 = el('button', { class: 'scr-pop-op' + (t.op === o ? ' is-sel' : ''), text: o });
        btn2.addEventListener('click', () => { t.op = o; pop.remove(); if (cb) cb(); });
        pop.append(btn2);
      }
    }

    const rm = el('button', { class: 'scr-pop-op scr-pop-rm', text: '✕ Remove' });
    rm.addEventListener('click', () => { tokens.splice(idx, 1); pop.remove(); if (cb) cb(); });
    pop.append(rm);

    document.body.append(pop);
    // Position near the builder
    pop.style.position = 'fixed'; pop.style.left = '200px'; pop.style.top = '300px'; pop.style.zIndex = '1000';
    autoClose(pop);
  }

  function pickTokenColumn(tokens, cb) {
    pickTokenColumnAt(tokens, tokens.length, cb);
  }

  function pickTokenColumnAt(tokens, idx, cb) {
    const ex = document.querySelector('.scr-pop'); if (ex) ex.remove();
    const pop = el('div', { class: 'scr-pop' });
    const tSel = el('select', { class: 'scr-pop-sel' });
    tSel.append(el('option', { value: '' }, '— table —'));
    for (const tb of tables) tSel.append(el('option', { value: tb }, tb));

    const cSel = el('select', { class: 'scr-pop-sel' });
    cSel.append(el('option', { value: '' }, '— column —'));
    tSel.addEventListener('change', () => {
      cSel.innerHTML = '<option value="">— column —</option>';
      for (const col of (ST.availableMetrics[tSel.value] || [])) cSel.append(el('option', { value: col }, col));
    });

    const ok = el('button', { class: 'scr-bld-add', text: 'OK' });
    ok.addEventListener('click', () => {
      if (!tSel.value || !cSel.value) return;
      const token = { type: 'column', table: tSel.value, column: cSel.value };
      if (idx < tokens.length) tokens[idx] = token; else tokens.push(token);
      pop.remove(); if (cb) cb();
    });

    pop.append(tSel, cSel, ok);
    document.body.append(pop);
    pop.style.position = 'fixed'; pop.style.left = '200px'; pop.style.top = '280px'; pop.style.zIndex = '1000';
    autoClose(pop);
  }

  function pickTokenValue(tokens, cb) {
    pickTokenValueAt(tokens, tokens.length, cb);
  }

  function pickTokenValueAt(tokens, idx, cb) {
    const ex = document.querySelector('.scr-pop'); if (ex) ex.remove();
    const pop = el('div', { class: 'scr-pop' });
    const inp = el('input', { class: 'scr-inp', type: 'text', placeholder: 'Number value…' });
    if (idx < tokens.length && tokens[idx].type === 'value') inp.value = String(tokens[idx].value ?? '');

    const ok = el('button', { class: 'scr-bld-add', text: 'OK' });
    ok.addEventListener('click', () => {
      const v = inp.value.trim();
      if (v === '') return;
      const num = isNaN(Number(v)) ? v : Number(v);
      const token = { type: 'value', value: num };
      if (idx < tokens.length) tokens[idx] = token; else tokens.push(token);
      pop.remove(); if (cb) cb();
    });

    pop.append(inp, ok);
    document.body.append(pop);
    pop.style.position = 'fixed'; pop.style.left = '200px'; pop.style.top = '300px'; pop.style.zIndex = '1000';
    autoClose(pop);
    setTimeout(() => inp.focus(), 50);
  }

  function previewText(left, op, right) {
    const fmt = (tok) => tok.type === 'column' ? `${tok.table}.${tok.column}`
      : tok.type === 'value' ? String(tok.value)
      : tok.op;
    const rightText = right.length ? right.map(fmt).join(' ') : (op === 'IS' || op === 'IS NOT' ? 'NULL' : '?');
    return `[ ${left.map(fmt).join(' ')}  ${op}  ${rightText} ]`;
  }

  renderBuilder();
}

// ---------------------------------------------------------------------------
// Column picker
// ---------------------------------------------------------------------------

function showColPicker() {
  const ex = document.querySelector('.scr-pop-col'); if (ex) { ex.remove(); return; }
  const pop = el('div', { class: 'scr-pop-col' });
  const inp = el('input', { class: 'scr-pick-srch', type: 'text', placeholder: 'Search columns…' });
  const list = el('div', { class: 'scr-pick-list' });

  function render(q) {
    list.innerHTML = '';
    const ql = (q || '').toLowerCase();
    const sel = new Set(colRefs());
    const groups = {};

    for (const [table, cols] of Object.entries(ST.availableMetrics)) {
      for (const col of cols) {
        const ref = `${table}.${col}`;
        if (ql && !ref.toLowerCase().includes(ql)) continue;
        if (!groups[table]) groups[table] = [];
        groups[table].push({ col, ref, checked: sel.has(ref) });
      }
    }

    if (!Object.keys(groups).length) {
      list.append(el('div', { class: 'scr-pick-empty', text: 'No matches' }));
      return;
    }

    for (const [table, items] of Object.entries(groups).sort()) {
      list.append(el('div', { class: 'scr-pick-grp', text: table }));
      for (const it of items) {
        const row = el('label', { class: 'scr-pick-row' },
          el('input', { type: 'checkbox', checked: it.checked ? 'checked' : undefined }),
          el('span', { text: it.col }),
        );
        row.querySelector('input').addEventListener('change', (e) => {
          if (e.target.checked) { if (!ST.columns.some(c => c.kind === 'col' && c.ref === it.ref)) ST.columns.push({ id: uid(), kind: 'col', ref: it.ref }); }
          else { ST.columns = ST.columns.filter(c => !(c.kind === 'col' && c.ref === it.ref)); }
          renderColumns();
        });
        list.append(row);
      }
    }
  }

  inp.addEventListener('input', () => render(inp.value));
  pop.append(inp, list);

  const anchor = $('#scr-add-col');
  if (anchor) anchor.insertAdjacentElement('afterend', pop);
  render('');
  inp.focus();

  setTimeout(() => document.addEventListener('click', function f(e) {
    if (!pop.contains(e.target) && e.target !== anchor) { pop.remove(); document.removeEventListener('click', f); }
  }), 0);
}

// ---------------------------------------------------------------------------
// Computed Column builder
// ---------------------------------------------------------------------------

function showComputedColBuilder() {
  const ex = document.querySelector('.scr-pop'); if (ex) ex.remove();
  const pop = el('div', { class: 'scr-pop scr-pop-comp' });
  const hdr = el('div', { class: 'scr-pop-hdr', text: 'Add Computed Column' });
  pop.append(hdr);

  // Tabs: Pre-built | Custom
  let tab = 'prebuilt';
  const tabRow = el('div', { class: 'scr-comp-tabs' });
  const preTab = el('button', { class: 'scr-comp-tab is-sel', text: 'Pre-built' });
  const custTab = el('button', { class: 'scr-comp-tab', text: 'Custom' });
  const body = el('div', { class: 'scr-comp-body' });

  function selectTab(t) {
    tab = t;
    preTab.className = 'scr-comp-tab' + (t === 'prebuilt' ? ' is-sel' : '');
    custTab.className = 'scr-comp-tab' + (t === 'custom' ? ' is-sel' : '');
    renderTab();
  }

  preTab.addEventListener('click', () => selectTab('prebuilt'));
  custTab.addEventListener('click', () => selectTab('custom'));
  tabRow.append(preTab, custTab);

  function renderTab() {
    body.innerHTML = '';
    if (tab === 'prebuilt') {
      if (!ST.prebuiltFormulas.length) {
        body.append(el('div', { class: 'scr-pick-empty', text: 'No pre-built formulas available' }));
        return;
      }
      for (const f of ST.prebuiltFormulas) {
        const row = el('div', { class: 'scr-comp-row' });
        const btn = el('button', { class: 'scr-comp-btn', text: f.name });
        btn.addEventListener('click', () => {
          ST.columns.push({
            id: uid(), kind: 'comp',
            name: f.name,
            formula_type: f.formula_type,
            numerator_table: f.numerator_table,
            numerator_column: f.numerator_column,
            denominator_table: f.denominator_table,
            denominator_column: f.denominator_column,
            formula: f.formula || null,
            _hint: `${f.numerator_table}.${f.numerator_column} / ${f.denominator_table}.${f.denominator_column}`,
          });
          pop.remove();
          renderColumns();
          reorderResults();
        });
        row.append(btn);
        const hint = el('span', { class: 'scr-comp-hint', text: `${f.numerator_table}.${f.numerator_column} ÷ ${f.denominator_table}.${f.denominator_column}` });
        row.append(hint);
        body.append(row);
      }
    } else {
      // Custom expression builder (token-based, like the criteria builder)
      const exprTokens = [];

      // Name
      const nameInp = el('input', { class: 'scr-inp scr-comp-inp', type: 'text', placeholder: 'Column name (e.g. My Ratio)' });
      body.append(el('div', { class: 'scr-comp-fld' }, el('label', { class: 'scr-comp-lbl', text: 'Name' }), nameInp));

      // Expression
      body.append(el('div', { class: 'scr-comp-fld' },
        el('label', { class: 'scr-comp-lbl', text: 'Expression' })));

      const tokenRow = el('div', { class: 'scr-bld-tokens' });
      const addTokBtn = el('button', { class: 'scr-chp scr-chp-add', text: '+' });
      const preview = el('div', { class: 'scr-bld-preview', text: '(empty expression)' });

      body.append(tokenRow, addTokBtn, preview);

      function refreshTokens() {
        tokenRow.innerHTML = '';
        for (let i = 0; i < exprTokens.length; i++) {
          const t = exprTokens[i];
          let chip;
          if (t.type === 'column') {
            chip = el('span', { class: 'scr-chp scr-chp-col', text: `${t.table}.${t.column}` });
          } else if (t.type === 'value') {
            chip = el('span', { class: 'scr-chp scr-chp-val', text: String(t.value ?? '?') });
          } else if (t.type === 'op') {
            chip = el('span', { class: 'scr-chp scr-chp-op', text: t.op });
          }
          chip.addEventListener('click', () => editExprToken(i));
          tokenRow.append(chip);
        }
        tokenRow.append(addTokBtn);
        preview.textContent = exprTokens.length
          ? compileTokensToSQL(exprTokens)
          : '(empty expression)';
      }

      function editExprToken(idx) {
        const t = exprTokens[idx];
        const exEl = pop.querySelector('.scr-pop'); if (exEl) exEl.remove();
        const epop = el('div', { class: 'scr-pop scr-pop-ops' });

        if (t.type === 'column') {
          const chg = el('button', { class: 'scr-pop-op', text: 'Change Column…' });
          chg.addEventListener('click', () => { epop.remove(); pickExprColumn(idx); });
          epop.append(chg);
        } else if (t.type === 'value') {
          const chg = el('button', { class: 'scr-pop-op', text: 'Edit Value…' });
          chg.addEventListener('click', () => { epop.remove(); pickExprValue(idx); });
          epop.append(chg);
        } else if (t.type === 'op') {
          for (const o of ['+', '-', '*', '/']) {
            const ob = el('button', { class: 'scr-pop-op' + (t.op === o ? ' is-sel' : ''), text: o });
            ob.addEventListener('click', () => { t.op = o; epop.remove(); refreshTokens(); });
            epop.append(ob);
          }
        }

        const rm = el('button', { class: 'scr-pop-op scr-pop-rm', text: '✕ Remove' });
        rm.addEventListener('click', () => { exprTokens.splice(idx, 1); epop.remove(); refreshTokens(); });
        epop.append(rm);

        pop.append(epop);
        epop.style.position = 'fixed'; epop.style.left = '240px'; epop.style.top = '200px'; epop.style.zIndex = '1001';
        autoClose(epop);
      }

      function pickExprColumn(idx) {
        const exEl = pop.querySelector('.scr-pop'); if (exEl) exEl.remove();
        const cpop = el('div', { class: 'scr-pop' });
        const tables = Object.keys(ST.availableMetrics);
        const tSel = el('select', { class: 'scr-pop-sel' });
        tSel.append(el('option', { value: '' }, '— table —'));
        for (const tb of tables) tSel.append(el('option', { value: tb }, tb));

        const cSel = el('select', { class: 'scr-pop-sel' });
        cSel.append(el('option', { value: '' }, '— column —'));
        tSel.addEventListener('change', () => {
          cSel.innerHTML = '<option value="">— column —</option>';
          for (const col of (ST.availableMetrics[tSel.value] || [])) cSel.append(el('option', { value: col }, col));
        });

        const ok = el('button', { class: 'scr-bld-add', text: 'OK' });
        ok.addEventListener('click', () => {
          if (!tSel.value || !cSel.value) return;
          const token = { type: 'column', table: tSel.value, column: cSel.value };
          if (idx < exprTokens.length) exprTokens[idx] = token; else exprTokens.push(token);
          cpop.remove(); refreshTokens();
        });

        cpop.append(tSel, cSel, ok);
        pop.append(cpop);
        cpop.style.position = 'fixed'; cpop.style.left = '240px'; cpop.style.top = '200px'; cpop.style.zIndex = '1001';
        autoClose(cpop);
      }

      function pickExprValue(idx) {
        const exEl = pop.querySelector('.scr-pop'); if (exEl) exEl.remove();
        const vpop = el('div', { class: 'scr-pop' });
        const inp = el('input', { class: 'scr-inp', type: 'text', placeholder: 'Number value…' });
        if (idx < exprTokens.length && exprTokens[idx].type === 'value') inp.value = String(exprTokens[idx].value ?? '');

        const ok = el('button', { class: 'scr-bld-add', text: 'OK' });
        ok.addEventListener('click', () => {
          const v = inp.value.trim();
          if (v === '') return;
          const num = isNaN(Number(v)) ? v : Number(v);
          const token = { type: 'value', value: num };
          if (idx < exprTokens.length) exprTokens[idx] = token; else exprTokens.push(token);
          vpop.remove(); refreshTokens();
        });

        vpop.append(inp, ok);
        pop.append(vpop);
        vpop.style.position = 'fixed'; vpop.style.left = '240px'; vpop.style.top = '200px'; vpop.style.zIndex = '1001';
        autoClose(vpop);
        setTimeout(() => inp.focus(), 50);
      }

      // + button popup
      addTokBtn.addEventListener('click', (e) => {
        const exEl = pop.querySelector('.scr-pop'); if (exEl) exEl.remove();
        const apop = el('div', { class: 'scr-pop scr-pop-ops' });
        for (const item of [
          { label: 'Add Column…', action: () => pickExprColumn(exprTokens.length) },
          { label: 'Add Value…', action: () => pickExprValue(exprTokens.length) },
          { label: '+', action: () => { exprTokens.push({ type: 'op', op: '+' }); refreshTokens(); } },
          { label: '-', action: () => { exprTokens.push({ type: 'op', op: '-' }); refreshTokens(); } },
          { label: '*', action: () => { exprTokens.push({ type: 'op', op: '*' }); refreshTokens(); } },
          { label: '/', action: () => { exprTokens.push({ type: 'op', op: '/' }); refreshTokens(); } },
        ]) {
          const btn2 = el('button', { class: 'scr-pop-op', text: item.label });
          btn2.addEventListener('click', () => { item.action(); queueMicrotask(() => apop.remove()); });
          apop.append(btn2);
        }
        pop.append(apop);
        const r = addTokBtn.getBoundingClientRect();
        apop.style.position = 'fixed'; apop.style.left = r.left + 'px'; apop.style.top = (r.bottom + 4) + 'px'; apop.style.zIndex = '1001';
        autoClose(apop);
      });

      // Add button
      const addBtn = el('button', { class: 'scr-bld-add', text: 'Add Computed Column' });
      addBtn.addEventListener('click', () => {
        const name = nameInp.value.trim();
        if (!name) return;
        if (!exprTokens.length) return;
        const entry = {
          id: uid(), kind: 'comp',
          name,
          formula_type: 'custom',
          _tokens: JSON.parse(JSON.stringify(exprTokens)),
          _hint: compileTokensToSQL(exprTokens),
        };
        ST.columns.push(entry);
        pop.remove();
        renderColumns();
        reorderResults();
      });
      body.append(el('div', { class: 'scr-comp-acts' }, addBtn));

      refreshTokens();
    }
  }

  pop.append(tabRow, body);
  document.body.append(pop);
  pop.style.position = 'fixed'; pop.style.left = '200px'; pop.style.top = '150px'; pop.style.zIndex = '1000';
  renderTab();
  autoClose(pop);
}

// ---------------------------------------------------------------------------
// Drag-and-drop helpers
// ---------------------------------------------------------------------------

let _drag = { kind: null, idx: -1 };

function _makeDraggable(bar, kind, idx, onReorder) {
  bar.draggable = true;
  bar.addEventListener('dragstart', (e) => {
    _drag = { kind, idx };
    bar.classList.add('is-dragging');
    e.dataTransfer.effectAllowed = 'move';
    e.dataTransfer.setData('text/plain', String(idx));
  });
  bar.addEventListener('dragend', () => {
    bar.classList.remove('is-dragging');
    document.querySelectorAll('.scr-col.is-over').forEach(el => el.classList.remove('is-over'));
    _drag = { kind: null, idx: -1 };
  });
  bar.addEventListener('dragover', (e) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'move';
    if (_drag.kind !== kind || _drag.idx === idx) return;
    bar.classList.add('is-over');
  });
  bar.addEventListener('dragleave', () => {
    bar.classList.remove('is-over');
  });
  bar.addEventListener('drop', (e) => {
    e.preventDefault();
    bar.classList.remove('is-over');
    if (_drag.kind !== kind || _drag.idx === idx) return;
    onReorder(_drag.idx, idx);
    _drag = { kind: null, idx: -1 };
  });
}

// ---------------------------------------------------------------------------
// Render columns
// ---------------------------------------------------------------------------

function renderColumns() {
  const ctr = $('#scr-columns'); if (!ctr) return;
  ctr.innerHTML = '';
  ST.columns.forEach((col, i) => {
    let bar;
    if (col.kind === 'comp') {
      bar = el('div', { class: 'scr-col scr-col-comp' },
        el('span', { class: 'scr-col-grip', text: '⠿' }),
        el('span', { class: 'scr-tok-comp', text: `[ ${col.name} ]` }),
        el('span', { class: 'scr-hint', text: col._hint || '' }),
        el('button', { class: 'scr-rm', text: '✕' }),
      );
    } else {
      const [table, column] = (col.ref || '?.?').split('.');
      bar = el('div', { class: 'scr-col' },
        el('span', { class: 'scr-col-grip', text: '⠿' }),
        el('span', { class: 'scr-tok-col', text: `[ [${table}].[${column}] ]` }),
        el('button', { class: 'scr-rm', text: '✕' }),
      );
    }
    bar.querySelector('.scr-rm').addEventListener('click', () => {
      ST.columns = ST.columns.filter(c => c.id !== col.id);
      renderColumns();
      reorderResults();
    });
    _makeDraggable(bar, 'col', i, (from, to) => {
      const item = ST.columns.splice(from, 1)[0];
      ST.columns.splice(to, 0, item);
      renderColumns();
      reorderResults();
    });
    ctr.append(bar);
  });
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

async function run() {
  if (!ST.dbPath || !Object.keys(ST.availableMetrics).length) { status('No database loaded'); return; }

  const criteria = ST.criteria.map(c => {
    const d = { table: c.table, column: c.column, operator: c.operator, comparison_mode: c.comparison_mode || 'fixed', value: c.value, field_type: c.field_type || 'num' };
    if (c.value2 != null) d.value2 = c.value2;
    if (c.comparison_mode === 'column') { d.compare_table = c.compare_table; d.compare_column = c.compare_column; if (c.offset != null) d.offset = c.offset; }
    if (c.comparison_mode === 'expression' && c.right_side) d.right_side = c.right_side;
    if (c.comparison_mode === 'stock_price' && c.left_expression) d.left_expression = c.left_expression;
    if (c.comparison_mode === 'full_expression') { d.left_side = c.left_side || []; d.right_side = c.right_side || []; }
    if (c.comparison_mode === 'in' && c.values) d.values = c.values;
    return d;
  });

  // Auto-include columns referenced in criteria
  const crSet = new Set(colRefs());
  for (const c of ST.criteria) {
    if (c.table && c.column) crSet.add(`${c.table}.${c.column}`);
    if (c.comparison_mode === 'column' && c.compare_table && c.compare_column) {
      crSet.add(`${c.compare_table}.${c.compare_column}`);
    }
    if (c.comparison_mode === 'full_expression') {
      for (const side of [c.left_side || [], c.right_side || []]) {
        for (const t of side) {
          if (t.type === 'column' && t.table && t.column) crSet.add(`${t.table}.${t.column}`);
        }
      }
    }
    if (c.comparison_mode === 'expression' && c.right_side) {
      for (const t of c.right_side) {
        if (t.type === 'column' && t.table && t.column) crSet.add(`${t.table}.${t.column}`);
      }
    }
  }
  const columns = [...crSet];

  const computedCols = computedColSpecs();

  ST.resultsLoading = true;
  status('Running…');
  renderResults();

  try {
    const data = await fetchJson('/api/screening/run', {
      method: 'POST', body: JSON.stringify({
        db_path: ST.dbPath, criteria, columns,
        computed_columns: computedCols,
        screening_date: ST.screeningDate || null,
        sort_by: ST.sortBy || null, sort_order: ST.sortOrder,
      }),
    });
    ST.results = data;
    ST.sqlDisplay = data.sql_display || '';
    cacheState();
    status('');
    const cnt = $('#scr-count'); if (cnt) cnt.textContent = data.row_count ? `${data.row_count} companies` : 'No results';
    log('info', `Screening: ${data.row_count} results`);
    renderResults();
  } catch (e) {
    status(`Error: ${e.message}`);
    log('error', `Screening: ${e.message}`);
    ST.results = null; renderResults();
  } finally { ST.resultsLoading = false; }
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

function renderResults() {
  const thead = $('#scr-thead'), tbody = $('#scr-tbody');
  if (!thead || !tbody) return;
  if (!ST.results || !ST.results.row_count) { thead.innerHTML = ''; tbody.innerHTML = ''; return; }

  // SQL display
  const sqlSection = $('#scr-sql-section');
  const sqlText = $('#scr-sql-text');
  if (sqlSection && sqlText && ST.sqlDisplay) {
    sqlSection.style.display = '';
    sqlText.textContent = ST.sqlDisplay;
  } else if (sqlSection) {
    sqlSection.style.display = 'none';
  }

  thead.innerHTML = '';
  const hr = el('tr');
  ST.results.columns.forEach((col, i) => {
    const th = el('th', { text: col, draggable: 'true' });
    th.addEventListener('click', () => sort(col));
    // Drag to reorder result columns + sync config
    th.addEventListener('dragstart', (e) => {
      _drag = { kind: 'head', idx: i };
      th.classList.add('is-dragging');
      e.dataTransfer.effectAllowed = 'move';
      e.dataTransfer.setData('text/plain', String(i));
    });
    th.addEventListener('dragend', () => {
      th.classList.remove('is-dragging');
      document.querySelectorAll('.scr-table th.is-over').forEach(el => el.classList.remove('is-over'));
      _drag = { kind: null, idx: -1 };
    });
    th.addEventListener('dragover', (e) => {
      e.preventDefault();
      e.dataTransfer.dropEffect = 'move';
      if (_drag.kind !== 'head' || _drag.idx === i) return;
      th.classList.add('is-over');
    });
    th.addEventListener('dragleave', () => { th.classList.remove('is-over'); });
    th.addEventListener('drop', (e) => {
      e.preventDefault();
      th.classList.remove('is-over');
      if (_drag.kind !== 'head' || _drag.idx === i) return;
      const fromIdx = _drag.idx, toIdx = i;
      const targetCol = ST.results.columns[toIdx]; // column originally at drop position

      // Move in results table
      const col = ST.results.columns.splice(fromIdx, 1)[0];
      ST.results.columns.splice(toIdx, 0, col);
      for (const row of ST.results.rows) {
        const val = row.splice(fromIdx, 1)[0];
        row.splice(toIdx, 0, val);
      }

      // Sync config: find config indices for moved column and target column
      const movedName = col;
      const movedCfgIdx = ST.columns.findIndex(c => (c.kind === 'comp' ? c.name : c.ref) === movedName);
      const targetCfgIdx = ST.columns.findIndex(c => (c.kind === 'comp' ? c.name : c.ref) === targetCol);

      if (movedCfgIdx >= 0) {
        const item = ST.columns.splice(movedCfgIdx, 1)[0];
        if (targetCfgIdx >= 0) {
          const insertAt = movedCfgIdx < targetCfgIdx ? targetCfgIdx : targetCfgIdx + 1;
          ST.columns.splice(insertAt, 0, item);
        } else {
          // Target has no config entry — place near neighbors
          const prevCol = toIdx > 0 ? ST.results.columns[toIdx - 1] : null;
          const nextCol = toIdx < ST.results.columns.length - 1 ? ST.results.columns[toIdx + 1] : null;
          let insertAt = ST.columns.length;
          if (prevCol) {
            const pi = ST.columns.findIndex(c => (c.kind === 'comp' ? c.name : c.ref) === prevCol);
            if (pi >= 0) insertAt = pi + 1;
          }
          if (insertAt === ST.columns.length && nextCol) {
            const ni = ST.columns.findIndex(c => (c.kind === 'comp' ? c.name : c.ref) === nextCol);
            if (ni >= 0) insertAt = ni;
          }
          ST.columns.splice(insertAt, 0, item);
        }
      }
      _drag = { kind: null, idx: -1 };
      renderResults();
      renderColumns();
    });
    hr.append(th);
  });
  thead.append(hr);

  tbody.innerHTML = '';
  for (const row of ST.results.rows) {
    const tr = el('tr');
    for (let i = 0; i < ST.results.columns.length; i++) {
      tr.append(el('td', { text: fmtVal(row[i], ST.results.columns[i]) }));
    }
    tr.addEventListener('click', () => drill(row));
    tbody.append(tr);
  }
}

function reorderResults() {
  if (!ST.results || !ST.results.columns.length) return;

  // Build the desired column order from the unified ST.columns
  const desired = ST.columns.map(c => c.kind === 'comp' ? c.name : c.ref);

  // Map each result column to its new index
  const oldCols = ST.results.columns;
  const indexMap = new Map();
  const seen = new Set();
  for (const d of desired) {
    // Try exact match first
    let found = -1;
    for (let i = 0; i < oldCols.length; i++) {
      if (oldCols[i] === d && !seen.has(i)) { found = i; break; }
    }
    // Try case-insensitive
    if (found === -1) {
      const dl = d.toLowerCase();
      for (let i = 0; i < oldCols.length; i++) {
        if (oldCols[i].toLowerCase() === dl && !seen.has(i)) { found = i; break; }
      }
    }
    if (found >= 0) {
      indexMap.set(found, indexMap.size);
      seen.add(found);
    }
  }
  // Append any remaining columns not in desired order
  for (let i = 0; i < oldCols.length; i++) {
    if (!seen.has(i)) { indexMap.set(i, indexMap.size); seen.add(i); }
  }

  // Build new column order
  const newCols = new Array(oldCols.length);
  for (const [oldIdx, newIdx] of indexMap) newCols[newIdx] = oldCols[oldIdx];

  // Reorder rows
  const newRows = ST.results.rows.map(row => {
    const newRow = new Array(oldCols.length);
    for (const [oldIdx, newIdx] of indexMap) newRow[newIdx] = row[oldIdx];
    return newRow;
  });

  ST.results.columns = newCols;
  ST.results.rows = newRows;
  renderResults();
}

function fmtVal(val, col) {
  if (val === null || val === undefined) return '—';
  if (!ST.formattedValues) return String(val);
  if (col === 'ScreeningRank') return String(Math.round(Number(val)));
  if (col === 'ScreeningScore') return Number(val).toFixed(3);
  if (typeof val === 'number') {
    const lo = String(col).toLowerCase();
    if (/margin|yield|payout|return|growth/i.test(lo)) return (val * 100).toFixed(2) + '%';
    if (/marketcap|enterprisevalue/i.test(lo)) return val.toLocaleString('en-US', { maximumFractionDigits: 0 });
    if (/price|shareprice/i.test(lo) && !/ratio/i.test(lo)) return val.toLocaleString('en-US', { maximumFractionDigits: 0 });
    if (/ratio|turnover|zscore/i.test(lo)) return Number(val).toFixed(2);
    const d = Math.abs(val - Math.round(val)) < 1e-9 ? 0 : 2;
    return val.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
  }
  return String(val);
}

function sort(col) {
  if (!ST.results?.rows) return;
  ST.sortBy = ST.sortBy === col ? '' : col;
  ST.sortOrder = ST.sortBy === col ? (ST.sortOrder === 'ASC' ? 'DESC' : 'ASC') : 'ASC';
  const idx = ST.results.columns.indexOf(col);
  if (idx === -1) return;
  const asc = ST.sortOrder === 'ASC';
  ST.results.rows.sort((a, b) => {
    const va = a[idx], vb = b[idx];
    if (va === null && vb === null) return 0;
    if (va === null) return 1; if (vb === null) return -1;
    if (typeof va === 'number' && typeof vb === 'number') return asc ? va - vb : vb - va;
    return asc ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va));
  });
  renderResults();
}

function drill(row) {
  if (!ST.results) return;
  let ei = -1, ti = -1;
  for (let i = 0; i < ST.results.columns.length; i++) {
    const c = ST.results.columns[i].toLowerCase();
    if (/edinetcode/.test(c)) ei = i;
    if (/company_ticker|^ticker$/.test(c)) ti = i;
  }
  const p = new URLSearchParams();
  if (ei >= 0 && row[ei]) p.set('edinet_code', String(row[ei]));
  if (ti >= 0 && row[ti]) p.set('ticker', String(row[ti]));
  // Store in sessionStorage for the security page to pick up (avoids exposing db_path in URL)
  if (ei >= 0 && row[ei]) sessionStorage.setItem('sa.lastEdinetCode', String(row[ei]));
  if (p.toString()) window.location.href = `/security?${p.toString()}`;
}

// ---------------------------------------------------------------------------
// Save / Load / Export
// ---------------------------------------------------------------------------

async function save() {
  const name = prompt('Name:');
  if (!name?.trim()) return;
  try {
    await fetchJson('/api/screening/save', {
      method: 'POST', body: JSON.stringify({
        name: name.trim(), criteria: ST.criteria, columns: colRefs(),
        computed_columns: computedColSpecs(),
        screening_date: ST.screeningDate || null,
      }),
    });
    log('info', `Saved "${name}"`);
  } catch (e) { log('error', `Save: ${e.message}`); }
}

async function load() {
  // Show a dropdown popup anchored to the Load button
  const anchor = $('#scr-btn-load');
  const ex = document.querySelector('.scr-pop'); if (ex) ex.remove();

  const pop = el('div', { class: 'scr-pop scr-pop-menu' });
  const hdr = el('div', { class: 'scr-pop-hdr', text: 'Load Screening' });
  pop.append(hdr);

  const names = await apiSavedList();
  if (!names.length) {
    pop.append(el('div', { class: 'scr-pop-empty', text: 'No saved screenings' }));
  } else {
    for (const name of names) {
      const row = el('div', { class: 'scr-pop-row' });
      const btn = el('button', { class: 'scr-pop-item', text: name });
      btn.addEventListener('click', async () => {
        pop.remove();
        try {
          const d = await fetchJson(`/api/screening/saved/${encodeURIComponent(name)}`);
          ST.criteria = (d.criteria || []).map(c => ({ id: uid(), ...c }));
          // Merge regular + computed columns into unified order (regular first, then computed)
          const regCols = (d.columns || []).map(ref => ({ id: uid(), kind: 'col', ref }));
          const compCols = (d.computed_columns || []).map(cc => ({
            id: uid(), kind: 'comp',
            name: cc.name,
            formula_type: cc.formula_type || 'price_ratio',
            numerator_table: cc.numerator_table || '',
            numerator_column: cc.numerator_column || '',
            denominator_table: cc.denominator_table || '',
            denominator_column: cc.denominator_column || '',
            formula: cc.formula || null,
            _hint: cc.formula ? 'custom SQL' : `${cc.numerator_table || '?'}.${cc.numerator_column || '?'} / ${cc.denominator_table || '?'}.${cc.denominator_column || '?'}`,
          }));
          ST.columns = [...regCols, ...compCols];
          ST.screeningDate = d.screening_date || '';
          if ($('#scr-date')) $('#scr-date').value = d.screening_date || '';
          renderAll();
          log('info', `Loaded "${name}"`);
        } catch (e) { log('error', `Load: ${e.message}`); }
      });
      row.append(btn);

      const del = el('button', { class: 'scr-pop-del', text: '✕', title: `Delete "${name}"` });
      del.addEventListener('click', async (e) => {
        e.stopPropagation();
        if (!confirm(`Delete "${name}"?`)) return;
        try {
          await fetchJson(`/api/screening/saved/${encodeURIComponent(name)}`, { method: 'DELETE' });
          log('info', `Deleted "${name}"`);
          pop.remove();
          load(); // reopen with updated list
        } catch (e) { log('error', `Delete: ${e.message}`); }
      });
      row.append(del);
      pop.append(row);
    }
  }

  document.body.append(pop);
  const r = anchor.getBoundingClientRect();
  pop.style.position = 'fixed'; pop.style.left = r.left + 'px'; pop.style.top = (r.bottom + 4) + 'px'; pop.style.zIndex = '1000';
  autoClose(pop);
}

async function updatePrices() {
  if (!ST.results?.row_count) { log('warn', 'No results to update'); return; }

  // Extract tickers from results
  let tickerIdx = -1;
  for (let i = 0; i < ST.results.columns.length; i++) {
    const c = ST.results.columns[i].toLowerCase();
    if (/company_ticker|^ticker$/.test(c)) { tickerIdx = i; break; }
  }
  if (tickerIdx === -1) {
    log('warn', 'No Ticker column in results — add CompanyInfo.Ticker to columns first');
    status('Add Ticker column to results first');
    return;
  }

  const tickers = [...new Set(ST.results.rows.map(r => {
    const v = r[tickerIdx];
    return v ? String(v).trim() : null;
  }).filter(Boolean))];

  if (!tickers.length) { log('warn', 'No tickers found in results'); return; }

  log('info', `Updating prices for ${tickers.length} tickers…`);
  const btn = $('#scr-btn-update-prices');
  if (btn) { btn.disabled = true; btn.textContent = 'Updating…'; }

  try {
    const data = await fetchJson('/api/screening/update-prices', {
      method: 'POST',
      body: JSON.stringify({ db_path: ST.dbPath, tickers }),
    });

    const results = data.results || [];
    const ok = results.filter(r => r.ok).length;
    const fail = results.filter(r => !r.ok).length;
    const inserted = results.reduce((s, r) => s + (r.rows_inserted || 0), 0);

    if (fail) {
      const failedTickers = results.filter(r => !r.ok).map(r => r.ticker).join(', ');
      log('warn', `Prices updated: ${ok} ok, ${fail} failed (${failedTickers})`);
    } else {
      log('info', `Prices updated for ${ok} tickers (${inserted} new rows)`);
    }

    // Re-run screening so price-dependent columns (e.g. P/E) reflect new prices
    if (inserted > 0) {
      await run();
    }
  } catch (e) {
    log('error', `Price update: ${e.message}`);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Update Prices'; }
  }
}

async function exportCSV() {
  if (!ST.results?.row_count) { log('warn', 'No results'); return; }
  try {
    const r = await fetch('/api/screening/export', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ db_path: ST.dbPath, criteria: ST.criteria, columns: colRefs(), computed_columns: computedColSpecs(), screening_date: ST.screeningDate || null, format: 'csv' }),
    });
    const blob = await r.blob();
    const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'screening.csv'; a.click();
  } catch (e) { log('error', `Export: ${e.message}`); }
}

// ---------------------------------------------------------------------------
// Export Backtest — equal-weight backtest CSV with benchmark + discount rate
// ---------------------------------------------------------------------------

function showBacktestExportPopup() {
  return new Promise((resolve) => {
    const ex = document.querySelector('.scr-pop'); if (ex) ex.remove();
    const pop = el('div', { class: 'scr-pop scr-pop-backtest' });

    pop.append(el('div', { class: 'scr-pop-hdr', text: 'Backtest Export Settings' }));

    // Benchmark ticker
    const benchRow = el('div', { class: 'scr-bt-row' },
      el('label', { class: 'scr-bt-lbl', text: 'Benchmark Ticker:' }),
      el('input', { id: 'scr-bt-bench', class: 'scr-inp', type: 'text', placeholder: 'e.g. 1306.T', value: '1306.T' }),
    );

    // Discount rate
    const rateRow = el('div', { class: 'scr-bt-row' },
      el('label', { class: 'scr-bt-lbl', text: 'Discount Rate:' }),
      el('input', { id: 'scr-bt-rate', class: 'scr-inp', type: 'text', placeholder: 'e.g. 0.02', value: '0.02' }),
    );

    // Info
    const info = el('div', { class: 'scr-bt-info', text: `Exporting ${ST.results.row_count} companies with equal weight in backtest CSV format.` });

    // Actions
    const cancelBtn = el('button', { class: 'scr-bld-cancel', text: 'Cancel' });
    cancelBtn.addEventListener('click', () => { pop.remove(); resolve(null); });

    const exportBtn = el('button', { class: 'scr-bld-add', text: 'Export' });
    exportBtn.addEventListener('click', () => {
      const benchmark = $('#scr-bt-bench')?.value?.trim() || '';
      const discountRate = $('#scr-bt-rate')?.value?.trim() || '0.02';
      pop.remove();
      resolve({ benchmark, discountRate });
    });

    const acts = el('div', { class: 'scr-bt-acts' }, cancelBtn, exportBtn);

    pop.append(benchRow, rateRow, info, acts);
    document.body.append(pop);
    pop.style.position = 'fixed';
    pop.style.left = '50%';
    pop.style.top = '40%';
    pop.style.transform = 'translate(-50%, -50%)';
    pop.style.zIndex = '1000';
    pop.style.minWidth = '380px';

    setTimeout(() => {
      const benchInput = $('#scr-bt-bench');
      if (benchInput) benchInput.focus();
    }, 50);

    // Close on Escape
    const onKey = (e) => {
      if (e.key === 'Escape') { pop.remove(); document.removeEventListener('keydown', onKey); resolve(null); }
    };
    document.addEventListener('keydown', onKey);
    // Auto-close on outside click (after a beat)
    setTimeout(() => document.addEventListener('click', function f(e) {
      if (!pop.contains(e.target)) { pop.remove(); document.removeEventListener('click', f); document.removeEventListener('keydown', onKey); resolve(null); }
    }), 0);
  });
}

async function exportBacktest() {
  if (!ST.results?.row_count) { log('warn', 'No results to export'); return; }

  const cfg = await showBacktestExportPopup();
  if (!cfg) return; // cancelled

  try {
    const rows = ST.results.rows;
    const cols = ST.results.columns;

    // Find relevant column indices
    let tickerIdx = -1, edinetIdx = -1, nameIdx = -1, industryIdx = -1, periodIdx = -1;
    for (let i = 0; i < cols.length; i++) {
      const c = cols[i].toLowerCase();
      if (/company_ticker|^ticker$/.test(c) && tickerIdx === -1) tickerIdx = i;
      if (/edinetcode/.test(c) && edinetIdx === -1) edinetIdx = i;
      if (/company_name|name|submitter|filer/i.test(c) && nameIdx === -1) nameIdx = i;
      if (/company_industry|industry|sector/i.test(c) && industryIdx === -1) industryIdx = i;
      if (/periodend/.test(c) && periodIdx === -1) periodIdx = i;
    }

    if (tickerIdx === -1) {
      status('Add Company_Ticker column to results first');
      log('warn', 'No ticker column in results — cannot export backtest');
      return;
    }

    const n = rows.length;
    const weight = n > 0 ? (1.0 / n) : 0;
    const year = ST.screeningDate ? ST.screeningDate.substring(0, 4) : (new Date().getFullYear().toString());

    // Build CSV lines
    const lines = [];

    // Header comments with config
    lines.push('# Backtest Configuration');
    lines.push(`# Benchmark: ${cfg.benchmark}`);
    lines.push(`# Discount Rate: ${cfg.discountRate}`);
    lines.push('# Generated: ' + new Date().toISOString().split('T')[0]);
    if (ST.screeningDate) lines.push(`# Screening Date: ${ST.screeningDate}`);
    lines.push('#');

    // Build header
    const header = ['Year', 'Tickers', 'Type', 'Amount'];
    if (edinetIdx >= 0) header.push('EdinetCode');
    if (nameIdx >= 0) header.push('CompanyName');
    if (industryIdx >= 0) header.push('Industry');
    if (periodIdx >= 0) header.push('PeriodEnd');
    lines.push(header.join(','));

    for (const row of rows) {
      const values = [year, escapeCSV(String(row[tickerIdx] || '')), 'weight', weight.toFixed(6)];
      if (edinetIdx >= 0) values.push(escapeCSV(String(row[edinetIdx] || '')));
      if (nameIdx >= 0) values.push(escapeCSV(String(row[nameIdx] || '')));
      if (industryIdx >= 0) values.push(escapeCSV(String(row[industryIdx] || '')));
      if (periodIdx >= 0) values.push(escapeCSV(String(row[periodIdx] || '')));
      lines.push(values.join(','));
    }

    const csv = lines.join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'screening_backtest.csv';
    a.click();
    URL.revokeObjectURL(a.href);

    status(`Exported ${n} companies (equal weight: ${(weight * 100).toFixed(1)}% each)`);
    log('info', `Backtest export: ${n} companies, benchmark=${cfg.benchmark}, discount=${cfg.discountRate}`);
  } catch (e) {
    log('error', `Backtest export: ${e.message}`);
  }
}

function escapeCSV(val) {
  if (/[,"\n\r]/.test(val)) {
    return '"' + val.replace(/"/g, '""') + '"';
  }
  return val;
}

// ---------------------------------------------------------------------------
// Open in Backtesting — pass ticker list directly (no re-screening)
// ---------------------------------------------------------------------------

function openInBacktesting() {
  if (!ST.results?.row_count) {
    log('warn', 'Run a screening first, then click Backtest →');
    return;
  }

  const rows = ST.results.rows;
  const cols = ST.results.columns;

  // Find ticker column
  let tickerIdx = -1;
  for (let i = 0; i < cols.length; i++) {
    if (/company_ticker|^ticker$/i.test(cols[i])) { tickerIdx = i; break; }
  }
  if (tickerIdx === -1) {
    log('warn', 'Add Company_Ticker column to results first');
    return;
  }

  // Extract unique tickers (preserving screening order)
  const tickers = [];
  for (const row of rows) {
    const t = String(row[tickerIdx] || '').trim();
    if (t && !tickers.includes(t)) tickers.push(t);
  }

  const year = ST.screeningDate
    ? ST.screeningDate.substring(0, 4)
    : new Date().getFullYear().toString();
  const weight = tickers.length > 0 ? (1.0 / tickers.length) : 0;

  // Build CSV lines — the backtesting page opens in CSV mode with this
  const csvLines = ['Year,Tickers,Type,Amount'];
  for (const t of tickers) {
    csvLines.push(`${year},${t},weight,${weight.toFixed(6)}`);
  }

  const payload = {
    csvContent: csvLines.join('\n'),
    tickerCount: tickers.length,
    screeningDate: ST.screeningDate,
  };

  const key = 'bt-screener-' + crypto.randomUUID();
  sessionStorage.setItem(key, JSON.stringify(payload));
  window.open('/backtesting#screener-key=' + encodeURIComponent(key), '_blank');
}

// ---------------------------------------------------------------------------
// Entry
// ---------------------------------------------------------------------------

export async function render() {
  buildShell();
  await init();
}
