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
  columns: [],
  sortBy: '',
  sortOrder: 'DESC',
  formattedValues: true,
  results: null,
  resultsLoading: false,
  sqlDisplay: '',
  _nextId: 1,
};

function uid() { return String(ST._nextId++); }

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

  // Row 1: actions
  cfg.append(
    el('div', { class: 'scr-row-actions' },
      el('button', { id: 'scr-btn-run', class: 'scr-btn-run', text: 'Run' }),
      el('button', { id: 'scr-btn-save', class: 'scr-btn-soft', text: 'Save' }),
      el('button', { id: 'scr-btn-load', class: 'scr-btn-soft', text: 'Load' }),
    ),
  );

  // Row 2: date
  cfg.append(
    el('div', { class: 'scr-row-date' },
      el('label', { class: 'scr-lbl', text: 'Screening Date:' }),
      el('input', { id: 'scr-date', class: 'scr-input-date', type: 'date' }),
      el('span', { id: 'scr-status', class: 'scr-status' }),
    ),
  );

  // Row 3: Criteria
  cfg.append(
    el('div', { class: 'scr-section' },
      el('div', { class: 'scr-section-head' }, el('span', { text: 'Criteria:' })),
      el('div', { id: 'scr-criteria' }),
      el('div', { id: 'scr-crit-builder' }),
      el('button', { id: 'scr-add-crit', class: 'scr-btn-add', text: '+ Add Criteria' }),
    ),
  );

  // Row 4: Columns
  cfg.append(
    el('div', { class: 'scr-section' },
      el('div', { class: 'scr-section-head' }, el('span', { text: 'Columns:' })),
      el('div', { id: 'scr-columns' }),
      el('button', { id: 'scr-add-col', class: 'scr-btn-add', text: '+ Add Column' }),
    ),
  );

  // Row 5: Results toolbar
  cfg.append(
    el('div', { class: 'scr-row-bottom' },
      el('button', { id: 'scr-btn-export', class: 'scr-btn-soft', text: 'Export CSV' }),
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
  $('#scr-date').addEventListener('change', () => { ST.screeningDate = $('#scr-date').value; });
  $('#scr-fmt').addEventListener('change', () => { ST.formattedValues = $('#scr-fmt').checked; renderResults(); });
  $('#scr-add-crit').addEventListener('click', showCritBuilder);
  $('#scr-add-col').addEventListener('click', showColPicker);
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

async function init() {
  status('Loading database…');
  const path = await defaultDbPath();
  if (!path) { status('No database found'); return; }

  ST.dbPath = path;
  try {
    await loadMetrics(path);
    if (ST.columns.length === 0) setDefaultColumns();
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
  ST.columns = refs.map(r => ({ id: uid(), ref: r }));
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
    if (rightTokens.length) bar.append(renderTokenList(rightTokens, crit, 'right_side'));
    else bar.append(el('span', { class: 'scr-tok-col', text: '[[?]]' }));
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
  const allOps = ['>', '>=', '<', '<=', '=', '!=', 'BETWEEN', 'IN', 'LIKE'];
  const ops = restricted
    ? ['>', '>=', '<', '<=', '=', '!=']
    : allOps;
  for (const o of ops) {
    const btn = el('button', { class: 'scr-pop-op' + (o === crit.operator ? ' is-sel' : ''), text: o });
    btn.addEventListener('click', () => {
      crit.operator = o;
      if (o === 'IN') { crit.comparison_mode = 'in'; crit.values = crit.value != null ? [String(crit.value)] : ['']; }
      else if (o === 'LIKE') { crit.comparison_mode = 'like'; }
      else if (o === 'BETWEEN') { crit.comparison_mode = 'fixed'; }
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
    if (!pop.contains(e.target)) { pop.remove(); document.removeEventListener('click', f); }
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
    for (const o of ['>', '>=', '<', '<=', '=', '!=']) {
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
      if (!leftTokens.length || !rightTokens.length) return;
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
    return `[ ${left.map(fmt).join(' ')}  ${op}  ${right.map(fmt).join(' ')} ]`;
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
    const sel = new Set(ST.columns.map(c => c.ref));
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
          if (e.target.checked) { if (!ST.columns.find(c => c.ref === it.ref)) ST.columns.push({ id: uid(), ref: it.ref }); }
          else { ST.columns = ST.columns.filter(c => c.ref !== it.ref); }
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
// Render columns
// ---------------------------------------------------------------------------

function renderColumns() {
  const ctr = $('#scr-columns'); if (!ctr) return;
  ctr.innerHTML = '';
  for (const col of ST.columns) {
    const [table, column] = col.ref.split('.');
    const bar = el('div', { class: 'scr-col' },
      el('span', { class: 'scr-tok-col', text: `[ [${table}].[${column}] ]` }),
      el('button', { class: 'scr-rm', text: '✕' }),
    );
    bar.querySelector('.scr-rm').addEventListener('click', () => {
      ST.columns = ST.columns.filter(c => c.id !== col.id);
      renderColumns();
    });
    ctr.append(bar);
  }
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
  const colRefs = new Set(ST.columns.map(c => c.ref));
  for (const c of ST.criteria) {
    if (c.table && c.column) colRefs.add(`${c.table}.${c.column}`);
    if (c.comparison_mode === 'column' && c.compare_table && c.compare_column) {
      colRefs.add(`${c.compare_table}.${c.compare_column}`);
    }
    if (c.comparison_mode === 'full_expression') {
      for (const side of [c.left_side || [], c.right_side || []]) {
        for (const t of side) {
          if (t.type === 'column' && t.table && t.column) colRefs.add(`${t.table}.${t.column}`);
        }
      }
    }
    if (c.comparison_mode === 'expression' && c.right_side) {
      for (const t of c.right_side) {
        if (t.type === 'column' && t.table && t.column) colRefs.add(`${t.table}.${t.column}`);
      }
    }
  }
  const columns = [...colRefs];

  ST.resultsLoading = true;
  status('Running…');
  renderResults();

  try {
    const data = await fetchJson('/api/screening/run', {
      method: 'POST', body: JSON.stringify({
        db_path: ST.dbPath, criteria, columns,
        screening_date: ST.screeningDate || null,
        sort_by: ST.sortBy || null, sort_order: ST.sortOrder,
      }),
    });
    ST.results = data;
    ST.sqlDisplay = data.sql_display || '';
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
  for (const col of ST.results.columns) {
    const th = el('th', { text: col });
    th.addEventListener('click', () => sort(col));
    hr.append(th);
  }
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
  if (ST.dbPath) p.set('db_path', ST.dbPath);
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
        name: name.trim(), criteria: ST.criteria, columns: ST.columns.map(c => c.ref),
        screening_date: ST.screeningDate || null,
      }),
    });
    log('info', `Saved "${name}"`);
  } catch (e) { log('error', `Save: ${e.message}`); }
}

async function load() {
  const names = await apiSavedList();
  if (!names.length) { alert('No saved screenings'); return; }
  const name = prompt(`Saved:\n${names.join('\n')}\n\nLoad:`);
  if (!name?.trim()) return;
  try {
    const d = await fetchJson(`/api/screening/saved/${encodeURIComponent(name.trim())}`);
    ST.criteria = (d.criteria || []).map(c => ({ id: uid(), ...c }));
    ST.columns = (d.columns || []).map(ref => ({ id: uid(), ref }));
    ST.screeningDate = d.screening_date || '';
    renderAll();
    log('info', `Loaded "${name}"`);
  } catch (e) { log('error', `Load: ${e.message}`); }
}

async function exportCSV() {
  if (!ST.results?.row_count) { log('warn', 'No results'); return; }
  try {
    const r = await fetch('/api/screening/export', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ db_path: ST.dbPath, criteria: ST.criteria, columns: ST.columns.map(c => c.ref), screening_date: ST.screeningDate || null, format: 'csv' }),
    });
    const blob = await r.blob();
    const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'screening.csv'; a.click();
  } catch (e) { log('error', `Export: ${e.message}`); }
}

// ---------------------------------------------------------------------------
// Entry
// ---------------------------------------------------------------------------

export async function render() {
  buildShell();
  await init();
}
