/**
 * Screening — application state & utility helpers.
 *
 * Central mutable state object (ST) and pure helper functions
 * for column references, SQL compilation, and computed column specs.
 */

// ---------------------------------------------------------------------------
// State object
// ---------------------------------------------------------------------------

export const ST = {
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

// ---------------------------------------------------------------------------
// ID generation
// ---------------------------------------------------------------------------

export function uid() { return String(ST._nextId++); }

// ---------------------------------------------------------------------------
// Column helpers
// ---------------------------------------------------------------------------

/** Extract column references from the unified columns array. */
export function colRefs() {
  return ST.columns.filter(c => c.kind === 'col').map(c => c.ref);
}

// ---------------------------------------------------------------------------
// Table alias mapping (must match _TABLE_ALIAS in screening.py)
// ---------------------------------------------------------------------------

const _ALIAS = {
  FinancialStatements: 'f', CompanyInfo: 'c', Stock_Prices: 's_p',
  PerShare: 'ps', Valuation: 'v', Quality: 'q',
  IncomeStatement: 'i', BalanceSheet: 'b', CashflowStatement: 'cf',
  Pershare_Historical: 'psh', Valuation_Historical: 'vh', Quality_Historical: 'qh',
};

// ---------------------------------------------------------------------------
// SQL compilation
// ---------------------------------------------------------------------------

/** Compile token list to a SQL expression string. */
export function compileTokensToSQL(tokens) {
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

// ---------------------------------------------------------------------------
// Computed column specs
// ---------------------------------------------------------------------------

/** Extract computed column specs from the unified columns array. */
export function computedColSpecs() {
  return ST.columns.filter(c => c.kind === 'comp').map(cc => {
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
