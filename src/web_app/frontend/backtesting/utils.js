/**
 * Backtesting — formatting & math utilities.
 *
 * Pure functions with no dependencies on backtesting state or DOM.
 */

// ---------------------------------------------------------------------------
// Date / math helpers
// ---------------------------------------------------------------------------

export function yearsBetween(start, end) {
  const s = new Date(start), e = new Date(end);
  return Math.max(0.25, (e - s) / (365.25 * 24 * 60 * 60 * 1000));
}

export function annualize(totalReturn, startDate, endDate) {
  const yrs = yearsBetween(startDate, endDate);
  return (1 + totalReturn) ** (1 / yrs) - 1;
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

export function fmtPct(v) {
  if (v == null || v === '' || (typeof v === 'number' && isNaN(v))) return '-';
  const n = typeof v === 'string' ? parseFloat(v) : v;
  if (n == null || isNaN(n)) return '-';
  return (n >= 0 ? '+' : '') + (n * 100).toFixed(2) + '%';
}

export function colorStyle(v) {
  if (v == null || v === '' || (typeof v === 'number' && isNaN(v))) return '';
  const n = typeof v === 'string' ? parseFloat(v) : v;
  if (n == null || isNaN(n)) return '';
  return n >= 0 ? 'color:var(--success)' : 'color:var(--danger)';
}
