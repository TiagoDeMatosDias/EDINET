// Shared helpers and state for the Portfolio frontend
import { $, $all, fetchJson } from '../common/utils.js';
import { log } from '../common/console.js';

export { $, fetchJson };
export const $$ = $all;

export const state = {
  holdings: [],
  transactions: [],
  activitySummary: {},
  performance: null,
  chartSettings: { currency: 'EUR', benchmark: '', inflation: true },
  chartData: null,
  chartRawData: {},
  chartViewMode: {},
  charts: {},
  uploadedFiles: [],
  sortColumn: null,
  sortAsc: true,
  displayCurrency: 'EUR',
  displayCurrencies: [],
  visibleColumns: null,
  pinnedColumns: new Set(['symbol', 'name', 'asset_type', 'industry', 'open_pos', 'native_ccy', 'quantity']),
  columnOrder: null,
  columnFilters: {},
  _colMap: null,
  closedPositions: [],
};

// ---------------------------------------------------------------------------
// Ticker mapping: IBKR format (5984.T) → db2 format (59840)
// ---------------------------------------------------------------------------
export function normalizeTicker(symbol) {
  if (!symbol) return symbol;
  if (symbol.startsWith('CASH:')) return symbol;
  if (symbol.match(/\s\d{6}[CP]\d{8}$/)) return symbol;
  const m = symbol.match(/^(\d{4})\.T$/);
  if (m) return m[1] + '0';
  return symbol;
}

export function formatMoney(v) {
  if (v == null || isNaN(v)) return '—';
  if (Math.abs(v) >= 1e6) return (v / 1e6).toFixed(2) + 'M';
  if (Math.abs(v) >= 1e3) return (v / 1e3).toFixed(1) + 'K';
  return v.toFixed(2);
}

export function formatPct(v) { return v != null ? (v * 100).toFixed(2) + '%' : '—'; }
export function formatNum(v, d) { return v != null ? v.toFixed(d || 4) : '—'; }

export function badgeClass(type) {
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
export async function refreshSummary() {
  try {
    const [counts, dateRange] = await Promise.all([
      fetchJson('/api/portfolio/activity-summary'),
      fetchJson('/api/portfolio/date-range'),
    ]);
    state.activitySummary = counts.by_activity || {};
    const totalTxns = Object.values(state.activitySummary).reduce((a, b) => a + b, 0);
    const statTxn = $('#pf-stat-txn');
    const statDates = $('#pf-stat-dates');
    if (statTxn) statTxn.textContent = totalTxns.toLocaleString();
    if (statDates) statDates.textContent = dateRange.min_date ? `${dateRange.min_date} → ${dateRange.max_date}` : '—';
  } catch (_) { /* server may not have data yet */ }
}

export function renderActivityBreakdown() {
  const div = $('#pf-activity-breakdown');
  const sa = state.activitySummary;
  const entries = Object.entries(sa);
  if (!div) return;
  if (!entries.length) {
    div.innerHTML = '<span class="muted">No data yet. Upload transactions above.</span>';
    return;
  }
  div.innerHTML = entries.map(([type, count]) =>
    `<div class="metric-tile" style="min-width:120px;"><div class="metric-label">${type.replace('_', ' ')}</div><div class="metric-value" style="font-size:1.1rem;">${count}</div></div>`
  ).join('');
}

// Chart loading overlays (used by charts module)
export function showChartLoading(canvasId) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  hideChartLoading(canvasId);
  const overlay = document.createElement('div');
  overlay.className = 'pf-chart-loading';
  overlay.id = canvasId + '-loading';
  overlay.innerHTML = '<div class="pf-chart-loading-spinner"></div><div class="pf-chart-loading-text">Loading…</div>';
  canvas.parentNode.appendChild(overlay);
}

export function hideChartLoading(canvasId) {
  const overlay = document.getElementById(canvasId + '-loading');
  if (overlay) overlay.remove();
}

export function _destroyAndClear(key) {
  if (state.charts[key]) { state.charts[key].destroy(); state.charts[key] = null; }
}
export function destroyChart(key) { _destroyAndClear(key); }
