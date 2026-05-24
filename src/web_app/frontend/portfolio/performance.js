import { $, fetchJson } from '../common/utils.js';
import { state, formatMoney, formatPct, formatNum } from './common.js';

export function wirePerformance() {
  const perfBtn = $('#pf-perf-compute');
  if (perfBtn) perfBtn.addEventListener('click', computePerformance);
  const detectBtn = $('#pf-perf-detect-rf');
  if (detectBtn) detectBtn.addEventListener('click', async () => {
    const curr = $('#pf-perf-currency')?.value;
    try {
      const d = await fetchJson(`/api/portfolio/risk-free-rate?base_currency=${curr}`);
      if ($('#pf-perf-rf')) $('#pf-perf-rf').value = d.risk_free_rate.toFixed(4);
    } catch (_) { /* ignore */ }
  });
  $$('#pf-bench-shortcuts .btn-ghost').forEach(btn => btn.addEventListener('click', () => { $('#pf-perf-benchmark').value = btn.dataset.bench; computePerformance(); }));
}

export async function computePerformance() {
  const perfBtn = $('#pf-perf-compute');
  if (perfBtn) { perfBtn.disabled = true; perfBtn.textContent = 'Computing…'; }
  try {
    const qs = new URLSearchParams({ base_currency: $('#pf-perf-currency')?.value });
    const bench = $('#pf-perf-benchmark')?.value.trim();
    const rf = $('#pf-perf-rf')?.value;
    if (bench) qs.set('benchmark_ticker', bench);
    if (rf) qs.set('risk_free_rate', rf);
    state.performance = await fetchJson('/api/portfolio/performance?' + qs.toString());
  } catch (e) {
    const div = $('#pf-metrics-grid'); if (div) div.innerHTML = `<span class="status-text error">${e.message}</span>`;
  } finally {
    if (perfBtn) { perfBtn.disabled = false; perfBtn.textContent = 'Compute'; }
  }
  renderMetrics();
}

export function renderPerformanceTab() {
  if (!state.performance || (!state.performance.sharpe_ratio && !state.performance.total_return)) {
    computePerformance().catch(() => {});
  } else {
    renderMetrics();
  }
}

export function renderMetrics() {
  const div = $('#pf-metrics-grid');
  const p = state.performance;
  if (!div || !p) return;
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

// local helper to provide $$ from common (avoid circular import)
function $$(sel, root) { return Array.from((root || document).querySelectorAll(sel)); }
