/**
 * Portfolio Charts Tab — complete implementation.
 *
 * 7 charts:
 *   1. Pie — Holdings by Value
 *   2. Pie — Holdings by Currency
 *   3. Stacked Line — Portfolio Value Over Time (per holding)
 *   4. Stacked Bar — Dividends by Company  (period: monthly/quarterly/yearly)
 *   5. Stacked Bar — Dividends by Currency (period: monthly/quarterly/yearly)
 *   6. Heatmap — Dividend Heatmap (year × month)
 *   7. Heatmap — Returns Heatmap (year × month)
 *
 * Each chart has a popup table toggle.
 */

import { $, fetchJson } from '../common/utils.js';
import { state, showChartLoading, hideChartLoading, destroyChart, formatMoney, formatPct } from './common.js';

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

const CHART_COLORS = [
  '#58a6ff', '#44d17b', '#e0af4f', '#ff6b6b', '#8ea0b8',
  '#58a6ffcc', '#44d17bcc', '#e0af4fcc', '#ff6b6bcc', '#8ea0b8cc',
  '#58a6ff88', '#44d17b88', '#e0af4f88', '#ff6b6b88', '#8ea0b888',
  '#a371f7', '#f78166', '#d2a8ff', '#7ee787', '#ffa657',
];

function _color(i) {
  return CHART_COLORS[i % CHART_COLORS.length];
}

function _colorAlpha(i, a) {
  const c = _color(i);
  return c.replace(/[\da-f]{2}$/i, a);
}

// ---------------------------------------------------------------------------
// Display Currency
// ---------------------------------------------------------------------------

export async function loadChartCurrencies() {
  const sel = $('#pf-chart-currency');
  if (!sel) return;
  try {
    const currencies = await fetchJson('/api/portfolio/charts/display-currencies');
    sel.innerHTML = currencies.map(c =>
      `<option value="${c.code}" ${c.code === state.chartSettings.currency ? 'selected' : ''}>${c.code}</option>`
    ).join('');
  } catch (_) {}
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

export function renderChartsTab() {
  refreshAllCharts();
}

export async function refreshAllCharts() {
  const btn = $('#pf-chart-refresh');
  if (btn) { btn.disabled = true; btn.textContent = '⏳'; }

  const dc = state.chartSettings.currency;

  const canvasIds = [
    'pf-holding-pie-chart', 'pf-currency-pie-chart',
    'pf-portfolio-value-chart', 'pf-div-company-chart',
    'pf-div-currency-chart', 'pf-div-heatmap-chart',
    'pf-return-heatmap-chart',
  ];
  canvasIds.forEach(id => showChartLoading(id));

  try {
    const [divByCo, divByCcy] = await _fetchDividendCharts(dc);
    const [holdingsByVal, holdingsByCcy, valueHistory, divHeat, retHeat] = await Promise.all([
      fetchJson(`/api/portfolio/charts/holdings-by-value?display_currency=${dc}`).catch(() => null),
      fetchJson(`/api/portfolio/charts/holdings-by-currency?display_currency=${dc}`).catch(() => null),
      fetchJson(`/api/portfolio/charts/portfolio-value-history?display_currency=${dc}`).catch(() => null),
      fetchJson(`/api/portfolio/charts/dividends-heatmap?display_currency=${dc}`).catch(() => null),
      fetchJson(`/api/portfolio/charts/returns-heatmap?display_currency=${dc}`).catch(() => null),
    ]);

    state.chartRawData = {
      holdingPie: holdingsByVal,
      currencyPie: holdingsByCcy,
      portfolioValue: valueHistory,
      divByCompany: divByCo,
      divByCurrency: divByCcy,
      divHeatmap: divHeat,
      returnHeatmap: retHeat,
    };

    _renderHoldingPie(holdingsByVal);
    _renderCurrencyPie(holdingsByCcy);
    _renderPortfolioValueStack(valueHistory);
    _renderDivStackedBar('pf-div-company-chart', 'divByCompany', divByCo, 'companies');
    _renderDivStackedBar('pf-div-currency-chart', 'divByCurrency', divByCcy, 'currencies');
    _renderHeatmap('pf-div-heatmap-chart', 'divHeatmap', divHeat, false);
    _renderHeatmap('pf-return-heatmap-chart', 'returnHeatmap', retHeat, true);
  } catch (e) {
    console.error('Failed to refresh charts:', e);
  } finally {
    canvasIds.forEach(id => hideChartLoading(id));
    if (btn) { btn.disabled = false; btn.textContent = 'Apply'; }
  }
}

async function _fetchDividendCharts(dc) {
  const period = $('#pf-div-period')?.value || 'monthly';
  return Promise.all([
    fetchJson(`/api/portfolio/charts/dividends-by-company?display_currency=${dc}&period=${period}`).catch(() => null),
    fetchJson(`/api/portfolio/charts/dividends-by-currency?display_currency=${dc}&period=${period}`).catch(() => null),
  ]);
}

async function refreshDividendCharts() {
  const dc = state.chartSettings.currency;
  ['pf-div-company-chart', 'pf-div-currency-chart'].forEach(id => showChartLoading(id));
  try {
    const [divByCo, divByCcy] = await _fetchDividendCharts(dc);
    state.chartRawData.divByCompany = divByCo;
    state.chartRawData.divByCurrency = divByCcy;
    _renderDivStackedBar('pf-div-company-chart', 'divByCompany', divByCo, 'companies');
    _renderDivStackedBar('pf-div-currency-chart', 'divByCurrency', divByCcy, 'currencies');
  } finally {
    ['pf-div-company-chart', 'pf-div-currency-chart'].forEach(id => hideChartLoading(id));
  }
}

// ---------------------------------------------------------------------------
// Chart 1: Holdings by Value (Pie)
// ---------------------------------------------------------------------------

function _renderHoldingPie(data) {
  const canvas = $('#pf-holding-pie-chart');
  if (!canvas) return;
  destroyChart('holdingPie');
  if (!data || !data.labels || !data.labels.length) {
    _showEmpty(canvas, 'No holdings data');
    return;
  }
  const ctx = canvas.getContext('2d');
  state.charts.holdingPie = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: data.labels,
      datasets: [{ data: data.values, backgroundColor: data.labels.map((_, i) => _color(i)), borderColor: 'transparent' }],
    },
    options: _pieOptions(data.currency),
  });
}

// ---------------------------------------------------------------------------
// Chart 2: Holdings by Currency (Pie)
// ---------------------------------------------------------------------------

function _renderCurrencyPie(data) {
  const canvas = $('#pf-currency-pie-chart');
  if (!canvas) return;
  destroyChart('currencyPie');
  if (!data || !data.labels || !data.labels.length) {
    _showEmpty(canvas, 'No currency data');
    return;
  }
  const ctx = canvas.getContext('2d');
  state.charts.currencyPie = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: data.labels.map(c => `${c} (${data.total > 0 ? ((data.values[data.labels.indexOf(c)] / data.total) * 100).toFixed(1) : 0}%)`),
      datasets: [{
        data: data.values,
        backgroundColor: ['#44d17b', '#58a6ff', '#e0af4f', '#ff6b6b', '#8ea0b8', '#a371f7'],
        borderColor: 'transparent',
      }],
    },
    options: _pieOptions(data.currency),
  });
}

function _pieOptions(currency) {
  return {
    responsive: true, maintainAspectRatio: false,
    plugins: {
      legend: { position: 'right', labels: { color: '#d9e2f2', usePointStyle: true, padding: 12 } },
      tooltip: { callbacks: { label: ctx => `${ctx.label}: ${formatMoney(ctx.raw)} ${currency || ''}` } },
    },
  };
}

// ---------------------------------------------------------------------------
// Chart 3: Portfolio Value Over Time (Stacked Line)
// ---------------------------------------------------------------------------

function _renderPortfolioValueStack(data) {
  const canvas = $('#pf-portfolio-value-chart');
  if (!canvas) return;
  destroyChart('portfolioValue');
  if (!data || !data.dates || !data.dates.length) {
    _showEmpty(canvas, 'No value history');
    return;
  }

  const syms = Object.keys(data.holdings);
  syms.sort((a, b) => {
    const avgA = (data.holdings[a] || []).reduce((s, v) => s + (v || 0), 0) / data.holdings[a].length;
    const avgB = (data.holdings[b] || []).reduce((s, v) => s + (v || 0), 0) / data.holdings[b].length;
    return avgB - avgA;
  });

  const datasets = syms.map((sym, i) => ({
    label: sym,
    data: data.holdings[sym],
    backgroundColor: _colorAlpha(i, '66'),
    borderColor: _color(i),
    borderWidth: 0.5, fill: true, tension: 0.1, pointRadius: 0,
  }));

  const ctx = canvas.getContext('2d');
  state.charts.portfolioValue = new Chart(ctx, {
    type: 'line',
    data: { labels: data.dates, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      scales: {
        x: { ticks: { color: '#8ea0b8', maxTicksLimit: 12 } },
        y: { stacked: true, ticks: { color: '#8ea0b8', callback: v => formatMoney(v) }, title: { display: true, text: data.currency || '', color: '#8ea0b8' } },
      },
      plugins: {
        legend: { position: 'right', labels: { color: '#d9e2f2', usePointStyle: true, padding: 8, font: { size: 10 } } },
        tooltip: { itemSort: (a, b) => (b.parsed.y ?? 0) - (a.parsed.y ?? 0), callbacks: { label: ctx => ctx.raw ? `${ctx.dataset.label}: ${formatMoney(ctx.raw)}` : null } },
      },
    },
  });
}

// ---------------------------------------------------------------------------
// Charts 4 & 5: Stacked Bar (Dividends by Company / Currency)
// ---------------------------------------------------------------------------

function _renderDivStackedBar(canvasId, chartKey, data, dimension) {
  const canvas = $(`#${canvasId}`);
  if (!canvas) return;
  destroyChart(chartKey);
  if (!data || !data.periods || !data.periods.length) {
    _showEmpty(canvas, 'No dividend data');
    return;
  }

  const keys = Object.keys(data[dimension] || {}).sort();
  const datasets = keys.map((k, i) => ({
    label: k,
    data: data[dimension][k],
    backgroundColor: _color(i),
    borderColor: _color(i),
    borderWidth: 0.5, borderRadius: 1,
  }));

  const ctx = canvas.getContext('2d');
  state.charts[chartKey] = new Chart(ctx, {
    type: 'bar',
    data: { labels: data.periods, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      scales: {
        x: { stacked: true, ticks: { color: '#8ea0b8', maxTicksLimit: 15 } },
        y: { stacked: true, ticks: { color: '#8ea0b8', callback: v => formatMoney(v) }, title: { display: true, text: data.currency || '', color: '#8ea0b8' } },
      },
      plugins: {
        legend: { position: 'right', labels: { color: '#d9e2f2', usePointStyle: true, padding: 8, font: { size: 10 } } },
        tooltip: { itemSort: (a, b) => (b.parsed.y ?? 0) - (a.parsed.y ?? 0), callbacks: { label: ctx => ctx.raw ? `${ctx.dataset.label}: ${formatMoney(ctx.raw)}` : null } },
      },
    },
  });
}

// ---------------------------------------------------------------------------
// Charts 6 & 7: Heatmaps (custom canvas with red-green color scale)
// ---------------------------------------------------------------------------

function _redGreenColor(val, cap) {
  // Red-green scale capped at ±cap. Green for positive, red for negative.
  if (val == null) return '#1a1e24';
  const absVal = Math.min(Math.abs(val), cap);
  const intensity = cap > 0 ? absVal / cap : 0;
  if (val >= 0) {
    // Green: dark green (0,80,0) to bright green (68,255,68)
    const g = Math.round(80 + (255 - 80) * intensity);
    const r = Math.round(30 * (1 - intensity));
    const b = Math.round(30 * (1 - intensity));
    return `rgb(${r},${g},${b})`;
  } else {
    // Red: dark red (80,0,0) to bright red (255,68,68)
    const r = Math.round(80 + (255 - 80) * intensity);
    const g = Math.round(30 * (1 - intensity));
    const b = Math.round(30 * (1 - intensity));
    return `rgb(${r},${g},${b})`;
  }
}

function _renderHeatmap(canvasId, chartKey, data, isReturns) {
  const canvas = $(`#${canvasId}`);
  if (!canvas) return;
  destroyChart(chartKey);
  if (!data || !data.years || !data.years.length) {
    _showEmpty(canvas, 'No data');
    return;
  }

  const ctx = canvas.getContext('2d');
  const rect = canvas.parentElement.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  const w = rect.width || 400;
  const h = rect.height || (isReturns ? 340 : 340);
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  canvas.style.width = w + 'px';
  canvas.style.height = h + 'px';
  ctx.scale(dpr, dpr);

  const years = data.years;
  const months = data.months || [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
  const values = data.values;

  // Cap for color scale: 5% for returns, max absolute value for dividends
  let cap = isReturns ? 5 : 0;
  if (!isReturns) {
    let maxAbs = 0;
    for (const row of values) {
      for (const v of row) {
        if (v != null && Math.abs(v) > maxAbs) maxAbs = Math.abs(v);
      }
    }
    cap = maxAbs || 1;
  }

  const cellW = Math.max(20, (w - 60) / months.length);
  const cellH = Math.max(18, (h - 40) / years.length);
  const xStart = 50;
  const yStart = 20;

  // Month labels
  ctx.fillStyle = '#8ea0b8';
  ctx.font = '10px monospace';
  ctx.textAlign = 'center';
  for (let m = 0; m < months.length; m++) {
    ctx.fillText(months[m], xStart + m * cellW + cellW / 2, 14);
  }

  // Draw cells
  for (let y = 0; y < values.length; y++) {
    ctx.fillStyle = '#d9e2f2';
    ctx.textAlign = 'right';
    ctx.fillText(String(years[y]), xStart - 6, yStart + y * cellH + cellH / 2 + 3);

    for (let m = 0; m < months.length; m++) {
      const val = values[y] && values[y][m];
      const x = xStart + m * cellW;
      const cy = yStart + y * cellH;

      ctx.fillStyle = _redGreenColor(val, cap);
      ctx.fillRect(x + 1, cy + 1, cellW - 2, cellH - 2);

      if (cellW > 40 && cellH > 16 && val != null) {
        ctx.fillStyle = '#d9e2f2';
        ctx.font = '9px monospace';
        ctx.textAlign = 'center';
        const label = isReturns ? val.toFixed(1) + '%' : (val >= 100 ? val.toFixed(0) : val.toFixed(1));
        ctx.fillText(label, x + cellW / 2, cy + cellH / 2 + 3);
      }
    }
  }

  state.charts[chartKey] = {
    canvas,
    destroy: () => { canvas.getContext('2d').clearRect(0, 0, canvas.width, canvas.height); },
    resize: () => { _renderHeatmap(canvasId, chartKey, data, isReturns); },
  };

  state.chartRawData[chartKey] = data;
}

// ---------------------------------------------------------------------------
// Empty state
// ---------------------------------------------------------------------------

function _showEmpty(canvas, msg) {
  const parent = canvas.parentElement;
  const existing = parent.querySelector('.pf-empty-chart');
  if (existing) existing.remove();
  const div = document.createElement('div');
  div.className = 'pf-empty-chart muted';
  div.style.cssText = 'text-align:center;padding:80px 20px;font-size:13px;';
  div.textContent = msg;
  parent.appendChild(div);
}

// ---------------------------------------------------------------------------
// Popup table view
// ---------------------------------------------------------------------------

function _popupOverlay() {
  let overlay = document.getElementById('pf-table-popup');
  if (!overlay) {
    overlay = document.createElement('div');
    overlay.id = 'pf-table-popup';
    overlay.style.cssText =
      'display:none;position:fixed;top:0;left:0;width:100vw;height:100vh;'
      + 'z-index:9999;background:rgba(0,0,0,0.6);'
      + 'align-items:center;justify-content:center;';
    overlay.innerHTML =
      '<div style="background:var(--panel-bg,#161b22);border:1px solid var(--line,#243244);'
      + 'border-radius:8px;max-width:90vw;max-height:85vh;display:flex;flex-direction:column;'
      + 'box-shadow:0 8px 32px rgba(0,0,0,0.6);overflow:hidden;">'
      + '<div style="display:flex;align-items:center;justify-content:space-between;padding:10px 16px;'
      + 'border-bottom:1px solid var(--line,#243244);flex-shrink:0;">'
      + '<span id="pf-popup-title" style="font-size:13px;font-weight:600;color:var(--text,#d9e2f2);"></span>'
      + '<button id="pf-popup-close" style="background:none;border:none;color:var(--muted,#8ea0b8);'
      + 'font-size:18px;cursor:pointer;padding:2px 8px;">✕</button>'
      + '</div>'
      + '<div id="pf-popup-body" style="overflow:auto;padding:12px;flex:1;font-size:11px;"></div>'
      + '</div>';
    document.body.appendChild(overlay);
    overlay.addEventListener('click', (e) => {
      if (e.target === overlay) overlay.style.display = 'none';
    });
    overlay.querySelector('#pf-popup-close').addEventListener('click', () => {
      overlay.style.display = 'none';
    });
  }
  return overlay;
}

export function wireTableToggles() {
  const chartsTab = document.querySelector('[data-panel="charts"]');
  if (!chartsTab) return;

  chartsTab.addEventListener('click', (e) => {
    const btn = e.target.closest('.pf-tbl-toggle');
    if (!btn) return;
    const chartKey = btn.dataset.chart;
    if (!chartKey) return;
    _openTablePopup(chartKey);
  });
}

function _openTablePopup(chartKey) {
  const data = state.chartRawData[chartKey];
  if (!data) return;
  const overlay = _popupOverlay();

  const titleMap = {
    holdingPie: 'Holdings by Value',
    currencyPie: 'Portfolio by Currency',
    portfolioValue: 'Portfolio Value Over Time',
    divByCompany: 'Dividends by Company',
    divByCurrency: 'Dividends by Currency',
    divHeatmap: 'Dividend Heatmap',
    returnHeatmap: 'Portfolio Returns Heatmap',
  };

  document.getElementById('pf-popup-title').textContent = titleMap[chartKey] || chartKey;
  const body = document.getElementById('pf-popup-body');

  let columns = [];
  let rows = [];

  switch (chartKey) {
    case 'holdingPie':
    case 'currencyPie': {
      const total = data.total || 0;
      columns = ['Item', `Value (${data.currency || ''})`, '% of Total'];
      rows = data.labels.map((l, i) => [
        l,
        (data.values[i] || 0).toFixed(2),
        total > 0 ? ((data.values[i] / total) * 100).toFixed(1) : '0.0',
      ]);
      break;
    }
    case 'portfolioValue': {
      const syms = Object.keys(data.holdings || {});
      columns = ['Date', ...syms];
      rows = (data.dates || []).map((d, i) => [d, ...syms.map(s => {
        const v = data.holdings[s] && data.holdings[s][i];
        return v != null ? v.toFixed(2) : '';
      })]);
      break;
    }
    case 'divByCompany': {
      const comps = Object.keys(data.companies || {}).sort();
      columns = ['Period', ...comps];
      rows = (data.periods || []).map((p, i) => [p, ...comps.map(s => (data.companies[s] && data.companies[s][i] || 0).toFixed(2))]);
      break;
    }
    case 'divByCurrency': {
      const ccies = Object.keys(data.currencies || {}).sort();
      columns = ['Period', ...ccies];
      rows = (data.periods || []).map((p, i) => [p, ...ccies.map(c => (data.currencies[c] && data.currencies[c][i] || 0).toFixed(2))]);
      break;
    }
    case 'divHeatmap':
    case 'returnHeatmap': {
      const isRet = chartKey === 'returnHeatmap';
      columns = ['Year', ...(data.months || []).map(m => `M${m}`)];
      rows = (data.years || []).map((y, i) => {
        const vals = data.values[i] || [];
        return [String(y), ...vals.map(v => v == null ? '—' : isRet ? v.toFixed(1) + '%' : v.toFixed(2))];
      });
      break;
    }
    default:
      body.innerHTML = '<div class="muted">Unknown chart</div>';
      overlay.style.display = 'flex';
      return;
  }

  let html = '<table class="data-table" style="font-size:11px;width:100%;"><thead><tr>';
  for (const col of columns) html += `<th>${col}</th>`;
  html += '</tr></thead><tbody>';
  for (const row of rows) {
    html += '<tr>';
    for (const cell of row) html += `<td>${cell ?? ''}</td>`;
    html += '</tr>';
  }
  html += '</tbody></table>';
  body.innerHTML = html;
  overlay.style.display = 'flex';
}

// ---------------------------------------------------------------------------
// Chart controls wiring
// ---------------------------------------------------------------------------

export function wireChartControls() {
  const refreshBtn = $('#pf-chart-refresh');
  const currencySel = $('#pf-chart-currency');
  const divPeriod = $('#pf-div-period');

  const fullHandler = () => {
    if (currencySel) state.chartSettings.currency = currencySel.value;
    refreshAllCharts();
  };

  if (refreshBtn) refreshBtn.addEventListener('click', fullHandler);
  if (currencySel) currencySel.addEventListener('change', fullHandler);
  if (divPeriod) divPeriod.addEventListener('change', () => refreshDividendCharts());

  wireTableToggles();
}
