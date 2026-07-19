/**
 * Backtesting — Chart.js visualizations.
 *
 * All chart creation and management functions.  The module receives a
 * reference to the backtesting state (ST) via `setChartsState()` so call
 * sites don't need to pass it explicitly.
 */

import { el } from '../common/utils.js';
import { annualize } from './utils.js';

// ---------------------------------------------------------------------------
// State injection
// ---------------------------------------------------------------------------

let _ST = null;

/**
 * Inject the backtesting state object so chart functions can access the
 * chart registry (ST.charts) without it being passed on every call.
 */
export function setChartsState(state) {
  _ST = state;
}

// ---------------------------------------------------------------------------
// Zero-line plugin (shared by scatter charts)
// ---------------------------------------------------------------------------

export const zeroLinePlugin = {
  id: 'zeroLine',
  afterDraw(chart) {
    const { ctx, scales: { x, y } } = chart;
    if (!x || !y) return;
    ctx.save();
    ctx.strokeStyle = 'rgba(255,255,255,0.12)';
    ctx.lineWidth = 1;
    ctx.setLineDash([4, 6]);
    // Vertical line at x=0
    if (x.min <= 0 && x.max >= 0) {
      const x0 = x.getPixelForValue(0);
      ctx.beginPath();
      ctx.moveTo(x0, y.top);
      ctx.lineTo(x0, y.bottom);
      ctx.stroke();
    }
    // Horizontal line at y=0
    if (y.min <= 0 && y.max >= 0) {
      const y0 = y.getPixelForValue(0);
      ctx.beginPath();
      ctx.moveTo(x.left, y0);
      ctx.lineTo(x.right, y0);
      ctx.stroke();
    }
    ctx.restore();
  },
};

// ---------------------------------------------------------------------------
// Chart export button
// ---------------------------------------------------------------------------

export function addChartExport(canvasId, filename) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const container = canvas.closest('.bt-chart-container');
  if (!container) return;

  const existing = container.querySelector('.bt-chart-export');
  if (existing) existing.remove();

  const btn = el('button', {
    class: 'bt-chart-export',
    text: '\u2b07',
    title: 'Download PNG',
    onclick: () => {
      const link = document.createElement('a');
      link.download = (filename || 'chart') + '.png';
      link.href = canvas.toDataURL('image/png');
      link.click();
    },
  });
  container.style.position = 'relative';
  container.appendChild(btn);
}

// ---------------------------------------------------------------------------
// Chart lifecycle
// ---------------------------------------------------------------------------

export function destroyChart(id) {
  if (_ST && _ST.charts[id]) {
    _ST.charts[id].destroy();
    delete _ST.charts[id];
  }
}

export function destroyAllCharts() {
  if (_ST) {
    Object.keys(_ST.charts).forEach(id => destroyChart(id));
  }
}

// ---------------------------------------------------------------------------
// Cumulative returns chart
// ---------------------------------------------------------------------------

export function createCumulativeChart(canvasId, chartData, hasBench) {
  /* global Chart */
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  destroyChart(canvasId);

  const ctx = canvas.getContext('2d');
  const dates = chartData.cumulative.map(d => d.date);
  const portfolio = chartData.cumulative.map(d => d.portfolio * 100);
  const hasVami = chartData.cumulative[0] && chartData.cumulative[0].vami != null;

  const datasets = [{
    label: 'Portfolio',
    data: portfolio,
    borderColor: '#58a6ff',
    backgroundColor: 'transparent',
    borderWidth: 2,
    pointRadius: 0,
    tension: 0.1,
    yAxisID: 'y',
  }];

  // VAMI: dollar value of the portfolio over time
  if (hasVami) {
    const vami = chartData.cumulative.map(d => d.vami);
    datasets.push({
      label: 'VAMI',
      data: vami,
      borderColor: '#3fb950',
      backgroundColor: 'transparent',
      borderWidth: 1.5,
      borderDash: [4, 4],
      pointRadius: 0,
      tension: 0.1,
      yAxisID: 'yVami',
    });
  }

  if (hasBench && chartData.cumulative[0] && chartData.cumulative[0].benchmark != null) {
    const benchmark = chartData.cumulative.map(d => (d.benchmark || 0) * 100);
    datasets.push({
      label: 'Benchmark',
      data: benchmark,
      borderColor: '#e0af4f',
      backgroundColor: 'transparent',
      borderWidth: 2,
      borderDash: [6, 3],
      pointRadius: 0,
      tension: 0.1,
      yAxisID: 'y',
    });
  }

  const scales = {
    x: { ticks: { color: '#8ea0b8', maxTicksLimit: 15, maxRotation: 0 } },
    y: {
      type: 'linear',
      position: 'left',
      ticks: { color: '#8ea0b8', callback: v => v.toFixed(1) + '%' },
      grid: { color: 'rgba(255,255,255,0.06)' },
    },
  };

  if (hasVami) {
    scales.yVami = {
      type: 'linear',
      position: 'right',
      ticks: { color: '#3fb950', callback: v => (v >= 1e6 ? (v / 1e6).toFixed(1) + 'M' : (v / 1e3).toFixed(0) + 'K') },
      grid: { display: false },
      title: { display: true, text: 'VAMI', color: '#3fb950' },
    };
  }

  _ST.charts[canvasId] = new Chart(ctx, {
    type: 'line',
    data: { labels: dates, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: '#d9e2f2', font: { family: 'IBM Plex Mono' } } },
        tooltip: { mode: 'index', intersect: false },
      },
      scales,
    },
  });
  addChartExport(canvasId, 'cumulative-returns');
}

// ---------------------------------------------------------------------------
// Drawdown chart
// ---------------------------------------------------------------------------

export function createDrawdownChart(canvasId, chartData, hasBench) {
  /* global Chart */
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  destroyChart(canvasId);

  const ctx = canvas.getContext('2d');
  const dates = chartData.drawdown.map(d => d.date);
  const portfolio = chartData.drawdown.map(d => d.portfolio * 100);
  const datasets = [{
    label: 'Portfolio Drawdown',
    data: portfolio,
    borderColor: 'rgba(220,50,50,0.6)',
    backgroundColor: 'rgba(220,50,50,0.1)',
    fill: { target: 'origin', above: 'transparent', below: 'rgba(220,50,50,0.1)' },
    borderWidth: 1.5,
    pointRadius: 0,
    tension: 0.1,
  }];

  if (hasBench && chartData.drawdown[0] && chartData.drawdown[0].benchmark != null) {
    const benchmark = chartData.drawdown.map(d => (d.benchmark || 0) * 100);
    datasets.push({
      label: 'Benchmark Drawdown',
      data: benchmark,
      borderColor: 'rgba(224,175,79,0.7)',
      backgroundColor: 'transparent',
      borderWidth: 1.5,
      borderDash: [6, 3],
      pointRadius: 0,
      tension: 0.1,
    });
  }

  _ST.charts[canvasId] = new Chart(ctx, {
    type: 'line',
    data: { labels: dates, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: '#d9e2f2', font: { family: 'IBM Plex Mono' } } },
        tooltip: { mode: 'index', intersect: false },
      },
      scales: {
        x: { ticks: { color: '#8ea0b8', maxTicksLimit: 15, maxRotation: 0 } },
        y: {
          ticks: { color: '#8ea0b8', callback: v => v.toFixed(1) + '%' },
          grid: { color: 'rgba(255,255,255,0.06)' },
        },
      },
    },
  });
  addChartExport(canvasId, 'drawdown');
}

// ---------------------------------------------------------------------------
// Return decomposition chart
// ---------------------------------------------------------------------------

export function createDecompositionChart(canvasId, chartData) {
  /* global Chart */
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  destroyChart(canvasId);

  const ctx = canvas.getContext('2d');
  const dates = chartData.decomposition.map(d => d.date);

  _ST.charts[canvasId] = new Chart(ctx, {
    type: 'line',
    data: {
      labels: dates,
      datasets: [
        {
          label: 'Price Return',
          data: chartData.decomposition.map(d => d.price_only * 100),
          borderColor: '#42A5F5',
          backgroundColor: 'rgba(66,165,245,0.25)',
          fill: { target: 'origin' },
          borderWidth: 1.5,
          pointRadius: 0,
          tension: 0.1,
          order: 2,
        },
        {
          label: 'Dividend Return',
          data: chartData.decomposition.map(d => (d.total - d.price_only) * 100),
          borderColor: '#66BB6A',
          backgroundColor: 'rgba(102,187,106,0.25)',
          fill: '-1',
          borderWidth: 1.5,
          pointRadius: 0,
          tension: 0.1,
          order: 1,
        },
        {
          label: 'Total Return',
          data: chartData.decomposition.map(d => d.total * 100),
          borderColor: '#1B5E20',
          backgroundColor: 'transparent',
          fill: false,
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.1,
          order: 0,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { labels: { color: '#d9e2f2', font: { family: 'IBM Plex Mono' } } },
        tooltip: { mode: 'index', intersect: false },
      },
      scales: {
        x: { ticks: { color: '#8ea0b8', maxTicksLimit: 10, maxRotation: 0 } },
        y: {
          stacked: true,
          ticks: { color: '#8ea0b8', callback: v => (v >= 0 ? '+' : '') + v.toFixed(1) + '%' },
          grid: { color: 'rgba(255,255,255,0.06)' },
        },
      },
    },
  });
  addChartExport(canvasId, 'decomposition');
}

// ---------------------------------------------------------------------------
// Per-company scatter chart
// ---------------------------------------------------------------------------

export function createPerCompanyChart(canvasId, companies, startDate, endDate) {
  /* global Chart */
  const canvas = document.getElementById(canvasId);
  if (!canvas || !companies.length) return;
  destroyChart(canvasId);

  const ctx = canvas.getContext('2d');
  const points = companies.map(c => {
    const annPrice = startDate && endDate ? annualize(c.price_return || 0, startDate, endDate) : c.price_return;
    const annDiv = startDate && endDate ? annualize(c.dividend_return || 0, startDate, endDate) : c.dividend_return;
    const annTotal = startDate && endDate ? annualize(c.total_return || 0, startDate, endDate) : c.total_return;
    return {
      x: annPrice * 100,
      y: annDiv * 100,
      r: Math.max(4, Math.min(14, Math.abs(annTotal) * 100)),
      ticker: c.Ticker,
      total: annTotal * 100,
      price: annPrice * 100,
      div: annDiv * 100,
    };
  });

  _ST.charts[canvasId] = new Chart(ctx, {
    type: 'bubble',
    data: {
      datasets: [{
        label: 'Companies',
        data: points,
        backgroundColor: points.map(p => {
          const t = Math.max(-1, Math.min(1, p.total / 25));
          if (t >= 0) {
            const g = 150 + Math.round(105 * t);
            return `rgba(68,${g},123,0.75)`;
          }
          const r = 200 + Math.round(55 * Math.abs(t));
          return `rgba(${r},68,68,0.75)`;
        }),
        borderColor: 'rgba(255,255,255,0.2)',
        borderWidth: 1,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const p = ctx.raw;
              const tk = p.ticker || '?';
              return `${tk}: ann. price ${p.price >= 0 ? '+' : ''}${p.price.toFixed(1)}%, ann. div ${p.div >= 0 ? '+' : ''}${p.div.toFixed(1)}% (ann. total ${p.total >= 0 ? '+' : ''}${p.total.toFixed(1)}%)`;
            },
          },
        },
      },
      scales: {
        x: {
          title: { display: true, text: 'Ann. Price Return (%)', color: '#8ea0b8' },
          ticks: { color: '#8ea0b8', callback: v => (v >= 0 ? '+' : '') + v.toFixed(1) + '%' },
          grid: { color: 'rgba(255,255,255,0.06)' },
        },
        y: {
          title: { display: true, text: 'Ann. Dividend Return (%)', color: '#8ea0b8' },
          ticks: { color: '#8ea0b8', callback: v => (v >= 0 ? '+' : '') + v.toFixed(1) + '%' },
          grid: { color: 'rgba(255,255,255,0.06)' },
        },
      },
    },
    plugins: [zeroLinePlugin],
  });
  addChartExport(canvasId, 'per-company-scatter');
}

// ---------------------------------------------------------------------------
// Yearly scatter chart
// ---------------------------------------------------------------------------

export function createYearlyScatterChart(canvasId, yearly) {
  /* global Chart */
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  destroyChart(canvasId);

  const ctx = canvas.getContext('2d');
  const points = yearly.map(y => {
    const pr = y['Price Return'] || y.price_return || 0;
    const dr = y['Dividend Return'] || y.dividend_return || 0;
    const tr = y['Total Return'] || y.total_return || 0;
    return {
      x: pr * 100,
      y: dr * 100,
      r: Math.max(5, Math.min(18, Math.abs(tr) * 80)),
      year: y.Year || y.year,
      total: tr * 100,
    };
  });

  _ST.charts[canvasId] = new Chart(ctx, {
    type: 'bubble',
    data: {
      datasets: [{
        label: 'Years',
        data: points,
        backgroundColor: points.map(p => {
          const t = Math.max(-1, Math.min(1, p.total / 40));
          if (t >= 0) {
            const g = 150 + Math.round(105 * t);
            return `rgba(68,${g},123,0.8)`;
          }
          const r = 200 + Math.round(55 * Math.abs(t));
          return `rgba(${r},68,68,0.8)`;
        }),
        borderColor: 'rgba(255,255,255,0.25)',
        borderWidth: 1.5,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const p = ctx.raw;
              const yr = p.year || '?';
              return `${yr}: price ${p.x >= 0 ? '+' : ''}${p.x.toFixed(1)}%, div ${p.y >= 0 ? '+' : ''}${p.y.toFixed(1)}% (total ${p.total >= 0 ? '+' : ''}${p.total.toFixed(1)}%)`;
            },
          },
        },
      },
      scales: {
        x: {
          title: { display: true, text: 'Price Return (%)', color: '#8ea0b8' },
          ticks: { color: '#8ea0b8', callback: v => (v >= 0 ? '+' : '') + v.toFixed(1) + '%' },
          grid: { color: 'rgba(255,255,255,0.06)' },
        },
        y: {
          title: { display: true, text: 'Dividend Return (%)', color: '#8ea0b8' },
          ticks: { color: '#8ea0b8', callback: v => (v >= 0 ? '+' : '') + v.toFixed(1) + '%' },
          grid: { color: 'rgba(255,255,255,0.06)' },
        },
      },
    },
    plugins: [zeroLinePlugin],
  });
  addChartExport(canvasId, 'yearly-scatter');
}
