/**
 * Stock Price tab — table + chart split view.
 */

import { log } from '../common/console.js';
import { fetchJson } from '../common/utils.js';
import { state, persist } from './state.js';
import { destroyChart } from './chartpanel.js';

const H = {
  $(id) { return document.getElementById(id); },
  el(tag, attrs = {}, ...children) {
    const el = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (k === 'class' || k === 'className') el.className = v;
      else if (k === 'text') el.textContent = v ?? '';
      else if (k === 'html') el.innerHTML = v;
      else if (k === 'style' && v && typeof v === 'object') Object.assign(el.style, v);
      else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2), v);
      else if (v !== undefined && v !== null) el.setAttribute(k, v);
    }
    for (const c of children.flat()) {
      if (c != null && c !== false) el.append(c.nodeType ? c : document.createTextNode(String(c)));
    }
    return el;
  },
};

let priceChart = null;

export async function renderPriceTab() {
  const treePanel = H.$('sa-tree-panel');
  const chartPanel = H.$('sa-chart-panel');
  const ticker = state.company?.company?.ticker;

  if (!ticker) {
    treePanel.innerHTML = '<div class="sa-loading">No ticker available</div>';
    chartPanel.innerHTML = '';
    return;
  }

  // Load price data if needed
  if (!state.priceData && !state.priceLoading) {
    await loadPriceData(ticker);
  }

  if (!state.priceData) {
    treePanel.innerHTML = '<div class="sa-loading">Loading price data…</div>';
    chartPanel.innerHTML = '';
    return;
  }

  renderPriceTable();
  renderPriceChartPanel();
}

async function loadPriceData(ticker) {
  state.priceLoading = true;
  try {
    const d = await fetchJson(`/api/security/price-history?ticker=${encodeURIComponent(ticker)}`);
    state.priceData = d.prices || [];
  } catch (e) {
    log('error', `Price history: ${e.message}`);
    state.priceData = [];
  } finally {
    state.priceLoading = false;
  }
}

// ── Left panel: Price table ──

function renderPriceTable() {
  const treePanel = H.$('sa-tree-panel');
  treePanel.innerHTML = '';

  const data = state.priceData;
  if (!data.length) {
    treePanel.innerHTML = '<div class="sa-loading">No price data</div>';
    return;
  }

  // Toolbar with period pills
  const toolbar = H.el('div', { class: 'sa-price-toolbar' });
  const periods = [
    { id: '1w', label: '1W' }, { id: '1m', label: '1M' }, { id: 'mtd', label: 'MTD' },
    { id: 'ytd', label: 'YTD' }, { id: '1y', label: '1Y' }, { id: '2y', label: '2Y' },
    { id: '3y', label: '3Y' }, { id: '5y', label: '5Y' }, { id: '10y', label: '10Y' },
    { id: 'all', label: 'All' },
  ];
  for (const p of periods) {
    toolbar.appendChild(H.el('button', {
      class: `sa-period-pill${state.pricePeriod === p.id ? ' is-active' : ''}`,
      text: p.label,
      onclick() {
        state.pricePeriod = p.id;
        persist();
        renderPriceChartPanel();
      },
    }));
  }
  treePanel.appendChild(toolbar);

  // Summary bar
  const allPrices = data.filter(d => d.price != null).map(d => d.price);
  const minPrice = allPrices.length ? Math.min(...allPrices) : null;
  const maxPrice = allPrices.length ? Math.max(...allPrices) : null;
  const firstPrice = data[0]?.price;
  const lastPrice = data[data.length - 1]?.price;
  const change = (firstPrice && lastPrice) ? lastPrice - firstPrice : null;
  const changePct = (change != null && firstPrice) ? (change / firstPrice) : null;

  treePanel.appendChild(H.el('div', { class: 'sa-price-summary' },
    H.el('span', { text: `${data[0]?.trade_date || ''} → ${data[data.length - 1]?.trade_date || ''}` }),
    H.el('span', { text: `| ${data.length} days` }),
    minPrice != null ? H.el('span', { text: `| Low ¥${Number(minPrice).toLocaleString()}` }) : null,
    maxPrice != null ? H.el('span', { text: `| High ¥${Number(maxPrice).toLocaleString()}` }) : null,
    changePct != null ? H.el('span', {
      style: change >= 0 ? 'color:var(--success);' : 'color:var(--danger);',
      text: `| ${change >= 0 ? '+' : ''}${(changePct * 100).toFixed(1)}% total`,
    }) : null,
  ));

  // Table
  const wrap = H.el('div', { class: 'sa-price-table-wrap' });
  const tbl = H.el('table', { class: 'sa-price-table' });

  // Sort
  const sf = state.priceSortField;
  const sd = state.priceSortDir === 'asc' ? 1 : -1;

  // Compute day change
  const withChange = [];
  for (let i = 0; i < data.length; i++) {
    const d = data[i];
    const prevPrice = i > 0 ? data[i - 1].price : null;
    const dayChange = (prevPrice != null && d.price != null) ? d.price - prevPrice : null;
    withChange.push({ ...d, _change: dayChange });
  }

  const sorted = [...withChange].sort((a, b) => {
    let va, vb;
    if (sf === 'price') { va = a.price ?? -Infinity; vb = b.price ?? -Infinity; }
    else if (sf === '_change') { va = a._change ?? -Infinity; vb = b._change ?? -Infinity; }
    else { va = a.trade_date || ''; vb = b.trade_date || ''; }
    if (va < vb) return -1 * sd; if (va > vb) return 1 * sd; return 0;
  });

  const thead = H.el('thead', {},
    H.el('tr', {},
      makeSortHdr('trade_date', 'Date'),
      makeSortHdr('price', 'Price'),
      makeSortHdr('_change', 'Change'),
    ));
  tbl.appendChild(thead);

  const tbody = H.el('tbody');
  const pageSize = state.priceTablePage;
  const shown = sorted.slice(0, pageSize);
  for (const d of shown) {
    const p = d.price;
    const dc = d._change;
    tbody.appendChild(H.el('tr', {},
      H.el('td', { style: 'text-align:left;', text: d.trade_date }),
      H.el('td', { text: p != null ? `¥${Number(p).toLocaleString()}` : '—' }),
      H.el('td', {
        style: dc != null ? (dc >= 0 ? 'color:var(--success);' : 'color:var(--danger);') : '',
        text: dc != null ? `${dc >= 0 ? '+' : ''}${Number(dc).toLocaleString()}` : '—',
      }),
    ));
  }
  tbl.appendChild(tbody);
  wrap.appendChild(tbl);
  treePanel.appendChild(wrap);

  // Load More
  if (pageSize < sorted.length) {
    const remaining = sorted.length - pageSize;
    treePanel.appendChild(H.el('div', { class: 'sa-load-more-wrap' },
      H.el('button', {
        class: 'sa-tree-btn',
        text: `Load More (${remaining} remaining)`,
        onclick() {
          state.priceTablePage = Math.min(state.priceTablePage + 60, sorted.length);
          persist();
          renderPriceTable();
          renderPriceChartPanel();
        },
      }),
    ));
  }

  function makeSortHdr(field, label) {
    const active = state.priceSortField === field;
    const arrow = active ? (state.priceSortDir === 'asc' ? ' ▲' : ' ▼') : '';
    return H.el('th', {
      text: label + arrow,
      onclick() {
        if (state.priceSortField === field) {
          state.priceSortDir = state.priceSortDir === 'asc' ? 'desc' : 'asc';
        } else {
          state.priceSortField = field;
          state.priceSortDir = field === 'trade_date' ? 'desc' : 'asc';
        }
        persist();
        renderPriceTable();
      },
    });
  }
}

// ── Right panel: Price chart ──

function renderPriceChartPanel() {
  const chartPanel = H.$('sa-chart-panel');
  chartPanel.innerHTML = '';

  const data = state.priceData;
  if (!data?.length) {
    chartPanel.innerHTML = '<div class="sa-chart-placeholder"><div>No price data</div></div>';
    return;
  }

  const filtered = filterPriceData(data, state.pricePeriod);
  if (!filtered.length) {
    chartPanel.innerHTML = '<div class="sa-chart-placeholder"><div>No data in selected period</div></div>';
    return;
  }

  // Destroy old chart
  destroyChart();
  if (priceChart) { try { priceChart.destroy(); } catch (e) { /* noop */ } priceChart = null; }

  // Head
  const head = H.el('div', { class: 'sa-chart-head' },
    H.el('div', {},
      H.el('div', { class: 'sa-chart-title', text: `${state.company?.company?.ticker || ''} — Price` }),
      H.el('div', { class: 'sa-chart-latest', text: `Latest: ¥${Number(filtered[filtered.length - 1].price).toLocaleString()}` }),
    ),
    H.el('div', { class: 'sa-chart-actions' },
      H.el('button', {
        class: 'sa-tree-btn', text: '↺ Reset',
        onclick() { if (priceChart) priceChart.resetZoom(); },
      }),
    ),
  );
  chartPanel.appendChild(head);

  // Body
  const body = H.el('div', { class: 'sa-chart-body' });
  const canvas = H.el('canvas');
  body.appendChild(canvas);
  chartPanel.appendChild(body);

  if (typeof Chart === 'undefined') {
    body.innerHTML = '<div class="sa-chart-placeholder"><div>Chart.js N/A</div></div>';
    return;
  }

  const labels = filtered.map(d => d.trade_date);
  const prices = filtered.map(d => d.price);

  const sma20 = calcSMA(prices, 20);
  const sma50 = calcSMA(prices, 50);
  const sma200 = calcSMA(prices, 200);

  const datasets = [
    { label: 'Close', data: prices, borderColor: '#58a6ff', backgroundColor: 'rgba(88,166,255,0.08)', borderWidth: 1.5, tension: 0, pointRadius: 0, spanGaps: false, fill: true },
  ];
  if (sma20.some(v => v != null)) datasets.push({ label: 'SMA 20', data: sma20, borderColor: '#e0af4f', borderWidth: 1, tension: 0, pointRadius: 0, spanGaps: true });
  if (sma50.some(v => v != null)) datasets.push({ label: 'SMA 50', data: sma50, borderColor: '#ff6b6b', borderWidth: 1, tension: 0, pointRadius: 0, spanGaps: true });
  if (sma200.some(v => v != null)) datasets.push({ label: 'SMA 200', data: sma200, borderColor: '#b794f4', borderWidth: 1, tension: 0, pointRadius: 0, spanGaps: true });

  const hasZoom = typeof window !== 'undefined' && window.chartjsPluginZoom;

  priceChart = new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      plugins: {
        legend: { position: 'top', labels: { color: '#8ea0b8', font: { size: 10, family: "'IBM Plex Mono', monospace" }, boxWidth: 10, padding: 8, usePointStyle: true } },
        tooltip: {
          backgroundColor: '#111c2a', borderColor: '#243244', borderWidth: 1,
          callbacks: { label: ctx => `¥${Number(ctx.raw).toLocaleString()}` },
        },
        zoom: hasZoom ? {
          pan: { enabled: true, mode: 'x', modifierKey: 'ctrl' },
          zoom: {
            wheel: { enabled: true, modifierKey: 'ctrl' },
            pinch: { enabled: true },
            drag: { enabled: true, backgroundColor: 'rgba(88,166,255,0.15)', borderColor: '#58a6ff', borderWidth: 1 },
            mode: 'x',
          },
          limits: { x: { min: 'original', max: 'original' } },
        } : undefined,
      },
      scales: {
        x: { ticks: { color: '#8ea0b8', font: { size: 9, family: "'IBM Plex Mono', monospace" }, maxTicksLimit: 15, autoSkip: true }, grid: { color: 'rgba(48,63,82,0.4)' } },
        y: { ticks: { color: '#8ea0b8', font: { size: 9, family: "'IBM Plex Mono', monospace" }, callback: v => `¥${Number(v).toLocaleString()}` }, grid: { color: 'rgba(48,63,82,0.3)' } },
      },
    },
  });
}

function filterPriceData(data, period) {
  if (period === 'all' || !data.length) return data;
  const lastDate = new Date(data[data.length - 1].trade_date);
  let cutoff = new Date(lastDate);
  switch (period) {
    case '1w': cutoff.setDate(cutoff.getDate() - 7); break;
    case '1m': cutoff.setMonth(cutoff.getMonth() - 1); break;
    case 'mtd': cutoff = new Date(lastDate.getFullYear(), lastDate.getMonth(), 1); break;
    case 'ytd': cutoff = new Date(lastDate.getFullYear(), 0, 1); break;
    case '1y': cutoff.setFullYear(cutoff.getFullYear() - 1); break;
    case '2y': cutoff.setFullYear(cutoff.getFullYear() - 2); break;
    case '3y': cutoff.setFullYear(cutoff.getFullYear() - 3); break;
    case '5y': cutoff.setFullYear(cutoff.getFullYear() - 5); break;
    case '10y': cutoff.setFullYear(cutoff.getFullYear() - 10); break;
    default: return data;
  }
  const cs = cutoff.toISOString().slice(0, 10);
  return data.filter(d => d.trade_date >= cs);
}

function calcSMA(data, period) {
  const result = new Array(data.length).fill(null);
  if (data.length < period) return result;
  let sum = 0;
  for (let i = 0; i < period; i++) sum += (data[i] || 0);
  result[period - 1] = sum / period;
  for (let i = period; i < data.length; i++) {
    sum += (data[i] || 0) - (data[i - period] || 0);
    result[i] = sum / period;
  }
  return result;
}
