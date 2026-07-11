/**
 * Chart panel — right-side Chart.js chart for selected metric(s).
 */

import { state } from './state.js';
import { fmtCur } from './summary.js';

// Lazy ref to avoid circular import — set by treegrid.js at init
let _renderTreeGrid = null;
export function setTreeGridRenderer(fn) { _renderTreeGrid = fn; }

const CHART_COLORS = [
  '#58a6ff', '#44d17b', '#e0af4f', '#ff6b6b', '#b794f4',
  '#56d4dd', '#f48fb1', '#a5d6a7', '#90caf9', '#ffcc80',
  '#ef9a9a', '#80cbc4', '#b0bec5', '#ffe082',
];

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

let chartInstance = null;

export function renderChartPanel() {
  const panel = H.$('sa-chart-panel');
  if (!panel) return;

  const split = H.$('sa-workspace-split');
  if (!split || !split.classList.contains('is-visible')) {
    panel.innerHTML = '';
    return;
  }

  // Price tab handles its own chart
  if (state.activeTab === '__stock_price__') {
    // chart is managed by price.js
    return;
  }

  panel.innerHTML = '';

  if (!state.selectedRows.length) {
    panel.appendChild(H.el('div', { class: 'sa-chart-placeholder' },
      H.el('div', { style: 'font-size:28px;opacity:0.3;', text: '📈' }),
      H.el('div', { text: 'Select a metric to view trend' }),
      H.el('div', { style: 'font-size:10px;opacity:0.6;', text: 'Click a row in the tree grid' }),
    ));
    return;
  }

  // Get the data for selected rows
  const family = state.selectedRows[0]?.family;
  const treeData = family ? state.taxonomyTrees[family] : null;
  const periods = treeData?.periods || [];

  if (!periods.length) {
    panel.appendChild(H.el('div', { class: 'sa-chart-placeholder' },
      H.el('div', { text: 'No period data available' }),
    ));
    return;
  }

  // Build chart head
  const primary = state.selectedRows[0];
  const primaryValues = getNodeValues(treeData?.tree || [], primary.concept_qname);
  const latestVal = primaryValues ? primaryValues[primaryValues.length - 1] : null;

  const head = H.el('div', { class: 'sa-chart-head' },
    H.el('div', {},
      H.el('div', { class: 'sa-chart-title', text: primary.label }),
      latestVal != null ? H.el('div', { class: 'sa-chart-latest', text: `Latest: ${fmtCur(latestVal)}` }) : null,
    ),
    H.el('div', { class: 'sa-chart-actions' },
      H.el('button', {
        class: 'sa-tree-btn',
        text: '✕ Clear',
        onclick() {
          state.selectedRows = [];
          renderChartPanel();
          if (_renderTreeGrid) _renderTreeGrid();
        },
      }),
    ),
  );
  panel.appendChild(head);

  // Chart body
  const body = H.el('div', { class: 'sa-chart-body' });
  const canvas = H.el('canvas');
  body.appendChild(canvas);
  panel.appendChild(body);

  // Build datasets
  const datasets = state.selectedRows.map((row, i) => {
    const values = getNodeValues(treeData?.tree || [], row.concept_qname) || [];
    const color = CHART_COLORS[i % CHART_COLORS.length];
    return {
      label: row.label,
      data: periods.map((_, j) => (j < values.length ? values[j] : null)),
      borderColor: color,
      backgroundColor: color + '30',
      borderWidth: 2,
      tension: 0.1,
      pointRadius: state.selectedRows.length > 4 ? 0 : 3,
      pointHoverRadius: 5,
      spanGaps: true,
    };
  });

  // Destroy previous chart
  if (chartInstance) {
    try { chartInstance.destroy(); } catch (e) { /* noop */ }
    chartInstance = null;
  }

  if (typeof Chart === 'undefined') {
    body.innerHTML = '<div class="sa-chart-placeholder"><div>Chart.js not loaded</div></div>';
    return;
  }

  chartInstance = new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: { labels: periods, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      plugins: {
        legend: {
          position: 'top',
          labels: {
            color: '#8ea0b8',
            font: { size: 10, family: "'IBM Plex Mono', monospace" },
            boxWidth: 10,
            padding: 8,
            usePointStyle: true,
          },
        },
        tooltip: {
          backgroundColor: '#111c2a',
          borderColor: '#243244',
          borderWidth: 1,
          titleFont: { family: "'IBM Plex Mono', monospace", size: 10 },
          bodyFont: { family: "'IBM Plex Mono', monospace", size: 10 },
          callbacks: {
            label(ctx) {
              return `${ctx.dataset.label}: ${fmtCur(ctx.raw)}`;
            },
          },
        },
        zoom: (typeof window !== 'undefined' && window.chartjsPluginZoom) ? {
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
        x: {
          ticks: { color: '#8ea0b8', font: { size: 9, family: "'IBM Plex Mono', monospace" }, maxTicksLimit: 12 },
          grid: { color: 'rgba(48,63,82,0.3)' },
        },
        y: {
          ticks: {
            color: '#8ea0b8',
            font: { size: 9, family: "'IBM Plex Mono', monospace" },
            callback(v) { return fmtCur(v); },
          },
          grid: { color: 'rgba(48,63,82,0.2)' },
        },
      },
    },
  });

  // Selected chips at bottom
  if (state.selectedRows.length > 0) {
    const chipsBar = H.el('div', { class: 'sa-chart-selected' });
    state.selectedRows.forEach((row, i) => {
      chipsBar.appendChild(H.el('span', {
        class: 'sa-chart-selected-chip',
        onclick() {
          state.selectedRows.splice(i, 1);
          renderChartPanel();
        },
      },
        H.el('span', { class: 'sa-chip-color', style: { background: CHART_COLORS[i % CHART_COLORS.length] } }),
        H.el('span', { text: row.label }),
      ));
    });
    panel.appendChild(chipsBar);
  }
}

function getNodeValues(tree, conceptQname) {
  for (const node of tree) {
    if (node.concept_qname === conceptQname) return node.values;
    if (node.children) {
      const found = getNodeValues(node.children, conceptQname);
      if (found) return found;
    }
  }
  return null;
}

export function destroyChart() {
  if (chartInstance) {
    try { chartInstance.destroy(); } catch (e) { /* noop */ }
    chartInstance = null;
  }
}
