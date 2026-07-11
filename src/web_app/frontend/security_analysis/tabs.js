/**
 * Tab bar for switching between statement families.
 */

import { state, persist } from './state.js';
import { renderWorkspace } from './treegrid.js';
import { renderChartPanel } from './chartpanel.js';
import { renderPriceTab } from './price.js';

const TAB_DEFS = [
  { key: 'IncomeStatement',  label: 'Income',       icon: '📈' },
  { key: 'BalanceSheet',     label: 'Balance',      icon: '📊' },
  { key: 'CashflowStatement',label: 'Cashflow',     icon: '💵' },
  { key: 'Financial_Ratios', label: 'Ratios',       icon: '📐' },
  { key: 'PerShare_Metrics', label: 'Per Share',    icon: '📋' },
  { key: 'ShareMetrics',     label: 'Share Data',   icon: '👥' },
];

const H = {
  $(id) { return document.getElementById(id); },
  el(tag, attrs = {}, ...children) {
    const el = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (k === 'class' || k === 'className') el.className = v;
      else if (k === 'text') el.textContent = v ?? '';
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

export function renderTabs() {
  const tabbar = H.$('sa-tabbar');
  const hasCompany = !!(state.company?.company?.company_code);
  const hasTicker = !!(state.company?.company?.ticker);

  if (!hasCompany && !hasTicker) {
    tabbar.classList.remove('is-visible');
    return;
  }

  tabbar.classList.add('is-visible');
  tabbar.innerHTML = '';

  // Stock Price tab (always first if ticker exists)
  if (hasTicker) {
    const isActive = state.activeTab === '__stock_price__' || state.activeTab === null;
    if (state.activeTab === null && hasTicker) {
      // Don't auto-set — let renderWorkspace decide
    }
    tabbar.appendChild(H.el('button', {
      class: `sa-tab${isActive ? ' is-active' : ''}`,
      text: '💹 Price',
      onclick() {
        state.activeTab = '__stock_price__';
        state.selectedRows = [];
        persist();
        renderWorkspace();
        renderChartPanel();
      },
    }));
  }

  // Financial statement tabs
  for (const def of TAB_DEFS) {
    const isActive = state.activeTab === def.key;
    if (state.activeTab === null && hasCompany && def.key === 'IncomeStatement') {
      // Default to Income Statement when first loading
      // (handled in renderWorkspace)
    }
    tabbar.appendChild(H.el('button', {
      class: `sa-tab${isActive ? ' is-active' : ''}`,
      text: `${def.icon} ${def.label}`,
      onclick() {
        state.activeTab = def.key;
        state.selectedRows = [];
        persist();
        renderWorkspace();
        renderChartPanel();
      },
    }));
  }
}

/**
 * Get the statement family for the given tab key.
 * For non-taxonomy tables (Financial_Ratios, PerShare_Metrics, ShareMetrics),
 * this maps to the table name.
 */
export function getStatementFamily(tabKey) {
  return tabKey;
}
