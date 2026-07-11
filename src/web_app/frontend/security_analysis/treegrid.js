/**
 * Hierarchical tree grid — collapsible rows, row selection, chart linkage.
 */

import { log } from '../common/console.js';
import { fetchJson } from '../common/utils.js';
import { state, persist } from './state.js';
import { renderChartPanel, setTreeGridRenderer } from './chartpanel.js';
import { renderPriceTab } from './price.js';
import { getStatementFamily } from './tabs.js';
import { fmtCur } from './summary.js';

// Register lazy callback so chartpanel can re-render tree after clear
setTreeGridRenderer(() => {
  const family = state.activeTab;
  if (family && state.taxonomyTrees[family]) {
    renderTreeGrid(state.taxonomyTrees[family], family);
  }
});

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

export async function renderWorkspace() {
  const split = H.$('sa-workspace-split');
  const treePanel = H.$('sa-tree-panel');
  const hasCompany = !!(state.company?.company?.company_code);
  const hasTicker = !!(state.company?.company?.ticker);

  // Default active tab
  if (state.activeTab === null) {
    if (hasTicker && !hasCompany) {
      state.activeTab = '__stock_price__';
    } else if (hasCompany) {
      state.activeTab = 'IncomeStatement';
    }
  }

  if (!state.activeTab) {
    split.classList.remove('is-visible');
    return;
  }

  split.classList.add('is-visible');

  if (state.activeTab === '__stock_price__') {
    renderPriceTab();
    return;
  }

  // Check if we have taxonomy tree data for this family
  const family = getStatementFamily(state.activeTab);
  if (!state.taxonomyTrees[family]) {
    await loadTaxonomyTree(family);
  }

  const treeData = state.taxonomyTrees[family];
  if (!treeData) {
    treePanel.innerHTML = '<div class="sa-loading">No data available</div>';
    return;
  }

  renderTreeGrid(treeData, family);
}

async function loadTaxonomyTree(family) {
  const code = state.company?.company?.company_code;
  if (!code) return;

  state.loadingTree = true;
  try {
    const data = await fetchJson(
      `/api/security/taxonomy-tree?company_code=${encodeURIComponent(code)}&statement_family=${encodeURIComponent(family)}&periods=20`
    );
    state.taxonomyTrees[family] = data;
  } catch (e) {
    log('error', `Taxonomy tree: ${e.message}`);
    state.taxonomyTrees[family] = null;
  } finally {
    state.loadingTree = false;
  }
}

function renderTreeGrid(treeData, family) {
  const treePanel = H.$('sa-tree-panel');
  treePanel.innerHTML = '';

  const periods = treeData.periods || [];
  const tree = treeData.tree || [];

  if (!tree.length && !periods.length) {
    treePanel.innerHTML = '<div class="sa-loading">No data for this statement family</div>';
    return;
  }

  // ── Toolbar ──
  const hiddenSet = state.hiddenMetrics[family] || new Set();
  const collapsedSet = state.collapsedNodes[family] || new Set();

  const toolbar = H.el('div', { class: 'sa-tree-toolbar' },
    H.el('input', {
      class: 'sa-tree-filter',
      placeholder: 'Filter metrics…',
      value: state.searchFilter || '',
      oninput() {
        state.searchFilter = this.value;
        renderTreeGrid(treeData, family);
      },
    }),
    H.el('button', {
      class: `sa-tree-btn${state.hideEmpty ? ' is-active' : ''}`,
      text: 'Hide Empty',
      onclick() {
        state.hideEmpty = !state.hideEmpty;
        persist();
        renderTreeGrid(treeData, family);
      },
    }),
    H.el('button', {
      class: 'sa-tree-btn',
      text: 'Collapse All',
      onclick() {
        const all = new Set();
        collectParentQNames(tree, all);
        state.collapsedNodes[family] = all;
        persist();
        renderTreeGrid(treeData, family);
      },
    }),
    H.el('button', {
      class: 'sa-tree-btn',
      text: 'Expand All',
      onclick() {
        state.collapsedNodes[family] = new Set();
        persist();
        renderTreeGrid(treeData, family);
      },
    }),
    H.el('label', {
      class: 'sa-tree-btn',
      style: { cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' },
    },
      H.el('input', {
        type: 'checkbox',
        checked: state.millions,
        onchange() {
          state.millions = this.checked;
          persist();
          renderTreeGrid(treeData, family);
        },
      }),
      'M',
    ),
  );
  treePanel.appendChild(toolbar);

  // ── Tree body ──
  const body = H.el('div', { class: 'sa-tree-body' });
  const table = H.el('table', { class: 'sa-tree-table' });

  // Header
  const thead = H.el('thead', {},
    H.el('tr', {},
      H.el('th', { text: 'Metric' }),
      ...periods.map(p => H.el('th', { text: p })),
      H.el('th', { style: 'width:24px;', text: '' }),
    ));
  table.appendChild(thead);

  // Body — flatten tree respecting collapse/hidden/filter
  const tbody = H.el('tbody');
  const filterQ = (state.searchFilter || '').toLowerCase();
  const hiddenCount = { value: 0 };

  flattenTree(tree, 0, collapsedSet, hiddenSet, filterQ, tbody, periods, family, hiddenCount);

  table.appendChild(tbody);
  body.appendChild(table);
  treePanel.appendChild(body);
}

function collectParentQNames(nodes, out) {
  for (const node of nodes) {
    if (node.children && node.children.length > 0) {
      out.add(node.concept_qname);
      collectParentQNames(node.children, out);
    }
  }
}

function flattenTree(nodes, depth, collapsedSet, hiddenSet, filterQ, tbody, periods, family, hiddenCount) {
  for (const node of nodes) {
    const isHidden = hiddenSet.has(node.concept_qname);
    const hasChildren = node.children && node.children.length > 0;
    const matchesFilter = !filterQ || node.label.toLowerCase().includes(filterQ);

    // Check if any descendant matches filter
    const descMatchesFilter = hasChildren && anyDescendantMatches(node.children, filterQ);

    if (filterQ && !matchesFilter && !descMatchesFilter) continue;

    // Hide Empty: skip data nodes where all values are null
    if (state.hideEmpty && node.has_data && !hasChildren) {
      const vals = node.values || [];
      if (vals.every(v => v == null)) continue;
    }

    // Hidden row
    if (isHidden && !hasChildren) {
      hiddenCount.value++;
      continue;
    }

    const isCollapsed = collapsedSet.has(node.concept_qname);
    const isSelected = state.selectedRows.some(
      r => r.concept_qname === node.concept_qname && r.family === family
    );

    const tr = H.el('tr', {
      class: `sa-tree-row${isSelected ? ' is-selected' : ''}${hasChildren && !node.has_data ? ' is-parent' : ''}`,
      dataset: { qname: node.concept_qname, family },
      onclick(e) {
        // Don't select if clicking toggle or checkbox
        if (e.target.closest('.sa-tree-toggle') || e.target.closest('.sa-tree-check') || e.target.closest('.sa-tree-hide-btn')) return;
        if (!node.has_data && hasChildren) {
          // Toggle collapse on abstract row click
          toggleCollapse(family, node.concept_qname);
          return;
        }
        if (!node.has_data) return;
        selectRow(node, family);
      },
    });

    // ── Metric column (with indent + toggle + checkbox + label) ──
    const labelWrap = H.el('div', { class: 'sa-tree-label-wrap' });

    // Indentation
    if (depth > 0) {
      labelWrap.appendChild(H.el('span', {
        class: 'sa-tree-indent',
        style: { width: `${depth * 18}px` },
      }));
    }

    // Toggle chevron
    if (hasChildren) {
      labelWrap.appendChild(H.el('span', {
        class: `sa-tree-toggle${isCollapsed ? '' : ' is-open'}`,
        html: '▶',
        onclick(e) {
          e.stopPropagation();
          toggleCollapse(family, node.concept_qname);
        },
      }));
    } else {
      labelWrap.appendChild(H.el('span', { class: 'sa-tree-toggle is-leaf', html: '▶' }));
    }

    // Checkbox (for multi-select on chart)
    if (node.has_data) {
      labelWrap.appendChild(H.el('input', {
        type: 'checkbox',
        class: 'sa-tree-check',
        checked: isSelected,
        onclick(e) {
          e.stopPropagation();
          toggleSelectRow(node, family);
        },
      }));
    } else {
      // Invisible spacer for alignment
      labelWrap.appendChild(H.el('span', { style: 'width:12px;display:inline-block;flex-shrink:0;' }));
    }

    // Label
    labelWrap.appendChild(H.el('span', {
      class: `sa-tree-label${!node.has_data ? ' is-abstract' : ' has-data'}`,
      text: node.label,
      title: node.concept_qname,
    }));

    tr.appendChild(H.el('td', {}, labelWrap));

    // ── Value columns ──
    const vals = node.values || [];
    for (let i = 0; i < periods.length; i++) {
      const v = i < vals.length ? vals[i] : null;
      const td = H.el('td', { class: 'sa-tree-val' });
      if (v == null) {
        td.className += ' is-null';
        td.textContent = '—';
      } else {
        const n = Number(v);
        if (n < 0) td.className += ' is-negative';
        td.textContent = fmtVal(n, state.millions);
      }
      tr.appendChild(td);
    }

    // ── Hide button ──
    tr.appendChild(H.el('td', {},
      H.el('button', {
        class: 'sa-tree-hide-btn',
        text: '×',
        title: 'Hide row',
        onclick(e) {
          e.stopPropagation();
          const h = state.hiddenMetrics[family] || new Set();
          h.add(node.concept_qname);
          state.hiddenMetrics[family] = h;
          persist();
          renderTreeGrid(state.taxonomyTrees[family], family);
        },
      }),
    ));

    tbody.appendChild(tr);

    // ── Children ──
    if (hasChildren && !isCollapsed) {
      flattenTree(node.children, depth + 1, collapsedSet, hiddenSet, filterQ, tbody, periods, family, hiddenCount);
    }
  }

  // Show hidden count
  if (depth === 0 && hiddenCount.value > 0) {
    tbody.appendChild(H.el('tr', { class: 'sa-tree-row' },
      H.el('td', {
        colSpan: String(periods.length + 2),
        style: 'color:var(--muted);font-size:10px;padding:6px 10px;',
        text: `${hiddenCount.value} hidden — use Show All to restore`,
      }),
    ));
  }
}

function anyDescendantMatches(nodes, filterQ) {
  if (!filterQ) return true;
  for (const node of nodes) {
    if (node.label.toLowerCase().includes(filterQ)) return true;
    if (node.children && anyDescendantMatches(node.children, filterQ)) return true;
  }
  return false;
}

function toggleCollapse(family, qname) {
  const set = state.collapsedNodes[family] || new Set();
  if (set.has(qname)) set.delete(qname);
  else set.add(qname);
  state.collapsedNodes[family] = set;
  persist();
  renderTreeGrid(state.taxonomyTrees[family], family);
}

function selectRow(node, family) {
  // Clear existing and select this one
  state.selectedRows = [{ concept_qname: node.concept_qname, label: node.label, family }];
  // Re-render tree for highlight + chart panel
  renderTreeGrid(state.taxonomyTrees[family], family);
  renderChartPanel();
}

function toggleSelectRow(node, family) {
  const idx = state.selectedRows.findIndex(
    r => r.concept_qname === node.concept_qname && r.family === family
  );
  if (idx >= 0) {
    state.selectedRows.splice(idx, 1);
  } else {
    state.selectedRows.push({ concept_qname: node.concept_qname, label: node.label, family });
  }
  renderTreeGrid(state.taxonomyTrees[family], family);
  renderChartPanel();
}

export function fmtVal(v, millions) {
  if (v == null) return '—';
  const n = Number(v);
  if (isNaN(n)) return '—';
  if (millions) return `${(n / 1e6).toFixed(1)}M`;
  if (Math.abs(n) >= 1e12) return `${(n / 1e12).toFixed(2)}T`;
  if (Math.abs(n) >= 1e9) return `${(n / 1e9).toFixed(2)}B`;
  if (Math.abs(n) >= 1e6) return `${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}
