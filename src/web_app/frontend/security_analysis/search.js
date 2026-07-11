/**
 * Search bar, dropdown, and company selection.
 */

import { log } from '../common/console.js';
import { fetchJson } from '../common/utils.js';
import { state, persist, addRecentSearch } from './state.js';
import { render } from './security.js';

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

export function init() {
  restoreSearch();
}

function restoreSearch() {
  // Restore recent searches from state
}

export function renderSearch() {
  const inp = H.$('sa-search');
  const status = H.$('sa-status');
  inp.disabled = !state.initDone;

  if (state.loadingTree || (state.company && !state.company.company)) {
    status.textContent = 'Loading…';
    status.className = 'sa-status sa-status-loading';
  } else if (state.company?.company?.company_name) {
    status.textContent = state.company.company.company_name;
    status.className = 'sa-status';
  } else if (state.initDone) {
    status.textContent = 'Ready';
    status.className = 'sa-status sa-status-ok';
  } else {
    status.textContent = 'Initializing…';
    status.className = 'sa-status sa-status-loading';
  }

  renderRecentChips();

  if (state.eventBound) return;
  state.eventBound = true;
  inp.addEventListener('input', onSearchInput);
  inp.addEventListener('keydown', onSearchKeydown);
  inp.addEventListener('focus', onSearchFocus);
  document.addEventListener('click', onClickOutside);
  document.addEventListener('keydown', onGlobalKey);
}

function renderRecentChips() {
  const wrap = H.$('sa-recent-chips');
  if (!wrap) return;
  wrap.innerHTML = '';
  if (!state.recentSearches.length) return;
  for (const entry of state.recentSearches.slice(0, 5)) {
    wrap.appendChild(H.el('button', {
      class: 'sa-recent-chip',
      text: entry.ticker || entry.company_code || entry.company_name,
      title: entry.company_name || entry.ticker,
      onclick() {
        if (entry.company_code) selectCompany(entry.company_code);
        else if (entry.ticker) selectTicker(entry.ticker);
      },
    }));
  }
}

// ── Search logic ──

function onClickOutside(e) {
  const w = document.querySelector('.sa-search-wrap');
  if (w && !w.contains(e.target)) closeDropdown();
}

function onGlobalKey(e) {
  if (e.key === '/' && document.activeElement !== H.$('sa-search') && document.activeElement?.tagName !== 'INPUT') {
    e.preventDefault();
    H.$('sa-search').focus();
  }
  if (e.key === 'Escape' && document.activeElement === H.$('sa-search')) {
    H.$('sa-search').value = '';
    closeDropdown();
  }
}

function onSearchInput() {
  const q = H.$('sa-search').value.trim();
  clearTimeout(state.searchTimer);
  if (!q) { closeDropdown(); return; }
  state.searchTimer = setTimeout(() => doSearch(q), 250);
}

async function doSearch(q) {
  try {
    const d = await fetchJson(`/api/security/search?q=${encodeURIComponent(q)}&limit=25`);
    state.searchResults = d.results || [];
    state.searchIdx = d.results.length > 0 ? 0 : -1;
    renderDropdown(q);
    if (state.searchResults.length) H.$('sa-search-dropdown').classList.add('is-open');
  } catch (e) { log('error', `Search: ${e.message}`); }
}

function onSearchKeydown(e) {
  const dd = H.$('sa-search-dropdown');
  const open = dd.classList.contains('is-open');
  if (!open && e.key === 'Enter') {
    e.preventDefault();
    const q = H.$('sa-search').value.trim();
    if (!q) return;
    clearTimeout(state.searchTimer);
    doSearch(q).then(() => {
      if (state.searchResults.length) {
        closeDropdown();
        const r = state.searchResults[0];
        if (r.company_code) selectCompany(r.company_code);
        else selectTicker(r.ticker);
      }
    });
    return;
  }
  if (!open) return;
  if (e.key === 'ArrowDown') {
    e.preventDefault();
    state.searchIdx = Math.min(state.searchIdx + 1, state.searchResults.length - 1);
    renderDropdown(H.$('sa-search').value.trim());
  } else if (e.key === 'ArrowUp') {
    e.preventDefault();
    state.searchIdx = Math.max(state.searchIdx - 1, 0);
    renderDropdown(H.$('sa-search').value.trim());
  } else if (e.key === 'Enter') {
    e.preventDefault();
    if (state.searchIdx >= 0) {
      const r = state.searchResults[state.searchIdx];
      closeDropdown();
      if (r.company_code) selectCompany(r.company_code);
      else selectTicker(r.ticker);
    }
  } else if (e.key === 'Escape') {
    closeDropdown();
  }
}

function onSearchFocus() {
  if (state.searchResults.length) H.$('sa-search-dropdown').classList.add('is-open');
}

function renderDropdown(query) {
  const dd = H.$('sa-search-dropdown');
  dd.innerHTML = '';
  if (!state.searchResults.length) {
    dd.appendChild(H.el('div', { class: 'sa-search-empty', text: 'No results' }));
    return;
  }
  const qLower = (query || '').toLowerCase();
  state.searchResults.forEach((r, i) => {
    const hasCompany = !!r.company_code;
    dd.appendChild(H.el('div', {
      class: `sa-search-item${i === state.searchIdx ? ' is-active' : ''}${!hasCompany ? ' sa-search-item-ticker-only' : ''}`,
      onmousedown(e) {
        e.preventDefault();
        closeDropdown();
        if (hasCompany) selectCompany(r.company_code);
        else selectTicker(r.ticker);
      },
    }, H.el('span', { class: 'sa-search-item-code', html: highlightMatch(r.ticker || '-', qLower) }),
       H.el('span', { class: 'sa-search-item-name', html: highlightMatch(r.company_name || r.ticker || '', qLower) }),
       r.latest_price != null ? H.el('span', { class: 'sa-search-item-price', text: `¥${Number(r.latest_price).toLocaleString()}` }) : null));
  });
}

function highlightMatch(text, query) {
  if (!query) return escapeHtml(text);
  const idx = text.toLowerCase().indexOf(query);
  if (idx === -1) return escapeHtml(text);
  return escapeHtml(text.slice(0, idx)) +
    `<mark>${escapeHtml(text.slice(idx, idx + query.length))}</mark>` +
    escapeHtml(text.slice(idx + query.length));
}

function escapeHtml(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function closeDropdown() {
  H.$('sa-search-dropdown').classList.remove('is-open');
  state.searchResults = [];
  state.searchIdx = -1;
}

// ── Company selection ──

export async function selectCompany(code) {
  code = String(code).trim();
  if (!code) return;
  state.loadingTree = true;
  state.company = null;
  state.taxonomyTrees = {};
  state.selectedRows = [];
  state.activeTab = null;
  render();
  try {
    const overview = await fetchJson(`/api/security/overview?company_code=${encodeURIComponent(code)}`);
    state.company = overview;
    sessionStorage.setItem('sa.lastCompanyCode', code);
    addRecentSearch({
      company_code: code,
      ticker: overview?.company?.ticker || '',
      company_name: overview?.company?.company_name || code,
    });
    state.loadingTree = false;
    persist();
    render();
    log('info', `Loaded: ${overview.company?.company_name || code}`);
  } catch (e) {
    state.loadingTree = false;
    state.company = null;
    log('error', `Failed: ${e.message}`);
    render();
  }
}

export async function selectTicker(ticker) {
  ticker = String(ticker).trim();
  if (!ticker) return;
  state.loadingTree = true;
  state.company = null;
  state.taxonomyTrees = {};
  state.selectedRows = [];
  state.activeTab = null;
  render();
  try {
    const overview = await fetchJson(`/api/security/overview?ticker=${encodeURIComponent(ticker)}`);
    state.company = overview;
    const code = overview?.company?.company_code;
    if (code) {
      sessionStorage.setItem('sa.lastCompanyCode', code);
      addRecentSearch({
        company_code: code,
        ticker: overview?.company?.ticker || ticker,
        company_name: overview?.company?.company_name || ticker,
      });
    }
    state.loadingTree = false;
    persist();
    render();
    log('info', `Loaded ticker: ${ticker}`);
  } catch (e) {
    state.loadingTree = false;
    state.company = null;
    log('error', `Failed: ${e.message}`);
    render();
  }
}
