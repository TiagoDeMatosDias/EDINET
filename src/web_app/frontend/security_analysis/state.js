/**
 * Shared state for the Security Analysis page.
 * All modules import from here — single source of truth.
 */

export const state = {
  initDone: false,
  company: null,           // overview API response
  activeTab: null,         // current statement family key
  taxonomyTrees: {},       // { [family]: taxonomy-tree API response }
  selectedRows: [],        // [{ concept_qname, label, family }] for chart overlay
  hiddenMetrics: {},       // { [family]: Set<concept_qname> }
  collapsedNodes: {},      // { [family]: Set<concept_qname> }
  searchFilter: '',
  hideEmpty: false,
  millions: false,
  loadingTree: false,

  // Price tab
  priceData: null,
  priceLoading: false,
  pricePeriod: 'all',
  priceSortField: 'trade_date',
  priceSortDir: 'desc',
  priceTablePage: 60,

  // Search
  searchResults: [],
  searchIdx: -1,
  searchTimer: null,
  eventBound: false,
  recentSearches: [],
};

const STORAGE_KEY = 'sa.state.v2';

export function persist() {
  try {
    const hm = {};
    for (const [key, s] of Object.entries(state.hiddenMetrics)) {
      hm[key] = s instanceof Set ? [...s] : [];
    }
    const cn = {};
    for (const [key, s] of Object.entries(state.collapsedNodes)) {
      cn[key] = s instanceof Set ? [...s] : [];
    }
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify({
      activeTab: state.activeTab,
      millions: state.millions,
      hideEmpty: state.hideEmpty,
      hiddenMetrics: hm,
      collapsedNodes: cn,
      pricePeriod: state.pricePeriod,
      priceSortField: state.priceSortField,
      priceSortDir: state.priceSortDir,
      recentSearches: state.recentSearches,
    }));
  } catch (e) { /* noop */ }
}

export function restore() {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return;
    const p = JSON.parse(raw);
    if (p.activeTab != null) state.activeTab = p.activeTab;
    if (p.millions != null) state.millions = p.millions;
    if (p.hideEmpty != null) state.hideEmpty = p.hideEmpty;
    if (p.pricePeriod != null) state.pricePeriod = p.pricePeriod;
    if (p.priceSortField != null) state.priceSortField = p.priceSortField;
    if (p.priceSortDir != null) state.priceSortDir = p.priceSortDir;
    if (p.recentSearches) state.recentSearches = p.recentSearches.slice(0, 5);

    if (p.hiddenMetrics) {
      const hm = {};
      for (const [key, val] of Object.entries(p.hiddenMetrics)) {
        hm[key] = new Set(Array.isArray(val) ? val : []);
      }
      state.hiddenMetrics = hm;
    }
    if (p.collapsedNodes) {
      const cn = {};
      for (const [key, val] of Object.entries(p.collapsedNodes)) {
        cn[key] = new Set(Array.isArray(val) ? val : []);
      }
      state.collapsedNodes = cn;
    }
  } catch (e) { /* noop */ }
}

/**
 * Add a search entry to recent searches (max 5, unique by code).
 */
export function addRecentSearch(entry) {
  state.recentSearches = [
    entry,
    ...state.recentSearches.filter(
      s => s.company_code !== entry.company_code && s.ticker !== entry.ticker
    ),
  ].slice(0, 5);
  persist();
}
