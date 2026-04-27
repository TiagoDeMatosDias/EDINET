// Shared application state singleton and browser-storage helpers.
// Every module that needs to read or mutate application state imports from here.

export const STORAGE_KEY = 'edinet.web.setups';
export const LAST_SETUP_KEY = 'edinet.web.lastSetup';
export const MAX_LOGS = 500;

export const STATE = {
  view: window.location.pathname.replace(/^\//, '') || 'main',
  apiHealthy: false,
  apiVersion: '',
  stepsMeta: [],
  jobs: [],
  jobsLoading: false,
  stepsLoading: false,
  setupName: 'Untitled setup',
  config: null,
  pipeline: [],
  selectedStepId: null,
  searchQuery: '',
  consoleFilter: 'all',
  consoleAutoscroll: true,
  consoleHidden: false,
  inspectorWidth: 340,
  logs: [],
  running: false,
  runAbort: null,
  localSetups: {},
  loadMenuOpen: false,
  dragIndex: null,
};

/** Cached DOM element references — populated during bootstrap in main.js. */
export const els = {};

/**
 * Cross-module callbacks registered by main.js during bootstrap.
 * Modules that need to trigger navigation or a jobs refresh import this object
 * and call the appropriate function, avoiding circular imports.
 */
// Default view mapping for multi-page navigation.
const _viewPaths = { main: '/', orchestrator: '/orchestrator', screening: '/screening', security: '/security' };

export const callbacks = {
  /** Navigate to a named view. Falls back to URL navigation when not overridden. @param {string} view */
  setView: (view) => { window.location.href = _viewPaths[view] || `/${view}`; },
  /** Re-fetch and render the jobs list. @returns {Promise<void>} */
  refreshJobs: null,
};

export function loadLocalSetups() {
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}');
  } catch {
    return {};
  }
}

export function persistLocalSetups() {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(STATE.localSetups));
}

export function saveLastSetupName(name) {
  localStorage.setItem(LAST_SETUP_KEY, name || '');
}
