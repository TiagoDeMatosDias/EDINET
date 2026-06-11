/**
 * Backtesting — application state.
 *
 * Central mutable state object (ST) and persistence helpers.
 * Imported by index.js and injected into the charts module.
 */

import { log } from '../common/console.js';

// ---------------------------------------------------------------------------
// State object
// ---------------------------------------------------------------------------

export const ST = {
  dbPath: '',
  mode: 'manual',              // 'manual' | 'screener' | 'csv'

  // Manual portfolio
  portfolio: [],               // [{id, ticker, mode, value}]
  startDate: '',
  endDate: '',
  benchmarkTicker: '',
  benchmarkMode: 'ticker',          // 'ticker' | 'portfolio'
  baseCurrency: '',                 // '' = native, 'EUR', 'USD', etc.
  availableCurrencies: [],         // [{code, label}, ...]
  initialCapital: 0,
  riskFreeRate: 0,

  // Screener import (from hash params via sessionStorage)
  screenerConfig: null,

  // CSV import
  csvContent: null,
  csvParsed: null,            // parsed rows for preview

  // Run state
  running: false,
  abortController: null,      // AbortController for cancel
  runPhase: '',               // current run phase label
  durations: ['1yr', '2yr', '3yr', '5yr', '10yr'],

  // UX state
  error: null,                // string error message (inline banner)
  warning: null,              // string warning message (inline banner)
  lastRun: null,              // {mode, params} for retry
  availableTickers: [],       // autocomplete list

  // Results — array for multi-result comparison
  resultsList: [],            // [{id, name, results, mode, params}]
  activeResultTab: 0,         // which result tab is active
  activeResultIdx: 0,         // for set results: which result is selected
  charts: {},                 // {id: Chart} — active Chart.js instances
  companySort: { column: 'total_return', asc: false },  // sortable per-company table

  // Saved backtests (persisted in localStorage)
  savedResults: [],           // [{id, name, mode, results, savedAt}]

  // Rolling screening backtest
  rollingConfig: null,         // {criteria, columns, computedColumns, rankingAlgorithm, rankingRules}
  rollingCadence: 'monthly',
  rollingWeightingModes: ['equal'],
  rollingMaxCompanies: 25,
  rollingBenchmark: '',
  rollingBenchmarkMode: 'ticker',
  rollingBaseCurrency: '',
  rollingStartPeriod: '',
  rollingEndPeriod: '',
  rollingResult: null,         // RollingBacktestResult
  rollingRunning: false,
  rollingAbortController: null,
  rollingProgress: null,       // current progress event
  rollingActive: {
    period: '',                // selected period for drill-down
    weighting: 'equal',        // selected weighting mode for heatmap
    duration: '1yr',           // selected duration for drill-down
  },
  rollingPeriodCount: null,   // estimated period count from API
  rollingEstimatedBacktests: null,

  _nextId: 1,
};

// ---------------------------------------------------------------------------
// Persistence helpers
// ---------------------------------------------------------------------------

export const SAVED_KEY = 'edinet.backtesting.saved';

export function loadSavedResults() {
  try {
    const raw = localStorage.getItem(SAVED_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      ST.savedResults = Array.isArray(parsed)
        ? parsed.filter(e => e && e.id && e.name && e.results)
        : [];
    } else {
      ST.savedResults = [];
    }
  } catch { ST.savedResults = []; }
}

export function persistSavedResults() {
  try {
    localStorage.setItem(SAVED_KEY, JSON.stringify(ST.savedResults));
  } catch (e) {
    log('warn', 'Failed to save backtest results: ' + e.message);
  }
}

// ---------------------------------------------------------------------------
// ID generation
// ---------------------------------------------------------------------------

export function uid() { return String(ST._nextId++); }

// ---------------------------------------------------------------------------
// Exported setters (called by backtesting.js bootstrap to avoid circular imports)
// ---------------------------------------------------------------------------

export function setAvailableTickers(tickers) {
  ST.availableTickers = tickers || [];
}

export function setAvailableCurrencies(currencies) {
  ST.availableCurrencies = currencies || [];
}
