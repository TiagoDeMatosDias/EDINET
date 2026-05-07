/**
 * Backtesting — screen logic.
 *
 * Three modes: Manual Portfolio, From Screener, From CSV.
 * Results: charts (Chart.js), metric tiles, tables, heatmap for sets.
 */

import { el, $, fetchJson } from '../common/utils.js';
import { log } from '../common/console.js';

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

const ST = {
  dbPath: '',
  mode: 'manual',              // 'manual' | 'screener' | 'csv'

  // Manual portfolio
  portfolio: [],               // [{id, ticker, mode, value}]
  startDate: '',
  endDate: '',
  benchmarkTicker: '',
  initialCapital: 0,
  riskFreeRate: 0,

  // Screener import (from hash params via sessionStorage)
  screenerConfig: null,

  // CSV import
  csvContent: null,
  csvParsed: null,            // parsed rows for preview

  // Run state
  running: false,
  durations: ['1yr', '2yr', '3yr', '5yr', '10yr'],

  // Results
  results: null,              // BacktestResult | BacktestSetResult
  activeResultIdx: 0,         // for set results: which result is selected
  charts: {},                 // {id: Chart} — active Chart.js instances

  _nextId: 1,
};

function uid() { return String(ST._nextId++); }

// ---------------------------------------------------------------------------
// Hash params — screener deep-link
// ---------------------------------------------------------------------------

export function handleHashParams() {
  const hash = window.location.hash.slice(1);
  if (!hash) return;

  const params = new URLSearchParams(hash);
  const key = params.get('screener-key');
  if (!key) return;

  try {
    const raw = sessionStorage.getItem(key);
    if (!raw) {
      log('warn', 'No screener config found in sessionStorage for key ' + key);
      return;
    }
    sessionStorage.removeItem(key);
    const payload = JSON.parse(raw);

    // New payload format: {csvContent, tickerCount, screeningDate}
    // Old payload format: {criteria, columns, screeningDate, …}
    if (payload.csvContent) {
      // Direct ticker list from screening — stay on screener tab, preload CSV
      ST.csvContent = payload.csvContent;
      parseCSVPreview(payload.csvContent);
      ST.mode = 'screener';
      ST.screenerConfig = { tickerCount: payload.tickerCount, screeningDate: payload.screeningDate };
      if (payload.screeningDate) ST.startDate = payload.screeningDate;
      log('info', `Loaded ${payload.tickerCount || '?'} tickers from screening results.`);
    } else {
      // Legacy screener config — keep screener mode for now
      ST.screenerConfig = payload;
      ST.mode = 'screener';
      log('warn', 'Using legacy screener config (will re-run screening).');
    }
  } catch (err) {
    log('error', 'Failed to parse screener config: ' + err.message);
    ST.mode = 'manual';
  }
}

// ---------------------------------------------------------------------------
// Ticker → Security Analysis deep-link
// ---------------------------------------------------------------------------

function tickerLink(ticker) {
  return el('button', {
    class: 'bt-ticker-link',
    text: ticker,
    title: 'Open ' + ticker + ' in Security Analysis',
    onclick: async () => {
      try {
        const data = await fetchJson(
          '/api/security/search?q=' + encodeURIComponent(ticker) + '&limit=1'
        );
        if (data.results && data.results.length) {
          window.open(
            '/security?edinet_code=' + encodeURIComponent(data.results[0].edinet_code),
            '_blank'
          );
        } else {
          window.open('/security', '_blank');
        }
      } catch {
        window.open('/security', '_blank');
      }
    },
  });
}

// ---------------------------------------------------------------------------
// Price updates before backtest
// ---------------------------------------------------------------------------

async function updatePricesForTickers(tickers, benchmarkTicker) {
  const all = [...tickers];
  if (benchmarkTicker && !all.includes(benchmarkTicker)) all.push(benchmarkTicker);
  if (!all.length) return;

  const statusEl = document.getElementById('bt-update-status');
  const setStatus = (msg) => { if (statusEl) statusEl.textContent = msg; };

  log('info', `Updating prices for ${all.length} ticker(s) before backtest…`);
  let updated = 0, failed = 0;
  for (let i = 0; i < all.length; i++) {
    const t = all[i];
    setStatus(`Updating prices… ${i + 1}/${all.length} (${t})`);
    try {
      // 15-second timeout per ticker so a hung request doesn't block the rest
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 15000);
      await fetchJson('/api/security/update-price', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ticker: t }),
        signal: ctrl.signal,
      });
      clearTimeout(timer);
      updated++;
    } catch (e) {
      failed++;
      const reason = e.name === 'AbortError' ? 'timed out' : e.message;
      log('warn', `Price update failed for ${t}: ${reason}`);
    }
  }
  setStatus('');
  log('info', `Price update complete: ${updated} updated, ${failed} failed out of ${all.length}.`);
}

function getTickersFromCSV() {
  if (!ST.csvContent) return [];
  const lines = ST.csvContent.split('\n');
  const tickers = [];
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#') || trimmed.startsWith('Year,')) continue;
    const cols = trimmed.split(',');
    if (cols.length >= 2) {
      const t = cols[1].trim();
      if (t && !tickers.includes(t)) tickers.push(t);
    }
  }
  return tickers;
}

// ---------------------------------------------------------------------------
// Render entry
// ---------------------------------------------------------------------------

export async function render() {
  const root = document.getElementById('bt-root');
  if (!root) return;

  root.replaceChildren();

  root.append(
    renderModeTabs(),
    renderConfigPanel(),
  );

  // If we have results, show them
  if (ST.results) {
    root.append(renderResults());
    // Smooth-scroll to results after DOM is painted
    requestAnimationFrame(() => {
      const el = document.querySelector('.bt-results');
      if (el && window.scrollY < el.offsetTop - 60) {
        el.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    });
  }
}

// ---------------------------------------------------------------------------
// Mode tabs
// ---------------------------------------------------------------------------

function renderModeTabs() {
  const modes = [
    { key: 'manual', label: 'Manual Portfolio' },
    { key: 'screener', label: 'From Screener' },
    { key: 'csv', label: 'From CSV' },
  ];

  return el('div', { class: 'bt-mode-tabs' },
    ...modes.map(m =>
      el('button', {
        class: 'bt-mode-tab' + (ST.mode === m.key ? ' is-active' : ''),
        text: m.label,
        onclick: () => {
          if (ST.running) return;
          ST.mode = m.key;
          ST.results = null;
          destroyAllCharts();
          render();
        },
      })
    ),
  );
}

// ---------------------------------------------------------------------------
// Configuration panel (mode-specific)
// ---------------------------------------------------------------------------

function renderConfigPanel() {
  const container = el('div', { class: 'bt-config' });

  if (ST.mode === 'manual') {
    container.append(...renderManualConfig());
  } else if (ST.mode === 'screener') {
    container.append(...renderScreenerConfig());
  } else if (ST.mode === 'csv') {
    container.append(...renderCSVConfig());
  }

  return container;
}

// ── Manual Portfolio ─────────────────────────────────────────────────

function renderManualConfig() {
  const children = [];

  // Date range row
  children.push(
    el('div', { class: 'bt-config-row' },
      el('label', { class: 'bt-config-label', text: 'Date Range' }),
      el('div', { class: 'bt-config-fields' },
        el('input', {
          type: 'date', class: 'bt-input',
          value: ST.startDate,
          onchange: (e) => { ST.startDate = e.target.value; },
        }),
        el('span', { text: ' → ', class: 'bt-config-sep' }),
        el('input', {
          type: 'date', class: 'bt-input',
          value: ST.endDate,
          onchange: (e) => { ST.endDate = e.target.value; },
        }),
      ),
    ),
  );

  // Portfolio builder
  children.push(
    el('div', { class: 'bt-config-row' },
      el('label', { class: 'bt-config-label', text: 'Portfolio' }),
      el('div', { class: 'bt-portfolio-list', id: 'bt-portfolio-list' },
        ...renderPortfolioRows(),
        el('button', {
          class: 'bt-portfolio-add btn-ghost',
          text: '+ Add Ticker',
          onclick: () => {
            ST.portfolio.push({ id: uid(), ticker: '', mode: 'weight', value: 0 });
            refreshPortfolioList();
          },
        }),
      ),
    ),
  );

  // Benchmark + params
  children.push(
    el('div', { class: 'bt-config-row' },
      el('label', { class: 'bt-config-label', text: 'Benchmark' }),
      el('div', { class: 'bt-config-fields' },
        el('input', {
          type: 'text', class: 'bt-input', style: 'width:160px',
          placeholder: 'e.g. 1321.T',
          value: ST.benchmarkTicker,
          oninput: (e) => { ST.benchmarkTicker = e.target.value; },
        }),
      ),
    ),
  );

  children.push(
    el('div', { class: 'bt-config-row' },
      el('label', { class: 'bt-config-label', text: 'Initial Capital' }),
      el('div', { class: 'bt-config-fields' },
        el('input', {
          type: 'number', class: 'bt-input', style: 'width:160px',
          placeholder: '0 = derive',
          value: ST.initialCapital || '',
          oninput: (e) => { ST.initialCapital = parseFloat(e.target.value) || 0; },
        }),
      ),
    ),
  );

  children.push(
    el('div', { class: 'bt-config-row' },
      el('label', { class: 'bt-config-label', text: 'Risk-Free Rate' }),
      el('div', { class: 'bt-config-fields' },
        el('input', {
          type: 'number', class: 'bt-input', style: 'width:120px',
          step: '0.001', placeholder: '0.02',
          value: ST.riskFreeRate || '',
          oninput: (e) => { ST.riskFreeRate = parseFloat(e.target.value) || 0; },
        }),
      ),
    ),
  );

  // Run button
  children.push(
    el('div', { class: 'bt-config-actions' },
      el('button', {
        class: 'bt-run-btn',
        text: ST.running ? 'Running…' : 'Run Backtest',
        disabled: ST.running,
        onclick: () => runManualBacktest(),
      }),
    ),
  );

  // Spinner overlay when running
  if (ST.running) {
    children.push(
      el('div', { class: 'bt-spinner' },
        el('div', { class: 'bt-spinner-text', text: 'Updating prices…' }),
        el('div', { id: 'bt-update-status', class: 'bt-update-status' }),
      ),
    );
  }

  return children;
}

function renderPortfolioRows() {
  return ST.portfolio.map((p, idx) => {
    const removeBtn = el('button', {
      class: 'bt-portfolio-remove',
      text: '✕',
      title: 'Remove ticker',
      onclick: () => {
        ST.portfolio.splice(idx, 1);
        refreshPortfolioList();
      },
    });

    const tickerInput = el('input', {
      type: 'text', class: 'bt-input', style: 'width:110px',
      placeholder: 'e.g. 7203',
      value: p.ticker,
      oninput: (e) => { p.ticker = e.target.value; },
    });

    const modeSelect = el('select', {
      class: 'bt-select',
      onchange: (e) => {
        p.mode = e.target.value;
        // Update the value placeholder to match the new mode
        const row = e.target.closest('.bt-portfolio-row');
        if (row) {
          const valInput = row.querySelector('input[type="number"]');
          if (valInput) {
            valInput.placeholder = p.mode === 'weight' ? 'e.g. 0.5' : p.mode === 'shares' ? 'e.g. 100' : 'e.g. 500000';
          }
        }
      },
    },
      el('option', { value: 'weight', selected: p.mode === 'weight', text: 'Weight' }),
      el('option', { value: 'shares', selected: p.mode === 'shares', text: 'Shares' }),
      el('option', { value: 'value', selected: p.mode === 'value', text: 'Value' }),
    );

    const valueInput = el('input', {
      type: 'number', class: 'bt-input', style: 'width:110px',
      step: 'any',
      placeholder: p.mode === 'weight' ? 'e.g. 0.5' : p.mode === 'shares' ? 'e.g. 100' : 'e.g. 500000',
      value: p.value || '',
      oninput: (e) => { p.value = parseFloat(e.target.value) || 0; },
    });

    return el('div', { class: 'bt-portfolio-row' },
      removeBtn,
      tickerInput,
      modeSelect,
      valueInput,
    );
  });
}

function refreshPortfolioList() {
  const list = document.getElementById('bt-portfolio-list');
  if (!list) return;
  list.replaceChildren(
    ...renderPortfolioRows(),
    el('button', {
      class: 'bt-portfolio-add btn-ghost',
      text: '+ Add Ticker',
      onclick: () => {
        ST.portfolio.push({ id: uid(), ticker: '', mode: 'weight', value: 0 });
        refreshPortfolioList();
      },
    }),
  );
}

async function runManualBacktest() {
  // Validation
  if (ST.portfolio.length === 0) {
    alert('Add at least one ticker to the portfolio.');
    return;
  }
  if (!ST.startDate || !ST.endDate) {
    alert('Select start and end dates.');
    return;
  }
  if (ST.startDate >= ST.endDate) {
    alert('Start date must be before end date.');
    return;
  }
  const weightSum = ST.portfolio
    .filter(p => p.mode === 'weight')
    .reduce((s, p) => s + (p.value || 0), 0);
  if (weightSum > 0 && Math.abs(weightSum - 1.0) > 0.01) {
    log('warn', 'Weight-mode allocations sum to ' + weightSum.toFixed(4) + ' — will be normalized.');
  }

  ST.running = true;
  render();

  // Update prices for all tickers + benchmark before backtest
  const allTickers = ST.portfolio.map(p => p.ticker);
  await updatePricesForTickers(allTickers, ST.benchmarkTicker);

  try {
    const portfolio = {};
    for (const p of ST.portfolio) {
      portfolio[p.ticker] = { mode: p.mode, value: p.value };
    }

    const result = await fetchJson('/api/backtesting/run', {
      method: 'POST',
      body: JSON.stringify({
        portfolio,
        start_date: ST.startDate,
        end_date: ST.endDate,
        benchmark_ticker: ST.benchmarkTicker,
        initial_capital: ST.initialCapital,
        risk_free_rate: ST.riskFreeRate,
      }),
    });

    ST.results = result;
    ST.activeResultIdx = 0;
    log('info', 'Backtest complete. Total return: ' +
      (result.metrics.total_return * 100).toFixed(2) + '%');
  } catch (err) {
    log('error', 'Backtest failed: ' + err.message);
    alert('Backtest failed: ' + err.message);
  } finally {
    ST.running = false;
    render();
  }
}

// ── From Screener ────────────────────────────────────────────────────

function renderScreenerConfig() {
  const children = [];
  const cfg = ST.screenerConfig;

  if (!cfg) {
    children.push(
      el('div', { class: 'bt-warning', text: 'No screener configuration loaded. Switch to manual mode or go to the Screening page to select criteria, then click "Backtest →".' }),
    );
    return children;
  }

  // Summary row — shows ticker count + screening date from CSV content or legacy config
  const tickerCount = ST.csvParsed?.totalLines || cfg.tickerCount || cfg.maxCompanies || '?';
  const screeningDate = cfg.screeningDate || (ST.startDate || '?');
  children.push(
    el('div', { class: 'bt-config-row' },
      el('label', { class: 'bt-config-label', text: 'Companies' }),
      el('div', { class: 'bt-config-fields' },
        el('span', { class: 'bt-config-summary', text: `${tickerCount} tickers as of ${screeningDate}` }),
      ),
    ),
  );

  // Show CSV preview if available
  if (ST.csvParsed && ST.csvParsed.rows && ST.csvParsed.rows.length) {
    children.push(renderCSVPreview());
  }

  // Duration checkboxes
  children.push(
    el('div', { class: 'bt-config-row' },
      el('label', { class: 'bt-config-label', text: 'Durations' }),
      el('div', { class: 'bt-duration-group' },
        ...['1yr', '2yr', '3yr', '5yr', '10yr'].map(d =>
          el('label', { class: 'bt-duration-chip' },
            el('input', {
              type: 'checkbox',
              checked: ST.durations.includes(d),
              onchange: (e) => {
                if (e.target.checked) {
                  if (!ST.durations.includes(d)) ST.durations.push(d);
                } else {
                  ST.durations = ST.durations.filter(x => x !== d);
                }
                render();
              },
            }),
            el('span', { text: d }),
          ),
        ),
      ),
    ),
  );

  children.push(
    el('div', { class: 'bt-config-row' },
      el('label', { class: 'bt-config-label', text: 'Benchmark' }),
      el('div', { class: 'bt-config-fields' },
        el('input', {
          type: 'text', class: 'bt-input', style: 'width:160px',
          placeholder: 'e.g. 1321.T',
          value: ST.benchmarkTicker,
          oninput: (e) => { ST.benchmarkTicker = e.target.value; },
        }),
      ),
    ),
  );

  children.push(
    el('div', { class: 'bt-config-row' },
      el('label', { class: 'bt-config-label', text: 'Initial Capital' }),
      el('div', { class: 'bt-config-fields' },
        el('input', {
          type: 'number', class: 'bt-input', style: 'width:160px',
          placeholder: '0 = derive',
          value: ST.initialCapital || '',
          oninput: (e) => { ST.initialCapital = parseFloat(e.target.value) || 0; },
        }),
      ),
    ),
  );

  children.push(
    el('div', { class: 'bt-config-actions' },
      el('button', {
        class: 'bt-run-btn',
        text: ST.running ? 'Running…' : 'Run Backtest Set',
        disabled: ST.running,
        onclick: () => runScreenerBacktest(),
      }),
      el('button', {
        class: 'bt-export-btn',
        text: 'Export CSV',
        title: 'Export screening results as a backtest CSV file',
        disabled: ST.running,
        onclick: () => exportScreenerConfigCSV(),
      }),
    ),
  );

  if (ST.running) {
    children.push(
      el('div', { class: 'bt-spinner' },
        el('div', { class: 'bt-spinner-text', text: 'Running screener backtest set…' }),
        el('div', { id: 'bt-update-status', class: 'bt-update-status' }),
      ),
    );
  }

  return children;
}

async function runScreenerBacktest() {
  // Old-style screener config: run screening once to get tickers,
  // then build a CSV and use the CSV backtest path.
  // New-style flow (from screening page) already arrives as CSV content.
  const cfg = ST.screenerConfig;
  if (!cfg) return;

  ST.running = true;
  render();

  try {
    // If we already have CSV content, use it directly
    if (ST.csvContent) {
      // Update prices for all tickers from the CSV + benchmark
      const csvTickers = getTickersFromCSV();
      await updatePricesForTickers(csvTickers, ST.benchmarkTicker);
    } else {
      // Legacy: run screening to get ticker list, then build CSV
      log('info', 'Running screening to get ticker list…');
      const screenData = await fetchJson('/api/screening/run', {
        method: 'POST',
        body: JSON.stringify({
          criteria: cfg.criteria,
          columns: [...(cfg.columns || []), 'CompanyInfo.Company_Ticker'],
          screening_date: cfg.screeningDate,
          ranking_algorithm: cfg.rankingAlgorithm || 'none',
          ranking_rules: cfg.rankingRules || null,
          computed_columns: cfg.computedColumns || null,
        }),
      });

      // Find ticker column
      const cols = screenData.columns || [];
      let tickerIdx = -1;
      for (let i = 0; i < cols.length; i++) {
        if (/company_ticker|^ticker$/i.test(cols[i])) { tickerIdx = i; break; }
      }
      if (tickerIdx === -1) throw new Error('No ticker column in screening results.');

      const rows = screenData.rows || [];
      const tickers = [];
      for (const row of rows) {
        const t = String(row[tickerIdx] || '').trim();
        if (t && !tickers.includes(t)) tickers.push(t);
      }

      if (cfg.maxCompanies && tickers.length > cfg.maxCompanies) {
        tickers.length = cfg.maxCompanies;
      }

      const weight = tickers.length > 0 ? (1.0 / tickers.length) : 0;
      const year = cfg.screeningDate
        ? cfg.screeningDate.substring(0, 4)
        : new Date().getFullYear().toString();

      const lines = ['Year,Tickers,Type,Amount'];
      for (const t of tickers) {
        lines.push(`${year},${t},weight,${weight.toFixed(6)}`);
      }
      ST.csvContent = lines.join('\n');

      // Update prices for all tickers + benchmark
      await updatePricesForTickers(tickers, ST.benchmarkTicker);
    }

    // Run via CSV endpoint
    const result = await fetchJson('/api/backtesting/run-from-csv', {
      method: 'POST',
      body: JSON.stringify({
        csv_content: ST.csvContent,
        benchmark_ticker: ST.benchmarkTicker,
        durations: ST.durations,
        initial_capital: ST.initialCapital,
        risk_free_rate: ST.riskFreeRate,
      }),
    });

    ST.results = result;
    ST.activeResultIdx = 0;
    log('info', `Screener backtest set complete. ${result.aggregate.successful}/${result.aggregate.total_runs} successful.`);
  } catch (err) {
    log('error', 'Screener backtest failed: ' + err.message);
    alert('Screener backtest failed: ' + err.message);
  } finally {
    ST.running = false;
    render();
  }
}

// ── From CSV ─────────────────────────────────────────────────────────

function renderCSVConfig() {
  const children = [];

  // Drop zone
  children.push(
    el('div', { class: 'bt-csv-drop', id: 'bt-csv-drop' },
      el('div', { class: 'bt-csv-drop-icon', text: '📁' }),
      el('div', { class: 'bt-csv-drop-label', text: 'Drop a backtest-set CSV here, or click to browse' }),
      el('div', { class: 'bt-csv-drop-hint', text: 'Format: Year, Tickers, Type, Amount. Optional # Benchmark: and # Discount Rate: headers.' }),
    ),
  );

  // Hidden file input
  children.push(
    el('input', {
      type: 'file', id: 'bt-csv-input',
      accept: '.csv', style: 'display:none',
      onchange: (e) => handleCSVFile(e.target.files[0]),
    }),
  );

  // CSV preview
  if (ST.csvParsed && ST.csvParsed.length) {
    children.push(renderCSVPreview());
  }

  // Duration checkboxes
  children.push(
    el('div', { class: 'bt-config-row' },
      el('label', { class: 'bt-config-label', text: 'Durations' }),
      el('div', { class: 'bt-duration-group' },
        ...['1yr', '2yr', '3yr', '5yr', '10yr'].map(d =>
          el('label', { class: 'bt-duration-chip' },
            el('input', {
              type: 'checkbox',
              checked: ST.durations.includes(d),
              onchange: (e) => {
                if (e.target.checked) ST.durations.push(d);
                else ST.durations = ST.durations.filter(x => x !== d);
                render();
              },
            }),
            el('span', { text: d }),
          ),
        ),
      ),
    ),
  );

  // Benchmark + params
  children.push(
    el('div', { class: 'bt-config-row' },
      el('label', { class: 'bt-config-label', text: 'Benchmark' }),
      el('div', { class: 'bt-config-fields' },
        el('input', {
          type: 'text', class: 'bt-input', style: 'width:160px',
          placeholder: 'e.g. 1321.T',
          value: ST.benchmarkTicker,
          oninput: (e) => { ST.benchmarkTicker = e.target.value; },
        }),
      ),
    ),
  );

  children.push(
    el('div', { class: 'bt-config-row' },
      el('label', { class: 'bt-config-label', text: 'Initial Capital' }),
      el('div', { class: 'bt-config-fields' },
        el('input', {
          type: 'number', class: 'bt-input', style: 'width:160px',
          placeholder: '0 = derive',
          value: ST.initialCapital || '',
          oninput: (e) => { ST.initialCapital = parseFloat(e.target.value) || 0; },
        }),
      ),
    ),
  );

  // Run button
  if (ST.csvContent) {
    children.push(
      el('div', { class: 'bt-config-actions' },
        el('button', {
          class: 'bt-run-btn',
          text: ST.running ? 'Running…' : 'Run Backtest Set',
          disabled: ST.running,
          onclick: () => runCSVBacktest(),
        }),
      ),
    );
  }

  if (ST.running) {
    children.push(
      el('div', { class: 'bt-spinner' },
        el('div', { class: 'bt-spinner-text', text: 'Running CSV backtest set…' }),
        el('div', { id: 'bt-update-status', class: 'bt-update-status' }),
      ),
    );
  }

  // Wire drop zone after render
  setTimeout(() => wireCSVDropZone(), 0);

  return children;
}

function wireCSVDropZone() {
  const drop = document.getElementById('bt-csv-drop');
  const input = document.getElementById('bt-csv-input');
  if (!drop || !input) return;

  drop.onclick = () => input.click();

  drop.ondragover = (e) => {
    e.preventDefault();
    drop.classList.add('bt-csv-drop-active');
  };
  drop.ondragleave = () => drop.classList.remove('bt-csv-drop-active');
  drop.ondrop = (e) => {
    e.preventDefault();
    drop.classList.remove('bt-csv-drop-active');
    const file = e.dataTransfer.files[0];
    if (file) handleCSVFile(file);
  };
}

function handleCSVFile(file) {
  if (!file) return;
  const reader = new FileReader();
  reader.onload = () => {
    ST.csvContent = reader.result;
    parseCSVPreview(reader.result);
    render();
  };
  reader.onerror = () => {
    log('error', 'Failed to read CSV file.');
  };
  reader.readAsText(file);
}

function parseCSVPreview(content) {
  const lines = content.split('\n').filter(l => l.trim() && !l.trim().startsWith('#'));
  if (lines.length < 2) {
    ST.csvParsed = [];
    return;
  }
  const header = lines[0].split(',').map(h => h.trim());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(',');
    if (cols.length >= 4) {
      rows.push({
        Year: cols[0].trim(),
        Tickers: cols[1].trim(),
        Type: cols[2].trim(),
        Amount: cols[3].trim(),
      });
    }
  }
  ST.csvParsed = { header, rows, totalLines: lines.length - 1 };
}

function renderCSVPreview() {
  const data = ST.csvParsed;
  if (!data || !data.rows.length) return el('div', { class: 'bt-warning', text: 'Could not parse CSV. Check format: Year,Tickers,Type,Amount' });

  return el('div', { class: 'bt-csv-preview' },
    el('div', { class: 'bt-csv-preview-title', text: `${data.totalLines} companies` }),
    el('div', { class: 'bt-csv-scroll' },
      el('table', { class: 'data-table' },
        el('thead', {},
          el('tr', {},
            ...data.header.map(h => el('th', { text: h })),
          ),
        ),
        el('tbody', {},
          ...data.rows.map(r =>
            el('tr', {},
              el('td', { text: r.Year }),
              el('td', {}, tickerLink(r.Tickers)),
              el('td', { text: r.Type }),
              el('td', { text: r.Amount }),
            ),
          ),
        ),
      ),
    ),
  );
}

async function runCSVBacktest() {
  if (!ST.csvContent) {
    alert('Please upload a CSV file first.');
    return;
  }

  ST.running = true;
  render();

  // Update prices for all tickers from the CSV + benchmark
  const csvTickers = getTickersFromCSV();
  await updatePricesForTickers(csvTickers, ST.benchmarkTicker);

  try {
    const result = await fetchJson('/api/backtesting/run-from-csv', {
      method: 'POST',
      body: JSON.stringify({
        csv_content: ST.csvContent,
        benchmark_ticker: ST.benchmarkTicker,
        durations: ST.durations,
        initial_capital: ST.initialCapital,
        risk_free_rate: ST.riskFreeRate,
      }),
    });

    ST.results = result;
    ST.activeResultIdx = 0;
    log('info', `CSV backtest set complete. ${result.aggregate.successful}/${result.aggregate.total_runs} successful.`);
  } catch (err) {
    log('error', 'CSV backtest failed: ' + err.message);
    alert('CSV backtest failed: ' + err.message);
  } finally {
    ST.running = false;
    render();
  }
}

// ---------------------------------------------------------------------------
// Export screener config as backtest CSV
// ---------------------------------------------------------------------------

async function exportScreenerConfigCSV() {
  // If we already have CSV content (new-style flow from screening page),
  // export it directly. Otherwise run screening once to get tickers.
  if (ST.csvContent) {
    const lines = [];
    if (ST.benchmarkTicker) lines.push('# Benchmark: ' + ST.benchmarkTicker);
    if (ST.riskFreeRate) lines.push('# Discount Rate: ' + ST.riskFreeRate);
    // Prepend header comments to existing CSV (skip existing Year header)
    const csvLines = ST.csvContent.split('\n').map(l => l.trim()).filter(l => l);
    const dataLines = csvLines.filter(l => !l.startsWith('#') && l !== 'Year,Tickers,Type,Amount');
    lines.push('Year,Tickers,Type,Amount');
    lines.push(...dataLines);
    const csv = lines.join('\n');
    _downloadCSV(csv, 'backtest_setup.csv');
    log('info', `Exported backtest CSV.`);
    return;
  }

  // Legacy: run screening
  const cfg = ST.screenerConfig;
  if (!cfg) return;

  try {
    log('info', 'Running screening to generate backtest CSV…');
    const data = await fetchJson('/api/screening/run', {
      method: 'POST',
      body: JSON.stringify({
        criteria: cfg.criteria,
        columns: [...(cfg.columns || []), 'CompanyInfo.Company_Ticker'],
        screening_date: cfg.screeningDate,
        ranking_algorithm: cfg.rankingAlgorithm || 'none',
        ranking_rules: cfg.rankingRules || null,
        computed_columns: cfg.computedColumns || null,
      }),
    });

    const cols = data.columns || [];
    let tickerIdx = -1;
    for (let i = 0; i < cols.length; i++) {
      if (/company_ticker|^ticker$/i.test(cols[i])) { tickerIdx = i; break; }
    }
    if (tickerIdx === -1) {
      alert('Screening results have no ticker column.');
      return;
    }

    const rows = data.rows || [];
    const weight = rows.length > 0 ? (1.0 / rows.length) : 0;
    const year = cfg.screeningDate ? cfg.screeningDate.substring(0, 4) : new Date().getFullYear().toString();

    const lines = [];
    if (ST.benchmarkTicker) lines.push('# Benchmark: ' + ST.benchmarkTicker);
    if (ST.riskFreeRate) lines.push('# Discount Rate: ' + ST.riskFreeRate);
    lines.push('Year,Tickers,Type,Amount');

    for (const row of rows) {
      const t = String(row[tickerIdx] || '').trim();
      if (t) lines.push(`${year},${t},weight,${weight.toFixed(6)}`);
    }

    _downloadCSV(lines.join('\n'), 'backtest_setup.csv');
    log('info', `Exported ${rows.length} tickers as backtest CSV.`);
  } catch (e) {
    log('error', 'Export failed: ' + e.message);
    alert('Export failed: ' + e.message);
  }
}

function _downloadCSV(content, filename) {
  const blob = new Blob([content], { type: 'text/csv' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
  URL.revokeObjectURL(a.href);
}

// ---------------------------------------------------------------------------
// Results rendering
// ---------------------------------------------------------------------------

function renderResults() {
  if (!ST.results) return null;

  const container = el('div', { class: 'bt-results' });

  // Check if it's a single result (has metrics directly) or a set (has aggregate + results)
  if (ST.results.aggregate) {
    container.append(...renderSetResults());
  } else {
    container.append(...renderSingleResults(ST.results));
  }

  return container;
}

// ── Single backtest results ──────────────────────────────────────────

function renderSingleResults(result) {
  const children = [];
  const m = result.metrics || {};
  const hasBench = m.benchmark_total_return != null;

  // Warnings banner
  if (result.warnings && result.warnings.length) {
    children.push(
      el('div', { class: 'bt-warning' },
        ...result.warnings.map(w => el('div', { text: '⚠ ' + w })),
      ),
    );
  }

  // Summary metric tiles
  children.push(renderMetricTiles(m, hasBench));

  // Charts — cumulative returns
  if (result.chart_data && result.chart_data.cumulative && result.chart_data.cumulative.length) {
    children.push(
      el('div', { class: 'bt-chart-container' },
        el('div', { class: 'bt-chart-title', text: 'Cumulative Returns' }),
        el('canvas', { id: 'bt-chart-cumulative' }),
      ),
    );

    // Drawdown
    children.push(
      el('div', { class: 'bt-chart-container' },
        el('div', { class: 'bt-chart-title', text: 'Drawdown' }),
        el('canvas', { id: 'bt-chart-drawdown' }),
      ),
    );

    // Decomposition + Per-Company scatter
    children.push(
      el('div', { class: 'bt-chart-row' },
        el('div', { class: 'bt-chart-container', style: 'flex:1' },
          el('div', { class: 'bt-chart-title', text: 'Return Decomposition' }),
          el('canvas', { id: 'bt-chart-decomposition' }),
        ),
        el('div', { class: 'bt-chart-container', style: 'flex:1' },
          el('div', { class: 'bt-chart-title', text: 'Per-Company: Price vs Dividend Return' }),
          el('canvas', { id: 'bt-chart-percompany' }),
        ),
      ),
    );
  }

  // Yearly returns scatter + table if we have yearly data
  if (result.yearly_returns && result.yearly_returns.length > 1) {
    children.push(
      el('div', { class: 'bt-chart-container' },
        el('div', { class: 'bt-chart-title', text: 'Annual Returns: Price vs Dividend' }),
        el('canvas', { id: 'bt-chart-yearly-scatter' }),
      ),
    );
  }

  // Per-company table
  if (result.per_company && result.per_company.length) {
    children.push(renderPerCompanyTable(result.per_company, m.start_date, m.end_date));
  }

  // Yearly returns table
  if (result.yearly_returns && result.yearly_returns.length) {
    children.push(renderYearlyTable(result.yearly_returns));
  }

  // Dividends by year
  if (result.dividends_by_year && result.dividends_by_year.length) {
    children.push(renderDividendTable(result.dividends_by_year));
  }

  // Defer chart creation
  setTimeout(() => {
    if (result.chart_data) {
      createCumulativeChart('bt-chart-cumulative', result.chart_data, hasBench);
      createDrawdownChart('bt-chart-drawdown', result.chart_data, hasBench);
      createDecompositionChart('bt-chart-decomposition', result.chart_data);
      createPerCompanyChart('bt-chart-percompany', result.per_company || [], m.start_date, m.end_date);
    }
    if (result.yearly_returns && result.yearly_returns.length > 1) {
      createYearlyScatterChart('bt-chart-yearly-scatter', result.yearly_returns);
    }
  }, 0);

  return children;
}

// ── Backtest set results ─────────────────────────────────────────────

function renderSetResults() {
  const children = [];
  const agg = ST.results.aggregate;

  // Aggregate summary tiles
  if (agg.stats) {
    children.push(
      el('div', { class: 'bt-metric-row' },
        el('div', { class: 'bt-metric-tile' },
          el('div', { class: 'bt-metric-label', text: 'Mean Total Return' }),
          el('div', { class: 'bt-metric-value', text: fmtPct(agg.stats.total_return.mean) }),
        ),
        el('div', { class: 'bt-metric-tile' },
          el('div', { class: 'bt-metric-label', text: 'Mean Sharpe' }),
          el('div', { class: 'bt-metric-value', text: agg.stats.sharpe_ratio.mean.toFixed(2) }),
        ),
        agg.benchmark_comparison
          ? el('div', { class: 'bt-metric-tile' },
              el('div', { class: 'bt-metric-label', text: 'Outperformed Benchmark' }),
              el('div', { class: 'bt-metric-value', text: `${agg.benchmark_comparison.outperformed}/${agg.benchmark_comparison.outperformed + agg.benchmark_comparison.underperformed}` }),
            )
          : null,
        el('div', { class: 'bt-metric-tile' },
          el('div', { class: 'bt-metric-label', text: 'Successful / Total' }),
          el('div', { class: 'bt-metric-value', text: `${agg.successful} / ${agg.total_runs}` }),
        ),
      ),
    );

    // Aggregate stats table
    children.push(renderAggregateStatsTable(agg.stats));
  }

  // Duration selector tabs
  const durations = [...new Set(ST.results.results.map(r => r.duration))].sort();
  children.push(
    el('div', { class: 'bt-duration-selector' },
      ...durations.map(d =>
        el('button', {
          class: 'bt-duration-tab' + (d === getActiveFilter() ? ' is-active' : ''),
          text: d,
          onclick: () => {
            ST.activeResultIdx = 0;
            ST._durationFilter = d;
            destroyAllCharts();
            render();
          },
        }),
      ),
    ),
  );

  // Filter results by active duration
  const filter = getActiveFilter();
  const filtered = filter
    ? ST.results.results.filter(r => r.duration === filter)
    : ST.results.results;

  // Heatmap grid
  if (filtered.length > 1) {
    children.push(renderHeatmap(filtered, filter));
  }

  // Selected result detail
  const selIdx = Math.min(ST.activeResultIdx, filtered.length - 1);
  if (filtered.length > 0 && filtered[selIdx]) {
    const sel = filtered[selIdx];
    const tickerLinks = (sel.tickers && sel.tickers.length)
      ? el('div', { class: 'bt-detail-tickers' },
          ...sel.tickers.map(t => tickerLink(t)),
        )
      : null;
    children.push(
      el('div', { class: 'bt-detail-panel' },
        el('div', { class: 'bt-detail-title', text: `${sel.year} — ${sel.duration} (${sel.start_date} → ${sel.end_date})` }),
        tickerLinks,
        ...(sel.metrics ? renderSingleResults({ metrics: sel.metrics, chart_data: sel.chart_data, per_company: sel.per_company, yearly_returns: sel.yearly_returns, dividends_by_year: sel.dividends_by_year, warnings: sel.warnings }) : []),
      ),
    );
  }

  return children;
}

function getActiveFilter() {
  return ST._durationFilter || null;
}

function renderAggregateStatsTable(stats) {
  if (!stats) return null;

  const rows = [
    { label: 'Total Return', key: 'total_return', fmt: fmtPct },
    { label: 'Annualized Return', key: 'annualized_return', fmt: fmtPct },
    { label: 'Sharpe Ratio', key: 'sharpe_ratio', fmt: v => v.toFixed(4) },
    { label: 'Max Drawdown', key: 'max_drawdown', fmt: fmtPct },
  ];

  return el('table', { class: 'bt-agg-stats data-table' },
    el('thead', {},
      el('tr', {},
        el('th', { text: 'Metric' }),
        el('th', { text: 'Mean' }),
        el('th', { text: 'Median' }),
        el('th', { text: 'Min' }),
        el('th', { text: 'Max' }),
      ),
    ),
    el('tbody', {},
      ...rows.map(r =>
        el('tr', {},
          el('td', { text: r.label }),
          el('td', { text: r.fmt(stats[r.key].mean) }),
          el('td', { text: r.fmt(stats[r.key].median) }),
          el('td', { text: r.fmt(stats[r.key].min) }),
          el('td', { text: r.fmt(stats[r.key].max) }),
        ),
      ),
    ),
  );
}

function renderHeatmap(results, activeFilter) {
  const years = [...new Set(results.map(r => r.year))].sort();
  const durations = [...new Set(results.map(r => r.duration))].sort();

  // Build lookup: year_duration → result index (in filtered array)
  const lookup = {};
  results.forEach((r, i) => {
    lookup[r.year + '_' + r.duration] = i;
  });

  // Determine color range
  const allReturns = results
    .filter(r => r.metrics)
    .map(r => r.metrics.total_return);
  const minR = Math.min(...allReturns, 0);
  const maxR = Math.max(...allReturns, 0);
  const range = maxR - minR || 1;

  return el('div', { class: 'bt-heatmap' },
    el('table', {},
      el('thead', {},
        el('tr', {},
          el('th', { text: 'Year \\ Dur' }),
          ...durations.map(d => el('th', { text: d })),
        ),
      ),
      el('tbody', {},
        ...years.map(y =>
          el('tr', {},
            el('td', { class: 'bt-heatmap-year', text: y }),
            ...durations.map(d => {
              const key = y + '_' + d;
              const idx = lookup[key];
              if (idx === undefined) return el('td', { class: 'bt-heatmap-cell bt-heatmap-na', text: '-' });
              const r = results[idx];
              const ret = r.metrics ? r.metrics.total_return : null;

              const t = ret != null ? Math.max(0, Math.min(1, (ret - minR) / range)) : 0;
              const r_c = Math.round(220 + (35 * (1 - t)));
              const g_c = Math.round(50 + (205 * t));
              const b_c = Math.round(50 + (25 * (1 - t)));
              const bg = `rgb(${r_c},${g_c},${b_c})`;

              const isSelected = idx === ST.activeResultIdx;
              return el('td', {
                class: 'bt-heatmap-cell' + (isSelected ? ' is-active' : ''),
                style: `background:${bg}`,
                text: ret != null ? (ret * 100).toFixed(1) + '%' : '-',
                onclick: () => {
                  ST.activeResultIdx = idx;
                  destroyAllCharts();
                  render();
                },
              });
            }),
          ),
        ),
      ),
    ),
  );
}

// ── Metric tiles ─────────────────────────────────────────────────────

function renderMetricTiles(m, hasBench) {
  const tiles = [
    { label: 'Total Return', value: fmtPct(m.total_return), color: m.total_return >= 0 ? 'var(--success)' : 'var(--danger)' },
    { label: 'Annualized Return', value: fmtPct(m.annualized_return), color: m.annualized_return >= 0 ? 'var(--success)' : 'var(--danger)' },
    { label: 'Volatility', value: fmtPct(m.volatility) },
    { label: 'Sharpe Ratio', value: (m.sharpe_ratio || 0).toFixed(3) },
    { label: 'Max Drawdown', value: fmtPct(m.max_drawdown), color: 'var(--danger)' },
  ];

  if (hasBench) {
    tiles.push(
      { label: 'Benchmark Return', value: fmtPct(m.benchmark_total_return), color: m.benchmark_total_return >= 0 ? 'var(--success)' : 'var(--danger)' },
      { label: 'Excess Return', value: fmtPct(m.excess_return), color: m.excess_return >= 0 ? 'var(--success)' : 'var(--danger)' },
      { label: 'Info Ratio', value: (m.information_ratio || 0).toFixed(3) },
    );
  }

  tiles.push(
    { label: 'Price Return', value: fmtPct(m.portfolio_price_return) },
    { label: 'Dividend Return', value: fmtPct(m.portfolio_dividend_return) },
    { label: 'Period', value: m.start_date + ' → ' + m.end_date },
  );

  return el('div', { class: 'bt-metric-row' },
    ...tiles.map(t =>
      el('div', { class: 'bt-metric-tile' },
        el('div', { class: 'bt-metric-label', text: t.label }),
        el('div', { class: 'bt-metric-value', style: t.color ? `color:${t.color}` : '', text: t.value }),
      ),
    ),
  );
}

// ── Tables ───────────────────────────────────────────────────────────

function renderPerCompanyTable(companies, startDate, endDate) {
  return el('div', { class: 'bt-company-table' },
    el('div', { class: 'bt-chart-title', text: 'Per-Company Breakdown' }),
    el('table', { class: 'data-table' },
      el('thead', {},
        el('tr', {},
          el('th', { text: 'Ticker' }),
          el('th', { text: 'Start Price' }),
          el('th', { text: 'End Price' }),
          el('th', { text: 'Total Return' }),
          el('th', { text: 'Ann. Return' }),
          el('th', { text: 'Price Return' }),
          el('th', { text: 'Div Return' }),
          el('th', { text: 'Weight' }),
          companies[0] && companies[0].capital_invested != null ? el('th', { text: 'Capital' }) : null,
          companies[0] && companies[0].shares_purchased != null ? el('th', { text: 'Shares' }) : null,
          companies[0] && companies[0].capital_invested != null ? el('th', { text: 'Divs Received' }) : null,
          companies[0] && companies[0].market_value != null ? el('th', { text: 'Market Value' }) : null,
        ),
      ),
      el('tbody', {},
        ...companies.map(c => {
          const ann = startDate && endDate ? annualize(c.total_return || 0, startDate, endDate) : null;
          return el('tr', {},
            el('td', {}, tickerLink(c.Ticker)),
            el('td', { class: 'num', text: (c.start_price || 0).toFixed(2) }),
            el('td', { class: 'num', text: (c.end_price || 0).toFixed(2) }),
            el('td', { class: 'num', style: colorStyle(c.total_return), text: fmtPct(c.total_return) }),
            el('td', { class: 'num', style: colorStyle(ann), text: ann != null ? fmtPct(ann) : '-' }),
            el('td', { class: 'num', style: colorStyle(c.price_return), text: fmtPct(c.price_return) }),
            el('td', { class: 'num', style: colorStyle(c.dividend_return), text: fmtPct(c.dividend_return) }),
            el('td', { class: 'num', text: fmtPct(c.weight) }),
            c.capital_invested != null ? el('td', { class: 'num', text: c.capital_invested.toFixed(0) }) : null,
            c.shares_purchased != null ? el('td', { class: 'num', text: c.shares_purchased.toFixed(2) }) : null,
            c.capital_invested != null ? el('td', { class: 'num', text: (c.dividends_received || 0).toFixed(2) }) : null,
            c.market_value != null ? el('td', { class: 'num', text: c.market_value.toFixed(0) }) : null,
          );
        }),
      ),
    ),
  );
}

function renderYearlyTable(yearly) {
  return el('div', { class: 'bt-yearly-table' },
    el('div', { class: 'bt-chart-title', text: 'Yearly Returns' }),
    el('table', { class: 'data-table' },
      el('thead', {},
        el('tr', {},
          el('th', { text: 'Year' }),
          el('th', { text: 'Price Return' }),
          el('th', { text: 'Dividend Return' }),
          el('th', { text: 'Total (Ann.)' }),
        ),
      ),
      el('tbody', {},
        ...yearly.map(y =>
          el('tr', {},
            el('td', { text: y.Year || y.year }),
            el('td', { class: 'num', style: colorStyle(y['Price Return'] || y.price_return), text: fmtPct(y['Price Return'] || y.price_return) }),
            el('td', { class: 'num', style: colorStyle(y['Dividend Return'] || y.dividend_return), text: fmtPct(y['Dividend Return'] || y.dividend_return) }),
            el('td', { class: 'num', style: colorStyle(y['Total Return'] || y.total_return), text: fmtPct(y['Total Return'] || y.total_return) }),
          ),
        ),
      ),
    ),
  );
}

function renderDividendTable(dividends) {
  if (!dividends.length) return null;

  // Collect all ticker columns (excluding 'year' and 'Total')
  const first = dividends[0];
  const tickerCols = Object.keys(first).filter(k => k !== 'year' && k !== 'Total');

  return el('div', { class: 'bt-dividend-table' },
    el('div', { class: 'bt-chart-title', text: 'Dividends by Year' }),
    el('table', { class: 'data-table' },
      el('thead', {},
        el('tr', {},
          el('th', { text: 'Year' }),
          ...tickerCols.map(t => el('th', {}, tickerLink(t))),
          el('th', { text: 'Total' }),
        ),
      ),
      el('tbody', {},
        ...dividends.map(d =>
          el('tr', {},
            el('td', { text: d.year }),
            ...tickerCols.map(t => el('td', { class: 'num', text: (d[t] || 0).toFixed(2) })),
            el('td', { class: 'num', text: (d.Total || 0).toFixed(2) }),
          ),
        ),
      ),
    ),
  );
}

// Zero-line plugin for scatter plots
const zeroLinePlugin = {
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

// ---------------------------------------------------------------------------
// Chart.js helpers
// ---------------------------------------------------------------------------

function destroyChart(id) {
  if (ST.charts[id]) {
    ST.charts[id].destroy();
    delete ST.charts[id];
  }
}

function destroyAllCharts() {
  Object.keys(ST.charts).forEach(id => destroyChart(id));
}

function createCumulativeChart(canvasId, chartData, hasBench) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  destroyChart(canvasId);

  const ctx = canvas.getContext('2d');
  const dates = chartData.cumulative.map(d => d.date);
  const portfolio = chartData.cumulative.map(d => d.portfolio * 100);
  const datasets = [{
    label: 'Portfolio',
    data: portfolio,
    borderColor: '#58a6ff',
    backgroundColor: 'transparent',
    borderWidth: 2,
    pointRadius: 0,
    tension: 0.1,
  }];

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
    });
  }

  ST.charts[canvasId] = new Chart(ctx, {
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
}

function createDrawdownChart(canvasId, chartData, hasBench) {
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

  ST.charts[canvasId] = new Chart(ctx, {
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
}

function createDecompositionChart(canvasId, chartData) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  destroyChart(canvasId);

  const ctx = canvas.getContext('2d');
  const dates = chartData.decomposition.map(d => d.date);
  const price = chartData.decomposition.map(d => d.price_only * 100);
  const total = chartData.decomposition.map(d => d.total * 100);

  ST.charts[canvasId] = new Chart(ctx, {
    type: 'line',
    data: {
      labels: dates,
      datasets: [
        {
          label: 'Price Return',
          data: price,
          borderColor: '#42A5F5',
          backgroundColor: 'rgba(66,165,245,0.2)',
          fill: true,
          borderWidth: 1.5,
          pointRadius: 0,
          tension: 0.1,
          order: 2,
        },
        {
          label: 'Dividend Return',
          data: total,
          borderColor: '#66BB6A',
          backgroundColor: 'rgba(102,187,106,0.2)',
          fill: '-1',
          borderWidth: 1.5,
          pointRadius: 0,
          tension: 0.1,
          order: 1,
        },
        {
          label: 'Total Return',
          data: total,
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
          ticks: { color: '#8ea0b8', callback: v => (v >= 0 ? '+' : '') + v.toFixed(1) + '%' },
          grid: { color: 'rgba(255,255,255,0.06)' },
        },
      },
    },
  });
}

function createPerCompanyChart(canvasId, companies, startDate, endDate) {
  const canvas = document.getElementById(canvasId);
  if (!canvas || !companies.length) return;
  destroyChart(canvasId);

  const ctx = canvas.getContext('2d');

  // Use annualized returns for the scatter
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

  ST.charts[canvasId] = new Chart(ctx, {
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
              return `${p.ticker}: ann. price ${p.price >= 0 ? '+' : ''}${p.price.toFixed(1)}%, ann. div ${p.div >= 0 ? '+' : ''}${p.div.toFixed(1)}% (ann. total ${p.total >= 0 ? '+' : ''}${p.total.toFixed(1)}%)`;
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
}

function createYearlyScatterChart(canvasId, yearly) {
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

  ST.charts[canvasId] = new Chart(ctx, {
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
              return `${p.year}: price ${p.x >= 0 ? '+' : ''}${p.x.toFixed(1)}%, div ${p.y >= 0 ? '+' : ''}${p.y.toFixed(1)}% (total ${p.total >= 0 ? '+' : ''}${p.total.toFixed(1)}%)`;
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
}

// Helpers
// ---------------------------------------------------------------------------

function yearsBetween(start, end) {
  const s = new Date(start), e = new Date(end);
  return Math.max(0.25, (e - s) / (365.25 * 24 * 60 * 60 * 1000));
}

function annualize(totalReturn, startDate, endDate) {
  const yrs = yearsBetween(startDate, endDate);
  return (1 + totalReturn) ** (1 / yrs) - 1;
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

function fmtPct(v) {
  if (v == null || isNaN(v)) return '-';
  return (v >= 0 ? '+' : '') + (v * 100).toFixed(2) + '%';
}

function colorStyle(v) {
  if (v == null || isNaN(v)) return '';
  return v >= 0 ? 'color:var(--success)' : 'color:var(--danger)';
}
