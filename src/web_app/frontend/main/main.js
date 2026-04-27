/**
 * Main dashboard page entry point (moved into the main folder).
 */

import { STATE, els, callbacks } from '../common/state.js';
import { $, fetchJson } from '../common/utils.js';
import { log, renderConsole, exportConsole } from '../common/console.js';
import { mountTopbar, mountConsoleFooter, refreshHealth, setConsoleHidden, wireTopbarEvents } from '../common/topbar.js';
import {
  renderMain,
  renderRecentJobs,
  setupDefaultConfig,
  initializeSetup,
} from '../orchestrator/index.js';

async function refreshJobs() {
  STATE.jobsLoading = true;
  try {
    const data = await fetchJson('/api/jobs?limit=12');
    STATE.jobs = data || [];
    renderRecentJobs();
    renderMain();
    log('debug', `Loaded ${STATE.jobs.length} jobs`);
  } catch (err) {
    log('error', `Jobs reload failed: ${err.message}`);
  } finally {
    STATE.jobsLoading = false;
  }
}

async function refreshSteps() {
  STATE.stepsLoading = true;
  try {
    const data = await fetchJson('/api/steps');
    STATE.stepsMeta = data.steps || [];
    if (!STATE.config) STATE.config = setupDefaultConfig();
    renderMain();
    log('info', `Loaded ${STATE.stepsMeta.length} steps from API`);
  } catch (err) {
    log('error', `Failed to load steps: ${err.message}`);
  } finally {
    STATE.stepsLoading = false;
  }
}

async function bootstrap() {
  wireTopbarEvents();

  // DOM element cache
  els.backendStatus    = document.getElementById('backend-status');
  els.refreshBtn       = document.getElementById('refresh-btn');
  els.toggleConsoleBtn = document.getElementById('toggle-console-btn');
  els.reloadJobs       = document.getElementById('reload-jobs');
  els.mainMetrics      = document.getElementById('main-metrics');
  els.jobsTable        = document.getElementById('jobs-table');
  els.setupList        = document.getElementById('setup-list');
  els.catalogPreview   = document.getElementById('step-catalog-preview');
  els.consoleLog       = document.getElementById('console-log');
  els.consoleToggle    = document.getElementById('console-toggle');
  els.consoleClear     = document.getElementById('console-clear');
  els.consoleExport    = document.getElementById('console-export');
  els.consoleAutoscroll = document.getElementById('console-autoscroll');
  els.consoleFilter    = document.getElementById('console-filter');

  callbacks.refreshJobs = refreshJobs;

  // Events
  if (els.reloadJobs) els.reloadJobs.addEventListener('click', refreshJobs);
  if (els.consoleClear) els.consoleClear.addEventListener('click', () => { STATE.logs = []; renderConsole(); });
  if (els.consoleExport) els.consoleExport.addEventListener('click', exportConsole);
  if (els.consoleAutoscroll) els.consoleAutoscroll.addEventListener('change', () => {
    STATE.consoleAutoscroll = els.consoleAutoscroll.checked;
  });
  if (els.consoleToggle) els.consoleToggle.addEventListener('click', () => {
    setConsoleHidden(!document.body.classList.contains('console-collapsed'));
  });
  if (els.consoleFilter) els.consoleFilter.addEventListener('change', () => {
    STATE.consoleFilter = els.consoleFilter.value;
    renderConsole();
  });

  window._pageRefresh = async () => {
    await Promise.all([refreshHealth(), refreshJobs()]);
  };

  initializeSetup();
  setConsoleHidden(false);

  log('info', 'Main dashboard initialized');
  await Promise.all([refreshHealth(), refreshSteps(), refreshJobs()]);
  renderMain();
}

bootstrap().catch(err => {
  log('error', `Fatal startup error: ${err.message}`);
  const statusEl = document.getElementById('backend-status');
  if (statusEl) { statusEl.textContent = 'startup failed'; statusEl.className = 'status-pill bad'; }
});
