/**
 * Main dashboard page entry point.
 */

import { STATE, els, callbacks } from '../common/state.js';
import { $, fetchJson } from '../common/utils.js';
import { log } from '../common/console.js';
import { refreshHealth, wireTopbarEvents } from '../common/topbar.js';
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

  els.backendStatus  = document.getElementById('backend-status');
  els.refreshBtn     = document.getElementById('refresh-btn');
  els.reloadJobs     = document.getElementById('reload-jobs');
  els.mainMetrics    = document.getElementById('main-metrics');
  els.jobsTable      = document.getElementById('jobs-table');
  els.setupList      = document.getElementById('setup-list');
  els.catalogPreview = document.getElementById('step-catalog-preview');
  callbacks.refreshJobs = refreshJobs;

  if (els.reloadJobs) els.reloadJobs.addEventListener('click', refreshJobs);

  window._pageRefresh = async () => {
    await Promise.all([refreshHealth(), refreshJobs()]);
  };

  initializeSetup();

  log('info', 'Main dashboard initialized');
  await Promise.all([refreshHealth(), refreshSteps(), refreshJobs()]);
  renderMain();
}

bootstrap().catch(err => {
  log('error', `Fatal startup error: ${err.message}`);
  const statusEl = document.getElementById('backend-status');
  if (statusEl) { statusEl.textContent = 'startup failed'; statusEl.className = 'status-pill bad'; }
});
