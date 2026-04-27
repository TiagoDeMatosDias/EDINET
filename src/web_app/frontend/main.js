/**
 * Application entry point.
 *
 * Responsibilities:
 *   - Bootstrap DOM element cache (els)
 *   - Register cross-module callbacks (setView, refreshJobs)
 *   - Perform initial API fetches (health, steps, jobs)
 *   - Attach all top-level event listeners
 *
 * Screen-specific logic lives in the screen folders; only the glue that spans
 * multiple screens belongs here.
 */

import { STATE, els, callbacks } from './common/state.js';
import { $, $all, fetchJson } from './common/utils.js';
import { log, renderConsole, exportConsole } from './common/console.js';
import {
  renderAll,
  renderOrchestrator,
  renderMain,
  renderRecentJobs,
  renderStepLibrary,
  setupDefaultConfig,
  initializeSetup,
  saveSetup,
  newSetup,
  syncSetupNameFromInput,
  showLoadMenu,
  closeLoadMenu,
  runPipeline,
  stopPipeline,
  moveStep,
  removeStepById,
  addStep,
  hydrateSetup,
  requiredKeysList,
} from './orchestrator/index.js';
import { render as renderScreening } from './screening/index.js';
import { render as renderSecurityAnalysis } from './security_analysis/index.js';

function setConsoleHidden(hidden) {
  STATE.consoleHidden = !!hidden;
  document.body.classList.toggle('console-collapsed', STATE.consoleHidden);
  const label = STATE.consoleHidden ? 'Show Console' : 'Hide Console';
  if (els.toggleConsoleBtn) els.toggleConsoleBtn.textContent = label;
  if (els.consoleToggle) els.consoleToggle.textContent = STATE.consoleHidden ? 'Show' : 'Hide';
}

function attachInspectorResizer() {
  if (!els.inspectorResizer || !els.workspaceGrid) return;

  const minInspector = 260;
  const maxInspector = 640;

  const setInspectorWidth = width => {
    const clamped = Math.max(minInspector, Math.min(maxInspector, width));
    STATE.inspectorWidth = clamped;
    document.documentElement.style.setProperty('--inspector', `${clamped}px`);
  };

  setInspectorWidth(STATE.inspectorWidth || 340);

  els.inspectorResizer.addEventListener('mousedown', ev => {
    ev.preventDefault();
    const startX = ev.clientX;
    const current = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--inspector'), 10) || 340;

    const onMove = moveEvent => {
      const delta = startX - moveEvent.clientX;
      setInspectorWidth(current + delta);
    };

    const onUp = () => {
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
    };

    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
  });
}

// =============================================================================
// Navigation
// =============================================================================

function setView(view) {
  STATE.view = view;
  location.hash = `#${view}`;
  $all('.tab').forEach(btn => btn.classList.toggle('is-active', btn.dataset.view === view));
  $all('.view').forEach(panel => panel.classList.toggle('is-active', panel.dataset.viewPanel === view));
  if (view === 'orchestrator') {
    renderOrchestrator();
  } else if (view === 'main') {
    renderMain();
  } else if (view === 'screening') {
    renderScreening();
  } else if (view === 'security') {
    renderSecurityAnalysis();
  }
}

// =============================================================================
// API calls
// =============================================================================

async function refreshHealth() {
  try {
    const data = await fetchJson('/health');
    STATE.apiHealthy = true;
    STATE.apiVersion = '1.0';
    els.backendStatus.textContent = `API OK • ${data.jobs_active || 0} running`;
    els.backendStatus.className = 'status-pill ok';
  } catch (err) {
    STATE.apiHealthy = false;
    els.backendStatus.textContent = 'API offline';
    els.backendStatus.className = 'status-pill bad';
    log('error', `Health check failed: ${err.message}`);
  }
  renderMain();
}

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
    STATE.requiredKeys = [
      ...new Set(STATE.stepsMeta.flatMap(step => step.required_keys || [])),
    ].sort();
    if (!STATE.config) {
      STATE.config = setupDefaultConfig();
    } else {
      const defaults = setupDefaultConfig();
      for (const [key, value] of Object.entries(defaults)) {
        if (!(key in STATE.config)) STATE.config[key] = value;
      }
    }
    if (!STATE.pipeline.length) STATE.pipeline = [];
    renderAll();
    log('info', `Loaded ${STATE.stepsMeta.length} steps from API`);
  } catch (err) {
    log('error', `Failed to load steps: ${err.message}`);
  } finally {
    STATE.stepsLoading = false;
  }
}

// =============================================================================
// Event wiring
// =============================================================================

function attachEvents() {
  $all('.tab').forEach(btn => btn.addEventListener('click', () => setView(btn.dataset.view)));

  els.refreshBtn.addEventListener('click', async () => {
    log('debug', 'Refreshing API status and jobs');
    await Promise.all([refreshHealth(), refreshJobs()]);
  });
  els.reloadJobs.addEventListener('click', refreshJobs);
  els.newSetupBtn.addEventListener('click', newSetup);
  els.saveSetupBtn.addEventListener('click', saveSetup);
  els.loadSetupBtn.addEventListener('click', ev => showLoadMenu(ev.currentTarget));
  els.runBtn.addEventListener('click', runPipeline);
  els.stopBtn.addEventListener('click', stopPipeline);

  els.stepSearch.addEventListener('input', () => {
    STATE.searchQuery = els.stepSearch.value.toLowerCase();
    renderStepLibrary();
  });
  els.stepSearch.addEventListener('keydown', ev => {
    if (ev.key === 'Enter') {
      import('./orchestrator/index.js').then(m => {
        const match = m.filteredSteps()[0];
        if (match) addStep(match.name);
      });
    }
  });

  els.setupNameInput.addEventListener('change', syncSetupNameFromInput);
  els.consoleClear.addEventListener('click', () => { STATE.logs = []; renderConsole(); });
  els.consoleExport.addEventListener('click', exportConsole);
  els.consoleAutoscroll.addEventListener('change', () => {
    STATE.consoleAutoscroll = els.consoleAutoscroll.checked;
  });
  els.consoleToggle.addEventListener('click', () => {
    setConsoleHidden(!STATE.consoleHidden);
  });
  els.toggleConsoleBtn.addEventListener('click', () => {
    setConsoleHidden(!STATE.consoleHidden);
  });
  els.consoleFilter.addEventListener('change', () => {
    STATE.consoleFilter = els.consoleFilter.value;
    renderConsole();
  });

  document.addEventListener('keydown', ev => {
    const tag = document.activeElement?.tagName?.toLowerCase();
    const typing = ['input', 'textarea', 'select'].includes(tag);
    if (ev.ctrlKey && ev.key === 'Enter') {
      ev.preventDefault();
      if (STATE.view === 'orchestrator') runPipeline();
    }
    if (ev.ctrlKey && ev.key.toLowerCase() === 's') {
      ev.preventDefault();
      if (STATE.view === 'orchestrator') saveSetup();
    }
    if (ev.ctrlKey && ev.key.toLowerCase() === 'n') {
      ev.preventDefault();
      if (STATE.view === 'orchestrator') newSetup();
    }
    if (!typing && STATE.view === 'orchestrator' && STATE.selectedStepId) {
      const idx = STATE.pipeline.findIndex(step => step.id === STATE.selectedStepId);
      if (ev.altKey && ev.key === 'ArrowUp') { ev.preventDefault(); moveStep(idx, -1); }
      if (ev.altKey && ev.key === 'ArrowDown') { ev.preventDefault(); moveStep(idx, 1); }
      if (ev.key === 'Delete') { ev.preventDefault(); removeStepById(STATE.selectedStepId); }
    }
    if (ev.key === 'Escape') closeLoadMenu();
  });
}

// =============================================================================
// Bootstrap
// =============================================================================

async function bootstrap() {
  // Populate the DOM element cache.
  els.backendStatus    = $('#backend-status');
  els.refreshBtn       = $('#refresh-btn');
  els.toggleConsoleBtn = $('#toggle-console-btn');
  els.reloadJobs       = $('#reload-jobs');
  els.mainMetrics      = $('#main-metrics');
  els.jobsTable        = $('#jobs-table');
  els.setupList        = $('#setup-list');
  els.catalogPreview   = $('#step-catalog-preview');
  els.setupNameInput   = $('#setup-name');
  els.newSetupBtn      = $('#new-setup-btn');
  els.loadSetupBtn     = $('#load-setup-btn');
  els.saveSetupBtn     = $('#save-setup-btn');
  els.runBtn           = $('#run-btn');
  els.stopBtn          = $('#stop-btn');
  els.stepSearch       = $('#step-search');
  els.stepLibrary      = $('#step-library');
  els.pipelineList     = $('#pipeline-list');
  els.pipelineSummary  = $('#pipeline-summary');
  els.workspaceGrid    = $('.workspace-grid');
  els.inspectorResizer = $('#inspector-resizer');
  els.inspectorTitle   = $('#inspector-title');
  els.inspectorSubtitle = $('#inspector-subtitle');
  els.inspectorBody    = $('#inspector-body');
  els.globalConfigForm = $('#global-config-form');
  els.globalConfigPanel = $('#global-config-panel');
  els.consoleLog       = $('#console-log');
  els.consoleToggle    = $('#console-toggle');
  els.consoleClear     = $('#console-clear');
  els.consoleExport    = $('#console-export');
  els.consoleAutoscroll = $('#console-autoscroll');
  els.consoleFilter    = $('#console-filter');

  // Register callbacks so screen modules can trigger navigation / data refresh
  // without importing main.js (avoiding circular dependencies).
  callbacks.setView    = setView;
  callbacks.refreshJobs = refreshJobs;

  initializeSetup();
  attachEvents();
  attachInspectorResizer();
  setConsoleHidden(false);
  setView(STATE.view);

  log('info', 'Web workstation initialized');
  log('info', 'Loading backend status and step catalog');
  await Promise.all([refreshHealth(), refreshSteps(), refreshJobs()]);
  renderStepLibrary();
  renderAll();

  const setups = Object.keys(STATE.localSetups);
  if (setups.length) log('debug', `Found ${setups.length} local setup(s)`);
}

bootstrap().catch(err => {
  log('error', `Fatal startup error: ${err.message}`);
  if (els.backendStatus) {
    els.backendStatus.textContent = 'startup failed';
    els.backendStatus.className = 'status-pill bad';
  }
});
