/**
 * Orchestrator page entry point (moved into the orchestrator folder).
 */

import { STATE, els, callbacks } from '../common/state.js';
import { $, fetchJson } from '../common/utils.js';
import { log, renderConsole, exportConsole } from '../common/console.js';
import { mountTopbar, mountConsoleFooter, refreshHealth, setConsoleHidden, wireTopbarEvents } from '../common/topbar.js';
import {
  renderAll,
  renderOrchestrator,
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
  filteredSteps,
} from './index.js';

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
    const onMove = moveEvent => { const delta = startX - moveEvent.clientX; setInspectorWidth(current + delta); };
    const onUp = () => { document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp); };
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
  });
}

async function refreshJobs() {
  STATE.jobsLoading = true;
  try {
    const data = await fetchJson('/api/jobs?limit=12');
    STATE.jobs = data || [];
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

async function bootstrap() {
  wireTopbarEvents();

  // DOM element cache
  els.backendStatus     = document.getElementById('backend-status');
  els.refreshBtn        = document.getElementById('refresh-btn');
  els.toggleConsoleBtn  = document.getElementById('toggle-console-btn');
  els.setupNameInput    = document.getElementById('setup-name');
  els.newSetupBtn       = document.getElementById('new-setup-btn');
  els.loadSetupBtn      = document.getElementById('load-setup-btn');
  els.saveSetupBtn      = document.getElementById('save-setup-btn');
  els.runBtn            = document.getElementById('run-btn');
  els.stopBtn           = document.getElementById('stop-btn');
  els.stepSearch        = document.getElementById('step-search');
  els.stepLibrary       = document.getElementById('step-library');
  els.pipelineList      = document.getElementById('pipeline-list');
  els.pipelineSummary   = document.getElementById('pipeline-summary');
  els.workspaceGrid     = document.querySelector('.workspace-grid');
  els.inspectorResizer  = document.getElementById('inspector-resizer');
  els.inspectorTitle    = document.getElementById('inspector-title');
  els.inspectorSubtitle = document.getElementById('inspector-subtitle');
  els.inspectorBody     = document.getElementById('inspector-body');
  els.globalConfigForm  = document.getElementById('global-config-form');
  els.globalConfigPanel = document.getElementById('global-config-panel');
  els.consoleLog        = document.getElementById('console-log');
  els.consoleToggle     = document.getElementById('console-toggle');
  els.consoleClear      = document.getElementById('console-clear');
  els.consoleExport     = document.getElementById('console-export');
  els.consoleAutoscroll = document.getElementById('console-autoscroll');
  els.consoleFilter     = document.getElementById('console-filter');

  callbacks.refreshJobs = refreshJobs;

  // Event listeners
  if (els.newSetupBtn)   els.newSetupBtn.addEventListener('click', newSetup);
  if (els.saveSetupBtn)  els.saveSetupBtn.addEventListener('click', saveSetup);
  if (els.loadSetupBtn)  els.loadSetupBtn.addEventListener('click', ev => showLoadMenu(ev.currentTarget));
  if (els.runBtn)        els.runBtn.addEventListener('click', runPipeline);
  if (els.stopBtn)       els.stopBtn.addEventListener('click', stopPipeline);
  if (els.setupNameInput) els.setupNameInput.addEventListener('change', syncSetupNameFromInput);
  if (els.stepSearch) {
    els.stepSearch.addEventListener('input', () => {
      STATE.searchQuery = els.stepSearch.value.toLowerCase();
      renderStepLibrary();
    });
    els.stepSearch.addEventListener('keydown', ev => {
      if (ev.key === 'Enter') {
        const match = filteredSteps()[0];
        if (match) addStep(match.name);
      }
    });
  }
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

  document.addEventListener('keydown', ev => {
    const tag = document.activeElement?.tagName?.toLowerCase();
    const typing = ['input', 'textarea', 'select'].includes(tag);
    if (ev.ctrlKey && ev.key === 'Enter') { ev.preventDefault(); runPipeline(); }
    if (ev.ctrlKey && ev.key.toLowerCase() === 's') { ev.preventDefault(); saveSetup(); }
    if (ev.ctrlKey && ev.key.toLowerCase() === 'n') { ev.preventDefault(); newSetup(); }
    if (!typing && STATE.selectedStepId) {
      const idx = STATE.pipeline.findIndex(step => step.id === STATE.selectedStepId);
      if (ev.altKey && ev.key === 'ArrowUp') { ev.preventDefault(); moveStep(idx, -1); }
      if (ev.altKey && ev.key === 'ArrowDown') { ev.preventDefault(); moveStep(idx, 1); }
      if (ev.key === 'Delete') { ev.preventDefault(); removeStepById(STATE.selectedStepId); }
    }
    if (ev.key === 'Escape') closeLoadMenu();
  });

  window._pageRefresh = async () => {
    await Promise.all([refreshHealth(), refreshJobs()]);
  };

  initializeSetup();
  attachInspectorResizer();
  setConsoleHidden(false);

  log('info', 'Orchestrator page initialized');
  await Promise.all([refreshHealth(), refreshSteps(), refreshJobs()]);
  renderStepLibrary();
  renderAll();
  renderOrchestrator();
}

bootstrap().catch(err => {
  log('error', `Fatal startup error: ${err.message}`);
  const statusEl = document.getElementById('backend-status');
  if (statusEl) { statusEl.textContent = 'startup failed'; statusEl.className = 'status-pill bad'; }
});
