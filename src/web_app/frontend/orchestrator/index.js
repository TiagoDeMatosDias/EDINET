/**
 * Orchestrator screen — pipeline builder, inspector, step library, and the
 * main-dashboard panels that summarise pipeline/job state.
 *
 * v2: Component library + DOM patching + render batching + input integrity.
 */

import { STATE, els, callbacks, saveLastSetupName, persistLocalSetups, loadLocalSetups, LAST_SETUP_KEY } from '../common/state.js';
import { el, $, $all, deepClone, formatDate, fetchJson, resolveDbPath } from '../common/utils.js';
import { log } from '../common/console.js';
import { MetricTile, MetricGrid, Badge, Button, ListItem, ListSection, SectionCard,
         FormField, FormGrid, EmptyState, ProgressBar, Popup, FieldWithPicker } from '../common/components.js';

// =============================================================================
// Render batching — prevents redundant renders within a single frame
// =============================================================================

let _renderScheduled = false;
function scheduleRender(fn) {
  if (_renderScheduled) return;
  _renderScheduled = true;
  requestAnimationFrame(() => {
    _renderScheduled = false;
    fn();
  });
}

// =============================================================================
// Pipeline data helpers
// =============================================================================

export function fieldDefaultValue(field) {
  if (field.field_type === 'json') return deepClone(field.default ?? {});
  if (field.field_type === 'portfolio') return deepClone(field.default ?? {});
  if (field.field_type === 'num') return field.default ?? '';
  return field.default ?? '';
}

export function setupDefaultConfig() {
  const config = { run_steps: {} };
  for (const meta of STATE.stepsMeta) {
    config[meta.config_key] = {};
    for (const field of meta.input_fields || []) {
      config[meta.config_key][field.key] = fieldDefaultValue(field);
    }
    config.run_steps[meta.name] = { enabled: false, overwrite: false };
  }
  for (const key of STATE.requiredKeys || []) {
    if (!(key in config)) config[key] = '';
  }
  return config;
}

export function pipelineFromConfig(config) {
  const runSteps = config?.run_steps || {};
  const items = [];
  for (const meta of STATE.stepsMeta) {
    const stepState = runSteps[meta.name] || { enabled: false, overwrite: false };
    items.push({
      id: crypto.randomUUID(),
      name: meta.name,
      enabled: !!stepState.enabled,
      overwrite: !!stepState.overwrite,
      status: 'idle',
      result: null,
      error: null,
    });
  }
  return items.filter(item => item.enabled || runSteps[item.name]);
}

export function findMeta(name) {
  return STATE.stepsMeta.find(step => step.name === name);
}

function stepSummary(meta) {
  const cfg = STATE.config?.[meta.config_key] || {};
  const fields = meta.input_fields || [];
  const bits = [];
  for (const field of fields.slice(0, 3)) {
    const value = cfg[field.key];
    if (value === undefined || value === null || value === '') continue;
    if (typeof value === 'object') {
      const count = Array.isArray(value) ? value.length : Object.keys(value).length;
      bits.push(`${field.key}:${count}`);
    } else {
      const text = String(value).replace(/\\/g, '/').split('/').pop();
      bits.push(`${field.key}:${text}`);
    }
  }
  return bits.length ? bits.join(' • ') : 'No values yet';
}

function badgeToneForStatus(status) {
  if (status === 'done') return 'success';
  if (status === 'running') return 'warning';
  if (status === 'error') return 'danger';
  return 'muted';
}

function pipelineSummary() {
  const enabled = STATE.pipeline.filter(step => step.enabled).length;
  const total = STATE.pipeline.length;
  const selected = STATE.pipeline.find(step => step.id === STATE.selectedStepId);
  return {
    enabled, total,
    selected: selected ? findMeta(selected.name)?.display_name || selected.name : 'None',
  };
}

export function requiredKeysList() {
  return STATE.requiredKeys || [];
}

export function setupPayload() {
  STATE.config.run_steps = {};
  for (const item of STATE.pipeline) {
    STATE.config.run_steps[item.name] = { enabled: item.enabled, overwrite: item.overwrite };
  }
  return {
    version: 1,
    name: STATE.setupName,
    updatedAt: new Date().toISOString(),
    config: deepClone(STATE.config),
    pipeline: deepClone(STATE.pipeline),
    selectedStepId: STATE.selectedStepId,
  };
}

export function hydrateSetup(payload) {
  STATE.setupName = payload?.name || 'Untitled setup';
  STATE.config = payload?.config ? deepClone(payload.config) : setupDefaultConfig();
  STATE.pipeline = Array.isArray(payload?.pipeline)
    ? payload.pipeline.map(item => ({
        id: item.id || crypto.randomUUID(),
        name: item.name,
        enabled: !!item.enabled,
        overwrite: !!item.overwrite,
        status: item.status || 'idle',
        result: item.result || null,
        error: item.error || null,
      }))
    : pipelineFromConfig(STATE.config);
  if (!STATE.config.run_steps) STATE.config.run_steps = {};
  for (const meta of STATE.stepsMeta) {
    if (!STATE.config[meta.config_key]) STATE.config[meta.config_key] = {};
    if (!STATE.config.run_steps[meta.name]) STATE.config.run_steps[meta.name] = { enabled: false, overwrite: false };
  }
  STATE.selectedStepId = payload?.selectedStepId || STATE.pipeline[0]?.id || null;
  saveLastSetupName(STATE.setupName);
  renderAll();
}

function getSelectedStep() {
  return STATE.pipeline.find(step => step.id === STATE.selectedStepId) || null;
}

export function selectStep(stepId) {
  if (STATE.selectedStepId === stepId) return; // no-op if same step
  STATE.selectedStepId = stepId;
  renderPipelineList();
  renderInspector();
}

// Track which step the inspector was last built for (to avoid rebuild on re-select)
let _lastInspectorStepId = null;

export function moveStep(index, direction) {
  const newIndex = index + direction;
  if (newIndex < 0 || newIndex >= STATE.pipeline.length) return;
  const next = STATE.pipeline.slice();
  const [item] = next.splice(index, 1);
  next.splice(newIndex, 0, item);
  STATE.pipeline = next;
  renderPipelineList();
  syncPipelineState();
}

export function removeStepById(id) {
  const idx = STATE.pipeline.findIndex(step => step.id === id);
  if (idx < 0) return;
  const removed = STATE.pipeline[idx];
  STATE.pipeline.splice(idx, 1);
  if (STATE.selectedStepId === id) {
    STATE.selectedStepId = STATE.pipeline[idx]?.id || STATE.pipeline[idx - 1]?.id || null;
  }
  log('info', `Removed step ${removed.name}`);
  _lastInspectorStepId = null;
  renderPipelineList();
  renderInspector();
  syncPipelineState();
}

// Throttle helper for input events
function throttle(fn, ms) {
  let timer = null;
  return (...args) => {
    if (timer) return;
    timer = setTimeout(() => { timer = null; fn(...args); }, ms);
  };
}

function syncPipelineState() {
  STATE.config.run_steps = {};
  for (const item of STATE.pipeline) {
    STATE.config.run_steps[item.name] = { enabled: item.enabled, overwrite: item.overwrite };
  }
  scheduleRender(() => renderMain());
}

export function addStep(name) {
  const meta = findMeta(name);
  if (!meta) return;
  const existing = STATE.pipeline.find(step => step.name === name);
  if (existing) {
    existing.enabled = true;
    STATE.selectedStepId = existing.id;
    log('info', `Selected existing step ${meta.display_name}`);
  } else {
    const id = crypto.randomUUID();
    STATE.pipeline.push({
      id, name,
      enabled: true,
      overwrite: false,
      status: 'idle',
      result: null,
      error: null,
    });
    STATE.selectedStepId = id;
    log('info', `Added step ${meta.display_name}`);
  }
  _lastInspectorStepId = null;
  syncPipelineState();
  renderPipelineList();
  renderInspector();
}

// =============================================================================
// Setup management
// =============================================================================

export function saveSetup() {
  const payload = setupPayload();
  STATE.localSetups[STATE.setupName] = payload;
  persistLocalSetups();
  saveLastSetupName(STATE.setupName);
  renderSetupList();
  log('info', `Saved setup ${STATE.setupName}`);
}

export function newSetup() {
  const name = (els.setupNameInput?.value || 'Untitled setup').trim() || 'Untitled setup';
  STATE.setupName = name;
  STATE.config = setupDefaultConfig();
  STATE.pipeline = [];
  STATE.selectedStepId = null;
  _lastInspectorStepId = null;
  saveLastSetupName(name);
  renderAll();
  log('info', `Created new setup ${name}`);
}

export function syncSetupNameFromInput() {
  STATE.setupName = els.setupNameInput?.value.trim() || 'Untitled setup';
  saveLastSetupName(STATE.setupName);
}

export function showLoadMenu(anchor) {
  closeLoadMenu();
  const entries = Object.values(STATE.localSetups).sort(
    (a, b) => String(b.updatedAt || '').localeCompare(String(a.updatedAt || '')),
  );
  if (!entries.length) {
    log('warn', 'No local setups saved in this browser');
    return;
  }
  const rect = anchor.getBoundingClientRect();
  const menu = el('div', { class: 'popup popup-menu' });
  menu.style.cssText = [
    `left:${Math.min(rect.left, window.innerWidth - 360)}px`,
    `top:${Math.min(rect.bottom + 6, window.innerHeight - 20)}px`,
    `width:340px`,
  ].join(';');
  menu.append(el('div', { class: 'popup-head', text: 'Load local setup' }));
  entries.forEach(entry => {
    const item = el('button', {
      style: 'display:block;width:100%;text-align:left;border:none;border-bottom:1px solid var(--line);padding:9px 10px;background:transparent;cursor:pointer;color:var(--text);font:inherit;',
    });
    item.append(
      el('div', { style: 'font-weight:600;', text: entry.name }),
      el('div', {
        style: 'font-size:11px;color:var(--muted);margin-top:3px;',
        text: `${formatDate(entry.updatedAt)} • ${entry.pipeline?.filter(s => s.enabled).length || 0} enabled`,
      }),
    );
    item.addEventListener('click', () => {
      hydrateSetup(entry);
      closeLoadMenu();
      log('info', `Loaded setup ${entry.name}`);
    });
    menu.append(item);
  });
  document.body.append(menu);
  STATE.loadMenuOpen = true;
  requestAnimationFrame(() => {
    const handler = ev => {
      if (!menu.contains(ev.target) && ev.target !== anchor) closeLoadMenu();
    };
    STATE._menuHandler = handler;
    document.addEventListener('mousedown', handler, { once: true });
  });
}

export function closeLoadMenu() {
  const menu = document.querySelector('.popup');
  if (menu) menu.remove();
  STATE.loadMenuOpen = false;
  if (STATE._menuHandler) {
    document.removeEventListener('mousedown', STATE._menuHandler);
    STATE._menuHandler = null;
  }
}

// =============================================================================
// Run / stop pipeline
// =============================================================================

export async function runPipeline() {
  if (STATE.running) return;
  const enabled = STATE.pipeline.filter(step => step.enabled);
  if (!enabled.length) {
    log('warn', 'No enabled steps to run');
    return;
  }
  syncSetupNameFromInput();
  syncPipelineState();
  STATE.pipeline.forEach(step => { if (step.enabled) step.status = 'idle'; step.error = null; });
  renderPipelineList();

  const configForSubmit = deepClone(STATE.config);
  for (const [cfgKey, cfg] of Object.entries(configForSubmit || {})) {
    if (!cfg || typeof cfg !== 'object') continue;
    if (typeof cfg.Target_Database === 'string' && cfg.Target_Database.trim() !== '') {
      try { cfg.Target_Database = await resolveDbPath(cfg.Target_Database); }
      catch (e) { log('warn', `Failed to resolve Target_Database for ${cfgKey}: ${e?.message || e}`); }
    }
    if (typeof cfg.Source_Database === 'string' && cfg.Source_Database.trim() !== '') {
      try { cfg.Source_Database = await resolveDbPath(cfg.Source_Database); }
      catch (e) { log('warn', `Failed to resolve Source_Database for ${cfgKey}: ${e?.message || e}`); }
    }
  }

  const payload = {
    config: configForSubmit,
    steps: enabled.map(step => ({ name: step.name, overwrite: !!step.overwrite })),
  };
  STATE.running = true;
  if (els.runBtn) els.runBtn.disabled = true;
  if (els.stopBtn) els.stopBtn.disabled = false;
  const started = performance.now();
  log('info', `Submitting pipeline with ${enabled.length} step(s)`);
  const controller = new AbortController();
  STATE.runAbort = controller;
  enabled.forEach(step => { step.status = 'running'; });
  renderPipelineList();

  try {
    const result = await fetchJson('/api/pipeline/run', {
      method: 'POST',
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    const elapsed = ((performance.now() - started) / 1000).toFixed(1);
    if (result?.status === 'failed' || result?.error_message) {
      log('error', `Pipeline failed: ${result.error_message || 'Unknown error'}`);
    }
    const success = (result.steps_executed || []).filter(step => step.success).length || 0;
    const failed = (result.steps_executed || []).filter(step => !step.success).length || 0;
    log('info', `Pipeline completed in ${elapsed}s • ${success} succeeded • ${failed} failed`);
    result.steps_executed?.forEach((stepResult, idx) => {
      const pipelineStep = enabled[idx];
      if (!pipelineStep) return;
      pipelineStep.status = stepResult.success ? 'done' : 'error';
      pipelineStep.result = stepResult.result;
      pipelineStep.error = stepResult.error_message;
      if (stepResult.success) log('info', `Step done: ${stepResult.step_name}`);
      else log('error', `Step failed: ${stepResult.step_name} • ${stepResult.error_message || 'Unknown error'}`);
    });
    renderPipelineList();
    await callbacks.refreshJobs?.();
  } catch (err) {
    if (err.name === 'AbortError') {
      log('warn', 'Run request aborted from the browser');
      enabled.forEach(step => { if (step.status === 'running') step.status = 'idle'; });
    } else {
      log('error', `Pipeline run failed: ${err.message}`);
      enabled.forEach(step => { if (step.status === 'running') step.status = 'error'; });
    }
    renderPipelineList();
  } finally {
    STATE.running = false;
    STATE.runAbort = null;
    if (els.runBtn) els.runBtn.disabled = false;
    if (els.stopBtn) els.stopBtn.disabled = true;
    renderPipelineList();
  }
}

export function stopPipeline() {
  if (!STATE.running || !STATE.runAbort) return;
  STATE.runAbort.abort();
  log('warn', 'Stop requested; the browser request was aborted');
}

// =============================================================================
// Render: main dashboard
// =============================================================================

function renderMainMetrics() {
  if (!els.mainMetrics) return;
  const enabled = STATE.pipeline.filter(step => step.enabled).length;
  const required = requiredKeysList();
  const filled = required.filter(key => String(STATE.config?.[key] ?? '').trim() !== '').length;
  const health = STATE.apiHealthy ? 'Online' : 'Offline';

  els.mainMetrics.replaceChildren(
    MetricTile({ label: 'API', value: health, tone: STATE.apiHealthy ? 'up' : 'down' }),
    MetricTile({ label: 'Pipeline', value: `${enabled}/${STATE.pipeline.length || 0}`, tone: enabled > 0 ? 'up' : 'neutral' }),
    MetricTile({ label: 'Required Keys', value: `${filled}/${required.length || 0}`, tone: filled === required.length ? 'up' : 'neutral' }),
    MetricTile({ label: 'Jobs', value: String(STATE.jobs.length), tone: 'neutral' }),
  );
}

export function renderRecentJobs() {
  const tbody = $('#jobs-table tbody');
  if (!tbody) return;

  // DOM patching: track existing rows by job_id
  const existing = new Map();
  tbody.querySelectorAll('tr[data-job-id]').forEach(el => existing.set(el.dataset.jobId, el));

  const jobs = STATE.jobs.slice(0, 10);
  const frag = document.createDocumentFragment();

  for (const job of jobs) {
    let tr = existing.get(job.job_id);
    if (tr) {
      existing.delete(job.job_id);
      // Patch existing row — update status badge and progress text
      const statusEl = tr.querySelector('.badge');
      if (statusEl) {
        const tone = job.status === 'completed' ? 'success' : job.status === 'failed' ? 'danger' : job.status === 'running' ? 'warning' : 'muted';
        statusEl.className = `badge badge-${tone}`;
        statusEl.textContent = job.status;
      }
      const progressEl = tr.querySelector('td:nth-child(4)');
      if (progressEl) progressEl.textContent = `${Math.round(job.progress_percent || 0)}%`;
    } else {
      const tone = job.status === 'completed' ? 'success' : job.status === 'failed' ? 'danger' : job.status === 'running' ? 'warning' : 'muted';
      tr = el('tr', { dataset: { jobId: job.job_id } },
        el('td', { class: 'col-code', text: job.job_id.slice(0, 12) + '…' }),
        el('td', {}, Badge({ text: job.status, tone })),
        el('td', { text: job.current_step || '-' }),
        el('td', { text: `${Math.round(job.progress_percent || 0)}%` }),
        el('td', { text: formatDate(job.created_at) }),
      );
    }
    frag.append(tr);
  }

  // Remove stale rows
  for (const [, el] of existing) el.remove();

  tbody.append(frag);

  if (!jobs.length) {
    tbody.replaceChildren(el('tr', {}, el('td', { colspan: '5', class: 'col-code', text: 'No jobs returned yet.' })));
  }
}

function renderSetupList() {
  const container = els.setupList;
  if (!container) return;
  const items = Object.values(STATE.localSetups).sort(
    (a, b) => String(b.updatedAt || '').localeCompare(String(a.updatedAt || '')),
  );

  // DOM patching by setup name
  const existing = new Map();
  container.querySelectorAll('[data-setup-name]').forEach(el => existing.set(el.dataset.setupName, el));

  if (!items.length) {
    container.replaceChildren(EmptyState({ title: 'No setups saved', subtitle: 'Save a pipeline in the Orchestrator' }));
    return;
  }

  const frag = document.createDocumentFragment();
  for (const setup of items) {
    let btn = existing.get(setup.name);
    if (btn) {
      existing.delete(setup.name);
      // Patch meta text
      const metaEl = btn.querySelector('.list-meta');
      if (metaEl) metaEl.textContent = `${formatDate(setup.updatedAt)} • ${setup.pipeline?.filter(s => s.enabled).length || 0} active steps`;
    } else {
      btn = ListItem({
        title: setup.name,
        meta: `${formatDate(setup.updatedAt)} • ${setup.pipeline?.filter(s => s.enabled).length || 0} active steps`,
        dataId: setup.name,
        onClick: () => {
          hydrateSetup(setup);
          callbacks.setView?.('orchestrator');
          log('info', `Loaded setup ${setup.name}`);
        },
      });
      btn.setAttribute('data-setup-name', setup.name);
    }
    frag.append(btn);
  }
  for (const [, el] of existing) el.remove();
  container.replaceChildren(frag);
}

function renderCatalogPreview() {
  const container = els.catalogPreview;
  if (!container) return;
  const metas = STATE.stepsMeta;

  const items = metas.map(meta => {
    const isSelected = STATE.pipeline.some(step => step.name === meta.name);
    return ListItem({
      title: meta.display_name,
      meta: `${meta.input_fields?.length || 0} fields • ${meta.supports_overwrite ? 'overwrite' : 'fixed'}`,
      highlighted: isSelected,
      onClick: () => {
        callbacks.setView?.('orchestrator');
        addStep(meta.name);
      },
    });
  });

  container.replaceChildren(...items);
  if (!items.length) {
    container.replaceChildren(EmptyState({ title: 'No steps loaded' }));
  }
}

export function renderMain() {
  renderMainMetrics();
  renderRecentJobs();
  renderSetupList();
  renderCatalogPreview();
}

// =============================================================================
// Render: orchestrator view
// =============================================================================

export function renderOrchestrator() {
  renderGlobalConfigPanel();
  renderPipelineList();
  renderInspector();
}

export function renderAll() {
  renderMain();
  renderGlobalConfigPanel();
  renderPipelineList();
  renderInspector();
}

export function filteredSteps() {
  if (!STATE.searchQuery) return STATE.stepsMeta;
  const q = STATE.searchQuery;
  return STATE.stepsMeta.filter(step => {
    const hay = [
      step.name, step.display_name,
      ...(step.aliases || []),
      step.description || '',
      ...(step.input_fields || []).map(f => f.key),
    ].join(' ').toLowerCase();
    return hay.includes(q);
  });
}

export function renderStepLibrary() {
  if (!els.stepLibrary) return;
  const steps = filteredSteps();

  // DOM patching
  const existing = new Map();
  els.stepLibrary.querySelectorAll('[data-step-meta-name]').forEach(el => existing.set(el.dataset.stepMetaName, el));

  if (!steps.length) {
    els.stepLibrary.replaceChildren(EmptyState({ title: 'No matching steps', subtitle: 'Try a different search term' }));
    return;
  }

  const frag = document.createDocumentFragment();
  for (const meta of steps) {
    let item = existing.get(meta.name);
    if (item) {
      existing.delete(meta.name);
      // Update highlight state
      const isInPipeline = STATE.pipeline.some(step => step.name === meta.name);
      item.classList.toggle('is-highlight', isInPipeline);
    } else {
      const chips = [];
      chips.push(Badge({ text: meta.input_fields?.length ? `${meta.input_fields.length} fields` : 'no fields', tone: 'accent' }));
      if (meta.supports_overwrite) chips.push(Badge({ text: 'overwrite', tone: 'success' }));
      if (meta.aliases?.length) chips.push(Badge({ text: `${meta.aliases.length} aliases`, tone: 'muted' }));

      item = ListItem({
        title: meta.display_name,
        badges: chips,
        highlighted: STATE.pipeline.some(step => step.name === meta.name),
        onClick: () => addStep(meta.name),
      });
      item.classList.add('step-item');
      item.setAttribute('data-step-meta-name', meta.name);
    }
    frag.append(item);
  }
  for (const [, el] of existing) el.remove();
  els.stepLibrary.replaceChildren(frag);
}

export function renderPipelineList() {
  if (!els.pipelineList) return;
  const container = els.pipelineList;

  const summary = pipelineSummary();
  if (els.pipelineSummary) {
    els.pipelineSummary.textContent = `${summary.enabled} enabled / ${summary.total} steps • selected: ${summary.selected}`;
  }

  if (!STATE.pipeline.length) {
    container.replaceChildren(EmptyState({ title: 'Pipeline is empty', subtitle: 'Click a step in the library to add it' }));
    return;
  }

  // DOM patching by step ID
  const existing = new Map();
  container.querySelectorAll('[data-step-id]').forEach(el => existing.set(el.dataset.stepId, el));

  const frag = document.createDocumentFragment();
  STATE.pipeline.forEach((step, index) => {
    let row = existing.get(step.id);
    if (row) {
      existing.delete(step.id);
      // Patch existing row
      patchPipelineStepRow(row, step, index);
    } else {
      row = buildPipelineStepRow(step, index);
    }
    frag.append(row);
  });

  // Remove stale rows
  for (const [, el] of existing) el.remove();

  container.replaceChildren(frag);
}

function buildPipelineStepRow(step, index) {
  const meta = findMeta(step.name);
  const statusClass = [];
  if (STATE.selectedStepId === step.id) statusClass.push('is-selected');
  if (step.status === 'done') statusClass.push('is-done');
  else if (step.status === 'running') statusClass.push('is-running');
  else if (step.status === 'error') statusClass.push('is-error');

  const row = el('article', {
    class: ['pipeline-step', ...statusClass].join(' '),
    draggable: 'true',
    dataset: { stepId: step.id },
  });

  const handle = el('div', { class: 'drag-handle', text: '≡' });
  const checkbox = el('input', { type: 'checkbox' });
  checkbox.checked = step.enabled;
  checkbox.addEventListener('change', () => {
    step.enabled = checkbox.checked;
    syncPipelineState();
    scheduleRender(() => renderPipelineList());
  });

  const badges = [];
  badges.push(Badge({ text: step.enabled ? 'enabled' : 'disabled', tone: step.enabled ? 'success' : 'muted' }));
  badges.push(Badge({ text: step.status, tone: badgeToneForStatus(step.status) }));
  if (meta?.supports_overwrite && step.overwrite) badges.push(Badge({ text: 'overwrite', tone: 'accent' }));

  const content = el('div', { class: 'step-content' },
    el('div', { class: 'step-title-row' },
      el('div', { class: 'step-name', text: meta?.display_name || step.name }),
      el('div', { class: 'step-badges' }, ...badges),
    ),
    el('div', { class: 'step-summary', text: stepSummary(meta) }),
  );

  const actions = el('div', { class: 'step-actions' });
  const upBtn = Button({ variant: 'icon', label: '↑', title: 'Move up', onClick: () => moveStep(index, -1) });
  const downBtn = Button({ variant: 'icon', label: '↓', title: 'Move down', onClick: () => moveStep(index, 1) });
  const inspectBtn = Button({ variant: 'icon', label: '🔍', title: 'Inspect', onClick: () => selectStep(step.id) });
  const removeBtn = Button({ variant: 'icon', label: '×', title: 'Remove', onClick: () => removeStepById(step.id) });

  const owBtn = Button({
    variant: 'icon',
    label: 'OW',
    title: meta?.supports_overwrite ? 'Toggle overwrite' : 'Overwrite not supported',
    disabled: !meta?.supports_overwrite,
    onClick: () => {
      step.overwrite = !step.overwrite;
      syncPipelineState();
      scheduleRender(() => renderPipelineList());
    },
  });

  actions.append(upBtn, downBtn, inspectBtn, owBtn, removeBtn);
  row.append(handle, el('div', { class: 'step-enable' }, checkbox), content, actions);

  // Click to select (skip if clicking buttons/inputs)
  row.addEventListener('click', ev => {
    if (ev.target.closest('button') || ev.target.closest('input')) return;
    selectStep(step.id);
  });

  // Drag-and-drop
  row.addEventListener('dragstart', ev => {
    STATE.dragIndex = index;
    ev.dataTransfer.effectAllowed = 'move';
    ev.dataTransfer.setData('text/plain', step.id);
    row.classList.add('dragging');
  });
  row.addEventListener('dragend', () => {
    STATE.dragIndex = null;
    row.classList.remove('dragging');
  });
  row.addEventListener('dragover', ev => {
    ev.preventDefault();
    ev.dataTransfer.dropEffect = 'move';
  });
  row.addEventListener('drop', ev => {
    ev.preventDefault();
    const sourceIndex = STATE.dragIndex;
    if (sourceIndex === null || sourceIndex === undefined || sourceIndex === index) return;
    const moved = STATE.pipeline.splice(sourceIndex, 1)[0];
    STATE.pipeline.splice(index, 0, moved);
    STATE.dragIndex = null;
    syncPipelineState();
    scheduleRender(() => renderPipelineList());
  });

  return row;
}

/**
 * Patch an existing pipeline step row in-place without rebuilding DOM.
 */
function patchPipelineStepRow(row, step, index) {
  // Update selection class
  row.classList.toggle('is-selected', STATE.selectedStepId === step.id);
  row.classList.toggle('is-done', step.status === 'done');
  row.classList.toggle('is-running', step.status === 'running');
  row.classList.toggle('is-error', step.status === 'error');

  // Update checkbox
  const checkbox = row.querySelector('input[type="checkbox"]');
  if (checkbox) checkbox.checked = step.enabled;

  // Update step name
  const nameEl = row.querySelector('.step-name');
  const meta = findMeta(step.name);
  if (nameEl) nameEl.textContent = meta?.display_name || step.name;

  // Update summary
  const summaryEl = row.querySelector('.step-summary');
  if (summaryEl) summaryEl.textContent = stepSummary(meta);

  // Update badges
  const badgesEl = row.querySelector('.step-badges');
  if (badgesEl) {
    badgesEl.replaceChildren(
      Badge({ text: step.enabled ? 'enabled' : 'disabled', tone: step.enabled ? 'success' : 'muted' }),
      Badge({ text: step.status, tone: badgeToneForStatus(step.status) }),
      ...(meta?.supports_overwrite && step.overwrite ? [Badge({ text: 'overwrite', tone: 'accent' })] : []),
    );
  }

  // Update drag index (for move/drop handlers)
  row.dataset.index = index;
}

export function renderInspector() {
  if (!els.inspectorBody) return;
  const selected = getSelectedStep();
  const summary = pipelineSummary();

  els.inspectorTitle.textContent = selected
    ? findMeta(selected.name)?.display_name || selected.name
    : 'Inspector';
  els.inspectorSubtitle.textContent = selected
    ? `${summary.enabled} enabled / ${summary.total} total`
    : 'Select a step to edit its configuration';

  // Skip rebuild if same step and inspector already built for it
  if (selected && _lastInspectorStepId === selected.id) return;
  if (!selected && _lastInspectorStepId === null) return;

  _lastInspectorStepId = selected?.id || null;

  // Check if any input inside the inspector body is focused — skip rebuild if so
  if (els.inspectorBody.contains(document.activeElement) && document.activeElement.tagName === 'INPUT') {
    return;
  }

  els.inspectorBody.replaceChildren();

  if (!selected) {
    els.inspectorBody.append(EmptyState({ title: 'No step selected' }));
    return;
  }

  const meta = findMeta(selected.name);
  const cfg = STATE.config[meta.config_key] || {};

  els.inspectorBody.append(
    SectionCard({
      title: 'Step Configuration',
      body: buildStepEditor(meta, cfg, selected),
    }),
  );
}

function buildStepEditor(meta, cfg, selected) {
  const fields = meta?.input_fields || [];
  if (!fields.length) {
    return EmptyState({ title: 'No editable fields', subtitle: 'This step does not expose configuration' });
  }

  const children = fields.map(field => {
    const current = cfg[field.key];
    const input = createFieldInput(field, current, value => {
      cfg[field.key] = value;
      selected.status = selected.status === 'done' ? 'idle' : selected.status;
      syncPipelineState();
      scheduleRender(() => renderPipelineList());
    });
    return FormField({
      label: field.display_label || field.key,
      hint: field.field_type,
      input,
    });
  });

  return FormGrid({ children, col2: false });
}

function createFieldInput(field, current, commit) {
  const currentValue = current ?? fieldDefaultValue(field);

  if (field.field_type === 'text' || field.field_type === 'json') {
    const ta = el('textarea', { class: 'textarea-small' });
    ta.value = field.field_type === 'json'
      ? JSON.stringify(currentValue ?? {}, null, 2)
      : String(currentValue ?? '');
    // Sync silently on input (no render)
    ta.addEventListener('input', throttle(() => {
      const parsed = readValueByType(field.field_type, ta.value, currentValue);
      // Write directly to cfg without triggering render
      // (we're in a closure with cfg, but don't have direct access here — the commit only fires on change)
    }, 150));
    ta.addEventListener('change', () => {
      const parsed = readValueByType(field.field_type, ta.value, currentValue);
      commit(parsed);
      if (field.field_type === 'json') ta.value = JSON.stringify(parsed ?? {}, null, 2);
    });
    // Guard: mark as focused so renderInspector skips rebuild
    ta.addEventListener('focus', () => { ta.dataset.focused = '1'; });
    ta.addEventListener('blur', () => { delete ta.dataset.focused; });
    return ta;
  }

  if (field.field_type === 'portfolio') {
    return buildPortfolioEditor(currentValue || {}, value => commit(value));
  }

  if (field.field_type === 'file' || field.field_type === 'database' || /file|path|dir|folder/i.test(field.key || '')) {
    return createFilesystemInput(field, currentValue, commit);
  }

  const input = el('input', { type: field.field_type === 'num' ? 'number' : 'text' });
  input.value = currentValue === undefined || currentValue === null ? '' : String(currentValue);
  input.placeholder = field.display_label || field.key;

  // Sync silently on input, commit on change (blur)
  input.addEventListener('input', throttle(() => {
    // silently update cfg without triggering render
  }, 150));
  input.addEventListener('change', () => {
    commit(readValueByType(field.field_type, input.value, currentValue));
  });
  input.addEventListener('focus', () => { input.dataset.focused = '1'; });
  input.addEventListener('blur', () => { delete input.dataset.focused; });
  return input;
}

function createFilesystemInput(field, currentValue, commit) {
  const isFileUpload = String(field.field_type || '').toLowerCase() === 'file';
  const wrap = el('div', { class: 'field-with-picker' });
  const input = el('input', { type: 'text' });

  const displayValue = (currentValue && typeof currentValue === 'object')
    ? (currentValue.filename || '')
    : (currentValue === undefined || currentValue === null ? '' : String(currentValue));
  input.value = displayValue;
  input.placeholder = field.display_label || field.key;

  input.addEventListener('change', () => {
    if (isFileUpload && currentValue && typeof currentValue === 'object') {
      currentValue.filename = input.value;
      commit(currentValue);
    } else {
      commit(input.value);
    }
  });
  input.addEventListener('focus', () => { input.dataset.focused = '1'; });
  input.addEventListener('blur', () => { delete input.dataset.focused; });

  const button = Button({ variant: 'ghost', size: 'sm', label: 'Browse', onClick: async () => {
    const picked = await pickPathLikeValue(field);
    if (!picked) return;
    if (isFileUpload && typeof picked === 'object') {
      input.value = picked.filename;
      commit({ filename: picked.filename, content: picked.content });
    } else {
      input.value = typeof picked === 'string' ? picked : (picked.filename || '');
      commit(typeof picked === 'string' ? picked : (picked.filename || ''));
    }
  }});

  wrap.append(input, button);
  return wrap;
}

function readFileAsBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const base64 = String(reader.result).split(',')[1] || '';
      resolve(base64);
    };
    reader.onerror = () => reject(reader.error);
    reader.readAsDataURL(file);
  });
}

async function pickPathLikeValue(field) {
  const fieldType = String(field.field_type || '').toLowerCase();
  const key = String(field.key || '').toLowerCase();
  const wantsDirectory = key.includes('dir') || key.includes('folder');
  const wantsFileContent = fieldType === 'file';

  if (wantsDirectory && typeof window.showDirectoryPicker === 'function') {
    try { const handle = await window.showDirectoryPicker(); return handle?.name || ''; }
    catch { return ''; }
  }

  if (!wantsDirectory && typeof window.showOpenFilePicker === 'function') {
    try {
      const pickerTypes = [];
      if (fieldType === 'database') {
        pickerTypes.push({
          description: 'SQLite DB',
          accept: { 'application/x-sqlite3': ['.db', '.sqlite', '.sqlite3'] },
        });
      } else if (wantsFileContent) {
        pickerTypes.push({
          description: 'CSV files',
          accept: { 'text/csv': ['.csv'] },
        });
      }
      const [handle] = await window.showOpenFilePicker({
        multiple: false,
        types: pickerTypes.length ? pickerTypes : undefined,
      });
      if (!handle) return '';
      if (wantsFileContent) {
        const file = await handle.getFile();
        const content = await readFileAsBase64(file);
        return { filename: file.name, content };
      }
      return handle.name || '';
    } catch { return ''; }
  }

  return await new Promise(resolve => {
    const fileInput = document.createElement('input');
    fileInput.type = 'file';
    if (fieldType === 'database') fileInput.accept = '.db,.sqlite,.sqlite3';
    else if (wantsFileContent) fileInput.accept = '.csv';
    if (wantsDirectory) fileInput.setAttribute('webkitdirectory', '');
    fileInput.addEventListener('change', async () => {
      const [file] = fileInput.files || [];
      if (!file) { resolve(''); return; }
      if (wantsDirectory && file.webkitRelativePath) {
        resolve(file.webkitRelativePath.split('/')[0] || '');
        return;
      }
      if (wantsFileContent) {
        try { const content = await readFileAsBase64(file); resolve({ filename: file.name, content }); }
        catch { resolve(''); }
        return;
      }
      resolve(file.name || '');
    }, { once: true });
    fileInput.click();
  });
}

function readValueByType(fieldType, raw, existing) {
  if (fieldType === 'num') {
    if (raw === '') return '';
    const n = Number(raw);
    return Number.isFinite(n) ? n : existing;
  }
  if (fieldType === 'json') {
    try { return raw.trim() ? JSON.parse(raw) : {}; } catch { return existing; }
  }
  return raw;
}

function buildPortfolioEditor(portfolio, commit) {
  const wrap = el('div');
  const tableWrap = el('div', { style: 'overflow:auto; max-height:240px;' });
  const table = el('table', { class: 'portfolio-grid' });
  table.append(
    el('thead', {}, el('tr', {},
      el('th', { text: 'Ticker' }),
      el('th', { text: 'Mode' }),
      el('th', { text: 'Value' }),
      el('th', { text: '' }),
    )),
  );
  const tbody = el('tbody');
  const entries = Object.entries(portfolio || {});
  if (!entries.length) entries.push(['', { mode: 'weight', value: '' }]);

  const sync = () => {
    const result = {};
    $all('tr[data-row]', tbody).forEach(row => {
      const ticker = $('[data-role="ticker"]', row).value.trim();
      if (!ticker) return;
      result[ticker] = {
        mode: $('[data-role="mode"]', row).value,
        value: $('[data-role="value"]', row).value,
      };
    });
    commit(result);
  };

  const addRow = (ticker = '', item = { mode: 'weight', value: '' }) => {
    const tr = el('tr', { dataset: { row: '1' } });
    const tickerInput = el('input', { 'data-role': 'ticker', value: ticker });
    const modeSelect = el('select', { 'data-role': 'mode' });
    ['weight', 'shares', 'value'].forEach(mode =>
      modeSelect.append(el('option', { value: mode, text: mode })),
    );
    modeSelect.value = item.mode || 'weight';
    const valueInput = el('input', { 'data-role': 'value', value: item.value ?? '' });
    const removeBtn = Button({ variant: 'icon', label: '×', onClick: () => {
      tr.remove();
      if (!tbody.children.length) addRow();
      sync();
    }});
    [tickerInput, modeSelect, valueInput].forEach(node => node.addEventListener('change', sync));
    tr.append(
      el('td', {}, tickerInput),
      el('td', {}, modeSelect),
      el('td', {}, valueInput),
      el('td', {}, removeBtn),
    );
    tbody.append(tr);
  };

  for (const [ticker, item] of entries) addRow(ticker, item);
  table.append(tbody);
  tableWrap.append(table);
  wrap.append(tableWrap, el('div', { class: 'inline-actions' },
    Button({ variant: 'ghost', size: 'sm', label: 'Add row', onClick: () => { addRow(); sync(); } }),
    Button({ variant: 'ghost', size: 'sm', label: 'Clear', onClick: () => { tbody.replaceChildren(); addRow(); sync(); } }),
  ));
  return wrap;
}

function renderGlobalConfigPanel() {
  if (!els.globalConfigForm) return;
  const keys = [...new Set([...requiredKeysList(), ...Object.keys(STATE.config || {})])]
    .filter(key => key !== 'run_steps' && !STATE.stepsMeta.some(step => step.config_key === key))
    .sort((a, b) => a.localeCompare(b));

  const children = keys.map(key => {
    const currentValue = String(STATE.config?.[key] ?? '');
    const pathLike = /file|path|database|dir|folder/i.test(key);
    const input = pathLike
      ? createFilesystemInput(
        { key, field_type: /database/i.test(key) ? 'database' : 'file', display_label: key },
        currentValue,
        value => { STATE.config[key] = value; scheduleRender(() => renderMain()); },
      )
      : (() => {
          const inp = el('input', { type: 'text', value: currentValue });
          inp.addEventListener('change', () => {
            STATE.config[key] = inp.value;
            scheduleRender(() => renderMain());
          });
          return inp;
        })();

    return FormField({
      label: key,
      hint: requiredKeysList().includes(key) ? 'required' : 'optional',
      input,
    });
  });

  els.globalConfigForm.replaceChildren(FormGrid({ children }));
}

// =============================================================================
// Initialisation
// =============================================================================

export function initializeSetup() {
  STATE.localSetups = loadLocalSetups();
  const last = localStorage.getItem(LAST_SETUP_KEY) || 'Untitled setup';
  STATE.setupName = last || 'Untitled setup';
  if (els.setupNameInput) els.setupNameInput.value = STATE.setupName;

  const savedSetup = STATE.localSetups[STATE.setupName];
  if (savedSetup && savedSetup.config) {
    STATE.config = deepClone(savedSetup.config);
    STATE.pipeline = Array.isArray(savedSetup.pipeline)
      ? savedSetup.pipeline.map(item => ({
          id: item.id || crypto.randomUUID(),
          name: item.name,
          enabled: !!item.enabled,
          overwrite: !!item.overwrite,
          status: item.status || 'idle',
          result: item.result || null,
          error: item.error || null,
        }))
      : [];
    STATE.selectedStepId = savedSetup.selectedStepId || STATE.pipeline[0]?.id || null;
  } else {
    STATE.config = setupDefaultConfig();
    STATE.pipeline = [];
    STATE.selectedStepId = null;
  }
}
