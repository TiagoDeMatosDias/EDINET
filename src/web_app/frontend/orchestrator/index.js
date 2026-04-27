/**
 * Orchestrator screen — pipeline builder, inspector, step library, and the
 * main-dashboard panels that summarise pipeline/job state.
 *
 * Exports both public render entry-points used by main.js and the mutation
 * helpers (addStep, moveStep, …) that are wired to UI events there.
 */

import { STATE, els, callbacks, saveLastSetupName, persistLocalSetups, loadLocalSetups, LAST_SETUP_KEY } from '../common/state.js';
import { el, $, $all, deepClone, formatDate, metric, kvLine, section, fetchJson } from '../common/utils.js';
import { log } from '../common/console.js';

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

function stepStatusClass(status) {
  if (status === 'done') return 'ok';
  if (status === 'running') return 'warn';
  if (status === 'error') return 'bad';
  return '';
}

function pipelineSummary() {
  const enabled = STATE.pipeline.filter(step => step.enabled).length;
  const total = STATE.pipeline.length;
  const selected = STATE.pipeline.find(step => step.id === STATE.selectedStepId);
  return {
    enabled,
    total,
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
  STATE.selectedStepId = stepId;
  renderPipelineList();
  renderInspector();
}

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
  renderPipelineList();
  renderInspector();
  syncPipelineState();
}

function syncPipelineState() {
  STATE.config.run_steps = {};
  for (const item of STATE.pipeline) {
    STATE.config.run_steps[item.name] = { enabled: item.enabled, overwrite: item.overwrite };
  }
  renderMain();
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
      id,
      name,
      enabled: true,
      overwrite: false,
      status: 'idle',
      result: null,
      error: null,
    });
    STATE.selectedStepId = id;
    log('info', `Added step ${meta.display_name}`);
  }
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
  const menu = el('div', { class: 'popup-menu' });
  menu.style.cssText = [
    `position:fixed`,
    `left:${Math.min(rect.left, window.innerWidth - 360)}px`,
    `top:${Math.min(rect.bottom + 6, window.innerHeight - 20)}px`,
    `width:340px`,
    `z-index:1000`,
    `border:1px solid var(--line)`,
    `background:var(--panel)`,
    `box-shadow:0 16px 44px var(--shadow)`,
  ].join(';');
  menu.append(
    el('div', {
      style: 'padding:10px 10px 8px; border-bottom:1px solid var(--line); color:var(--muted); font-size:11px;',
      text: 'Load local setup',
    }),
  );
  entries.forEach(entry => {
    const item = el('button', {
      style: 'display:block; width:100%; text-align:left; border:none; border-bottom:1px solid var(--line); padding:9px 10px; background:transparent; cursor:pointer;',
    });
    item.append(
      el('div', { style: 'font-weight:600; color:var(--text);', text: entry.name }),
      el('div', {
        style: 'font-size:11px; color:var(--muted); margin-top:3px;',
        text: `${formatDate(entry.updatedAt)} • ${entry.pipeline?.filter(s => s.enabled).length || 0} enabled`,
      }),
    );
    item.addEventListener('click', () => {
      hydrateSetup(entry);
      callbacks.setView?.('orchestrator');
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
  const menu = document.querySelector('.popup-menu');
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

  const payload = {
    config: deepClone(STATE.config),
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
    // Surface server-side pipeline-level errors that are returned in the
    // response (e.g. configuration missing). The server currently returns
    // a 200 with an error_message in some failure cases, so log that here
    // so it appears in the browser console panel.
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
    metric('API', health, STATE.apiHealthy ? 'green' : 'red'),
    metric('Pipeline', `${enabled}/${STATE.pipeline.length || 0}`, 'blue'),
    metric('Required keys', `${filled}/${required.length || 0}`, filled === required.length ? 'green' : 'amber'),
    metric('Jobs', String(STATE.jobs.length), 'blue'),
  );
}

export function renderRecentJobs() {
  const tbody = $('#jobs-table tbody');
  if (!tbody) return;
  tbody.replaceChildren();
  for (const job of STATE.jobs.slice(0, 10)) {
    const tr = document.createElement('tr');
    const statusClass = job.status === 'completed' ? 'ok'
      : job.status === 'failed' ? 'bad'
      : job.status === 'running' ? 'warn'
      : '';
    tr.append(
      el('td', { class: 'code', text: job.job_id.slice(0, 12) + '…' }),
      el('td', {}, el('span', { class: `status-tag ${statusClass}`, text: job.status })),
      el('td', { text: job.current_step || '-' }),
      el('td', { text: `${Math.round(job.progress_percent || 0)}%` }),
      el('td', { text: formatDate(job.created_at) }),
    );
    tbody.append(tr);
  }
  if (!STATE.jobs.length) {
    const tr = document.createElement('tr');
    tr.append(el('td', { colspan: '5', class: 'code', text: 'No jobs returned yet.' }));
    tbody.append(tr);
  }
}

function renderSetupList() {
  const container = els.setupList;
  if (!container) return;
  container.replaceChildren();
  const items = Object.values(STATE.localSetups).sort(
    (a, b) => String(b.updatedAt || '').localeCompare(String(a.updatedAt || '')),
  );
  if (!items.length) {
    container.append(el('div', { class: 'panel-empty', text: 'No local setups yet.' }));
    return;
  }
  for (const setup of items) {
    const btn = el('button', { class: 'setup-row' });
    btn.append(
      el('div', { class: 'title', text: setup.name }),
      el('div', {
        class: 'meta',
        text: `${formatDate(setup.updatedAt)} • ${setup.pipeline?.filter(s => s.enabled).length || 0} active steps`,
      }),
    );
    btn.addEventListener('click', () => {
      hydrateSetup(setup);
      callbacks.setView?.('orchestrator');
      log('info', `Loaded setup ${setup.name}`);
    });
    container.append(btn);
  }
}

function renderCatalogPreview() {
  const container = els.catalogPreview;
  if (!container) return;
  container.replaceChildren();
  const metas = STATE.stepsMeta.slice(0, 6);
  for (const meta of metas) {
    const isSelected = STATE.pipeline.some(step => step.name === meta.name);
    const item = el('div', {
      class: `catalog-item${isSelected ? ' is-selected' : ''}`,
      dataset: { name: meta.name },
    });
    item.append(
      el('div', { class: 'name', text: meta.display_name }),
      el('div', {
        class: 'meta',
        text: `${meta.input_fields?.length || 0} fields • ${meta.supports_overwrite ? 'supports overwrite' : 'fixed run'}`,
      }),
    );
    item.addEventListener('click', () => {
      callbacks.setView?.('orchestrator');
      addStep(meta.name);
    });
    container.append(item);
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
      step.name,
      step.display_name,
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
  els.stepLibrary.replaceChildren();
  if (!steps.length) {
    els.stepLibrary.append(el('div', { class: 'panel-empty', text: 'No steps match the current search.' }));
    return;
  }
  for (const meta of steps) {
    const item = el('div', { class: 'step-item' });
    const chips = el('div', { class: 'chips' });
    chips.append(el('span', {
      class: 'chip accent',
      text: meta.input_fields?.length ? `${meta.input_fields.length} fields` : 'no fields',
    }));
    if (meta.supports_overwrite) chips.append(el('span', { class: 'chip success', text: 'overwrite' }));
    if (meta.aliases?.length) chips.append(el('span', { class: 'chip', text: `${meta.aliases.length} aliases` }));
    item.append(
      el('div', { class: 'top' },
        el('div', {},
          el('div', { class: 'name', text: meta.display_name }),
        ),
        chips,
      ),
    );
    item.addEventListener('click', () => addStep(meta.name));
    if (STATE.pipeline.some(step => step.name === meta.name)) item.classList.add('is-highlight');
    els.stepLibrary.append(item);
  }
}

export function renderPipelineList() {
  if (!els.pipelineList) return;
  const container = els.pipelineList;
  container.replaceChildren();
  const summary = pipelineSummary();
  els.pipelineSummary.textContent =
    `${summary.enabled} enabled / ${summary.total} steps • selected: ${summary.selected}`;

  if (!STATE.pipeline.length) {
    container.append(el('div', { class: 'panel-empty', text: 'No steps in the pipeline yet. Add one from the library.' }));
    return;
  }

  STATE.pipeline.forEach((step, index) => {
    const meta = findMeta(step.name);
    const row = el('article', {
      class: [
        'pipeline-step',
        STATE.selectedStepId === step.id ? 'is-selected' : '',
        step.status === 'done' ? 'is-done' : step.status === 'running' ? 'is-running' : step.status === 'error' ? 'is-error' : '',
      ].filter(Boolean).join(' '),
      draggable: 'true',
      dataset: { id: step.id },
    });

    const handle = el('div', { class: 'drag-handle', text: '≡' });
    const checkbox = el('input', { type: 'checkbox' });
    checkbox.checked = step.enabled;
    checkbox.addEventListener('change', () => {
      step.enabled = checkbox.checked;
      syncPipelineState();
      renderPipelineList();
    });

    const content = el('div', { class: 'step-content' },
      el('div', { class: 'step-title-row' },
        el('div', { class: 'step-name', text: meta?.display_name || step.name }),
        el('div', { class: 'step-badges' },
          el('span', { class: `chip ${step.enabled ? 'success' : ''}`, text: step.enabled ? 'enabled' : 'disabled' }),
          el('span', { class: `chip ${step.status === 'idle' ? '' : stepStatusClass(step.status)}`, text: step.status }),
          meta?.supports_overwrite && step.overwrite ? el('span', { class: 'chip accent', text: 'overwrite' }) : null,
        ),
      ),
      el('div', { class: 'step-summary', text: stepSummary(meta) }),
    );

    const actions = el('div', { class: 'step-actions' });
    const upBtn = el('button', { text: '↑', title: 'Move up' });
    upBtn.addEventListener('click', () => moveStep(index, -1));
    const downBtn = el('button', { text: '↓', title: 'Move down' });
    downBtn.addEventListener('click', () => moveStep(index, 1));
    const inspectBtn = el('button', { text: 'Inspect', title: 'Inspect step' });
    inspectBtn.addEventListener('click', () => selectStep(step.id));
    const removeBtn = el('button', { text: '×', title: 'Remove' });
    removeBtn.addEventListener('click', () => removeStepById(step.id));
    const owBtn = el('button', { text: 'OW', title: 'Toggle overwrite' });
    if (meta?.supports_overwrite) {
      owBtn.classList.toggle('is-active', step.overwrite);
      owBtn.addEventListener('click', () => {
        step.overwrite = !step.overwrite;
        syncPipelineState();
        renderPipelineList();
      });
    } else {
      owBtn.disabled = true;
      owBtn.title = 'Overwrite not supported';
    }
    actions.append(upBtn, downBtn, inspectBtn, owBtn, removeBtn);
    row.append(handle, checkbox, content, actions);

    row.addEventListener('click', ev => {
      if (ev.target.closest('button') || ev.target.closest('input')) return;
      selectStep(step.id);
    });
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
      renderPipelineList();
    });

    container.append(row);
  });
}

export function renderInspector() {
  if (!els.inspectorBody) return;
  const selected = getSelectedStep();
  const summary = pipelineSummary();
  els.inspectorTitle.textContent = selected
    ? findMeta(selected.name)?.display_name || selected.name
    : 'Inspector';
  els.inspectorSubtitle.textContent = selected
    ? `Selected step • ${summary.enabled} enabled / ${summary.total} total`
    : 'Select a step to edit its configuration.';
  els.inspectorBody.replaceChildren();

  if (!selected) {
    els.inspectorBody.append(el('div', { class: 'panel-empty', text: 'No step selected.' }));
    return;
  }

  const meta = findMeta(selected.name);
  const cfg = STATE.config[meta.config_key] || {};
  const root = el('div');
  root.append(
    section(
      'Step configuration',
      'Dense inline editor',
      'Edit the selected step configuration without leaving the pipeline.',
      buildStepEditor(meta, cfg, selected),
    ),
  );
  els.inspectorBody.append(root);
}

function buildStepEditor(meta, cfg, selected) {
  const wrap = el('div', { class: 'kv-grid' });
  for (const field of meta?.input_fields || []) {
    const current = cfg[field.key];
    const input = createFieldInput(field, current, value => {
      cfg[field.key] = value;
      selected.status = selected.status === 'done' ? 'idle' : selected.status;
      syncPipelineState();
      renderPipelineList();
    });
    wrap.append(createFieldRow(field.display_label || field.key, field.field_type, input));
  }
  if (!(meta?.input_fields || []).length) {
    wrap.append(el('div', { class: 'panel-empty', text: 'This step does not expose any editable fields.' }));
  }
  return wrap;
}

function createFieldRow(labelText, hintText, inputNode) {
  const row = el('div', { class: 'kv-row' },
    el('label', { text: labelText }),
    inputNode,
  );
  if (hintText) row.append(el('div', { class: 'hint', text: hintText }));
  return row;
}

function createFieldInput(field, current, commit) {
  const currentValue = current ?? fieldDefaultValue(field);
  if (field.field_type === 'text' || field.field_type === 'json') {
    const ta = el('textarea', { class: 'textarea-small' });
    ta.value = field.field_type === 'json'
      ? JSON.stringify(currentValue ?? {}, null, 2)
      : String(currentValue ?? '');
    ta.addEventListener('change', () => {
      const parsed = readValueByType(field.field_type, ta.value, currentValue);
      commit(parsed);
      if (field.field_type === 'json') ta.value = JSON.stringify(parsed ?? {}, null, 2);
    });
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
  input.addEventListener('change', () => {
    commit(readValueByType(field.field_type, input.value, currentValue));
  });
  return input;
}

function createFilesystemInput(field, currentValue, commit) {
  const wrap = el('div', { class: 'field-with-picker' });
  const input = el('input', { type: 'text' });
  input.value = currentValue === undefined || currentValue === null ? '' : String(currentValue);
  input.placeholder = field.display_label || field.key;
  input.addEventListener('change', () => {
    commit(input.value);
  });

  const button = el('button', { class: 'tiny-btn', type: 'button', text: 'Browse' });
  button.addEventListener('click', async () => {
    const picked = await pickPathLikeValue(field);
    if (!picked) return;
    input.value = picked;
    commit(picked);
  });

  wrap.append(input, button);
  return wrap;
}

async function pickPathLikeValue(field) {
  const fieldType = String(field.field_type || '').toLowerCase();
  const key = String(field.key || '').toLowerCase();
  const wantsDirectory = key.includes('dir') || key.includes('folder');

  if (wantsDirectory && typeof window.showDirectoryPicker === 'function') {
    try {
      const handle = await window.showDirectoryPicker();
      return handle?.name || '';
    } catch {
      return '';
    }
  }

  if (!wantsDirectory && typeof window.showOpenFilePicker === 'function') {
    try {
      const pickerTypes = [];
      if (fieldType === 'database') {
        pickerTypes.push({
          description: 'SQLite DB',
          accept: { 'application/x-sqlite3': ['.db', '.sqlite', '.sqlite3'] },
        });
      }
      const handles = await window.showOpenFilePicker({
        multiple: false,
        types: pickerTypes.length ? pickerTypes : undefined,
      });
      return handles?.[0]?.name || '';
    } catch {
      return '';
    }
  }

  return await new Promise(resolve => {
    const fileInput = document.createElement('input');
    fileInput.type = 'file';
    if (fieldType === 'database') {
      fileInput.accept = '.db,.sqlite,.sqlite3';
    }
    if (wantsDirectory) {
      fileInput.setAttribute('webkitdirectory', '');
    }
    fileInput.addEventListener('change', () => {
      const [file] = fileInput.files || [];
      if (!file) {
        resolve('');
        return;
      }
      if (wantsDirectory && file.webkitRelativePath) {
        resolve(file.webkitRelativePath.split('/')[0] || '');
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
    const removeBtn = el('button', { type: 'button', text: '×' });
    removeBtn.addEventListener('click', () => {
      tr.remove();
      if (!tbody.children.length) addRow();
      sync();
    });
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
    el('button', { type: 'button', text: 'Add row', onClick: () => { addRow(); sync(); } }),
    el('button', { type: 'button', text: 'Clear', onClick: () => { tbody.replaceChildren(); addRow(); sync(); } }),
  ));
  return wrap;
}

function renderGlobalConfigPanel() {
  if (!els.globalConfigForm) return;
  const root = el('div');
  const keys = [...new Set([...requiredKeysList(), ...Object.keys(STATE.config || {})])]
    .filter(key => key !== 'run_steps' && !STATE.stepsMeta.some(step => step.config_key === key))
    .sort((a, b) => a.localeCompare(b));

  root.append(el('div', { class: 'kv-grid' },
    ...keys.map(key => {
      const currentValue = String(STATE.config?.[key] ?? '');
      const pathLike = /file|path|database|dir|folder/i.test(key);
      const input = pathLike
        ? createFilesystemInput(
          {
            key,
            field_type: /database/i.test(key) ? 'database' : 'file',
            display_label: key,
            filetypes: /database/i.test(key) ? [["SQLite DB", "*.db"], ["All files", "*.*"]] : [],
          },
          currentValue,
          value => {
            STATE.config[key] = value;
            renderMain();
          },
        )
        : el('input', { type: 'text', value: currentValue });

      if (!pathLike) {
        input.addEventListener('change', () => {
          STATE.config[key] = input.value;
          renderMain();
        });
      }
      return createFieldRow(key, requiredKeysList().includes(key) ? 'required' : 'optional', input);
    }),
  ));

  els.globalConfigForm?.replaceChildren(root);
}

// =============================================================================
// Initialisation
// =============================================================================

export function initializeSetup() {
  STATE.localSetups = loadLocalSetups();
  const last = localStorage.getItem(LAST_SETUP_KEY) || 'Untitled setup';
  STATE.setupName = last || 'Untitled setup';
  if (els.setupNameInput) els.setupNameInput.value = STATE.setupName;
  STATE.config = setupDefaultConfig();
  STATE.pipeline = [];
  STATE.selectedStepId = null;
}
