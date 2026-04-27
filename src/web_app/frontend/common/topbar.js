/**
 * Shared topbar helpers.
 *
 * The topbar and console footer HTML are embedded directly in each page file.
 * This module only wires events and exports utilities called by per-page scripts.
 */

import { fetchJson } from './utils.js';
import { log } from './console.js';

export function setConsoleHidden(hidden) {
  document.body.classList.toggle('console-collapsed', !!hidden);
  const topBtn  = document.getElementById('toggle-console-btn');
  const footBtn = document.getElementById('console-toggle');
  const label   = hidden ? 'Show Console' : 'Hide Console';
  if (topBtn)  topBtn.textContent  = label;
  if (footBtn) footBtn.textContent = hidden ? 'Show' : 'Hide';
}

export function wireTopbarEvents() {
  const toggleBtn = document.getElementById('toggle-console-btn');
  if (toggleBtn) {
    toggleBtn.addEventListener('click', () => {
      setConsoleHidden(!document.body.classList.contains('console-collapsed'));
    });
  }

  const refreshBtn = document.getElementById('refresh-btn');
  if (refreshBtn) {
    refreshBtn.addEventListener('click', () => {
      if (typeof window._pageRefresh === 'function') window._pageRefresh();
    });
  }

  const footToggle = document.getElementById('console-toggle');
  if (footToggle) {
    footToggle.addEventListener('click', () => {
      setConsoleHidden(!document.body.classList.contains('console-collapsed'));
    });
  }
}

export async function refreshHealth() {
  const statusEl = document.getElementById('backend-status');
  if (!statusEl) return;
  try {
    const data = await fetchJson('/health');
    statusEl.textContent = `API OK \u2022 ${data.jobs_active || 0} running`;
    statusEl.className = 'status-pill ok';
  } catch (err) {
    statusEl.textContent = 'API offline';
    statusEl.className = 'status-pill bad';
    log('error', `Health check failed: ${err.message}`);
  }
}

// Legacy no-ops kept so existing imports don't break.
export function mountTopbar() {}
export function mountConsoleFooter() {}
