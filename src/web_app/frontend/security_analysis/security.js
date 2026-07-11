/**
 * Security Analysis page entry point.
 * Thin bootstrap — delegates to modules.
 */

import { log } from '../common/console.js';
import { els } from '../common/state.js';
import { refreshHealth, wireTopbarEvents } from '../common/topbar.js';
import { state, restore } from './state.js';
import { init as initSearch, renderSearch, selectCompany, selectTicker } from './search.js';
import { renderSummary } from './summary.js';
import { renderTabs } from './tabs.js';
import { renderWorkspace } from './treegrid.js';
import { renderChartPanel } from './chartpanel.js';

export { selectCompany, selectTicker };

function markReady() { state.initDone = true; }

export function render() {
  renderSearch();
  renderSummary();
  renderTabs();
  renderWorkspace();
  renderChartPanel();
}

async function bootstrap() {
  wireTopbarEvents();
  els.backendStatus = document.getElementById('sa-backend-status');
  restore();
  initSearch();
  markReady();

  window._pageRefresh = () => { refreshHealth(); render(); };

  log('info', 'Security Analysis page initialized');
  await refreshHealth();

  // Check URL params first, then sessionStorage
  const params = new URLSearchParams(window.location.search);
  const urlCode = params.get('company_code');
  const urlSymbol = params.get('symbol');
  if (urlCode) {
    try { await selectCompany(urlCode); } catch (e) { log('warn', e.message); }
  } else if (urlSymbol) {
    try { await selectTicker(urlSymbol); } catch (e) { log('warn', e.message); }
  } else {
    const lastCode = sessionStorage.getItem('sa.lastCompanyCode');
    if (lastCode) {
      try { await selectCompany(lastCode); } catch (e) { log('warn', e.message); }
    }
  }

  render();
}

bootstrap().catch(err => log('error', `Startup: ${err.message}`));
