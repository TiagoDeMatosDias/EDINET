/**
 * Security Analysis page entry point.
 */

import { log } from '../common/console.js';
import { els } from '../common/state.js';
import { refreshHealth, wireTopbarEvents } from '../common/topbar.js';
import { init, render, selectCompany, markReady } from './index.js';

async function bootstrap() {
  wireTopbarEvents();
  els.backendStatus = document.getElementById('sa-backend-status');
  init();
  markReady();

  window._pageRefresh = () => { refreshHealth(); render(); };

  log('info', 'Security Analysis page initialized');
  await refreshHealth();

  // Check URL params first, then fall back to sessionStorage
  const params = new URLSearchParams(window.location.search);
  const urlCode = params.get('company_code');
  const lastCode = urlCode || sessionStorage.getItem('sa.lastCompanyCode');
  if (lastCode) {
    try { await selectCompany(lastCode); } catch (e) { log('warn', e.message); }
  }

  render();
}

bootstrap().catch(err => log('error', `Startup: ${err.message}`));
