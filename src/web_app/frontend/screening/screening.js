/**
 * Screening page entry point.
 *
 * Bootstraps the screening view, wires topbar events, health check, and
 * initializes the screening render logic.
 */

import { log } from '../common/console.js';
import { els } from '../common/state.js';
import { refreshHealth, wireTopbarEvents } from '../common/topbar.js';
import { render as renderScreening } from './index.js';

async function bootstrap() {
  wireTopbarEvents();

  els.backendStatus = document.getElementById('backend-status');

  window._pageRefresh = () => refreshHealth();
  log('info', 'Screening page initialized');
  await refreshHealth();
  await renderScreening();
}

bootstrap().catch(err => {
  log('error', `Fatal startup error: ${err.message}`);
});
