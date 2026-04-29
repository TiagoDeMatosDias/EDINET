/**
 * Screening page entry point (moved into the screening folder).
 */

import { log } from '../common/console.js';
import { els } from '../common/state.js';
import { refreshHealth, wireTopbarEvents } from '../common/topbar.js';
import { render as renderScreening } from './index.js';

async function bootstrap() {
  wireTopbarEvents();

  els.backendStatus    = document.getElementById('backend-status');

  window._pageRefresh = () => refreshHealth();
  log('info', 'Screening page initialized');
  await refreshHealth();
  renderScreening();
}

bootstrap().catch(err => {
  log('error', `Fatal startup error: ${err.message}`);
});
