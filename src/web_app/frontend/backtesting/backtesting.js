/**
 * Backtesting page entry point.
 *
 * Bootstraps the backtesting view, wires topbar events, health check,
 * and initializes the render logic.
 */

import { log } from '../common/console.js';
import { els } from '../common/state.js';
import { refreshHealth, wireTopbarEvents } from '../common/topbar.js';
import { render, handleHashParams } from './index.js';

async function bootstrap() {
  wireTopbarEvents();

  els.backendStatus = document.getElementById('bt-backend-status');

  window._pageRefresh = () => refreshHealth();
  log('info', 'Backtesting page initialized');
  await refreshHealth();

  // Parse hash params first so render() can show the correct mode
  handleHashParams();
  await render();
}

bootstrap().catch(err => {
  log('error', `Fatal startup error: ${err.message}`);
});
