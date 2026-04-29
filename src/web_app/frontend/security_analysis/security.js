/**
 * Security Analysis page entry point (moved into the security_analysis folder).
 */

import { log } from '../common/console.js';
import { els } from '../common/state.js';
import { refreshHealth, wireTopbarEvents } from '../common/topbar.js';
import { render as renderSecurityAnalysis } from './index.js';

async function bootstrap() {
  wireTopbarEvents();

  els.backendStatus    = document.getElementById('backend-status');

  window._pageRefresh = () => refreshHealth();
  log('info', 'Security Analysis page initialized');
  await refreshHealth();
  renderSecurityAnalysis();
}

bootstrap().catch(err => {
  log('error', `Fatal startup error: ${err.message}`);
});
