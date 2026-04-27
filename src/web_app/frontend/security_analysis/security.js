/**
 * Security Analysis page entry point (moved into the security_analysis folder).
 */

import { log, renderConsole, exportConsole } from '../common/console.js';
import { STATE, els } from '../common/state.js';
import { mountConsoleFooter, refreshHealth, setConsoleHidden, wireTopbarEvents } from '../common/topbar.js';
import { render as renderSecurityAnalysis } from './index.js';

async function bootstrap() {
  wireTopbarEvents();

  els.backendStatus    = document.getElementById('backend-status');
  els.consoleLog       = document.getElementById('console-log');
  els.consoleToggle    = document.getElementById('console-toggle');
  els.consoleClear     = document.getElementById('console-clear');
  els.consoleExport    = document.getElementById('console-export');
  els.consoleAutoscroll = document.getElementById('console-autoscroll');
  els.consoleFilter    = document.getElementById('console-filter');

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

  window._pageRefresh = () => refreshHealth();

  setConsoleHidden(false);
  log('info', 'Security Analysis page initialized');
  await refreshHealth();
  renderSecurityAnalysis();
}

bootstrap().catch(err => {
  log('error', `Fatal startup error: ${err.message}`);
});
