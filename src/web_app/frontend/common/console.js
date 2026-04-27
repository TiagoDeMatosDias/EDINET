// Console log panel — rendering, appending log entries, and exporting.

import { STATE, els, MAX_LOGS } from './state.js';
import { el, nearBottom, nowStamp } from './utils.js';

export function log(level, message) {
  STATE.logs.push({
    ts: new Date().toISOString(),
    time: nowStamp(),
    level,
    message,
  });
  if (STATE.logs.length > MAX_LOGS) STATE.logs.shift();
  renderConsole();
}

export function renderConsole() {
  const lines = STATE.logs.filter(
    line => STATE.consoleFilter === 'all' || line.level === STATE.consoleFilter,
  );
  const shouldStick = STATE.consoleAutoscroll && nearBottom(els.consoleLog);
  els.consoleLog.replaceChildren();
  for (const line of lines) {
    const lvl = line.level === 'warn' ? 'warn'
      : line.level === 'error' ? 'error'
      : line.level === 'debug' ? 'debug'
      : 'info';
    els.consoleLog.append(
      el('div', { class: `log-line ${lvl}` },
        el('span', { class: 'log-time', text: line.time }),
        ' ',
        el('span', { class: `log-level ${lvl}`, text: line.level.toUpperCase().padEnd(5) }),
        line.message,
      ),
    );
  }
  if (shouldStick) els.consoleLog.scrollTop = els.consoleLog.scrollHeight;
}

export function exportConsole() {
  const text = STATE.logs
    .map(line => `[${line.time}] ${line.level.toUpperCase()} ${line.message}`)
    .join('\n');
  const blob = new Blob([text], { type: 'text/plain;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `edinet-console-${Date.now()}.log`;
  a.click();
  URL.revokeObjectURL(url);
}
