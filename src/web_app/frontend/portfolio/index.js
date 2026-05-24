import { $, el, fetchJson } from '../common/utils.js';
import { log } from '../common/console.js';
import { state, $$, refreshSummary, renderActivityBreakdown } from './common.js';
import { initUpload } from './upload.js';
import loadHoldings, { loadDisplayCurrencies, wireDisplayCurrency, wireColumnVisibility, renderHoldingsTab } from './holdings.js';
import { loadTransactions } from './transactions.js';
import { wireChartControls, wireChartExpand, wireTableViewToggles, renderChartsTab } from './charts.js';
import { wirePerformance, renderPerformanceTab } from './performance.js';

function wireTabs() {
  const tabs = document.getElementById('pf-tabs');
  if (!tabs) return;
  const buttons = Array.from(tabs.querySelectorAll('.tab-btn'));
  if (!buttons.length) return;

  function activateTab(btn) {
    buttons.forEach(b => b.classList.remove('is-active'));
    btn.classList.add('is-active');
    const tab = btn.dataset.tab;
    Array.from(document.querySelectorAll('.tab-panel')).forEach(p => p.classList.remove('is-active'));
    const panel = document.querySelector(`[data-panel="${tab}"]`);
    if (panel) panel.classList.add('is-active');
    if (tab === 'holdings') renderHoldingsTab();
    if (tab === 'transactions') loadTransactions();
    if (tab === 'charts') renderChartsTab();
    if (tab === 'performance') renderPerformanceTab();
  }

  buttons.forEach(btn => {
    btn.addEventListener('click', (e) => { e.preventDefault(); activateTab(btn); });
    btn.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); activateTab(btn); } });
  });
}

function wireRebuild() {
  const btn = $('#pf-rebuild-btn');
  if (!btn) return;
  btn.addEventListener('click', async () => {
    btn.disabled = true; btn.textContent = '⏳ Rebuilding…';
    try {
      const resp = await fetch('/api/portfolio/rebuild', { method: 'POST' });
      const data = await resp.json();
      btn.textContent = `✓ ${data.daily_rows} days, ${data.holdings_count} holdings`;
    } catch (e) { btn.textContent = `✗ Failed`; }
    finally {
      setTimeout(() => { btn.disabled = false; btn.textContent = 'Rebuild State'; }, 3000);
      await refreshSummary();
      await loadHoldings();
      await loadTransactions();
    }
  });
}

async function init() {
  wireTabs();
  initUpload();
  await refreshSummary();
  renderActivityBreakdown();
  await loadDisplayCurrencies();
  wireDisplayCurrency();
  wireColumnVisibility();
  await loadHoldings();
  await loadTransactions();
  wirePerformance();
  wireChartControls();
  wireChartExpand();
  wireTableViewToggles();
  wireRebuild();
}

init().catch(err => console.error('Portfolio boot error:', err));
