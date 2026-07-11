/**
 * Summary section — company identity, metrics grid, 52W range, description.
 */

import { log } from '../common/console.js';
import { fetchJson } from '../common/utils.js';
import { state, persist } from './state.js';
import { selectCompany } from './search.js';

const H = {
  $(id) { return document.getElementById(id); },
  el(tag, attrs = {}, ...children) {
    const el = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (k === 'class' || k === 'className') el.className = v;
      else if (k === 'text') el.textContent = v ?? '';
      else if (k === 'html') el.innerHTML = v;
      else if (k === 'style' && v && typeof v === 'object') Object.assign(el.style, v);
      else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2), v);
      else if (v !== undefined && v !== null) el.setAttribute(k, v);
    }
    for (const c of children.flat()) {
      if (c != null && c !== false) el.append(c.nodeType ? c : document.createTextNode(String(c)));
    }
    return el;
  },
};

export function renderSummary() {
  const el = H.$('sa-summary');
  const banner = H.$('sa-banner');
  const empty = H.$('sa-empty');

  if (!state.initDone) {
    el.classList.remove('is-visible');
    banner.style.display = 'none';
    return;
  }

  if (!state.company) {
    el.classList.remove('is-visible');
    banner.style.display = 'none';
    empty.classList.remove('hidden');
    return;
  }

  empty.classList.add('hidden');

  // Banner
  const flags = state.company?.metadata?.data_quality_flags || [];
  if (flags.length) {
    banner.style.display = 'flex';
    banner.textContent = '⚠ ' + flags.map(f => {
      if (f === 'missing_latest_price') return 'No price data.';
      if (f === 'missing_financial_statements') return 'No financials.';
      if (f === 'ticker_only_no_company_record') return 'Ticker-only — no company record.';
      return f;
    }).join(' ');
  } else {
    banner.style.display = 'none';
  }

  const c = state.company;
  if (!c?.company) {
    el.classList.remove('is-visible');
    return;
  }

  el.classList.add('is-visible');
  el.innerHTML = '';

  const co = c.company;
  const mkt = c.market || {};
  const metrics = c.metrics || {};

  // ── Company identity row ──
  const metaItems = [
    co.ticker ? H.el('span', { text: co.ticker }) : null,
    co.company_code ? H.el('span', { text: co.company_code }) : null,
    co.industry ? H.el('span', { text: co.industry }) : null,
    co.market ? H.el('span', { text: co.market }) : null,
  ].filter(Boolean);

  const filingMeta = c.metadata?.last_financial_period_end
    ? H.el('span', { class: 'sa-meta-filing', text: `📅 ${c.metadata.last_financial_period_end}` })
    : null;
  if (filingMeta) metaItems.push(filingMeta);

  el.appendChild(H.el('div', { class: 'sa-company-identity' },
    H.el('div', { class: 'sa-company-name', text: co.company_name || co.company_code || co.ticker }),
    H.el('div', { class: 'sa-company-meta' }, ...metaItems),
  ));

  // ── Metrics grid ──
  const metricDefs = [
    { id: 'LatestPrice',   label: 'Price',       fmt: 'price',    change: mkt.change_pct_1d },
    { id: 'MarketCap',     label: 'Market Cap',  fmt: 'currency' },
    { id: 'PERatio',       label: 'P/E',         fmt: 'ratio' },
    { id: 'PriceToBook',   label: 'P/B',         fmt: 'ratio' },
    { id: 'PriceToSales',  label: 'P/S',         fmt: 'ratio' },
    { id: 'DividendsYield',label: 'Div Yield',   fmt: 'percent' },
    { id: 'ReturnOnEquity',label: 'ROE',         fmt: 'percent' },
    { id: 'ReturnOnAssets',label: 'ROA',         fmt: 'percent' },
    { id: 'CurrentRatio',  label: 'Cur Ratio',   fmt: 'ratio' },
    { id: 'PayoutRatio',   label: 'Payout',      fmt: 'percent' },
  ];

  // Add EPS and BVPS from fundamentals
  const fundamentals = c.fundamentals_latest || {};
  const perShareExtras = [];
  if (fundamentals.SharesOutstanding != null) {
    const price = metrics.LatestPrice;
    const shares = fundamentals.SharesOutstanding;
    const equity = fundamentals.ShareholdersEquity;
    // Compute EPS and BVPS from available data
    if (equity != null && shares != null && shares > 0) {
      perShareExtras.push({ label: 'BVPS', value: equity / shares, fmt: 'currency' });
    }
  }

  const grid = H.el('div', { class: 'sa-metrics-grid' });

  for (const def of metricDefs) {
    const val = metrics[def.id];
    grid.appendChild(buildTile(def, val, mkt));
  }
  for (const extra of perShareExtras) {
    grid.appendChild(H.el('div', { class: 'sa-metric-tile' },
      H.el('div', { class: 'sa-metric-label', text: extra.label }),
      H.el('div', { class: 'sa-metric-value', text: fmtCur(extra.value) }),
    ));
  }

  el.appendChild(grid);

  // ── 52-week range ──
  const price = mkt.latest_price;
  if (mkt.range_52w_low != null && mkt.range_52w_high != null && price != null) {
    const pct = (price - mkt.range_52w_low) / (mkt.range_52w_high - mkt.range_52w_low);
    el.appendChild(H.el('div', { class: 'sa-range-bar-wrap' },
      H.el('span', { text: `¥${Number(mkt.range_52w_low).toLocaleString()}` }),
      H.el('div', { class: 'sa-range-bar' },
        H.el('div', { class: 'sa-range-dot', style: { left: `${Math.min(100, Math.max(0, pct * 100))}%` } })),
      H.el('span', { text: `¥${Number(mkt.range_52w_high).toLocaleString()}` }),
    ));
  }

  // ── Description ──
  const desc = co.description_summary || co.description || '';
  if (desc.length > 10) {
    const dd = H.el('div', { class: 'sa-description' });
    dd.textContent = desc.length > 250 ? desc.slice(0, 250) : desc;
    if (desc.length > 250) {
      dd.classList.remove('expanded');
      const tog = H.el('button', {
        class: 'sa-description-toggle', text: ' …more',
        onclick() {
          const x = dd.classList.toggle('expanded');
          tog.textContent = x ? ' …less' : ' …more';
          dd.textContent = x ? desc : desc.slice(0, 250);
        },
      });
      dd.appendChild(tog);
    }
    el.appendChild(dd);
  }
}

function buildTile(def, val, mkt) {
  const isPrice = def.id === 'LatestPrice';
  const tile = H.el('div', {
    class: `sa-metric-tile${isPrice ? ' price-tile' : ''}`,
  }, H.el('div', { class: 'sa-metric-label', text: def.label }));

  if (val == null) {
    tile.appendChild(H.el('div', { class: 'sa-metric-value muted', text: '—' }));
    return tile;
  }

  const n = Number(val);
  let display = '—';
  if (def.fmt === 'percent') display = `${(n * 100).toFixed(1)}%`;
  else if (def.fmt === 'currency') display = fmtCur(n);
  else if (def.fmt === 'price') display = `¥${n.toLocaleString()}`;
  else display = n.toFixed(2);

  const vEl = H.el('div', { class: 'sa-metric-value', text: display });

  if (isPrice && def.change != null) {
    const chg = def.change;
    const isUp = chg >= 0;
    vEl.className += isUp ? ' up' : ' down';
    vEl.textContent += ` ${isUp ? '▲' : '▼'}${(Math.abs(chg) * 100).toFixed(1)}%`;
  }

  tile.appendChild(vEl);

  if (isPrice && mkt.latest_price_date) {
    tile.appendChild(H.el('div', { class: 'sa-metric-sub', text: mkt.latest_price_date }));
  }

  if (isPrice && state.company?.company?.ticker) {
    tile.appendChild(H.el('button', {
      class: 'sa-update-price-btn',
      text: 'Update',
      title: 'Refresh price data from provider',
      onclick(e) {
        e.stopPropagation();
        const btn = this;
        btn.disabled = true;
        btn.textContent = 'Updating…';
        const ticker = state.company.company.ticker;
        const code = state.company.company.company_code;
        fetchJson('/api/security/update-price', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ticker }),
        })
          .then(r => {
            log('info', r.message || 'Price updated');
            if (code) selectCompany(code);
          })
          .catch(err => {
            log('error', err.message);
            btn.disabled = false;
            btn.textContent = 'Update';
          });
      },
    }));
  }

  return tile;
}

export function fmtCur(v) {
  const n = Number(v);
  if (Math.abs(n) >= 1e12) return `¥${(n / 1e12).toFixed(2)}T`;
  if (Math.abs(n) >= 1e9) return `¥${(n / 1e9).toFixed(2)}B`;
  if (Math.abs(n) >= 1e6) return `¥${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `¥${(n / 1e3).toFixed(1)}K`;
  return `¥${n.toLocaleString()}`;
}
