import { useQuery } from '@tanstack/react-query'
import { X } from 'lucide-react'
import { useMemo, useState, type KeyboardEvent } from 'react'
import { Bar, Line } from 'react-chartjs-2'
import { Link } from 'react-router-dom'

import { apiRequest } from '../../api/client'
import { ErrorState, LoadingState } from '../../components/Feedback'
import { SEMANTIC_CHART_COLORS, SERIES_COLORS } from '../../brand'
import { dec, finite, heatColor, heatText, holdingLabel, pct, tone, type BacktestDetail, type HoldingDetail } from './backtestModel'
import { asPercent, BENCHMARK_COLOR, percentOptions } from './charts'

const VIEWS = [['contribution', 'Contribution'], ['years', 'By year'], ['allocation', 'Allocation'], ['dividends', 'Dividends']] as const
type View = typeof VIEWS[number][0]
const TOP = SERIES_COLORS.length - 1
const OTHER_COLOR = '#A39E94'

const money = (value: unknown) => {
  const number = finite(value)
  return number == null ? '—' : number.toLocaleString(undefined, { maximumFractionDigits: Math.abs(number) >= 1000 ? 0 : 2 })
}

/** Colours follow the holding (by contribution rank), never the current filter. */
function colorFor(index: number) { return index < TOP ? SERIES_COLORS[index] : OTHER_COLOR }

function ContributionBars({ holdings, names, selected, onSelect }: { holdings: HoldingDetail[]; names?: Record<string, string>; selected: string | null; onSelect: (ticker: string) => void }) {
  const labels = holdings.map(item => holdingLabel(item.ticker, names?.[item.ticker] ?? item.name))
  const values = holdings.map(item => asPercent(item.contribution))
  return <div className="bt-chart" style={{ height: Math.max(120, holdings.length * 24 + 40) }}>
    <Bar
      data={{ labels, datasets: [{ label: 'Contribution', data: values, backgroundColor: holdings.map(item => (item.contribution ?? 0) >= 0 ? SEMANTIC_CHART_COLORS.positive : SEMANTIC_CHART_COLORS.negative), borderRadius: 4, borderSkipped: 'start', barThickness: 14, borderColor: holdings.map(item => item.ticker === selected ? '#1C1B19' : 'transparent'), borderWidth: holdings.map(item => item.ticker === selected ? 2 : 0) }] }}
      options={{
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        onClick: (_event, elements) => { if (elements[0]) onSelect(holdings[elements[0].index].ticker) },
        plugins: { legend: { display: false }, tooltip: { callbacks: { label: context => `Added ${pct((context.parsed.x ?? 0) / 100, 2, true)} to the total return` } } },
        scales: {
          x: { position: 'top', grid: { color: '#E4DFD3' }, ticks: { callback: value => `${Number(value).toFixed(0)}%`, font: { size: 9 } } },
          y: { grid: { display: false }, ticks: { font: { size: 10 }, autoSkip: false } },
        },
      }}
    />
  </div>
}

function HoldingsTable({ holdings, names, selected, onSelect }: { holdings: HoldingDetail[]; names?: Record<string, string>; selected: string | null; onSelect: (ticker: string) => void }) {
  const onKeyDown = (event: KeyboardEvent<HTMLTableRowElement>, ticker: string) => {
    if (event.key === 'Enter') { event.preventDefault(); onSelect(ticker); return }
    if (!['j', 'k', 'ArrowDown', 'ArrowUp'].includes(event.key)) return
    const next = (event.key === 'j' || event.key === 'ArrowDown' ? event.currentTarget.nextElementSibling : event.currentTarget.previousElementSibling) as HTMLElement | null
    if (next) { event.preventDefault(); next.focus() }
  }
  return <div className="bt-scroll">
    <table className="bt-table">
      <thead><tr><th>Holding</th><th className="num">Weight</th><th className="num">Start → end</th><th className="num">Price</th><th className="num">Dividends</th><th className="num">Total</th><th className="num">Contribution</th><th className="num">Dividends received</th><th className="num">End value</th></tr></thead>
      <tbody>{holdings.map((item, index) => <tr key={item.ticker} tabIndex={0} aria-selected={selected === item.ticker} className={selected === item.ticker ? 'is-selected' : ''} onClick={() => onSelect(item.ticker)} onKeyDown={event => onKeyDown(event, item.ticker)}>
        <td><span className="bt-swatch" style={{ background: colorFor(index) }} aria-hidden="true" />{holdingLabel(item.ticker, names?.[item.ticker] ?? item.name)}<small>{item.currency}</small></td>
        <td className="num">{pct(item.weight)}</td>
        <td className="num muted">{dec(item.start_price, 0)} → {dec(item.end_price, 0)}</td>
        <td className={`num ${tone(item.price_return)}`}>{pct(item.price_return, 1, true)}</td>
        <td className="num">{pct(item.dividend_return)}</td>
        <td className={`num ${tone(item.total_return)}`}>{pct(item.total_return, 1, true)}</td>
        <td className={`num ${tone(item.contribution)}`}><b>{pct(item.contribution, 2, true)}</b></td>
        <td className="num">{money(item.dividends_received)}</td>
        <td className="num">{money(item.market_value)}</td>
      </tr>)}</tbody>
    </table>
  </div>
}

function YearsTable({ detail, names, onSelect }: { detail: BacktestDetail; names?: Record<string, string>; onSelect: (ticker: string) => void }) {
  const tickers = detail.holdings.map(item => item.ticker)
  return <div className="bt-scroll">
    <table className="bt-table bt-heat">
      <thead><tr><th>Year</th>{tickers.map(ticker => <th key={ticker} className="num"><button type="button" className="text-button" title={holdingLabel(ticker, names?.[ticker])} onClick={() => onSelect(ticker)}>{ticker}</button></th>)}<th className="num">Portfolio</th></tr></thead>
      <tbody>{detail.contribution_by_year.map(row => {
        const total = tickers.reduce((sum, ticker) => sum + (finite(row[ticker]) ?? 0), 0)
        return <tr key={String(row.year)}>
          <th scope="row">{row.year}</th>
          {tickers.map(ticker => <td key={ticker} className="num" style={{ background: heatColor(row[ticker], 0.05), color: heatText(row[ticker], 0.05) }}>{row[ticker] == null ? '' : pct(row[ticker], 2, true)}</td>)}
          <td className={`num ${tone(total)}`}><b>{pct(total, 2, true)}</b></td>
        </tr>
      })}</tbody>
    </table>
    <p className="muted bt-footnote">Each holding's start-of-year weight × its return for the year; a row adds up to the portfolio's return for that year.</p>
  </div>
}

function Allocation({ detail }: { detail: BacktestDetail }) {
  const top = detail.holdings.slice(0, TOP)
  const rest = detail.holdings.slice(TOP)
  const sum = (series: Record<string, Array<number | null>>, index: number) => rest.reduce((total, item) => total + (series[item.ticker]?.[index] ?? 0), 0)
  const labels = detail.dates
  const stacked = [
    ...top.map((item, index) => ({ label: item.ticker, data: (detail.allocation.holdings[item.ticker] ?? []).map(value => asPercent(value)), backgroundColor: `${colorFor(index)}d9`, borderColor: '#F7F5EF', borderWidth: 1, fill: index === 0 ? 'origin' : '-1', pointRadius: 0 })),
    ...(rest.length ? [{ label: `Other ${rest.length}`, data: labels.map((_, index) => asPercent(sum(detail.allocation.holdings, index))), backgroundColor: `${OTHER_COLOR}d9`, borderColor: '#F7F5EF', borderWidth: 1, fill: '-1', pointRadius: 0 }] : []),
    { label: 'Cash (dividends)', data: detail.allocation.cash.map(value => asPercent(value)), backgroundColor: `${BENCHMARK_COLOR}55`, borderColor: '#F7F5EF', borderWidth: 1, fill: '-1', pointRadius: 0 },
  ]
  const added = [
    ...top.map((item, index) => ({ label: item.ticker, data: (detail.contribution[item.ticker] ?? []).map(value => asPercent(value)), borderColor: colorFor(index), borderWidth: 1.5, pointRadius: 0 })),
    ...(rest.length ? [{ label: `Other ${rest.length}`, data: labels.map((_, index) => asPercent(sum(detail.contribution, index))), borderColor: OTHER_COLOR, borderWidth: 1.5, pointRadius: 0 }] : []),
  ]
  return <div className="bt-grid bt-grid--two">
    <figure className="bt-panel">
      <figcaption>Allocation over time · buy-and-hold weights drift; dividends build up as cash</figcaption>
      <div className="bt-chart bt-chart--tall"><Line data={{ labels, datasets: stacked }} options={percentOptions<'line'>({ xLabels: true, stacked: true, yMin: 0, yMax: 100 })} /></div>
    </figure>
    <figure className="bt-panel">
      <figcaption>What each holding has added to the portfolio's return so far</figcaption>
      <div className="bt-chart bt-chart--tall"><Line data={{ labels, datasets: added }} options={percentOptions<'line'>({ xLabels: true })} /></div>
    </figure>
  </div>
}

function DividendLedger({ detail, names }: { detail: BacktestDetail; names?: Record<string, string> }) {
  if (!detail.dividend_payments.length) return <p className="muted bt-empty">No dividend payments are recorded for this backtest{detail.holdings.some(item => (item.dividends_received ?? 0) > 0) ? ' (it was saved before payments were kept; run it again to see them)' : ''}.</p>
  return <div className="bt-scroll bt-scroll--tall">
    <table className="bt-table">
      <thead><tr><th>Record date</th><th>Holding</th><th>Payment</th><th className="num">As paid</th><th className="num">Split factor</th><th className="num">Per adjusted share</th><th className="num">Shares</th><th className="num">Cash</th></tr></thead>
      <tbody>{detail.dividend_payments.map((item, index) => <tr key={`${item.ticker}-${item.record_date}-${index}`}>
        <td>{item.record_date}</td><td>{holdingLabel(item.ticker, names?.[item.ticker])}</td><td>{item.payment || '—'}</td>
        <td className="num">{dec(item.reported_per_share)} <small>{item.currency}</small></td>
        <td className={`num ${item.split_factor !== 1 ? 'is-flagged' : 'muted'}`}>{dec(item.split_factor, 3)}</td>
        <td className="num">{dec(item.per_share, 3)}</td><td className="num">{dec(item.shares, 1)}</td><td className="num">{money(item.cash)}</td>
      </tr>)}</tbody>
    </table>
    <p className="muted bt-footnote">Dividends are credited on their record dates while the holding is owned and kept as cash. As paid × split factor puts them on the split-adjusted share basis of the prices.</p>
  </div>
}

function HoldingPanel({ holding, index, detail, names, onClose }: { holding: HoldingDetail; index: number; detail: BacktestDetail; names?: Record<string, string>; onClose: () => void }) {
  const label = holdingLabel(holding.ticker, names?.[holding.ticker] ?? holding.name)
  return <section className="bt-holding-detail" aria-label={`${label} detail`}>
    <header>
      <h3><span className="bt-swatch" style={{ background: colorFor(index) }} aria-hidden="true" />{label}</h3>
      <Link className="text-button" to={`/analyze?ticker=${encodeURIComponent(holding.ticker)}&from=backtest`}>Analysis</Link>
      <button type="button" className="icon-button" aria-label="Close the holding" onClick={onClose}><X /></button>
    </header>
    <dl className="bt-facts">
      <div><dt>Weight at purchase</dt><dd>{pct(holding.weight)}</dd></div>
      <div><dt>Total return</dt><dd className={tone(holding.total_return)}>{pct(holding.total_return, 1, true)}</dd></div>
      <div><dt>Price · dividends</dt><dd>{pct(holding.price_return, 1, true)} · {pct(holding.dividend_return)}</dd></div>
      <div><dt>Contribution</dt><dd className={tone(holding.contribution)}>{pct(holding.contribution, 2, true)}</dd></div>
      <div><dt>Invested</dt><dd>{money(holding.capital_invested)}</dd></div>
      <div><dt>Dividends received</dt><dd>{money(holding.dividends_received)}</dd></div>
      <div><dt>End value</dt><dd>{money(holding.market_value)}</dd></div>
      <div><dt>Shares</dt><dd>{dec(holding.shares, 1)}</dd></div>
    </dl>
    <figure className="bt-panel">
      <figcaption>Growth with dividends kept as cash, against the whole portfolio</figcaption>
      <div className="bt-chart"><Line data={{ labels: detail.dates, datasets: [
        { label: holding.ticker, data: holding.growth.map(value => value == null ? null : asPercent(value - 1)), borderColor: colorFor(index), borderWidth: 2, pointRadius: 0 },
        { label: 'Portfolio', data: detail.portfolio.map(value => asPercent(value)), borderColor: BENCHMARK_COLOR, borderWidth: 1.2, pointRadius: 0 },
      ] }} options={percentOptions<'line'>({ xLabels: true })} /></div>
    </figure>
    <div className="bt-scroll">
      <table className="bt-table">
        <thead><tr><th>Year</th><th>From → to</th><th className="num">Start</th><th className="num">End</th><th className="num">Price</th><th className="num">Dividend</th><th className="num">Total</th><th className="num">Weight at start</th><th className="num">Contribution</th><th className="num">Dividend / share</th></tr></thead>
        <tbody>{holding.years.map(year => <tr key={year.year}>
          <th scope="row">{year.year}</th><td className="muted">{year.start_date ?? '—'} → {year.end_date ?? '—'}</td>
          <td className="num">{dec(year.start_price)}</td><td className="num">{dec(year.end_price)}</td>
          <td className={`num ${tone(year.price_return)}`}>{pct(year.price_return, 1, true)}</td><td className="num">{pct(year.dividend_return, 1, true)}</td>
          <td className="num" style={{ background: heatColor(year.total_return, 0.4), color: heatText(year.total_return, 0.4) }}>{pct(year.total_return, 1, true)}</td>
          <td className="num">{pct(year.start_weight)}</td>
          <td className={`num ${tone(year.contribution)}`}>{pct(year.contribution, 2, true)}</td><td className="num">{dec(year.dividend_per_share)}</td>
        </tr>)}</tbody>
      </table>
    </div>
    {holding.dividends.length > 0 && <details className="bt-details">
      <summary>{holding.dividends.length} dividend payments · {money(holding.dividends_received)} received</summary>
      <DividendLedger detail={{ ...detail, dividend_payments: holding.dividends }} names={names} />
    </details>}
  </section>
}

/** Below the portfolio totals: each holding's part, by year, over time, and every dividend. */
export function HoldingDrilldown({ id, names }: { id: string; names?: Record<string, string> }) {
  const detail = useQuery({
    queryKey: ['backtest-detail', id],
    queryFn: () => apiRequest<BacktestDetail>(`/api/backtesting/result/${encodeURIComponent(id)}/detail`),
    staleTime: Infinity,
    retry: false,
  })
  const [view, setView] = useState<View>('contribution')
  const [selected, setSelected] = useState<string | null>(null)
  const holdings = useMemo(() => detail.data?.holdings ?? [], [detail.data])
  if (detail.isLoading) return <LoadingState label="Breaking the backtest down by holding" />
  if (detail.isError || !detail.data) return <ErrorState error={detail.error} retry={() => detail.refetch()} />
  const index = holdings.findIndex(item => item.ticker === selected)
  const select = (ticker: string) => setSelected(current => current === ticker ? null : ticker)
  return <section className="bt-panel bt-panel--full bt-drill" aria-label="Holdings">
    <header className="bt-drill__head">
      <h3>Holdings · {holdings.length}</h3>
      <div className="bt-tabs" role="tablist" aria-label="Holding views">{VIEWS.map(([value, label]) => <button key={value} type="button" role="tab" aria-selected={view === value} className={view === value ? 'active' : ''} onClick={() => setView(value)}>{label}</button>)}</div>
      <span className="muted">Choose a holding for its growth, years, and dividends</span>
    </header>
    {view === 'contribution' && <>
      <figure className="bt-panel"><figcaption>Contribution to the total return · weight × the holding's total return</figcaption><ContributionBars holdings={holdings} names={names} selected={selected} onSelect={select} /></figure>
      <HoldingsTable holdings={holdings} names={names} selected={selected} onSelect={select} />
    </>}
    {view === 'years' && <YearsTable detail={detail.data} names={names} onSelect={select} />}
    {view === 'allocation' && <Allocation detail={detail.data} />}
    {view === 'dividends' && <DividendLedger detail={detail.data} names={names} />}
    {index >= 0 && <HoldingPanel holding={holdings[index]} index={index} detail={detail.data} names={names} onClose={() => setSelected(null)} />}
  </section>
}
