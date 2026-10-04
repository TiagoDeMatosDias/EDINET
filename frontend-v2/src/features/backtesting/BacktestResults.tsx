import { AlertTriangle } from 'lucide-react'
import { useMemo, useState } from 'react'
import { Bar, Line } from 'react-chartjs-2'
import { Link } from 'react-router-dom'

import { DownloadButton } from '../../components/DownloadButton'
import { calendarYears, dec, finite, monthlyReturns, heatColor, heatText, pct, tone, type SingleResult } from './backtestModel'
import { asPercent, BENCHMARK_COLOR, NEGATIVE_COLOR, PORTFOLIO_COLOR, percentOptions } from './charts'

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

export function Kpi({ label, value, detail, className }: { label: string; value: string; detail?: string; className?: string }) {
  return <div className={`bt-kpi ${className ?? ''}`}><span>{label}</span><strong>{value}</strong>{detail && <small>{detail}</small>}</div>
}

export function Warnings({ items }: { items: string[] }) {
  const [open, setOpen] = useState(false)
  if (!items.length) return null
  const shown = open ? items : items.slice(0, 2)
  return <div className="bt-warnings" role="status">
    <AlertTriangle aria-hidden="true" />
    <ul>{shown.map((item, index) => <li key={index}>{item}</li>)}</ul>
    {items.length > 2 && <button type="button" className="text-button" onClick={() => setOpen(value => !value)}>{open ? 'Fewer' : `${items.length - 2} more`}</button>}
  </div>
}

type SortKey = 'Ticker' | 'weight' | 'price_return' | 'dividend_return' | 'total_return' | 'weighted_total'

function HoldingsTable({ rows }: { rows: Array<Record<string, unknown>> }) {
  const [sort, setSort] = useState<{ key: SortKey; desc: boolean }>({ key: 'weighted_total', desc: true })
  const sorted = useMemo(() => [...rows].sort((a, b) => {
    const left = sort.key === 'Ticker' ? String(a.Ticker ?? '') : finite(a[sort.key]) ?? -Infinity
    const right = sort.key === 'Ticker' ? String(b.Ticker ?? '') : finite(b[sort.key]) ?? -Infinity
    const order = typeof left === 'string' ? left.localeCompare(String(right)) : (left as number) - (right as number)
    return sort.desc ? -order : order
  }), [rows, sort])
  const header = (key: SortKey, label: string, numeric = true) => <th className={numeric ? 'num' : ''} aria-sort={sort.key === key ? (sort.desc ? 'descending' : 'ascending') : 'none'}>
    <button type="button" onClick={() => setSort(current => ({ key, desc: current.key === key ? !current.desc : numeric }))}>{label}{sort.key === key ? (sort.desc ? ' ↓' : ' ↑') : ''}</button>
  </th>
  return <div className="bt-scroll">
    <table className="bt-table">
      <thead><tr>{header('Ticker', 'Holding', false)}{header('weight', 'Weight')}<th className="num">Start → end</th>{header('price_return', 'Price')}{header('dividend_return', 'Dividend')}{header('total_return', 'Total')}{header('weighted_total', 'Contribution')}</tr></thead>
      <tbody>{sorted.map(row => {
        const ticker = String(row.Ticker ?? '')
        return <tr key={ticker}>
          <td><Link to={`/analyze?ticker=${encodeURIComponent(ticker)}&from=backtest`}>{ticker}</Link><small>{String(row.Currency ?? '')}</small></td>
          <td className="num">{pct(row.weight)}</td>
          <td className="num muted">{dec(row.start_price, 0)} → {dec(row.end_price, 0)}</td>
          <td className={`num ${tone(row.price_return)}`}>{pct(row.price_return, 1, true)}</td>
          <td className="num">{pct(row.dividend_return)}</td>
          <td className={`num ${tone(row.total_return)}`}>{pct(row.total_return, 1, true)}</td>
          <td className={`num ${tone(row.weighted_total)}`}>{pct(row.weighted_total, 2, true)}</td>
        </tr>
      })}</tbody>
    </table>
  </div>
}

function MonthlyTable({ points }: { points: NonNullable<SingleResult['chart_data']>['cumulative'] }) {
  const rows = useMemo(() => monthlyReturns(points ?? []), [points])
  if (!rows.length) return <p className="muted bt-empty">No monthly data.</p>
  return <div className="bt-scroll">
    <table className="bt-table bt-heat">
      <thead><tr><th>Year</th>{MONTHS.map(name => <th key={name} className="num">{name}</th>)}<th className="num">Year</th></tr></thead>
      <tbody>{rows.map(row => <tr key={row.year}>
        <th scope="row">{row.year}</th>
        {row.months.map((value, index) => <td key={index} className="num" style={{ background: heatColor(value, 0.1), color: heatText(value, 0.1) }}>{value == null ? '' : (value * 100).toFixed(1)}</td>)}
        <td className="num" style={{ background: heatColor(row.total, 0.4), color: heatText(row.total, 0.4) }}><b>{(row.total * 100).toFixed(1)}</b></td>
      </tr>)}</tbody>
    </table>
  </div>
}

/** One portfolio over one period: headline numbers, charts, and holdings. */
export function BacktestResults({ data }: { data: SingleResult }) {
  const summary = data.summary
  const charts = data.chart_data ?? {}
  const cumulative = useMemo(() => charts.cumulative ?? [], [charts.cumulative])
  const drawdown = charts.drawdown ?? []
  const years = useMemo(() => calendarYears(cumulative), [cumulative])
  const hasBenchmark = finite(summary.benchmark_total_return) != null
  const warnings = [...new Set([...(data.warnings ?? []), ...((summary.warnings as string[] | undefined) ?? [])])]
  const sampled = cumulative.filter((_, index) => index % Math.max(1, Math.floor(cumulative.length / 600)) === 0 || index === cumulative.length - 1)
  const sampledDrawdown = drawdown.filter((_, index) => index % Math.max(1, Math.floor(drawdown.length / 600)) === 0 || index === drawdown.length - 1)
  const compared = summary.comparison_start && summary.comparison_end && (summary.comparison_start !== summary.start_date || summary.comparison_end !== summary.end_date)
    ? `compared ${String(summary.comparison_start)} → ${String(summary.comparison_end)}` : undefined

  return <section className="bt-result" aria-label="Backtest result">
    <header className="bt-result__head">
      <h2>{(summary.tickers as string[] | undefined)?.slice(0, 6).join(', ') || 'Backtest'}{((summary.tickers as string[] | undefined)?.length ?? 0) > 6 ? ` +${(summary.tickers as string[]).length - 6}` : ''}</h2>
      <span className="muted">{String(summary.start_date ?? '—')} → {String(summary.end_date ?? '—')} · saved {data.id}</span>
      <DownloadButton className="button button--ghost button--small" path={`/api/backtesting/download/${encodeURIComponent(data.id)}`} filename={`backtest_${data.id}.zip`}>Download</DownloadButton>
    </header>
    <Warnings items={warnings} />
    {summary.no_data ? <p className="bt-empty">No prices were found for these holdings in this period.</p> : <>
      <div className="bt-kpis">
        <Kpi label="Total return" value={pct(summary.total_return, 1, true)} className={tone(summary.total_return)} />
        <Kpi label="Annualized" value={pct(summary.annualized_return, 2, true)} className={tone(summary.annualized_return)} />
        <Kpi label="Benchmark ann." value={pct(summary.benchmark_annualized_return, 2, true)} detail={hasBenchmark ? `total ${pct(summary.benchmark_total_return, 1, true)}` : 'none'} />
        <Kpi label="Excess ann." value={pct(summary.excess_annualized_return, 2, true)} className={tone(summary.excess_annualized_return)} detail={compared ?? (hasBenchmark ? `total ${pct(summary.excess_return, 1, true)}` : undefined)} />
        <Kpi label="Volatility" value={pct(summary.volatility)} detail={hasBenchmark ? `bench ${pct(summary.benchmark_volatility)}` : undefined} />
        <Kpi label="Sharpe" value={dec(summary.sharpe_ratio)} detail={hasBenchmark ? `bench ${dec(summary.benchmark_sharpe_ratio)}` : undefined} />
        <Kpi label="Max drawdown" value={pct(summary.max_drawdown)} className="is-down" detail={hasBenchmark ? `bench ${pct(summary.benchmark_max_drawdown)}` : undefined} />
        <Kpi label="Tracking error" value={pct(summary.tracking_error)} />
        <Kpi label="Information ratio" value={dec(summary.information_ratio)} className={tone(summary.information_ratio)} />
        <Kpi label="Price · dividend" value={`${pct(summary.price_return, 1, true)} · ${pct(summary.dividend_return)}`} />
        <Kpi label="Initial capital" value={finite(summary.initial_capital)?.toLocaleString() ?? '—'} />
      </div>
      <div className="bt-grid">
        <figure className="bt-panel bt-panel--wide">
          <figcaption>Cumulative return{hasBenchmark ? ' vs benchmark' : ''}</figcaption>
          <div className="bt-chart"><Line data={{ labels: sampled.map(point => point.date), datasets: [
            { label: 'Portfolio', data: sampled.map(point => asPercent(point.portfolio)), borderColor: PORTFOLIO_COLOR, borderWidth: 1.5, pointRadius: 0 },
            ...(hasBenchmark ? [{ label: 'Benchmark', data: sampled.map(point => asPercent(point.benchmark)), borderColor: BENCHMARK_COLOR, borderWidth: 1.2, borderDash: [4, 3], pointRadius: 0, spanGaps: true }] : []),
          ] }} options={percentOptions<'line'>({ xLabels: true })} /></div>
        </figure>
        <figure className="bt-panel">
          <figcaption>Drawdown</figcaption>
          <div className="bt-chart"><Line data={{ labels: sampledDrawdown.map(point => point.date), datasets: [
            { label: 'Portfolio', data: sampledDrawdown.map(point => asPercent(point.portfolio ?? 0)), borderColor: NEGATIVE_COLOR, backgroundColor: `${NEGATIVE_COLOR}26`, fill: true, borderWidth: 1, pointRadius: 0 },
            ...(hasBenchmark ? [{ label: 'Benchmark', data: sampledDrawdown.map(point => asPercent(point.benchmark)), borderColor: BENCHMARK_COLOR, borderWidth: 1, pointRadius: 0, spanGaps: true }] : []),
          ] }} options={percentOptions<'line'>({ yMax: 0 })} /></div>
        </figure>
        <figure className="bt-panel">
          <figcaption>Calendar years</figcaption>
          <div className="bt-chart"><Bar data={{ labels: years.map(row => row.year), datasets: [
            { label: 'Portfolio', data: years.map(row => asPercent(row.portfolio)), backgroundColor: years.map(row => (row.portfolio ?? 0) >= 0 ? PORTFOLIO_COLOR : NEGATIVE_COLOR) },
            ...(hasBenchmark ? [{ label: 'Benchmark', data: years.map(row => asPercent(row.benchmark)), backgroundColor: `${BENCHMARK_COLOR}88` }] : []),
          ] }} options={percentOptions<'bar'>({ xLabels: true })} /></div>
        </figure>
        <figure className="bt-panel bt-panel--wide">
          <figcaption>Monthly returns, %</figcaption>
          <MonthlyTable points={cumulative} />
        </figure>
        <figure className="bt-panel bt-panel--full">
          <figcaption>Holdings · {data.per_company?.length ?? 0}</figcaption>
          <HoldingsTable rows={data.per_company ?? []} />
        </figure>
      </div>
    </>}
  </section>
}
