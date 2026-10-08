import { AlertTriangle } from 'lucide-react'
import { useMemo, useState } from 'react'
import { Bar, Line } from 'react-chartjs-2'

import { calendarYears, dec, finite, monthlyReturns, heatColor, heatText, pct, tone, type SingleResult } from './backtestModel'
import { asPercent, BENCHMARK_COLOR, NEGATIVE_COLOR, PORTFOLIO_COLOR, percentOptions } from './charts'
import { HoldingDrilldown } from './HoldingDrilldown'
import { ReportActions } from './ReportActions'

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
      <ReportActions id={data.id} />
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
        <HoldingDrilldown id={data.id} names={data.names} />
      </div>
    </>}
  </section>
}
