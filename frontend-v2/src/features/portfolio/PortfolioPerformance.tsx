import { useState, type ReactNode } from 'react'

import { LoadingState } from '../../components/Feedback'
import { Tip } from '../../components/Tooltip'
import { BENCHMARK_COLOR, PORTFOLIO_COLOR } from './chartTheme'
import { MonthlyReturnsTable, ReturnHistogram } from './PortfolioCharts'
import { compactMoney, decimal, formatDay, percent, signedPercent } from './portfolioFormat'
import { SectionCard } from './PortfolioPrimitives'
import type { Benchmark, ContributionData, Performance } from './portfolioTypes'

type Props = {
  performance?: Performance
  isLoading: boolean
  contribution?: ContributionData
  currency: string
  benchmarkLabel?: string
}

type Row = { label: string; tip: ReactNode; portfolio: ReactNode; benchmark?: ReactNode; note?: ReactNode }

function ComparisonTable({ caption, rows, benchmark }: { caption: string; rows: Row[]; benchmark?: Benchmark }) {
  return <table className="pf-compare">
    <caption className="sr-only">{caption}</caption>
    <thead><tr><th scope="col">Measure</th><th scope="col" className="num"><i style={{ background: PORTFOLIO_COLOR }} aria-hidden="true" />Portfolio</th>{benchmark && <th scope="col" className="num"><i style={{ background: BENCHMARK_COLOR }} aria-hidden="true" />{benchmark.ticker}</th>}</tr></thead>
    <tbody>{rows.map(row => <tr key={row.label}>
      <th scope="row"><Tip content={row.tip}>{row.label}</Tip>{row.note && <small>{row.note}</small>}</th>
      <td className="num">{row.portfolio}</td>
      {benchmark && <td className="num">{row.benchmark ?? <span className="muted">—</span>}</td>}
    </tr>)}</tbody>
  </table>
}

function Returns({ performance }: { performance?: Performance }) {
  const bench = performance?.benchmark?.available ? performance.benchmark : undefined
  const sameWindow = bench?.full_coverage !== false
  const inflation = performance?.inflation
  const rows: Row[] = [
    { label: 'Total return', tip: 'Time-weighted: each day’s return with deposits and withdrawals removed, chained over the period.', portfolio: signedPercent(performance?.total_return), benchmark: bench && signedPercent(bench.total_return) },
    { label: 'Annual return', tip: 'Compound annual growth of the total return. Not shown for periods under a year.', portfolio: performance?.annualized_return != null ? signedPercent(performance.annualized_return) : 'Under a year', benchmark: bench && (bench.annualized_return != null ? signedPercent(bench.annualized_return) : '—') },
    { label: 'Money-weighted return', tip: 'The annual internal rate of return of your deposits, withdrawals, and the closing value: what your money earned, including the timing of your deposits.', portfolio: performance?.money_weighted_return != null ? signedPercent(performance.money_weighted_return) : `${signedPercent(performance?.money_weighted_period_return)} over the period` },
    { label: 'From prices', tip: 'The time-weighted return with dividends taken out as they arrive.', portfolio: signedPercent(performance?.price_return) },
    { label: 'From dividends', tip: 'Total return less the price-only return: what reinvesting net dividends added.', portfolio: signedPercent(performance?.income_return) },
    { label: 'Return after inflation', tip: inflation ? `Total return deflated by consumer prices (${inflation.ticker}) over the same dates: (1 + return) ÷ (1 + inflation) − 1.` : 'No consumer price index is stored for this currency.', portfolio: signedPercent(performance?.return_attribution?.real_return), note: inflation ? `Prices ${signedPercent(inflation.total)}${inflation.estimated_months ? `, last ${inflation.estimated_months} mo. estimated` : ''}` : undefined },
    { label: 'Cash over the period', tip: 'What the short-term government rate would have earned over the same days.', portfolio: signedPercent(performance?.cash_return) },
    ...(bench ? [
      { label: 'Ahead of the benchmark', tip: `Your annual return relative to ${bench.ticker}'s over ${sameWindow ? 'the period' : 'the dates both have prices'}: (1 + yours) ÷ (1 + theirs) − 1.`, portfolio: bench.excess_return != null ? signedPercent(bench.excess_return) + ' a year' : `${signedPercent(bench.relative_return)} in total` },
    ] : []),
    { label: 'Positive months', tip: 'Calendar months with a gain, of all months in the period.', portfolio: performance?.months ? `${performance.positive_months} of ${performance.months}` : '—' },
  ]
  return <ComparisonTable caption="Returns" rows={rows} benchmark={bench} />
}

function Risk({ performance }: { performance?: Performance }) {
  const bench = performance?.benchmark?.available ? performance.benchmark : undefined
  const distribution = performance?.return_distribution
  const rows: Row[] = [
    { label: 'Volatility', tip: 'Standard deviation of weekday returns, annualized with √261.', portfolio: percent(performance?.volatility), benchmark: bench && percent(bench.volatility) },
    { label: 'Sharpe ratio', tip: 'Average return above cash, annualized, divided by volatility. Above 1 is strong over several years.', portfolio: decimal(performance?.sharpe_ratio), benchmark: bench && decimal(bench.sharpe_ratio) },
    { label: 'Sortino ratio', tip: 'Like Sharpe, but divides by downside deviation: only returns below cash count as risk.', portfolio: decimal(performance?.sortino_ratio) },
    { label: 'Max drawdown', tip: 'Largest fall from a previous high on the time-weighted path.', portfolio: percent(performance?.max_drawdown), benchmark: bench && percent(bench.max_drawdown), note: performance?.max_dd_peak_date ? `${formatDay(performance.max_dd_peak_date)} → ${formatDay(performance.max_dd_trough_date)}${performance.max_dd_recovery_date ? `, recovered ${formatDay(performance.max_dd_recovery_date)}` : ', not yet recovered'}` : undefined },
    { label: 'Calmar ratio', tip: 'Annual return divided by the size of the max drawdown. Needs at least a year.', portfolio: decimal(performance?.calmar_ratio) },
    ...(bench ? [
      { label: 'Beta', tip: `How much the portfolio moved with ${bench.ticker}: 1 moves in step, 0.5 half as much. From weekly returns above cash, because Tokyo, Europe, and New York close at different hours.`, portfolio: decimal(bench.beta) },
      { label: 'Correlation', tip: 'How closely weekly returns moved together (1 is lockstep).', portfolio: decimal(bench.correlation) },
      { label: 'Alpha', tip: 'Annual return above what the beta to the benchmark explains (Jensen’s alpha, weekly).', portfolio: bench.alpha != null ? signedPercent(bench.alpha) : '—' },
      { label: 'Tracking error', tip: 'Annualized volatility of the weekly difference from the benchmark.', portfolio: percent(bench.tracking_error) },
      { label: 'Information ratio', tip: 'Average weekly lead over the benchmark, annualized, per unit of tracking error.', portfolio: decimal(bench.information_ratio) },
      { label: 'Up / down capture', tip: 'Average monthly return in the benchmark’s rising months (and falling months) as a share of the benchmark’s. Below 100% down capture means smaller falls.', portfolio: `${percent(bench.up_capture, 0)} / ${percent(bench.down_capture, 0)}` },
    ] : []),
    { label: 'Value at risk (95%)', tip: 'One weekday in twenty did worse than this (historical 5th percentile). Not a forecast.', portfolio: percent(performance?.var_95) },
    { label: 'Expected shortfall', tip: 'The average of those worst one-in-twenty weekdays (conditional VaR).', portfolio: percent(performance?.cvar_95) },
    { label: 'Best / worst day', tip: 'The largest weekday gain and loss.', portfolio: `${signedPercent(distribution?.max)} / ${signedPercent(distribution?.min)}`, note: distribution?.best_day_date ? `${formatDay(distribution.best_day_date)} / ${formatDay(distribution.worst_day_date)}` : undefined },
    { label: 'Days up', tip: 'Weekdays with a gain, of the days that moved.', portfolio: percent(performance?.win_rate) },
  ]
  return <ComparisonTable caption="Risk" rows={rows} benchmark={bench} />
}

function Contribution({ data, currency }: { data?: ContributionData; currency: string }) {
  const years = data?.years ?? []
  const [chosen, setChosen] = useState<number | null>(null)
  const yearIndex = chosen != null && years.includes(chosen) ? years.indexOf(chosen) : years.length - 1
  if (yearIndex < 0) return <p className="portfolio-empty-copy">Contribution history is not available.</p>
  const rows = Object.entries(data?.companies ?? {}).map(([symbol, values]) => ({ symbol, amount: values.contribution_eur[yearIndex], share: values.contribution_pct[yearIndex] }))
    .filter(row => row.amount != null && Math.abs(Number(row.amount)) >= 0.5)
    .sort((left, right) => Number(right.amount) - Number(left.amount))
  const shown = rows.length > 12 ? [...rows.slice(0, 6), ...rows.slice(-6)] : rows
  const bound = Math.max(...shown.map(row => Math.abs(Number(row.amount))), 1)
  return <>
    <div className="period-tabs" role="group" aria-label="Contribution year">{years.slice(-6).map(year => <button key={year} type="button" className={`period-tab${years[yearIndex] === year ? ' active' : ''}`} aria-pressed={years[yearIndex] === year} onClick={() => setChosen(year)}>{year}</button>)}</div>
    <ol className="pf-diverging">{shown.map(row => {
      const amount = Number(row.amount)
      return <li key={row.symbol}>
        <strong>{row.symbol}</strong>
        <span className="pf-diverging__track" aria-hidden="true"><i className={amount >= 0 ? 'is-gain' : 'is-loss'} style={{ width: `${Math.max(1, Math.abs(amount) / bound * 50)}%` }} /></span>
        <b className={amount < 0 ? 'number-negative' : undefined}>{amount >= 0 ? '+' : '−'}{compactMoney(Math.abs(amount), currency)}<small>{row.share != null ? `${row.share >= 0 ? '+' : '−'}${Math.abs(row.share).toFixed(1)} pts` : ''}</small></b>
      </li>
    })}</ol>
    {rows.length > shown.length && <p className="pf-footnote">Largest six gains and losses of {rows.length} holdings.</p>}
  </>
}

export function PortfolioPerformance(props: Props) {
  const { performance } = props
  if (props.isLoading) return <LoadingState label="Calculating returns" />
  const bench = performance?.benchmark?.available ? performance.benchmark : undefined
  return <div className="portfolio-section-stack">
    {(performance?.warnings ?? []).length > 0 && <ul className="pf-notes">{performance!.warnings!.map(warning => <li key={warning.code}>{warning.message}</li>)}</ul>}
    <div className="pf-performance-grid">
      <SectionCard title="Returns" description={performance?.period ? `${formatDay(performance.period.start)} to ${formatDay(performance.period.end)} in ${performance.base_currency}` : undefined}><Returns performance={performance} /></SectionCard>
      <SectionCard title="Risk" description={bench ? `Against ${bench.ticker}${bench.full_coverage === false ? ` from ${formatDay(bench.coverage_start)}` : ''}` : 'Choose a benchmark (B) to compare'}><Risk performance={performance} /></SectionCard>
    </div>
    <SectionCard title="Monthly returns" description="Time-weighted, by calendar month; the last columns are the year in total">
      <MonthlyReturnsTable monthly={performance?.monthly_returns ?? []} annual={performance?.annual_returns ?? []} benchmarkLabel={props.benchmarkLabel} />
    </SectionCard>
    <div className="pf-performance-grid">
      <SectionCard title="Daily returns" description="How many weekdays fell in each half-percent band; the outer bars collect everything beyond ±4%">
        <ReturnHistogram series={performance?.series ?? []} />
      </SectionCard>
      <SectionCard title="Contribution by holding" description="Change in value after net purchases, dividends included, for the calendar year">
        <Contribution data={props.contribution} currency={props.currency} />
      </SectionCard>
    </div>
  </div>
}
