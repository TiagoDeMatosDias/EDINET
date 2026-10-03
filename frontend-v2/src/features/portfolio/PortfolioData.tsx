import { AlertCircle, AlertTriangle, CheckCircle2, CloudDownload, Info, RefreshCw } from 'lucide-react'
import { useState } from 'react'

import { LoadingState } from '../../components/Feedback'
import { formatDay, formatMonth, percent } from './portfolioFormat'
import { DetailList, SectionCard } from './PortfolioPrimitives'
import { PortfolioTable, type TableColumn } from './PortfolioTable'
import type { BenchmarkChoice, DataIssue, DataQuality, HoldingDataStatus, Performance } from './portfolioTypes'

type Props = {
  quality?: DataQuality
  isLoading: boolean
  performance?: Performance
  benchmarks: BenchmarkChoice[]
  currency: string
  canRefresh: boolean
  busy: boolean
  riskFreeOverride: number | null
  onRiskFreeOverride: (value: number | null) => void
  onRefresh: () => void
  onRebuild: () => void
}

const ISSUE_ICON = { error: AlertCircle, warning: AlertTriangle, info: Info }

function Issues({ issues }: { issues: DataIssue[] }) {
  if (!issues.length) return <p className="pf-issue is-ok"><CheckCircle2 aria-hidden="true" />Every holding has a current market price in its own currency, and the exchange, interest-rate, and inflation series are up to date.</p>
  const order = { error: 0, warning: 1, info: 2 }
  return <ul className="pf-issues">{[...issues].sort((a, b) => order[a.level] - order[b.level]).map(issue => {
    const Icon = ISSUE_ICON[issue.level]
    return <li key={`${issue.code}-${issue.message}`} className={`pf-issue is-${issue.level}`}><Icon aria-label={issue.level} /><span>{issue.message}</span></li>
  })}</ul>
}

const STATUS_TEXT: Record<HoldingDataStatus['status'], string> = { ok: 'Current', stale: 'Stale', cost: 'At cost', missing: 'No price' }

const COLUMNS: TableColumn<HoldingDataStatus>[] = [
  { id: 'symbol', header: 'Holding', rowHeader: true, sortValue: row => row.symbol, cell: row => <strong>{row.symbol}</strong> },
  { id: 'weight', header: 'Weight', numeric: true, sortValue: row => row.weight, cell: row => percent(row.weight) },
  { id: 'status', header: 'Status', sortValue: row => row.status, cell: row => <span className={`pf-badge is-${row.status}`}>{STATUS_TEXT[row.status]}</span> },
  { id: 'date', header: 'Price used', sortFirst: 'desc', tip: 'The date of the close that values the holding on the valuation date.', sortValue: row => row.price_date, cell: row => row.price_source === 'cost' ? 'Average cost' : formatDay(row.price_date) },
  { id: 'age', header: 'Age', numeric: true, tip: 'Days between that close and the valuation date.', sortValue: row => row.stale_days, cell: row => row.stale_days == null ? '—' : `${row.stale_days} d` },
  { id: 'source', header: 'Quote', tip: 'The stored ticker and the currency its prices are in.', sortValue: row => row.price_ticker, cell: row => row.price_ticker ? <span>{row.price_ticker} · {row.quote_currency ?? '?'}{row.converted && <small> → {row.currency}</small>}</span> : '—' },
  { id: 'history', header: 'History', tip: 'Years with only weekly prices make daily statistics approximate there.', cell: row => row.weekly_years.length ? `Weekly ${row.weekly_years[0]}–${row.weekly_years[row.weekly_years.length - 1]}` : 'Daily' },
  { id: 'latest', header: 'Latest stored', tip: 'The newest close in the price database; refreshing prices extends it.', sortValue: row => row.latest_stored_price, cell: row => formatDay(row.latest_stored_price) },
]

function RiskFreeInput({ value, onChange }: { value: number | null; onChange: (value: number | null) => void }) {
  const [draft, setDraft] = useState(value == null ? '' : String(+(value * 100).toFixed(3)))
  const apply = () => {
    const parsed = Number(draft.replace(',', '.'))
    onChange(draft.trim() === '' || !Number.isFinite(parsed) ? null : parsed / 100)
  }
  return <form className="pf-inline-form" onSubmit={event => { event.preventDefault(); apply() }}>
    <label>Use my own rate <input className="input" inputMode="decimal" value={draft} placeholder="e.g. 2.5" aria-label="Risk-free rate override in percent" onChange={event => setDraft(event.target.value)} /> %</label>
    <button type="submit" className="button button--secondary button--small">Apply</button>
    {value != null && <button type="button" className="button button--ghost button--small" onClick={() => { setDraft(''); onChange(null) }}>Use the stored series</button>}
  </form>
}

const METHOD = [
  { label: 'Valuation', value: 'Every calendar day from the first transaction: each holding at its latest close on or before the day (as traded, in its own currency), cash per currency, all converted at that day’s ECB euro reference rate.', tip: 'Quotes from another listing (CSPX from London in USD) are converted to the holding’s currency first. A holding without any stored price is valued at its average cost until one exists.' },
  { label: 'Daily return', value: '(Vₜ − Vₜ₋₁ − Fₜ) ÷ (Vₜ₋₁ + Fₜ), with Fₜ the day’s deposits less withdrawals, so money moving in or out is never a gain or a loss.' },
  { label: 'Total return', value: 'The daily returns chained over the period (time-weighted). The period runs from the close of its first day.' },
  { label: 'Annual return', value: '(1 + total return)^(365.25 ÷ days) − 1; not shown for periods under a year, where it would extrapolate.' },
  { label: 'Money-weighted return', value: 'The annual rate that discounts the opening value, every deposit and withdrawal, and the closing value to zero (XIRR).' },
  { label: 'Price and dividend parts', value: 'The price part removes net dividends on the day they arrive; the dividend part is the total less the price part.' },
  { label: 'Volatility', value: 'Standard deviation of weekday returns × √261. Weekend days carry into Monday, as no market trades then.' },
  { label: 'Sharpe ratio', value: 'Mean daily return above cash ÷ standard deviation of daily returns × √261. Cash accrues the short-term rate over calendar days.' },
  { label: 'Sortino ratio', value: 'Mean daily return above cash ÷ downside deviation (root mean square of shortfalls below cash) × √261.' },
  { label: 'Drawdown', value: 'Each day’s distance below the highest earlier point of the time-weighted path.' },
  { label: 'Beta, alpha, correlation', value: 'Regression of weekly portfolio returns above cash on the benchmark’s; weekly because markets in Tokyo, Europe, and New York close hours apart.' },
  { label: 'Tracking error and information ratio', value: 'Annualized volatility of the weekly difference from the benchmark (× √52), and the average difference per unit of it.' },
  { label: 'Value at risk', value: 'Historical: the 5th percentile of weekday returns; expected shortfall is the average below it.' },
  { label: 'Return after inflation', value: '(1 + return) ÷ (1 + consumer-price change) − 1, using the display currency’s monthly index; months not yet published grow at the trailing twelve-month rate.' },
]

export function PortfolioData(props: Props) {
  const { quality, performance } = props
  if (props.isLoading) return <LoadingState label="Checking the data" />
  if (!quality) return <p className="portfolio-empty-copy">Data checks are not available.</p>
  const riskFree = performance?.risk_free
  const bench = performance?.benchmark
  const inflation = performance?.inflation
  const benchChoice = props.benchmarks.find(choice => choice.ticker === bench?.ticker)
  return <div className="portfolio-section-stack">
    <SectionCard
      title="What the figures rest on"
      description={quality.valuation_date ? `Valued as of ${formatDay(quality.valuation_date)} · activity to ${formatDay(quality.last_transaction)} · checked ${formatDay(quality.today)}` : 'Not valued yet'}
      actions={<div className="card-action-row">
        {props.canRefresh && <button type="button" className="button button--secondary button--small" disabled={props.busy} onClick={props.onRefresh} title="Shift+R"><CloudDownload />Refresh prices</button>}
        <button type="button" className="button button--ghost button--small" disabled={props.busy} onClick={props.onRebuild} title="R"><RefreshCw />Rebuild</button>
      </div>}
    >
      <Issues issues={quality.issues} />
      {!props.canRefresh && quality.issues.some(issue => issue.level !== 'info') && <p className="pf-footnote">Refreshing prices writes shared market data, so it needs an operator or administrator.</p>}
    </SectionCard>

    <SectionCard title="Prices behind each holding" description="The quote that values each open holding">
      <PortfolioTable label="Price sources" rows={quality.holdings} columns={COLUMNS} rowKey={row => row.symbol} initialSort={{ column: 'weight', direction: 'desc' }} hotkeys={false} rowClassName={row => row.status !== 'ok' ? `is-${row.status}` : undefined} />
    </SectionCard>

    <div className="pf-performance-grid">
      <SectionCard title="Rates and indexes" description={`For the ${props.currency} view of the selected period`}>
        <DetailList rows={[
          { label: 'Exchange rates', value: quality.fx.source, detail: Object.entries(quality.fx.last_dates).map(([code, last]) => `${code} to ${formatDay(last)}`).join(' · ') || 'Everything is in euros', tip: 'Every value and flow is converted at the reference rate of its own day.' },
          { label: 'Cash (risk-free) rate', value: riskFree?.kind === 'override' ? `Your rate: ${percent(props.riskFreeOverride, 2)}` : riskFree?.source ?? quality.risk_free.source ?? 'None', detail: riskFree?.kind === 'series' ? `Averaged ${percent(performance?.risk_free_rate, 2)} a year over the period · data to ${formatDay(riskFree.last_date)}` : riskFree?.kind === 'missing' ? `No ${props.currency} rate stored; Sharpe and Sortino assume 0%` : undefined, tip: 'The return of a short-term government bill or overnight deposit in the display currency; Sharpe and Sortino measure returns above it.' },
          { label: 'Consumer prices', value: inflation ? inflation.ticker.replace('Inflation_', '') + ' index' : 'None stored', detail: inflation ? `Published to ${formatMonth(inflation.last_observation)}${inflation.estimated_months ? `; ${inflation.estimated_months} later month(s) estimated at ${percent(inflation.trailing_annual_rate)} a year` : ''}` : undefined },
          { label: 'Benchmark', value: bench ? `${bench.ticker}${benchChoice ? ` · ${benchChoice.label}` : ''}` : 'None', detail: bench?.available ? `${benchChoice?.detail ?? ''}${benchChoice?.detail ? ' · ' : ''}${bench.price_currency ?? ''} prices converted at ECB rates · ${formatDay(bench.coverage_start)} to ${formatDay(bench.coverage_end)}` : bench?.message, tip: 'Funds that reinvest dividends (accumulating) make a fair total-return benchmark; for funds that pay them out the price leaves dividends out.' },
        ]} />
        <RiskFreeInput key={props.riskFreeOverride ?? 'series'} value={props.riskFreeOverride} onChange={props.onRiskFreeOverride} />
      </SectionCard>
      <SectionCard title="Unusual days" description="Whole-portfolio moves of 8% or more usually mean a bad quote or a missing transaction">
        {quality.large_moves.length ? <ul className="pf-moves">{quality.large_moves.map(move => <li key={move.date}><span>{formatDay(move.date)}</span><strong className={move.return < 0 ? 'number-negative' : undefined}>{move.return > 0 ? '+' : '−'}{Math.abs(move.return * 100).toFixed(1)}%</strong></li>)}</ul> : <p className="pf-issue is-ok"><CheckCircle2 aria-hidden="true" />No day moved the portfolio by 8% or more.</p>}
      </SectionCard>
    </div>

    <SectionCard title="How each figure is calculated" description="One daily return series feeds every statistic on this page">
      <DetailList rows={METHOD} />
    </SectionCard>
  </div>
}
