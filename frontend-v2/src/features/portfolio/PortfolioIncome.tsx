import { Card, Metric } from '../../components/Page'
import { DividendGrowthChart, DividendsByCompanyChart, DividendsByCurrencyChart } from './PortfolioCharts'
import { money, percent } from './portfolioFormat'
import { ExploreButton } from './PortfolioPrimitives'
import type { DividendCurrencyHistory, DividendGrowthData, DividendHistory, Performance, PortfolioDetail } from './portfolioTypes'

type Period = 'monthly' | 'quarterly' | 'yearly'

type Props = {
  performance?: Performance
  byCurrency?: DividendCurrencyHistory
  byCompany?: DividendHistory
  growth?: DividendGrowthData
  currency: string
  period: Period
  onPeriod: (period: Period) => void
  onOpenDetail: (detail: PortfolioDetail) => void
}

function IncomeSummary({ performance, byCompany, currency }: Pick<Props, 'performance' | 'byCompany' | 'currency'>) {
  const breakdown = performance?.dividend_breakdown
  const gross = Number(breakdown?.total_gross ?? 0)
  const tax = Math.abs(Number(breakdown?.total_tax ?? 0))
  const payers = Object.keys(byCompany?.companies ?? {}).length
  return <div className="portfolio-section-metrics">
    <Metric label="Gross income" value={money(gross, currency)} />
    <Metric label="Withholding tax" value={money(tax, currency)} detail={gross ? percent(tax / gross) : '—'} />
    <Metric label="Net income" value={money(breakdown?.total_net, currency)} />
    <Metric label="Return contribution" value={percent(performance?.return_attribution?.dividend_yield)} />
    <Metric label="Income payers" value={payers.toLocaleString()} />
    <Metric label="Capital appreciation" value={percent(performance?.return_attribution?.capital_appreciation)} />
  </div>
}

function TopPayers({ data, currency }: { data?: DividendHistory; currency: string }) {
  const rows = Object.entries(data?.companies ?? {}).map(([symbol, values]) => ({
    symbol,
    total: values.reduce((sum, value) => sum + value, 0),
  })).sort((left, right) => right.total - left.total).slice(0, 10)
  const total = rows.reduce((sum, row) => sum + row.total, 0)
  return <div className="income-payer-list">{rows.map(row => <div key={row.symbol}>
    <span><strong>{row.symbol}</strong><small>{money(row.total, currency)}</small></span>
    <div><i style={{ width: `${Math.max(2, total ? row.total / total * 100 : 0)}%` }} /></div>
    <b>{percent(total ? row.total / total : 0)}</b>
  </div>)}</div>
}

function PeriodControl({ value, onChange }: { value: Period; onChange: (period: Period) => void }) {
  return <div className="period-tabs" aria-label="Income aggregation">{(['monthly', 'quarterly', 'yearly'] as const).map(period => <button key={period} className={`period-tab${value === period ? ' active' : ''}`} onClick={() => onChange(period)}>{period}</button>)}</div>
}

export function PortfolioIncome(props: Props) {
  return <div className="portfolio-section-stack">
    <IncomeSummary performance={props.performance} byCompany={props.byCompany} currency={props.currency} />
    <Card title="Income through time" description="Net dividends after withholding, grouped by payment currency" actions={<div className="card-action-row"><PeriodControl value={props.period} onChange={props.onPeriod} /><ExploreButton label="Income details" onClick={() => props.onOpenDetail({ kind: 'income' })} /></div>}><DividendsByCurrencyChart data={props.byCurrency} /></Card>
    <div className="portfolio-income-grid">
      <Card title="Largest income sources" description="Top ten payers across imported history" actions={<ExploreButton label="All payers" onClick={() => props.onOpenDetail({ kind: 'income' })} />}><TopPayers data={props.byCompany} currency={props.currency} /></Card>
      <Card title="Annual income by company" description="Top ten payers, stacked by year" actions={<ExploreButton label="Income details" onClick={() => props.onOpenDetail({ kind: 'income' })} />}><DividendsByCompanyChart data={props.byCompany} /></Card>
    </div>
    <Card title="Dividend-per-share growth" description="Largest current holdings plus the market-value-weighted portfolio average" actions={<ExploreButton label="Growth details" onClick={() => props.onOpenDetail({ kind: 'income' })} />}><DividendGrowthChart data={props.growth} /></Card>
  </div>
}
