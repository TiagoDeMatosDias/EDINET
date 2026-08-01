import { Card, Metric } from '../../components/Page'
import { CostReturnChart, ReturnHeatmap } from './PortfolioCharts'
import { PortfolioAdvancedAnalytics } from './PortfolioAdvancedAnalytics'
import { decimal, money, percent, percentPoints } from './portfolioFormat'
import { ExploreButton, StatButton } from './PortfolioPrimitives'
import type { ContributionData, HeatmapData, Holding, Performance, PieData, PortfolioDetail, ScatterPoint, ValueHistory } from './portfolioTypes'

type Props = {
  performance?: Performance
  valueHistory?: ValueHistory
  heatmap?: HeatmapData
  scatter?: ScatterPoint[]
  contribution?: ContributionData
  allocation?: PieData
  holdings: Holding[]
  currency: string
  onOpenDetail: (detail: PortfolioDetail) => void
}

function ReturnComposition({ performance, onOpenDetail }: Pick<Props, 'performance' | 'onOpenDetail'>) {
  const attribution = performance?.return_attribution
  return <Card title="Return composition" description="Nominal, real, capital, and income contribution" actions={<ExploreButton label="Return details" onClick={() => onOpenDetail({ kind: 'performance' })} />}>
    <div className="performance-metric-grid">
      <StatButton label="Total return" value={percent(performance?.total_return)} detail="Time-weighted" onClick={() => onOpenDetail({ kind: 'performance' })} tone={Number(performance?.total_return) >= 0 ? 'positive' : 'negative'} />
      <StatButton label="Annualized" value={percent(performance?.annualized_return)} detail={`${performance?.start_date ?? '—'} to ${performance?.end_date ?? '—'}`} onClick={() => onOpenDetail({ kind: 'performance' })} tone={Number(performance?.annualized_return) >= 0 ? 'positive' : 'negative'} />
      <StatButton label="Real return" value={percent(attribution?.real_return)} detail={`${percent(attribution?.inflation_total)} inflation`} onClick={() => onOpenDetail({ kind: 'performance' })} />
      <StatButton label="Dividend contribution" value={percent(attribution?.dividend_yield)} detail={`${percent(attribution?.capital_appreciation)} capital`} onClick={() => onOpenDetail({ kind: 'income' })} />
    </div>
  </Card>
}

function RiskScorecard({ performance, onOpenDetail }: Pick<Props, 'performance' | 'onOpenDetail'>) {
  return <Card title="Risk scorecard" description="Downside, consistency, and tail-risk measures" actions={<ExploreButton label="Risk details" onClick={() => onOpenDetail({ kind: 'risk' })} />}>
    <div className="risk-score-grid">
      <Metric label="Volatility" value={percent(performance?.volatility)} />
      <Metric label="Max drawdown" value={percent(performance?.max_drawdown)} detail={`${performance?.max_dd_peak_date ?? '—'} → ${performance?.max_dd_trough_date ?? '—'}`} />
      <Metric label="VaR 95%" value={percent(performance?.var_95)} detail="Historical daily" />
      <Metric label="CVaR 95%" value={percent(performance?.cvar_95)} detail="Average tail loss" />
      <Metric label="Sharpe" value={decimal(performance?.sharpe_ratio)} detail={`${percent(performance?.risk_free_rate)} risk-free rate`} />
      <Metric label="Sortino" value={decimal(performance?.sortino_ratio)} />
      <Metric label="Calmar" value={decimal(performance?.calmar_ratio)} />
      <Metric label="Profit factor" value={decimal(performance?.profit_factor)} />
      <Metric label="Win rate" value={percent(performance?.win_rate)} />
      <Metric label="Avg. win" value={percent(performance?.avg_win)} />
      <Metric label="Avg. loss" value={percent(performance?.avg_loss)} />
      <Metric label="Best / worst day" value={`${percent(performance?.return_distribution?.max)} / ${percent(performance?.return_distribution?.min)}`} />
    </div>
  </Card>
}

function ContributionLeaders({ data, currency }: { data?: ContributionData; currency: string }) {
  const yearIndex = (data?.years.length ?? 0) - 1
  if (yearIndex < 0) return <p className="portfolio-empty-copy">Contribution history is not available.</p>
  const rows = Object.entries(data?.companies ?? {}).map(([symbol, values]) => ({
    symbol,
    amount: values.contribution_eur[yearIndex],
    percent: values.contribution_pct[yearIndex],
  })).filter(row => row.amount != null).sort((left, right) => Math.abs(Number(right.amount)) - Math.abs(Number(left.amount))).slice(0, 8)
  const bound = Math.max(...rows.map(row => Math.abs(Number(row.amount))), 1)
  return <div className="contribution-list">{rows.map(row => <div key={row.symbol}>
    <strong>{row.symbol}</strong>
    <div><i className={Number(row.amount) >= 0 ? 'positive' : 'negative'} style={{ width: `${Math.max(3, Math.abs(Number(row.amount)) / bound * 100)}%` }} /></div>
    <span className={Number(row.amount) >= 0 ? 'number-positive' : 'number-negative'}>{money(row.amount, currency)}<small>{row.percent == null ? '—' : percentPoints(row.percent)}</small></span>
  </div>)}</div>
}

export function PortfolioPerformance(props: Props) {
  return <div className="portfolio-section-stack">
    <div className="portfolio-performance-grid"><ReturnComposition performance={props.performance} onOpenDetail={props.onOpenDetail} /><RiskScorecard performance={props.performance} onOpenDetail={props.onOpenDetail} /></div>
    <Card title="Monthly return map" description="Flow-adjusted return by calendar month" actions={<ExploreButton label="Risk methodology" onClick={() => props.onOpenDetail({ kind: 'risk' })} />}><ReturnHeatmap data={props.heatmap} /></Card>
    <PortfolioAdvancedAnalytics valueHistory={props.valueHistory} allocation={props.allocation} holdings={props.holdings} onInspect={kind => props.onOpenDetail({ kind })} />
    <div className="portfolio-performance-grid">
      <Card title={`Contribution leaders · ${props.contribution?.years.at(-1) ?? 'latest year'}`} description="Value change after net investment, including dividends" actions={<ExploreButton label="Exposure details" onClick={() => props.onOpenDetail({ kind: 'allocation' })} />}><ContributionLeaders data={props.contribution} currency={props.currency} /></Card>
      <Card title="Return versus cost basis" description="Open and closed positions; annualized return in percent" actions={<ExploreButton label="Position details" onClick={() => props.onOpenDetail({ kind: 'allocation' })} />}><CostReturnChart data={props.scatter} /></Card>
    </div>
  </div>
}
