import { BarElement, CategoryScale, Chart as ChartJS, Filler, Legend, LinearScale, LineElement, PointElement, Tooltip } from 'chart.js'
import { ChevronRight } from 'lucide-react'
import { Bar, Line } from 'react-chartjs-2'

import { BRAND_COLORS, SEMANTIC_CHART_COLORS } from '../../brand'
import { Card, Metric } from '../../components/Page'
import { percent } from './portfolioFormat'
import type { Holding, PieData, ValueHistory } from './portfolioTypes'

ChartJS.register(BarElement, CategoryScale, Filler, Legend, LinearScale, LineElement, PointElement, Tooltip)

type AnalyticsRows = {
  dates: string[]
  returns: number[]
  returnDates: string[]
  drawdown: Array<number | null>
  volatility: Array<number | null>
}

function isTradingDay(date: string) {
  const day = new Date(`${date}T00:00:00Z`).getUTCDay()
  return day !== 0 && day !== 6
}

function sourceReturns(data: ValueHistory) {
  if (data.daily_returns?.length === data.dates.length) return data.daily_returns
  const values = data.dates.map((_, index) => Object.values(data.holdings)
    .reduce((sum, holding) => sum + Number(holding[index] ?? 0), 0))
  return values.map((value, index) => {
    const previous = values[index - 1]
    if (!index || !(previous > 0) || !Number.isFinite(value)) return 0
    return Math.max(-1, Math.min(1, value / previous - 1))
  })
}

function buildAnalyticsSeries(data?: ValueHistory): AnalyticsRows {
  const dates = data?.dates ?? []
  const validReturns = data ? sourceReturns(data).map(value => value == null || !Number.isFinite(value) ? null : value) : []
  const returnDates: string[] = []
  const returns: number[] = []
  validReturns.forEach((value, index) => {
    if (value != null && isTradingDay(dates[index])) {
      returns.push(value * 100)
      returnDates.push(dates[index])
    }
  })
  const cumulative = data?.cumulative_returns?.length === dates.length
    ? data.cumulative_returns
    : validReturns.reduce<number[]>((values, value, index) => values.concat((values[index - 1] ?? 0) * (1 + (value ?? 0)) + (value ?? 0)), [])
  let peak = 1
  const drawdown = cumulative.map(value => {
    if (value == null || !Number.isFinite(value)) return null
    const wealth = 1 + value
    peak = Math.max(peak, wealth)
    return (wealth / peak - 1) * 100
  })
  const volatility = validReturns.map((_, index) => rollingVolatility(validReturns, dates, index))
  return { dates, returns, returnDates, drawdown, volatility }
}

function rollingVolatility(values: Array<number | null>, dates: string[], endIndex: number) {
  if (!isTradingDay(dates[endIndex])) return null
  const sample = values.slice(Math.max(0, endIndex - 59), endIndex + 1)
    .filter((value, index): value is number => value != null && isTradingDay(dates[Math.max(0, endIndex - 59) + index]))
    .slice(-30)
  if (sample.length < 2) return null
  const mean = sample.reduce((sum, value) => sum + value, 0) / sample.length
  const variance = sample.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (sample.length - 1)
  return Math.sqrt(variance * 252) * 100
}

function RiskTrend({ data }: { data?: ValueHistory }) {
  const rows = buildAnalyticsSeries(data)
  const drawdowns = rows.drawdown.filter((value): value is number => value != null)
  const volatilities = rows.volatility.filter((value): value is number => value != null)
  const currentDrawdown = drawdowns.at(-1) ?? 0
  const currentVolatility = volatilities.at(-1) ?? 0
  const maxDrawdown = drawdowns.length ? Math.min(...drawdowns) : 0
  const volatilityMax = Math.max(5, Math.ceil(Math.max(...volatilities, 0) / 5) * 5)
  const common = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index' as const, intersect: false },
    plugins: { legend: { display: false }, tooltip: { callbacks: { label: (item: { parsed: { y: number | null } }) => `${item.parsed.y?.toFixed(2) ?? '—'}%` } } },
  }
  return <div className="analytics-risk-content">
    <p className="analytics-explainer">Drawdown measures the fall from a previous high. Rolling volatility annualizes the latest 30 trading-day return sample; deposits and withdrawals are removed first.</p>
    <div className="analytics-stat-strip"><Metric label="Current drawdown" value={`${currentDrawdown.toFixed(1)}%`} /><Metric label="Max drawdown" value={`${maxDrawdown.toFixed(1)}%`} /><Metric label="30-day volatility" value={`${currentVolatility.toFixed(1)}%`} /><Metric label="Observations" value={String(rows.returns.length)} /></div>
    <div className="analytics-risk-charts">
      <div className="analytics-risk-panel"><strong>Drawdown from peak</strong><div className="analytics-risk-chart"><Line data={{ labels: rows.dates, datasets: [{ label: 'Drawdown', data: rows.drawdown, borderColor: SEMANTIC_CHART_COLORS.negative, backgroundColor: `${SEMANTIC_CHART_COLORS.negative}18`, fill: true, pointRadius: 0 }] }} options={{ ...common, scales: { x: { display: false }, y: { min: Math.min(-5, Math.floor(maxDrawdown / 5) * 5), max: 0, ticks: { callback: value => `${value}%` } } } }} /></div></div>
      <div className="analytics-risk-panel"><strong>30-day annualized volatility</strong><div className="analytics-risk-chart"><Line data={{ labels: rows.dates, datasets: [{ label: 'Volatility', data: rows.volatility, borderColor: BRAND_COLORS.coral, pointRadius: 0 }] }} options={{ ...common, scales: { x: { display: false }, y: { beginAtZero: true, max: volatilityMax, ticks: { callback: value => `${value}%` } } } }} /></div></div>
    </div>
  </div>
}

function ReturnDistribution({ data }: { data?: ValueHistory }) {
  const returns = buildAnalyticsSeries(data).returns.filter(Number.isFinite)
  const sorted = [...returns].sort((left, right) => left - right)
  const percentile = (fraction: number) => sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * fraction))] : 1
  const bound = Math.max(1, Math.abs(percentile(0.01)), Math.abs(percentile(0.99)))
  const width = (bound * 2) / 12
  const bins = Array.from({ length: 12 }, (_, index) => ({ start: -bound + index * width, end: -bound + (index + 1) * width, count: 0 }))
  for (const value of returns) bins[Math.min(11, Math.max(0, Math.floor((Math.min(bound, Math.max(-bound, value)) + bound) / width)))].count += 1
  const mean = returns.length ? returns.reduce((sum, value) => sum + value, 0) / returns.length : 0
  const median = sorted.length ? sorted[Math.floor(sorted.length / 2)] : 0
  const positive = returns.filter(value => value > 0).length
  return <div className="analytics-distribution-content">
    <p className="analytics-explainer">Each bar counts flow-adjusted trading days in a return range. The outer one percent of observations is grouped into the end bins.</p>
    <div className="analytics-stat-strip"><Metric label="Average day" value={`${mean.toFixed(2)}%`} /><Metric label="Median day" value={`${median.toFixed(2)}%`} /><Metric label="Positive days" value={`${positive} / ${returns.length}`} /><Metric label="Displayed range" value={`±${bound.toFixed(1)}%`} /></div>
    <div className="portfolio-advanced-chart"><Bar data={{
      labels: bins.map((bin, index) => index === 0 ? `≤ ${bin.end.toFixed(1)}%` : index === 11 ? `≥ ${bin.start.toFixed(1)}%` : `${bin.start.toFixed(1)}%`),
      datasets: [{ label: 'Trading days', data: bins.map(bin => bin.count), backgroundColor: bins.map(bin => bin.end <= 0 ? `${SEMANTIC_CHART_COLORS.negative}aa` : bin.start >= 0 ? `${SEMANTIC_CHART_COLORS.positive}aa` : `${SEMANTIC_CHART_COLORS.neutral}aa`) }],
    }} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { grid: { display: false } }, y: { beginAtZero: true, title: { display: true, text: 'Trading days' } } } }} /></div>
  </div>
}

function Concentration({ allocation, holdings }: { allocation?: PieData; holdings: Holding[] }) {
  const total = allocation?.values.reduce((sum, value) => sum + value, 0) ?? 0
  const weights = (allocation?.values ?? []).map(value => total ? value / total : 0).sort((left, right) => right - left)
  const hhi = weights.reduce((sum, weight) => sum + weight ** 2, 0)
  const ranked = holdings.filter(holding => holding.asset_category !== 'CASH').map(holding => ({
    symbol: holding.symbol,
    value: Number(holding.performance?.total_return_display ?? holding.performance?.total_return_native ?? 0),
  })).sort((left, right) => right.value - left.value)
  return <div className="concentration-panel">
    <div className="concentration-metrics"><Metric label="Largest position" value={percent(weights[0] ?? 0)} /><Metric label="Top five" value={percent(weights.slice(0, 5).reduce((sum, value) => sum + value, 0))} /><Metric label="Effective holdings" value={hhi ? (1 / hhi).toFixed(1) : '—'} /><Metric label="HHI" value={hhi.toFixed(3)} /></div>
    <div className="leader-grid"><div><strong>Leaders</strong>{ranked.slice(0, 5).map(row => <span key={row.symbol}>{row.symbol}<b>{percent(row.value)}</b></span>)}</div><div><strong>Laggards</strong>{ranked.slice(-5).reverse().map(row => <span key={row.symbol}>{row.symbol}<b>{percent(row.value)}</b></span>)}</div></div>
  </div>
}

function ExploreButton({ label, onClick }: { label: string; onClick: () => void }) {
  return <button className="card-explore" onClick={onClick}>{label}<ChevronRight /></button>
}

export function PortfolioAdvancedAnalytics({ valueHistory, allocation, holdings, onInspect }: {
  valueHistory?: ValueHistory
  allocation?: PieData
  holdings: Holding[]
  onInspect: (kind: 'risk' | 'allocation') => void
}) {
  return <div className="portfolio-advanced-grid">
    <Card title="Drawdown and rolling risk" className="analytics-risk-card" actions={<ExploreButton label="Risk details" onClick={() => onInspect('risk')} />}><RiskTrend data={valueHistory} /></Card>
    <Card title="Daily return distribution" className="analytics-distribution-card" actions={<ExploreButton label="Methodology" onClick={() => onInspect('risk')} />}><ReturnDistribution data={valueHistory} /></Card>
    <Card title="Concentration and leaders" className="analytics-concentration-card" actions={<ExploreButton label="Exposure details" onClick={() => onInspect('allocation')} />}><Concentration allocation={allocation} holdings={holdings} /></Card>
  </div>
}

export { Concentration, ReturnDistribution, RiskTrend }
