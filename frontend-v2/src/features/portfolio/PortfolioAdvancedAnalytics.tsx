import { BarElement, CategoryScale, Chart as ChartJS, Legend, LinearScale, LineElement, PointElement, Tooltip } from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

import { Card, Metric } from '../../components/Page'

ChartJS.register(BarElement, CategoryScale, Legend, LinearScale, LineElement, PointElement, Tooltip)

export type ValueHistory = {
  dates: string[]
  holdings: Record<string, Array<number | null>>
  portfolio_values?: Array<number | null>
  daily_returns?: Array<number | null>
  cumulative_returns?: Array<number | null>
}
type PieData = { labels: string[]; values: number[] }
type Holding = { symbol: string; market_value?: number | null; performance?: Record<string, unknown> }
type AnalyticsRows = { dates: string[]; returns: number[]; returnDates: string[]; drawdown: Array<number | null>; volatility: Array<number | null> }

function isTradingDay(date: string) {
  const day = new Date(date + 'T00:00:00Z').getUTCDay()
  return day !== 0 && day !== 6
}

function buildAnalyticsSeries(data?: ValueHistory): AnalyticsRows {
  const dates = data?.dates ?? []
  let daily = data?.daily_returns?.length === dates.length ? data.daily_returns : undefined
  if (!daily) {
    const values = dates.map((_, index) => Object.values(data?.holdings ?? {}).reduce((sum, holding) => sum + Number(holding[index] ?? 0), 0))
    daily = values.map((value, index) => {
      const previous = values[index - 1]
      if (!index || !(previous > 0) || !Number.isFinite(value)) return 0
      return Math.max(-1, Math.min(1, value / previous - 1))
    })
  }
  const validReturns = daily.map(value => value == null || !Number.isFinite(value) ? null : value)
  const returnDates: string[] = []
  const returns: number[] = []
  validReturns.forEach((value, index) => {
    if (value != null && isTradingDay(dates[index])) {
      returns.push(value * 100)
      returnDates.push(dates[index])
    }
  })
  const cumulative = data?.cumulative_returns?.length === dates.length ? data.cumulative_returns : validReturns.reduce<number[]>((values, value, index) => values.concat((values[index - 1] ?? 0) * (1 + (value ?? 0)) + (value ?? 0)), [])
  let peak = 1
  const drawdown = cumulative.map(value => {
    if (value == null || !Number.isFinite(value)) return null
    const wealth = 1 + value
    peak = Math.max(peak, wealth)
    return (wealth / peak - 1) * 100
  })
  const volatility = validReturns.map((_, index) => {
    const sample = validReturns.slice(0, index + 1).filter((value, sampleIndex) => value != null && isTradingDay(dates[sampleIndex])) as number[]
    if (sample.length < 2 || !isTradingDay(dates[index])) return null
    const window = sample.slice(-30)
    if (window.length < 2) return null
    const mean = window.reduce((sum, value) => sum + value, 0) / window.length
    const variance = window.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (window.length - 1)
    return Math.sqrt(variance * 252) * 100
  })
  return { dates, returns, returnDates, drawdown, volatility }
}

function niceCeiling(value: number, minimum: number) {
  return Math.max(minimum, Math.ceil(Math.max(value, minimum) / 5) * 5)
}

function RiskTrend({ data }: { data?: ValueHistory }) {
  const rows = buildAnalyticsSeries(data)
  const drawdowns = rows.drawdown.filter((value): value is number => value != null)
  const volatilities = rows.volatility.filter((value): value is number => value != null)
  const currentDrawdown = drawdowns.at(-1) ?? 0
  const currentVolatility = volatilities.at(-1) ?? 0
  const maxDrawdown = drawdowns.length ? Math.min(...drawdowns) : 0
  const volatilityMax = niceCeiling(Math.max(...volatilities, 0), 5)
  const axisPercent = (value: number | string) => Number(value).toFixed(0) + '%'
  const common = { responsive: true, maintainAspectRatio: false, interaction: { mode: 'index' as const, intersect: false }, plugins: { legend: { display: false }, tooltip: { callbacks: { label: (item: { parsed: { y: number | null } }) => (item.parsed.y == null ? '—' : item.parsed.y.toFixed(2)) + '%' } } } }
  return <div className="analytics-risk-content">
    <p className="analytics-explainer">Drawdown is the fall from the previous peak after deposits and withdrawals are removed. Rolling volatility is the annualized standard deviation of the last 30 trading-day returns.</p>
    <div className="analytics-stat-strip"><Metric label="Current drawdown" value={currentDrawdown.toFixed(1) + '%'} /><Metric label="Max drawdown" value={maxDrawdown.toFixed(1) + '%'} /><Metric label="30-day volatility" value={currentVolatility.toFixed(1) + '%'} /><Metric label="Trading-day observations" value={String(rows.returns.length)} /></div>
    <div className="analytics-risk-charts">
      <div className="analytics-risk-panel"><strong>Drawdown from peak</strong><div className="analytics-risk-chart"><Line data={{ labels: rows.dates, datasets: [{ label: 'Drawdown', data: rows.drawdown, borderColor: '#e14d5a', backgroundColor: '#e14d5a18', fill: true, pointRadius: 0 }] }} options={{ ...common, scales: { x: { display: false }, y: { min: Math.min(-5, Math.floor(maxDrawdown / 5) * 5), max: 0, ticks: { callback: axisPercent }, title: { display: true, text: '%' } } } }} /></div></div>
      <div className="analytics-risk-panel"><strong>30-day annualized volatility</strong><div className="analytics-risk-chart"><Line data={{ labels: rows.dates, datasets: [{ label: 'Volatility', data: rows.volatility, borderColor: '#8b5cf6', pointRadius: 0 }] }} options={{ ...common, scales: { x: { display: false }, y: { beginAtZero: true, max: volatilityMax, ticks: { callback: axisPercent }, title: { display: true, text: '%' } } } }} /></div></div>
    </div>
  </div>
}

function ReturnDistribution({ data }: { data?: ValueHistory }) {
  const rows = buildAnalyticsSeries(data)
  const returns = rows.returns.filter(Number.isFinite)
  const sorted = [...returns].sort((a, b) => a - b)
  const percentile = (p: number) => sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p))] : 1
  const bound = Math.max(1, Math.abs(percentile(0.01)), Math.abs(percentile(0.99)))
  const width = (bound * 2) / 12
  const bins = Array.from({ length: 12 }, (_, index) => ({ start: -bound + index * width, end: -bound + (index + 1) * width, count: 0 }))
  for (const value of returns) bins[Math.min(11, Math.max(0, Math.floor((Math.min(bound, Math.max(-bound, value)) + bound) / width)))].count += 1
  const mean = returns.length ? returns.reduce((sum, value) => sum + value, 0) / returns.length : 0
  const median = sorted.length ? sorted[Math.floor(sorted.length / 2)] : 0
  const positive = returns.filter(value => value > 0).length
  const chart = { labels: bins.map((bin, index) => index === 0 ? '≤ ' + bin.end.toFixed(1) + '%' : index === 11 ? '≥ ' + bin.start.toFixed(1) + '%' : bin.start.toFixed(1) + '%'), datasets: [{ label: 'Trading days', data: bins.map(bin => bin.count), backgroundColor: bins.map(bin => bin.end <= 0 ? '#e14d5aaa' : bin.start >= 0 ? '#12a56caa' : '#64748baa') }] }
  return <div className="analytics-distribution-content">
    <p className="analytics-explainer">Flow-adjusted daily portfolio returns. Each bar counts trading days in a return range; weekends and cash-flow effects are excluded.</p>
    <div className="analytics-stat-strip"><Metric label="Average day" value={mean.toFixed(2) + '%'} /><Metric label="Median day" value={median.toFixed(2) + '%'} /><Metric label="Positive days" value={positive + ' / ' + returns.length} /><Metric label="Displayed range" value={'±' + bound.toFixed(1) + '%'} /></div>
    <div className="portfolio-advanced-chart"><Bar data={chart} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false }, tooltip: { callbacks: { label: item => (item.parsed.y ?? 0) + ' trading days' } } }, scales: { x: { grid: { display: false } }, y: { beginAtZero: true, title: { display: true, text: 'Trading days' } } } }} /></div>
  </div>
}

function percentage(value: number) { return (value * 100).toFixed(1) + '%' }

function Concentration({ allocation, holdings }: { allocation?: PieData; holdings: Holding[] }) {
  const total = allocation?.values.reduce((sum, value) => sum + value, 0) ?? 0
  const weights = (allocation?.values ?? []).map(value => total ? value / total : 0).sort((a, b) => b - a)
  const hhi = weights.reduce((sum, weight) => sum + weight ** 2, 0)
  const ranked = [...holdings].map(holding => ({ symbol: holding.symbol, value: Number(holding.performance?.total_return_display ?? holding.performance?.total_return_native ?? 0) })).sort((a, b) => b.value - a.value)
  return <div className="concentration-panel"><div className="concentration-metrics"><Metric label="Largest position" value={percentage(weights[0] ?? 0)} /><Metric label="Top five" value={percentage(weights.slice(0, 5).reduce((sum, value) => sum + value, 0))} /><Metric label="Effective holdings" value={hhi ? (1 / hhi).toFixed(1) : '—'} /><Metric label="HHI" value={hhi.toFixed(3)} /></div><div className="leader-grid"><div><strong>Leaders</strong>{ranked.slice(0, 5).map(row => <span key={row.symbol}>{row.symbol}<b>{percentage(row.value)}</b></span>)}</div><div><strong>Laggards</strong>{ranked.slice(-5).reverse().map(row => <span key={row.symbol}>{row.symbol}<b>{percentage(row.value)}</b></span>)}</div></div></div>
}

export function PortfolioAdvancedAnalytics({ valueHistory, allocation, holdings }: { valueHistory?: ValueHistory; allocation?: PieData; holdings: Holding[] }) {
  return <><Card title="Drawdown and rolling risk" className="analytics-risk-card"><RiskTrend data={valueHistory} /></Card><Card title="Daily return distribution" className="analytics-distribution-card"><ReturnDistribution data={valueHistory} /></Card><Card title="Concentration and leaders" className="analytics-concentration-card"><Concentration allocation={allocation} holdings={holdings} /></Card></>
}
