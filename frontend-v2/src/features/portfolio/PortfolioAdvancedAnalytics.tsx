import { BarElement, CategoryScale, Chart as ChartJS, Legend, LinearScale, LineElement, PointElement, Tooltip } from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

import { Card, Metric } from '../../components/Page'

ChartJS.register(BarElement, CategoryScale, Legend, LinearScale, LineElement, PointElement, Tooltip)

type ValueHistory = { dates: string[]; holdings: Record<string, Array<number | null>> }
type PieData = { labels: string[]; values: number[] }
type Holding = { symbol: string; market_value?: number | null; performance?: Record<string, unknown> }

function series(data?: ValueHistory) {
  const values = data?.dates.map((_, index) => Object.values(data.holdings).reduce((sum, holding) => sum + Number(holding[index] ?? 0), 0)) ?? []
  const returns = values.map((value, index) => index && values[index - 1] ? (value / values[index - 1] - 1) * 100 : 0)
  let peak = 0
  const drawdown = values.map(value => { peak = Math.max(peak, value); return peak ? (value / peak - 1) * 100 : 0 })
  const volatility = returns.map((_, index) => { const sample = returns.slice(Math.max(1, index - 29), index + 1); if (sample.length < 2) return null; const mean = sample.reduce((sum, value) => sum + value, 0) / sample.length; const variance = sample.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (sample.length - 1); return Math.sqrt(variance) * Math.sqrt(252) })
  return { dates: data?.dates ?? [], returns: returns.slice(1), returnDates: (data?.dates ?? []).slice(1), drawdown, volatility }
}

function RiskTrend({ data }: { data?: ValueHistory }) {
  const rows = series(data)
  const chart = { labels: rows.dates, datasets: [{ label: 'Drawdown %', data: rows.drawdown, borderColor: '#e14d5a', backgroundColor: '#e14d5a18', fill: true, pointRadius: 0, yAxisID: 'y' }, { label: '30-day volatility %', data: rows.volatility, borderColor: '#8b5cf6', pointRadius: 0, yAxisID: 'y1' }] }
  return <div className="portfolio-advanced-chart"><Line data={chart} options={{ responsive: true, maintainAspectRatio: false, interaction: { mode: 'index', intersect: false }, plugins: { legend: { position: 'bottom', labels: { boxWidth: 9, font: { size: 10 } } } }, scales: { x: { display: false }, y: { position: 'left', title: { display: true, text: 'Drawdown' } }, y1: { position: 'right', grid: { drawOnChartArea: false }, title: { display: true, text: 'Volatility' } } } }} /></div>
}

function ReturnDistribution({ data }: { data?: ValueHistory }) {
  const returns = series(data).returns.filter(Number.isFinite)
  const min = Math.min(...returns, -1)
  const max = Math.max(...returns, 1)
  const width = (max - min) / 12 || 1
  const bins = Array.from({ length: 12 }, (_, index) => ({ start: min + index * width, count: 0 }))
  for (const value of returns) bins[Math.min(11, Math.floor((value - min) / width))].count += 1
  const chart = { labels: bins.map(bin => `${bin.start.toFixed(1)}%`), datasets: [{ label: 'Trading days', data: bins.map(bin => bin.count), backgroundColor: bins.map(bin => bin.start >= 0 ? '#12a56caa' : '#e14d5aaa') }] }
  return <div className="portfolio-advanced-chart"><Bar data={chart} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { grid: { display: false } }, y: { position: 'right', beginAtZero: true } } }} /></div>
}

function percentage(value: number) { return `${(value * 100).toFixed(1)}%` }

function Concentration({ allocation, holdings }: { allocation?: PieData; holdings: Holding[] }) {
  const total = allocation?.values.reduce((sum, value) => sum + value, 0) ?? 0
  const weights = (allocation?.values ?? []).map(value => total ? value / total : 0).sort((a, b) => b - a)
  const hhi = weights.reduce((sum, weight) => sum + weight ** 2, 0)
  const ranked = [...holdings].map(holding => ({ symbol: holding.symbol, value: Number(holding.performance?.total_return_display ?? holding.performance?.total_return_native ?? 0) })).sort((a, b) => b.value - a.value)
  return <div className="concentration-panel"><div className="concentration-metrics"><Metric label="Largest position" value={percentage(weights[0] ?? 0)} /><Metric label="Top five" value={percentage(weights.slice(0, 5).reduce((sum, value) => sum + value, 0))} /><Metric label="Effective holdings" value={hhi ? (1 / hhi).toFixed(1) : '—'} /><Metric label="HHI" value={hhi.toFixed(3)} /></div><div className="leader-grid"><div><strong>Leaders</strong>{ranked.slice(0, 5).map(row => <span key={row.symbol}>{row.symbol}<b>{percentage(row.value)}</b></span>)}</div><div><strong>Laggards</strong>{ranked.slice(-5).reverse().map(row => <span key={row.symbol}>{row.symbol}<b>{percentage(row.value)}</b></span>)}</div></div></div>
}

export function PortfolioAdvancedAnalytics({ valueHistory, allocation, holdings }: { valueHistory?: ValueHistory; allocation?: PieData; holdings: Holding[] }) {
  return <><Card title="Drawdown and rolling risk"><RiskTrend data={valueHistory} /></Card><Card title="Daily return distribution"><ReturnDistribution data={valueHistory} /></Card><Card title="Concentration and leaders"><Concentration allocation={allocation} holdings={holdings} /></Card></>
}
