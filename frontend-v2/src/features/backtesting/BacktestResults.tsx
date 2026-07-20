import { BarElement, CategoryScale, Chart as ChartJS, Filler, Legend, LinearScale, LineElement, PointElement, Tooltip } from 'chart.js'
import { Download } from 'lucide-react'
import { Bar, Line } from 'react-chartjs-2'

import { Card, Metric } from '../../components/Page'

ChartJS.register(BarElement, CategoryScale, Filler, Legend, LinearScale, LineElement, PointElement, Tooltip)

type Point = { date: string; portfolio?: number | null; benchmark?: number | null; price_only?: number | null; dividend_only?: number | null; total?: number | null }
type ResultRecord = { summary?: Record<string, unknown>; aggregate?: Record<string, unknown>; chart_data?: { cumulative?: Point[]; drawdown?: Point[]; decomposition?: Point[] }; per_company?: Array<Record<string, unknown>> }

function number(summary: Record<string, unknown>, key: string) {
  const value = Number(summary[key])
  return Number.isFinite(value) ? value : null
}

function pct(value: number | null) { return value == null ? '—' : `${(value * 100).toFixed(1)}%` }
function decimal(value: number | null) { return value == null ? '—' : value.toFixed(2) }

function ResultMetrics({ summary }: { summary: Record<string, unknown> }) {
  return <div className="metric-strip backtest-metrics"><Metric label="Total return" value={pct(number(summary, 'total_return'))} /><Metric label="Annualized" value={pct(number(summary, 'annualized_return'))} /><Metric label="Price return" value={pct(number(summary, 'price_return'))} /><Metric label="Dividend return" value={pct(number(summary, 'dividend_return'))} /><Metric label="Volatility" value={pct(number(summary, 'volatility'))} /><Metric label="Sharpe" value={decimal(number(summary, 'sharpe_ratio'))} /><Metric label="Max drawdown" value={pct(number(summary, 'max_drawdown'))} /><Metric label="Benchmark" value={pct(number(summary, 'benchmark_total_return'))} /><Metric label="Excess return" value={pct(number(summary, 'excess_return'))} /><Metric label="Initial capital" value={number(summary, 'initial_capital')?.toLocaleString() ?? '—'} /><Metric label="Start" value={String(summary.start_date ?? '—')} /><Metric label="End" value={String(summary.end_date ?? '—')} /></div>
}

function ReturnChart({ rows }: { rows: Point[] }) {
  const data = { labels: rows.map(row => row.date), datasets: [{ label: 'Portfolio', data: rows.map(row => row.portfolio == null ? null : row.portfolio * 100), borderColor: '#146ef5', backgroundColor: '#146ef518', fill: true, pointRadius: 0 }, { label: 'Benchmark', data: rows.map(row => row.benchmark == null ? null : row.benchmark * 100), borderColor: '#64748b', pointRadius: 0 }] }
  return <Line data={data} options={{ responsive: true, maintainAspectRatio: false, interaction: { mode: 'index', intersect: false }, plugins: { legend: { position: 'bottom' } }, scales: { x: { display: false }, y: { position: 'right', ticks: { callback: value => `${value}%` } } } }} />
}

function DrawdownChart({ rows }: { rows: Point[] }) {
  const data = { labels: rows.map(row => row.date), datasets: [{ label: 'Portfolio', data: rows.map(row => (row.portfolio ?? 0) * 100), borderColor: '#e14d5a', backgroundColor: '#e14d5a22', fill: true, pointRadius: 0 }, { label: 'Benchmark', data: rows.map(row => row.benchmark == null ? null : row.benchmark * 100), borderColor: '#64748b', pointRadius: 0 }] }
  return <Line data={data} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } }, scales: { x: { display: false }, y: { position: 'right', max: 0, ticks: { callback: value => `${value}%` } } } }} />
}

function DecompositionChart({ rows }: { rows: Point[] }) {
  const sampled = rows.filter((_, index) => index % Math.max(1, Math.floor(rows.length / 80)) === 0 || index === rows.length - 1)
  const data = { labels: sampled.map(row => row.date), datasets: [{ label: 'Price', data: sampled.map(row => (row.price_only ?? 0) * 100), backgroundColor: '#146ef5aa' }, { label: 'Dividend', data: sampled.map(row => (row.dividend_only ?? 0) * 100), backgroundColor: '#12a56caa' }] }
  return <Bar data={data} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } }, scales: { x: { display: false }, y: { position: 'right', ticks: { callback: value => `${value}%` } } } }} />
}

function HoldingsBreakdown({ rows }: { rows: Array<Record<string, unknown>> }) {
  const ranked = [...rows].sort((a, b) => Number(b.total_return ?? 0) - Number(a.total_return ?? 0)).slice(0, 12)
  return <div className="backtest-breakdown"><div className="breakdown-head"><span>Ticker</span><span>Weight</span><span>Price</span><span>Dividend</span><span>Total</span></div>{ranked.map(row => <div key={String(row.Ticker)}><strong>{String(row.Ticker ?? '—')}</strong><span>{pct(number(row, 'weight'))}</span><span>{pct(number(row, 'price_return'))}</span><span>{pct(number(row, 'dividend_return'))}</span><b>{pct(number(row, 'total_return'))}</b></div>)}</div>
}

function RollingSummary({ aggregate }: { aggregate: Record<string, unknown> }) {
  const stats = (aggregate.stats ?? {}) as Record<string, Record<string, unknown>>
  return <div className="metric-strip backtest-metrics"><Metric label="Total runs" value={String(aggregate.total_runs ?? 0)} /><Metric label="Successful" value={String(aggregate.successful ?? 0)} /><Metric label="Failed" value={String(aggregate.failed ?? 0)} /><Metric label="Periods" value={String(aggregate.periods ?? 0)} /><Metric label="Mean return" value={pct(Number(stats.total_return?.mean ?? 0))} /><Metric label="Median return" value={pct(Number(stats.total_return?.median ?? 0))} /><Metric label="Mean Sharpe" value={decimal(Number(stats.sharpe_ratio?.mean ?? 0))} /><Metric label="Mean drawdown" value={pct(Number(stats.max_drawdown?.mean ?? 0))} /></div>
}

export function BacktestResults({ data, resultId }: { data: unknown; resultId: string }) {
  const record = data as ResultRecord
  const summary = record.summary
  if (!summary && record.aggregate) return <Card title="Rolling backtest results" actions={resultId && <a className="button button--secondary" href={`/api/backtesting/download/${resultId}`}><Download />Download</a>}><RollingSummary aggregate={record.aggregate} /><details className="details"><summary>Technical aggregate</summary><pre>{JSON.stringify(record.aggregate, null, 2)}</pre></details></Card>
  if (!summary) return null
  const charts = record.chart_data ?? {}
  return <Card className="backtest-results" title="Backtest results" description={`Saved as ${resultId}`} actions={<a className="button button--secondary" href={`/api/backtesting/download/${resultId}`}><Download />Download full result</a>}><ResultMetrics summary={summary} /><div className="backtest-chart-grid"><section><strong>Cumulative return</strong><div><ReturnChart rows={charts.cumulative ?? []} /></div></section><section><strong>Drawdown</strong><div><DrawdownChart rows={charts.drawdown ?? []} /></div></section><section><strong>Price and dividend return</strong><div><DecompositionChart rows={charts.decomposition ?? []} /></div></section><section><strong>Holding contribution</strong><HoldingsBreakdown rows={record.per_company ?? []} /></section></div><details className="details"><summary>Technical result summary</summary><pre>{JSON.stringify(summary, null, 2)}</pre></details></Card>
}
