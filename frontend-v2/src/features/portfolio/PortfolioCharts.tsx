import {
  ArcElement,
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Filler,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  Tooltip,
} from 'chart.js'
import { Bar, Doughnut, Line, Scatter } from 'react-chartjs-2'

import { BRAND_CHART_COLORS, BRAND_COLORS } from '../../brand'
import { EmptyState } from '../../components/Feedback'
import { money, percentPoints } from './portfolioFormat'
import type {
  DividendCurrencyHistory,
  DividendGrowthData,
  DividendHistory,
  HeatmapData,
  HoldingHistoryPoint,
  PieData,
  ScatterPoint,
  ValueHistory,
} from './portfolioTypes'

ChartJS.register(ArcElement, BarElement, CategoryScale, Filler, Legend, LinearScale, LineElement, PointElement, Tooltip)
const COLORS = [...BRAND_CHART_COLORS]

function ChartFrame({ children, size = 'medium' }: { children: React.ReactNode; size?: 'small' | 'medium' | 'large' }) {
  return <div className={`portfolio-chart-frame portfolio-chart-frame--${size}`}>{children}</div>
}

function portfolioTotals(data?: ValueHistory) {
  if (!data) return []
  if (data.portfolio_values?.length === data.dates.length) return data.portfolio_values
  return data.dates.map((_, index) => Object.values(data.holdings).reduce((sum, values) => sum + Number(values[index] ?? 0), 0))
}

export function PortfolioValueChart({ data, currency, large = false }: { data?: ValueHistory; currency: string; large?: boolean }) {
  const values = portfolioTotals(data)
  if (!data?.dates.length || !values.length) return <EmptyState title="No value history" description="Rebuild the portfolio after importing activity." />
  const chart = {
    labels: data.dates,
    datasets: [{
      label: `Portfolio value (${currency})`,
      data: values,
      borderColor: BRAND_COLORS.indigo,
      backgroundColor: `${BRAND_COLORS.indigo}16`,
      borderWidth: 2.25,
      fill: true,
      pointRadius: 0,
      tension: 0.16,
    }],
  }
  return <ChartFrame size={large ? 'large' : 'medium'}><Line data={chart} options={{
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: { callbacks: { label: item => money(item.parsed.y, currency) } },
    },
    scales: {
      x: { grid: { display: false }, ticks: { maxTicksLimit: 7, maxRotation: 0 } },
      y: { position: 'right', ticks: { callback: value => Number(value).toLocaleString(undefined, { notation: 'compact' }) } },
    },
  }} /></ChartFrame>
}

function compactAllocation(data?: PieData, limit = 8) {
  const total = data?.values.reduce((sum, value) => sum + value, 0) ?? 0
  const rows = (data?.labels ?? []).map((label, index) => ({ label, value: data?.values[index] ?? 0 }))
    .sort((left, right) => right.value - left.value)
  const shown = rows.slice(0, limit)
  const other = rows.slice(limit).reduce((sum, row) => sum + row.value, 0)
  if (other > 0) shown.push({ label: 'Other', value: other })
  return { rows: shown, total }
}

export function AllocationBreakdown({ data, currency, label }: { data?: PieData; currency: string; label: string }) {
  const { rows, total } = compactAllocation(data)
  if (!rows.length) return <EmptyState title={`No ${label.toLowerCase()}`} description="Current exposure is not available." />
  return <div className="allocation-breakdown">
    <div className="allocation-donut">
      <Doughnut data={{
        labels: rows.map(row => row.label),
        datasets: [{ data: rows.map(row => row.value), backgroundColor: rows.map((_, index) => COLORS[index % COLORS.length]), borderWidth: 0 }],
      }} options={{ responsive: true, maintainAspectRatio: false, cutout: '70%', plugins: { legend: { display: false } } }} />
      <div className="allocation-donut-label"><strong>{data?.labels.length ?? 0}</strong><span>{label}</span></div>
    </div>
    <div className="allocation-legend">
      {rows.map((row, index) => <div key={row.label} className="allocation-legend-row">
        <i style={{ background: COLORS[index % COLORS.length] }} />
        <span>{row.label}</span>
        <strong>{total ? percentPoints(row.value / total * 100) : '—'}</strong>
        <small>{money(row.value, currency)}</small>
      </div>)}
    </div>
  </div>
}

export function ReturnHeatmap({ data }: { data?: HeatmapData }) {
  if (!data?.years.length) return <EmptyState title="No monthly returns" description="Return history is not available." />
  const color = (value: number | null) => {
    if (value == null) return undefined
    const strength = Math.min(Math.abs(value) / 10, 1)
    return value >= 0 ? `rgba(40,116,90,${0.12 + strength * 0.72})` : `rgba(163,58,69,${0.12 + strength * 0.72})`
  }
  return <div className="return-heatmap-frame"><div className="return-heatmap">
    <div className="heatmap-row heatmap-head"><span>Year</span>{data.months.map(month => <span key={month}>{new Date(2020, month - 1).toLocaleString(undefined, { month: 'short' })}</span>)}</div>
    {data.years.map((year, row) => <div className="heatmap-row" key={year}>
      <strong>{year}</strong>
      {data.values[row].map((value, column) => <span key={column} style={{ background: color(value) }} title={value == null ? 'No data' : `${value.toFixed(2)}%`}>{value == null ? '·' : value.toFixed(1)}</span>)}
    </div>)}
  </div></div>
}

export function DividendsByCurrencyChart({ data }: { data?: DividendCurrencyHistory }) {
  const rows = Object.entries(data?.currencies ?? {}).map(([currency, values]) => ({
    currency,
    values,
    total: values.reduce((sum, value) => sum + value, 0),
  })).sort((left, right) => right.total - left.total)
  if (!rows.length) return <EmptyState title="No dividend history" description="No income transactions were found." />
  const chart = {
    labels: data?.periods ?? [],
    datasets: rows.map((row, index) => ({ label: row.currency, data: row.values, backgroundColor: COLORS[index % COLORS.length] })),
  }
  return <ChartFrame><Bar data={chart} options={{
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'bottom', labels: { boxWidth: 10, usePointStyle: true } } },
    scales: { x: { stacked: true, grid: { display: false } }, y: { stacked: true, position: 'right', beginAtZero: true } },
  }} /></ChartFrame>
}

export function DividendsByCompanyChart({ data, limit = 10 }: { data?: DividendHistory; limit?: number }) {
  const rows = Object.entries(data?.companies ?? {}).map(([symbol, values]) => ({
    symbol,
    values,
    total: values.reduce((sum, value) => sum + value, 0),
  })).sort((left, right) => right.total - left.total).slice(0, limit)
  if (!rows.length) return <EmptyState title="No company income" description="No payer history is available." />
  return <ChartFrame><Bar data={{
    labels: data?.periods ?? [],
    datasets: rows.map((row, index) => ({ label: row.symbol, data: row.values, backgroundColor: COLORS[index % COLORS.length] })),
  }} options={{
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    plugins: { legend: { position: 'bottom', labels: { boxWidth: 9, usePointStyle: true } } },
    scales: { x: { stacked: true, grid: { display: false } }, y: { stacked: true, position: 'right', beginAtZero: true } },
  }} /></ChartFrame>
}

export function DividendGrowthChart({ data }: { data?: DividendGrowthData }) {
  const rows = Object.entries(data?.companies ?? {}).map(([symbol, company]) => ({
    symbol,
    values: company.yoy_growth,
    weight: company.avg_market_value_eur.reduce<number>((sum, value) => sum + Number(value ?? 0), 0),
  })).filter(row => row.values.some(value => value != null)).sort((left, right) => right.weight - left.weight).slice(0, 8)
  if (!rows.length) return <EmptyState title="No dividend growth data" description="No companies have comparable per-share history." />
  const datasets: any[] = rows.map((row, index) => ({
    label: row.symbol,
    data: row.values,
    borderColor: COLORS[index % COLORS.length],
    backgroundColor: COLORS[index % COLORS.length],
    pointRadius: 3,
    tension: 0.16,
    spanGaps: true,
  }))
  if (data?.weighted_average_growth.some(value => value != null)) datasets.push({
    label: 'Portfolio weighted average',
    data: data.weighted_average_growth,
    borderColor: BRAND_COLORS.indigo,
    backgroundColor: BRAND_COLORS.indigo,
    borderWidth: 3,
    pointRadius: 4,
    spanGaps: true,
  })
  return <ChartFrame><Line data={{ labels: data?.years ?? [], datasets }} options={{
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    plugins: { legend: { position: 'bottom', labels: { boxWidth: 9, usePointStyle: true } }, tooltip: { callbacks: { label: item => `${item.dataset.label}: ${Number(item.parsed.y).toFixed(1)}%` } } },
    scales: { x: { grid: { display: false } }, y: { position: 'right', ticks: { callback: value => `${value}%` }, title: { display: true, text: 'YoY DPS growth' } } },
  }} /></ChartFrame>
}

export function CostReturnChart({ data }: { data?: ScatterPoint[] }) {
  if (!data?.length) return <EmptyState title="No position returns" description="Cost and return history is not available." />
  const symbols = [...new Set(data.map(point => point.symbol))].sort()
  const colors = Object.fromEntries(symbols.map((symbol, index) => [symbol, COLORS[index % COLORS.length]]))
  const dataset = (isOpen: boolean) => data.filter(point => point.is_open === isOpen).map(point => ({
    x: point.cost_basis_display,
    y: point.annualized_return,
    symbol: point.symbol,
  }))
  const chart: any = { datasets: [
    { label: 'Open positions', data: dataset(true), backgroundColor: data.filter(point => point.is_open).map(point => `${colors[point.symbol]}cc`), pointRadius: 5 },
    { label: 'Closed positions', data: dataset(false), backgroundColor: data.filter(point => !point.is_open).map(point => `${colors[point.symbol]}88`), pointRadius: 5, pointStyle: 'triangle' },
  ] }
  return <ChartFrame><Scatter data={chart} options={{
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { position: 'bottom', labels: { boxWidth: 9, usePointStyle: true } },
      tooltip: { callbacks: { label: context => {
        const point = context.raw as { x: number; y: number; symbol: string }
        return `${point.symbol}: ${point.y.toFixed(1)}% on ${point.x.toLocaleString()}`
      } } },
    },
    scales: {
      x: { title: { display: true, text: 'Cost basis' }, ticks: { callback: value => Number(value).toLocaleString(undefined, { notation: 'compact' }) } },
      y: { position: 'right', title: { display: true, text: 'Annualized return' }, ticks: { callback: value => `${value}%` } },
    },
  }} /></ChartFrame>
}

export function HoldingHistoryChart({ data, currency }: { data?: HoldingHistoryPoint[]; currency: string }) {
  if (!data?.length) return <EmptyState title="No holding history" description="This position has no daily valuation history." />
  return <ChartFrame><Line data={{
    labels: data.map(point => point.date),
    datasets: [{
      label: `Market value (${currency})`,
      data: data.map(point => point.market_value),
      borderColor: BRAND_COLORS.indigo,
      backgroundColor: `${BRAND_COLORS.indigo}14`,
      fill: true,
      pointRadius: 0,
      tension: 0.16,
    }],
  }} options={{
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    plugins: { legend: { display: false }, tooltip: { callbacks: { label: item => money(item.parsed.y, currency) } } },
    scales: { x: { grid: { display: false }, ticks: { maxTicksLimit: 6, maxRotation: 0 } }, y: { position: 'right', beginAtZero: true } },
  }} /></ChartFrame>
}
