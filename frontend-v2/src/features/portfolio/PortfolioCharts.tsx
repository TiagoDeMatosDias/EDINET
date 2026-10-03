import {
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Filler,
  LinearScale,
  LineElement,
  PointElement,
  Tooltip,
  type Chart,
  type ChartOptions,
  type Plugin,
} from 'chart.js'
import { useMemo } from 'react'
import { Bar, Line } from 'react-chartjs-2'

import { BRAND_COLORS, SERIES_COLORS } from '../../brand'
import { EmptyState } from '../../components/Feedback'
import { crosshairPlugin } from '../analysis/chartPlugins'
import { priceTicks } from '../analysis/priceHistoryRanges'
import { BENCHMARK_COLOR, divergingColor, GAIN_COLOR as GAIN, GRID_COLOR as GRID, LOSS_COLOR as LOSS, PORTFOLIO_COLOR, REFERENCE_COLOR } from './chartTheme'
import { compactMoney, formatMonth, money, signedPercent } from './portfolioFormat'
import type { HoldingHistoryPoint, PerformancePoint } from './portfolioTypes'

ChartJS.register(BarElement, CategoryScale, Filler, LinearScale, LineElement, PointElement, Tooltip)

const tooltipDay = new Intl.DateTimeFormat('en-GB', { weekday: 'short', day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' })
function tooltipTitle(label?: string) {
  const time = Date.parse(`${label ?? ''}T00:00:00Z`)
  return Number.isFinite(time) ? tooltipDay.format(time) : label ?? ''
}

/** Writes each line's name and last value just past its end, so identity never rests on colour alone. */
const endLabelPlugin: Plugin<'line'> = {
  id: 'endLabels',
  afterDatasetsDraw(chart: Chart<'line'>) {
    const { ctx, chartArea } = chart
    const placed: number[] = []
    chart.data.datasets.forEach((dataset, index) => {
      const meta = chart.getDatasetMeta(index)
      const label = (dataset as { endLabel?: string }).endLabel
      if (!label || meta.hidden) return
      const point = [...meta.data].reverse().find(element => element && Number.isFinite((element as { y: number }).y)) as { x: number; y: number } | undefined
      if (!point) return
      let y = Math.min(Math.max(point.y, chartArea.top + 6), chartArea.bottom - 6)
      // Nudge apart labels that would overlap.
      for (const other of placed) if (Math.abs(other - y) < 13) y = other + (y >= other ? 13 : -13)
      placed.push(y)
      ctx.save()
      ctx.font = '500 11px "IBM Plex Mono", ui-monospace, monospace'
      ctx.fillStyle = BRAND_COLORS.ink
      ctx.textBaseline = 'middle'
      ctx.fillText(label, chartArea.right + 8, y)
      ctx.restore()
    })
  },
}

export function SeriesLegend({ items }: { items: Array<{ label: string; color: string; dashed?: boolean; value?: string }> }) {
  return <ul className="pf-legend">{items.map(item => <li key={item.label}>
    <i className={item.dashed ? 'is-dashed' : undefined} style={{ borderColor: item.color }} aria-hidden="true" />
    <span>{item.label}</span>{item.value && <strong>{item.value}</strong>}
  </li>)}</ul>
}

/** Stacked charts that share dates keep the same axis width, so their days line up. */
function fixAxisWidth(scale: { width: number }) {
  scale.width = 48
}

function timeTicks(labels: string[]) {
  let ticks = priceTicks(labels)
  // Phone-width charts have room for every other date label.
  if (typeof window !== 'undefined' && window.innerWidth < 720) ticks = new Map([...ticks].filter((_entry, index) => index % 2 === 0))
  return (_value: unknown, index: number) => ticks.get(index) ?? null
}

/**
 * Cumulative time-weighted return of the portfolio against the benchmark and
 * consumer prices, all from the start of the period.
 */
export function GrowthChart({ series, benchmarkLabel, height = 300, showDates = true }: { series: PerformancePoint[]; benchmarkLabel?: string; height?: number; showDates?: boolean }) {
  const labels = useMemo(() => series.map(point => point.date), [series])
  if (series.length < 2) return <EmptyState title="Not enough history" description="Choose a longer period or rebuild after importing activity." />
  const last = series[series.length - 1]
  const hasBenchmark = series.some(point => point.benchmark != null)
  const hasInflation = series.some(point => point.inflation != null)
  const datasets = [
    { label: 'Portfolio', endLabel: `Portfolio ${signedPercent(last.cumulative_return, 0)}`, data: series.map(point => point.cumulative_return * 100), borderColor: PORTFOLIO_COLOR, borderWidth: 2, pointRadius: 0, pointHoverRadius: 3, pointHoverBackgroundColor: PORTFOLIO_COLOR, tension: 0, order: 0 },
    ...(hasBenchmark ? [{ label: benchmarkLabel ?? 'Benchmark', endLabel: last.benchmark != null ? `${benchmarkLabel ?? 'Benchmark'} ${signedPercent(last.benchmark, 0)}` : undefined, data: series.map(point => point.benchmark == null ? null : point.benchmark * 100), borderColor: BENCHMARK_COLOR, borderWidth: 1.5, pointRadius: 0, pointHoverRadius: 3, pointHoverBackgroundColor: BENCHMARK_COLOR, tension: 0, order: 1 }] : []),
    ...(hasInflation ? [{ label: 'Inflation', endLabel: last.inflation != null ? `Prices ${signedPercent(last.inflation, 0)}` : undefined, data: series.map(point => point.inflation == null ? null : point.inflation * 100), borderColor: REFERENCE_COLOR, borderWidth: 1, borderDash: [4, 3], pointRadius: 0, pointHoverRadius: 0, tension: 0, order: 2 }] : []),
  ]
  const options: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    layout: { padding: { right: 118 } },
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: { callbacks: { title: items => tooltipTitle(items[0]?.label), label: item => `${item.dataset.label}: ${signedPercent((item.parsed.y ?? 0) / 100)}` } },
    },
    scales: {
      x: { grid: { display: false }, ticks: { display: showDates, autoSkip: false, maxRotation: 0, callback: timeTicks(labels) } },
      y: { position: 'left', grid: { color: GRID }, afterFit: fixAxisWidth, ticks: { maxTicksLimit: 6, callback: value => `${Number(value).toFixed(0)}%` } },
    },
  }
  return <div className="pf-chart" style={{ height }}><Line data={{ labels, datasets }} options={options} plugins={[crosshairPlugin, endLabelPlugin]} /></div>
}

/** How far below its previous high the portfolio stood each day. */
export function DrawdownChart({ series, height = 150 }: { series: PerformancePoint[]; height?: number }) {
  const labels = useMemo(() => series.map(point => point.date), [series])
  if (series.length < 2) return null
  const values = series.map(point => point.drawdown * 100)
  const floor = Math.min(-5, Math.floor(Math.min(...values) / 5) * 5)
  const options: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    layout: { padding: { right: 118 } },
    interaction: { mode: 'index', intersect: false },
    plugins: { legend: { display: false }, tooltip: { displayColors: false, callbacks: { title: items => tooltipTitle(items[0]?.label), label: item => `${(item.parsed.y ?? 0).toFixed(1)}% below the previous high` } } },
    scales: {
      x: { grid: { display: false }, ticks: { autoSkip: false, maxRotation: 0, callback: timeTicks(labels) } },
      y: { position: 'left', min: floor, max: 0, grid: { color: GRID }, afterFit: fixAxisWidth, ticks: { maxTicksLimit: 4, callback: value => `${Number(value).toFixed(0)}%` } },
    },
  }
  return <div className="pf-chart" style={{ height }}><Line data={{ labels, datasets: [{ label: 'Drawdown', data: values, borderColor: LOSS, backgroundColor: 'rgb(196 70 44 / 14%)', fill: 'origin', borderWidth: 1.25, pointRadius: 0, pointHoverRadius: 3, pointHoverBackgroundColor: LOSS, tension: 0 }] }} options={options} plugins={[crosshairPlugin]} /></div>
}

/** Market value against the money put in (deposits less withdrawals); the gap is the gain. */
export function ValueChart({ series, currency, height = 260 }: { series: PerformancePoint[]; currency: string; height?: number }) {
  const labels = useMemo(() => series.map(point => point.date), [series])
  if (series.length < 2) return <EmptyState title="No value history" description="Rebuild the portfolio after importing activity." />
  const last = series[series.length - 1]
  const options: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    layout: { padding: { right: 136 } },
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: {
        callbacks: {
          title: items => tooltipTitle(items[0]?.label),
          label: item => `${item.dataset.label}: ${money(item.parsed.y, currency)}`,
          footer: items => {
            const point = series[items[0]?.dataIndex ?? -1]
            return point ? `Gain: ${money(point.value - point.invested, currency)}` : ''
          },
        },
      },
    },
    scales: {
      x: { grid: { display: false }, ticks: { autoSkip: false, maxRotation: 0, callback: timeTicks(labels) } },
      y: { position: 'left', grid: { color: GRID }, ticks: { maxTicksLimit: 6, callback: value => compactMoney(value, currency) } },
    },
  }
  const datasets = [
    { label: 'Value', endLabel: `Value ${compactMoney(last.value, currency)}`, data: series.map(point => point.value), borderColor: PORTFOLIO_COLOR, backgroundColor: 'rgb(52 96 168 / 9%)', fill: 'origin' as const, borderWidth: 2, pointRadius: 0, pointHoverRadius: 3, pointHoverBackgroundColor: PORTFOLIO_COLOR, tension: 0 },
    { label: 'Net invested', endLabel: `Invested ${compactMoney(last.invested, currency)}`, data: series.map(point => point.invested), borderColor: REFERENCE_COLOR, borderWidth: 1.25, borderDash: [4, 3], stepped: true as const, pointRadius: 0, pointHoverRadius: 0, tension: 0 },
  ]
  return <div className="pf-chart" style={{ height }}><Line data={{ labels, datasets }} options={options} plugins={[crosshairPlugin, endLabelPlugin]} /></div>
}

/** Calendar-year returns for the portfolio and the benchmark; partial years are marked. */
export function AnnualReturnsChart({ rows, benchmarkLabel, height = 230 }: { rows: Array<{ year: number; portfolio: number; benchmark?: number | null; partial: boolean }>; benchmarkLabel?: string; height?: number }) {
  if (!rows.length) return <EmptyState title="No yearly returns" description="Return history is not available." />
  const hasBenchmark = rows.some(row => row.benchmark != null)
  const labels = rows.map(row => row.partial ? `${row.year}*` : String(row.year))
  const datasets = [
    { label: 'Portfolio', data: rows.map(row => row.portfolio * 100), backgroundColor: PORTFOLIO_COLOR, borderRadius: 3, maxBarThickness: 26 },
    ...(hasBenchmark ? [{ label: benchmarkLabel ?? 'Benchmark', data: rows.map(row => row.benchmark == null ? null : row.benchmark * 100), backgroundColor: BENCHMARK_COLOR, borderRadius: 3, maxBarThickness: 26 }] : []),
  ]
  return <div className="pf-chart" style={{ height }}><Bar data={{ labels, datasets }} options={{
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    interaction: { mode: 'index', intersect: false },
    datasets: { bar: { categoryPercentage: 0.7, barPercentage: 0.9 } },
    plugins: {
      legend: { display: false },
      tooltip: { callbacks: { title: items => { const row = rows[items[0]?.dataIndex ?? 0]; return row?.partial ? `${row.year} (part of the year)` : String(row?.year ?? '') }, label: item => `${item.dataset.label}: ${signedPercent((item.parsed.y ?? 0) / 100)}` } },
    },
    scales: {
      x: { grid: { display: false } },
      y: { position: 'left', grid: { color: GRID }, ticks: { maxTicksLimit: 6, callback: value => `${Number(value).toFixed(0)}%` } },
    },
  }} /></div>
}

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

/** Each month's return as a coloured cell with its value, then the year's total and the benchmark's. */
export function MonthlyReturnsTable({ monthly, annual, benchmarkLabel }: {
  monthly: Array<{ month: string; portfolio: number; benchmark?: number | null }>
  annual: Array<{ year: number; portfolio: number; benchmark?: number | null; partial: boolean }>
  benchmarkLabel?: string
}) {
  if (!monthly.length) return <EmptyState title="No monthly returns" description="Return history is not available." />
  const byMonth = new Map(monthly.map(row => [row.month, row]))
  const years = [...new Set(monthly.map(row => Number(row.month.slice(0, 4))))].sort((a, b) => b - a)
  const yearRow = new Map(annual.map(row => [row.year, row]))
  const hasBenchmark = annual.some(row => row.benchmark != null)
  return <div className="pf-heatmap-scroll"><table className="pf-heatmap">
    <caption className="sr-only">Monthly time-weighted returns by year, with the year's total{hasBenchmark ? ` and ${benchmarkLabel}'s` : ''}.</caption>
    <thead><tr><th scope="col">Year</th>{MONTHS.map(month => <th key={month} scope="col">{month}</th>)}<th scope="col" className="pf-heatmap__total">Year</th>{hasBenchmark && <th scope="col" className="pf-heatmap__bench">{benchmarkLabel}</th>}</tr></thead>
    <tbody>{years.map(year => {
      const total = yearRow.get(year)
      const totalColor = divergingColor(total?.portfolio, 0.3)
      return <tr key={year}>
        <th scope="row">{year}</th>
        {MONTHS.map((_name, index) => {
          const row = byMonth.get(`${year}-${String(index + 1).padStart(2, '0')}`)
          const color = divergingColor(row?.portfolio)
          return <td key={index} style={color ? { background: color.background, color: color.ink } : undefined} title={row ? `${formatMonth(row.month)}: ${signedPercent(row.portfolio, 2)}${row.benchmark != null ? ` · ${benchmarkLabel} ${signedPercent(row.benchmark, 2)}` : ''}` : 'No return this month'}>{row ? signedPercent(row.portfolio) : ''}</td>
        })}
        <td className="pf-heatmap__total" style={totalColor ? { background: totalColor.background, color: totalColor.ink } : undefined} title={total?.partial ? 'Part of the year' : undefined}>{total ? `${signedPercent(total.portfolio)}${total.partial ? '*' : ''}` : ''}</td>
        {hasBenchmark && <td className="pf-heatmap__bench">{total?.benchmark != null ? signedPercent(total.benchmark) : '—'}</td>}
      </tr>
    })}</tbody>
  </table></div>
}

/** Weekday returns (from the cumulative path) grouped into half-percent bins. */
export function ReturnHistogram({ series, height = 220 }: { series: PerformancePoint[]; height?: number }) {
  const bins = useMemo(() => {
    const returns: number[] = []
    for (let index = 1; index < series.length; index++) {
      returns.push((1 + series[index].cumulative_return) / (1 + series[index - 1].cumulative_return) - 1)
    }
    const width = 0.005
    const edge = 0.04
    const count = Math.round((edge * 2) / width)
    const out = Array.from({ length: count }, (_, index) => ({ start: -edge + index * width, days: 0 }))
    for (const value of returns) {
      const index = Math.min(count - 1, Math.max(0, Math.floor((value + edge) / width)))
      out[index].days += 1
    }
    return out
  }, [series])
  if (series.length < 20) return <EmptyState title="Not enough days" description="Choose a longer period." />
  const label = (start: number, index: number) => index === 0 ? `≤ ${(start * 100 + 0.5).toFixed(1)}%` : index === bins.length - 1 ? `≥ ${(start * 100).toFixed(1)}%` : `${(start * 100).toFixed(1)}%`
  return <div className="pf-chart" style={{ height }}><Bar data={{
    labels: bins.map((bin, index) => label(bin.start, index)),
    datasets: [{ label: 'Weekdays', data: bins.map(bin => bin.days), backgroundColor: bins.map(bin => bin.start + 0.0025 < 0 ? LOSS : GAIN), borderRadius: 2, categoryPercentage: 0.92, barPercentage: 0.95 }],
  }} options={{
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    plugins: { legend: { display: false }, tooltip: { displayColors: false, callbacks: { title: items => { const bin = bins[items[0]?.dataIndex ?? 0]; return `${(bin.start * 100).toFixed(1)}% to ${((bin.start + 0.005) * 100).toFixed(1)}%` }, label: item => `${item.parsed.y} weekdays` } } },
    scales: { x: { grid: { display: false }, ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 9 } }, y: { grid: { color: GRID }, beginAtZero: true, ticks: { maxTicksLimit: 5 } } },
  }} /></div>
}

/** Horizontal weight bars: the clearest way to compare many parts of a whole. */
export function WeightBars({ rows, currency, limit = 12, onSelect }: { rows: Array<{ label: string; value: number; detail?: string }>; currency: string; limit?: number; onSelect?: (label: string) => void }) {
  const total = rows.reduce((sum, row) => sum + row.value, 0)
  const sorted = [...rows].sort((left, right) => right.value - left.value)
  const shown = sorted.slice(0, limit)
  const rest = sorted.slice(limit).reduce((sum, row) => sum + row.value, 0)
  if (rest > 0) shown.push({ label: `${sorted.length - limit} others`, value: rest })
  if (!shown.length) return <EmptyState title="Nothing to show" description="No current exposure." />
  const largest = Math.max(...shown.map(row => row.value), 1)
  return <ol className="pf-bars">{shown.map(row => {
    const content = <>
      <span className="pf-bars__label"><strong>{row.label}</strong>{row.detail && <small>{row.detail}</small>}</span>
      <span className="pf-bars__track" aria-hidden="true"><i style={{ width: `${Math.max(1, row.value / largest * 100)}%` }} /></span>
      <span className="pf-bars__value"><strong>{total ? `${(row.value / total * 100).toFixed(1)}%` : '—'}</strong><small>{compactMoney(row.value, currency)}</small></span>
    </>
    return <li key={row.label}>{onSelect && !row.label.endsWith(' others') ? <button type="button" onClick={() => onSelect(row.label)}>{content}</button> : <div>{content}</div>}</li>
  })}</ol>
}

const OTHERS_COLOR = '#B9B3A6'

/** Income per period, stacked by company or currency in the fixed categorical order; the remainder is grey. */
export function IncomeStackChart({ periods, series, currency, measureLabel, height = 260 }: { periods: string[]; series: Array<{ key: string; label: string; values: number[] }>; currency: string; measureLabel: string; height?: number }) {
  if (!periods.length) return <EmptyState title="No dividends" description="No payments match these filters." />
  const colors = series.map((row, index) => row.label.endsWith(' others') ? OTHERS_COLOR : SERIES_COLORS[index % SERIES_COLORS.length])
  return <>
    <SeriesLegend items={series.map((row, index) => ({ label: row.label, color: colors[index], value: compactMoney(row.values.reduce((sum, value) => sum + value, 0), currency) }))} />
    <div className="pf-chart" style={{ height }}><Bar data={{
      labels: periods,
      datasets: series.map((row, index) => ({ label: row.label, data: row.values, backgroundColor: colors[index], borderColor: '#F3F0E8', borderWidth: { top: 2 }, borderSkipped: false, maxBarThickness: 40 })),
    }} options={{
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          filter: item => Number(item.parsed.y) !== 0,
          callbacks: {
            label: item => `${item.dataset.label}: ${money(item.parsed.y, currency, 2)}`,
            footer: items => `${measureLabel}: ${money(items.reduce((sum, item) => sum + Number(item.parsed.y ?? 0), 0), currency, 2)}`,
          },
        },
      },
      scales: { x: { stacked: true, grid: { display: false }, ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 14 } }, y: { stacked: true, beginAtZero: true, grid: { color: GRID }, ticks: { maxTicksLimit: 6, callback: value => compactMoney(value, currency) } } },
    }} /></div>
  </>
}

/** The amount per share of each payment, in the currency it was declared in. */
export function PerShareChart({ payments, currency, height = 230 }: { payments: Array<{ date: string; per_share: number | null; shares: number | null; net_native: number; withholding_rate: number | null }>; currency: string; height?: number }) {
  const rows = payments.filter(payment => payment.per_share)
  if (!rows.length) return <EmptyState title="No amounts per share" description="The payments do not state an amount per share." />
  return <div className="pf-chart" style={{ height }}><Bar data={{
    labels: rows.map(row => row.date),
    datasets: [{ label: 'Per share', data: rows.map(row => row.per_share), backgroundColor: PORTFOLIO_COLOR, borderRadius: 2, maxBarThickness: 22 }],
  }} options={{
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    plugins: {
      legend: { display: false },
      tooltip: { displayColors: false, callbacks: {
        title: items => tooltipTitle(items[0]?.label),
        label: item => {
          const row = rows[item.dataIndex]
          return [`${money(row.per_share, currency, 4)} per share`, row.shares ? `${row.shares.toLocaleString()} shares` : '', `${money(row.net_native, currency, 2)} after ${row.withholding_rate != null ? `${(row.withholding_rate * 100).toFixed(1)}%` : 'no'} withholding`].filter(Boolean)
        },
      } },
    },
    scales: {
      x: { grid: { display: false }, ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 8, callback: (_value, index) => rows[index]?.date.slice(0, 7) } },
      y: { beginAtZero: true, grid: { color: GRID }, ticks: { maxTicksLimit: 5, callback: value => money(value, currency, Number(value) < 10 ? 2 : 0) } },
    },
  }} /></div>
}

/** Dividend per share in each calendar year; partial years (first held, current) are lighter. */
export function AnnualPerShareChart({ years, currency, height = 230 }: { years: Array<{ year: number; per_share: number; partial: boolean; per_share_growth: number | null; payments: number }>; currency: string; height?: number }) {
  if (!years.length) return null
  return <div className="pf-chart" style={{ height }}><Bar data={{
    labels: years.map(year => year.partial ? `${year.year}*` : String(year.year)),
    datasets: [{ label: 'Per share in the year', data: years.map(year => year.per_share), backgroundColor: years.map(year => year.partial ? 'rgb(52 96 168 / 35%)' : PORTFOLIO_COLOR), borderRadius: 3, maxBarThickness: 34 }],
  }} options={{
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    plugins: {
      legend: { display: false },
      tooltip: { displayColors: false, callbacks: {
        label: item => {
          const year = years[item.dataIndex]
          return [`${money(year.per_share, currency, 4)} per share from ${year.payments} payment${year.payments === 1 ? '' : 's'}`, year.partial ? 'Part of a year' : year.per_share_growth != null ? `${signedPercent(year.per_share_growth)} on the year before` : ''].filter(Boolean)
        },
      } },
    },
    scales: { x: { grid: { display: false } }, y: { beginAtZero: true, grid: { color: GRID }, ticks: { maxTicksLimit: 5, callback: value => money(value, currency, Number(value) < 10 ? 2 : 0) } } },
  }} /></div>
}

/** Income-weighted year-on-year growth in dividend per share. */
export function GrowthByYearChart({ rows, height = 220 }: { rows: Array<{ year: number; growth: number; companies: number }>; height?: number }) {
  if (!rows.length) return <EmptyState title="No comparable years" description="Growth needs two complete years of payments from the same company." />
  return <div className="pf-chart" style={{ height }}><Bar data={{
    labels: rows.map(row => row.year),
    datasets: [{ label: 'Dividend growth', data: rows.map(row => row.growth * 100), backgroundColor: rows.map(row => row.growth < 0 ? LOSS : GAIN), borderRadius: 3, maxBarThickness: 34 }],
  }} options={{
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    plugins: { legend: { display: false }, tooltip: { displayColors: false, callbacks: { label: item => [`${signedPercent((item.parsed.y ?? 0) / 100)} weighted by income`, `${rows[item.dataIndex].companies} compan${rows[item.dataIndex].companies === 1 ? 'y' : 'ies'}`] } } },
    scales: { x: { grid: { display: false } }, y: { grid: { color: GRID }, ticks: { maxTicksLimit: 5, callback: value => `${value}%` } } },
  }} /></div>
}

/** Dividend per share indexed to 100 in each company's first complete year, to compare growth across currencies. */
export function IndexedPerShareChart({ years, series, height = 240 }: { years: number[]; series: Array<{ symbol: string; values: Array<number | null> }>; height?: number }) {
  if (!series.length) return <EmptyState title="Not enough history" description="Indexing needs two complete years of payments." />
  const colors = series.map((_row, index) => SERIES_COLORS[index % SERIES_COLORS.length])
  return <>
    <SeriesLegend items={series.map((row, index) => ({ label: row.symbol, color: colors[index] }))} />
    <div className="pf-chart" style={{ height }}><Line data={{
      labels: years,
      datasets: series.map((row, index) => ({ label: row.symbol, endLabel: `${row.symbol} ${Math.round([...row.values].reverse().find(value => value != null) ?? 100)}`, data: row.values, borderColor: colors[index], backgroundColor: colors[index], borderWidth: 2, pointRadius: 3, spanGaps: true, tension: 0 })),
    }} options={{
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      layout: { padding: { right: 70 } },
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: item => `${item.dataset.label}: ${Number(item.parsed.y).toFixed(0)}` } } },
      scales: { x: { grid: { display: false } }, y: { grid: { color: GRID }, ticks: { maxTicksLimit: 6 } } },
    }} plugins={[endLabelPlugin]} /></div>
  </>
}

/** A holding's daily close in its own currency, with its average cost as a dashed guide. */
export function HoldingPriceChart({ data, currency, averageCost, height = 240 }: { data?: HoldingHistoryPoint[]; currency: string; averageCost?: number | null; height?: number }) {
  const points = useMemo(() => (data ?? []).filter(point => point.market_price != null && new Date(`${point.date}T00:00:00Z`).getUTCDay() % 6 !== 0), [data])
  const labels = useMemo(() => points.map(point => point.date), [points])
  if (points.length < 2) return <EmptyState title="No price history" description="This position has no daily valuation history." />
  const datasets = [
    { label: 'Close', data: points.map(point => point.market_price ?? null), borderColor: BRAND_COLORS.ink, borderWidth: 1.5, pointRadius: 0, pointHoverRadius: 3, pointHoverBackgroundColor: BRAND_COLORS.ink, tension: 0 },
    ...(averageCost ? [{ label: 'Average cost', data: points.map(() => averageCost), borderColor: REFERENCE_COLOR, borderWidth: 1, borderDash: [4, 3], pointRadius: 0, pointHoverRadius: 0 }] : []),
  ]
  return <div className="pf-chart" style={{ height }}><Line data={{ labels, datasets }} options={{
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    interaction: { mode: 'index', intersect: false },
    plugins: { legend: { display: false }, tooltip: { callbacks: { title: items => tooltipTitle(items[0]?.label), label: item => `${item.dataset.label}: ${money(item.parsed.y, currency, 2)}` } } },
    scales: {
      x: { grid: { display: false }, ticks: { autoSkip: false, maxRotation: 0, callback: timeTicks(labels) } },
      y: { position: 'right', grid: { color: GRID }, ticks: { maxTicksLimit: 6, callback: value => money(value, currency, Number(value) < 100 ? 2 : 0) } },
    },
  }} plugins={[crosshairPlugin]} /></div>
}

