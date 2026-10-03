import {
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  Tooltip,
  type Chart,
  type ChartOptions,
  type Plugin,
} from 'chart.js'
import type { ReactNode } from 'react'
import { Bar, Line, Scatter } from 'react-chartjs-2'

import { BRAND_COLORS } from '../../brand'
import { formatMetricValue, metricDefinition, type MetricCurrencies, type MetricDefinition } from '../../metrics'
import { abbreviate, alignTrends, companyName, indexToHundred, median, seriesStyle, shortName, sortCompanies } from './comparisonModel'
import type { ComparisonCompany, TrendsResponse } from './comparisonTypes'

ChartJS.register(BarElement, CategoryScale, Legend, LinearScale, LineElement, PointElement, Tooltip)

type Guides = { x?: number | null; y?: number | null }

/** Dashed median guides: on a ranking, the middle value; on a scatter, both axes' medians split it into quadrants. */
const medianGuides: Plugin = {
  id: 'comparisonMedians',
  afterDatasetsDraw(chart: Chart) {
    const guides = (chart.options.plugins as Record<string, Guides | undefined>)?.comparisonMedians
    if (!guides) return
    const { ctx, chartArea: area } = chart
    ctx.save()
    ctx.strokeStyle = BRAND_COLORS.stone
    ctx.globalAlpha = .55
    ctx.setLineDash([3, 3])
    ctx.lineWidth = 1
    if (guides.x != null && chart.scales.x) {
      const x = chart.scales.x.getPixelForValue(guides.x)
      if (x >= area.left && x <= area.right) { ctx.beginPath(); ctx.moveTo(x, area.top); ctx.lineTo(x, area.bottom); ctx.stroke() }
    }
    if (guides.y != null && chart.scales.y) {
      const y = chart.scales.y.getPixelForValue(guides.y)
      if (y >= area.top && y <= area.bottom) { ctx.beginPath(); ctx.moveTo(area.left, y); ctx.lineTo(area.right, y); ctx.stroke() }
    }
    ctx.restore()
  },
}

/** Each scatter point carries its company's short name, so the chart reads without a legend. */
const pointLabels: Plugin<'scatter'> = {
  id: 'comparisonPointLabels',
  afterDatasetsDraw(chart) {
    const { ctx } = chart
    ctx.save()
    ctx.font = `10px ${ChartJS.defaults.font.family}`
    ctx.fillStyle = BRAND_COLORS.ink
    ctx.textBaseline = 'middle'
    const placed: Array<{ left: number; right: number; y: number }> = []
    chart.data.datasets.forEach((dataset, index) => {
      const meta = chart.getDatasetMeta(index)
      if (meta.hidden) return
      meta.data.forEach(point => {
        const label = String(dataset.label ?? '')
        const width = ctx.measureText(label).width
        const right = point.x + 7 + width < chart.chartArea.right
        const left = right ? point.x + 7 : point.x - 7 - width
        // Labels that would overlap one already drawn step down a line.
        let y = point.y
        while (placed.some(box => Math.abs(box.y - y) < 11 && left < box.right && left + width > box.left)) y += 11
        placed.push({ left, right: left + width, y })
        ctx.textAlign = 'left'
        ctx.fillText(label, left, y)
      })
    })
    ctx.restore()
  },
}

function currencies(company?: ComparisonCompany): MetricCurrencies {
  return { price: company?.market?.price_currency, reporting: company?.reporting_currency }
}

function tick(definition: MetricDefinition, company?: ComparisonCompany) {
  return (value: number | string) => abbreviate(formatMetricValue(definition, Number(value), currencies(company)))
}

function withAlpha(hex: string, alpha: number) {
  const value = Number.parseInt(hex.slice(1), 16)
  return `rgb(${(value >> 16) & 255} ${(value >> 8) & 255} ${value & 255} / ${alpha})`
}

function ChartPanel({ title, meta, controls, children }: { title: string; meta?: string; controls?: ReactNode; children: ReactNode }) {
  return <section className="panel cmp-chart">
    <header className="cmp-panel__header">
      <h3>{title}</h3>
      {meta && <span className="cmp-panel__meta">{meta}</span>}
      {controls}
    </header>
    {children}
  </section>
}

export function RankingChart({ companies, metric, definitions, colorIndex }: {
  companies: ComparisonCompany[]
  metric: string | null
  definitions: Record<string, MetricDefinition>
  colorIndex: Record<string, number>
}) {
  const definition = metric ? metricDefinition(metric, definitions) : undefined
  const ranked = metric && definition ? sortCompanies(companies, metric, definition.direction).filter(company => company.metrics[metric] != null) : []
  const values = ranked.map(company => company.metrics[metric ?? ''] as number)
  const middle = median(values)
  const mixedCurrency = definition?.format === 'money' && new Set(ranked.map(company => currencies(company)[definition.currency ?? 'reporting'])).size > 1
  const order = definition?.direction === 'lower' ? 'lowest first' : definition?.direction === 'higher' ? 'highest first' : 'largest first'
  const options: ChartOptions<'bar'> = {
    indexAxis: 'y',
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    plugins: {
      legend: { display: false },
      tooltip: { callbacks: { label: item => formatMetricValue(definition, Number(item.raw), currencies(ranked[item.dataIndex])) } },
      ...({ comparisonMedians: { x: ranked.length >= 3 ? middle : null } } as object),
    },
    scales: {
      x: { grid: { color: 'rgb(228 223 211 / 80%)' }, ticks: { maxTicksLimit: 5, callback: definition ? tick(definition, ranked[0]) : undefined } },
      y: { grid: { display: false }, ticks: { autoSkip: false, font: { size: 10 } } },
    },
  }
  return <ChartPanel title={definition ? definition.label : 'Ranking'} meta={definition ? `Ranking, ${order}${ranked.length >= 3 ? ' · dashed line is the median' : ''}` : undefined}>
    {!definition || ranked.length < 2
      ? <p className="cmp-panel__empty">{definition ? `Fewer than two companies report ${definition.label}.` : 'Move through the table (J/K) to rank companies by a metric.'}</p>
      : <div className="cmp-chart__canvas" style={{ height: `${Math.max(240, ranked.length * 22 + 36)}px` }} role="img" aria-label={`${definition.label}, ${order}: ${ranked.map(company => `${companyName(company)} ${formatMetricValue(definition, company.metrics[metric ?? ''], currencies(company))}`).join('; ')}`}>
        <Bar
          data={{
            labels: ranked.map(company => shortName(companyName(company))),
            datasets: [{
              data: values,
              backgroundColor: ranked.map(company => { const style = seriesStyle(colorIndex[company.company_code] ?? 0); return style.dashed ? withAlpha(style.color, .4) : style.color }),
              borderColor: ranked.map(company => seriesStyle(colorIndex[company.company_code] ?? 0).color),
              borderWidth: 1,
              barThickness: 14,
            }],
          }}
          options={options}
          plugins={[medianGuides as Plugin<'bar'>]}
        />
      </div>}
    {mixedCurrency && <p className="cmp-panel__note">Amounts are in different currencies.</p>}
  </ChartPanel>
}

export function ScatterChart({ companies, axes, choices, definitions, colorIndex, onAxes }: {
  companies: ComparisonCompany[]
  axes: { x: string; y: string }
  choices: string[]
  definitions: Record<string, MetricDefinition>
  colorIndex: Record<string, number>
  onAxes: (axes: { x: string; y: string }) => void
}) {
  const x = metricDefinition(axes.x, definitions)
  const y = metricDefinition(axes.y, definitions)
  const plotted = companies.filter(company => company.metrics[axes.x] != null && company.metrics[axes.y] != null)
  const missing = companies.length - plotted.length
  const select = (axis: 'x' | 'y', label: string) => <label className="cmp-chart__select">{label}
    <select className="select" aria-label={`${label} axis metric`} value={axes[axis]} onChange={event => onAxes({ ...axes, [axis]: event.target.value })}>
      {choices.map(metric => <option key={metric} value={metric}>{metricDefinition(metric, definitions).label}</option>)}
    </select>
  </label>
  const options: ChartOptions<'scatter'> = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    layout: { padding: { right: 8, top: 4 } },
    plugins: {
      legend: { display: false },
      tooltip: { callbacks: { label: item => `${companyName(plotted[item.datasetIndex])}: ${x.label} ${formatMetricValue(x, (item.raw as { x: number }).x, currencies(plotted[item.datasetIndex]))}, ${y.label} ${formatMetricValue(y, (item.raw as { y: number }).y, currencies(plotted[item.datasetIndex]))}` } },
      ...({ comparisonMedians: plotted.length >= 3 ? { x: median(plotted.map(company => company.metrics[axes.x])), y: median(plotted.map(company => company.metrics[axes.y])) } : undefined } as object),
    },
    scales: {
      x: { title: { display: true, text: x.label, font: { size: 10 } }, grid: { color: 'rgb(228 223 211 / 80%)' }, ticks: { maxTicksLimit: 6, callback: tick(x, plotted[0]) } },
      y: { title: { display: true, text: y.label, font: { size: 10 } }, grid: { color: 'rgb(228 223 211 / 80%)' }, ticks: { maxTicksLimit: 6, callback: tick(y, plotted[0]) } },
    },
  }
  return <ChartPanel title="Scatter" meta={plotted.length >= 3 ? 'Dashed lines are the medians' : undefined} controls={<span className="cmp-chart__controls">{select('x', 'X')}{select('y', 'Y')}</span>}>
    {plotted.length < 2
      ? <p className="cmp-panel__empty">Fewer than two companies have both {x.label} and {y.label}.</p>
      : <div className="cmp-chart__canvas cmp-chart__canvas--square" role="img" aria-label={`${y.label} against ${x.label}: ${plotted.map(company => `${companyName(company)} ${formatMetricValue(x, company.metrics[axes.x], currencies(company))}, ${formatMetricValue(y, company.metrics[axes.y], currencies(company))}`).join('; ')}`}>
        <Scatter
          data={{
            datasets: plotted.map(company => {
              const style = seriesStyle(colorIndex[company.company_code] ?? 0)
              return {
                label: shortName(companyName(company)),
                data: [{ x: company.metrics[axes.x] as number, y: company.metrics[axes.y] as number }],
                backgroundColor: style.color,
                borderColor: style.color,
                pointStyle: style.dashed ? 'triangle' : 'circle',
                pointRadius: 5,
                pointHoverRadius: 7,
              }
            }),
          }}
          options={options}
          plugins={[medianGuides as Plugin<'scatter'>, pointLabels]}
        />
      </div>}
    {missing > 0 && plotted.length >= 2 && <p className="cmp-panel__note">{missing} {missing === 1 ? 'company lacks' : 'companies lack'} one of these values.</p>}
  </ChartPanel>
}

export function TrendChart({ companies, trends, loading, metric, indexed, colorIndex, onMetric, onIndexed }: {
  companies: ComparisonCompany[]
  trends?: TrendsResponse
  loading: boolean
  metric: string
  indexed: boolean
  colorIndex: Record<string, number>
  onMetric: (metric: string) => void
  onIndexed: (indexed: boolean) => void
}) {
  const definitions = trends?.metric_definitions ?? {}
  const definition = metricDefinition(metric, definitions)
  const canIndex = definition.format === 'money' || !definitions[metric]?.direction && definition.format !== 'percent'
  const useIndex = indexed && canIndex
  const byCode = new Map(companies.map(company => [company.company_code, company]))
  const aligned = alignTrends(trends?.companies ?? [], metric, companies.map(company => company.company_code))
  const rows = aligned.rows.filter(row => row.values.some(value => value != null))
  const mixedCurrency = definition.format === 'money' && new Set(companies.map(company => company.reporting_currency).filter(Boolean)).size > 1
  const valueTick = useIndex ? (value: number | string) => Number(value).toFixed(0) : tick(definition, companies[0])
  const options: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { position: 'bottom', labels: { font: { size: 10 }, usePointStyle: true, pointStyle: 'line' } },
      tooltip: { itemSort: (a, b) => Number(b.raw) - Number(a.raw), callbacks: { label: item => `${item.dataset.label}: ${useIndex ? Number(item.raw).toFixed(0) : formatMetricValue(definition, Number(item.raw), currencies(byCode.get(rows[item.datasetIndex]?.code ?? '')))}` } },
    },
    scales: {
      x: { grid: { display: false } },
      y: { grid: { color: 'rgb(228 223 211 / 80%)' }, ticks: { maxTicksLimit: 6, callback: valueTick } },
    },
  }
  return <ChartPanel
    title="Trend"
    meta={useIndex ? 'First year = 100' : 'By fiscal year end'}
    controls={<span className="cmp-chart__controls">
      <select className="select" aria-label="Trend metric" value={metric} onChange={event => onMetric(event.target.value)}>
        {(trends?.metrics ?? [metric]).map(key => <option key={key} value={key}>{metricDefinition(key, definitions).label}</option>)}
      </select>
      <label className="inline-toggle" title="Rebase each company to 100 in its first year, to compare growth (I)"><input type="checkbox" checked={useIndex} disabled={!canIndex} onChange={event => onIndexed(event.target.checked)} />Index</label>
    </span>}
  >
    {loading && !trends ? <p className="cmp-panel__empty">Loading statement history…</p>
      : rows.length === 0 ? <p className="cmp-panel__empty">No yearly {definition.label} values in the stored filings.</p>
        : <div className="cmp-chart__canvas" style={{ height: '250px' }} role="img" aria-label={`${definition.label} by fiscal year for ${rows.length} companies`}>
          <Line
            data={{
              labels: aligned.years.map(String),
              datasets: rows.map(row => {
                const company = byCode.get(row.code)
                const style = seriesStyle(colorIndex[row.code] ?? 0)
                return {
                  label: company ? shortName(companyName(company)) : row.code,
                  data: useIndex ? indexToHundred(row.values) : row.values,
                  borderColor: style.color,
                  backgroundColor: style.color,
                  borderDash: style.dashed ? [5, 3] : undefined,
                  pointRadius: 1.5,
                  pointHoverRadius: 4,
                  spanGaps: true,
                }
              }),
            }}
            options={options}
          />
        </div>}
    {mixedCurrency && !useIndex && <p className="cmp-panel__note">Amounts are in each company's own currency; Index compares growth instead.</p>}
  </ChartPanel>
}
