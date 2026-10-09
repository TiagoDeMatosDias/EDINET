import { useMemo } from 'react'
import { Bar, Line, Scatter } from 'react-chartjs-2'
import type { ChartOptions, TooltipItem } from 'chart.js'

import { BRAND_COLORS, CHART_GRID_COLOR, FONT_MONO, SERIES_COLORS } from '../../brand'
import { baseOptions } from '../research/pricing/pricingCharts'
import { formatBp, formatYen, RATING_GROUPS, ratingGroup, type RatingGroup } from './bondFormat'
import type { CurvePayload } from './bondTypes'

/** The company's own bonds and its subsidiaries': a pair checked for colour-vision separation on paper. */
const PARENT_COLOR = SERIES_COLORS[0]
const GROUP_COLOR = '#BE8410'
const NEUTRAL = '#9A958A'
/** Rating groups take the categorical colours in order; unrated bonds are neutral grey. */
const RATING_COLORS: Record<RatingGroup, string> = {
  AAA: SERIES_COLORS[0],
  AA: SERIES_COLORS[2],
  A: SERIES_COLORS[3],
  BBB: SERIES_COLORS[1],
  'Below BBB': SERIES_COLORS[4],
  'Not rated': NEUTRAL,
}

const tickFont = { size: 10 }

/** Amounts falling due each year, the company's own bonds and its subsidiaries' stacked. */
export function MaturityLadderChart({ ladder }: { ladder: Array<{ year: number; parent: number; group: number }> }) {
  const hasGroup = ladder.some(item => item.group > 0)
  const data = useMemo(() => ({
    labels: ladder.map(item => String(item.year)),
    datasets: [
      { label: 'Issued by the company', data: ladder.map(item => item.parent / 1e9), backgroundColor: PARENT_COLOR, borderColor: '#fff', borderWidth: hasGroup ? { top: 2 } : 0, borderRadius: hasGroup ? 0 : 3, stack: 'due', maxBarThickness: 28 },
      ...(hasGroup ? [{ label: 'Subsidiaries', data: ladder.map(item => item.group / 1e9), backgroundColor: GROUP_COLOR, borderColor: '#fff', borderWidth: 0, borderRadius: 3, stack: 'due', maxBarThickness: 28 }] : []),
    ],
  }), [ladder, hasGroup])
  const options: ChartOptions<'bar'> = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    plugins: {
      legend: { display: hasGroup, position: 'bottom', labels: { boxWidth: 10, boxHeight: 10, font: tickFont } },
      tooltip: { bodyFont: { family: FONT_MONO }, callbacks: { label: (item: TooltipItem<'bar'>) => `${item.dataset.label}: ${formatYen(Number(item.parsed.y) * 1e9)}` } },
    },
    scales: {
      x: { stacked: true, grid: { display: false }, ticks: { font: tickFont, maxRotation: 0, autoSkipPadding: 6 } },
      y: { stacked: true, beginAtZero: true, grid: { color: CHART_GRID_COLOR }, title: { display: true, text: '¥ billion due', font: tickFont }, ticks: { font: tickFont, maxTicksLimit: 6 } },
    },
  }
  return <figure className="rs-chart">
    <figcaption className="rs-chart__caption"><span>Maturity ladder</span><span className="rs-dim">yen bonds outstanding, by the year they fall due</span></figcaption>
    <div className="rs-chart__canvas bd-chart--short" role="img" aria-label="Bonds outstanding by year of maturity">
      <Bar data={data} options={options} />
    </div>
  </figure>
}

interface CurveBond { label: string; horizon: number; yield: number }

/** The government curve today and a year ago, with the company's bonds at today's constant-spread yield. */
export function CurveChart({ curve, bonds }: { curve: CurvePayload; bonds: CurveBond[] }) {
  const data = useMemo(() => ({
    datasets: [
      { type: 'line' as const, label: `JGB ${curve.date}`, data: curve.points.map(point => ({ x: point.tenor, y: point.yield * 100 })), borderColor: BRAND_COLORS.ink, backgroundColor: BRAND_COLORS.ink, borderWidth: 2, pointRadius: 0 },
      ...(curve.year_ago ? [{ type: 'line' as const, label: `JGB ${curve.year_ago.date}`, data: curve.year_ago.points.map(point => ({ x: point.tenor, y: point.yield * 100 })), borderColor: NEUTRAL, backgroundColor: NEUTRAL, borderWidth: 1.5, borderDash: [4, 3], pointRadius: 0 }] : []),
      { type: 'line' as const, label: 'This company’s bonds', data: bonds.map(bond => ({ x: bond.horizon, y: bond.yield * 100, label: bond.label })), showLine: false, borderColor: PARENT_COLOR, backgroundColor: PARENT_COLOR, pointBorderColor: '#fff', pointBorderWidth: 2, pointRadius: 5, pointHoverRadius: 7 },
    ],
  }), [curve, bonds])
  const options = useMemo(() => {
    const base = baseOptions({ x: 'Years to maturity (or first call)', y: 'Yield (%)', xTicks: value => String(value), yTicks: value => value.toFixed(1) })
    return {
      ...base,
      interaction: { mode: 'nearest' as const, intersect: true },
      scales: { ...base.scales, x: { ...base.scales?.x, min: 0, max: Math.min(40, Math.max(12, ...bonds.map(bond => Math.ceil(bond.horizon + 1)))) } },
      plugins: {
        ...base.plugins,
        tooltip: {
          ...base.plugins?.tooltip,
          callbacks: {
            title: (items: TooltipItem<'line'>[]) => (items[0]?.raw as { label?: string })?.label ?? `${(items[0]?.parsed.x ?? 0).toFixed(1)} years`,
            label: (item: TooltipItem<'line'>) => `${item.dataset.label}: ${(item.parsed.y ?? 0).toFixed(3)}% at ${(item.parsed.x ?? 0).toFixed(1)}y`,
          },
        },
      },
    }
  }, [bonds])
  return <figure className="rs-chart">
    <figcaption className="rs-chart__caption"><span>Against the government curve</span><span className="rs-dim">quoted bonds at their JSDA yield, others at today’s JGB yield plus their spread at issue</span></figcaption>
    <div className="rs-chart__canvas" role="img" aria-label="Japanese government bond yield curve with this company's bonds">
      <Line data={data as never} options={options as never} />
    </div>
  </figure>
}

export interface SpreadPoint {
  id: string
  horizon: number
  spread: number
  notch?: number | null
  title: string
  detail: string
}

function quantile(sorted: number[], share: number) {
  if (!sorted.length) return 0
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.round(share * (sorted.length - 1))))]
}

/** Axis limits around the middle 99% of bonds, so a few outliers do not flatten the rest; the selected bond always shows. */
function axisBounds(points: SpreadPoint[], chosen?: SpreadPoint) {
  const spreads = points.map(point => point.spread * 10_000).sort((a, b) => a - b)
  const horizons = points.map(point => point.horizon).sort((a, b) => a - b)
  let yMin = Math.min(0, Math.floor(quantile(spreads, 0.005) / 25) * 25)
  let yMax = Math.max(50, Math.ceil(quantile(spreads, 0.995) / 25) * 25 + 25)
  let xMax = Math.max(5, Math.ceil(quantile(horizons, 0.995)) + 1)
  if (chosen) {
    yMin = Math.min(yMin, Math.floor(chosen.spread * 10_000 / 25) * 25)
    yMax = Math.max(yMax, Math.ceil(chosen.spread * 10_000 / 25) * 25 + 25)
    xMax = Math.max(xMax, Math.ceil(chosen.horizon) + 1)
  }
  return { yMin, yMax, xMax }
}

/**
 * Spread at issue against years left, one dot per bond coloured by rating group.
 * The selected bond is drawn larger with an ink ring; clicking a dot selects it.
 */
export function SpreadScatterChart({ points, selected, onSelect, caption, note }: { points: SpreadPoint[]; selected?: string; onSelect?: (id: string) => void; caption: string; note?: string }) {
  const groups = useMemo(() => RATING_GROUPS.map(group => ({ group, points: points.filter(point => ratingGroup(point.notch) === group) })).filter(item => item.points.length), [points])
  const chosen = points.find(point => point.id === selected)
  const bounds = useMemo(() => axisBounds(points, chosen), [points, chosen])
  const hidden = points.filter(point => point.spread * 10_000 < bounds.yMin || point.spread * 10_000 > bounds.yMax || point.horizon > bounds.xMax).length
  const data = useMemo(() => ({
    datasets: [
      ...groups.map(({ group, points: members }) => ({
        label: group,
        data: members.map(point => ({ x: point.horizon, y: point.spread * 10_000, point })),
        backgroundColor: RATING_COLORS[group],
        borderColor: '#fff',
        borderWidth: 1,
        order: 1,
        pointRadius: members.length > 600 ? 2.5 : 3.5,
        pointHoverRadius: 6,
      })),
      ...(chosen ? [{ label: 'Selected', data: [{ x: chosen.horizon, y: chosen.spread * 10_000, point: chosen }], backgroundColor: RATING_COLORS[ratingGroup(chosen.notch)], borderColor: BRAND_COLORS.ink, borderWidth: 2.5, pointRadius: 8, pointHoverRadius: 9, order: 0 }] : []),
    ],
  }), [groups, chosen])
  const options: ChartOptions<'scatter'> = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    interaction: { mode: 'nearest', intersect: true },
    onClick: (_event, elements, chart) => {
      const element = elements[0]
      if (!element || !onSelect) return
      const raw = chart.data.datasets[element.datasetIndex]?.data[element.index] as { point?: SpreadPoint } | undefined
      if (raw?.point) onSelect(raw.point.id)
    },
    plugins: {
      legend: { position: 'bottom', labels: { boxWidth: 8, boxHeight: 8, usePointStyle: true, font: tickFont, filter: item => item.text !== 'Selected' } },
      tooltip: {
        titleFont: { family: FONT_MONO },
        bodyFont: { family: FONT_MONO },
        callbacks: {
          title: items => (items[0]?.raw as { point?: SpreadPoint })?.point?.title ?? '',
          label: item => {
            const point = (item.raw as { point?: SpreadPoint }).point
            return point ? [point.detail, `${formatBp(point.spread)} at issue · ${point.horizon.toFixed(1)}y left`] : ''
          },
        },
      },
    },
    scales: {
      x: { type: 'linear', min: 0, max: bounds.xMax, title: { display: true, text: 'Years left (to first call for callable bonds)', font: tickFont }, grid: { color: CHART_GRID_COLOR }, ticks: { font: tickFont, maxTicksLimit: 9 } },
      y: { min: bounds.yMin, max: bounds.yMax, title: { display: true, text: 'Spread over JGBs (bp)', font: tickFont }, grid: { color: CHART_GRID_COLOR }, ticks: { font: tickFont, maxTicksLimit: 7 } },
    },
  }
  return <figure className="rs-chart">
    <figcaption className="rs-chart__caption"><span>{caption}</span><span className="rs-dim">{[note, hidden ? `${hidden} outlying ${hidden === 1 ? 'bond' : 'bonds'} off the chart` : ''].filter(Boolean).join(' · ')}</span></figcaption>
    <div className="rs-chart__canvas bd-chart--tall" role="img" aria-label={`${caption}: spread over JGBs by years left, coloured by rating`}>
      <Scatter data={data} options={options} />
    </div>
  </figure>
}

/** Comparable bonds' spreads, the issuer's other bonds, this bond, and the peer median used for fair value. */
export function PeerSpreadChart({ peers, issuer, target, median, window }: {
  peers: Array<{ horizon: number; spread: number; company_name: string; rating: string | null; quoted: boolean }>
  issuer: Array<{ horizon: number; spread: number }>
  target: { horizon: number; spread: number | null; label: string }
  median: number | null
  window: number | null
}) {
  const data = useMemo(() => ({
    datasets: [
      { label: 'Comparable bonds', data: peers.map(peer => ({ x: peer.horizon, y: peer.spread * 10_000, title: `${peer.company_name}${peer.rating ? ` · ${peer.rating}` : ''}${peer.quoted ? '' : ' · at issue'}` })), showLine: false, backgroundColor: 'rgb(154 149 138 / 55%)', borderColor: 'rgb(154 149 138 / 0%)', pointRadius: 2.5, pointHoverRadius: 5, order: 3 },
      ...(issuer.length ? [{ label: 'Same issuer', data: issuer.map(item => ({ x: item.horizon, y: item.spread * 10_000, title: 'Same issuer' })), showLine: false, backgroundColor: PARENT_COLOR, borderColor: '#fff', borderWidth: 1.5, pointRadius: 4.5, pointHoverRadius: 6, order: 1 }] : []),
      ...(median != null && window != null ? [{ label: 'Peer median', data: [{ x: Math.max(0, target.horizon - window), y: median * 10_000, title: 'Peer median' }, { x: target.horizon + window, y: median * 10_000, title: 'Peer median' }], showLine: true, borderColor: BRAND_COLORS.ink, backgroundColor: BRAND_COLORS.ink, borderWidth: 2, borderDash: [5, 3], pointRadius: 0, order: 2 }] : []),
      ...(target.spread != null ? [{ label: 'This bond', data: [{ x: target.horizon, y: target.spread * 10_000, title: target.label }], showLine: false, backgroundColor: GROUP_COLOR, borderColor: BRAND_COLORS.ink, borderWidth: 2, pointRadius: 7, pointHoverRadius: 8, order: 0 }] : []),
    ],
  }), [peers, issuer, target, median, window])
  const bounds = useMemo(() => axisBounds(
    [...peers, ...issuer].map(item => ({ id: '', horizon: item.horizon, spread: item.spread, title: '', detail: '' })),
    target.spread != null ? { id: '', horizon: target.horizon, spread: target.spread, title: '', detail: '' } : undefined,
  ), [peers, issuer, target])
  const options = useMemo(() => {
    const base = baseOptions({ x: 'Years left', y: 'Spread over JGBs (bp)', xTicks: value => String(value), yTicks: value => value.toFixed(0) })
    return {
      ...base,
      interaction: { mode: 'nearest' as const, intersect: true },
      scales: { ...base.scales, x: { ...base.scales?.x, min: 0, max: bounds.xMax }, y: { ...base.scales?.y, min: bounds.yMin, max: bounds.yMax } },
      plugins: {
        ...base.plugins,
        legend: { position: 'bottom' as const, labels: { boxWidth: 8, boxHeight: 8, usePointStyle: true, font: tickFont } },
        tooltip: { ...base.plugins?.tooltip, callbacks: { title: (items: TooltipItem<'line'>[]) => (items[0]?.raw as { title?: string })?.title ?? '', label: (item: TooltipItem<'line'>) => `${Math.round(item.parsed.y ?? 0)} bp · ${(item.parsed.x ?? 0).toFixed(1)}y` } },
      },
    }
  }, [bounds])
  return <figure className="rs-chart">
    <figcaption className="rs-chart__caption"><span>Spread against comparable bonds</span><span className="rs-dim">same ranking, rating within a notch; JSDA spreads where quoted; the dashed line is the median used for fair value</span></figcaption>
    <div className="rs-chart__canvas" role="img" aria-label="This bond's spread at issue against comparable bonds">
      <Line data={data as never} options={options as never} />
    </div>
  </figure>
}

/** The JSDA reference price day by day, from the files the bond step has read. */
export function MarketHistoryChart({ history }: { history: Array<{ date: string; price: number; yield: number | null }> }) {
  const data = useMemo(() => ({
    labels: history.map(point => point.date),
    datasets: [{ label: 'Reference price', data: history.map(point => point.price), borderColor: PARENT_COLOR, backgroundColor: PARENT_COLOR, borderWidth: 2, pointRadius: history.length > 40 ? 0 : 2 }],
  }), [history])
  const options: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: { bodyFont: { family: FONT_MONO }, callbacks: { label: item => { const point = history[item.dataIndex]; return `${point.price.toFixed(2)}${point.yield != null ? ` · yield ${(point.yield * 100).toFixed(3)}%` : ''}` } } },
    },
    scales: {
      x: { grid: { display: false }, ticks: { font: tickFont, maxTicksLimit: 6, maxRotation: 0 } },
      y: { grid: { color: CHART_GRID_COLOR }, title: { display: true, text: 'Price', font: tickFont }, ticks: { font: tickFont, maxTicksLimit: 5 } },
    },
  }
  return <figure className="rs-chart">
    <figcaption className="rs-chart__caption"><span>Reference price</span><span className="rs-dim">JSDA, per 100 of face value</span></figcaption>
    <div className="rs-chart__canvas bd-chart--short" role="img" aria-label="JSDA reference price over time">
      <Line data={data} options={options} />
    </div>
  </figure>
}
