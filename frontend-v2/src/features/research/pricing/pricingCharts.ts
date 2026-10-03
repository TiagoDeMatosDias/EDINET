import {
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Filler,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  Tooltip,
  type Chart,
  type ChartOptions,
  type Plugin,
} from 'chart.js'

import { BRAND_COLORS, CHART_GRID_COLOR, FONT_MONO } from '../../../brand'

ChartJS.register(BarElement, CategoryScale, Filler, Legend, LinearScale, LineElement, PointElement, Tooltip)

export interface Marker { value: number; label: string; color?: string; dashed?: boolean }
export interface Band { from: number; to: number; label?: string }
export interface MarkerOptions { markers?: Marker[]; band?: Band | null; zeroLine?: boolean }

/** Vertical reference lines (spot, strike, breakevens), an optional shaded x-range, and a zero line. */
export const markersPlugin: Plugin<'line'> = {
  id: 'pricingMarkers',
  beforeDatasetsDraw(chart: Chart<'line'>) {
    const options = (chart.options.plugins as Record<string, MarkerOptions | undefined>)?.pricingMarkers
    const band = options?.band
    const x = chart.scales.x
    if (!band || !x) return
    const { ctx, chartArea: area } = chart
    const left = Math.max(x.getPixelForValue(band.from), area.left)
    const right = Math.min(x.getPixelForValue(band.to), area.right)
    if (right <= left) return
    ctx.save()
    ctx.fillStyle = 'rgb(52 96 168 / 7%)'
    ctx.fillRect(left, area.top, right - left, area.bottom - area.top)
    if (band.label) {
      ctx.fillStyle = BRAND_COLORS.stone
      ctx.font = `10px ${ChartJS.defaults.font.family}`
      ctx.textAlign = 'center'
      ctx.fillText(band.label, (left + right) / 2, area.bottom - 4)
    }
    ctx.restore()
  },
  afterDatasetsDraw(chart: Chart<'line'>) {
    const options = (chart.options.plugins as Record<string, MarkerOptions | undefined>)?.pricingMarkers
    if (!options) return
    const { ctx, chartArea: area } = chart
    const x = chart.scales.x
    const y = chart.scales.y
    ctx.save()
    if (options.zeroLine && y) {
      const zero = y.getPixelForValue(0)
      if (zero >= area.top && zero <= area.bottom) {
        ctx.strokeStyle = BRAND_COLORS.ink
        ctx.globalAlpha = .45
        ctx.lineWidth = 1
        ctx.beginPath(); ctx.moveTo(area.left, zero); ctx.lineTo(area.right, zero); ctx.stroke()
        ctx.globalAlpha = 1
      }
    }
    ctx.font = `10px ${ChartJS.defaults.font.family}`
    ctx.textBaseline = 'top'
    const placed: Array<{ x: number; row: number }> = []
    for (const marker of options.markers ?? []) {
      if (!x) break
      const position = x.getPixelForValue(marker.value)
      if (position < area.left || position > area.right) continue
      ctx.strokeStyle = marker.color ?? BRAND_COLORS.stone
      ctx.fillStyle = marker.color ?? BRAND_COLORS.stone
      ctx.setLineDash(marker.dashed === false ? [] : [3, 3])
      ctx.lineWidth = 1
      ctx.beginPath(); ctx.moveTo(position, area.top); ctx.lineTo(position, area.bottom); ctx.stroke()
      // Labels that would collide step down a line.
      let row = 0
      while (placed.some(other => other.row === row && Math.abs(other.x - position) < 56)) row += 1
      placed.push({ x: position, row })
      const width = ctx.measureText(marker.label).width
      const left = position + 3 + width > area.right ? position - 3 - width : position + 3
      ctx.fillText(marker.label, left, area.top + 2 + row * 11)
    }
    ctx.restore()
  },
}

export function baseOptions(axes: { x: string; y: string; xTicks?: (value: number) => string; yTicks?: (value: number) => string }): ChartOptions<'line'> {
  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    interaction: { mode: 'index', intersect: false },
    parsing: false,
    normalized: true,
    plugins: {
      legend: { position: 'bottom', labels: { boxWidth: 12, boxHeight: 2, font: { size: 10 } } },
      tooltip: { titleFont: { family: FONT_MONO }, bodyFont: { family: FONT_MONO } },
    },
    scales: {
      x: { type: 'linear', title: { display: true, text: axes.x, font: { size: 10 } }, grid: { color: CHART_GRID_COLOR }, ticks: { font: { size: 10 }, maxTicksLimit: 8, callback: value => axes.xTicks ? axes.xTicks(Number(value)) : String(value) } },
      y: { title: { display: true, text: axes.y, font: { size: 10 } }, grid: { color: CHART_GRID_COLOR }, ticks: { font: { size: 10 }, maxTicksLimit: 7, callback: value => axes.yTicks ? axes.yTicks(Number(value)) : String(value) } },
    },
  }
}

/** Short tick labels for amounts: 2.9k, 1.2M. */
export function shortAmount(value: number) {
  const magnitude = Math.abs(value)
  if (magnitude >= 1e6) return `${(value / 1e6).toFixed(magnitude >= 1e7 ? 0 : 1)}M`
  if (magnitude >= 1e4) return `${(value / 1e3).toFixed(0)}k`
  if (magnitude >= 1e3) return `${(value / 1e3).toFixed(1)}k`
  return Number(value.toPrecision(4)).toString()
}
