import type { Chart, Plugin } from 'chart.js'

import { BRAND_COLORS } from '../../brand'

/** A vertical hairline at the hovered position, so readers aim at a date rather than a 2px line. */
export const crosshairPlugin: Plugin<'line'> = {
  id: 'analysisCrosshair',
  afterDatasetsDraw(chart: Chart<'line'>) {
    const active = chart.tooltip?.getActiveElements?.() ?? []
    if (!active.length) return
    const x = active[0].element.x
    const { top, bottom } = chart.chartArea
    const { ctx } = chart
    ctx.save()
    ctx.strokeStyle = BRAND_COLORS.stone
    ctx.globalAlpha = .55
    ctx.lineWidth = 1
    ctx.beginPath()
    ctx.moveTo(x, top)
    ctx.lineTo(x, bottom)
    ctx.stroke()
    ctx.restore()
  },
}

/** A dashed horizontal guide at ``options.plugins.referenceLine.value`` (the price where the range starts). */
export const referenceLinePlugin: Plugin<'line'> = {
  id: 'referenceLine',
  afterDraw(chart: Chart<'line'>) {
    const options = (chart.options.plugins as Record<string, { value?: number | null } | undefined>)?.referenceLine
    const value = options?.value
    const scale = chart.scales.y
    if (value == null || !scale) return
    const y = scale.getPixelForValue(value)
    const { left, right, top, bottom } = chart.chartArea
    if (y < top || y > bottom) return
    const { ctx } = chart
    ctx.save()
    ctx.strokeStyle = BRAND_COLORS.stone
    ctx.globalAlpha = .6
    ctx.setLineDash([3, 3])
    ctx.lineWidth = 1
    ctx.beginPath()
    ctx.moveTo(left, y)
    ctx.lineTo(right, y)
    ctx.stroke()
    ctx.restore()
  },
}
