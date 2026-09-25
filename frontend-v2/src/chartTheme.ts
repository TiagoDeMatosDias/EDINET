import { BarElement, Chart as ChartJS, Legend, LineElement, Tooltip } from 'chart.js'

import { BRAND_COLORS, CHART_GRID_COLOR, FONT_MONO } from './brand'

// Elements and plugins are registered here so their defaults exist before they are themed; later registrations are no-ops.
ChartJS.register(LineElement, BarElement, Tooltip, Legend)

// Quiet chart defaults: stone ticks in the figure face, hairline grid, thin lines.
ChartJS.defaults.color = BRAND_COLORS.stone
ChartJS.defaults.borderColor = CHART_GRID_COLOR
ChartJS.defaults.font.family = FONT_MONO
ChartJS.defaults.font.size = 11
// Figures are monospaced and wide, so leave clear air between auto-skipped axis labels.
ChartJS.defaults.set('scale', { ticks: { autoSkipPadding: 16 } })
ChartJS.defaults.elements.line.borderWidth = 1.5
ChartJS.defaults.elements.bar.borderWidth = 0
ChartJS.defaults.plugins.tooltip.backgroundColor = BRAND_COLORS.ink
ChartJS.defaults.plugins.tooltip.titleColor = BRAND_COLORS.paper
ChartJS.defaults.plugins.tooltip.bodyColor = BRAND_COLORS.paper
ChartJS.defaults.plugins.tooltip.cornerRadius = 2
ChartJS.defaults.plugins.legend.labels.boxWidth = 10
ChartJS.defaults.plugins.legend.labels.boxHeight = 10

// Canvas text does not wait for web fonts: re-measure every chart once the figure face has loaded.
if (typeof document !== 'undefined' && document.fonts) {
  void document.fonts.load(`11px ${FONT_MONO}`).then(() => {
    Object.values(ChartJS.instances).forEach(chart => chart.update('none'))
  })
}
