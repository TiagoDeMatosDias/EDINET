import { BarElement, CategoryScale, Chart as ChartJS, Filler, Legend, LinearScale, LineElement, PointElement, Tooltip, type ChartOptions, type TooltipItem } from 'chart.js'

import { CHART_GRID_COLOR, FONT_MONO, FONT_SANS, SERIES_COLORS } from '../../brand'

ChartJS.register(BarElement, CategoryScale, Filler, Legend, LinearScale, LineElement, PointElement, Tooltip)

export const PORTFOLIO_COLOR = SERIES_COLORS[0]
export const BENCHMARK_COLOR = '#6B675F'
export const NEGATIVE_COLOR = SERIES_COLORS[1]
export const DURATION_COLORS: Record<string, string> = { '1yr': SERIES_COLORS[0], '2yr': SERIES_COLORS[2], '3yr': SERIES_COLORS[3], '5yr': SERIES_COLORS[4], '10yr': SERIES_COLORS[5] }

const percentTick = (value: string | number) => `${Number(value).toFixed(0)}%`

/** Compact defaults: legend at the bottom, a right-hand percent axis, mono tick labels. */
export function percentOptions<T extends 'line' | 'bar'>(extra: { xLabels?: boolean; yMax?: number; yMin?: number; stacked?: boolean; onClick?: (index: number) => void; tooltipLabel?: (value: number, label: string) => string } = {}): ChartOptions<T> {
  const options: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    interaction: { mode: 'index', intersect: false },
    onClick: extra.onClick ? (_event, elements) => { if (elements[0]) extra.onClick?.(elements[0].index) } : undefined,
    plugins: {
      legend: { position: 'bottom', labels: { boxWidth: 10, boxHeight: 10, font: { family: FONT_SANS, size: 10 } } },
      tooltip: {
        callbacks: {
          label: (context: TooltipItem<'line'>) => {
            const value = Number(context.parsed.y)
            return extra.tooltipLabel ? extra.tooltipLabel(value, context.dataset.label ?? '') : `${context.dataset.label}: ${Number.isFinite(value) ? value.toFixed(1) : '—'}%`
          },
        },
      },
    },
    scales: {
      x: { display: extra.xLabels ?? false, stacked: extra.stacked, grid: { display: false }, ticks: { font: { family: FONT_MONO, size: 9 }, maxRotation: 0, autoSkipPadding: 12 } },
      y: { position: 'right', stacked: extra.stacked, max: extra.yMax, min: extra.yMin, grid: { color: CHART_GRID_COLOR }, ticks: { font: { family: FONT_MONO, size: 9 }, callback: percentTick } },
    },
  }
  return options as unknown as ChartOptions<T>
}

export const asPercent = (value: number | null | undefined) => value == null || !Number.isFinite(value) ? null : value * 100
