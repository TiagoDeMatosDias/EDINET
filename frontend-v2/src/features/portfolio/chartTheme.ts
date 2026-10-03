import { BRAND_COLORS, SERIES_COLORS } from '../../brand'

/** The portfolio and its benchmark: a two-colour pair checked for colour-vision separation on the paper surface. */
export const PORTFOLIO_COLOR = SERIES_COLORS[0]
export const BENCHMARK_COLOR = '#BE8410'
/** Inflation and invested capital are reference lines, not series: neutral and dashed. */
export const REFERENCE_COLOR = BRAND_COLORS.stone
export const GAIN_COLOR = SERIES_COLORS[0]
export const LOSS_COLOR = SERIES_COLORS[1]
export const GRID_COLOR = 'rgb(228 223 211 / 80%)'

/**
 * The diverging scale for returns: vermilion for losses, a paper midpoint, indigo
 * for gains, saturating at ±cap. Light cells keep ink text, dark cells white.
 */
export function divergingColor(value: number | null | undefined, cap = 0.08) {
  if (value == null || !Number.isFinite(value)) return undefined
  const strength = Math.min(Math.abs(value) / cap, 1)
  const [r, g, b] = value >= 0 ? [52, 96, 168] : [196, 70, 44]
  const mid = [236, 232, 223]
  const mix = (end: number, start: number) => Math.round(start + (end - start) * strength)
  return { background: `rgb(${mix(r, mid[0])} ${mix(g, mid[1])} ${mix(b, mid[2])})`, ink: strength > 0.55 ? '#fff' : BRAND_COLORS.ink }
}
