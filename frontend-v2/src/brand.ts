export const BRAND_COLORS = {
  ink: '#1C1B19',
  indigo: '#2B3A55',
  vermilion: '#B0301F',
  ochre: '#9A6B2F',
  stone: '#6B675F',
  paper: '#F3F0E8',
} as const

export const BRAND_CHART_COLORS = [
  BRAND_COLORS.indigo,
  BRAND_COLORS.vermilion,
  BRAND_COLORS.ink,
  BRAND_COLORS.ochre,
  '#7D8BA6',
  '#CC8574',
  '#5F7466',
  '#A99A7C',
  '#44506B',
  '#7A3A2E',
] as const

/** Gains are indigo and losses vermilion; charts and tables also carry a sign, never colour alone. */
export const SEMANTIC_CHART_COLORS = {
  positive: BRAND_COLORS.indigo,
  negative: BRAND_COLORS.vermilion,
  neutral: BRAND_COLORS.stone,
} as const

export const CHART_GRID_COLOR = '#E4DFD3'
export const FONT_SANS = "'Zen Kaku Gothic New', ui-sans-serif, system-ui, sans-serif"
export const FONT_MONO = "'IBM Plex Mono', ui-monospace, SFMono-Regular, Consolas, monospace"

/**
 * Categorical series colours for multi-series charts, in fixed assignment order: indigo,
 * vermilion, teal, ochre, plum, moss. Checked against the paper surface for lightness band,
 * chroma, colour-vision-deficiency separation, and contrast; keep this order when charting.
 */
export const SERIES_COLORS = ['#3460A8', '#C4462C', '#13917A', '#BE8410', '#A9568F', '#55891F'] as const
