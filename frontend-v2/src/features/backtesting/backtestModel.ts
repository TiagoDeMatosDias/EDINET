/** Types, request builders, and the arithmetic behind the backtest charts. */

export type Mode = 'manual' | 'screen' | 'csv'
export type Allocation = 'weight' | 'shares' | 'value'
export interface Holding { id: string; ticker: string; mode: Allocation; value: number }

export interface Benchmark { ticker: string; label: string; detail: string; available: boolean; first_date: string | null; last_date: string | null; currency: string | null }
/** The benchmark select's value: a ticker, ``portfolio`` (your own), or empty for none. */
export const OWN_PORTFOLIO = 'portfolio'

export interface SavedBacktest {
  id: string
  created: string
  has_zip: boolean
  kind?: 'single' | 'rolling' | 'csv' | null
  title?: string | null
  subtitle?: string | null
  headline?: Record<string, number | null | undefined>
}

export type RunStatus = 'ok' | 'truncated' | 'no_data' | 'failed'
export interface RunRow {
  period: string
  weighting: string
  duration: string
  companies?: number
  /** How many companies the screen matched; ``companies`` were held. */
  matches?: number | null
  warnings?: string[]
  status: RunStatus
  start?: string | null
  end?: string | null
  requested_end?: string | null
  years?: number | null
  total_return?: number | null
  annualized_return?: number | null
  price_return?: number | null
  dividend_return?: number | null
  volatility?: number | null
  sharpe_ratio?: number | null
  max_drawdown?: number | null
  benchmark_total_return?: number | null
  benchmark_annualized_return?: number | null
  excess_return?: number | null
  excess_annualized_return?: number | null
  beat_benchmark?: boolean | null
}
/** ``[month-end date, portfolio cumulative return, benchmark cumulative return]``. */
export type PathPoint = [string, number | null, number | null]

export interface SetResult {
  id: string
  kind?: 'rolling' | 'csv'
  aggregate: Record<string, unknown>
  config?: Record<string, unknown>
  runs?: RunRow[]
  paths?: Record<string, PathPoint[]>
  period_holdings?: Record<string, string[]>
}

export interface ChartPoint { date: string; portfolio?: number | null; vami?: number | null; benchmark?: number | null; price_only?: number | null; dividend_only?: number | null; total?: number | null }
export interface SingleResult {
  id: string
  summary: Record<string, unknown>
  chart_data?: { cumulative?: ChartPoint[]; drawdown?: ChartPoint[]; decomposition?: ChartPoint[] }
  per_company?: Array<Record<string, unknown>>
  yearly_returns?: Array<Record<string, unknown>>
  warnings?: string[]
}

export interface RollingJob {
  job_id: string
  status: 'queued' | 'running' | 'saving' | 'complete' | 'failed' | 'cancelled'
  progress: { completed_backtests?: number; total_backtests?: number; period_index?: number; total_periods?: number; phase?: string; period?: string; status?: string }
  result_id: string | null
  error: string | null
  created_at: number
}

export interface ScreenDraft {
  name?: string
  criteria: Array<Record<string, unknown>>
  criteria_match: 'all' | 'any'
  columns: string[]
  computed_columns: Array<Record<string, unknown>>
  ranking_algorithm: string
  ranking_rules: Array<Record<string, unknown>>
}

export const DRAFT_KEY = 'shade.screening.draft'
export const DURATIONS = ['1yr', '2yr', '3yr', '5yr', '10yr'] as const
export const WEIGHTINGS: Array<[string, string]> = [['equal', 'Equal weight'], ['market_cap', 'Market cap']]

export function isSingle(result: unknown): result is SingleResult {
  return Boolean(result && typeof result === 'object' && 'summary' in result && (result as SingleResult).summary)
}

/** The Screening page's current draft, or null when it has no rules. */
export function readScreenDraft(): ScreenDraft | null {
  try {
    const draft = JSON.parse(localStorage.getItem(DRAFT_KEY) ?? 'null') as Partial<ScreenDraft> | null
    const criteria = (draft?.criteria ?? []).filter(item => item && item.enabled !== false)
    if (!draft || !criteria.length) return null
    return {
      name: draft.name,
      criteria: criteria.map(item => Object.fromEntries(Object.entries(item).filter(([key]) => key !== 'id'))),
      criteria_match: draft.criteria_match === 'any' ? 'any' : 'all',
      columns: draft.columns ?? [],
      computed_columns: draft.computed_columns ?? [],
      ranking_algorithm: draft.ranking_algorithm || 'none',
      ranking_rules: draft.ranking_rules ?? [],
    }
  } catch {
    return null
  }
}

/** The ``portfolio`` request field. Weights are typed in percent; the engine takes fractions. */
export function portfolioPayload(holdings: Holding[]) {
  const portfolio: Record<string, { mode: Allocation; value: number }> = {}
  for (const holding of holdings) {
    const ticker = holding.ticker.trim().toUpperCase()
    if (!ticker || !(holding.value > 0)) continue
    const value = holding.mode === 'weight' ? holding.value / 100 : holding.value
    const existing = portfolio[ticker]
    portfolio[ticker] = existing && existing.mode === holding.mode ? { mode: holding.mode, value: existing.value + value } : { mode: holding.mode, value }
  }
  return portfolio
}

export function weightTotal(holdings: Holding[]) {
  return holdings.filter(item => item.mode === 'weight' && item.ticker.trim()).reduce((sum, item) => sum + (Number.isFinite(item.value) ? item.value : 0), 0)
}

export function benchmarkFields(choice: string) {
  return choice === OWN_PORTFOLIO ? { benchmark_mode: 'portfolio' as const, benchmark_ticker: '' } : { benchmark_mode: 'ticker' as const, benchmark_ticker: choice }
}

// ── formatting ────────────────────────────────────────────────────────────

export function finite(value: unknown): number | null {
  const number = typeof value === 'number' ? value : typeof value === 'string' && value.trim() ? Number(value) : NaN
  return Number.isFinite(number) ? number : null
}
export function pct(value: unknown, digits = 1, signed = false) {
  const number = finite(value)
  if (number == null) return '—'
  return `${signed && number > 0 ? '+' : ''}${(number * 100).toFixed(digits)}%`
}
export function dec(value: unknown, digits = 2) {
  const number = finite(value)
  return number == null ? '—' : number.toFixed(digits)
}
export function tone(value: unknown) {
  const number = finite(value)
  return number == null || number === 0 ? '' : number > 0 ? 'is-up' : 'is-down'
}
export function month(period: string) { return period.slice(0, 7) }

/** Saved-result ids are ``YYYYMMDD_HHMMSS_xxxxxxxx``. */
export function savedWhen(id: string) {
  const match = /^(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})/.exec(id)
  return match ? `${match[1]}-${match[2]}-${match[3]} ${match[4]}:${match[5]}` : id
}

// ── single-run analytics ──────────────────────────────────────────────────

/** Calendar-year returns of the portfolio and benchmark from their cumulative series. */
export function calendarYears(points: ChartPoint[]) {
  const years: Array<{ year: string; portfolio: number | null; benchmark: number | null }> = []
  let prevPortfolio = 1
  let prevBenchmark: number | null = null
  let benchmarkBase: number | null = null
  for (let index = 0; index < points.length; index += 1) {
    const point = points[index]
    const next = points[index + 1]
    if (point.benchmark != null && benchmarkBase == null) benchmarkBase = 1
    if (next && next.date.slice(0, 4) === point.date.slice(0, 4)) continue
    const portfolio = point.portfolio == null ? null : 1 + point.portfolio
    const benchmark = point.benchmark == null ? null : 1 + point.benchmark
    years.push({
      year: point.date.slice(0, 4),
      portfolio: portfolio == null ? null : portfolio / prevPortfolio - 1,
      benchmark: benchmark == null ? null : benchmark / (prevBenchmark ?? benchmarkBase ?? 1) - 1,
    })
    if (portfolio != null) prevPortfolio = portfolio
    if (benchmark != null) prevBenchmark = benchmark
  }
  return years
}

/** Monthly returns as ``{year: [12 values]}`` from a daily cumulative series. */
export function monthlyReturns(points: ChartPoint[]) {
  const table = new Map<string, Array<number | null>>()
  let previous = 1
  for (let index = 0; index < points.length; index += 1) {
    const point = points[index]
    const next = points[index + 1]
    if (next && next.date.slice(0, 7) === point.date.slice(0, 7)) continue
    if (point.portfolio == null) continue
    const level = 1 + point.portfolio
    const year = point.date.slice(0, 4)
    const row = table.get(year) ?? Array<number | null>(12).fill(null)
    row[Number(point.date.slice(5, 7)) - 1] = level / previous - 1
    table.set(year, row)
    previous = level
  }
  return [...table.entries()].map(([year, months]) => ({ year, months, total: months.reduce<number>((product, value) => product * (1 + (value ?? 0)), 1) - 1 }))
}

// ── rolling-run analytics ─────────────────────────────────────────────────

export const runKey = (row: Pick<RunRow, 'period' | 'weighting' | 'duration'>) => `${row.period}|${row.weighting}|${row.duration}`

export function quantile(sorted: number[], q: number) {
  if (!sorted.length) return NaN
  const position = (sorted.length - 1) * q
  const lower = Math.floor(position)
  const upper = Math.ceil(position)
  return sorted[lower] + (sorted[upper] - sorted[lower]) * (position - lower)
}

export interface Summary { count: number; mean: number | null; median: number | null; best: number | null; worst: number | null; positive: number | null; winRate: number | null; excess: number | null; sharpe: number | null; drawdown: number | null; benchmark: number | null }

/** Statistics over complete runs only; truncated and empty runs are counted elsewhere. */
export function summarize(rows: RunRow[]): Summary {
  const complete = rows.filter(row => row.status === 'ok' && finite(row.annualized_return) != null)
  const values = complete.map(row => row.annualized_return as number).sort((a, b) => a - b)
  const mean = (list: number[]) => list.length ? list.reduce((sum, value) => sum + value, 0) / list.length : null
  const pick = (key: keyof RunRow) => complete.map(row => finite(row[key])).filter((value): value is number => value != null)
  const compared = complete.filter(row => row.beat_benchmark != null)
  return {
    count: complete.length,
    mean: mean(values),
    median: values.length ? quantile(values, 0.5) : null,
    best: values.length ? values[values.length - 1] : null,
    worst: values.length ? values[0] : null,
    positive: values.length ? values.filter(value => value > 0).length / values.length : null,
    winRate: compared.length ? compared.filter(row => row.beat_benchmark).length / compared.length : null,
    excess: mean(pick('excess_annualized_return')),
    sharpe: mean(pick('sharpe_ratio')),
    drawdown: mean(pick('max_drawdown')),
    benchmark: mean(pick('benchmark_annualized_return')),
  }
}

/** Histogram bins of ``width`` covering both series. */
export function histogram(portfolio: number[], benchmark: number[], width = 0.05) {
  const all = [...portfolio, ...benchmark]
  if (!all.length) return []
  const low = Math.floor(Math.min(...all) / width) * width
  const high = Math.ceil(Math.max(...all) / width + 1e-9) * width
  const count = Math.max(1, Math.min(80, Math.round((high - low) / width)))
  const bins = Array.from({ length: count }, (_, index) => ({ from: low + index * width, to: low + (index + 1) * width, portfolio: 0, benchmark: 0 }))
  const place = (value: number) => Math.min(count - 1, Math.max(0, Math.floor((value - low) / width)))
  portfolio.forEach(value => { bins[place(value)].portfolio += 1 })
  benchmark.forEach(value => { bins[place(value)].benchmark += 1 })
  return bins
}

/**
 * Percentile bands of growth by months since the start, over every run's path.
 * Values are growth multiples (1 = flat).
 */
export function fanBands(paths: PathPoint[][]) {
  const longest = Math.max(0, ...paths.map(path => path.length))
  const bands: Array<{ month: number; p10: number; p25: number; p50: number; p75: number; p90: number; bench: number | null; n: number }> = []
  for (let offset = 0; offset < longest; offset += 1) {
    const values = paths.map(path => path[offset]?.[1]).filter((value): value is number => value != null).map(value => 1 + value).sort((a, b) => a - b)
    const bench = paths.map(path => path[offset]?.[2]).filter((value): value is number => value != null).map(value => 1 + value).sort((a, b) => a - b)
    // Late months with only a few runs left would make the bands jump.
    if (values.length < Math.max(3, paths.length * 0.25)) break
    bands.push({ month: offset, p10: quantile(values, 0.1), p25: quantile(values, 0.25), p50: quantile(values, 0.5), p75: quantile(values, 0.75), p90: quantile(values, 0.9), bench: bench.length ? quantile(bench, 0.5) : null, n: values.length })
  }
  return bands
}

/**
 * Back-to-back runs: start at the first period, hold for the duration, start
 * again at the next period on or after it ends. The growth of following the
 * strategy with one rebalance per holding period.
 */
export function chainRuns(rows: RunRow[]) {
  const usable = rows.filter(row => (row.status === 'ok' || row.status === 'truncated') && finite(row.total_return) != null).sort((a, b) => a.period.localeCompare(b.period))
  const chain: Array<{ date: string; portfolio: number; benchmark: number | null; period: string }> = []
  if (!usable.length) return chain
  let portfolio = 1
  let benchmark: number | null = 1
  let cursor = ''
  chain.push({ date: usable[0].period, portfolio, benchmark, period: usable[0].period })
  for (const row of usable) {
    if (row.period < cursor) continue
    portfolio *= 1 + (row.total_return as number)
    benchmark = benchmark == null || finite(row.benchmark_total_return) == null ? null : benchmark * (1 + (row.benchmark_total_return as number))
    const end = String(row.end ?? row.requested_end ?? row.period)
    chain.push({ date: end, portfolio, benchmark, period: row.period })
    cursor = String(row.requested_end ?? row.end ?? '9999')
  }
  return chain
}

/** A diverging colour for a return: vermilion below zero, indigo above, paler near zero. */
export function heatColor(value: number | null | undefined, scale = 0.3) {
  if (value == null || !Number.isFinite(value)) return 'transparent'
  const strength = Math.min(1, Math.abs(value) / scale)
  const alpha = (0.12 + strength * 0.78).toFixed(2)
  return value >= 0 ? `rgba(52, 96, 168, ${alpha})` : `rgba(196, 70, 44, ${alpha})`
}
export function heatText(value: number | null | undefined, scale = 0.3) {
  return value != null && Math.abs(value) / scale > 0.55 ? '#fff' : 'inherit'
}

export function sortRuns(rows: RunRow[], key: keyof RunRow, descending: boolean) {
  const direction = descending ? -1 : 1
  return [...rows].sort((a, b) => {
    const left = a[key]
    const right = b[key]
    if (left == null && right == null) return 0
    if (left == null) return 1
    if (right == null) return -1
    if (typeof left === 'number' && typeof right === 'number') return (left - right) * direction
    return String(left).localeCompare(String(right)) * direction
  })
}
