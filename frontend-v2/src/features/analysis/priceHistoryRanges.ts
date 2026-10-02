export type PriceRangeKey = '1m' | '2m' | '3m' | '6m' | 'ytd' | '1y' | '2y' | '3y' | '5y' | '10y' | '15y' | 'all'

export type PriceHistoryRow = {
  Date?: string
  date?: string
  trade_date?: string
  Price?: number
  price?: number
  source_price?: number
  raw_price?: number
  price_basis?: 'raw' | 'adjusted' | 'unknown' | string
  provider?: string | null
  source_id?: string | null
  source_revision?: string | null
  retrieved_at?: string | null
  split_adjustment_factor?: number | null
  adjusted_price?: number | null
}

function parsedDate(row: PriceHistoryRow) {
  const value = row.trade_date ?? row.Date ?? row.date
  if (!value) return null
  const date = new Date(`${value.slice(0, 10)}T00:00:00Z`)
  return Number.isNaN(date.getTime()) ? null : date
}

function rangeCutoff(latest: Date, range: PriceRangeKey) {
  const cutoff = new Date(latest)
  if (range === 'ytd') return new Date(Date.UTC(latest.getUTCFullYear(), 0, 1))
  const day = cutoff.getUTCDate()
  cutoff.setUTCDate(1)
  if (range.endsWith('m')) cutoff.setUTCMonth(cutoff.getUTCMonth() - Number.parseInt(range, 10))
  if (range.endsWith('y')) cutoff.setUTCFullYear(cutoff.getUTCFullYear() - Number.parseInt(range, 10))
  const lastDay = new Date(Date.UTC(cutoff.getUTCFullYear(), cutoff.getUTCMonth() + 1, 0)).getUTCDate()
  cutoff.setUTCDate(Math.min(day, lastDay))
  return cutoff
}

export function filterPriceHistory(rows: PriceHistoryRow[], range: PriceRangeKey) {
  if (range === 'all') return rows
  const dated = rows.map(row => ({ row, date: parsedDate(row) })).filter(item => item.date !== null)
  const latest = dated.reduce<Date | null>((current, item) => !current || item.date! > current ? item.date : current, null)
  if (!latest) return rows
  const cutoff = rangeCutoff(latest, range)
  return dated.filter(item => item.date! >= cutoff).map(item => item.row)
}

export const PRICE_RANGES: Array<{ key: PriceRangeKey; label: string; title: string }> = [
  { key: '1m', label: '1M', title: 'One month' },
  { key: '3m', label: '3M', title: 'Three months' },
  { key: '6m', label: '6M', title: 'Six months' },
  { key: 'ytd', label: 'YTD', title: 'Year to date' },
  { key: '1y', label: '1Y', title: 'One year' },
  { key: '3y', label: '3Y', title: 'Three years' },
  { key: '5y', label: '5Y', title: 'Five years' },
  { key: '10y', label: '10Y', title: 'Ten years' },
  { key: 'all', label: 'All', title: 'All stored prices' },
]
export function priceDate(row: PriceHistoryRow) {
  return (row.trade_date ?? row.Date ?? row.date ?? '').slice(0, 10)
}

export function priceValue(row: PriceHistoryRow) {
  const value = row.Price ?? row.price
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

const DAY = 24 * 3600 * 1000

const dayTick = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', timeZone: 'UTC' })
const monthTick = new Intl.DateTimeFormat('en-GB', { month: 'short', year: '2-digit', timeZone: 'UTC' })

/**
 * Axis ticks on calendar boundaries at the range's grain (days, months, or years), at most
 * about eight, so a five-year chart never repeats a year label.
 */
export function priceTicks(labels: string[]): Map<number, string> {
  const ticks = new Map<number, string>()
  const times = labels.map(label => Date.parse(`${label}T00:00:00Z`))
  if (labels.length < 2 || !Number.isFinite(times[0]) || !Number.isFinite(times[times.length - 1])) return ticks
  const span = (times[times.length - 1] - times[0]) / DAY
  if (span <= 200) {
    const step = Math.max(1, Math.ceil(labels.length / 7))
    times.forEach((time, index) => { if (index % step === 0 && Number.isFinite(time)) ticks.set(index, dayTick.format(time)) })
    return ticks
  }
  const byYear = span > 4 * 366
  const units = byYear ? span / 365.25 : span / 30.44
  const step = (byYear ? [1, 2, 5, 10] : [1, 2, 3, 6, 12]).find(candidate => units / candidate <= 8) ?? (byYear ? 10 : 12)
  let previous = Number.NaN
  times.forEach((time, index) => {
    if (!Number.isFinite(time)) return
    const date = new Date(time)
    const bucket = byYear ? date.getUTCFullYear() : date.getUTCFullYear() * 12 + date.getUTCMonth()
    if (index > 0 && bucket !== previous && (byYear ? bucket : date.getUTCMonth()) % step === 0) {
      ticks.set(index, byYear ? String(bucket) : monthTick.format(time))
    }
    previous = bucket
  })
  return ticks
}
