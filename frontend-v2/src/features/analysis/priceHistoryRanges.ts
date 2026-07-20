export type PriceRangeKey = '1m' | '2m' | '3m' | '6m' | 'ytd' | '1y' | '2y' | '3y' | '5y' | '10y' | '15y' | 'all'

export type PriceHistoryRow = {
  Date?: string
  date?: string
  trade_date?: string
  Price?: number
  price?: number
}

export const PRICE_RANGE_OPTIONS: Array<{ key: PriceRangeKey; label: string }> = [
  { key: '1m', label: '1M' }, { key: '2m', label: '2M' }, { key: '3m', label: '3M' },
  { key: '6m', label: '6M' }, { key: 'ytd', label: 'YTD' }, { key: '1y', label: '1Y' },
  { key: '2y', label: '2Y' }, { key: '3y', label: '3Y' }, { key: '5y', label: '5Y' },
  { key: '10y', label: '10Y' }, { key: '15y', label: '15Y' }, { key: 'all', label: 'All' },
]

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
