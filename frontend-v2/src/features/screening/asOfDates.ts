/** As-of dates for point-in-time screens: presets, typed shorthands, and display. An empty value means the latest data. */

export interface DateChoice { value: string; label: string; detail?: string }

const RECENT_KEY = 'shade.screening.recent-dates'
const RECENT_LIMIT = 4
const display = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' })
const UNITS: Record<string, 'd' | 'w' | 'm' | 'y'> = { d: 'd', day: 'd', days: 'd', w: 'w', wk: 'w', week: 'w', weeks: 'w', m: 'm', mo: 'm', month: 'm', months: 'm', y: 'y', yr: 'y', year: 'y', years: 'y' }

const pad = (value: number) => String(value).padStart(2, '0')
const iso = (year: number, month: number, day: number) => `${year}-${pad(month)}-${pad(day)}`
const daysIn = (year: number, month: number) => new Date(Date.UTC(year, month, 0)).getUTCDate()

/** Today in the user's calendar, as YYYY-MM-DD. */
export function todayIso(now = new Date()) {
  return iso(now.getFullYear(), now.getMonth() + 1, now.getDate())
}

/** The same day ``count`` months or years earlier, kept inside shorter months (31 Mar − 1 month = 28/29 Feb). */
function shift(from: string, unit: 'd' | 'w' | 'm' | 'y', count: number) {
  const [year, month, day] = from.split('-').map(Number)
  if (unit === 'd' || unit === 'w') {
    const date = new Date(Date.UTC(year, month - 1, day - count * (unit === 'w' ? 7 : 1)))
    return iso(date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate())
  }
  const months = year * 12 + (month - 1) - count * (unit === 'y' ? 12 : 1)
  const nextYear = Math.floor(months / 12)
  const nextMonth = months % 12 + 1
  return iso(nextYear, nextMonth, Math.min(day, daysIn(nextYear, nextMonth)))
}

export function formatAsOf(value: string) {
  if (!value) return 'Latest data'
  const [year, month, day] = value.split('-').map(Number)
  return Number.isFinite(year) && month && day ? display.format(Date.UTC(year, month - 1, day)) : value
}

export type ParsedDate = { value: string } | { error: string }

/**
 * Reads a typed as-of date: 2023-06-30 (also 2023/06/30 or 20230630), 2023-06
 * (that month's end), 2023 (that year's end), 18m / 5y / 2 years ago / 90d, or
 * "latest". Returns null for empty text.
 */
export function parseAsOf(text: string, now = new Date()): ParsedDate | null {
  const input = text.trim().toLowerCase()
  if (!input) return null
  if (['latest', 'today', 'now', 'none', 'clear'].includes(input)) return { value: '' }
  const today = todayIso(now)
  const inPast = (value: string): ParsedDate => value > today ? { error: `${formatAsOf(value)} is after today.` } : { value }

  const relative = input.match(/^-?(\d{1,3})\s*([a-z]+)(\s+ago)?$/)
  if (relative && UNITS[relative[2]]) return { value: shift(today, UNITS[relative[2]], Number(relative[1])) }

  const full = input.match(/^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})$/) ?? input.match(/^(\d{4})(\d{2})(\d{2})$/)
  if (full) {
    const [year, month, day] = full.slice(1).map(Number)
    if (month < 1 || month > 12 || day < 1 || day > daysIn(year, month)) return { error: `${text.trim()} is not a calendar date.` }
    return inPast(iso(year, month, day))
  }
  const monthOnly = input.match(/^(\d{4})[-/.](\d{1,2})$/)
  if (monthOnly) {
    const [year, month] = monthOnly.slice(1).map(Number)
    if (month < 1 || month > 12) return { error: `${text.trim()} is not a month.` }
    const end = iso(year, month, daysIn(year, month))
    return end > today && `${year}-${pad(month)}` === today.slice(0, 7) ? { value: today } : inPast(end)
  }
  const yearOnly = input.match(/^(\d{4})$/)
  if (yearOnly) {
    const year = Number(yearOnly[1])
    return year === now.getFullYear() ? { value: today } : inPast(iso(year, 12, 31))
  }
  return { error: 'Try 2023-06-30, 2023-06, 2023, or 18m / 5y for months or years ago.' }
}

/** The quick choices: latest, round distances back from today, and recent year ends. */
export function presetDates(now = new Date()): DateChoice[] {
  const today = todayIso(now)
  const year = now.getFullYear()
  const back = (unit: 'm' | 'y', count: number, label: string): DateChoice => {
    const value = shift(today, unit, count)
    return { value, label, detail: formatAsOf(value) }
  }
  return [
    { value: '', label: 'Latest data', detail: 'Newest filings and prices' },
    back('m', 3, '3 months ago'),
    back('m', 6, '6 months ago'),
    back('y', 1, '1 year ago'),
    back('y', 2, '2 years ago'),
    back('y', 3, '3 years ago'),
    back('y', 5, '5 years ago'),
    back('y', 10, '10 years ago'),
    ...[1, 2, 3].map(offset => ({ value: iso(year - offset, 12, 31), label: `End of ${year - offset}`, detail: formatAsOf(iso(year - offset, 12, 31)) })),
  ]
}

export function readRecentDates(): string[] {
  try {
    const stored = JSON.parse(localStorage.getItem(RECENT_KEY) ?? '[]') as unknown
    return Array.isArray(stored) ? stored.filter((item): item is string => typeof item === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(item)).slice(0, RECENT_LIMIT) : []
  } catch {
    return []
  }
}

export function rememberDate(value: string) {
  if (!value) return
  try {
    localStorage.setItem(RECENT_KEY, JSON.stringify([value, ...readRecentDates().filter(item => item !== value)].slice(0, RECENT_LIMIT)))
  } catch { /* recent dates are a convenience */ }
}
