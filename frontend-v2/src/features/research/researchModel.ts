import { csvCell } from '../analysis/downloads'
import { formatMetricValue, type MetricDefinition } from '../../metrics'
import type { BookAlert, BookCompany, Note, ThesisStatus } from './researchTypes'

export const THESIS_STATUSES: Array<{ key: ThesisStatus; label: string; hint: string }> = [
  { key: 'watch', label: 'Watch', hint: 'Following, no position view yet' },
  { key: 'buy', label: 'Buy', hint: 'Would add at this price' },
  { key: 'hold', label: 'Hold', hint: 'Keep, but not add' },
  { key: 'sell', label: 'Sell', hint: 'Would reduce or exit' },
]

/** Tags kept in step with the portfolio (``src/research/positions.py``); they cannot be edited by hand. */
export const POSITION_TAGS = { open: 'Open position', closed: 'Closed position' } as const

export function isPositionTag(tag: string) {
  return tag === POSITION_TAGS.open || tag === POSITION_TAGS.closed
}

/** Research keys companies by EDINET code, and other holdings (a US share, an ETF) by their symbol. */
export function isEdinetCode(code: string) {
  return /^E\d{5}$/.test(code)
}

export function analysisHref(code: string) {
  return isEdinetCode(code) ? `/analyze/${encodeURIComponent(code)}` : `/analyze?ticker=${encodeURIComponent(code)}`
}

export function statusLabel(status?: string | null) {
  return THESIS_STATUSES.find(item => item.key === status)?.label ?? ''
}

/** How far the target is above (or below) today's price, when both are in one currency. */
export function upside(company: Pick<BookCompany, 'target_value' | 'target_currency' | 'LatestPrice' | 'price_currency'>) {
  const { target_value: target, LatestPrice: price } = company
  if (target == null || price == null || !(price > 0)) return null
  if (company.target_currency && company.price_currency && company.target_currency !== company.price_currency) return null
  return target / price - 1
}

const DAY = 86_400_000

function dayNumber(value: string) {
  const time = Date.parse(`${value.slice(0, 10)}T00:00:00Z`)
  return Number.isFinite(time) ? Math.floor(time / DAY) : null
}

export type ReviewState = 'overdue' | 'due' | 'later'

/** Overdue once the review date has passed; due within the next two weeks. */
export function reviewState(reviewOn: string | null | undefined, today: string): ReviewState | null {
  if (!reviewOn) return null
  const [due, now] = [dayNumber(reviewOn), dayNumber(today)]
  if (due == null || now == null) return null
  if (due < now) return 'overdue'
  return due - now <= 14 ? 'due' : 'later'
}

export function todayIso(now = new Date()) {
  return now.toISOString().slice(0, 10)
}

/** Today's date where the user is, which is what a date picker shows. */
export function localToday(now = new Date()) {
  return new Date(now.getTime() - now.getTimezoneOffset() * 60_000).toISOString().slice(0, 10)
}

const dayFormat = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' })
const shortDayFormat = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', timeZone: 'UTC' })

export function formatDay(value: string | null | undefined, withYear = true) {
  if (!value) return ''
  const time = Date.parse(value.length <= 10 ? `${value}T00:00:00Z` : value)
  return Number.isFinite(time) ? (withYear ? dayFormat : shortDayFormat).format(time) : ''
}

/** "today", "3 days ago", "in 5 days", or the date beyond a month. */
export function relativeDay(value: string | null | undefined, today: string) {
  if (!value) return ''
  const [then, now] = [dayNumber(value), dayNumber(today)]
  if (then == null || now == null) return ''
  const gap = then - now
  if (gap === 0) return 'today'
  if (gap === -1) return 'yesterday'
  if (gap === 1) return 'tomorrow'
  if (Math.abs(gap) <= 30) return gap < 0 ? `${-gap} days ago` : `in ${gap} days`
  return formatDay(value, Math.abs(gap) > 300)
}

export interface BookFilter {
  tag: string
  status: string
  query: string
}

export function filterBook(companies: BookCompany[], filter: BookFilter, today: string) {
  const query = filter.query.trim().toLocaleLowerCase()
  return companies.filter(company => {
    if (filter.tag && !company.tags.includes(filter.tag)) return false
    if (filter.status === 'review') { if (reviewState(company.review_on, today) !== 'overdue' && reviewState(company.review_on, today) !== 'due') return false }
    else if (filter.status === 'alerts') { if (!company.alerts_triggered) return false }
    else if (filter.status && company.thesis_status !== filter.status) return false
    if (!query) return true
    return [company.company_name, company.ticker, company.company_code, company.industry, company.thesis, ...company.tags]
      .some(value => String(value ?? '').toLocaleLowerCase().includes(query))
  })
}

export type BookSortKey = 'updated' | 'name' | 'status' | 'upside' | 'review' | 'price' | 'pe' | 'yield'

const STATUS_ORDER: Record<string, number> = { buy: 0, hold: 1, watch: 2, sell: 3 }

/** Missing values always sort last, whichever way the column runs. */
export function sortBook(companies: BookCompany[], key: BookSortKey, descending: boolean) {
  const value = (company: BookCompany): string | number | null => {
    switch (key) {
      case 'name': return company.company_name.toLocaleLowerCase()
      case 'status': return company.thesis_status ? STATUS_ORDER[company.thesis_status] ?? 9 : null
      case 'upside': return upside(company)
      case 'review': return company.review_on ?? null
      case 'price': return company.LatestPrice ?? null
      case 'pe': return company.PERatio ?? null
      case 'yield': return company.DividendsYield ?? null
      default: return company.updated_at ?? null
    }
  }
  return [...companies].sort((a, b) => {
    const [x, y] = [value(a), value(b)]
    if (x == null || y == null) return x == null && y == null ? 0 : x == null ? 1 : -1
    const order = x < y ? -1 : x > y ? 1 : 0
    return descending ? -order : order
  })
}

/** A note needs a title; without one, its first line serves. */
export function noteTitle(title: string, body: string) {
  const explicit = title.trim()
  if (explicit) return explicit.slice(0, 200)
  const line = body.trim().split('\n', 1)[0].replace(/^#+\s*/, '').trim()
  return line.length > 80 ? `${line.slice(0, 79)}…` : line || 'Note'
}

export const ALERT_OPERATORS = ['>', '>=', '<', '<=', '='] as const

export function alertCondition(alert: Pick<BookAlert, 'metric' | 'operator' | 'value' | 'price_currency'>, definitions: Record<string, MetricDefinition>) {
  const definition = definitions[alert.metric]
  const label = definition?.label ?? alert.metric
  return `${label} ${alert.operator.replace('>=', '≥').replace('<=', '≤')} ${formatMetricValue(definition, alert.value, { price: alert.price_currency })}`
}

/** How far the current value is from the threshold, relative to the threshold. */
export function alertDistance(alert: Pick<BookAlert, 'current_value' | 'value'>) {
  if (alert.current_value == null || !alert.value) return null
  return alert.current_value / alert.value - 1
}

export function bookCsv(companies: BookCompany[]) {
  const header = ['Company', 'Ticker', 'EDINET code', 'Industry', 'Tags', 'Status', 'Thesis', 'Price', 'Price currency', 'Target', 'Target currency', 'Upside', 'Review on', 'Notes', 'Alerts', 'Alerts triggered', 'P/E', 'Dividend yield', 'Last activity']
  const rows = companies.map(company => [
    company.company_name, company.ticker, company.company_code, company.industry ?? '', company.tags.join('; '),
    statusLabel(company.thesis_status), company.thesis ?? '', company.LatestPrice ?? '', company.price_currency ?? '',
    company.target_value ?? '', company.target_currency ?? '', upside(company) ?? '', company.review_on ?? '',
    company.note_count, company.alert_count, company.alerts_triggered, company.PERatio ?? '', company.DividendsYield ?? '', company.updated_at ?? '',
  ])
  return [header, ...rows].map(row => row.map(cell => csvCell(String(cell))).join(',')).join('\n')
}

/** A starting risk-free rate by currency; an editable assumption, not market data. */
export const DEFAULT_RATES: Record<string, number> = { JPY: 0.01, USD: 0.04, EUR: 0.02, GBP: 0.04, CHF: 0.005 }

export function defaultRate(currency: string | null | undefined) {
  return DEFAULT_RATES[currency ?? 'JPY'] ?? 0.03
}

export function formatPercent(value: number | null | undefined, digits = 1) {
  if (value == null || !Number.isFinite(value)) return '—'
  const text = (value * 100).toFixed(digits)
  // A tiny negative rounds to "-0"; show it as zero.
  return `${Number(text) === 0 ? text.replace('-', '') : text}%`
}

export function formatSignedPercent(value: number | null | undefined, digits = 1) {
  if (value == null || !Number.isFinite(value)) return '—'
  const text = (value * 100).toFixed(digits)
  return `${value > 0 && Number(text) !== 0 ? '+' : ''}${Number(text) === 0 ? (0).toFixed(digits) : text}%`
}

export function formatNumber(value: number | null | undefined, digits = 2) {
  if (value == null || !Number.isFinite(value)) return '—'
  const fixed = value.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits })
  return /^-0(\.0+)?$/.test(fixed) ? fixed.slice(1) : fixed
}

/** Notes as one Markdown document, newest first, each headed by its title. */
export function notesMarkdown(notes: Note[], names: Map<string, string>) {
  return notes.map(note => [
    `## ${note.title}`,
    [note.edinet_code ? names.get(note.edinet_code) ?? note.edinet_code : 'General', note.updated_at ? formatDay(note.updated_at) : ''].filter(Boolean).join(' · '),
    '',
    note.body.trim(),
  ].join('\n')).join('\n\n')
}
