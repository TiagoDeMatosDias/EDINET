/**
 * Types and display helpers for a filing's statement tables. The server builds
 * the tables from the filing's own XBRL linkbases (statements, line items,
 * nesting, dimension members, and periods), so nothing here knows about
 * specific accounting concepts.
 */

import { csvCell } from '../analysis/downloads'

export interface StatementPeriod {
  key: string
  start: string | null
  end: string
  label: string
  detail: string
  /** Number of rows with a value in this period. */
  filled: number
}

export interface StatementRow {
  kind: 'heading' | 'item' | 'total'
  label: string
  concept: string
  depth: number
  member?: string | null
  unit?: string | null
  values?: Record<string, number>
}

export interface StatementTable {
  id: string
  name: string
  member_axes: string[]
  periods: StatementPeriod[]
  rows: StatementRow[]
}

export interface StatementTablesResponse {
  source: 'linkbase' | 'facts'
  fact_count: number
  statements: StatementTable[]
}

/**
 * Periods filled for fewer than this share of the fullest period's rows (for
 * example a single prior-year balance carried for another statement) are
 * hidden until the reader asks for them.
 */
export const SPARSE_PERIOD_SHARE = 0.2

export function splitPeriods(periods: StatementPeriod[]) {
  const threshold = Math.max(0, ...periods.map(period => period.filled)) * SPARSE_PERIOD_SHARE
  return {
    shown: periods.filter(period => period.filled >= threshold),
    sparse: periods.filter(period => period.filled < threshold),
  }
}

/** Rows whose label, element name, or member matches, with the headings above each match. */
export function filterRows(rows: StatementRow[], filter: string): StatementRow[] {
  const needle = filter.trim().toLowerCase()
  if (!needle) return rows
  const result: StatementRow[] = []
  const open: StatementRow[] = []
  for (const row of rows) {
    while (open.length && open[open.length - 1].depth >= row.depth) open.pop()
    if (row.kind === 'heading') {
      open.push(row)
      continue
    }
    if (![row.label, row.concept, row.member].some(value => (value ?? '').toLowerCase().includes(needle))) continue
    for (const heading of open) if (!result.includes(heading)) result.push(heading)
    result.push(row)
  }
  return result
}

export function lineItemCount(rows: StatementRow[]) {
  return rows.filter(row => row.kind !== 'heading').length
}

export type StatementGroup = 'results' | 'statements' | 'notes' | 'other'

export const STATEMENT_GROUPS: Array<{ key: StatementGroup; label: string }> = [
  { key: 'results', label: 'Business results' },
  { key: 'statements', label: 'Financial statements' },
  { key: 'notes', label: 'Notes' },
  { key: 'other', label: 'Other disclosures' },
]

/** Where a linkbase role belongs: multi-year summaries, primary statements, notes, or other disclosures. */
export function statementGroup(statement: Pick<StatementTable, 'id' | 'name'>): StatementGroup {
  const id = statement.id.replace(/^rol_/, '')
  if (/^BusinessResults/i.test(id)) return 'results'
  if (/^Notes/i.test(id) || /^notes\b/i.test(statement.name)) return 'notes'
  if (/^(Consolidated|Semi|Interim|Quarterly)?\w*(StatementOf|BalanceSheet|StatementsOf)/.test(id)) return 'statements'
  return 'other'
}

/** A readable title plus tags for what the role name carries: "Consolidated statement of cash flows IFRS" → title + IFRS. */
export function statementTitle(statement: Pick<StatementTable, 'name'>) {
  let title = statement.name.trim()
  const tags: string[] = []
  if (/\sIFRS$/.test(title)) { tags.push('IFRS'); title = title.replace(/\sIFRS$/, '') }
  if (/^Notes\s/i.test(title)) {
    if (/\sconsolidated financial statements$/i.test(title)) { tags.push('Consolidated'); title = title.replace(/\sconsolidated financial statements$/i, '') }
    title = title.replace(/^Notes\s/i, '')
  }
  return { title: title.charAt(0).toUpperCase() + title.slice(1), tags }
}

const CURRENCY_UNIT = /^[A-Z]{3}$/
const PERCENT_LABEL = /ratio|rate|return on|margin|yield|payout/i

export interface MoneyScale { scale: number; label: string; digits: number }

/**
 * One scale for a table's currency amounts. The smaller lines decide it (the lower
 * quartile reads with at least two whole digits), so interest and tax lines never
 * collapse to 0.0 beside revenue; decimals drop once a typical line has three digits.
 */
export function moneyScale(rows: StatementRow[]): MoneyScale | null {
  const magnitudes = rows
    .filter(row => row.unit && CURRENCY_UNIT.test(row.unit))
    .flatMap(row => Object.values(row.values ?? {}).map(Math.abs))
    .filter(value => Number.isFinite(value) && value > 0)
    .sort((a, b) => a - b)
  if (!magnitudes.length) return null
  const lowerQuartile = magnitudes[Math.floor(magnitudes.length / 4)]
  const median = magnitudes[Math.floor(magnitudes.length / 2)]
  const units: Array<[number, string]> = [[1e12, 'trillions'], [1e9, 'billions'], [1e6, 'millions'], [1e3, 'thousands']]
  const [scale, label] = units.find(([candidate]) => lowerQuartile / candidate >= 10) ?? [1, '']
  return { scale, label, digits: median / scale >= 100 ? 0 : 1 }
}

function grouped(value: number, digits: number) {
  return value.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits })
}

export function isPercentRow(row: StatementRow) {
  return row.unit === 'pure' && PERCENT_LABEL.test(row.label) && Object.values(row.values ?? {}).every(value => Math.abs(value) <= 5)
}

/** A value in its row's terms: scaled currency, per-share amounts, share counts, or ratios. */
export function formatStatementValue(value: number | undefined, row: StatementRow, money: MoneyScale | null) {
  if (value == null || !Number.isFinite(value)) return ''
  if (row.unit && CURRENCY_UNIT.test(row.unit) && money) return grouped(value / money.scale, money.digits)
  if (row.unit && /PerShare/i.test(row.unit)) return grouped(value, 2)
  if (isPercentRow(row)) return `${(value * 100).toFixed(1)}%`
  if (Number.isInteger(value)) return grouped(value, 0)
  return value.toLocaleString('en-US', { maximumFractionDigits: Math.abs(value) >= 100 ? 1 : 3 })
}

/** The unit shown beside a row when it differs from the table's currency. */
export function unitNote(unit?: string | null) {
  if (!unit || CURRENCY_UNIT.test(unit)) return ''
  if (/PerShare/i.test(unit)) return `${unit.replace(/PerShares?$/i, '')} / share`
  if (unit === 'pure') return 'ratio'
  return unit.toLowerCase()
}

/** Change from the prior period to the current one: relative for amounts, percentage points for percents. */
export function periodChange(row: StatementRow, current?: StatementPeriod, prior?: StatementPeriod) {
  if (!current || !prior || row.kind === 'heading') return null
  const now = row.values?.[current.key]
  const before = row.values?.[prior.key]
  if (now == null || before == null) return null
  if (isPercentRow(row)) return { value: now - before, points: true }
  return before === 0 ? null : { value: (now - before) / Math.abs(before), points: false }
}

/** The table as CSV with unrounded values, one column per period. */
export function statementCsv(statement: StatementTable, rows: StatementRow[], periods: StatementPeriod[]) {
  const header = ['line_item', 'concept', ...(statement.member_axes.length ? ['member'] : []), 'unit', ...periods.map(period => `${period.label} ${period.detail}`.trim())]
  const lines = [header.map(csvCell).join(',')]
  for (const row of rows) {
    if (row.kind === 'heading') continue
    const cells = [row.label, row.concept, ...(statement.member_axes.length ? [row.member ?? ''] : []), row.unit ?? '', ...periods.map(period => {
      const value = row.values?.[period.key]
      return value == null ? '' : String(value)
    })]
    lines.push(cells.map(csvCell).join(','))
  }
  return lines.join('\r\n')
}
