/**
 * Types and display helpers for a filing's statement tables. The server builds
 * the tables from the filing's own XBRL linkbases (statements, line items,
 * nesting, dimension members, and periods), so nothing here knows about
 * specific accounting concepts.
 */

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
