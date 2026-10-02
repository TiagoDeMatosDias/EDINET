import { describe, expect, it } from 'vitest'

import { filterRows, formatStatementValue, lineItemCount, moneyScale, periodChange, splitPeriods, statementCsv, statementGroup, statementTitle, unitNote, type StatementPeriod, type StatementRow } from './statements'

function period(key: string, filled: number): StatementPeriod {
  return { key, start: null, end: key, label: key, detail: 'As of', filled }
}

const ROWS: StatementRow[] = [
  { kind: 'heading', label: 'Assets', concept: 'AssetsAbstract', depth: 0 },
  { kind: 'heading', label: 'Current assets', concept: 'CurrentAssetsAbstract', depth: 1 },
  { kind: 'item', label: 'Cash and deposits', concept: 'CashAndDeposits', depth: 2, values: { a: 1 } },
  { kind: 'total', label: 'Total current assets', concept: 'CurrentAssets', depth: 2, values: { a: 2 } },
  { kind: 'heading', label: 'Non-current assets', concept: 'NoncurrentAssetsAbstract', depth: 1 },
  { kind: 'item', label: 'Goodwill', concept: 'Goodwill', depth: 2, values: { a: 3 } },
  { kind: 'total', label: 'Total assets', concept: 'Assets', depth: 1, values: { a: 4 } },
]

describe('statement table helpers', () => {
  it('hides only periods far sparser than the fullest one', () => {
    const { shown, sparse } = splitPeriods([period('2026', 60), period('2025', 58), period('2024', 1)])

    expect(shown.map(item => item.key)).toEqual(['2026', '2025'])
    expect(sparse.map(item => item.key)).toEqual(['2024'])
  })

  it('keeps small tables whole', () => {
    expect(splitPeriods([period('2026', 2), period('2025', 1)]).sparse).toEqual([])
  })

  it('filters rows and keeps the headings above each match', () => {
    expect(filterRows(ROWS, 'goodwill').map(row => row.label)).toEqual(['Assets', 'Non-current assets', 'Goodwill'])
    expect(filterRows(ROWS, 'CurrentAssets').map(row => row.label)).toEqual(['Assets', 'Current assets', 'Total current assets'])
    expect(filterRows(ROWS, '')).toBe(ROWS)
    expect(lineItemCount(filterRows(ROWS, 'total'))).toBe(2)
  })
})

describe('statement presentation', () => {
  const row = (label: string, unit: string | null, values: Record<string, number>): StatementRow => ({ kind: 'item', label, concept: label, depth: 0, unit, values })

  it('groups linkbase roles and cleans their names', () => {
    expect(statementGroup({ id: 'rol_BusinessResultsOfGroup', name: 'Business results of group' })).toBe('results')
    expect(statementGroup({ id: 'rol_ConsolidatedStatementOfCashFlowsIFRS', name: 'x' })).toBe('statements')
    expect(statementGroup({ id: 'rol_BalanceSheet', name: 'Balance sheet' })).toBe('statements')
    expect(statementGroup({ id: 'rol_NotesSegmentInformationConsolidatedFinancialStatementsIFRS', name: 'x' })).toBe('notes')
    expect(statementGroup({ id: 'rol_MajorShareholders-01', name: 'Major shareholders (1)' })).toBe('other')
    expect(statementTitle({ name: 'Consolidated statement of cash flows IFRS' })).toEqual({ title: 'Consolidated statement of cash flows', tags: ['IFRS'] })
    expect(statementTitle({ name: 'Notes segment information consolidated financial statements IFRS' })).toEqual({ title: 'Segment information', tags: ['IFRS', 'Consolidated'] })
  })

  it('formats each row in its own unit and scales currency once per table', () => {
    const rows = [row('Revenue', 'JPY', { a: 50_684_960_000_000, b: 48_036_704_000_000 }), row('Interest', 'JPY', { a: 44_800_000_000 }), row('EPS', 'JPYPerShares', { a: 295.25 }), row('Equity-to-asset ratio', 'pure', { a: 0.3785 }), row('Issued shares', 'shares', { a: 15_794_987_460 })]
    const money = moneyScale(rows)
    expect(money).toEqual({ scale: 1e9, label: 'billions', digits: 0 })
    expect(formatStatementValue(rows[0].values?.a, rows[0], money)).toBe('50,685')
    expect(formatStatementValue(rows[2].values?.a, rows[2], money)).toBe('295.25')
    expect(formatStatementValue(rows[3].values?.a, rows[3], money)).toBe('37.9%')
    expect(formatStatementValue(rows[4].values?.a, rows[4], money)).toBe('15,794,987,460')
    expect(unitNote('JPYPerShares')).toBe('JPY / share')
    expect(unitNote('JPY')).toBe('')
  })

  it('compares the current period with the prior one', () => {
    const current = { key: 'a', start: null, end: '2026-03-31', label: '2026-03-31', detail: '12 months', filled: 1 }
    const prior = { ...current, key: 'b', end: '2025-03-31', label: '2025-03-31' }
    expect(periodChange(row('Revenue', 'JPY', { a: 110, b: 100 }), current, prior)).toEqual({ value: 0.1, points: false })
    expect(periodChange(row('Payout ratio', 'pure', { a: 0.3, b: 0.25 }), current, prior)?.points).toBe(true)
    expect(periodChange(row('Revenue', 'JPY', { a: 110 }), current, prior)).toBeNull()
  })

  it('exports a statement as CSV with unrounded values', () => {
    const statement = { id: 's', name: 'Income', member_axes: [], periods: [], rows: [] }
    const period = { key: 'a', start: null, end: '2026-03-31', label: '2026-03-31', detail: '12 months', filled: 1 }
    const csv = statementCsv(statement, [{ kind: 'heading', label: 'Revenue', concept: 'RevenueAbstract', depth: 0 }, row('Net sales, total', 'JPY', { a: 1234567 })], [period])
    expect(csv).toBe('line_item,concept,unit,2026-03-31 12 months\r\n"Net sales, total","Net sales, total",JPY,1234567')
  })
})
