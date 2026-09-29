import { describe, expect, it } from 'vitest'

import { filterRows, lineItemCount, splitPeriods, type StatementPeriod, type StatementRow } from './statements'

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
