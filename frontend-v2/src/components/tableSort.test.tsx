import { act, renderHook } from '@testing-library/react'
import { describe, expect, it } from 'vitest'

import { sortRows, useTableSort, type SortColumn } from './tableSort'

interface Row { name: string; shares: number | null }
const ROWS: Row[] = [
  { name: 'Bond No. 10', shares: 5 },
  { name: 'Bond No. 9', shares: null },
  { name: 'alpha', shares: 12 },
  { name: 'Beta', shares: 5 },
]
const COLUMNS = {
  name: { value: row => row.name },
  shares: { value: row => row.shares, descending: true },
} satisfies Record<string, SortColumn<Row>>
const names = (rows: readonly Row[]) => rows.map(row => row.name)

describe('table sorting', () => {
  it('puts missing values last both ways and keeps ties in their order', () => {
    expect(names(sortRows(ROWS, COLUMNS.shares, true))).toEqual(['alpha', 'Bond No. 10', 'Beta', 'Bond No. 9'])
    expect(names(sortRows(ROWS, COLUMNS.shares, false))).toEqual(['Bond No. 10', 'Beta', 'alpha', 'Bond No. 9'])
  })

  it('compares names without case and numbers within them by value', () => {
    expect(names(sortRows(ROWS, COLUMNS.name, false))).toEqual(['alpha', 'Beta', 'Bond No. 9', 'Bond No. 10'])
  })

  it('cycles a column through its natural order, the reverse, and the rows’ own order', () => {
    const { result } = renderHook(() => useTableSort(ROWS, COLUMNS))
    expect(result.current[0]).toBe(ROWS)
    act(() => result.current[1].toggle('shares'))
    expect(result.current[1]).toMatchObject({ key: 'shares', descending: true })
    expect(names(result.current[0])[0]).toBe('alpha')
    act(() => result.current[1].toggle('shares'))
    expect(result.current[1]).toMatchObject({ key: 'shares', descending: false })
    expect(names(result.current[0])[0]).toBe('Bond No. 10')
    act(() => result.current[1].toggle('shares'))
    expect(result.current[1].key).toBeNull()
    expect(result.current[0]).toBe(ROWS)
    // Another column starts again from its own natural order: A to Z for names.
    act(() => result.current[1].toggle('shares'))
    act(() => result.current[1].toggle('name'))
    expect(result.current[1]).toMatchObject({ key: 'name', descending: false })
  })
})
