import { useMemo, useState } from 'react'

type SortValue = number | string | null | undefined

/** One sortable column: the value it compares, and whether a first click puts the largest first. */
export interface SortColumn<T> { value: (row: T) => SortValue; descending?: boolean }

export interface TableSort<K extends string> { key: K | null; descending: boolean; toggle: (key: K) => void }

const collator = new Intl.Collator('ja', { numeric: true, sensitivity: 'base' })
const missing = (value: SortValue) => value == null || value === '' || Number.isNaN(value)

/** ``rows`` ordered by ``column``; missing values go last whichever way it runs, and ties keep their order. */
export function sortRows<T>(rows: readonly T[], column: SortColumn<T>, descending: boolean) {
  const direction = descending ? -1 : 1
  return rows.map(row => ({ row, value: column.value(row) })).sort((a, b) => {
    if (missing(a.value) || missing(b.value)) return missing(a.value) ? (missing(b.value) ? 0 : 1) : -1
    const order = typeof a.value === 'number' && typeof b.value === 'number' ? a.value - b.value : collator.compare(String(a.value), String(b.value))
    return order * direction
  }).map(item => item.row)
}

/**
 * Sorts a table by the column header last clicked. A first click sorts the column its natural way
 * (largest first for amounts, A to Z for names), a second reverses it, and a third restores the
 * order the rows came in: the report's own, which usually means something.
 */
export function useTableSort<T, K extends string>(rows: readonly T[], columns: Record<K, SortColumn<T>>) {
  const [state, setState] = useState<{ key: K; descending: boolean } | null>(null)
  const active = state && columns[state.key] ? state : null
  const sorted = useMemo(() => active ? sortRows(rows, columns[active.key], active.descending) : rows, [rows, columns, active])
  const toggle = (key: K) => setState(current => {
    const first = Boolean(columns[key].descending)
    if (current?.key !== key) return { key, descending: first }
    return current.descending === first ? { key, descending: !first } : null
  })
  const sort: TableSort<K> = { key: active?.key ?? null, descending: active?.descending ?? false, toggle }
  return [sorted, sort] as const
}
