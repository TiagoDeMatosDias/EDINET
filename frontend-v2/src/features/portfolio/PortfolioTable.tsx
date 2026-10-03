import { ArrowDown, ArrowUp, ChevronLeft, ChevronRight } from 'lucide-react'
import { useEffect, useMemo, useRef, useState, type KeyboardEvent, type ReactNode } from 'react'

import { Tip } from '../../components/Tooltip'
import { useHotkeys } from '../../hooks/useHotkeys'

export interface TableColumn<T> {
  id: string
  header: string
  /** Shown on hover and focus of the header. */
  tip?: ReactNode
  numeric?: boolean
  /** Rendered as the row's header cell (the holding's name). */
  rowHeader?: boolean
  sortValue?: (row: T) => number | string | null | undefined
  cell: (row: T) => ReactNode
  className?: string
}

export type TableSort = { column: string; direction: 'asc' | 'desc' } | null

function missing(value: unknown) {
  return value == null || value === '' || (typeof value === 'number' && !Number.isFinite(value))
}

function compare(a: unknown, b: unknown) {
  if (missing(a) || missing(b)) return missing(a) === missing(b) ? 0 : missing(a) ? 1 : -1
  if (typeof a === 'number' && typeof b === 'number') return a - b
  return String(a).localeCompare(String(b))
}

/**
 * A sortable table you can drive from the keyboard. ↓ or J anywhere on the page
 * enters it, ↑/↓ or J/K move, Home/End jump, Enter runs ``onOpen``, A runs
 * ``onSecondary``, and [ ] change pages. Clicking a row opens it too.
 */
export function PortfolioTable<T>({
  rows,
  columns,
  rowKey,
  label,
  onOpen,
  onSecondary,
  secondaryLabel,
  openLabel = 'details',
  emptyText = 'Nothing to show.',
  pageSize = 100,
  hotkeys = true,
  initialSort = null,
  initialCursor,
  rowClassName,
  onOrderChange,
}: {
  rows: T[]
  columns: TableColumn<T>[]
  rowKey: (row: T) => string
  label: string
  onOpen?: (row: T) => void
  onSecondary?: (row: T) => void
  secondaryLabel?: string
  openLabel?: string
  emptyText?: string
  pageSize?: number
  hotkeys?: boolean
  initialSort?: TableSort
  /** A row to start on with keyboard focus (coming back to the list). */
  initialCursor?: string
  rowClassName?: (row: T) => string | undefined
  /** Called with the rows in their displayed order, for stepping through them elsewhere. */
  onOrderChange?: (rows: T[]) => void
}) {
  const [sort, setSort] = useState<TableSort>(initialSort)
  const [cursor, setCursor] = useState<string | null>(initialCursor ?? null)
  const rowRefs = useRef<Array<HTMLTableRowElement | null>>([])
  const body = useRef<HTMLTableSectionElement>(null)
  const focusPending = useRef(Boolean(initialCursor))

  const sorted = useMemo(() => {
    const column = columns.find(item => item.id === sort?.column)
    if (!sort || !column?.sortValue) return rows
    const factor = sort.direction === 'asc' ? 1 : -1
    return [...rows].sort((a, b) => {
      const left = column.sortValue!(a)
      const right = column.sortValue!(b)
      // Missing values stay last in both directions.
      return missing(left) || missing(right) ? compare(left, right) : compare(left, right) * factor
    })
  }, [columns, rows, sort])
  // Report the displayed order only when it changes, not on every new array.
  const orderKey = sorted.map(rowKey).join('\u0000')
  const reported = useRef<string | null>(null)
  useEffect(() => {
    if (!onOrderChange || reported.current === orderKey) return
    reported.current = orderKey
    onOrderChange(sorted)
  }, [onOrderChange, orderKey, sorted])

  const cursorIndex = Math.max(0, sorted.findIndex(row => rowKey(row) === cursor))
  const pages = Math.max(1, Math.ceil(sorted.length / pageSize))
  const page = Math.min(pages - 1, Math.floor(cursorIndex / pageSize))
  const visible = sorted.slice(page * pageSize, page * pageSize + pageSize)
  const cursorInPage = cursorIndex - page * pageSize

  useEffect(() => {
    if (!focusPending.current) return
    focusPending.current = false
    const row = rowRefs.current[cursorInPage]
    row?.focus({ preventScroll: true })
    row?.scrollIntoView?.({ block: 'nearest' })
  }, [cursorInPage, page])

  const select = (index: number, focus: boolean) => {
    const next = Math.max(0, Math.min(sorted.length - 1, index))
    const row = sorted[next]
    if (!row) return
    if (focus && next === cursorIndex) rowRefs.current[cursorInPage]?.focus()
    else if (focus) focusPending.current = true
    setCursor(rowKey(row))
  }
  const focusInList = () => Boolean(body.current?.contains(document.activeElement))
  const enterList = () => select(cursorIndex, true)
  const goToPage = (target: number) => select(Math.max(0, Math.min(pages - 1, target)) * pageSize, focusInList())
  useHotkeys({
    j: enterList, k: enterList, ArrowDown: enterList, ArrowUp: enterList,
    '[': () => goToPage(page - 1), ']': () => goToPage(page + 1),
  }, hotkeys && sorted.length > 0)

  const toggleSort = (column: TableColumn<T>) => {
    if (!column.sortValue) return
    // Numbers sort largest first, text A to Z; a third click restores the original order.
    const first = column.numeric ? 'desc' : 'asc'
    setSort(sort?.column !== column.id ? { column: column.id, direction: first } : sort.direction === first ? { column: column.id, direction: first === 'desc' ? 'asc' : 'desc' } : null)
    setCursor(null)
  }
  const onKeyDown = (event: KeyboardEvent<HTMLTableSectionElement>) => {
    if (event.ctrlKey || event.metaKey || event.altKey) return
    const current = sorted[cursorIndex]
    const keys: Record<string, () => void> = {
      ArrowDown: () => select(cursorIndex + 1, true), j: () => select(cursorIndex + 1, true),
      ArrowUp: () => select(cursorIndex - 1, true), k: () => select(cursorIndex - 1, true),
      Home: () => select(0, true), End: () => select(sorted.length - 1, true),
      PageDown: () => select(cursorIndex + pageSize, true), PageUp: () => select(cursorIndex - pageSize, true),
      ...(onOpen ? { Enter: () => { if (current) onOpen(current) } } : {}),
      ...(onSecondary ? { a: () => { if (current) onSecondary(current) } } : {}),
    }
    const action = keys[event.key]
    // Enter on a link or button inside a row keeps its own meaning.
    if (!action || (event.key === 'Enter' && (event.target as HTMLElement).tagName !== 'TR')) return
    event.preventDefault()
    event.stopPropagation()
    action()
  }

  if (!rows.length) return <div className="table-empty">{emptyText}</div>
  return <div className="pf-table">
    <div className="pf-table__scroll">
      <table aria-label={label}>
        <thead>
          <tr>{columns.map(column => {
            const active = sort?.column === column.id
            const content = <><span>{column.header}</span>{active && (sort!.direction === 'asc' ? <ArrowUp aria-hidden="true" /> : <ArrowDown aria-hidden="true" />)}</>
            return <th key={column.id} scope="col" className={[column.numeric ? 'num' : '', column.className ?? ''].join(' ').trim() || undefined} aria-sort={active ? (sort!.direction === 'asc' ? 'ascending' : 'descending') : column.sortValue ? 'none' : undefined}>
              {column.sortValue
                ? <button type="button" onClick={() => toggleSort(column)} title={`Sort by ${column.header}`}>{column.tip ? <Tip content={column.tip} focusable={false}>{content}</Tip> : content}</button>
                : column.tip ? <Tip content={column.tip} focusable={false}>{content}</Tip> : content}
            </th>
          })}</tr>
        </thead>
        <tbody ref={body} onKeyDown={onKeyDown}>
          {visible.map((row, index) => {
            const key = rowKey(row)
            const isCursor = index === cursorInPage && cursor !== null
            return <tr
              key={key}
              ref={element => { rowRefs.current[index] = element }}
              className={[isCursor ? 'is-cursor' : '', rowClassName?.(row) ?? ''].join(' ').trim() || undefined}
              tabIndex={index === cursorInPage ? 0 : -1}
              onFocus={() => setCursor(key)}
              onClick={event => {
                setCursor(key)
                if (!(event.target as HTMLElement).closest('a, button, input')) onOpen?.(row)
              }}
            >
              {columns.map(column => column.rowHeader
                ? <th key={column.id} scope="row" className={column.className}>{column.cell(row)}</th>
                : <td key={column.id} className={[column.numeric ? 'num' : '', column.className ?? ''].join(' ').trim() || undefined}>{column.cell(row)}</td>)}
            </tr>
          })}
        </tbody>
      </table>
    </div>
    <footer className="pf-table__foot">
      {(hotkeys || onOpen) ? <span className="pf-table__keys" aria-hidden="true"><kbd>↓</kbd><kbd>J</kbd> browse{onOpen && <> · <kbd>Enter</kbd> {openLabel}</>}{onSecondary && secondaryLabel && <> · <kbd>A</kbd> {secondaryLabel}</>}{pages > 1 && <> · <kbd>[</kbd><kbd>]</kbd> pages</>}</span> : <span>{sorted.length.toLocaleString()} rows</span>}
      {pages > 1 && <span className="pf-table__pages" aria-label="Table pagination">
        <span>{(page * pageSize + 1).toLocaleString()}–{Math.min((page + 1) * pageSize, sorted.length).toLocaleString()} of {sorted.length.toLocaleString()}</span>
        <button type="button" className="icon-button" aria-label="Previous page" disabled={page === 0} onClick={() => goToPage(page - 1)}><ChevronLeft /></button>
        <button type="button" className="icon-button" aria-label="Next page" disabled={page >= pages - 1} onClick={() => goToPage(page + 1)}><ChevronRight /></button>
      </span>}
    </footer>
  </div>
}
