import { ArrowDown, ArrowUp, ChevronLeft, ChevronRight } from 'lucide-react'
import { useEffect, useMemo, useRef, useState, type KeyboardEvent, type ReactNode } from 'react'
import { Link, useNavigate } from 'react-router-dom'

import type { ScreeningResult } from '../../api/types'
import { Tip } from '../../components/Tooltip'
import { useHotkeys } from '../../hooks/useHotkeys'
import { cellText } from './resultFormat'
import { analysisHref, readCursor, resultSignature, writeCursor, writeTrail, type ResultsSort } from './screenTrail'

const PAGE_SIZE = 100
const NAME_COLUMNS = ['Company_Name', 'CompanyName']
const TICKER_COLUMNS = ['Company_Ticker', 'Ticker']
const CODE_COLUMNS = ['EdinetCode', 'Company_Code', 'CompanyInfo.EdinetCode', 'edinetCode']
const DATE_COLUMN = 'PriceDate'
// Where each row sat in the result, for rows without a company code.
const ROW_INDEX = '\u0000row'
// Column widths: the name and text columns are fixed, numbers share the rest but never squeeze below this.
const NAME_WIDTH = 230
const TEXT_WIDTH = 140
const NUMBER_MIN_WIDTH = 76

export interface ResultColumnInfo { label: string; description?: ReactNode }

type Row = Record<string, unknown>

function firstPresent(columns: string[], candidates: string[]) {
  return candidates.find(candidate => columns.includes(candidate))
}

function isMissing(value: unknown) {
  return value == null || value === '' || (typeof value === 'number' && !Number.isFinite(value))
}

function compareValues(a: unknown, b: unknown) {
  if (isMissing(a) || isMissing(b)) return isMissing(a) === isMissing(b) ? 0 : isMissing(a) ? 1 : -1
  if (typeof a === 'number' && typeof b === 'number') return a - b
  return String(a).localeCompare(String(b))
}

function sortRows(rows: Row[], sort: ResultsSort) {
  if (!sort) return rows
  const factor = sort.direction === 'asc' ? 1 : -1
  return [...rows].sort((a, b) => {
    const order = compareValues(a[sort.column], b[sort.column])
    // Missing values stay last in both directions.
    return isMissing(a[sort.column]) || isMissing(b[sort.column]) ? order : order * factor
  })
}

/**
 * Screening results: company name (with ticker and code) first and linked to
 * Analysis, then each output column. Sortable headers and 100 rows a page.
 * Keyboard: ↓ or J from anywhere on the page enters the list, ↑/↓ or J/K move
 * (across pages), Enter opens the company, Shift+Enter opens it in a new tab,
 * and [ ] change pages. The place is remembered, so coming back from Analysis
 * lands on the company last opened.
 */
export function ResultsTable({ result, columnInfo, hotkeys = true }: { result: ScreeningResult; columnInfo: (column: string) => ResultColumnInfo; hotkeys?: boolean }) {
  const navigate = useNavigate()
  const nameColumn = firstPresent(result.columns, NAME_COLUMNS)
  const tickerColumn = firstPresent(result.columns, TICKER_COLUMNS)
  const codeColumn = firstPresent(result.columns, CODE_COLUMNS)
  const formats = result.column_formats ?? {}
  // Ticker, code, and price date ride along in the name and price cells instead of their own columns.
  const folded = new Set([tickerColumn, codeColumn, DATE_COLUMN].filter(Boolean) as string[])
  const shown = [
    ...(nameColumn ? [nameColumn] : []),
    ...result.columns.filter(column => column !== nameColumn && !folded.has(column) && column.startsWith('Company')),
    ...result.columns.filter(column => column === 'LatestPrice'),
    ...result.columns.filter(column => column !== nameColumn && !folded.has(column) && !column.startsWith('Company') && column !== 'LatestPrice'),
  ]
  const rows = useMemo<Row[]>(() => result.rows.map((row, index) => ({ ...Object.fromEntries(result.columns.map((column, position) => [column, row[position]])), [ROW_INDEX]: index })), [result])
  const codeOf = (row: Row) => codeColumn ? String(row[codeColumn] ?? '').trim() : ''
  const keyOf = (row: Row) => codeOf(row) || `#${String(row[ROW_INDEX])}`
  const signature = resultSignature(result.columns, result.row_count, rows.map(keyOf))
  const numeric = new Set(shown.filter(column => column !== nameColumn && rows.some(row => typeof row[column] === 'number')))
  const tableWidth = shown.reduce((total, column) => total + (column === nameColumn ? NAME_WIDTH : numeric.has(column) ? NUMBER_MIN_WIDTH : TEXT_WIDTH), 0)

  // Showing the same results again restores the place, and the focus after a keyboard open.
  const [restored] = useState(() => {
    const cursor = readCursor()
    return cursor?.signature === signature ? cursor : null
  })
  const [sort, setSort] = useState<ResultsSort>(() => restored?.sort && shown.includes(restored.sort.column) ? restored.sort : null)
  const [cursor, setCursor] = useState<string | null>(restored?.code ?? null)
  const rowRefs = useRef<Array<HTMLTableRowElement | null>>([])
  const body = useRef<HTMLTableSectionElement>(null)
  const focusPending = useRef(Boolean(restored?.refocus))

  const sorted = useMemo(() => sortRows(rows, sort), [rows, sort])
  const cursorIndex = Math.max(0, sorted.findIndex(row => keyOf(row) === cursor))
  const pages = Math.max(1, Math.ceil(sorted.length / PAGE_SIZE))
  const page = Math.floor(cursorIndex / PAGE_SIZE)
  const visible = sorted.slice(page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE)
  const cursorInPage = cursorIndex - page * PAGE_SIZE

  useEffect(() => {
    writeCursor({ signature, code: cursor, sort })
  }, [cursor, signature, sort])
  useEffect(() => {
    if (!focusPending.current) return
    focusPending.current = false
    const row = rowRefs.current[cursorInPage]
    row?.focus({ preventScroll: true })
    row?.scrollIntoView?.({ block: 'nearest' })
  }, [cursorInPage, page])

  /** Moves the cursor to a row anywhere in the results, taking keyboard focus along if asked. */
  const select = (index: number, focus: boolean) => {
    const next = Math.max(0, Math.min(sorted.length - 1, index))
    const row = sorted[next]
    if (!row) return
    if (focus && next === cursorIndex) rowRefs.current[cursorInPage]?.focus()
    else if (focus) focusPending.current = true
    setCursor(keyOf(row))
  }
  const focusInList = () => Boolean(body.current?.contains(document.activeElement))
  const goToPage = (target: number) => select(Math.max(0, Math.min(pages - 1, target)) * PAGE_SIZE, focusInList())
  const remember = (row: Row, refocus: boolean) => {
    const listed = sorted.filter(item => codeOf(item))
    writeTrail({ codes: listed.map(codeOf), names: listed.map(item => String((nameColumn && item[nameColumn]) ?? '').trim() || codeOf(item)) })
    writeCursor({ signature, code: keyOf(row), sort, refocus })
  }
  const open = (row: Row, { newTab = false, keyboard = false } = {}) => {
    const code = codeOf(row)
    if (!code) return
    setCursor(keyOf(row))
    remember(row, keyboard && !newTab)
    if (newTab) window.open(analysisHref(code), '_blank', 'noopener')
    else navigate(analysisHref(code))
  }
  const toggleSort = (column: string) => {
    // Numbers sort largest first, text A to Z; a third click restores the screen's order.
    const first = numeric.has(column) ? 'desc' : 'asc'
    const next: ResultsSort = sort?.column !== column ? { column, direction: first } : sort.direction === first ? { column, direction: first === 'desc' ? 'asc' : 'desc' } : null
    setSort(next)
    // A new order starts at its top.
    const top = sortRows(rows, next)[0]
    setCursor(top ? keyOf(top) : null)
  }

  const enterList = () => select(cursorIndex, true)
  useHotkeys({
    j: enterList, k: enterList, ArrowDown: enterList, ArrowUp: enterList,
    '[': () => goToPage(page - 1), ']': () => goToPage(page + 1),
  }, hotkeys && sorted.length > 0)
  const onKeyDown = (event: KeyboardEvent<HTMLTableSectionElement>) => {
    if (event.ctrlKey || event.metaKey || event.altKey) return
    const keys: Record<string, () => void> = {
      ArrowDown: () => select(cursorIndex + 1, true), j: () => select(cursorIndex + 1, true),
      ArrowUp: () => select(cursorIndex - 1, true), k: () => select(cursorIndex - 1, true),
      Home: () => select(0, true), End: () => select(sorted.length - 1, true),
      PageDown: () => select((page + 1) * PAGE_SIZE, true), PageUp: () => select((page - 1) * PAGE_SIZE, true),
      Enter: () => { const row = sorted[cursorIndex]; if (row) open(row, { newTab: event.shiftKey, keyboard: true }) },
    }
    const action = keys[event.key]
    if (!action || (event.key === 'Enter' && (event.target as HTMLElement).tagName === 'A')) return
    event.preventDefault()
    event.stopPropagation()
    action()
  }

  return <div className="results-grid">
    <div className="results-grid__scroll">
      <table className="results-table" style={{ minWidth: tableWidth }}>
        <colgroup>{shown.map(column => <col key={column} className={column === nameColumn ? 'is-name' : numeric.has(column) ? undefined : 'is-text'} />)}</colgroup>
        <thead>
          <tr>
            {shown.map(column => {
              const info = columnInfo(column)
              const active = sort?.column === column
              return <th key={column} scope="col" className={[numeric.has(column) ? 'num' : '', column === nameColumn ? 'results-table__name' : ''].filter(Boolean).join(' ') || undefined} aria-sort={active ? (sort!.direction === 'asc' ? 'ascending' : 'descending') : 'none'}>
                <button type="button" onClick={() => toggleSort(column)} title={`Sort by ${info.label}`}>
                  <Tip content={info.description ?? info.label} focusable={false}><span>{info.label}</span></Tip>
                  {active && (sort!.direction === 'asc' ? <ArrowUp aria-hidden="true" /> : <ArrowDown aria-hidden="true" />)}
                </button>
              </th>
            })}
          </tr>
        </thead>
        <tbody ref={body} onKeyDown={onKeyDown}>
          {visible.map((row, index) => {
            const code = codeOf(row)
            const isCursor = index === cursorInPage
            return <tr key={keyOf(row)} ref={element => { rowRefs.current[index] = element }} className={isCursor ? 'is-cursor' : undefined} tabIndex={isCursor ? 0 : -1} onFocus={() => setCursor(keyOf(row))} onDoubleClick={() => open(row)}>
              {shown.map(column => {
                const value = row[column]
                if (column === nameColumn) {
                  // Some companies have no name on record; their code still identifies and links them.
                  const companyName = String(value ?? '').trim()
                  return <th key={column} scope="row" className="results-table__name">
                    {code ? <Link to={analysisHref(code)} tabIndex={-1} title={companyName || 'No company name on record'} onClick={() => { setCursor(keyOf(row)); remember(row, false) }}>{companyName || code}</Link> : <span>{companyName || '—'}</span>}
                    <small>{[tickerColumn ? row[tickerColumn] : null, code].filter(Boolean).join(' · ')}</small>
                  </th>
                }
                const text = cellText(value, formats[column])
                const title = column === 'LatestPrice' && row[DATE_COLUMN] ? `${typeof value === 'number' ? value.toLocaleString('en-US') : ''} on ${String(row[DATE_COLUMN])}` : typeof value === 'number' ? value.toLocaleString('en-US') : typeof value === 'string' && value.length > 18 ? value : undefined
                return <td key={column} className={[typeof value === 'number' ? 'num' : '', typeof value === 'number' && value < 0 ? 'neg' : ''].filter(Boolean).join(' ') || undefined} title={title}>{text}</td>
              })}
            </tr>
          })}
        </tbody>
      </table>
    </div>
    <footer className="results-grid__foot">
      <span>{sorted.length ? `${(page * PAGE_SIZE + 1).toLocaleString()}–${Math.min(sorted.length, (page + 1) * PAGE_SIZE).toLocaleString()} of ${sorted.length.toLocaleString()}` : 'No rows'}{sort ? ` · sorted by ${columnInfo(sort.column).label}` : ''}</span>
      <span className="results-grid__keys"><kbd>↓</kbd><kbd>J</kbd> browse · <kbd>Enter</kbd> open · <kbd>Shift Enter</kbd> new tab · <kbd>[</kbd><kbd>]</kbd> pages</span>
      {pages > 1 && <span className="results-grid__pager">
        <button type="button" className="icon-button" aria-label="Previous page" disabled={page === 0} onClick={() => goToPage(page - 1)}><ChevronLeft /></button>
        <span>Page {page + 1} of {pages}</span>
        <button type="button" className="icon-button" aria-label="Next page" disabled={page >= pages - 1} onClick={() => goToPage(page + 1)}><ChevronRight /></button>
      </span>}
    </footer>
  </div>
}
