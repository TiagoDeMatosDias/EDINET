/**
 * The place in the screen results, kept in this browser so a company opened from
 * the results can step to the next one (Analysis) and coming back lands on it.
 */
const CURSOR_KEY = 'shade.screening.cursor'
const TRAIL_KEY = 'shade.screening.trail'

export type ResultsSort = { column: string; direction: 'asc' | 'desc' } | null

export interface ResultsCursor {
  /** Which results this applies to; see ``resultSignature``. */
  signature: string
  code: string | null
  sort: ResultsSort
  /** Put keyboard focus back on the row when the results show again. */
  refocus?: boolean
}

/** The results in the order the user last saw them, for stepping through in Analysis. */
export interface ScreenTrail { codes: string[]; names: string[] }

function read<T>(key: string): T | null {
  try {
    return JSON.parse(localStorage.getItem(key) ?? 'null') as T | null
  } catch {
    return null
  }
}

function write(key: string, value: unknown) {
  try {
    localStorage.setItem(key, JSON.stringify(value))
  } catch { /* a convenience: without storage the place is simply not kept */ }
}

export const readCursor = () => read<ResultsCursor>(CURSOR_KEY)
export const writeCursor = (cursor: ResultsCursor) => write(CURSOR_KEY, cursor)
export const readTrail = () => read<ScreenTrail>(TRAIL_KEY)
export const writeTrail = (trail: ScreenTrail) => write(TRAIL_KEY, trail)

/** Identifies one set of results: a rerun with other rules, date, or columns gets a new signature. */
export function resultSignature(columns: string[], rowCount: number, codes: string[]) {
  return [rowCount, columns.join(','), ...codes.slice(0, 5), ...codes.slice(-3)].join('|')
}

export function analysisHref(code: string) {
  return `/analyze/${encodeURIComponent(code)}?from=screen`
}

/** Moves the remembered place to ``code`` (used when Analysis steps to another company). */
export function moveCursorTo(code: string) {
  const cursor = readCursor()
  if (cursor) writeCursor({ ...cursor, code, refocus: true })
}
