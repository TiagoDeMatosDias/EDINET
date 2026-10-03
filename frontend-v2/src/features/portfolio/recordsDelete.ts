import { csvCell } from '../analysis/downloads'
import type { Transaction } from './portfolioTypes'

/** What to delete: everything, whole imported files, or chosen records. */
export type DeleteSelection =
  | { kind: 'everything' }
  | { kind: 'files'; files: string[] }
  | { kind: 'records'; ids: number[] }

export type DeletePreview = {
  records: number
  by_type: Record<string, number>
  first_date: string | null
  last_date: string | null
  symbols: number
  source_files: string[]
  remaining: number
}

export type DeleteResult = { deleted: number; remaining: number; daily_rows: number; holdings_count: number }

export function selectionPayload(selection: DeleteSelection) {
  if (selection.kind === 'everything') return { everything: true }
  if (selection.kind === 'files') return { source_files: selection.files }
  return { ids: selection.ids }
}

export function selectionTitle(selection: DeleteSelection) {
  if (selection.kind === 'everything') return 'Clear all portfolio data'
  if (selection.kind === 'files') return selection.files.length === 1 ? `Delete the import “${selection.files[0] || 'without a file name'}”` : `Delete ${selection.files.length} imports`
  return `Delete ${selection.ids.length.toLocaleString()} selected record${selection.ids.length === 1 ? '' : 's'}`
}

/** The loaded records a selection covers, for downloading them first. */
export function selectedRecords(records: Transaction[], selection: DeleteSelection) {
  if (selection.kind === 'everything') return records
  if (selection.kind === 'files') {
    const files = new Set(selection.files)
    return records.filter(record => files.has(record.source_file ?? ''))
  }
  const ids = new Set(selection.ids)
  return records.filter(record => record.id != null && ids.has(record.id))
}

const CSV_COLUMNS: Array<keyof Transaction> = ['id', 'trade_date', 'activity_type', 'symbol', 'description', 'quantity', 'trade_price', 'trade_money', 'proceeds', 'amount', 'net_cash', 'commission', 'currency', 'buy_sell', 'source_file']

export function recordsCsv(records: Transaction[]) {
  const lines = [CSV_COLUMNS.join(',')]
  for (const record of records) lines.push(CSV_COLUMNS.map(column => csvCell(record[column] == null ? '' : String(record[column]))).join(','))
  return `${lines.join('\r\n')}\r\n`
}
