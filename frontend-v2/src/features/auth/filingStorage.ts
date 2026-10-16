/** Admin → Filing storage: the response types and the wording shared with the overview banner. */

export const FILING_STORAGE_KEY = ['admin-filing-storage'] as const

/** Where filing storage stands: the old single file, a split in progress, ready to finish at restart, or split. */
export type FilingStorageState = 'legacy' | 'splitting' | 'ready' | 'split'

export interface FilingStorageYear {
  shard: string
  path: string
  file: string
  state: string
  filings: number
  archive_bytes: number
  file_bytes: number | null
  checked_at: string | null
  check_result: string | null
}

export interface FilingStoragePreflight {
  layout: string
  needed: boolean
  source_bytes?: number
  filings?: number
  target_folder?: string
  default_folder?: boolean
  free_bytes?: number
  written_bytes?: number
  needed_bytes?: number
  enough_space?: boolean
  undated?: string[]
  undated_count?: number
  state?: string | null
  years?: Array<{ shard: string; filings: number; estimated_bytes: number; path: string }>
  files?: string[]
}

export interface FilingStorageJob {
  job_id: string
  kind: string
  status: string
  title: string
  progress_percent: number | null
  status_message: string | null
  error_message: string | null
  created_at: string
  completed_at: string | null
}

export interface FilingStorageResponse {
  layout: string
  state: FilingStorageState
  index: { path: string; bytes: number }
  shards_folder: string
  shards: FilingStorageYear[]
  retired?: { path: string; bytes: number }
  next?: { path: string; values: Record<string, string>; years: Array<{ shard: string; filing_count: number }> }
  preflight?: FilingStoragePreflight
  jobs: FilingStorageJob[]
}

/** Shown on the overview and in Admin while filings.db still has the old layout. */
export interface FilingStorageNotice {
  state: 'legacy' | 'splitting' | 'ready'
  needed_bytes: number
}

export function gigabytes(bytes: number | null | undefined): string {
  if (bytes == null) return '—'
  if (bytes < 1e9) return `${(bytes / 1e6).toFixed(bytes < 1e7 ? 1 : 0)} MB`
  return `${(bytes / 1e9).toFixed(bytes < 1e10 ? 1 : 0)} GB`
}

/** The one-line notice for the old layout, the same on the overview and in Admin. */
export function filingStorageNoticeText(notice: FilingStorageNotice): string {
  if (notice.state === 'ready') return 'Filing storage is reorganised — restart the server to finish'
  if (notice.state === 'splitting') return 'Filing storage is being reorganised — the split resumes where it stopped'
  return `Filing storage needs reorganising — ≈${Math.round(notice.needed_bytes / 1e9)} GB free space needed`
}

/** The banner notice for an Admin response; null once filing storage is split. */
export function noticeOf(data: FilingStorageResponse | undefined): FilingStorageNotice | null {
  if (!data || data.state === 'split') return null
  return { state: data.state, needed_bytes: data.preflight?.needed_bytes ?? data.index.bytes }
}
