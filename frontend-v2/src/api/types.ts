export type JsonRecord = Record<string, unknown>

export interface Health {
  status: string
  timestamp: string
  jobs_active: number
}

export interface Job {
  job_id: string
  status: string
  current_step?: string | null
  progress_percent?: number
  created_at?: string
  started_at?: string | null
  completed_at?: string | null
  error_message?: string | null
}

export interface PipelineField {
  name: string
  type?: string
  description?: string
  required?: boolean
  default?: unknown
  choices?: string[]
}

export interface PipelineStep {
  name: string
  display_name?: string
  canonical_name?: string
  description?: string
  category?: string
  aliases?: string[]
  input_fields?: PipelineField[]
  parameters?: PipelineField[]
  supports_overwrite?: boolean
}

export interface SecuritySearchResult {
  company_code: string
  ticker: string
  company_name: string
  industry?: string
  market?: string
  latest_price?: number | null
  latest_price_date?: string | null
}

export interface SecurityOverview {
  company: Record<string, unknown>
  market: Record<string, unknown>
  metrics: Record<string, number | null>
  quality?: Record<string, unknown>
  metadata?: Record<string, unknown>
}

export interface HistoryMetric {
  field: string
  display_name: string
  values: Array<number | string | null>
}

export interface HistoryTable {
  display_name: string
  metrics: HistoryMetric[]
}

export interface SecurityHistory {
  periods: string[]
  tables: Record<string, HistoryTable>
}

export interface ScreeningCriterion {
  id: string
  table: string
  column: string
  operator: string
  value: string | number
  field_type: string
  comparison_mode: string
}

export interface ScreeningResult {
  columns: string[]
  rows: unknown[][]
  row_count: number
  sql_display?: string
}
