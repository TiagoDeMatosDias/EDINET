export type MetricCatalog = Record<string, string[]>

export type ExpressionToken =
  | { type: 'column'; table: string; column: string }
  | { type: 'value'; value: string | number }
  | { type: 'tag'; value: string }
  | { type: 'op'; op: '+' | '-' | '*' | '/' }
  | { type: 'paren'; value: '(' | ')' }

export interface Criterion {
  id: string
  table?: string
  column?: string
  operator?: string
  value?: string | number | null
  value2?: string | number | null
  values?: Array<string | number>
  field_type?: 'num' | 'text' | 'percent' | 'date' | string
  comparison_mode: string
  split_action?: 'exclude' | 'include' | string
  split_status?: 'confirmed' | 'rejected' | 'pending' | 'any' | string
  split_date_operator?: 'on_or_after' | 'on_or_before' | string
  split_window_days?: number | null
  compare_table?: string
  compare_column?: string
  offset?: number | null
  left_side?: ExpressionToken[]
  right_side?: ExpressionToken[]
  left_expression?: string
  /** Rules sharing a group id form one term, combined by ``group_match``. */
  group?: string | null
  group_match?: 'any' | 'all'
  /** Disabled rules stay in the screen but are not applied. */
  enabled?: boolean
}

export type CriteriaMatch = 'all' | 'any'

export interface ComputedColumn {
  name: string
  formula_type: string
  expression_tokens?: ExpressionToken[]
  numerator_table?: string
  numerator_column?: string
  denominator_table?: string
  denominator_column?: string
  formula?: string | null
  /** ``percent`` shows fractions such as yields as percentages. */
  format?: 'percent' | 'number' | null
}

export interface SavedScreen {
  name?: string
  criteria?: Criterion[]
  criteria_match?: CriteriaMatch
  columns?: string[]
  computed_columns?: ComputedColumn[]
  screening_date?: string | null
  ranking_algorithm?: string
  ranking_rules?: Array<Record<string, unknown>>
}

export interface SavedScreenSummary {
  screen_id: string
  name: string
  updated_at?: string | null
  created_at?: string | null
  rule_count: number
  column_count: number
  criteria_match?: CriteriaMatch
  screening_date?: string | null
}

/** ``{"Table.Column": "percent"}`` for catalog columns with a declared display format. */
export type ColumnFormats = Record<string, string>
