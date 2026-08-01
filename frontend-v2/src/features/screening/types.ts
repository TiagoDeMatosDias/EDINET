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
  operator: string
  value?: string | number | null
  value2?: string | number | null
  values?: Array<string | number>
  field_type?: 'num' | 'text' | 'percent' | 'date' | string
  comparison_mode: string
  split_action?: 'exclude' | 'include' | string
  split_status?: 'confirmed' | 'rejected' | 'pending' | 'any' | string
  split_date_operator?: 'on_or_after' | 'on_or_before' | string
  compare_table?: string
  compare_column?: string
  offset?: number | null
  left_side?: ExpressionToken[]
  right_side?: ExpressionToken[]
  left_expression?: string
}

export interface ComputedColumn {
  name: string
  formula_type: string
  expression_tokens?: ExpressionToken[]
  numerator_table?: string
  numerator_column?: string
  denominator_table?: string
  denominator_column?: string
  formula?: string | null
}

export interface SavedScreen {
  name?: string
  criteria?: Criterion[]
  columns?: string[]
  computed_columns?: ComputedColumn[]
  screening_date?: string | null
  ranking_algorithm?: string
  ranking_rules?: Array<Record<string, unknown>>
}
