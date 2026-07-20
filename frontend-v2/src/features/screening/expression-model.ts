import type { Criterion, ExpressionToken } from './types'

export function metricRef(table = '', column = '') {
  return table && column ? `${table}.${column}` : ''
}

export function splitMetricRef(ref: string) {
  const index = ref.indexOf('.')
  return index < 1 ? { table: '', column: '' } : {
    table: ref.slice(0, index),
    column: ref.slice(index + 1),
  }
}

export function normalizeCriterion(raw: Partial<Criterion>): Criterion {
  const base = { ...raw, id: raw.id || crypto.randomUUID() }
  if (raw.comparison_mode === 'full_expression') {
    return { ...base, operator: raw.operator || '>', comparison_mode: 'full_expression', left_side: raw.left_side ?? [], right_side: raw.right_side ?? [] }
  }
  if (raw.operator === 'IN' || raw.comparison_mode === 'in') {
    return { ...base, operator: 'IN', comparison_mode: 'in', values: raw.values ?? [] }
  }
  if (raw.operator === 'LIKE' || raw.comparison_mode === 'like') {
    return { ...base, operator: 'LIKE', comparison_mode: 'like', value: raw.value ?? '' }
  }
  if (raw.operator === 'BETWEEN') {
    return { ...base, operator: 'BETWEEN', comparison_mode: 'fixed' }
  }
  const left: ExpressionToken[] = [{ type: 'column', table: raw.table ?? '', column: raw.column ?? '' }]
  let right: ExpressionToken[] = [{ type: 'value', value: raw.value ?? 0 }]
  if (raw.comparison_mode === 'column' && raw.compare_table && raw.compare_column) {
    right = [{ type: 'column', table: raw.compare_table, column: raw.compare_column }]
    if (raw.offset) right.push({ type: 'op', op: '+' }, { type: 'value', value: raw.offset })
  } else if (raw.comparison_mode === 'expression' && raw.right_side?.length) {
    right = raw.right_side
  } else if (raw.comparison_mode === 'stock_price') {
    right = [{ type: 'column', table: 'Stock_Prices', column: 'Price' }]
  }
  return { ...base, operator: raw.operator || '>', comparison_mode: 'full_expression', left_side: left, right_side: right }
}

export function newExpressionCriterion(): Criterion {
  return normalizeCriterion({
    operator: '>',
    comparison_mode: 'full_expression',
    left_side: [{ type: 'column', table: 'Stock_Prices', column: 'Price' }],
    right_side: [{ type: 'value', value: 0 }],
  })
}
