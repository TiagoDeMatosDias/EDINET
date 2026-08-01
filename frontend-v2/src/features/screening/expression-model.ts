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
  if (raw.comparison_mode === 'recent_split') {
    return {
      ...base,
      operator: raw.operator || '=',
      comparison_mode: 'recent_split',
      value: raw.value ?? '',
      split_action: raw.split_action === 'include' ? 'include' : 'exclude',
      split_status: ['confirmed', 'rejected', 'pending', 'any'].includes(raw.split_status ?? '') ? raw.split_status : 'confirmed',
      split_date_operator: raw.split_date_operator === 'on_or_before' ? 'on_or_before' : 'on_or_after',
    }
  }
  const stockSplitDate = raw.table === 'Stock_Splits' && ['split_date', 'announced_at', 'ex_date', 'effective_date', 'record_date'].includes(raw.column ?? '')
  if (stockSplitDate && (!raw.comparison_mode || raw.comparison_mode === 'fixed')) {
    return { ...base, operator: raw.operator || '>', comparison_mode: 'fixed', value: raw.value ?? '', value2: raw.value2 ?? '', field_type: 'date' }
  }
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

export function newRecentSplitCriterion(): Criterion {
  return {
    id: crypto.randomUUID(),
    operator: '=',
    comparison_mode: 'recent_split',
    value: '',
    field_type: 'date',
    split_action: 'exclude',
    split_status: 'confirmed',
    split_date_operator: 'on_or_after',
  }
}

export function newExpressionCriterion(): Criterion {
  return normalizeCriterion({
    operator: '>',
    comparison_mode: 'full_expression',
    left_side: [{ type: 'column', table: 'Stock_Prices', column: 'Price' }],
    right_side: [{ type: 'value', value: 0 }],
  })
}
