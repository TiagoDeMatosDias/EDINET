import type { ComputedColumn, Criterion, ExpressionToken } from './types'

function persistedValue(value: string | number | null | undefined) {
  if (typeof value !== 'string') return value
  const trimmed = value.trim()
  return trimmed !== '' && Number.isFinite(Number(trimmed)) ? Number(trimmed) : value
}

function persistedToken(token: ExpressionToken): ExpressionToken {
  return token.type === 'value' ? { ...token, value: persistedValue(token.value) ?? '' } : token
}

export function serializeExpressionTokens(tokens: ExpressionToken[] | undefined) {
  return tokens?.map(persistedToken)
}

export function serializeCriterion({ id: _id, ...criterion }: Criterion) {
  void _id
  const serialized = { ...criterion }
  if (criterion.operator !== 'LIKE') serialized.value = persistedValue(criterion.value)
  serialized.value2 = persistedValue(criterion.value2)
  serialized.values = criterion.values?.map(value => persistedValue(value) ?? '')
  serialized.left_side = serializeExpressionTokens(criterion.left_side)
  serialized.right_side = serializeExpressionTokens(criterion.right_side)
  return serialized
}
export function serializeComputedColumn(column: ComputedColumn): ComputedColumn {
  return { ...column, expression_tokens: serializeExpressionTokens(column.expression_tokens) }
}