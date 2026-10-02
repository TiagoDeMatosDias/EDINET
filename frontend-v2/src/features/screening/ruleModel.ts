import { isDateMetric, isTextMetric, presetForTokens, toPercentText } from './metricCatalog'
import type { CriteriaMatch, Criterion, ExpressionToken } from './types'

/**
 * Rules keep the wire shapes the screening API has always accepted; this
 * module decides how a rule is edited (a one-line filter, a split event, or a
 * full expression) and converts between operators without losing anything.
 */

export type RuleShape = 'split' | 'simple' | 'expression'

export const OPERATORS: Array<{ value: string; label: string; hint: string }> = [
  { value: '>', label: '>', hint: 'Greater than' },
  { value: '>=', label: '≥', hint: 'Greater than or equal to' },
  { value: '<', label: '<', hint: 'Less than' },
  { value: '<=', label: '≤', hint: 'Less than or equal to' },
  { value: '=', label: '=', hint: 'Equal to' },
  { value: '!=', label: '≠', hint: 'Not equal to' },
  { value: 'BETWEEN', label: 'between', hint: 'Between two values, inclusive' },
  { value: 'IN', label: 'is one of', hint: 'Matches any value in a comma-separated list' },
  { value: 'LIKE', label: 'contains', hint: 'Contains the text; % and _ work as wildcards' },
  { value: 'IS', label: 'is empty', hint: 'No value is reported' },
  { value: 'IS NOT', label: 'has a value', hint: 'Any value is reported' },
]

const NUMERIC_ONLY = new Set(['>', '>=', '<', '<=', 'BETWEEN'])
const SINGLE_COLUMN_ONLY = new Set(['BETWEEN', 'IN', 'LIKE'])
const COMPARISONS = new Set(['>', '>=', '<', '<=', '=', '!='])

function only(tokens: ExpressionToken[] | undefined) {
  return tokens?.length === 1 ? tokens[0] : undefined
}

function isMetricTokens(tokens: ExpressionToken[] | undefined) {
  return only(tokens)?.type === 'column' || Boolean(presetForTokens(tokens))
}

export function ruleShape(criterion: Criterion): RuleShape {
  if (criterion.comparison_mode === 'recent_split') return 'split'
  if (criterion.comparison_mode !== 'full_expression') return 'simple'
  const right = criterion.right_side ?? []
  const simpleRight = right.length === 0
    ? criterion.operator === 'IS' || criterion.operator === 'IS NOT'
    : right.length === 1 && ['value', 'column', 'tag'].includes(right[0].type)
  // A rule with no metric yet is a blank filter waiting for one.
  const left = criterion.left_side ?? []
  return (left.length === 0 || isMetricTokens(left)) && simpleRight ? 'simple' : 'expression'
}

/** The left side of any simple rule as tokens: its single column or formula. */
export function ruleLeft(criterion: Criterion): ExpressionToken[] {
  if (criterion.comparison_mode === 'full_expression') return criterion.left_side ?? []
  return criterion.table || criterion.column ? [{ type: 'column', table: criterion.table ?? '', column: criterion.column ?? '' }] : []
}

/** The single column of a simple rule, when it has one (formulas do not). */
export function ruleColumn(criterion: Criterion) {
  const token = only(ruleLeft(criterion))
  return token?.type === 'column' ? token : undefined
}

function scalarValue(criterion: Criterion): string | number {
  if (criterion.comparison_mode === 'full_expression') {
    const token = only(criterion.right_side)
    return token && (token.type === 'value' || token.type === 'tag') ? token.value : ''
  }
  if (criterion.operator === 'IN') return criterion.values?.[0] ?? ''
  if (criterion.operator === 'LIKE') return String(criterion.value ?? '').replace(/^%|%$/g, '')
  return criterion.value ?? ''
}

function carry(criterion: Criterion): Partial<Criterion> {
  const kept: Partial<Criterion> = { id: criterion.id }
  if (criterion.group) { kept.group = criterion.group; kept.group_match = criterion.group_match ?? 'any' }
  if (criterion.enabled === false) kept.enabled = false
  return kept
}

function fieldType(table?: string, column?: string) {
  if (isDateMetric(table, column)) return 'date'
  return isTextMetric(table, column) ? 'text' : 'num'
}

/** Operators a simple rule on ``left`` can use. */
export function operatorsFor(left: ExpressionToken[]) {
  const column = only(left)?.type === 'column' ? only(left) as Extract<ExpressionToken, { type: 'column' }> : undefined
  if (column?.table === 'Company_Tags') return OPERATORS.filter(operator => ['=', '!=', 'IN', 'LIKE'].includes(operator.value))
  if (!column) return OPERATORS.filter(operator => !SINGLE_COLUMN_ONLY.has(operator.value))
  if (isTextMetric(column.table, column.column)) return OPERATORS.filter(operator => !NUMERIC_ONLY.has(operator.value))
  return OPERATORS
}

/** The same rule with another operator, reshaped to what that operator needs. */
export function withOperator(criterion: Criterion, operator: string): Criterion {
  const left = ruleLeft(criterion)
  const column = ruleColumn(criterion)
  const value = scalarValue(criterion)
  const base = carry(criterion)
  if (column && (column.table === 'Company_Tags' || SINGLE_COLUMN_ONLY.has(operator))) {
    const fixed = { ...base, table: column.table, column: column.column, field_type: fieldType(column.table, column.column) } as Criterion
    if (operator === 'BETWEEN') return { ...fixed, operator, comparison_mode: 'fixed', value: criterion.operator === 'BETWEEN' ? criterion.value ?? '' : value, value2: criterion.operator === 'BETWEEN' ? criterion.value2 ?? '' : '' }
    if (operator === 'IN') return { ...fixed, operator, comparison_mode: 'in', values: criterion.operator === 'IN' ? criterion.values ?? [''] : [value] }
    if (operator === 'LIKE') return { ...fixed, operator, comparison_mode: 'like', value: value === '' ? '' : `%${String(value)}%` }
    return { ...fixed, operator, comparison_mode: 'fixed', value }
  }
  if (operator === 'IS' || operator === 'IS NOT') return { ...base, operator, comparison_mode: 'full_expression', left_side: left, right_side: [] } as Criterion
  const right = criterion.comparison_mode === 'full_expression' && criterion.right_side?.length ? criterion.right_side : [{ type: 'value', value } as ExpressionToken]
  return { ...base, operator: COMPARISONS.has(operator) ? operator : '>', comparison_mode: 'full_expression', left_side: left, right_side: right } as Criterion
}

/** The same rule measuring another metric (a column, or a formula's tokens). */
export function withMetric(criterion: Criterion, tokens: ExpressionToken[]): Criterion {
  const column = only(tokens)?.type === 'column' ? only(tokens) as Extract<ExpressionToken, { type: 'column' }> : undefined
  const operator = criterion.operator ?? '>'
  const allowed = operatorsFor(tokens).map(item => item.value)
  if (column && column.table === 'Company_Tags') {
    return withOperator({ ...carry(criterion), comparison_mode: 'fixed', table: column.table, column: column.column, operator: '=', value: '' } as Criterion, allowed.includes(operator) ? operator : '=')
  }
  if (column && SINGLE_COLUMN_ONLY.has(operator) && allowed.includes(operator) && criterion.comparison_mode !== 'full_expression') {
    return { ...criterion, table: column.table, column: column.column, field_type: fieldType(column.table, column.column) }
  }
  const nextOperator = allowed.includes(operator) && !SINGLE_COLUMN_ONLY.has(operator) ? operator : column && isTextMetric(column.table, column.column) ? '=' : '>'
  const right = criterion.comparison_mode === 'full_expression' && criterion.right_side?.length ? criterion.right_side : [{ type: 'value', value: scalarValue(criterion) } as ExpressionToken]
  const shaped = { ...carry(criterion), operator: nextOperator, comparison_mode: 'full_expression', left_side: tokens, right_side: nextOperator === 'IS' || nextOperator === 'IS NOT' ? [] : right } as Criterion
  if (column && isDateMetric(column.table, column.column)) shaped.field_type = 'date'
  return shaped
}

/** Why a rule cannot run yet, or ``null`` when it can. */
export function ruleProblem(criterion: Criterion): string | null {
  if (criterion.comparison_mode === 'recent_split') {
    const window = Number(criterion.split_window_days)
    return Number.isFinite(window) && window >= 1 ? null : criterion.value ? null : 'Choose a cutoff date or a day window.'
  }
  const empty = (value: unknown) => value == null || String(value).trim() === ''
  if (criterion.comparison_mode !== 'full_expression') {
    if (!criterion.table || !criterion.column) return 'Choose a metric.'
    if (criterion.operator === 'BETWEEN') return empty(criterion.value) || empty(criterion.value2) ? 'Enter both ends of the range.' : null
    if (criterion.operator === 'IN') return (criterion.values ?? []).some(value => !empty(value)) ? null : 'List at least one value.'
    if (criterion.operator === 'IS' || criterion.operator === 'IS NOT') return null
    return empty(criterion.value) || criterion.value === '%%' ? 'Enter a value.' : null
  }
  const leftProblem = expressionProblem(criterion.left_side ?? [], 'left')
  if (leftProblem) return leftProblem
  if (criterion.operator === 'IS' || criterion.operator === 'IS NOT') return null
  return expressionProblem(criterion.right_side ?? [], 'right')
}

/** Client-side copy of the server's expression grammar, so mistakes show before running. */
export function expressionProblem(tokens: ExpressionToken[], side: 'left' | 'right' | 'formula') {
  const where = side === 'formula' ? 'The formula' : `The ${side} side`
  if (!tokens.length) return side === 'left' ? 'Choose a metric.' : `${where} needs a value or metric.`
  let expectOperand = true
  let depth = 0
  for (const token of tokens) {
    if (token.type === 'column' && (!token.table || !token.column)) return `${where} has a metric that is not chosen.`
    if (token.type === 'value' && String(token.value ?? '').trim() === '') return `${where} has an empty value.`
    if (token.type === 'value' || token.type === 'tag' || token.type === 'column') {
      if (!expectOperand) return `${where} needs an operator (+ − × ÷) between terms.`
      expectOperand = false
    } else if (token.type === 'op') {
      if (expectOperand) return `${where} has an operator without a term before it.`
      expectOperand = true
    } else if (token.value === '(') {
      if (!expectOperand) return `${where} needs an operator before “(”.`
      depth += 1
    } else {
      if (expectOperand || depth === 0) return `${where} has an unmatched “)”.`
      depth -= 1
    }
  }
  if (expectOperand) return `${where} cannot end with an operator or “(”.`
  return depth ? `${where} has an unmatched “(”.` : null
}

function tokenText(token: ExpressionToken, label: (table: string, column: string) => string) {
  if (token.type === 'column') return label(token.table, token.column)
  if (token.type === 'op') return { '*': '×', '/': '÷', '+': '+', '-': '−' }[token.op]
  return String(token.value)
}

/** A one-line reading of a rule: "P/E ratio < 15", "ROE > 15%". */
export function describeRule(criterion: Criterion, label: (table: string, column: string) => string, percent: (tokens: ExpressionToken[]) => boolean): string {
  if (criterion.comparison_mode === 'recent_split') {
    const action = criterion.split_action === 'include' ? 'Only companies with' : 'Exclude companies with'
    return criterion.split_window_days ? `${action} a split in the last ${criterion.split_window_days} days` : `${action} a split ${criterion.split_date_operator === 'on_or_before' ? 'on or before' : 'on or after'} ${criterion.value || '…'}`
  }
  const left = ruleLeft(criterion)
  const preset = presetForTokens(left)
  const leftText = preset ? preset.label : left.map(token => tokenText(token, label)).join(' ') || '…'
  const pct = percent(left)
  const show = (value: unknown) => pct && value !== '' && Number.isFinite(Number(value)) ? `${toPercentText(value)}%` : String(value ?? '…')
  const operator = OPERATORS.find(item => item.value === criterion.operator)?.label ?? criterion.operator ?? ''
  if (criterion.operator === 'IS' || criterion.operator === 'IS NOT') return `${leftText} ${operator}`
  if (criterion.operator === 'BETWEEN') return `${leftText} between ${show(criterion.value)} and ${show(criterion.value2)}`
  if (criterion.operator === 'IN') return `${leftText} is one of ${(criterion.values ?? []).join(', ')}`
  if (criterion.operator === 'LIKE') return `${leftText} contains “${String(criterion.value ?? '').replace(/^%|%$/g, '')}”`
  const right = criterion.comparison_mode === 'full_expression' ? criterion.right_side ?? [] : [{ type: 'value', value: criterion.value ?? '' } as ExpressionToken]
  const rightText = right.length === 1 && right[0].type === 'value' ? show(right[0].value) : right.map(token => tokenText(token, label)).join(' ')
  return `${leftText} ${operator} ${rightText}`
}

/** The whole screen as one line, groups in parentheses: “P/E ratio < 12 and (ROE > 15% or Dividend yield > 3%)”. */
export function describeScreen(criteria: Criterion[], match: CriteriaMatch, describe: (criterion: Criterion) => string) {
  const active = criteria.filter(criterion => criterion.enabled !== false)
  const described = new Set<string>()
  const terms: string[] = []
  for (const criterion of active) {
    if (!criterion.group) { terms.push(describe(criterion)); continue }
    if (described.has(criterion.group)) continue
    described.add(criterion.group)
    const members = active.filter(item => item.group === criterion.group)
    const inner = members.map(describe).join(criterion.group_match === 'all' ? ' and ' : ' or ')
    terms.push(members.length > 1 ? `(${inner})` : inner)
  }
  return terms.join(match === 'any' ? ' or ' : ' and ')
}

export function newGroupId() {
  return `g-${crypto.randomUUID().slice(0, 8)}`
}

/** A blank one-line filter whose metric picker opens for the user. */
export function newFilterRule(group?: { id: string; match: 'any' | 'all' }): Criterion {
  return { id: crypto.randomUUID(), comparison_mode: 'full_expression', operator: '>', left_side: [], right_side: [{ type: 'value', value: '' }], ...(group ? { group: group.id, group_match: group.match } : {}) }
}
