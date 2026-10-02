import { describe, expect, it } from 'vitest'

import { FORMULA_PRESETS } from './metricCatalog'
import { describeRule, describeScreen, expressionProblem, operatorsFor, ruleProblem, ruleShape, withMetric, withOperator } from './ruleModel'
import type { Criterion, ExpressionToken } from './types'

const price: ExpressionToken = { type: 'column', table: 'Stock_Prices', column: 'Price' }
const industry: ExpressionToken = { type: 'column', table: 'CompanyInfo', column: 'Company_Industry' }
const pe = FORMULA_PRESETS.find(preset => preset.id === 'pe')!.tokens
const rule = (left: ExpressionToken[], operator: string, right: ExpressionToken[], extra: Partial<Criterion> = {}): Criterion => ({ id: 'r', comparison_mode: 'full_expression', operator, left_side: left, right_side: right, ...extra })

describe('rule shapes and conversions', () => {
  it('edits single metrics and formulas as filters, and anything richer as an expression', () => {
    expect(ruleShape(rule([price], '>', [{ type: 'value', value: 1 }]))).toBe('simple')
    expect(ruleShape(rule(pe, '<', [{ type: 'value', value: 15 }]))).toBe('simple')
    expect(ruleShape(rule([price], '>', [price]))).toBe('simple')
    expect(ruleShape(rule([price, { type: 'op', op: '*' }, { type: 'value', value: 2 }], '>', [price]))).toBe('expression')
    expect(ruleShape({ id: 's', comparison_mode: 'recent_split' })).toBe('split')
  })

  it('converts between operators without losing the metric, value, or group', () => {
    const grouped = rule([price], '>', [{ type: 'value', value: 100 }], { group: 'g', group_match: 'all', enabled: false })
    const between = withOperator(grouped, 'BETWEEN')
    expect(between).toMatchObject({ table: 'Stock_Prices', column: 'Price', operator: 'BETWEEN', value: 100, value2: '', group: 'g', group_match: 'all', enabled: false })
    const oneOf = withOperator(rule([industry], '=', [{ type: 'value', value: 'Banks' }]), 'IN')
    expect(oneOf).toMatchObject({ operator: 'IN', comparison_mode: 'in', values: ['Banks'] })
    expect(withOperator(oneOf, 'LIKE')).toMatchObject({ operator: 'LIKE', value: '%Banks%' })
    expect(withOperator(rule([price], '>', [{ type: 'value', value: 1 }]), 'IS')).toMatchObject({ operator: 'IS', right_side: [] })
  })

  it('keeps comparisons sensible when the metric changes', () => {
    expect(withMetric(rule([price], '>', [{ type: 'value', value: 5 }]), [industry]).operator).toBe('=')
    expect(withMetric(rule([price], '<', [{ type: 'value', value: 5 }]), pe)).toMatchObject({ left_side: pe, operator: '<', right_side: [{ type: 'value', value: 5 }] })
    expect(operatorsFor(pe).map(operator => operator.value)).not.toContain('BETWEEN')
    expect(operatorsFor([industry]).map(operator => operator.value)).not.toContain('>')
  })
})

describe('rule validation and reading', () => {
  it('explains what a rule is missing', () => {
    expect(ruleProblem(rule([], '>', [{ type: 'value', value: '' }]))).toBe('Choose a metric.')
    expect(ruleProblem(rule([price], '>', [{ type: 'value', value: '' }]))).toBe('The right side has an empty value.')
    expect(ruleProblem({ id: 'b', table: 'Stock_Prices', column: 'Price', operator: 'BETWEEN', value: 1, value2: '', comparison_mode: 'fixed' })).toBe('Enter both ends of the range.')
    expect(ruleProblem(rule([price], 'IS', []))).toBeNull()
    expect(expressionProblem([price, { type: 'op', op: '*' }], 'left')).toBe('The left side cannot end with an operator or “(”.')
    expect(expressionProblem([{ type: 'paren', value: '(' }, price], 'formula')).toBe('The formula has an unmatched “(”.')
  })

  it('reads a rule back as one line', () => {
    const label = (_table: string, column: string) => column
    expect(describeRule(rule(pe, '<', [{ type: 'value', value: 15 }]), label, () => false)).toBe('P/E ratio < 15')
    expect(describeRule(rule([{ type: 'column', table: 'Financial_Ratios', column: 'ROE' }], '>=', [{ type: 'value', value: 0.15 }]), label, () => true)).toBe('ROE ≥ 15%')
    expect(describeRule({ id: 's', comparison_mode: 'recent_split', split_action: 'exclude', split_window_days: 90 }, label, () => false)).toBe('Exclude companies with a split in the last 90 days')
  })

  it('reads the whole screen with its groups and skips rules that are off', () => {
    const named = (id: string, extra: Partial<Criterion> = {}): Criterion => ({ ...rule([price], '>', [{ type: 'value', value: 1 }]), id, ...extra })
    const criteria = [named('a'), named('b', { group: 'g', group_match: 'any' }), named('c', { group: 'g', group_match: 'any' }), named('d', { enabled: false })]
    expect(describeScreen(criteria, 'all', criterion => criterion.id.toUpperCase())).toBe('A and (B or C)')
    expect(describeScreen(criteria.map(criterion => criterion.group ? { ...criterion, group_match: 'all' as const } : criterion), 'any', criterion => criterion.id.toUpperCase())).toBe('A or (B and C)')
  })
})
