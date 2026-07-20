import { describe, expect, it } from 'vitest'

import { serializeComputedColumn, serializeCriterion } from './criterion-values'

describe('serializeCriterion', () => {
  it('converts decimal expression strings at the API boundary', () => {
    const criterion = serializeCriterion({
      id: 'rule', comparison_mode: 'full_expression', operator: '>',
      left_side: [{ type: 'column', table: 'FinancialRatios', column: 'ROA' }],
      right_side: [{ type: 'value', value: '0.05' }],
    })
    expect(criterion.right_side).toEqual([{ type: 'value', value: 0.05 }])
  })

  it('normalizes between and list values without changing text filters', () => {
    expect(serializeCriterion({ id: 'between', comparison_mode: 'fixed', operator: 'BETWEEN', value: '.5', value2: '1.2' })).toMatchObject({ value: 0.5, value2: 1.2 })
    expect(serializeCriterion({ id: 'like', comparison_mode: 'like', operator: 'LIKE', value: '123' }).value).toBe('123')
  })

  it('serializes derived expression values and preserves parentheses', () => {
    expect(serializeComputedColumn({
      name: 'Adjusted value',
      formula_type: 'expression',
      expression_tokens: [
        { type: 'paren', value: '(' },
        { type: 'value', value: '0.8' },
        { type: 'op', op: '*' },
        { type: 'column', table: 'Stock_Prices', column: 'Price' },
        { type: 'paren', value: ')' },
      ],
    }).expression_tokens).toEqual([
      { type: 'paren', value: '(' },
      { type: 'value', value: 0.8 },
      { type: 'op', op: '*' },
      { type: 'column', table: 'Stock_Prices', column: 'Price' },
      { type: 'paren', value: ')' },
    ])
  })})
