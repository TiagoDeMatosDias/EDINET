import { describe, expect, it } from 'vitest'

import { normalizeCriterion } from './expression-model'

describe('normalizeCriterion', () => {
  it('preserves saved full-expression operands', () => {
    const left = [{ type: 'column' as const, table: 'ShareMetrics', column: 'Net assets per share' }]
    const right = [
      { type: 'column' as const, table: 'Stock_Prices', column: 'Price' },
      { type: 'op' as const, op: '*' as const },
      { type: 'value' as const, value: 0.8 },
    ]

    const criterion = normalizeCriterion({ comparison_mode: 'full_expression', operator: '>', left_side: left, right_side: right })

    expect(criterion.left_side).toEqual(left)
    expect(criterion.right_side).toEqual(right)
  })

  it('upgrades legacy stock-price comparisons to editable expressions', () => {
    const criterion = normalizeCriterion({ table: 'ShareMetrics', column: 'Net assets per share', comparison_mode: 'stock_price', operator: '>' })

    expect(criterion.left_side).toEqual([{ type: 'column', table: 'ShareMetrics', column: 'Net assets per share' }])
    expect(criterion.right_side).toEqual([{ type: 'column', table: 'Stock_Prices', column: 'Price' }])
  })

  it('preserves the recent-split date criterion', () => {
    const criterion = normalizeCriterion({ comparison_mode: 'recent_split', operator: '=', value: '2024-11-01' })

    expect(criterion.comparison_mode).toBe('recent_split')
    expect(criterion.value).toBe('2024-11-01')
    expect(criterion.split_action).toBe('exclude')
    expect(criterion.split_status).toBe('confirmed')
    expect(criterion.split_date_operator).toBe('on_or_after')
  })

  it('preserves configurable recent-split options', () => {
    const criterion = normalizeCriterion({
      comparison_mode: 'recent_split', operator: '=', value: '2024-11-01',
      split_action: 'include', split_status: 'pending', split_date_operator: 'on_or_before',
    })

    expect(criterion.split_action).toBe('include')
    expect(criterion.split_status).toBe('pending')
    expect(criterion.split_date_operator).toBe('on_or_before')
  })

  it('keeps saved Stock_Splits date filters as date-input rules', () => {
    const criterion = normalizeCriterion({
      table: 'Stock_Splits', column: 'split_date', comparison_mode: 'fixed', operator: '>=', value: '2024-11-01',
    })

    expect(criterion.comparison_mode).toBe('fixed')
    expect(criterion.field_type).toBe('date')
    expect(criterion.value).toBe('2024-11-01')
  })
})
