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
})
