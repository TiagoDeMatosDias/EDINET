import { describe, expect, it } from 'vitest'

import type { ExpressionToken } from '../screening/types'
import { describeColumnMetric } from './comparisonModel'
import { describeFormulaMetric, formulaMetric, parseFormulaMetric } from './formulaMetric'

const col = (table: string, column: string): ExpressionToken => ({ type: 'column', table, column })

describe('formula metrics', () => {
  it('writes a formula as text and reads it back', () => {
    const tokens: ExpressionToken[] = [{ type: 'paren', value: '(' }, col('Stock_Prices', 'Price'), { type: 'op', op: '-' }, { type: 'value', value: '-0.5' }, { type: 'paren', value: ')' }, { type: 'op', op: '/' }, col('Odd', 'name ] here')]
    const metric = formulaMetric(tokens)
    expect(metric).toBe('=( [Stock_Prices.Price] - -0.5 ) / [Odd.name ]] here]')
    expect(parseFormulaMetric(metric)).toEqual([...tokens.slice(0, 3), { type: 'value', value: -0.5 }, ...tokens.slice(4)])
  })

  it('reads only formulas over at least one column', () => {
    expect(parseFormulaMetric('IncomeStatement.Net sales')).toBeNull()
    expect(parseFormulaMetric('=2 * 3')).toBeNull()
    expect(parseFormulaMetric('=[Price] * 2')).toBeNull()
    expect(parseFormulaMetric('=[A.x] ^ 2')).toBeNull()
  })

  it('names a preset formula by the preset and any other by its formula', () => {
    expect(describeFormulaMetric('=[ShareMetrics.Basic earnings (loss) per share] / [Stock_Prices.Price]')).toMatchObject({ label: 'Earnings yield', group: 'Formula', format: 'percent' })
    expect(describeColumnMetric('=[Stock_Prices.Price] * [ShareMetrics.Basic earnings (loss) per share]')).toMatchObject({ label: 'Share price × Basic earnings (loss) per share', group: 'Formula' })
    expect(describeColumnMetric('IncomeStatement_Rolling.Net sales_Growth_3_Year')).toEqual({ label: 'Net sales · 3-yr growth', group: 'Income statement · rolling' })
  })
})
