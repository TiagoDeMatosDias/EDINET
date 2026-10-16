import { describe, expect, it } from 'vitest'

import { FORMULA_PRESETS, presetColumn } from './metricCatalog'
import { comparisonMetrics, ruleMetrics, ruleOutput } from './ruleOutput'
import type { Criterion, ExpressionToken } from './types'

const col = (table: string, column: string): ExpressionToken => ({ type: 'column', table, column })
const op = (value: '+' | '-' | '*' | '/'): ExpressionToken => ({ type: 'op', op: value })
const num = (value: number | string): ExpressionToken[] => [{ type: 'value', value }]
const preset = (id: string) => FORMULA_PRESETS.find(item => item.id === id)!.tokens
const rule = (left: ExpressionToken[], operator: string, right: ExpressionToken[], extra: Partial<Criterion> = {}): Criterion => ({ id: crypto.randomUUID(), comparison_mode: 'full_expression', operator, left_side: left, right_side: right, ...extra })

const PRICE = col('Stock_Prices', 'Price')
const EPS = col('ShareMetrics', 'Basic earnings (loss) per share')
const ROE = col('Financial_Ratios', 'Return on Equity')
const NCAV = col('PerShare_Metrics', 'NCAV Per Share')

describe('what the rules measure', () => {
  it('lists each column a rule reads and the result of each calculation, in rule order', () => {
    const metrics = ruleMetrics([
      rule([ROE], '>', num(0.15)),
      rule([PRICE, op('*'), EPS], '>', num('1000')),
      rule([NCAV], '>', [PRICE, op('*'), { type: 'value', value: 0.66 }]),
      { id: 'industry', table: 'CompanyInfo', column: 'Company_Industry', operator: 'IN', values: ['Banks'], comparison_mode: 'in' },
    ])
    expect(metrics).toEqual([
      { type: 'column', reference: 'Financial_Ratios.Return on Equity' },
      // The price is always shown, so only its partner and the product are added.
      { type: 'column', reference: 'ShareMetrics.Basic earnings (loss) per share' },
      { type: 'formula', tokens: [PRICE, op('*'), EPS] },
      { type: 'column', reference: 'PerShare_Metrics.NCAV Per Share' },
      { type: 'formula', tokens: [PRICE, op('*'), { type: 'value', value: 0.66 }] },
      { type: 'column', reference: 'CompanyInfo.Company_Industry' },
    ])
  })

  it('takes a named formula as one metric and lists each metric once', () => {
    const metrics = ruleMetrics([rule(preset('pe'), '>', num(0)), rule(preset('pe'), '<', num(12)), rule([ROE], '>', num(0.1)), rule([ROE], '<', num(0.5))])
    expect(metrics.map(metric => metric.type === 'column' ? metric.reference : metric.preset?.id)).toEqual(['pe', 'Financial_Ratios.Return on Equity'])
  })

  it('skips disabled rules, split events, and calculations that are not finished', () => {
    expect(ruleMetrics([
      rule([ROE], '>', num(0.15), { enabled: false }),
      { id: 'split', comparison_mode: 'recent_split', split_window_days: 365 },
      rule([PRICE, op('/')], '>', num(1)),
    ])).toEqual([])
  })
})

describe('rule output columns', () => {
  it('adds only what the screen does not show, naming new calculations by their formula', () => {
    const pe = presetColumn(FORMULA_PRESETS.find(item => item.id === 'pe')!)
    const output = ruleOutput(
      [rule(preset('pe'), '<', num(12)), rule([ROE], '>', num(0.15)), rule([EPS, op('/'), NCAV], '>', num(1)), rule(preset('pb'), '<', num(1))],
      { columns: ['CompanyInfo.Company_Name', 'Financial_Ratios.Return on Equity'], computed: [pe] },
    )
    expect(output.columns).toEqual(['ShareMetrics.Basic earnings (loss) per share', 'PerShare_Metrics.NCAV Per Share'])
    expect(output.computed.map(column => [column.name, column.format])).toEqual([['Basic earnings (loss) per share ÷ NCAV Per Share', 'number'], ['P/B ratio', 'number']])
    expect(output.shown.map(item => [item.column ?? item.computed, item.existing])).toEqual([
      ['P/E ratio', true],
      ['Financial_Ratios.Return on Equity', true],
      ['ShareMetrics.Basic earnings (loss) per share', false],
      ['PerShare_Metrics.NCAV Per Share', false],
      ['Basic earnings (loss) per share ÷ NCAV Per Share', false],
      ['P/B ratio', false],
    ])
  })

  it('names a calculation apart from a different column of the same name', () => {
    const custom = { name: 'P/E ratio', formula_type: 'expression', expression_tokens: [PRICE, op('/'), col('ShareMetrics', 'Diluted earnings per share')] }
    const output = ruleOutput([rule(preset('pe'), '<', num(12))], { columns: [], computed: [custom] })
    expect(output.computed.map(column => column.name)).toEqual(['P/E ratio · rule'])
  })
})

describe('comparison metrics from the rules', () => {
  it('passes statement columns and formulas, leaving standard metrics and company facts out', () => {
    expect(comparisonMetrics([
      rule(preset('pe'), '<', num(12)),
      rule(preset('earnings_yield'), '>', num(0.05)),
      rule([ROE], '>', num(0.15)),
      rule([PRICE, op('*'), EPS], '>', num(1000)),
      rule([col('CompanyInfo', 'Capital_Stock'), op('/'), EPS], '>', num(1)),
      { id: 'industry', table: 'CompanyInfo', column: 'Company_Industry', operator: 'LIKE', value: '%Bank%', comparison_mode: 'like' },
    ])).toEqual([
      '=[ShareMetrics.Basic earnings (loss) per share] / [Stock_Prices.Price]',
      'Financial_Ratios.Return on Equity',
      'ShareMetrics.Basic earnings (loss) per share',
      '=[Stock_Prices.Price] * [ShareMetrics.Basic earnings (loss) per share]',
    ])
  })
})
