import { describe, expect, it } from 'vitest'

import { indexedPerShare, periodKey, periodRange, selectPayments, stackIncome, summarizeIncome, weightedGrowthByYear, withheld } from './incomeModel'
import { durationText, heldDays } from './portfolioFormat'
import type { IncomeCompany, IncomePayment } from './portfolioTypes'

function payment(date: string, symbol: string, gross: number, tax = -gross * 0.15, currency = 'USD'): IncomePayment {
  return { date, symbol, type: 'Dividend', currency, per_share: 1, shares: gross, gross_native: gross, tax_native: tax, net_native: gross + tax, gross, tax, net: gross + tax, withholding_rate: -tax / gross }
}

function company(symbol: string, annual: Array<[number, number, boolean, number | null, number]>): IncomeCompany {
  return {
    symbol, name: symbol, currency: 'USD', is_held: true, shares_held: 1, gross: 0, tax: 0, net: 0, withholding_rate: 0.15, share_of_income: null,
    payments: 4, first_date: '2022-01-01', last_date: '2025-12-01', frequency: 4, ttm_net: 0, ttm_per_share: null, latest_per_share: null, latest_date: null,
    per_share_growth_1y: null, current_yield: null, yield_on_cost: null,
    annual: annual.map(([year, perShare, partial, growth, net]) => ({ year, per_share: perShare, partial, per_share_growth: growth, net, gross: net, tax: 0, payments: 4 })),
  }
}

describe('income periods', () => {
  it('keys and fills months, quarters, and years', () => {
    expect(periodKey('2026-04-30', 'quarterly')).toBe('2026 Q2')
    expect(periodRange('2025-11-10', '2026-02-02', 'monthly')).toEqual(['2025-11', '2025-12', '2026-01', '2026-02'])
    expect(periodRange('2025-08-01', '2026-02-02', 'quarterly')).toEqual(['2025 Q3', '2025 Q4', '2026 Q1'])
    expect(periodRange('2023-05-01', '2025-01-01', 'yearly')).toEqual(['2023', '2024', '2025'])
  })

  it('stacks the largest payers and groups the rest, with withholding shown as positive', () => {
    const rows = [payment('2025-01-10', 'A', 100), payment('2025-02-10', 'B', 50), payment('2025-04-10', 'C', 20), payment('2025-04-11', 'D', 10)]
    const stacked = stackIncome(rows, 'quarterly', 'net', 'company', 3)
    expect(stacked.periods).toEqual(['2025 Q1', '2025 Q2'])
    expect(stacked.series.map(series => series.label)).toEqual(['A', 'B', '2 others'])
    expect(stacked.series[2].values).toEqual([0, (20 + 10) * 0.85])
    const tax = stackIncome(rows, 'yearly', 'tax', 'currency')
    expect(tax.series[0]).toMatchObject({ label: 'Paid in USD', values: [180 * 0.15] })
    expect(withheld(0)).toBe(0)
    expect(Object.is(withheld(-0), -0)).toBe(false)
  })

  it('summarizes the chosen companies against all income in the period', () => {
    const all = [payment('2024-06-01', 'A', 100), payment('2025-06-01', 'A', 100), payment('2025-06-01', 'B', 300)]
    const chosen = selectPayments(all, ['A'], '2024-12-31')
    expect(chosen).toHaveLength(1)
    const summary = summarizeIncome(chosen, selectPayments(all, [], '2024-12-31'), '2025-12-31', true)
    expect(summary).toMatchObject({ gross: 100, payments: 1, companies: 1 })
    expect(summary.rate).toBeCloseTo(0.15)
    expect(summary.shareOfAll).toBeCloseTo(0.25)
    expect(summary.lastTwelveMonths).toBeCloseTo(85)
    // Before the data arrives there is no valuation date; nothing throws.
    expect(summarizeIncome([], [], '', false).lastTwelveMonths).toBe(0)
  })

  it('weights dividend growth by income and indexes complete years only', () => {
    const big = company('BIG', [[2022, 1, true, null, 10], [2023, 1.1, false, null, 300], [2024, 1.21, false, 0.1, 300]])
    const small = company('SMALL', [[2023, 2, false, null, 100], [2024, 2.4, false, 0.2, 100]])
    const growth = weightedGrowthByYear([big, small])
    expect(growth).toHaveLength(1)
    expect(growth[0].year).toBe(2024)
    expect(growth[0].growth).toBeCloseTo((0.1 * 300 + 0.2 * 100) / 400)
    const indexed = indexedPerShare([big, small])
    expect(indexed.years).toEqual([2023, 2024])
    expect(indexed.series.find(series => series.symbol === 'BIG')?.values).toEqual([100, expect.closeTo(110, 6)])
  })
})

describe('holding periods', () => {
  it('measures the latest continuous period and writes it plainly', () => {
    expect(heldDays({ held_since: '2026-01-01', held_until: '2026-01-31' })).toBe(31)
    expect(durationText(20)).toBe('20 d')
    expect(durationText(200)).toBe('6 mo')
    expect(durationText(2050)).toBe('5 y 7 mo')
    expect(durationText(730)).toBe('2 y')
    expect(durationText(null)).toBe('—')
  })
})
