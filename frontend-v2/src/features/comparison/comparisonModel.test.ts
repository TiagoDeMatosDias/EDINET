import { describe, expect, it } from 'vitest'

import {
  alignTrends,
  comparisonCsv,
  currencyNote,
  describeColumnMetric,
  describeTag,
  emptyMetrics,
  fiscalYearNote,
  heatColor,
  indexToHundred,
  median,
  orderMetrics,
  parseCodes,
  rankValues,
  shortName,
  sizeRatio,
  sortCompanies,
} from './comparisonModel'
import type { ComparisonCompany, TagSet } from './comparisonTypes'

function company(code: string, metrics: Record<string, number | null>, extra: Partial<ComparisonCompany> = {}): ComparisonCompany {
  return { company_code: code, company: { company_name: `${code} Co., Ltd.`, ticker: code.slice(1) }, metrics, percentiles: {}, ...extra }
}

describe('comparison ranks', () => {
  it('ranks highest first where higher is better, sharing ranks on ties', () => {
    expect(rankValues('higher', [0.1, 0.3, 0.1, null]).map(rank => rank?.rank ?? null)).toEqual([2, 1, 2, null])
    expect(rankValues('higher', [0.1, 0.3])[1]).toEqual({ rank: 1, of: 2, score: 1 })
  })

  it('ranks negative multiples after every positive one where lower is better', () => {
    const ranks = rankValues('lower', [10.97, -1.84, -34.15])
    expect(ranks.map(rank => rank?.rank)).toEqual([1, 2, 2])
    expect(ranks.map(rank => rank?.score)).toEqual([1, 0, 0])
  })

  it('does not rank metrics without a direction or with fewer than two values', () => {
    expect(rankValues(undefined, [1, 2])).toEqual([null, null])
    expect(rankValues('higher', [1, null])).toEqual([null, null])
  })

  it('tints best indigo, worst vermilion, and the middle not at all', () => {
    expect(heatColor(1)).toMatch(/^rgb\(52 96 168/)
    expect(heatColor(0)).toMatch(/^rgb\(196 70 44/)
    expect(heatColor(0.5)).toBeUndefined()
    expect(heatColor(null)).toBeUndefined()
  })

  it('sorts companies best first and keeps gaps last', () => {
    const companies = [company('E1', { PERatio: 20 }), company('E2', { PERatio: null }), company('E3', { PERatio: 8 })]
    expect(sortCompanies(companies, 'PERatio', 'lower').map(item => item.company_code)).toEqual(['E3', 'E1', 'E2'])
    // Without a direction, largest first.
    expect(sortCompanies(companies, 'PERatio').map(item => item.company_code)).toEqual(['E1', 'E3', 'E2'])
  })
})

describe('comparison helpers', () => {
  it('reads company codes from a link', () => {
    expect(parseCodes(' E1,E2,,E1 ,E3')).toEqual(['E1', 'E2', 'E3'])
    expect(parseCodes(Array.from({ length: 14 }, (_, index) => `E${index}`).join(','))).toHaveLength(12)
  })

  it('keeps standard metrics in their order and added columns after them', () => {
    expect(orderMetrics(['IncomeStatement.Net sales', 'ReturnOnEquity', 'PERatio'], ['PERatio', 'PriceToBook', 'ReturnOnEquity'])).toEqual(['PERatio', 'ReturnOnEquity', 'IncomeStatement.Net sales'])
  })

  it('names added columns readably', () => {
    expect(describeColumnMetric('IncomeStatement_Rolling.Net sales_Growth_3_Year')).toEqual({ label: 'Net sales · 3-yr growth', group: 'Income statement · rolling' })
  })

  it('computes medians', () => {
    expect(median([3, null, 1, 2])).toBe(2)
    expect(median([4, 1, 2, 3])).toBe(2.5)
    expect(median([null])).toBeNull()
  })

  it('finds metrics no company reports', () => {
    expect([...emptyMetrics(['A', 'B'], [company('E1', { A: 1, B: null }), company('E2', { A: null, B: null })])]).toEqual(['B'])
  })

  it('warns when fiscal years end in different months or currencies differ', () => {
    const march = company('E1', {}, { period_end: '2026-03-31', reporting_currency: 'JPY' })
    const december = company('E2', {}, { period_end: '2025-12-31', reporting_currency: 'USD' })
    expect(fiscalYearNote([march, march])).toBeNull()
    expect(fiscalYearNote([march, march, december])).toBe('Latest fiscal years end in Mar 2026 (2) and Dec 2025 (1).')
    expect(currencyNote([march, december])).toBe("Amounts are in each company's reporting currency: JPY (1) and USD (1).")
  })

  it('shortens legal names for charts', () => {
    expect(shortName('TOYOTA MOTOR CORPORATION')).toBe('TOYOTA MOTOR')
    expect(shortName('NISSAN MOTOR CO., LTD.')).toBe('NISSAN MOTOR')
    expect(shortName('Sony Group Corporation')).toBe('Sony Group')
  })

  it('aligns yearly series on fiscal years and indexes them to 100', () => {
    const aligned = alignTrends([
      { company_code: 'E1', periods: ['2024-03-31', '2025-03-31'], series: { Revenue: [100, 120] } },
      { company_code: 'E2', periods: ['2024-12-31', '2025-12-31', '2026-12-31'], series: { Revenue: [50, null, 60] } },
    ], 'Revenue', ['E1', 'E2'])
    expect(aligned.years).toEqual([2024, 2025, 2026])
    expect(aligned.rows).toEqual([{ code: 'E1', values: [100, 120, null] }, { code: 'E2', values: [50, null, 60] }])
    expect(indexToHundred([null, -5, 50, 75])).toEqual([null, -10, 100, 150])
  })

  it('exports unformatted values with a median column', () => {
    const csv = comparisonCsv([company('E1', { PERatio: 10 }), company('E2', { PERatio: 20 })], ['PERatio'], { PERatio: { label: 'P/E', group: 'Valuation' } })
    expect(csv.split('\n')[0]).toBe('Metric,Group,"E1 Co., Ltd.","E2 Co., Ltd.",Median')
    expect(csv).toContain('P/E,Valuation,10,20,15')
  })

  it('describes peer size relative to the nearest selected company', () => {
    expect(sizeRatio(1.333)).toBe('1.3×')
    expect(sizeRatio(0.4219)).toBe('0.42×')
    expect(sizeRatio(null)).toBe('')
  })

  it('says what a tag adds to the comparison', () => {
    const tag = (size: number, members = size): TagSet => ({ name: 'Tag', member_count: members, companies: Array.from({ length: size }, (_, index) => ({ company_code: `E${index + 1}`, company_name: `Company ${index + 1} Co., Ltd.` })) })
    const codes = (result: ReturnType<typeof describeTag>) => result.adds.map(company => company.company_code)

    expect(describeTag(tag(3), [])).toMatchObject({ summary: '3 companies', names: 'Company 1, Company 2, Company 3', disabled: false })
    expect(describeTag(tag(1), []).summary).toBe('1 company')
    // Companies already compared are not added again.
    const partly = describeTag(tag(3), ['E2', 'X1'])
    expect(partly.summary).toBe('3 companies · adds 2')
    expect(codes(partly)).toEqual(['E1', 'E3'])
    // Twelve is the most a comparison holds: the first that fit are added.
    const large = describeTag(tag(15, 24), ['X1', 'X2'])
    expect(large.summary).toBe('15 of 24 have EDINET filings · adds the first 10')
    expect(codes(large)).toEqual(['E1', 'E2', 'E3', 'E4', 'E5', 'E6', 'E7', 'E8', 'E9', 'E10'])
  })

  it('does not offer a tag that would add nothing', () => {
    const one = { company_code: 'E1', company_name: 'One' }
    expect(describeTag({ name: 'Empty', member_count: 0, companies: [] }, [])).toMatchObject({ summary: 'No companies tagged yet', disabled: true })
    expect(describeTag({ name: 'Funds', member_count: 2, companies: [] }, [])).toMatchObject({ summary: 'No company with EDINET filings to compare', disabled: true })
    expect(describeTag({ name: 'One', member_count: 1, companies: [one] }, ['E1'])).toMatchObject({ summary: 'Already in the comparison', disabled: true })
    expect(describeTag({ name: 'Two', member_count: 2, companies: [one, { company_code: 'E2' }] }, ['E2', 'E1'])).toMatchObject({ summary: 'All 2 are in the comparison', disabled: true })
  })
})
