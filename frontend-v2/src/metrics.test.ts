import { describe, expect, it } from 'vitest'

import { formatMetricValue, groupMetrics, metricDefinition, type MetricDefinition } from './metrics'

const DEFINITIONS: Record<string, MetricDefinition> = {
  LatestPrice: { label: 'Price', group: 'Market', format: 'money', currency: 'price' },
  PERatio: { label: 'P/E', group: 'Valuation', direction: 'lower' },
  Revenue: { label: 'Revenue', group: 'Income', format: 'money', currency: 'reporting' },
  ReturnOnEquity: { label: 'ROE', group: 'Quality', format: 'percent' },
}

describe('metric formatting', () => {
  it('uses each company currency instead of assuming yen', () => {
    const currencies = { price: 'JPY', reporting: 'USD' }
    expect(formatMetricValue(DEFINITIONS.LatestPrice, 1520, currencies)).toBe('¥1,520')
    expect(formatMetricValue(DEFINITIONS.Revenue, 10_000_000_000, currencies)).toBe('$10 Billion')
    expect(formatMetricValue(DEFINITIONS.Revenue, -2_500_000, currencies)).toBe('-$2.5 Million')
    expect(formatMetricValue(DEFINITIONS.Revenue, 2_500_000, { reporting: 'CHF' })).toBe('CHF 2.5 Million')
  })

  it('leaves money unmarked when the currency is unknown, and formats percents and plain numbers', () => {
    expect(formatMetricValue(DEFINITIONS.Revenue, 2_500_000)).toBe('2.5 Million')
    expect(formatMetricValue(DEFINITIONS.ReturnOnEquity, 0.125)).toBe('12.5%')
    expect(formatMetricValue(DEFINITIONS.PERatio, 14.678)).toBe('14.68')
    expect(formatMetricValue(DEFINITIONS.PERatio, null)).toBe('—')
  })

  it('derives definitions for table columns and groups metrics in order', () => {
    expect(metricDefinition('IncomeStatement.Gross profit', DEFINITIONS)).toEqual({ label: 'Gross profit', group: 'IncomeStatement' })
    expect(groupMetrics(['LatestPrice', 'Revenue', 'PERatio', 'IncomeStatement.Gross profit'], DEFINITIONS)).toEqual([
      { group: 'Market', metrics: ['LatestPrice'] },
      { group: 'Income', metrics: ['Revenue'] },
      { group: 'Valuation', metrics: ['PERatio'] },
      { group: 'IncomeStatement', metrics: ['IncomeStatement.Gross profit'] },
    ])
  })
})
