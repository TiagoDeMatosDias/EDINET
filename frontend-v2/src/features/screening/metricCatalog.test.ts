import { describe, expect, it } from 'vitest'

import { buildMetricOptions, columnLabel, parsePercentText, presetForTokens, searchMetrics, tableInfo, toPercentText } from './metricCatalog'

const CATALOG = {
  Stock_Prices: ['Price'],
  ShareMetrics: ['Basic earnings (loss) per share', 'Net assets per share'],
  Financial_Ratios: ['Return on Equity', 'Return on Assets'],
  Financial_Ratios_Rolling: ['Return on Equity_Average_3_Year'],
  Taxonomy_Dictionary: ['label_en'],
}

describe('metric catalog', () => {
  it('names tables and columns for people', () => {
    expect(columnLabel('Financial_Ratios_Rolling', 'Return on Equity_Average_3_Year')).toBe('Return on Equity · 3-yr avg')
    expect(columnLabel('CompanyInfo', 'Company_Name')).toBe('Company name')
    expect(tableInfo('IncomeStatement_Rolling').label).toBe('Income statement · rolling')
  })

  it('hides internal tables and offers only formulas this database can compute', () => {
    const options = buildMetricOptions(CATALOG)
    expect(options.some(option => option.table === 'Taxonomy_Dictionary')).toBe(false)
    expect(options.filter(option => option.preset).map(option => option.label)).toEqual(['P/E ratio', 'P/B ratio', 'Earnings yield'])
  })

  it('ranks label matches first and needs every word to match', () => {
    const options = buildMetricOptions(CATALOG)
    expect(searchMetrics(options, 'return equity').map(option => option.label)).toEqual(['Return on Equity', 'Return on Equity · 3-yr avg'])
    expect(searchMetrics(options, 'p/e')[0].label).toBe('P/E ratio')
    expect(searchMetrics(options, '')[0].label).toBe('P/E ratio')
    expect(presetForTokens(options.find(option => option.label === 'P/B ratio')?.preset?.tokens)?.id).toBe('pb')
  })

  it('recognises a saved formula whose tokens come back with sorted keys', () => {
    const saved = JSON.parse('[{"column":"Price","table":"Stock_Prices","type":"column"},{"op":"/","type":"op"},{"column":"Basic earnings (loss) per share","table":"ShareMetrics","type":"column"}]')
    expect(presetForTokens(saved)?.id).toBe('pe')
  })

  it('converts between stored fractions and typed percentages', () => {
    expect(toPercentText(0.153)).toBe('15.3')
    expect(toPercentText(0.07)).toBe('7')
    expect(parsePercentText('15.3')).toBe(0.153)
    expect(parsePercentText('-')).toBeNull()
  })
})
