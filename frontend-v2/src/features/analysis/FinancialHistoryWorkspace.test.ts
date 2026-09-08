import { describe, expect, it } from 'vitest'

import type { HistoryMetric } from '../../api/types'
import { chooseFinancialUnit, completeMetricFields, defaultMetricFields, selectedMetricsCsv } from './FinancialHistoryWorkspace'

describe('financial history helpers', () => {
  it('uses one unit for the whole table', () => {
    const unit = chooseFinancialUnit([{ field: 'sales', display_name: 'Sales', values: [83_000_000, 51_000_000_000] }])
    expect(unit).toEqual({ scale: 1_000_000_000, label: 'Billion' })
  })

  it('ignores globally empty periods when selecting complete metrics', () => {
    const metrics = [
      { field: 'complete', display_name: 'Complete', values: [1, 2, null] },
      { field: 'partial', display_name: 'Partial', values: [1, null, null] },
    ]
    expect(completeMetricFields(metrics, ['2024', '2025', '2026'])).toEqual(['complete'])
  })

  it('uses the requested Income Statement defaults in the requested order', () => {
    const metrics = [
      { field: 'other', display_name: 'Other', values: [1] },
      { field: 'profit', display_name: 'Profit (loss)', values: [1] },
      { field: 'sales', display_name: 'Net Sales', values: [1] },
      { field: 'operating', display_name: 'Operating Income - Operating Profit (loss)', values: [1] },
    ]
    const table = { display_name: 'Income Statement', metrics }
    expect(defaultMetricFields('IncomeStatement', table, metrics)).toEqual(['sales', 'operating', 'profit'])
  })

  it('keeps the four-metric fallback for other tables', () => {
    const metrics = Array.from({ length: 5 }, (_, index) => ({ field: 'field-' + index, display_name: 'Metric ' + index, values: [index] }))
    expect(defaultMetricFields('BalanceSheet', { display_name: 'Balance Sheet', metrics }, metrics)).toEqual(['field-0', 'field-1', 'field-2', 'field-3'])
  })
})

describe('selectedMetricsCsv', () => {
  const metrics: HistoryMetric[] = [
    { field: 'revenue', display_name: 'Sales, "Retail"', values: [1_200, null, 'note, with comma'] },
    { field: 'profit', display_name: 'Profit', values: [null, -5, 3] },
    { field: 'excluded', display_name: 'Excluded', values: [9] },
  ]

  it('emits selected rows only, with periods as columns and empty cells for missing values', () => {
    const csv = selectedMetricsCsv(metrics, ['2024-03', '2025-03', '2026-03'], ['revenue', 'profit'])
    const lines = csv.split('\r\n')
    expect(lines[0]).toBe('field,display_name,2024-03,2025-03,2026-03')
    expect(lines[1]).toBe('revenue,"Sales, ""Retail""",1200,,"note, with comma"')
    expect(lines[2]).toBe('profit,Profit,,-5,3')
    expect(lines).toHaveLength(3)
    expect(csv).not.toContain('excluded')
  })

  it('quotes only cells that need it', () => {
    const csv = selectedMetricsCsv([{ field: 'plain', display_name: 'Plain', values: [1] }], ['2025'], ['plain'])
    expect(csv).toBe('field,display_name,2025\r\nplain,Plain,1')
  })
})
