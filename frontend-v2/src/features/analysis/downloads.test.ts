import { describe, expect, it } from 'vitest'

import type { HistoryMetric } from '../../api/types'
import { selectedMetricsCsv } from './downloads'

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
