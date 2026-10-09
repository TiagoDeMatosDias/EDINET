import { describe, expect, it } from 'vitest'

import type { SecurityHistory } from '../../api/types'
import { buildCompanyReport, type CompanyReportInput, type SnapshotGroup } from './markdownReport'

const SNAPSHOT_GROUPS: SnapshotGroup[] = [{ title: 'Valuation', metrics: [['PERatio', 'P/E'], ['MarketCap', 'Market cap']] }]

function report(overrides: Partial<CompanyReportInput> = {}): string {
  return buildCompanyReport({
    name: 'ACME Corp',
    ticker: 'TKR',
    companyCode: 'E12345',
    industry: 'Software',
    snapshotGroups: SNAPSHOT_GROUPS,
    metrics: { PERatio: 8.5, MarketCap: 1_000_000_000 },
    formatSnapshotMetric: (key, value) => (key === 'MarketCap' && value ? `¥${value.toLocaleString()}` : String(value ?? '—')),
    history: {
      periods: ['2024-03', '2025-03'],
      tables: {
        IncomeStatement: {
          display_name: 'Income Statement',
          metrics: [
            { field: 'sales', display_name: 'Net Sales | Core', values: [100, 120] },
            { field: 'costs', display_name: 'Costs', values: [null, 'n/a'] },
            { field: 'dead', display_name: 'Never reported', values: [null, null] },
          ],
        },
      },
    },
    ...overrides,
  })
}

describe('buildCompanyReport', () => {
  it('leads with the company identity and snapshot', () => {
    const text = report()
    expect(text).toContain('# ACME Corp')
    expect(text).toContain('TKR · E12345 · Software')
    expect(text).toContain('## Company snapshot')
    expect(text).toContain('### Valuation')
    expect(text).toContain('| P/E | 8.5 |')
    expect(text).toContain('| Market cap | ¥1,000,000,000 |')
  })

  it('escapes pipes in table cells and omits globally empty metrics', () => {
    const text = report()
    expect(text).toContain('| Net Sales \\| Core |')
    expect(text).not.toContain('Never reported')
  })

  it('renders the financial history with period columns and blanks', () => {
    const text = report()
    expect(text).toContain('## Financial history')
    expect(text).toContain('### Income Statement')
    expect(text).toContain('| Metric | 2024-03 | 2025-03 |')
    expect(text).toContain('| Net Sales \\| Core | 100 | 120 |')
    expect(text).toContain('| Costs |  | n/a |')
  })

  it('says which splits the per-share figures are adjusted for', () => {
    const history: SecurityHistory = {
      periods: ['2021-03', '2022-03'],
      tables: { ShareMetrics: { display_name: 'Share Metrics', metrics: [{ field: 'eps', display_name: 'EPS', values: [116.56, 121.98], reported_values: [582.8, 121.98] }] } },
      share_basis: { splits: [{ date: '2022-03-31', after: '2021-03-31', multiplier: 5, source: 'share counts' }] },
    }
    const text = report({ history })
    expect(text).toContain("adjusted to today's shares for the 5-for-1 split in the year to 2022-03")
    expect(text).toContain('| EPS | 116.56 | 121.98 |')
  })

  it('omits the financial history when no table has values', () => {
    const emptyHistory: SecurityHistory = {
      periods: ['2025-03'],
      tables: { IncomeStatement: { display_name: 'Income Statement', metrics: [{ field: 'sales', display_name: 'Sales', values: [null] }] } },
    }
    const text = report({ history: emptyHistory })
    expect(text).not.toContain('## Financial history')
    expect(text).toContain('# ACME Corp')
  })

  it('includes the description when provided', () => {
    const text = report({ description: 'Makes things.' })
    expect(text).toContain('### Business description')
    expect(text).toContain('Makes things.')
  })
})
