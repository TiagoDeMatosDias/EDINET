import type { HistoryMetric, SecurityHistory } from '../../api/types'
import { formatGranularNumber } from './numberFormat'
import { describeSplit } from './shareBasis'

export interface SnapshotGroup {
  title: string
  metrics: Array<[string, string]>
}

export interface CompanyReportInput {
  name: string
  ticker?: string
  companyCode?: string
  industry?: string
  market?: string
  description?: string
  snapshotPeriod?: string
  snapshotGroups: SnapshotGroup[]
  metrics: Record<string, number | null>
  formatSnapshotMetric: (key: string, value: number | null | undefined) => string
  history?: SecurityHistory
}

function markdownCell(value: string): string {
  return value.replace(/\|/g, '\\|').replace(/\r?\n/g, ' ')
}

function markdownTable(headers: string[], rows: Array<Array<string | null>>): string {
  const lines = [`| ${headers.map(markdownCell).join(' | ')} |`, `| ${headers.map(() => '---').join(' | ')} |`]
  for (const row of rows) lines.push(`| ${row.map(value => markdownCell(value ?? '')).join(' | ')} |`)
  return lines.join('\n')
}

function formatValue(value: number | string | null | undefined): string {
  if (value == null || value === '') return ''
  if (typeof value === 'number' && Number.isFinite(value)) return formatGranularNumber(value)
  return String(value)
}

function hasAnyValue(metric: HistoryMetric): boolean {
  return metric.values.some(value => value != null && value !== '')
}

export function buildCompanyReport(input: CompanyReportInput): string {
  const parts: string[] = []
  parts.push(`# ${input.name}`)
  const subtitle = [input.ticker, input.companyCode, input.industry, input.market].filter(Boolean).join(' · ')
  if (subtitle) parts.push('', subtitle)

  parts.push('', '## Company snapshot')
  if (input.snapshotPeriod) parts.push('', `Financial metrics: ${input.snapshotPeriod}`)
  for (const group of input.snapshotGroups) {
    const rows = group.metrics.map(([key, label]) => [label, input.formatSnapshotMetric(key, input.metrics[key] ?? null)] as Array<string | null>)
    parts.push('', `### ${group.title}`, '', markdownTable(['Metric', 'Value'], rows))
  }
  if (input.description) parts.push('', '### Business description', '', input.description)

  if (input.history && input.history.periods.length) {
    const tables = Object.entries(input.history.tables)
      .map(([source, table]) => ({
        source,
        table,
        rows: table.metrics.filter(hasAnyValue).map(metric => [metric.display_name, ...metric.values.map(value => formatValue(value))] as Array<string | null>),
      }))
      .filter(entry => entry.rows.length > 0)
    if (tables.length) {
      parts.push('', '## Financial history', '', 'Statement values are unrounded and in the filing currency.')
      const splits = input.history.share_basis?.splits ?? []
      if (splits.length) parts.push('', `Per-share figures and share counts are adjusted to today's shares for the ${splits.map(describeSplit).join('; ')}.`)
      for (const { table, rows } of tables) {
        parts.push('', `### ${table.display_name}`, '', markdownTable(['Metric', ...input.history.periods], rows))
      }
    }
  }

  return `${parts.join('\n').trim()}\n`
}
