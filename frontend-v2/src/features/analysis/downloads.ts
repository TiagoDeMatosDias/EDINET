import { downloadBlob } from '../../api/download'
import type { HistoryMetric } from '../../api/types'

export function safeFileName(value: string): string {
  const cleaned = value.replace(/[\\/:*?"<>|\s]+/g, '-').replace(/-+/g, '-').replace(/^[-.]+|[-.]+$/g, '')
  return cleaned || 'download'
}

export function downloadTextFile(filename: string, content: string, mime = 'text/plain;charset=utf-8'): void {
  downloadBlob(filename, new Blob([content], { type: mime }))
}

export function csvCell(value: string): string {
  return /[",\r\n]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value
}

export function selectedMetricsCsv(metrics: HistoryMetric[], periods: string[], selected: string[]): string {
  const chosen = metrics.filter(metric => selected.includes(metric.field))
  const lines = [['field', 'display_name', ...periods].map(csvCell).join(',')]
  for (const metric of chosen) {
    const cells = [metric.field, metric.display_name, ...metric.values.map(value => value == null ? '' : String(value))]
    lines.push(cells.map(csvCell).join(','))
  }
  return lines.join('\r\n')
}
