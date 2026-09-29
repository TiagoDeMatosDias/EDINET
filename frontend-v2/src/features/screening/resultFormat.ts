import { formatCompactNumber } from '../analysis/numberFormat'

/** Display format a column declares (see the screening API's ``column_formats``). */
export type ColumnFormat = 'percent'

/** Display text for one screening result cell. */
export function formatResultValue(value: unknown, format?: ColumnFormat | string): string {
  if (value == null || value === '') return '—'
  if (typeof value === 'boolean') return value ? 'Yes' : 'No'
  if (typeof value !== 'number') return String(value)
  if (!Number.isFinite(value)) return '—'
  // Ratios are stored as fractions (0.035 = 3.5%).
  if (format === 'percent') return `${(value * 100).toFixed(1)}%`
  if (Math.abs(value) >= 1_000_000) return formatCompactNumber(value)
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 })
}
