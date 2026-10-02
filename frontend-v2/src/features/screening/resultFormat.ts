function compact(value: number) {
  const magnitude = Math.abs(value)
  const units: Array<[number, string]> = [[1e12, 'T'], [1e9, 'B'], [1e6, 'M']]
  const unit = units.find(([scale]) => magnitude >= scale)
  if (!unit) return value.toLocaleString('en-US', { maximumFractionDigits: magnitude >= 100 ? 0 : 2 })
  return `${(value / unit[0]).toLocaleString('en-US', { maximumFractionDigits: 2 })}${unit[1]}`
}

/** A result cell as text: percents from fractions, large amounts abbreviated, missing as a dash. */
export function cellText(value: unknown, format?: string) {
  if (value == null || value === '') return '—'
  if (typeof value === 'boolean') return value ? 'Yes' : 'No'
  if (typeof value !== 'number') return String(value)
  if (!Number.isFinite(value)) return '—'
  if (format === 'percent') return `${(value * 100).toFixed(1)}%`
  return compact(value)
}
