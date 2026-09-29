/** Render a stored number at full precision (shortest round-trip digits) with thousands separators. */
export function formatGranularNumber(value: number): string {
  const text = String(value)
  // Exponential forms (very large/small magnitudes) have no useful grouping; show them verbatim.
  if (/[eE]/.test(text)) return text
  const [whole, fraction] = text.split('.')
  const grouped = whole.replace(/\B(?=(\d{3})+(?!\d))/g, ',')
  return fraction === undefined ? grouped : `${grouped}.${fraction}`
}

/** Abbreviate large magnitudes (e.g. "1.23 Billion"); smaller values keep two decimals. */
export function formatCompactNumber(value: number): string {
  if (Math.abs(value) < 1_000) return value.toLocaleString(undefined, { maximumFractionDigits: 2 })
  const units: Array<[number, string]> = [[1e12, 'Trillion'], [1e9, 'Billion'], [1e6, 'Million'], [1e3, 'Thousand']]
  const [scale, label] = units.find(([threshold]) => Math.abs(value) >= threshold) ?? [1, '']
  return `${(value / scale).toLocaleString(undefined, { maximumFractionDigits: 2 })} ${label}`
}
