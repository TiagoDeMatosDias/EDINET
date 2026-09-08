/** Render a stored number at full precision (shortest round-trip digits) with thousands separators. */
export function formatGranularNumber(value: number): string {
  const text = String(value)
  // Exponential forms (very large/small magnitudes) have no useful grouping; show them verbatim.
  if (/[eE]/.test(text)) return text
  const [whole, fraction] = text.split('.')
  const grouped = whole.replace(/\B(?=(\d{3})+(?!\d))/g, ',')
  return fraction === undefined ? grouped : `${grouped}.${fraction}`
}
