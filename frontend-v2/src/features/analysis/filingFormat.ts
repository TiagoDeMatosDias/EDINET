// EDINET form codes for the reports Shade Research retains; anything else shows its code.
const FORM_LABELS: Record<string, string> = {
  '030000': 'Annual securities report',
  '030001': 'Annual securities report (amended)',
}

export function formLabel(code?: string | null) {
  if (!code) return 'XBRL report'
  return FORM_LABELS[code] ?? `Form ${code}`
}

export function formatBytes(size?: number | null) {
  if (size == null || !Number.isFinite(size)) return ''
  if (size >= 1024 * 1024) return `${(size / 1024 / 1024).toFixed(1)} MB`
  if (size >= 1024) return `${Math.round(size / 1024)} KB`
  return `${size} B`
}

const monthYear = new Intl.DateTimeFormat('en-GB', { month: 'short', year: 'numeric', timeZone: 'UTC' })

/** "Apr 2025 – Mar 2026 · 12 months" from the filing's period bounds. */
export function periodSpan(start?: string | null, end?: string | null) {
  const from = start ? Date.parse(`${start.slice(0, 10)}T00:00:00Z`) : Number.NaN
  const to = end ? Date.parse(`${end.slice(0, 10)}T00:00:00Z`) : Number.NaN
  if (!Number.isFinite(from) || !Number.isFinite(to)) return ''
  const months = Math.round((to - from) / (30.44 * 24 * 3600 * 1000))
  return `${monthYear.format(from)} – ${monthYear.format(to)} · ${months} months`
}
