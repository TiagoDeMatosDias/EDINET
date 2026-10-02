/**
 * Names for EDINET form codes, checked against EDINET's own document
 * descriptions (DocumentList.docDescription), and the report families the
 * Filing Explorer filters by.
 */
const FORM_LABELS: Record<string, string> = {
  '030000': 'Annual securities report',
  '030001': 'Annual securities report (amended)',
  '032000': 'Annual securities report (small offering)',
  '040000': 'Annual securities report (Art. 24-3)',
  '043000': 'Quarterly report',
  '043001': 'Quarterly report (amended)',
  '043A00': 'Semi-annual report',
  '043A01': 'Semi-annual report (amended)',
  '06G000': 'Annual report · investment trust',
  '06I000': 'Annual report · trust beneficiary certificates',
  '07A000': 'Annual report · investment trust',
  '07B000': 'Annual report · investment corporation',
  '080000': 'Annual securities report (foreign company)',
  '082000': 'Foreign company report',
  '09A000': 'Annual report · trust beneficiary certificates',
  '09E000': 'Annual report · investment business rights',
}

export const FORM_CATEGORIES = [
  { key: 'annual', label: 'Annual reports', codes: ['030000', '032000', '040000'] },
  { key: 'interim', label: 'Semi-annual and quarterly', codes: ['043A00', '043000'] },
  { key: 'amended', label: 'Amendments', codes: ['030001', '043001', '043A01'] },
  { key: 'funds', label: 'Funds and trusts', codes: ['06G000', '06I000', '07A000', '07B000', '09A000', '09E000'] },
  { key: 'foreign', label: 'Foreign companies', codes: ['080000', '082000'] },
] as const

export type FormCategoryKey = typeof FORM_CATEGORIES[number]['key']

export function formLabel(code?: string | null) {
  if (!code) return 'XBRL report'
  return FORM_LABELS[code] ?? `Form ${code}`
}

export function formCategory(code?: string | null): FormCategoryKey | 'other' {
  return FORM_CATEGORIES.find(category => (category.codes as readonly string[]).includes(code ?? ''))?.key ?? 'other'
}

export function formatBytes(size?: number | null) {
  if (size == null || !Number.isFinite(size)) return ''
  if (size >= 1024 * 1024) return `${(size / 1024 / 1024).toFixed(1)} MB`
  if (size >= 1024) return `${Math.round(size / 1024)} KB`
  return `${size} B`
}

const monthYear = new Intl.DateTimeFormat('en-GB', { month: 'short', year: 'numeric', timeZone: 'UTC' })
const dayMonthYear = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' })

function utcDay(value?: string | null) {
  return value ? Date.parse(`${value.slice(0, 10)}T00:00:00Z`) : Number.NaN
}

/** "Apr 2025 – Mar 2026 · 12 months" from the filing's period bounds. */
export function periodSpan(start?: string | null, end?: string | null) {
  const from = utcDay(start)
  const to = utcDay(end)
  if (!Number.isFinite(from) || !Number.isFinite(to)) return ''
  const months = Math.round((to - from) / (30.44 * 24 * 3600 * 1000))
  return `${monthYear.format(from)} – ${monthYear.format(to)} · ${months} months`
}

/** "10 Jun 2026" from an EDINET timestamp such as "2026-06-10 15:33". */
export function formatDay(value?: string | null) {
  const time = utcDay(value)
  return Number.isFinite(time) ? dayMonthYear.format(time) : ''
}

/** The fiscal year a report covers, as its period end month: "FY 2026-03". */
export function fiscalLabel(periodEnd?: string | null) {
  return periodEnd ? `FY ${periodEnd.slice(0, 7)}` : 'Period unavailable'
}

export interface FilingCompany {
  edinet_code?: string | null
  company_name?: string | null
  submitter_name?: string | null
}

/** English name when the research database knows the company, otherwise the name as filed. */
export function companyName(filing: FilingCompany) {
  return filing.company_name || filing.submitter_name || filing.edinet_code || 'Unknown filer'
}

export function filingHref(docId: string, companyCode?: string | null, from?: string | null) {
  const params = new URLSearchParams()
  if (from) params.set('from', from)
  if (companyCode) params.set('company', companyCode)
  const query = params.toString()
  return `/filings/${encodeURIComponent(docId)}${query ? `?${query}` : ''}`
}
