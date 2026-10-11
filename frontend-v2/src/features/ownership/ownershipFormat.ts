import { csvCell } from '../analysis/downloads'
import type { EntityDetail, EntityHolding, EntityKind, EntityNames, EntitySummary, MarketCaps, OfficerRow, ShareholderRow } from './ownershipTypes'

export const KIND_LABELS: Record<EntityKind, string> = {
  person: 'Officer',
  individual: 'Individual',
  company: 'Company',
  organization: 'Institution',
}

export const KIND_HINTS: Record<EntityKind, string> = {
  person: 'A director, auditor, or executive officer, identified by name and date of birth across companies.',
  individual: 'An individual shareholder, identified by name only: shareholder lists give no date of birth.',
  company: 'A holder that files with EDINET itself, named as on its own Analysis page.',
  organization: 'A bank, fund, association, or company that does not file annual reports with EDINET.',
}

export const KIND_FILTERS: Array<{ key: EntityKind | ''; label: string }> = [
  { key: '', label: 'Everyone' },
  { key: 'person', label: 'Officers' },
  { key: 'individual', label: 'Individual shareholders' },
  { key: 'company', label: 'Companies' },
  { key: 'organization', label: 'Institutions' },
]

/** The Research page of a person or organisation. */
export function entityHref(entityId: string) {
  return `/research?tab=people&entity=${encodeURIComponent(entityId)}`
}

/** A company's Analysis page at its Major shareholders or Related people section. */
export function ownershipHref(code: string, section: 'shareholders' | 'people' = 'shareholders') {
  return `/analyze/${encodeURIComponent(code)}#${section}`
}

/** Hover text: the name as filed, where the label shown is not it. */
export function filedName(names: Pick<EntityNames, 'name' | 'name_ja'>) {
  return names.name_ja && names.name_ja !== names.name ? `Filed as ${names.name_ja}` : undefined
}

const compact = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 2 })
const whole = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 })

/** Share counts: exact below a million, compact above ("24.1M"); the exact figure belongs in a title. */
export function formatShares(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return '—'
  return Math.abs(value) < 1e6 ? whole.format(value) : compact.format(value)
}

export function exactShares(value: number | null | undefined) {
  return value == null || !Number.isFinite(value) ? undefined : `${whole.format(value)} shares`
}

/** A stake as a percentage: two decimals, three below 0.1% so a director's holding is not "0.00%". */
export function formatStake(ratio: number | null | undefined) {
  if (ratio == null || !Number.isFinite(ratio)) return '—'
  const percent = ratio * 100
  if (percent === 0) return '0%'
  if (Math.abs(percent) < 0.001) return '<0.001%'
  return `${percent.toFixed(Math.abs(percent) < 0.1 ? 3 : 2)}%`
}

/** A change in a stake in percentage points: "+0.42 pp". */
export function formatPoints(change: number | null | undefined) {
  if (change == null || !Number.isFinite(change)) return ''
  const points = change * 100
  if (Math.abs(points) < 0.005) return '0.00 pp'
  return `${points > 0 ? '+' : '−'}${Math.abs(points).toFixed(2)} pp`
}

export function formatYen(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return '—'
  const magnitude = Math.abs(value)
  const [divisor, unit] = magnitude >= 1e12 ? [1e12, 'tn'] : magnitude >= 1e9 ? [1e9, 'bn'] : magnitude >= 1e6 ? [1e6, 'm'] : magnitude >= 1e3 ? [1e3, 'k'] : [1, '']
  const scaled = value / divisor
  const digits = Math.abs(scaled) >= 100 ? 0 : Math.abs(scaled) >= 10 ? 1 : 2
  return `¥${scaled.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: digits })}${unit}`
}

/** What a stake is worth: its share of the company times the company's market capitalisation. */
export function stakeValue(ratio: number | null | undefined, marketCap: number | null | undefined) {
  return ratio != null && marketCap != null && Number.isFinite(ratio) && Number.isFinite(marketCap) ? ratio * marketCap : null
}

const monthYear = new Intl.DateTimeFormat('en-GB', { month: 'short', year: 'numeric', timeZone: 'UTC' })

/** "2026-03" or "2026-03-31" → "Mar 2026"; a bare year stays a year. */
export function formatMonth(value: string | null | undefined) {
  if (!value) return ''
  if (/^\d{4}$/.test(value)) return value
  const time = Date.parse(`${value.slice(0, 7)}-01T00:00:00Z`)
  return Number.isFinite(time) ? monthYear.format(time) : value
}

/** The fiscal year a report covers, as its year label: "2026-03-31" → "FY2026/3". */
export function fiscalYear(periodEnd: string | null | undefined) {
  const match = /^(\d{4})-(\d{2})/.exec(periodEnd ?? '')
  return match ? `FY${match[1]}/${Number(match[2])}` : ''
}

/** When a term of office ends: at the shareholders' meeting that follows the fiscal period. */
export function termText(officer: Pick<OfficerRow, 'term_end' | 'term'>) {
  if (officer.term_end) return `AGM for ${fiscalYear(officer.term_end)}`
  return officer.term ?? ''
}

/** How an entity is described under its name in lists. */
export function entityMeta(entity: Pick<EntitySummary, 'kind' | 'headline' | 'holdings' | 'positions' | 'past_holdings' | 'past_positions' | 'ticker' | 'industry'>) {
  const parts: string[] = []
  if (entity.positions) parts.push(`${entity.positions} ${entity.positions === 1 ? 'board' : 'boards'}`)
  if (entity.holdings) parts.push(`${entity.holdings} ${entity.holdings === 1 ? 'holding' : 'holdings'}`)
  if (!entity.positions && !entity.holdings) parts.push(entity.kind === 'person' ? 'former officer' : 'former holder')
  return parts.join(' · ')
}

export function shareholdersCsv(rows: ShareholderRow[], marketCap: number | null | undefined) {
  const header = ['Rank', 'Shareholder', 'Filed name', 'Kind', 'EDINET code', 'Account', 'Address', 'Shares', 'Stake', 'Change (points)', 'Value (JPY)', 'Listed since']
  const lines = rows.map(row => [
    row.rank, row.name, row.filed_name, KIND_LABELS[row.kind], row.edinet_code ?? '', row.account ?? '', row.address ?? '', row.shares ?? '', row.ratio ?? '',
    row.change != null ? row.change * 100 : '', stakeValue(row.ratio, marketCap) ?? '', row.since ?? '',
  ])
  return [header, ...lines].map(line => line.map(cell => csvCell(String(cell))).join(',')).join('\n')
}

export function officersCsv(rows: OfficerRow[], marketCap: number | null | undefined) {
  const header = ['Name', 'Filed name', 'Title', 'Filed title', 'Outside', 'Born', 'Age', 'Shares', 'Stake', 'Value (JPY)', 'Term ends', 'Other boards']
  const lines = rows.map(row => [
    row.name, row.name_ja ?? row.name, row.title, row.title_ja ?? row.title, row.outside ? 'yes' : '', row.birth_date ?? '', row.age ?? '', row.shares ?? '', row.ratio ?? '',
    stakeValue(row.ratio, marketCap) ?? '', row.term_end ?? '', row.other_positions,
  ])
  return [header, ...lines].map(line => line.map(cell => csvCell(String(cell))).join(',')).join('\n')
}

export function holdingsCsv(rows: EntityHolding[], caps: MarketCaps = {}) {
  const header = ['Company', 'Ticker', 'EDINET code', 'Industry', 'Current', 'Rank', 'Shares', 'Stake', 'Change (points)', 'Value (JPY)', 'As of', 'Listed since', 'Account']
  const lines = rows.map(row => [
    row.company_name, row.ticker, row.edinet_code, row.industry, row.current ? 'yes' : '', row.rank, row.shares ?? '', row.ratio ?? '',
    row.change != null ? row.change * 100 : '', (row.current ? stakeValue(row.ratio, caps[row.edinet_code]) : null) ?? '', row.as_of, row.first_period, row.account ?? '',
  ])
  return [header, ...lines].map(line => line.map(cell => csvCell(String(cell))).join(',')).join('\n')
}

/** How many of an entity's largest stakes are valued; each takes the server a moment the first time. */
export const VALUED_STAKES = 150

/** The listed companies whose market value an entity's page needs: where it serves, and its largest current stakes. */
export function valuedCodes(data: EntityDetail) {
  const codes = [
    ...data.positions.filter(item => item.current && item.listed).map(item => item.edinet_code),
    ...data.holdings.filter(item => item.current && item.listed).slice(0, VALUED_STAKES).map(item => item.edinet_code),
  ]
  return [...new Set(codes)]
}

/** The company an entity is most closely tied to: itself, else where it serves, else its largest stake. */
export function mainCompany(data: EntityDetail) {
  if (data.entity.kind === 'company' && data.entity.edinet_code && data.entity.has_reports) return data.entity.edinet_code
  return data.positions.find(item => item.current)?.edinet_code ?? data.holdings.find(item => item.current)?.edinet_code ?? data.positions[0]?.edinet_code ?? data.holdings[0]?.edinet_code ?? ''
}
