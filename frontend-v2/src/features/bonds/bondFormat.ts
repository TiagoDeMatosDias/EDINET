import { isPositionTag } from '../research/researchModel'
import type { Bond, CouponKind, MarketBond, Seniority } from './bondTypes'

export const SENIORITY_LABELS: Record<Seniority, string> = {
  senior: 'Senior unsecured',
  secured: 'Secured',
  subordinated: 'Subordinated',
  hybrid: 'Hybrid',
  convertible: 'Convertible',
}

/** What the filings say about a bond, in plain words. */
export const FEATURE_LABELS: Record<string, string> = {
  callable: 'Callable',
  subordinated: 'Subordinated',
  'non-viability': 'Bail-in (written off if the bank fails)',
  deferrable: 'Interest can be deferred',
  convertible: 'Convertible into shares',
  'general-mortgage': 'General mortgage (statutory security)',
  secured: 'Secured',
  guaranteed: 'Guaranteed',
  private: 'Private placement',
  green: 'Green bond',
  social: 'Social bond',
  sustainability: 'Sustainability bond',
  transition: 'Transition bond',
  blue: 'Blue bond',
  retail: 'For retail investors',
}

const LABELLED_FEATURES = ['green', 'social', 'sustainability', 'transition', 'blue', 'callable', 'non-viability', 'deferrable', 'private', 'retail']

export const COUPON_KIND_LABELS: Record<CouponKind, string> = {
  fixed: 'Fixed',
  step: 'Fixed, then reset',
  'fixed-to-floating': 'Fixed, then floating',
  floating: 'Floating',
  zero: 'Zero coupon',
  unknown: 'Not stated',
}

/** Rating groups for filters and chart colours, best first. */
export const RATING_GROUPS = ['AAA', 'AA', 'A', 'BBB', 'Below BBB', 'Not rated'] as const
export type RatingGroup = typeof RATING_GROUPS[number]

export function ratingGroup(notch: number | null | undefined): RatingGroup {
  if (notch == null) return 'Not rated'
  if (notch <= 1) return 'AAA'
  if (notch <= 4) return 'AA'
  if (notch <= 7) return 'A'
  if (notch <= 10) return 'BBB'
  return 'Below BBB'
}

/** Basis points with a sign when asked: 0.0034 → "34 bp". */
export function formatBp(value: number | null | undefined, signed = false) {
  if (value == null || !Number.isFinite(value)) return '—'
  const bp = Math.round(value * 10_000)
  return `${signed && bp > 0 ? '+' : ''}${bp} bp`
}

/** Yen amounts in short form: 30e9 → "¥30bn", 1.25e12 → "¥1.25tn", 500e6 → "¥500m". */
export function formatYen(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return '—'
  const magnitude = Math.abs(value)
  const [divisor, unit] = magnitude >= 1e12 ? [1e12, 'tn'] : magnitude >= 1e9 ? [1e9, 'bn'] : magnitude >= 1e6 ? [1e6, 'm'] : [1, '']
  const scaled = value / divisor
  const digits = Math.abs(scaled) >= 100 || Number.isInteger(scaled) ? 0 : Math.abs(scaled) >= 10 ? 1 : 2
  return `¥${scaled.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: digits })}${unit}`
}

export function formatYears(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return '—'
  return `${value.toFixed(value < 10 ? 1 : 0)}y`
}

export function formatCoupon(bond: { coupon?: number | null; coupon_kind?: CouponKind | null }) {
  if (bond.coupon == null) return bond.coupon_kind === 'floating' ? 'Floating' : '—'
  return `${(bond.coupon * 100).toFixed(3).replace(/0$/, '')}%`
}

export function ratingText(bond: { rating?: string | null; rating_agency?: string | null; rating_inferred?: number | null }) {
  if (!bond.rating) return 'Not rated'
  return `${bond.rating}${bond.rating_agency ? ` (${bond.rating_agency})` : ''}${bond.rating_inferred ? ' *' : ''}`
}

/** The few features worth a tag in a table row. */
export function featureTags(bond: { features?: string[] | null }) {
  return (bond.features ?? []).filter(feature => LABELLED_FEATURES.includes(feature))
}

/** A hover title for a name shown in English: the name, then how the filing writes it where that differs. */
export function filedTitle(name: string, filed: string | null | undefined) {
  return filed && filed !== name ? `${name} · filed as ${filed}` : name
}

/** Securities codes are stored with a check digit: "72720" → "7272". */
export function displayTicker(ticker: string | null | undefined) {
  if (!ticker) return ''
  return /^[0-9][0-9A-Z]{3}0$/.test(ticker) ? ticker.slice(0, 4) : ticker
}

const JAPANESE = /[\u3000-\u30ff\u3400-\u9fff]/

/** Filed wording this file has no English for is not shown as it is: the hover text beside it quotes the filing. */
function inEnglish(filed: string) {
  return JAPANESE.test(filed) ? 'See the filing' : filed
}

/**
 * The security a bond has, in English: the schedule's "なし" or a supplement's
 * sentence become a word. Footnote marks ("(注)2") are not part of the answer.
 */
export function securityText(bond: { features?: string[] | null; collateral?: string | null }) {
  const features = bond.features ?? []
  if (features.includes('general-mortgage')) return 'General mortgage'
  if (features.includes('secured')) return 'Secured'
  const filed = (bond.collateral ?? '').trim()
  const text = filed.replace(/[(（]注\s*\d*[)）]\s*\d*|[※*]\s*\d*/g, '').replace(/\s+/g, '')
  if (!text) return filed ? 'See the filing’s notes' : 'Unsecured'
  if (/付さず|付されておらず|付されていない/.test(text)) return 'Unsecured'
  if (text.includes('保証')) return text.includes('銀行') ? 'Bank guarantee' : 'Guaranteed'
  if (/^(なし|無し|無|無担保.*|[―－—-]+)$/.test(text)) return 'Unsecured'
  if (/^(有|あり|担保付)/.test(text)) return 'Secured'
  return inEnglish(filed)
}

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
const ERA_YEARS: Record<string, number> = { 令和: 2018, 平成: 1988, 昭和: 1925 }
const FILED_DATE = /(?:(令和|平成|昭和)(?:年月日)?\s*)?(\d{1,4})\s*(?:年\s*(?:(\d{1,2})\s*月\s*(?:(\d{1,2})\s*日)?)?|[./]\s*(\d{1,2})\s*[./]\s*(\d{1,2}))/g

const ONLY_RANGE_MARKS = /^[\s~〜\-―－—–]*$/

/**
 * When a bond is due, in English, where a schedule gives a range or a month
 * rather than one date: "2026年9月~ 2031年3月" → "Sep 2026 – Mar 2031", a blank or
 * a dash → "". Japanese wording that is not dates becomes "See the filing".
 */
export function maturityText(text: string | null | undefined) {
  const filed = (text ?? '').normalize('NFKC').trim()
  if (/定めず|定めなし|定めはない/.test(filed)) return 'No fixed maturity'
  if (ONLY_RANGE_MARKS.test(filed)) return ''
  const dates: string[] = []
  const rest = filed.replace(FILED_DATE, (_, era: string | undefined, yearText: string, month: string | undefined, day: string | undefined, dottedMonth: string | undefined, dottedDay: string | undefined) => {
    const year = Number(yearText) + (era ? ERA_YEARS[era] : 0)
    const monthNumber = Number(month ?? dottedMonth ?? 0)
    const name = MONTHS[monthNumber - 1]
    const dayText = day ?? dottedDay
    dates.push(year < 1900 || (monthNumber && !name) ? '' : !monthNumber ? String(year) : dayText ? `${Number(dayText)} ${name} ${year}` : `${name} ${year}`)
    return ''
  })
  // Anything left besides range marks is not a date this understands.
  if (!dates.length || dates.includes('') || !ONLY_RANGE_MARKS.test(rest)) return inEnglish(filed)
  if (dates.length === 1 && /[~〜]\s*$/.test(filed)) return `From ${dates[0]}`
  return dates.join(' – ')
}

/** How the bond was sold: "一般募集" is a public offering. */
export function offeringText(offering: string | null | undefined) {
  const text = (offering ?? '').trim()
  if (!text) return ''
  if (text.includes('適格機関投資家')) return 'Qualified institutional investors only'
  if (text.includes('個人')) return 'Public offering to retail investors'
  if (text.includes('一般募集')) return 'Public offering'
  if (text.includes('私募')) return 'Private placement'
  return inEnglish(text)
}

/** Years left in a list: to the first call for callable bonds. */
export function termText(bond: { horizon?: number | null; horizon_to?: string; years_to_maturity?: number | null }) {
  return bond.horizon_to === 'call' ? `${formatYears(bond.horizon)} to call` : formatYears(bond.years_to_maturity)
}

export function bondMarketHref(bondId: string, companyCode?: string) {
  const params = new URLSearchParams({ tab: 'bond-market', bond: bondId })
  if (companyCode) params.set('issuer', companyCode)
  return `/research?${params.toString()}`
}

export function calculatorHref(bond: Pick<Bond, 'bond_id' | 'edinet_code'>) {
  return `/research?${new URLSearchParams({ tab: 'bonds', company: bond.edinet_code, bond: bond.bond_id }).toString()}`
}

export interface MarketFilters {
  query: string
  ratings: RatingGroup[]
  seniorities: Seniority[]
  industry: string
  minYears: number | null
  maxYears: number | null
  includePrivate: boolean
  yenOnly: boolean
  issuer: string
  /** Show only bonds whose issuer carries any of these tags. */
  tags: string[]
}

/** A tag as a bond filter: of the companies under it, those with bonds in the market. */
export interface MarketTag {
  name: string
  members: number
  issuers: string[]
}

/**
 * The user's tags against the bonds listed: each tag with its companies that
 * issued any of them (position tags first, then by name), and each issuer's
 * tags. A tag none of whose companies has a bond listed is left out.
 */
export function marketTags(tagged: Array<{ company_code: string; tags: string[] }>, bonds: MarketBond[]) {
  const issuers = new Set(bonds.map(bond => bond.edinet_code))
  const byName = new Map<string, MarketTag>()
  const byIssuer: Record<string, string[]> = {}
  for (const company of tagged) {
    const issues = issuers.has(company.company_code)
    if (issues && company.tags.length) byIssuer[company.company_code] = company.tags
    for (const name of company.tags) {
      const tag = byName.get(name) ?? { name, members: 0, issuers: [] }
      tag.members += 1
      if (issues) tag.issuers.push(company.company_code)
      byName.set(name, tag)
    }
  }
  const tags = [...byName.values()]
    .filter(tag => tag.issuers.length > 0)
    .sort((a, b) => Number(isPositionTag(b.name)) - Number(isPositionTag(a.name)) || a.name.localeCompare(b.name))
  return { tags, byIssuer }
}

/** The tag the typed text names: one starting with it, otherwise one containing it. */
export function matchTag<T extends { name: string }>(tags: T[], query: string) {
  const text = query.trim().toLowerCase()
  if (!text) return undefined
  return tags.find(tag => tag.name.toLowerCase().startsWith(text)) ?? tags.find(tag => tag.name.toLowerCase().includes(text))
}

export const DEFAULT_FILTERS: MarketFilters = {
  query: '',
  ratings: [],
  seniorities: [],
  industry: '',
  minYears: null,
  maxYears: null,
  includePrivate: false,
  yenOnly: true,
  issuer: '',
  tags: [],
}

/** ``issuerTags`` gives each issuer's tags: the tag filter reads them, and the text filter matches their names too. */
export function filterBonds(bonds: MarketBond[], companies: Record<string, { company_name: string; company_name_ja?: string | null; ticker?: string; industry?: string }>, filters: MarketFilters, issuerTags: Record<string, string[]> = {}) {
  const query = filters.query.trim().toLowerCase()
  return bonds.filter(bond => {
    const company = companies[bond.edinet_code]
    const tags = issuerTags[bond.edinet_code] ?? []
    if (filters.issuer && bond.edinet_code !== filters.issuer) return false
    if (filters.tags.length && !filters.tags.some(tag => tags.includes(tag))) return false
    if (!filters.includePrivate && bond.private) return false
    if (filters.yenOnly && (bond.currency ?? 'JPY') !== 'JPY') return false
    if (filters.ratings.length && !filters.ratings.includes(ratingGroup(bond.rating_notch))) return false
    if (filters.seniorities.length && !filters.seniorities.includes(bond.seniority)) return false
    if (filters.industry && company?.industry !== filters.industry) return false
    const years = bond.years_to_maturity
    if (filters.minYears != null && (years == null || years < filters.minYears)) return false
    if (filters.maxYears != null && (years == null || years > filters.maxYears)) return false
    if (query) {
      // The filed Japanese names are searched as well, so a bond can be found the way a filing writes it.
      const text = [bond.label, bond.label_ja, company?.company_name, company?.company_name_ja, company?.ticker, bond.edinet_code, ...tags].filter(Boolean).join(' ').toLowerCase()
      if (!text.includes(query)) return false
    }
    return true
  })
}

export type MarketSortKey = 'company' | 'coupon' | 'maturity' | 'rating' | 'spread' | 'yield' | 'price' | 'amount'

export function sortBonds(bonds: MarketBond[], companies: Record<string, { company_name: string }>, key: MarketSortKey, descending: boolean) {
  const value = (bond: MarketBond): number | string | null => {
    switch (key) {
      case 'company': return companies[bond.edinet_code]?.company_name ?? ''
      case 'coupon': return bond.coupon ?? null
      case 'maturity': return bond.maturity ?? null
      case 'rating': return bond.rating_notch ?? null
      case 'spread': return bond.spread ?? null
      case 'yield': return bond.market_yield ?? bond.model_yield ?? null
      case 'price': return bond.market_price ?? bond.model_price ?? null
      case 'amount': return bond.outstanding ?? null
    }
  }
  const direction = descending ? -1 : 1
  return [...bonds].sort((a, b) => {
    const left = value(a)
    const right = value(b)
    // Missing values sort last whichever way the column runs.
    if (left == null || right == null) return left == null ? (right == null ? 0 : 1) : -1
    if (typeof left === 'string' || typeof right === 'string') return String(left).localeCompare(String(right), 'ja') * direction
    return (left - right) * direction
  })
}

export function marketCsv(bonds: MarketBond[], companies: Record<string, { company_name: string; company_name_ja?: string | null; ticker?: string; industry?: string }>) {
  const header = ['Company', 'Ticker', 'EDINET code', 'Industry', 'Bond', 'Ranking', 'Currency', 'Coupon %', 'Issue date', 'Maturity', 'First call', 'Years left', 'Outstanding (JPY)', 'Rating', 'Agency', 'Spread at issue (bp)', 'JSDA date', 'JSDA price', 'JSDA yield %', 'Market spread (bp)', 'JGB now %', 'Constant-spread yield %', 'Constant-spread price', 'Company as filed', 'Bond as filed']
  const cell = (value: unknown) => {
    const text = value == null ? '' : String(value)
    return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text
  }
  const percent = (value: number | null | undefined) => value == null ? '' : (value * 100).toFixed(3)
  const rows = bonds.map(bond => {
    const company = companies[bond.edinet_code]
    return [
      company?.company_name, company?.ticker, bond.edinet_code, company?.industry, bond.label, SENIORITY_LABELS[bond.seniority] ?? bond.seniority, bond.currency ?? 'JPY',
      percent(bond.coupon), bond.issue_date, bond.maturity, bond.call_date, bond.years_to_maturity?.toFixed(2), bond.outstanding,
      bond.rating, bond.rating_agency, bond.issue_spread == null ? '' : Math.round(bond.issue_spread * 10_000),
      bond.market_date, bond.market_price, percent(bond.market_yield), bond.market_spread == null ? '' : Math.round(bond.market_spread * 10_000),
      percent(bond.jgb_now), percent(bond.model_yield), bond.model_price?.toFixed(3),
      company?.company_name_ja ?? company?.company_name, bond.label_ja ?? bond.label,
    ].map(cell).join(',')
  })
  return [header.join(','), ...rows].join('\n')
}
