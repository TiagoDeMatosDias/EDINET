/**
 * Who a row in a shareholder list or an officer table is.
 * - person: an officer, known by name and date of birth across companies;
 * - individual: a shareholder known by name only (a list gives no date of birth);
 * - company: a holder that files with EDINET itself, named as Company Analysis names it;
 * - organization: any other bank, fund, association, or company.
 */
export type EntityKind = 'person' | 'individual' | 'company' | 'organization'

/** ``name`` is in English where a reliable source gives one; ``name_ja`` is the filed name when that differs. */
export interface EntityNames {
  entity_id: string | null
  kind: EntityKind
  name: string
  name_ja?: string | null
  edinet_code?: string | null
  ticker?: string
  industry?: string
  listed?: boolean
}

export interface ShareholderRow extends EntityNames {
  rank: number
  filed_name: string
  /** The account or proxy the filed name states, in English where its words are standard ones. */
  account: string | null
  account_ja: string | null
  address: string | null
  shares: number | null
  /** Share of the company as a decimal: 0.128 is 12.8%. */
  ratio: number | null
  /** The company's own shares, where it lists itself. */
  own_shares: boolean
  other_holdings: number
  positions: number
  previous_ratio?: number | null
  change?: number | null
  /** Not in the previous year's list. */
  new?: boolean
  /** First of the unbroken run of years in which the holder is listed. */
  since?: string | null
}

export interface HolderHistory extends EntityNames {
  /** One ratio per period in ``periods``; null where the holder was not listed. */
  ratios: Array<number | null>
}

export interface DistributionSlice {
  key: string
  label: string
  ratio: number | null
  holders: number | null
}

export interface CompanyShareholders {
  doc_id: string
  period_end: string
  as_of: string
  edinet_url: string
  top_ratio: number | null
  top_shares: number | null
  issued_shares: number | null
  /** The company's own shares and those its affiliates hold in it, and their share of the shares issued. */
  treasury_shares: number | null
  treasury_ratio: number | null
  shareholders: number | null
  distribution: DistributionSlice[]
  rows: ShareholderRow[]
  /** A second list by voting rights, filed by companies with several classes of shares. */
  voting: ShareholderRow[]
  history: { periods: string[]; holders: HolderHistory[] }
}

export interface OfficerRow {
  entity_id: string | null
  name: string
  name_ja: string | null
  /** In English when the filed title names roles this recognises; otherwise as filed. */
  title: string
  title_ja: string | null
  representative: boolean
  outside: boolean
  auditor: boolean
  birth_date: string | null
  age: number | null
  shares: number | null
  other_shares: number | null
  ratio: number | null
  term_start: string | null
  /** The fiscal period (YYYY-MM) whose shareholders' meeting ends the term. */
  term_end: string | null
  term: string | null
  other_positions: number
  holdings: number
  since?: string | null
}

export interface DepartedOfficer {
  entity_id: string | null
  name: string
  name_ja: string | null
  title: string
  title_ja: string | null
  other_positions: number
}

export interface CompanyPeople {
  doc_id: string
  period_end: string
  as_of: string | null
  edinet_url: string
  /** The slate proposed to the shareholders' meeting, from a report filed before it. */
  proposed: boolean
  source: 'tagged' | 'table' | null
  men: number | null
  women: number | null
  outside: number
  average_age: number | null
  total_shares: number | null
  total_ratio: number | null
  rows: OfficerRow[]
  departed: DepartedOfficer[]
  previous_period: string | null
}

export interface CompanyOwnership {
  edinet_code: string
  company_name: string
  company_name_ja: string | null
  ticker: string
  today: string
  latest_period: string | null
  /** The latest report is more than two years older than the newest stored. */
  stale: boolean
  reports: number
  /** The company as a shareholder of other companies, when their reports list it. */
  holder: { entity_id: string; holdings: number; past_holdings: number } | null
  shareholders: CompanyShareholders | null
  people: CompanyPeople | null
}

export interface EntitySummary extends EntityNames {
  entity_id: string
  birth_date: string | null
  age: number | null
  address: string | null
  headline: string | null
  holdings: number
  past_holdings: number
  positions: number
  past_positions: number
  has_reports?: boolean
}

export interface EntityList {
  total: number
  counts: Partial<Record<EntityKind, number>>
  entities: EntitySummary[]
}

interface CompanyFields {
  edinet_code: string
  company_name: string
  company_name_ja: string | null
  ticker: string
  industry: string
  listed: boolean
}

export interface EntityHolding extends CompanyFields {
  doc_id: string
  period_end: string
  as_of: string
  /** In the company's latest report, and that report is recent. */
  current: boolean
  rank: number
  shares: number | null
  ratio: number | null
  change: number | null
  first_period: string
  account: string | null
  account_ja: string | null
  /** The address filed with the name, sent for holders known by name only. */
  address?: string | null
  /** [period end, stake] for each stored year the holder was listed, oldest first; empty for a stake no longer listed. */
  history: Array<[string, number | null]>
}

export interface EntityPosition extends CompanyFields {
  doc_id: string
  period_end: string
  as_of: string | null
  current: boolean
  title: string
  title_ja: string | null
  representative: boolean
  outside: boolean
  shares: number | null
  ratio: number | null
  term_end: string | null
  first_period: string
  titles: Array<{ period_end: string; title: string }>
  share_history: Array<{ period_end: string; shares: number }>
}

/** Market capitalisation by EDINET code, as Company Analysis computes it; absent for unlisted companies. */
export type MarketCaps = Record<string, number | null>

export interface CareerEntry {
  /** ``YYYY-MM``, ``YYYY``, or empty when the filing gives no date for the line. */
  date: string
  text: string
}

export interface EntityCareer {
  edinet_code: string
  company_name: string
  ticker: string
  doc_id: string
  period_end: string
  entries: CareerEntry[]
}

export interface EntityDetail {
  today: string
  entity: EntitySummary
  holdings: EntityHolding[]
  positions: EntityPosition[]
  careers: EntityCareer[]
  /** Others filed under the same name: possibly the same person, which the filings cannot confirm. */
  same_name: EntitySummary[]
  totals: {
    holdings: number
    past_holdings: number
    positions: number
    past_positions: number
    /** How many different addresses the holdings were filed under. */
    addresses: number
  }
}

export interface OwnershipStatus {
  reports: number
  companies: number
  latest_period: string | null
  entities: number
  updates: Record<string, { updated_at: string } & Record<string, unknown>>
}
