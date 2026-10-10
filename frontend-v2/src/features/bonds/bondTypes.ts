export type Seniority = 'senior' | 'secured' | 'subordinated' | 'hybrid' | 'convertible'
export type CouponKind = 'fixed' | 'step' | 'fixed-to-floating' | 'floating' | 'zero' | 'unknown'

export interface Rating { agency: string; rating: string }

export interface CurvePoint { tenor: number; yield: number }

export interface CurvePayload {
  date: string
  points: CurvePoint[]
  year_ago: { date: string; points: CurvePoint[] } | null
}

/** Today's valuation of a bond on the JGB curve, added by the server. */
export interface BondValuation {
  years_to_maturity?: number | null
  /** Years to the date the yield runs to: the first call for a callable bond, otherwise maturity. */
  horizon?: number | null
  horizon_to?: 'call' | 'maturity'
  jgb_now?: number | null
  /** Today's government yield plus the bond's spread at issue. */
  model_yield?: number | null
  model_price?: number | null
  duration?: number | null
  /** The market spread when JSDA quotes the bond, otherwise the spread at issue. */
  spread?: number | null
  spread_basis?: 'market' | 'issue'
}

/** The JSDA OTC reference price matched to a bond, and the yield and spread it implies. */
export interface MarketQuote {
  market_date?: string | null
  market_price?: number | null
  market_yield?: number | null
  market_spread?: number | null
  market_reporters?: number | null
}

/**
 * One bond as the market view sends it; company fields are in ``companies``.
 * ``label`` is the bond's title in English, ``label_ja`` the title as filed where that differs.
 */
export interface MarketBond extends BondValuation, MarketQuote {
  bond_id: string
  edinet_code: string
  label: string
  label_ja?: string | null
  series?: number
  currency?: string
  seniority: Seniority
  features?: string[]
  coupon?: number
  coupon_kind?: CouponKind
  frequency?: number
  issue_date?: string
  maturity?: string
  perpetual?: number
  call_date?: string
  amount_issued?: number
  outstanding?: number
  private?: number
  rating?: string
  rating_agency?: string
  rating_notch?: number
  rating_inferred?: number
  issue_spread?: number
}

/** A company as Company Analysis names it; ``company_name_ja`` is the filer's Japanese name where that differs. */
export interface BondCompany {
  company_name: string
  company_name_ja?: string | null
  ticker?: string
  industry?: string
  listed?: number
}

export interface BondMarket {
  today: string
  curve: CurvePayload | null
  companies: Record<string, BondCompany>
  bonds: MarketBond[]
  industries: string[]
}

/** A bond with every stored field, as the company and detail views send it. */
export interface Bond extends BondValuation, MarketQuote {
  bond_id: string
  edinet_code: string
  company_name: string
  company_name_ja?: string | null
  ticker?: string
  industry?: string
  /** The issuer in English: the company, or a subsidiary as its annual report names it unless it files with EDINET itself. */
  issuer: string
  issuer_ja?: string | null
  is_parent: number
  /** The bond's full title as filed. */
  name: string
  label: string
  label_ja?: string | null
  series: number | null
  currency: string
  seniority: Seniority
  features: string[]
  coupon: number | null
  coupon_kind: CouponKind
  frequency: number | null
  issue_date: string | null
  maturity: string | null
  maturity_text: string | null
  perpetual: number
  call_date: string | null
  amount_issued: number | null
  issue_price: number | null
  outstanding: number | null
  outstanding_as_of: string | null
  current_portion: number | null
  status: 'outstanding' | 'redeemed' | 'matured'
  collateral: string | null
  offering: string | null
  private: number
  ratings: Rating[]
  rating: string | null
  rating_agency: string | null
  rating_notch: number | null
  rating_inferred: number | null
  issue_yield: number | null
  issue_tenor: number | null
  jgb_at_issue: number | null
  issue_spread: number | null
  issuance_doc_id: string | null
  issuance_submitted_at: string | null
  schedule_doc_id: string | null
  schedule_period_end: string | null
  issuance_url: string | null
  schedule_url: string | null
  jsda_code: string | null
  jsda_name: string | null
  market_change: number | null
}

export interface BondDocument {
  doc_id: string
  kind: 'issuance' | 'annual'
  submitted_at: string | null
  period_end: string | null
  bond_count: number
  description: string | null
  edinet_url: string
  stored: boolean
  in_catalog: boolean
}

export interface CompanyBonds {
  edinet_code: string
  company_name: string | null
  today: string
  curve: CurvePayload | null
  summary: {
    outstanding_count: number
    total_outstanding: number | null
    foreign_currency_count: number
    not_separately_reported: number
    average_coupon: number | null
    average_years: number | null
    next_maturity: string | null
    due_within_year: number | null
    as_of: string | null
    ratings: Rating[]
    rating: string | null
    rated_on: string | null
    median_spread: number | null
    rating_peer_spread: number | null
  }
  ladder: Array<{ year: number; parent: number; group: number }>
  bonds: Bond[]
  documents: BondDocument[]
}

export interface PeerValue {
  peer_count: number
  tenor_window: number
  peer_spread: number
  fair_yield: number
  fair_price: number
  spread_quartiles: number[] | null
  quoted_peers: number
  relative_spread?: number
}

/** A comparable bond: market fields plus the company it belongs to. */
export type PeerBond = MarketBond & BondCompany

export interface BondIssuance {
  doc_id: string
  name: string
  amount: number | null
  denomination: number | null
  issue_price: number | null
  coupon_text: string
  interest_dates: string
  maturity_text: string
  offering: string
  collateral: string
  negative_pledge: number
  covenants: string
  ratings: Rating[]
  stored: number
}

export interface BondDetail {
  today: string
  bond: Bond
  issuance: BondIssuance | null
  valuation: PeerValue | null
  market_history: Array<{ date: string; price: number; yield: number | null }>
  issuer_bonds: PeerBond[]
  similar: PeerBond[]
  spread_curve: {
    peers: Array<{ bond_id: string; company_name: string; horizon: number; spread: number; rating: string | null; quoted: boolean }>
    issuer: Array<{ bond_id: string; horizon: number; spread: number }>
  }
  curve: CurvePayload | null
}

export interface BondStatus {
  bonds: number
  outstanding: number
  companies: number
  curve_date: string | null
  updates: Record<string, { updated_at: string } & Record<string, unknown>>
}
