import type { MetricDefinition } from '../../metrics'

export type ThesisStatus = 'watch' | 'buy' | 'hold' | 'sell'

/** One company in the research book: the user's research and its live market values. */
export interface BookCompany {
  company_code: string
  company_name: string
  ticker: string
  industry?: string | null
  /** ``security`` for a holding with no EDINET company, keyed by its symbol. */
  kind?: 'company' | 'security'
  /** Whether the portfolio holds it now or held it before. */
  position?: 'open' | 'closed' | null
  tags: string[]
  thesis_status?: ThesisStatus | null
  thesis?: string | null
  target_value?: number | null
  target_currency?: string | null
  review_on?: string | null
  note_count: number
  last_note_at?: string | null
  alert_count: number
  alerts_triggered: number
  updated_at?: string | null
  price_currency?: string | null
  LatestPrice?: number | null
  MarketCap?: number | null
  PERatio?: number | null
  PriceToBook?: number | null
  DividendsYield?: number | null
  ReturnOnEquity?: number | null
}

export interface BookAlert {
  alert_id: string
  name: string
  edinet_code?: string | null
  company_name?: string | null
  metric: string
  operator: string
  value: number
  enabled: boolean
  current_value?: number | null
  triggered: boolean
  price_currency?: string | null
  created_at?: string | null
}

export interface TagSummary { name: string; member_count: number }

export interface ResearchBook {
  companies: BookCompany[]
  tags: TagSummary[]
  alerts: BookAlert[]
  metric_definitions: Record<string, MetricDefinition>
  position_tags?: { open: string; closed: string }
  /** Companies tagged from the portfolio; ``null`` when it could not be read. */
  positions?: { open: number; closed: number } | null
}

export interface Note {
  note_id: string
  title: string
  body: string
  edinet_code?: string | null
  version?: number
  created_at?: string
  updated_at?: string
}

export interface CompanyResearch {
  edinet_code?: string
  thesis_status?: ThesisStatus | null
  thesis?: string | null
  target_value?: number | null
  target_currency?: string | null
  review_on?: string | null
  version?: number
}

export interface VolatilityEstimate { window: string; days: number | null; value: number | null }

/** ``GET /api/research/pricing/{code}``: what a company's data says for the calculators. */
export interface PricingInputs {
  company: { company_code: string; company_name: string; ticker: string; industry: string }
  currency: { price?: string | null; reporting?: string | null }
  spot: number | null
  price_date?: string | null
  dividend_yield: number | null
  market_cap: number | null
  volatility: { estimates: VolatilityEstimate[]; history: Array<{ date: string; value: number }>; observations: number }
  credit: {
    period: string | null
    lines: Partial<Record<'Revenue' | 'CostOfSales' | 'OperatingIncome' | 'NetIncome' | 'TotalAssets' | 'TotalEquity' | 'TotalLiabilities' | 'CurrentAssets' | 'CurrentLiabilities' | 'RetainedEarnings' | 'InterestExpense' | 'Cash', number | null>>
    debt: Record<string, number | null>
    debt_total: number | null
    previous_debt_total: number | null
    interest_coverage: number | null
    cost_of_debt: number | null
  }
  financial: boolean
}
