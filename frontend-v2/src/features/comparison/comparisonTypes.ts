import type { MetricDefinition } from '../../metrics'

export interface ComparisonCompany {
  company_code: string
  company: { company_name?: string; ticker?: string; industry?: string; market?: string }
  metrics: Record<string, number | null>
  percentiles: Record<string, number | null>
  market?: { price_currency?: string | null }
  reporting_currency?: string | null
  period_end?: string | null
  price_date?: string | null
  data_quality_flags?: string[]
}

export interface ComparisonResponse {
  companies: ComparisonCompany[]
  requested: string[]
  missing: string[]
  metrics: string[]
  metric_definitions?: Record<string, MetricDefinition>
}

export interface MetricCatalogResponse {
  tables: Record<string, string[]>
  definitions?: Record<string, MetricDefinition>
  default_metrics?: string[]
}

export interface Peer {
  company_code: string
  ticker: string
  company_name: string
  industry: string
  market?: string | null
  MarketCap: number | null
  PERatio: number | null
  PriceToBook: number | null
  ReturnOnEquity: number | null
  DividendsYield: number | null
  /** The selected company closest in market cap, and this peer's size relative to it. */
  nearest_code: string | null
  size_ratio: number | null
  price_currency?: string | null
}

export interface PeersResponse {
  company_codes: string[]
  industries: Array<{ industry: string; candidates: number }>
  total: number
  peers: Peer[]
}

export interface TrendCompany {
  company_code: string
  periods: string[]
  series: Record<string, Array<number | null>>
}

export interface TrendsResponse {
  companies: TrendCompany[]
  metrics: string[]
  metric_definitions: Record<string, MetricDefinition>
}

/** What the page knows about a selected company before (or without) a comparison result. */
export interface CompanyInfo {
  company_code: string
  company_name?: string
  ticker?: string
  industry?: string
}

export interface SavedComparison {
  template_id: string
  name: string
  companies_json: string
  metrics_json: string
  updated_at?: string
}
