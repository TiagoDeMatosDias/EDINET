export type HoldingPerformance = {
  name?: string | null
  industry?: string | null
  first_purchase?: string | null
  last_purchase?: string | null
  first_trade?: string | null
  last_trade?: string | null
  num_buys?: number | null
  num_sells?: number | null
  current_value_display?: number | null
  cost_basis_display?: number | null
  pnl_display?: number | null
  realized_pnl_display?: number | null
  dividends_display?: number | null
  total_return_display?: number | null
  total_return_native?: number | null
  annualized_return?: number | null
  annualized_return_native?: number | null
  fx_return?: number | null
  volatility?: number | null
  dividend_income?: number | null
  dividend_gross?: number | null
  dividend_tax?: number | null
  dividend_yield?: number | null
  longest_holding_days?: number | null
  latest_holding_days?: number | null
  num_holding_periods?: number | null
  edinet_code?: string | null
  broker_description?: string | null
  total_pnl_display?: number | null
  holding_days?: number | null
  /** Start and end of the latest continuous holding period (a full sale and later purchase starts a new one). */
  held_since?: string | null
  held_until?: string | null
}

export type Holding = {
  symbol: string
  asset_category?: string
  quantity?: number
  avg_cost?: number | null
  market_price?: number | null
  market_value?: number | null
  market_value_native?: number | null
  market_value_display?: number | null
  valuation_date?: string
  currency?: string
  is_open?: boolean
  price_date?: string | null
  price_source?: string | null
  price_ticker?: string | null
  price_currency?: string | null
  performance?: HoldingPerformance | null
}

export type Transaction = {
  id?: number
  trade_date?: string
  settle_date?: string | null
  activity_type?: string
  asset_category?: string | null
  symbol?: string
  description?: string
  quantity?: number
  trade_price?: number | null
  trade_money?: number | null
  amount?: number
  proceeds?: number | null
  net_cash?: number | null
  commission?: number
  taxes?: number
  currency?: string
  buy_sell?: string | null
  source_file?: string
}

/** One weekday on the performance path; returns are cumulative from the period's start. */
export type PerformancePoint = {
  date: string
  cumulative_return: number
  drawdown: number
  value: number
  invested: number
  benchmark?: number
  inflation?: number
}

export type Benchmark = {
  ticker: string
  available?: boolean
  message?: string
  price_ticker?: string
  price_currency?: string
  coverage_start?: string
  coverage_end?: string
  full_coverage?: boolean
  last_price_date?: string
  stale_days?: number | null
  total_return?: number | null
  annualized_return?: number | null
  portfolio_total_return?: number | null
  portfolio_annualized_return?: number | null
  excess_return?: number | null
  relative_return?: number | null
  volatility?: number | null
  sharpe_ratio?: number | null
  max_drawdown?: number | null
  beta?: number | null
  alpha?: number | null
  correlation?: number | null
  r_squared?: number | null
  tracking_error?: number | null
  information_ratio?: number | null
  up_capture?: number | null
  down_capture?: number | null
  weeks?: number | null
}

export type RiskFreeInfo = {
  kind: 'series' | 'override' | 'missing'
  currency: string
  ticker?: string | null
  source?: string
  first_date?: string
  last_date?: string
  stale?: boolean
}

export type InflationInfo = {
  ticker: string
  total: number
  annualized: number | null
  last_observation: string
  estimated_months: number
  trailing_annual_rate: number
}

export type PerformanceWarning = { code: string; message: string }

export type Performance = {
  start_date?: string
  end_date?: string
  base_currency?: string
  period?: { start: string; end: string; days: number; years: number; observations: number; annualized: boolean; requested_start?: string | null }
  total_return?: number | null
  annualized_return?: number | null
  price_return?: number | null
  income_return?: number | null
  money_weighted_return?: number | null
  money_weighted_period_return?: number | null
  volatility?: number | null
  downside_deviation?: number | null
  sharpe_ratio?: number | null
  sortino_ratio?: number | null
  max_drawdown?: number | null
  max_dd_peak_date?: string | null
  max_dd_trough_date?: string | null
  max_dd_recovery_date?: string | null
  max_dd_days?: number | null
  current_drawdown?: number | null
  calmar_ratio?: number | null
  win_rate?: number | null
  avg_win?: number | null
  avg_loss?: number | null
  profit_factor?: number | null
  var_95?: number | null
  cvar_95?: number | null
  positive_months?: number
  months?: number
  total_dividend_income?: number | null
  risk_free_rate?: number | null
  cash_return?: number | null
  risk_free?: RiskFreeInfo
  inflation?: InflationInfo | null
  dividend_breakdown?: {
    total_gross?: number | null
    total_tax?: number | null
    total_net?: number | null
  }
  return_distribution?: {
    min?: number | null
    p25?: number | null
    median?: number | null
    p75?: number | null
    max?: number | null
    skewness?: number | null
    kurtosis?: number | null
    positive_days?: number | null
    negative_days?: number | null
    zero_days?: number | null
    best_day_date?: string
    worst_day_date?: string
  } | null
  return_attribution?: {
    total_return?: number | null
    dividend_yield?: number | null
    capital_appreciation?: number | null
    real_return?: number | null
    inflation_total?: number | null
  }
  benchmark?: Benchmark | null
  series?: PerformancePoint[]
  monthly_returns?: Array<{ month: string; portfolio: number; benchmark?: number | null }>
  annual_returns?: Array<{ year: number; portfolio: number; benchmark?: number | null; partial: boolean }>
  warnings?: PerformanceWarning[]
}

export type BenchmarkChoice = {
  ticker: string
  label: string
  detail: string
  available: boolean
  first_date?: string | null
  last_date?: string | null
  currency?: string | null
}

export type DataIssue = { level: 'error' | 'warning' | 'info'; code: string; message: string; symbols?: string[] }

export type HoldingDataStatus = {
  symbol: string
  currency: string
  weight: number | null
  price_source: 'market' | 'cost' | 'model' | null
  price_date: string | null
  stale_days: number | null
  price_ticker: string | null
  quote_currency: string | null
  converted: boolean
  latest_stored_price: string | null
  market_age_days: number | null
  newer_price_stored: boolean
  weekly_years: number[]
  status: 'ok' | 'stale' | 'cost' | 'missing'
}

export type DataQuality = {
  today: string
  display_currency: string
  valuation_date: string | null
  first_date: string | null
  days_behind: number | null
  last_transaction: string | null
  holdings: HoldingDataStatus[]
  large_moves: Array<{ date: string; return: number }>
  fx: { source: string; last_dates: Record<string, string | null> }
  risk_free: { kind: string; ticker?: string | null; source?: string | null; last_date?: string | null; latest_rate?: number | null; supported: boolean }
  inflation: { ticker?: string | null; last_observation?: string | null }
  issues: DataIssue[]
}

export type RefreshResult = {
  tickers: number
  updated: string[]
  failed: string[]
  refetched_daily_history?: string[]
  relabelled_prices: number
  fx_rows: number
  risk_free_rows: number
  daily_rows: number
  holdings_count: number
}

export type IncomePayment = {
  date: string
  symbol: string
  type: 'Dividend' | 'In lieu' | 'Tax adjustment' | 'Adjustment'
  currency: string
  per_share: number | null
  shares: number | null
  gross_native: number
  tax_native: number
  net_native: number
  in_lieu_native?: number
  gross: number
  tax: number
  net: number
  withholding_rate: number | null
}

export type IncomeYear = { year: number; gross: number; tax: number; net: number; per_share: number; payments: number; partial: boolean; per_share_growth: number | null }

export type IncomeCompany = {
  symbol: string
  name: string
  edinet_code?: string | null
  currency: string
  is_held: boolean
  shares_held: number | null
  gross: number
  tax: number
  net: number
  withholding_rate: number | null
  share_of_income: number | null
  payments: number
  first_date: string
  last_date: string
  frequency: number
  ttm_net: number
  ttm_per_share: number | null
  latest_per_share: number | null
  latest_date: string | null
  per_share_growth_1y: number | null
  current_yield: number | null
  yield_on_cost: number | null
  annual: IncomeYear[]
}

export type IncomeData = {
  currency: string
  valuation_date: string
  total_net: number
  payments: IncomePayment[]
  companies: IncomeCompany[]
}

export type PieData = {
  labels: string[]
  values: number[]
  total: number
  currency: string
}

export type DividendHistory = {
  periods: string[]
  companies: Record<string, number[]>
  currency: string
}

export type DividendCurrencyHistory = {
  periods: string[]
  currencies: Record<string, number[]>
  currency: string
}

export type DividendGrowthData = {
  years: number[]
  companies: Record<string, {
    currency: string
    dps: Array<number | null>
    yoy_growth: Array<number | null>
    avg_market_value_eur: Array<number | null>
  }>
  weighted_average_growth: Array<number | null>
}

export type HoldingHistoryPoint = {
  date: string
  market_price?: number | null
  market_value?: number | null
  market_value_native?: number | null
}

export type ContributionData = {
  years: number[]
  companies: Record<string, {
    contribution_eur: Array<number | null>
    contribution_pct: Array<number | null>
  }>
  portfolio_start?: Array<number | null>
}

export type PortfolioTab = 'overview' | 'holdings' | 'performance' | 'income' | 'activity' | 'data'
export type PerformanceRange = 'all' | '5y' | '3y' | '1y' | 'ytd'

export type PortfolioDetail =
  | { kind: 'holding'; holding: Holding }
  | { kind: 'transaction'; transaction: Transaction }

export type PortfolioSummary = {
  totalValue: number
  investedValue: number
  cashValue: number
  cashWeight: number
  costBasis: number
  pnl: number
  positionCount: number
  topHolding?: { symbol: string; value: number; weight: number }
}
