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
}

export type Holding = {
  symbol: string
  asset_category?: string
  quantity?: number
  avg_cost?: number | null
  market_price?: number | null
  market_value?: number | null
  market_value_native?: number | null
  currency?: string
  is_open?: boolean
  performance?: HoldingPerformance
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

export type Performance = {
  start_date?: string
  end_date?: string
  base_currency?: string
  total_return?: number | null
  annualized_return?: number | null
  volatility?: number | null
  sharpe_ratio?: number | null
  sortino_ratio?: number | null
  max_drawdown?: number | null
  max_dd_peak_date?: string | null
  max_dd_trough_date?: string | null
  calmar_ratio?: number | null
  win_rate?: number | null
  avg_win?: number | null
  avg_loss?: number | null
  profit_factor?: number | null
  var_95?: number | null
  cvar_95?: number | null
  total_dividend_income?: number | null
  risk_free_rate?: number | null
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
  }
  return_attribution?: {
    total_return?: number | null
    dividend_yield?: number | null
    capital_appreciation?: number | null
    real_return?: number | null
    inflation_total?: number | null
  }
}

export type PieData = {
  labels: string[]
  values: number[]
  total: number
  currency: string
}

export type ValueHistory = {
  dates: string[]
  holdings: Record<string, Array<number | null>>
  currency?: string
  portfolio_values?: Array<number | null>
  net_inflows?: Array<number | null>
  daily_returns?: Array<number | null>
  cumulative_returns?: Array<number | null>
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

export type HeatmapData = {
  years: number[]
  months: number[]
  values: Array<Array<number | null>>
}

export type ScatterPoint = {
  symbol: string
  cost_basis_display: number
  annualized_return: number
  is_open: boolean
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

export type PortfolioTab = 'overview' | 'holdings' | 'performance' | 'income' | 'activity'
export type PerformanceRange = 'all' | '5y' | '3y' | '1y' | 'ytd'

export type PortfolioDetail =
  | { kind: 'value' }
  | { kind: 'performance' }
  | { kind: 'risk' }
  | { kind: 'allocation' }
  | { kind: 'income' }
  | { kind: 'activity' }
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
