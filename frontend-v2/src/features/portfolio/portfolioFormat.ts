import type { Holding, PieData, PortfolioSummary, Transaction, ValueHistory } from './portfolioTypes'

export function money(value: unknown, currency = 'EUR', digits = 0) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return '—'
  return new Intl.NumberFormat(undefined, {
    style: 'currency',
    currency,
    maximumFractionDigits: digits,
  }).format(parsed)
}

export function percent(value: unknown, digits = 1) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? `${(parsed * 100).toFixed(digits)}%` : '—'
}

export function percentPoints(value: unknown, digits = 1) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? `${parsed.toFixed(digits)}%` : '—'
}

export function decimal(value: unknown, digits = 2) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed.toFixed(digits) : '—'
}

export function quantity(value: unknown) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return '—'
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 4 }).format(parsed)
}

export function titleCase(value: string) {
  return value.replaceAll('_', ' ').toLowerCase().replace(/\b\w/g, letter => letter.toUpperCase())
}

export function transactionCashEffect(row: Transaction) {
  const candidates = row.activity_type === 'TRADE'
    ? [row.net_cash, row.proceeds, row.trade_money, row.amount]
    : [row.amount, row.net_cash]
  const value = candidates.find(candidate => candidate != null && Number.isFinite(Number(candidate)))
  return value == null ? undefined : Number(value)
}

export function buildPortfolioSummary(holdings: Holding[], allocation?: PieData): PortfolioSummary {
  const open = holdings.filter(holding => holding.is_open !== false)
  const totalValue = open.reduce((sum, holding) => sum + Number(holding.market_value ?? 0), 0)
  const cashValue = open
    .filter(holding => holding.asset_category === 'CASH' || holding.symbol.startsWith('CASH'))
    .reduce((sum, holding) => sum + Number(holding.market_value ?? 0), 0)
  const costBasis = open.reduce((sum, holding) => sum + Number(holding.performance?.cost_basis_display ?? 0), 0)
  const pnl = open.reduce((sum, holding) => sum + Number(holding.performance?.pnl_display ?? 0), 0)
  const rows = (allocation?.labels ?? []).map((symbol, index) => ({
    symbol,
    value: allocation?.values[index] ?? 0,
  })).sort((left, right) => right.value - left.value)
  const investedValue = allocation?.total ?? Math.max(0, totalValue - cashValue)
  const top = rows[0]
  return {
    totalValue,
    investedValue,
    cashValue,
    cashWeight: totalValue ? cashValue / totalValue : 0,
    costBasis,
    pnl,
    positionCount: open.filter(holding => holding.asset_category !== 'CASH').length,
    topHolding: top ? { ...top, weight: investedValue ? top.value / investedValue : 0 } : undefined,
  }
}

export function performanceStart(range: string, endDate?: string) {
  if (range === 'all' || !endDate) return undefined
  const end = new Date(`${endDate}T00:00:00Z`)
  if (Number.isNaN(end.getTime())) return undefined
  if (range === 'ytd') return `${end.getUTCFullYear()}-01-01`
  const years = range === '5y' ? 5 : range === '3y' ? 3 : 1
  end.setUTCFullYear(end.getUTCFullYear() - years)
  return end.toISOString().slice(0, 10)
}

export function sliceValueHistory(data?: ValueHistory, startDate?: string): ValueHistory | undefined {
  if (!data || !startDate) return data
  const startIndex = data.dates.findIndex(date => date >= startDate)
  if (startIndex <= 0) return data
  const slice = <T,>(values?: T[]) => values?.slice(startIndex)
  return {
    ...data,
    dates: data.dates.slice(startIndex),
    holdings: Object.fromEntries(Object.entries(data.holdings).map(([symbol, values]) => [symbol, values.slice(startIndex)])),
    portfolio_values: slice(data.portfolio_values),
    net_inflows: slice(data.net_inflows),
    daily_returns: slice(data.daily_returns),
    cumulative_returns: slice(data.cumulative_returns),
  }
}
