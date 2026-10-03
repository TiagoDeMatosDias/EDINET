import type { Holding, PerformanceRange, PieData, PortfolioSummary, Transaction } from './portfolioTypes'

export function money(value: unknown, currency = 'EUR', digits = 0) {
  const parsed = Number(value)
  if (value == null || value === '' || !Number.isFinite(parsed)) return '—'
  return new Intl.NumberFormat(undefined, {
    style: 'currency',
    currency,
    maximumFractionDigits: digits,
  }).format(parsed)
}

/** Large amounts in charts and tight cells: €261.8k, ¥1.2M. */
export function compactMoney(value: unknown, currency = 'EUR') {
  const parsed = Number(value)
  if (value == null || !Number.isFinite(parsed)) return '—'
  return new Intl.NumberFormat(undefined, { style: 'currency', currency, notation: 'compact', maximumFractionDigits: 1 }).format(parsed)
}

export function percent(value: unknown, digits = 1) {
  const parsed = Number(value)
  return value != null && Number.isFinite(parsed) ? `${(parsed * 100).toFixed(digits)}%` : '—'
}

/** A change with its sign spelled out (+4.2%, −1.3%), so direction never rests on colour. */
export function signedPercent(value: unknown, digits = 1) {
  const parsed = Number(value)
  if (value == null || !Number.isFinite(parsed)) return '—'
  const text = Math.abs(parsed * 100).toFixed(digits)
  return Number(text) === 0 ? `${text}%` : `${parsed > 0 ? '+' : '−'}${text}%`
}

export function percentPoints(value: unknown, digits = 1) {
  const parsed = Number(value)
  return value != null && Number.isFinite(parsed) ? `${parsed.toFixed(digits)}%` : '—'
}

export function decimal(value: unknown, digits = 2) {
  const parsed = Number(value)
  return value != null && Number.isFinite(parsed) ? parsed.toFixed(digits) : '—'
}

export function quantity(value: unknown) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return '—'
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 4 }).format(parsed)
}

const dayFormat = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' })
const monthFormat = new Intl.DateTimeFormat('en-GB', { month: 'short', year: 'numeric', timeZone: 'UTC' })

/** 2026-10-02 → "2 Oct 2026"; anything else is returned as it came. */
export function formatDay(value?: string | null) {
  if (!value) return '—'
  const time = Date.parse(`${value.slice(0, 10)}T00:00:00Z`)
  return Number.isFinite(time) ? dayFormat.format(time) : value
}

export function formatMonth(value?: string | null) {
  if (!value) return '—'
  const time = Date.parse(`${value.slice(0, 7)}-01T00:00:00Z`)
  return Number.isFinite(time) ? monthFormat.format(time) : value
}

/** Days in the latest continuous holding period (to the sale for closed holdings, else the valuation date). */
export function heldDays(performance?: { held_since?: string | null; held_until?: string | null; latest_holding_days?: number | null } | null) {
  if (performance?.held_since && performance.held_until) {
    return Math.round((Date.parse(`${performance.held_until}T00:00:00Z`) - Date.parse(`${performance.held_since}T00:00:00Z`)) / 86_400_000) + 1
  }
  return performance?.latest_holding_days ?? null
}

/** 2,050 days → "5 y 7 mo"; 200 → "6 mo"; 20 → "20 d". */
export function durationText(days: number | null | undefined) {
  if (days == null || !Number.isFinite(days) || days <= 0) return '—'
  if (days < 31) return `${Math.round(days)} d`
  // Whole months held; a day or so short of a full month still counts as one.
  const months = Math.floor(days / 30.4375 + 0.05)
  if (months < 12) return `${months} mo`
  const years = Math.floor(months / 12)
  const rest = months % 12
  return rest ? `${years} y ${rest} mo` : `${years} y`
}

export function heldFor(performance?: { held_since?: string | null; held_until?: string | null; latest_holding_days?: number | null } | null) {
  const days = heldDays(performance)
  return days ? durationText(days) : ''
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

export function isCash(holding: Holding) {
  return holding.asset_category === 'CASH' || holding.symbol.startsWith('CASH')
}

/** A holding's value in the display currency (older responses only carry the EUR value). */
export function displayValue(holding: Holding) {
  return Number(holding.market_value_display ?? holding.market_value ?? 0)
}

/** Why a holding's quote deserves a second look: stale, at cost, or converted from another listing. */
export function priceNote(holding: Holding): { level: 'error' | 'warning' | 'info'; text: string } | null {
  if (holding.price_source === 'cost') return { level: 'error', text: 'No stored prices: valued at average cost' }
  if (holding.price_date && holding.valuation_date) {
    const days = (Date.parse(holding.valuation_date) - Date.parse(holding.price_date)) / 86_400_000
    if (days > 7) return { level: 'warning', text: `Last price ${formatDay(holding.price_date)}, ${Math.round(days)} days before the valuation date` }
  }
  if (holding.price_currency && holding.currency && holding.price_currency !== holding.currency) return { level: 'info', text: `Quoted in ${holding.price_currency} and converted to ${holding.currency} at ECB rates` }
  return null
}

export function holdingName(holding: Holding) {
  if (isCash(holding)) return `Cash in ${holding.currency ?? holding.symbol.replace('CASH ', '')}`
  return holding.performance?.name || holding.performance?.broker_description || holding.asset_category || ''
}

/**
 * Where a holding opens in Analysis: Tokyo-listed holdings by their EDINET company,
 * others by ticker (stored prices only, as there are no Japanese filings).
 */
export function holdingAnalysisHref(holding: Holding) {
  const code = holding.performance?.edinet_code
  return code ? `/analyze/${encodeURIComponent(code)}?from=portfolio` : `/analyze?ticker=${encodeURIComponent(holding.symbol)}&from=portfolio`
}

export function buildPortfolioSummary(holdings: Holding[], allocation?: PieData): PortfolioSummary {
  const open = holdings.filter(holding => holding.is_open !== false)
  const totalValue = open.reduce((sum, holding) => sum + displayValue(holding), 0)
  const cashValue = open.filter(isCash).reduce((sum, holding) => sum + displayValue(holding), 0)
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
    positionCount: open.filter(holding => !isCash(holding)).length,
    topHolding: top ? { ...top, weight: investedValue ? top.value / investedValue : 0 } : undefined,
  }
}

export const RANGES: Array<{ key: PerformanceRange; label: string; title: string }> = [
  { key: 'ytd', label: 'YTD', title: 'Year to date' },
  { key: '1y', label: '1Y', title: 'The last year' },
  { key: '3y', label: '3Y', title: 'The last three years' },
  { key: '5y', label: '5Y', title: 'The last five years' },
  { key: 'all', label: 'All', title: 'Since the first transaction' },
]

/**
 * The period's base date: returns run from that day's close. Year to date starts
 * at the previous year's last close, so 2 January's move counts.
 */
export function performanceStart(range: string, endDate?: string) {
  if (range === 'all' || !endDate) return undefined
  const end = new Date(`${endDate}T00:00:00Z`)
  if (Number.isNaN(end.getTime())) return undefined
  if (range === 'ytd') return `${end.getUTCFullYear() - 1}-12-31`
  const years = range === '5y' ? 5 : range === '3y' ? 3 : 1
  end.setUTCFullYear(end.getUTCFullYear() - years)
  return end.toISOString().slice(0, 10)
}
