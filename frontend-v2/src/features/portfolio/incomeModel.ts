import type { IncomeCompany, IncomePayment } from './portfolioTypes'

export type IncomeGrouping = 'monthly' | 'quarterly' | 'yearly'
export type IncomeMeasure = 'net' | 'gross' | 'tax'
export type IncomeStack = 'company' | 'currency'

export const MEASURE_LABEL: Record<IncomeMeasure, string> = { net: 'Net dividends', gross: 'Gross dividends', tax: 'Withholding tax' }

/** 2026-04-30 → "2026-04", "2026 Q2", or "2026". */
export function periodKey(date: string, grouping: IncomeGrouping) {
  const year = date.slice(0, 4)
  if (grouping === 'yearly') return year
  const month = Number(date.slice(5, 7))
  return grouping === 'quarterly' ? `${year} Q${Math.ceil(month / 3)}` : date.slice(0, 7)
}

/** Every period from the first to the last, so quiet months still show as gaps. */
export function periodRange(first: string, last: string, grouping: IncomeGrouping) {
  const out: string[] = []
  let year = Number(first.slice(0, 4))
  let month = Number(first.slice(5, 7))
  const end = periodKey(last, grouping)
  for (let guard = 0; guard < 2400; guard++) {
    const key = periodKey(`${year}-${String(month).padStart(2, '0')}-01`, grouping)
    if (out[out.length - 1] !== key) out.push(key)
    if (key === end) break
    month += grouping === 'yearly' ? 12 : grouping === 'quarterly' ? 3 : 1
    while (month > 12) { month -= 12; year += 1 }
  }
  return out
}

/** Withholding tax as a positive amount paid (tax is stored as a negative cash flow; never "−0"). */
export function withheld(tax: number) {
  return tax === 0 ? 0 : -tax
}

export function measureValue(payment: IncomePayment, measure: IncomeMeasure) {
  // Withholding is shown as a positive amount paid.
  return measure === 'tax' ? withheld(payment.tax) : payment[measure]
}

export function selectPayments(payments: IncomePayment[], symbols: string[], start?: string) {
  const chosen = new Set(symbols)
  return payments.filter(payment => (!chosen.size || chosen.has(payment.symbol)) && (!start || payment.date > start))
}

export type IncomeSeries = { key: string; label: string; values: number[] }

/**
 * Totals per period for the chart, one series per company (largest first, the
 * rest grouped beyond ``limit``) or per payment currency.
 */
export function stackIncome(payments: IncomePayment[], grouping: IncomeGrouping, measure: IncomeMeasure, stack: IncomeStack, limit = 6) {
  if (!payments.length) return { periods: [] as string[], series: [] as IncomeSeries[] }
  const periods = periodRange(payments[0].date, payments[payments.length - 1].date, grouping)
  const index = new Map(periods.map((period, position) => [period, position]))
  const totals = new Map<string, number>()
  for (const payment of payments) {
    const key = stack === 'company' ? payment.symbol : payment.currency
    totals.set(key, (totals.get(key) ?? 0) + measureValue(payment, measure))
  }
  const ranked = [...totals.entries()].sort((left, right) => Math.abs(right[1]) - Math.abs(left[1])).map(([key]) => key)
  const shown = ranked.length > limit ? ranked.slice(0, limit - 1) : ranked
  const others = ranked.length > limit ? `${ranked.length - shown.length} others` : null
  const series = new Map<string, IncomeSeries>(shown.map(key => [key, { key, label: stack === 'currency' ? `Paid in ${key}` : key, values: periods.map(() => 0) }]))
  if (others) series.set(others, { key: others, label: others, values: periods.map(() => 0) })
  for (const payment of payments) {
    const key = stack === 'company' ? payment.symbol : payment.currency
    const target = series.get(series.has(key) ? key : others ?? key)
    const position = index.get(periodKey(payment.date, grouping))
    if (target && position != null) target.values[position] += measureValue(payment, measure)
  }
  return { periods, series: [...series.values()] }
}

export type IncomeSummary = {
  gross: number
  tax: number
  net: number
  rate: number | null
  payments: number
  companies: number
  lastTwelveMonths: number
  shareOfAll: number | null
}

export function summarizeIncome(selected: IncomePayment[], all: IncomePayment[], valuationDate: string, filtered: boolean): IncomeSummary {
  const sum = (rows: IncomePayment[], field: 'gross' | 'tax' | 'net') => rows.reduce((total, row) => total + row[field], 0)
  const gross = sum(selected, 'gross')
  const tax = sum(selected, 'tax')
  const yearAgo = new Date(`${valuationDate}T00:00:00Z`)
  yearAgo.setUTCDate(yearAgo.getUTCDate() - 365)
  const cutoff = Number.isNaN(yearAgo.getTime()) ? '9999' : yearAgo.toISOString().slice(0, 10)
  const allNet = sum(all, 'net')
  return {
    gross,
    tax,
    net: gross + tax,
    rate: gross > 0 ? -tax / gross : null,
    payments: selected.filter(row => row.type !== 'Tax adjustment').length,
    companies: new Set(selected.map(row => row.symbol)).size,
    lastTwelveMonths: sum(selected.filter(row => row.date > cutoff), 'net'),
    shareOfAll: filtered && allNet ? (gross + tax) / allNet : null,
  }
}

/**
 * Year-on-year growth in dividend per share, averaged over the chosen
 * companies with each weighted by its net income that year. Only years both
 * complete for a company count (its first year held and the current year do not).
 */
export function weightedGrowthByYear(companies: IncomeCompany[], fromYear?: number) {
  const byYear = new Map<number, { weighted: number; weight: number; companies: number }>()
  for (const company of companies) {
    for (const year of company.annual) {
      if (year.per_share_growth == null || year.net <= 0 || (fromYear && year.year < fromYear)) continue
      const bucket = byYear.get(year.year) ?? { weighted: 0, weight: 0, companies: 0 }
      bucket.weighted += year.per_share_growth * year.net
      bucket.weight += year.net
      bucket.companies += 1
      byYear.set(year.year, bucket)
    }
  }
  return [...byYear.entries()].sort((left, right) => left[0] - right[0]).map(([year, bucket]) => ({ year, growth: bucket.weighted / bucket.weight, companies: bucket.companies }))
}

/** Each company's dividend per share by calendar year, indexed to 100 in its first complete year. */
export function indexedPerShare(companies: IncomeCompany[]) {
  const years = [...new Set(companies.flatMap(company => company.annual.filter(year => !year.partial).map(year => year.year)))].sort((a, b) => a - b)
  return {
    years,
    series: companies.map(company => {
      const complete = company.annual.filter(year => !year.partial && year.per_share > 0)
      const base = complete[0]?.per_share
      const values = years.map(year => {
        const found = complete.find(row => row.year === year)
        return found && base ? found.per_share / base * 100 : null
      })
      return { symbol: company.symbol, values }
    }).filter(series => series.values.filter(value => value != null).length >= 2),
  }
}
