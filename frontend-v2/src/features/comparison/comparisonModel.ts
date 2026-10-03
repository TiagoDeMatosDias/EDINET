import { SERIES_COLORS } from '../../brand'
import { metricDefinition, type MetricDefinition } from '../../metrics'
import { csvCell } from '../analysis/downloads'
import { columnLabel, tableInfo } from '../screening/metricCatalog'
import type { MetricDirection } from './bestValue'
import type { ComparisonCompany, TrendCompany } from './comparisonTypes'

export const MAX_COMPANIES = 12

/** Standard metrics in their fixed order, then added columns in the order they were added. */
export function orderMetrics(metrics: string[], standard: string[]) {
  const chosen = new Set(metrics)
  return [...standard.filter(metric => chosen.has(metric)), ...[...chosen].filter(metric => !standard.includes(metric))]
}

/** A readable definition for an added ``Table.Column``: "Net sales · 3-yr growth" from "Income statement · rolling". */
export function describeColumnMetric(metric: string): MetricDefinition {
  const [table, ...rest] = metric.split('.')
  const column = rest.join('.')
  return { label: column ? columnLabel(table, column) : metric, group: column ? tableInfo(table).label : 'Other' }
}

/** A stored JSON list of strings (saved comparisons keep codes and metrics this way); anything else is empty. */
export function parseList(text: string) {
  try {
    const value = JSON.parse(text) as unknown
    return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : []
  } catch {
    return []
  }
}

/** Company codes from a ``companies=`` parameter: trimmed, unique, at most twelve. */
export function parseCodes(value: string | null | undefined) {
  return [...new Set((value ?? '').split(',').map(code => code.trim()).filter(Boolean))].slice(0, MAX_COMPANIES)
}

export interface Rank { rank: number; of: number; score: number }

/**
 * Each value's place among the companies when a direction says which way is
 * favourable: rank 1 is best, and ``score`` runs from 1 (best) to 0 (worst).
 * A negative multiple or leverage ratio reflects losses or negative equity,
 * so where lower is better it ranks after every non-negative value.
 */
export function rankValues(direction: MetricDirection | undefined, values: Array<number | null | undefined>): Array<Rank | null> {
  if (!direction) return values.map(() => null)
  const key = (value: number) => direction === 'higher' ? -value : value < 0 ? Number.POSITIVE_INFINITY : value
  const comparable = values.filter((value): value is number => value != null && Number.isFinite(value))
  if (comparable.length < 2) return values.map(() => null)
  const keys = comparable.map(key).sort((a, b) => a - b)
  return values.map(value => {
    if (value == null || !Number.isFinite(value)) return null
    const position = key(value)
    const rank = keys.indexOf(position) + 1
    const score = position === Number.POSITIVE_INFINITY ? 0 : 1 - (rank - 1) / (comparable.length - 1)
    return { rank, of: comparable.length, score }
  })
}

/** A light tint from vermilion (worst) through clear to indigo (best); text stays ink. */
export function heatColor(score: number | null | undefined) {
  if (score == null) return undefined
  const strength = Math.abs(score - 0.5) * 2
  if (strength < 0.05) return undefined
  const alpha = (0.06 + strength * 0.2).toFixed(3)
  return score > 0.5 ? `rgb(52 96 168 / ${alpha})` : `rgb(196 70 44 / ${alpha})`
}

export function median(values: Array<number | null | undefined>) {
  const sorted = values.filter((value): value is number => value != null && Number.isFinite(value)).sort((a, b) => a - b)
  if (!sorted.length) return null
  const middle = Math.floor(sorted.length / 2)
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2
}

/** Companies ordered by one metric: best first where a direction exists, otherwise largest first; gaps last. */
export function sortCompanies<T extends ComparisonCompany>(companies: T[], metric: string, direction?: MetricDirection) {
  const values = companies.map(company => company.metrics[metric])
  const ranks = rankValues(direction ?? 'higher', values)
  return companies
    .map((company, index) => ({ company, index, rank: ranks[index]?.rank ?? Number.POSITIVE_INFINITY }))
    .sort((a, b) => a.rank - b.rank || a.index - b.index)
    .map(item => item.company)
}

/** Metrics no selected company has a value for. */
export function emptyMetrics(metrics: string[], companies: ComparisonCompany[]) {
  return new Set(metrics.filter(metric => companies.every(company => company.metrics[metric] == null)))
}

const monthYear = new Intl.DateTimeFormat('en-GB', { month: 'short', year: 'numeric', timeZone: 'UTC' })

export function formatPeriod(period: string | null | undefined) {
  if (!period) return ''
  const time = Date.parse(`${period.slice(0, 10)}T00:00:00Z`)
  return Number.isFinite(time) ? monthYear.format(time) : period
}

function tally(values: string[]) {
  const counts = new Map<string, number>()
  for (const value of values) counts.set(value, (counts.get(value) ?? 0) + 1)
  return [...counts].sort((a, b) => b[1] - a[1])
}

function listCounts(entries: Array<[string, number]>) {
  const parts = entries.map(([value, count]) => `${value} (${count})`)
  return parts.length > 1 ? `${parts.slice(0, -1).join(', ')} and ${parts[parts.length - 1]}` : parts[0]
}

/** A warning when the latest fiscal years end in different months, so figures cover different periods. */
export function fiscalYearNote(companies: ComparisonCompany[]) {
  const months = tally(companies.map(company => formatPeriod(company.period_end)).filter(Boolean))
  return months.length > 1 ? `Latest fiscal years end in ${listCounts(months)}.` : null
}

/** A warning when amounts are reported in different currencies and cannot be compared directly. */
export function currencyNote(companies: ComparisonCompany[]) {
  const currencies = tally(companies.map(company => company.reporting_currency ?? '').filter(Boolean))
  return currencies.length > 1 ? `Amounts are in each company's reporting currency: ${listCounts(currencies)}.` : null
}

/** Chart styling for the company in position ``index``: six colours, then the same six dashed. */
export function seriesStyle(index: number) {
  return { color: SERIES_COLORS[index % SERIES_COLORS.length], dashed: index >= SERIES_COLORS.length }
}

const LEGAL_SUFFIX = /[\s,.]*(co\.?,?\s*ltd\.?|company,?\s*limited|corporation|corp\.?|holdings,?\s*inc\.?|inc\.?|limited|ltd\.?|k\.?k\.?|plc)\s*$/i

/** "TOYOTA MOTOR CORPORATION" → "TOYOTA MOTOR"; short labels for chart points and narrow columns. */
export function shortName(name: string) {
  let result = name.trim()
  for (let pass = 0; pass < 2; pass++) result = result.replace(LEGAL_SUFFIX, '').trim()
  return result || name.trim()
}

export interface AlignedTrend { years: number[]; rows: Array<{ code: string; values: Array<number | null> }> }

/** One metric's yearly values per company on a shared axis of fiscal years (the year each period ends). */
export function alignTrends(companies: TrendCompany[], metric: string, codes: string[]): AlignedTrend {
  const byCode = new Map(companies.map(company => [company.company_code, company]))
  const perCompany = codes.map(code => {
    const company = byCode.get(code)
    const years = new Map<number, number | null>()
    company?.periods.forEach((period, index) => {
      const year = Number(period.slice(0, 4))
      const value = company.series[metric]?.[index] ?? null
      if (Number.isFinite(year) && (value != null || !years.has(year))) years.set(year, value)
    })
    return { code, years }
  })
  const years = [...new Set(perCompany.flatMap(item => [...item.years.keys()]))].sort((a, b) => a - b)
  return { years, rows: perCompany.map(item => ({ code: item.code, values: years.map(year => item.years.get(year) ?? null) })) }
}

/** Values rebased so each company's first positive value is 100: growth paths of different-sized companies. */
export function indexToHundred(values: Array<number | null>) {
  const base = values.find(value => value != null && value > 0)
  return values.map(value => value == null || base == null ? null : (value / base) * 100)
}

/** Dense cells abbreviate magnitudes: "¥23.66 Trillion" → "¥23.66T". */
export function abbreviate(text: string) {
  return text.replace(/ (Trillion|Billion|Million|Thousand)$/, (_, unit: string) => unit[0])
}

export function companyName(company: ComparisonCompany) {
  return company.company.company_name || company.company.ticker || company.company_code
}

/** The comparison as CSV with unformatted numbers, ready for a spreadsheet. */
export function comparisonCsv(companies: ComparisonCompany[], metrics: string[], definitions: Record<string, MetricDefinition>) {
  const lines = [
    ['Metric', 'Group', ...companies.map(companyName), 'Median'],
    ['Ticker', '', ...companies.map(company => company.company.ticker ?? ''), ''],
    ['EDINET code', '', ...companies.map(company => company.company_code), ''],
    ['Fiscal year end', '', ...companies.map(company => company.period_end ?? ''), ''],
    ['Reporting currency', '', ...companies.map(company => company.reporting_currency ?? ''), ''],
  ]
  for (const metric of metrics) {
    const definition = metricDefinition(metric, definitions)
    const values = companies.map(company => company.metrics[metric])
    const middle = median(values)
    lines.push([definition.label, definition.group, ...values.map(value => value == null ? '' : String(value)), middle == null ? '' : String(middle)])
  }
  return lines.map(line => line.map(csvCell).join(',')).join('\n') + '\n'
}

/** "1.3× Nissan" / "0.42× Toyota": a peer's market cap relative to the selected company nearest in size. */
export function sizeRatio(ratio: number | null | undefined) {
  if (ratio == null || !Number.isFinite(ratio)) return ''
  return `${ratio >= 10 ? ratio.toFixed(0) : ratio >= 1 ? ratio.toFixed(1) : ratio.toFixed(2)}×`
}
