import type { MetricDirection } from './features/comparison/bestValue'
import { formatCompactNumber } from './features/analysis/numberFormat'

/** How the server describes a metric (``METRIC_DEFINITIONS`` in the comparison service). */
export interface MetricDefinition {
  label: string
  group: string
  direction?: MetricDirection
  /** ``money`` amounts are in the ``price`` or ``reporting`` currency; ``percent`` values are fractions. */
  format?: 'money' | 'percent'
  currency?: 'price' | 'reporting'
  /** How the value is calculated, for tooltips. */
  description?: string
}

/** A company's currencies: its share price's and its financial statements'. */
export interface MetricCurrencies {
  price?: string | null
  reporting?: string | null
}

/** A known definition, or one derived from a ``Table.Column`` reference. */
export function metricDefinition(metric: string, definitions?: Record<string, MetricDefinition>): MetricDefinition {
  const known = definitions?.[metric]
  if (known) return known
  const [table, column] = metric.includes('.') ? metric.split('.', 2) : ['Other', metric]
  return { label: column || metric, group: table || 'Other' }
}

/** Metrics grouped by their definition's group, in first-appearance order. */
export function groupMetrics(metrics: string[], definitions?: Record<string, MetricDefinition>) {
  const groups = new Map<string, string[]>()
  for (const metric of metrics) {
    const group = metricDefinition(metric, definitions).group
    groups.set(group, [...(groups.get(group) ?? []), metric])
  }
  return [...groups].map(([group, keys]) => ({ group, metrics: keys }))
}

function currencySymbol(code: string) {
  try {
    const parts = new Intl.NumberFormat(undefined, { style: 'currency', currency: code, currencyDisplay: 'narrowSymbol' }).formatToParts(0)
    return parts.find(part => part.type === 'currency')?.value ?? code
  } catch {
    return code
  }
}

export function formatMetricValue(
  definition: MetricDefinition | undefined,
  value: number | null | undefined,
  currencies: MetricCurrencies = {},
): string {
  if (value == null || !Number.isFinite(value)) return '—'
  if (definition?.format === 'percent') return `${(value * 100).toFixed(1)}%`
  const magnitude = Math.abs(value)
  const amount = magnitude >= 1_000_000 ? formatCompactNumber(magnitude) : magnitude.toLocaleString(undefined, { maximumFractionDigits: 2 })
  const sign = value < 0 ? '-' : ''
  const code = definition?.format === 'money' ? currencies[definition.currency ?? 'reporting'] : null
  if (!code) return `${sign}${amount}`
  const symbol = currencySymbol(code)
  // A symbol that is just letters (e.g. "CHF") reads better as a separate word.
  return /^[A-Za-z]{2,}$/.test(symbol) ? `${sign}${symbol} ${amount}` : `${sign}${symbol}${amount}`
}
