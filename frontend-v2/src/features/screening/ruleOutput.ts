import { formulaMetric, formulaText } from '../comparison/formulaMetric'
import { isTextMetric, presetColumn, presetForTokens, type FormulaPreset } from './metricCatalog'
import { expressionProblem, ruleLeft } from './ruleModel'
import type { ComputedColumn, Criterion, ExpressionToken } from './types'

/**
 * What a screen's rules measure, shown beside its results so every match says
 * why it matched: each column a rule reads, and the result of each
 * calculation. It follows the rules on every run and is never saved with the
 * screen.
 */

export type RuleMetric =
  | { type: 'column'; reference: string }
  | { type: 'formula'; tokens: ExpressionToken[]; preset?: FormulaPreset }

type ColumnToken = Extract<ExpressionToken, { type: 'column' }>

// Results and comparisons always show the latest price.
const PRICE = 'Stock_Prices.Price'

/** The columns and formulas the enabled rules use, in the order the rules use them. */
export function ruleMetrics(criteria: Criterion[]): RuleMetric[] {
  const found: RuleMetric[] = []
  const seen = new Set<string>()
  const add = (key: string, metric: RuleMetric) => {
    if (seen.has(key)) return
    seen.add(key)
    found.push(metric)
  }
  for (const criterion of criteria) {
    if (criterion.enabled === false || criterion.comparison_mode === 'recent_split') continue
    const sides = [ruleLeft(criterion), criterion.comparison_mode === 'full_expression' ? criterion.right_side ?? [] : []]
    for (const side of sides) {
      // A formula picked by name, such as P/E ratio, is one metric.
      const preset = presetForTokens(side)
      if (preset) { add(formulaMetric(side), { type: 'formula', tokens: side, preset }); continue }
      const columns = side.filter((token): token is ColumnToken => token.type === 'column' && Boolean(token.table && token.column))
      for (const { table, column } of columns) {
        const reference = `${table}.${column}`
        if (reference !== PRICE) add(reference, { type: 'column', reference })
      }
      if (columns.length && side.some(token => token.type === 'op') && !expressionProblem(side, 'formula')) add(formulaMetric(side), { type: 'formula', tokens: side })
    }
  }
  return found
}

export interface RuleOutput {
  /** Columns the rules read that the screen does not show yet. */
  columns: string[]
  /** Derived columns for the rules' calculations, named apart from the screen's own. */
  computed: ComputedColumn[]
  /** Every rule metric with where it shows: a ``Table.Column`` reference, or a derived column's name. */
  shown: Array<{ metric: RuleMetric; column?: string; computed?: string; existing: boolean }>
}

// Result columns the server adds itself.
const RESERVED = ['LatestPrice', 'PriceDate', 'ScreeningRank']

function uniqueName(base: string, taken: Set<string>) {
  if (!taken.has(base)) return base
  for (let count = 1; ; count++) {
    const name = count === 1 ? `${base} · rule` : `${base} · rule ${count}`
    if (!taken.has(name)) return name
  }
}

/** What the rules add to a screen's output; a column or formula the screen already shows is used as it is. */
export function ruleOutput(criteria: Criterion[], output: { columns: string[]; computed: ComputedColumn[] }): RuleOutput {
  const result: RuleOutput = { columns: [], computed: [], shown: [] }
  const taken = new Set([...RESERVED, ...output.computed.map(column => column.name)])
  for (const metric of ruleMetrics(criteria)) {
    if (metric.type === 'column') {
      const existing = output.columns.includes(metric.reference)
      if (!existing) result.columns.push(metric.reference)
      result.shown.push({ metric, column: metric.reference, existing })
      continue
    }
    const key = formulaMetric(metric.tokens)
    const same = output.computed.find(column => column.expression_tokens?.length && formulaMetric(column.expression_tokens) === key)
    if (same) {
      result.shown.push({ metric, computed: same.name, existing: true })
      continue
    }
    const base: ComputedColumn = metric.preset ? presetColumn(metric.preset) : { name: formulaText(metric.tokens), formula_type: 'expression', expression_tokens: metric.tokens, format: 'number' }
    const name = uniqueName(base.name, taken)
    taken.add(name)
    result.computed.push({ ...base, name })
    result.shown.push({ metric, computed: name, existing: false })
  }
  return result
}

// The comparison's standard metrics already show these formulas.
const STANDARD_PRESETS = new Set(['pe', 'pb', 'ps', 'dividend_yield', 'market_cap'])
// Company facts, filing details, tags, and split events are not figures a comparison lines up.
const NOT_COMPARED = new Set(['CompanyInfo', 'FinancialStatements', 'Company_Tags', 'Stock_Splits', 'Stock_Prices'])
// The comparison API takes 50 metrics, standard ones included.
const MAX_COMPARED = 20

function comparable(table: string, column: string) {
  return `${table}.${column}` === PRICE || (!NOT_COMPARED.has(table) && !isTextMetric(table, column))
}

/** What the rules measure as comparison metrics beyond the standard ones: statement columns and formulas. */
export function comparisonMetrics(criteria: Criterion[]): string[] {
  const metrics = ruleMetrics(criteria).flatMap(metric => {
    if (metric.type === 'column') {
      const index = metric.reference.indexOf('.')
      return comparable(metric.reference.slice(0, index), metric.reference.slice(index + 1)) ? [metric.reference] : []
    }
    if (metric.preset && STANDARD_PRESETS.has(metric.preset.id)) return []
    return metric.tokens.every(token => token.type !== 'column' || comparable(token.table, token.column)) ? [formulaMetric(metric.tokens)] : []
  })
  return metrics.slice(0, MAX_COMPARED)
}
