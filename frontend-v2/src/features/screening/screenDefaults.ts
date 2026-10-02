import { FORMULA_PRESETS, presetAvailable, presetColumn } from './metricCatalog'
import type { ComputedColumn, Criterion, CriteriaMatch, ExpressionToken, MetricCatalog } from './types'

/**
 * What a new screen shows, the column sets a user can add in one step, and
 * starter screens that double as examples of what rules can express.
 */

interface ColumnSet { id: string; label: string; hint: string; refs: string[]; presets: string[] }

const IDENTITY = ['CompanyInfo.Company_Name', 'CompanyInfo.Company_Ticker', 'CompanyInfo.Company_Code', 'CompanyInfo.Company_Industry']

export const COLUMN_SETS: ColumnSet[] = [
  {
    id: 'overview', label: 'Overview', hint: 'Identity, valuation, quality, and growth at a glance',
    refs: [...IDENTITY, 'Financial_Ratios.Return on Equity', 'Financial_Ratios.Net Margin', 'ShareMetrics.Equity-to-asset ratio', 'IncomeStatement_Rolling.Net sales_Growth_3_Year', 'IncomeStatement.Net sales'],
    presets: ['market_cap', 'pe', 'pb', 'dividend_yield'],
  },
  { id: 'valuation', label: 'Valuation', hint: 'Price multiples and yields', refs: [], presets: ['market_cap', 'pe', 'pb', 'ps', 'earnings_yield', 'dividend_yield'] },
  {
    id: 'quality', label: 'Quality', hint: 'Returns, margins, and balance-sheet strength',
    refs: ['Financial_Ratios.Return on Equity', 'Financial_Ratios_Rolling.Return on Equity_Average_3_Year', 'Financial_Ratios.Return on Assets', 'Financial_Ratios.Gross Margin', 'Financial_Ratios.Net Margin', 'Financial_Ratios.Current Ratio', 'ShareMetrics.Equity-to-asset ratio'],
    presets: [],
  },
  {
    id: 'growth', label: 'Growth', hint: 'Multi-year compound growth of sales, earnings, and dividends',
    refs: ['IncomeStatement_Rolling.Net sales_Growth_3_Year', 'IncomeStatement_Rolling.Net sales_Growth_5_Year', 'ShareMetrics_Rolling.Basic earnings (loss) per share_Growth_3_Year', 'ShareMetrics_Rolling.Dividend paid per share_Growth_3_Year', 'PerShare_Metrics_Rolling.Sales Per Share_Growth_3_Year'],
    presets: [],
  },
  { id: 'dividends', label: 'Dividends', hint: 'Yield, payout, and dividend growth', refs: ['ShareMetrics.Dividend paid per share', 'ShareMetrics.Payout ratio', 'ShareMetrics_Rolling.Dividend paid per share_Growth_3_Year'], presets: ['dividend_yield'] },
  { id: 'size', label: 'Size', hint: 'Scale of the business', refs: ['IncomeStatement.Net sales', 'IncomeStatement.Profit (loss)', 'BalanceSheet.Assets', 'ShareMetrics.Number of employees'], presets: ['market_cap'] },
]

function has(catalog: MetricCatalog, reference: string) {
  const index = reference.indexOf('.')
  return (catalog[reference.slice(0, index)] ?? []).includes(reference.slice(index + 1))
}

/** The columns and derived formulas a column set adds, limited to what this database has. */
export function columnSet(id: string, catalog: MetricCatalog) {
  const set = COLUMN_SETS.find(item => item.id === id) ?? COLUMN_SETS[0]
  return {
    columns: set.refs.filter(reference => has(catalog, reference)),
    computed: set.presets
      .map(presetId => FORMULA_PRESETS.find(preset => preset.id === presetId))
      .filter((preset): preset is NonNullable<typeof preset> => Boolean(preset) && presetAvailable(preset!, catalog))
      .map(presetColumn),
  }
}

export function defaultOutput(catalog: MetricCatalog): { columns: string[]; computed: ComputedColumn[] } {
  return columnSet('overview', catalog)
}

/** Adds a set's columns and formulas that are not already shown. */
export function mergeColumnSet(current: { columns: string[]; computed: ComputedColumn[] }, id: string, catalog: MetricCatalog) {
  const set = columnSet(id, catalog)
  const names = new Set(current.computed.map(column => column.name))
  return {
    columns: [...current.columns, ...set.columns.filter(reference => !current.columns.includes(reference))],
    computed: [...current.computed, ...set.computed.filter(column => !names.has(column.name))],
  }
}

const col = (table: string, column: string): ExpressionToken => ({ type: 'column', table, column })
const preset = (id: string) => FORMULA_PRESETS.find(item => item.id === id)!.tokens
const value = (number: number): ExpressionToken[] => [{ type: 'value', value: number }]

function rule(left: ExpressionToken[], operator: string, right: ExpressionToken[], group?: { id: string; match: 'any' | 'all' }): Criterion {
  return { id: crypto.randomUUID(), comparison_mode: 'full_expression', operator, left_side: left, right_side: right, ...(group ? { group: group.id, group_match: group.match } : {}) }
}

export interface StarterScreen { id: string; name: string; description: string; match: CriteriaMatch; build: () => Criterion[] }

export const STARTER_SCREENS: StarterScreen[] = [
  {
    id: 'value', name: 'Value: cheap on earnings and book', match: 'all',
    description: 'Profitable companies with a P/E below 12 and trading below book value.',
    build: () => [rule(preset('pe'), '>', value(0)), rule(preset('pe'), '<', value(12)), rule(preset('pb'), '<', value(1))],
  },
  {
    id: 'quality', name: 'Quality: high returns, strong balance sheet', match: 'all',
    description: 'ROE above 15%, more than half the assets funded by equity, and net margin above 8%.',
    build: () => [rule([col('Financial_Ratios', 'Return on Equity')], '>', value(0.15)), rule([col('ShareMetrics', 'Equity-to-asset ratio')], '>', value(0.5)), rule([col('Financial_Ratios', 'Net Margin')], '>', value(0.08))],
  },
  {
    id: 'dividend', name: 'Dividends: covered income', match: 'all',
    description: 'Dividend yield above 3% with less than 60% of earnings paid out.',
    build: () => [rule(preset('dividend_yield'), '>', value(0.03)), rule([col('ShareMetrics', 'Payout ratio')], '>', value(0)), rule([col('ShareMetrics', 'Payout ratio')], '<', value(0.6))],
  },
  {
    id: 'net_net', name: 'Net-nets: below net current asset value', match: 'all',
    description: 'Net current asset value per share above the share price, compared metric to metric.',
    build: () => [rule([col('PerShare_Metrics', 'NCAV Per Share')], '>', [col('Stock_Prices', 'Price')])],
  },
  {
    id: 'garp', name: 'Growth at a reasonable price', match: 'all',
    description: 'Sales compounding above 10% a year over three years at a P/E under 20.',
    build: () => [rule([col('IncomeStatement_Rolling', 'Net sales_Growth_3_Year')], '>', value(0.1)), rule(preset('pe'), '>', value(0)), rule(preset('pe'), '<', value(20))],
  },
  {
    id: 'either', name: 'Deep value or high quality', match: 'any',
    description: 'Either very cheap (P/E under 10 and P/B under 0.8) or very profitable (ROE above 20% and equity ratio above 60%). Shows groups.',
    build: () => {
      const cheap = { id: 'g-cheap', match: 'all' as const }
      const quality = { id: 'g-quality', match: 'all' as const }
      return [
        rule(preset('pe'), '>', value(0), cheap), rule(preset('pe'), '<', value(10), cheap), rule(preset('pb'), '<', value(0.8), cheap),
        rule([col('Financial_Ratios', 'Return on Equity')], '>', value(0.2), quality), rule([col('ShareMetrics', 'Equity-to-asset ratio')], '>', value(0.6), quality),
      ]
    },
  },
]

/** Starters whose every metric exists in this database. */
export function availableStarters(catalog: MetricCatalog) {
  return STARTER_SCREENS.filter(starter => starter.build().every(criterion => [...(criterion.left_side ?? []), ...(criterion.right_side ?? [])].every(token => token.type !== 'column' || has(catalog, `${token.table}.${token.column}`))))
}
