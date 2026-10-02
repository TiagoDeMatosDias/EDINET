import type { ColumnFormats, ComputedColumn, ExpressionToken, MetricCatalog } from './types'

/**
 * How the screener presents its metric catalog: readable table and column
 * names, which tables are internal, the formulas people reach for most, and
 * search across thousands of statement columns.
 */

interface TableInfo { label: string; order: number; hidden?: boolean }

const TABLES: Record<string, TableInfo> = {
  CompanyInfo: { label: 'Company', order: 0 },
  Stock_Prices: { label: 'Price', order: 1 },
  Financial_Ratios: { label: 'Ratios', order: 2 },
  ShareMetrics: { label: 'Share data', order: 3 },
  PerShare_Metrics: { label: 'Per share', order: 4 },
  IncomeStatement: { label: 'Income statement', order: 5 },
  BalanceSheet: { label: 'Balance sheet', order: 6 },
  CashflowStatement: { label: 'Cash flow', order: 7 },
  Company_Tags: { label: 'Your tags', order: 8 },
  Stock_Splits: { label: 'Splits', order: 9 },
  FinancialStatements: { label: 'Filing', order: 10 },
  // Pipeline bookkeeping and reference tables, not company metrics.
  Company_Descriptions: { label: 'Descriptions', order: 90, hidden: true },
  Split_Detection_Watermarks: { label: 'Split detection', order: 90, hidden: true },
  Statement_Hierarchy: { label: 'Statement hierarchy', order: 90, hidden: true },
  Taxonomy: { label: 'Taxonomy', order: 90, hidden: true },
  Taxonomy_Dictionary: { label: 'Taxonomy dictionary', order: 90, hidden: true },
  Tdnet_Disclosures: { label: 'TDnet disclosures', order: 90, hidden: true },
}

export function tableInfo(table: string): TableInfo {
  if (TABLES[table]) return TABLES[table]
  const base = table.replace(/_Rolling$/, '')
  if (base !== table && TABLES[base]) return { label: `${TABLES[base].label} · rolling`, order: TABLES[base].order + 20 }
  return { label: table.replace(/_/g, ' '), order: 50 }
}

const COLUMN_LABELS: Record<string, string> = {
  'CompanyInfo.Company_Code': 'EDINET code',
  'CompanyInfo.Company_Ticker': 'Ticker',
  'CompanyInfo.Company_Name': 'Company name',
  'CompanyInfo.Company_Industry': 'Industry',
  'CompanyInfo.Listed': 'Listing',
  'CompanyInfo.Capital_Stock': 'Capital stock',
  'CompanyInfo.Closing_Date': 'Fiscal year end',
  'Stock_Prices.Price': 'Share price',
  'Company_Tags.tag': 'Tag',
  'FinancialStatements.periodEnd': 'Fiscal period end',
}

/** "Net sales_Growth_3_Year" → "Net sales · 3-yr growth"; "Company_Name" → "Company name". */
export function columnLabel(table: string, column: string) {
  const known = COLUMN_LABELS[`${table}.${column}`]
  if (known) return known
  const rolling = /^(.*)_(Average|Growth)_(\d+)_Year$/.exec(column)
  if (rolling) return `${rolling[1]} · ${rolling[3]}-yr ${rolling[2] === 'Average' ? 'avg' : 'growth'}`
  return column.replace(/_/g, ' ')
}

const column = (table: string, name: string): ExpressionToken => ({ type: 'column', table, column: name })
const divide: ExpressionToken = { type: 'op', op: '/' }
const times: ExpressionToken = { type: 'op', op: '*' }
const PRICE = column('Stock_Prices', 'Price')
const EPS = column('ShareMetrics', 'Basic earnings (loss) per share')

export interface FormulaPreset {
  id: string
  label: string
  description: string
  format?: 'percent'
  tokens: ExpressionToken[]
}

/** Valuation formulas built from the latest price and the latest annual filing. */
export const FORMULA_PRESETS: FormulaPreset[] = [
  { id: 'pe', label: 'P/E ratio', description: 'Share price ÷ basic earnings per share from the latest annual filing.', tokens: [PRICE, divide, EPS] },
  { id: 'pb', label: 'P/B ratio', description: 'Share price ÷ net assets per share.', tokens: [PRICE, divide, column('ShareMetrics', 'Net assets per share')] },
  { id: 'ps', label: 'P/S ratio', description: 'Share price ÷ sales per share.', tokens: [PRICE, divide, column('PerShare_Metrics', 'Sales Per Share')] },
  { id: 'dividend_yield', label: 'Dividend yield', description: 'Dividend paid per share ÷ share price.', format: 'percent', tokens: [column('ShareMetrics', 'Dividend paid per share'), divide, PRICE] },
  { id: 'earnings_yield', label: 'Earnings yield', description: 'Basic earnings per share ÷ share price; the inverse of P/E.', format: 'percent', tokens: [EPS, divide, PRICE] },
  { id: 'market_cap', label: 'Market cap', description: 'Share price × shares issued as of the latest filing date.', tokens: [PRICE, times, column('ShareMetrics', 'Number of issued shares as of filing date')] },
]

/** A token's identity whatever its key order: saved screens come back from the server with sorted keys. */
function tokenIdentity(token: ExpressionToken) {
  const fields = token as unknown as Record<string, unknown>
  return JSON.stringify(Object.keys(fields).sort().map(key => [key, fields[key]]))
}

function sameTokens(a: ExpressionToken[], b: ExpressionToken[]) {
  return a.length === b.length && a.every((token, index) => tokenIdentity(token) === tokenIdentity(b[index]))
}

export function presetForTokens(tokens: ExpressionToken[] | undefined) {
  return tokens?.length ? FORMULA_PRESETS.find(preset => sameTokens(preset.tokens, tokens)) : undefined
}

export function presetAvailable(preset: FormulaPreset, catalog: MetricCatalog) {
  return preset.tokens.every(token => token.type !== 'column' || (catalog[token.table] ?? []).includes(token.column))
}

export interface MetricOption {
  /** ``Table.Column``, or ``preset:<id>`` for a formula. */
  key: string
  label: string
  tableLabel: string
  table?: string
  column?: string
  preset?: FormulaPreset
  format?: string
  description?: string
  order: number
  search: string
}

export function metricKey(table?: string, columnName?: string) {
  return table && columnName ? `${table}.${columnName}` : ''
}

/** Every choosable metric: formulas first, then columns of the visible tables. */
export function buildMetricOptions(catalog: MetricCatalog, formats: ColumnFormats = {}, options: { includePresets?: boolean; hideTables?: string[] } = {}): MetricOption[] {
  const result: MetricOption[] = []
  if (options.includePresets !== false) {
    for (const preset of FORMULA_PRESETS) {
      if (!presetAvailable(preset, catalog)) continue
      result.push({ key: `preset:${preset.id}`, label: preset.label, tableLabel: 'Formula', preset, format: preset.format, description: preset.description, order: -1, search: `${preset.label} ${preset.description} formula valuation`.toLowerCase() })
    }
  }
  for (const [table, columns] of Object.entries(catalog)) {
    const info = tableInfo(table)
    if (info.hidden || options.hideTables?.includes(table)) continue
    for (const name of columns) {
      const label = columnLabel(table, name)
      result.push({ key: `${table}.${name}`, label, tableLabel: info.label, table, column: name, format: formats[`${table}.${name}`], order: info.order, search: `${label} ${name} ${info.label} ${table}`.toLowerCase() })
    }
  }
  return result
}

/** Metrics people screen on most, shown before anything is typed. */
export const POPULAR_METRICS = [
  'preset:pe', 'preset:pb', 'preset:dividend_yield', 'preset:market_cap', 'Stock_Prices.Price',
  'Financial_Ratios.Return on Equity', 'Financial_Ratios.Return on Assets', 'Financial_Ratios.Net Margin',
  'Financial_Ratios.Gross Margin', 'Financial_Ratios.Current Ratio', 'ShareMetrics.Equity-to-asset ratio',
  'ShareMetrics.Payout ratio', 'IncomeStatement_Rolling.Net sales_Growth_3_Year', 'IncomeStatement.Net sales',
  'IncomeStatement.Profit (loss)', 'CompanyInfo.Company_Industry', 'CompanyInfo.Listed', 'Company_Tags.tag',
]

/** Options matching every word of ``query``, best first: label starts, then word starts, then anywhere. */
export function searchMetrics(options: MetricOption[], query: string, limit = 60): MetricOption[] {
  const words = query.trim().toLowerCase().split(/\s+/).filter(Boolean)
  if (!words.length) {
    const popular = POPULAR_METRICS.map(key => options.find(option => option.key === key)).filter((option): option is MetricOption => Boolean(option))
    return popular.length ? popular : options.slice(0, limit)
  }
  const scored: Array<{ option: MetricOption; score: number }> = []
  for (const option of options) {
    if (!words.every(word => option.search.includes(word))) continue
    const label = option.label.toLowerCase()
    let score = 0
    if (label.startsWith(words[0])) score -= 30
    else if (label.split(/[\s(/·-]+/).some(part => part.startsWith(words[0]))) score -= 15
    if (label === words.join(' ')) score -= 50
    if (option.preset) score -= 10
    if (option.table?.endsWith('_Rolling')) score += 5
    score += option.order * 0.5 + label.length * 0.02
    scored.push({ option, score })
  }
  return scored.sort((a, b) => a.score - b.score).slice(0, limit).map(item => item.option)
}

export function optionForTokens(options: MetricOption[], tokens: ExpressionToken[] | undefined) {
  const preset = presetForTokens(tokens)
  if (preset) return options.find(option => option.preset?.id === preset.id)
  const only = tokens?.length === 1 ? tokens[0] : undefined
  return only?.type === 'column' ? options.find(option => option.key === metricKey(only.table, only.column)) : undefined
}

/** Display format of an expression: a preset's declared format or the single column's. */
export function tokensFormat(tokens: ExpressionToken[] | undefined, formats: ColumnFormats) {
  const preset = presetForTokens(tokens)
  if (preset) return preset.format
  if (tokens?.length === 1 && tokens[0].type === 'column') return formats[metricKey(tokens[0].table, tokens[0].column)]
  return undefined
}

const TEXT_METRICS = new Set(['CompanyInfo', 'Company_Tags', 'FinancialStatements'])
const NUMERIC_COMPANY_COLUMNS = new Set(['Capital_Stock'])
const NUMERIC_PRICE_COLUMNS = new Set(['Price', 'Adjusted_Price', 'Split_Adjustment_Factor', 'Adjustment_Factor'])

/** Text metrics compare as words (equals, one of, contains); everything else as numbers. */
export function isTextMetric(table?: string, columnName?: string) {
  if (!table || !columnName) return false
  if (table === 'Stock_Prices') return !NUMERIC_PRICE_COLUMNS.has(columnName)
  return TEXT_METRICS.has(table) && !NUMERIC_COMPANY_COLUMNS.has(columnName)
}

export const SPLIT_DATE_COLUMNS = ['split_date', 'announced_at', 'ex_date', 'effective_date', 'record_date']

export function isDateMetric(table?: string, columnName?: string) {
  return table === 'Stock_Splits' && SPLIT_DATE_COLUMNS.includes(columnName ?? '')
}

/** A derived output column computing ``preset``. */
export function presetColumn(preset: FormulaPreset): ComputedColumn {
  return { name: preset.label, formula_type: 'expression', expression_tokens: preset.tokens, format: preset.format ?? 'number' }
}

/** Stored fractions shown as percentages: 0.153 → "15.3". */
export function toPercentText(value: unknown) {
  const number = typeof value === 'number' ? value : Number(value)
  if (value === '' || value == null || !Number.isFinite(number)) return String(value ?? '')
  return String(Number((number * 100).toPrecision(12)))
}

/** "15.3" → 0.153; anything that is not a number yet gives ``null``. */
export function parsePercentText(text: string) {
  const trimmed = text.trim()
  return trimmed !== '' && Number.isFinite(Number(trimmed)) ? Number((Number(trimmed) / 100).toPrecision(12)) : null
}
