import type { HistoryMetric } from '../../api/types'

/** How a row's numbers read: scaled currency, a fraction shown as a percent, or a plain figure. */
export type RowKind = 'money' | 'percent' | 'number'
export type StatementView = 'values' | 'yoy' | 'common'
export type FinancialUnit = { scale: number; label: string; digits?: number }

export interface StatementRow {
  field: string
  /** Cleaned, sentence-case label (the leaf when the row is nested under a parent line). */
  label: string
  /** The name as stored, kept for tooltips and exports. */
  fullName: string
  /** Stored names of earlier concepts folded into this row (EDINET renames lines over the years). */
  mergedFrom: string[]
  depth: number
  values: Array<number | null>
  kind: RowKind
  /** Number of periods with a value. */
  coverage: number
  /** A headline subtotal such as gross profit or total assets, rendered with emphasis. */
  total: boolean
}

export function finiteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value)
}

const MONEY_FAMILIES = new Set(['IncomeStatement', 'BalanceSheet', 'CashflowStatement'])
const PERCENT_LABEL = /margin|growth|return on|rate of return|shareholder return|payout|equity-to-asset|yield|ppe on assets/i

/** The statement a table belongs to: ``IncomeStatement_Rolling`` → ``IncomeStatement``. */
export function statementFamily(sourceKey: string) {
  return sourceKey.replace(/_Rolling$/, '')
}

export function isRollingTable(sourceKey: string) {
  return sourceKey.endsWith('_Rolling')
}

/** Statement tables first, in reading order, then their rolling variants. */
const TABLE_ORDER = ['IncomeStatement', 'BalanceSheet', 'CashflowStatement', 'ShareMetrics', 'PerShare_Metrics', 'Financial_Ratios']
const SHORT_TABLE_NAMES: Record<string, string> = {
  IncomeStatement: 'Income',
  BalanceSheet: 'Balance sheet',
  CashflowStatement: 'Cash flow',
  ShareMetrics: 'Share data',
  PerShare_Metrics: 'Per share',
  Financial_Ratios: 'Ratios',
}

export function orderTableKeys(keys: string[]) {
  const rank = (key: string) => {
    const index = TABLE_ORDER.indexOf(statementFamily(key))
    return (isRollingTable(key) ? 100 : 0) + (index === -1 ? 50 : index)
  }
  return [...keys].sort((a, b) => rank(a) - rank(b) || a.localeCompare(b))
}

export function shortTableName(sourceKey: string, displayName: string) {
  return SHORT_TABLE_NAMES[statementFamily(sourceKey)] ?? displayName.replace(/\s*\(Rolling\)$/, '')
}

function isAcronym(word: string) {
  return /[A-Z]{2,}/.test(word) && word === word.toUpperCase()
}

/** "Cost Of Sales" → "Cost of sales"; acronyms such as PPE and NCAV keep their capitals. */
export function sentenceCase(text: string) {
  return text.split(' ').map((word, index) => index === 0 || isAcronym(word) ? word : word.toLowerCase()).join(' ')
}

function normalise(text: string) {
  return text.toLowerCase().replace(/\s[2-9]$/, '').replace(/[^a-z0-9]+/g, '')
}

function firstWord(text: string) {
  return normalise(text.split(/\s+/)[0] ?? '')
}

/** Rolling tables name rows "Net Sales Average 3 Year"; read them as "Net sales · 3-yr avg". */
function rollingLabel(text: string) {
  const match = /^(.*) (Average|Growth) (\d+) Year$/.exec(text)
  if (!match) return text
  return `${match[1]} · ${match[3]}-yr ${match[2] === 'Average' ? 'avg' : 'growth'}`
}

function cleanLabel(text: string) {
  return sentenceCase(rollingLabel(text.replace(/\s[2-9]$/, '').trim()))
}

/** ``under`` nests a line beneath the subtotal it belongs to when the filing names no parent. */
interface CanonicalLine { match: RegExp; total?: boolean; under?: RegExp }

const CURRENT_ASSETS = /^current assets$/
const NON_CURRENT_ASSETS = /^non-current assets$/
const PPE = /^property, plant and equipment$/
const INVESTMENTS = /^investments and other assets$/
const CURRENT_LIABILITIES = /^current liabilities$/
const NON_CURRENT_LIABILITIES = /^non-current liabilities$/
const SHAREHOLDERS_EQUITY = /^shareholders' equity$/

// Statement presentation order for the lines most filings share. Everything else
// follows, ordered by how much history it has.
const CANONICAL_LINES: Record<string, CanonicalLine[]> = {
  IncomeStatement: [
    { match: /^(net sales|revenues?|operating revenues?|net revenues?|sales|ordinary revenues?)\b/, total: true },
    { match: /^cost of (sales|revenues?)/ },
    { match: /^gross profit/, total: true },
    { match: /^(selling, general and administrative|general and administrative|operating expenses)/ },
    { match: /^operating (income|profit)/, total: true },
    { match: /^non-operating income/ },
    { match: /^non-operating expenses/ },
    { match: /^ordinary (income|profit)/, total: true },
    { match: /^extraordinary income/ },
    { match: /^extraordinary loss/ },
    { match: /^(profit|income)( \(loss\))? before income taxes/, total: true },
    { match: /^income taxes$/ },
    { match: /^(profit|net income)( \(loss\))?$/, total: true },
    { match: /owners of (the )?parent/, total: true },
    { match: /non-controlling interests/ },
  ],
  BalanceSheet: [
    { match: CURRENT_ASSETS, total: true },
    { match: /^cash and (deposits|cash equivalents)/, under: CURRENT_ASSETS },
    { match: /^long-term loans receivable/, under: INVESTMENTS },
    { match: /receivable/, under: CURRENT_ASSETS },
    { match: /^securities$/, under: CURRENT_ASSETS },
    { match: /^(inventories|merchandise|work in process|raw materials)/, under: CURRENT_ASSETS },
    { match: NON_CURRENT_ASSETS, total: true },
    { match: PPE, under: NON_CURRENT_ASSETS },
    { match: /^(land|construction in progress)$/, under: PPE },
    { match: /^intangible assets/, under: NON_CURRENT_ASSETS },
    { match: INVESTMENTS, under: NON_CURRENT_ASSETS },
    { match: /^investment securities/, under: INVESTMENTS },
    { match: /^deferred tax assets$/, under: INVESTMENTS },
    { match: /^(total )?assets$/, total: true },
    { match: CURRENT_LIABILITIES, total: true },
    { match: /payable - trade|^accounts payable/, under: CURRENT_LIABILITIES },
    { match: /^short-term (borrowings|loans payable)/, under: CURRENT_LIABILITIES },
    { match: /^current portion/, under: CURRENT_LIABILITIES },
    { match: /^(income taxes payable|accrued expenses)/, under: CURRENT_LIABILITIES },
    { match: NON_CURRENT_LIABILITIES, total: true },
    { match: /^bonds payable/, under: NON_CURRENT_LIABILITIES },
    { match: /^long-term (borrowings|loans payable)/, under: NON_CURRENT_LIABILITIES },
    { match: /retirement benefit/, under: NON_CURRENT_LIABILITIES },
    { match: /^(total )?liabilities$/, total: true },
    { match: SHAREHOLDERS_EQUITY, total: true },
    { match: /^(capital stock|share capital)$/, under: SHAREHOLDERS_EQUITY },
    { match: /^capital surplus$/, under: SHAREHOLDERS_EQUITY },
    { match: /^(legal|other) capital surplus$/, under: /^capital surplus$/ },
    { match: /^retained earnings$/, under: SHAREHOLDERS_EQUITY },
    { match: /^(legal retained earnings|retained earnings brought forward)/, under: /^retained earnings$/ },
    { match: /^treasury shares/, under: SHAREHOLDERS_EQUITY },
    { match: /^(valuation and translation adjustments|accumulated other comprehensive income)/ },
    { match: /^valuation difference on available-for-sale/, under: /^(valuation and translation adjustments|accumulated other comprehensive income)/ },
    { match: /^(subscription rights to shares|share acquisition rights)/ },
    { match: /^non-controlling interests/ },
    { match: /^(total )?net assets$/, total: true },
    { match: /^(total )?liabilities and net assets$/, total: true },
  ],
  CashflowStatement: [
    { match: /operating activities/, total: true },
    { match: /investing activities/, total: true },
    { match: /financing activities/, total: true },
    { match: /exchange rate/ },
    { match: /^net (increase|decrease|change)/, total: true },
    { match: /beginning of (the )?(period|year)/ },
    { match: /end of (the )?(period|year)/, total: true },
  ],
}

const DEFAULT_CHART_LINES: Record<string, RegExp[]> = {
  IncomeStatement: [/^(net sales|revenues?|operating revenues?|net revenues?|sales|ordinary revenues?)\b/, /^operating (income|profit)/, /^(profit|net income)( \(loss\))?$/],
  BalanceSheet: [/^(total )?assets$/, /^(total )?liabilities$/, /^(total )?net assets$/],
  CashflowStatement: [/operating activities/, /investing activities/, /financing activities/],
}

// Later English taxonomy labels for the same concept; rows under either name are one line.
const LABEL_ALIASES: Record<string, string> = {
  sharecapital: 'capitalstock',
  currentportionofbondspayable: 'currentportionofbonds',
  shorttermborrowings: 'shorttermloanspayable',
  longtermborrowings: 'longtermloanspayable',
  shareacquisitionrights: 'subscriptionrightstoshares',
  provisionforbonusesfordirectorsandotherofficers: 'provisionfordirectorsbonuses',
  averagenumberoftemporaryemployees: 'averagenumberoftemporaryworkers',
}

function conceptKey(label: string) {
  const key = normalise(label)
  // "Miscellaneous loss" and "Miscellaneous losses" are one line under two spellings.
  const singular = label.toLowerCase().replace(/\s[2-9]$/, '').split(/\s+/).map(word => word.replace(/(es|s)$/, '')).join(' ').replace(/[^a-z0-9]+/g, '')
  return LABEL_ALIASES[key] ?? singular
}

function canonicalLine(family: string, label: string) {
  const lines = CANONICAL_LINES[family]
  const text = label.toLowerCase()
  const index = lines ? lines.findIndex(line => line.match.test(text)) : -1
  return index === -1
    ? { rank: Number.POSITIVE_INFINITY, total: false, under: undefined }
    : { rank: index, total: Boolean(lines[index].total), under: lines[index].under }
}

/** Two series describe one line when no period holds conflicting values. */
function compatible(a: Array<number | null>, b: Array<number | null>) {
  return a.every((value, index) => {
    const other = b[index]
    if (!finiteNumber(value) || !finiteNumber(other)) return true
    return Math.abs(value - other) <= Math.max(Math.abs(value), Math.abs(other)) * 0.005
  })
}

function lastIndex(values: Array<number | null>) {
  for (let index = values.length - 1; index >= 0; index -= 1) if (finiteNumber(values[index])) return index
  return -1
}

function latestMagnitude(values: Array<number | null>) {
  for (let index = values.length - 1; index >= 0; index -= 1) {
    const value = values[index]
    if (finiteNumber(value)) return Math.abs(value)
  }
  return 0
}

export function rowKind(family: string, label: string, values: Array<number | null>): RowKind {
  const magnitude = Math.max(0, ...values.filter(finiteNumber).map(Math.abs))
  if (PERCENT_LABEL.test(label) && magnitude <= 5) return 'percent'
  if (MONEY_FAMILIES.has(family) && magnitude >= 1e5) return 'money'
  return 'number'
}

interface LayoutItem {
  row: StatementRow
  sourceIndex: number
  /** Parent named by the "Parent - Child" form of the stored name. */
  namedParent?: string
  rank: number
  under?: RegExp
}

function prepareItems(family: string, metrics: HistoryMetric[]): LayoutItem[] {
  const byName = new Map(metrics.map(metric => [normalise(metric.display_name), metric.field]))
  const nameByField = new Map(metrics.map(metric => [metric.field, metric.display_name]))
  return metrics.map((metric, sourceIndex) => {
    const values = metric.values.map(value => finiteNumber(value) ? value : null)
    const separator = metric.display_name.indexOf(' - ')
    let label = metric.display_name
    let namedParent: string | undefined
    if (separator > 0) {
      const parent = metric.display_name.slice(0, separator)
      const leaf = metric.display_name.slice(separator + 3)
      const parentRow = byName.get(normalise(parent))
      // "Operating Income - Operating Profit (loss)" restates its parent; "Income Taxes - Current" is a component.
      if (leaf.split(/\s+/).map(normalise).includes(firstWord(parent))) label = leaf
      else if (parentRow && parentRow !== metric.field && !nameByField.get(parentRow)?.includes(' - ')) {
        label = leaf
        namedParent = parentRow
      }
    }
    const cleaned = cleanLabel(label)
    const line = canonicalLine(family, cleaned)
    return {
      row: {
        field: metric.field,
        label: cleaned,
        fullName: metric.display_name,
        mergedFrom: [],
        depth: 0,
        values,
        kind: rowKind(family, cleaned, values),
        coverage: values.filter(finiteNumber).length,
        total: !namedParent && line.total,
      },
      sourceIndex,
      namedParent,
      rank: line.rank,
      under: namedParent ? undefined : line.under,
    }
  })
}

function hostFor(item: LayoutItem, items: LayoutItem[]) {
  const under = item.under
  return under ? items.find(candidate => candidate !== item && under.test(candidate.row.label.toLowerCase())) : undefined
}

/** Fold rows that are one concept under successive names into the most recent one. */
function mergeRenamedConcepts(items: LayoutItem[]) {
  const redirect = new Map<string, string>()
  const groups = new Map<string, LayoutItem[]>()
  for (const item of items) {
    if (!item.row.coverage || /^other\b/i.test(item.row.label)) continue
    // Lines only merge within one section: current and non-current deferred tax assets stay apart.
    const section = item.namedParent ?? hostFor(item, items)?.row.field ?? ''
    const key = `${section}|${conceptKey(item.row.label)}`
    groups.set(key, [...(groups.get(key) ?? []), item])
  }
  for (const group of groups.values()) {
    if (group.length < 2) continue
    group.sort((a, b) => lastIndex(b.row.values) - lastIndex(a.row.values) || b.row.coverage - a.row.coverage)
    const [primary, ...others] = group
    for (const other of others) {
      if (!compatible(primary.row.values, other.row.values)) continue
      primary.row.values = primary.row.values.map((value, index) => value ?? other.row.values[index] ?? null)
      primary.row.mergedFrom.push(other.row.fullName)
      redirect.set(other.row.field, primary.row.field)
    }
    primary.row.coverage = primary.row.values.filter(finiteNumber).length
  }
  return {
    items: items.filter(item => !redirect.has(item.row.field)),
    redirect,
  }
}

/**
 * Lay out one statement table: clean labels, fold renamed concepts together, nest lines
 * under the subtotal they belong to, order key lines the way a filing presents them, and
 * drop rows with no values unless ``includeEmpty`` is set.
 */
export function layoutStatement(sourceKey: string, metrics: HistoryMetric[], includeEmpty = false): StatementRow[] {
  const family = statementFamily(sourceKey)
  const statementOrder = family in CANONICAL_LINES
  const merged = mergeRenamedConcepts(prepareItems(family, metrics))
  const visible = merged.items.filter(item => includeEmpty || item.row.coverage > 0)
  const byField = new Map(visible.map(item => [item.row.field, item]))
  const parentOf = new Map<string, string>()
  for (const item of visible) {
    const named = item.namedParent && (merged.redirect.get(item.namedParent) ?? item.namedParent)
    if (named && byField.has(named)) { parentOf.set(item.row.field, named); continue }
    if (item.namedParent) {
      // The named parent has no values: the line stands alone under its full name.
      item.row.label = cleanLabel(item.row.fullName)
      const line = canonicalLine(family, item.row.label)
      item.rank = line.rank
      item.under = line.under
      item.row.total = line.total
    }
    const host = hostFor(item, visible)
    if (host) parentOf.set(item.row.field, host.row.field)
  }
  const children = new Map<string, LayoutItem[]>()
  const roots: LayoutItem[] = []
  for (const item of visible) {
    const parent = parentOf.get(item.row.field)
    if (parent) children.set(parent, [...(children.get(parent) ?? []), item])
    else roots.push(item)
  }
  const bySourceOrder = (a: LayoutItem, b: LayoutItem) => a.sourceIndex - b.sourceIndex
  const byStatementOrder = (a: LayoutItem, b: LayoutItem) => a.rank - b.rank || b.row.coverage - a.row.coverage || latestMagnitude(b.row.values) - latestMagnitude(a.row.values)
  const byComponentSize = (a: LayoutItem, b: LayoutItem) => a.rank - b.rank || latestMagnitude(b.row.values) - latestMagnitude(a.row.values)
  const rows: StatementRow[] = []
  const visit = (item: LayoutItem, depth: number) => {
    rows.push({ ...item.row, depth })
    if (depth > 4) return
    for (const child of [...(children.get(item.row.field) ?? [])].sort(byComponentSize)) visit(child, depth + 1)
  }
  for (const root of roots.sort(statementOrder ? byStatementOrder : bySourceOrder)) visit(root, 0)
  return rows
}

/** The largest top-level line matching ``pattern``: a parent-only filer can report both small net sales and a larger operating revenue. */
function largestLine(rows: StatementRow[], pattern: RegExp) {
  return rows
    .filter(row => row.depth === 0 && row.coverage > 0 && pattern.test(row.label.toLowerCase()))
    .sort((a, b) => latestMagnitude(b.values) - latestMagnitude(a.values))[0]
}

/** The headline lines charted when a table first opens. */
export function defaultChartFields(sourceKey: string, rows: StatementRow[]) {
  const patterns = DEFAULT_CHART_LINES[statementFamily(sourceKey)]
  const roots = rows.filter(row => row.depth === 0 && row.coverage > 0)
  if (patterns && !isRollingTable(sourceKey)) {
    const chosen = patterns.flatMap(pattern => {
      const row = largestLine(roots, pattern)
      return row ? [row.field] : []
    })
    if (chosen.length) return chosen
  }
  const best = Math.max(0, ...roots.map(row => row.coverage))
  return roots.filter(row => row.coverage >= best / 2).slice(0, 3).map(row => row.field)
}

/**
 * One unit for a table's currency rows, chosen so a typical line reads with at least two
 * whole digits: the largest totals stay readable without the small lines collapsing to 0.0.
 */
export function chooseFinancialUnit(metrics: Array<{ values: Array<number | string | null> }>): FinancialUnit {
  const magnitudes = metrics.flatMap(metric => metric.values.filter(finiteNumber).map(Math.abs)).filter(value => value > 0).sort((a, b) => a - b)
  if (!magnitudes.length) return { scale: 1, label: 'Units' }
  const middle = Math.floor(magnitudes.length / 2)
  const median = magnitudes.length % 2 ? magnitudes[middle] : (magnitudes[middle - 1] + magnitudes[middle]) / 2
  const units: Array<[number, string]> = [[1e12, 'Trillion'], [1e9, 'Billion'], [1e6, 'Million'], [1e3, 'Thousand']]
  const [scale, label] = units.find(([candidate]) => median / candidate >= 10) ?? [1, 'Units']
  return { scale, label }
}

/** Whole units once a typical line runs to three digits; one decimal below that keeps small lines distinct. */
export function moneyDigits(metrics: Array<{ values: Array<number | string | null> }>, unit: FinancialUnit) {
  const scaled = metrics.flatMap(metric => metric.values.filter(finiteNumber).map(value => Math.abs(value) / unit.scale)).filter(value => value > 0).sort((a, b) => a - b)
  return scaled.length && scaled[Math.floor(scaled.length / 2)] >= 100 ? 0 : 1
}

/** Change against the prior period: relative for amounts, in percentage points for percents. */
export function periodChanges(row: StatementRow): Array<number | null> {
  return row.values.map((value, index) => {
    const previous = index > 0 ? row.values[index - 1] : null
    if (!finiteNumber(value) || !finiteNumber(previous)) return null
    if (row.kind === 'percent') return value - previous
    return previous === 0 ? null : (value - previous) / Math.abs(previous)
  })
}

/** Each value as a share of the statement's base line (revenue, or total assets). */
export function commonSize(row: StatementRow, base: StatementRow | undefined): Array<number | null> {
  if (!base || row.kind !== 'money') return row.values.map(() => null)
  return row.values.map((value, index) => {
    const denominator = base.values[index]
    return finiteNumber(value) && finiteNumber(denominator) && denominator !== 0 ? value / denominator : null
  })
}

export function commonSizeBase(sourceKey: string, rows: StatementRow[]) {
  const family = statementFamily(sourceKey)
  if (isRollingTable(sourceKey)) return undefined
  const pattern = family === 'IncomeStatement' ? DEFAULT_CHART_LINES.IncomeStatement[0] : family === 'BalanceSheet' ? /^(total )?assets$/ : null
  return pattern ? largestLine(rows, pattern) : undefined
}

function yearsBetween(start: string, end: string) {
  const from = Date.parse(start)
  const to = Date.parse(end)
  return Number.isFinite(from) && Number.isFinite(to) ? (to - from) / (365.25 * 24 * 3600 * 1000) : Number.NaN
}

/** Compound annual growth between the first and last reported values, when both are positive. */
export function compoundGrowth(row: StatementRow, periods: string[]) {
  if (row.kind === 'percent') return null
  const first = row.values.findIndex(finiteNumber)
  let last = -1
  row.values.forEach((value, index) => { if (finiteNumber(value)) last = index })
  if (first === -1 || last <= first) return null
  const start = row.values[first]!
  const end = row.values[last]!
  const years = yearsBetween(periods[first] ?? '', periods[last] ?? '')
  if (start <= 0 || end <= 0 || !(years >= 1.5)) return null
  return { rate: (end / start) ** (1 / years) - 1, years: Math.round(years) }
}

function groupThousands(value: number, digits: number) {
  return value.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits })
}

/** A cell value in the table's chosen presentation. */
export function formatCell(value: number | null, kind: RowKind, unit: FinancialUnit) {
  if (!finiteNumber(value)) return ''
  if (kind === 'percent') return `${(value * 100).toFixed(1)}%`
  if (kind === 'money') return groupThousands(value / unit.scale, unit.digits ?? 1)
  const magnitude = Math.abs(value)
  if (magnitude >= 1e9) return `${groupThousands(value / 1e9, 2)}B`
  if (magnitude >= 1e5 || Number.isInteger(value)) return groupThousands(value, 0)
  if (magnitude >= 1) return groupThousands(value, 2)
  return value === 0 ? '0' : value.toLocaleString('en-US', { maximumFractionDigits: 3 })
}

export function formatChange(value: number | null, kind: RowKind) {
  if (!finiteNumber(value)) return ''
  const sign = value > 0 ? '+' : value < 0 ? '−' : ''
  if (kind === 'percent') return `${sign}${Math.abs(value * 100).toFixed(1)} pp`
  const percent = Math.abs(value * 100)
  return `${sign}${percent >= 1000 ? groupThousands(percent, 0) : percent.toFixed(1)}%`
}

export function formatShare(value: number | null) {
  return finiteNumber(value) ? `${(value * 100).toFixed(1)}%` : ''
}
