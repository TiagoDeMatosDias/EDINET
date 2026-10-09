import { describe, expect, it } from 'vitest'

import type { HistoryMetric } from '../../api/types'
import {
  chooseFinancialUnit,
  commonSize,
  commonSizeBase,
  compoundGrowth,
  defaultChartFields,
  formatCell,
  formatChange,
  layoutStatement,
  orderTableKeys,
  periodChanges,
  rowKind,
  sentenceCase,
} from './statementLayout'

const metric = (field: string, values: Array<number | null>, display_name = field): HistoryMetric => ({ field, display_name, values })
const labels = (rows: Array<{ label: string; depth: number }>) => rows.map(row => `${'  '.repeat(row.depth)}${row.label}`)

describe('statement layout', () => {
  it('orders an income statement the way the filing presents it and nests components', () => {
    const rows = layoutStatement('IncomeStatement', [
      metric('Profit (loss)', [8], 'Profit (loss)'),
      metric('Income taxes - current', [2], 'Income Taxes - Current'),
      metric('Cost of sales', [60], 'Cost Of Sales'),
      metric('Income taxes', [2], 'Income Taxes'),
      metric('Net sales', [100], 'Net Sales'),
      metric('Operating Income - Operating profit', [12], 'Operating Income - Operating Profit (loss)'),
      metric('Ordinary Income - Ordinary income_2', [11], 'Ordinary Income - Ordinary Income 2'),
      metric('Gross profit', [40], 'Gross Profit (loss)'),
      metric('Never reported', [null], 'Extraordinary Losses'),
    ])
    expect(labels(rows)).toEqual([
      'Net sales',
      'Cost of sales',
      'Gross profit (loss)',
      'Operating profit (loss)',
      'Ordinary income',
      'Income taxes',
      '  Current',
      'Profit (loss)',
    ])
    expect(rows.find(row => row.field === 'Gross profit')?.total).toBe(true)
  })

  it('keeps unreported lines out unless asked for', () => {
    const metrics = [metric('a', [1], 'Net Sales'), metric('b', [null, null], 'Extraordinary Losses')]
    expect(layoutStatement('IncomeStatement', metrics).map(row => row.field)).toEqual(['a'])
    expect(layoutStatement('IncomeStatement', metrics, true).map(row => row.field)).toEqual(['a', 'b'])
  })

  it('folds a concept renamed between years into one line, but never conflicting values', () => {
    const rows = layoutStatement('BalanceSheet', [
      metric('Capital stock', [5, 5, null], 'Capital Stock'),
      metric('Share capital', [null, null, 5], 'Share Capital'),
      metric('Securities', [1, null], 'Securities'),
      metric('Securities 2', [2, 3], 'Securities 2'),
    ])
    const capital = rows.find(row => row.label === 'Share capital')
    expect(capital?.values).toEqual([5, 5, 5])
    expect(capital?.mergedFrom).toEqual(['Capital Stock'])
    expect(rows.filter(row => row.label === 'Securities')).toHaveLength(2)
  })

  it('nests standard lines under the balance sheet subtotal they belong to', () => {
    const rows = layoutStatement('BalanceSheet', [
      metric('Assets', [100], 'Assets'),
      metric('Cash and deposits', [10], 'Cash And Deposits'),
      metric('Current assets', [40], 'Current Assets'),
      metric('Current Assets - Accounts receivable', [30], 'Current Assets - Accounts Receivable - Trade'),
      metric('Long-term loans receivable', [1], 'Long-term Loans Receivable'),
    ])
    // Long-term loans belong under investments; with no such line they stay in the non-current block, before total assets.
    expect(labels(rows)).toEqual(['Current assets', '  Cash and deposits', '  Accounts receivable - trade', 'Long-term loans receivable', 'Assets'])
  })

  it('keeps source order for tables without a statement layout and reads rolling labels', () => {
    const rows = layoutStatement('Financial_Ratios_Rolling', [
      metric('z', [0.1], 'Return On Equity Average 3 Year'),
      metric('a', [0.2], 'Net Margin Growth 2 Year'),
    ])
    expect(rows.map(row => row.label)).toEqual(['Return on equity · 3-yr avg', 'Net margin · 2-yr growth'])
    expect(rows.every(row => row.kind === 'percent')).toBe(true)
  })

  it('charts the headline lines first', () => {
    const rows = layoutStatement('IncomeStatement', [
      metric('other', [1], 'Other'),
      metric('profit', [1], 'Profit (loss)'),
      metric('sales', [1], 'Net Sales'),
      metric('operating', [1], 'Operating Income - Operating Profit (loss)'),
    ])
    expect(defaultChartFields('IncomeStatement', rows)).toEqual(['sales', 'operating', 'profit'])
    const ratios = layoutStatement('Financial_Ratios', Array.from({ length: 5 }, (_, index) => metric(`field-${index}`, [index], `Metric ${index}`)))
    expect(defaultChartFields('Financial_Ratios', ratios)).toEqual(['field-0', 'field-1', 'field-2'])
  })

  it('orders statement tables before their rolling variants', () => {
    expect(orderTableKeys(['Financial_Ratios_Rolling', 'ShareMetrics', 'BalanceSheet', 'IncomeStatement_Rolling', 'IncomeStatement', 'Custom']))
      .toEqual(['IncomeStatement', 'BalanceSheet', 'ShareMetrics', 'Custom', 'IncomeStatement_Rolling', 'Financial_Ratios_Rolling'])
  })

  it('writes labels in sentence case and keeps acronyms', () => {
    expect(sentenceCase('Selling, General And Administrative Expenses')).toBe('Selling, general and administrative expenses')
    expect(sentenceCase('NCAV Per Share')).toBe('NCAV per share')
  })
})

describe('statement figures', () => {
  it('uses one currency unit that keeps a typical line readable', () => {
    expect(chooseFinancialUnit([{ values: [83_000_000, 51_000_000_000] }])).toEqual({ scale: 1e9, label: 'Billion' })
    // Totals in the trillions would push interest lines to 0.0 Trillion; billions keep both readable.
    expect(chooseFinancialUnit([{ values: [18e12, 4e12, 9e11, 5e11, 4.5e10, 6e9] }])).toEqual({ scale: 1e9, label: 'Billion' })
    expect(chooseFinancialUnit([{ values: [2e9, 1.5e9, 3e8] }])).toEqual({ scale: 1e6, label: 'Million' })
    expect(chooseFinancialUnit([])).toEqual({ scale: 1, label: 'Units' })
  })

  it('classifies rows by how their numbers read', () => {
    expect(rowKind('Financial_Ratios', 'Gross margin', [0.21])).toBe('percent')
    expect(rowKind('Financial_Ratios', 'Current ratio', [2.6])).toBe('number')
    expect(rowKind('IncomeStatement', 'Net sales', [1e12])).toBe('money')
    expect(rowKind('ShareMetrics', 'Number of employees', [390_927])).toBe('number')
  })

  it('formats cells for their kind', () => {
    const unit = { scale: 1e9, label: 'Billion' }
    expect(formatCell(18_259_979_000_000, 'money', unit)).toBe('18,260.0')
    expect(formatCell(-53_100_000_000, 'money', unit)).toBe('-53.1')
    expect(formatCell(0.1791, 'percent', unit)).toBe('17.9%')
    expect(formatCell(15_794_987_460, 'number', unit)).toBe('15.79B')
    expect(formatCell(99_032, 'number', unit)).toBe('99,032')
    expect(formatCell(260.284, 'number', unit)).toBe('260.28')
    expect(formatCell(null, 'number', unit)).toBe('')
  })

  it('measures change as a relative move, or in percentage points for percents', () => {
    const [sales] = layoutStatement('IncomeStatement', [metric('sales', [100, 110, null, 99], 'Net Sales')])
    expect(periodChanges(sales).map(value => value === null ? null : Number(value.toFixed(3)))).toEqual([null, 0.1, null, null])
    const [margin] = layoutStatement('Financial_Ratios', [metric('margin', [0.2, 0.15], 'Net Margin')])
    expect(formatChange(periodChanges(margin)[1], margin.kind)).toBe('−5.0 pp')
    expect(formatChange(0.1, 'money')).toBe('+10.0%')
  })

  it('expresses lines as a share of revenue', () => {
    const rows = layoutStatement('IncomeStatement', [metric('sales', [1e9, 2e9], 'Net Sales'), metric('cost', [6e8, 1.5e9], 'Cost Of Sales')])
    const base = commonSizeBase('IncomeStatement', rows)
    expect(base?.field).toBe('sales')
    expect(commonSize(rows[1], base)).toEqual([0.6, 0.75])
  })

  it('compounds growth between the first and last positive values', () => {
    const [sales] = layoutStatement('IncomeStatement', [metric('sales', [100, null, 121], 'Net Sales')])
    const growth = compoundGrowth(sales, ['2023-03-31', '2024-03-31', '2025-03-31'])
    expect(growth?.years).toBe(2)
    expect(growth?.rate).toBeCloseTo(0.1, 2)
    const [loss] = layoutStatement('IncomeStatement', [metric('loss', [-5, 10], 'Profit (loss)')])
    expect(compoundGrowth(loss, ['2024-03-31', '2025-03-31'])).toBeNull()
  })
})

describe('revenue base line', () => {
  it('uses the largest revenue line when a filer reports more than one', () => {
    const rows = layoutStatement('IncomeStatement', [
      metric('sales', [100, 164], 'Net Sales'),
      metric('operating revenue', [700, 800], 'Operating Revenue'),
      metric('operating', [50, 60], 'Operating Income - Operating Profit (loss)'),
    ])
    expect(commonSizeBase('IncomeStatement', rows)?.field).toBe('operating revenue')
    expect(defaultChartFields('IncomeStatement', rows)).toEqual(['operating revenue', 'operating'])
  })
})

describe('share basis', () => {
  // Toyota's 5-for-1 split in the year to March 2022: EPS 582.80 as filed is 116.56 on today's shares.
  const eps: HistoryMetric = { field: 'Basic earnings (loss) per share', display_name: 'Basic Earnings (loss) Per Share', values: [116.56, 121.98], reported_values: [582.8, 121.98] }
  const employees = metric('Number of employees', [370_000, 375_000])

  it('reads split-adjusted figures and keeps the figures as filed beside them', () => {
    const [row, plain] = layoutStatement('ShareMetrics', [eps, employees])
    expect(row.values).toEqual([116.56, 121.98])
    expect(row.alternate).toEqual([582.8, 121.98])
    expect(periodChanges(row)[1]).toBeCloseTo(121.98 / 116.56 - 1)
    expect(plain.alternate).toBeUndefined()
  })

  it('shows the figures as filed on request, and leaves lines no split changed alone', () => {
    const [row, plain] = layoutStatement('ShareMetrics', [eps, employees], false, 'filed')
    expect(row.values).toEqual([582.8, 121.98])
    expect(row.alternate).toEqual([116.56, 121.98])
    expect(plain.values).toEqual([370_000, 375_000])
  })
})
