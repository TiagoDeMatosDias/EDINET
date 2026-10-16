import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import { useState } from 'react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { ColumnsPanel, DerivedColumns } from './ColumnsPanel'
import { buildMetricOptions } from './metricCatalog'
import type { ComputedColumn, MetricCatalog } from './types'

const CATALOG: MetricCatalog = {
  Stock_Prices: ['Price'],
  CompanyInfo: ['Company_Name', 'Company_Code'],
  ShareMetrics: ['Basic earnings (loss) per share', 'Net assets per share', 'Dividend paid per share'],
  Financial_Ratios: ['Return on Equity', 'Return on Assets'],
}

afterEach(cleanup)

function DerivedHarness() {
  const [columns, setColumns] = useState<ComputedColumn[]>([{ name: 'Derived metric', formula_type: 'expression', expression_tokens: [{ type: 'value', value: 1 }] }])
  return <DerivedColumns value={columns} options={buildMetricOptions(CATALOG)} onChange={setColumns} />
}

describe('output columns', () => {
  it('keeps the derived column name focused while typing', () => {
    render(<DerivedHarness />)
    const input = screen.getByRole('textbox', { name: 'Derived column name' })
    input.focus()
    fireEvent.change(input, { target: { value: 'P' } })
    expect(document.activeElement).toBe(input)
    expect(input).toHaveValue('P')
  })

  it('adds a column set and formulas in one step without duplicates', () => {
    const changes = vi.fn<(value: { columns: string[]; computed: ComputedColumn[] }) => void>()
    function Harness() {
      const [value, setValue] = useState({ columns: ['CompanyInfo.Company_Name'], computed: [] as ComputedColumn[] })
      return <ColumnsPanel catalog={CATALOG} options={buildMetricOptions(CATALOG)} columns={value.columns} computed={value.computed} rules={{ items: [], shown: true, onShow: () => undefined }} onChange={next => { setValue(next); changes(next) }} onClose={() => undefined} />
    }
    render(<Harness />)

    fireEvent.click(screen.getByRole('button', { name: 'Valuation' }))
    fireEvent.click(screen.getByRole('button', { name: 'Valuation' }))
    fireEvent.click(screen.getByRole('button', { name: 'Quality' }))

    const state = changes.mock.lastCall![0]
    expect(state.computed.map(column => column.name)).toEqual(['P/E ratio', 'P/B ratio', 'Earnings yield', 'Dividend yield'])
    expect(state.computed.find(column => column.name === 'Dividend yield')?.format).toBe('percent')
    expect(state.columns).toEqual(['CompanyInfo.Company_Name', 'Financial_Ratios.Return on Equity', 'Financial_Ratios.Return on Assets'])
  })

  it('lists what the rules add, counts it while shown, and turns it off', () => {
    const items = [
      { key: 'Financial_Ratios.Return on Equity', label: 'Return on Equity', detail: 'Ratios · also among your columns', existing: true },
      { key: 'formula:Share price × EPS', label: 'Share price × EPS', detail: 'Calculation', existing: false },
    ]
    function Harness() {
      const [shown, setShown] = useState(true)
      return <ColumnsPanel catalog={CATALOG} options={buildMetricOptions(CATALOG)} columns={['CompanyInfo.Company_Name', 'Financial_Ratios.Return on Equity']} computed={[]} rules={{ items, shown, onShow: setShown }} onChange={() => undefined} onClose={() => undefined} />
    }
    render(<Harness />)

    expect(screen.getByRole('list', { name: 'Columns from your rules' })).toHaveTextContent('Share price × EPSCalculation')
    expect(screen.getByRole('heading', { name: /Output columns/ })).toHaveTextContent('Output columns 3')
    fireEvent.click(screen.getByRole('checkbox', { name: 'Show the columns and calculations the rules use' }))
    expect(screen.getByRole('heading', { name: /Output columns/ })).toHaveTextContent('Output columns 2')
    expect(screen.getByRole('list', { name: 'Columns from your rules' })).toHaveClass('is-off')
  })
})
