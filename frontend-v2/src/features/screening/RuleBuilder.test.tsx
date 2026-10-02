import { cleanup, fireEvent, render, screen, within } from '@testing-library/react'
import { useState } from 'react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { buildMetricOptions } from './metricCatalog'
import { RuleBuilder } from './RuleBuilder'
import type { ColumnFormats, Criterion, CriteriaMatch, MetricCatalog } from './types'

const CATALOG: MetricCatalog = {
  Stock_Prices: ['Price'],
  Stock_Splits: ['split_date'],
  Financial_Ratios: ['Return on Equity'],
  ShareMetrics: ['Basic earnings (loss) per share'],
}
const FORMATS: ColumnFormats = { 'Financial_Ratios.Return on Equity': 'percent' }

const changes = vi.fn<(criteria: Criterion[]) => void>()
const matchChanges = vi.fn<(match: CriteriaMatch) => void>()
const latest = () => changes.mock.lastCall?.[0] ?? []

function Harness({ initial, match = 'all' }: { initial: Criterion[]; match?: CriteriaMatch }) {
  const [criteria, setCriteria] = useState(initial)
  const [combine, setCombine] = useState<CriteriaMatch>(match)
  const options = buildMetricOptions(CATALOG, FORMATS)
  const ruleOptions = buildMetricOptions(CATALOG, FORMATS, { hideTables: ['Stock_Splits'] })
  return <RuleBuilder criteria={criteria} match={combine} options={options} ruleOptions={ruleOptions} formats={FORMATS} tagNames={['Watchlist']} openRuleId={null}
    onChange={next => { setCriteria(next); changes(next) }} onMatchChange={next => { setCombine(next); matchChanges(next) }} onAddRule={() => undefined} />
}

const expression = (id: string, left: Criterion['left_side'], operator: string, right: Criterion['right_side'], extra: Partial<Criterion> = {}): Criterion => ({ id, comparison_mode: 'full_expression', operator, left_side: left, right_side: right, ...extra })
const priceRule = (extra: Partial<Criterion> = {}) => expression('price', [{ type: 'column', table: 'Stock_Prices', column: 'Price' }], '>', [{ type: 'value', value: 0 }], extra)
const splitRule = (): Criterion => ({ id: 'split', comparison_mode: 'recent_split', value: '', field_type: 'date', split_action: 'exclude', split_status: 'confirmed', split_window_days: 365, split_date_operator: 'on_or_after' })

afterEach(() => {
  cleanup()
  changes.mockReset()
  matchChanges.mockReset()
})

describe('rule builder', () => {
  it('shows a one-metric rule as a filter row and edits its value', () => {
    render(<Harness initial={[priceRule()]} />)

    expect(screen.getByRole('button', { name: /Rule metric: Share price/ })).toBeInTheDocument()
    const input = screen.getByRole('textbox', { name: 'Filter value' })
    fireEvent.focus(input)
    fireEvent.change(input, { target: { value: '0.' } })
    expect(input).toHaveValue('0.')
    fireEvent.change(input, { target: { value: '0.05' } })
    expect(latest()[0].right_side).toEqual([{ type: 'value', value: '0.05' }])
  })

  it('takes percentages for percent metrics and stores fractions', () => {
    render(<Harness initial={[expression('roe', [{ type: 'column', table: 'Financial_Ratios', column: 'Return on Equity' }], '>', [{ type: 'value', value: 0.15 }])]} />)

    const input = screen.getByRole('textbox', { name: 'Filter value' })
    expect(input).toHaveValue('15')
    fireEvent.focus(input)
    fireEvent.change(input, { target: { value: '20' } })
    expect(latest()[0].right_side).toEqual([{ type: 'value', value: 0.2 }])
  })

  it('finds metrics by typing and keeps split tables out of ordinary filters', () => {
    render(<Harness initial={[priceRule()]} />)

    fireEvent.click(screen.getByRole('button', { name: /Rule metric/ }))
    const search = screen.getByRole('combobox', { name: 'Search rule metric' })
    fireEvent.change(search, { target: { value: 'split' } })
    expect(screen.getByText('No metric matches “split”.')).toBeInTheDocument()
    fireEvent.change(search, { target: { value: 'equity' } })
    fireEvent.keyDown(search, { key: 'Enter' })

    expect(latest()[0].left_side).toEqual([{ type: 'column', table: 'Financial_Ratios', column: 'Return on Equity' }])
  })

  it('offers formulas such as P/E and reads them back as one metric', () => {
    render(<Harness initial={[priceRule()]} />)

    fireEvent.click(screen.getByRole('button', { name: /Rule metric/ }))
    fireEvent.change(screen.getByRole('combobox', { name: 'Search rule metric' }), { target: { value: 'p/e' } })
    fireEvent.keyDown(screen.getByRole('combobox', { name: 'Search rule metric' }), { key: 'Enter' })

    expect(latest()[0].left_side).toHaveLength(3)
    expect(screen.getByRole('button', { name: /Rule metric: P\/E ratio/ })).toBeInTheDocument()
  })

  it('reshapes a rule for between and keeps its metric', () => {
    render(<Harness initial={[priceRule()]} />)

    fireEvent.change(screen.getByRole('combobox', { name: 'Rule comparison' }), { target: { value: 'BETWEEN' } })

    expect(latest()[0]).toMatchObject({ table: 'Stock_Prices', column: 'Price', operator: 'BETWEEN', comparison_mode: 'fixed' })
    expect(screen.getByRole('textbox', { name: 'Minimum value' })).toBeInTheDocument()
    expect(screen.getByRole('textbox', { name: 'Maximum value' })).toBeInTheDocument()
  })

  it('uses date inputs for split dates', () => {
    render(<Harness initial={[{ id: 'split-date', table: 'Stock_Splits', column: 'split_date', operator: 'BETWEEN', comparison_mode: 'fixed', value: '2024-01-01', value2: '2024-12-31', field_type: 'date' }]} />)

    expect(screen.getByLabelText('Start date')).toHaveAttribute('type', 'date')
    expect(screen.getByLabelText('End date')).toHaveAttribute('type', 'date')
  })

  it('keeps full expressions with explicit parentheses and date tokens', () => {
    render(<Harness initial={[expression('expr', [{ type: 'column', table: 'Stock_Splits', column: 'split_date' }, { type: 'op', op: '+' }, { type: 'value', value: 1 }], '>=', [{ type: 'value', value: '2024-01-01' }])]} />)
    const add = screen.getByRole('combobox', { name: 'Add right expression token' })

    expect(within(add).getByRole('option', { name: 'Date' })).toBeInTheDocument()
    fireEvent.change(add, { target: { value: 'lparen' } })
    fireEvent.change(add, { target: { value: 'rparen' } })
    expect(screen.getByLabelText('Open parenthesis')).toHaveTextContent('(')
    expect(screen.getByLabelText('Close parenthesis')).toHaveTextContent(')')
    expect(screen.getAllByLabelText('Expression date value')[0]).toHaveAttribute('type', 'date')
  })

  it('edits split events: window, exact cutoff, action, status, and direction', () => {
    render(<Harness initial={[splitRule()]} />)

    expect(screen.getByRole('spinbutton', { name: 'Split window days' })).toHaveValue(365)
    fireEvent.change(screen.getByRole('combobox', { name: 'Split match action' }), { target: { value: 'include' } })
    fireEvent.click(screen.getByRole('button', { name: 'Advanced' }))
    fireEvent.change(screen.getByRole('combobox', { name: 'Split confirmation status' }), { target: { value: 'pending' } })
    fireEvent.change(screen.getByRole('combobox', { name: 'Split date mode' }), { target: { value: 'exact' } })
    fireEvent.change(screen.getByRole('combobox', { name: 'Split date comparison' }), { target: { value: 'on_or_before' } })

    expect(screen.getByLabelText('Recent split cutoff date')).toHaveAttribute('type', 'date')
    expect(latest()[0]).toMatchObject({ split_action: 'include', split_status: 'pending', split_window_days: null, split_date_operator: 'on_or_before' })
  })

  it('combines rules by all or any, groups alternatives, and switches rules off', () => {
    render(<Harness initial={[priceRule({ group: 'g1', group_match: 'any' }), expression('roe', [{ type: 'column', table: 'Financial_Ratios', column: 'Return on Equity' }], '>', [{ type: 'value', value: 0.1 }], { group: 'g1', group_match: 'any' })]} />)

    const group = screen.getByRole('listitem', { name: 'Rule group A' })
    fireEvent.change(within(group).getByRole('combobox', { name: 'How group A combines' }), { target: { value: 'all' } })
    expect(latest().map(criterion => criterion.group_match)).toEqual(['all', 'all'])

    fireEvent.change(screen.getByRole('combobox', { name: 'How rules combine' }), { target: { value: 'any' } })
    expect(matchChanges.mock.lastCall?.[0]).toBe('any')

    fireEvent.click(screen.getByRole('checkbox', { name: 'Apply rule 1' }))
    expect(latest()[0].enabled).toBe(false)

    fireEvent.click(within(group).getByRole('button', { name: 'Ungroup' }))
    expect(latest().every(criterion => !criterion.group)).toBe(true)
  })

  it('flags rules that cannot run yet', () => {
    render(<Harness initial={[expression('blank', [], '>', [{ type: 'value', value: '' }])]} />)

    expect(screen.getByLabelText('Choose a metric.')).toBeInTheDocument()
  })
})
