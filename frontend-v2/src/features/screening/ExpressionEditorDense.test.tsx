import { fireEvent, render, screen, within } from '@testing-library/react'
import { useState } from 'react'
import { describe, expect, it } from 'vitest'

import { CriterionEditor } from './ExpressionEditorDense'
import type { Criterion } from './types'

function DecimalHarness() {
  const [criterion, setCriterion] = useState<Criterion>({
    id: 'rule', comparison_mode: 'full_expression', operator: '>',
    left_side: [{ type: 'column', table: 'Stock_Prices', column: 'Price' }],
    right_side: [{ type: 'value', value: 0 }],
  })
  return <CriterionEditor criterion={criterion} catalog={{ Stock_Prices: ['Price'] }} tagNames={[]} index={0} onChange={setCriterion} onRemove={() => undefined} />
}

function RecentSplitHarness() {
  const [criterion, setCriterion] = useState<Criterion>({
    id: 'split', comparison_mode: 'recent_split', operator: '=', value: '', field_type: 'date',
  })
  return <CriterionEditor criterion={criterion} catalog={{ Stock_Splits: ['split_date'] }} tagNames={[]} index={0} onChange={setCriterion} onRemove={() => undefined} />
}

function SplitDateHarness() {
  const [criterion, setCriterion] = useState<Criterion>({
    id: 'split-date', table: 'Stock_Splits', column: 'split_date', operator: 'BETWEEN',
    comparison_mode: 'fixed', value: '2024-01-01', value2: '2024-12-31', field_type: 'date',
  })
  return <CriterionEditor criterion={criterion} catalog={{ Stock_Splits: ['split_date'] }} tagNames={[]} index={0} onChange={setCriterion} onRemove={() => undefined} />
}

function SplitExpressionHarness() {
  const [criterion, setCriterion] = useState<Criterion>({
    id: 'split-expression', comparison_mode: 'full_expression', operator: '>=',
    left_side: [{ type: 'column', table: 'Stock_Splits', column: 'split_date' }], right_side: [],
  })
  return <CriterionEditor criterion={criterion} catalog={{ Stock_Splits: ['split_date'] }} tagNames={[]} index={0} onChange={setCriterion} onRemove={() => undefined} />
}

describe('CriterionEditor decimal values', () => {
  it('preserves decimal edit states while typing', () => {
    render(<DecimalHarness />)
    const input = screen.getByRole('textbox', { name: 'Expression value' })
    fireEvent.change(input, { target: { value: '0.' } })
    expect(input).toHaveValue('0.')
    fireEvent.change(input, { target: { value: '0.05' } })
    expect(input).toHaveValue('0.05')
  })
  it('adds explicit opening and closing parenthesis tokens', () => {
    const view = render(<DecimalHarness />)
    const scoped = within(view.container)
    const add = scoped.getByRole('combobox', { name: 'Add right expression token' })
    fireEvent.change(add, { target: { value: 'lparen' } })
    fireEvent.change(add, { target: { value: 'rparen' } })
    expect(scoped.getByLabelText('Open parenthesis')).toHaveTextContent('(')
    expect(scoped.getByLabelText('Close parenthesis')).toHaveTextContent(')')
  })
  it('renders a date picker for the recent-split exclusion', () => {
    const view = render(<RecentSplitHarness />)
    const scoped = within(view.container)

    expect(scoped.getByRole('combobox', { name: 'Rule type' })).toHaveValue('recent_split')
    expect(scoped.getByLabelText('Recent split cutoff date')).toHaveAttribute('type', 'date')
  })
  it('supports split action, confirmation status, and date direction', () => {
    const view = render(<RecentSplitHarness />)
    const scoped = within(view.container)
    const action = scoped.getByRole('combobox', { name: 'Split match action' })
    const status = scoped.getByRole('combobox', { name: 'Split confirmation status' })
    const dateComparison = scoped.getByRole('combobox', { name: 'Split date comparison' })

    fireEvent.change(action, { target: { value: 'include' } })
    fireEvent.change(status, { target: { value: 'pending' } })
    fireEvent.change(dateComparison, { target: { value: 'on_or_before' } })

    expect(action).toHaveValue('include')
    expect(status).toHaveValue('pending')
    expect(dateComparison).toHaveValue('on_or_before')
  })
  it('renders date inputs for Stock_Splits date fields', () => {
    const view = render(<SplitDateHarness />)
    const scoped = within(view.container)

    expect(scoped.getByLabelText('Start date')).toHaveAttribute('type', 'date')
    expect(scoped.getByLabelText('End date')).toHaveAttribute('type', 'date')
  })
  it('offers a date token for Stock_Splits date expressions', () => {
    const view = render(<SplitExpressionHarness />)
    const scoped = within(view.container)
    const add = scoped.getByRole('combobox', { name: 'Add right expression token' })

    expect(scoped.getByRole('option', { name: 'Date' })).toBeInTheDocument()
    fireEvent.change(add, { target: { value: 'value' } })

    expect(scoped.getByLabelText('Expression date value')).toHaveAttribute('type', 'date')
  })
})
