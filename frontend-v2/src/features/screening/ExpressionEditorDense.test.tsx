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
  return <CriterionEditor criterion={criterion} catalog={{ Stock_Prices: ['Price'] }} index={0} onChange={setCriterion} onRemove={() => undefined} />
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
})
