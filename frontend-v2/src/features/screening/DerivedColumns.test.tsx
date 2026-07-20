import { fireEvent, render, screen } from '@testing-library/react'
import { useState } from 'react'
import { describe, expect, it } from 'vitest'

import { DerivedColumns } from './ScreeningWorkspaceDense'
import type { ComputedColumn } from './types'

function Harness() {
  const [columns, setColumns] = useState<ComputedColumn[]>([{
    name: 'Derived metric',
    formula_type: 'expression',
    expression_tokens: [{ type: 'value', value: 1 }],
  }])
  return <DerivedColumns value={columns} catalog={{ Stock_Prices: ['Price'] }} onChange={setColumns} />
}

describe('DerivedColumns', () => {
  it('keeps the name input focused while typing a multi-character name', () => {
    render(<Harness />)

    const input = screen.getByRole('textbox', { name: 'Derived column name' })
    input.focus()
    fireEvent.change(input, { target: { value: 'P' } })

    expect(document.activeElement).toBe(input)
    expect(input).toHaveValue('P')
  })
})