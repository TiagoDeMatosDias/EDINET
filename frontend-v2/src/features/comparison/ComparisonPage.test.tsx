import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import { MetricPicker } from './ComparisonPage'

describe('Comparison metric picker', () => {
  it('adds a table column and removes selected metrics', async () => {
    const onChange = vi.fn()
    render(
      <MetricPicker
        catalog={{ IncomeStatement: ['Gross profit', 'Operating income'], BalanceSheet: ['Assets'] }}
        isLoading={false}
        selected={['Revenue']}
        onChange={onChange}
      />,
    )

    fireEvent.click(screen.getByRole('button', { name: 'Add metric' }))
    fireEvent.change(screen.getByRole('combobox', { name: 'Metric table' }), { target: { value: 'IncomeStatement' } })
    await waitFor(() => expect(screen.getByRole('option', { name: 'Gross profit' })).toBeInTheDocument())
    fireEvent.change(screen.getByRole('combobox', { name: 'Metric column' }), { target: { value: 'Operating income' } })
    fireEvent.click(screen.getByRole('button', { name: 'Add selected metric' }))

    expect(onChange).toHaveBeenCalledWith(['Revenue', 'IncomeStatement.Operating income'])

    fireEvent.click(screen.getByRole('button', { name: 'Remove metric Revenue' }))
    expect(onChange).toHaveBeenCalledWith([])
  })
})
