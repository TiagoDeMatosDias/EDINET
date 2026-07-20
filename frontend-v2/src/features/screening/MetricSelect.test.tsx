import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import { MetricSelect } from './MetricSelect'

describe('MetricSelect', () => {
  it('selects a table before exposing its columns', () => {
    const change = vi.fn()
    render(<MetricSelect catalog={{ ShareMetrics: ['Net assets per share', 'Book value'], Stock_Prices: ['Price'] }} table="" column="" label="Rule metric" onChange={change} />)

    expect(screen.getByRole('combobox', { name: 'Rule metric column' })).toBeDisabled()
    fireEvent.change(screen.getByRole('combobox', { name: 'Rule metric table' }), { target: { value: 'ShareMetrics' } })

    expect(change).toHaveBeenCalledWith('ShareMetrics', 'Net assets per share')
  })

  it('lists only columns from the selected table', () => {
    render(<MetricSelect catalog={{ ShareMetrics: ['Net assets per share'], Stock_Prices: ['Price'] }} table="Stock_Prices" column="Price" label="Expression metric" onChange={() => undefined} />)

    const options = screen.getByRole('combobox', { name: 'Expression metric column' }).querySelectorAll('option')
    expect([...options].map(option => option.textContent)).toEqual(['Column…', 'Price'])
  })
})
