import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { IncomeCompanyPicker } from './IncomeCompanyPicker'
import type { IncomeCompany } from './portfolioTypes'

afterEach(cleanup)

function company(symbol: string, name: string, net: number, isHeld = true): IncomeCompany {
  return {
    symbol, name, currency: 'USD', is_held: isHeld, shares_held: 1, gross: net, tax: 0, net, withholding_rate: 0, share_of_income: null, payments: 1,
    first_date: '2024-01-01', last_date: '2025-01-01', frequency: 4, ttm_net: 0, ttm_per_share: null, latest_per_share: null, latest_date: null,
    per_share_growth_1y: null, current_yield: null, yield_on_cost: null, annual: [],
  }
}

const COMPANIES = [company('MO', 'Altria', 300), company('AFL', 'Aflac', 200), company('O', 'Realty Income', 100, false)]

describe('IncomeCompanyPicker', () => {
  it('adds companies by typing and Enter, and removes the last with Backspace', () => {
    const onChange = vi.fn()
    const { rerender } = render(<IncomeCompanyPicker companies={COMPANIES} selected={[]} currency="EUR" onChange={onChange} />)
    const input = screen.getByRole('combobox', { name: 'Filter companies' })

    fireEvent.focus(input)
    fireEvent.change(input, { target: { value: 'afl' } })
    expect(screen.getAllByRole('option')).toHaveLength(1)
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(onChange).toHaveBeenLastCalledWith(['AFL'])

    rerender(<IncomeCompanyPicker companies={COMPANIES} selected={['AFL', 'MO']} currency="EUR" onChange={onChange} />)
    fireEvent.keyDown(input, { key: 'Backspace' })
    expect(onChange).toHaveBeenLastCalledWith(['AFL'])
    fireEvent.click(screen.getByRole('button', { name: 'Remove AFL' }))
    expect(onChange).toHaveBeenLastCalledWith(['MO'])
  })

  it('moves through the list with the arrow keys and offers quick selections', () => {
    const onChange = vi.fn()
    render(<IncomeCompanyPicker companies={COMPANIES} selected={['O']} currency="EUR" onChange={onChange} />)
    const input = screen.getByRole('combobox', { name: 'Filter companies' })
    fireEvent.keyDown(input, { key: 'ArrowDown' })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(onChange).toHaveBeenLastCalledWith(['O', 'AFL'])
    expect(screen.getByRole('option', { name: /Realty Income · sold/ })).toHaveAttribute('aria-checked', 'true')

    fireEvent.click(screen.getByRole('button', { name: 'Held now' }))
    expect(onChange).toHaveBeenLastCalledWith(['MO', 'AFL'])
    fireEvent.click(screen.getByRole('button', { name: 'All payers' }))
    expect(onChange).toHaveBeenLastCalledWith([])
  })
})
