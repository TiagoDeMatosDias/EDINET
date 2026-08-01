import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { PortfolioActivity } from './PortfolioActivity'
import type { Transaction } from './portfolioTypes'

afterEach(cleanup)

function transactions(count: number): Transaction[] {
  return Array.from({ length: count }, (_, index) => ({
    id: index,
    trade_date: `2026-07-${String(31 - (index % 28)).padStart(2, '0')}`,
    activity_type: index % 2 ? 'TRADE' : 'DIVIDEND',
    symbol: `SYM${index}`,
    description: `Record ${index}`,
    amount: index + 1,
    currency: 'EUR',
    source_file: 'shade.xml',
  }))
}

describe('PortfolioActivity', () => {
  it('paginates a large activity ledger and keeps every record reachable', () => {
    render(<PortfolioActivity
      data={transactions(55)}
      activity={{ TRADE: 27, DIVIDEND: 28 }}
      dateRange={{ min_date: '2020-12-14', max_date: '2026-07-31' }}
      isLoading={false}
      onOpenDetail={vi.fn()}
    />)

    expect(screen.getByText('1–50 of 55')).toBeInTheDocument()
    expect(screen.getByText('Record 49')).toBeInTheDocument()
    expect(screen.queryByText('Record 54')).not.toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Next page' }))

    expect(screen.getByText('51–55 of 55')).toBeInTheDocument()
    expect(screen.getByText('Record 54')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Next page' })).toBeDisabled()
  })

  it('opens an individual transaction from the filtered ledger', () => {
    const onOpenDetail = vi.fn()
    render(<PortfolioActivity data={transactions(3)} activity={{ TRADE: 1, DIVIDEND: 2 }} isLoading={false} onOpenDetail={onOpenDetail} />)

    fireEvent.change(screen.getByRole('textbox', { name: 'Search activity' }), { target: { value: 'SYM2' } })
    fireEvent.click(screen.getByRole('button', { name: 'View transaction from 2026-07-29' }))

    expect(onOpenDetail).toHaveBeenCalledWith(expect.objectContaining({ kind: 'transaction', transaction: expect.objectContaining({ symbol: 'SYM2' }) }))
  })
})
