import { cleanup, render, screen, within } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { nativeMoney } from './portfolioFormat'
import { PortfolioHoldings } from './PortfolioHoldings'
import type { Holding } from './portfolioTypes'

afterEach(cleanup)

const HOLDINGS: Holding[] = [
  { symbol: 'MO', asset_category: 'STK', currency: 'USD', quantity: 205, avg_cost: 44.35, market_value_display: 12_300, is_open: true, performance: { name: 'Altria', held_since: '2021-06-24', held_until: '2026-10-02' } },
  { symbol: 'CASH JPY', asset_category: 'CASH', currency: 'JPY', quantity: 56_102.82, market_value_native: 56_102.82, market_value_display: 317, is_open: true },
  { symbol: 'CASH USD', asset_category: 'CASH', currency: 'USD', quantity: 646.15, market_value_native: 646.15, market_value_display: 576, is_open: true },
]

function row(symbol: string) {
  return screen.getByRole('rowheader', { name: new RegExp(symbol) }).closest('tr')!
}

describe('PortfolioHoldings', () => {
  it('shows shares for holdings and each cash balance in its own currency', () => {
    render(<PortfolioHoldings
      data={HOLDINGS}
      summary={{ totalValue: 13_193, investedValue: 12_300, cashValue: 893, cashWeight: 0.07, costBasis: 9_000, pnl: 3_300, positionCount: 1 }}
      currency="EUR"
      includeClosed={false}
      isLoading={false}
      onIncludeClosed={vi.fn()}
      onOpenDetail={vi.fn()}
      onAnalyze={vi.fn()}
    />)

    expect(within(row('MO')).getByText('205')).toBeInTheDocument()
    expect(within(row('CASH JPY')).getByText(nativeMoney(56_102.82, 'JPY'))).toBeInTheDocument()
    expect(within(row('CASH USD')).getByText(nativeMoney(646.15, 'USD'))).toBeInTheDocument()
    // Yen has no minor unit; dollars keep their cents.
    expect(nativeMoney(56_102.82, 'JPY')).not.toContain('.82')
    expect(nativeMoney(646.15, 'USD')).toContain('646.15')
  })
})
