import { describe, expect, it } from 'vitest'

import { buildPortfolioSummary, performanceStart, sliceValueHistory, transactionCashEffect } from './portfolioFormat'
import type { Holding, ValueHistory } from './portfolioTypes'

describe('portfolio formatting helpers', () => {
  it('summarizes open positions, cash, allocation, cost, and profit', () => {
    const holdings: Holding[] = [
      { symbol: 'CASH EUR', asset_category: 'CASH', market_value: 100, is_open: true },
      { symbol: 'AAA', asset_category: 'STK', market_value: 900, is_open: true, performance: { cost_basis_display: 600, pnl_display: 300 } },
      { symbol: 'BBB', asset_category: 'STK', market_value: 1_000, is_open: true, performance: { cost_basis_display: 700, pnl_display: 300 } },
      { symbol: 'CLOSED', asset_category: 'STK', market_value: 500, is_open: false, performance: { cost_basis_display: 400, pnl_display: 100 } },
    ]

    const summary = buildPortfolioSummary(holdings, {
      labels: ['BBB', 'AAA'],
      values: [1_000, 900],
      total: 1_900,
      currency: 'EUR',
    })

    expect(summary).toMatchObject({
      totalValue: 2_000,
      investedValue: 1_900,
      cashValue: 100,
      cashWeight: 0.05,
      costBasis: 1_300,
      pnl: 600,
      positionCount: 2,
    })
    expect(summary.topHolding).toEqual({ symbol: 'BBB', value: 1_000, weight: 1_000 / 1_900 })
  })

  it('maps period choices to deterministic start dates', () => {
    expect(performanceStart('all', '2026-07-31')).toBeUndefined()
    expect(performanceStart('ytd', '2026-07-31')).toBe('2026-01-01')
    expect(performanceStart('1y', '2026-07-31')).toBe('2025-07-31')
    expect(performanceStart('5y', '2026-07-31')).toBe('2021-07-31')
  })

  it('keeps every historical series aligned when applying a range', () => {
    const history: ValueHistory = {
      dates: ['2024-01-01', '2024-01-02', '2024-01-03'],
      holdings: { AAA: [10, 11, 12], BBB: [20, 21, 22] },
      portfolio_values: [30, 32, 34],
      net_inflows: [30, 0, 0],
      daily_returns: [0, 0.01, 0.02],
      cumulative_returns: [0, 0.01, 0.0302],
    }

    expect(sliceValueHistory(history, '2024-01-02')).toEqual({
      ...history,
      dates: ['2024-01-02', '2024-01-03'],
      holdings: { AAA: [11, 12], BBB: [21, 22] },
      portfolio_values: [32, 34],
      net_inflows: [0, 0],
      daily_returns: [0.01, 0.02],
      cumulative_returns: [0.01, 0.0302],
    })
  })

  it('uses net cash for trades and reported amount for income events', () => {
    expect(transactionCashEffect({ activity_type: 'TRADE', amount: 0, trade_money: 290_000, proceeds: -290_000, net_cash: -290_161 })).toBe(-290_161)
    expect(transactionCashEffect({ activity_type: 'DIVIDEND', amount: 217.3, net_cash: 0 })).toBe(217.3)
  })
})
