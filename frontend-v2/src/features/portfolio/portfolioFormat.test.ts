import { describe, expect, it } from 'vitest'

import { buildPortfolioSummary, holdingAnalysisHref, performanceStart, priceNote, signedPercent, transactionCashEffect } from './portfolioFormat'
import type { Holding } from './portfolioTypes'

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
    // Year to date runs from the previous year's last close.
    expect(performanceStart('ytd', '2026-07-31')).toBe('2025-12-31')
    expect(performanceStart('1y', '2026-07-31')).toBe('2025-07-31')
    expect(performanceStart('5y', '2026-07-31')).toBe('2021-07-31')
  })

  it('signs changes in words and symbols, not colour alone', () => {
    expect(signedPercent(0.1234)).toBe('+12.3%')
    expect(signedPercent(-0.05)).toBe('−5.0%')
    expect(signedPercent(0)).toBe('0.0%')
    expect(signedPercent(null)).toBe('—')
  })

  it('opens Tokyo holdings by EDINET code and others by ticker, marked as coming from the portfolio', () => {
    expect(holdingAnalysisHref({ symbol: '5984.T', performance: { edinet_code: 'E01437' } })).toBe('/analyze/E01437?from=portfolio')
    expect(holdingAnalysisHref({ symbol: 'AFL' })).toBe('/analyze?ticker=AFL&from=portfolio')
  })

  it('flags quotes valued at cost, stale quotes, and quotes converted from another currency', () => {
    expect(priceNote({ symbol: 'X', price_source: 'cost' })?.level).toBe('error')
    expect(priceNote({ symbol: 'X', price_source: 'market', price_date: '2026-05-21', valuation_date: '2026-10-02' })?.level).toBe('warning')
    expect(priceNote({ symbol: 'CSPX', currency: 'EUR', price_currency: 'USD', price_date: '2026-10-02', valuation_date: '2026-10-02' })).toEqual({ level: 'info', text: 'Quoted in USD and converted to EUR at ECB rates' })
    expect(priceNote({ symbol: 'AFL', currency: 'USD', price_currency: 'USD', price_date: '2026-10-01', valuation_date: '2026-10-02' })).toBeNull()
  })

  it('uses net cash for trades and reported amount for income events', () => {
    expect(transactionCashEffect({ activity_type: 'TRADE', amount: 0, trade_money: 290_000, proceeds: -290_000, net_cash: -290_161 })).toBe(-290_161)
    expect(transactionCashEffect({ activity_type: 'DIVIDEND', amount: 217.3, net_cash: 0 })).toBe(217.3)
  })
})
