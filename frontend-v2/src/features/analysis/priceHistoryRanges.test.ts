import { describe, expect, it } from 'vitest'

import { filterPriceHistory, priceTicks } from './priceHistoryRanges'

const rows = [
  { trade_date: '2024-12-31', price: 100 },
  { Date: '2025-01-01', Price: 110 },
  { Date: '2025-06-30', Price: 120 },
  { Date: '2025-12-31', Price: 130 },
]

describe('price history ranges', () => {
  it('calculates YTD from the latest available observation', () => {
    expect(filterPriceHistory(rows, 'ytd')).toEqual(rows.slice(1))
  })

  it('supports rolling month and year windows', () => {
    expect(filterPriceHistory(rows, '6m')).toEqual(rows.slice(2))
    expect(filterPriceHistory(rows, '1y')).toEqual(rows)
  })

  it('keeps the full dataset for All', () => {
    expect(filterPriceHistory(rows, 'all')).toBe(rows)
  })
})

describe('price axis ticks', () => {
  const tradingDays = (start: string, count: number, stepDays = 1) => Array.from({ length: count }, (_, index) => new Date(Date.parse(`${start}T00:00:00Z`) + index * stepDays * 86_400_000).toISOString().slice(0, 10))

  it('labels each year once on long ranges', () => {
    const labels = tradingDays('2021-06-01', 5 * 52, 7)
    const ticks = [...priceTicks(labels).values()]
    expect(ticks).toEqual(['2022', '2023', '2024', '2025', '2026'])
  })

  it('labels month boundaries on ranges of a few years', () => {
    const ticks = [...priceTicks(tradingDays('2025-01-15', 365)).values()]
    expect(ticks.length).toBeLessThanOrEqual(8)
    expect(new Set(ticks).size).toBe(ticks.length)
    expect(ticks[0]).toBe('Mar 25')
  })

  it('spaces day labels evenly on short ranges', () => {
    expect(priceTicks(tradingDays('2026-09-01', 21)).size).toBeLessThanOrEqual(7)
    expect(priceTicks(['2026-09-01']).size).toBe(0)
  })
})
