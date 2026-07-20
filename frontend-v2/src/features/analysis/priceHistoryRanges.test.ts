import { describe, expect, it } from 'vitest'

import { filterPriceHistory } from './priceHistoryRanges'

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
