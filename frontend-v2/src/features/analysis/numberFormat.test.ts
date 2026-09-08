import { describe, expect, it } from 'vitest'

import { formatGranularNumber } from './numberFormat'

describe('formatGranularNumber', () => {
  it('keeps the full stored precision instead of rounding to an integer', () => {
    expect(formatGranularNumber(0.1656)).toBe('0.1656')
    expect(formatGranularNumber(10_809_697_864.87)).toBe('10,809,697,864.87')
  })

  it('groups the integer part and keeps whole numbers unchanged', () => {
    expect(formatGranularNumber(72_000_000)).toBe('72,000,000')
    expect(formatGranularNumber(18.4)).toBe('18.4')
    expect(formatGranularNumber(1234)).toBe('1,234')
    expect(formatGranularNumber(-1_234_567.891)).toBe('-1,234,567.891')
  })

  it('leaves exponential magnitudes readable without fake precision', () => {
    expect(formatGranularNumber(1e21)).toBe('1e+21')
    expect(formatGranularNumber(1e-7)).toBe('1e-7')
  })
})
