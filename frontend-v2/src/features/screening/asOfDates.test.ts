import { describe, expect, it } from 'vitest'

import { formatAsOf, parseAsOf, presetDates } from './asOfDates'

const NOW = new Date(2026, 9, 2)

describe('as-of dates', () => {
  it('reads full dates in several spellings', () => {
    expect(parseAsOf('2023-06-30', NOW)).toEqual({ value: '2023-06-30' })
    expect(parseAsOf('2023/6/30', NOW)).toEqual({ value: '2023-06-30' })
    expect(parseAsOf('20230630', NOW)).toEqual({ value: '2023-06-30' })
  })

  it('reads a month or a year as its last day, and the current one as today', () => {
    expect(parseAsOf('2023-06', NOW)).toEqual({ value: '2023-06-30' })
    expect(parseAsOf('2024-02', NOW)).toEqual({ value: '2024-02-29' })
    expect(parseAsOf('2023', NOW)).toEqual({ value: '2023-12-31' })
    expect(parseAsOf('2026', NOW)).toEqual({ value: '2026-10-02' })
    expect(parseAsOf('2026-10', NOW)).toEqual({ value: '2026-10-02' })
  })

  it('reads distances back from today', () => {
    expect(parseAsOf('18m', NOW)).toEqual({ value: '2025-04-02' })
    expect(parseAsOf('5y', NOW)).toEqual({ value: '2021-10-02' })
    expect(parseAsOf('2 years ago', NOW)).toEqual({ value: '2024-10-02' })
    expect(parseAsOf('-2w', NOW)).toEqual({ value: '2026-09-18' })
    expect(parseAsOf('1m', new Date(2026, 2, 31))).toEqual({ value: '2026-02-28' })
  })

  it('clears for latest and explains what it cannot use', () => {
    expect(parseAsOf('latest', NOW)).toEqual({ value: '' })
    expect(parseAsOf('  ', NOW)).toBeNull()
    expect(parseAsOf('2023-02-30', NOW)).toEqual({ error: '2023-02-30 is not a calendar date.' })
    expect(parseAsOf('2027-01-01', NOW)).toEqual({ error: '1 Jan 2027 is after today.' })
    expect(parseAsOf('soon', NOW)).toMatchObject({ error: expect.stringContaining('Try 2023-06-30') })
  })

  it('offers latest, round distances, and recent year ends', () => {
    const presets = presetDates(NOW)
    expect(presets[0]).toMatchObject({ value: '', label: 'Latest data' })
    expect(presets.find(preset => preset.label === '1 year ago')?.value).toBe('2025-10-02')
    expect(presets.find(preset => preset.label === 'End of 2025')?.value).toBe('2025-12-31')
    expect(formatAsOf('2025-12-31')).toBe('31 Dec 2025')
    expect(formatAsOf('')).toBe('Latest data')
  })
})
