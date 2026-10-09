import { describe, expect, it } from 'vitest'

import { describeSplit, splitName } from './shareBasis'

describe('share splits', () => {
  it('names splits and consolidations by their whole-number ratio', () => {
    expect(splitName(5)).toBe('5-for-1 split')
    expect(splitName(1.5)).toBe('3-for-2 split')
    expect(splitName(0.1)).toBe('10-to-1 consolidation')
    expect(splitName(0.5)).toBe('2-to-1 consolidation')
  })

  it('dates a recorded split by its ex-date and a share-count split by its fiscal year', () => {
    expect(describeSplit({ date: '2022-03-30', after: null, multiplier: 2, source: 'split record' })).toBe('2-for-1 split, ex-date 2022-03-30')
    expect(describeSplit({ date: '2022-03-31', after: '2021-03-31', multiplier: 5, source: 'share counts' })).toBe('5-for-1 split in the year to 2022-03 (from share counts)')
    expect(describeSplit({ date: '2025-06-26', after: '2025-03-31', multiplier: 2, source: 'filing' })).toBe('2-for-1 split after the 2025-03 year end, by the report of 2025-06-26')
  })
})
