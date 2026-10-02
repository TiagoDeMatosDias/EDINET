import { describe, expect, it } from 'vitest'

import { formatBytes, formLabel, periodSpan } from './filingFormat'

describe('filing formatting', () => {
  it('names known EDINET forms and shows other codes as they are', () => {
    expect(formLabel('030000')).toBe('Annual securities report')
    expect(formLabel('043000')).toBe('Form 043000')
    expect(formLabel(null)).toBe('XBRL report')
  })

  it('sizes archives in readable units', () => {
    expect(formatBytes(2_410_352)).toBe('2.3 MB')
    expect(formatBytes(947 * 1024)).toBe('947 KB')
    expect(formatBytes(null)).toBe('')
  })

  it('describes the fiscal period a report covers', () => {
    expect(periodSpan('2025-04-01', '2026-03-31')).toBe('Apr 2025 – Mar 2026 · 12 months')
    expect(periodSpan(null, '2026-03-31')).toBe('')
  })
})
