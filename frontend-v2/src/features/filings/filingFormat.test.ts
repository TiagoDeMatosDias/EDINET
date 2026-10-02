import { describe, expect, it } from 'vitest'

import { companyName, filingHref, formatBytes, formCategory, formLabel, periodSpan } from './filingFormat'

describe('filing formatting', () => {
  it('names known EDINET forms and shows other codes as they are', () => {
    expect(formLabel('030000')).toBe('Annual securities report')
    expect(formLabel('07A000')).toBe('Annual report · investment trust')
    expect(formLabel('999999')).toBe('Form 999999')
    expect(formLabel(null)).toBe('XBRL report')
  })

  it('groups forms into the report families the explorer filters by', () => {
    expect(formCategory('030000')).toBe('annual')
    expect(formCategory('043A00')).toBe('interim')
    expect(formCategory('07A000')).toBe('funds')
    expect(formCategory('999999')).toBe('other')
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

  it('prefers the English company name and keeps the return path', () => {
    expect(companyName({ company_name: 'TOYOTA MOTOR CORPORATION', submitter_name: 'トヨタ自動車株式会社' })).toBe('TOYOTA MOTOR CORPORATION')
    expect(companyName({ company_name: '', submitter_name: '株式会社ＴＯブックス' })).toBe('株式会社ＴＯブックス')
    expect(filingHref('S100Y8NY', 'E02144', 'analysis')).toBe('/filings/S100Y8NY?from=analysis&company=E02144')
    expect(filingHref('S100Y8NY')).toBe('/filings/S100Y8NY')
  })
})
