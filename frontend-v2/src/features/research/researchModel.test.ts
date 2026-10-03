import { describe, expect, it } from 'vitest'

import { alertCondition, alertDistance, bookCsv, filterBook, formatPercent, formatSignedPercent, noteTitle, notesMarkdown, relativeDay, reviewState, sortBook, upside } from './researchModel'
import type { BookCompany } from './researchTypes'

function company(code: string, patch: Partial<BookCompany> = {}): BookCompany {
  return { company_code: code, company_name: `Company ${code}`, ticker: `${code}0`, tags: [], note_count: 0, alert_count: 0, alerts_triggered: 0, price_currency: 'JPY', ...patch }
}

describe('research model', () => {
  it('measures the target against the price only in one currency', () => {
    expect(upside({ target_value: 3300, target_currency: 'JPY', LatestPrice: 3000, price_currency: 'JPY' })).toBeCloseTo(0.1, 9)
    expect(upside({ target_value: 30, target_currency: 'USD', LatestPrice: 3000, price_currency: 'JPY' })).toBeNull()
    expect(upside({ target_value: 3300, target_currency: null, LatestPrice: null, price_currency: 'JPY' })).toBeNull()
  })

  it('says when a review is due', () => {
    expect(reviewState('2026-10-01', '2026-10-03')).toBe('overdue')
    expect(reviewState('2026-10-10', '2026-10-03')).toBe('due')
    expect(reviewState('2026-12-01', '2026-10-03')).toBe('later')
    expect(reviewState(null, '2026-10-03')).toBeNull()
    expect(relativeDay('2026-10-01T10:00:00+00:00', '2026-10-03')).toBe('2 days ago')
    expect(relativeDay('2026-10-04', '2026-10-03')).toBe('tomorrow')
    expect(relativeDay('2026-08-01', '2026-10-03')).toBe('1 Aug')
  })

  it('filters by tag, status, review, alerts, and text', () => {
    const book = [
      company('E1', { tags: ['Autos'], thesis_status: 'buy', thesis: 'Hybrid lead', review_on: '2026-10-01' }),
      company('E2', { tags: ['Autos', 'Yield'], thesis_status: 'sell', alerts_triggered: 1 }),
      company('E3', { company_name: 'Sony Group', thesis_status: 'watch' }),
    ]
    const codes = (filter: Partial<{ tag: string; status: string; query: string }>) => filterBook(book, { tag: '', status: '', query: '', ...filter }, '2026-10-03').map(item => item.company_code)
    expect(codes({ tag: 'Autos' })).toEqual(['E1', 'E2'])
    expect(codes({ status: 'sell' })).toEqual(['E2'])
    expect(codes({ status: 'review' })).toEqual(['E1'])
    expect(codes({ status: 'alerts' })).toEqual(['E2'])
    expect(codes({ query: 'hybrid' })).toEqual(['E1'])
    expect(codes({ query: 'yield' })).toEqual(['E2'])
    expect(codes({ query: 'sony' })).toEqual(['E3'])
  })

  it('sorts with missing values last either way', () => {
    const book = [company('E1', { LatestPrice: 100 }), company('E2'), company('E3', { LatestPrice: 300 })]
    expect(sortBook(book, 'price', true).map(item => item.company_code)).toEqual(['E3', 'E1', 'E2'])
    expect(sortBook(book, 'price', false).map(item => item.company_code)).toEqual(['E1', 'E3', 'E2'])
    const statuses = [company('E1', { thesis_status: 'watch' }), company('E2', { thesis_status: 'buy' }), company('E3')]
    expect(sortBook(statuses, 'status', false).map(item => item.company_code)).toEqual(['E2', 'E1', 'E3'])
  })

  it('titles a note from its first line when it has no title', () => {
    expect(noteTitle('', '# Q1 margins\nHeld up.')).toBe('Q1 margins')
    expect(noteTitle(' Explicit ', 'Body')).toBe('Explicit')
    expect(noteTitle('', 'x'.repeat(100))).toHaveLength(80)
  })

  it('describes alerts and how far they are from triggering', () => {
    const definitions = { PERatio: { label: 'P/E', group: 'Valuation' }, DividendsYield: { label: 'Dividend yield', group: 'Income', format: 'percent' as const } }
    expect(alertCondition({ metric: 'PERatio', operator: '<=', value: 12 }, definitions)).toBe('P/E ≤ 12')
    expect(alertCondition({ metric: 'DividendsYield', operator: '>', value: 0.04 }, definitions)).toBe('Dividend yield > 4.0%')
    expect(alertDistance({ current_value: 11, value: 10 })).toBeCloseTo(0.1, 9)
    expect(alertDistance({ current_value: null, value: 10 })).toBeNull()
  })

  it('exports the book and notes', () => {
    const csv = bookCsv([company('E1', { tags: ['A', 'B'], thesis: 'Says "cheap", for now', thesis_status: 'buy', target_value: 110, target_currency: 'JPY', LatestPrice: 100 })])
    expect(csv.split('\n')[0]).toContain('Upside')
    expect(csv.split('\n')[1]).toMatch(/^Company E1,E10,E1,,A; B,Buy,"Says ""cheap"", for now",100,JPY,110,JPY,0\.1\d*,,0,0,0,,,$/)
    const markdown = notesMarkdown([{ note_id: 'n', title: 'Margins', body: 'Held up.', edinet_code: 'E1', updated_at: '2026-10-02T00:00:00Z' }], new Map([['E1', 'Alpha']]))
    expect(markdown).toBe('## Margins\nAlpha · 2 Oct 2026\n\nHeld up.')
  })

  it('never shows a negative zero', () => {
    expect(formatPercent(-0.0001, 0)).toBe('0%')
    expect(formatPercent(-0.02, 0)).toBe('-2%')
    expect(formatSignedPercent(0.12)).toBe('+12.0%')
    expect(formatSignedPercent(-0.00001)).toBe('0.0%')
  })
})
