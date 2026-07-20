import { describe, expect, it } from 'vitest'

import { yahooFinanceSymbol } from './AnalysisWorkspaceUnified'

describe('yahooFinanceSymbol', () => {
  it('converts the stored five-digit Japanese ticker to Yahoo format', () => {
    expect(yahooFinanceSymbol('75750')).toBe('7575.T')
  })

  it('keeps existing four-digit Japanese tickers', () => {
    expect(yahooFinanceSymbol('7203')).toBe('7203.T')
  })
})
