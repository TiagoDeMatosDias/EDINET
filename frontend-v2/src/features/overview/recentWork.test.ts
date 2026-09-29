import { describe, expect, it } from 'vitest'

import { recentWorkTitle } from './recentWork'

const details = (backtestId: string) => JSON.stringify({ backtest_id: backtestId })

describe('recent work titles', () => {
  it('drops the stored run id from legacy backtest titles', () => {
    expect(recentWorkTitle({ title: 'Backtest · 20260929_180717_019df714', details_json: details('20260929_180717_019df714') })).toBe('Backtest')
    expect(recentWorkTitle({ title: 'Rolling screen backtest · run-7', details_json: details('run-7') })).toBe('Rolling screen backtest')
  })

  it('keeps descriptive titles and items without a run id', () => {
    expect(recentWorkTitle({ title: 'Backtest · 7203, 6758', details_json: details('20260929_180717') })).toBe('Backtest · 7203, 6758')
    expect(recentWorkTitle({ title: 'Alpha · 20260929_180717', details_json: '{"company_code":"E1"}' })).toBe('Alpha · 20260929_180717')
    expect(recentWorkTitle({ title: 'Screen · 3 matches', details_json: 'not json' })).toBe('Screen · 3 matches')
  })
})
