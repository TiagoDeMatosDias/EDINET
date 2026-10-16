import { describe, expect, it } from 'vitest'

import { addedSummary, draftProblem, emptyDraft, manualRequest, optionContracts, optionSymbol, parseOptionSymbol, tradeCash, type Draft } from './manualModel'
import type { Transaction } from './portfolioTypes'
import { selectedRecords } from './recordsDelete'

const draft = (patch: Partial<Draft>): Draft => ({ ...emptyDraft('USD'), day: '2024-03-01', ...patch })

const records: Transaction[] = [
  { id: 1, trade_date: '2024-01-03', activity_type: 'TRADE', asset_category: 'OPT', symbol: 'AAA   240315P00100000', quantity: -1, currency: 'USD', put_call: 'P', strike: 100, expiry: '2024-03-15', underlying_symbol: 'AAA', multiplier: 100 },
  { id: 2, trade_date: '2023-06-01', activity_type: 'TRADE', asset_category: 'OPT', symbol: 'AAA   230915C00090000', quantity: 2, currency: 'USD' },
  { id: 3, trade_date: '2023-09-15', activity_type: 'TRADE', asset_category: 'OPT', symbol: 'AAA   230915C00090000', quantity: -2, currency: 'USD' },
  { id: 4, trade_date: '2023-01-02', activity_type: 'TRADE', asset_category: 'STK', symbol: 'AAA', quantity: 10, currency: 'USD' },
]

describe('manual entries', () => {
  it('writes and reads option symbols as the broker does', () => {
    expect(optionSymbol('afl', '2021-02-19', 'P', 40)).toBe('AFL   210219P00040000')
    expect(parseOptionSymbol('AFL   210219P00040000')).toEqual({ underlying: 'AFL', expiry: '2021-02-19', putCall: 'P', strike: 40 })
    expect(parseOptionSymbol('AAPL')).toBeNull()
  })

  it('lists the account’s contracts, those held first, with their side', () => {
    const contracts = optionContracts(records)
    expect(contracts.map(item => [item.symbol, item.net])).toEqual([['AAA   240315P00100000', -1], ['AAA   230915C00090000', 0]])
    // Terms come from the records, else from the symbol.
    expect(contracts[1]).toMatchObject({ underlying: 'AAA', putCall: 'C', strike: 90, expiry: '2023-09-15', multiplier: 100 })
  })

  it('sends only the fields each kind uses', () => {
    expect(manualRequest(draft({ kind: 'sell_short', category: 'OPT', underlying: 'aaa', putCall: 'P', strike: '100', expiry: '2024-06-21', quantity: '1', price: '2.5' }))).toEqual({
      path: '/api/portfolio/transactions/manual',
      body: { kind: 'sell_short', trade_date: '2024-03-01', currency: 'USD', description: '', asset_category: 'OPT', symbol: 'AAA   240621P00100000', underlying: 'AAA', put_call: 'P', strike: 100, expiry: '2024-06-21', multiplier: 100, quantity: 1, price: 2.5, commission: 0, amount: 0 },
    })
    expect(manualRequest(draft({ kind: 'option_exercise', contract: 'AAA   240315P00100000', quantity: '1', settlement: 'cash', settlementPrice: '90' })).body).toEqual({
      kind: 'option_exercise', trade_date: '2024-03-01', currency: 'USD', description: '', symbol: 'AAA   240315P00100000', quantity: 1, commission: 0, position: '', settlement: 'cash', settlement_price: 90,
    })
    expect(manualRequest(draft({ kind: 'split', symbol: 'AAA', ratioFrom: '1', ratioTo: '3' })).body).toMatchObject({ kind: 'split', symbol: 'AAA', ratio_from: 1, ratio_to: 3 })
    expect(manualRequest(draft({ kind: 'spinoff', symbol: 'AAA', newSymbol: 'newco', quantity: '3' })).body).toEqual({ kind: 'spinoff', trade_date: '2024-03-01', currency: 'USD', description: '', symbol: 'AAA', new_symbol: 'newco', quantity: 3 })
    expect(manualRequest(draft({ kind: 'dividend', symbol: 'AAA', amount: '30', tax: '4.5' })).body).toMatchObject({ amount: 30, withholding_tax: 4.5 })
    expect(manualRequest(draft({ kind: 'dividend', symbol: 'AAA', amount: '30' })).body).not.toHaveProperty('withholding_tax')
    expect(manualRequest(draft({ kind: 'asset', symbol: 'NEWCO', name: 'New Co', category: 'ETF', currency: 'EUR', fetchPrices: true }))).toEqual({
      path: '/api/portfolio/assets', body: { symbol: 'NEWCO', name: 'New Co', asset_category: 'ETF', currency: 'EUR', isin: '', notes: '', fetch_prices: true },
    })
    expect(manualRequest(draft({ kind: 'price', symbol: 'NEWCO', price: '12.5', description: 'NAV' }))).toEqual({
      path: '/api/portfolio/prices', body: { symbol: 'NEWCO', date: '2024-03-01', price: 12.5, currency: 'USD', note: 'NAV' },
    })
  })

  it('says what is missing before anything is sent', () => {
    expect(draftProblem(draft({ kind: 'buy', symbol: 'AAA', quantity: '1', price: '1' }))).toBeNull()
    expect(draftProblem(draft({ kind: 'buy', symbol: 'AAA', quantity: '1' }))).toBe('Enter a positive quantity and price')
    expect(draftProblem(draft({ kind: 'deposit', amount: '100' }))).toBeNull()
    expect(draftProblem(draft({ kind: 'buy', category: 'OPT', underlying: 'AAA', strike: '100', quantity: '1', price: '1' }))).toBe('Enter the expiry')
    expect(draftProblem(draft({ kind: 'option_exercise', contract: 'AAA   240315P00100000', quantity: '1', settlement: 'cash' }))).toBe('Enter the underlying’s settlement price')
    expect(draftProblem(draft({ kind: 'split', symbol: 'AAA', ratioFrom: '2', ratioTo: '2' }))).toBe('A split changes the number of shares')
    expect(draftProblem(draft({ kind: 'spinoff', symbol: 'AAA', newSymbol: 'aaa', quantity: '1' }))).toBe('The spun-off company needs its own symbol')
    expect(draftProblem(draft({ kind: 'dividend', symbol: 'AAA', amount: '5', tax: '6' }))).toBe('The tax withheld must be between zero and the dividend')
    expect(draftProblem(draft({ kind: 'asset', symbol: 'X', day: '' }))).toBe('Give the asset a name')
    expect(draftProblem(draft({ kind: 'price', symbol: 'X', price: '2', day: '2999-01-01' }))).toBe('Enter a date, today or earlier')
  })

  it('works out the cash a trade moves and words what was recorded', () => {
    expect(tradeCash(draft({ kind: 'sell_short', category: 'OPT', multiplier: '100', quantity: '2', price: '1.5', commission: '1' }))).toBe(299)
    expect(tradeCash(draft({ kind: 'buy_to_cover', symbol: 'AAA', quantity: '4', price: '140' }))).toBe(-560)
    expect(addedSummary(draft({ kind: 'sell_short', symbol: 'aaa', quantity: '10' }))).toBe('Sold 10 AAA short on 1 Mar 2024')
    expect(addedSummary(draft({ kind: 'option_exercise', contract: 'AAA   240315P00100000', quantity: '1' }), [{ buy_sell: 'BUY' }], optionContracts(records))).toBe('Recorded the assignment of 1 AAA 100 put (15 Mar 2024) on 1 Mar 2024')
    expect(addedSummary(draft({ kind: 'split', symbol: 'AAA', ratioFrom: '10', ratioTo: '1' }))).toBe('Recorded a 1-for-10 consolidation of AAA on 1 Mar 2024')
  })

  it('deletes the records of one entry together', () => {
    const rows: Transaction[] = [
      { id: 1, activity_type: 'DIVIDEND', action_id: 'manual-abc' },
      { id: 2, activity_type: 'WITHHOLDING_TAX', action_id: 'manual-abc' },
      { id: 3, activity_type: 'DIVIDEND', action_id: '12345' },
      { id: 4, activity_type: 'WITHHOLDING_TAX', action_id: '12345' },
    ]
    expect(selectedRecords(rows, { kind: 'records', ids: [2] }).map(row => row.id)).toEqual([1, 2])
    // The broker's own action ids are not followed.
    expect(selectedRecords(rows, { kind: 'records', ids: [3] }).map(row => row.id)).toEqual([3])
  })
})
