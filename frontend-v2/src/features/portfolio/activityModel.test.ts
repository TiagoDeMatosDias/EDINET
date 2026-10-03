import { describe, expect, it } from 'vitest'

import { correctionLabel, findCorrections, fxLegs, orderActivity } from './activityModel'
import { importSummary, transactionCashEffect } from './portfolioFormat'
import type { Transaction } from './portfolioTypes'

const TAX = 'O(US7561091049) CASH DIVIDEND USD 0.2345 PER SHARE - US TAX'

// How the broker corrects US withholding tax: the next year's statement
// reverses the original charge and books the corrected one, both under the
// dividend's date.
const RECORDS: Transaction[] = [
  { id: 900, trade_date: '2021-03-15', report_date: '2022-02-04', activity_type: 'WITHHOLDING_TAX', symbol: 'O', currency: 'USD', amount: -1.09, description: TAX },
  { id: 120, trade_date: '2021-03-15', report_date: '2021-03-15', activity_type: 'WITHHOLDING_TAX', symbol: 'O', currency: 'USD', amount: -3.52, description: TAX },
  { id: 890, trade_date: '2021-03-15', report_date: '2022-02-04', activity_type: 'WITHHOLDING_TAX', symbol: 'O', currency: 'USD', amount: 3.52, description: TAX },
  { id: 119, trade_date: '2021-03-15', report_date: '2021-03-15', activity_type: 'DIVIDEND', symbol: 'O', currency: 'USD', amount: 23.45, description: 'O CASH DIVIDEND' },
  { id: 130, trade_date: '2021-03-15', activity_type: 'DIVIDEND', symbol: 'MO', currency: 'USD', amount: 40, description: 'MO CASH DIVIDEND' },
  { id: 140, trade_date: '2021-03-16', activity_type: 'DEPOSIT_WITHDRAWAL', currency: 'EUR', amount: 100, description: 'TRANSFER' },
  { id: 141, trade_date: '2021-03-16', activity_type: 'DEPOSIT_WITHDRAWAL', currency: 'EUR', amount: -100, description: 'TRANSFER' },
]

describe('activity records', () => {
  it('orders each day by company, payment before tax, corrections after the original', () => {
    expect(orderActivity(RECORDS).map(row => row.id)).toEqual([140, 141, 130, 119, 120, 890, 900])
  })

  it('reads reversals and corrected charges as corrections, not duplicates', () => {
    const found = findCorrections(RECORDS)
    expect(found.get(120)).toEqual({ kind: 'reversed' })
    expect(found.get(890)).toEqual({ kind: 'reversal', booked: '2022-02-04' })
    expect(found.get(900)).toEqual({ kind: 'corrected', booked: '2022-02-04' })
    expect(correctionLabel(found.get(890))).toBe('Reversal')
    // A dividend, and transfers between accounts, are left alone.
    expect(found.has(119)).toBe(false)
    expect(found.has(140)).toBe(false)
  })

  it('works out corrections without booking dates, from the import order', () => {
    const found = findCorrections(RECORDS.map(row => ({ ...row, report_date: null })))
    expect([found.get(120)?.kind, found.get(890)?.kind, found.get(900)?.kind]).toEqual(['reversed', 'reversal', 'corrected'])
    expect(found.get(890)?.booked).toBeUndefined()
  })

  it('shows a currency conversion as two legs rather than no cash at all', () => {
    const conversion: Transaction = { activity_type: 'TRADE', asset_category: 'CASH', symbol: 'EUR.USD', currency: 'USD', quantity: -97, trade_money: -117.9229, net_cash: 0 }
    expect(fxLegs(conversion)).toEqual([{ currency: 'EUR', amount: -97 }, { currency: 'USD', amount: 117.9229 }])
    expect(transactionCashEffect(conversion)).toBeUndefined()
    expect(fxLegs({ activity_type: 'TRADE', asset_category: 'STK', symbol: 'MO', quantity: 1 })).toBeNull()
  })

  it('summarises an import, including records that gained details', () => {
    expect(importSummary(7, { inserted: 1, skipped: 1201, updated: 1201 })).toBe('Imported 7 files: 1 new record, 1,201 already stored (1,201 gained details).')
    expect(importSummary(1, { inserted: 3, skipped: 0, updated: 0 })).toBe('Imported 1 file: 3 new records.')
  })
})
