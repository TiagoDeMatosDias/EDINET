import type { Transaction } from './portfolioTypes'

/** A currency conversion moves two balances; its commission is charged separately. */
export type FxLeg = { currency: string; amount: number }

const PAIR = /^([A-Z]{3})\.([A-Z]{3})$/

export function fxLegs(row: Transaction): [FxLeg, FxLeg] | null {
  if (row.activity_type !== 'TRADE') return null
  const pair = PAIR.exec((row.symbol ?? '').trim())
  if (!pair || (row.asset_category && row.asset_category !== 'CASH')) return null
  return [
    { currency: pair[1], amount: Number(row.quantity ?? 0) },
    { currency: pair[2], amount: -Number(row.trade_money ?? 0) },
  ]
}

// Within a day: trades, then each payment followed by its taxes and fees.
const TYPE_ORDER = ['TRADE', 'SPINOFF', 'DIVIDEND', 'PIL_DIVIDEND', 'WITHHOLDING_TAX', 'OTHER_FEE', 'COMMISSION_ADJ', 'BROKER_INTEREST', 'BOND_INTEREST', 'OTHER_CASH', 'DEPOSIT_WITHDRAWAL']

function typeRank(type?: string) {
  const index = TYPE_ORDER.indexOf(type ?? '')
  return index < 0 ? TYPE_ORDER.length : index
}

/**
 * Newest day first; within a day each company's records sit together, the
 * payment before its taxes, and a correction after the record it corrects.
 * Sorting the table by date keeps this order for records on the same day.
 */
export function orderActivity(records: Transaction[]) {
  return [...records].sort((a, b) =>
    (b.trade_date ?? '').localeCompare(a.trade_date ?? '')
    || (a.symbol ?? '').localeCompare(b.symbol ?? '')
    || typeRank(a.activity_type) - typeRank(b.activity_type)
    || (a.description ?? '').localeCompare(b.description ?? '')
    || (a.report_date ?? '').localeCompare(b.report_date ?? '')
    || Number(a.id ?? 0) - Number(b.id ?? 0))
}

export type Correction = {
  /** A reversal cancels an earlier record; a corrected record replaces one that was reversed. */
  kind?: 'reversal' | 'corrected' | 'reversed'
  /** When the broker booked it, if well after the date it is listed under. */
  booked?: string
}

const BOOKED_LATER_DAYS = 5

function daysBetween(from: string, to: string) {
  return (Date.parse(to) - Date.parse(from)) / 86_400_000
}

/**
 * Brokers correct withholding tax, fees, and dividends by reversing the
 * original record and booking a corrected one, both dated like the original.
 * This finds those records so they read as corrections rather than duplicates.
 * Deposits are left alone: transfers between accounts look the same.
 */
export function findCorrections(records: Transaction[]) {
  const result = new Map<number, Correction>()
  const note = (row: Transaction, value: Correction) => {
    if (row.id != null) result.set(row.id, { ...result.get(row.id), ...value })
  }
  for (const row of records) {
    if (row.report_date && row.trade_date && daysBetween(row.trade_date, row.report_date) > BOOKED_LATER_DAYS) note(row, { booked: row.report_date })
  }
  const groups = new Map<string, Transaction[]>()
  for (const row of records) {
    if (row.activity_type === 'TRADE' || row.activity_type === 'DEPOSIT_WITHDRAWAL' || row.id == null) continue
    const key = [row.trade_date, row.symbol, row.activity_type, row.currency, row.description].join('\u0000')
    groups.set(key, [...(groups.get(key) ?? []), row])
  }
  for (const group of groups.values()) {
    if (group.length < 2) continue
    const ordered = [...group].sort((a, b) => (a.report_date ?? '').localeCompare(b.report_date ?? '') || Number(a.id) - Number(b.id))
    const matched = new Set<Transaction>()
    let firstReversed = -1
    ordered.forEach((row, index) => {
      const amount = Number(row.amount ?? 0)
      if (!amount) return
      const original = ordered.slice(0, index).find(earlier => !matched.has(earlier) && Math.abs(Number(earlier.amount ?? 0) + amount) < 0.005)
      if (!original) return
      matched.add(original).add(row)
      note(original, { kind: 'reversed' })
      note(row, { kind: 'reversal' })
      const at = ordered.indexOf(original)
      if (firstReversed < 0 || at < firstReversed) firstReversed = at
    })
    if (firstReversed < 0) continue
    ordered.forEach((row, index) => {
      if (index > firstReversed && !matched.has(row)) note(row, { kind: 'corrected' })
    })
  }
  return result
}

export function correctionLabel(correction?: Correction) {
  if (!correction?.kind) return undefined
  return { reversal: 'Reversal', corrected: 'Corrected', reversed: 'Reversed' }[correction.kind]
}
