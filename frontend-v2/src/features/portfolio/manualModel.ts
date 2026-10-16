import { formatDay } from './portfolioFormat'
import type { ManualAsset, Transaction } from './portfolioTypes'

export type Kind =
  | 'buy' | 'sell' | 'sell_short' | 'buy_to_cover'
  | 'dividend' | 'withholding_tax'
  | 'deposit' | 'withdrawal' | 'fee' | 'interest'
  | 'split' | 'spinoff'
  | 'option_exercise' | 'option_expiration'
  | 'asset' | 'price'

export const KIND_GROUPS: Array<{ label: string; kinds: Array<{ value: Kind; label: string }> }> = [
  { label: 'Trades', kinds: [{ value: 'buy', label: 'Buy' }, { value: 'sell', label: 'Sell' }, { value: 'sell_short', label: 'Sell short' }, { value: 'buy_to_cover', label: 'Buy to cover' }] },
  { label: 'Income', kinds: [{ value: 'dividend', label: 'Dividend' }, { value: 'withholding_tax', label: 'Withholding tax' }] },
  { label: 'Cash', kinds: [{ value: 'deposit', label: 'Deposit' }, { value: 'withdrawal', label: 'Withdrawal' }, { value: 'fee', label: 'Fee' }, { value: 'interest', label: 'Interest' }] },
  { label: 'Corporate actions', kinds: [{ value: 'split', label: 'Split or consolidation' }, { value: 'spinoff', label: 'Spin-off' }] },
  { label: 'Options', kinds: [{ value: 'option_exercise', label: 'Exercise or assignment' }, { value: 'option_expiration', label: 'Expiration' }] },
  { label: 'Assets and prices', kinds: [{ value: 'asset', label: 'Define an asset' }, { value: 'price', label: 'Price update' }] },
]

export const TRADE_KINDS: Kind[] = ['buy', 'sell', 'sell_short', 'buy_to_cover']
export const CASH_KINDS: Kind[] = ['deposit', 'withdrawal', 'fee', 'interest']
export const OPTION_KINDS: Kind[] = ['option_exercise', 'option_expiration']

/** Asset types a trade can have; an empty value lets the account's records decide. */
export const TRADE_CATEGORIES = [
  { value: '', label: 'As held' },
  { value: 'STK', label: 'Stock' },
  { value: 'ETF', label: 'ETF' },
  { value: 'FUND', label: 'Fund' },
  { value: 'BOND', label: 'Bond' },
  { value: 'OTHER', label: 'Other' },
  { value: 'OPT', label: 'Option' },
]
export const ASSET_CATEGORIES = TRADE_CATEGORIES.filter(item => item.value && item.value !== 'OPT')

/** Everything the form holds, as typed. */
export type Draft = {
  kind: Kind
  day: string
  symbol: string
  currency: string
  category: string
  quantity: string
  price: string
  commission: string
  amount: string
  tax: string
  description: string
  /** A known option contract's symbol, or empty to enter its terms. */
  contract: string
  underlying: string
  putCall: 'C' | 'P'
  strike: string
  expiry: string
  multiplier: string
  position: '' | 'long' | 'short'
  settlement: 'shares' | 'cash'
  settlementPrice: string
  ratioFrom: string
  ratioTo: string
  newSymbol: string
  newCategory: string
  name: string
  isin: string
  fetchPrices: boolean
}

export const today = () => new Date().toISOString().slice(0, 10)

export function emptyDraft(currency: string): Draft {
  return {
    kind: 'buy', day: today(), symbol: '', currency, category: '', quantity: '', price: '', commission: '', amount: '', tax: '', description: '',
    contract: '', underlying: '', putCall: 'C', strike: '', expiry: '', multiplier: '100', position: '', settlement: 'shares', settlementPrice: '',
    ratioFrom: '1', ratioTo: '2', newSymbol: '', newCategory: '', name: '', isin: '', fetchPrices: false,
  }
}

/** A number as typed: thousands separators allowed, empty is NaN. */
export function number(text: string) {
  const cleaned = text.replace(/,/g, '').trim()
  return cleaned ? Number(cleaned) : Number.NaN
}

const positive = (text: string) => number(text) > 0
const DAY = /^\d{4}-\d{2}-\d{2}$/

export function isOption(draft: Pick<Draft, 'kind' | 'category'>) {
  return OPTION_KINDS.includes(draft.kind) || (TRADE_KINDS.includes(draft.kind) && draft.category === 'OPT')
}

/** The contract's OCC symbol, as the broker and the server write it: ``AFL   210219P00040000``. */
export function optionSymbol(underlying: string, expiry: string, putCall: string, strike: number) {
  const [year, month, day] = expiry.split('-')
  return `${underlying.trim().toUpperCase().padEnd(6)}${year.slice(2)}${month}${day}${putCall.toUpperCase()}${String(Math.round(strike * 1000)).padStart(8, '0')}`
}

export function parseOptionSymbol(symbol: string) {
  const match = /^(\S{1,6})\s*(\d{2})(\d{2})(\d{2})([CP])(\d{8})$/.exec(symbol.trim().toUpperCase())
  if (!match) return null
  return { underlying: match[1], expiry: `20${match[2]}-${match[3]}-${match[4]}`, putCall: match[5] as 'C' | 'P', strike: Number(match[6]) / 1000 }
}

export type Contract = {
  symbol: string
  underlying: string
  putCall: 'C' | 'P'
  strike: number
  expiry: string
  multiplier: number
  currency: string
  /** Contracts held, from the account's trades: negative when short. */
  net: number
}

export function contractLabel(contract: Pick<Contract, 'underlying' | 'putCall' | 'strike' | 'expiry'>) {
  return `${contract.underlying} ${contract.strike.toLocaleString(undefined, { maximumFractionDigits: 3 })} ${contract.putCall === 'P' ? 'put' : 'call'} · ${formatDay(contract.expiry)}`
}

/** The contract in a sentence: "AFL 100 put (19 Feb 2021)". */
export function contractName(contract: Pick<Contract, 'underlying' | 'putCall' | 'strike' | 'expiry'>) {
  return `${contract.underlying} ${contract.strike.toLocaleString(undefined, { maximumFractionDigits: 3 })} ${contract.putCall === 'P' ? 'put' : 'call'} (${formatDay(contract.expiry)})`
}

/** Option contracts in the account's records, those still held first. */
export function optionContracts(records: Transaction[]): Contract[] {
  const found = new Map<string, Contract>()
  for (const record of records) {
    if (record.asset_category !== 'OPT' || !record.symbol) continue
    const parsed = parseOptionSymbol(record.symbol)
    const known = found.get(record.symbol)
    const contract = known ?? {
      symbol: record.symbol,
      underlying: record.underlying_symbol || parsed?.underlying || record.symbol.split(' ')[0],
      putCall: ((record.put_call || parsed?.putCall || 'C').toUpperCase() === 'P' ? 'P' : 'C') as 'C' | 'P',
      strike: Number(record.strike ?? parsed?.strike ?? 0),
      expiry: (record.expiry || parsed?.expiry || '').slice(0, 10),
      multiplier: Number(record.multiplier) || 100,
      currency: record.currency ?? '',
      net: 0,
    }
    if (record.activity_type === 'TRADE') contract.net += Number(record.quantity ?? 0)
    // A later record (a split) carries the contract's current terms.
    if (known && record.strike) contract.strike = Number(record.strike)
    if (known && record.multiplier) contract.multiplier = Number(record.multiplier)
    found.set(record.symbol, contract)
  }
  return [...found.values()]
    .map(contract => ({ ...contract, net: Math.round(contract.net * 1e6) / 1e6 }))
    .sort((a, b) => Number(b.net !== 0) - Number(a.net !== 0) || b.expiry.localeCompare(a.expiry) || a.symbol.localeCompare(b.symbol))
}

/** Securities to offer for the symbol: the account's own (not cash, not options) and the assets it defined. */
export function knownSymbols(records: Transaction[], assets: ManualAsset[] = []) {
  const symbols = new Map<string, string>()
  for (const record of records) {
    if (record.symbol && record.asset_category !== 'CASH' && record.asset_category !== 'OPT' && record.activity_type !== 'DEPOSIT_WITHDRAWAL') symbols.set(record.symbol, record.currency ?? '')
  }
  for (const asset of assets) symbols.set(asset.symbol, asset.currency ?? symbols.get(asset.symbol) ?? '')
  return [...symbols.entries()].sort((a, b) => a[0].localeCompare(b[0]))
}

/** The contract a draft names: a known one, or the terms typed in. */
function contractFields(draft: Draft) {
  if (draft.contract) return { symbol: draft.contract }
  return {
    // Its symbol lets the server find the side it is held on.
    symbol: optionSymbol(draft.underlying, draft.expiry, draft.putCall, number(draft.strike)),
    underlying: draft.underlying.trim().toUpperCase(),
    put_call: draft.putCall,
    strike: number(draft.strike),
    expiry: draft.expiry,
    multiplier: number(draft.multiplier),
  }
}

/** Why the draft cannot be added yet, or null when it can. */
export function draftProblem(draft: Draft): string | null {
  const option = isOption(draft)
  if (draft.kind !== 'asset' && (!DAY.test(draft.day) || draft.day > today())) return 'Enter a date, today or earlier'
  if (option && !draft.contract) {
    if (!draft.underlying.trim()) return 'Name the underlying'
    if (!positive(draft.strike)) return 'Enter the strike'
    if (!DAY.test(draft.expiry)) return 'Enter the expiry'
    if (!positive(draft.multiplier)) return 'Enter the multiplier (usually 100)'
  }
  const needsSymbol = !option && !CASH_KINDS.includes(draft.kind)
  if (needsSymbol && !draft.symbol.trim()) return draft.kind === 'asset' ? 'Give the asset a symbol' : 'Name the security (its symbol)'
  if (draft.commission.trim() && !(number(draft.commission) >= 0)) return 'Enter the commission as a positive number'
  if (TRADE_KINDS.includes(draft.kind)) return positive(draft.quantity) && positive(draft.price) ? null : 'Enter a positive quantity and price'
  switch (draft.kind) {
    case 'option_exercise':
      if (!positive(draft.quantity)) return 'Enter how many contracts'
      return draft.settlement === 'cash' && !positive(draft.settlementPrice) ? 'Enter the underlying’s settlement price' : null
    case 'option_expiration':
      return positive(draft.quantity) ? null : 'Enter how many contracts'
    case 'split':
      if (!positive(draft.ratioFrom) || !positive(draft.ratioTo)) return 'Enter the ratio as new shares for old shares'
      return number(draft.ratioFrom) === number(draft.ratioTo) ? 'A split changes the number of shares' : null
    case 'spinoff':
      if (!draft.newSymbol.trim()) return 'Name the company spun off'
      if (draft.newSymbol.trim().toUpperCase() === draft.symbol.trim().toUpperCase()) return 'The spun-off company needs its own symbol'
      return positive(draft.quantity) ? null : 'Enter the shares received'
    case 'asset':
      return draft.name.trim() ? null : 'Give the asset a name'
    case 'price':
      return positive(draft.price) ? null : 'Enter a positive price'
    default:
      if (!positive(draft.amount)) return 'Enter a positive amount'
      if (draft.kind === 'dividend' && draft.tax.trim() && !(number(draft.tax) >= 0 && number(draft.tax) <= number(draft.amount))) return 'The tax withheld must be between zero and the dividend'
      return null
  }
}

/** The request that records the draft. Only the fields its kind uses are sent. */
export function manualRequest(draft: Draft): { path: string; body: Record<string, unknown> } {
  const symbol = draft.symbol.trim()
  const description = draft.description.trim()
  const commission = number(draft.commission) || 0
  if (draft.kind === 'asset') {
    return { path: '/api/portfolio/assets', body: { symbol, name: draft.name.trim(), asset_category: draft.category && draft.category !== 'OPT' ? draft.category : 'STK', currency: draft.currency, isin: draft.isin.trim(), notes: description, fetch_prices: draft.fetchPrices } }
  }
  if (draft.kind === 'price') {
    return { path: '/api/portfolio/prices', body: { symbol, date: draft.day, price: number(draft.price), currency: draft.currency, note: description } }
  }
  const base = { kind: draft.kind, trade_date: draft.day, currency: draft.currency, description }
  const path = '/api/portfolio/transactions/manual'
  if (TRADE_KINDS.includes(draft.kind)) {
    const option = isOption(draft)
    return { path, body: { ...base, ...(option ? { asset_category: 'OPT', ...contractFields(draft) } : { symbol, ...(draft.category ? { asset_category: draft.category } : {}) }), quantity: number(draft.quantity), price: number(draft.price), commission, amount: 0 } }
  }
  if (draft.kind === 'option_exercise') {
    return { path, body: { ...base, ...contractFields(draft), quantity: number(draft.quantity), commission, position: draft.position, settlement: draft.settlement, ...(draft.settlement === 'cash' ? { settlement_price: number(draft.settlementPrice) } : {}) } }
  }
  if (draft.kind === 'option_expiration') {
    return { path, body: { ...base, ...contractFields(draft), quantity: number(draft.quantity), position: draft.position } }
  }
  if (draft.kind === 'split') return { path, body: { ...base, symbol, ratio_from: number(draft.ratioFrom), ratio_to: number(draft.ratioTo) } }
  if (draft.kind === 'spinoff') {
    return { path, body: { ...base, symbol, new_symbol: draft.newSymbol.trim(), quantity: number(draft.quantity), ...(draft.newCategory ? { new_category: draft.newCategory } : {}) } }
  }
  const tax = draft.kind === 'dividend' ? number(draft.tax) || 0 : 0
  return { path, body: { ...base, symbol, amount: number(draft.amount), ...(tax > 0 ? { withholding_tax: tax } : {}) } }
}

const amountText = (text: string) => number(text).toLocaleString(undefined, { maximumFractionDigits: 6 })

/** What was recorded, for the status line: "sold 10 AAA short on 3 Jan 2024". */
export function addedSummary(draft: Draft, records: Transaction[] = [], contracts: Contract[] = []) {
  const known = contracts.find(contract => contract.symbol === draft.contract)
  const parsed = draft.contract ? parseOptionSymbol(draft.contract) : null
  const what = isOption(draft)
    ? contractName(known ?? parsed ?? { underlying: draft.underlying.trim().toUpperCase(), putCall: draft.putCall, strike: number(draft.strike), expiry: draft.expiry })
    : draft.symbol.trim().toUpperCase()
  const on = `on ${formatDay(draft.day)}`
  const quantity = amountText(draft.quantity)
  switch (draft.kind) {
    case 'buy': return `Bought ${quantity} ${what} ${on}`
    case 'sell': return `Sold ${quantity} ${what} ${on}`
    case 'sell_short': return `Sold ${quantity} ${what} short ${on}`
    case 'buy_to_cover': return `Bought back ${quantity} ${what} ${on}`
    case 'dividend': return `Recorded a dividend from ${what} ${on}`
    case 'withholding_tax': return `Recorded withholding tax on ${what} ${on}`
    case 'deposit': return `Recorded a deposit ${on}`
    case 'withdrawal': return `Recorded a withdrawal ${on}`
    case 'fee': return `Recorded a fee ${on}`
    case 'interest': return `Recorded interest ${on}`
    case 'split': return `Recorded a ${amountText(draft.ratioTo)}-for-${amountText(draft.ratioFrom)} ${number(draft.ratioTo) > number(draft.ratioFrom) ? 'split' : 'consolidation'} of ${what} ${on}`
    case 'spinoff': return `Recorded the spin-off of ${draft.newSymbol.trim().toUpperCase()} from ${what} ${on}`
    case 'option_exercise': {
      // The option leg closes the side held: selling a long contract is an exercise.
      const side = records[0]?.buy_sell
      return `Recorded the ${side === 'BUY' ? 'assignment' : side === 'SELL' ? 'exercise' : 'exercise or assignment'} of ${quantity} ${what} ${on}`
    }
    case 'option_expiration': return `Recorded the expiry of ${quantity} ${what} ${on}`
    case 'asset': return `Saved the asset ${what}${draft.name.trim() ? ` (${draft.name.trim()})` : ''}`
    case 'price': return `Set the price of ${what} to ${amountText(draft.price)} ${draft.currency} ${on}, for your portfolio only`
  }
}

/** The cash a trade moves, with its sign: out for purchases, in for sales. */
export function tradeCash(draft: Draft, contracts: Contract[] = []) {
  if (!TRADE_KINDS.includes(draft.kind) || !positive(draft.quantity) || !positive(draft.price)) return null
  const known = contracts.find(contract => contract.symbol === draft.contract)
  const multiplier = isOption(draft) ? (known?.multiplier ?? number(draft.multiplier)) || 100 : 1
  const buying = draft.kind === 'buy' || draft.kind === 'buy_to_cover'
  const commission = number(draft.commission) || 0
  return buying ? -(number(draft.quantity) * number(draft.price) * multiplier + commission) : number(draft.quantity) * number(draft.price) * multiplier - commission
}
