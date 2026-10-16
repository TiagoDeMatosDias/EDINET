import { useMutation } from '@tanstack/react-query'
import { Plus, X } from 'lucide-react'
import { useEffect, useMemo, useRef, useState, type FormEvent, type KeyboardEvent, type ReactNode } from 'react'

import { apiPost } from '../../api/client'
import {
  addedSummary,
  ASSET_CATEGORIES,
  CASH_KINDS,
  contractLabel,
  draftProblem,
  emptyDraft,
  isOption,
  KIND_GROUPS,
  knownSymbols,
  manualRequest,
  number,
  OPTION_KINDS,
  optionContracts,
  optionSymbol,
  today,
  TRADE_CATEGORIES,
  TRADE_KINDS,
  tradeCash,
  type Contract,
  type Draft,
  type Kind,
} from './manualModel'
import type { ManualAsset, Transaction } from './portfolioTypes'

export type ManualResult = {
  transaction?: Transaction | null
  transactions?: Transaction[]
  base_currency?: string
  fx_rate_to_base?: number | null
  daily_rows: number
  holdings_count: number
  /** What was recorded, in words, for the status line. */
  summary?: string
}

const HINTS: Record<Kind, string> = {
  buy: 'A purchase; it first covers a short position in the same security.',
  sell: 'A sale of shares held; it stops at the shares held on the date. Use Sell short to go below zero.',
  sell_short: 'Opens or adds to a short position: the proceeds come in as cash and the position is held at a negative quantity until bought back.',
  buy_to_cover: 'Closes a short position, up to the shares short on the date.',
  dividend: 'The gross dividend. Tax withheld is stored with it as withholding tax, and the two are deleted together.',
  withholding_tax: 'Tax withheld from a dividend recorded on its own.',
  deposit: 'Money paid into the account.',
  withdrawal: 'Money taken out of the account.',
  fee: 'A fee the account paid.',
  interest: 'Interest the account received.',
  split: 'New shares for old: 2 for 1 doubles the shares, 1 for 10 consolidates them. Shares, average cost, and options on it change from the date. If the market data already records the split, it is applied once.',
  spinoff: 'The new shares arrive at zero cost; the parent keeps its whole cost basis.',
  option_exercise: 'Long: you exercise. Short: you are assigned. In shares, the underlying is bought or sold at the strike (an assigned call without the shares opens a short); in cash, the contract closes at its intrinsic value. The premium stays with the contract as realized P&L.',
  option_expiration: 'The contract closes at no value; the premium paid or received becomes realized P&L.',
  asset: 'A security the market data does not have, such as a foreign stock, an ETF, or a private fund. Its name shows across your portfolio. Unless market prices are used too, it is valued only at the prices you enter.',
  price: 'Values your holding from this date until a newer price, yours or the market’s. It applies to your portfolio only; the shared market prices do not change.',
}

function Field({ label, wide, children }: { label: string; wide?: boolean; children: ReactNode }) {
  return <label className={`field${wide ? ' pf-manual__wide' : ''}`}><span>{label}</span>{children}</label>
}

/** What an exercise does with the shares: "Buys 200 AAA at 100". */
function exerciseEffect(draft: Draft, contract: Contract | undefined) {
  if (draft.kind !== 'option_exercise' || !(number(draft.quantity) > 0)) return null
  const terms = contract ?? { underlying: draft.underlying.trim().toUpperCase(), putCall: draft.putCall, strike: number(draft.strike), multiplier: number(draft.multiplier), net: 0 }
  const short = draft.position ? draft.position === 'short' : terms.net < 0
  if (draft.settlement === 'cash') {
    const price = number(draft.settlementPrice)
    if (!(price > 0) || !(terms.strike > 0)) return null
    const value = Math.max(0, terms.putCall === 'C' ? price - terms.strike : terms.strike - price) * number(draft.quantity) * (terms.multiplier || 100)
    return `${short ? 'Pays' : 'Receives'} ${value.toLocaleString(undefined, { maximumFractionDigits: 2 })} ${draft.currency}`
  }
  const buys = (terms.putCall === 'C') !== short
  const shares = number(draft.quantity) * (terms.multiplier || 100)
  return `${buys ? 'Buys' : 'Sells'} ${shares.toLocaleString()} ${terms.underlying || 'shares'} at ${terms.strike > 0 ? terms.strike.toLocaleString() : 'the strike'}`
}

/**
 * Record what the broker file does not have: a trade elsewhere, a short
 * sale, an option exercised or expired, a split or spin-off, a dividend paid
 * in cash, a deposit; or define an asset the market data does not know and
 * set its price. Records are stored under "Manual entries" and the portfolio
 * rebuilds with them. Enter adds and keeps the form open for the next.
 */
export function ManualTransactionForm({ records, assets = [], initial, onAdded, onClose }: {
  records: Transaction[]
  assets?: ManualAsset[]
  initial?: Partial<Draft>
  onAdded: (result: ManualResult) => void
  onClose: () => void
}) {
  const [draft, setDraft] = useState<Draft>(() => ({ ...emptyDraft(records.find(record => record.currency)?.currency ?? 'EUR'), ...initial }))
  const [tried, setTried] = useState(false)
  const first = useRef<HTMLSelectElement>(null)
  useEffect(() => { first.current?.focus() }, [])

  const contracts = useMemo(() => optionContracts(records), [records])
  const symbols = useMemo(() => knownSymbols(records, assets), [assets, records])
  const currencies = [...new Set([...records.map(record => record.currency).filter(Boolean) as string[], ...assets.map(asset => asset.currency).filter(Boolean) as string[], 'EUR', 'USD', 'JPY', 'GBP', draft.currency])].sort()
  const set = (patch: Partial<Draft>) => setDraft(current => ({ ...current, ...patch }))
  const { kind } = draft
  const option = isOption(draft)
  const trade = TRADE_KINDS.includes(kind)
  const contract = contracts.find(item => item.symbol === draft.contract)
  const problem = draftProblem(draft)
  const cash = tradeCash(draft, contracts)
  const effect = exerciseEffect(draft, contract)

  const chooseContract = (symbol: string) => {
    const chosen = contracts.find(item => item.symbol === symbol)
    set({ contract: symbol, ...(chosen?.currency ? { currency: chosen.currency } : {}) })
  }
  const chooseKind = (next: Kind) => {
    const patch: Partial<Draft> = { kind: next }
    // Exercises and expirations are of a contract held: the one whose terms were typed, else the first held.
    if (OPTION_KINDS.includes(next) && !draft.contract) {
      const typed = draft.underlying.trim() && number(draft.strike) > 0 && /^\d{4}-\d{2}-\d{2}$/.test(draft.expiry) ? optionSymbol(draft.underlying, draft.expiry, draft.putCall, number(draft.strike)) : ''
      const held = typed ? contracts.find(item => item.symbol === typed) : contracts.find(item => item.net !== 0)
      if (held) Object.assign(patch, { contract: held.symbol, currency: held.currency || draft.currency })
    }
    if (next === 'asset' && (draft.category === '' || draft.category === 'OPT')) patch.category = 'STK'
    if (TRADE_KINDS.includes(next) && draft.kind === 'asset') patch.category = ''
    set(patch)
  }
  const pickSymbol = (value: string) => {
    const known = symbols.find(([item]) => item.toUpperCase() === value.trim().toUpperCase())
    const priced = contracts.find(item => item.symbol === value.trim().toUpperCase())
    const currency = known?.[1] || priced?.currency || (/^\d{3}[0-9A-Z]\.T$/i.test(value.trim()) ? 'JPY' : '')
    set({ symbol: value, ...(currency ? { currency } : {}) })
  }

  const add = useMutation({
    mutationFn: () => {
      const request = manualRequest(draft)
      return apiPost<ManualResult & { transactions?: Transaction[] }>(request.path, request.body)
    },
    onSuccess: result => {
      onAdded({ ...result, summary: addedSummary(draft, result.transactions ?? [], contracts) })
      set({ quantity: '', price: '', commission: '', amount: '', tax: '', description: '', settlementPrice: '', name: '', isin: '', newSymbol: '' })
      setTried(false)
      first.current?.focus()
    },
  })
  const submit = (event: FormEvent) => {
    event.preventDefault()
    setTried(true)
    if (!problem && !add.isPending) add.mutate()
  }
  const onKeyDown = (event: KeyboardEvent<HTMLFormElement>) => {
    if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); onClose() }
  }

  const input = (key: keyof Draft, extra: Record<string, unknown> = {}) => <input className="input" value={String(draft[key])} onChange={event => set({ [key]: event.target.value } as Partial<Draft>)} {...extra} />
  const decimal = (key: keyof Draft, placeholder?: string) => input(key, { inputMode: 'decimal', placeholder })
  const symbolField = (label: string) => <Field label={label}><input className="input" list="pf-manual-symbols" placeholder="7203.T, AAPL" value={draft.symbol} onChange={event => pickSymbol(event.target.value)} /></Field>
  const currencyField = <Field label="Currency"><select className="select" value={draft.currency} onChange={event => set({ currency: event.target.value })}>{currencies.map(item => <option key={item}>{item}</option>)}</select></Field>
  const contractFields = option && <>
    <Field label="Contract" wide><select className="select" value={draft.contract} onChange={event => chooseContract(event.target.value)}>
      {contracts.map(item => <option key={item.symbol} value={item.symbol}>{contractLabel(item)}{item.net ? ` · ${item.net < 0 ? 'short' : 'long'} ${Math.abs(item.net).toLocaleString()}` : ''}</option>)}
      <option value="">{contracts.length ? 'Another contract: enter its terms' : 'Enter the contract’s terms'}</option>
    </select></Field>
    {!draft.contract && <>
      <Field label="Underlying">{input('underlying', { placeholder: 'AAPL' })}</Field>
      <Field label="Call or put"><select className="select" value={draft.putCall} onChange={event => set({ putCall: event.target.value as Draft['putCall'] })}><option value="C">Call</option><option value="P">Put</option></select></Field>
      <Field label="Strike">{decimal('strike')}</Field>
      <Field label="Expiry">{input('expiry', { type: 'date' })}</Field>
      <Field label="Multiplier">{decimal('multiplier', '100')}</Field>
    </>}
  </>
  const sideField = OPTION_KINDS.includes(kind) && <Field label="Held"><select className="select" value={draft.position} onChange={event => set({ position: event.target.value as Draft['position'] })}>
    <option value="">{contract?.net ? `As held (${contract.net < 0 ? 'short' : 'long'})` : 'As held'}</option>
    <option value="long">{kind === 'option_exercise' ? 'Long: I exercise' : 'Long'}</option>
    <option value="short">{kind === 'option_exercise' ? 'Short: I am assigned' : 'Short'}</option>
  </select></Field>

  let fields: ReactNode
  if (trade) {
    fields = <>
      <Field label="Type"><select className="select" value={draft.category} onChange={event => set({ category: event.target.value })}>{TRADE_CATEGORIES.map(item => <option key={item.value} value={item.value}>{item.label}</option>)}</select></Field>
      {option ? contractFields : symbolField('Symbol')}
      {currencyField}
      <Field label="Quantity">{decimal('quantity')}</Field>
      <Field label="Price">{decimal('price')}</Field>
      <Field label="Commission">{decimal('commission', '0')}</Field>
    </>
  } else if (OPTION_KINDS.includes(kind)) {
    fields = <>
      {contractFields}
      {sideField}
      {currencyField}
      <Field label="Contracts">{decimal('quantity')}</Field>
      {kind === 'option_exercise' && <>
        <Field label="Settled"><select className="select" value={draft.settlement} onChange={event => set({ settlement: event.target.value as Draft['settlement'] })}><option value="shares">In shares</option><option value="cash">In cash</option></select></Field>
        {draft.settlement === 'cash' && <Field label="Settlement price">{decimal('settlementPrice')}</Field>}
        <Field label="Commission">{decimal('commission', '0')}</Field>
      </>}
    </>
  } else if (kind === 'split') {
    fields = <>
      {symbolField('Symbol')}
      {currencyField}
      <Field label="New shares">{decimal('ratioTo')}</Field>
      <Field label="For old shares">{decimal('ratioFrom')}</Field>
    </>
  } else if (kind === 'spinoff') {
    fields = <>
      {symbolField('Parent symbol')}
      {currencyField}
      <Field label="New symbol">{input('newSymbol')}</Field>
      <Field label="New type"><select className="select" value={draft.newCategory} onChange={event => set({ newCategory: event.target.value })}>{TRADE_CATEGORIES.filter(item => item.value !== 'OPT').map(item => <option key={item.value} value={item.value}>{item.value ? item.label : 'As held, else stock'}</option>)}</select></Field>
      <Field label="Shares received">{decimal('quantity')}</Field>
    </>
  } else if (kind === 'asset') {
    fields = <>
      {symbolField('Symbol')}
      <Field label="Name" wide>{input('name', { maxLength: 200, placeholder: 'Vanguard FTSE All-World' })}</Field>
      <Field label="Type"><select className="select" value={draft.category || 'STK'} onChange={event => set({ category: event.target.value })}>{ASSET_CATEGORIES.map(item => <option key={item.value} value={item.value}>{item.label}</option>)}</select></Field>
      {currencyField}
      <Field label="ISIN (optional)">{input('isin', { maxLength: 12 })}</Field>
      <label className="portfolio-check pf-manual__check" title="Leave off for a private fund, or when another security uses the same symbol in the market data."><input type="checkbox" checked={draft.fetchPrices} onChange={event => set({ fetchPrices: event.target.checked })} /><span>Also use market prices</span></label>
    </>
  } else if (kind === 'price') {
    fields = <>
      <Field label="Symbol"><input className="input" list="pf-manual-priced" placeholder="NEWCO, 7203.T" value={draft.symbol} onChange={event => pickSymbol(event.target.value)} /></Field>
      <datalist id="pf-manual-priced">{[...symbols.map(([item, itemCurrency]) => [item, itemCurrency]), ...contracts.map(item => [item.symbol, contractLabel(item)])].map(([item, detail]) => <option key={item} value={item}>{detail}</option>)}</datalist>
      {currencyField}
      <Field label="Price">{decimal('price')}</Field>
    </>
  } else {
    const cashKind = CASH_KINDS.includes(kind)
    fields = <>
      {symbolField(cashKind ? 'Symbol (optional)' : 'Symbol')}
      {currencyField}
      <Field label="Amount">{decimal('amount')}</Field>
      {kind === 'dividend' && <Field label="Tax withheld">{decimal('tax', '0')}</Field>}
    </>
  }

  return <form className="pf-manual" onSubmit={submit} onKeyDown={onKeyDown} aria-label="Add a transaction">
    <Field label="Kind" wide><select ref={first} className="select" value={kind} onChange={event => chooseKind(event.target.value as Kind)}>
      {KIND_GROUPS.map(group => <optgroup key={group.label} label={group.label}>{group.kinds.map(item => <option key={item.value} value={item.value}>{item.label}</option>)}</optgroup>)}
    </select></Field>
    {kind !== 'asset' && <Field label="Date"><input className="input" type="date" max={today()} value={draft.day} onChange={event => set({ day: event.target.value })} /></Field>}
    {fields}
    <datalist id="pf-manual-symbols">{symbols.map(([item, itemCurrency]) => <option key={item} value={item}>{itemCurrency}</option>)}</datalist>
    <Field label={kind === 'asset' ? 'Notes (optional)' : kind === 'price' ? 'Note (optional)' : 'Description (optional)'} wide>{input('description', { maxLength: kind === 'asset' ? 500 : 300 })}</Field>
    <div className="pf-manual__actions">
      <button type="submit" className="button button--primary button--small" disabled={Boolean(problem) || add.isPending} title={problem ?? undefined}><Plus aria-hidden="true" />{add.isPending ? 'Saving…' : kind === 'asset' ? 'Save' : 'Add'}</button>
      <button type="button" className="icon-button" aria-label="Close the form" title="Close (Esc)" onClick={onClose}><X /></button>
    </div>
    <p className="pf-manual__hint">
      {cash != null && <>Cash {cash < 0 ? 'out' : 'in'}: <strong className="mono">{Math.abs(cash).toLocaleString(undefined, { maximumFractionDigits: 2 })} {draft.currency}</strong> · </>}
      {effect && <><strong>{effect}</strong> · </>}
      {option && trade ? 'Price per share; the multiplier sets the cash. The premium stays with the contract and becomes realized P&L when it is closed, exercised, or expires. ' : `${HINTS[kind]} `}
      {kind === 'asset' || kind === 'price' ? 'Saved to your account only.' : <>Amounts are positive; the kind sets the direction. Stored under “Manual entries”, which you can delete like any import.</>} <kbd>Enter</kbd> {kind === 'asset' ? 'saves' : 'adds'}, <kbd>Esc</kbd> closes.
      {tried && problem && <span className="form-error" role="alert"> {problem}.</span>}
      {add.isError && <span className="form-error" role="alert"> {(add.error as Error).message}</span>}
    </p>
  </form>
}

