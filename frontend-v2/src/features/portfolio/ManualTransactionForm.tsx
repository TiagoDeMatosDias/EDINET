import { useMutation } from '@tanstack/react-query'
import { Plus, X } from 'lucide-react'
import { useEffect, useRef, useState, type FormEvent, type KeyboardEvent } from 'react'

import { apiPost } from '../../api/client'
import type { Transaction } from './portfolioTypes'

export type ManualResult = { transaction: Transaction | null; base_currency: string; fx_rate_to_base: number | null; daily_rows: number; holdings_count: number }

const KINDS = [
  { value: 'buy', label: 'Buy', trade: true },
  { value: 'sell', label: 'Sell', trade: true },
  { value: 'dividend', label: 'Dividend', symbol: true },
  { value: 'withholding_tax', label: 'Withholding tax', symbol: true },
  { value: 'deposit', label: 'Deposit' },
  { value: 'withdrawal', label: 'Withdrawal' },
  { value: 'fee', label: 'Fee' },
  { value: 'interest', label: 'Interest' },
] as const
type Kind = typeof KINDS[number]['value']

const today = () => new Date().toISOString().slice(0, 10)
const number = (text: string) => Number(text.replace(/,/g, '').trim())

/**
 * Record a transaction the broker file does not have: a trade elsewhere, a
 * dividend paid in cash, a deposit. It is stored under "Manual entries" and the
 * portfolio rebuilds with it. Enter adds it and keeps the form open for the next.
 */
export function ManualTransactionForm({ records, onAdded, onClose }: {
  records: Transaction[]
  onAdded: (result: ManualResult) => void
  onClose: () => void
}) {
  const [kind, setKind] = useState<Kind>('buy')
  const [day, setDay] = useState(today)
  const [symbol, setSymbol] = useState('')
  const [currency, setCurrency] = useState(() => records.find(record => record.currency)?.currency ?? 'EUR')
  const [quantity, setQuantity] = useState('')
  const [price, setPrice] = useState('')
  const [commission, setCommission] = useState('')
  const [amount, setAmount] = useState('')
  const [description, setDescription] = useState('')
  const first = useRef<HTMLSelectElement>(null)
  useEffect(() => { first.current?.focus() }, [])

  const spec = KINDS.find(item => item.value === kind)!
  const trade = 'trade' in spec
  const needsSymbol = trade || 'symbol' in spec
  const symbols = [...new Map(records.filter(record => record.symbol && record.asset_category !== 'CASH').map(record => [record.symbol!, record.currency ?? ''])).entries()].sort()
  const currencies = [...new Set([...records.map(record => record.currency).filter(Boolean) as string[], 'EUR', 'USD', 'JPY', 'GBP'])].sort()
  const total = trade && number(quantity) > 0 && number(price) > 0 ? number(quantity) * number(price) + (kind === 'buy' ? 1 : -1) * (number(commission) || 0) : null
  const valid = /^\d{4}-\d{2}-\d{2}$/.test(day) && (!needsSymbol || symbol.trim()) && (trade ? number(quantity) > 0 && number(price) > 0 && !(number(commission) < 0) : number(amount) > 0)

  const add = useMutation({
    mutationFn: () => apiPost<ManualResult>('/api/portfolio/transactions/manual', {
      kind, trade_date: day, currency, symbol: symbol.trim(), description: description.trim(),
      quantity: trade ? number(quantity) : 0, price: trade ? number(price) : 0, commission: trade ? number(commission) || 0 : 0,
      amount: trade ? 0 : number(amount),
    }),
    onSuccess: result => {
      onAdded(result)
      setQuantity(''); setPrice(''); setCommission(''); setAmount(''); setDescription('')
      first.current?.focus()
    },
  })
  const submit = (event: FormEvent) => { event.preventDefault(); if (valid && !add.isPending) add.mutate() }
  const onKeyDown = (event: KeyboardEvent<HTMLFormElement>) => {
    if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); onClose() }
  }
  const pickSymbol = (value: string) => {
    setSymbol(value)
    const known = symbols.find(([item]) => item.toUpperCase() === value.trim().toUpperCase())
    if (known?.[1]) setCurrency(known[1])
    else if (/^\d{3}[0-9A-Z]\.T$/i.test(value.trim())) setCurrency('JPY')
  }

  return <form className="pf-manual" onSubmit={submit} onKeyDown={onKeyDown} aria-label="Add a transaction">
    <label className="field"><span>Kind</span><select ref={first} className="select" value={kind} onChange={event => setKind(event.target.value as Kind)}>{KINDS.map(item => <option key={item.value} value={item.value}>{item.label}</option>)}</select></label>
    <label className="field"><span>Date</span><input className="input" type="date" max={today()} value={day} onChange={event => setDay(event.target.value)} /></label>
    <label className="field pf-manual__symbol"><span>Symbol{needsSymbol ? '' : ' (optional)'}</span><input className="input" list="pf-manual-symbols" placeholder="7203.T, AAPL" value={symbol} onChange={event => pickSymbol(event.target.value)} /></label>
    <datalist id="pf-manual-symbols">{symbols.map(([item, itemCurrency]) => <option key={item} value={item}>{itemCurrency}</option>)}</datalist>
    <label className="field"><span>Currency</span><select className="select" value={currency} onChange={event => setCurrency(event.target.value)}>{currencies.map(item => <option key={item}>{item}</option>)}</select></label>
    {trade ? <>
      <label className="field"><span>Quantity</span><input className="input" inputMode="decimal" value={quantity} onChange={event => setQuantity(event.target.value)} /></label>
      <label className="field"><span>Price</span><input className="input" inputMode="decimal" value={price} onChange={event => setPrice(event.target.value)} /></label>
      <label className="field"><span>Commission</span><input className="input" inputMode="decimal" placeholder="0" value={commission} onChange={event => setCommission(event.target.value)} /></label>
    </> : <label className="field"><span>Amount</span><input className="input" inputMode="decimal" value={amount} onChange={event => setAmount(event.target.value)} /></label>}
    <label className="field pf-manual__description"><span>Description (optional)</span><input className="input" maxLength={300} value={description} onChange={event => setDescription(event.target.value)} /></label>
    <div className="pf-manual__actions">
      <button type="submit" className="button button--primary button--small" disabled={!valid || add.isPending}><Plus aria-hidden="true" />{add.isPending ? 'Adding…' : 'Add'}</button>
      <button type="button" className="icon-button" aria-label="Close the form" title="Close (Esc)" onClick={onClose}><X /></button>
    </div>
    <p className="pf-manual__hint">
      {total != null ? <>Cash {kind === 'buy' ? 'out' : 'in'}: <strong className="mono">{total.toLocaleString(undefined, { maximumFractionDigits: 2 })} {currency}</strong> · </> : null}
      Amounts are positive; the kind sets the direction. Japanese shares use the code with .T (7203.T). Stored under “Manual entries”, which you can delete like any import. <kbd>Enter</kbd> adds, <kbd>Esc</kbd> closes.
      {add.isError && <span className="form-error" role="alert"> {(add.error as Error).message}</span>}
    </p>
  </form>
}
