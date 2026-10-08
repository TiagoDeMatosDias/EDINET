import { Minus, Plus, RotateCcw, Trash2 } from 'lucide-react'
import { useMemo, useRef, useState, type KeyboardEvent } from 'react'

import { HotkeyKbd } from '../../../hotkeys/HotkeyKbd'
import { useHotkeyScope } from '../../../hotkeys/useHotkeyScope'
import { useHotkeyText } from '../../../hotkeys/useHotkeyText'
import { optionsScope } from '../researchHotkeys'
import { usePersistentState } from '../../../hooks/usePersistentState'
import { formatMetricValue } from '../../../metrics'
import { defaultRate, formatNumber, formatPercent, localToday } from '../researchModel'
import { moveCursorKey, useListCursor } from '../useListCursor'
import { NumberField } from './NumberField'
import { Segmented } from './Segmented'
import { PayoffChart, SensitivityChart, VolatilityHistoryChart } from './OptionCharts'
import {
  addDays,
  binomialPrice,
  daysBetween,
  blackScholes,
  expectedMove,
  impliedVolatility,
  legPremium,
  roundToStep,
  STRATEGY_PRESETS,
  strategyFees,
  strategyGreeks,
  strikeLadder,
  strikeStep,
  summarizeStrategy,
  type Leg,
  type LegKind,
  type OptionKind,
  type TradingFees,
} from './optionsModel'
import { PRICE_FORMAT, usePricingInputs } from '../researchQueries'
import { PricingCompany } from './PricingCompany'

const DAY_CHOICES = [30, 60, 90, 180, 365, 730]

interface Assumptions {
  spot: number
  strike: number
  days: number
  volatility: number
  /** Which estimate the volatility came from, or ``manual``. */
  volSource: string
  rate: number
  dividendYield: number
  preset: string
  legs: Leg[]
}

const MANUAL: Assumptions = { spot: 100, strike: 100, days: 90, volatility: 0.25, volSource: 'manual', rate: 0.01, dividendYield: 0, preset: 'long-call', legs: STRATEGY_PRESETS[0].legs(100, 2) }

function presetLegs(preset: string, strike: number, spot: number) {
  return (STRATEGY_PRESETS.find(item => item.key === preset) ?? STRATEGY_PRESETS[0]).legs(strike, strikeStep(spot))
}

export function OptionsView({ companyCode, onCompany, active }: { companyCode: string; onCompany: (code: string) => void; active: boolean }) {
  const inputs = usePricingInputs(companyCode)
  const data = companyCode ? inputs.data : undefined
  const currency = data?.currency.price ?? null
  const [rates, setRates] = usePersistentState<Record<string, number>>('research.rates', {})
  const [fees, setFees] = usePersistentState<TradingFees>('research.options.fees', { mode: 'contract', perContract: 0, percent: 0, contractSize: 100, roundTrip: false })
  const feeMode = fees.mode ?? 'contract'
  const today = useMemo(() => localToday(), [])
  const rateFor = (code: string | null) => rates[code ?? 'JPY'] ?? defaultRate(code)
  const [state, setState] = useState<Assumptions>(MANUAL)
  const [marketPrice, setMarketPrice] = useState<number | null>(null)
  const [marketKind, setMarketKind] = useState<OptionKind>('call')
  const [ladderFocus, setLadderFocus] = useState(0)
  const pickerRef = useRef<HTMLInputElement>(null)
  const daysRef = useRef<HTMLInputElement>(null)
  const marketRef = useRef<HTMLInputElement>(null)
  const presetRef = useRef<HTMLSelectElement>(null)
  const [applied, setApplied] = useState('')

  const estimates = useMemo(() => (data?.volatility.estimates ?? []).filter(item => item.value != null), [data])
  const fromCompany = (): Assumptions | null => {
    if (!data || data.spot == null) return null
    const preferred = estimates.find(item => item.window === '1Y') ?? estimates[0]
    const strike = roundToStep(data.spot, strikeStep(data.spot))
    return {
      spot: data.spot,
      strike,
      days: state.days,
      volatility: preferred?.value ?? state.volatility,
      volSource: preferred?.window ?? 'manual',
      rate: rateFor(data.currency.price ?? null),
      dividendYield: data.dividend_yield ?? 0,
      preset: state.preset,
      legs: presetLegs(state.preset, strike, data.spot),
    }
  }
  // Prefill once per company, while rendering, so the first paint already shows its numbers; later edits are the user's.
  const loadedCode = data?.company.company_code ?? ''
  if (loadedCode && applied !== loadedCode) {
    setApplied(loadedCode)
    const next = fromCompany()
    if (next) { setState(next); setMarketPrice(null) }
  }

  const set = (patch: Partial<Assumptions>) => setState(current => {
    const next = { ...current, ...patch }
    if ((patch.strike !== undefined || patch.spot !== undefined || patch.preset !== undefined) && next.preset !== 'custom' && patch.legs === undefined) next.legs = presetLegs(next.preset, next.strike, next.spot)
    return next
  })
  const setRate = (rate: number) => { set({ rate }); setRates({ ...rates, [currency ?? 'JPY']: rate }) }
  const years = state.days / 365
  const market = { spot: state.spot, rate: state.rate, dividendYield: state.dividendYield, volatility: state.volatility }
  const optionInput = { ...market, strike: state.strike, years }
  const call = blackScholes('call', optionInput)
  const put = blackScholes('put', optionInput)
  const american = useMemo(() => ({
    call: binomialPrice('call', { ...market, strike: state.strike, years }),
    put: binomialPrice('put', { ...market, strike: state.strike, years }),
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }), [state.spot, state.strike, state.days, state.rate, state.dividendYield, state.volatility])
  const implied = marketPrice != null ? impliedVolatility(marketKind, marketPrice, { spot: state.spot, strike: state.strike, years, rate: state.rate, dividendYield: state.dividendYield }) : null
  const totalFees = strategyFees(state.legs, fees, years, market)
  // One leg's fees per share, for the single call and put (a percentage fee depends on each one's premium).
  const callFee = strategyFees([{ kind: 'call', side: 1, quantity: 1, strike: state.strike }], fees, years, market)
  const putFee = strategyFees([{ kind: 'put', side: 1, quantity: 1, strike: state.strike }], fees, years, market)
  const legFee = Math.max(callFee, putFee)
  const summary = useMemo(() => summarizeStrategy(state.legs, years, market, totalFees),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [state.legs, state.spot, state.days, state.rate, state.dividendYield, state.volatility, totalFees])
  const greeks = strategyGreeks(state.legs, years, market)
  const move = expectedMove(state.spot, state.volatility, years)
  const step = strikeStep(state.spot)
  const ladder = useMemo(() => {
    const strikes = strikeLadder(state.spot, 15)
    return strikes.includes(state.strike) ? strikes : [...strikes, state.strike].sort((a, b) => a - b)
  }, [state.spot, state.strike])
  const ladderIndex = Math.max(0, ladder.indexOf(state.strike))
  const ladderBody = useListCursor<HTMLTableSectionElement>(ladderIndex, ladderFocus)
  const money = (value: number | null | undefined) => formatMetricValue(PRICE_FORMAT, value, { price: currency })
  const sources = [...estimates.map(item => item.window), ...(implied != null ? ['implied'] : [])]
  const chooseSource = (source: string) => {
    if (source === 'implied' && implied != null) set({ volatility: implied, volSource: 'implied' })
    const estimate = estimates.find(item => item.window === source)
    if (estimate?.value != null) set({ volatility: estimate.value, volSource: source })
  }
  const focus = (element: HTMLElement | null) => { element?.focus(); element?.scrollIntoView?.({ block: 'center', behavior: 'smooth' }) }

  useHotkeyScope(optionsScope, {
    company: () => focus(pickerRef.current),
    manual: () => onCompany(''),
    'strike-down': () => set({ strike: Math.max(step, roundToStep(state.strike, step) - step) }),
    'strike-up': () => set({ strike: roundToStep(state.strike, step) + step }),
    volatility: () => { if (sources.length) chooseSource(sources[(sources.indexOf(state.volSource) + 1) % sources.length]) },
    days: () => focus(daysRef.current),
    market: () => focus(marketRef.current),
    strategy: () => focus(presetRef.current),
    ladder: () => setLadderFocus(value => value + 1),
    reset: () => { const next = fromCompany(); if (next) { setState(next); setMarketPrice(null) } },
  }, { enabled: active })
  const companyKey = useHotkeyText(optionsScope.byId.company)
  const manualKey = useHotkeyText(optionsScope.byId.manual)

  const onLadderKeyDown = (event: KeyboardEvent<HTMLTableSectionElement>) => {
    if (moveCursorKey(event, ladderIndex, ladder.length, next => set({ strike: ladder[next] }))) return
  }
  const updateLeg = (index: number, patch: Partial<Leg>) => set({ preset: 'custom', legs: state.legs.map((leg, position) => position === index ? { ...leg, ...patch } : leg) })
  const noPrice = Boolean(companyCode && data && data.spot == null)

  return <div className="rs-pricing">
    <section className="panel rs-pricing__inputs" aria-label="Option inputs">
      <PricingCompany code={companyCode} inputs={data} loading={inputs.isLoading} error={inputs.error} inputRef={pickerRef} onChange={onCompany} keys={{ company: companyKey, manual: manualKey }} />
      {noPrice && <p className="callout callout--warning">This company has no stored price, so the inputs below are manual.</p>}
      <div className="rs-fields">
        <NumberField label="Spot" value={state.spot} step={step} min={0.0001} onChange={spot => set({ spot })} digits={2} hint={data?.spot != null && state.spot !== data.spot ? <button type="button" className="text-button" onClick={() => set({ spot: data.spot as number })}>Latest {money(data.spot)}</button> : currency ?? undefined} />
        <NumberField label="Strike" value={state.strike} step={step} min={0.0001} onChange={strike => set({ strike })} digits={2} hint={`${formatPercent(state.strike / state.spot - 1)} from spot · step ${step}`}>
          <button type="button" className="icon-button" aria-label="Lower strike" title="Lower strike ([)" onClick={() => set({ strike: Math.max(step, roundToStep(state.strike, step) - step) })}><Minus /></button>
          <button type="button" className="icon-button" aria-label="Higher strike" title="Higher strike (])" onClick={() => set({ strike: roundToStep(state.strike, step) + step })}><Plus /></button>
        </NumberField>
        <NumberField label="Days to expiry" value={state.days} min={1} max={3650} step={1} digits={0} inputRef={daysRef} onChange={days => set({ days: Math.round(days) })} hint={<span className="rs-quick">{DAY_CHOICES.map(days => <button key={days} type="button" aria-pressed={state.days === days} onClick={() => set({ days })}>{days < 365 ? `${days}d` : `${days / 365}y`}</button>)}</span>} />
        <label className="rs-field">
          <span className="rs-field__label">Expiry date</span>
          <span className="rs-field__control">
            <input
              type="date"
              className="input"
              aria-label="Expiry date"
              min={addDays(today, 1)}
              max={addDays(today, 3650)}
              value={addDays(today, state.days)}
              onChange={event => { const days = daysBetween(today, event.target.value); if (days != null && days >= 1 && days <= 3650) set({ days }) }}
            />
          </span>
          <small className="rs-field__hint">{new Date(`${addDays(today, state.days)}T00:00:00Z`).toLocaleDateString('en-GB', { weekday: 'long', timeZone: 'UTC' })}, {formatNumber(years, 2)} years</small>
        </label>
        <NumberField label="Volatility" value={state.volatility} scale={100} step={1} digits={1} min={0.0001} suffix="%" onChange={volatility => set({ volatility, volSource: 'manual' })} hint={sources.length ? <span className="rs-quick" title="Realised volatility of daily returns over each window (V cycles)">{sources.map(source => {
          const value = source === 'implied' ? implied : estimates.find(item => item.window === source)?.value
          return <button key={source} type="button" aria-pressed={state.volSource === source} onClick={() => chooseSource(source)}>{source === 'implied' ? 'Implied' : source} <small>{formatPercent(value, 0)}</small></button>
        })}</span> : 'Annualised; no price history to estimate it'} />
        <NumberField label="Risk-free rate" value={state.rate} scale={100} step={0.25} digits={2} suffix="%" onChange={setRate} hint={`Assumption${currency ? ` for ${currency}` : ''}; use the government yield for the term`} />
        <NumberField label="Dividend yield" value={state.dividendYield} scale={100} step={0.25} digits={2} min={0} suffix="%" onChange={dividendYield => set({ dividendYield })} hint={data?.dividend_yield != null ? `Trailing ${formatPercent(data.dividend_yield, 2)}, as continuous` : 'Continuous yield'} />
        <NumberField label="Market price" value={marketPrice} step={step / 10} min={0} inputRef={marketRef} onChange={value => setMarketPrice(value || null)} hint={marketPrice == null ? 'Optional: solves for implied volatility' : implied == null ? 'Outside no-arbitrage bounds' : <>Implied {formatPercent(implied)} · <button type="button" className="text-button" onClick={() => chooseSource('implied')}>Use</button></>}>
          <select className="select" aria-label="Market price is for a" value={marketKind} onChange={event => setMarketKind(event.target.value as OptionKind)}><option value="call">Call</option><option value="put">Put</option></select>
        </NumberField>
      </div>
      <fieldset className="rs-source rs-fees">
        <legend>Trading fees</legend>
        <Segmented label="Fee type" value={feeMode} onChange={mode => setFees({ ...fees, mode })} options={[{ value: 'contract', label: 'Set amount', title: 'A set amount per contract' }, { value: 'percent', label: '% of value', title: 'A share of each trade’s premium' }]} />
        {feeMode === 'contract'
          ? <NumberField className="rs-field--inline" label="Fee per contract" value={fees.perContract} step={50} digits={2} min={0} suffix={currency ?? undefined} onChange={perContract => setFees({ ...fees, perContract })} />
          : <NumberField className="rs-field--inline" label="Fee rate" value={fees.percent ?? 0} scale={100} step={0.05} digits={3} min={0} max={20} suffix="% of premium" onChange={percent => setFees({ ...fees, percent })} />}
        <NumberField className="rs-field--inline" label="Contract size" value={fees.contractSize} step={1} digits={0} min={1} suffix="shares" onChange={contractSize => setFees({ ...fees, contractSize: Math.max(1, Math.round(contractSize)) })} />
        <label><input type="checkbox" checked={fees.roundTrip} onChange={event => setFees({ ...fees, roundTrip: event.target.checked })} />Also when closing or exercising</label>
        <small className="rs-dim">{legFee <= 0 ? 'Fees reduce every profit and move the breakevens.'
          : feeMode === 'percent' ? `Call ${money(callFee)}, put ${money(putFee)} a share; a share leg pays the rate on the share price.${fees.roundTrip ? ' Closing is charged on today’s value.' : ''}`
            : `${money(legFee)} a share for each leg; a share leg counts as one contract.`}</small>
      </fieldset>
      {companyCode && data && <button type="button" className="text-button rs-reset" onClick={() => { const next = fromCompany(); if (next) { setState(next); setMarketPrice(null) } }}><RotateCcw aria-hidden="true" />Reset to {data.company.company_name}’s data <span aria-hidden="true"><HotkeyKbd hotkey={optionsScope.byId.reset} /></span></button>}
    </section>

    <section className="panel rs-pricing__values" aria-label="Option values">
      <header className="rs-panel__header"><h3>Black–Scholes</h3><span className="rs-panel__meta">European exercise · strike {money(state.strike)} · {state.days} days</span></header>
      <table className="rs-table rs-values">
        <thead><tr><th scope="col" /><th scope="col" className="num">Call</th><th scope="col" className="num">Put</th></tr></thead>
        <tbody>
          <tr className="rs-values__price"><th scope="row">Price</th><td className="num">{money(call.price)}</td><td className="num">{money(put.price)}</td></tr>
          <tr><th scope="row" title="Price per share as a share of spot">Premium</th><td className="num">{formatPercent(call.price / state.spot, 2)}</td><td className="num">{formatPercent(put.price / state.spot, 2)}</td></tr>
          <tr><th scope="row" title="What exercising now would be worth">Intrinsic value</th><td className="num">{formatNumber(Math.max(state.spot - state.strike, 0))}</td><td className="num">{formatNumber(Math.max(state.strike - state.spot, 0))}</td></tr>
          <tr><th scope="row" title="Price above intrinsic value: what time and volatility add">Time value</th><td className="num">{formatNumber(call.price - Math.max(state.spot - state.strike, 0))}</td><td className="num">{formatNumber(put.price - Math.max(state.strike - state.spot, 0))}</td></tr>
          <tr><th scope="row" title="Change in option price per 1 change in the share price">Delta</th><td className="num">{formatNumber(call.delta, 3)}</td><td className="num">{formatNumber(put.delta, 3)}</td></tr>
          <tr><th scope="row" title="Change in delta per 1 change in the share price">Gamma</th><td className="num">{formatNumber(call.gamma, 4)}</td><td className="num">{formatNumber(put.gamma, 4)}</td></tr>
          <tr><th scope="row" title="Change in price per 1 percentage point of volatility">Vega <small>/ vol pt</small></th><td className="num">{formatNumber(call.vega)}</td><td className="num">{formatNumber(put.vega)}</td></tr>
          <tr><th scope="row" title="Change in price per calendar day, all else equal">Theta <small>/ day</small></th><td className="num">{formatNumber(call.theta)}</td><td className="num">{formatNumber(put.theta)}</td></tr>
          <tr><th scope="row" title="Change in price per 1 percentage point of the interest rate">Rho <small>/ 1%</small></th><td className="num">{formatNumber(call.rho)}</td><td className="num">{formatNumber(put.rho)}</td></tr>
          <tr><th scope="row" title="Risk-neutral probability of finishing in the money">Chance in the money</th><td className="num">{formatPercent(call.probabilityInTheMoney)}</td><td className="num">{formatPercent(put.probabilityInTheMoney)}</td></tr>
          <tr><th scope="row" title={legFee > 0 ? 'Including the trading fees' : undefined}>Breakeven at expiry{legFee > 0 && <small> with fees</small>}</th><td className="num">{money(state.strike + call.price + callFee)}</td><td className="num">{money(state.strike - put.price - putFee)}</td></tr>
          <tr><th scope="row" title={`Price × ${fees.contractSize} shares${legFee > 0 ? ', plus fees' : ''}`}>Per contract{legFee > 0 && <small> with fees</small>}</th><td className="num">{money((call.price + callFee) * fees.contractSize)}</td><td className="num">{money((put.price + putFee) * fees.contractSize)}</td></tr>
          <tr><th scope="row" title="Cox–Ross–Rubinstein tree with 200 steps, allowing exercise at any time">American value</th><td className="num">{money(american.call)}</td><td className="num">{money(american.put)}</td></tr>
          <tr><th scope="row" title="What the right to exercise early adds (binomial tree less Black–Scholes)">Early exercise adds</th><td className="num">{formatNumber(Math.max(american.call - call.price, 0))}</td><td className="num">{formatNumber(Math.max(american.put - put.price, 0))}</td></tr>
        </tbody>
      </table>
      <p className="rs-hint">Put–call parity: call − put = {formatNumber(call.price - put.price)} = S·e<sup>−qT</sup> − K·e<sup>−rT</sup>. One standard deviation by expiry: {money(move.low)} to {money(move.high)}.</p>
    </section>

    <section className="panel rs-pricing__strategy" aria-label="Strategy">
      <header className="rs-panel__header">
        <h3>Strategy</h3>
        <select ref={presetRef} className="select" aria-label="Strategy preset" value={state.preset} onChange={event => set({ preset: event.target.value })}>
          {STRATEGY_PRESETS.map(preset => <option key={preset.key} value={preset.key}>{preset.label}</option>)}
          {state.preset === 'custom' && <option value="custom">Custom</option>}
        </select>
        <span aria-hidden="true"><HotkeyKbd hotkey={optionsScope.byId.strategy} /></span>
        <span className="rs-panel__meta">{STRATEGY_PRESETS.find(preset => preset.key === state.preset)?.description ?? 'Your own combination of legs.'}</span>
      </header>
      <div className="rs-table-scroll"><table className="rs-table rs-legs">
        <thead><tr><th scope="col">Leg</th><th scope="col">Side</th><th scope="col" className="num">Qty</th><th scope="col" className="num">Strike</th><th scope="col" className="num">Premium</th><th scope="col"><span className="sr-only">Remove</span></th></tr></thead>
        <tbody>
          {state.legs.map((leg, index) => <tr key={index}>
            <td><select className="select" aria-label={`Leg ${index + 1} instrument`} value={leg.kind} onChange={event => updateLeg(index, { kind: event.target.value as LegKind, strike: leg.strike || state.strike })}><option value="call">Call</option><option value="put">Put</option><option value="stock">Shares</option></select></td>
            <td><select className="select" aria-label={`Leg ${index + 1} side`} value={leg.side} onChange={event => updateLeg(index, { side: Number(event.target.value) as 1 | -1 })}><option value={1}>Buy</option><option value={-1}>Sell</option></select></td>
            <td className="num"><input className="input" inputMode="decimal" aria-label={`Leg ${index + 1} quantity`} value={leg.quantity} onChange={event => { const quantity = Number(event.target.value); if (Number.isFinite(quantity) && quantity >= 0) updateLeg(index, { quantity }) }} /></td>
            <td className="num">{leg.kind === 'stock' ? <span className="rs-dim">—</span> : <input className="input" inputMode="decimal" aria-label={`Leg ${index + 1} strike`} value={leg.strike} onChange={event => { const strike = Number(event.target.value); if (Number.isFinite(strike) && strike > 0) updateLeg(index, { strike }) }} />}</td>
            <td className="num">{money(legPremium(leg, years, market))}</td>
            <td><button type="button" className="icon-button" aria-label={`Remove leg ${index + 1}`} disabled={state.legs.length === 1} onClick={() => set({ preset: 'custom', legs: state.legs.filter((_, position) => position !== index) })}><Trash2 /></button></td>
          </tr>)}
        </tbody>
      </table></div>
      <div className="rs-legs__add">
        <button type="button" className="text-button" onClick={() => set({ preset: 'custom', legs: [...state.legs, { kind: 'call', side: 1, quantity: 1, strike: state.strike }] })}>+ Add leg</button>
      </div>
      <dl className="rs-summary">
        <div><dt>{summary.cost >= 0 ? 'Net cost' : 'Net credit'}</dt><dd>{money(Math.abs(summary.cost))}</dd></div>
        <div><dt title="For the whole position, per share of the underlying">Fees</dt><dd>{summary.fees > 0 ? money(summary.fees) : '—'}</dd></div>
        <div><dt title={`${fees.contractSize} shares a contract, fees included`}>{summary.cost + summary.fees >= 0 ? 'Cost per contract' : 'Credit per contract'}</dt><dd>{money(Math.abs(summary.cost + summary.fees) * fees.contractSize)}</dd></div>
        <div><dt>Max profit</dt><dd>{summary.maxProfit == null ? 'Unlimited' : money(summary.maxProfit)}</dd></div>
        <div><dt>Max loss</dt><dd>{summary.maxLoss == null ? 'Unlimited' : money(summary.maxLoss)}</dd></div>
        <div><dt>Breakeven</dt><dd>{summary.breakevens.length ? summary.breakevens.map(value => money(value)).join(' · ') : '—'}</dd></div>
        <div><dt title="Risk-neutral chance the position is profitable at expiry">Chance of profit</dt><dd>{formatPercent(summary.probabilityOfProfit)}</dd></div>
        <div><dt>Delta</dt><dd>{formatNumber(greeks.delta, 2)}</dd></div>
        <div><dt>Gamma</dt><dd>{formatNumber(greeks.gamma, 4)}</dd></div>
        <div><dt>Vega · Theta / day</dt><dd>{formatNumber(greeks.vega)} · {formatNumber(greeks.theta)}</dd></div>
      </dl>
      <PayoffChart legs={state.legs} years={years} market={market} fees={totalFees} breakevens={summary.breakevens} move={move} currency={currency} />
    </section>

    <section className="panel rs-pricing__ladder" aria-label="Strike ladder">
      <header className="rs-panel__header"><h3>Strike ladder <span aria-hidden="true"><HotkeyKbd hotkey={optionsScope.byId.ladder} /></span></h3><span className="rs-panel__meta">{state.days} days · vol {formatPercent(state.volatility)}</span></header>
      <div className="rs-table-scroll">
        <table className="rs-table rs-ladder">
          <thead><tr><th scope="col" className="num">Call Δ</th><th scope="col" className="num">Call</th><th scope="col" className="num">Strike</th><th scope="col" className="num">Put</th><th scope="col" className="num">Put Δ</th><th scope="col" className="num" title="Strike against spot">Moneyness</th></tr></thead>
          <tbody ref={ladderBody} onKeyDown={onLadderKeyDown}>
            {ladder.map((strike, index) => {
              const values = { call: blackScholes('call', { ...market, strike, years }), put: blackScholes('put', { ...market, strike, years }) }
              const isCursor = index === ladderIndex
              const itmCall = strike < state.spot
              return <tr key={strike} data-cursor={isCursor} tabIndex={isCursor ? 0 : -1} aria-selected={isCursor} className={isCursor ? 'is-cursor' : undefined} onClick={() => set({ strike })}>
                <td className={itmCall ? 'num is-itm' : 'num'}>{formatNumber(values.call.delta, 2)}</td>
                <td className={itmCall ? 'num is-itm' : 'num'}>{formatNumber(values.call.price)}</td>
                <th scope="row" className="num">{formatNumber(strike, step < 1 ? 2 : 0)}</th>
                <td className={!itmCall ? 'num is-itm' : 'num'}>{formatNumber(values.put.price)}</td>
                <td className={!itmCall ? 'num is-itm' : 'num'}>{formatNumber(values.put.delta, 2)}</td>
                <td className="num rs-dim">{formatPercent(strike / state.spot - 1, 0)}</td>
              </tr>
            })}
          </tbody>
        </table>
      </div>
    </section>

    <section className="panel" aria-label="Sensitivity">
      <SensitivityChart input={optionInput} implied={implied} currency={currency} />
    </section>
    {data && data.volatility.history.length > 1 && <section className="panel" aria-label="Volatility history">
      <VolatilityHistoryChart history={data.volatility.history} chosen={state.volatility} />
    </section>}
  </div>
}
