import { RotateCcw } from 'lucide-react'
import { useMemo, useRef, useState } from 'react'

import { Tip } from '../../../components/Tooltip'
import { HotkeyKbd } from '../../../hotkeys/HotkeyKbd'
import { useHotkeyScope } from '../../../hotkeys/useHotkeyScope'
import { useHotkeyText } from '../../../hotkeys/useHotkeyText'
import { bondsScope } from '../researchHotkeys'
import { usePersistentState } from '../../../hooks/usePersistentState'
import { formatMetricValue } from '../../../metrics'
import { abbreviate } from '../../comparison/comparisonModel'
import { divergingColor } from '../../portfolio/chartTheme'
import { defaultRate, formatDay, formatNumber, formatPercent, formatSignedPercent, localToday } from '../researchModel'
import type { PricingInputs } from '../researchTypes'
import { DefaultTimelineChart, OutcomesChart, PriceYieldChart } from './BondCharts'
import {
  accruedInterest,
  hazardFromProbability,
  hazardFromSpread,
  impliedHazard,
  outcomes,
  priceWithCredit,
  riskMeasures,
  scenarioChange,
  yieldFromPrice,
  type BondTerms,
} from './bondModel'
import { creditProfile, type CreditProfile, type ZScore } from './creditModel'
import { NumberField } from './NumberField'
import { Segmented } from './Segmented'
import { addDays, daysBetween } from './optionsModel'
import { usePricingInputs } from '../researchQueries'
import { PricingCompany } from './PricingCompany'
import { useBondDetail } from '../../bonds/bondQueries'
import { bondMarketHref } from '../../bonds/bondFormat'
import type { Bond } from '../../bonds/bondTypes'
import { Link } from 'react-router-dom'

type CreditSource = 'merton' | 'spread' | 'probability' | 'none'

interface BondState {
  face: number
  coupon: number
  frequency: number
  years: number
  riskFree: number
  source: CreditSource
  spread: number
  annualDefault: number
  recovery: number
  equityWindow: string
}

const MANUAL: BondState = { face: 100, coupon: 0.02, frequency: 2, years: 5, riskFree: 0.01, source: 'spread', spread: 0.01, annualDefault: 0.01, recovery: 0.4, equityWindow: '1Y' }

const RATE_SHIFTS = [-0.02, -0.01, -0.005, 0, 0.005, 0.01, 0.02]
const DAYS_PER_YEAR = 365.25
const SPREAD_SHIFTS = [-0.005, 0, 0.005, 0.01, 0.03]

function bp(value: number) {
  return `${value > 0 ? '+' : ''}${Math.round(value * 10_000)}`
}

/** A stored bond's terms for the calculator: its coupon, time to maturity (or first call), today's JGB yield, and its spread at issue. */
function bondTerms(bond: Bond): Partial<BondState> {
  const terms: Partial<BondState> = {}
  if (bond.coupon != null) terms.coupon = bond.coupon
  if (bond.horizon != null && bond.horizon > 0) terms.years = Math.round(bond.horizon * 100) / 100
  if (bond.frequency) terms.frequency = bond.frequency
  if (bond.jgb_now != null) terms.riskFree = bond.jgb_now
  if (bond.issue_spread != null) { terms.source = 'spread'; terms.spread = Math.max(bond.issue_spread, 0) }
  return terms
}

export function BondsView({ companyCode, bondId = '', onCompany, active }: { companyCode: string; bondId?: string; onCompany: (code: string) => void; active: boolean }) {
  const inputs = usePricingInputs(companyCode)
  const data = companyCode ? inputs.data : undefined
  const bondQuery = useBondDetail(bondId)
  const bond = bondId && bondQuery.data?.bond.edinet_code === companyCode ? bondQuery.data.bond : null
  const currency = data?.currency.reporting ?? data?.currency.price ?? null
  const [rates, setRates] = usePersistentState<Record<string, number>>('research.rates', {})
  const [state, setState] = useState<BondState>(MANUAL)
  const [marketPrice, setMarketPrice] = useState<number | null>(null)
  // Commission or dealer markup paid when buying: a share of face value, or a set amount per bond.
  const [feeRate, setFeeRate] = usePersistentState<number>('research.bonds.fee', 0)
  const [feeMode, setFeeMode] = usePersistentState<'percent' | 'amount'>('research.bonds.feeMode', 'percent', ['percent', 'amount'])
  const [feeAmount, setFeeAmount] = usePersistentState<number>('research.bonds.feeAmount', 0)
  const today = useMemo(() => localToday(), [])
  const pickerRef = useRef<HTMLInputElement>(null)
  const couponRef = useRef<HTMLInputElement>(null)
  const yieldRef = useRef<HTMLInputElement>(null)
  const priceRef = useRef<HTMLInputElement>(null)
  const maturityRef = useRef<HTMLInputElement>(null)
  const [applied, setApplied] = useState('')

  const fromCompany = (): BondState | null => {
    if (!data) return null
    const cost = data.credit.cost_of_debt
    const company: BondState = {
      ...state,
      // The company's own borrowing cost, rounded, is a natural coupon for a new bond.
      coupon: cost != null && cost > 0 && cost < 0.3 ? Math.round(cost * 2000) / 2000 : state.coupon,
      riskFree: rates[data.currency.price ?? 'JPY'] ?? defaultRate(data.currency.price),
      source: data.market_cap ? 'merton' : 'spread',
    }
    // A bond chosen in the bond market brings its own terms.
    return bond ? { ...company, ...bondTerms(bond) } : company
  }
  // Prefill once per company (and bond), while rendering, so the first paint already shows its numbers; later edits are the user's.
  const loadedCode = data?.company.company_code ?? ''
  const loadedKey = loadedCode && (!bondId || bond || bondQuery.isError) ? `${loadedCode}|${bond?.bond_id ?? ''}` : ''
  if (loadedKey && applied !== loadedKey) {
    setApplied(loadedKey)
    const next = fromCompany()
    if (next) { setState(next); setMarketPrice(null) }
  }
  const set = (patch: Partial<BondState>) => setState(current => ({ ...current, ...patch }))
  const setRiskFree = (riskFree: number) => { set({ riskFree }); setRates({ ...rates, [data?.currency.price ?? 'JPY']: riskFree }) }

  const terms: BondTerms = { face: state.face, coupon: state.coupon, frequency: state.frequency, years: state.years }
  const profile = useMemo(() => data ? creditProfile(data, state) : null,
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [data, state.riskFree, state.years, state.equityWindow])
  const mertonAvailable = Boolean(profile?.atMaturity) && !data?.financial
  const source: CreditSource = state.source === 'merton' && !mertonAvailable ? 'spread' : state.source
  const hazard = source === 'merton' && profile?.atMaturity ? hazardFromProbability(profile.atMaturity.probabilityOfDefault, Math.max(state.years, 0.25))
    : source === 'spread' ? hazardFromSpread(state.spread, state.recovery)
      : source === 'probability' ? hazardFromProbability(state.annualDefault, 1)
        : 0
  const credit = { riskFree: state.riskFree, hazard, recovery: state.recovery }
  const priced = priceWithCredit(terms, credit)
  const risk = riskMeasures(terms, priced.yield ?? state.riskFree)
  const fee = feeMode === 'amount' ? Math.max(feeAmount, 0) : Math.max(feeRate, 0) * state.face
  const maturityDate = addDays(today, state.years * DAYS_PER_YEAR)
  const yieldAfterFee = fee > 0 ? yieldFromPrice(terms, priced.price + fee) : priced.yield
  // Returns are on everything paid, fee included.
  const ends = useMemo(() => outcomes(terms, priced.price + fee, credit),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [state.face, state.coupon, state.frequency, state.years, priced.price, fee, hazard, state.recovery])
  const marketYield = marketPrice != null ? yieldFromPrice(terms, marketPrice) : null
  const marketYieldAfterFee = marketPrice != null && fee > 0 ? yieldFromPrice(terms, marketPrice + fee) : null
  const marketHazard = marketPrice != null ? impliedHazard(terms, marketPrice, state.riskFree, state.recovery) : null
  const amount = (value: number | null | undefined) => abbreviate(formatMetricValue({ label: '', group: '', format: 'money', currency: 'reporting' }, value, { reporting: currency }))
  const focus = (element: HTMLElement | null) => { element?.focus(); element?.scrollIntoView?.({ block: 'center', behavior: 'smooth' }) }
  const reset = () => { const next = fromCompany(); if (next) { setState(next); setMarketPrice(null) } }

  useHotkeyScope(bondsScope, {
    company: () => focus(pickerRef.current),
    manual: () => onCompany(''),
    shorter: () => set({ years: Math.max(1, Math.round(state.years) - 1) }),
    longer: () => set({ years: Math.min(50, Math.round(state.years) + 1) }),
    coupon: () => focus(couponRef.current),
    yield: () => focus(yieldRef.current),
    price: () => focus(priceRef.current),
    maturity: () => focus(maturityRef.current),
    fee: () => setFeeMode(feeMode === 'percent' ? 'amount' : 'percent'),
    reset,
  }, { enabled: active })
  const companyKey = useHotkeyText(bondsScope.byId.company)
  const manualKey = useHotkeyText(bondsScope.byId.manual)
  const feeKey = useHotkeyText(bondsScope.byId.fee)

  const issuer = data?.company.company_name
  const feeModeToggle = <Segmented label="Fee type" value={feeMode} onChange={setFeeMode} options={[{ value: 'percent', label: '%', title: `A share of face value (${feeKey})` }, { value: 'amount', label: 'Set', title: `A set amount per bond (${feeKey})` }]} />
  const feeHint = <>{fee > 0
    ? feeMode === 'percent' ? `${formatNumber(fee, 3)} per ${formatNumber(state.face, 0)} face, paid when buying` : `${formatPercent(fee / state.face, 3)} of face, paid when buying`
    : 'Commission or dealer markup paid when buying'}</>
  return <div className="rs-pricing rs-bonds">
    <section className="panel rs-pricing__inputs" aria-label="Bond inputs">
      <PricingCompany code={companyCode} inputs={data} loading={inputs.isLoading} error={inputs.error} inputRef={pickerRef} onChange={onCompany} keys={{ company: companyKey, manual: manualKey }} />
      {bond && <p className="rs-market-read">Pricing <strong>{bond.label}</strong>: {bond.horizon_to === 'call' ? 'to its first call' : 'to maturity'}, at the {formatNumber(Math.round((bond.issue_spread ?? 0) * 10_000), 0)} bp spread it was issued at over today’s JGB yield. <Link to={bondMarketHref(bond.bond_id, bond.edinet_code)}>Compare it with similar bonds</Link></p>}
      <div className="rs-fields">
        <NumberField label="Coupon" value={state.coupon} scale={100} step={0.125} digits={3} min={0} suffix="%" inputRef={couponRef} onChange={coupon => set({ coupon })} hint={data?.credit.cost_of_debt != null ? <>Company pays {formatPercent(data.credit.cost_of_debt, 2)} on its debt</> : 'Annual rate'}>
          <select className="select" aria-label="Coupons per year" value={state.frequency} onChange={event => set({ frequency: Number(event.target.value) })}><option value={1}>Annual</option><option value={2}>Semi-annual</option><option value={4}>Quarterly</option></select>
        </NumberField>
        <NumberField label="Years to maturity" value={state.years} step={1} digits={2} min={0.1} max={50} onChange={years => set({ years })} hint={<span className="rs-quick">{[1, 3, 5, 7, 10, 20].map(years => <button key={years} type="button" aria-pressed={state.years === years} onClick={() => set({ years })}>{years}y</button>)}</span>} />
        <label className="rs-field">
          <span className="rs-field__label">Maturity date</span>
          <span className="rs-field__control">
            <input
              ref={maturityRef}
              type="date"
              className="input"
              aria-label="Maturity date"
              min={addDays(today, Math.ceil(0.1 * DAYS_PER_YEAR))}
              max={addDays(today, 50 * DAYS_PER_YEAR)}
              value={maturityDate}
              onChange={event => { const days = daysBetween(today, event.target.value); if (days != null && days >= 0.1 * DAYS_PER_YEAR && days <= 50 * DAYS_PER_YEAR) set({ years: days / DAYS_PER_YEAR }) }}
            />
          </span>
          <small className="rs-field__hint">{formatNumber(state.years, 2)} years; the first coupon period is short when it is not a whole number of periods</small>
        </label>
        <NumberField label="Face value" value={state.face} step={100} digits={2} min={1} onChange={face => set({ face })} hint="Prices are per this amount" />
        <NumberField label="Risk-free yield" value={state.riskFree} scale={100} step={0.25} digits={2} suffix="%" inputRef={yieldRef} onChange={setRiskFree} hint={`Assumption${data?.currency.price ? ` for ${data.currency.price}` : ''}; the government yield for the maturity`} />
        <NumberField label="Recovery if default" value={state.recovery} scale={100} step={5} digits={0} min={0} max={100} suffix="%" onChange={recovery => set({ recovery })} hint="Share of face recovered; 40% is a common senior unsecured assumption" />
        {feeMode === 'percent'
          ? <NumberField label="Trading fee" value={feeRate} scale={100} step={0.05} digits={3} min={0} max={20} suffix="% of face" onChange={setFeeRate} hint={feeHint}>{feeModeToggle}</NumberField>
          : <NumberField label="Trading fee" value={feeAmount} step={0.1} digits={3} min={0} max={state.face} suffix="per bond" onChange={setFeeAmount} hint={feeHint}>{feeModeToggle}</NumberField>}
        <NumberField label="Market price" value={marketPrice} step={0.5} digits={3} min={0} inputRef={priceRef} onChange={value => setMarketPrice(value || null)} hint={marketPrice == null ? 'Optional clean price: solves for yield and implied default risk' : marketYield == null ? 'No yield gives this price' : `Yield ${formatPercent(marketYield, 2)}`} />
      </div>
      <fieldset className="rs-source">
        <legend>Default risk from</legend>
        <label className={mertonAvailable ? undefined : 'is-disabled'}><input type="radio" name="credit-source" checked={source === 'merton'} disabled={!mertonAvailable} onChange={() => set({ source: 'merton' })} />{issuer ? `${issuer}’s Merton model` : 'Company Merton model'}</label>
        <label><input type="radio" name="credit-source" checked={source === 'spread'} onChange={() => set({ source: 'spread' })} />A credit spread</label>
        <label><input type="radio" name="credit-source" checked={source === 'probability'} onChange={() => set({ source: 'probability' })} />An annual default rate</label>
        <label><input type="radio" name="credit-source" checked={source === 'none'} onChange={() => set({ source: 'none' })} />None</label>
        {source === 'spread' && <NumberField className="rs-field--inline" label="Spread" value={state.spread} scale={10_000} step={5} digits={0} min={0} suffix="bp" onChange={spread => set({ spread })} />}
        {source === 'probability' && <NumberField className="rs-field--inline" label="Annual default" value={state.annualDefault} scale={100} step={0.1} digits={2} min={0} max={99} suffix="%" onChange={annualDefault => set({ annualDefault })} />}
      </fieldset>
      {companyCode && data && <button type="button" className="text-button rs-reset" onClick={reset}><RotateCcw aria-hidden="true" />Reset to {data.company.company_name}’s data <span aria-hidden="true"><HotkeyKbd hotkey={bondsScope.byId.reset} /></span></button>}
    </section>

    <section className="panel rs-pricing__values" aria-label="Bond price">
      <header className="rs-panel__header"><h3>Price</h3><span className="rs-panel__meta">per {formatNumber(state.face, 0)} face · {state.frequency === 1 ? 'annual' : state.frequency === 2 ? 'semi-annual' : 'quarterly'} coupons</span></header>
      <dl className="rs-summary rs-summary--grid">
        <div className="is-key"><dt>Price with default risk</dt><dd>{formatNumber(priced.price, 3)}</dd></div>
        <div><dt>Without default risk</dt><dd>{formatNumber(priced.riskFreePrice, 3)}</dd></div>
        <div><dt>Yield to maturity</dt><dd>{formatPercent(priced.yield, 3)}</dd></div>
        <div><dt>Credit spread</dt><dd>{priced.spread == null ? '—' : `${Math.round(priced.spread * 10_000)} bp`}</dd></div>
        <div><dt title="Present value lost to expected defaults">Expected loss</dt><dd>{formatNumber(priced.expectedLoss, 3)}</dd></div>
        <div><dt>Default by maturity</dt><dd>{formatPercent(priced.cumulativeDefault, 2)}</dd></div>
        <div><dt title="Constant annual hazard rate">Annual default rate</dt><dd>{formatPercent(1 - Math.exp(-hazard), 2)}</dd></div>
        <div><dt title="Annual coupon ÷ clean price">Current yield</dt><dd>{formatPercent(priced.price > 0 ? state.coupon * state.face / priced.price : null, 2)}</dd></div>
        <div><dt title="Percentage price change for a 1% change in yield">Modified duration</dt><dd>{formatNumber(risk.modified)}</dd></div>
        <div><dt>Convexity</dt><dd>{formatNumber(risk.convexity, 1)}</dd></div>
        <div><dt title="Price change for a one basis point fall in yield">DV01</dt><dd>{formatNumber(risk.dv01, 4)}</dd></div>
        <div><dt title="Coupon earned since the last payment, paid on top of the clean price">Accrued</dt><dd>{formatNumber(accruedInterest(terms), 3)}</dd></div>
        <div><dt title="Clean price plus the trading fee">Price with fee</dt><dd>{fee > 0 ? formatNumber(priced.price + fee, 3) : '—'}</dd></div>
        <div><dt title="Yield to maturity on the price plus the fee, if the bond is repaid">Yield after fee</dt><dd>{fee > 0 ? formatPercent(yieldAfterFee, 3) : '—'}</dd></div>
      </dl>
      {marketPrice != null && <p className="rs-market-read">
        At {formatNumber(marketPrice, 3)}, the bond yields <strong>{formatPercent(marketYield, 2)}</strong>{marketYield != null && <>, {Math.round((marketYield - state.riskFree) * 10_000)} bp over the risk-free yield</>}{marketYieldAfterFee != null && <>; <strong>{formatPercent(marketYieldAfterFee, 2)}</strong> after the fee</>}.{' '}
        {marketHazard == null ? 'That is above the default-free price, so it implies no default risk.' : <>With {formatPercent(state.recovery, 0)} recovery, that implies a <strong>{formatPercent(1 - Math.exp(-marketHazard), 2)}</strong> annual default rate and <strong>{formatPercent(1 - Math.exp(-marketHazard * state.years), 1)}</strong> by maturity{source !== 'none' && <>, against {formatPercent(priced.cumulativeDefault, 1)} in this model</>}.</>}
      </p>}
      <PriceYieldChart terms={terms} yieldRate={priced.yield ?? state.riskFree} marketPrice={marketPrice} />
    </section>

    {data && profile && <CreditProfilePanel inputs={data} profile={profile} years={state.years} equityWindow={state.equityWindow} onEquityWindow={equityWindow => set({ equityWindow })} amount={amount} />}

    <section className="panel rs-pricing__ladder" aria-label="Scenarios">
      <header className="rs-panel__header"><h3>Scenarios</h3><span className="rs-panel__meta">Price change when rates and the spread move (bp)</span></header>
      <div className="rs-table-scroll">
        <table className="rs-table rs-scenarios">
          <thead><tr><th scope="col" className="rs-scenarios__corner"><span className="rs-dim">rate ↓<br />spread →</span></th>{SPREAD_SHIFTS.map(shift => <th key={shift} scope="col" className="num">{bp(shift)}</th>)}</tr></thead>
          <tbody>{RATE_SHIFTS.map(rateShift => <tr key={rateShift}>
            <th scope="row" className="num">{bp(rateShift)}</th>
            {SPREAD_SHIFTS.map(spreadShift => {
              const change = scenarioChange(terms, credit, rateShift, spreadShift)
              const tone = divergingColor(change, 0.15)
              return <td key={spreadShift} className={rateShift === 0 && spreadShift === 0 ? 'num is-base' : 'num'} style={tone ? { background: tone.background, color: tone.ink } : undefined}>{formatSignedPercent(change)}</td>
            })}
          </tr>)}</tbody>
        </table>
      </div>
      <p className="rs-hint">A spread change moves the default rate by spread ÷ (1 − recovery). Duration alone predicts {formatSignedPercent(-risk.modified * 0.01)} for +100 bp; convexity adds {formatSignedPercent(0.5 * risk.convexity * 0.0001)}.</p>
    </section>

    <section className="panel" aria-label="Default risk by year">
      <DefaultTimelineChart hazard={hazard} years={state.years} />
    </section>
    <section className="panel" aria-label="Outcomes">
      <OutcomesChart outcomes={ends} frequency={state.frequency} />
    </section>
  </div>
}

function zoneLabel(score: ZScore | null) {
  if (!score) return '—'
  return `${score.score.toFixed(2)} · ${score.zone === 'safe' ? 'safe' : score.zone === 'grey' ? 'grey zone' : 'distress'}`
}

function CreditProfilePanel({ inputs, profile, years, equityWindow, onEquityWindow, amount }: {
  inputs: PricingInputs
  profile: CreditProfile
  years: number
  equityWindow: string
  onEquityWindow: (window: string) => void
  amount: (value: number | null | undefined) => string
}) {
  const { credit } = inputs
  const lines = credit.lines
  const netDebt = credit.debt_total != null ? credit.debt_total - (lines.Cash ?? 0) : null
  const windows = inputs.volatility.estimates.filter(item => item.value != null && item.window !== 'EWMA')
  const zTip = (score: ZScore | null) => score ? score.components.map(part => `${part.label}: ${part.ratio.toFixed(3)} × ${part.weight}`).join('\n') : 'Needs every input line'
  return <section className="panel rs-credit" aria-label="Credit profile">
    <header className="rs-panel__header">
      <h3>Credit profile</h3>
      <span className="rs-panel__meta">{credit.period ? `Statements for the year ending ${formatDay(credit.period)}` : 'No balance sheet stored'}</span>
    </header>
    {inputs.financial && <p className="callout callout--warning rs-credit__note">{inputs.company.industry}: a bank's or insurer's liabilities are mostly deposits or policy reserves, so these models overstate its risk. Use a spread or default rate instead.</p>}
    <div className="rs-credit__grid">
      <dl className="rs-summary">
        <div><dt>Interest-bearing debt</dt><dd>{amount(credit.debt_total)}{credit.previous_debt_total != null && <small> · was {amount(credit.previous_debt_total)}</small>}</dd></div>
        <div><dt>Cash · net debt</dt><dd>{amount(lines.Cash)} · {amount(netDebt)}</dd></div>
        <div><dt title="Operating income ÷ interest expense">Interest coverage</dt><dd className={credit.interest_coverage != null && credit.interest_coverage < 1.5 ? 'is-down' : undefined}>{credit.interest_coverage == null ? '—' : `${formatNumber(credit.interest_coverage, 1)}×`}</dd></div>
        <div><dt title="Interest expense ÷ average interest-bearing debt">Cost of debt</dt><dd>{formatPercent(credit.cost_of_debt, 2)}</dd></div>
        <div><dt>Market cap · liabilities</dt><dd>{amount(inputs.market_cap)} · {amount(lines.TotalLiabilities)}</dd></div>
      </dl>
      <dl className="rs-summary">
        <div><dt><Tip content="Equity is a call option on the firm's assets. Solving for the asset value and volatility that match the share price and its volatility gives the distance to the default point (short-term liabilities plus half the long-term ones) in standard deviations.">Merton distance to default</Tip></dt><dd>{profile.firm ? `${formatNumber(profile.firm.distanceToDefault, 2)} σ` : '—'}</dd></div>
        <div><dt>Default within 1 year</dt><dd>{formatPercent(profile.firm?.probabilityOfDefault, 2)}</dd></div>
        <div><dt>Default within {formatNumber(years, years % 1 ? 1 : 0)} years</dt><dd>{formatPercent(profile.atMaturity?.probabilityOfDefault, 2)}</dd></div>
        <div><dt>Asset value · volatility</dt><dd>{amount(profile.firm?.assets)} · {formatPercent(profile.firm?.assetVolatility)}</dd></div>
        <div><dt>Equity volatility</dt><dd>
          <select className="select" aria-label="Equity volatility window" value={equityWindow} onChange={event => onEquityWindow(event.target.value)}>{windows.map(item => <option key={item.window} value={item.window}>{item.window} · {formatPercent(item.value)}</option>)}</select>
        </dd></div>
      </dl>
      <dl className="rs-summary">
        <div><dt><Tip content={<span style={{ whiteSpace: 'pre-line' }}>{`Altman (1968), for listed manufacturers. Above 2.99 is safe, below 1.81 distress.\n${zTip(profile.z)}`}</span>}>Altman Z</Tip></dt><dd className={profile.z ? `is-${profile.z.zone}` : undefined}>{zoneLabel(profile.z)}</dd></div>
        <div><dt><Tip content={<span style={{ whiteSpace: 'pre-line' }}>{`Altman Z″, for non-manufacturers; it leaves out sales. Above 2.6 is safe, below 1.1 distress.\n${zTip(profile.zDouble)}`}</span>}>Altman Z″</Tip></dt><dd className={profile.zDouble ? `is-${profile.zDouble.zone}` : undefined}>{zoneLabel(profile.zDouble)}</dd></div>
        <div><dt>Default point</dt><dd>{amount(profile.debt)}</dd></div>
        <div><dt title="Debt yield over the risk-free rate in the Merton model; it is known to understate market spreads">Merton spread</dt><dd>{profile.atMaturity ? `${Math.round(profile.atMaturity.spread * 10_000)} bp` : '—'}</dd></div>
      </dl>
    </div>
    <p className="rs-hint">Probabilities are risk-neutral (assets grow at the risk-free rate), so they run above historical default rates. Statements may be the parent company's alone where consolidated figures are not stored.</p>
  </section>
}
