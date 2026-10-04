import { FileSpreadsheet, Plus, Trash2 } from 'lucide-react'
import { forwardRef, useImperativeHandle, useRef } from 'react'
import { Link } from 'react-router-dom'

import { DURATIONS, OWN_PORTFOLIO, WEIGHTINGS, weightTotal, type Benchmark, type Holding, type Mode, type ScreenDraft } from './backtestModel'

export interface Settings {
  holdings: Holding[]
  startDate: string
  endDate: string
  benchmark: string
  baseCurrency: string
  capital: number
  commissionBps: number
  slippageBps: number
  spreadBps: number
  cadence: 'monthly' | 'quarterly' | 'yearly'
  durations: string[]
  weightings: string[]
  maxCompanies: number
  startPeriod: string
  endPeriod: string
}

export interface BacktestSetupHandle { focusFirst: () => void; focusBenchmark: () => void; addHolding: () => void }

interface Props {
  mode: Mode
  settings: Settings
  onChange: (patch: Partial<Settings>) => void
  benchmarks: Benchmark[]
  currencies: string[]
  draft: ScreenDraft | null
  estimate?: { count: number; estimated_backtests: number } | null
  tickers: string[]
  csvContent: string
  onCsvContent: (value: string) => void
  onRun: () => void
}

function toggle(list: string[], value: string, on: boolean) {
  return on ? [...new Set([...list, value])] : list.filter(item => item !== value)
}

/** The inputs for one backtest. Ctrl+Enter anywhere in it runs. */
export const BacktestSetup = forwardRef<BacktestSetupHandle, Props>(function BacktestSetup({ mode, settings, onChange, benchmarks, currencies, draft, estimate, tickers, csvContent, onCsvContent, onRun }, ref) {
  const root = useRef<HTMLDivElement>(null)
  const benchmarkRef = useRef<HTMLSelectElement>(null)
  const pendingFocus = useRef(false)

  const addHolding = () => {
    pendingFocus.current = true
    onChange({ holdings: [...settings.holdings, { id: crypto.randomUUID(), ticker: '', mode: 'weight', value: 0 }] })
  }
  useImperativeHandle(ref, () => ({
    focusFirst: () => root.current?.querySelector<HTMLElement>('input, select, textarea')?.focus(),
    focusBenchmark: () => benchmarkRef.current?.focus(),
    addHolding,
  }))
  const focusNewHolding = (element: HTMLInputElement | null) => {
    if (element && pendingFocus.current) {
      pendingFocus.current = false
      element.focus()
    }
  }

  const holdings = settings.holdings
  const setHolding = (id: string, patch: Partial<Holding>) => onChange({ holdings: holdings.map(item => item.id === id ? { ...item, ...patch } : item) })
  const total = weightTotal(holdings)
  const hasWeights = holdings.some(item => item.mode === 'weight' && item.ticker.trim())
  const chosen = benchmarks.find(item => item.ticker === settings.benchmark)
  const benchmarkHint = settings.benchmark === OWN_PORTFOLIO ? 'Your imported portfolio, as a total return.'
    : !settings.benchmark ? 'No comparison.'
      : chosen ? (chosen.available ? `${chosen.detail} · ${chosen.first_date} → ${chosen.last_date}` : `${chosen.detail} · no prices stored`)
        : `“${settings.benchmark}” is not a known benchmark.`

  return <div ref={root} className="bt-form" onKeyDown={event => { if (event.key === 'Enter' && (event.ctrlKey || event.metaKey)) { event.preventDefault(); onRun() } }}>
    {mode === 'manual' && <fieldset>
      <legend>Holdings <span className="muted">{hasWeights ? `· weights total ${total.toFixed(0)}%${Math.abs(total - 100) > 0.5 ? ', scaled to 100%' : ''}` : ''}</span></legend>
      <datalist id="bt-tickers">{tickers.slice(0, 5000).map(ticker => <option key={ticker} value={ticker} />)}</datalist>
      <div className="bt-holdings-edit">
        {holdings.map((holding, index) => <div className="bt-holding" key={holding.id}>
          <input ref={index === holdings.length - 1 ? focusNewHolding : undefined} className="input input--small" list="bt-tickers" aria-label={`Ticker ${index + 1}`} value={holding.ticker} placeholder="7203" onChange={event => setHolding(holding.id, { ticker: event.target.value })} />
          <input className="input input--small" type="number" min="0" step="any" aria-label={`Amount ${index + 1}`} value={Number.isFinite(holding.value) ? holding.value : ''} onChange={event => setHolding(holding.id, { value: event.target.valueAsNumber })} />
          <select className="select select--small" aria-label={`Allocation type ${index + 1}`} value={holding.mode} onChange={event => setHolding(holding.id, { mode: event.target.value as Holding['mode'] })}><option value="weight">%</option><option value="shares">shares</option><option value="value">value</option></select>
          <button type="button" className="icon-button" aria-label={`Remove ticker ${index + 1}`} onClick={() => onChange({ holdings: holdings.filter(item => item.id !== holding.id) })}><Trash2 /></button>
        </div>)}
      </div>
      <div className="bt-row">
        <button type="button" className="button button--ghost button--small" onClick={addHolding}><Plus />Add <kbd>A</kbd></button>
        {holdings.length > 1 && hasWeights && <button type="button" className="text-button" onClick={() => { const named = holdings.filter(item => item.ticker.trim()); onChange({ holdings: named.map(item => ({ ...item, mode: 'weight', value: Math.round(10000 / named.length) / 100 })) }) }}>Equal weights</button>}
      </div>
      <div className="bt-pair">
        <label>Start<input className="input input--small" type="date" value={settings.startDate} onChange={event => onChange({ startDate: event.target.value })} /></label>
        <label>End<input className="input input--small" type="date" value={settings.endDate} onChange={event => onChange({ endDate: event.target.value })} /></label>
      </div>
    </fieldset>}

    {mode === 'screen' && <fieldset>
      <legend>Screen, re-run at every period</legend>
      {draft ? <div className="bt-draft">
        <strong>{draft.name || 'Current Screening draft'}</strong>
        <span>{draft.criteria.length} rules · match {draft.criteria_match} · {draft.computed_columns.length} computed columns</span>
        <span className={draft.ranking_algorithm === 'none' ? 'bt-warn' : ''}>{draft.ranking_algorithm === 'none' ? 'No ranking: the top companies are an arbitrary pick.' : `Ranked by ${draft.ranking_algorithm} (${draft.ranking_rules.length} rules)`}</span>
        <Link to="/screen">Edit in Screening</Link>
      </div> : <p className="bt-warn">No screen yet. <Link to="/screen">Build one in Screening</Link>; its draft is used here.</p>}
      <div className="bt-pair">
        <label>Rebalance<select className="select select--small" value={settings.cadence} onChange={event => onChange({ cadence: event.target.value as Settings['cadence'] })}><option value="monthly">Monthly</option><option value="quarterly">Quarterly</option><option value="yearly">Yearly</option></select></label>
        <label>Top companies<input className="input input--small" type="number" min="1" max="500" value={settings.maxCompanies} onChange={event => onChange({ maxCompanies: event.target.valueAsNumber })} /></label>
      </div>
      <div className="bt-pair">
        <label>First month<input className="input input--small" type="month" value={settings.startPeriod} onChange={event => onChange({ startPeriod: event.target.value })} /></label>
        <label>Last month<input className="input input--small" type="month" value={settings.endPeriod} onChange={event => onChange({ endPeriod: event.target.value })} /></label>
      </div>
      <div className="bt-checks" role="group" aria-label="Weighting">{WEIGHTINGS.map(([value, label]) => <label key={value}><input type="checkbox" checked={settings.weightings.includes(value)} onChange={event => onChange({ weightings: toggle(settings.weightings, value, event.target.checked) })} />{label}</label>)}</div>
    </fieldset>}

    {mode === 'csv' && <fieldset>
      <legend>Portfolio per year</legend>
      <label className="bt-file"><FileSpreadsheet /><span>Choose a CSV file, or paste below</span><input type="file" accept=".csv,text/csv" onChange={event => { const file = event.target.files?.[0]; if (file) void file.text().then(onCsvContent) }} /></label>
      <textarea className="textarea bt-csv" aria-label="CSV content" value={csvContent} onChange={event => onCsvContent(event.target.value)} placeholder={'Year,Tickers,Type,Amount\n2018,7203,weight,50\n2018,6758,weight,50\n2019,9984,shares,100'} spellCheck={false} />
      <p className="muted bt-note">Each year starts a backtest on 1 January for each holding period. Type is weight (fractions or percent), shares, or value.</p>
    </fieldset>}

    {mode !== 'manual' && <fieldset>
      <legend>Holding periods</legend>
      <div className="bt-checks" role="group" aria-label="Holding periods">{DURATIONS.map(duration => <label key={duration}><input type="checkbox" checked={settings.durations.includes(duration)} onChange={event => onChange({ durations: toggle(settings.durations, duration, event.target.checked) })} />{duration}</label>)}</div>
      {mode === 'screen' && estimate && <p className="muted bt-note">{estimate.count} periods · {estimate.estimated_backtests.toLocaleString()} backtests</p>}
    </fieldset>}

    <fieldset>
      <legend>Comparison and money</legend>
      <label>Benchmark <kbd>B</kbd>
        <select ref={benchmarkRef} className="select select--small" value={settings.benchmark} onChange={event => onChange({ benchmark: event.target.value })}>
          {benchmarks.map(item => <option key={item.ticker} value={item.ticker} disabled={!item.available}>{item.label} · {item.ticker}{item.available ? '' : ' (no prices)'}</option>)}
          {settings.benchmark && settings.benchmark !== OWN_PORTFOLIO && !benchmarks.some(item => item.ticker === settings.benchmark) && <option value={settings.benchmark}>{settings.benchmark}</option>}
          <option value={OWN_PORTFOLIO}>My portfolio</option>
          <option value="">None</option>
        </select>
      </label>
      <small className={`bt-hint ${chosen && !chosen.available ? 'bt-warn' : ''}`}>{benchmarkHint}</small>
      <div className="bt-pair">
        <label>Currency<select className="select select--small" value={settings.baseCurrency} onChange={event => onChange({ baseCurrency: event.target.value })}><option value="">Native</option>{(currencies.length ? currencies : ['JPY', 'EUR', 'USD']).map(code => <option key={code}>{code}</option>)}</select></label>
        <label>Capital<input className="input input--small" type="number" min="0" step="any" value={settings.capital} onChange={event => onChange({ capital: event.target.valueAsNumber })} /></label>
      </div>
      {mode === 'manual' && <div className="bt-triple" title="Trading costs in basis points (1 bp = 0.01%), charged on entry">
        <label>Commission bp<input className="input input--small" type="number" min="0" max="500" step="any" value={settings.commissionBps} onChange={event => onChange({ commissionBps: event.target.valueAsNumber || 0 })} /></label>
        <label>Slippage bp<input className="input input--small" type="number" min="0" max="500" step="any" value={settings.slippageBps} onChange={event => onChange({ slippageBps: event.target.valueAsNumber || 0 })} /></label>
        <label>Spread bp<input className="input input--small" type="number" min="0" max="500" step="any" value={settings.spreadBps} onChange={event => onChange({ spreadBps: event.target.valueAsNumber || 0 })} /></label>
      </div>}
    </fieldset>
  </div>
})
