import { useMutation, useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'
import { CircleStop, Download, FileSpreadsheet, FlaskConical, Plus, Trash2 } from 'lucide-react'
import { useMemo, useRef, useState } from 'react'
import { useSearchParams } from 'react-router-dom'

import { apiPost, apiRequest } from '../../api/client'
import { apiStream, type StreamMessage } from '../../api/stream'
import { DataTable } from '../../components/DataTable'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { Card, Field, PageHeader } from '../../components/Page'
import { BacktestResults } from './BacktestResults'

type Holding = { id: string; ticker: string; mode: 'weight' | 'shares' | 'value'; value: number }
type BacktestSummary = Record<string, unknown>
type SavedBacktest = { id: string; created: string; has_zip: boolean }

function isoDate(offsetYears = 0) {
  const date = new Date()
  date.setFullYear(date.getFullYear() + offsetYears)
  return date.toISOString().slice(0, 10)
}

function resultSummary(data: unknown): BacktestSummary | undefined {
  if (!data || typeof data !== 'object') return undefined
  const record = data as Record<string, unknown>
  const summary = record.summary ?? record.aggregate
  return summary && typeof summary === 'object' ? summary as BacktestSummary : undefined
}

export default function BacktestingPage() {
  const [params] = useSearchParams()
  const initialSymbol = params.get('symbol') ?? ''
  const source = params.get('source')
  const [mode, setMode] = useState<'manual' | 'screen' | 'csv'>(source === 'screen' ? 'screen' : 'manual')
  const [holdings, setHoldings] = useState<Holding[]>([{ id: crypto.randomUUID(), ticker: initialSymbol, mode: 'weight', value: 100 }])
  const [startDate, setStartDate] = useState(isoDate(-10))
  const [endDate, setEndDate] = useState(isoDate())
  const [benchmark, setBenchmark] = useState('^TPX')
  const [baseCurrency, setBaseCurrency] = useState('JPY')
  const [capital, setCapital] = useState(1_000_000)
  const [csvContent, setCsvContent] = useState('')
  const [cadence, setCadence] = useState('quarterly')
  const [durations, setDurations] = useState(['1yr', '3yr', '5yr'])
  const [weightingModes, setWeightingModes] = useState(['equal'])
  const [maxCompanies, setMaxCompanies] = useState(25)
  const [startPeriod, setStartPeriod] = useState('')
  const [endPeriod, setEndPeriod] = useState('')
  const [progress, setProgress] = useState<StreamMessage>()
  const abortRef = useRef<AbortController | null>(null)
  const db = useQuery({ queryKey: ['backtesting-db'], queryFn: () => apiRequest<{ db_path: string }>('/api/backtesting/db-path') })
  const currencies = useQuery({ queryKey: ['backtesting-currencies'], queryFn: () => apiRequest<{ currencies: Array<{ code?: string } | string> }>('/api/backtesting/base-currencies') })
  const saved = useQuery({ queryKey: ['saved-backtests'], queryFn: () => apiRequest<{ backtests: SavedBacktest[] }>('/api/backtesting/list') })
  const run = useMutation({ mutationFn: async () => {
    if (mode === 'csv') return apiPost<{ id: string; aggregate: BacktestSummary }>('/api/backtesting/run-from-csv', { db_path: db.data?.db_path ?? '', csv_content: csvContent, benchmark_ticker: benchmark, benchmark_mode: 'ticker', base_currency: baseCurrency, durations, initial_capital: capital, risk_free_rate: 0 })
    if (mode === 'screen') {
      const draft = JSON.parse(localStorage.getItem('shade.screening.draft') ?? '{}') as { criteria?: Array<Record<string, unknown>>; columns?: string[] }
      if (!draft.criteria?.length) throw new Error('Build or load a screen before starting this backtest.')
      if (!durations.length || !weightingModes.length) throw new Error('Select at least one holding period and weighting method.')
      const controller = new AbortController()
      abortRef.current = controller
      setProgress({ type: 'starting', message: 'Preparing rolling periods' })
      try {
        return await apiStream('/api/backtesting/run-rolling', {
          db_path: db.data?.db_path ?? '',
          criteria: draft.criteria.map(criterion => Object.fromEntries(Object.entries(criterion).filter(([key]) => key !== 'id'))),
          columns: draft.columns ?? [],
          computed_columns: [],
          cadence,
          durations,
          weighting_modes: weightingModes,
          max_companies: maxCompanies,
          ranking_algorithm: 'none',
          ranking_rules: [],
          benchmark_ticker: benchmark,
          benchmark_mode: 'ticker',
          base_currency: baseCurrency,
          initial_capital: capital,
          risk_free_rate: 0,
          start_period: startPeriod || null,
          end_period: endPeriod || null,
        }, setProgress, controller.signal)
      } finally {
        abortRef.current = null
      }
    }
    const portfolio = Object.fromEntries(holdings.filter(item => item.ticker.trim()).map(item => [item.ticker.trim(), { mode: item.mode, value: item.value }]))
    return apiPost<{ id: string; summary: BacktestSummary }>('/api/backtesting/run', { db_path: db.data?.db_path ?? '', portfolio, start_date: startDate, end_date: endDate, benchmark_ticker: benchmark, benchmark_mode: 'ticker', base_currency: baseCurrency, initial_capital: capital, risk_free_rate: 0 })
  }, onSuccess: () => saved.refetch() })
  const summary = resultSummary(run.data)
  const resultId = run.data && typeof run.data === 'object' && 'id' in run.data ? String(run.data.id ?? '') : ''
  const currencyCodes = (currencies.data?.currencies ?? []).map(item => typeof item === 'string' ? item : item.code ?? '').filter(Boolean)
  const savedColumns = useMemo<ColumnDef<SavedBacktest>[]>(() => [{ accessorKey: 'created', header: 'Created' }, { accessorKey: 'id', header: 'ID' }, { id: 'download', header: '', cell: ({ row }) => row.original.has_zip ? <a className="button button--ghost" href={`/api/backtesting/download/${encodeURIComponent(row.original.id)}`}><Download />Download</a> : <span className="muted">Preparing</span> }], [])

  return <div className="stack dense-page backtesting-workspace">
    <PageHeader eyebrow="Strategy research" title="Backtest an investment idea" description="Define the universe first, then portfolio construction, period, and benchmark. Inputs from Screening or Company Analysis arrive preselected." actions={run.isPending && mode === 'screen' ? <button className="button button--danger" onClick={() => abortRef.current?.abort()}><CircleStop />Cancel rolling backtest</button> : <button className="button button--primary" disabled={run.isPending || db.isLoading} onClick={() => run.mutate()}><FlaskConical />{run.isPending ? 'Running…' : 'Run backtest'}</button>} />
    <div className="step-tabs"><button className={mode === 'manual' ? 'active' : ''} onClick={() => setMode('manual')}>Manual portfolio</button><button className={mode === 'screen' ? 'active' : ''} onClick={() => setMode('screen')}>Saved screen</button><button className={mode === 'csv' ? 'active' : ''} onClick={() => setMode('csv')}>CSV set</button></div>
    {run.data && <BacktestResults data={run.data} resultId={resultId} />}
    <details className="backtest-setup" open={!summary}><summary>Backtest setup</summary><div className="two-column">
      <Card title="1. Universe and portfolio" description={mode === 'manual' ? 'Add tickers and choose how each allocation is expressed.' : mode === 'screen' ? 'The current Screening draft is attached to this backtest.' : 'Upload or paste a yearly portfolio CSV.'}>
        {mode === 'manual' && <div className="stack">{holdings.map((holding, index) => <div className="holding-row" key={holding.id}><Field label={`Ticker ${index + 1}`}><input className="input" list="ticker-options" value={holding.ticker} onChange={event => setHoldings(items => items.map(item => item.id === holding.id ? { ...item, ticker: event.target.value } : item))} placeholder="6201" /></Field><Field label="Allocation type"><select className="select" value={holding.mode} onChange={event => setHoldings(items => items.map(item => item.id === holding.id ? { ...item, mode: event.target.value as Holding['mode'] } : item))}><option value="weight">Weight (%)</option><option value="shares">Shares</option><option value="value">Value</option></select></Field><Field label="Amount"><input className="input" type="number" value={holding.value} onChange={event => setHoldings(items => items.map(item => item.id === holding.id ? { ...item, value: Number(event.target.value) } : item))} /></Field><button className="icon-button rule-remove" aria-label={`Remove ticker ${index + 1}`} onClick={() => setHoldings(items => items.filter(item => item.id !== holding.id))}><Trash2 /></button></div>)}<button className="button button--secondary" onClick={() => setHoldings(items => [...items, { id: crypto.randomUUID(), ticker: '', mode: 'weight', value: 0 }])}><Plus />Add ticker</button></div>}
        {mode === 'screen' && <div className="stack"><EmptyState title="Screen attached" description="The current Screening draft will be rerun at every selected period using point-in-time financial data." /><div className="field-row"><Field label="Rebalance cadence"><select className="select" value={cadence} onChange={event => setCadence(event.target.value)}><option value="monthly">Monthly</option><option value="quarterly">Quarterly</option><option value="yearly">Yearly</option></select></Field><Field label="Maximum companies"><input className="input" type="number" min="1" max="500" value={maxCompanies} onChange={event => setMaxCompanies(Number(event.target.value))} /></Field></div><div><span className="field-label">Holding periods</span><div className="check-row">{['1yr', '2yr', '3yr', '5yr', '10yr'].map(duration => <label className="check" key={duration}><input type="checkbox" checked={durations.includes(duration)} onChange={event => setDurations(items => event.target.checked ? [...items, duration] : items.filter(item => item !== duration))} />{duration}</label>)}</div></div><div><span className="field-label">Weighting</span><div className="check-row">{[['equal', 'Equal weight'], ['market_cap', 'Market cap']] .map(([value, label]) => <label className="check" key={value}><input type="checkbox" checked={weightingModes.includes(value)} onChange={event => setWeightingModes(items => event.target.checked ? [...items, value] : items.filter(item => item !== value))} />{label}</label>)}</div></div><div className="field-row"><Field label="First screening month"><input className="input" type="month" value={startPeriod} onChange={event => setStartPeriod(event.target.value)} /></Field><Field label="Last screening month"><input className="input" type="month" value={endPeriod} onChange={event => setEndPeriod(event.target.value)} /></Field></div></div>}
        {mode === 'csv' && <div className="stack"><label className="file-drop"><FileSpreadsheet /><strong>Choose a CSV file</strong><span>or paste its content below</span><input type="file" accept=".csv,text/csv" onChange={event => { const file = event.target.files?.[0]; if (file) void file.text().then(setCsvContent) }} /></label><textarea className="textarea" value={csvContent} onChange={event => setCsvContent(event.target.value)} placeholder="Year,Ticker,Weight" /></div>}
      </Card>
      <Card title="2. Period and assumptions" description="All returns are converted consistently when a base currency is selected."><div className="stack"><div className="field-row"><Field label="Start date"><input className="input" type="date" value={startDate} onChange={event => setStartDate(event.target.value)} /></Field><Field label="End date"><input className="input" type="date" value={endDate} onChange={event => setEndDate(event.target.value)} /></Field></div><Field label="Benchmark ticker"><input className="input" value={benchmark} onChange={event => setBenchmark(event.target.value)} /></Field><Field label="Base currency"><select className="select" value={baseCurrency} onChange={event => setBaseCurrency(event.target.value)}>{currencyCodes.length ? currencyCodes.map(code => <option key={code}>{code}</option>) : <><option>JPY</option><option>EUR</option><option>USD</option></>}</select></Field><Field label="Initial capital"><input className="input" type="number" value={capital} onChange={event => setCapital(Number(event.target.value))} /></Field></div></Card>
    </div></details>
    {run.isPending && <Card><LoadingState label={mode === 'screen' ? String(progress?.message ?? progress?.stage ?? 'Running point-in-time screens and backtests') : 'Running backtest. Large portfolios can take up to two minutes.'} /></Card>}
    {run.isError && <ErrorState error={run.error} retry={() => run.mutate()} />}
    <Card title="Saved backtests" description="Completed result packages remain downloadable.">{saved.isLoading ? <LoadingState label="Loading saved backtests" /> : saved.isError ? <ErrorState error={saved.error} /> : <DataTable data={saved.data?.backtests ?? []} columns={savedColumns} emptyText="No saved backtests yet." dense />}</Card>
  </div>
}
