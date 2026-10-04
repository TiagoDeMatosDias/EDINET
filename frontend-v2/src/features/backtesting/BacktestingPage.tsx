import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { CircleStop, FlaskConical, Keyboard, PanelLeftClose, PanelLeftOpen } from 'lucide-react'
import { useCallback, useEffect, useRef, useState, type KeyboardEvent } from 'react'
import { useSearchParams } from 'react-router-dom'

import { apiPost, apiRequest } from '../../api/client'
import { downloadApiFile } from '../../api/download'
import { ErrorState, LoadingState } from '../../components/Feedback'
import { ShortcutsDialog, type ShortcutGroup } from '../../components/ShortcutsDialog'
import { useHotkeys } from '../../hooks/useHotkeys'
import { usePersistentState } from '../../hooks/usePersistentState'
import { BacktestResults } from './BacktestResults'
import { BacktestSetup, type BacktestSetupHandle, type Settings } from './BacktestSetup'
import { benchmarkFields, isSingle, pct, portfolioPayload, readScreenDraft, savedWhen, type Benchmark, type Mode, type RollingJob, type SavedBacktest, type SetResult, type SingleResult } from './backtestModel'
import { SetResults, type SetResultsHandle } from './SetResults'
import './backtesting.css'

const SETTINGS_KEY = 'shade.backtest.settings'
const MODES: Array<[Mode, string]> = [['manual', 'Portfolio'], ['screen', 'Rolling screen'], ['csv', 'CSV set']]
const TERMINAL = new Set(['complete', 'failed', 'cancelled'])

const SHORTCUTS: ShortcutGroup[] = [
  { title: 'Set up and run', shortcuts: [
    { keys: ['1', '2', '3'], label: 'Portfolio, rolling screen, or CSV set' },
    { keys: ['R'], label: 'Run the backtest (Ctrl+Enter in the form)' },
    { keys: ['X'], label: 'Cancel the running rolling backtest' },
    { keys: ['E'], label: 'Show or hide the setup panel' },
    { keys: ['S'], label: 'Focus the first setup field' },
    { keys: ['A'], label: 'Add a holding' },
    { keys: ['B'], label: 'Choose the benchmark' },
  ] },
  { title: 'Results', shortcuts: [
    { keys: ['[', ']'], label: 'Previous or next view (rolling and CSV results)' },
    { keys: ['D', 'Shift+D'], label: 'Next or previous holding period' },
    { keys: ['W'], label: 'Next weighting' },
    { keys: ['M'], label: 'Heatmap: return or excess' },
    { keys: ['J', 'K'], label: 'Move through table rows; Enter opens' },
    { keys: ['Esc'], label: 'Close the selected run' },
  ] },
  { title: 'Saved results', shortcuts: [
    { keys: ['L'], label: 'Focus the saved list' },
    { keys: ['Enter'], label: 'Open the focused result' },
    { keys: ['O'], label: 'Download the focused result' },
    { keys: ['?'], label: 'This list' },
  ] },
]

function isoDate(offsetYears = 0) {
  const date = new Date()
  date.setFullYear(date.getFullYear() + offsetYears)
  return date.toISOString().slice(0, 10)
}

const DEFAULT_SETTINGS: Settings = {
  holdings: [{ id: 'h1', ticker: '', mode: 'weight', value: 100 }],
  startDate: isoDate(-10),
  endDate: isoDate(),
  benchmark: 'TPX',
  baseCurrency: 'JPY',
  capital: 1_000_000,
  commissionBps: 0,
  slippageBps: 0,
  spreadBps: 0,
  cadence: 'quarterly',
  durations: ['1yr', '3yr', '5yr'],
  weightings: ['equal'],
  maxCompanies: 25,
  startPeriod: '',
  endPeriod: '',
}

function readSettings(symbol: string | null): Settings {
  let stored: Partial<Settings> = {}
  try { stored = JSON.parse(localStorage.getItem(SETTINGS_KEY) ?? '{}') as Partial<Settings> } catch { /* defaults */ }
  const settings = { ...DEFAULT_SETTINGS, ...stored }
  // The old default "^TPX" has no stored prices.
  if (settings.benchmark === '^TPX') settings.benchmark = 'TPX'
  if (symbol) settings.holdings = [{ id: crypto.randomUUID(), ticker: symbol, mode: 'weight', value: 100 }]
  return settings
}

function kindLabel(kind: SavedBacktest['kind']) { return kind === 'rolling' ? 'Rolling' : kind === 'csv' ? 'CSV' : kind === 'single' ? 'Portfolio' : '—' }

function SavedList({ items, current, onOpen }: { items: SavedBacktest[]; current: string; onOpen: (id: string) => void }) {
  const [kind, setKind] = useState<'all' | 'single' | 'rolling' | 'csv'>('all')
  const shown = items.filter(item => kind === 'all' || item.kind === kind)
  const onKeyDown = (event: KeyboardEvent<HTMLTableRowElement>, item: SavedBacktest) => {
    if (event.key === 'Enter') { event.preventDefault(); onOpen(item.id); return }
    if (event.key === 'o' && item.has_zip) { event.preventDefault(); void downloadApiFile(`/api/backtesting/download/${encodeURIComponent(item.id)}`, `backtest_${item.id}.zip`); return }
    if (!['j', 'k', 'ArrowDown', 'ArrowUp'].includes(event.key)) return
    const next = (event.key === 'j' || event.key === 'ArrowDown' ? event.currentTarget.nextElementSibling : event.currentTarget.previousElementSibling) as HTMLElement | null
    if (next) { event.preventDefault(); next.focus() }
  }
  return <section className="bt-saved" aria-labelledby="bt-saved-title">
    <header>
      <h2 id="bt-saved-title">Saved results <kbd>L</kbd></h2>
      <div className="bt-chips" aria-label="Kind">{(['all', 'single', 'rolling', 'csv'] as const).map(value => <button key={value} type="button" aria-pressed={kind === value} className={kind === value ? 'active' : ''} onClick={() => setKind(value)}>{value === 'all' ? `All · ${items.length}` : kindLabel(value)}</button>)}</div>
    </header>
    {shown.length ? <div className="bt-scroll bt-scroll--saved"><table className="bt-table bt-saved__table">
      <thead><tr><th>Saved</th><th>Kind</th><th>Backtest</th><th className="num">Result</th><th className="num">vs bench</th><th /></tr></thead>
      <tbody>{shown.map(item => {
        const headline = item.headline ?? {}
        const result = headline.annualized_return ?? headline.mean_annualized_return
        const versus = headline.excess_return ?? headline.win_rate
        return <tr key={item.id} tabIndex={0} className={item.id === current ? 'is-selected' : ''} onClick={() => onOpen(item.id)} onKeyDown={event => onKeyDown(event, item)}>
          <td className="muted">{savedWhen(item.id)}</td>
          <td><span className={`bt-kind bt-kind--${item.kind ?? 'unknown'}`}>{kindLabel(item.kind)}</span></td>
          <td className="bt-saved__title"><strong>{item.title ?? item.id}</strong>{item.subtitle && <small>{item.subtitle}</small>}</td>
          <td className="num" title={headline.mean_annualized_return != null ? 'Mean annualized return' : 'Annualized return'}>{pct(result, 1, true)}</td>
          <td className="num" title={headline.win_rate != null ? 'Share of runs ahead of the benchmark' : 'Excess return'}>{headline.win_rate != null ? pct(headline.win_rate, 0) : pct(versus, 1, true)}</td>
          <td>{item.has_zip ? <button type="button" className="text-button" onClick={event => { event.stopPropagation(); void downloadApiFile(`/api/backtesting/download/${encodeURIComponent(item.id)}`, `backtest_${item.id}.zip`) }}>Download</button> : <span className="muted">Preparing</span>}</td>
        </tr>
      })}</tbody>
    </table></div> : <p className="muted bt-empty">No saved backtests{kind === 'all' ? ' yet' : ' of this kind'}.</p>}
  </section>
}

function JobProgress({ job, onCancel }: { job: RollingJob; onCancel: () => void }) {
  const progress = job.progress ?? {}
  const done = progress.completed_backtests ?? 0
  const total = progress.total_backtests ?? 0
  const share = total ? done / total : 0
  return <section className="bt-progress" aria-live="polite">
    <div><strong>{job.status === 'queued' ? 'Waiting for a free slot…' : job.status === 'saving' ? 'Saving results…' : progress.phase ?? 'Starting…'}</strong>
      <span className="muted">{total ? `${done.toLocaleString()} of ${total.toLocaleString()} backtests · period ${(progress.period_index ?? 0) + 1} of ${progress.total_periods ?? '—'}` : 'Preparing rolling periods'}</span>
      <button type="button" className="button button--danger button--small" onClick={onCancel}><CircleStop />Cancel <kbd>X</kbd></button></div>
    <progress max={1} value={share} aria-label="Backtest progress" />
    <small className="muted">Runs on the server: you can leave this page and come back.</small>
  </section>
}

export default function BacktestingPage() {
  const [params, setParams] = useSearchParams()
  const queryClient = useQueryClient()
  const resultId = params.get('result') ?? ''
  const [storedMode, setStoredMode] = usePersistentState<Mode>('shade.backtest.mode', 'manual', ['manual', 'screen', 'csv'])
  // Arriving from Screening ("Backtest this screen") opens the rolling screen.
  const mode: Mode = params.get('source') === 'screen' ? 'screen' : storedMode
  const [settings, setSettings] = useState<Settings>(() => readSettings(params.get('symbol')))
  const [csvContent, setCsvContent] = useState('')
  const [setupOpen, setSetupOpen] = usePersistentState('shade.backtest.setupOpen', true)
  const [startedJob, setStartedJob] = useState<string | null>(null)
  const [help, setHelp] = useState(false)
  const [draft, setDraft] = useState(readScreenDraft)
  const closeHelp = useCallback(() => setHelp(false), [])
  const setupRef = useRef<BacktestSetupHandle>(null)
  const setRef = useRef<SetResultsHandle>(null)
  const savedRef = useRef<HTMLDivElement>(null)

  const updateSettings = (patch: Partial<Settings>) => setSettings(current => {
    const next = { ...current, ...patch }
    try { localStorage.setItem(SETTINGS_KEY, JSON.stringify(next)) } catch { /* convenience only */ }
    return next
  })
  const setMode = (next: Mode) => {
    setStoredMode(next)
    if (params.has('source')) setParams(current => { const nextParams = new URLSearchParams(current); nextParams.delete('source'); return nextParams }, { replace: true })
    if (next === 'screen') setDraft(readScreenDraft())
  }

  const benchmarks = useQuery({ queryKey: ['backtest-benchmarks'], queryFn: () => apiRequest<{ benchmarks: Benchmark[] }>('/api/backtesting/benchmarks'), staleTime: 300_000 })
  const currencies = useQuery({ queryKey: ['backtesting-currencies'], queryFn: () => apiRequest<{ currencies: Array<{ code?: string } | string> }>('/api/backtesting/base-currencies'), staleTime: 300_000 })
  const tickers = useQuery({ queryKey: ['backtest-tickers'], queryFn: () => apiRequest<{ tickers: string[] }>('/api/backtesting/available-tickers'), enabled: mode === 'manual', staleTime: 600_000 })
  const saved = useQuery({ queryKey: ['saved-backtests'], queryFn: () => apiRequest<{ backtests: SavedBacktest[] }>('/api/backtesting/list') })
  const estimate = useQuery({
    queryKey: ['rolling-estimate', settings.cadence, settings.startPeriod, settings.endPeriod, settings.durations.join(','), settings.weightings.join(',')],
    enabled: mode === 'screen' && settings.durations.length > 0 && settings.weightings.length > 0,
    queryFn: () => apiRequest<{ count: number; estimated_backtests: number }>(`/api/backtesting/rolling-periods?${new URLSearchParams({ cadence: settings.cadence, durations: settings.durations.join(','), weighting_modes: settings.weightings.join(','), ...(settings.startPeriod ? { start_period: settings.startPeriod } : {}), ...(settings.endPeriod ? { end_period: settings.endPeriod } : {}) })}`),
    retry: false,
  })
  const result = useQuery({
    queryKey: ['backtest-result', resultId],
    enabled: Boolean(resultId),
    queryFn: () => apiRequest<SingleResult | SetResult>(`/api/backtesting/result/${encodeURIComponent(resultId)}`),
    staleTime: Infinity,
  })
  // A rolling run started earlier (or in another tab) is picked up again.
  const jobs = useQuery({ queryKey: ['rolling-jobs'], queryFn: () => apiRequest<{ jobs: RollingJob[] }>('/api/backtesting/rolling-jobs'), staleTime: Infinity, retry: false })
  const jobId = startedJob ?? jobs.data?.jobs.find(item => !TERMINAL.has(item.status))?.job_id ?? null
  const job = useQuery({
    queryKey: ['rolling-job', jobId],
    enabled: Boolean(jobId),
    queryFn: () => apiRequest<RollingJob>(`/api/backtesting/rolling-jobs/${jobId}`),
    refetchInterval: query => TERMINAL.has(query.state.data?.status ?? '') ? false : 1500,
  })
  const running = Boolean(job.data && !TERMINAL.has(job.data.status))
  const finishedId = job.data?.status === 'complete' ? job.data.result_id : null

  const open = useCallback((id: string) => {
    setParams(current => { const next = new URLSearchParams(current); next.set('result', id); next.delete('symbol'); next.delete('source'); return next })
    window.scrollTo({ top: 0 })
  }, [setParams])

  useEffect(() => {
    if (!finishedId) return
    void queryClient.invalidateQueries({ queryKey: ['saved-backtests'] })
    open(finishedId)
  }, [finishedId, open, queryClient])

  const run = useMutation({
    mutationFn: async () => {
      const comparison = benchmarkFields(settings.benchmark)
      const base = { ...comparison, base_currency: settings.baseCurrency, initial_capital: settings.capital || 0, risk_free_rate: 0 }
      if (mode === 'manual') {
        const portfolio = portfolioPayload(settings.holdings)
        if (!Object.keys(portfolio).length) throw new Error('Add at least one ticker with an amount above zero.')
        if (!settings.startDate || !settings.endDate || settings.startDate >= settings.endDate) throw new Error('The start date must come before the end date.')
        const response = await apiPost<SingleResult>('/api/backtesting/run', { ...base, portfolio, start_date: settings.startDate, end_date: settings.endDate, commission_bps: settings.commissionBps, slippage_bps: settings.slippageBps, spread_bps: settings.spreadBps })
        return { kind: 'done' as const, id: response.id, data: response as SingleResult | SetResult }
      }
      if (!settings.durations.length) throw new Error('Choose at least one holding period.')
      if (mode === 'csv') {
        if (!csvContent.trim()) throw new Error('Choose or paste a CSV file first.')
        const response = await apiPost<SetResult>('/api/backtesting/run-from-csv', { ...base, csv_content: csvContent, durations: settings.durations })
        return { kind: 'done' as const, id: response.id, data: response as SingleResult | SetResult }
      }
      const screen = readScreenDraft()
      setDraft(screen)
      if (!screen) throw new Error('Build a screen in Screening first; its current draft is what gets backtested.')
      if (!settings.weightings.length) throw new Error('Choose at least one weighting.')
      const started = await apiPost<RollingJob>('/api/backtesting/rolling-jobs', {
        ...base,
        criteria: screen.criteria,
        criteria_match: screen.criteria_match,
        columns: screen.columns,
        computed_columns: screen.computed_columns,
        ranking_algorithm: screen.ranking_algorithm,
        ranking_rules: screen.ranking_rules,
        cadence: settings.cadence,
        durations: settings.durations,
        weighting_modes: settings.weightings,
        max_companies: settings.maxCompanies || 25,
        start_period: settings.startPeriod || null,
        end_period: settings.endPeriod || null,
      })
      return { kind: 'job' as const, id: started.job_id, job: started }
    },
    onSuccess: outcome => {
      if (outcome.kind === 'job') {
        queryClient.setQueryData(['rolling-job', outcome.id], outcome.job)
        setStartedJob(outcome.id)
        return
      }
      queryClient.setQueryData(['backtest-result', outcome.id], outcome.data)
      void queryClient.invalidateQueries({ queryKey: ['saved-backtests'] })
      open(outcome.id)
    },
  })
  const cancel = useMutation({
    mutationFn: () => apiPost<RollingJob>(`/api/backtesting/rolling-jobs/${jobId}/cancel`, {}),
    onSuccess: view => queryClient.setQueryData(['rolling-job', view.job_id], view),
  })
  const start = () => {
    if (run.isPending || running) return
    if (mode !== storedMode) setStoredMode(mode)
    run.mutate()
  }

  useHotkeys({
    '1': () => setMode('manual'),
    '2': () => setMode('screen'),
    '3': () => setMode('csv'),
    r: start,
    x: () => { if (running) cancel.mutate() },
    e: () => setSetupOpen(!setupOpen),
    s: () => { if (!setupOpen) setSetupOpen(true); requestAnimationFrame(() => setupRef.current?.focusFirst()) },
    a: () => { if (mode === 'manual') { if (!setupOpen) setSetupOpen(true); setupRef.current?.addHolding() } },
    b: () => { if (!setupOpen) setSetupOpen(true); requestAnimationFrame(() => setupRef.current?.focusBenchmark()) },
    l: () => savedRef.current?.querySelector<HTMLElement>('tbody tr')?.focus(),
    '[': () => setRef.current?.cycleTab(-1),
    ']': () => setRef.current?.cycleTab(1),
    d: () => setRef.current?.cycleDuration(1),
    D: () => setRef.current?.cycleDuration(-1),
    w: () => setRef.current?.cycleWeighting(),
    m: () => setRef.current?.toggleMetric(),
    Escape: () => { setRef.current?.closeDetail() },
    '?': () => setHelp(true),
  }, !help)

  const currencyCodes = (currencies.data?.currencies ?? []).map(item => typeof item === 'string' ? item : item.code ?? '').filter(Boolean)
  const shown = resultId ? result.data : undefined
  const failedJob = job.data && (job.data.status === 'failed' || job.data.status === 'cancelled') && job.data.job_id === startedJob ? job.data : null

  return <div className="bt-page">
    <header className="bt-head">
      <div><span className="eyebrow">Strategy research</span><h1>Backtest</h1></div>
      <div className="bt-modes" role="tablist" aria-label="What to backtest">{MODES.map(([value, label], index) => <button key={value} type="button" role="tab" aria-selected={mode === value} className={mode === value ? 'active' : ''} onClick={() => setMode(value)}>{label} <kbd>{index + 1}</kbd></button>)}</div>
      <div className="bt-actions">
        <button type="button" className="icon-button" onClick={() => setSetupOpen(!setupOpen)} title={setupOpen ? 'Hide setup (E)' : 'Show setup (E)'} aria-label={setupOpen ? 'Hide setup' : 'Show setup'}>{setupOpen ? <PanelLeftClose /> : <PanelLeftOpen />}</button>
        <button type="button" className="icon-button" onClick={() => setHelp(true)} title="Keyboard shortcuts (?)" aria-label="Keyboard shortcuts"><Keyboard /></button>
        <button type="button" className="button button--primary button--small" disabled={run.isPending || running} onClick={start}><FlaskConical />{run.isPending ? 'Running…' : running ? 'Rolling backtest running' : 'Run'} <kbd>R</kbd></button>
      </div>
    </header>
    <div className={`bt-layout ${setupOpen ? '' : 'is-collapsed'}`}>
      {setupOpen && <aside className="bt-setup" aria-label="Backtest setup">
        <BacktestSetup ref={setupRef} mode={mode} settings={settings} onChange={updateSettings} benchmarks={benchmarks.data?.benchmarks ?? []} currencies={currencyCodes} draft={draft} estimate={estimate.data} tickers={tickers.data?.tickers ?? []} csvContent={csvContent} onCsvContent={setCsvContent} onRun={start} />
      </aside>}
      <main className="bt-main">
        {running && job.data && <JobProgress job={job.data} onCancel={() => cancel.mutate()} />}
        {failedJob && <p className="bt-warnings" role="alert">{failedJob.status === 'cancelled' ? 'The rolling backtest was cancelled.' : `The rolling backtest failed: ${failedJob.error ?? 'unknown error'}`}</p>}
        {run.isPending && mode !== 'screen' && <LoadingState label={mode === 'manual' ? 'Running the backtest…' : 'Running each year and holding period…'} />}
        {run.isError && <ErrorState error={run.error} retry={start} />}
        {resultId && result.isLoading && <LoadingState label="Loading the saved backtest" />}
        {resultId && result.isError && <ErrorState error={result.error} retry={() => result.refetch()} />}
        {shown && (isSingle(shown) ? <BacktestResults key={resultId} data={shown} /> : <SetResults key={resultId} ref={setRef} data={shown as SetResult} />)}
        {!resultId && !run.isPending && !running && <section className="bt-intro">
          <h2>Test an idea against history</h2>
          <dl>
            <div><dt>Portfolio <kbd>1</kbd></dt><dd>Fixed holdings over one period: return, risk, drawdowns, calendar and monthly returns, and each holding's contribution, against a benchmark.</dd></div>
            <div><dt>Rolling screen <kbd>2</kbd></dt><dd>Your Screening draft re-run every month, quarter, or year with only the filings public at the time, then held for each period: how often and by how much it beat the benchmark.</dd></div>
            <div><dt>CSV set <kbd>3</kbd></dt><dd>A portfolio per year from a file, each held for the periods you choose.</dd></div>
          </dl>
          <p className="muted">Press <kbd>R</kbd> to run, <kbd>?</kbd> for every shortcut.</p>
        </section>}
        <div ref={savedRef}>
          {saved.isLoading ? <LoadingState label="Loading saved backtests" /> : saved.isError ? <ErrorState error={saved.error} retry={() => saved.refetch()} /> : <SavedList items={saved.data?.backtests ?? []} current={resultId} onOpen={id => { run.reset(); open(id) }} />}
        </div>
      </main>
    </div>
    {help && <ShortcutsDialog groups={SHORTCUTS} onClose={closeHelp} />}
  </div>
}
