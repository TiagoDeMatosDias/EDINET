import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { CalendarClock, ChevronDown, ChevronUp, Columns3, Download, FlaskConical, FolderOpen, GitCompare, LoaderCircle, Save } from 'lucide-react'
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'

import { apiPost, apiRequest, authenticatedFetch } from '../../api/client'
import { downloadBlob } from '../../api/download'
import type { ScreeningResult } from '../../api/types'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { Tip } from '../../components/Tooltip'
import { HotkeyHelpButton } from '../../hotkeys/HotkeyHelpButton'
import { HotkeyKbd } from '../../hotkeys/HotkeyKbd'
import { useHotkeyScope } from '../../hotkeys/useHotkeyScope'
import { useHotkeyText } from '../../hotkeys/useHotkeyText'
import { usePersistentState } from '../../hooks/usePersistentState'
import { formatAsOf } from './asOfDates'
import { AsOfMenu } from './AsOfMenu'
import { ColumnsPanel } from './ColumnsPanel'
import { serializeComputedColumn, serializeCriterion } from './criterion-values'
import { normalizeCriterion } from './expression-model'
import { buildMetricOptions, columnLabel, presetForTokens, tableInfo, tokensFormat } from './metricCatalog'
import { ResultsTable, type ResultColumnInfo } from './ResultsTable'
import { RuleBuilder } from './RuleBuilder'
import { describeRule, describeScreen, newFilterRule, newGroupId, ruleProblem } from './ruleModel'
import { availableStarters, defaultOutput, type StarterScreen } from './screenDefaults'
import { ScreensMenu } from './ScreensMenu'
import type { ColumnFormats, ComputedColumn, Criterion, CriteriaMatch, MetricCatalog, SavedScreen, SavedScreenSummary } from './types'
import { screeningCommandsScope, screeningScope } from './screeningHotkeys'
import './screening.css'

const DRAFT_KEY = 'shade.screening.draft'
const LEGACY_DEFAULT_COLUMNS = ['CompanyInfo.EdinetCode', 'CompanyInfo.Company_Ticker', 'CompanyInfo.Company_Name', 'CompanyInfo.Company_Industry']

interface Draft extends SavedScreen { loaded_name?: string | null; saved_key?: string | null }

function readDraft(): Draft | null {
  try {
    return JSON.parse(localStorage.getItem(DRAFT_KEY) ?? 'null') as Draft | null
  } catch {
    return null
  }
}

interface LastRun { result: ScreeningResult; key: string; ms: number; at: number }

/** Result aliases the server gives requested columns (the column name, or Table.Column when names repeat). */
function resultAliases(columns: string[]) {
  const counts = new Map<string, number>()
  for (const reference of columns) {
    const name = reference.slice(reference.indexOf('.') + 1).toLowerCase()
    counts.set(name, (counts.get(name) ?? 0) + 1)
  }
  return new Map(columns.map(reference => {
    const name = reference.slice(reference.indexOf('.') + 1)
    return [(counts.get(name.toLowerCase()) ?? 0) > 1 ? reference : name, reference] as const
  }))
}

function withIdentity(columns: string[], catalog: MetricCatalog) {
  const company = catalog.CompanyInfo ?? []
  const required = [['Company_Name'], ['Company_Code', 'EdinetCode']]
  const missing = required
    .filter(candidates => !candidates.some(name => columns.includes(`CompanyInfo.${name}`)))
    .map(candidates => candidates.find(name => company.includes(name)))
    .filter((name): name is string => Boolean(name))
    .map(name => `CompanyInfo.${name}`)
  return [...missing, ...columns]
}

function resultCompanyCodes(result?: ScreeningResult) {
  if (!result) return []
  const index = ['EdinetCode', 'Company_Code'].map(name => result.columns.indexOf(name)).find(position => position >= 0) ?? -1
  return index < 0 ? [] : [...new Set(result.rows.map(row => String(row[index] ?? '').trim()).filter(Boolean))]
}

interface ScreenState {
  criteria: Criterion[]
  match: CriteriaMatch
  columns: string[]
  computed: ComputedColumn[]
  screeningDate: string
  rankingAlgorithm: string
  rankingRules: Array<Record<string, unknown>>
}

/** What the API runs and saves for a screen; its JSON is also how changes are detected. */
function screenDefinition(state: ScreenState, catalog: MetricCatalog) {
  return {
    criteria: state.criteria.map(serializeCriterion),
    criteria_match: state.match,
    columns: withIdentity(state.columns, catalog),
    computed_columns: state.computed.map(serializeComputedColumn),
    screening_date: state.screeningDate || null,
    ranking_algorithm: state.rankingAlgorithm,
    ranking_rules: state.rankingRules,
  }
}

function firstProblem(criteria: Criterion[]) {
  for (const [index, criterion] of criteria.entries()) {
    const problem = criterion.enabled === false ? null : ruleProblem(criterion)
    if (problem) return `Rule ${index + 1}: ${problem} Fix it, or untick it to run without it.`
  }
  return null
}

export default function ScreeningWorkspace() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const draft = useMemo(() => readDraft(), [])
  const metrics = useQuery({ queryKey: ['screening-metrics'], queryFn: () => apiRequest<{ tables: MetricCatalog; formats?: ColumnFormats }>('/api/screening/metrics') })
  const saved = useQuery({ queryKey: ['saved-screenings'], retry: false, queryFn: () => apiRequest<{ screenings: string[]; items?: SavedScreenSummary[] }>('/api/screening/saved') })
  const lastStored = useQuery({ queryKey: ['screening-last-result'], retry: false, queryFn: () => apiRequest<{ result: { result: ScreeningResult; updated_at?: string } | null }>('/api/screening/last-result') })
  const tags = useQuery({ queryKey: ['tags'], queryFn: () => apiRequest<{ tags: Array<{ name: string; member_count: number }> }>('/api/tags') })
  const catalog = useMemo(() => metrics.data?.tables ?? {}, [metrics.data])
  const formats = useMemo(() => metrics.data?.formats ?? {}, [metrics.data])
  const options = useMemo(() => buildMetricOptions(catalog, formats), [catalog, formats])
  const ruleOptions = useMemo(() => buildMetricOptions(catalog, formats, { hideTables: ['Stock_Splits'] }), [catalog, formats])
  const tagNames = useMemo(() => (tags.data?.tags ?? []).map(tag => tag.name), [tags.data])
  const starters = useMemo(() => availableStarters(catalog), [catalog])

  const [criteria, setCriteria] = useState<Criterion[]>(() => (draft?.criteria ?? []).map(normalizeCriterion))
  const [match, setMatch] = useState<CriteriaMatch>(draft?.criteria_match === 'any' ? 'any' : 'all')
  // ``null`` columns mean the default overview set (also for drafts still on the old four columns).
  const [columns, setColumns] = useState<string[] | null>(() => {
    const stored = draft?.columns ?? []
    const legacy = stored.length === LEGACY_DEFAULT_COLUMNS.length && stored.every((column, index) => column === LEGACY_DEFAULT_COLUMNS[index]) && !draft?.computed_columns?.length
    return stored.length && !legacy ? stored : null
  })
  const [computed, setComputed] = useState<ComputedColumn[] | null>(() => columns === null ? null : draft?.computed_columns ?? [])
  const [screeningDate, setScreeningDate] = useState(draft?.screening_date ?? '')
  const [rankingAlgorithm, setRankingAlgorithm] = useState(draft?.ranking_algorithm ?? 'none')
  const [rankingRules, setRankingRules] = useState<Array<Record<string, unknown>>>(draft?.ranking_rules ?? [])
  const [name, setName] = useState(draft?.name ?? '')
  const [loadedName, setLoadedName] = useState<string | null>(draft?.loaded_name ?? null)
  const [savedKey, setSavedKey] = useState<string | null>(draft?.saved_key ?? null)
  const [lastRun, setLastRun] = useState<LastRun | null>(null)
  const [notice, setNotice] = useState<{ text: string; tone: 'info' | 'warning' } | null>(null)
  const [openRuleId, setOpenRuleId] = useState<string | null>(null)
  const [showColumns, setShowColumns] = useState(false)
  const [showScreens, setShowScreens] = useState(false)
  const [showDate, setShowDate] = useState(false)
  const [collapsed, setCollapsed] = usePersistentState('shade.screening.builder-collapsed', false, [true, false])
  const nameInput = useRef<HTMLInputElement>(null)
  const layout = useRef<HTMLDivElement>(null)

  const defaults = useMemo(() => defaultOutput(catalog), [catalog])
  const shownColumns = columns ?? defaults.columns
  const shownComputed = computed ?? defaults.computed
  const state: ScreenState = { criteria, match, columns: shownColumns, computed: shownComputed, screeningDate, rankingAlgorithm, rankingRules }
  const definition = screenDefinition(state, catalog)
  const currentKey = JSON.stringify(definition)
  const outputColumns = definition.columns
  const dirty = savedKey !== currentKey
  useEffect(() => {
    try {
      localStorage.setItem(DRAFT_KEY, JSON.stringify({ ...JSON.parse(currentKey), name, loaded_name: loadedName, saved_key: savedKey }))
    } catch { /* drafts are a convenience */ }
  }, [currentKey, loadedName, name, savedKey])

  const run = useMutation({
    mutationFn: async ({ body, key }: { body: ReturnType<typeof screenDefinition>; key: string }) => {
      const started = performance.now()
      const result = await apiPost<ScreeningResult>('/api/screening/run', { ...body, sort_order: 'DESC' })
      return { result, key, ms: performance.now() - started, at: Date.now() }
    },
    onSuccess: data => {
      setLastRun(data)
      void queryClient.invalidateQueries({ queryKey: ['screening-last-result'] })
    },
  })
  /** Runs ``target`` (the screen as shown, unless a just-opened screen is passed). */
  const runScreen = (target: ScreenState = state) => {
    const problem = firstProblem(target.criteria)
    if (problem) {
      setNotice({ text: problem, tone: 'warning' })
      return
    }
    setNotice(null)
    const body = screenDefinition(target, catalog)
    run.mutate({ body, key: JSON.stringify(body) })
  }

  const savedItems = useMemo(() => saved.data?.items ?? (saved.data?.screenings ?? []).map(screen => ({ screen_id: screen, name: screen, rule_count: 0, column_count: 0 })), [saved.data])
  const saveMutation = useMutation({
    mutationFn: ({ target, overwrite }: { target: string; overwrite: boolean; key: string }) => apiPost<{ updated: boolean }>('/api/screening/save', { name: target, ...definition, overwrite }),
    onSuccess: (response, { target, key }) => {
      setLoadedName(target)
      setSavedKey(key)
      setNotice({ text: response.updated ? `Saved changes to “${target}”.` : `Saved “${target}”.`, tone: 'info' })
      void queryClient.invalidateQueries({ queryKey: ['saved-screenings'] })
    },
    onError: error => setNotice({ text: error instanceof Error ? error.message : 'Saving failed', tone: 'warning' }),
  })
  const saveScreen = () => {
    const target = name.trim()
    if (!target) {
      setNotice({ text: 'Name the screen first, then save.', tone: 'warning' })
      nameInput.current?.focus()
      return
    }
    const exists = savedItems.some(item => item.name === target)
    if (exists && target !== loadedName && !window.confirm(`A saved screen named “${target}” exists. Replace it with this one?`)) return
    saveMutation.mutate({ target, overwrite: exists, key: currentKey })
  }

  /** Show ``screen``; a saved one starts out unchanged, and opened screens run straight away. */
  const applyDefinition = (screen: SavedScreen, savedAs: string | null, runNow = true) => {
    const next: ScreenState = {
      criteria: (screen.criteria ?? []).map(normalizeCriterion),
      match: screen.criteria_match === 'any' ? 'any' : 'all',
      columns: screen.columns?.length ? screen.columns : defaults.columns,
      computed: screen.columns?.length ? screen.computed_columns ?? [] : defaults.computed,
      screeningDate: screen.screening_date ?? '',
      rankingAlgorithm: screen.ranking_algorithm ?? 'none',
      rankingRules: screen.ranking_rules ?? [],
    }
    setCriteria(next.criteria)
    setMatch(next.match)
    setColumns(next.columns)
    setComputed(next.computed)
    setScreeningDate(next.screeningDate)
    setRankingAlgorithm(next.rankingAlgorithm)
    setRankingRules(next.rankingRules)
    setName(savedAs ?? screen.name ?? '')
    setLoadedName(savedAs)
    setSavedKey(savedAs ? JSON.stringify(screenDefinition(next, catalog)) : null)
    setNotice(null)
    if (runNow) runScreen(next)
  }
  const openSaved = async (screenName: string) => {
    try {
      applyDefinition(await apiRequest<SavedScreen>(`/api/screening/saved/${encodeURIComponent(screenName)}`), screenName)
    } catch (error) {
      setNotice({ text: error instanceof Error ? error.message : 'Could not open the screen', tone: 'warning' })
    }
  }
  const openStarter = (starter: StarterScreen) => applyDefinition({ name: starter.name, criteria: starter.build(), criteria_match: starter.match, columns: shownColumns, computed_columns: shownComputed }, null)
  const newScreen = () => {
    applyDefinition({ name: '', criteria: [], columns: [], computed_columns: [] }, null, false)
    setLastRun(null)
  }
  /** A new as-of date reruns the screen straight away, like opening a screen. */
  const applyDate = (value: string) => {
    setShowDate(false)
    setScreeningDate(value)
    runScreen({ ...state, screeningDate: value })
  }
  const deleteSaved = useMutation({
    mutationFn: (screenName: string) => apiRequest(`/api/screening/saved/${encodeURIComponent(screenName)}`, { method: 'DELETE' }),
    onSuccess: (_, screenName) => {
      if (screenName === loadedName) { setLoadedName(null); setSavedKey(null) }
      setNotice({ text: `Deleted “${screenName}”.`, tone: 'info' })
      void queryClient.invalidateQueries({ queryKey: ['saved-screenings'] })
    },
  })
  const removeSaved = (screenName: string) => { if (window.confirm(`Delete the saved screen “${screenName}”?`)) deleteSaved.mutate(screenName) }

  const addRule = (group?: { id: string; match: 'any' | 'all' }) => {
    const rule = newFilterRule(group)
    setOpenRuleId(rule.id)
    setCollapsed(false)
    setCriteria(items => {
      if (!group) return [...items, rule]
      const last = items.map(item => item.group).lastIndexOf(group.id)
      return last === -1 ? [...items, rule] : [...items.slice(0, last + 1), rule, ...items.slice(last + 1)]
    })
  }

  const exportCsv = async () => {
    const response = await authenticatedFetch('/api/screening/export', { method: 'POST', body: JSON.stringify({ ...definition, format: 'csv' }) })
    if (!response.ok) { setNotice({ text: `Export failed (${response.status})`, tone: 'warning' }); return }
    downloadBlob(`${(name.trim() || 'screen').replace(/[^\w.-]+/g, '-')}.csv`, await response.blob())
  }

  const screensKey = useHotkeyText(screeningScope.byId.screens)
  const saveKey = useHotkeyText(screeningCommandsScope.byId.save)
  const runKey = useHotkeyText(screeningCommandsScope.byId.run)
  // Run and save work from inside fields and open menus too, so they have their own scope.
  useHotkeyScope(screeningCommandsScope, { run: () => runScreen(), save: saveScreen })
  const overlayOpen = showColumns || showScreens || showDate
  // G is kept free for the app-wide "G then a letter" page shortcuts; results keys live in ResultsTable.
  useHotkeyScope(screeningScope, {
    'add-rule': () => addRule(),
    'add-group': () => addRule({ id: newGroupId(), match: 'any' }),
    screens: () => setShowScreens(true),
    date: () => setShowDate(true),
    columns: () => setShowColumns(true),
    collapse: () => setCollapsed(!collapsed),
    run: () => runScreen(),
  }, { enabled: !overlayOpen && Boolean(metrics.data) })

  // The workspace fills the window below its top edge; results scroll inside it, the page does not.
  useLayoutEffect(() => {
    const element = layout.current
    if (!element) return
    const fit = () => {
      const wide = window.innerWidth > 960 && window.innerHeight > 640
      element.style.height = wide ? `${Math.max(560, window.innerHeight - element.getBoundingClientRect().top - window.scrollY - 16)}px` : ''
    }
    fit()
    window.addEventListener('resize', fit)
    return () => window.removeEventListener('resize', fit)
  })

  const displayed = lastRun?.result ?? lastStored.data?.result?.result
  const stale = Boolean(lastRun && lastRun.key !== currentKey)
  const compareCodes = useMemo(() => resultCompanyCodes(displayed), [displayed])
  const aliases = resultAliases(outputColumns)
  const columnInfo = (column: string): ResultColumnInfo => {
    if (column === 'LatestPrice') return { label: 'Price', description: 'Latest close, split-adjusted. Hover a price for its date.' }
    if (column === 'ScreeningRank') return { label: 'Rank' }
    const derived = shownComputed.find(item => item.name === column)
    if (derived) {
      const preset = presetForTokens(derived.expression_tokens)
      const formula = (derived.expression_tokens ?? []).map(token => token.type === 'column' ? columnLabel(token.table, token.column) : token.type === 'op' ? { '*': '×', '/': '÷', '+': '+', '-': '−' }[token.op] : String(token.value)).join(' ')
      return { label: column, description: <span className="tip-lines"><strong>{column}</strong><span>{preset?.description ?? `Derived: ${formula}`}</span></span> }
    }
    const reference = aliases.get(column)
    if (!reference) return { label: column }
    const [table, ...rest] = reference.split('.')
    const label = columnLabel(table, rest.join('.'))
    return { label, description: <span className="tip-lines"><strong>{label}</strong><span>{tableInfo(table).label} · {reference}{formats[reference] === 'percent' ? ' · shown as %' : ''}</span></span> }
  }
  const enabledRules = criteria.filter(criterion => criterion.enabled !== false)
  const summary = describeScreen(criteria, match, criterion => describeRule(criterion, columnLabel, tokens => tokensFormat(tokens, formats) === 'percent'))
  const canSave = !saved.isError

  if (metrics.isLoading) return <LoadingState label="Loading the metric catalog" />
  if (metrics.isError) return <ErrorState error={metrics.error} retry={() => void metrics.refetch()} />

  return <div className="screening">
    <header className="screening-head">
      <div className="screening-head__title">
        <span className="eyebrow">Company discovery</span>
        <div className="screening-name">
          <input ref={nameInput} aria-label="Screen name" placeholder="Untitled screen" size={Math.max(14, Math.min(48, name.length + 2))} value={name} onChange={event => setName(event.target.value)} onKeyDown={event => { if (event.key === 'Enter') event.currentTarget.blur() }} />
          {loadedName
            ? dirty ? <span className="screening-state is-dirty" title={`Changed since it was saved as “${loadedName}”`}>Unsaved changes</span> : <span className="screening-state">Saved</span>
            : criteria.length > 0 && <span className="screening-state" title="Name the screen and press Ctrl+S to keep it">Not saved</span>}
        </div>
      </div>
      <div className="screening-head__actions">
        <div className="screens-anchor">
          <button type="button" data-screens-trigger className="button button--ghost button--small" aria-expanded={showScreens} onClick={() => setShowScreens(!showScreens)} title={`Open a saved or starter screen (${screensKey})`}><FolderOpen aria-hidden="true" />Screens <HotkeyKbd hotkey={screeningScope.byId.screens} /></button>
          {showScreens && <ScreensMenu saved={savedItems} starters={starters} current={loadedName} canSave={canSave} onOpenSaved={screen => void openSaved(screen)} onOpenStarter={openStarter} onNew={newScreen} onDelete={removeSaved} onClose={() => setShowScreens(false)} />}
        </div>
        <button type="button" className="button button--secondary button--small" disabled={saveMutation.isPending || !canSave} onClick={saveScreen} title={loadedName && name.trim() === loadedName ? `Save changes to “${loadedName}” (${saveKey})` : `Save under this name (${saveKey})`}><Save aria-hidden="true" />{saveMutation.isPending ? 'Saving…' : 'Save'} <HotkeyKbd hotkey={screeningCommandsScope.byId.save} /></button>
        <div className="screens-anchor">
          <button type="button" data-asof-trigger className={screeningDate ? 'screening-date is-set' : 'screening-date'} aria-expanded={showDate} aria-label={`As of: ${formatAsOf(screeningDate)}. Change the date (D)`} onClick={() => setShowDate(!showDate)}>
            <CalendarClock aria-hidden="true" />
            <Tip content="Point in time: rules see only filings published by this date and prices up to it. Press D to change it; the screen reruns." focusable={false}><span>As of</span></Tip>
            <strong>{formatAsOf(screeningDate)}</strong>
            <HotkeyKbd hotkey={screeningScope.byId.date} />
          </button>
          {showDate && <AsOfMenu value={screeningDate} onChoose={applyDate} onClose={() => setShowDate(false)} />}
        </div>
        <button type="button" className="button button--primary button--small" disabled={run.isPending} onClick={() => runScreen()} title={`Run the screen (${runKey})`}><FlaskConical aria-hidden="true" />{run.isPending ? 'Running…' : 'Run'} <HotkeyKbd hotkey={screeningCommandsScope.byId.run} /></button>
        <HotkeyHelpButton />
      </div>
    </header>
    {notice && <p className={notice.tone === 'warning' ? 'screening-notice is-warning' : 'screening-notice'} role="status">{notice.text}</p>}

    <div ref={layout} className={collapsed ? 'screening-layout is-collapsed' : 'screening-layout'}>
      <section className="panel screening-rules" aria-label="Rules">
        {collapsed
          ? <button type="button" className="screening-rules__summary" onClick={() => setCollapsed(false)} title="Show the rules (B)">
            <ChevronDown aria-hidden="true" />
            <strong>{enabledRules.length} {enabledRules.length === 1 ? 'rule' : 'rules'}</strong>
            <span>{summary || 'No rules: every company matches'}</span>
            <HotkeyKbd hotkey={screeningScope.byId.collapse} />
          </button>
          : <>
            <RuleBuilder criteria={criteria} match={match} options={options} ruleOptions={ruleOptions} formats={formats} tagNames={tagNames} openRuleId={openRuleId}
              onChange={setCriteria} onMatchChange={setMatch} onAddRule={addRule} />
            {criteria.length > 0 && <button type="button" className="text-button screening-rules__collapse" onClick={() => setCollapsed(true)} title="Collapse the rules to a summary (B)"><ChevronUp aria-hidden="true" />Collapse rules <HotkeyKbd hotkey={screeningScope.byId.collapse} /></button>}
          </>}
      </section>

      <section className="panel screening-results" aria-label="Results">
        <header className="screening-results__head">
          <h2>{displayed ? `${displayed.row_count.toLocaleString()} ${displayed.row_count === 1 ? 'company' : 'companies'}` : 'Results'}</h2>
          {lastRun && <span className="muted">{(lastRun.ms / 1000).toFixed(1)} s · {screeningDate ? `as of ${screeningDate}` : 'latest data'}</span>}
          {!lastRun && displayed && <span className="muted">Your last run{lastStored.data?.result?.updated_at ? ` · ${new Date(lastStored.data.result.updated_at).toLocaleString()}` : ''}</span>}
          {stale && displayed && !run.isPending && <button type="button" className="screening-stale" onClick={() => runScreen()} title="The rules or columns changed since these results">Rules changed · run again <HotkeyKbd hotkey={screeningCommandsScope.byId.run} /></button>}
          <span className="rule-builder__spacer" />
          <button type="button" className="button button--ghost button--small" onClick={() => setShowColumns(true)} title="Choose output columns"><Columns3 aria-hidden="true" />Columns {outputColumns.length + shownComputed.length} <HotkeyKbd hotkey={screeningScope.byId.columns} /></button>
          {displayed && <>
            <button type="button" className="button button--ghost button--small" onClick={() => void exportCsv()} title="Download every matching company with the current columns"><Download aria-hidden="true" />CSV</button>
            <button type="button" className="button button--ghost button--small" disabled={compareCodes.length < 2} onClick={() => navigate(`/compare?companies=${encodeURIComponent(compareCodes.slice(0, 12).join(','))}&source=screen`)} title="Open the first 12 matches in Comparison"><GitCompare aria-hidden="true" />Compare</button>
            <button type="button" className="button button--ghost button--small" onClick={() => navigate('/backtest?source=screen')} title="Rerun these rules at past dates in a rolling backtest"><FlaskConical aria-hidden="true" />Backtest</button>
          </>}
        </header>
        <div className={run.isPending && displayed ? 'screening-results__body is-running' : 'screening-results__body'}>
          {run.isPending && <div className="screening-running" role="status"><LoaderCircle className="spin" aria-hidden="true" />Running the screen…</div>}
          {run.isError && <ErrorState error={run.error} retry={() => runScreen()} />}
          {!run.isError && displayed && <ResultsTable key={lastRun?.at ?? 'stored'} hotkeys={!overlayOpen} result={displayed} columnInfo={columnInfo} />}
          {!run.isError && !displayed && !run.isPending && <EmptyState title="Run the screen to see matching companies" description={criteria.length ? 'Press Ctrl+Enter, or Run above.' : 'Add rules with N, or open a starter screen with O. Running with no rules lists every company.'} />}
        </div>
      </section>
    </div>

    {showColumns && <><div className="drawer-backdrop" onClick={() => setShowColumns(false)} /><ColumnsPanel catalog={catalog} options={options} columns={shownColumns} computed={shownComputed} onChange={next => { setColumns(next.columns); setComputed(next.computed) }} onClose={() => setShowColumns(false)} /></>}
  </div>
}
