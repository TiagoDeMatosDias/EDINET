import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ArrowDown, ArrowUp, CircleStop, Keyboard, Play, Plus, Save, Trash2, Zap } from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState, type KeyboardEvent, type ReactNode } from 'react'

import { apiPost, apiRequest } from '../../api/client'
import type { Job, JobCreateResponse, JobOutput, PipelineStep } from '../../api/types'
import { ErrorState, LoadingState } from '../../components/Feedback'
import { ShortcutsDialog, type ShortcutGroup } from '../../components/ShortcutsDialog'
import { useHotkeys } from '../../hooks/useHotkeys'
import { ConfigField } from './ConfigField'
import { RunOutput, StepStateTable } from './JobDetails'
import { configKey, DAILY_RECIPE, formatMs, isTerminalJob, jobDuration, readSetups, runPayload, SETUPS_KEY, stepLabel, stripFileUploads, type SavedSetup, type SelectedStep } from './pipelineModel'
import './pipeline.css'

export { ConfigField } from './ConfigField'

const SHORTCUTS: ShortcutGroup[] = [
  { title: 'Moving around', shortcuts: [
    { keys: ['1', '2', '3', '4', '5'], label: 'Library, Sequence, Configuration, Latest run, History' },
    { keys: ['J', 'K'], label: 'Next or previous item in the focused list (↓ ↑)' },
    { keys: ['F'], label: 'Filter the step library' },
    { keys: ['?'], label: 'Show or hide this list' },
  ] },
  { title: 'Building a run', shortcuts: [
    { keys: ['Enter', 'Space'], label: 'Library: add the step · Sequence: configure it' },
    { keys: ['Shift+J', 'Shift+K'], label: 'Sequence: move the step down or up' },
    { keys: ['O'], label: 'Sequence: overwrite on or off' },
    { keys: ['X', 'Del'], label: 'Sequence: remove the step' },
    { keys: ['D'], label: 'Use the daily refresh recipe' },
    { keys: ['S'], label: 'Save the sequence as a setup' },
    { keys: ['L'], label: 'Load a saved setup' },
  ] },
  { title: 'Running', shortcuts: [
    { keys: ['Shift+R'], label: 'Run the sequence' },
    { keys: ['C'], label: 'Cancel the running job (press twice)' },
    { keys: ['Enter'], label: 'History: show that run' },
  ] },
]

function statusClass(status?: string) {
  return `pl-status pl-status--${status ?? 'unknown'}`
}

/** A list the keyboard can walk: ↑ ↓ (and J K) move, other keys are handed to ``onKey``. */
function useRovingList(count: number) {
  const [index, setIndex] = useState(0)
  const ref = useRef<HTMLUListElement | HTMLTableSectionElement>(null)
  const current = Math.min(index, Math.max(0, count - 1))
  const focus = (next: number) => {
    const bounded = Math.max(0, Math.min(count - 1, next))
    setIndex(bounded)
    ref.current?.querySelectorAll<HTMLElement>('[data-row]')[bounded]?.focus()
  }
  const move = (event: KeyboardEvent, onKey?: (key: string, shift: boolean) => boolean) => {
    if (onKey?.(event.key, event.shiftKey)) { event.preventDefault(); event.stopPropagation(); return }
    if ((event.key === 'ArrowDown' || event.key === 'j') && !event.shiftKey) { event.preventDefault(); event.stopPropagation(); focus(current + 1) }
    if ((event.key === 'ArrowUp' || event.key === 'k') && !event.shiftKey) { event.preventDefault(); event.stopPropagation(); focus(current - 1) }
  }
  return { index: current, setIndex, ref, focus, move }
}

function Region({ index, title, meta, actions, children, className = '' }: { index: number; title: string; meta?: ReactNode; actions?: ReactNode; children: ReactNode; className?: string }) {
  return <section className={`pl-region ${className}`} id={`pl-region-${index}`} aria-labelledby={`pl-region-${index}-title`}>
    <header><kbd aria-hidden="true">{index}</kbd><h2 id={`pl-region-${index}-title`} tabIndex={-1}>{title}</h2>{meta && <span className="pl-region__meta">{meta}</span>}{actions && <div className="pl-region__actions">{actions}</div>}</header>
    {children}
  </section>
}

export default function PipelinePage() {
  const [selected, setSelected] = useState<SelectedStep[]>([])
  const [config, setConfig] = useState<Record<string, unknown>>({})
  const [search, setSearch] = useState('')
  const [setupName, setSetupName] = useState('Daily data refresh')
  const [setups, setSetups] = useState<SavedSetup[]>(readSetups)
  const [activeJobId, setActiveJobId] = useState<string>()
  const [focusedStep, setFocusedStep] = useState<string | null>(null)
  const [showAllConfig, setShowAllConfig] = useState(false)
  const [armedCancel, setArmedCancel] = useState(false)
  const [help, setHelp] = useState(false)
  const [notice, setNotice] = useState<string | null>(null)
  const closeHelp = useCallback(() => setHelp(false), [])
  const filterInput = useRef<HTMLInputElement>(null)
  const setupSelect = useRef<HTMLSelectElement>(null)
  const queryClient = useQueryClient()

  const steps = useQuery({ queryKey: ['pipeline-steps'], queryFn: () => apiRequest<{ steps: PipelineStep[] }>('/api/steps') })
  const serverConfig = useQuery({ queryKey: ['server-config'], queryFn: () => apiRequest<{ max_upload_bytes: number }>('/api/config'), retry: false })
  const jobs = useQuery({
    queryKey: ['jobs'],
    queryFn: () => apiRequest<Job[]>('/api/jobs?limit=30'),
    refetchInterval: query => query.state.data?.some(job => !isTerminalJob(job.status)) ? 1000 : false,
  })
  const recoveredJobId = activeJobId ?? jobs.data?.find(job => !isTerminalJob(job.status))?.job_id ?? jobs.data?.[0]?.job_id
  const activeJob = useQuery({
    queryKey: ['job', recoveredJobId],
    queryFn: () => apiRequest<Job>(`/api/jobs/${encodeURIComponent(recoveredJobId ?? '')}`),
    enabled: Boolean(recoveredJobId),
    refetchInterval: query => isTerminalJob(query.state.data?.status) ? false : 750,
  })
  const activeStatus = activeJob.data?.status
  const running = Boolean(jobs.data?.some(job => !isTerminalJob(job.status)))
  const runningJobId = jobs.data?.find(job => !isTerminalJob(job.status))?.job_id
  const library = useMemo(() => (steps.data?.steps ?? []).filter(step => `${step.name} ${step.display_name ?? ''} ${step.description ?? ''} ${step.category ?? ''}`.toLowerCase().includes(search.toLowerCase())), [steps.data, search])
  const metaFor = useCallback((name: string) => steps.data?.steps.find(step => step.name === name), [steps.data])

  const run = useMutation({
    mutationFn: () => {
      const { payload, files } = runPayload(selected, config, steps.data?.steps ?? [])
      if (!files.length) return apiPost<JobCreateResponse>('/api/pipeline/run', payload)
      const form = new FormData()
      form.set('config', JSON.stringify(payload))
      for (const upload of files) form.append(`upload:${upload.key}`, upload.file, upload.file.name)
      return apiRequest<JobCreateResponse>('/api/pipeline/run', { method: 'POST', body: form })
    },
    onSuccess: created => { setActiveJobId(created.job_id); void queryClient.invalidateQueries({ queryKey: ['jobs'] }) },
  })
  const cancel = useMutation({
    mutationFn: () => {
      if (!runningJobId) throw new Error('No pipeline job is running')
      return apiPost<Job>(`/api/jobs/${encodeURIComponent(runningJobId)}/cancel`, { force: false })
    },
    onSuccess: job => { queryClient.setQueryData(['job', job.job_id], job); void queryClient.invalidateQueries({ queryKey: ['jobs'] }) },
  })
  const output = useQuery({
    queryKey: ['job-output', recoveredJobId],
    queryFn: () => apiRequest<JobOutput>(`/api/jobs/${encodeURIComponent(recoveredJobId ?? '')}/output`),
    enabled: Boolean(recoveredJobId && isTerminalJob(activeStatus)),
  })
  useEffect(() => {
    if (recoveredJobId && isTerminalJob(activeStatus)) void queryClient.invalidateQueries({ queryKey: ['jobs'] })
  }, [recoveredJobId, activeStatus, queryClient])
  useEffect(() => {
    if (!armedCancel) return
    const timer = setTimeout(() => setArmedCancel(false), 4000)
    return () => clearTimeout(timer)
  }, [armedCancel])
  useEffect(() => {
    if (!notice) return
    const timer = setTimeout(() => setNotice(null), 4000)
    return () => clearTimeout(timer)
  }, [notice])

  const { index: libIndex, setIndex: setLibIndex, ref: libRef, focus: libFocus, move: libMove } = useRovingList(library.length)
  const { index: seqIndex, setIndex: setSeqIndex, ref: seqRef, focus: seqFocus, move: seqMove } = useRovingList(selected.length)
  const { index: historyIndex, setIndex: setHistoryIndex, ref: historyRef, move: historyMove } = useRovingList(jobs.data?.length ?? 0)

  const add = (step: PipelineStep) => {
    if (selected.some(item => item.name === step.name)) { setNotice(`${stepLabel(step)} is already in the sequence`); return }
    const id = crypto.randomUUID()
    setSelected(items => [...items, { id, name: step.name, overwrite: false }])
    setFocusedStep(id)
  }
  const move = (index: number, direction: number) => {
    const target = index + direction
    if (target < 0 || target >= selected.length) return
    setSelected(items => { const next = [...items]; [next[index], next[target]] = [next[target], next[index]]; return next })
    setSeqIndex(target)
    requestAnimationFrame(() => seqFocus(target))
  }
  const remove = (index: number) => setSelected(items => items.filter((_, position) => position !== index))
  const toggleOverwrite = (index: number) => setSelected(items => items.map((item, position) => position === index ? { ...item, overwrite: !item.overwrite } : item))
  const saveSetup = () => {
    const next = [...setups.filter(setup => setup.name !== setupName), { name: setupName, steps: selected, config: stripFileUploads(config) }]
    setSetups(next)
    localStorage.setItem(SETUPS_KEY, JSON.stringify(next))
    setNotice(`Saved “${setupName}”`)
  }
  const loadSetup = (name: string) => {
    const setup = setups.find(item => item.name === name)
    if (!setup) return
    setSetupName(setup.name)
    setSelected(setup.steps)
    setConfig(setup.config)
    setNotice(`Loaded “${setup.name}”`)
  }
  const useDaily = () => {
    const matches = DAILY_RECIPE.map(name => metaFor(name)).filter(Boolean) as PipelineStep[]
    setSelected(matches.map(step => ({ id: crypto.randomUUID(), name: step.name, overwrite: false })))
  }
  const focusRegion = (index: number) => {
    const region = document.getElementById(`pl-region-${index}`)
    ;(region?.querySelector<HTMLElement>('[data-row][tabindex="0"]') ?? region?.querySelector<HTMLElement>('input, select, button, h2'))?.focus()
  }
  const startRun = () => { if (selected.length && !running && !run.isPending) run.mutate() }
  const cancelRun = () => {
    if (!running) return
    if (armedCancel) { setArmedCancel(false); cancel.mutate() } else setArmedCancel(true)
  }

  useHotkeys({
    ...Object.fromEntries([1, 2, 3, 4, 5].map(index => [String(index), () => focusRegion(index)])),
    f: () => filterInput.current?.focus(),
    d: useDaily,
    s: saveSetup,
    l: () => setupSelect.current?.focus(),
    R: startRun,
    c: cancelRun,
    '?': () => setHelp(true),
  }, !help)

  const configured = selected.map(item => ({ item, meta: metaFor(item.name) })).filter(entry => entry.meta && (entry.meta.input_fields ?? entry.meta.parameters ?? []).length)
  const shownConfig = showAllConfig ? configured : configured.filter(entry => entry.item.id === (focusedStep ?? selected[seqIndex]?.id))
  const job = activeJob.data
  const progress = Math.round(job?.progress_percent ?? 0)

  return <div className="pl-page">
    <header className="pl-head">
      <div><span className="eyebrow">Data operations</span><h1>Data pipeline</h1></div>
      <span className={statusClass(running ? 'running' : job?.status)}>{running ? `running · ${progress}%` : job ? `last run ${job.status}` : 'idle'}</span>
      <div className="pl-head__setups">
        <select ref={setupSelect} className="select" aria-label="Load saved setup" value="" onChange={event => loadSetup(event.target.value)}><option value="">Load setup… (L)</option>{setups.map(setup => <option key={setup.name}>{setup.name}</option>)}</select>
        <input className="input" aria-label="Setup name" value={setupName} onChange={event => setSetupName(event.target.value)} onKeyDown={event => { if (event.key === 'Enter') saveSetup() }} />
        <button type="button" className="button button--secondary button--small" onClick={saveSetup} title="Save the sequence (S)"><Save aria-hidden="true" />Save</button>
        <button type="button" className="button button--secondary button--small" onClick={useDaily} title="Daily refresh recipe (D)"><Zap aria-hidden="true" />Daily recipe</button>
      </div>
      <div className="pl-head__run">
        {running
          ? <button type="button" className="button button--danger button--small" disabled={cancel.isPending || activeStatus === 'cancelling'} onClick={cancelRun}><CircleStop aria-hidden="true" />{activeStatus === 'cancelling' ? 'Cancelling…' : armedCancel ? 'Cancel: sure?' : 'Cancel run'} <kbd>C</kbd></button>
          : <button type="button" className="button button--primary button--small" disabled={!selected.length || run.isPending} onClick={startRun}><Play aria-hidden="true" />{run.isPending ? 'Queueing…' : `Run ${selected.length} step${selected.length === 1 ? '' : 's'}`} <kbd>⇧R</kbd></button>}
        <button type="button" className="icon-button" onClick={() => setHelp(true)} title="Keyboard shortcuts (?)" aria-label="Keyboard shortcuts"><Keyboard /></button>
      </div>
    </header>
    {notice && <div className="pl-notice" role="status">{notice}</div>}
    {run.isError && <ErrorState error={run.error} retry={() => run.mutate()} />}
    {cancel.isError && <ErrorState error={cancel.error} />}

    <div className="pl-grid">
      <Region index={1} title="Step library" meta={`${library.length} of ${steps.data?.steps.length ?? 0}`}>
        <label className="pl-filter"><input ref={filterInput} className="input" placeholder="Filter steps (F)" aria-label="Filter steps" value={search} onChange={event => setSearch(event.target.value)} onKeyDown={event => {
          if (event.key === 'ArrowDown' || event.key === 'Enter') { event.preventDefault(); libFocus(0) }
          if (event.key === 'Escape') { setSearch(''); event.currentTarget.blur() }
        }} /></label>
        {steps.isLoading ? <LoadingState label="Loading pipeline steps" /> : steps.isError ? <ErrorState error={steps.error} /> : <ul className="pl-list" ref={libRef as React.RefObject<HTMLUListElement>} aria-label="Step library" onKeyDown={event => libMove(event, key => {
          if (key === 'Enter' || key === ' ') { const step = library[libIndex]; if (step) add(step); return true }
          return false
        })}>
          {library.map((step, index) => {
            const added = selected.some(item => item.name === step.name)
            return <li key={step.name} data-row tabIndex={index === libIndex ? 0 : -1} className={[index === libIndex && 'is-cursor', added && 'is-added'].filter(Boolean).join(' ')} onFocus={() => setLibIndex(index)} onClick={() => setLibIndex(index)} onDoubleClick={() => add(step)}>
              <div><strong>{stepLabel(step)}</strong><small>{step.description || step.category}</small></div>
              <button type="button" className="icon-button" aria-label={added ? `${stepLabel(step)} is in the sequence` : `Add ${stepLabel(step)}`} disabled={added} onClick={event => { event.stopPropagation(); add(step) }}><Plus /></button>
            </li>
          })}
        </ul>}
      </Region>

      <Region index={2} title="Sequence" meta={selected.length ? 'top to bottom' : 'empty'} actions={selected.length > 0 && <button type="button" className="text-button" onClick={() => setSelected([])}>Clear</button>}>
        {selected.length ? <ul className="pl-list pl-list--sequence" ref={seqRef as React.RefObject<HTMLUListElement>} aria-label="Sequence" onKeyDown={event => seqMove(event, (key, shift) => {
          if (shift && (key === 'J' || key === 'ArrowDown')) { move(seqIndex, 1); return true }
          if (shift && (key === 'K' || key === 'ArrowUp')) { move(seqIndex, -1); return true }
          if (key === 'o') { toggleOverwrite(seqIndex); return true }
          if (key === 'x' || key === 'Delete') { remove(seqIndex); return true }
          if (key === 'Enter' || key === ' ') { setFocusedStep(selected[seqIndex]?.id ?? null); focusRegion(3); return true }
          return false
        })}>
          {selected.map((item, index) => {
            const meta = metaFor(item.name)
            const fields = (meta?.input_fields ?? meta?.parameters ?? []).length
            return <li key={item.id} data-row tabIndex={index === seqIndex ? 0 : -1} className={[index === seqIndex && 'is-cursor', item.id === focusedStep && 'is-focused'].filter(Boolean).join(' ')} onFocus={() => { setSeqIndex(index); setFocusedStep(item.id) }} onClick={() => { setSeqIndex(index); setFocusedStep(item.id) }}>
              <span className="pl-index">{index + 1}</span>
              <div><strong>{meta ? stepLabel(meta) : item.name}</strong><small>{fields ? `${fields} setting${fields === 1 ? '' : 's'}` : 'no settings'}{item.overwrite ? ' · overwrite' : ''}</small></div>
              <label className="pl-overwrite" title="Overwrite existing data (O)"><input type="checkbox" checked={item.overwrite} onChange={() => toggleOverwrite(index)} />Overwrite</label>
              <span className="pl-step-actions">
                <button type="button" className="icon-button" aria-label="Move step up" onClick={event => { event.stopPropagation(); move(index, -1) }}><ArrowUp /></button>
                <button type="button" className="icon-button" aria-label="Move step down" onClick={event => { event.stopPropagation(); move(index, 1) }}><ArrowDown /></button>
                <button type="button" className="icon-button" aria-label="Remove step" onClick={event => { event.stopPropagation(); remove(index) }}><Trash2 /></button>
              </span>
            </li>
          })}
        </ul> : <div className="pl-empty"><p>Add steps from the library (Enter), or start from the daily recipe.</p><button type="button" className="button button--primary button--small" onClick={useDaily}><Zap aria-hidden="true" />Use daily refresh recipe <kbd>D</kbd></button></div>}
      </Region>

      <Region index={3} title="Configuration" meta={configured.length ? `${configured.length} step${configured.length === 1 ? '' : 's'} with settings` : 'nothing to set'} actions={configured.length > 1 && <label className="pl-overwrite"><input type="checkbox" checked={showAllConfig} onChange={event => setShowAllConfig(event.target.checked)} />All steps</label>}>
        {shownConfig.length ? shownConfig.map(({ item, meta }) => <div key={item.id} className="pl-config">
          <h3>{stepLabel(meta!)} <code>{meta!.name}</code></h3>
          {meta!.description && <p>{meta!.description}</p>}
          <div className="pl-config__fields">
            {(meta!.input_fields ?? meta!.parameters ?? []).map(field => <ConfigField
              key={field.name}
              field={field}
              value={config[configKey(item.name, field.name)]}
              maxUploadBytes={serverConfig.data?.max_upload_bytes}
              onChange={value => setConfig(current => ({ ...current, [configKey(item.name, field.name)]: value }))}
            />)}
          </div>
        </div>) : <p className="pl-muted">{configured.length ? 'Pick a step in the sequence to set it up.' : 'The steps in the sequence take no settings.'}</p>}
      </Region>
    </div>

    <div className="pl-grid pl-grid--runs">
      <Region index={4} title="Latest run" meta={job ? `${job.completed_step_count ?? 0}/${job.step_count ?? job.steps?.length ?? 0} steps · ${formatMs(jobDuration(job))}` : undefined} actions={job && <span className={statusClass(job.status)}>{job.status}</span>}>
        {activeJob.isLoading && recoveredJobId ? <LoadingState label="Loading pipeline job" /> : activeJob.isError ? <ErrorState error={activeJob.error} retry={() => activeJob.refetch()} /> : job ? <>
          <div className="pl-progress" role="progressbar" aria-valuenow={progress} aria-valuemin={0} aria-valuemax={100} aria-label="Run progress"><span style={{ width: `${progress}%` }} /></div>
          {(job.status_message || job.error_message) && <p className={job.error_message ? 'pl-error' : 'pl-muted'}>{job.error_message || job.status_message}</p>}
          <StepStateTable steps={job.steps ?? []} />
          {output.data && <details className="details pl-output"><summary>Run output</summary><RunOutput output={output.data.output} /></details>}
          {output.isError && <ErrorState error={output.error} />}
        </> : <p className="pl-muted">No runs yet.</p>}
      </Region>

      <Region index={5} title="History" meta={jobs.data ? `${jobs.data.length} runs` : undefined}>
        {jobs.isLoading ? <LoadingState label="Loading pipeline history" /> : jobs.isError ? <ErrorState error={jobs.error} /> : jobs.data?.length ? <table className="pl-table">
          <thead><tr><th>Status</th><th>Steps</th><th>Started</th><th className="num">Took</th><th>Message</th></tr></thead>
          <tbody ref={historyRef as React.RefObject<HTMLTableSectionElement>} onKeyDown={event => historyMove(event, key => {
            if (key === 'Enter') { const row = jobs.data?.[historyIndex]; if (row) setActiveJobId(row.job_id); return true }
            return false
          })}>
            {jobs.data.map((row, index) => <tr key={row.job_id} data-row tabIndex={index === historyIndex ? 0 : -1} className={[index === historyIndex && 'is-cursor', row.job_id === recoveredJobId && 'is-shown'].filter(Boolean).join(' ')} onFocus={() => setHistoryIndex(index)} onClick={() => { setHistoryIndex(index); setActiveJobId(row.job_id) }}>
              <td><span className={statusClass(row.status)}>{row.status}</span></td>
              <td className="pl-table__steps" title={row.steps?.map(step => step.step_name).join(' → ')}>{row.steps?.map(step => step.step_name).join(' → ') || row.current_step || '—'}</td>
              <td className="mono">{row.created_at ? new Date(row.created_at).toLocaleString('en-GB', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' }) : '—'}</td>
              <td className="num mono">{formatMs(jobDuration(row))}</td>
              <td className={row.error_message ? 'pl-error' : 'pl-muted'} title={row.error_message ?? row.status_message ?? ''}>{row.error_message || row.status_message || '—'}</td>
            </tr>)}
          </tbody>
        </table> : <p className="pl-muted">No pipeline runs yet.</p>}
      </Region>
    </div>
    {help && <ShortcutsDialog groups={SHORTCUTS} onClose={closeHelp} />}
  </div>
}
