import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'
import { ChevronDown, ChevronUp, CircleStop, Play, Plus, Save, Trash2 } from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'

import { apiPost, apiRequest } from '../../api/client'
import type {
  Job,
  JobCreateResponse,
  JobOutput,
  PipelineField,
  PipelineStep,
} from '../../api/types'
import { DataTable } from '../../components/DataTable'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { Card, Field, PageHeader } from '../../components/Page'

type SelectedStep = { id: string; name: string; overwrite: boolean }
type SavedSetup = { name: string; steps: SelectedStep[]; config: Record<string, unknown> }
const SETUPS_KEY = 'shade.pipeline.setups'
const TERMINAL_JOB_STATUSES = new Set(['cancelled', 'completed', 'failed', 'interrupted'])
function isTerminalJob(status?: string) { return status ? TERMINAL_JOB_STATUSES.has(status) : false }


function readSetups(): SavedSetup[] { try { return JSON.parse(localStorage.getItem(SETUPS_KEY) ?? '[]') } catch { return [] } }
function label(step: PipelineStep) { return step.display_name || step.name.replaceAll('_', ' ').replace(/\b\w/g, char => char.toUpperCase()) }

function ConfigField({ field, value, onChange }: { field: PipelineField; value: unknown; onChange: (value: unknown) => void }) {
  const inputType = field.type?.toLowerCase() ?? 'text'
  if (field.choices?.length) return <Field label={field.name} hint={field.description}><select className="select" value={String(value ?? field.default ?? '')} onChange={event => onChange(event.target.value)}>{field.choices.map(choice => <option key={choice}>{choice}</option>)}</select></Field>
  if (inputType.includes('bool')) return <label className="check"><input type="checkbox" checked={Boolean(value ?? field.default)} onChange={event => onChange(event.target.checked)} />{field.name}</label>
  if (inputType.includes('int') || inputType.includes('float') || inputType.includes('number')) return <Field label={field.name} hint={field.description}><input className="input" type="number" value={Number(value ?? field.default ?? 0)} onChange={event => onChange(Number(event.target.value))} /></Field>
  return <Field label={field.name} hint={field.description}><input className="input" value={String(value ?? field.default ?? '')} onChange={event => onChange(event.target.value)} /></Field>
}

export default function PipelinePage() {
  const [selected, setSelected] = useState<SelectedStep[]>([])
  const [config, setConfig] = useState<Record<string, unknown>>({})
  const [search, setSearch] = useState('')
  const [setupName, setSetupName] = useState('Daily data refresh')
  const [setups, setSetups] = useState<SavedSetup[]>(readSetups)
  const queryClient = useQueryClient()
  const [activeJobId, setActiveJobId] = useState<string>()
  const steps = useQuery({ queryKey: ['pipeline-steps'], queryFn: () => apiRequest<{ steps: PipelineStep[] }>('/api/steps') })
  const jobs = useQuery({
    queryKey: ['jobs'],
    queryFn: () => apiRequest<Job[]>('/api/jobs?limit=20'),
    refetchInterval: query => query.state.data?.some(job => !isTerminalJob(job.status)) ? 1000 : false,
  })
  const recoveredJobId = activeJobId ?? jobs.data?.find(job => !isTerminalJob(job.status))?.job_id
  const activeJob = useQuery({
    queryKey: ['job', recoveredJobId],
    queryFn: () => apiRequest<Job>(`/api/jobs/${encodeURIComponent(recoveredJobId ?? '')}`),
    enabled: Boolean(recoveredJobId),
    refetchInterval: query => isTerminalJob(query.state.data?.status) ? false : 750,
  })
  const activeStatus = activeJob.data?.status
  const isJobActive = Boolean(recoveredJobId && !isTerminalJob(activeStatus))
  const run = useMutation({
    mutationFn: () => apiPost<JobCreateResponse>('/api/pipeline/run', { steps: selected.map(step => ({ name: step.name, overwrite: step.overwrite })), config }),
    onSuccess: created => { setActiveJobId(created.job_id); void queryClient.invalidateQueries({ queryKey: ['jobs'] }) },
  })
  const cancel = useMutation({
    mutationFn: () => {
      if (!recoveredJobId) throw new Error('No active pipeline job')
      return apiPost<Job>(`/api/jobs/${encodeURIComponent(recoveredJobId)}/cancel`, { force: false })
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
  const filtered = (steps.data?.steps ?? []).filter(step => `${step.name} ${step.display_name ?? ''} ${step.description ?? ''}`.toLowerCase().includes(search.toLowerCase()))
  const selectedMeta = selected.map(item => ({ item, meta: steps.data?.steps.find(step => step.name === item.name) })).filter(entry => entry.meta)
  const requiredFields = useMemo(() => { const map = new Map<string, PipelineField>(); for (const entry of selectedMeta) for (const field of entry.meta?.input_fields ?? entry.meta?.parameters ?? []) map.set(field.name, field); return [...map.values()] }, [selectedMeta])
  const move = (index: number, direction: number) => setSelected(items => { const next = [...items]; const target = index + direction; if (target < 0 || target >= next.length) return items; [next[index], next[target]] = [next[target], next[index]]; return next })
  const saveSetup = () => { const next = [...setups.filter(setup => setup.name !== setupName), { name: setupName, steps: selected, config }]; setSetups(next); localStorage.setItem(SETUPS_KEY, JSON.stringify(next)) }
  const loadSetup = (name: string) => { const setup = setups.find(item => item.name === name); if (setup) { setSetupName(setup.name); setSelected(setup.steps); setConfig(setup.config) } }
  const jobColumns = useMemo<ColumnDef<Job>[]>(() => [{ accessorKey: 'status', header: 'Status', cell: info => <span className={`badge ${info.getValue() === 'completed' ? 'badge--success' : info.getValue() === 'failed' ? 'badge--danger' : ''}`}>{String(info.getValue())}</span> }, { accessorKey: 'current_step', header: 'Current step', cell: info => String(info.getValue() ?? '—') }, { accessorKey: 'created_at', header: 'Started', cell: info => info.getValue() ? new Date(String(info.getValue())).toLocaleString() : '—' }, { accessorKey: 'error_message', header: 'Message', cell: info => String(info.getValue() ?? '—') }], [])

  return <div className="stack">
    <PageHeader eyebrow="Data operations" title="Data pipeline" description="Run common updates as a recipe, or assemble dynamic steps and configuration." actions={<div className="button-row">{run.isPending ? <button className="button button--primary" disabled><Play />Queueing?</button> : isJobActive ? <button className="button button--danger" disabled={cancel.isPending || activeStatus === 'cancelling'} onClick={() => cancel.mutate()}><CircleStop />{activeStatus === 'cancelling' ? 'Cancelling?' : 'Cancel run'}</button> : <button className="button button--primary" disabled={!selected.length} onClick={() => run.mutate()}><Play />Run {selected.length} step{selected.length === 1 ? '' : 's'}</button>}</div>} />
    <div className="two-column">
      <Card title="Pipeline sequence" description="Steps execute from top to bottom." actions={<div className="button-row"><select className="select" aria-label="Load saved setup" value="" onChange={event => loadSetup(event.target.value)}><option value="">Load setup</option>{setups.map(setup => <option key={setup.name}>{setup.name}</option>)}</select><input className="input" aria-label="Setup name" value={setupName} onChange={event => setSetupName(event.target.value)} /><button className="button button--secondary" onClick={saveSetup}><Save />Save</button></div>}>
        {!selected.length ? <EmptyState title="No steps selected" description="Choose a prepared recipe or add individual steps from the library." action={<button className="button button--primary" onClick={() => { const preferred = ['download_documents', 'generate_financial_statements', 'generate_ratios', 'update_stock_prices']; const matches = preferred.map(name => steps.data?.steps.find(step => step.name === name)).filter(Boolean) as PipelineStep[]; setSelected(matches.map(step => ({ id: crypto.randomUUID(), name: step.name, overwrite: false }))) }}>Use daily refresh recipe</button>} /> : <div className="pipeline-list">{selected.map((item, index) => { const meta = steps.data?.steps.find(step => step.name === item.name); return <div className="pipeline-step" key={item.id}><span className="step-index">{index + 1}</span><div><strong>{meta ? label(meta) : item.name}</strong><small>{meta?.description}</small></div><label className="check"><input type="checkbox" checked={item.overwrite} onChange={event => setSelected(items => items.map(step => step.id === item.id ? { ...step, overwrite: event.target.checked } : step))} />Overwrite</label><div className="step-actions"><button className="icon-button" aria-label="Move step up" onClick={() => move(index, -1)}><ChevronUp /></button><button className="icon-button" aria-label="Move step down" onClick={() => move(index, 1)}><ChevronDown /></button><button className="icon-button" aria-label="Remove step" onClick={() => setSelected(items => items.filter(step => step.id !== item.id))}><Trash2 /></button></div></div> })}</div>}
      </Card>
      <Card title="Step library" description={`${filtered.length} available operations`}><input className="input" placeholder="Filter steps" value={search} onChange={event => setSearch(event.target.value)} />{steps.isLoading ? <LoadingState label="Loading pipeline steps" /> : steps.isError ? <ErrorState error={steps.error} /> : <div className="step-library">{filtered.map(step => <button key={step.name} disabled={selected.some(item => item.name === step.name)} onClick={() => setSelected(items => [...items, { id: crypto.randomUUID(), name: step.name, overwrite: false }])}><Plus /><span><strong>{label(step)}</strong><small>{step.description || step.category}</small></span></button>)}</div>}</Card>
    </div>
    {requiredFields.length > 0 && <Card title="Configuration" description="Only fields required by the selected steps are shown."><div className="field-row">{requiredFields.map(field => <ConfigField key={field.name} field={field} value={config[field.name]} onChange={value => setConfig(current => ({ ...current, [field.name]: value }))} />)}</div></Card>}
    {run.isError && <ErrorState error={run.error} retry={() => run.mutate()} />}
    {cancel.isError && <ErrorState error={cancel.error} />}
    {activeJob.isLoading && recoveredJobId && <Card><LoadingState label="Loading pipeline job" /></Card>}
    {activeJob.isError && <ErrorState error={activeJob.error} retry={() => activeJob.refetch()} />}
    {activeJob.data && <Card title="Latest run" description={activeJob.data.current_step ? 'Current step: ' + activeJob.data.current_step : 'Persisted pipeline job state'} actions={<span className={`badge ${activeJob.data.status === 'completed' ? 'badge--success' : activeJob.data.status === 'failed' || activeJob.data.status === 'interrupted' ? 'badge--danger' : ''}`}>{activeJob.data.status}</span>}><p>Progress: {Math.round(activeJob.data.progress_percent ?? 0)}% ? {activeJob.data.completed_step_count ?? 0}/{activeJob.data.step_count ?? activeJob.data.steps?.length ?? 0} steps complete</p>{activeJob.data.status_message && <p>{activeJob.data.status_message}</p>}{activeJob.data.error_message && <p>{activeJob.data.error_message}</p>}<details className="details"><summary>View step state</summary><pre>{JSON.stringify(activeJob.data.steps ?? [], null, 2)}</pre></details></Card>}
    {output.isError && <ErrorState error={output.error} />}
    {output.data && <Card title="Run output" description="Bounded, redacted output persisted by the backend."><details className="details"><summary>View run output</summary><pre>{JSON.stringify(output.data.output, null, 2)}</pre></details></Card>}
    <Card title="Recent runs" description="Pipeline job history from the backend.">{jobs.isLoading ? <LoadingState label="Loading pipeline history" /> : jobs.isError ? <ErrorState error={jobs.error} /> : <DataTable data={jobs.data ?? []} columns={jobColumns} emptyText="No pipeline runs yet." dense />}</Card>
  </div>
}
