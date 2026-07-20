import { useMutation, useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'
import { ChevronDown, ChevronUp, CircleStop, Play, Plus, Save, Trash2 } from 'lucide-react'
import { useMemo, useRef, useState } from 'react'

import { apiPost, apiRequest } from '../../api/client'
import type { Job, PipelineField, PipelineStep } from '../../api/types'
import { DataTable } from '../../components/DataTable'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { Card, Field, PageHeader } from '../../components/Page'

type SelectedStep = { id: string; name: string; overwrite: boolean }
type SavedSetup = { name: string; steps: SelectedStep[]; config: Record<string, unknown> }
const SETUPS_KEY = 'shade.pipeline.setups'

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
  const [result, setResult] = useState<Record<string, unknown>>()
  const abortRef = useRef<AbortController | null>(null)
  const steps = useQuery({ queryKey: ['pipeline-steps'], queryFn: () => apiRequest<{ steps: PipelineStep[] }>('/api/steps') })
  const jobs = useQuery({ queryKey: ['jobs'], queryFn: () => apiRequest<Job[]>('/api/jobs?limit=20') })
  const run = useMutation({ mutationFn: async () => { const controller = new AbortController(); abortRef.current = controller; try { return await apiPost<Record<string, unknown>>('/api/pipeline/run', { steps: selected.map(step => ({ name: step.name, overwrite: step.overwrite })), config }, controller.signal) } finally { abortRef.current = null } }, onSuccess: data => { setResult(data); jobs.refetch() } })
  const filtered = (steps.data?.steps ?? []).filter(step => `${step.name} ${step.display_name ?? ''} ${step.description ?? ''}`.toLowerCase().includes(search.toLowerCase()))
  const selectedMeta = selected.map(item => ({ item, meta: steps.data?.steps.find(step => step.name === item.name) })).filter(entry => entry.meta)
  const requiredFields = useMemo(() => { const map = new Map<string, PipelineField>(); for (const entry of selectedMeta) for (const field of entry.meta?.input_fields ?? entry.meta?.parameters ?? []) map.set(field.name, field); return [...map.values()] }, [selectedMeta])
  const move = (index: number, direction: number) => setSelected(items => { const next = [...items]; const target = index + direction; if (target < 0 || target >= next.length) return items; [next[index], next[target]] = [next[target], next[index]]; return next })
  const saveSetup = () => { const next = [...setups.filter(setup => setup.name !== setupName), { name: setupName, steps: selected, config }]; setSetups(next); localStorage.setItem(SETUPS_KEY, JSON.stringify(next)) }
  const loadSetup = (name: string) => { const setup = setups.find(item => item.name === name); if (setup) { setSetupName(setup.name); setSelected(setup.steps); setConfig(setup.config) } }
  const jobColumns = useMemo<ColumnDef<Job>[]>(() => [{ accessorKey: 'status', header: 'Status', cell: info => <span className={`badge ${info.getValue() === 'completed' ? 'badge--success' : info.getValue() === 'failed' ? 'badge--danger' : ''}`}>{String(info.getValue())}</span> }, { accessorKey: 'current_step', header: 'Current step', cell: info => String(info.getValue() ?? '—') }, { accessorKey: 'created_at', header: 'Started', cell: info => info.getValue() ? new Date(String(info.getValue())).toLocaleString() : '—' }, { accessorKey: 'error_message', header: 'Message', cell: info => String(info.getValue() ?? '—') }], [])

  return <div className="stack">
    <PageHeader eyebrow="Data operations" title="Data pipeline" description="Run common updates as a recipe, or open the advanced editor to assemble dynamic steps and configuration." actions={<div className="button-row">{run.isPending ? <button className="button button--danger" onClick={() => abortRef.current?.abort()}><CircleStop />Stop waiting</button> : <button className="button button--primary" disabled={!selected.length} onClick={() => run.mutate()}><Play />Run {selected.length} step{selected.length === 1 ? '' : 's'}</button>}</div>} />
    <div className="two-column">
      <Card title="Pipeline sequence" description="Steps execute from top to bottom." actions={<div className="button-row"><select className="select" aria-label="Load saved setup" value="" onChange={event => loadSetup(event.target.value)}><option value="">Load setup</option>{setups.map(setup => <option key={setup.name}>{setup.name}</option>)}</select><input className="input" aria-label="Setup name" value={setupName} onChange={event => setSetupName(event.target.value)} /><button className="button button--secondary" onClick={saveSetup}><Save />Save</button></div>}>
        {!selected.length ? <EmptyState title="No steps selected" description="Choose a prepared recipe or add individual steps from the library." action={<button className="button button--primary" onClick={() => { const preferred = ['download_documents', 'generate_financial_statements', 'generate_ratios', 'update_stock_prices']; const matches = preferred.map(name => steps.data?.steps.find(step => step.name === name)).filter(Boolean) as PipelineStep[]; setSelected(matches.map(step => ({ id: crypto.randomUUID(), name: step.name, overwrite: false }))) }}>Use daily refresh recipe</button>} /> : <div className="pipeline-list">{selected.map((item, index) => { const meta = steps.data?.steps.find(step => step.name === item.name); return <div className="pipeline-step" key={item.id}><span className="step-index">{index + 1}</span><div><strong>{meta ? label(meta) : item.name}</strong><small>{meta?.description}</small></div><label className="check"><input type="checkbox" checked={item.overwrite} onChange={event => setSelected(items => items.map(step => step.id === item.id ? { ...step, overwrite: event.target.checked } : step))} />Overwrite</label><div className="step-actions"><button className="icon-button" aria-label="Move step up" onClick={() => move(index, -1)}><ChevronUp /></button><button className="icon-button" aria-label="Move step down" onClick={() => move(index, 1)}><ChevronDown /></button><button className="icon-button" aria-label="Remove step" onClick={() => setSelected(items => items.filter(step => step.id !== item.id))}><Trash2 /></button></div></div> })}</div>}
      </Card>
      <Card title="Step library" description={`${filtered.length} available operations`}><input className="input" placeholder="Filter steps" value={search} onChange={event => setSearch(event.target.value)} />{steps.isLoading ? <LoadingState label="Loading pipeline steps" /> : steps.isError ? <ErrorState error={steps.error} /> : <div className="step-library">{filtered.map(step => <button key={step.name} disabled={selected.some(item => item.name === step.name)} onClick={() => setSelected(items => [...items, { id: crypto.randomUUID(), name: step.name, overwrite: false }])}><Plus /><span><strong>{label(step)}</strong><small>{step.description || step.category}</small></span></button>)}</div>}</Card>
    </div>
    {requiredFields.length > 0 && <Card title="Configuration" description="Only fields required by the selected steps are shown."><div className="field-row">{requiredFields.map(field => <ConfigField key={field.name} field={field} value={config[field.name]} onChange={value => setConfig(current => ({ ...current, [field.name]: value }))} />)}</div></Card>}
    {run.isPending && <Card><LoadingState label="Pipeline is running" /></Card>}
    {run.isError && <ErrorState error={run.error} retry={() => run.mutate()} />}
    {result && <Card title="Latest run" description="Technical output remains available without occupying the workspace."><details className="details"><summary>View run result</summary><pre>{JSON.stringify(result, null, 2)}</pre></details></Card>}
    <Card title="Recent runs" description="Pipeline job history from the backend.">{jobs.isLoading ? <LoadingState label="Loading pipeline history" /> : jobs.isError ? <ErrorState error={jobs.error} /> : <DataTable data={jobs.data ?? []} columns={jobColumns} emptyText="No pipeline runs yet." dense />}</Card>
  </div>
}
