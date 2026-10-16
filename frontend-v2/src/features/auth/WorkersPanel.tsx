import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Copy, Play, Plus, RotateCw } from 'lucide-react'
import { useState, type FormEvent } from 'react'

import { apiRequest } from '../../api/client'
import {
  isJobPending,
  type JobPendingBody,
  type EnrollmentResponse,
  type JobDetail,
  type LauncherStatus,
  type WorkersAdminResponse,
  type WorkerView,
} from '../../api/jobs'
import { LoadingState } from '../../components/Feedback'

export const WORKERS_KEY = ['admin-workers'] as const
const INTERACTIVE_MAX = 4
const BATCH_MAX = 8

const LAUNCHER_TEXT: Record<LauncherStatus['state'], string> = {
  running: 'Running',
  stopped: 'Stopped',
  missing: 'Not found (the worker program is missing)',
  disabled: 'Turned off',
  backoff: 'Restarting after an exit',
}

function ago(iso: string | null): string {
  if (!iso) return 'never'
  const seconds = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000))
  if (seconds < 10) return 'just now'
  if (seconds < 90) return `${seconds} s ago`
  if (seconds < 5400) return `${Math.round(seconds / 60)} min ago`
  if (seconds < 172800) return `${Math.round(seconds / 3600)} h ago`
  return new Date(iso).toLocaleDateString()
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : 'Something went wrong.'
}

function WorkerCard({ worker, onError }: { worker: WorkerView; onError: (message: string | null) => void }) {
  const client = useQueryClient()
  const [name, setName] = useState(worker.name)
  const [interactive, setInteractive] = useState(String(worker.interactive_slots))
  const [batch, setBatch] = useState(String(worker.batch_slots))
  const [armed, setArmed] = useState(false)
  const path = `/api/admin/workers/${encodeURIComponent(worker.worker_id)}`
  const refresh = () => { onError(null); void client.invalidateQueries({ queryKey: WORKERS_KEY }) }
  const patch = useMutation({
    mutationFn: (changes: Record<string, unknown>) => apiRequest<WorkerView>(path, { method: 'PATCH', body: JSON.stringify(changes) }),
    onSuccess: refresh,
    onError: (error: Error) => onError(errorText(error)),
  })
  const revoke = useMutation({
    mutationFn: () => apiRequest<{ revoked: boolean }>(path, { method: 'DELETE' }),
    onSuccess: refresh,
    onError: (error: Error) => { setArmed(false); onError(errorText(error)) },
  })
  const interactiveValue = Number(interactive)
  const batchValue = Number(batch)
  const valid = name.trim().length > 0
    && Number.isInteger(interactiveValue) && interactiveValue >= 0 && interactiveValue <= INTERACTIVE_MAX
    && Number.isInteger(batchValue) && batchValue >= 0 && batchValue <= BATCH_MAX
  const changed = name.trim() !== worker.name || interactiveValue !== worker.interactive_slots || batchValue !== worker.batch_slots
  const submit = (event: FormEvent) => {
    event.preventDefault()
    if (valid && changed) patch.mutate({ name: name.trim(), interactive_slots: interactiveValue, batch_slots: batchValue })
  }
  const label = `${worker.name}`
  return <li className="worker-card">
    <div className="worker-card__head">
      <span className={worker.online ? 'worker-dot worker-dot--on' : 'worker-dot'} role="img" aria-label={worker.online ? 'Online' : 'Offline'} />
      <strong>{worker.name}</strong>
      <span className="console-pill">{worker.kind === 'local' ? 'Local' : 'Enrolled'}</span>
      {!worker.enabled && <span className="console-pill console-pill--off">Disabled</span>}
      <small className="console-muted">{worker.online ? 'Online' : 'Offline'} · last seen {ago(worker.last_seen_at)}</small>
    </div>
    <dl className="worker-card__facts">
      <div><dt>Version</dt><dd>{worker.version ?? '—'}</dd></div>
      <div><dt>Host</dt><dd>{worker.hostname ? `${worker.hostname}${worker.pid ? ` (pid ${worker.pid})` : ''}` : '—'}</dd></div>
      <div><dt>Data access</dt><dd>{worker.data_access ? 'Yes' : 'No'}{worker.data_access_detail ? <small className="console-muted"> {worker.data_access_detail}</small> : null}</dd></div>
      <div><dt>Interactive slots</dt><dd>{worker.interactive_busy} of {worker.interactive_slots} busy</dd></div>
      <div><dt>Batch slots</dt><dd>{worker.batch_busy} of {worker.batch_slots} busy</dd></div>
    </dl>
    {worker.running_jobs.length > 0 && <ul className="worker-card__jobs" aria-label={`Jobs running on ${worker.name}`}>
      {worker.running_jobs.map(job => <li key={job.job_id}>{job.title} <small className="console-muted">({job.kind}, {Math.round(job.progress_percent)}%)</small></li>)}
    </ul>}
    <form className="console-inline" onSubmit={submit}>
      <label className="console-field console-field--grow"><span>Name</span><input className="input" aria-label={`Name of ${label}`} value={name} maxLength={120} onChange={event => setName(event.target.value)} /></label>
      <label className="console-field"><span>Interactive (0–{INTERACTIVE_MAX})</span><input className="input worker-slots" type="number" min={0} max={INTERACTIVE_MAX} aria-label={`Interactive slots of ${label}`} value={interactive} onChange={event => setInteractive(event.target.value)} /></label>
      <label className="console-field"><span>Batch (0–{BATCH_MAX})</span><input className="input worker-slots" type="number" min={0} max={BATCH_MAX} aria-label={`Batch slots of ${label}`} value={batch} onChange={event => setBatch(event.target.value)} /></label>
      <button type="submit" className="button button--secondary button--small" disabled={!valid || !changed || patch.isPending}>Save</button>
      <button type="button" className="button button--secondary button--small" disabled={patch.isPending} onClick={() => patch.mutate({ enabled: !worker.enabled })}>{worker.enabled ? 'Disable' : 'Enable'}</button>
      {worker.kind !== 'local' && <button type="button" className={armed ? 'text-button is-danger' : 'text-button'} disabled={revoke.isPending} onClick={() => { if (armed) revoke.mutate(); else setArmed(true) }} onBlur={() => setArmed(false)}>{armed ? 'Revoke: sure?' : 'Revoke'}</button>}
    </form>
  </li>
}

function EnrollForm() {
  const [name, setName] = useState('')
  const [copied, setCopied] = useState(false)
  const create = useMutation({
    mutationFn: () => apiRequest<EnrollmentResponse>('/api/admin/workers/enrollments', { method: 'POST', body: JSON.stringify({ name: name.trim() || 'Worker' }) }),
    onSuccess: () => setCopied(false),
  })
  const copy = async (text: string) => {
    try { await navigator.clipboard.writeText(text); setCopied(true) } catch { setCopied(false) }
  }
  return <div className="worker-enroll">
    <form className="console-inline" onSubmit={(event: FormEvent) => { event.preventDefault(); create.mutate() }}>
      <label className="console-field console-field--grow"><span>Name of the new worker</span><input className="input" value={name} maxLength={120} placeholder="Worker" onChange={event => setName(event.target.value)} /></label>
      <button type="submit" className="button button--primary button--small" disabled={create.isPending}><Plus aria-hidden="true" />Add a worker</button>
    </form>
    {create.isError && <p className="form-error" role="alert">{errorText(create.error)}</p>}
    {create.data && <div className="console-link worker-enroll__result" role="status">
      <strong>Enrollment command</strong>
      <code>{create.data.command}</code>
      <button type="button" className="button button--secondary button--small" onClick={() => void copy(create.data.command)}><Copy aria-hidden="true" />{copied ? 'Copied' : 'Copy'}</button>
      <small>Run this on the worker machine (<code>ShadeWorker enroll …</code>). It works once and expires {new Date(create.data.expires_at).toLocaleTimeString()}. For now the worker must run on this same machine, because it needs the same disk to reach the data.</small>
    </div>}
  </div>
}

function LocalWorker({ launcher, launchLocal }: { launcher: LauncherStatus; launchLocal: 'on' | 'off' }) {
  const client = useQueryClient()
  const [message, setMessage] = useState<string | null>(null)
  const restart = useMutation({
    mutationFn: () => apiRequest<LauncherStatus>('/api/admin/workers/local/restart', { method: 'POST' }),
    onSuccess: () => { setMessage('Restart requested.'); void client.invalidateQueries({ queryKey: WORKERS_KEY }) },
    onError: (error: Error) => setMessage(errorText(error)),
  })
  const setLaunch = useMutation({
    mutationFn: (value: 'on' | 'off') => apiRequest('/api/admin/settings/workers.launch_local', { method: 'PUT', body: JSON.stringify({ value }) }),
    onSuccess: () => { setMessage('Saved. This applies after the server restarts.'); void client.invalidateQueries({ queryKey: WORKERS_KEY }); void client.invalidateQueries({ queryKey: ['admin-server-settings'] }) },
    onError: (error: Error) => setMessage(errorText(error)),
  })
  return <div className="worker-local">
    <div className="worker-local__status">
      <strong>Local worker</strong>
      <span>{LAUNCHER_TEXT[launcher.state]}</span>
      <small className="console-muted">pid {launcher.pid ?? '—'} · {launcher.restarts} restart{launcher.restarts === 1 ? '' : 's'} · last exit code {launcher.last_exit_code ?? '—'}</small>
      <button type="button" className="button button--secondary button--small" disabled={restart.isPending} onClick={() => restart.mutate()}><RotateCw aria-hidden="true" />Restart</button>
    </div>
    <label className="console-check">
      <input type="checkbox" checked={launchLocal === 'on'} disabled={setLaunch.isPending} onChange={event => setLaunch.mutate(event.target.checked ? 'on' : 'off')} />
      Start the local worker with the server (applies after a server restart)
    </label>
    {message && <small role="status">{message}</small>}
  </div>
}

function TestJob() {
  const client = useQueryClient()
  const [outcome, setOutcome] = useState<string | null>(null)
  const run = useMutation({
    mutationFn: async () => {
      const started = performance.now()
      const body = await apiRequest<JobDetail | JobPendingBody>('/api/admin/workers/echo', { method: 'POST', body: JSON.stringify({ wait: true, result: 'ok' }) })
      return isJobPending(body) ? body.detail : `Test job completed in ${Math.round(performance.now() - started)} ms`
    },
    onMutate: () => setOutcome(null),
    onSuccess: text => { setOutcome(text); void client.invalidateQueries({ queryKey: ['my-jobs'] }) },
    onError: (error: Error) => setOutcome(`Test job failed: ${errorText(error)}`),
  })
  return <div className="worker-test">
    <button type="button" className="button button--secondary button--small" disabled={run.isPending} onClick={() => run.mutate()}><Play aria-hidden="true" />{run.isPending ? 'Running…' : 'Run test job'}</button>
    <small className="console-muted">Sends a tiny job to a worker to check that the whole path works.</small>
    {outcome && <small role="status">{outcome}</small>}
  </div>
}

/** Admin → Workers: the registry, slots, enrolment, local worker and a test job. */
export function WorkersPanel() {
  const [error, setError] = useState<string | null>(null)
  const workers = useQuery({
    queryKey: WORKERS_KEY,
    queryFn: () => apiRequest<WorkersAdminResponse>('/api/admin/workers'),
    refetchInterval: 5_000,
  })
  if (workers.isError) return <p className="form-error" role="alert">{errorText(workers.error)}</p>
  if (!workers.data) return <LoadingState label="Loading workers" />
  return <div className="worker-panel">
    {error && <p className="form-error" role="alert">{error} <button type="button" className="text-button" onClick={() => setError(null)}>Dismiss</button></p>}
    <LocalWorker launcher={workers.data.launcher} launchLocal={workers.data.settings.launch_local} />
    {workers.data.workers.length === 0 ? <p className="console-muted">No workers yet.</p> : <ul className="worker-list">{workers.data.workers.map(worker => <WorkerCard key={`${worker.worker_id}:${worker.name}:${worker.interactive_slots}:${worker.batch_slots}`} worker={worker} onError={setError} />)}</ul>}
    <EnrollForm />
    <TestJob />
  </div>
}
