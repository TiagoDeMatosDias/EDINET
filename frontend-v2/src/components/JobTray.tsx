import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Layers } from 'lucide-react'
import { useEffect, useId, useRef, useState } from 'react'
import { Link } from 'react-router-dom'

import { ACTIVE_JOB_STATUSES, cancelJob, type JobSummary, type MyJobsResponse } from '../api/jobs'
import { MY_JOBS_KEY, useMyJobs } from '../hooks/useMyJobs'
import { useToasts } from './Toasts'
import './jobs.css'

const RECENT_LIMIT = 10

const STATUS_TEXT: Record<string, string> = {
  open: 'Queued',
  in_progress: 'Running',
  cancelling: 'Cancelling',
  completed: 'Finished',
  failed: 'Failed',
  cancelled: 'Cancelled',
  interrupted: 'Interrupted',
}

export function toastFor(job: JobSummary): { message: string; tone: 'success' | 'danger' | 'info' } {
  if (job.status === 'completed') return { message: `${job.title} finished`, tone: 'success' }
  if (job.status === 'cancelled') return { message: `${job.title} was cancelled`, tone: 'info' }
  return { message: `${job.title} failed${job.error_message ? `: ${job.error_message}` : ''}`, tone: 'danger' }
}

/**
 * Toast when a job the user should hear about (``notify``) goes from active to finished while the app is open.
 * Jobs that were already finished on the first load stay quiet.
 */
function useJobToasts(data: MyJobsResponse | undefined) {
  const { push } = useToasts()
  const seen = useRef<Map<string, string> | null>(null)
  const firstServerTime = useRef<string | null>(null)
  useEffect(() => {
    if (!data?.jobs) return
    const { jobs } = data
    firstServerTime.current ??= data.server_time
    const previous = seen.current
    seen.current = new Map(jobs.map(job => [job.job_id, job.status]))
    if (!previous) return
    for (const job of jobs) {
      if (!job.notify || ACTIVE_JOB_STATUSES.has(job.status)) continue
      const before = previous.get(job.job_id)
      // A job we never saw is only news when it changed after the app opened (it ran and ended between two polls).
      const fresh = before === undefined && job.updated_at >= (firstServerTime.current ?? '')
      if ((before !== undefined && ACTIVE_JOB_STATUSES.has(before)) || fresh) {
        const { message, tone } = toastFor(job)
        push({ message, tone, link: job.link })
      }
    }
  }, [data, push])
}

function JobRow({ job, onOpen }: { job: JobSummary; onOpen: () => void }) {
  const client = useQueryClient()
  const [error, setError] = useState<string | null>(null)
  const cancel = useMutation({
    mutationFn: () => cancelJob(job.job_id),
    onSuccess: () => { setError(null); void client.invalidateQueries({ queryKey: MY_JOBS_KEY }) },
    onError: (err: Error) => setError(err.message),
  })
  const active = ACTIVE_JOB_STATUSES.has(job.status)
  const percent = Math.max(0, Math.min(100, Math.round(job.progress_percent)))
  return <li className={`job-row job-row--${job.status}`}>
    <div className="job-row__head">
      <strong className="job-row__title">{job.title}</strong>
      <span className="job-row__kind">{job.kind_label}</span>
    </div>
    <div className="job-row__status">
      <span>{STATUS_TEXT[job.status] ?? job.status}</span>
      {job.waiting_for_worker && <span className="badge job-row__waiting">Waiting for a worker</span>}
    </div>
    {job.status === 'in_progress' && <>
      <div className="job-row__bar" role="progressbar" aria-label={`${job.title} progress`} aria-valuemin={0} aria-valuemax={100} aria-valuenow={percent}><span style={{ width: `${percent}%` }} /></div>
      <small className="job-row__message">{percent}%{job.status_message ? ` · ${job.status_message}` : ''}</small>
    </>}
    {active && job.status !== 'in_progress' && job.status_message && <small className="job-row__message">{job.status_message}</small>}
    {(job.status === 'failed' || job.status === 'interrupted') && job.error_message && <small className="job-row__error">{job.error_message}</small>}
    {error && <small className="job-row__error" role="alert">{error}</small>}
    {(job.cancellable || job.link) && <div className="job-row__actions">
      {job.link && <Link className="text-button" to={job.link} onClick={onOpen}>Open</Link>}
      {job.cancellable && <button type="button" className="text-button text-button--danger" disabled={cancel.isPending || job.status === 'cancelling'} onClick={() => cancel.mutate()}>{job.status === 'cancelling' ? 'Cancelling…' : 'Cancel'}</button>}
    </div>}
  </li>
}

/** Top-bar button with the count of active jobs; opens a panel of active and recent jobs. */
export function JobTray({ enabled }: { enabled: boolean }) {
  const jobsQuery = useMyJobs(enabled)
  const [open, setOpen] = useState(false)
  const root = useRef<HTMLDivElement>(null)
  const button = useRef<HTMLButtonElement>(null)
  const panelId = useId()
  const jobs = jobsQuery.data?.jobs ?? undefined
  useJobToasts(jobsQuery.data)

  useEffect(() => {
    if (!open) return
    const onPointer = (event: MouseEvent) => { if (root.current && !root.current.contains(event.target as Node)) setOpen(false) }
    const onKey = (event: KeyboardEvent) => { if (event.key === 'Escape') { setOpen(false); button.current?.focus() } }
    document.addEventListener('mousedown', onPointer)
    document.addEventListener('keydown', onKey)
    return () => { document.removeEventListener('mousedown', onPointer); document.removeEventListener('keydown', onKey) }
  }, [open])

  if (!enabled) return null
  const active = (jobs ?? []).filter(job => ACTIVE_JOB_STATUSES.has(job.status))
  const recent = (jobs ?? []).filter(job => !ACTIVE_JOB_STATUSES.has(job.status)).sort((a, b) => b.updated_at.localeCompare(a.updated_at)).slice(0, RECENT_LIMIT)
  return <div className="job-tray" ref={root}>
    <button ref={button} type="button" className="job-tray__button" aria-expanded={open} aria-controls={open ? panelId : undefined} aria-label={active.length ? `Jobs, ${active.length} active` : 'Jobs'} onClick={() => setOpen(value => !value)}>
      <Layers aria-hidden="true" size={16} />
      <span className="job-tray__label">Jobs</span>
      {active.length > 0 && <span className="job-tray__count" data-testid="job-count">{active.length}</span>}
    </button>
    {open && <div id={panelId} className="job-tray__panel" role="region" aria-label="Background jobs">
      {jobsQuery.isError && <p className="job-tray__empty" role="alert">Jobs could not be loaded.</p>}
      {!jobsQuery.isError && jobs && jobs.length === 0 && <p className="job-tray__empty">No background jobs yet.</p>}
      {!jobsQuery.isError && !jobs && <p className="job-tray__empty">Loading jobs…</p>}
      {active.length > 0 && <section><h3>Active</h3><ul>{active.map(job => <JobRow key={job.job_id} job={job} onOpen={() => setOpen(false)} />)}</ul></section>}
      {recent.length > 0 && <section><h3>Recent</h3><ul>{recent.map(job => <JobRow key={job.job_id} job={job} onOpen={() => setOpen(false)} />)}</ul></section>}
    </div>}
  </div>
}
