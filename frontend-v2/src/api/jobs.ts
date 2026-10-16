// Types and helpers for work the server hands to job workers.
// Pinned by docs/Feature Development/Job Workers Plan.md (§8, §11).
import { apiRequest } from './client'

export type JobStatus = 'open' | 'in_progress' | 'cancelling' | 'completed' | 'failed' | 'cancelled' | 'interrupted'

export const TERMINAL_JOB_STATUSES: ReadonlySet<string> = new Set(['completed', 'failed', 'cancelled', 'interrupted'])
export const ACTIVE_JOB_STATUSES: ReadonlySet<string> = new Set(['open', 'in_progress', 'cancelling'])

export interface JobSummary {
  job_id: string
  kind: string
  kind_label: string
  title: string
  status: JobStatus
  progress_percent: number
  status_message: string | null
  progress: Record<string, unknown> | null
  created_at: string
  started_at: string | null
  completed_at: string | null
  updated_at: string
  error_message: string | null
  notify: boolean
  link: string | null
  cancellable: boolean
  waiting_for_worker: boolean
}

export interface JobDetail extends JobSummary {
  result: Record<string, unknown> | null
}

export interface WorkerCapacity {
  online: number
  interactive_slots: number
  batch_slots: number
}

export interface MyJobsResponse {
  jobs: JobSummary[]
  server_time: string
  workers: WorkerCapacity
}

/** Body of a 202 answer from an endpoint whose work is still running on a worker. */
export interface JobPendingBody {
  code: 'job_pending'
  job_id: string
  waiting_for_worker: boolean
  detail: string
}

export interface BacktestJobView {
  job_id: string
  kind: 'single' | 'csv' | 'rolling'
  status: 'queued' | 'running' | 'saving' | 'complete' | 'failed' | 'cancelled'
  progress: { completed_backtests?: number; total_backtests?: number; period_index?: number; total_periods?: number; phase?: string; period?: string; status?: string }
  result_id: string | null
  error: string | null
  created_at: number
  title: string
  waiting_for_worker: boolean
}

export interface WorkerRunningJob {
  job_id: string
  kind: string
  title: string
  started_at: string | null
  progress_percent: number
}

export interface WorkerView {
  worker_id: string
  name: string
  kind: 'local' | 'enrolled'
  enabled: boolean
  interactive_slots: number
  batch_slots: number
  created_at: string
  online: boolean
  last_seen_at: string | null
  version: string | null
  hostname: string | null
  pid: number | null
  platform: string | null
  data_access: boolean
  data_access_detail: string | null
  interactive_busy: number
  batch_busy: number
  running_jobs: WorkerRunningJob[]
}

export interface LauncherStatus {
  state: 'running' | 'stopped' | 'missing' | 'disabled' | 'backoff'
  pid: number | null
  restarts: number
  last_exit_code: number | null
  command: string[] | null
}

export interface WorkersAdminResponse {
  workers: WorkerView[]
  launcher: LauncherStatus
  settings: { launch_local: 'on' | 'off' }
}

export interface EnrollmentResponse {
  enrollment: string
  command: string
  expires_at: string
}

export function isJobPending(value: unknown): value is JobPendingBody {
  return typeof value === 'object' && value !== null && (value as { code?: unknown }).code === 'job_pending'
    && typeof (value as { job_id?: unknown }).job_id === 'string'
}

export function getJob(jobId: string, signal?: AbortSignal) {
  return apiRequest<JobDetail>(`/api/my-jobs/${encodeURIComponent(jobId)}`, { signal })
}

export function cancelJob(jobId: string, force = false) {
  return apiRequest<JobSummary>(`/api/my-jobs/${encodeURIComponent(jobId)}/cancel`, { method: 'POST', body: JSON.stringify({ force }) })
}

function sleep(ms: number, signal?: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(resolve, ms)
    signal?.addEventListener('abort', () => { clearTimeout(timer); reject(signal.reason ?? new DOMException('Aborted', 'AbortError')) }, { once: true })
  })
}

/** Poll a job until it reaches a terminal status; resolves with its final detail. */
export async function waitForJob(jobId: string, { signal, intervalMs = 1500 }: { signal?: AbortSignal; intervalMs?: number } = {}): Promise<JobDetail> {
  for (;;) {
    const job = await getJob(jobId, signal)
    if (TERMINAL_JOB_STATUSES.has(job.status)) return job
    await sleep(intervalMs, signal)
  }
}

/** Error thrown by ``resultOfJob`` when the job ends without completing. */
export class JobEndedError extends Error {
  readonly job: JobDetail
  constructor(job: JobDetail) {
    super(job.error_message || (job.status === 'cancelled' ? 'The job was cancelled.' : 'The job did not finish.'))
    this.name = 'JobEndedError'
    this.job = job
  }
}

/**
 * Follow a ``job_pending`` answer to its end and return the job's stored result.
 * Only for endpoints whose job result is the same body the endpoint returns.
 */
export async function resultOfJob<T>(pending: JobPendingBody, options: { signal?: AbortSignal; intervalMs?: number } = {}): Promise<T> {
  const job = await waitForJob(pending.job_id, options)
  if (job.status !== 'completed') throw new JobEndedError(job)
  return job.result as T
}
