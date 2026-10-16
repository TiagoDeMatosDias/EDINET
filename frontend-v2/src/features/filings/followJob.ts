import { isJobPending, JobEndedError, waitForJob, type JobPendingBody } from '../../api/jobs'

/**
 * Run a request whose work may be handed to a job worker. A 202 `job_pending`
 * answer is followed to the end of its job, then the request is made again:
 * the worker has filled the translation cache, so the second answer is a plain
 * 200 that returns at once. `onPending` receives the first pending answer so
 * the page can show its detail text.
 */
export async function requestFollowingJob<T>(
  request: () => Promise<T | JobPendingBody>,
  { onPending, afterJob, signal }: {
    onPending?: (pending: JobPendingBody) => void
    /** What to ask once the job is done, when it differs from `request` (a forced retranslation is read back without `force`). */
    afterJob?: () => Promise<T | JobPendingBody>
    signal?: AbortSignal
  } = {},
): Promise<T> {
  const first = await request()
  if (!isJobPending(first)) return first
  onPending?.(first)
  const job = await waitForJob(first.job_id, { signal })
  if (job.status !== 'completed') throw new JobEndedError(job)
  const second = await (afterJob ?? request)()
  if (isJobPending(second)) throw new Error(second.detail)
  return second
}
