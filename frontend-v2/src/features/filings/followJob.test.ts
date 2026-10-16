import { describe, expect, it, vi } from 'vitest'

import { requestFollowingJob } from './followJob'

const waitForJob = vi.hoisted(() => vi.fn())
vi.mock('../../api/jobs', async importOriginal => ({ ...(await importOriginal<typeof import('../../api/jobs')>()), waitForJob }))

const pending = { code: 'job_pending' as const, job_id: 'j1', waiting_for_worker: false, detail: 'Still running.' }

describe('requestFollowingJob', () => {
  it('returns an ordinary answer untouched', async () => {
    const request = vi.fn().mockResolvedValue({ ok: 1 })
    await expect(requestFollowingJob(request)).resolves.toEqual({ ok: 1 })
    expect(waitForJob).not.toHaveBeenCalled()
  })

  it('waits for the job and asks again, using afterJob when given', async () => {
    waitForJob.mockResolvedValue({ status: 'completed' })
    const request = vi.fn().mockResolvedValue(pending)
    const afterJob = vi.fn().mockResolvedValue({ ok: 2 })
    const onPending = vi.fn()
    await expect(requestFollowingJob(request, { onPending, afterJob })).resolves.toEqual({ ok: 2 })
    expect(onPending).toHaveBeenCalledWith(pending)
    expect(request).toHaveBeenCalledTimes(1)
  })

  it('fails when the job does not complete', async () => {
    waitForJob.mockResolvedValue({ status: 'failed', error_message: 'Engine missing' })
    await expect(requestFollowingJob(vi.fn().mockResolvedValue(pending))).rejects.toThrow('Engine missing')
  })
})
