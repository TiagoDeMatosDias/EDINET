import { useQuery } from '@tanstack/react-query'

import { apiRequest } from '../api/client'
import { ACTIVE_JOB_STATUSES, type MyJobsResponse } from '../api/jobs'

export const MY_JOBS_KEY = ['my-jobs'] as const
export const ACTIVE_POLL_MS = 2_000
export const IDLE_POLL_MS = 15_000

/** The signed-in user's background jobs, polled quickly while any is active and slowly otherwise. */
export function useMyJobs(enabled: boolean) {
  return useQuery({
    queryKey: MY_JOBS_KEY,
    queryFn: () => apiRequest<MyJobsResponse>('/api/my-jobs?limit=30'),
    enabled,
    retry: false,
    refetchInterval: query => query.state.data?.jobs?.some(job => ACTIVE_JOB_STATUSES.has(job.status)) ? ACTIVE_POLL_MS : IDLE_POLL_MS,
  })
}
