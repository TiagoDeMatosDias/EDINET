import { useQuery } from '@tanstack/react-query'

import { apiRequest } from '../api/client'
import type { Health, SystemStatus } from '../api/types'

export function useHealth() {
  return useQuery({
    queryKey: ['health'],
    queryFn: () => apiRequest<Health>('/health'),
    refetchInterval: 60_000,
  })
}

/** Pipeline queue state; only operators and administrators may read it. */
export function useSystemStatus(enabled: boolean) {
  return useQuery({
    queryKey: ['system-status'],
    queryFn: () => apiRequest<SystemStatus>('/api/system/status'),
    enabled,
    refetchInterval: 15_000,
    retry: false,
  })
}
