import { useQuery } from '@tanstack/react-query'

import { apiRequest } from '../api/client'
import type { Health } from '../api/types'

export function useHealth() {
  return useQuery({
    queryKey: ['health'],
    queryFn: () => apiRequest<Health>('/health'),
    refetchInterval: 60_000,
  })
}
