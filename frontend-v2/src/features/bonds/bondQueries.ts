import { useQuery } from '@tanstack/react-query'

import { apiRequest } from '../../api/client'
import type { BondDetail, BondMarket, BondStatus, CompanyBonds } from './bondTypes'

const STALE = 5 * 60_000

export function useBondStatus() {
  return useQuery({ queryKey: ['bonds', 'status'], queryFn: () => apiRequest<BondStatus>('/api/bonds/status'), retry: false, staleTime: STALE })
}

/** A company's bonds; a 503 means the bond step has never run. */
export function useCompanyBonds(code: string) {
  return useQuery({
    queryKey: ['bonds', 'company', code],
    enabled: Boolean(code),
    queryFn: () => apiRequest<CompanyBonds>(`/api/bonds/company/${encodeURIComponent(code)}`),
    retry: false,
    staleTime: STALE,
  })
}

export function useBondMarket(includeGroup: boolean, enabled = true) {
  return useQuery({
    queryKey: ['bonds', 'market', includeGroup],
    enabled,
    queryFn: () => apiRequest<BondMarket>(`/api/bonds/market${includeGroup ? '?include_group=true' : ''}`),
    retry: false,
    staleTime: STALE,
  })
}

export function useBondDetail(bondId: string) {
  return useQuery({
    queryKey: ['bonds', 'bond', bondId],
    enabled: Boolean(bondId),
    queryFn: () => apiRequest<BondDetail>(`/api/bonds/bond/${encodeURIComponent(bondId)}`),
    retry: false,
    staleTime: STALE,
  })
}
