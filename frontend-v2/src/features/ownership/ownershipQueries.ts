import { keepPreviousData, useQuery } from '@tanstack/react-query'

import { apiPost, apiRequest, queryString } from '../../api/client'
import type { JobPendingBody } from '../../api/jobs'
import { requestFollowingJob } from '../filings/followJob'
import type { CompanyOwnership, EntityDetail, EntityKind, EntityList, MarketCaps, OwnershipStatus } from './ownershipTypes'

const STALE = 5 * 60_000

export function useOwnershipStatus() {
  return useQuery({ queryKey: ['ownership', 'status'], queryFn: () => apiRequest<OwnershipStatus>('/api/ownership/status'), retry: false, staleTime: STALE })
}

/** A company's shareholders and officers; a 503 means the ownership step has never run. */
export function useCompanyOwnership(code: string) {
  return useQuery({
    queryKey: ['ownership', 'company', code],
    enabled: Boolean(code),
    queryFn: () => apiRequest<CompanyOwnership>(`/api/ownership/company/${encodeURIComponent(code)}`),
    retry: false,
    staleTime: STALE,
  })
}

/** People and organisations by name; the previous page stays on screen while the next loads. */
export function useEntities(query: string, kind: EntityKind | '', limit: number, enabled = true) {
  return useQuery({
    queryKey: ['ownership', 'entities', query, kind, limit],
    enabled,
    queryFn: () => apiRequest<EntityList>(`/api/ownership/entities${queryString({ q: query || undefined, kind: kind || undefined, limit })}`),
    retry: false,
    staleTime: STALE,
    placeholderData: keepPreviousData,
  })
}

export function useEntity(entityId: string) {
  return useQuery({
    queryKey: ['ownership', 'entity', entityId],
    enabled: Boolean(entityId),
    queryFn: () => apiRequest<EntityDetail>(`/api/ownership/entities/${encodeURIComponent(entityId)}`),
    retry: false,
    staleTime: STALE,
  })
}

/**
 * Market capitalisation of the companies an entity holds or serves, to value
 * its stakes. Requested after the stakes are on screen: the figure is Company
 * Analysis's own and takes a moment per company the first time.
 */
export function useMarketCaps(codes: string[]) {
  return useQuery({
    queryKey: ['ownership', 'market-caps', codes.join(',')],
    enabled: codes.length > 0,
    queryFn: async () => (await apiRequest<{ market_caps: MarketCaps }>(`/api/ownership/market-caps${queryString({ codes: codes.join(',') })}`)).market_caps,
    retry: false,
    staleTime: STALE,
  })
}

/**
 * The lines of a career summary in English, by the filed line. Standard lines
 * are rewritten by rule; the rest go through the server's translation engine,
 * which keeps its results, so only the first request for a person can take a
 * few seconds.
 */
export function useCareerTranslation(career: { doc_id: string; edinet_code: string; period_end: string; entries: Array<{ text: string }> }) {
  const lines = career.entries.map(entry => entry.text).filter(Boolean)
  return useQuery({
    queryKey: ['ownership', 'career', career.doc_id, lines.length, lines[0] ?? ''],
    enabled: lines.length > 0,
    queryFn: async () => {
      const result = await requestFollowingJob(() => apiPost<{ translations: Array<{ source: string; english: string }> } | JobPendingBody>('/api/ownership/careers/translate', { lines, company: career.edinet_code, period_end: career.period_end }))
      return Object.fromEntries(result.translations.map(item => [item.source, item.english]))
    },
    retry: false,
    staleTime: Infinity,
  })
}
