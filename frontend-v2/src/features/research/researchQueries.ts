import { useQuery, type QueryClient } from '@tanstack/react-query'

import { apiRequest } from '../../api/client'
import type { PricingInputs, ResearchBook } from './researchTypes'

export const BOOK_KEY = ['research-book']

/** The research book: followed companies with live values, tags, and alerts with today's status. */
export function useResearchBook(enabled = true) {
  return useQuery({ queryKey: BOOK_KEY, queryFn: () => apiRequest<ResearchBook>('/api/research/book'), enabled, retry: false })
}

/** Everything that shows research state; refreshed together after any change. */
export function invalidateResearch(client: QueryClient, code?: string) {
  for (const key of [BOOK_KEY, ['research-notes'], ['research-tags'], ['tags']]) void client.invalidateQueries({ queryKey: key })
  if (code) {
    void client.invalidateQueries({ queryKey: ['company-tags', code] })
    void client.invalidateQueries({ queryKey: ['company-research', code] })
  }
}

/** A company's price, volatility, dividend yield, and credit lines for the calculators. */
export function usePricingInputs(code: string) {
  return useQuery({
    queryKey: ['research-pricing', code],
    enabled: Boolean(code),
    queryFn: () => apiRequest<PricingInputs>(`/api/research/pricing/${encodeURIComponent(code)}`),
    retry: false,
    staleTime: 5 * 60_000,
  })
}

export const PRICE_FORMAT = { label: 'Price', group: '', format: 'money', currency: 'price' } as const
