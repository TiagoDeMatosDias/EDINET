import { useQuery } from '@tanstack/react-query'

import { apiRequest } from '../../api/client'
import { formatMetricValue } from '../../metrics'
import { formatSignedPercent, relativeDay, reviewState, statusLabel, todayIso, upside } from './researchModel'
import type { CompanyResearch } from './researchTypes'
import './research.css'

/** A one-line summary of the user's view on a company — status, target, review — for a page header. */
export function ResearchBadge({ code, price, priceCurrency, onClick }: { code: string; price?: number | null; priceCurrency?: string | null; onClick?: () => void }) {
  const research = useQuery({ queryKey: ['company-research', code], enabled: Boolean(code), queryFn: () => apiRequest<CompanyResearch>(`/api/research/companies/${encodeURIComponent(code)}`), retry: false })
  const record = research.data
  if (!record || (!record.thesis_status && record.target_value == null && !record.review_on)) return null
  const gap = upside({ target_value: record.target_value, target_currency: record.target_currency, LatestPrice: price, price_currency: priceCurrency })
  const review = reviewState(record.review_on, todayIso())
  const parts = [
    record.target_value != null && `Target ${formatMetricValue({ label: '', group: '', format: 'money', currency: 'price' }, record.target_value, { price: record.target_currency })}${gap != null ? ` (${formatSignedPercent(gap)})` : ''}`,
    review && review !== 'later' && `Review ${relativeDay(record.review_on, todayIso())}`,
  ].filter(Boolean)
  return <button type="button" className="rs-research-badge" onClick={onClick} title="Your research on this company">
    {record.thesis_status && <span className={`rs-pill rs-pill--${record.thesis_status}`}>{statusLabel(record.thesis_status)}</span>}
    {parts.length > 0 && <span className={review === 'overdue' ? 'is-overdue' : undefined}>{parts.join(' · ')}</span>}
  </button>
}
