import { ChevronRight } from 'lucide-react'
import type { ReactNode } from 'react'

export function ExploreButton({ label = 'Explore', onClick }: { label?: string; onClick: () => void }) {
  return <button className="card-explore" onClick={onClick}>{label}<ChevronRight /></button>
}

export function StatButton({ label, value, detail, onClick, tone = 'neutral' }: {
  label: string
  value: ReactNode
  detail?: ReactNode
  onClick: () => void
  tone?: 'positive' | 'negative' | 'neutral'
}) {
  return <button className={`portfolio-stat portfolio-stat--${tone}`} onClick={onClick}>
    <span>{label}</span>
    <strong>{value}</strong>
    {detail && <small>{detail}</small>}
    <ChevronRight aria-hidden="true" />
  </button>
}

export function DetailList({ rows }: { rows: Array<{ label: string; value: ReactNode; detail?: ReactNode }> }) {
  return <dl className="portfolio-detail-list">{rows.map(row => <div key={row.label}>
    <dt>{row.label}</dt>
    <dd>{row.value}{row.detail && <small>{row.detail}</small>}</dd>
  </div>)}</dl>
}
