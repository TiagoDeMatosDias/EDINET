import { ChevronRight, Info } from 'lucide-react'
import type { ReactNode } from 'react'

import { Tip } from '../../components/Tooltip'

export function ExploreButton({ label = 'Explore', onClick }: { label?: string; onClick: () => void }) {
  return <button type="button" className="card-explore" onClick={onClick}>{label}<ChevronRight /></button>
}

/** A headline figure that opens where it is explained; the info mark shows how it is calculated. */
export function StatButton({ label, value, detail, onClick, tone = 'neutral', tip }: {
  label: string
  value: ReactNode
  detail?: ReactNode
  onClick: () => void
  tone?: 'positive' | 'negative' | 'neutral'
  tip?: ReactNode
}) {
  return <div className={`portfolio-stat portfolio-stat--${tone}`}>
    <button type="button" className="portfolio-stat__open" onClick={onClick} aria-label={`${label}: ${typeof value === 'string' ? value : ''}`.trim()} />
    <span className="portfolio-stat__label">{label}{tip && <Tip content={tip} className="portfolio-stat__tip"><Info aria-label={`How ${label.toLowerCase()} is calculated`} /></Tip>}</span>
    <strong>{value}</strong>
    {detail && <small>{detail}</small>}
    <ChevronRight className="portfolio-stat__chevron" aria-hidden="true" />
  </div>
}

/** A label, its value, and an optional explanation (shown on hover and focus). */
export function DetailList({ rows }: { rows: Array<{ label: string; value: ReactNode; detail?: ReactNode; tip?: ReactNode }> }) {
  return <dl className="portfolio-detail-list">{rows.map(row => <div key={row.label}>
    <dt>{row.tip ? <Tip content={row.tip}>{row.label}</Tip> : row.label}</dt>
    <dd>{row.value}{row.detail && <small>{row.detail}</small>}</dd>
  </div>)}</dl>
}

export function SectionCard({ title, description, actions, children, className = '' }: { title: string; description?: ReactNode; actions?: ReactNode; children: ReactNode; className?: string }) {
  return <section className={`card pf-card ${className}`}>
    <header className="card-header"><div><h2>{title}</h2>{description && <p>{description}</p>}</div>{actions && <div className="card-actions">{actions}</div>}</header>
    <div className="card-body">{children}</div>
  </section>
}
