import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'

import { apiRequest } from '../../api/client'
import { parseCodes, parseList } from './comparisonModel'
import type { SavedComparison } from './comparisonTypes'

interface RecentWork { work_id: string; kind: string; title: string; subtitle?: string | null; href: string; occurred_at: string }

const day = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', timeZone: 'UTC' })

function formatDay(value: string) {
  const time = Date.parse(value)
  return Number.isFinite(time) ? day.format(time) : ''
}

/** With no companies chosen: pick up a saved or recent comparison. */
export function ComparisonStart({ onLoad }: { onLoad: (codes: string[], metrics: string[]) => void }) {
  const saved = useQuery({
    queryKey: ['comparison-templates'],
    queryFn: () => apiRequest<{ templates: SavedComparison[] }>('/api/research/comparison-templates'),
    retry: false,
  })
  const recent = useQuery({
    queryKey: ['recent-work'],
    queryFn: () => apiRequest<{ items: RecentWork[] }>('/api/research/recent-work?limit=50'),
    retry: false,
  })
  const templates = (saved.data?.templates ?? []).slice(0, 8)
  const comparisons = (recent.data?.items ?? []).filter(item => item.kind === 'comparison' && parseCodes(new URL(item.href, 'http://x').searchParams.get('companies')).length >= 2).slice(0, 8)
  if (!templates.length && !comparisons.length) {
    return <p className="cmp-start">Add two or more companies to see the table, rankings, a scatter plot, and trends.</p>
  }
  return <div className="cmp-start-grid">
    {templates.length > 0 && <section className="panel cmp-panel" aria-labelledby="cmp-start-saved">
      <header className="cmp-panel__header"><h2 id="cmp-start-saved">Saved comparisons <kbd aria-hidden="true">O</kbd></h2></header>
      <ul className="cmp-start-list">{templates.map(item => {
        const codes = parseList(item.companies_json)
        return <li key={item.template_id}><button type="button" onClick={() => onLoad(codes, parseList(item.metrics_json))}><strong>{item.name}</strong><small>{codes.length} companies</small></button></li>
      })}</ul>
    </section>}
    {comparisons.length > 0 && <section className="panel cmp-panel" aria-labelledby="cmp-start-recent">
      <header className="cmp-panel__header"><h2 id="cmp-start-recent">Recent comparisons</h2></header>
      <ul className="cmp-start-list">{comparisons.map(item => <li key={item.work_id}><Link to={item.href}><strong>{item.title.replace(/^Comparison · /, '')}</strong><small>{[item.subtitle?.split(' · ')[0], formatDay(item.occurred_at)].filter(Boolean).join(' · ')}</small></Link></li>)}</ul>
    </section>}
  </div>
}
