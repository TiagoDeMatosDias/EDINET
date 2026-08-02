import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'
import { ArrowRight, BarChart3, Building2, DatabaseZap, ListFilter } from 'lucide-react'
import { Link } from 'react-router-dom'

import { apiRequest } from '../../api/client'
import type { Job, PipelineStep } from '../../api/types'
import { DataTable } from '../../components/DataTable'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { Card, Metric, PageHeader } from '../../components/Page'
import { useAuth } from '../auth/AuthProvider'
import { useHealth } from '../../hooks/useHealth'

const jobColumns: ColumnDef<Job>[] = [
  { accessorKey: 'status', header: 'Status', cell: info => <span className={`badge ${info.getValue() === 'completed' ? 'badge--success' : info.getValue() === 'failed' ? 'badge--danger' : ''}`}>{String(info.getValue())}</span> },
  { accessorKey: 'current_step', header: 'Current step', cell: info => String(info.getValue() ?? '—') },
  { accessorKey: 'progress_percent', header: 'Progress', cell: info => `${Math.round(Number(info.getValue() ?? 0))}%` },
  { accessorKey: 'created_at', header: 'Created', cell: info => info.getValue() ? new Date(String(info.getValue())).toLocaleString() : '—' },
]

interface RecentWorkItem {
  work_id: string
  kind: 'screen' | 'company' | 'comparison' | 'filing' | 'backtest' | string
  title: string
  subtitle?: string | null
  href: string
  occurred_at: string
}

const recentGroups: Array<{ kind: RecentWorkItem['kind']; title: string; empty: string }> = [
  { kind: 'screen', title: 'Recent screens', empty: 'Run a screen to keep its result here.' },
  { kind: 'company', title: 'Recently viewed companies', empty: 'Open a company analysis to keep it here.' },
  { kind: 'comparison', title: 'Recent comparisons', empty: 'Compare companies to keep the comparison here.' },
  { kind: 'filing', title: 'Recent filings', empty: 'Open a filing to keep it here.' },
  { kind: 'backtest', title: 'Recent backtests', empty: 'Run a backtest to keep it here.' },
]

function RecentWork() {
  const recent = useQuery({
    queryKey: ['recent-work'],
    queryFn: () => apiRequest<{ items: RecentWorkItem[] }>('/api/research/recent-work?limit=50'),
  })
  const items = recent.data?.items ?? []

  return <section className="stack recent-work-section">
    <div className="section-heading"><div><p className="eyebrow">Workspace history</p><h2>Recent work</h2></div><span className="muted">Saved to your account</span></div>
    {recent.isLoading && <LoadingState label="Loading recent work" />}
    {recent.isError && <ErrorState error={recent.error} retry={() => recent.refetch()} />}
    {!recent.isLoading && !recent.isError && <div className="recent-work-grid">
      {recentGroups.map(group => {
        const groupItems = items.filter(item => item.kind === group.kind).slice(0, 8)
        return <Card key={group.kind} title={group.title}>
          {groupItems.length ? <div className="recent-work-list">{groupItems.map(item => <Link className="recent-work-item" key={item.work_id} to={item.href}><span><strong>{item.title}</strong><small>{item.subtitle || 'Open to review'}</small></span><span><small>{new Date(item.occurred_at).toLocaleString()}</small><ArrowRight /></span></Link>)}</div> : <EmptyState title="Nothing here yet" description={group.empty} />}
        </Card>
      })}
    </div>}
  </section>
}

function StartSomething({ isAdmin }: { isAdmin: boolean }) {
  return <Card title="Start something" description="Begin with the outcome you need"><div className="button-row"><Link className="button button--primary" to="/screen"><ListFilter />Find matching companies</Link><Link className="button button--secondary" to="/analyze"><Building2 />Analyze a company</Link><Link className="button button--secondary" to="/backtest"><BarChart3 />Test an investment idea</Link>{isAdmin && <Link className="button button--secondary" to="/pipeline"><DatabaseZap />Refresh research data</Link>}</div></Card>
}

function AdminOverview() {
  const health = useHealth()
  const jobs = useQuery({ queryKey: ['jobs'], queryFn: () => apiRequest<Job[]>('/api/jobs?limit=8') })
  const steps = useQuery({ queryKey: ['pipeline-steps'], queryFn: () => apiRequest<{ steps: PipelineStep[] }>('/api/steps') })
  const portfolio = useQuery({ queryKey: ['portfolio-activity'], queryFn: () => apiRequest<{ by_activity: Record<string, number> }>('/api/portfolio/activity-summary'), retry: false })
  const activityTotal = Object.values(portfolio.data?.by_activity ?? {}).reduce((sum, value) => sum + value, 0)

  return <div className="stack admin-overview">
    <div className="metric-grid"><Metric label="Data service" value={health.isError ? 'Unavailable' : 'Ready'} detail={health.data ? `Checked ${new Date(health.data.timestamp).toLocaleTimeString()}` : 'Checking now'} /><Metric label="Active jobs" value={health.data?.jobs_active ?? '—'} detail="Pipeline executions" /><Metric label="Available steps" value={steps.data?.steps.length ?? '—'} detail="Dynamic data operations" /><Metric label="Portfolio activity" value={portfolio.isError ? 'Not loaded' : activityTotal.toLocaleString()} detail="Imported activity records" /></div>
    <Card title="Recent pipeline jobs" description="Execution history from the data service" actions={<Link className="button button--ghost" to="/pipeline">Open pipeline<ArrowRight /></Link>}>
      {jobs.isLoading ? <LoadingState label="Loading recent jobs" /> : jobs.isError ? <ErrorState error={jobs.error} retry={() => jobs.refetch()} /> : <DataTable data={jobs.data ?? []} columns={jobColumns} emptyText="No pipeline jobs yet." dense />}
    </Card>
    {steps.isError && <EmptyState title="Pipeline metadata unavailable" description="Research tools still work, but data refresh shortcuts cannot be shown." />}
  </div>
}

export default function OverviewPage() {
  const auth = useAuth()
  const isAdmin = auth.user?.role === 'admin'

  return <div className="stack">
    <PageHeader eyebrow="Research workspace" title="Overview" description={isAdmin ? 'Pick up recent work, check data freshness, or start a research workflow.' : 'Pick up recent work or start a research workflow.'} actions={<Link className="button button--primary" to="/screen"><ListFilter />Build a screen</Link>} />
    <StartSomething isAdmin={isAdmin} />
    {isAdmin && <AdminOverview />}
    <RecentWork />
  </div>
}
