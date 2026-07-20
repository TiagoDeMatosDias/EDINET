import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'
import { ArrowRight, BarChart3, Building2, DatabaseZap, ListFilter } from 'lucide-react'
import { Link } from 'react-router-dom'

import { apiRequest } from '../../api/client'
import type { Job, PipelineStep } from '../../api/types'
import { DataTable } from '../../components/DataTable'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { Card, Metric, PageHeader } from '../../components/Page'
import { useHealth } from '../../hooks/useHealth'

const jobColumns: ColumnDef<Job>[] = [
  { accessorKey: 'status', header: 'Status', cell: info => <span className={`badge ${info.getValue() === 'completed' ? 'badge--success' : info.getValue() === 'failed' ? 'badge--danger' : ''}`}>{String(info.getValue())}</span> },
  { accessorKey: 'current_step', header: 'Current step', cell: info => String(info.getValue() ?? '—') },
  { accessorKey: 'progress_percent', header: 'Progress', cell: info => `${Math.round(Number(info.getValue() ?? 0))}%` },
  { accessorKey: 'created_at', header: 'Created', cell: info => info.getValue() ? new Date(String(info.getValue())).toLocaleString() : '—' },
]

export default function OverviewPage() {
  const health = useHealth()
  const jobs = useQuery({ queryKey: ['jobs'], queryFn: () => apiRequest<Job[]>('/api/jobs?limit=8') })
  const steps = useQuery({ queryKey: ['pipeline-steps'], queryFn: () => apiRequest<{ steps: PipelineStep[] }>('/api/steps') })
  const portfolio = useQuery({ queryKey: ['portfolio-activity'], queryFn: () => apiRequest<{ by_activity: Record<string, number> }>('/api/portfolio/activity-summary'), retry: false })
  const activityTotal = Object.values(portfolio.data?.by_activity ?? {}).reduce((sum, value) => sum + value, 0)

  return <div className="stack">
    <PageHeader eyebrow="Research workspace" title="Overview" description="Pick up recent work, check data freshness, or start a research workflow." actions={<Link className="button button--primary" to="/screen"><ListFilter />Build a screen</Link>} />
    <div className="metric-grid"><Metric label="Data service" value={health.isError ? 'Unavailable' : 'Ready'} detail={health.data ? `Checked ${new Date(health.data.timestamp).toLocaleTimeString()}` : 'Checking now'} /><Metric label="Active jobs" value={health.data?.jobs_active ?? '—'} detail="Pipeline executions" /><Metric label="Available steps" value={steps.data?.steps.length ?? '—'} detail="Dynamic data operations" /><Metric label="Portfolio activity" value={portfolio.isError ? 'Not loaded' : activityTotal.toLocaleString()} detail="Imported activity records" /></div>
    <div className="two-column">
      <Card title="Recent pipeline jobs" description="Execution history from the data service" actions={<Link className="button button--ghost" to="/pipeline">Open pipeline<ArrowRight /></Link>}>
        {jobs.isLoading ? <LoadingState label="Loading recent jobs" /> : jobs.isError ? <ErrorState error={jobs.error} retry={() => jobs.refetch()} /> : <DataTable data={jobs.data ?? []} columns={jobColumns} emptyText="No pipeline jobs yet." dense />}
      </Card>
      <Card title="Start something" description="Begin with the outcome you need"><div className="stack"><Link className="button button--primary" to="/screen"><ListFilter />Find matching companies</Link><Link className="button button--secondary" to="/analyze"><Building2 />Analyze a company</Link><Link className="button button--secondary" to="/backtest"><BarChart3 />Test an investment idea</Link><Link className="button button--secondary" to="/pipeline"><DatabaseZap />Refresh research data</Link></div></Card>
    </div>
    {steps.isError && <EmptyState title="Pipeline metadata unavailable" description="Research tools still work, but data refresh shortcuts cannot be shown." />}
  </div>
}
