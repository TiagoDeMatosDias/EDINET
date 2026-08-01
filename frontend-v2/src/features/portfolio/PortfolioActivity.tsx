import type { ColumnDef } from '@tanstack/react-table'
import { Eye, Search } from 'lucide-react'
import { useMemo, useState } from 'react'

import { DataTable } from '../../components/DataTable'
import { ErrorState, LoadingState } from '../../components/Feedback'
import { Card, Field, Metric } from '../../components/Page'
import { money, quantity, titleCase, transactionCashEffect } from './portfolioFormat'
import { ExploreButton } from './PortfolioPrimitives'
import type { PortfolioDetail, Transaction } from './portfolioTypes'

type Props = {
  data: Transaction[]
  activity: Record<string, number>
  dateRange?: { min_date?: string | null; max_date?: string | null }
  isLoading: boolean
  error?: unknown
  onOpenDetail: (detail: PortfolioDetail) => void
}

function activityTone(value?: string) {
  if (value === 'DIVIDEND' || value === 'PIL_DIVIDEND') return 'positive'
  if (value === 'WITHHOLDING_TAX' || value === 'OTHER_FEE' || value === 'COMMISSION_ADJ') return 'negative'
  return 'neutral'
}

function useColumns(onOpenDetail: Props['onOpenDetail']) {
  return useMemo<ColumnDef<Transaction>[]>(() => [
    { accessorKey: 'trade_date', header: 'Date' },
    { accessorKey: 'activity_type', header: 'Activity', cell: info => <span className={`activity-badge activity-badge--${activityTone(String(info.getValue()))}`}>{titleCase(String(info.getValue() ?? ''))}</span> },
    { accessorKey: 'symbol', header: 'Symbol', cell: info => <strong>{String(info.getValue() || '—')}</strong> },
    { accessorKey: 'description', header: 'Description', cell: info => <span className="transaction-description" title={String(info.getValue() ?? '')}>{String(info.getValue() || '—')}</span> },
    { accessorKey: 'quantity', header: 'Quantity', cell: info => Number(info.getValue()) ? quantity(info.getValue()) : '—' },
    { id: 'cash_effect', accessorFn: row => transactionCashEffect(row), header: 'Cash effect', cell: info => <span className={Number(info.getValue()) >= 0 ? 'number-positive' : 'number-negative'}>{money(info.getValue(), String(info.row.original.currency ?? 'EUR'), 2)}</span> },
    { accessorKey: 'source_file', header: 'Source' },
    { id: 'details', header: 'Details', enableSorting: false, cell: ({ row }) => <button className="portfolio-row-action" aria-label={`View transaction from ${row.original.trade_date ?? 'unknown date'}`} onClick={() => onOpenDetail({ kind: 'transaction', transaction: row.original })}><Eye /></button> },
  ], [onOpenDetail])
}

function ActivitySummary({ activity, dateRange }: Pick<Props, 'activity' | 'dateRange'>) {
  const total = Object.values(activity).reduce((sum, value) => sum + value, 0)
  const income = Number(activity.DIVIDEND ?? 0) + Number(activity.PIL_DIVIDEND ?? 0)
  const fees = Number(activity.OTHER_FEE ?? 0) + Number(activity.COMMISSION_ADJ ?? 0)
  return <div className="portfolio-section-metrics">
    <Metric label="Imported records" value={total.toLocaleString()} />
    <Metric label="Trades" value={Number(activity.TRADE ?? 0).toLocaleString()} />
    <Metric label="Income events" value={income.toLocaleString()} />
    <Metric label="Tax events" value={Number(activity.WITHHOLDING_TAX ?? 0).toLocaleString()} />
    <Metric label="Cash movements" value={Number(activity.DEPOSIT_WITHDRAWAL ?? 0).toLocaleString()} />
    <Metric label="Fees & adjustments" value={fees.toLocaleString()} detail={`${dateRange?.min_date ?? '—'} to ${dateRange?.max_date ?? '—'}`} />
  </div>
}

export function PortfolioActivity(props: Props) {
  const [search, setSearch] = useState('')
  const [activityType, setActivityType] = useState('all')
  const activityTypes = [...new Set(props.data.map(row => row.activity_type).filter(Boolean) as string[])].sort()
  const term = search.trim().toLowerCase()
  const filtered = props.data.filter(row => {
    const haystack = `${row.symbol ?? ''} ${row.description ?? ''} ${row.source_file ?? ''}`.toLowerCase()
    return (!term || haystack.includes(term)) && (activityType === 'all' || row.activity_type === activityType)
  })
  const columns = useColumns(props.onOpenDetail)
  return <div className="portfolio-section-stack">
    <ActivitySummary activity={props.activity} dateRange={props.dateRange} />
    <Card title="Activity ledger" description={`${filtered.length} of the latest ${props.data.length} records`} actions={<ExploreButton label="Activity summary" onClick={() => props.onOpenDetail({ kind: 'activity' })} />}>
      <div className="portfolio-table-toolbar">
        <Field label="Search activity"><div className="input-with-icon"><Search /><input className="input" value={search} placeholder="Symbol, description, or source" onChange={event => setSearch(event.target.value)} /></div></Field>
        <Field label="Activity type"><select className="select" value={activityType} onChange={event => setActivityType(event.target.value)}><option value="all">All activity</option>{activityTypes.map(value => <option key={value} value={value}>{titleCase(value)}</option>)}</select></Field>
      </div>
      {props.isLoading ? <LoadingState label="Loading activity" /> : props.error ? <ErrorState error={props.error} /> : <div className="portfolio-table-frame"><DataTable data={filtered} columns={columns} emptyText="No activity matches these filters." dense pageSize={50} /></div>}
    </Card>
  </div>
}
