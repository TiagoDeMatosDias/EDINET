import { Eye, Search } from 'lucide-react'
import { useMemo, useState } from 'react'

import { ErrorState, LoadingState } from '../../components/Feedback'
import { Field, Metric } from '../../components/Page'
import { formatDay, money, quantity, titleCase, transactionCashEffect } from './portfolioFormat'
import { SectionCard } from './PortfolioPrimitives'
import { PortfolioTable, type TableColumn } from './PortfolioTable'
import type { PortfolioDetail, Transaction } from './portfolioTypes'

type Props = {
  data: Transaction[]
  activity: Record<string, number>
  dateRange?: { min_date?: string | null; max_date?: string | null }
  isLoading: boolean
  error?: unknown
  hotkeys?: boolean
  onOpenDetail: (detail: PortfolioDetail) => void
}

function activityTone(value?: string) {
  if (value === 'DIVIDEND' || value === 'PIL_DIVIDEND') return 'positive'
  if (value === 'WITHHOLDING_TAX' || value === 'OTHER_FEE' || value === 'COMMISSION_ADJ') return 'negative'
  return 'neutral'
}

function useColumns(onOpenDetail: Props['onOpenDetail']) {
  return useMemo<TableColumn<Transaction>[]>(() => [
    { id: 'date', header: 'Date', sortValue: row => row.trade_date, cell: row => <span className="mono">{row.trade_date ?? '—'}</span> },
    { id: 'activity', header: 'Activity', sortValue: row => row.activity_type, cell: row => <span className={`activity-badge activity-badge--${activityTone(row.activity_type)}`}>{titleCase(row.activity_type ?? '')}{row.buy_sell ? ` · ${titleCase(row.buy_sell)}` : ''}</span> },
    { id: 'symbol', header: 'Symbol', rowHeader: true, sortValue: row => row.symbol, cell: row => <strong>{row.symbol || '—'}</strong> },
    { id: 'description', header: 'Description', cell: row => <span className="transaction-description" title={row.description ?? ''}>{row.description || '—'}</span> },
    { id: 'quantity', header: 'Quantity', numeric: true, sortValue: row => row.quantity, cell: row => Number(row.quantity) ? quantity(row.quantity) : '—' },
    { id: 'cash', header: 'Cash effect', numeric: true, tip: 'Cash in (+) or out (−) of the account, in the record’s currency.', sortValue: row => transactionCashEffect(row), cell: row => {
      const effect = transactionCashEffect(row)
      return <span className={Number(effect) < 0 ? 'number-negative' : undefined}>{money(effect, row.currency ?? 'EUR', 2)}</span>
    } },
    { id: 'details', header: '', cell: row => <button type="button" className="icon-button" aria-label={`View transaction from ${row.trade_date ?? 'unknown date'}`} title="Details (Enter)" onClick={() => onOpenDetail({ kind: 'transaction', transaction: row })}><Eye /></button> },
  ], [onOpenDetail])
}

function ActivitySummary({ activity, dateRange }: Pick<Props, 'activity' | 'dateRange'>) {
  const total = Object.values(activity).reduce((sum, value) => sum + value, 0)
  const income = Number(activity.DIVIDEND ?? 0) + Number(activity.PIL_DIVIDEND ?? 0)
  const fees = Number(activity.OTHER_FEE ?? 0) + Number(activity.COMMISSION_ADJ ?? 0)
  return <div className="portfolio-section-metrics">
    <Metric label="Imported records" value={total.toLocaleString()} detail={`${formatDay(dateRange?.min_date)} to ${formatDay(dateRange?.max_date)}`} />
    <Metric label="Trades" value={Number(activity.TRADE ?? 0).toLocaleString()} />
    <Metric label="Dividends" value={income.toLocaleString()} />
    <Metric label="Withholding tax" value={Number(activity.WITHHOLDING_TAX ?? 0).toLocaleString()} />
    <Metric label="Deposits & withdrawals" value={Number(activity.DEPOSIT_WITHDRAWAL ?? 0).toLocaleString()} />
    <Metric label="Fees & adjustments" value={fees.toLocaleString()} />
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
    <SectionCard title="Activity" description={`${filtered.length.toLocaleString()} of ${props.data.length.toLocaleString()} records, newest first`}>
      <div className="portfolio-table-toolbar">
        <Field label="Search activity"><div className="input-with-icon"><Search /><input className="input" data-portfolio-find value={search} placeholder="Symbol, description, or source" onChange={event => setSearch(event.target.value)} onKeyDown={event => { if (event.key === 'Escape') { setSearch(''); event.currentTarget.blur() } }} /><kbd className="input-kbd" aria-hidden="true">F</kbd></div></Field>
        <Field label="Activity type"><select className="select" value={activityType} onChange={event => setActivityType(event.target.value)}><option value="all">All activity</option>{activityTypes.map(value => <option key={value} value={value}>{titleCase(value)}</option>)}</select></Field>
      </div>
      {props.isLoading ? <LoadingState label="Loading activity" /> : props.error ? <ErrorState error={props.error} /> : <PortfolioTable
        label="Activity"
        rows={filtered}
        columns={columns}
        rowKey={row => String(row.id ?? `${row.trade_date}-${row.symbol}-${row.amount}`)}
        pageSize={50}
        hotkeys={props.hotkeys}
        onOpen={row => props.onOpenDetail({ kind: 'transaction', transaction: row })}
        emptyText="No activity matches these filters."
      />}
    </SectionCard>
  </div>
}
