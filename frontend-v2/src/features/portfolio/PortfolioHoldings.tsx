import type { ColumnDef } from '@tanstack/react-table'
import { Building2, Eye, Search, WalletCards } from 'lucide-react'
import { useMemo, useState } from 'react'

import { DataTable } from '../../components/DataTable'
import { ErrorState, LoadingState } from '../../components/Feedback'
import { Card, Field, Metric } from '../../components/Page'
import { money, percent, quantity } from './portfolioFormat'
import { ExploreButton } from './PortfolioPrimitives'
import type { Holding, PortfolioDetail, PortfolioSummary } from './portfolioTypes'

type HoldingRow = Holding & { portfolioWeight: number }

type Props = {
  data: Holding[]
  summary: PortfolioSummary
  currency: string
  includeClosed: boolean
  isLoading: boolean
  error?: unknown
  onIncludeClosed: (value: boolean) => void
  onOpenDetail: (detail: PortfolioDetail) => void
}

function holdingName(holding: Holding) {
  return holding.performance?.name || holding.asset_category || ''
}

function isCash(holding: Holding) {
  return holding.asset_category === 'CASH' || holding.symbol.startsWith('CASH')
}

function HoldingCell({ holding, onOpenDetail }: { holding: Holding; onOpenDetail: Props['onOpenDetail'] }) {
  const Icon = isCash(holding) ? WalletCards : Building2
  return <button className="company-link" onClick={() => onOpenDetail({ kind: 'holding', holding })}>
    <Icon /><span><strong>{holding.symbol}</strong><small>{holdingName(holding)}</small></span>
  </button>
}

function useColumns(currency: string, onOpenDetail: Props['onOpenDetail']) {
  return useMemo<ColumnDef<HoldingRow>[]>(() => [
    { accessorKey: 'symbol', header: 'Holding', cell: ({ row }) => <HoldingCell holding={row.original} onOpenDetail={onOpenDetail} /> },
    { accessorKey: 'portfolioWeight', header: 'Weight', cell: info => percent(info.getValue()) },
    { accessorKey: 'quantity', header: 'Quantity', cell: info => quantity(info.getValue()) },
    { accessorKey: 'market_price', header: 'Price', cell: info => money(info.getValue(), String(info.row.original.currency ?? currency), 2) },
    { accessorKey: 'market_value', header: `Value (${currency})`, cell: info => money(info.getValue(), currency) },
    { id: 'pnl', header: 'P&L', accessorFn: row => row.performance?.pnl_display, cell: info => <span className={Number(info.getValue()) >= 0 ? 'number-positive' : 'number-negative'}>{money(info.getValue(), currency)}</span> },
    { id: 'return', header: 'Return', accessorFn: row => row.performance?.total_return_display ?? row.performance?.total_return_native, cell: info => percent(info.getValue()) },
    { id: 'annualized', header: 'Annualized', accessorFn: row => row.performance?.annualized_return, cell: info => percent(info.getValue()) },
    { id: 'income', header: 'Income', accessorFn: row => row.performance?.dividends_display, cell: info => money(info.getValue(), currency) },
    { id: 'details', header: 'Details', enableSorting: false, cell: ({ row }) => <button className="portfolio-row-action" aria-label={`View ${row.original.symbol} details`} onClick={() => onOpenDetail({ kind: 'holding', holding: row.original })}><Eye /></button> },
  ], [currency, onOpenDetail])
}

function HoldingSummary({ summary, currency }: Pick<Props, 'summary' | 'currency'>) {
  return <div className="portfolio-section-metrics">
    <Metric label="Invested value" value={money(summary.investedValue, currency)} />
    <Metric label="Cost basis" value={money(summary.costBasis, currency)} />
    <Metric label="Open P&L" value={money(summary.pnl, currency)} />
    <Metric label="Positions" value={summary.positionCount.toLocaleString()} />
    <Metric label="Top weight" value={percent(summary.topHolding?.weight)} detail={summary.topHolding?.symbol} />
    <Metric label="Cash weight" value={percent(summary.cashWeight)} />
  </div>
}

export function PortfolioHoldings(props: Props) {
  const [search, setSearch] = useState('')
  const [category, setCategory] = useState('all')
  const rows = props.data.map(holding => ({
    ...holding,
    portfolioWeight: props.summary.totalValue ? Number(holding.market_value ?? 0) / props.summary.totalValue : 0,
  }))
  const categories = [...new Set(rows.map(row => row.asset_category).filter(Boolean) as string[])].sort()
  const filtered = rows.filter(row => {
    const term = search.trim().toLowerCase()
    const matchesSearch = !term || `${row.symbol} ${holdingName(row)}`.toLowerCase().includes(term)
    return matchesSearch && (category === 'all' || row.asset_category === category)
  })
  const columns = useColumns(props.currency, props.onOpenDetail)
  return <div className="portfolio-section-stack">
    <HoldingSummary summary={props.summary} currency={props.currency} />
    <Card title="Position ledger" description={`${filtered.length} of ${rows.length} positions shown`} actions={<ExploreButton label="Exposure details" onClick={() => props.onOpenDetail({ kind: 'allocation' })} />}>
      <div className="portfolio-table-toolbar">
        <Field label="Find a position"><div className="input-with-icon"><Search /><input className="input" value={search} placeholder="Symbol or company" onChange={event => setSearch(event.target.value)} /></div></Field>
        <Field label="Asset class"><select className="select" value={category} onChange={event => setCategory(event.target.value)}><option value="all">All asset classes</option>{categories.map(value => <option key={value} value={value}>{value}</option>)}</select></Field>
        <label className="portfolio-check"><input type="checkbox" checked={props.includeClosed} onChange={event => props.onIncludeClosed(event.target.checked)} /><span>Include closed positions</span></label>
      </div>
      {props.isLoading ? <LoadingState label="Loading positions" /> : props.error ? <ErrorState error={props.error} /> : <div className="portfolio-table-frame"><DataTable data={filtered} columns={columns} emptyText="No positions match these filters." dense /></div>}
    </Card>
  </div>
}
