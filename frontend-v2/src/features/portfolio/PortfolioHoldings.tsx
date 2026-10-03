import { AlertTriangle, ArrowUpRight, Building2, Eye, Search, WalletCards } from 'lucide-react'
import { useMemo, useState } from 'react'

import { ErrorState, LoadingState } from '../../components/Feedback'
import { Field, Metric } from '../../components/Page'
import { Tip } from '../../components/Tooltip'
import { displayValue, formatDay, heldDays, heldFor, holdingName, isCash, money, nativeMoney, percent, priceNote, quantity, signedPercent } from './portfolioFormat'
import { SectionCard } from './PortfolioPrimitives'
import { PortfolioTable, type TableColumn } from './PortfolioTable'
import type { Holding, PortfolioDetail, PortfolioSummary } from './portfolioTypes'

type HoldingRow = Holding & { weight: number }

type Props = {
  data: Holding[]
  summary: PortfolioSummary
  currency: string
  includeClosed: boolean
  isLoading: boolean
  error?: unknown
  hotkeys?: boolean
  returnTo?: string
  onIncludeClosed: (value: boolean) => void
  onOpenDetail: (detail: PortfolioDetail) => void
  onAnalyze: (holding: Holding) => void
  onOrderChange?: (rows: Holding[]) => void
}

function HoldingCell({ holding }: { holding: Holding }) {
  const Icon = isCash(holding) ? WalletCards : Building2
  const note = priceNote(holding)
  return <span className="pf-holding">
    <Icon aria-hidden="true" />
    <span><strong>{holding.symbol}{holding.is_open === false && <em> closed</em>}</strong><small>{holdingName(holding)}</small></span>
    {note && note.level !== 'info' && <Tip content={note.text} className={`pf-holding__flag is-${note.level}`}><AlertTriangle aria-label={note.text} /></Tip>}
  </span>
}

function useColumns(currency: string, onOpenDetail: Props['onOpenDetail'], onAnalyze: Props['onAnalyze']) {
  return useMemo<TableColumn<HoldingRow>[]>(() => [
    { id: 'symbol', header: 'Holding', rowHeader: true, sortValue: row => row.symbol, cell: row => <HoldingCell holding={row} />, className: 'pf-col-holding' },
    { id: 'shares', header: 'Shares', numeric: true, tip: 'Shares (or units) held now, after any share splits. For cash, the balance in its own currency.', sortValue: row => isCash(row) ? null : row.quantity, cell: row => {
      if (isCash(row)) {
        const balance = row.market_value_native ?? row.quantity
        return <Tip content={`Cash in ${row.currency}: worth ${money(displayValue(row), currency, 2)}`} focusable={false}><span>{nativeMoney(balance, row.currency ?? currency)}</span></Tip>
      }
      if (row.is_open === false) return '—'
      return <Tip content={row.avg_cost ? `Average cost ${money(row.avg_cost, row.currency ?? currency, 2)} a share` : 'Shares held'} focusable={false}><span>{quantity(row.quantity)}</span></Tip>
    } },
    { id: 'held', header: 'Held', numeric: true, tip: 'How long the holding has been in the portfolio without a break. After a full sale and a later purchase, only the latest period counts.', sortValue: row => isCash(row) ? null : heldDays(row.performance), cell: row => {
      if (isCash(row) || !row.performance?.held_since) return '—'
      const periods = row.performance.num_holding_periods ?? 1
      const text = row.is_open === false ? `Held ${formatDay(row.performance.held_since)} to ${formatDay(row.performance.held_until)}` : `Since ${formatDay(row.performance.held_since)}`
      return <Tip content={periods > 1 ? `${text}; held ${periods} separate times` : text} focusable={false}><span>{heldFor(row.performance)}</span></Tip>
    } },
    { id: 'weight', header: 'Weight', numeric: true, tip: 'Share of the portfolio’s value, cash included.', sortValue: row => row.weight, cell: row => <span className="pf-weight"><i style={{ width: Math.max(1, Math.min(56, row.weight * 100)) }} aria-hidden="true" />{percent(row.weight)}</span> },
    { id: 'value', header: `Value (${currency})`, numeric: true, sortValue: row => displayValue(row), cell: row => money(displayValue(row), currency) },
    { id: 'price', header: 'Price', numeric: true, tip: 'Latest close in the holding’s own currency.', sortValue: row => row.market_price, cell: row => {
      if (isCash(row) || row.market_price == null) return '—'
      const note = priceNote(row)
      return <Tip content={note?.text ?? (row.price_date ? `Close on ${formatDay(row.price_date)}${row.price_ticker && row.price_ticker !== row.symbol ? ` (${row.price_ticker})` : ''}` : 'Price date unknown — rebuild to record it')} focusable={false}><span className={note && note.level !== 'info' ? `pf-price is-${note.level}` : 'pf-price'}>{money(row.market_price, row.currency ?? currency, 2)}</span></Tip>
    } },
    { id: 'pnl', header: 'Unrealized', numeric: true, tip: 'Value less the cost of the shares still held.', sortValue: row => row.performance?.pnl_display, cell: row => row.performance ? <span className={Number(row.performance.pnl_display) < 0 ? 'number-negative' : undefined}>{money(row.performance.pnl_display, currency)}</span> : '—' },
    { id: 'return', header: 'Return', numeric: true, tip: 'Unrealized gain on the shares still held, in the display currency, as a share of their cost.', sortValue: row => row.performance?.total_return_display, cell: row => signedPercent(row.performance?.total_return_display) },
    { id: 'annual', header: 'Per year', numeric: true, tip: 'That return compounded per year since the current holding period began.', sortValue: row => row.performance?.annualized_return, cell: row => signedPercent(row.performance?.annualized_return) },
    { id: 'income', header: 'Dividends', numeric: true, tip: 'Net dividends received, converted on each payment date.', sortValue: row => row.performance?.dividends_display, cell: row => row.performance?.dividends_display ? money(row.performance.dividends_display, currency) : '—' },
    { id: 'total', header: 'Total P&L', numeric: true, tip: 'Unrealized plus realized gains plus net dividends, over every holding period.', sortValue: row => row.performance?.total_pnl_display, cell: row => row.performance?.total_pnl_display != null ? <span className={row.performance.total_pnl_display < 0 ? 'number-negative' : undefined}>{money(row.performance.total_pnl_display, currency)}</span> : '—' },
    { id: 'actions', header: '', cell: row => <span className="pf-row-actions">
      <button type="button" className="icon-button" aria-label={`View ${row.symbol} details`} title="Details (Enter)" onClick={() => onOpenDetail({ kind: 'holding', holding: row })}><Eye /></button>
      {!isCash(row) && <button type="button" className="icon-button" aria-label={`Open ${row.symbol} in Analysis`} title="Open in Analysis (A)" onClick={() => onAnalyze(row)}><ArrowUpRight /></button>}
    </span> },
  ], [currency, onAnalyze, onOpenDetail])
}

export function PortfolioHoldings(props: Props) {
  const [search, setSearch] = useState('')
  const [category, setCategory] = useState('all')
  const total = props.summary.totalValue
  const rows = useMemo<HoldingRow[]>(() => props.data.map(holding => ({ ...holding, weight: total ? displayValue(holding) / total : 0 })), [props.data, total])
  const categories = [...new Set(rows.map(row => row.asset_category).filter(Boolean) as string[])].sort()
  const term = search.trim().toLowerCase()
  const filtered = useMemo(() => rows.filter(row => (!term || `${row.symbol} ${holdingName(row)}`.toLowerCase().includes(term)) && (category === 'all' || row.asset_category === category)), [category, rows, term])
  const columns = useColumns(props.currency, props.onOpenDetail, props.onAnalyze)
  const totalPnl = rows.filter(row => row.is_open !== false).reduce((sum, row) => sum + Number(row.performance?.total_pnl_display ?? 0), 0)
  const income = rows.reduce((sum, row) => sum + Number(row.performance?.dividends_display ?? 0), 0)
  return <div className="portfolio-section-stack">
    <div className="portfolio-section-metrics">
      <Metric label="Invested" value={money(props.summary.investedValue, props.currency)} detail={`${props.summary.positionCount} holdings`} />
      <Metric label="Cost of shares held" value={money(props.summary.costBasis, props.currency)} />
      <Metric label="Unrealized P&L" value={money(props.summary.pnl, props.currency)} detail={props.summary.costBasis ? signedPercent(props.summary.pnl / props.summary.costBasis) : undefined} />
      <Metric label="Dividends received" value={money(income, props.currency)} detail="Holdings shown" />
      <Metric label="Total P&L, open holdings" value={money(totalPnl, props.currency)} detail="Unrealized, realized, dividends" />
      <Metric label="Cash" value={money(props.summary.cashValue, props.currency)} detail={percent(props.summary.cashWeight)} />
    </div>
    <SectionCard title="Holdings" description={`${filtered.length} of ${rows.length} shown · values in ${props.currency}, prices in each holding’s currency`}>
      <div className="portfolio-table-toolbar">
        <Field label="Find a holding"><div className="input-with-icon"><Search /><input className="input" data-portfolio-find value={search} placeholder="Symbol or name" onChange={event => setSearch(event.target.value)} onKeyDown={event => { if (event.key === 'Escape') { setSearch(''); event.currentTarget.blur() } }} /><kbd className="input-kbd" aria-hidden="true">F</kbd></div></Field>
        <Field label="Asset class"><select className="select" value={category} onChange={event => setCategory(event.target.value)}><option value="all">All asset classes</option>{categories.map(value => <option key={value} value={value}>{value}</option>)}</select></Field>
        <label className="portfolio-check"><input type="checkbox" checked={props.includeClosed} onChange={event => props.onIncludeClosed(event.target.checked)} /><span>Include closed holdings</span></label>
      </div>
      {props.isLoading ? <LoadingState label="Loading holdings" /> : props.error ? <ErrorState error={props.error} /> : <PortfolioTable
        label="Holdings"
        rows={filtered}
        columns={columns}
        rowKey={row => row.symbol}
        initialSort={{ column: 'value', direction: 'desc' }}
        initialCursor={props.returnTo}
        hotkeys={props.hotkeys}
        onOpen={row => props.onOpenDetail({ kind: 'holding', holding: row })}
        onSecondary={row => props.onAnalyze(row)}
        secondaryLabel="analysis"
        onOrderChange={props.onOrderChange}
        rowClassName={row => row.is_open === false ? 'is-closed' : undefined}
        emptyText="No holdings match these filters."
      />}
      <p className="pf-footnote">Click a row or press Enter for details. A opens the company in Analysis, where Shift+J and Shift+K step through your holdings in this order.</p>
    </SectionCard>
  </div>
}
