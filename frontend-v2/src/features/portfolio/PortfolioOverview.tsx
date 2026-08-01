import { ArrowDownRight, ArrowUpRight, CalendarDays, Landmark, WalletCards } from 'lucide-react'

import { Card, Metric } from '../../components/Page'
import { AllocationBreakdown, PortfolioValueChart } from './PortfolioCharts'
import { money, percent, titleCase } from './portfolioFormat'
import { ExploreButton, StatButton } from './PortfolioPrimitives'
import type { Performance, PieData, PortfolioDetail, PortfolioSummary, Transaction, ValueHistory } from './portfolioTypes'

type Props = {
  summary: PortfolioSummary
  performance?: Performance
  valueHistory?: ValueHistory
  allocation?: PieData
  currencies?: PieData
  activity: Record<string, number>
  transactions: Transaction[]
  currency: string
  onOpenDetail: (detail: PortfolioDetail) => void
}

function Snapshot({ performance, summary, currency, onOpenDetail }: Pick<Props, 'performance' | 'summary' | 'currency' | 'onOpenDetail'>) {
  const attribution = performance?.return_attribution
  return <div className="overview-snapshot-grid">
    <StatButton label="Total return" value={percent(performance?.total_return)} detail="Time-weighted" tone={Number(performance?.total_return) >= 0 ? 'positive' : 'negative'} onClick={() => onOpenDetail({ kind: 'performance' })} />
    <StatButton label="Real return" value={percent(attribution?.real_return)} detail="After inflation" tone={Number(attribution?.real_return) >= 0 ? 'positive' : 'negative'} onClick={() => onOpenDetail({ kind: 'performance' })} />
    <StatButton label="Portfolio P&L" value={money(summary.pnl, currency)} detail={`${money(summary.costBasis, currency)} cost basis`} tone={summary.pnl >= 0 ? 'positive' : 'negative'} onClick={() => onOpenDetail({ kind: 'performance' })} />
    <StatButton label="Net dividends" value={money(performance?.dividend_breakdown?.total_net, currency)} detail={percent(attribution?.dividend_yield) + ' return contribution'} onClick={() => onOpenDetail({ kind: 'income' })} />
  </div>
}

function PositionWatch({ summary, allocation, currency, onOpenDetail }: Pick<Props, 'summary' | 'allocation' | 'currency' | 'onOpenDetail'>) {
  const rows = (allocation?.labels ?? []).map((symbol, index) => ({ symbol, value: allocation?.values[index] ?? 0 }))
    .sort((left, right) => right.value - left.value).slice(0, 5)
  return <Card title="Position watch" description={`${summary.positionCount} invested positions`} actions={<ExploreButton label="All exposures" onClick={() => onOpenDetail({ kind: 'allocation' })} />}>
    <div className="position-watch-list">{rows.map(row => <div key={row.symbol}>
      <span><strong>{row.symbol}</strong><small>{money(row.value, currency)}</small></span>
      <div><i style={{ width: `${Math.max(2, summary.investedValue ? row.value / summary.investedValue * 100 : 0)}%` }} /></div>
      <b>{percent(summary.investedValue ? row.value / summary.investedValue : 0)}</b>
    </div>)}</div>
  </Card>
}

function RecentActivity({ activity, transactions, onOpenDetail }: Pick<Props, 'activity' | 'transactions' | 'onOpenDetail'>) {
  const latest = transactions[0]
  const total = Object.values(activity).reduce((sum, count) => sum + count, 0)
  const topTypes = Object.entries(activity).sort((left, right) => right[1] - left[1]).slice(0, 4)
  return <Card title="Activity pulse" description={`${total.toLocaleString()} imported records`} actions={<ExploreButton label="Activity ledger" onClick={() => onOpenDetail({ kind: 'activity' })} />}>
    <div className="activity-pulse">
      <div className="activity-latest"><CalendarDays /><span><small>Latest record</small><strong>{latest?.trade_date ?? '—'}</strong><em>{latest?.symbol ? `${latest.symbol} · ` : ''}{latest?.activity_type ? titleCase(latest.activity_type) : 'No activity'}</em></span></div>
      <div className="activity-type-list">{topTypes.map(([label, value]) => <div key={label}><span>{titleCase(label)}</span><strong>{value.toLocaleString()}</strong></div>)}</div>
    </div>
  </Card>
}

export function PortfolioOverview(props: Props) {
  const { summary, performance, valueHistory, allocation, currencies, currency, onOpenDetail } = props
  return <div className="portfolio-overview-content">
    <div className="portfolio-overview-layout">
      <Card title="Portfolio value" description={`${performance?.start_date ?? '—'} to ${performance?.end_date ?? '—'}`} actions={<ExploreButton label="Value details" onClick={() => onOpenDetail({ kind: 'value' })} />}>
        <div className="value-card-summary">
          <div><WalletCards /><span><small>Current value</small><strong>{money(summary.totalValue, currency)}</strong></span></div>
          <div className={summary.pnl >= 0 ? 'positive' : 'negative'}>{summary.pnl >= 0 ? <ArrowUpRight /> : <ArrowDownRight />}<span><small>Unrealized P&L</small><strong>{money(summary.pnl, currency)}</strong></span></div>
        </div>
        <PortfolioValueChart data={valueHistory} currency={currency} />
      </Card>
      <Card title="Current allocation" description="Invested assets, excluding cash" actions={<ExploreButton label="Exposure details" onClick={() => onOpenDetail({ kind: 'allocation' })} />}>
        <AllocationBreakdown data={allocation} currency={currency} label="positions" />
        <div className="allocation-foot"><Metric label="Cash reserve" value={money(summary.cashValue, currency)} detail={percent(summary.cashWeight)} /><Metric label="Top position" value={summary.topHolding?.symbol ?? '—'} detail={percent(summary.topHolding?.weight)} /><Metric label="Currencies" value={String(currencies?.labels.length ?? 0)} detail="Native exposure" /></div>
      </Card>
    </div>
    <div className="portfolio-overview-lower">
      <Card title="Performance snapshot" description="Open a metric for the complete calculation" actions={<ExploreButton label="Performance lab" onClick={() => onOpenDetail({ kind: 'performance' })} />}><Snapshot performance={performance} summary={summary} currency={currency} onOpenDetail={onOpenDetail} /></Card>
      <PositionWatch summary={summary} allocation={allocation} currency={currency} onOpenDetail={onOpenDetail} />
      <RecentActivity activity={props.activity} transactions={props.transactions} onOpenDetail={onOpenDetail} />
    </div>
    <div className="overview-footnote"><Landmark /><span>Returns are flow-adjusted. Current values use the selected display currency; native currency exposure remains visible in the detail panel.</span></div>
  </div>
}
