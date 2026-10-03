import { useState } from 'react'

import { LoadingState } from '../../components/Feedback'
import { CashEffect } from './ActivityCells'
import { BENCHMARK_COLOR, PORTFOLIO_COLOR, REFERENCE_COLOR } from './chartTheme'
import { AnnualReturnsChart, DrawdownChart, GrowthChart, SeriesLegend, ValueChart, WeightBars } from './PortfolioCharts'
import { displayValue, formatDay, holdingName, money, percent, signedPercent, titleCase } from './portfolioFormat'
import { ExploreButton, SectionCard } from './PortfolioPrimitives'
import type { Holding, Performance, PieData, PortfolioDetail, PortfolioSummary, PortfolioTab, Transaction } from './portfolioTypes'

type Props = {
  performance?: Performance
  isLoading: boolean
  summary: PortfolioSummary
  holdings: Holding[]
  allocation?: PieData
  currencies?: PieData
  transactions: Transaction[]
  currency: string
  benchmarkLabel?: string
  onOpenDetail: (detail: PortfolioDetail) => void
  onTab: (tab: PortfolioTab) => void
}

function Allocation({ holdings, currencies, summary, currency, onOpenDetail }: Pick<Props, 'holdings' | 'currencies' | 'summary' | 'currency' | 'onOpenDetail'>) {
  const [view, setView] = useState<'holdings' | 'currencies'>('holdings')
  const holdingRows = holdings.map(holding => ({ label: holding.symbol, value: displayValue(holding), detail: holdingName(holding) }))
  const currencyRows = (currencies?.labels ?? []).map((label, index) => ({ label, value: currencies?.values[index] ?? 0, detail: label === currency ? 'Display currency' : 'Currency exposure' }))
  return <SectionCard
    title="Allocation"
    description={view === 'holdings' ? `${summary.positionCount} holdings, cash excluded · largest ${summary.topHolding?.symbol ?? '—'} ${percent(summary.topHolding?.weight)}` : 'Holdings by the currency they are priced in'}
    actions={<div className="period-tabs" role="group" aria-label="Allocation view">
      <button type="button" className={`period-tab${view === 'holdings' ? ' active' : ''}`} aria-pressed={view === 'holdings'} onClick={() => setView('holdings')}>Holdings</button>
      <button type="button" className={`period-tab${view === 'currencies' ? ' active' : ''}`} aria-pressed={view === 'currencies'} onClick={() => setView('currencies')}>Currencies</button>
    </div>}
  >
    {view === 'holdings'
      ? <WeightBars rows={holdingRows} currency={currency} limit={10} onSelect={symbol => { const holding = holdings.find(item => item.symbol === symbol); if (holding) onOpenDetail({ kind: 'holding', holding }) }} />
      : <WeightBars rows={currencyRows} currency={currency} />}
    <p className="pf-footnote">Cash: {money(summary.cashValue, currency)} ({percent(summary.cashWeight)} of the portfolio).</p>
  </SectionCard>
}

function LatestActivity({ transactions, onOpenDetail, onTab }: Pick<Props, 'transactions' | 'onOpenDetail' | 'onTab'>) {
  return <SectionCard title="Latest activity" actions={<ExploreButton label="All activity (5)" onClick={() => onTab('activity')} />}>
    <ul className="pf-activity">{transactions.slice(0, 7).map((row, index) => {
      return <li key={`${row.id ?? index}-${row.trade_date}`}>
        <button type="button" onClick={() => onOpenDetail({ kind: 'transaction', transaction: row })}>
          <span><strong>{row.symbol || titleCase(row.activity_type ?? '')}</strong><small>{formatDay(row.trade_date)} · {titleCase(row.activity_type ?? '')}</small></span>
          <b><CashEffect row={row} /></b>
        </button>
      </li>
    })}</ul>
  </SectionCard>
}

export function PortfolioOverview(props: Props) {
  const { performance, currency, benchmarkLabel } = props
  const series = performance?.series ?? []
  const last = series.at(-1)
  const bench = performance?.benchmark?.available ? performance.benchmark : undefined
  return <div className="pf-overview">
    <div className="pf-overview__main">
      <SectionCard
        title="Growth"
        description={performance?.period ? `Time-weighted return from ${formatDay(performance.period.start)} to ${formatDay(performance.period.end)}` : undefined}
        actions={<ExploreButton label="Performance (3)" onClick={() => props.onTab('performance')} />}
      >
        {props.isLoading ? <LoadingState label="Calculating returns" /> : <>
          <SeriesLegend items={[
            { label: 'Portfolio', color: PORTFOLIO_COLOR, value: signedPercent(last?.cumulative_return) },
            ...(bench ? [{ label: `${bench.ticker} (benchmark)`, color: BENCHMARK_COLOR, value: signedPercent(bench.total_return) }] : []),
            ...(last?.inflation != null ? [{ label: 'Consumer prices', color: REFERENCE_COLOR, dashed: true, value: signedPercent(last.inflation) }] : []),
          ]} />
          <GrowthChart series={series} benchmarkLabel={benchmarkLabel} height={280} showDates={false} />
          <h3 className="pf-subhead">Below the previous high <span>max {percent(performance?.max_drawdown)}, now {percent(performance?.current_drawdown)}</span></h3>
          <DrawdownChart series={series} height={130} />
        </>}
      </SectionCard>
      <SectionCard title="Value and money put in" description="Market value against deposits less withdrawals; the gap is your gain">
        {props.isLoading ? <LoadingState label="Loading values" /> : <ValueChart series={series} currency={currency} height={240} />}
      </SectionCard>
      <SectionCard title="Calendar years" description={bench ? `Time-weighted return against ${bench.ticker}; * part of a year` : 'Time-weighted return; * part of a year'}>
        {bench && <SeriesLegend items={[{ label: 'Portfolio', color: PORTFOLIO_COLOR }, { label: bench.ticker, color: BENCHMARK_COLOR }]} />}
        <AnnualReturnsChart rows={performance?.annual_returns ?? []} benchmarkLabel={benchmarkLabel} height={220} />
      </SectionCard>
    </div>
    <div className="pf-overview__side">
      <Allocation holdings={props.holdings} currencies={props.currencies} summary={props.summary} currency={currency} onOpenDetail={props.onOpenDetail} />
      <LatestActivity transactions={props.transactions} onOpenDetail={props.onOpenDetail} onTab={props.onTab} />
    </div>
  </div>
}
