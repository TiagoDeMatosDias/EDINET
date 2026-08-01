import { ArrowUpRight, Building2, Info, WalletCards } from 'lucide-react'

import { LoadingState } from '../../components/Feedback'
import { Metric } from '../../components/Page'
import { HoldingHistoryChart, PortfolioValueChart } from './PortfolioCharts'
import { ReturnDistribution, RiskTrend } from './PortfolioAdvancedAnalytics'
import { decimal, money, percent, quantity, titleCase, transactionCashEffect } from './portfolioFormat'
import { DetailList } from './PortfolioPrimitives'
import type {
  DividendHistory,
  HoldingHistoryPoint,
  Performance,
  PieData,
  PortfolioDetail,
  PortfolioSummary,
  Transaction,
  ValueHistory,
} from './portfolioTypes'

type Props = {
  detail: PortfolioDetail
  performance?: Performance
  summary: PortfolioSummary
  valueHistory?: ValueHistory
  allocation?: PieData
  currencies?: PieData
  dividends?: DividendHistory
  activity: Record<string, number>
  transactions: Transaction[]
  holdingHistory?: HoldingHistoryPoint[]
  holdingHistoryLoading?: boolean
  currency: string
  onAnalyze: (symbol: string) => void
}

function ValueDetails(props: Props) {
  const values = props.valueHistory?.portfolio_values ?? []
  const first = values.find(value => value != null) ?? 0
  const last = [...values].reverse().find(value => value != null) ?? props.summary.totalValue
  const netFlows = (props.valueHistory?.net_inflows ?? []).reduce<number>((sum, value) => sum + Number(value ?? 0), 0)
  return <div className="drawer-stack">
    <div className="drawer-metric-grid"><Metric label="Latest daily value" value={money(last, props.currency)} /><Metric label="First observed value" value={money(first, props.currency)} /><Metric label="Net cash flows" value={money(netFlows, props.currency)} /><Metric label="Unrealized P&L" value={money(props.summary.pnl, props.currency)} /></div>
    <PortfolioValueChart data={props.valueHistory} currency={props.currency} large />
    <div className="portfolio-method-note"><Info /><p>The value line includes deposits and withdrawals. Return statistics elsewhere on this screen remove those flows using the portfolio’s daily flow-adjusted series.</p></div>
  </div>
}

function PerformanceDetails({ performance, currency }: Pick<Props, 'performance' | 'currency'>) {
  const attribution = performance?.return_attribution
  const distribution = performance?.return_distribution
  return <div className="drawer-stack">
    <div className="drawer-metric-grid"><Metric label="Total return" value={percent(performance?.total_return)} /><Metric label="Annualized return" value={percent(performance?.annualized_return)} /><Metric label="Real return" value={percent(attribution?.real_return)} /><Metric label="Net dividends" value={money(performance?.dividend_breakdown?.total_net, currency)} /></div>
    <section className="drawer-section"><h3>Return composition</h3><DetailList rows={[
      { label: 'Capital appreciation', value: percent(attribution?.capital_appreciation), detail: 'Market-price contribution before income' },
      { label: 'Dividend contribution', value: percent(attribution?.dividend_yield), detail: 'Net dividends relative to average portfolio value' },
      { label: 'Cumulative inflation', value: percent(attribution?.inflation_total), detail: 'Selected-currency inflation series or the documented fallback' },
      { label: 'Real return', value: percent(attribution?.real_return), detail: 'Nominal return adjusted with the Fisher equation' },
    ]} /></section>
    <section className="drawer-section"><h3>Consistency</h3><DetailList rows={[
      { label: 'Sharpe ratio', value: decimal(performance?.sharpe_ratio), detail: `Uses a ${percent(performance?.risk_free_rate)} annual risk-free rate` },
      { label: 'Sortino ratio', value: decimal(performance?.sortino_ratio), detail: 'Penalizes downside variation only' },
      { label: 'Win rate', value: percent(performance?.win_rate), detail: `${distribution?.positive_days ?? 0} positive, ${distribution?.negative_days ?? 0} negative trading days` },
      { label: 'Profit factor', value: decimal(performance?.profit_factor), detail: 'Gross positive daily returns divided by gross losses' },
    ]} /></section>
  </div>
}

function RiskDetails({ performance, valueHistory }: Pick<Props, 'performance' | 'valueHistory'>) {
  return <div className="drawer-stack">
    <div className="drawer-metric-grid"><Metric label="Volatility" value={percent(performance?.volatility)} /><Metric label="Max drawdown" value={percent(performance?.max_drawdown)} /><Metric label="VaR 95%" value={percent(performance?.var_95)} /><Metric label="CVaR 95%" value={percent(performance?.cvar_95)} /></div>
    <RiskTrend data={valueHistory} />
    <ReturnDistribution data={valueHistory} />
    <div className="portfolio-method-note"><Info /><p>Historical VaR is the fifth percentile of observed daily returns. CVaR is the average return beyond that threshold; neither is a forecast or loss limit.</p></div>
  </div>
}

function ExposureList({ data, currency }: { data?: PieData; currency: string }) {
  const total = data?.values.reduce((sum, value) => sum + value, 0) ?? 0
  const rows = (data?.labels ?? []).map((label, index) => ({ label, value: data?.values[index] ?? 0 })).sort((left, right) => right.value - left.value)
  return <div className="drawer-exposure-list">{rows.map(row => <div key={row.label}><strong>{row.label}</strong><div><i style={{ width: `${Math.max(2, total ? row.value / total * 100 : 0)}%` }} /></div><span>{percent(total ? row.value / total : 0)}<small>{money(row.value, currency)}</small></span></div>)}</div>
}

function AllocationDetails(props: Props) {
  return <div className="drawer-stack">
    <div className="drawer-metric-grid"><Metric label="Invested value" value={money(props.summary.investedValue, props.currency)} /><Metric label="Positions" value={props.summary.positionCount.toLocaleString()} /><Metric label="Largest position" value={percent(props.summary.topHolding?.weight)} detail={props.summary.topHolding?.symbol} /><Metric label="Cash reserve" value={percent(props.summary.cashWeight)} detail={money(props.summary.cashValue, props.currency)} /></div>
    <section className="drawer-section"><h3>Position weights</h3><ExposureList data={props.allocation} currency={props.currency} /></section>
    <section className="drawer-section"><h3>Native currency exposure</h3><ExposureList data={props.currencies} currency={props.currency} /></section>
  </div>
}

function IncomeDetails(props: Props) {
  const breakdown = props.performance?.dividend_breakdown
  const gross = Number(breakdown?.total_gross ?? 0)
  const tax = Math.abs(Number(breakdown?.total_tax ?? 0))
  const payers = Object.entries(props.dividends?.companies ?? {}).map(([symbol, values]) => ({ symbol, total: values.reduce((sum, value) => sum + value, 0) })).sort((left, right) => right.total - left.total)
  return <div className="drawer-stack">
    <div className="drawer-metric-grid"><Metric label="Gross income" value={money(gross, props.currency)} /><Metric label="Withholding" value={money(tax, props.currency)} detail={gross ? percent(tax / gross) : '—'} /><Metric label="Net income" value={money(breakdown?.total_net, props.currency)} /><Metric label="Return contribution" value={percent(props.performance?.return_attribution?.dividend_yield)} /></div>
    <section className="drawer-section"><h3>All payers</h3><DetailList rows={payers.map(row => ({ label: row.symbol, value: money(row.total, props.currency), detail: gross ? percent(row.total / Math.max(1, Number(breakdown?.total_net ?? 0))) : undefined }))} /></section>
    <div className="portfolio-method-note"><Info /><p>Income totals are converted from each transaction’s payment currency. Net income subtracts withholding tax and includes payments in lieu of dividends.</p></div>
  </div>
}

function ActivityDetails({ activity, transactions }: Pick<Props, 'activity' | 'transactions'>) {
  const counts = Object.entries(activity).sort((left, right) => right[1] - left[1])
  return <div className="drawer-stack">
    <section className="drawer-section"><h3>Records by type</h3><DetailList rows={counts.map(([label, value]) => ({ label: titleCase(label), value: value.toLocaleString() }))} /></section>
    <section className="drawer-section"><h3>Latest records</h3><div className="drawer-activity-list">{transactions.slice(0, 10).map((row, index) => <div key={`${row.id ?? index}-${row.trade_date}`}><span><strong>{row.symbol || titleCase(row.activity_type ?? '')}</strong><small>{row.trade_date}</small></span><p>{row.description || titleCase(row.activity_type ?? '')}</p><b>{money(row.amount, row.currency ?? 'EUR', 2)}</b></div>)}</div></section>
  </div>
}

function HoldingDetails(props: Props & { detail: Extract<PortfolioDetail, { kind: 'holding' }> }) {
  const holding = props.detail.holding
  const performance = holding.performance
  const cash = holding.asset_category === 'CASH' || holding.symbol.startsWith('CASH')
  return <div className="drawer-stack">
    <div className="holding-detail-heading">{cash ? <WalletCards /> : <Building2 />}<span><strong>{performance?.name || holding.symbol}</strong><small>{holding.asset_category || 'Position'} · {holding.currency || props.currency}</small></span>{!cash && <button className="button button--secondary" onClick={() => props.onAnalyze(holding.symbol)}>Open analysis<ArrowUpRight /></button>}</div>
    <div className="drawer-metric-grid"><Metric label="Market value" value={money(holding.market_value, props.currency)} /><Metric label="Portfolio weight" value={percent(props.summary.totalValue ? Number(holding.market_value ?? 0) / props.summary.totalValue : 0)} /><Metric label="Cost basis" value={money(performance?.cost_basis_display, props.currency)} /><Metric label="P&L" value={money(performance?.pnl_display, props.currency)} /><Metric label="Total return" value={percent(performance?.total_return_display)} /><Metric label="Annualized" value={percent(performance?.annualized_return)} /><Metric label="Dividends" value={money(performance?.dividends_display, props.currency)} /><Metric label="FX contribution" value={percent(performance?.fx_return)} /></div>
    {!cash && (props.holdingHistoryLoading ? <LoadingState label="Loading position history" /> : <HoldingHistoryChart data={props.holdingHistory} currency={props.currency} />)}
    <section className="drawer-section"><h3>Position record</h3><DetailList rows={[
      { label: 'Quantity', value: quantity(holding.quantity) },
      { label: 'Average cost', value: money(holding.avg_cost, holding.currency || props.currency, 2) },
      { label: 'Current price', value: money(holding.market_price, holding.currency || props.currency, 2) },
      { label: 'First purchase', value: performance?.first_purchase || '—' },
      { label: 'Latest holding period', value: performance?.latest_holding_days ? `${performance.latest_holding_days.toLocaleString()} days` : '—' },
      { label: 'Buy / sell records', value: `${performance?.num_buys ?? 0} / ${performance?.num_sells ?? 0}` },
      { label: 'Native return', value: percent(performance?.total_return_native) },
      { label: 'Annualized volatility', value: percent(performance?.volatility) },
      { label: 'Industry', value: performance?.industry || 'Not classified' },
    ]} /></section>
  </div>
}

function TransactionDetails({ detail }: { detail: Extract<PortfolioDetail, { kind: 'transaction' }> }) {
  const row = detail.transaction
  return <div className="drawer-stack"><section className="drawer-section"><h3>{titleCase(row.activity_type ?? 'Activity')}</h3><p className="drawer-description">{row.description || 'No description was supplied by the imported source.'}</p><DetailList rows={[
    { label: 'Trade date', value: row.trade_date || '—' },
    { label: 'Settlement date', value: row.settle_date || '—' },
    { label: 'Symbol', value: row.symbol || '—' },
    { label: 'Asset category', value: row.asset_category || '—' },
    { label: 'Side', value: row.buy_sell || '—' },
    { label: 'Quantity', value: Number(row.quantity) ? quantity(row.quantity) : '—' },
    { label: 'Trade price', value: row.activity_type === 'TRADE' ? money(row.trade_price, row.currency || 'EUR', 2) : '—' },
    { label: 'Gross trade value', value: row.activity_type === 'TRADE' ? money(row.trade_money, row.currency || 'EUR', 2) : '—' },
    { label: 'Reported amount', value: money(row.amount, row.currency || 'EUR', 2) },
    { label: 'Cash effect', value: money(transactionCashEffect(row), row.currency || 'EUR', 2) },
    { label: 'Commission', value: money(row.commission, row.currency || 'EUR', 2) },
    { label: 'Taxes', value: money(row.taxes, row.currency || 'EUR', 2) },
    { label: 'Source file', value: row.source_file || '—' },
  ]} /></section></div>
}

export function PortfolioDetailContent(props: Props) {
  if (props.detail.kind === 'value') return <ValueDetails {...props} />
  if (props.detail.kind === 'performance') return <PerformanceDetails performance={props.performance} currency={props.currency} />
  if (props.detail.kind === 'risk') return <RiskDetails performance={props.performance} valueHistory={props.valueHistory} />
  if (props.detail.kind === 'allocation') return <AllocationDetails {...props} />
  if (props.detail.kind === 'income') return <IncomeDetails {...props} />
  if (props.detail.kind === 'activity') return <ActivityDetails activity={props.activity} transactions={props.transactions} />
  if (props.detail.kind === 'holding') return <HoldingDetails {...props} detail={props.detail} />
  return <TransactionDetails detail={props.detail} />
}
