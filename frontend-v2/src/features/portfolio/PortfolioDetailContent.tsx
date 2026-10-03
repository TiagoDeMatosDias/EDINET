import { ArrowUpRight, Building2, ChevronLeft, ChevronRight, WalletCards } from 'lucide-react'

import { LoadingState } from '../../components/Feedback'
import { Metric } from '../../components/Page'
import { CashEffect } from './ActivityCells'
import { fxLegs } from './activityModel'
import { HoldingPriceChart } from './PortfolioCharts'
import { displayValue, formatDay, heldFor, holdingName, isCash, money, percent, priceNote, quantity, signedPercent, titleCase } from './portfolioFormat'
import { DetailList } from './PortfolioPrimitives'
import type { Holding, HoldingHistoryPoint, PortfolioDetail, PortfolioSummary } from './portfolioTypes'

type Props = {
  detail: PortfolioDetail
  summary: PortfolioSummary
  holdingHistory?: HoldingHistoryPoint[]
  holdingHistoryLoading?: boolean
  currency: string
  position?: { index: number; count: number }
  onStep: (offset: number) => void
  onAnalyze: (holding: Holding) => void
  onIncome: (symbol: string) => void
}

function HoldingDetails(props: Props & { holding: Holding }) {
  const { holding, currency } = props
  const performance = holding.performance
  const cash = isCash(holding)
  const native = holding.currency || currency
  const note = priceNote(holding)
  return <div className="drawer-stack">
    <div className="holding-detail-heading">
      {cash ? <WalletCards /> : <Building2 />}
      <span><strong>{holdingName(holding) || holding.symbol}</strong><small>{[holding.symbol, holding.asset_category, native, performance?.edinet_code, performance?.industry].filter(Boolean).join(' · ')}</small></span>
      {props.position && <span className="pf-stepper">
        <button type="button" className="icon-button" aria-label="Previous holding" disabled={props.position.index === 0} onClick={() => props.onStep(-1)} title="Previous holding (Shift+K)"><ChevronLeft /></button>
        <span>{props.position.index + 1} of {props.position.count}</span>
        <button type="button" className="icon-button" aria-label="Next holding" disabled={props.position.index >= props.position.count - 1} onClick={() => props.onStep(1)} title="Next holding (Shift+J)"><ChevronRight /></button>
      </span>}
      {!cash && Number(performance?.dividends_display) > 0 && <button type="button" className="button button--ghost button--small" onClick={() => props.onIncome(holding.symbol)} title="Dividends from this holding in the Income tab">Dividends</button>}
      {!cash && <button type="button" className="button button--secondary button--small" onClick={() => props.onAnalyze(holding)} title={performance?.edinet_code ? 'Company analysis with filings (A)' : 'Stored prices for this ticker (A)'}>Open in Analysis<ArrowUpRight /><kbd>A</kbd></button>}
    </div>
    {note && <p className={`pf-issue is-${note.level}`}>{note.text}.</p>}
    <div className="drawer-metric-grid">
      <Metric label="Value" value={money(displayValue(holding), currency)} detail={props.summary.totalValue ? `${percent(displayValue(holding) / props.summary.totalValue)} of the portfolio` : undefined} />
      {!cash && <>
        <Metric label="Unrealized P&L" value={money(performance?.pnl_display, currency)} detail={signedPercent(performance?.total_return_display)} />
        <Metric label="Total P&L" value={money(performance?.total_pnl_display, currency)} detail="With realized gains and dividends" />
        <Metric label="Dividends" value={money(performance?.dividends_display, currency)} detail="Net of withholding" />
      </>}
    </div>
    {!cash && (props.holdingHistoryLoading ? <LoadingState label="Loading the price history" /> : <section className="drawer-section">
      <h3>Close in {native} <small>with the average cost (dashed)</small></h3>
      <HoldingPriceChart data={props.holdingHistory} currency={native} averageCost={holding.avg_cost} />
    </section>)}
    <section className="drawer-section"><h3>Position</h3><DetailList rows={cash ? [
      { label: 'Balance', value: money(holding.market_value_native ?? holding.quantity, native, 2) },
      { label: `Value in ${currency}`, value: money(displayValue(holding), currency, 2) },
    ] : [
      { label: 'Quantity', value: quantity(holding.quantity) },
      { label: 'Average cost', value: money(holding.avg_cost, native, 2), tip: 'Cost per share of the shares still held, commission included.' },
      { label: 'Latest close', value: money(holding.market_price, native, 2), detail: holding.price_date ? `${formatDay(holding.price_date)}${holding.price_ticker && holding.price_ticker !== holding.symbol ? ` · ${holding.price_ticker}` : ''}${holding.price_currency && holding.price_currency !== native ? ` · quoted in ${holding.price_currency}` : ''}` : undefined },
      { label: 'Cost of shares held', value: money(performance?.cost_basis_display, currency) },
      { label: 'Realized P&L', value: money(performance?.realized_pnl_display, currency), tip: 'Gains and losses locked in by sales, over every holding period.' },
      { label: 'Return in its own currency', value: signedPercent(performance?.total_return_native), detail: performance?.fx_return ? `Currency moves ${signedPercent(performance.fx_return)} in ${currency}` : undefined },
      { label: 'Return a year', value: signedPercent(performance?.annualized_return), tip: 'The unrealized return compounded per year since the current holding period began.' },
      { label: 'Held since', value: formatDay(performance?.held_since ?? performance?.first_purchase), detail: heldFor(performance) ? `${heldFor(performance)} in this holding period${(performance?.num_holding_periods ?? 0) > 1 ? ` (held ${performance?.num_holding_periods} separate times)` : ''}` : undefined, tip: 'The start of the latest continuous holding period: a full sale and later purchase starts a new one.' },
      { label: 'Buys / sells', value: `${performance?.num_buys ?? 0} / ${performance?.num_sells ?? 0}` },
      { label: 'Price volatility', value: percent(performance?.volatility), tip: 'Annualized standard deviation of weekday price changes in its own currency.' },
    ]} /></section>
  </div>
}

function TransactionDetails({ row }: { row: Extract<PortfolioDetail, { kind: 'transaction' }>['transaction'] }) {
  const native = row.currency || 'EUR'
  const trade = row.activity_type === 'TRADE'
  const conversion = fxLegs(row)
  const booked = row.report_date && row.report_date !== row.trade_date ? row.report_date : null
  return <div className="drawer-stack"><section className="drawer-section"><h3>{titleCase(row.activity_type ?? 'Activity')}</h3><p className="drawer-description">{row.description || 'No description was supplied by the imported source.'}</p><DetailList rows={[
    { label: trade ? 'Trade date' : 'Date', value: formatDay(row.trade_date) },
    ...(booked ? [{ label: 'Booked by the broker', value: formatDay(booked), tip: 'Corrections keep the date of the record they correct but are booked later.' }] : []),
    { label: 'Settlement date', value: formatDay(row.settle_date) },
    { label: 'Symbol', value: row.symbol || '—' },
    { label: 'Asset category', value: conversion ? 'Currency conversion' : row.asset_category || '—' },
    { label: 'Side', value: row.buy_sell || '—' },
    { label: 'Quantity', value: Number(row.quantity) ? quantity(row.quantity) : '—' },
    { label: conversion ? 'Exchange rate' : 'Trade price', value: trade ? (conversion ? String(row.trade_price ?? '—') : money(row.trade_price, native, 2)) : '—' },
    { label: 'Gross trade value', value: trade ? money(row.trade_money, native, 2) : '—' },
    { label: 'Reported amount', value: money(row.amount, native, 2) },
    { label: 'Cash effect', value: <CashEffect row={row} /> },
    { label: 'Commission', value: money(row.commission, row.commission_currency || native, 2), tip: conversion ? 'Charged separately from the conversion, in the currency shown.' : undefined },
    { label: 'Taxes', value: money(row.taxes, native, 2) },
    ...(row.account_id ? [{ label: 'Account', value: row.account_id }] : []),
    { label: 'Source file', value: row.source_file || '—' },
  ]} /></section></div>
}

export function PortfolioDetailContent(props: Props) {
  if (props.detail.kind === 'holding') return <HoldingDetails {...props} holding={props.detail.holding} />
  if (props.detail.kind === 'transaction') return <TransactionDetails row={props.detail.transaction} />
  return null
}
