import { Calculator, ExternalLink, FileText } from 'lucide-react'
import type { ReactNode } from 'react'
import { Link } from 'react-router-dom'

import { ErrorState, LoadingState } from '../../components/Feedback'
import { Tip } from '../../components/Tooltip'
import { filingHref } from '../filings/filingFormat'
import { analysisHref, formatDay, formatNumber, formatPercent } from '../research/researchModel'
import { MarketHistoryChart, PeerSpreadChart } from './BondCharts'
import { calculatorHref, COUPON_KIND_LABELS, displayTicker, FEATURE_LABELS, filedTitle, formatBp, formatCoupon, formatYears, formatYen, maturityText, offeringText, ratingText, securityText, SENIORITY_LABELS } from './bondFormat'
import { useBondDetail } from './bondQueries'
import type { BondDetail, PeerBond } from './bondTypes'

const FREQUENCY = { 1: 'annual', 2: 'semi-annual', 4: 'quarterly', 12: 'monthly' } as Record<number, string>

export function BondDetailPanel({ bondId, onSelect, keys }: { bondId: string; onSelect: (bondId: string) => void; keys?: { calculator?: string; open?: string } }) {
  const detail = useBondDetail(bondId)
  if (!bondId) return <p className="rs-empty">Choose a bond to see its terms, its value on today’s curve, and similar bonds of other issuers.</p>
  if (detail.isLoading) return <LoadingState label="Loading the bond" />
  if (detail.isError || !detail.data) return <ErrorState error={detail.error} retry={() => void detail.refetch()} />
  return <BondDetailContent data={detail.data} onSelect={onSelect} keys={keys} />
}

function Term({ label, children, tip }: { label: string; children: ReactNode; tip?: string }) {
  return <div><dt>{tip ? <Tip content={tip}>{label}</Tip> : label}</dt><dd>{children}</dd></div>
}

function BondDetailContent({ data, onSelect, keys }: { data: BondDetail; onSelect: (bondId: string) => void; keys?: { calculator?: string; open?: string } }) {
  const { bond, issuance, valuation } = data
  const callable = bond.horizon_to === 'call'
  const relative = valuation?.relative_spread
  const inLine = relative != null && Math.abs(relative) < 0.0005
  return <div className="bd-detail">
    <header className="rs-detail__header">
      <div>
        <h2><Link to={analysisHref(bond.edinet_code)} title={`Open in Analysis${keys?.open ? ` (${keys.open})` : ''}`}>{bond.company_name}</Link></h2>
        <p className="rs-detail__meta" title={bond.name && bond.name !== bond.label ? `Filed as ${bond.name}` : undefined}>{bond.label}</p>
        <p className="bd-tags">
          <span className="bd-tag">{SENIORITY_LABELS[bond.seniority]}</span>
          {bond.features.filter(feature => feature !== bond.seniority).map(feature => <span key={feature} className="bd-tag">{FEATURE_LABELS[feature] ?? feature}</span>)}
          {bond.currency !== 'JPY' && <span className="bd-tag">{bond.currency}</span>}
        </p>
      </div>
      <nav className="rs-detail__links" aria-label="Open this bond">
        {bond.coupon != null && <Link to={calculatorHref(bond)} title={`Price it in the Bonds & credit calculator${keys?.calculator ? ` (${keys.calculator})` : ''}`}><Calculator aria-hidden="true" />Price</Link>}
        {bond.issuance_url && <a href={bond.issuance_url} target="_blank" rel="noreferrer" title="The bond supplement on EDINET"><ExternalLink aria-hidden="true" />Terms</a>}
        {bond.schedule_doc_id && <Link to={filingHref(bond.schedule_doc_id, bond.edinet_code, 'analysis')} title="The annual report with the bond schedule"><FileText aria-hidden="true" />Report</Link>}
      </nav>
    </header>

    <section className="bd-section" aria-label="Terms">
      <h3>Terms</h3>
      <dl className="rs-summary rs-summary--grid bd-terms">
        <Term label="Coupon">{formatCoupon(bond)}{bond.frequency ? ` ${FREQUENCY[bond.frequency] ?? ''}` : ''}{bond.coupon_kind !== 'fixed' && <small> · {COUPON_KIND_LABELS[bond.coupon_kind]}</small>}</Term>
        <Term label="Issued">{bond.issue_date ? formatDay(bond.issue_date) : '—'}{bond.issue_price != null && bond.issue_price !== 100 && <small> at {formatNumber(bond.issue_price, 3)}</small>}</Term>
        <Term label="Maturity">{bond.maturity ? formatDay(bond.maturity) : bond.perpetual ? 'Perpetual' : maturityText(bond.maturity_text) || '—'}</Term>
        {bond.call_date && <Term label="First call" tip="The first date the issuer may repay early; the coupon resets after it. Yields and prices here run to this date.">{formatDay(bond.call_date)}</Term>}
        <Term label="Years left">{bond.years_to_maturity != null ? formatYears(bond.years_to_maturity) : bond.perpetual ? 'Perpetual' : '—'}{callable && <small> · {formatYears(bond.horizon)} to call</small>}</Term>
        <Term label="Amount issued">{formatYen(bond.amount_issued)}</Term>
        <Term label="Outstanding">{bond.outstanding == null ? 'In a group' : formatYen(bond.outstanding)}{bond.outstanding_as_of && <small> · {formatDay(bond.outstanding_as_of)}</small>}</Term>
        <Term label="Rating">{bond.ratings.length ? bond.ratings.map(item => `${item.agency} ${item.rating}`).join(' · ') : 'Not rated'}{bond.rating_inferred ? <small> · the issuer’s latest</small> : null}</Term>
        <Term label="Security">{bond.collateral && securityText(bond) !== bond.collateral ? <Tip content={`Filed as: ${bond.collateral}`}>{securityText(bond)}</Tip> : securityText(bond)}</Term>
        {issuance && <Term label="Negative pledge" tip="A covenant in the bond supplement that limits giving collateral: the issuer will not secure other bonds without securing this one equally.">{issuance.negative_pledge ? 'Yes' : 'No'}</Term>}
        {issuance?.offering && <Term label="Offering">{offeringText(issuance.offering) !== issuance.offering ? <Tip content={`Filed as: ${issuance.offering}`}>{offeringText(issuance.offering)}</Tip> : issuance.offering}</Term>}
        {bond.issuer !== bond.company_name && <Term label="Issued by"><span title={filedTitle(bond.issuer, bond.issuer_ja)}>{bond.issuer}</span></Term>}
      </dl>
    </section>

    <section className="bd-section" aria-label="Valuation">
      <h3>Valuation</h3>
      <dl className="rs-summary rs-summary--grid">
        {bond.market_price != null && <>
          <Term label="Reference price" tip={`JSDA OTC reference price: the average of ${bond.market_reporters ?? 'several'} dealers' quotes${bond.jsda_code ? ` for JSDA issue ${bond.jsda_code}` : ''}.`}>{formatNumber(bond.market_price, 2)}{bond.market_date && <small> · {formatDay(bond.market_date)}</small>}</Term>
          <Term label={callable ? 'Yield to call' : 'Yield'} tip="Computed from the reference price on the same conventions as the spread at issue.">{formatPercent(bond.market_yield, 3)}</Term>
          <Term label="Spread now" tip="That yield less the JGB yield of the same tenor on the price date.">{formatBp(bond.market_spread)}{bond.issue_spread != null && bond.market_spread != null && <small> · {formatBp(bond.market_spread - bond.issue_spread, true)} since issue</small>}</Term>
        </>}
        <Term label="Spread at issue" tip="Yield at issue less the JGB par yield of the same tenor on the issue date.">{formatBp(bond.issue_spread)}{bond.jgb_at_issue != null && <small> over {formatPercent(bond.jgb_at_issue, 2)}</small>}</Term>
        <Term label={`JGB ${formatYears(bond.horizon)} today`} tip={`Ministry of Finance par yield${data.curve ? ` on ${formatDay(data.curve.date)}` : ''}, interpolated to the bond’s remaining tenor.`}>{formatPercent(bond.jgb_now, 3)}</Term>
        {bond.market_price == null && <>
          <Term label="Yield at that spread" tip="No JSDA quote: today’s JGB yield plus the spread at issue.">{formatPercent(bond.model_yield, 3)}</Term>
          <Term label="Price at that spread" tip="Clean price per 100 of face value at that yield: an estimate, not a traded price.">{formatNumber(bond.model_price, 3)}</Term>
        </>}
        <Term label="Modified duration">{formatNumber(bond.duration, 2)}</Term>
        {valuation && <>
          <Term label="Peer spread" tip={`Median spread of ${valuation.peer_count} bonds of other issuers (${valuation.quoted_peers} at JSDA reference prices, the rest at issue within the last three years): same ranking, rated within a notch, within ${valuation.tenor_window} years of this bond’s tenor.`}>{formatBp(valuation.peer_spread)}{valuation.spread_quartiles && <small> · {formatBp(valuation.spread_quartiles[0])}–{formatBp(valuation.spread_quartiles[2])}</small>}</Term>
          <Term label="Fair yield">{formatPercent(valuation.fair_yield, 3)}</Term>
          <Term label="Fair price">{formatNumber(valuation.fair_price, 3)}</Term>
        </>}
      </dl>
      {valuation && relative != null && <p className="rs-market-read">At {formatBp(bond.spread)}{bond.spread_basis === 'market' ? ' in the market' : ' at issue'}, this bond is {inLine
        ? <><strong>in line with</strong> its peers (within 5 bp)</>
        : <><strong>{relative > 0 ? 'cheaper' : 'richer'}</strong> than its peers by <strong>{formatBp(Math.abs(relative))}</strong>{relative > 0 ? ': it pays more spread for its rating' : ': it pays less spread for its rating'}</>}. At the peer spread it would be worth <strong>{formatNumber(valuation.fair_price, 2)}</strong>{(bond.market_price ?? bond.model_price) != null && <>, against {formatNumber(bond.market_price ?? bond.model_price, 2)} {bond.market_price != null ? 'quoted' : 'at its own spread'}</>}.</p>}
      {!valuation && <p className="rs-hint">{bond.coupon == null || bond.coupon_kind === 'floating'
        ? 'A floating or unstated coupon cannot be valued on the fixed-rate government curve.'
        : bond.horizon == null || bond.horizon <= 0 ? 'The bond has matured or has no stated maturity or call date to value it to.'
          : 'Too few comparable bonds are quoted or were issued recently to estimate a peer fair value.'}</p>}
      {bond.horizon != null && <PeerSpreadChart
        peers={data.spread_curve.peers}
        issuer={data.spread_curve.issuer}
        target={{ horizon: bond.horizon, spread: bond.spread ?? null, label: bond.label }}
        median={valuation?.peer_spread ?? null}
        window={valuation?.tenor_window ?? null}
      />}
      {data.market_history.length > 1 && <MarketHistoryChart history={data.market_history} />}
    </section>

    <section className="bd-section" aria-label="Similar bonds">
      <h3>Similar bonds of other issuers</h3>
      <p className="rs-hint">Closest first by years left, rating, ranking, and industry; one bond per company.</p>
      <PeerTable bonds={data.similar} onSelect={onSelect} />
    </section>

    {data.issuer_bonds.length > 0 && <section className="bd-section" aria-label="The issuer's other bonds">
      <h3>{bond.company_name}’s other bonds</h3>
      <PeerTable bonds={data.issuer_bonds} onSelect={onSelect} hideCompany />
    </section>}
  </div>
}

function PeerTable({ bonds, onSelect, hideCompany = false }: { bonds: PeerBond[]; onSelect: (bondId: string) => void; hideCompany?: boolean }) {
  if (!bonds.length) return <p className="rs-empty">None found.</p>
  return <div className="rs-table-scroll">
    <table className="rs-table bd-table bd-table--compact">
      <thead><tr>
        {!hideCompany && <th scope="col">Company</th>}
        <th scope="col">Bond</th>
        <th scope="col">Rating</th>
        <th scope="col" className="num">Coupon</th>
        <th scope="col" className="num">Left</th>
        <th scope="col" className="num">Spread</th>
        <th scope="col" className="num">Price*</th>
      </tr></thead>
      <tbody>{bonds.map(item => <tr key={item.bond_id} tabIndex={0} onClick={() => onSelect(item.bond_id)} onKeyDown={event => { if (event.key === 'Enter') onSelect(item.bond_id) }}>
        {!hideCompany && <th scope="row"><span className="bd-bond-cell"><strong title={filedTitle(item.company_name, item.company_name_ja)}>{item.company_name}</strong><small>{[displayTicker(item.ticker), item.industry].filter(Boolean).join(' · ')}</small></span></th>}
        <td className="bd-label" title={filedTitle(item.label, item.label_ja)}>{item.label}</td>
        <td>{ratingText(item)}</td>
        <td className="num">{formatCoupon(item)}</td>
        <td className="num">{formatYears(item.horizon)}</td>
        <td className="num">{formatBp(item.issue_spread)}</td>
        <td className="num">{formatNumber(item.model_price, 2)}</td>
      </tr>)}</tbody>
    </table>
  </div>
}
