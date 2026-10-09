import { Calculator, Download, ExternalLink, FileText, LineChart } from 'lucide-react'
import { useMemo, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'

import { ApiError } from '../../api/client'
import { downloadApiFile } from '../../api/download'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { Tip } from '../../components/Tooltip'
import { HotkeyKbd } from '../../hotkeys/HotkeyKbd'
import { useHotkeyScope } from '../../hotkeys/useHotkeyScope'
import { bondsPanelScope } from '../analysis/analysisHotkeys'
import { filingHref } from '../filings/filingFormat'
import { formatDay, formatNumber, formatPercent } from '../research/researchModel'
import { CurveChart, MaturityLadderChart } from './BondCharts'
import { bondMarketHref, calculatorHref, COUPON_KIND_LABELS, FEATURE_LABELS, featureTags, formatBp, formatCoupon, formatYears, formatYen, ratingText, SENIORITY_LABELS } from './bondFormat'
import { useCompanyBonds } from './bondQueries'
import type { Bond, CompanyBonds } from './bondTypes'
import '../research/research.css'
import './bonds.css'

function Kpi({ label, value, detail, tip }: { label: string; value: string; detail?: string; tip?: string }) {
  return <div className="bd-kpi"><span>{tip ? <Tip content={tip}>{label}</Tip> : label}</span><strong>{value}</strong>{detail && <small>{detail}</small>}</div>
}

export function CompanyBondsPanel({ companyCode }: { companyCode: string }) {
  const navigate = useNavigate()
  const query = useCompanyBonds(companyCode)
  const [showAll, setShowAll] = useState(false)
  const data = query.data
  const hasBonds = Boolean(data?.bonds.length)
  useHotkeyScope(bondsPanelScope, {
    market: () => navigate(`/research?tab=bond-market&issuer=${encodeURIComponent(companyCode)}`),
    redeemed: () => setShowAll(value => !value),
  }, { enabled: hasBonds })

  if (query.isLoading) return <LoadingState label="Loading bonds" />
  if (query.error instanceof ApiError && query.error.status === 503) {
    return <EmptyState title="No bond data yet" description="An administrator can run the Update bonds pipeline step to read bond terms from EDINET filings." />
  }
  if (query.isError) return <ErrorState error={query.error} retry={() => void query.refetch()} />
  if (!data || !data.bonds.length) {
    return <p className="bd-none">No bonds in this company’s latest annual-report bond schedule or its bond supplements{data?.documents.length ? '' : ' (no annual report with a schedule has been read yet)'}.</p>
  }
  return <CompanyBondsContent data={data} companyCode={companyCode} showAll={showAll} onToggle={() => setShowAll(!showAll)} />
}

function CompanyBondsContent({ data, companyCode, showAll, onToggle }: { data: CompanyBonds; companyCode: string; showAll: boolean; onToggle: () => void }) {
  const navigate = useNavigate()
  const { summary } = data
  const live = data.bonds.filter(bond => bond.status === 'outstanding')
  const shown = showAll ? data.bonds : live
  const past = data.bonds.length - live.length
  const hasGroup = data.bonds.some(bond => !bond.is_parent)
  const curveBonds = useMemo(() => live
    .filter(bond => bond.is_parent && bond.horizon != null && (bond.market_yield ?? bond.model_yield) != null)
    .map(bond => ({ label: bond.label, horizon: bond.horizon as number, yield: (bond.market_yield ?? bond.model_yield) as number })), [live])
  const ratings = summary.ratings.map(item => `${item.agency} ${item.rating}`).join(' · ')
  const relative = summary.median_spread != null && summary.rating_peer_spread != null ? summary.median_spread - summary.rating_peer_spread : null

  return <div className="bd-company">
    <div className="bd-actions">
      <span className="muted">{live.length} outstanding{summary.as_of ? ` · balances at ${formatDay(summary.as_of)}` : ''}{summary.foreign_currency_count ? ` · ${summary.foreign_currency_count} in foreign currency` : ''}</span>
      <span className="statement-toolbar__spacer" />
      <Link className="button button--ghost button--small" to={`/research?tab=bond-market&issuer=${encodeURIComponent(companyCode)}`} title="See these bonds among every issuer’s (M)"><LineChart aria-hidden="true" />Bond market <span aria-hidden="true"><HotkeyKbd hotkey={bondsPanelScope.byId.market} /></span></Link>
    </div>
    <div className="bd-kpis">
      <Kpi label="Outstanding" value={formatYen(summary.total_outstanding)} detail={`${summary.outstanding_count} ${summary.outstanding_count === 1 ? 'bond' : 'bonds'}${summary.not_separately_reported ? `, ${summary.not_separately_reported} reported in a group` : ''}`} tip="Yen bonds at their latest reported balance; bonds issued since the last annual report at their issue amount." />
      <Kpi label="Average coupon" value={formatPercent(summary.average_coupon, 2)} detail="weighted by amount" />
      <Kpi label="Average life" value={formatYears(summary.average_years)} detail={summary.next_maturity ? `next due ${formatDay(summary.next_maturity)}` : undefined} />
      <Kpi label="Due within a year" value={summary.due_within_year ? formatYen(summary.due_within_year) : 'None'} />
      <Kpi label="Rating" value={summary.rating ?? 'Not rated'} detail={ratings ? `${ratings}${summary.rated_on ? ` · ${formatDay(summary.rated_on)}` : ''}` : 'no rating in its bond supplements'} tip="Ratings at the latest issue, from its shelf-registration supplement. Comparisons use R&I, then JCR, S&P, Moody’s." />
      <Kpi label="Spread vs rating peers" value={relative == null ? '—' : formatBp(relative, true)} detail={summary.median_spread != null ? `median ${formatBp(summary.median_spread)} at issue; peers ${formatBp(summary.rating_peer_spread)}` : undefined} tip="The median spread over JGBs at which this company’s senior bonds were issued, against the median of senior bonds rated within a notch issued in the last three years. Positive: investors asked for more than its rating suggests." />
    </div>
    <div className="bd-charts">
      {data.ladder.length > 0 && <section className="panel bd-chart-panel" aria-label="Maturity ladder"><MaturityLadderChart ladder={data.ladder} /></section>}
      {data.curve && curveBonds.length > 0 && <section className="panel bd-chart-panel" aria-label="Bonds against the government curve"><CurveChart curve={data.curve} bonds={curveBonds} /></section>}
    </div>
    <div className="rs-table-scroll bd-table-wrap">
      <table className="rs-table bd-table" aria-label="Bonds">
        <thead><tr>
          <th scope="col">Bond</th>
          {hasGroup && <th scope="col">Issuer</th>}
          <th scope="col">Ranking</th>
          <th scope="col" className="num">Coupon</th>
          <th scope="col">Issued</th>
          <th scope="col">Maturity</th>
          <th scope="col" className="num">Years left</th>
          <th scope="col" className="num">Outstanding</th>
          <th scope="col">Rating</th>
          <th scope="col" className="num"><Tip content="Over JGBs of the same tenor: from the JSDA reference price where quoted, with the spread at issue below it; i marks a bond with no quote.">Spread</Tip></th>
          <th scope="col" className="num"><Tip content="The JSDA reference price per 100 of face value; * marks an estimate at today’s JGB yield plus the spread at issue.">Price</Tip></th>
          <th scope="col">Sources</th>
        </tr></thead>
        <tbody>{shown.map(bond => <BondRow key={bond.bond_id} bond={bond} hasGroup={hasGroup} companyCode={companyCode} onOpen={() => navigate(bondMarketHref(bond.bond_id, companyCode))} />)}</tbody>
      </table>
    </div>
    <p className="rs-hint bd-hint">
      {past > 0 && <><button type="button" className="text-button" onClick={onToggle}>{showAll ? 'Hide' : 'Show'} {past} matured or redeemed</button> <span aria-hidden="true"><HotkeyKbd hotkey={bondsPanelScope.byId.redeemed} /></span> · </>}
      Click a bond to compare it with similar bonds. Prices are JSDA reference prices{live.some(bond => bond.market_date) ? ` of ${formatDay(live.find(bond => bond.market_date)?.market_date)}` : ''}; * marks an estimate from the spread at issue, not a traded price. Ratings marked * are the issuer’s latest for that ranking.
    </p>
    <Sources data={data} companyCode={companyCode} />
  </div>
}

function BondRow({ bond, hasGroup, companyCode, onOpen }: { bond: Bond; hasGroup: boolean; companyCode: string; onOpen: () => void }) {
  const tags = featureTags(bond)
  const inactive = bond.status !== 'outstanding'
  return <tr className={inactive ? 'is-inactive' : undefined} tabIndex={0} onClick={onOpen} onKeyDown={event => { if (event.key === 'Enter' && event.target === event.currentTarget) onOpen() }} title={bond.name}>
    <th scope="row">
      <span className="bd-bond-cell">
        <strong>{bond.label}</strong>
        <small>
          {bond.currency !== 'JPY' && <span className="bd-tag">{bond.currency}</span>}
          {tags.map(tag => <span key={tag} className="bd-tag" title={FEATURE_LABELS[tag]}>{FEATURE_LABELS[tag]?.split(' (')[0] ?? tag}</span>)}
          {bond.coupon_kind !== 'fixed' && bond.coupon_kind !== 'unknown' && <span className="bd-tag">{COUPON_KIND_LABELS[bond.coupon_kind]}</span>}
          {inactive && <span className="bd-tag bd-tag--muted">{bond.status === 'matured' ? 'Matured' : 'Redeemed'}</span>}
        </small>
      </span>
    </th>
    {hasGroup && <td className="bd-issuer">{bond.is_parent ? <span className="rs-dim">Company</span> : bond.issuer}</td>}
    <td>{SENIORITY_LABELS[bond.seniority] ?? bond.seniority}</td>
    <td className="num">{formatCoupon(bond)}</td>
    <td>{bond.issue_date ? formatDay(bond.issue_date) : <span className="rs-dim">—</span>}</td>
    <td>{bond.maturity ? formatDay(bond.maturity) : bond.perpetual ? 'Perpetual' : <span className="rs-dim" title={bond.maturity_text ?? undefined}>{bond.maturity_text || '—'}</span>}{bond.call_date && <small className="bd-sub">call {formatDay(bond.call_date)}</small>}</td>
    <td className="num">{formatYears(bond.years_to_maturity)}</td>
    <td className="num" title={bond.outstanding_as_of ? `As of ${formatDay(bond.outstanding_as_of)}` : bond.outstanding == null ? 'Reported in a group of bonds in the annual report' : undefined}>{bond.outstanding == null ? <span className="rs-dim">in group</span> : bond.currency === 'JPY' ? formatYen(bond.outstanding) : <>{formatYen(bond.outstanding)}<small className="bd-sub">yen value</small></>}</td>
    <td>{ratingText(bond)}</td>
    <td className="num">{formatBp(bond.spread)}{bond.spread_basis === 'issue' ? <sup className="bd-basis">i</sup> : bond.issue_spread != null && <small className="bd-sub">{formatBp(bond.issue_spread)} at issue</small>}</td>
    <td className="num" title={bond.market_date ? `JSDA reference price, ${formatDay(bond.market_date)}` : undefined}>{bond.market_price != null ? formatNumber(bond.market_price, 2) : <>{formatNumber(bond.model_price, 2)}{bond.model_price != null && <sup className="bd-basis">*</sup>}</>}</td>
    <td className="bd-links" onClick={event => event.stopPropagation()}>
      {bond.issuance_doc_id && <a href={bond.issuance_url ?? undefined} target="_blank" rel="noreferrer" title="Bond supplement on EDINET (opens in a new tab)"><ExternalLink aria-hidden="true" />Terms</a>}
      {bond.schedule_doc_id && <Link to={filingHref(bond.schedule_doc_id, companyCode, 'analysis')} title="Annual report with the bond schedule, in the Filing Explorer"><FileText aria-hidden="true" />Report</Link>}
      {bond.coupon != null && bond.status === 'outstanding' && <Link to={calculatorHref(bond)} title="Price this bond in the Bonds & credit calculator"><Calculator aria-hidden="true" />Price</Link>}
    </td>
  </tr>
}

function Sources({ data, companyCode }: { data: CompanyBonds; companyCode: string }) {
  const [error, setError] = useState('')
  if (!data.documents.length) return null
  const download = (docId: string) => {
    setError('')
    downloadApiFile(`/api/bonds/documents/${encodeURIComponent(docId)}`, `${docId}.zip`).catch(reason => setError(reason instanceof Error ? reason.message : 'Download failed'))
  }
  return <details className="bd-sources">
    <summary>Sources: {data.documents.filter(item => item.kind === 'annual').length} annual report, {data.documents.filter(item => item.kind === 'issuance').length} bond supplements</summary>
    <ul>
      {data.documents.map(document => <li key={document.doc_id}>
        <span className="mono">{document.doc_id}</span>
        <span>{document.kind === 'annual' ? `Annual report, year ending ${formatDay(document.period_end)}` : `Bond supplement, ${formatDay(document.submitted_at)}`}</span>
        <span className="rs-dim">{document.bond_count} {document.bond_count === 1 ? 'bond' : 'bonds'}</span>
        <a href={document.edinet_url} target="_blank" rel="noreferrer"><ExternalLink aria-hidden="true" />EDINET</a>
        {document.in_catalog && <Link to={filingHref(document.doc_id, companyCode, 'analysis')}><FileText aria-hidden="true" />Filing Explorer</Link>}
        {document.stored && <button type="button" className="text-button" onClick={() => download(document.doc_id)}><Download aria-hidden="true" />ZIP</button>}
      </li>)}
    </ul>
    {error && <p className="form-error" role="alert">{error}</p>}
    <p className="rs-hint">Terms come from the shelf-registration supplements (発行登録追補書類) filed when each bond was priced; balances from the bond schedule (社債明細表) in the annual report. EDINET shows supplements while their shelf registration is open; the ZIP is the copy kept here.</p>
  </details>
}
