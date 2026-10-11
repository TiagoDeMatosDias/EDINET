import { Download, ExternalLink, FileText, History, Users } from 'lucide-react'
import { useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'

import { ApiError } from '../../api/client'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { Tip } from '../../components/Tooltip'
import { HotkeyKbd } from '../../hotkeys/HotkeyKbd'
import { useHotkeyScope } from '../../hotkeys/useHotkeyScope'
import { shareholdersPanelScope } from '../analysis/analysisHotkeys'
import { downloadTextFile } from '../analysis/downloads'
import { filingHref } from '../filings/filingFormat'
import { formatDay } from '../research/researchModel'
import { entityHref, exactShares, fiscalYear, formatPoints, formatShares, formatStake, formatYen, KIND_LABELS, shareholdersCsv, stakeValue } from './ownershipFormat'
import { EntityLink, KindIcon, Kpi, StakeBar } from './ownershipParts'
import { useCompanyOwnership } from './ownershipQueries'
import type { CompanyOwnership, CompanyShareholders, DistributionSlice, ShareholderRow } from './ownershipTypes'
import '../research/research.css'
import './ownership.css'

const NO_DATA = 'An administrator can run the Update ownership pipeline step to read shareholders and officers from the stored annual reports.'

/** The largest shareholders in the company's latest annual report. ``marketCap`` values each stake. */
export function ShareholdersPanel({ companyCode, marketCap }: { companyCode: string; marketCap: number | null }) {
  const navigate = useNavigate()
  const query = useCompanyOwnership(companyCode)
  const [history, setHistory] = useState(false)
  const data = query.data?.shareholders ?? null
  useHotkeyScope(shareholdersPanelScope, {
    history: () => setHistory(value => !value),
    directory: () => navigate('/research?tab=people'),
  }, { enabled: Boolean(data) })

  if (query.isLoading) return <LoadingState label="Loading shareholders" />
  if (query.error instanceof ApiError && query.error.status === 503) return <EmptyState title="No ownership data yet" description={NO_DATA} />
  if (query.isError) return <ErrorState error={query.error} retry={() => void query.refetch()} />
  if (!data) {
    return <p className="ow-none">No list of major shareholders has been read for this company{query.data?.reports ? '' : ' (none of its annual reports is in the filing catalog, or the Update ownership step has not run since they were stored)'}.</p>
  }
  return <ShareholdersContent data={data} companyCode={companyCode} companyName={query.data?.company_name ?? companyCode} stale={Boolean(query.data?.stale)} holder={query.data?.holder ?? null} marketCap={marketCap} history={history} onHistory={() => setHistory(!history)} />
}

function ShareholdersContent({ data, companyCode, companyName, stale, holder, marketCap, history, onHistory }: {
  data: CompanyShareholders
  companyCode: string
  companyName: string
  stale: boolean
  holder: CompanyOwnership['holder']
  marketCap: number | null
  history: boolean
  onHistory: () => void
}) {
  const navigate = useNavigate()
  const rows = data.rows
  const largest = rows[0]
  const max = Math.max(...rows.map(row => row.ratio ?? 0), 0)
  const foreign = sliceRatio(data.distribution, ['foreign'])
  const previous = data.history.periods.length > 1 ? data.history.periods[data.history.periods.length - 2] : null
  const hasValue = marketCap != null && rows.some(row => row.ratio != null)
  const download = () => downloadTextFile(`${companyCode}-shareholders-${data.period_end}.csv`, shareholdersCsv(rows, marketCap), 'text/csv;charset=utf-8')

  return <div className="ow-company">
    {stale && <div className="callout callout--warning" role="status">The latest annual report stored for {companyName} covers {fiscalYear(data.period_end)}. Its shareholders may have changed since.</div>}
    <div className="ow-actions">
      <span className="muted">{rows.length} largest shareholders at {formatDay(data.as_of)} · annual report for {fiscalYear(data.period_end)}</span>
      <span className="statement-toolbar__spacer" />
      {data.history.periods.length > 1 && <button type="button" className="button button--ghost button--small" aria-pressed={history} onClick={onHistory} title="Each holder’s stake in every stored year"><History aria-hidden="true" />{history ? 'Hide history' : 'History'} <span aria-hidden="true"><HotkeyKbd hotkey={shareholdersPanelScope.byId.history} /></span></button>}
      <button type="button" className="button button--ghost button--small" onClick={download} title="Download the list as CSV"><Download aria-hidden="true" />CSV</button>
      <Link className="button button--ghost button--small" to="/research?tab=people" title="Look up any person or holder in Research"><Users aria-hidden="true" />People &amp; holders <span aria-hidden="true"><HotkeyKbd hotkey={shareholdersPanelScope.byId.directory} /></span></Link>
    </div>
    <div className="ow-kpis">
      <Kpi label="Held by those listed" value={formatStake(data.top_ratio)} detail={`${rows.length} holders${hasValue && data.top_ratio != null ? ` · ${formatYen(stakeValue(data.top_ratio, marketCap))}` : ''}`} tip="The stakes of the shareholders listed, added up as the report states them. Shares the company holds in itself are left out of the base where the report says so." />
      {largest && <Kpi label="Largest holder" value={formatStake(largest.ratio)} detail={largest.name} />}
      {data.shareholders != null && <Kpi label="Shareholders" value={data.shareholders.toLocaleString('en-US')} detail="holders of record" tip="Everyone on the shareholder register at the record date, from the report’s ownership statistics." />}
      {foreign != null && <Kpi label="Foreign investors" value={formatStake(foreign)} detail="institutions and individuals" tip="Share of the units held by investors outside Japan, from the report’s breakdown by kind of holder." />}
      {data.treasury_ratio != null && <Kpi label="Own shares" value={formatStake(data.treasury_ratio)} detail={data.treasury_shares != null ? `${formatShares(data.treasury_shares)} shares` : undefined} tip="Shares the company holds in itself (treasury shares), with any its affiliates hold in it, as a share of the shares issued. They carry no votes and are left out of the list." />}
    </div>
    {holder != null && holder.holdings > 0 && <p className="ow-also"><Link to={entityHref(holder.entity_id)}>{companyName} is itself among the largest shareholders of {holder.holdings.toLocaleString('en-US')} {holder.holdings === 1 ? 'company' : 'companies'}</Link></p>}
    {data.distribution.length > 0 && <Distribution slices={data.distribution} />}
    <div className="rs-table-scroll ow-table-wrap">
      <table className="rs-table ow-table" aria-label="Major shareholders">
        <thead><tr>
          <th scope="col" className="num">#</th>
          <th scope="col">Shareholder</th>
          <th scope="col" className="num">Stake</th>
          <th scope="col" className="num">{previous ? <Tip content={`Change in the holder’s whole stake since the report for ${fiscalYear(previous)}, in percentage points.`}>Change</Tip> : 'Change'}</th>
          <th scope="col" className="num">Shares</th>
          {hasValue && <th scope="col" className="num"><Tip content="The stake times the company’s market capitalisation today: an estimate from the shares held at the record date.">Value</Tip></th>}
          <th scope="col"><Tip content="How many other companies list this holder among their largest shareholders in their latest report.">Also holds</Tip></th>
          <th scope="col"><Tip content="The first of the unbroken run of stored years in which the holder is listed.">Listed since</Tip></th>
        </tr></thead>
        <tbody>{rows.map(row => <HolderRow key={`${row.rank}-${row.filed_name}`} row={row} max={max} marketCap={hasValue ? marketCap : null} showValue={hasValue} onOpen={() => { if (row.entity_id) navigate(entityHref(row.entity_id)) }} />)}</tbody>
      </table>
    </div>
    {history && <HistoryTable data={data} />}
    {data.voting.length > 0 && <details className="ow-more">
      <summary>By voting rights: {data.voting.length} largest holders</summary>
      <div className="rs-table-scroll">
        <table className="rs-table ow-table" aria-label="Largest holders by voting rights">
          <thead><tr><th scope="col" className="num">#</th><th scope="col">Shareholder</th><th scope="col" className="num">Share of votes</th><th scope="col" className="num">Voting rights</th></tr></thead>
          <tbody>{data.voting.map(row => <tr key={`${row.rank}-${row.filed_name}`}>
            <td className="num">{row.rank}</td>
            <th scope="row"><span className="ow-name"><KindIcon kind={row.kind} /><EntityLink entity={row} /></span></th>
            <td className="num">{formatStake(row.ratio)}</td>
            <td className="num">{formatShares(row.shares)}</td>
          </tr>)}</tbody>
        </table>
      </div>
    </details>}
    <p className="rs-hint ow-hint">
      Click a shareholder for everything else it holds. Names are in English where the holder files with EDINET or is a well-known institution; hover for the name as filed. A trust or custody bank holds on behalf of funds and pensions, not for itself.
      {' '}<Link to={filingHref(data.doc_id, companyCode, 'analysis')}><FileText aria-hidden="true" />Annual report</Link>
      {' '}<a href={data.edinet_url} target="_blank" rel="noreferrer"><ExternalLink aria-hidden="true" />EDINET</a>
    </p>
  </div>
}

function sliceRatio(slices: DistributionSlice[], keys: string[]) {
  const found = slices.filter(slice => keys.includes(slice.key) && slice.ratio != null)
  return found.length ? found.reduce((total, slice) => total + (slice.ratio ?? 0), 0) : null
}

function HolderRow({ row, max, marketCap, showValue, onOpen }: { row: ShareholderRow; max: number; marketCap: number | null; showValue: boolean; onOpen: () => void }) {
  const change = row.change ?? null
  const tags = [row.own_shares ? 'Own shares' : '', row.account ?? ''].filter(Boolean)
  return <tr tabIndex={row.entity_id ? 0 : undefined} onClick={onOpen} onKeyDown={event => { if (event.key === 'Enter' && event.target === event.currentTarget) onOpen() }}>
    <td className="num">{row.rank}</td>
    <th scope="row">
      <span className="ow-name">
        <KindIcon kind={row.kind} />
        <span className="ow-name__text">
          <EntityLink entity={row} className="ow-name__link" />
          {(tags.length > 0 || row.ticker) && <small title={row.account_ja ? `Filed as ${row.account_ja}` : undefined}>
            {row.ticker && row.edinet_code && <Link className="ow-code" to={`/analyze/${encodeURIComponent(row.edinet_code)}`} onClick={event => event.stopPropagation()} title="Open this company in Analysis">{row.ticker.replace(/^(\d[0-9A-Z]{3})0$/, '$1')}</Link>}
            {tags.map(tag => <span key={tag} className="ow-tag">{tag}</span>)}
          </small>}
        </span>
      </span>
    </th>
    <td className="num ow-stake"><span>{formatStake(row.ratio)}</span><StakeBar ratio={row.ratio} max={max} /></td>
    <td className={`num ${change != null && change > 0.00005 ? 'is-up' : change != null && change < -0.00005 ? 'is-down' : ''}`} title={row.previous_ratio != null ? `${formatStake(row.previous_ratio)} the year before` : undefined}>{row.new ? <span className="ow-tag ow-tag--new">New</span> : change != null ? formatPoints(change) : <span className="rs-dim">—</span>}</td>
    <td className="num" title={exactShares(row.shares)}>{formatShares(row.shares)}</td>
    {showValue && <td className="num">{formatYen(stakeValue(row.ratio, marketCap))}</td>}
    <td>{row.other_holdings > 0 ? `${row.other_holdings.toLocaleString('en-US')} ${row.other_holdings === 1 ? 'company' : 'companies'}` : row.positions > 0 ? <span className="rs-dim">officer</span> : <span className="rs-dim">—</span>}</td>
    <td>{row.since ? fiscalYear(row.since) : <span className="rs-dim">—</span>}</td>
  </tr>
}

/** Ownership by kind of holder, as one bar. */
function Distribution({ slices }: { slices: DistributionSlice[] }) {
  const shown = slices.filter(slice => (slice.ratio ?? 0) > 0)
  const total = shown.reduce((sum, slice) => sum + (slice.ratio ?? 0), 0)
  if (!shown.length || !(total > 0)) return null
  return <figure className="ow-distribution" aria-label="Ownership by kind of holder">
    <div className="ow-distribution__bar" role="img" aria-label={shown.map(slice => `${slice.label} ${formatStake(slice.ratio)}`).join(', ')}>
      {shown.map(slice => <span key={slice.key} className={`ow-slice ow-slice--${slice.key}`} style={{ width: `${((slice.ratio ?? 0) / total) * 100}%` }} title={`${slice.label}: ${formatStake(slice.ratio)}${slice.holders != null ? ` · ${slice.holders.toLocaleString('en-US')} holders` : ''}`} />)}
    </div>
    <figcaption>{shown.map(slice => <span key={slice.key}><i className={`ow-slice ow-slice--${slice.key}`} aria-hidden="true" />{slice.label} <strong>{formatStake(slice.ratio)}</strong>{slice.holders != null && <small>{slice.holders.toLocaleString('en-US')}</small>}</span>)}</figcaption>
  </figure>
}

/** Every holder that has been listed, with its stake in each stored year. */
function HistoryTable({ data }: { data: CompanyShareholders }) {
  const { periods, holders } = data.history
  const max = Math.max(...holders.flatMap(holder => holder.ratios.map(ratio => ratio ?? 0)), 0)
  return <div className="rs-table-scroll ow-table-wrap">
    <table className="rs-table ow-table ow-history" aria-label="Shareholders by year">
      <thead><tr>
        <th scope="col">Shareholder</th>
        {periods.map(period => <th key={period} scope="col" className="num">{fiscalYear(period)}</th>)}
      </tr></thead>
      <tbody>{holders.map(holder => <tr key={holder.entity_id ?? holder.name}>
        <th scope="row"><span className="ow-name"><KindIcon kind={holder.kind} /><EntityLink entity={holder} /></span></th>
        {holder.ratios.map((ratio, index) => <td key={periods[index]} className="num" style={ratio != null && max > 0 ? { background: `rgb(52 96 168 / ${Math.round((ratio / max) * 22) + 2}%)` } : undefined}>{ratio != null ? formatStake(ratio) : <span className="rs-dim">·</span>}</td>)}
      </tr>)}</tbody>
    </table>
    <p className="rs-hint">A holder listed under several accounts in one year shows its combined stake. A dot means the holder was not among the largest that year, not that it sold everything. {KIND_LABELS.organization}s renamed or merged since are shown under the name of the time.</p>
  </div>
}
