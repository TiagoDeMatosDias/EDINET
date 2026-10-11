import { Download, ExternalLink, FileText } from 'lucide-react'
import { useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'

import { ApiError } from '../../api/client'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { Tip } from '../../components/Tooltip'
import { HotkeyKbd } from '../../hotkeys/HotkeyKbd'
import { useHotkeyScope } from '../../hotkeys/useHotkeyScope'
import { peoplePanelScope } from '../analysis/analysisHotkeys'
import { downloadTextFile } from '../analysis/downloads'
import { filingHref } from '../filings/filingFormat'
import { formatDay } from '../research/researchModel'
import { entityHref, exactShares, fiscalYear, formatShares, formatStake, formatYen, officersCsv, stakeValue, termText } from './ownershipFormat'
import { EntityLink, Kpi } from './ownershipParts'
import { useCompanyOwnership } from './ownershipQueries'
import type { CompanyPeople, OfficerRow } from './ownershipTypes'
import '../research/research.css'
import './ownership.css'

const NO_DATA = 'An administrator can run the Update ownership pipeline step to read shareholders and officers from the stored annual reports.'

/** The directors, auditors, and executive officers in the company's latest annual report. */
export function PeoplePanel({ companyCode, marketCap }: { companyCode: string; marketCap: number | null }) {
  const query = useCompanyOwnership(companyCode)
  const [showDeparted, setShowDeparted] = useState(false)
  const data = query.data?.people ?? null
  useHotkeyScope(peoplePanelScope, { departed: () => setShowDeparted(value => !value) }, { enabled: Boolean(data?.departed.length) })

  if (query.isLoading) return <LoadingState label="Loading officers" />
  if (query.error instanceof ApiError && query.error.status === 503) return <EmptyState title="No ownership data yet" description={NO_DATA} />
  if (query.isError) return <ErrorState error={query.error} retry={() => void query.refetch()} />
  if (!data) {
    return <p className="ow-none">No officers have been read for this company{query.data?.reports ? ': its annual reports state none in a form that can be read' : ' (none of its annual reports is in the filing catalog, or the Update ownership step has not run since they were stored)'}.</p>
  }
  return <PeopleContent data={data} companyCode={companyCode} marketCap={marketCap} showDeparted={showDeparted} onDeparted={() => setShowDeparted(!showDeparted)} />
}

function PeopleContent({ data, companyCode, marketCap, showDeparted, onDeparted }: { data: CompanyPeople; companyCode: string; marketCap: number | null; showDeparted: boolean; onDeparted: () => void }) {
  const navigate = useNavigate()
  const rows = data.rows
  const counted = data.men != null || data.women != null
  const headcount = counted ? (data.men ?? 0) + (data.women ?? 0) : rows.length
  const hasShares = rows.some(row => row.shares != null)
  const hasValue = marketCap != null && rows.some(row => row.ratio != null)
  const hasTerm = rows.some(row => termText(row))
  const hasSince = rows.some(row => row.since)
  const download = () => downloadTextFile(`${companyCode}-officers-${data.period_end}.csv`, officersCsv(rows, marketCap), 'text/csv;charset=utf-8')

  return <div className="ow-company">
    <div className="ow-actions">
      <span className="muted">
        {rows.length} officers {data.proposed ? 'on the slate proposed to the shareholders’ meeting' : `at ${formatDay(data.as_of)}`} · annual report for {fiscalYear(data.period_end)}{data.proposed && data.as_of ? `, filed ${formatDay(data.as_of)}` : ''}
      </span>
      <span className="statement-toolbar__spacer" />
      <button type="button" className="button button--ghost button--small" onClick={download} title="Download the officers as CSV"><Download aria-hidden="true" />CSV</button>
    </div>
    <div className="ow-kpis">
      <Kpi label="Officers" value={headcount.toLocaleString('en-US')} detail={counted ? `${data.men ?? 0} men, ${data.women ?? 0} women` : 'directors and auditors'} tip="Directors, auditors, and executive officers the report lists; men and women as the report counts them." />
      <Kpi label="Outside officers" value={String(data.outside)} detail={rows.length ? `${Math.round((data.outside / rows.length) * 100)}% of those listed` : undefined} tip="Outside directors and outside auditors: named so in their title or in the report’s notes to the table." />
      {data.average_age != null && <Kpi label="Average age" value={data.average_age.toFixed(1)} detail={ageRange(rows)} />}
      {data.total_shares != null && <Kpi label="Shares held by officers" value={formatStake(data.total_ratio)} detail={`${formatShares(data.total_shares)} shares${hasValue && data.total_ratio != null ? ` · ${formatYen(stakeValue(data.total_ratio, marketCap))}` : ''}`} tip="Ordinary shares the officers hold in the company, as the report states for each of them, over the shares issued at the fiscal year end." />}
    </div>
    <div className="rs-table-scroll ow-table-wrap">
      <table className="rs-table ow-table" aria-label="Related people">
        <thead><tr>
          <th scope="col">Name</th>
          <th scope="col">Title</th>
          <th scope="col" className="num">Age</th>
          {hasSince && <th scope="col"><Tip content="The first stored annual report that lists the person as an officer of this company. Blank where that is the oldest report stored.">Listed since</Tip></th>}
          {hasShares && <th scope="col" className="num">Shares held</th>}
          {hasShares && <th scope="col" className="num">Stake</th>}
          {hasValue && <th scope="col" className="num"><Tip content="The stake times the company’s market capitalisation today.">Value</Tip></th>}
          {hasTerm && <th scope="col"><Tip content="The term ends at the close of the shareholders’ meeting held for this fiscal year.">Term ends</Tip></th>}
          <th scope="col"><Tip content="Other companies whose latest report lists the same person (same name and date of birth) as an officer.">Other boards</Tip></th>
        </tr></thead>
        <tbody>{rows.map((row, index) => <OfficerLine key={`${row.entity_id}-${index}`} row={row} marketCap={hasValue ? marketCap : null} columns={{ hasShares, hasValue, hasTerm, hasSince }} onOpen={() => { if (row.entity_id) navigate(entityHref(row.entity_id)) }} />)}</tbody>
      </table>
    </div>
    {data.departed.length > 0 && <p className="rs-hint ow-hint">
      <button type="button" className="text-button" aria-expanded={showDeparted} onClick={onDeparted}>{showDeparted ? 'Hide' : 'Show'} {data.departed.length} who left{data.previous_period ? ` since ${fiscalYear(data.previous_period)}` : ''}</button> <span aria-hidden="true"><HotkeyKbd hotkey={peoplePanelScope.byId.departed} /></span>
    </p>}
    {showDeparted && data.departed.length > 0 && <ul className="ow-departed" aria-label="Officers who left">
      {data.departed.map((person, index) => <li key={`${person.entity_id}-${index}`}><EntityLink entity={person} /><span className="rs-dim" title={person.title_ja ? `Filed as ${person.title_ja}` : undefined}>{person.title}</span></li>)}
    </ul>}
    <p className="rs-hint ow-hint">
      Click a person for their career and the other companies they serve or hold. Names are the company’s own English spelling, and titles are rewritten from the roles they state; hover for either as filed.
      {data.proposed && ' The report was filed before the shareholders’ meeting, so the board shown is the one proposed to it.'}
      {data.source === 'table' && ' This report does not tag each officer, so the table was read as text and has no English names.'}
      {' '}<Link to={filingHref(data.doc_id, companyCode, 'analysis')}><FileText aria-hidden="true" />Annual report</Link>
      {' '}<a href={data.edinet_url} target="_blank" rel="noreferrer"><ExternalLink aria-hidden="true" />EDINET</a>
    </p>
  </div>
}

function ageRange(rows: OfficerRow[]) {
  const ages = rows.map(row => row.age).filter((value): value is number => value != null)
  return ages.length ? `${Math.min(...ages)} to ${Math.max(...ages)}` : undefined
}

function OfficerLine({ row, marketCap, columns, onOpen }: { row: OfficerRow; marketCap: number | null; columns: { hasShares: boolean; hasValue: boolean; hasTerm: boolean; hasSince: boolean }; onOpen: () => void }) {
  const badges = [row.representative ? 'Representative' : '', row.outside ? 'Outside' : ''].filter(Boolean)
  return <tr tabIndex={row.entity_id ? 0 : undefined} onClick={onOpen} onKeyDown={event => { if (event.key === 'Enter' && event.target === event.currentTarget) onOpen() }}>
    <th scope="row">
      <span className="ow-name__text">
        <EntityLink entity={row} className="ow-name__link" />
        {badges.length > 0 && <small>{badges.map(badge => <span key={badge} className="ow-tag">{badge}</span>)}</small>}
      </span>
    </th>
    <td className="ow-title" title={row.title_ja ? `Filed as ${row.title_ja}` : undefined}>{row.title || <span className="rs-dim">—</span>}</td>
    <td className="num" title={row.birth_date ? `Born ${formatDay(row.birth_date)}` : undefined}>{row.age ?? <span className="rs-dim">—</span>}</td>
    {columns.hasSince && <td>{row.since ? fiscalYear(row.since) : <span className="rs-dim">—</span>}</td>}
    {columns.hasShares && <td className="num" title={exactShares(row.shares)}>{row.shares == null ? <span className="rs-dim">—</span> : formatShares(row.shares)}</td>}
    {columns.hasShares && <td className="num">{row.ratio == null ? <span className="rs-dim">—</span> : formatStake(row.ratio)}</td>}
    {columns.hasValue && <td className="num">{row.ratio == null ? <span className="rs-dim">—</span> : formatYen(stakeValue(row.ratio, marketCap))}</td>}
    {columns.hasTerm && <td title={row.term ?? undefined}>{termText(row) || <span className="rs-dim">—</span>}</td>}
    <td>{row.other_positions > 0 ? `${row.other_positions} ${row.other_positions === 1 ? 'company' : 'companies'}` : <span className="rs-dim">—</span>}</td>
  </tr>
}
