import { BarChart3, FileText, Users } from 'lucide-react'
import { useMemo, useState, type ReactNode } from 'react'
import { Link, useNavigate } from 'react-router-dom'

import { ErrorState, LoadingState } from '../../components/Feedback'
import { Tip } from '../../components/Tooltip'
import { displayTicker } from '../bonds/bondFormat'
import { filingHref } from '../filings/filingFormat'
import { analysisHref, formatDay } from '../research/researchModel'
import { CareerList } from './CareerList'
import { entityHref, entityMeta, exactShares, filedName, fiscalYear, formatPoints, formatShares, formatStake, formatYen, KIND_HINTS, KIND_LABELS, ownershipHref, stakeValue, VALUED_STAKES, valuedCodes } from './ownershipFormat'
import { KindIcon } from './ownershipParts'
import { useEntity, useMarketCaps } from './ownershipQueries'
import type { EntityDetail, EntityHolding, EntityPosition, EntitySummary, MarketCaps } from './ownershipTypes'

const PAGE = 60

export function EntityDetailPanel({ entityId, onSelect, keys }: { entityId: string; onSelect: (entityId: string) => void; keys?: { open?: string } }) {
  const detail = useEntity(entityId)
  if (!entityId) return <p className="rs-empty">Choose a person or holder to see the companies they hold or serve, the value of each stake, and their career.</p>
  if (detail.isLoading) return <LoadingState label="Loading" />
  if (detail.isError || !detail.data) return <ErrorState error={detail.error} retry={() => void detail.refetch()} />
  return <EntityDetailContent key={entityId} data={detail.data} onSelect={onSelect} keys={keys} />
}

function Fact({ label, children, tip }: { label: string; children: ReactNode; tip?: string }) {
  return <div><dt>{tip ? <Tip content={tip}>{label}</Tip> : label}</dt><dd>{children}</dd></div>
}

function EntityDetailContent({ data, onSelect, keys }: { data: EntityDetail; onSelect: (entityId: string) => void; keys?: { open?: string } }) {
  const { entity, totals } = data
  const codes = useMemo(() => valuedCodes(data), [data])
  const caps = useMarketCaps(codes)
  const values = caps.data ?? {}
  const ownCode = entity.kind === 'company' && entity.edinet_code ? entity.edinet_code : ''
  const filed = entity.name_ja && entity.name_ja !== entity.name ? entity.name_ja : ''
  const born = entity.birth_date ? `Born ${formatDay(entity.birth_date)}${entity.age != null ? ` (age ${entity.age})` : ''}` : ''
  const current = data.holdings.filter(item => item.current)
  const stakes = current.map(item => stakeValue(item.ratio, values[item.edinet_code])).filter((value): value is number => value != null)
  const officerStakes = data.positions.filter(item => item.current).map(item => stakeValue(item.ratio, values[item.edinet_code])).filter((value): value is number => value != null)
  const sum = (list: number[]) => list.reduce((total, value) => total + value, 0)
  const pending = caps.isLoading && codes.length > 0
  const valueTip = `Each stake times the company’s market capitalisation today, for listed companies${current.length > VALUED_STAKES ? `; the ${VALUED_STAKES} largest stakes are valued` : ''}. An estimate from the shares held at each record date.`
  return <div className="ow-detail">
    <header className="rs-detail__header">
      <div>
        <h2 title={filedName(entity)}>{ownCode && entity.has_reports ? <Link to={analysisHref(ownCode)} title={`Open in Analysis${keys?.open ? ` (${keys.open})` : ''}`}>{entity.name}</Link> : entity.name}</h2>
        <p className="rs-detail__meta">
          <span className="ow-kind-label" title={KIND_HINTS[entity.kind]}><KindIcon kind={entity.kind} />{KIND_LABELS[entity.kind]}</span>
          {filed && <span lang="ja">{filed}</span>}
          {entity.ticker && <span className="mono">{displayTicker(entity.ticker)}</span>}
          {entity.industry && <span>{entity.industry}</span>}
        </p>
        {(born || (entity.kind !== 'person' && entity.address)) && <p className="rs-detail__meta">{[born, entity.kind !== 'person' && entity.address ? entity.address : ''].filter(Boolean).join(' · ')}</p>}
      </div>
      {ownCode && entity.has_reports && <nav className="rs-detail__links" aria-label="Open this company">
        <Link to={analysisHref(ownCode)} title="Statements, prices, and filings"><BarChart3 aria-hidden="true" />Analysis</Link>
        <Link to={ownershipHref(ownCode)} title="Who holds this company"><Users aria-hidden="true" />Its shareholders</Link>
      </nav>}
    </header>

    <dl className="rs-summary rs-summary--grid ow-facts">
      {data.holdings.length > 0 && <Fact label="Holdings" tip="Companies whose latest annual report lists this holder among their largest shareholders.">{totals.holdings.toLocaleString('en-US')}{totals.past_holdings > 0 && <small> · {totals.past_holdings.toLocaleString('en-US')} earlier</small>}</Fact>}
      {current.length > 0 && <Fact label="Value of stakes" tip={valueTip}>{stakes.length ? formatYen(sum(stakes)) : pending ? <span className="rs-dim">valuing…</span> : '—'}{stakes.length > 0 && stakes.length < current.length && <small> · {stakes.length} of {current.length.toLocaleString('en-US')}</small>}</Fact>}
      {data.positions.length > 0 && <Fact label="Boards" tip="Companies whose latest annual report lists the person as a director, auditor, or executive officer.">{totals.positions}{totals.past_positions > 0 && <small> · {totals.past_positions} earlier</small>}</Fact>}
      {data.positions.some(item => item.current && item.shares) && <Fact label="Shares held as officer" tip="The shares each company reports the officer holds, at today’s market value.">{officerStakes.length ? formatYen(sum(officerStakes)) : pending ? <span className="rs-dim">valuing…</span> : '—'}</Fact>}
    </dl>
    {entity.kind === 'individual' && <p className="rs-market-read">Identified by name only. A shareholder list gives a name and an address but no date of birth, so two people with the same name would appear here together{totals.addresses > 1 ? `; these rows were filed under ${totals.addresses} different addresses` : ''}.</p>}
    {entity.kind === 'organization' && totals.holdings > 300 && <p className="rs-market-read">A custody or trust bank is the holder of record for funds and pensions: these stakes belong to its clients, and the same client may appear under other banks too.</p>}

    {data.positions.length > 0 && <PositionsSection positions={data.positions} caps={values} />}
    {data.holdings.length > 0 && <HoldingsSection holdings={data.holdings} caps={values} showAddress={entity.kind === 'individual'} />}
    {data.careers.length > 0 && <CareerList careers={data.careers} />}
    {data.same_name.length > 0 && <SameName entities={data.same_name} kind={entity.kind} onSelect={onSelect} />}
    {!data.positions.length && !data.holdings.length && <p className="rs-empty">No stored annual report lists this name any more.</p>}
  </div>
}

function CompanyCell({ row, section, children }: { row: EntityHolding | EntityPosition; section: 'shareholders' | 'people'; children?: ReactNode }) {
  return <span className="ow-name__text">
    <Link className="ow-name__link" to={ownershipHref(row.edinet_code, section)} title={row.company_name_ja ? `${row.company_name} · filed as ${row.company_name_ja}` : undefined} onClick={event => event.stopPropagation()}>{row.company_name}</Link>
    <small>{[displayTicker(row.ticker), row.industry].filter(Boolean).join(' · ')}{children}</small>
  </span>
}

/** A stake's history as a line: one point per stored year. */
function Sparkline({ history }: { history: EntityHolding['history'] }) {
  const points = history.map(([, value], index) => ({ value, index })).filter((point): point is { value: number; index: number } => point.value != null)
  if (points.length < 2) return null
  const [low, high] = [Math.min(...points.map(point => point.value)), Math.max(...points.map(point => point.value))]
  const [width, height] = [56, 16]
  const x = (index: number) => (index / (history.length - 1)) * (width - 2) + 1
  const y = (value: number) => high > low ? height - 2 - ((value - low) / (high - low)) * (height - 4) : height / 2
  const label = `${fiscalYear(history[0][0])} ${formatStake(points[0].value)} to ${fiscalYear(history[history.length - 1][0])} ${formatStake(points[points.length - 1].value)}`
  return <svg className="ow-spark" width={width} height={height} viewBox={`0 0 ${width} ${height}`} role="img" aria-label={label}>
    <title>{label}</title>
    <polyline fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinejoin="round" strokeLinecap="round" points={points.map(point => `${x(point.index).toFixed(1)},${y(point.value).toFixed(1)}`).join(' ')} />
  </svg>
}

function HoldingsSection({ holdings, caps, showAddress }: { holdings: EntityHolding[]; caps: MarketCaps; showAddress: boolean }) {
  const navigate = useNavigate()
  const [limit, setLimit] = useState(PAGE)
  const [showPast, setShowPast] = useState(false)
  const current = holdings.filter(item => item.current)
  const past = holdings.length - current.length
  const rows = showPast || !current.length ? holdings : current
  const hasValue = rows.some(item => item.current && item.listed)
  return <section className="ow-section" aria-label="Shareholdings">
    <h3>Shareholdings <small>{current.length ? `${current.length.toLocaleString('en-US')} in companies’ latest reports` : 'none in a latest report'}</small></h3>
    <div className="rs-table-scroll">
      <table className="rs-table ow-table ow-table--compact" aria-label="Shareholdings">
        <thead><tr>
          <th scope="col">Company</th>
          <th scope="col" className="num">Stake</th>
          <th scope="col" className="num"><Tip content="Change in the stake since the previous year’s report, in percentage points, and the stake in every stored year.">Change</Tip></th>
          {hasValue && <th scope="col" className="num"><Tip content="The stake times the company’s market capitalisation today.">Value</Tip></th>}
          <th scope="col">As of</th>
        </tr></thead>
        <tbody>{rows.slice(0, limit).map(row => <tr key={row.edinet_code} className={row.current ? undefined : 'is-inactive'} tabIndex={0} onClick={() => navigate(ownershipHref(row.edinet_code))} onKeyDown={event => { if (event.key === 'Enter' && event.target === event.currentTarget) navigate(ownershipHref(row.edinet_code)) }}>
          <th scope="row"><CompanyCell row={row} section="shareholders">{row.account && <span className="ow-tag" title={row.account_ja ? `Filed as ${row.account_ja}` : undefined}>{row.account}</span>}{showAddress && row.address && <span lang="ja">{row.address}</span>}</CompanyCell></th>
          <td className="num" title={exactShares(row.shares)}>{formatStake(row.ratio)}<small className="ow-sub">{formatShares(row.shares)} · No. {row.rank}</small></td>
          <td className={`num ${row.change != null && row.change > 0.00005 ? 'is-up' : row.change != null && row.change < -0.00005 ? 'is-down' : ''}`}>{row.change != null ? formatPoints(row.change) : <span className="rs-dim">—</span>}<Sparkline history={row.history} /></td>
          {hasValue && <td className="num">{row.current && caps[row.edinet_code] != null ? formatYen(stakeValue(row.ratio, caps[row.edinet_code])) : <span className="rs-dim">—</span>}</td>}
          <td title={`Listed since ${fiscalYear(row.first_period)}`}>{row.current ? formatDay(row.as_of) : <span title={`Last listed in the report for ${fiscalYear(row.period_end)}`}>until {fiscalYear(row.period_end)}</span>}</td>
        </tr>)}</tbody>
      </table>
    </div>
    <p className="rs-hint">
      {rows.length > limit && <><button type="button" className="text-button" onClick={() => setLimit(limit + PAGE * 4)}>Show {Math.min(PAGE * 4, rows.length - limit)} more of {(rows.length - limit).toLocaleString('en-US')}</button> · </>}
      {past > 0 && current.length > 0 && <><button type="button" className="text-button" aria-expanded={showPast} onClick={() => setShowPast(!showPast)}>{showPast ? 'Hide' : 'Show'} {past.toLocaleString('en-US')} earlier {past === 1 ? 'holding' : 'holdings'}</button> · </>}
      A company’s report lists only its ten or so largest shareholders, so a stake shown as ended may only have fallen below that line.
    </p>
  </section>
}

function PositionsSection({ positions, caps }: { positions: EntityPosition[]; caps: MarketCaps }) {
  const navigate = useNavigate()
  const hasShares = positions.some(item => item.shares != null)
  return <section className="ow-section" aria-label="Boards and offices">
    <h3>Boards and offices <small>{positions.filter(item => item.current).length} current</small></h3>
    <div className="rs-table-scroll">
      <table className="rs-table ow-table ow-table--compact" aria-label="Boards and offices">
        <thead><tr>
          <th scope="col">Company</th>
          <th scope="col">Title</th>
          <th scope="col"><Tip content="The first and the latest stored annual report that lists the person as an officer of the company.">Reports</Tip></th>
          {hasShares && <th scope="col" className="num"><Tip content="Shares the company reports the officer holds, with their value at today’s market capitalisation.">Shares held</Tip></th>}
          <th scope="col">Source</th>
        </tr></thead>
        <tbody>{positions.map(row => {
          const value = row.current ? stakeValue(row.ratio, caps[row.edinet_code]) : null
          return <tr key={row.edinet_code} className={row.current ? undefined : 'is-inactive'} tabIndex={0} onClick={() => navigate(ownershipHref(row.edinet_code, 'people'))} onKeyDown={event => { if (event.key === 'Enter' && event.target === event.currentTarget) navigate(ownershipHref(row.edinet_code, 'people')) }}>
            <th scope="row"><CompanyCell row={row} section="people" /></th>
            <td className="ow-title" title={[row.title_ja ? `Filed as ${row.title_ja}` : '', row.titles.length > 1 ? row.titles.map(item => `${fiscalYear(item.period_end)}: ${item.title}`).join('\n') : ''].filter(Boolean).join('\n') || undefined}>
              {row.title || '—'}
              {(row.outside || row.representative) && <small className="ow-sub">{[row.representative ? 'Representative' : '', row.outside ? 'Outside' : ''].filter(Boolean).join(' · ')}</small>}
            </td>
            <td>{fiscalYear(row.first_period)}{row.first_period !== row.period_end ? `–${fiscalYear(row.period_end).replace('FY', '')}` : ''}{!row.current && <small className="ow-sub">no longer listed</small>}</td>
            {hasShares && <td className="num" title={exactShares(row.shares)}>{row.shares == null ? <span className="rs-dim">—</span> : <>{formatShares(row.shares)}<small className="ow-sub">{[formatStake(row.ratio), value != null ? formatYen(value) : ''].filter(Boolean).join(' · ')}</small></>}</td>}
            <td className="ow-links" onClick={event => event.stopPropagation()}><Link to={filingHref(row.doc_id, row.edinet_code, 'research')} title={`Annual report for ${fiscalYear(row.period_end)} in the Filing Explorer`}><FileText aria-hidden="true" />Report</Link></td>
          </tr>
        })}</tbody>
      </table>
    </div>
  </section>
}

function SameName({ entities, kind, onSelect }: { entities: EntitySummary[]; kind: EntitySummary['kind']; onSelect: (entityId: string) => void }) {
  return <section className="ow-section" aria-label="Others filed under the same name">
    <h3>Filed under the same name</h3>
    <p className="rs-hint">{kind === 'person'
      ? 'A shareholder list gives no date of birth, and an officer elsewhere may only share the name: these cannot be confirmed as the same person.'
      : 'Officers with this name, each identified by their own date of birth. The filings cannot say whether one of them is this shareholder.'}</p>
    <ul className="ow-same">
      {entities.map(item => <li key={item.entity_id}>
        <Link to={entityHref(item.entity_id)} onClick={event => { event.preventDefault(); onSelect(item.entity_id) }}>{item.name}</Link>
        <span className="rs-dim">{[KIND_LABELS[item.kind], item.birth_date ? `born ${item.birth_date.slice(0, 4)}` : '', item.headline ?? '', entityMeta(item)].filter(Boolean).join(' · ')}</span>
      </li>)}
    </ul>
  </section>
}
