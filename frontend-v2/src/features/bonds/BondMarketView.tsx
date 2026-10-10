import { ArrowDown, ArrowUp, Briefcase, Download, Search, Tag, X } from 'lucide-react'
import { useMemo, useRef, useState, type KeyboardEvent } from 'react'
import { useNavigate } from 'react-router-dom'

import { ApiError } from '../../api/client'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { HotkeyKbd } from '../../hotkeys/HotkeyKbd'
import { useHotkeyScope } from '../../hotkeys/useHotkeyScope'
import { useHotkeyText } from '../../hotkeys/useHotkeyText'
import { usePersistentState } from '../../hooks/usePersistentState'
import { downloadTextFile, safeFileName } from '../analysis/downloads'
import { bondMarketScope } from '../research/researchHotkeys'
import { analysisHref, formatDay, formatNumber, formatPercent, isPositionTag } from '../research/researchModel'
import type { ResearchBook } from '../research/researchTypes'
import { moveCursorKey, useListCursor } from '../research/useListCursor'
import { SpreadScatterChart, type SpreadPoint } from './BondCharts'
import { BondDetailPanel } from './BondDetailPanel'
import {
  calculatorHref,
  DEFAULT_FILTERS,
  displayTicker,
  featureTags,
  FEATURE_LABELS,
  filedTitle,
  filterBonds,
  formatBp,
  formatCoupon,
  formatYen,
  marketCsv,
  marketTags,
  matchTag,
  RATING_GROUPS,
  ratingText,
  SENIORITY_LABELS,
  sortBonds,
  termText,
  type MarketFilters,
  type MarketSortKey,
  type MarketTag,
  type RatingGroup,
} from './bondFormat'
import { useBondMarket } from './bondQueries'
import type { MarketBond, Seniority } from './bondTypes'
import './bonds.css'

const PAGE = 200
const SORT_KEYS: readonly MarketSortKey[] = ['company', 'coupon', 'maturity', 'rating', 'spread', 'yield', 'amount']
const COLUMNS: Array<{ key: MarketSortKey; label: string; numeric?: boolean; title?: string }> = [
  { key: 'company', label: 'Issuer and bond' },
  { key: 'coupon', label: 'Coupon', numeric: true },
  { key: 'maturity', label: 'Maturity' },
  { key: 'rating', label: 'Rating' },
  { key: 'amount', label: 'Outstanding', numeric: true },
  { key: 'spread', label: 'Spread', numeric: true, title: 'Over JGBs of the same tenor: from the JSDA reference price where quoted, otherwise at issue (marked i)' },
  { key: 'yield', label: 'Yield', numeric: true, title: 'From the JSDA reference price where quoted; otherwise today’s JGB yield plus the spread at issue (marked *)' },
]
const SENIORITIES: Seniority[] = ['senior', 'secured', 'subordinated', 'hybrid', 'convertible']
const TENORS: Array<{ label: string; min: number | null; max: number | null }> = [
  { label: 'Any term', min: null, max: null },
  { label: 'Up to 3y', min: null, max: 3 },
  { label: '3–7y', min: 3, max: 7 },
  { label: '7–12y', min: 7, max: 12 },
  { label: 'Over 12y', min: 12, max: null },
]

/** ``book`` supplies the user's tags: bonds can be narrowed to the issuers under one or more of them. */
export function BondMarketView({ bondId, issuer, book, onChange, active }: { bondId: string; issuer: string; book?: ResearchBook; onChange: (patch: Record<string, string>) => void; active: boolean }) {
  const navigate = useNavigate()
  const [includeGroup, setIncludeGroup] = usePersistentState('research.bondMarket.group', false)
  const market = useBondMarket(includeGroup)
  const [filters, setFilters] = useState<MarketFilters>(DEFAULT_FILTERS)
  const [includePrivate, setIncludePrivate] = usePersistentState('research.bondMarket.private', false)
  const [yenOnly, setYenOnly] = usePersistentState('research.bondMarket.yenOnly', true)
  // Largest issues first: the bonds most investors can actually find.
  const [sort, setSort] = usePersistentState<MarketSortKey>('research.bondMarket.sort', 'amount', SORT_KEYS)
  const [descending, setDescending] = usePersistentState('research.bondMarket.descending', true)
  const [limit, setLimit] = useState(PAGE)
  const [focusRequest, setFocusRequest] = useState(0)
  const filterRef = useRef<HTMLInputElement>(null)

  const companies = useMemo(() => market.data?.companies ?? {}, [market.data])
  const tagged = useMemo(() => marketTags(book?.companies ?? [], market.data?.bonds ?? []), [book, market.data])
  const rows = useMemo(
    () => sortBonds(filterBonds(market.data?.bonds ?? [], companies, { ...filters, includePrivate, yenOnly, issuer }, tagged.byIssuer), companies, sort, descending),
    [market.data, companies, filters, includePrivate, yenOnly, issuer, tagged, sort, descending],
  )
  const selected = bondId || rows[0]?.bond_id || ''
  const cursor = rows.findIndex(row => row.bond_id === selected)
  const current = rows[cursor] ?? market.data?.bonds.find(row => row.bond_id === selected)
  const body = useListCursor<HTMLTableSectionElement>(cursor, focusRequest)
  const points = useMemo<SpreadPoint[]>(() => rows
    .filter(row => row.spread != null && row.horizon != null)
    .map(row => ({
      id: row.bond_id,
      horizon: row.horizon as number,
      spread: row.spread as number,
      notch: row.rating_notch,
      title: companies[row.edinet_code]?.company_name ?? row.edinet_code,
      detail: `${row.label} · ${ratingText(row)} · ${formatCoupon(row)}`,
    })), [rows, companies])

  const select = (id: string) => onChange({ bond: id })
  const move = (next: number) => { const row = rows[next]; if (row) { select(row.bond_id); if (next >= limit) setLimit(next + PAGE) } }
  const step = (delta: number) => { if (rows.length) { move(Math.max(0, Math.min(rows.length - 1, (cursor < 0 ? -1 : cursor) + delta))); setFocusRequest(value => value + 1) } }
  const ratingFilter = filters.ratings.length === 1 ? filters.ratings[0] : ''
  const cycleRating = (delta: number) => {
    const options: Array<RatingGroup | ''> = ['', ...RATING_GROUPS]
    const next = options[(options.indexOf(ratingFilter) + delta + options.length) % options.length]
    setFilters({ ...filters, ratings: next ? [next] : [] })
  }
  const toggleIssuer = () => onChange({ issuer: issuer ? '' : current?.edinet_code ?? '' })
  const download = () => downloadTextFile(`${safeFileName(['bonds', issuer, ...filters.tags].filter(Boolean).join('-'))}.csv`, marketCsv(rows, companies), 'text/csv;charset=utf-8')
  const focus = (element: HTMLElement | null) => { element?.focus(); element?.scrollIntoView?.({ block: 'center', behavior: 'smooth' }) }

  useHotkeyScope(bondMarketScope, {
    next: () => step(1),
    previous: () => step(-1),
    open: () => { if (current) navigate(analysisHref(current.edinet_code)) },
    filter: () => focus(filterRef.current),
    issuer: toggleIssuer,
    calculator: () => { if (current && current.coupon != null) navigate(calculatorHref(current)) },
    'previous-rating': () => cycleRating(-1),
    'next-rating': () => cycleRating(1),
    download,
  }, { enabled: active })
  const calculatorKey = useHotkeyText(bondMarketScope.byId.calculator)
  const openKey = useHotkeyText(bondMarketScope.byId.open)

  if (market.isLoading) return <LoadingState label="Loading the bond market" />
  if (market.error instanceof ApiError && market.error.status === 503) {
    return <EmptyState title="No bond data yet" description="Run the Update bonds pipeline step: it reads bond terms from EDINET shelf-registration supplements and annual-report bond schedules, and the JGB curve from the Ministry of Finance." />
  }
  if (market.isError || !market.data) return <ErrorState error={market.error} retry={() => void market.refetch()} />

  const toggleSort = (key: MarketSortKey) => {
    if (sort === key) setDescending(!descending)
    else { setSort(key); setDescending(key === 'amount' || key === 'spread' || key === 'yield') }
  }
  const toggleIn = <T,>(list: T[], value: T) => list.includes(value) ? list.filter(item => item !== value) : [...list, value]
  const tenor = TENORS.find(item => item.min === filters.minYears && item.max === filters.maxYears) ?? TENORS[0]
  const onBodyKeyDown = (event: KeyboardEvent<HTMLTableSectionElement>) => {
    if (moveCursorKey(event, cursor, rows.length, move)) return
    if (event.key === 'Enter' && current) { event.preventDefault(); navigate(analysisHref(current.edinet_code)) }
  }
  const issuerName = issuer ? companies[issuer]?.company_name ?? issuer : ''
  const curveDate = market.data.curve?.date
  // A chosen tag stays in the row, so it can be turned off, even when none of its companies has a bond listed any more.
  const tagChips: MarketTag[] = [...tagged.tags, ...filters.tags.filter(name => !tagged.tags.some(tag => tag.name === name)).map(name => ({ name, members: 0, issuers: [] }))]
  // Typing a tag's name points at its chip; Enter then turns that tag on or off.
  const typedTag = matchTag(tagChips, filters.query)
  const toggleTag = (name: string) => { setFilters({ ...filters, query: name === typedTag?.name ? '' : filters.query, tags: toggleIn(filters.tags, name) }); setLimit(PAGE) }
  const tagTitle = (tag: MarketTag) => tag.issuers.length
    ? `${tag.issuers.length} of the ${tag.members} ${tag.members === 1 ? 'company' : 'companies'} tagged “${tag.name}” ${tag.issuers.length === 1 ? 'has' : 'have'} bonds listed: ${tag.issuers.map(code => companies[code]?.company_name ?? code).join(', ')}`
    : `No company tagged “${tag.name}” has bonds listed`

  return <div className="rs-book bd-market">
    <div className="rs-book__list panel">
      <div className="rs-toolbar">
        <label className="rs-search">
          <Search aria-hidden="true" />
          <input
            ref={filterRef}
            className="input"
            value={filters.query}
            placeholder={tagChips.length ? 'Filter by issuer, ticker, bond, or tag' : 'Filter by issuer, ticker, or bond'}
            title={tagChips.length ? 'Type a tag’s name and press Enter to show only its companies’ bonds' : undefined}
            aria-label="Filter bonds"
            onChange={event => { setFilters({ ...filters, query: event.target.value }); setLimit(PAGE) }}
            onKeyDown={event => {
              if (event.key === 'Escape') { setFilters({ ...filters, query: '' }); event.currentTarget.blur() }
              if (event.key === 'ArrowDown') { event.preventDefault(); setFocusRequest(value => value + 1) }
              if (event.key === 'Enter' && typedTag) { event.preventDefault(); toggleTag(typedTag.name) }
            }}
          />
          <span aria-hidden="true"><HotkeyKbd hotkey={bondMarketScope.byId.filter} /></span>
        </label>
        {issuer && <button type="button" className="rs-chip" aria-pressed="true" onClick={() => onChange({ issuer: '' })} title="Show every issuer (I)">{issuerName}<X aria-hidden="true" /></button>}
        <select className="select" aria-label="Industry" value={filters.industry} onChange={event => setFilters({ ...filters, industry: event.target.value })}>
          <option value="">All industries</option>
          {market.data.industries.map(industry => <option key={industry} value={industry}>{industry}</option>)}
        </select>
        <span className="rs-toolbar__spacer" />
        <span className="rs-dim">{rows.length.toLocaleString()} of {market.data.bonds.length.toLocaleString()} bonds{curveDate ? ` · JGB curve ${formatDay(curveDate)}` : ''}</span>
        <button type="button" className="button button--ghost button--small" disabled={!rows.length} onClick={download} title="Download the bonds listed as CSV (D)"><Download aria-hidden="true" />CSV</button>
      </div>
      <div className="rs-filters">
        {tagChips.length > 0 && <div className="rs-chips" role="group" aria-label="Filter by tag">
          <span className="bd-chips-label"><Tag aria-hidden="true" />Tags</span>
          {tagChips.map(tag => <button
            key={tag.name}
            type="button"
            className={['rs-chip', isPositionTag(tag.name) && 'rs-chip--position', tag === typedTag && 'bd-chip--typed'].filter(Boolean).join(' ')}
            aria-pressed={filters.tags.includes(tag.name)}
            title={tagTitle(tag)}
            onClick={() => toggleTag(tag.name)}
          >{isPositionTag(tag.name) && <Briefcase aria-hidden="true" />}{tag.name} <small>{tag.issuers.length}</small>{tag === typedTag && <kbd aria-hidden="true">Enter</kbd>}</button>)}
        </div>}
        <div className="rs-chips" role="group" aria-label="Filter by rating">
          <button type="button" className="rs-chip" aria-pressed={!filters.ratings.length} onClick={() => setFilters({ ...filters, ratings: [] })}>All ratings</button>
          {RATING_GROUPS.map(group => <button key={group} type="button" className="rs-chip" aria-pressed={filters.ratings.includes(group)} onClick={() => setFilters({ ...filters, ratings: toggleIn(filters.ratings, group) })}>{group}</button>)}
          <span aria-hidden="true" title="Previous or next rating group"><HotkeyKbd hotkey={bondMarketScope.byId['previous-rating']} /> <HotkeyKbd hotkey={bondMarketScope.byId['next-rating']} /></span>
        </div>
        <div className="rs-chips" role="group" aria-label="Filter by ranking">
          {SENIORITIES.map(item => <button key={item} type="button" className="rs-chip rs-chip--plain" aria-pressed={filters.seniorities.includes(item)} onClick={() => setFilters({ ...filters, seniorities: toggleIn(filters.seniorities, item) })}>{SENIORITY_LABELS[item]}</button>)}
          <span className="bd-divider" aria-hidden="true" />
          {TENORS.map(item => <button key={item.label} type="button" className="rs-chip rs-chip--plain" aria-pressed={tenor === item} onClick={() => setFilters({ ...filters, minYears: item.min, maxYears: item.max })}>{item.label}</button>)}
        </div>
        <div className="bd-toggles">
          <label className="check"><input type="checkbox" checked={yenOnly} onChange={event => setYenOnly(event.target.checked)} />Yen bonds only</label>
          <label className="check"><input type="checkbox" checked={includePrivate} onChange={event => setIncludePrivate(event.target.checked)} />Private placements</label>
          <label className="check" title="Bonds issued by consolidated subsidiaries, as listed in their parent’s annual report"><input type="checkbox" checked={includeGroup} onChange={event => setIncludeGroup(event.target.checked)} />Subsidiaries’ bonds</label>
        </div>
      </div>
      <div className="bd-scatter">
        <SpreadScatterChart
          points={points}
          selected={selected}
          onSelect={id => { select(id); setFocusRequest(value => value + 1) }}
          caption="Credit curve"
          note={`${points.filter(point => rows.find(row => row.bond_id === point.id)?.spread_basis === 'market').length} at JSDA reference prices, the rest at issue · click a dot to see the bond`}
        />
      </div>
      {!rows.length
        ? <p className="rs-empty">No bonds match. <button type="button" className="text-button" onClick={() => { setFilters(DEFAULT_FILTERS); onChange({ issuer: '' }) }}>Clear filters</button></p>
        : <div className="rs-table-scroll">
          <table className="rs-table bd-table" aria-label="Bonds">
            <thead><tr>
              {COLUMNS.map(column => <th key={column.key} scope="col" className={column.numeric ? 'num' : undefined} aria-sort={sort === column.key ? (descending ? 'descending' : 'ascending') : undefined}>
                <button type="button" className="rs-sort" title={column.title} onClick={() => toggleSort(column.key)}>{column.label}{sort === column.key && (descending ? <ArrowDown aria-hidden="true" /> : <ArrowUp aria-hidden="true" />)}</button>
              </th>)}
            </tr></thead>
            <tbody ref={body} onKeyDown={onBodyKeyDown}>
              {rows.slice(0, limit).map((row, index) => <MarketRow key={row.bond_id} bond={row} company={companies[row.edinet_code]?.company_name ?? row.edinet_code} companyFiled={companies[row.edinet_code]?.company_name_ja} ticker={companies[row.edinet_code]?.ticker} isCursor={index === cursor} onSelect={() => select(row.bond_id)} />)}
            </tbody>
          </table>
          {rows.length > limit && <button type="button" className="text-button rs-more" onClick={() => setLimit(limit + PAGE)}>Show {Math.min(PAGE, rows.length - limit)} more of {rows.length - limit}</button>}
        </div>}
      <p className="rs-hint bd-hint">Yields and spreads come from JSDA OTC reference prices{market.data.bonds.some(bond => bond.market_date) ? ` (${formatDay(market.data.bonds.find(bond => bond.market_date)?.market_date ?? '')})` : ''} where a bond is quoted. Otherwise the spread is the one at issue (i) and the yield adds it to today’s JGB curve (*): an estimate, not a traded price. Callable bonds run to their first call. Ratings marked * are the issuer’s latest for that ranking.</p>
    </div>
    <aside className="rs-book__detail panel" aria-label="Bond">
      <BondDetailPanel bondId={selected} onSelect={id => select(id)} keys={{ calculator: calculatorKey, open: openKey }} />
    </aside>
  </div>
}

/** Names are in English; hovering one shows it as the filing writes it. */
function MarketRow({ bond, company, companyFiled, ticker, isCursor, onSelect }: { bond: MarketBond; company: string; companyFiled?: string | null; ticker?: string; isCursor: boolean; onSelect: () => void }) {
  const tags = featureTags(bond)
  const code = displayTicker(ticker)
  const badges = [SENIORITY_LABELS[bond.seniority], bond.currency && bond.currency !== 'JPY' ? bond.currency : '', ...tags.map(tag => FEATURE_LABELS[tag]?.split(' (')[0] ?? tag)].filter(Boolean)
  return <tr data-cursor={isCursor} className={isCursor ? 'is-cursor' : undefined} tabIndex={isCursor ? 0 : -1} aria-selected={isCursor} onClick={onSelect} onFocus={() => { if (!isCursor) onSelect() }}>
    <th scope="row">
      <span className="bd-bond-cell">
        <strong title={filedTitle(company, companyFiled)}>{company}{code && <span className="bd-ticker">{code}</span>}</strong>
        <small title={filedTitle(bond.label, bond.label_ja)}><span className="bd-label">{bond.label}</span>{badges.map(text => <span key={text} className="bd-tag">{text}</span>)}</small>
      </span>
    </th>
    <td className="num">{formatCoupon(bond)}</td>
    <td>{bond.maturity ? formatDay(bond.maturity) : bond.perpetual ? 'Perpetual' : '—'}<small className="bd-sub">{termText(bond)}</small></td>
    <td title={ratingText(bond)}>{bond.rating ? `${bond.rating}${bond.rating_inferred ? ' *' : ''}` : <span className="rs-dim">NR</span>}</td>
    <td className="num">{bond.outstanding == null ? <span className="rs-dim">—</span> : formatYen(bond.outstanding)}</td>
    <td className="num" title={bond.spread_basis === 'issue' ? 'At issue: no JSDA quote' : bond.market_date ? `JSDA ${bond.market_date}; ${formatBp(bond.issue_spread)} at issue` : undefined}>{formatBp(bond.spread)}{bond.spread_basis === 'issue' && <sup className="bd-basis">i</sup>}</td>
    <td className="num" title={bond.market_price != null ? `JSDA price ${formatNumber(bond.market_price, 2)}` : bond.model_price != null ? `Estimated price ${formatNumber(bond.model_price, 2)}` : undefined}>{bond.market_yield != null ? formatPercent(bond.market_yield, 2) : <>{formatPercent(bond.model_yield, 2)}{bond.model_yield != null && <sup className="bd-basis">*</sup>}</>}</td>
  </tr>
}
