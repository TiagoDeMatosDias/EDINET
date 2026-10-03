import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ArrowDown, ArrowUp, Bell, Briefcase, Download, GitCompare, Search } from 'lucide-react'
import { useMemo, useRef, useState, type KeyboardEvent } from 'react'
import { Link, useNavigate } from 'react-router-dom'

import { apiRequest } from '../../api/client'
import type { SecuritySearchResult } from '../../api/types'
import { CompanyPicker, searchCompanies } from '../../components/CompanyPicker'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { useHotkeys } from '../../hooks/useHotkeys'
import { usePersistentState } from '../../hooks/usePersistentState'
import { formatMetricValue } from '../../metrics'
import { downloadTextFile } from '../analysis/downloads'
import { CompanyResearchPanel } from './CompanyResearchPanel'
import { ConfirmButton } from './ConfirmButton'
import { invalidateResearch, PRICE_FORMAT } from './researchQueries'
import { analysisHref, bookCsv, filterBook, formatDay, formatPercent, formatSignedPercent, isEdinetCode, isPositionTag, relativeDay, reviewState, sortBook, statusLabel, upside, type BookSortKey } from './researchModel'
import type { BookCompany, ResearchBook } from './researchTypes'
import { moveCursorKey, useListCursor } from './useListCursor'

const STATUS_FILTERS = [
  { key: '', label: 'All' },
  { key: 'buy', label: 'Buy' },
  { key: 'hold', label: 'Hold' },
  { key: 'watch', label: 'Watch' },
  { key: 'sell', label: 'Sell' },
  { key: 'review', label: 'Review due' },
  { key: 'alerts', label: 'Alert triggered' },
]

// ``wide`` columns give way on mid-width screens, where the research panel sits beside the list.
const COLUMNS: Array<{ key: BookSortKey; label: string; numeric?: boolean; wide?: boolean; title?: string }> = [
  { key: 'name', label: 'Company' },
  { key: 'status', label: 'Status' },
  { key: 'price', label: 'Price', numeric: true },
  { key: 'upside', label: 'To target', numeric: true, title: 'Target price against the latest price' },
  { key: 'review', label: 'Review' },
  { key: 'pe', label: 'P/E', numeric: true, wide: true },
  { key: 'yield', label: 'Yield', numeric: true },
  { key: 'updated', label: 'Activity', wide: true, title: 'Last tag, note, thesis, or alert change' },
]

const SORT_KEYS = COLUMNS.map(column => column.key)

export function BookView({ book, loading, error, retry, selectedCode, onSelect, tag, status, onFilter, active, today }: {
  book?: ResearchBook
  loading: boolean
  error: unknown
  retry: () => void
  selectedCode: string
  onSelect: (code: string) => void
  tag: string
  status: string
  onFilter: (next: { tag?: string; status?: string }) => void
  active: boolean
  today: string
}) {
  const navigate = useNavigate()
  const client = useQueryClient()
  const [query, setQuery] = useState('')
  const [sort, setSort] = usePersistentState<BookSortKey>('research.book.sort', 'updated', SORT_KEYS)
  const [descending, setDescending] = usePersistentState('research.book.descending', true)
  const [focusRequest, setFocusRequest] = useState(0)
  const [picked, setPicked] = useState<SecuritySearchResult | null>(null)
  const pickerRef = useRef<HTMLInputElement>(null)
  const filterRef = useRef<HTMLInputElement>(null)
  const tagRef = useRef<HTMLInputElement>(null)
  const noteRef = useRef<HTMLTextAreaElement>(null)
  const thesisRef = useRef<HTMLTextAreaElement>(null)
  const statusRef = useRef<HTMLDivElement>(null)

  const companies = useMemo(() => book?.companies ?? [], [book])
  const rows = useMemo(() => sortBook(filterBook(companies, { tag, status, query }, today), sort, descending), [companies, tag, status, query, today, sort, descending])
  const code = selectedCode || rows[0]?.company_code || ''
  const cursor = rows.findIndex(row => row.company_code === code)
  const current = companies.find(company => company.company_code === code)
  const body = useListCursor<HTMLTableSectionElement>(cursor, focusRequest)
  const lookup = useQuery({
    queryKey: ['company-lookup', code],
    enabled: Boolean(code) && !current && !(picked?.company_code === code),
    queryFn: () => searchCompanies(code, 1),
    staleTime: Infinity,
  })
  const outside = current ? null : picked?.company_code === code ? picked : lookup.data?.results.find(item => item.company_code === code) ?? null

  const move = (next: number) => { const row = rows[next]; if (row) onSelect(row.company_code) }
  const step = (delta: number) => { if (rows.length) { move(Math.max(0, Math.min(rows.length - 1, (cursor < 0 ? -1 : cursor) + delta))); setFocusRequest(value => value + 1) } }
  // Position tags lead, as the portfolio's own grouping; then the user's tags by name.
  const tags = [...(book?.tags ?? []).filter(item => isPositionTag(item.name)), ...(book?.tags ?? []).filter(item => !isPositionTag(item.name))]
  const cycleTag = (delta: number) => {
    const names = ['', ...tags.map(item => item.name)]
    const index = names.indexOf(tag)
    onFilter({ tag: names[(index + delta + names.length) % names.length] })
  }
  // Comparison works on EDINET companies, so other holdings are left out.
  const compareCodes = rows.filter(row => isEdinetCode(row.company_code)).slice(0, 12).map(row => row.company_code)
  const compare = () => { if (compareCodes.length >= 2) navigate(`/compare?companies=${compareCodes.map(encodeURIComponent).join(',')}`) }
  const download = () => downloadTextFile(`research-${tag || status || 'companies'}.csv`.replace(/\s+/g, '-').toLowerCase(), bookCsv(rows), 'text/csv;charset=utf-8')
  const focus = (element: HTMLElement | null) => { element?.focus(); element?.scrollIntoView?.({ block: 'center', behavior: 'smooth' }) }

  useHotkeys({
    j: () => step(1),
    k: () => step(-1),
    o: () => { if (code) navigate(analysisHref(code)) },
    a: () => focus(pickerRef.current),
    f: () => focus(filterRef.current),
    '[': () => cycleTag(-1),
    ']': () => cycleTag(1),
    s: () => focus(statusRef.current?.querySelector<HTMLButtonElement>('[aria-pressed="true"]') ?? statusRef.current?.querySelector<HTMLButtonElement>('button') ?? null),
    e: () => focus(thesisRef.current),
    t: () => focus(tagRef.current),
    n: () => focus(noteRef.current),
    c: compare,
    d: download,
  }, active)

  const onBodyKeyDown = (event: KeyboardEvent<HTMLTableSectionElement>) => {
    if (moveCursorKey(event, cursor, rows.length, move)) return
    if (event.key === 'Enter' && code) { event.preventDefault(); navigate(analysisHref(code)) }
  }
  const toggleSort = (key: BookSortKey) => {
    if (sort === key) setDescending(!descending)
    else { setSort(key); setDescending(key === 'updated' || key === 'upside' || key === 'yield') }
  }

  const renameTag = useMutation({
    mutationFn: ({ from, to }: { from: string; to: string }) => apiRequest(`/api/tags/${encodeURIComponent(from)}`, { method: 'PATCH', body: JSON.stringify({ name: to }) }),
    onSuccess: (_result, { to }) => { onFilter({ tag: to }); invalidateResearch(client) },
  })
  const deleteTag = useMutation({
    mutationFn: (name: string) => apiRequest(`/api/tags/${encodeURIComponent(name)}`, { method: 'DELETE' }),
    onSuccess: () => { onFilter({ tag: '' }); invalidateResearch(client) },
  })
  const [renaming, setRenaming] = useState<string | null>(null)

  if (loading && !book) return <LoadingState label="Loading your research" />
  if (error && !book) return <ErrorState error={error} retry={retry} />

  const detailName = current?.company_name ?? outside?.company_name ?? code
  const detailTicker = current?.ticker ?? outside?.ticker ?? ''
  const detailIndustry = current?.industry ?? (outside as { industry?: string } | null)?.industry ?? ''

  return <div className="rs-book">
    <div className="rs-book__list panel">
      <div className="rs-toolbar">
        <div className="rs-toolbar__picker">
          <CompanyPicker selected={null} clearOnSelect inputRef={pickerRef} label="Add a company" placeholder="Add a company to research…" onSelect={company => { if (company?.company_code) { setPicked(company); onSelect(company.company_code) } }} />
          <kbd aria-hidden="true">A</kbd>
        </div>
        <label className="rs-search">
          <Search aria-hidden="true" />
          <input ref={filterRef} className="input" value={query} placeholder="Filter by name, tag, or thesis" aria-label="Filter companies" onChange={event => setQuery(event.target.value)} onKeyDown={event => { if (event.key === 'Escape') { setQuery(''); event.currentTarget.blur() } if (event.key === 'ArrowDown') { event.preventDefault(); setFocusRequest(value => value + 1) } }} />
          <kbd aria-hidden="true">F</kbd>
        </label>
        <span className="rs-toolbar__spacer" />
        <button type="button" className="button button--ghost button--small" disabled={compareCodes.length < 2} onClick={compare} title={compareCodes.length < 2 ? 'List at least two companies to compare them' : `Compare the first ${compareCodes.length} companies listed (C)`}><GitCompare aria-hidden="true" />Compare</button>
        <button type="button" className="button button--ghost button--small" disabled={!rows.length} onClick={download} title="Download the companies listed as CSV (D)"><Download aria-hidden="true" />CSV</button>
      </div>
      <div className="rs-filters">
        <div className="rs-chips" role="group" aria-label="Filter by tag">
          <button type="button" className="rs-chip" aria-pressed={!tag} onClick={() => onFilter({ tag: '' })}>All tags <small>{companies.length}</small></button>
          {tags.map(item => <button
            key={item.name}
            type="button"
            className={isPositionTag(item.name) ? 'rs-chip rs-chip--position' : 'rs-chip'}
            aria-pressed={tag === item.name}
            title={isPositionTag(item.name) ? 'Follows your portfolio' : undefined}
            onClick={() => onFilter({ tag: tag === item.name ? '' : item.name })}
          >{isPositionTag(item.name) && <Briefcase aria-hidden="true" />}{item.name} <small>{item.member_count}</small></button>)}
          {tags.length > 0 && <kbd aria-hidden="true" title="Previous or next tag">[ ]</kbd>}
        </div>
        <div className="rs-chips" role="group" aria-label="Filter by status">
          {STATUS_FILTERS.map(item => <button key={item.key} type="button" className="rs-chip rs-chip--plain" aria-pressed={status === item.key} onClick={() => onFilter({ status: item.key })}>{item.label}</button>)}
        </div>
        {tag && isPositionTag(tag) && <p className="rs-tag-actions rs-dim">“{tag}” follows your portfolio: a company moves between Open position and Closed position when the portfolio is rebuilt after a trade.</p>}
        {tag && !isPositionTag(tag) && <div className="rs-tag-actions">
          {renaming === tag
            ? <form onSubmit={event => { event.preventDefault(); const value = new FormData(event.currentTarget).get('name'); if (typeof value === 'string' && value.trim() && value.trim() !== tag) renameTag.mutate({ from: tag, to: value.trim() }); setRenaming(null) }}>
                <input className="input" name="name" aria-label={`Rename tag ${tag}`} defaultValue={tag} autoFocus onKeyDown={event => { if (event.key === 'Escape') { event.stopPropagation(); setRenaming(null) } }} />
                <button type="submit" className="text-button">Rename</button>
              </form>
            : <button type="button" className="text-button" onClick={() => setRenaming(tag)}>Rename “{tag}”</button>}
          <ConfirmButton label={`Delete tag ${tag}`} confirmLabel="Delete for every company?" onConfirm={() => deleteTag.mutate(tag)}>Delete tag</ConfirmButton>
          {(renameTag.error || deleteTag.error) && <span className="form-error">{((renameTag.error || deleteTag.error) as Error).message}</span>}
        </div>}
      </div>
      {!companies.length
        ? <EmptyState title="No research yet" description="Add a company above (A), or tag, note, or set a target from any company's Analysis page." />
        : <div className="rs-table-scroll">
          <table className="rs-table" aria-label="Companies in your research">
            <thead><tr>{COLUMNS.map(column => <th key={column.key} scope="col" className={[column.numeric && 'num', column.wide && 'rs-col--wide'].filter(Boolean).join(' ') || undefined} aria-sort={sort === column.key ? (descending ? 'descending' : 'ascending') : undefined}>
              <button type="button" className="rs-sort" title={column.title} onClick={() => toggleSort(column.key)}>{column.label}{sort === column.key && (descending ? <ArrowDown aria-hidden="true" /> : <ArrowUp aria-hidden="true" />)}</button>
            </th>)}<th scope="col" className="num" title="Notes and alerts"><span className="rs-sort">Notes</span></th></tr></thead>
            <tbody ref={body} onKeyDown={onBodyKeyDown}>
              {rows.map((company, index) => <BookRow key={company.company_code} company={company} isCursor={index === cursor} today={today} onSelect={() => onSelect(company.company_code)} />)}
            </tbody>
          </table>
          {!rows.length && <p className="rs-empty">No companies match. <button type="button" className="text-button" onClick={() => { setQuery(''); onFilter({ tag: '', status: '' }) }}>Clear filters</button></p>}
        </div>}
    </div>
    <aside className="rs-book__detail panel" aria-label="Company research">
      {code ? <>
        <header className="rs-detail__header">
          <div>
            <h2><Link to={analysisHref(code)}>{detailName}</Link>{current?.position && <PositionBadge position={current.position} />}</h2>
            <p className="rs-detail__meta">{(isEdinetCode(code) ? [detailTicker, code, detailIndustry] : [code, 'no EDINET filings']).filter(Boolean).join(' · ')}{current?.LatestPrice != null && <> · <span className="mono">{formatMetricValue(PRICE_FORMAT, current.LatestPrice, { price: current.price_currency })}</span></>}{current?.PERatio != null && ` · P/E ${current.PERatio.toFixed(1)}`}{current?.DividendsYield != null && ` · yield ${formatPercent(current.DividendsYield)}`}</p>
            {!current && <p className="rs-detail__new">Not in your research yet — a status, target, tag, note, or alert adds it.</p>}
          </div>
          <nav className="rs-detail__links" aria-label="Open this company">
            <Link to={analysisHref(code)} title="Open in Analysis (O)">Analysis</Link>
            {isEdinetCode(code) && <Link to={`/compare?companies=${encodeURIComponent(code)}`}>Peers</Link>}
            <Link to={`/research?tab=options&company=${encodeURIComponent(code)}`}>Options</Link>
            {isEdinetCode(code) && <Link to={`/research?tab=bonds&company=${encodeURIComponent(code)}`}>Credit</Link>}
          </nav>
        </header>
        <CompanyResearchPanel key={code} code={code} price={current?.LatestPrice} priceCurrency={current?.price_currency} today={today} tagRef={tagRef} noteRef={noteRef} thesisRef={thesisRef} statusRef={statusRef} keys={{ thesis: 'E', tag: 'T', note: 'N' }} />
      </> : <p className="rs-empty">Choose a company to see and edit its research.</p>}
    </aside>
  </div>
}

function BookRow({ company, isCursor, today, onSelect }: { company: BookCompany; isCursor: boolean; today: string; onSelect: () => void }) {
  const gap = upside(company)
  const review = reviewState(company.review_on, today)
  return <tr data-cursor={isCursor} className={isCursor ? 'is-cursor' : undefined} tabIndex={isCursor ? 0 : -1} aria-selected={isCursor} onClick={onSelect} onFocus={() => { if (!isCursor) onSelect() }}>
    <th scope="row">
      <span className="rs-company-cell">
        <strong>{company.company_name}{company.position && <PositionBadge position={company.position} />}</strong>
        <small>{[company.kind === 'security' ? company.company_code : company.ticker, ...company.tags.filter(item => !isPositionTag(item))].filter(Boolean).join(' · ')}</small>
      </span>
    </th>
    <td>{company.thesis_status ? <span className={`rs-pill rs-pill--${company.thesis_status}`}>{statusLabel(company.thesis_status)}</span> : <span className="rs-dim">—</span>}</td>
    <td className="num">{formatMetricValue(PRICE_FORMAT, company.LatestPrice, { price: company.price_currency })}</td>
    <td className={`num ${gap == null ? '' : gap >= 0 ? 'is-up' : 'is-down'}`} title={company.target_value != null ? `Target ${formatMetricValue(PRICE_FORMAT, company.target_value, { price: company.target_currency })}` : undefined}>{gap == null ? (company.target_value != null ? formatMetricValue(PRICE_FORMAT, company.target_value, { price: company.target_currency }) : '—') : formatSignedPercent(gap)}</td>
    <td className={review === 'overdue' ? 'is-overdue' : review === 'due' ? 'is-due' : undefined} title={company.review_on ? formatDay(company.review_on) : undefined}>{company.review_on ? relativeDay(company.review_on, today) : <span className="rs-dim">—</span>}</td>
    <td className="num rs-col--wide">{company.PERatio == null ? '—' : company.PERatio.toFixed(1)}</td>
    <td className="num">{formatPercent(company.DividendsYield)}</td>
    <td className="rs-dim rs-col--wide">{relativeDay(company.updated_at, today)}</td>
    <td className="num">
      {company.note_count > 0 && <span title={`${company.note_count} notes`}>{company.note_count}</span>}
      {company.alert_count > 0 && <span className={company.alerts_triggered ? 'rs-bell is-triggered' : 'rs-bell'} title={`${company.alert_count} alerts, ${company.alerts_triggered} triggered now`}><Bell aria-hidden="true" />{company.alerts_triggered || company.alert_count}</span>}
    </td>
  </tr>
}

function PositionBadge({ position }: { position: 'open' | 'closed' }) {
  return <span className={`rs-position rs-position--${position}`} title={position === 'open' ? 'Open position: in your portfolio now' : 'Closed position: held before, sold since'}>{position === 'open' ? 'Open' : 'Closed'}</span>
}
