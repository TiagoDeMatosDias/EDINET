import { Download, Search } from 'lucide-react'
import { useEffect, useMemo, useRef, useState, type KeyboardEvent } from 'react'
import { useNavigate } from 'react-router-dom'

import { ApiError } from '../../api/client'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { HotkeyKbd } from '../../hotkeys/HotkeyKbd'
import { useHotkeyScope } from '../../hotkeys/useHotkeyScope'
import { useHotkeyText } from '../../hotkeys/useHotkeyText'
import { downloadTextFile, safeFileName } from '../analysis/downloads'
import { displayTicker } from '../bonds/bondFormat'
import { peopleScope } from '../research/researchHotkeys'
import { analysisHref } from '../research/researchModel'
import { moveCursorKey, useListCursor } from '../research/useListCursor'
import { EntityDetailPanel } from './EntityDetailPanel'
import { entityMeta, filedName, holdingsCsv, KIND_FILTERS, KIND_LABELS, mainCompany, valuedCodes } from './ownershipFormat'
import { KindIcon } from './ownershipParts'
import { useEntities, useEntity, useMarketCaps } from './ownershipQueries'
import type { EntityKind, EntitySummary } from './ownershipTypes'
import '../research/research.css'
import './ownership.css'

const PAGE = 100
const TYPING_PAUSE_MS = 250
const KINDS = KIND_FILTERS.map(item => item.key)

/**
 * Everyone the annual reports name: officers, and the people and organisations
 * among companies' largest shareholders. The list searches by name; the panel
 * beside it shows one of them in full.
 */
export function EntitiesView({ entityId, query, kind, onChange, active }: { entityId: string; query: string; kind: string; onChange: (patch: Record<string, string>) => void; active: boolean }) {
  const navigate = useNavigate()
  const kindFilter: EntityKind | '' = KINDS.includes(kind as EntityKind) ? kind as EntityKind : ''
  const [typed, setTyped] = useState(query)
  const [limit, setLimit] = useState(PAGE)
  const [focusRequest, setFocusRequest] = useState(0)
  const filterRef = useRef<HTMLInputElement>(null)
  // The address bar follows the search box once typing pauses, and the list follows the address bar.
  useEffect(() => {
    if (typed.trim() === query) return
    const timer = window.setTimeout(() => onChange({ q: typed.trim() }), TYPING_PAUSE_MS)
    return () => window.clearTimeout(timer)
  // eslint-disable-next-line react-hooks/exhaustive-deps -- onChange is recreated on every render of the page
  }, [typed, query])

  const list = useEntities(query, kindFilter, limit)
  const rows = useMemo(() => list.data?.entities ?? [], [list.data])
  const selected = entityId || rows[0]?.entity_id || ''
  const cursor = rows.findIndex(row => row.entity_id === selected)
  const detail = useEntity(selected)
  // The panel asks for the same market values; the download includes them once they have arrived.
  const caps = useMarketCaps(useMemo(() => detail.data ? valuedCodes(detail.data) : [], [detail.data]))
  const body = useListCursor<HTMLTableSectionElement>(cursor, focusRequest)

  const select = (id: string) => onChange({ entity: id })
  const move = (next: number) => { const row = rows[next]; if (row) select(row.entity_id) }
  const step = (delta: number) => { if (rows.length) { move(Math.max(0, Math.min(rows.length - 1, (cursor < 0 ? -1 : cursor) + delta))); setFocusRequest(value => value + 1) } }
  const setKind = (next: EntityKind | '') => { setLimit(PAGE); onChange({ kind: next }) }
  const cycleKind = (delta: number) => setKind(KINDS[(KINDS.indexOf(kindFilter) + delta + KINDS.length) % KINDS.length])
  const openCompany = () => { const code = detail.data ? mainCompany(detail.data) : ''; if (code) navigate(analysisHref(code)) }
  const download = () => { if (detail.data?.holdings.length) downloadTextFile(`${safeFileName(detail.data.entity.name)}-holdings.csv`, holdingsCsv(detail.data.holdings, caps.data), 'text/csv;charset=utf-8') }
  const focus = (element: HTMLElement | null) => { element?.focus(); element?.scrollIntoView?.({ block: 'center', behavior: 'smooth' }) }

  useHotkeyScope(peopleScope, {
    next: () => step(1),
    previous: () => step(-1),
    open: openCompany,
    filter: () => focus(filterRef.current),
    'previous-kind': () => cycleKind(-1),
    'next-kind': () => cycleKind(1),
    download,
  }, { enabled: active })
  const openKey = useHotkeyText(peopleScope.byId.open)

  if (list.isLoading && !list.data) return <LoadingState label="Loading people and holders" />
  if (list.error instanceof ApiError && list.error.status === 503) {
    return <EmptyState title="No ownership data yet" description="Run the Update ownership pipeline step: it reads the major shareholders and officers in every stored annual report and works out who is who across companies." />
  }
  if (list.isError || !list.data) return <ErrorState error={list.error} retry={() => void list.refetch()} />
  const counts = list.data.counts
  const everyone = Object.values(counts).reduce((total, count) => total + (count ?? 0), 0)
  if (!everyone) {
    return <EmptyState title="No ownership data yet" description="Run the Update ownership pipeline step: it reads the major shareholders and officers in every stored annual report and works out who is who across companies." />
  }
  const onBodyKeyDown = (event: KeyboardEvent<HTMLTableSectionElement>) => {
    if (moveCursorKey(event, cursor, rows.length, move)) return
    if (event.key === 'Enter') { event.preventDefault(); openCompany() }
  }
  // Someone opened from a company's page is rarely among the first hundred by connections: keep them in view.
  const pinned = selected && cursor < 0 && detail.data ? detail.data.entity : null

  return <div className="rs-book ow-directory">
    <div className="rs-book__list panel">
      <div className="rs-toolbar">
        <label className="rs-search">
          <Search aria-hidden="true" />
          <input
            ref={filterRef}
            className="input"
            value={typed}
            placeholder="Find a person, holder, or company (English or Japanese)"
            aria-label="Find a person or holder"
            onChange={event => { setTyped(event.target.value); setLimit(PAGE) }}
            onKeyDown={event => {
              if (event.key === 'Escape') { setTyped(''); event.currentTarget.blur() }
              if (event.key === 'ArrowDown') { event.preventDefault(); setFocusRequest(value => value + 1) }
            }}
          />
          <span aria-hidden="true"><HotkeyKbd hotkey={peopleScope.byId.filter} /></span>
        </label>
        <span className="rs-toolbar__spacer" />
        <span className="rs-dim" aria-live="polite">{list.data.total.toLocaleString('en-US')} of {everyone.toLocaleString('en-US')}{list.isFetching ? ' · searching…' : ''}</span>
        <button type="button" className="button button--ghost button--small" disabled={!detail.data?.holdings.length} onClick={download} title="Download the selected holder’s shareholdings as CSV (D)"><Download aria-hidden="true" />CSV</button>
      </div>
      <div className="rs-filters">
        <div className="rs-chips" role="group" aria-label="Filter by kind">
          {KIND_FILTERS.map(item => <button key={item.key || 'all'} type="button" className="rs-chip" aria-pressed={kindFilter === item.key} onClick={() => setKind(item.key)}>{item.label} <small>{(item.key ? counts[item.key] ?? 0 : everyone).toLocaleString('en-US')}</small></button>)}
          <span aria-hidden="true" title="Previous or next kind"><HotkeyKbd hotkey={peopleScope.byId['previous-kind']} /> <HotkeyKbd hotkey={peopleScope.byId['next-kind']} /></span>
        </div>
      </div>
      {!rows.length && !pinned
        ? <p className="rs-empty">Nobody matches “{query}”. <button type="button" className="text-button" onClick={() => { setTyped(''); onChange({ q: '', kind: '' }) }}>Clear the search</button></p>
        : <div className="rs-table-scroll">
          <table className="rs-table ow-table ow-list" aria-label="People and holders">
            <thead><tr>
              <th scope="col">Name</th>
              <th scope="col">Known for</th>
              <th scope="col" className="num" title="Companies whose latest report lists them among the largest shareholders">Holdings</th>
              <th scope="col" className="num" title="Companies whose latest report lists them as an officer">Boards</th>
            </tr></thead>
            <tbody ref={body} onKeyDown={onBodyKeyDown}>
              {pinned && <EntityRow entity={pinned} isCursor onSelect={() => select(pinned.entity_id)} />}
              {rows.map((row, index) => <EntityRow key={row.entity_id} entity={row} isCursor={index === cursor} onSelect={() => select(row.entity_id)} />)}
            </tbody>
          </table>
          {list.data.total > rows.length && <button type="button" className="text-button rs-more" onClick={() => setLimit(limit + PAGE)}>Show {Math.min(PAGE, list.data.total - rows.length)} more of {(list.data.total - rows.length).toLocaleString('en-US')}</button>}
        </div>}
      <p className="rs-hint ow-hint">Everyone named in the stored annual reports: officers, and the largest shareholders of each company. The most connected come first. Officers are told apart by date of birth; individual shareholders only by name.</p>
    </div>
    <aside className="rs-book__detail panel" aria-label="Person or holder">
      <EntityDetailPanel entityId={selected} onSelect={select} keys={{ open: openKey }} />
    </aside>
  </div>
}

function EntityRow({ entity, isCursor, onSelect }: { entity: EntitySummary; isCursor: boolean; onSelect: () => void }) {
  const code = displayTicker(entity.ticker)
  return <tr data-cursor={isCursor} className={isCursor ? 'is-cursor' : undefined} tabIndex={isCursor ? 0 : -1} aria-selected={isCursor} onClick={onSelect} onFocus={() => { if (!isCursor) onSelect() }}>
    <th scope="row">
      <span className="ow-name">
        <KindIcon kind={entity.kind} />
        <span className="ow-name__text">
          <strong title={filedName(entity)}>{entity.name}{code && <span className="bd-ticker">{code}</span>}</strong>
          <small>{[KIND_LABELS[entity.kind], entity.birth_date ? `born ${entity.birth_date.slice(0, 4)}` : '', entity.name_ja && entity.name_ja !== entity.name ? entity.name_ja : ''].filter(Boolean).join(' · ')}</small>
        </span>
      </span>
    </th>
    <td className="ow-headline" title={entity.headline ?? undefined}>{entity.headline ?? <span className="rs-dim">{entityMeta(entity)}</span>}</td>
    <td className="num">{entity.holdings ? entity.holdings.toLocaleString('en-US') : <span className="rs-dim">{entity.past_holdings ? `(${entity.past_holdings})` : '—'}</span>}</td>
    <td className="num">{entity.positions ? entity.positions : <span className="rs-dim">{entity.past_positions ? `(${entity.past_positions})` : '—'}</span>}</td>
  </tr>
}
