import { Plus } from 'lucide-react'
import { useEffect, useRef, useState, type KeyboardEvent, type Ref } from 'react'

import { Tip } from '../../components/Tooltip'
import { formatMetricValue, type MetricDefinition } from '../../metrics'
import { abbreviate, MAX_COMPANIES, sizeRatio } from './comparisonModel'
import type { CompanyInfo, Peer, PeersResponse } from './comparisonTypes'
import { Swatch } from './CompanyPanel'

const COLLAPSED = 8
const PERCENT: MetricDefinition = { label: '', group: '', format: 'percent' }
const MONEY: MetricDefinition = { label: '', group: '', format: 'money', currency: 'price' }
const RATIO: MetricDefinition = { label: '', group: '' }

/**
 * Listed companies in the selected companies' industries, closest in market cap
 * first. ↑/↓ (J/K) move through the list and Enter adds the focused company.
 */
export function PeersPanel({ codes, info, colorIndex, data, loading, error, listRef, onAdd }: {
  codes: string[]
  colorIndex: Record<string, number>
  info: Record<string, CompanyInfo | undefined>
  data?: PeersResponse
  loading: boolean
  error?: unknown
  listRef: Ref<HTMLTableSectionElement>
  onAdd: (peers: Peer[]) => void
}) {
  const [expanded, setExpanded] = useState(false)
  const [cursor, setCursor] = useState(0)
  const rows = useRef<Array<HTMLTableRowElement | null>>([])
  const peers = data?.peers ?? []
  const shown = expanded ? peers : peers.slice(0, COLLAPSED)
  const room = MAX_COMPANIES - codes.length
  const index = Math.min(cursor, Math.max(0, shown.length - 1))
  // A peer added from the keyboard leaves the list; the focus moves to the row that takes its place.
  const refocus = useRef(false)
  const atCursor = shown[index]?.company_code
  useEffect(() => {
    if (!refocus.current || !atCursor) return
    refocus.current = false
    rows.current[index]?.focus()
  }, [atCursor, index])
  const add = (peer?: Peer) => {
    if (!peer || room < 1) return
    refocus.current = true
    onAdd([peer])
  }

  const move = (next: number) => {
    const target = Math.max(0, Math.min(shown.length - 1, next))
    setCursor(target)
    rows.current[target]?.focus()
  }
  const onKeyDown = (event: KeyboardEvent<HTMLTableSectionElement>) => {
    if (event.ctrlKey || event.metaKey || event.altKey) return
    const keys: Record<string, () => void> = {
      ArrowDown: () => move(index + 1), j: () => move(index + 1),
      ArrowUp: () => move(index - 1), k: () => move(index - 1),
      Home: () => move(0), End: () => move(shown.length - 1),
      Enter: () => add(shown[index]),
      '+': () => add(shown[index]),
    }
    const action = keys[event.key]
    if (!action) return
    event.preventDefault()
    event.stopPropagation()
    action()
  }
  const industries = data?.industries ?? []

  return <section className="panel cmp-panel cmp-peers" aria-labelledby="cmp-peers-title">
    <header className="cmp-panel__header">
      <h2 id="cmp-peers-title">Suggested peers <kbd aria-hidden="true">P</kbd></h2>
      <span className="cmp-panel__meta">{industries.filter(item => item.candidates > 0).map(item => `${item.industry} · ${item.candidates} more listed`).join('; ')}</span>
      {peers.length > 0 && <button type="button" className="button button--secondary button--small" disabled={room < 1} onClick={() => onAdd(peers.slice(0, Math.min(3, room)))} title="Add the three companies closest in size (Shift+P adds the closest one)"><Plus aria-hidden="true" />{room > 0 ? `Add ${Math.min(3, room)} closest` : 'Comparison full'}</button>}
    </header>
    {!codes.length && <p className="cmp-panel__empty">Peers appear here once you add a company: listed companies in the same industry, closest in market cap first.</p>}
    {codes.length > 0 && loading && !peers.length && <p className="cmp-panel__empty">Finding companies in the same industry…</p>}
    {Boolean(error) && <p className="form-error">Could not load peer suggestions.</p>}
    {codes.length > 0 && !loading && !error && !peers.length && <p className="cmp-panel__empty">No other listed companies share {codes.length > 1 ? 'these industries' : 'this industry'}.</p>}
    {peers.length > 0 && <div className="cmp-peers__scroll">
      <table className="cmp-table cmp-peers__table">
        <thead><tr>
          <th scope="col"><span className="sr-only">Add</span></th>
          <th scope="col">Company</th>
          <th scope="col" className="num">Market cap</th>
          <th scope="col" className="num"><Tip content="Market cap relative to the selected company closest in size; the dot is that company's colour.">Size</Tip></th>
          <th scope="col" className="num">P/E</th>
          <th scope="col" className="num">P/B</th>
          <th scope="col" className="num"><Tip content="Return on equity, three-year average.">ROE</Tip></th>
          <th scope="col" className="num">Yield</th>
        </tr></thead>
        <tbody ref={listRef} onKeyDown={onKeyDown}>
          {shown.map((peer, row) => <tr
            key={peer.company_code}
            ref={element => { rows.current[row] = element }}
            tabIndex={row === index ? 0 : -1}
            className={row === index ? 'is-cursor' : undefined}
            onFocus={() => setCursor(row)}
            onDoubleClick={() => room > 0 && onAdd([peer])}
          >
            <td><button type="button" className="icon-button" tabIndex={-1} disabled={room < 1} onClick={() => onAdd([peer])} aria-label={`Add ${peer.company_name}`} title="Add to the comparison (Enter)"><Plus /></button></td>
            <th scope="row"><span className="cmp-peers__name" title={`${peer.company_name} · ${peer.industry}`}>{peer.company_name}</span> <small className="mono">{peer.ticker}</small></th>
            <td className="num" title={formatMetricValue(MONEY, peer.MarketCap, { price: peer.price_currency })}>{abbreviate(formatMetricValue(MONEY, peer.MarketCap, { price: peer.price_currency }))}</td>
            <td className="num muted" title={peer.nearest_code ? `Market cap ${sizeRatio(peer.size_ratio)} that of ${info[peer.nearest_code]?.company_name ?? peer.nearest_code}` : undefined}>{peer.size_ratio != null ? <>{sizeRatio(peer.size_ratio)}{codes.length > 1 && peer.nearest_code && <> <Swatch index={colorIndex[peer.nearest_code] ?? 0} /></>}</> : '—'}</td>
            <td className="num">{formatMetricValue(RATIO, peer.PERatio)}</td>
            <td className="num">{formatMetricValue(RATIO, peer.PriceToBook)}</td>
            <td className="num">{formatMetricValue(PERCENT, peer.ReturnOnEquity)}</td>
            <td className="num">{formatMetricValue(PERCENT, peer.DividendsYield)}</td>
          </tr>)}
        </tbody>
      </table>
    </div>}
    {peers.length > COLLAPSED && <button type="button" className="cmp-panel__more" onClick={() => setExpanded(!expanded)}>{expanded ? 'Show fewer' : `Show ${peers.length - COLLAPSED} more`}</button>}
  </section>
}
