import { useQuery } from '@tanstack/react-query'
import { Download, Search } from 'lucide-react'
import { useMemo, useRef, useState } from 'react'

import { apiRequest } from '../../../api/client'
import { EmptyState, ErrorState, LoadingState } from '../../../components/Feedback'
import { Tip } from '../../../components/Tooltip'
import { HotkeyKbd } from '../../../hotkeys/HotkeyKbd'
import { useHotkeyScope } from '../../../hotkeys/useHotkeyScope'
import { statementsTabScope } from '../filingsHotkeys'
import { downloadTextFile, safeFileName } from '../../analysis/downloads'
import { formatChange } from '../../analysis/statementLayout'
import {
  filterRows,
  formatStatementValue,
  lineItemCount,
  moneyScale,
  periodChange,
  splitPeriods,
  STATEMENT_GROUPS,
  statementCsv,
  statementGroup,
  statementTitle,
  unitNote,
  type StatementRow,
  type StatementTable,
  type StatementTablesResponse,
} from '../statements'

function StatementGrid({ statement, rows, showSparse }: { statement: StatementTable; rows: StatementRow[]; showSparse: boolean }) {
  const { shown } = splitPeriods(statement.periods)
  const periods = showSparse ? statement.periods : shown
  const money = moneyScale(rows)
  const memberHeader = statement.member_axes.join(' · ')
  const currency = rows.find(row => row.unit && /^[A-Z]{3}$/.test(row.unit))?.unit
  const showUnits = rows.some(row => unitNote(row.unit))
  // Compare like with like: the latest period against the prior one of the same kind.
  const [current, prior] = periods.filter(period => period.detail === periods[0]?.detail)
  const showChange = Boolean(current && prior)
  const columnCount = 1 + (memberHeader ? 1 : 0) + (showUnits ? 1 : 0) + periods.length + (showChange ? 1 : 0)
  return <div className="statement-grid-wrap">
    <table className="statement-grid">
      <thead>
        <tr>
          <th scope="col" className="statement-grid__label">{currency && money ? `${currency} ${money.label}`.trim() : 'Line item'}</th>
          {memberHeader && <th scope="col" className="statement-grid__member">{memberHeader}</th>}
          {showUnits && <th scope="col" className="statement-grid__unit">Unit</th>}
          {periods.map(period => <th key={period.key} scope="col" className="num" title={period.start ? `${period.start} to ${period.end}` : `As of ${period.end}`}>{period.label}<small>{period.detail}</small></th>)}
          {showChange && <th scope="col" className="num statement-grid__change"><Tip content={`Change from ${prior.label} to ${current.label}; percentage points for ratios.`} focusable={false}>Change</Tip></th>}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, index) => {
          if (row.kind === 'heading') return <tr key={index} className="statement-grid__heading"><th scope="rowgroup" colSpan={columnCount} style={{ paddingLeft: `${10 + row.depth * 14}px` }}>{row.label}</th></tr>
          const change = showChange ? periodChange(row, current, prior) : null
          return <tr key={index} className={row.kind === 'total' ? 'statement-grid__total' : undefined}>
            <th scope="row" className="statement-grid__label" style={{ paddingLeft: `${10 + row.depth * 14}px` }} title={row.concept}>{row.label}</th>
            {memberHeader && <td className="statement-grid__member">{row.member}</td>}
            {showUnits && <td className="statement-grid__unit">{unitNote(row.unit)}</td>}
            {periods.map(period => {
              const value = row.values?.[period.key]
              return <td key={period.key} className={value != null && value < 0 ? 'num neg' : 'num'} title={value != null ? value.toLocaleString('en-US') : undefined}>{formatStatementValue(value, row, money)}</td>
            })}
            {showChange && <td className={change && change.value < 0 ? 'num neg statement-grid__change' : 'num statement-grid__change'}>{change ? (change.points ? `${change.value > 0 ? '+' : change.value < 0 ? '−' : ''}${Math.abs(change.value * 100).toFixed(1)} pp` : formatChange(change.value, 'money')) : ''}</td>}
          </tr>
        })}
      </tbody>
    </table>
  </div>
}

export function StatementsTab({ docId, fileStem, active, onSelect }: { docId: string; fileStem: string; active: string | null; onSelect: (id: string) => void }) {
  const [filter, setFilter] = useState('')
  const [showSparse, setShowSparse] = useState(false)
  const filterInput = useRef<HTMLInputElement>(null)
  const tables = useQuery({
    queryKey: ['filing-statement-tables', docId],
    queryFn: () => apiRequest<StatementTablesResponse>(`/api/filings/${encodeURIComponent(docId)}/statement-tables`),
  })
  const matches = useMemo(() => (tables.data?.statements ?? [])
    .map(statement => ({ statement, rows: filterRows(statement.rows, filter), group: statementGroup(statement) }))
    .filter(({ rows }) => lineItemCount(rows) > 0)
    .sort((a, b) => STATEMENT_GROUPS.findIndex(group => group.key === a.group) - STATEMENT_GROUPS.findIndex(group => group.key === b.group)), [filter, tables.data])
  const selected = matches.find(({ statement }) => statement.id === active) ?? matches[0]
  const index = selected ? matches.indexOf(selected) : -1
  const step = (delta: number) => {
    const next = matches[Math.max(0, Math.min(matches.length - 1, index + delta))]
    if (next) onSelect(next.statement.id)
  }
  useHotkeyScope(statementsTabScope, { next: () => step(1), previous: () => step(-1), filter: () => filterInput.current?.focus() }, { enabled: matches.length > 0 })

  if (tables.isLoading) return <LoadingState label="Building statement tables from the filing's XBRL" />
  if (tables.isError) return <ErrorState error={tables.error} retry={() => void tables.refetch()} />
  const sparse = selected ? splitPeriods(selected.statement.periods).sparse : []
  const exportCsv = () => {
    if (!selected) return
    const { shown } = splitPeriods(selected.statement.periods)
    downloadTextFile(`${safeFileName(fileStem)}-${safeFileName(statementTitle(selected.statement).title)}.csv`, statementCsv(selected.statement, selected.rows, showSparse ? selected.statement.periods : shown), 'text/csv;charset=utf-8')
  }
  return <div className="viewer-split">
    <nav className="viewer-sidebar" aria-label="Statements and notes">
      <label className="viewer-search">
        <Search aria-hidden="true" />
        <input ref={filterInput} value={filter} onChange={event => setFilter(event.target.value)} onKeyDown={event => { if (event.key === 'Escape') { setFilter(''); event.currentTarget.blur() } }} placeholder="Filter line items" aria-label="Filter line items" />
        <HotkeyKbd hotkey={statementsTabScope.byId.filter} />
      </label>
      {tables.data && <p className="viewer-sidebar__count">{tables.data.fact_count.toLocaleString()} facts in {tables.data.statements.length} tables{filter ? ` · ${matches.length} match` : ''}</p>}
      {STATEMENT_GROUPS.map(group => {
        const items = matches.filter(match => match.group === group.key)
        if (!items.length) return null
        return <section key={group.key}>
          <h3>{group.label}</h3>
          <ul>{items.map(({ statement, rows }) => {
            const { title, tags } = statementTitle(statement)
            return <li key={statement.id}>
              <button type="button" className={statement.id === selected?.statement.id ? 'viewer-item active' : 'viewer-item'} aria-current={statement.id === selected?.statement.id ? 'true' : undefined} onClick={() => onSelect(statement.id)}>
                <span>{title}</span>
                <small>{lineItemCount(rows)} line items{tags.length ? ` · ${tags.join(' · ')}` : ''}</small>
              </button>
            </li>
          })}</ul>
        </section>
      })}
      {matches.length > 0 && <p className="viewer-sidebar__keys"><HotkeyKbd hotkey={statementsTabScope.byId.next} /><HotkeyKbd hotkey={statementsTabScope.byId.previous} /> next and previous table</p>}
    </nav>
    <div className="viewer-main">
      {!selected ? <EmptyState title={filter ? 'No matching line items' : 'No numeric facts'} description={filter ? `No table has a line item matching “${filter}”.` : 'This filing carries no numeric XBRL facts.'} /> : <>
        <header className="viewer-toolbar">
          <div className="viewer-toolbar__title">
            <h2>{statementTitle(selected.statement).title}</h2>
            {statementTitle(selected.statement).tags.map(tag => <span key={tag} className="viewer-tag">{tag}</span>)}
          </div>
          {sparse.length > 0 && <button type="button" className="text-button" aria-pressed={showSparse} onClick={() => setShowSparse(!showSparse)} title="Periods reported for only a few lines, such as a single prior-year balance">{showSparse ? 'Hide sparse periods' : `Show ${sparse.length} sparse ${sparse.length === 1 ? 'period' : 'periods'}`}</button>}
          <button type="button" className="button button--ghost button--small" onClick={exportCsv} title="Download this table as CSV with unrounded values"><Download aria-hidden="true" />CSV</button>
        </header>
        <section className="statement-block" aria-label={selected.statement.name}>
          <StatementGrid key={selected.statement.id} statement={selected.statement} rows={selected.rows} showSparse={showSparse} />
        </section>
      </>}
    </div>
  </div>
}
