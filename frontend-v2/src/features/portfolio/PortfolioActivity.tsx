import { Check, Eye, FileText, Minus, Plus, Search, Trash2 } from 'lucide-react'
import { useCallback, useMemo, useState } from 'react'

import { ErrorState, LoadingState } from '../../components/Feedback'
import { Field, Metric } from '../../components/Page'
import { useHotkeys } from '../../hooks/useHotkeys'
import { CashEffect } from './ActivityCells'
import { correctionLabel, findCorrections, orderActivity, type Correction } from './activityModel'
import { DeleteRecordsDialog } from './DeleteRecordsDialog'
import { ManualTransactionForm, type ManualResult } from './ManualTransactionForm'
import { formatDay, quantity, titleCase, transactionCashEffect } from './portfolioFormat'
import { SectionCard } from './PortfolioPrimitives'
import { PortfolioTable, type TableColumn } from './PortfolioTable'
import type { ImportFile, PortfolioDetail, Transaction } from './portfolioTypes'
import type { DeleteResult, DeleteSelection } from './recordsDelete'

type Props = {
  data: Transaction[]
  activity: Record<string, number>
  dateRange?: { min_date?: string | null; max_date?: string | null }
  imports?: ImportFile[]
  isLoading: boolean
  error?: unknown
  hotkeys?: boolean
  onOpenDetail: (detail: PortfolioDetail) => void
  onDeleted?: (result: DeleteResult, selection: DeleteSelection) => void
  onAdded?: (result: ManualResult) => void
}

function activityTone(value?: string) {
  if (value === 'DIVIDEND' || value === 'PIL_DIVIDEND') return 'positive'
  if (value === 'WITHHOLDING_TAX' || value === 'OTHER_FEE' || value === 'COMMISSION_ADJ') return 'negative'
  return 'neutral'
}

const CORRECTION_TIPS: Record<NonNullable<Correction['kind']>, string> = {
  reversal: 'Cancels the record above it. The broker reverses a record, then books the right amount, under the original date.',
  reversed: 'Cancelled by a reversal below it, so together they come to nothing.',
  corrected: 'The amount the broker booked to replace the reversed record.',
}

function ActivityCell({ row, correction }: { row: Transaction; correction?: Correction }) {
  const label = correctionLabel(correction)
  return <span className="pf-activity-cell">
    <span className={`activity-badge activity-badge--${activityTone(row.activity_type)}`}>{titleCase(row.activity_type ?? '')}{row.buy_sell ? ` · ${titleCase(row.buy_sell)}` : ''}</span>
    {(label || correction?.booked) && <small title={correction?.kind ? CORRECTION_TIPS[correction.kind] : 'When the broker booked it, after the date it is listed under.'}>
      {label}{label && correction?.booked ? ' · ' : ''}{correction?.booked ? `booked ${formatDay(correction.booked)}` : ''}
    </small>}
  </span>
}

function useColumns(onOpenDetail: Props['onOpenDetail'], selected: Set<number>, onToggle: (row: Transaction) => void, corrections: Map<number, Correction>, mainAccount: string | null, selectAll: { state: 'none' | 'some' | 'all'; count: number; toggle: () => void }) {
  return useMemo<TableColumn<Transaction>[]>(() => [
    { id: 'pick', header: 'Select', headerCell: <button type="button" className={`pf-check${selectAll.state !== 'none' ? ' is-on' : ''}`} role="checkbox" aria-checked={selectAll.state === 'all' ? true : selectAll.state === 'some' ? 'mixed' : false} aria-label={selectAll.state === 'all' ? 'Unselect all records shown' : `Select all ${selectAll.count.toLocaleString()} records shown`} title={`${selectAll.state === 'all' ? 'Unselect' : 'Select'} all ${selectAll.count.toLocaleString()} shown (Shift+A)`} onClick={selectAll.toggle}>{selectAll.state === 'all' ? <Check aria-hidden="true" /> : selectAll.state === 'some' ? <Minus aria-hidden="true" /> : null}</button>, cell: row => row.id == null ? null : <button type="button" className={`pf-check${selected.has(row.id) ? ' is-on' : ''}`} role="checkbox" aria-checked={selected.has(row.id)} aria-label={`Select the record from ${row.trade_date ?? 'an unknown date'}`} onClick={() => onToggle(row)}>{selected.has(row.id) && <Check aria-hidden="true" />}</button> },
    { id: 'date', header: 'Date', sortFirst: 'desc', sortValue: row => row.trade_date, cell: row => <span className="mono">{row.trade_date ?? '—'}</span> },
    { id: 'activity', header: 'Activity', sortValue: row => row.activity_type, cell: row => <ActivityCell row={row} correction={row.id == null ? undefined : corrections.get(row.id)} /> },
    { id: 'symbol', header: 'Symbol', rowHeader: true, sortValue: row => row.symbol, cell: row => <strong>{row.symbol || '—'}</strong> },
    { id: 'description', header: 'Description', cell: row => <span className="transaction-description" title={row.description ?? ''}>{row.description || '—'}{mainAccount && row.account_id && row.account_id !== mainAccount ? <small>Account {row.account_id}</small> : null}</span> },
    { id: 'quantity', header: 'Quantity', numeric: true, sortValue: row => row.quantity, cell: row => Number(row.quantity) ? quantity(row.quantity) : '—' },
    { id: 'cash', header: 'Cash effect', numeric: true, tip: 'Cash in (+) or out (−) of the account, in the record’s currency. A currency conversion shows both currencies.', sortValue: row => transactionCashEffect(row), cell: row => <CashEffect row={row} /> },
    { id: 'details', header: '', cell: row => <button type="button" className="icon-button" aria-label={`View transaction from ${row.trade_date ?? 'unknown date'}`} title="Details (Enter)" onClick={() => onOpenDetail({ kind: 'transaction', transaction: row })}><Eye /></button> },
  ], [corrections, mainAccount, onOpenDetail, onToggle, selected, selectAll])
}

function mostCommon(values: Array<string | null | undefined>) {
  const counts = new Map<string, number>()
  for (const value of values) if (value) counts.set(value, (counts.get(value) ?? 0) + 1)
  return counts.size > 1 ? [...counts].sort((a, b) => b[1] - a[1])[0][0] : null
}

function ActivitySummary({ activity, dateRange }: Pick<Props, 'activity' | 'dateRange'>) {
  const total = Object.values(activity).reduce((sum, value) => sum + value, 0)
  const income = Number(activity.DIVIDEND ?? 0) + Number(activity.PIL_DIVIDEND ?? 0)
  const fees = Number(activity.OTHER_FEE ?? 0) + Number(activity.COMMISSION_ADJ ?? 0)
  return <div className="portfolio-section-metrics">
    <Metric label="Imported records" value={total.toLocaleString()} detail={`${formatDay(dateRange?.min_date)} to ${formatDay(dateRange?.max_date)}`} />
    <Metric label="Trades" value={Number(activity.TRADE ?? 0).toLocaleString()} />
    <Metric label="Dividends" value={income.toLocaleString()} />
    <Metric label="Withholding tax" value={Number(activity.WITHHOLDING_TAX ?? 0).toLocaleString()} />
    <Metric label="Deposits & withdrawals" value={Number(activity.DEPOSIT_WITHDRAWAL ?? 0).toLocaleString()} />
    <Metric label="Fees & adjustments" value={fees.toLocaleString()} />
  </div>
}

function Imports({ imports, onDelete }: { imports?: ImportFile[]; onDelete: (selection: DeleteSelection) => void }) {
  if (!imports?.length) return null
  return <SectionCard title="Imported data" description="Each record belongs to the file that first imported it; overlapping files skip records already stored" actions={<button type="button" className="button button--danger button--small" onClick={() => onDelete({ kind: 'everything' })}><Trash2 aria-hidden="true" />Clear all portfolio data…</button>}>
    <ul className="pf-imports">{imports.map(file => <li key={file.source_file}>
      <FileText aria-hidden="true" />
      <span><strong>{file.source_file || 'Records without a file name'}</strong><small>{file.records.toLocaleString()} records · {formatDay(file.first_date)} to {formatDay(file.last_date)}{file.imported_at ? ` · imported ${formatDay(file.imported_at)}` : ''}</small></span>
      <button type="button" className="button button--ghost button--small" onClick={() => onDelete({ kind: 'files', files: [file.source_file] })} aria-label={`Delete the records imported from ${file.source_file || 'records without a file name'}`}><Trash2 aria-hidden="true" />Delete…</button>
    </li>)}</ul>
  </SectionCard>
}

export function PortfolioActivity(props: Props) {
  const [search, setSearch] = useState('')
  const [activityType, setActivityType] = useState('all')
  const [file, setFile] = useState('all')
  const [from, setFrom] = useState('')
  const [to, setTo] = useState('')
  const [hideReversed, setHideReversed] = useState(false)
  const [selected, setSelected] = useState<Set<number>>(() => new Set())
  const [pending, setPending] = useState<DeleteSelection | null>(null)
  const [adding, setAdding] = useState(false)
  const activityTypes = [...new Set(props.data.map(row => row.activity_type).filter(Boolean) as string[])].sort()
  const files = [...new Set(props.data.map(row => row.source_file ?? ''))].sort()
  // Only records from another account than the usual one are tagged with it.
  const mainAccount = useMemo(() => mostCommon(props.data.map(row => row.account_id)), [props.data])
  const ordered = useMemo(() => orderActivity(props.data), [props.data])
  const corrections = useMemo(() => findCorrections(props.data), [props.data])
  const reversedCount = useMemo(() => [...corrections.values()].filter(item => item.kind === 'reversal' || item.kind === 'reversed').length, [corrections])
  const term = search.trim().toLowerCase()
  const filtered = useMemo(() => ordered.filter(row => {
    const haystack = `${row.symbol ?? ''} ${row.description ?? ''} ${row.source_file ?? ''}`.toLowerCase()
    const day = row.trade_date ?? ''
    const kind = row.id == null ? undefined : corrections.get(row.id)?.kind
    return (!term || haystack.includes(term))
      && (activityType === 'all' || row.activity_type === activityType)
      && (file === 'all' || (row.source_file ?? '') === file)
      && (!from || day >= from) && (!to || day <= to)
      && !(hideReversed && (kind === 'reversal' || kind === 'reversed'))
  }), [activityType, corrections, file, from, hideReversed, ordered, term, to])
  // The selection only ever covers records the filters show.
  const shownSelected = useMemo(() => filtered.filter(row => row.id != null && selected.has(row.id)).map(row => row.id as number), [filtered, selected])
  const toggle = (row: Transaction) => {
    if (row.id == null) return
    const next = new Set(selected)
    if (next.has(row.id)) next.delete(row.id)
    else next.add(row.id)
    setSelected(next)
  }
  const deleteSelected = () => { if (shownSelected.length) setPending({ kind: 'records', ids: shownSelected }) }
  const shownIds = useMemo(() => filtered.flatMap(row => row.id == null ? [] : [row.id]), [filtered])
  const selectAllState: 'none' | 'some' | 'all' = !shownSelected.length ? 'none' : shownSelected.length === shownIds.length ? 'all' : 'some'
  // Selects every record the filters show, on every page; again, unselects them.
  const toggleAll = useCallback(() => {
    setSelected(current => {
      const next = new Set(current)
      if (shownIds.length && shownIds.every(id => next.has(id))) for (const id of shownIds) next.delete(id)
      else for (const id of shownIds) next.add(id)
      return next
    })
  }, [shownIds])
  const selectAll = useMemo(() => ({ state: selectAllState, count: shownIds.length, toggle: toggleAll }), [selectAllState, shownIds.length, toggleAll])
  useHotkeys({ Delete: deleteSelected }, Boolean(props.hotkeys) && !pending && shownSelected.length > 0)
  useHotkeys({ A: toggleAll, n: () => setAdding(true) }, Boolean(props.hotkeys) && !pending && !adding)
  const columns = useColumns(props.onOpenDetail, selected, toggle, corrections, mainAccount, selectAll)
  const filtersOn = Boolean(term || activityType !== 'all' || file !== 'all' || from || to || hideReversed)

  return <div className="portfolio-section-stack">
    <ActivitySummary activity={props.activity} dateRange={props.dateRange} />
    <SectionCard title="Activity" description={`${filtered.length.toLocaleString()} of ${props.data.length.toLocaleString()} records`} actions={!adding && <button type="button" className="button button--secondary button--small" onClick={() => setAdding(true)} title="Record a transaction by hand (N)"><Plus aria-hidden="true" />Add transaction <kbd aria-hidden="true">N</kbd></button>}>
      {adding && <ManualTransactionForm records={props.data} onClose={() => setAdding(false)} onAdded={result => props.onAdded?.(result)} />}
      <div className="portfolio-table-toolbar pf-activity-toolbar">
        <Field label="Search activity"><div className="input-with-icon"><Search /><input className="input" data-portfolio-find value={search} placeholder="Symbol, description, or source" onChange={event => setSearch(event.target.value)} onKeyDown={event => { if (event.key === 'Escape') { setSearch(''); event.currentTarget.blur() } }} /><kbd className="input-kbd" aria-hidden="true">F</kbd></div></Field>
        <Field label="Activity type"><select className="select" value={activityType} onChange={event => setActivityType(event.target.value)}><option value="all">All activity</option>{activityTypes.map(value => <option key={value} value={value}>{titleCase(value)}</option>)}</select></Field>
        <Field label="Imported from"><select className="select" value={file} onChange={event => setFile(event.target.value)}><option value="all">Every file</option>{files.map(value => <option key={value} value={value}>{value || 'No file name'}</option>)}</select></Field>
        <Field label="From"><input className="input" type="date" value={from} max={to || undefined} onChange={event => setFrom(event.target.value)} /></Field>
        <Field label="To"><input className="input" type="date" value={to} min={from || undefined} onChange={event => setTo(event.target.value)} /></Field>
        {reversedCount > 0 && <label className="portfolio-check" title="Hide records the broker later reversed, together with their reversals. What remains adds up to the same cash."><input type="checkbox" checked={hideReversed} onChange={event => setHideReversed(event.target.checked)} /><span>Hide {reversedCount.toLocaleString()} reversed records</span></label>}
      </div>
      <div className="pf-selection-bar" role="status">
        {shownSelected.length
          ? <><strong>{shownSelected.length.toLocaleString()} selected</strong><button type="button" className="text-button" onClick={() => setSelected(new Set())}>Clear selection</button><button type="button" className="button button--danger button--small" onClick={deleteSelected}><Trash2 aria-hidden="true" />Delete {shownSelected.length.toLocaleString()} selected…<kbd aria-hidden="true">Del</kbd></button></>
          : <span className="muted">Select records with their boxes or <kbd>Space</kbd> to delete them.</span>}
        {shownIds.length > 0 && shownSelected.length < shownIds.length && <button type="button" className="text-button" onClick={toggleAll}>Select all {shownIds.length.toLocaleString()} {filtersOn ? 'shown' : 'records'}<span aria-hidden="true"> <kbd>Shift</kbd>+<kbd>A</kbd></span></button>}
      </div>
      {props.isLoading ? <LoadingState label="Loading activity" /> : props.error ? <ErrorState error={props.error} /> : <PortfolioTable
        label="Activity"
        rows={filtered}
        columns={columns}
        rowKey={row => String(row.id ?? `${row.trade_date}-${row.symbol}-${row.amount}`)}
        pageSize={50}
        hotkeys={props.hotkeys && !pending}
        onOpen={row => props.onOpenDetail({ kind: 'transaction', transaction: row })}
        rowKeys={{ ' ': toggle }}
        keysHint={<><kbd>Space</kbd> select</>}
        rowClassName={row => row.id != null && selected.has(row.id) ? 'is-chosen' : undefined}
        initialSort={{ column: 'date', direction: 'desc' }}
        emptyText="No activity matches these filters."
      />}
      {reversedCount > 0 && <p className="pf-footnote">Rows marked Reversal and Corrected are the broker’s own corrections, not duplicates. When it recalculates withholding tax or a fee, it cancels the original record and books the right amount, all under the original date; “booked” shows when that happened.</p>}
    </SectionCard>
    <Imports imports={props.imports} onDelete={setPending} />
    {pending && <DeleteRecordsDialog
      selection={pending}
      records={props.data}
      onCancel={() => setPending(null)}
      onDeleted={result => {
        const done = pending
        setPending(null)
        setSelected(new Set())
        props.onDeleted?.(result, done)
      }}
    />}
  </div>
}
