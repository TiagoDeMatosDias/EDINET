import { useMutation, useQuery } from '@tanstack/react-query'
import { AlertTriangle, Download, Trash2, X } from 'lucide-react'
import { useEffect, useRef, useState, type KeyboardEvent } from 'react'

import { apiPost } from '../../api/client'
import { downloadTextFile } from '../analysis/downloads'
import { formatDay, titleCase } from './portfolioFormat'
import type { Transaction } from './portfolioTypes'
import { recordsCsv, selectedRecords, selectionPayload, selectionTitle, type DeletePreview, type DeleteResult, type DeleteSelection } from './recordsDelete'

const CONFIRM_WORD = 'delete'

/**
 * Confirms deleting portfolio records. It shows exactly what the server would
 * remove, offers the records as CSV first, and for clearing everything asks for
 * the word "delete". Holdings and history are rebuilt from what remains.
 * Esc or Cancel closes without changing anything.
 */
export function DeleteRecordsDialog({ selection, records, onCancel, onDeleted }: {
  selection: DeleteSelection
  records: Transaction[]
  onCancel: () => void
  onDeleted: (result: DeleteResult) => void
}) {
  const cancel = useRef<HTMLButtonElement>(null)
  const [typed, setTyped] = useState('')
  const payload = selectionPayload(selection)
  const preview = useQuery({
    queryKey: ['portfolio-delete-preview', payload],
    queryFn: () => apiPost<{ preview: DeletePreview }>('/api/portfolio/transactions/delete', payload),
    retry: false,
    gcTime: 0,
  })
  const remove = useMutation({
    mutationFn: () => apiPost<DeleteResult>('/api/portfolio/transactions/delete', { ...payload, confirm: true }),
    onSuccess: onDeleted,
  })
  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null
    cancel.current?.focus()
    return () => previous?.focus?.()
  }, [])
  const summary = preview.data?.preview
  const everything = selection.kind === 'everything'
  const ready = Boolean(summary?.records) && (!everything || typed.trim().toLowerCase() === CONFIRM_WORD) && !remove.isPending
  const local = selectedRecords(records, selection)
  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    // Page shortcuts stay inert while the dialog is open.
    if (event.key !== 'Tab') event.stopPropagation()
    if (event.key === 'Escape' && !remove.isPending) onCancel()
  }

  return <div className="shortcuts-backdrop" onMouseDown={event => { if (event.target === event.currentTarget && !remove.isPending) onCancel() }}>
    <div className="pf-dialog" role="alertdialog" aria-modal="true" aria-labelledby="delete-records-title" aria-describedby="delete-records-summary" onKeyDown={onKeyDown}>
      <header>
        <h2 id="delete-records-title"><Trash2 aria-hidden="true" />{selectionTitle(selection)}</h2>
        <button type="button" className="icon-button" aria-label="Close" onClick={onCancel} disabled={remove.isPending}><X /></button>
      </header>
      <div id="delete-records-summary" className="pf-dialog__body">
        {preview.isLoading ? <p>Checking what would be deleted…</p> : preview.isError ? <p className="pf-issue is-error">{preview.error instanceof Error ? preview.error.message : 'The records could not be checked.'}</p> : summary && <>
          <p className="pf-dialog__lead">This permanently deletes <strong>{summary.records.toLocaleString()} record{summary.records === 1 ? '' : 's'}</strong>{summary.first_date ? <> dated {formatDay(summary.first_date)}{summary.last_date !== summary.first_date ? <> to {formatDay(summary.last_date)}</> : null}</> : null}{summary.symbols ? `, across ${summary.symbols} symbol${summary.symbols === 1 ? '' : 's'}` : ''}. {summary.remaining ? `${summary.remaining.toLocaleString()} records stay.` : 'No records will remain.'}</p>
          <dl className="pf-dialog__types">{Object.entries(summary.by_type).map(([type, count]) => <div key={type}><dt>{titleCase(type)}</dt><dd>{count.toLocaleString()}</dd></div>)}</dl>
          {summary.source_files.length > 0 && <p className="pf-footnote">From {summary.source_files.map(file => file || 'records without a file name').join(', ')}.</p>}
          <p className="pf-issue is-warning"><AlertTriangle aria-hidden="true" /><span>Holdings, values, and returns are rebuilt from the records that remain. To restore deleted records, import the original Flex Query files again.</span></p>
          {everything && <label className="pf-dialog__confirm">Type <kbd>{CONFIRM_WORD}</kbd> to clear everything <input className="input" value={typed} autoComplete="off" aria-label={`Type ${CONFIRM_WORD} to confirm`} onChange={event => setTyped(event.target.value)} onKeyDown={event => { if (event.key === 'Enter' && ready) remove.mutate() }} /></label>}
          {remove.isError && <p className="pf-issue is-error">{remove.error instanceof Error ? remove.error.message : 'The records could not be deleted.'}</p>}
        </>}
      </div>
      <footer>
        <button type="button" className="button button--ghost button--small" disabled={!local.length} onClick={() => downloadTextFile(`portfolio-records-${new Date().toISOString().slice(0, 10)}.csv`, recordsCsv(local), 'text/csv;charset=utf-8')} title="Save the records as a spreadsheet before deleting them"><Download aria-hidden="true" />Download {local.length.toLocaleString()} as CSV</button>
        <span className="pf-dialog__spacer" />
        <button ref={cancel} type="button" className="button button--secondary button--small" onClick={onCancel} disabled={remove.isPending}>Cancel</button>
        <button type="button" className="button button--danger button--small" disabled={!ready} onClick={() => remove.mutate()}>{remove.isPending ? 'Deleting…' : summary ? `Delete ${summary.records.toLocaleString()} record${summary.records === 1 ? '' : 's'}` : 'Delete'}</button>
      </footer>
    </div>
  </div>
}
