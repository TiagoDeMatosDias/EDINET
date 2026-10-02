import { AlertCircle, CheckCircle2 } from 'lucide-react'
import { useRef, useState, type KeyboardEvent } from 'react'
import { Link, useNavigate } from 'react-router-dom'

import { Tip } from '../../components/Tooltip'
import { companyName, filingHref, fiscalLabel, formatBytes, formatDay, formLabel, periodSpan } from './filingFormat'
import './filings.css'

export interface FilingRow {
  doc_id: string
  edinet_code?: string | null
  company_name?: string | null
  ticker?: string | null
  submitter_name?: string | null
  period_start?: string | null
  period_end?: string | null
  submitted_at?: string | null
  form_code?: string | null
  archive_size?: number | null
  status: string
  parse_error?: string | null
}

export function FilingStatus({ filing }: { filing: Pick<FilingRow, 'status' | 'parse_error'> }) {
  const failed = Boolean(filing.parse_error) || /fail|error/i.test(filing.status)
  return failed
    ? <Tip content={filing.parse_error || 'Parsing failed for this report.'} className="status status--error"><AlertCircle aria-hidden="true" />{filing.status}</Tip>
    : <span className="status status--ok"><CheckCircle2 aria-hidden="true" />{filing.status}</span>
}

/**
 * Filings as a dense table: one keyboard-navigable row per report (↑/↓ or J/K to
 * move, Enter to open), newest first as the caller orders them.
 */
export function FilingsTable({ filings, showCompany = false, from, label }: { filings: FilingRow[]; showCompany?: boolean; from?: string; label: string }) {
  const navigate = useNavigate()
  const [focusIndex, setFocusIndex] = useState(0)
  const rows = useRef<Array<HTMLTableRowElement | null>>([])
  const safeFocus = Math.min(focusIndex, Math.max(0, filings.length - 1))
  const hrefFor = (filing: FilingRow) => filingHref(filing.doc_id, filing.edinet_code, from)
  const move = (index: number) => {
    const next = Math.max(0, Math.min(filings.length - 1, index))
    setFocusIndex(next)
    rows.current[next]?.focus()
  }
  const onKeyDown = (event: KeyboardEvent<HTMLTableSectionElement>) => {
    if (event.ctrlKey || event.metaKey || event.altKey) return
    const keys: Record<string, () => void> = {
      ArrowDown: () => move(safeFocus + 1), j: () => move(safeFocus + 1),
      ArrowUp: () => move(safeFocus - 1), k: () => move(safeFocus - 1),
      Home: () => move(0), End: () => move(filings.length - 1),
      Enter: () => { const filing = filings[safeFocus]; if (filing) navigate(hrefFor(filing)) },
    }
    const action = keys[event.key]
    if (!action || (event.key === 'Enter' && (event.target as HTMLElement).tagName === 'A')) return
    event.preventDefault()
    event.stopPropagation()
    action()
  }
  return <div className="filings-table-wrap">
    <table className={showCompany ? 'filings-table filings-table--companies' : 'filings-table'} aria-label={label}>
      <thead>
        <tr>
          <th scope="col">Fiscal year</th>
          {showCompany && <th scope="col">Company</th>}
          <th scope="col">Report</th>
          <th scope="col">Submitted</th>
          <th scope="col">Document</th>
          <th scope="col" className="num">Size</th>
          <th scope="col">Status</th>
        </tr>
      </thead>
      <tbody onKeyDown={onKeyDown}>
        {filings.map((filing, index) => {
          const href = hrefFor(filing)
          return <tr
            key={filing.doc_id}
            ref={element => { rows.current[index] = element }}
            tabIndex={index === safeFocus ? 0 : -1}
            onFocus={() => setFocusIndex(index)}
            onClick={event => { if (!(event.target as HTMLElement).closest('a') && !window.getSelection()?.toString()) navigate(href) }}
          >
            <td title={periodSpan(filing.period_start, filing.period_end) || undefined}><Link to={href} className="filings-table__period" tabIndex={-1}>{fiscalLabel(filing.period_end)}</Link><small className="filings-table__span">{periodSpan(filing.period_start, filing.period_end)}</small></td>
            {showCompany && <td className="filings-table__company">
              <span title={filing.submitter_name ?? undefined}>{companyName(filing)}</span>
              <small>{[filing.ticker, filing.edinet_code].filter(Boolean).join(' · ')}</small>
            </td>}
            <td>{formLabel(filing.form_code)}</td>
            <td><span className="mono">{formatDay(filing.submitted_at)}</span><small>{filing.submitted_at?.slice(11, 16)}</small></td>
            <td className="mono">{filing.doc_id}</td>
            <td className="num mono">{formatBytes(filing.archive_size)}</td>
            <td><FilingStatus filing={filing} /></td>
          </tr>
        })}
      </tbody>
    </table>
  </div>
}
