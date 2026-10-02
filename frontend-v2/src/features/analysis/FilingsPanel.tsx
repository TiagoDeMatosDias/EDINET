import { useQuery } from '@tanstack/react-query'
import { AlertCircle, CheckCircle2, Download, ExternalLink } from 'lucide-react'
import { useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'

import { apiRequest, authenticatedFetch } from '../../api/client'
import { downloadBlob } from '../../api/download'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { Tip } from '../../components/Tooltip'
import { useHotkeys } from '../../hooks/useHotkeys'
import { safeFileName } from './downloads'
import { formatBytes, formLabel, periodSpan } from './filingFormat'

export interface FilingSummary {
  doc_id: string
  submitted_at?: string | null
  period_start?: string | null
  period_end?: string | null
  form_code?: string | null
  archive_size?: number | null
  status: string
  parse_error?: string | null
}

const COLLAPSED_ROWS = 10

function filingHref(docId: string, companyCode: string) {
  return `/filings/${encodeURIComponent(docId)}?from=analysis&company=${encodeURIComponent(companyCode)}`
}

export function FilingsPanel({ companyCode }: { companyCode: string }) {
  const navigate = useNavigate()
  const filings = useQuery({ queryKey: ['company-filings', companyCode], queryFn: () => apiRequest<{ filings: FilingSummary[] }>(`/api/filings/company/${encodeURIComponent(companyCode)}`) })
  const [expanded, setExpanded] = useState(false)
  const [exporting, setExporting] = useState(false)
  const [exportError, setExportError] = useState('')
  const rows = filings.data?.filings ?? []
  useHotkeys({ o: () => { if (rows[0]) navigate(filingHref(rows[0].doc_id, companyCode)) } }, rows.length > 0)
  const exportAll = async () => {
    setExporting(true)
    setExportError('')
    try {
      const response = await authenticatedFetch(`/api/filings/company/${encodeURIComponent(companyCode)}/export`)
      if (!response.ok) throw new Error(response.status === 404 ? 'No filing archives are available to export.' : response.status === 401 ? 'Sign in to export filing archives.' : `Export failed (${response.status})`)
      downloadBlob(`${safeFileName(companyCode)}-filings.zip`, await response.blob())
    } catch (error) {
      setExportError(error instanceof Error ? error.message : 'Export failed')
    } finally {
      setExporting(false)
    }
  }
  if (filings.isLoading) return <LoadingState label="Loading filings" />
  if (filings.isError) return <ErrorState error={filings.error} retry={() => void filings.refetch()} />
  if (!rows.length) return <EmptyState title="No retained XBRL reports" description="Annual reports appear here once the filing pipeline has acquired them." action={<Link className="button button--ghost button--small" to={`/filings?company=${encodeURIComponent(companyCode)}&from=analysis`}>Open Filing Explorer</Link>} />
  const shown = expanded ? rows : rows.slice(0, COLLAPSED_ROWS)
  return <div className="filings-panel">
    <div className="filings-panel__actions">
      <span className="muted">{rows.length} retained {rows.length === 1 ? 'report' : 'reports'} · newest first · <kbd>O</kbd> opens the latest</span>
      <span className="statement-toolbar__spacer" />
      <Link className="button button--ghost button--small" to={`/filings?company=${encodeURIComponent(companyCode)}&from=analysis`}><ExternalLink aria-hidden="true" />Filing Explorer</Link>
      <button type="button" className="button button--ghost button--small" disabled={exporting} onClick={() => void exportAll()} title="Download a ZIP of every retained filing archive with a manifest"><Download aria-hidden="true" />{exporting ? 'Preparing…' : 'Export all'}</button>
    </div>
    {exportError && <p className="form-error" role="alert">{exportError}</p>}
    <div className="filings-table-wrap">
      <table className="filings-table">
        <thead>
          <tr>
            <th scope="col">Fiscal year</th>
            <th scope="col">Report</th>
            <th scope="col">Submitted</th>
            <th scope="col">Document</th>
            <th scope="col" className="num">Size</th>
            <th scope="col">Status</th>
          </tr>
        </thead>
        <tbody>
          {shown.map(filing => {
            const href = filingHref(filing.doc_id, companyCode)
            const failed = Boolean(filing.parse_error) || /fail|error/i.test(filing.status)
            return <tr key={filing.doc_id} onClick={event => { if (!(event.target as HTMLElement).closest('a') && !window.getSelection()?.toString()) navigate(href) }}>
              <td><Link to={href} className="filings-table__period">{filing.period_end?.slice(0, 7) || 'Period unavailable'}</Link><small>{periodSpan(filing.period_start, filing.period_end)}</small></td>
              <td>{formLabel(filing.form_code)}</td>
              <td className="mono">{filing.submitted_at?.slice(0, 16) || '—'}</td>
              <td className="mono">{filing.doc_id}</td>
              <td className="num mono">{formatBytes(filing.archive_size)}</td>
              <td>{failed
                ? <Tip content={filing.parse_error || 'Parsing failed for this report.'} className="status status--error"><AlertCircle aria-hidden="true" />{filing.status}</Tip>
                : <span className="status status--ok"><CheckCircle2 aria-hidden="true" />{filing.status}</span>}</td>
            </tr>
          })}
        </tbody>
      </table>
    </div>
    {rows.length > COLLAPSED_ROWS && <button type="button" className="text-button filings-panel__more" onClick={() => setExpanded(!expanded)}>{expanded ? 'Show fewer' : `Show all ${rows.length} reports`}</button>}
  </div>
}
