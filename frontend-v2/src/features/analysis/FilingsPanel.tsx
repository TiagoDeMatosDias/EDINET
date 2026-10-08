import { useQuery } from '@tanstack/react-query'
import { Download, ExternalLink } from 'lucide-react'
import { useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'

import { apiRequest } from '../../api/client'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { HotkeyKbd } from '../../hotkeys/HotkeyKbd'
import { useHotkeyScope } from '../../hotkeys/useHotkeyScope'
import { filingsPanelScope } from './analysisHotkeys'
import { exportCompanyFilings } from '../filings/exportFilings'
import { filingHref } from '../filings/filingFormat'
import { FilingsTable, type FilingRow } from '../filings/FilingsTable'

const COLLAPSED_ROWS = 10

export function FilingsPanel({ companyCode }: { companyCode: string }) {
  const navigate = useNavigate()
  const filings = useQuery({ queryKey: ['company-filings', companyCode], queryFn: () => apiRequest<{ filings: FilingRow[] }>(`/api/filings/company/${encodeURIComponent(companyCode)}`) })
  const [expanded, setExpanded] = useState(false)
  const [exporting, setExporting] = useState(false)
  const [exportError, setExportError] = useState('')
  const rows = filings.data?.filings ?? []
  useHotkeyScope(filingsPanelScope, { 'open-latest': () => { if (rows[0]) navigate(filingHref(rows[0].doc_id, companyCode, 'analysis')) } }, { enabled: rows.length > 0 })
  const exportAll = async () => {
    setExporting(true)
    setExportError('')
    try {
      await exportCompanyFilings(companyCode)
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
      <span className="muted">{rows.length} retained {rows.length === 1 ? 'report' : 'reports'} · newest first · <HotkeyKbd hotkey={filingsPanelScope.byId['open-latest']} /> opens the latest</span>
      <span className="statement-toolbar__spacer" />
      <Link className="button button--ghost button--small" to={`/filings?company=${encodeURIComponent(companyCode)}&from=analysis`}><ExternalLink aria-hidden="true" />Filing Explorer</Link>
      <button type="button" className="button button--ghost button--small" disabled={exporting} onClick={() => void exportAll()} title="Download a ZIP of every retained filing archive with a manifest"><Download aria-hidden="true" />{exporting ? 'Preparing…' : 'Export all'}</button>
    </div>
    {exportError && <p className="form-error" role="alert">{exportError}</p>}
    <FilingsTable filings={shown} from="analysis" label="Retained filings" />
    {rows.length > COLLAPSED_ROWS && <button type="button" className="text-button filings-panel__more" onClick={() => setExpanded(!expanded)}>{expanded ? 'Show fewer' : `Show all ${rows.length} reports`}</button>}
  </div>
}
