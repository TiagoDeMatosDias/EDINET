import { ExternalLink } from 'lucide-react'
import { useState } from 'react'

import { openApiPage } from '../../api/download'
import { DownloadButton } from '../../components/DownloadButton'

/** Open the shareable HTML report, save it, or download everything as a ZIP. */
export function ReportActions({ id }: { id: string }) {
  const [error, setError] = useState<string | null>(null)
  const path = `/api/backtesting/report/${encodeURIComponent(id)}`
  return <div className="bt-report-actions">
    <button type="button" className="button button--ghost button--small" title="Open the report in a new tab" onClick={() => { setError(null); openApiPage(`${path}?inline=true`).catch(err => setError(err instanceof Error ? err.message : 'Could not open the report')) }}><ExternalLink aria-hidden="true" />Report</button>
    <DownloadButton className="button button--ghost button--small" path={path} filename={`backtest_${id}.html`}>Share HTML</DownloadButton>
    <DownloadButton className="button button--ghost button--small" path={`/api/backtesting/download/${encodeURIComponent(id)}`} filename={`backtest_${id}.zip`}>ZIP</DownloadButton>
    {error && <span className="form-error" role="alert">{error}</span>}
  </div>
}
