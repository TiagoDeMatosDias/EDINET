import { Download } from 'lucide-react'
import { useState, type ReactNode } from 'react'

import { downloadApiFile } from '../api/download'

/** Button that downloads an authenticated API file and reports failures inline. */
export function DownloadButton({
  path,
  filename,
  className = 'button button--secondary',
  children,
}: {
  path: string
  filename: string
  className?: string
  children: ReactNode
}) {
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const download = async () => {
    setBusy(true)
    setError(null)
    try {
      await downloadApiFile(path, filename)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Download failed')
    } finally {
      setBusy(false)
    }
  }

  return <>
    <button type="button" className={className} disabled={busy} onClick={() => void download()}>
      <Download aria-hidden="true" />{busy ? 'Preparing…' : children}
    </button>
    {error && <span className="form-error" role="alert">{error}</span>}
  </>
}
