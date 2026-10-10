import { useMutation } from '@tanstack/react-query'
import { AlertTriangle, Power } from 'lucide-react'
import { useEffect, useRef, useState, type KeyboardEvent } from 'react'
import { createPortal } from 'react-dom'

import { apiPost } from '../../api/client'
import { useSystemStatus } from '../../hooks/useHealth'

/** The Admin page's "Shut down server" button, which asks before stopping anything. */
export function ShutdownServer() {
  const [open, setOpen] = useState(false)
  return <>
    <button type="button" className="button button--danger button--small" title="Stop the workstation for everyone" onClick={() => setOpen(true)}><Power aria-hidden="true" />Shut down server</button>
    {open && createPortal(<ShutdownDialog onCancel={() => setOpen(false)} />, document.body)}
  </>
}

/**
 * Confirms shutting the server down. Once it has stopped, nothing on the page
 * can load, so the dialog stays open and says how to bring the workstation
 * back. Esc or Cancel closes it before that without changing anything.
 */
function ShutdownDialog({ onCancel }: { onCancel: () => void }) {
  const cancel = useRef<HTMLButtonElement>(null)
  const reload = useRef<HTMLButtonElement>(null)
  const [stillStopped, setStillStopped] = useState(false)
  const system = useSystemStatus(true)
  const shutdown = useMutation({ mutationFn: () => apiPost('/api/admin/server/shutdown', { confirm: true }) })
  const stopped = shutdown.isSuccess
  const busy = shutdown.isPending
  const activeJobs = system.data?.jobs.active ?? 0

  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null
    cancel.current?.focus()
    return () => previous?.focus?.()
  }, [])
  useEffect(() => { if (stopped) reload.current?.focus() }, [stopped])

  const reloadPage = async () => {
    // Reloading while the server is down would replace this page with the browser's error page.
    const running = await fetch('/health').then(response => response.ok, () => false)
    if (running) window.location.reload()
    else setStillStopped(true)
  }
  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    // Page shortcuts stay inert while the dialog is open.
    if (event.key !== 'Tab') event.stopPropagation()
    if (event.key === 'Escape' && !busy && !stopped) onCancel()
  }

  return <div className="shortcuts-backdrop" onMouseDown={event => { if (event.target === event.currentTarget && !busy && !stopped) onCancel() }}>
    <div className="console-dialog" role="alertdialog" aria-modal="true" aria-labelledby="shutdown-title" aria-describedby="shutdown-summary" onKeyDown={onKeyDown}>
      <header><h2 id="shutdown-title"><Power aria-hidden="true" />{stopped ? 'The server has shut down' : 'Shut down the server?'}</h2></header>
      <div id="shutdown-summary" className="console-dialog__body">
        {stopped ? <>
          <p>Start Shade Research again on the machine it runs on, then reload this page.</p>
          {stillStopped && <p className="console-warn" role="status">The server is not running yet.</p>}
        </> : <>
          <p>The workstation stops for everyone using it, and scheduled pipeline runs do not start while it is off. It can only be started again on the machine it runs on, not from a browser.</p>
          {activeJobs > 0 && <p className="console-warn"><AlertTriangle aria-hidden="true" /> {activeJobs === 1 ? 'A pipeline job is active. It' : `${activeJobs} pipeline jobs are active. They`} will be interrupted and must be run again.</p>}
          {shutdown.isError && <p className="form-error" role="alert">{shutdown.error instanceof Error ? shutdown.error.message : 'The server could not be shut down.'}</p>}
        </>}
      </div>
      <footer>
        {stopped ? <button ref={reload} type="button" className="button button--secondary button--small" onClick={() => { setStillStopped(false); void reloadPage() }}>Reload page</button> : <>
          <button ref={cancel} type="button" className="button button--secondary button--small" onClick={onCancel} disabled={busy}>Cancel</button>
          <button type="button" className="button button--danger button--small" onClick={() => shutdown.mutate()} disabled={busy}>{busy ? 'Shutting down…' : 'Shut down'}</button>
        </>}
      </footer>
    </div>
  </div>
}
