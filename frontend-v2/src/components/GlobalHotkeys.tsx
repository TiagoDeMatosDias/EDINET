import { useEffect, useMemo, useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'

import { isTypingTarget } from '../hooks/useHotkeys'
import { pageShortcuts } from './pageShortcuts'
import { ShortcutsDialog, type ShortcutGroup } from './ShortcutsDialog'

const PENDING_MS = 3000
const ANYWHERE: ShortcutGroup[] = [{ title: 'Anywhere', shortcuts: [
  { keys: ['/'], label: 'Search companies' },
  { keys: ['?'], label: 'Show or hide this list' },
  { keys: ['Esc'], label: 'Close a menu or leave a field' },
] }]

const modalOpen = () => Boolean(document.querySelector('[aria-modal="true"]'))

/**
 * Shortcuts that work on every page. "G then a letter" goes to a page, as in
 * Gmail or GitHub: after G a hint lists the destinations, and Esc or any other
 * key cancels. "?" lists shortcuts on pages that have no list of their own.
 */
export function GlobalHotkeys({ isAdmin }: { isAdmin: boolean }) {
  const navigate = useNavigate()
  const location = useLocation()
  const [pending, setPending] = useState(false)
  const [showHelp, setShowHelp] = useState(false)
  const pages = useMemo(() => pageShortcuts(isAdmin), [isAdmin])

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.ctrlKey || event.metaKey || event.altKey || event.isComposing || isTypingTarget(event.target) || modalOpen()) return
      if (event.key === 'g' && !event.defaultPrevented) {
        event.preventDefault()
        setPending(true)
      } else if (event.key === '?') {
        // A page with its own list claims "?" during this dispatch; otherwise show the global one.
        window.setTimeout(() => { if (!event.defaultPrevented) setShowHelp(true) })
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [])

  useEffect(() => {
    if (!pending) return
    const timer = window.setTimeout(() => setPending(false), PENDING_MS)
    // The second key is read in the capture phase, before any page shortcut can act on it.
    const onKeyDown = (event: KeyboardEvent) => {
      if (['Shift', 'Control', 'Alt', 'Meta'].includes(event.key)) return
      setPending(false)
      const page = event.ctrlKey || event.metaKey || event.altKey ? undefined : pages.find(item => item.key === event.key.toLowerCase())
      if (!page && event.key !== 'Escape') return
      event.preventDefault()
      event.stopPropagation()
      if (page) navigate(page.to)
    }
    const cancel = () => setPending(false)
    window.addEventListener('keydown', onKeyDown, { capture: true })
    window.addEventListener('mousedown', cancel)
    return () => {
      window.clearTimeout(timer)
      window.removeEventListener('keydown', onKeyDown, { capture: true })
      window.removeEventListener('mousedown', cancel)
    }
  }, [navigate, pages, pending])

  return <>
    {pending && <div className="goto-hint" role="status" aria-live="polite">
      <strong>Go to</strong>
      {pages.map(page => <span key={page.key} className={location.pathname.startsWith(page.to) ? 'is-current' : undefined}><kbd>{page.key.toUpperCase()}</kbd>{page.label}</span>)}
      <span className="goto-hint__cancel"><kbd>Esc</kbd>cancel</span>
    </div>}
    {showHelp && <ShortcutsDialog groups={ANYWHERE} onClose={() => setShowHelp(false)} />}
  </>
}
