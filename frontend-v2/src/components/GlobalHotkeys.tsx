import { useCallback, useEffect, useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'

import { globalScope } from '../hotkeys/globalScopes'
import { useGotoPages } from '../hotkeys/goto'
import { HotkeyHelpDialog } from '../hotkeys/HotkeyHelpDialog'
import { isTypingTarget } from '../hotkeys/isTypingTarget'
import { keyFromEvent, keysMatch } from '../hotkeys/keys'
import { onHotkeyHelpRequest } from '../hotkeys/registry'
import { useHotkeyScope } from '../hotkeys/useHotkeyScope'

const PENDING_MS = 3000

const modalOpen = () => Boolean(document.querySelector('[aria-modal="true"]'))

/**
 * Shortcuts that work on every page. "G then a letter" goes to a page, as in
 * Gmail or GitHub: after G a hint lists the destinations, and Esc or any other
 * key cancels. "?" lists the shortcuts of the screen in view.
 */
export function GlobalHotkeys({ isAdmin, signedIn = true, onPendingChange }: { isAdmin: boolean; signedIn?: boolean; onPendingChange?: (pending: boolean) => void }) {
  const navigate = useNavigate()
  const location = useLocation()
  const [pending, setPending] = useState(false)
  const [showHelp, setShowHelp] = useState(false)
  const pages = useGotoPages(isAdmin, signedIn)
  const closeHelp = useCallback(() => setShowHelp(false), [])

  useHotkeyScope(globalScope, {
    goto: () => { if (!modalOpen()) setPending(true) },
    help: () => { if (!modalOpen()) setShowHelp(true) },
  }, { enabled: !showHelp })

  useEffect(() => onHotkeyHelpRequest(() => setShowHelp(true)), [])

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      // Shift+Tab leaves the focused field so the page shortcuts work again.
      if (event.key === 'Tab' && event.shiftKey && !event.ctrlKey && !event.metaKey && !event.altKey && isTypingTarget(event.target) && !modalOpen()) {
        event.preventDefault()
        ;(event.target as HTMLElement).blur()
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [])

  useEffect(() => { onPendingChange?.(pending) }, [onPendingChange, pending])

  useEffect(() => {
    if (!pending) return
    const timer = window.setTimeout(() => setPending(false), PENDING_MS)
    // The second key is read in the capture phase, before any page shortcut can act on it.
    const onKeyDown = (event: KeyboardEvent) => {
      if (['Shift', 'Control', 'Alt', 'Meta'].includes(event.key)) return
      setPending(false)
      const pressed = keyFromEvent(event)
      const page = pages.find(item => item.keys.some(spec => keysMatch(spec, pressed)))
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
      {pages.map(page => <span key={page.id} className={location.pathname.startsWith(page.to) ? 'is-current' : undefined}><kbd>{page.hint}</kbd>{page.label}</span>)}
      <span className="goto-hint__cancel"><kbd>Esc</kbd>cancel</span>
    </div>}
    {showHelp && <HotkeyHelpDialog onClose={closeHelp} />}
  </>
}
