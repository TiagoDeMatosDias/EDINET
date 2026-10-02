import { X } from 'lucide-react'
import { useEffect, useRef } from 'react'

export interface ShortcutGroup {
  title: string
  shortcuts: Array<{ keys: string[]; label: string }>
}

/** A keyboard reference opened with "?"; Escape or the backdrop closes it and focus returns where it was. */
export function ShortcutsDialog({ groups, onClose }: { groups: ShortcutGroup[]; onClose: () => void }) {
  const closeButton = useRef<HTMLButtonElement>(null)
  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null
    closeButton.current?.focus()
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Tab') return
      // While the list is open, page shortcuts underneath stay inert.
      event.stopPropagation()
      if (event.key === 'Escape' || event.key === '?') {
        event.preventDefault()
        onClose()
      }
    }
    window.addEventListener('keydown', onKeyDown, { capture: true })
    return () => {
      window.removeEventListener('keydown', onKeyDown, { capture: true })
      previous?.focus?.()
    }
  }, [onClose])
  return <div className="shortcuts-backdrop" onMouseDown={event => { if (event.target === event.currentTarget) onClose() }}>
    <div className="shortcuts-dialog" role="dialog" aria-modal="true" aria-labelledby="shortcuts-title">
      <header>
        <h2 id="shortcuts-title">Keyboard shortcuts</h2>
        <button ref={closeButton} type="button" className="icon-button" aria-label="Close keyboard shortcuts" onClick={onClose}><X /></button>
      </header>
      <div className="shortcuts-dialog__groups">
        {groups.map(group => <section key={group.title}>
          <h3>{group.title}</h3>
          <dl>{group.shortcuts.map(shortcut => <div key={shortcut.label}><dt>{shortcut.keys.map((key, index) => <span key={key}>{index > 0 && <span className="shortcuts-dialog__or">/</span>}<kbd>{key}</kbd></span>)}</dt><dd>{shortcut.label}</dd></div>)}</dl>
        </section>)}
      </div>
      <p className="shortcuts-dialog__foot">Shortcuts pause while you type in a field. Press <kbd>Esc</kbd> to leave the field.</p>
    </div>
  </div>
}
