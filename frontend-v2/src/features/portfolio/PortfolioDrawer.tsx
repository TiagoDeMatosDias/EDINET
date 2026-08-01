import { X } from 'lucide-react'
import { useEffect, useRef, type ReactNode } from 'react'

type Props = {
  open: boolean
  eyebrow?: string
  title: string
  description?: string
  children: ReactNode
  onClose: () => void
}

export function PortfolioDrawer({ open, eyebrow, title, description, children, onClose }: Props) {
  const closeRef = useRef<HTMLButtonElement>(null)

  useEffect(() => {
    if (!open) return
    closeRef.current?.focus()
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [onClose, open])

  if (!open) return null
  return <div className="portfolio-drawer-layer" role="presentation" onMouseDown={event => {
    if (event.target === event.currentTarget) onClose()
  }}>
    <aside className="portfolio-drawer" role="dialog" aria-modal="true" aria-labelledby="portfolio-drawer-title">
      <header className="portfolio-drawer-header">
        <div>
          {eyebrow && <span className="eyebrow">{eyebrow}</span>}
          <h2 id="portfolio-drawer-title">{title}</h2>
          {description && <p>{description}</p>}
        </div>
        <button ref={closeRef} className="icon-button" aria-label="Close details" onClick={onClose}><X /></button>
      </header>
      <div className="portfolio-drawer-body">{children}</div>
    </aside>
  </div>
}
