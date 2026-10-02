import { useEffect, useId, useLayoutEffect, useRef, useState, type ReactNode } from 'react'
import { createPortal } from 'react-dom'

const GUTTER = 8
const HOVER_DELAY_MS = 180

function Bubble({ id, anchor, children }: { id: string; anchor: DOMRect; children: ReactNode }) {
  const ref = useRef<HTMLDivElement>(null)
  const [position, setPosition] = useState<{ left: number; top: number } | null>(null)
  useLayoutEffect(() => {
    const bubble = ref.current
    if (!bubble) return
    const { width, height } = bubble.getBoundingClientRect()
    const left = Math.min(Math.max(GUTTER, anchor.left), window.innerWidth - width - GUTTER)
    const below = anchor.bottom + 6
    const top = below + height > window.innerHeight - GUTTER ? Math.max(GUTTER, anchor.top - height - 6) : below
    setPosition({ left, top })
  }, [anchor])
  return <div ref={ref} id={id} role="tooltip" className="tip-bubble" style={position ?? { left: -9999, top: -9999 }}>{children}</div>
}

/**
 * An explanation shown on hover and on keyboard focus. The bubble is portalled to the
 * body so scrolling tables and clipped cards never cut it off.
 */
export function Tip({ content, children, className = '', focusable = true }: { content: ReactNode; children: ReactNode; className?: string; focusable?: boolean }) {
  const id = useId()
  const ref = useRef<HTMLSpanElement>(null)
  const timer = useRef<number | undefined>(undefined)
  const [anchor, setAnchor] = useState<DOMRect | null>(null)
  useEffect(() => () => window.clearTimeout(timer.current), [])
  useEffect(() => {
    if (!anchor) return
    // The bubble is placed against the anchor's position when shown; scrolling would strand it.
    const dismiss = () => setAnchor(null)
    window.addEventListener('scroll', dismiss, { capture: true, passive: true })
    return () => window.removeEventListener('scroll', dismiss, { capture: true })
  }, [anchor])
  if (!content) return <>{children}</>
  const show = (delay: number) => {
    window.clearTimeout(timer.current)
    timer.current = window.setTimeout(() => setAnchor(ref.current?.getBoundingClientRect() ?? null), delay)
  }
  const hide = () => { window.clearTimeout(timer.current); setAnchor(null) }
  return <>
    <span
      ref={ref}
      className={`tip ${className}`}
      tabIndex={focusable ? 0 : undefined}
      aria-describedby={anchor ? id : undefined}
      onMouseEnter={() => show(HOVER_DELAY_MS)}
      onMouseLeave={hide}
      onFocus={() => show(0)}
      onBlur={hide}
      onKeyDown={event => { if (event.key === 'Escape') hide() }}
    >{children}</span>
    {anchor && createPortal(<Bubble id={id} anchor={anchor}>{content}</Bubble>, document.body)}
  </>
}
