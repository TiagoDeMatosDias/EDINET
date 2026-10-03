import { useEffect, useRef, type KeyboardEvent, type RefObject } from 'react'

/**
 * Keeps keyboard focus on the cursor row of a list or table body: after a
 * shortcut asks for it (``focusRequest`` changes) or while focus is already in
 * the list, focus follows the cursor and the row scrolls into view.
 */
export function useListCursor<T extends HTMLElement>(cursor: number, focusRequest: number): RefObject<T | null> {
  const container = useRef<T>(null)
  const handled = useRef(focusRequest)
  useEffect(() => {
    const requested = handled.current !== focusRequest
    handled.current = focusRequest
    const element = container.current
    if (!element) return
    const row = element.querySelector<HTMLElement>('[data-cursor="true"]')
    if (!row) return
    if (requested || element.contains(document.activeElement)) {
      row.focus({ preventScroll: true })
      row.scrollIntoView?.({ block: 'nearest' })
    }
  }, [cursor, focusRequest])
  return container
}

/** Arrow keys, Home, and End move a list cursor; returns true when it handled the key. */
export function moveCursorKey(event: KeyboardEvent, cursor: number, count: number, move: (next: number) => void) {
  const next = event.key === 'ArrowDown' ? cursor + 1
    : event.key === 'ArrowUp' ? cursor - 1
      : event.key === 'Home' ? 0
        : event.key === 'End' ? count - 1
          : null
  if (next == null || !count) return false
  event.preventDefault()
  move(Math.max(0, Math.min(count - 1, next)))
  return true
}
