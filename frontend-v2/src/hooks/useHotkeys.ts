import { useEffect, useRef } from 'react'

export type HotkeyMap = Record<string, (event: KeyboardEvent) => void>

/** True when a key press belongs to a text field rather than to a page shortcut. */
export function isTypingTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false
  if (target.isContentEditable) return true
  const tag = target.tagName
  if (tag === 'TEXTAREA' || tag === 'SELECT') return true
  if (tag !== 'INPUT') return false
  const type = (target as HTMLInputElement).type
  return !['checkbox', 'radio', 'button', 'submit', 'reset', 'range', 'color'].includes(type)
}

/**
 * Single-key page shortcuts, keyed by ``KeyboardEvent.key`` (``'/'``, ``'?'``, ``'['``, ``'j'``).
 * Shortcuts never fire while typing in a field, with a modifier held (so browser and OS
 * shortcuts keep working), or after another handler has already claimed the event.
 */
export function useHotkeys(bindings: HotkeyMap, enabled = true) {
  const latest = useRef(bindings)
  useEffect(() => { latest.current = bindings })
  useEffect(() => {
    if (!enabled) return
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.defaultPrevented || event.ctrlKey || event.metaKey || event.altKey || event.isComposing) return
      if (isTypingTarget(event.target)) return
      const handler = latest.current[event.key]
      if (!handler) return
      event.preventDefault()
      handler(event)
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [enabled])
}
