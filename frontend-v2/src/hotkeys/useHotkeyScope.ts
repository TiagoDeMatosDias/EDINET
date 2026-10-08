import { useEffect, useRef } from 'react'

import { isTypingTarget } from './isTypingTarget'
import { isModifierOnly, keyFromEvent, keysMatch } from './keys'
import { activateScope, effectiveKeys } from './registry'
import { useHotkeyOverrides } from './settingsContext'
import type { HotkeyHandler, Scope } from './types'

export type ScopeHandlers<Id extends string> = Partial<Record<Id, HotkeyHandler>>

/**
 * Mount a scope: while enabled, its keys (default or user override) run the
 * matching handler. Hotkeys without a handler are listed but inert.
 *
 * Keys never fire while typing in a field (unless the hotkey is marked
 * ``whileTyping``), with a modifier the binding does not name, or after another
 * listener claimed the event. Each scope listens on
 * ``window`` and the first match calls ``preventDefault``; React mounts inner
 * components first, so a tab's keys win over the page that contains it.
 */
export function useHotkeyScope<Id extends string>(scope: Scope<Id>, handlers: ScopeHandlers<Id>, options: { enabled?: boolean } = {}) {
  const enabled = options.enabled ?? true
  const overrides = useHotkeyOverrides()
  const latest = useRef({ handlers, overrides })
  useEffect(() => { latest.current = { handlers, overrides } })
  useEffect(() => {
    if (!enabled) return
    const release = activateScope(scope as Scope)
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.defaultPrevented || event.isComposing) return
      const typing = isTypingTarget(event.target)
      const pressed = keyFromEvent(event)
      if (isModifierOnly(pressed)) return
      const current = latest.current
      for (const hotkey of scope.hotkeys) {
        const handler = current.handlers[hotkey.id as Id]
        if (!handler || hotkey.fixed || (typing && !hotkey.whileTyping)) continue
        if (effectiveKeys(hotkey, current.overrides).some(spec => keysMatch(spec, pressed))) {
          event.preventDefault()
          handler(event)
          return
        }
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => {
      window.removeEventListener('keydown', onKeyDown)
      release()
    }
  }, [enabled, scope])
}
