import { Keyboard } from 'lucide-react'

import { globalScope } from './globalScopes'
import { useHotkeyText } from './useHotkeyText'
import { openHotkeyHelp } from './registry'

/** The keyboard icon in a page header: opens the same list as "?". */
export function HotkeyHelpButton({ className = 'icon-button' }: { className?: string }) {
  const key = useHotkeyText(globalScope.byId.help)
  return <button type="button" className={className} onClick={openHotkeyHelp} title={`Keyboard shortcuts (${key})`} aria-label="Keyboard shortcuts"><Keyboard /></button>
}
