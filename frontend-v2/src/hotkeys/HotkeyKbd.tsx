import type { Hotkey } from './types'
import { useHotkeyText } from './useHotkeyText'

/** An inline key hint that follows the user's binding. */
export function HotkeyKbd({ hotkey, all = false, className }: { hotkey: Hotkey; all?: boolean; className?: string }) {
  return <kbd className={className}>{useHotkeyText(hotkey, all)}</kbd>
}
