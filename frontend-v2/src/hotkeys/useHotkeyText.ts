import { formatKey, formatKeys } from './keys'
import { effectiveKeys } from './registry'
import { useHotkeyOverrides } from './settingsContext'
import type { Hotkey, KeySpec } from './types'

/** The keys a hotkey answers to right now: the user's override, else its defaults. */
export function useHotkeyKeys(hotkey: Hotkey): KeySpec[] {
  return effectiveKeys(hotkey, useHotkeyOverrides())
}

/** ``'Shift+N'``; with ``all``, every equivalent key (``'J / ↓'``). */
export function useHotkeyText(hotkey: Hotkey, all = false): string {
  const keys = useHotkeyKeys(hotkey)
  return all ? formatKeys(keys) : formatKey(keys[0])
}
