import type { ShortcutGroup } from '../components/ShortcutsDialog'
import { formatKey } from './keys'
import { effectiveKeys } from './registry'
import type { HotkeyOverrides, Scope } from './types'

/** One group per scope (split by the hotkey's group), keys as the user has them bound. */
export function scopeGroups(scopes: Scope[], overrides: HotkeyOverrides): ShortcutGroup[] {
  return scopes.flatMap(scope => {
    const byGroup = new Map<string, ShortcutGroup['shortcuts']>()
    for (const hotkey of scope.hotkeys) {
      const title = hotkey.group ? `${scope.label} · ${hotkey.group}` : scope.label
      const list = byGroup.get(title) ?? []
      list.push({ keys: effectiveKeys(hotkey, overrides).map(formatKey), label: hotkey.label })
      byGroup.set(title, list)
    }
    return [...byGroup.entries()].map(([title, shortcuts]) => ({ title, shortcuts }))
  })
}
