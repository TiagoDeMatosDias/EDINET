import { useMemo, useSyncExternalStore } from 'react'

import { ShortcutsDialog } from '../components/ShortcutsDialog'
import { globalScope } from './globalScopes'
import { formatKey } from './keys'
import { activeScopes, effectiveKeys, getScope, subscribeActiveScopes } from './registry'
import { scopeGroups } from './helpGroups'
import { useHotkeyOverrides } from './settingsContext'
import type { Scope } from './types'

function depth(scope: Scope): number {
  let count = 0
  let parent = scope.parent
  while (parent && count < 10) { count += 1; parent = getScope(parent)?.parent }
  return count
}

/** The "?" list: every scope mounted right now (outermost first), then the keys that work anywhere. */
export function HotkeyHelpDialog({ onClose }: { onClose: () => void }) {
  const mounted = useSyncExternalStore(subscribeActiveScopes, activeScopes)
  const overrides = useHotkeyOverrides()
  const groups = useMemo(() => {
    const unique = [...new Map(mounted.filter(scope => scope.id !== globalScope.id).map(scope => [scope.id, scope])).values()]
    unique.sort((a, b) => depth(a) - depth(b))
    return scopeGroups([...unique, globalScope as Scope], overrides)
  }, [mounted, overrides])
  const gotoKey = formatKey(effectiveKeys(globalScope.byId.goto, overrides)[0])
  return <ShortcutsDialog groups={groups} onClose={onClose} gotoKey={gotoKey} />
}
