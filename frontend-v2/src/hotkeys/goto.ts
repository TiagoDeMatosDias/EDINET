import { useMemo } from 'react'

import { pageShortcuts, type PageShortcut } from '../components/pageShortcuts'
import { gotoScope } from './globalScopes'
import { formatKey } from './keys'
import { effectiveKeys } from './registry'
import { useHotkeyOverrides } from './settingsContext'
import type { KeySpec } from './types'

export interface GotoPage extends PageShortcut { keys: KeySpec[]; hint: string }

/** "G then a letter" destinations this viewer can reach, with their current keys. */
export function useGotoPages(isAdmin: boolean, signedIn = true): GotoPage[] {
  const overrides = useHotkeyOverrides()
  return useMemo(() => pageShortcuts(isAdmin, signedIn).map(page => {
    const keys = effectiveKeys(gotoScope.byId[page.id], overrides)
    return { ...page, keys, hint: formatKey(keys[0]) }
  }), [isAdmin, signedIn, overrides])
}
