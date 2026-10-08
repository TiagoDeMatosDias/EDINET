import { createContext, useContext } from 'react'

import type { HotkeyOverrides } from './types'

export const hotkeySettingsKey = ['settings', 'hotkeys'] as const

export interface HotkeySettingsValue {
  overrides: HotkeyOverrides
  /** True when overrides are saved against an account (or the local workspace user). */
  persistent: boolean
  loading: boolean
  saving: boolean
  error: Error | null
  /** Replace the whole override map; takes effect at once and is saved to the server. */
  save: (overrides: HotkeyOverrides) => Promise<void>
  reset: () => Promise<void>
}

const NO_SETTINGS: HotkeySettingsValue = {
  overrides: {}, persistent: false, loading: false, saving: false, error: null,
  save: async () => undefined, reset: async () => undefined,
}

/** Without a ``HotkeyProvider`` (tests, signed-out pages) every hotkey uses its default. */
export const HotkeySettingsContext = createContext<HotkeySettingsValue>(NO_SETTINGS)

export const useHotkeySettings = () => useContext(HotkeySettingsContext)

export const useHotkeyOverrides = () => useContext(HotkeySettingsContext).overrides
