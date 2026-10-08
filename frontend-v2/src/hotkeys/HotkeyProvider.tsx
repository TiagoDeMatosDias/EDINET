import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useCallback, useContext, useMemo, type ReactNode } from 'react'

import { apiRequest } from '../api/client'
import { AuthContext } from '../features/auth/authContext'
import { canonical } from './keys'
import { hotkeySettingsKey, HotkeySettingsContext, type HotkeySettingsValue } from './settingsContext'
import type { HotkeyOverrides, KeySpec } from './types'

const ENDPOINT = '/api/settings/hotkeys'

function compact(overrides: HotkeyOverrides): HotkeyOverrides {
  return Object.fromEntries(Object.entries(overrides).filter(([, keys]) => keys.length).map(([id, keys]) => [id, keys.map(canonical)]))
}

/**
 * Loads the signed-in user's hotkey overrides from the server and shares them
 * with every scope. Without it (tests, signed-out pages) every hotkey uses its
 * default.
 */
export function HotkeyProvider({ children }: { children: ReactNode }) {
  const auth = useContext(AuthContext)
  const local = auth?.status?.mode === 'disabled'
  const enabled = Boolean(auth?.user) || local
  const owner = auth?.user?.user_id ?? (local ? 'local' : 'signed-out')
  const queryKey = useMemo(() => [...hotkeySettingsKey, owner], [owner])
  const queryClient = useQueryClient()
  const query = useQuery({
    queryKey,
    queryFn: () => apiRequest<{ overrides: Record<string, KeySpec[]> }>(ENDPOINT),
    enabled,
    staleTime: Infinity,
    retry: false,
  })
  const write = useMutation({
    mutationFn: (overrides: HotkeyOverrides) => Object.keys(overrides).length
      ? apiRequest<{ overrides: HotkeyOverrides }>(ENDPOINT, { method: 'PUT', body: JSON.stringify({ overrides }) })
      : apiRequest<{ overrides: HotkeyOverrides }>(ENDPOINT, { method: 'DELETE' }),
    // Rebinding takes effect immediately; a failed save restores what the server has.
    onMutate: async overrides => {
      await queryClient.cancelQueries({ queryKey })
      const previous = queryClient.getQueryData(queryKey)
      queryClient.setQueryData(queryKey, { overrides })
      return { previous }
    },
    onError: (_error, _overrides, context) => { queryClient.setQueryData(queryKey, context?.previous) },
    onSuccess: saved => { queryClient.setQueryData(queryKey, saved) },
  })
  const { mutateAsync } = write
  const save = useCallback(async (overrides: HotkeyOverrides) => { await mutateAsync(compact(overrides)) }, [mutateAsync])
  const reset = useCallback(async () => { await mutateAsync({}) }, [mutateAsync])
  const value = useMemo<HotkeySettingsValue>(() => ({
    overrides: enabled ? query.data?.overrides ?? {} : {},
    persistent: enabled,
    loading: enabled && query.isLoading,
    saving: write.isPending,
    error: (write.error as Error | null) ?? (query.error as Error | null),
    save,
    reset,
  }), [enabled, query.data, query.isLoading, query.error, write.isPending, write.error, save, reset])
  return <HotkeySettingsContext.Provider value={value}>{children}</HotkeySettingsContext.Provider>
}
