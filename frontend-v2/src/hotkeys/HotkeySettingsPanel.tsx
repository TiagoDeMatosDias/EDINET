import { RotateCcw, Search } from 'lucide-react'
import { useEffect, useMemo, useRef, useState } from 'react'

import './catalog'
import './hotkeys.css'
import { canonical, formatKey, isModifierOnly, keyChips, keyFromEvent, reservedReason, sameKey } from './keys'
import { allScopes, effectiveKeys, findConflicts, isChord, isCustomized } from './registry'
import { useHotkeySettings } from './settingsContext'
import type { Hotkey, HotkeyOverrides, KeySpec, Scope } from './types'

function Keys({ keys }: { keys: KeySpec[] }) {
  return <span className="hk-keys">{keys.map((spec, index) => <span key={formatKey(spec)} className="hk-key">
    {index > 0 && <span className="hk-or">or</span>}
    {keyChips(spec).map((chip, chipIndex) => <span key={chip}>{chipIndex > 0 && <span className="hk-plus">+</span>}<kbd>{chip}</kbd></span>)}
  </span>)}</span>
}

interface CaptureState { hotkey: Hotkey; message: string | null }

function HotkeyRow({ hotkey, scopeLabel, overrides, capturing, onCapture, onReset }: {
  hotkey: Hotkey
  scopeLabel: string
  overrides: HotkeyOverrides
  capturing: CaptureState | null
  onCapture: (hotkey: Hotkey | null) => void
  onReset: (hotkey: Hotkey) => void
}) {
  const custom = isCustomized(hotkey, overrides)
  const active = capturing?.hotkey.fullId === hotkey.fullId
  const box = useRef<HTMLDivElement>(null)
  useEffect(() => { if (active) box.current?.focus() }, [active])
  return <tr className={active ? 'is-capturing' : undefined} aria-label={`${hotkey.label} (${scopeLabel})`}>
    <th scope="row">{hotkey.label}{hotkey.description && <small>{hotkey.description}</small>}</th>
    <td>
      {active
        ? <div ref={box} className="hk-capture" tabIndex={-1} role="status" aria-live="polite">
          <span>Press the new key…</span><small>Esc cancels</small>
          {capturing?.message && <p className="hk-capture__error" role="alert">{capturing.message}</p>}
        </div>
        : <Keys keys={effectiveKeys(hotkey, overrides)} />}
    </td>
    <td className="hk-state">{hotkey.fixed ? <span className="hk-badge">Built in</span> : custom ? <span className="hk-badge hk-badge--custom">Custom</span> : <span className="hk-badge hk-badge--default">Default</span>}</td>
    <td className="hk-actions">
      {!hotkey.fixed && (active
        ? <button type="button" className="text-button" onClick={() => onCapture(null)}>Cancel</button>
        : <button type="button" className="text-button" onClick={() => onCapture(hotkey)} aria-label={`Change the key for ${hotkey.label}`}>Change</button>)}
      {custom && !active && <button type="button" className="text-button" onClick={() => onReset(hotkey)} aria-label={`Reset ${hotkey.label} to its default`} title={`Default: ${hotkey.defaults.map(formatKey).join(' or ')}`}><RotateCcw aria-hidden="true" size={12} />Reset</button>}
    </td>
  </tr>
}

function matches(hotkey: Hotkey, scope: Scope, query: string, overrides: HotkeyOverrides) {
  if (!query) return true
  const haystack = [hotkey.label, hotkey.description, hotkey.group, scope.label, scope.screen, ...effectiveKeys(hotkey, overrides).map(formatKey)]
    .filter(Boolean).join(' ').toLowerCase()
  return query.toLowerCase().split(/\s+/).every(term => haystack.includes(term))
}

/**
 * Every hotkey on every screen, grouped by screen then panel. "Change" waits
 * for the next key press; a key another hotkey on the same screen already uses
 * is refused. Changes apply at once and are saved to the account.
 */
export function HotkeySettingsPanel() {
  const settings = useHotkeySettings()
  const { overrides, save } = settings
  const scopes = useMemo(() => allScopes(), [])
  const screens = useMemo(() => [...new Set(scopes.map(scope => scope.screen))], [scopes])
  const [screen, setScreen] = useState<string>('all')
  const [query, setQuery] = useState('')
  const [capturing, setCapturing] = useState<CaptureState | null>(null)
  const [confirmReset, setConfirmReset] = useState(false)
  const customCount = scopes.flatMap(scope => scope.hotkeys).filter(hotkey => isCustomized(hotkey, overrides)).length

  const latest = useRef({ overrides, save })
  useEffect(() => { latest.current = { overrides, save } })

  useEffect(() => {
    if (!capturing) return
    const hotkey = capturing.hotkey
    // Capture phase on window: the key never reaches page or global shortcuts.
    const onKeyDown = (event: KeyboardEvent) => {
      const pressed = keyFromEvent(event)
      if (isModifierOnly(pressed)) return
      event.preventDefault()
      event.stopPropagation()
      if (pressed.key === 'Escape' && !pressed.shift && !pressed.ctrl && !pressed.alt && !pressed.meta) { setCapturing(null); return }
      const spec = canonical(pressed)
      const reserved = reservedReason(spec)
      if (reserved) { setCapturing({ hotkey, message: reserved }); return }
      if (hotkey.whileTyping && !isChord(spec)) { setCapturing({ hotkey, message: 'This shortcut also works while typing, so it needs Ctrl, Alt, or Cmd.' }); return }
      const { overrides: current, save: persist } = latest.current
      const conflicts = findConflicts(hotkey, [spec], current)
      if (conflicts.length) {
        const other = conflicts[0]
        const where = other.scopeId === hotkey.scopeId ? '' : ` (${scopes.find(scope => scope.id === other.scopeId)?.label ?? other.scopeId})`
        setCapturing({ hotkey, message: `${formatKey(spec)} already does “${other.label}”${where}. Change that one first, or choose another key.` })
        return
      }
      const next = { ...current }
      if (hotkey.defaults.length === 1 && sameKey(hotkey.defaults[0], spec)) delete next[hotkey.fullId]
      else next[hotkey.fullId] = [spec]
      setCapturing(null)
      void persist(next)
    }
    window.addEventListener('keydown', onKeyDown, { capture: true })
    return () => window.removeEventListener('keydown', onKeyDown, { capture: true })
  }, [capturing, scopes])

  const resetOne = (hotkey: Hotkey) => {
    const next = { ...overrides }
    delete next[hotkey.fullId]
    void save(next)
  }

  const visible = screens.filter(name => screen === 'all' || name === screen).map(name => ({
    name,
    scopes: scopes.filter(scope => scope.screen === name).map(scope => ({
      scope,
      hotkeys: scope.hotkeys.filter(hotkey => matches(hotkey, scope, query.trim(), overrides)),
    })).filter(entry => entry.hotkeys.length),
  })).filter(entry => entry.scopes.length)

  return <div className="hk-settings">
    <div className="hk-toolbar">
      <label className="hk-search"><Search aria-hidden="true" size={13} /><span className="sr-only">Search shortcuts</span><input type="search" value={query} onChange={event => setQuery(event.target.value)} placeholder="Search by action or key" /></label>
      <label className="hk-screen"><span>Screen</span>
        <select value={screen} onChange={event => setScreen(event.target.value)} aria-label="Screen">
          <option value="all">All screens</option>
          {screens.map(name => {
            const count = scopes.filter(scope => scope.screen === name).flatMap(scope => scope.hotkeys).filter(hotkey => isCustomized(hotkey, overrides)).length
            return <option key={name} value={name}>{count ? `${name} (${count} custom)` : name}</option>
          })}
        </select>
      </label>
      <span className="hk-status" role="status">{settings.saving ? 'Saving…' : settings.error ? `Could not save: ${settings.error.message}` : settings.persistent ? `${customCount} custom ${customCount === 1 ? 'key' : 'keys'} · saved` : 'Sign in to keep your keys'}</span>
      <button type="button" className={confirmReset ? 'button button--small button--danger' : 'button button--small button--secondary'} disabled={!customCount} onClick={() => {
        if (!confirmReset) { setConfirmReset(true); return }
        setConfirmReset(false)
        void settings.reset()
      }} onBlur={() => setConfirmReset(false)}>{confirmReset ? 'Press again to reset all' : 'Reset all to defaults'}</button>
    </div>
    {visible.length === 0 && <p className="hk-empty">No shortcut matches “{query}”.</p>}
    {visible.map(entry => <section key={entry.name} className="hk-screen-block" aria-label={`${entry.name} shortcuts`}>
      <h3>{entry.name}</h3>
      {entry.scopes.map(({ scope, hotkeys }) => <div key={scope.id} className="hk-scope">
        {(entry.scopes.length > 1 || scope.label !== entry.name) && <h4>{scope.label}</h4>}
        {scope.description && <p className="hk-scope__note">{scope.description}</p>}
        <table className="hk-table">
          <thead className="sr-only"><tr><th>Action</th><th>Keys</th><th>State</th><th>Change</th></tr></thead>
          <tbody>{hotkeys.map(hotkey => <HotkeyRow key={hotkey.fullId} hotkey={hotkey} scopeLabel={scope.label} overrides={overrides} capturing={capturing} onCapture={target => setCapturing(target ? { hotkey: target, message: null } : null)} onReset={resetOne} />)}</tbody>
        </table>
      </div>)}
    </section>)}
  </div>
}
