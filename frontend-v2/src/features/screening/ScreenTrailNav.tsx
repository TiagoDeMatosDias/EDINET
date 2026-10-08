import { ArrowLeft, ChevronLeft, ChevronRight } from 'lucide-react'
import { useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'

import { useHotkeyScope } from '../../hotkeys/useHotkeyScope'
import { useHotkeyText } from '../../hotkeys/useHotkeyText'
import { screenTrailScope } from './screeningHotkeys'
import { analysisHref, moveCursorTo, readTrail } from './screenTrail'

/**
 * On a company opened from screen results: back to the results, and the
 * previous / next screened company (Shift+K / Shift+J) without going back.
 */
export function ScreenTrailNav({ current }: { current: string }) {
  const navigate = useNavigate()
  const [trail] = useState(() => readTrail())
  const index = trail ? trail.codes.indexOf(current) : -1
  const step = (offset: number) => {
    const code = trail?.codes[index + offset]
    if (!code) return
    moveCursorTo(code)
    navigate(analysisHref(code))
  }
  useHotkeyScope(screenTrailScope, { next: () => step(1), previous: () => step(-1) }, { enabled: index >= 0 })
  const nextKey = useHotkeyText(screenTrailScope.byId.next)
  const previousKey = useHotkeyText(screenTrailScope.byId.previous)
  const neighbour = (offset: number) => trail?.names[index + offset]
  const previous = neighbour(-1)
  const next = neighbour(1)

  return <span className="screen-trail">
    <Link className="button button--ghost button--small" to="/screen" title="Back to the screen results (G then S)"><ArrowLeft aria-hidden="true" />Screen</Link>
    {trail && index >= 0 && <>
      <button type="button" className="icon-button" aria-label="Previous screened company" disabled={!previous} onClick={() => step(-1)} title={previous ? `Previous: ${previous} (${previousKey})` : 'This is the first company'}><ChevronLeft /></button>
      <span className="screen-trail__position" title="Position in your screen results">{(index + 1).toLocaleString()} of {trail.codes.length.toLocaleString()}</span>
      <button type="button" className="icon-button" aria-label="Next screened company" disabled={!next} onClick={() => step(1)} title={next ? `Next: ${next} (${nextKey})` : 'This is the last company'}><ChevronRight /></button>
    </>}
  </span>
}
