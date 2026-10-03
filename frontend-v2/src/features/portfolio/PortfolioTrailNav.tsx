import { ArrowLeft, ChevronLeft, ChevronRight } from 'lucide-react'
import { useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'

import { useHotkeys } from '../../hooks/useHotkeys'
import { moveTrailTo, readPortfolioTrail } from './portfolioTrail'

/**
 * On a holding opened from the Portfolio page: back to the portfolio, and the
 * previous / next holding (Shift+K / Shift+J) without going back.
 */
export function PortfolioTrailNav({ current }: { current: string }) {
  const navigate = useNavigate()
  const [trail] = useState(() => readPortfolioTrail())
  const entries = trail?.entries ?? []
  const index = entries.findIndex(entry => entry.key === current || entry.symbol === current)
  const step = (offset: number) => {
    const entry = entries[index + offset]
    if (!entry) return
    moveTrailTo(entry.key)
    navigate(entry.href)
  }
  useHotkeys({ J: () => step(1), K: () => step(-1) }, index >= 0)
  const previous = entries[index - 1]
  const next = entries[index + 1]

  return <span className="screen-trail">
    <Link className="button button--ghost button--small" to="/portfolio" title="Back to the portfolio (G then P)" onClick={() => moveTrailTo(current)}><ArrowLeft aria-hidden="true" />Portfolio</Link>
    {index >= 0 && <>
      <button type="button" className="icon-button" aria-label="Previous holding" disabled={!previous} onClick={() => step(-1)} title={previous ? `Previous: ${previous.label} (Shift+K)` : 'This is the first holding'}><ChevronLeft /></button>
      <span className="screen-trail__position" title="Position in your holdings list">{(index + 1).toLocaleString()} of {entries.length.toLocaleString()}</span>
      <button type="button" className="icon-button" aria-label="Next holding" disabled={!next} onClick={() => step(1)} title={next ? `Next: ${next.label} (Shift+J)` : 'This is the last holding'}><ChevronRight /></button>
    </>}
  </span>
}
