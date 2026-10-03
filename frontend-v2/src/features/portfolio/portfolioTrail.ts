/**
 * The holdings in the order last shown on the Portfolio page, kept in this
 * browser so a holding opened in Analysis can step to the next one and coming
 * back lands on the last holding opened.
 */
import type { Holding } from './portfolioTypes'
import { holdingAnalysisHref, holdingName } from './portfolioFormat'

const TRAIL_KEY = 'shade.portfolio.trail'

export interface PortfolioTrailEntry {
  /** How Analysis identifies the holding: its EDINET code, else the ticker. */
  key: string
  symbol: string
  href: string
  label: string
}

export interface PortfolioTrail {
  entries: PortfolioTrailEntry[]
  /** The holding last opened, and whether to put keyboard focus back on it. */
  current?: string
  refocus?: boolean
}

export function trailEntry(holding: Holding): PortfolioTrailEntry {
  return {
    key: holding.performance?.edinet_code || holding.symbol,
    symbol: holding.symbol,
    href: holdingAnalysisHref(holding),
    label: holdingName(holding) || holding.symbol,
  }
}

export function readPortfolioTrail(): PortfolioTrail | null {
  try {
    const stored = JSON.parse(localStorage.getItem(TRAIL_KEY) ?? 'null') as PortfolioTrail | null
    return stored && Array.isArray(stored.entries) ? stored : null
  } catch {
    return null
  }
}

export function writePortfolioTrail(trail: PortfolioTrail) {
  try {
    localStorage.setItem(TRAIL_KEY, JSON.stringify(trail))
  } catch { /* a convenience: without storage the place is simply not kept */ }
}

/** Marks *key* as the holding being viewed (Analysis stepping, or opening one). */
export function moveTrailTo(key: string, refocus = true) {
  const trail = readPortfolioTrail()
  if (trail) writePortfolioTrail({ ...trail, current: key, refocus })
}
