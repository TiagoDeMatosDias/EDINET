import type { ShareSplit } from '../../api/types'

/** Per-share figures on today's shares (split-adjusted), or as each filing reported them. */
export type ShareBasis = 'adjusted' | 'filed'
export const SHARE_BASES: ShareBasis[] = ['adjusted', 'filed']

/** The smallest whole-number ratio for a multiplier: 1.5 → [3, 2]. */
function ratio(value: number): [number, number] {
  for (let denominator = 1; denominator <= 10; denominator += 1) {
    const numerator = Math.round(value * denominator)
    if (numerator > 0 && Math.abs(numerator / denominator - value) < 1e-6) return [numerator, denominator]
  }
  return [Number(value.toPrecision(3)), 1]
}

/** "5-for-1 split", "3-for-2 split", "10-to-1 consolidation". */
export function splitName(multiplier: number) {
  if (multiplier >= 1) {
    const [shares, per] = ratio(multiplier)
    return `${shares}-for-${per} split`
  }
  const [shares, into] = ratio(1 / multiplier)
  return `${shares}-to-${into} consolidation`
}

/** "2-for-1 split, ex-date 2022-03-30", or when the filings date it, the dates it fell between. */
export function describeSplit(split: ShareSplit) {
  const name = splitName(split.multiplier)
  if (!split.after) return `${name}, ex-date ${split.date}`
  if (split.source === 'filing') return `${name} after the ${split.after.slice(0, 7)} year end, by the report of ${split.date}`
  return `${name} in the year to ${split.date.slice(0, 7)} (from share counts)`
}
