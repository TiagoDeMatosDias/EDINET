export type MetricDirection = 'higher' | 'lower'

/**
 * The most favourable value among companies, or null when "best" is not
 * meaningful. The direction comes from the server's metric definitions;
 * metrics without one (size metrics, custom columns) are never highlighted.
 */
export function bestValue(direction: MetricDirection | undefined, values: Array<number | null | undefined>): number | null {
  if (!direction) return null
  let candidates = values.filter((value): value is number => value != null && Number.isFinite(value))
  // A negative multiple or leverage ratio reflects losses or negative equity, not a better value.
  if (direction === 'lower') candidates = candidates.filter(value => value >= 0)
  if (candidates.length < 2) return null
  return direction === 'higher' ? Math.max(...candidates) : Math.min(...candidates)
}
