function backtestIdOf(details: string | null | undefined): string | null {
  try {
    const parsed: unknown = JSON.parse(details || '{}')
    const id = parsed && typeof parsed === 'object' ? (parsed as { backtest_id?: unknown }).backtest_id : null
    return typeof id === 'string' && id ? id : null
  } catch {
    return null
  }
}

/**
 * Backtests recorded before descriptive titles ended with " · <run id>"; the
 * run time is already shown, so that stored id is dropped from the title.
 */
export function recentWorkTitle(item: { title: string; details_json?: string | null }) {
  const runId = backtestIdOf(item.details_json)
  const suffix = runId ? ` · ${runId}` : ''
  return suffix && item.title.endsWith(suffix) ? item.title.slice(0, -suffix.length) : item.title
}
