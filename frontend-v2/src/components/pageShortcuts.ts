/** "G then a letter" destinations, in sidebar order. */
export interface PageShortcut { key: string; to: string; label: string; adminOnly?: boolean }

export const PAGE_SHORTCUTS: PageShortcut[] = [
  { key: 'o', to: '/overview', label: 'Overview' },
  { key: 's', to: '/screen', label: 'Screen' },
  { key: 'a', to: '/analyze', label: 'Analyze' },
  { key: 'b', to: '/backtest', label: 'Backtest' },
  { key: 'p', to: '/portfolio', label: 'Portfolio' },
  { key: 'd', to: '/pipeline', label: 'Data pipeline', adminOnly: true },
  { key: 'f', to: '/filings', label: 'Filings' },
  { key: 'c', to: '/compare', label: 'Compare' },
  { key: 'r', to: '/research', label: 'Research' },
]

export function pageShortcuts(isAdmin: boolean) {
  return PAGE_SHORTCUTS.filter(page => isAdmin || !page.adminOnly)
}

export function pageShortcutFor(path: string) {
  return PAGE_SHORTCUTS.find(page => page.to === path)
}
