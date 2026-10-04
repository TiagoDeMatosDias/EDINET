/** "G then a letter" destinations, in sidebar order. */
export interface PageShortcut { key: string; to: string; label: string; adminOnly?: boolean; signedInOnly?: boolean }

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
  { key: 'm', to: '/chat', label: 'Chat' },
  { key: 'u', to: '/account', label: 'Account', signedInOnly: true },
  { key: 'n', to: '/admin', label: 'Admin', adminOnly: true },
]

export function pageShortcuts(isAdmin: boolean, signedIn = true) {
  return PAGE_SHORTCUTS.filter(page => (isAdmin || !page.adminOnly) && (signedIn || !page.signedInOnly))
}

export function pageShortcutFor(path: string) {
  return PAGE_SHORTCUTS.find(page => page.to === path)
}
