/** "G then a letter" destinations, in sidebar order. Users can rebind the letters (see hotkeys/globalScopes). */
export interface PageShortcut { id: string; key: string; to: string; label: string; adminOnly?: boolean; signedInOnly?: boolean }

export const PAGE_SHORTCUTS: PageShortcut[] = [
  { id: 'overview', key: 'o', to: '/overview', label: 'Overview' },
  { id: 'screen', key: 's', to: '/screen', label: 'Screen' },
  { id: 'analyze', key: 'a', to: '/analyze', label: 'Analyze' },
  { id: 'backtest', key: 'b', to: '/backtest', label: 'Backtest' },
  { id: 'portfolio', key: 'p', to: '/portfolio', label: 'Portfolio' },
  { id: 'pipeline', key: 'd', to: '/pipeline', label: 'Data pipeline', adminOnly: true },
  { id: 'filings', key: 'f', to: '/filings', label: 'Filings' },
  { id: 'compare', key: 'c', to: '/compare', label: 'Compare' },
  { id: 'research', key: 'r', to: '/research', label: 'Research' },
  { id: 'chat', key: 'm', to: '/chat', label: 'Chat' },
  { id: 'account', key: 'u', to: '/account', label: 'Account', signedInOnly: true },
  { id: 'admin', key: 'n', to: '/admin', label: 'Admin', adminOnly: true },
]

export function pageShortcuts(isAdmin: boolean, signedIn = true) {
  return PAGE_SHORTCUTS.filter(page => (isAdmin || !page.adminOnly) && (signedIn || !page.signedInOnly))
}
