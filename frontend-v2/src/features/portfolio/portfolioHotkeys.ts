import { defineScope } from '../../hotkeys/registry'

export const PORTFOLIO_TABS = [
  { id: 'overview', label: 'Overview' },
  { id: 'holdings', label: 'Holdings' },
  { id: 'performance', label: 'Performance' },
  { id: 'income', label: 'Income' },
  { id: 'activity', label: 'Activity' },
  { id: 'data', label: 'Data & method' },
] as const

export const portfolioScope = defineScope({
  id: 'portfolio',
  label: 'Anywhere on this page',
  screen: 'Portfolio',
  hotkeys: [
    ...PORTFOLIO_TABS.map((tab, index) => ({ id: `tab-${tab.id}`, keys: String(index + 1), label: `${tab.label} tab`, group: 'Tabs', role: 'tab' as const })),
    { id: 'longer', keys: '-', label: 'Longer period' },
    { id: 'shorter', keys: ['=', '+'], label: 'Shorter period' },
    { id: 'currency', keys: 'c', label: 'Choose the display currency' },
    { id: 'benchmark', keys: 'b', label: 'Choose the benchmark' },
    { id: 'find', keys: 'f', label: 'Find a holding, record, or paying company', role: 'filter' },
    { id: 'rebuild', keys: 'r', label: 'Rebuild the portfolio from your activity', role: 'refresh' },
    { id: 'refresh-prices', keys: 'Shift+R', label: 'Refresh market prices, then rebuild (operators)', role: 'refresh' },
    { id: 'import', keys: 'i', label: 'Import an IBKR Flex Query file' },
  ],
})

export const portfolioTableScope = defineScope({
  id: 'portfolio.table',
  label: 'Holdings and activity lists',
  screen: 'Portfolio',
  parent: 'portfolio',
  hotkeys: [
    { id: 'enter', keys: ['j', 'ArrowDown', 'k', 'ArrowUp'], label: 'Enter the list', role: 'next' },
    { id: 'previous-page', keys: '[', label: 'Previous page', role: 'previous' },
    { id: 'next-page', keys: ']', label: 'Next page', role: 'next' },
    { id: 'move', keys: ['j', 'k'], label: 'In the list: move down or up (also ↓ ↑)', role: 'next', fixed: true },
    { id: 'ends', keys: ['Home', 'End'], label: 'In the list: first or last row', fixed: true },
    { id: 'open', keys: 'Enter', label: 'In the list: open the details', role: 'open', fixed: true },
    { id: 'analyze', keys: 'a', label: 'In the list: open the holding in Analysis', description: 'Screen-specific: opens Analysis rather than adding.', fixed: true },
  ],
})

export const portfolioActivityScope = defineScope({
  id: 'portfolio.activity',
  label: 'Activity records',
  screen: 'Portfolio',
  parent: 'portfolio',
  hotkeys: [
    { id: 'add', keys: 'n', label: 'Add a transaction by hand', role: 'new' },
    { id: 'select-all', keys: 'Shift+A', label: 'Select every record shown (again: unselect)', description: 'Screen-specific: selects rather than adds.' },
    { id: 'select', keys: 'Space', label: 'In the list: select or unselect the record', fixed: true },
    { id: 'delete', keys: ['Delete', 'x'], label: 'Delete the selected records (asks first)', role: 'delete' },
  ],
})

export const portfolioIncomeScope = defineScope({
  id: 'portfolio.income',
  label: 'Income',
  screen: 'Portfolio',
  parent: 'portfolio',
  hotkeys: [
    { id: 'show-all', keys: 'x', label: 'Show every payer again', role: 'reset', description: 'Clears the chosen companies.' },
    { id: 'choose', keys: 'Enter', label: 'In the payers list: show only that company', role: 'open', fixed: true },
    { id: 'toggle', keys: 'a', label: 'In the payers list: add or remove it', role: 'add', fixed: true },
  ],
})

export const holdingDetailScope = defineScope({
  id: 'portfolio.detail',
  label: 'Holding details',
  screen: 'Portfolio',
  parent: 'portfolio',
  hotkeys: [
    { id: 'analyze', keys: 'a', label: 'Open in Analysis', description: 'Screen-specific: opens Analysis rather than adding.' },
    { id: 'next', keys: 'Shift+J', label: 'Next holding', role: 'next' },
    { id: 'previous', keys: 'Shift+K', label: 'Previous holding', role: 'previous' },
    { id: 'close', keys: 'Escape', label: 'Close', role: 'close', fixed: true },
  ],
})

/** Shown on Analysis after opening a holding from the portfolio. */
export const portfolioTrailScope = defineScope({
  id: 'portfolio-trail',
  label: 'Portfolio holdings',
  screen: 'Analysis',
  parent: 'analysis',
  hotkeys: [
    { id: 'next', keys: 'Shift+J', label: 'Next holding', role: 'next' },
    { id: 'previous', keys: 'Shift+K', label: 'Previous holding', role: 'previous' },
  ],
})
