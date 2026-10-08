import { defineScope } from '../../hotkeys/registry'

export const filingsScope = defineScope({
  id: 'filings',
  label: 'Filing explorer',
  screen: 'Filings',
  hotkeys: [
    { id: 'find', keys: 'f', label: "Find a company's filings", role: 'filter' },
    { id: 'previous-type', keys: '[', label: 'Previous report type', role: 'previous' },
    { id: 'next-type', keys: ']', label: 'Next report type', role: 'next' },
    { id: 'enter-list', keys: 'j', label: 'Jump into the filing list', role: 'next', description: 'Moves focus into the list; J and K then walk it.' },
    { id: 'clear-company', keys: 'x', label: 'Clear the company', role: 'delete' },
    { id: 'analyze', keys: 'a', label: "Open the company's analysis", description: 'Screen-specific: opens Analysis rather than adding.' },
    { id: 'list-move', keys: ['ArrowDown', 'ArrowUp'], label: 'In the list: move between filings (also J, K)', role: 'next', fixed: true },
    { id: 'open', keys: 'Enter', label: 'Open the focused filing', role: 'open', fixed: true },
  ],
})

export const FILING_TABS = [
  { id: 'report', label: 'Report' },
  { id: 'sections', label: 'Sections' },
  { id: 'statements', label: 'Statements' },
  { id: 'details', label: 'Details' },
] as const

export const filingViewerScope = defineScope({
  id: 'filing-viewer',
  label: 'This filing',
  screen: 'Filing viewer',
  hotkeys: [
    ...FILING_TABS.map((tab, index) => ({ id: `tab-${tab.id}`, keys: String(index + 1), label: `${tab.label} tab`, group: 'Tabs', role: 'tab' as const })),
    { id: 'older', keys: '[', label: 'Older report from this company', role: 'previous' },
    { id: 'newer', keys: ']', label: 'Newer report from this company', role: 'next' },
    { id: 'analyze', keys: 'a', label: "Open the company's analysis", description: 'Screen-specific: opens Analysis rather than adding.' },
    { id: 'list', keys: 'l', label: "List the company's filings" },
  ],
})

export const reportTabScope = defineScope({
  id: 'filing-viewer.report',
  label: 'Report tab',
  screen: 'Filing viewer',
  parent: 'filing-viewer',
  hotkeys: [
    { id: 'next', keys: 'j', label: 'Next document', role: 'next' },
    { id: 'previous', keys: 'k', label: 'Previous document', role: 'previous' },
    { id: 'language', keys: 't', label: 'Cycle the report language' },
  ],
})

export const sectionsTabScope = defineScope({
  id: 'filing-viewer.sections',
  label: 'Sections tab',
  screen: 'Filing viewer',
  parent: 'filing-viewer',
  hotkeys: [
    { id: 'search', keys: 'f', label: 'Search the text', role: 'filter' },
    { id: 'side-by-side', keys: 't', label: 'Show the English translation side by side' },
    { id: 'next', keys: 'j', label: 'Next section', role: 'next' },
    { id: 'previous', keys: 'k', label: 'Previous section', role: 'previous' },
  ],
})

export const statementsTabScope = defineScope({
  id: 'filing-viewer.statements',
  label: 'Statements tab',
  screen: 'Filing viewer',
  parent: 'filing-viewer',
  hotkeys: [
    { id: 'next', keys: 'j', label: 'Next table', role: 'next' },
    { id: 'previous', keys: 'k', label: 'Previous table', role: 'previous' },
    { id: 'filter', keys: 'f', label: 'Filter line items', role: 'filter' },
  ],
})
