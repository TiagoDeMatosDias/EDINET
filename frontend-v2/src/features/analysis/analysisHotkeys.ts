import { defineScope } from '../../hotkeys/registry'

export const ANALYSIS_SECTIONS = [
  { id: 'overview', label: 'Overview' },
  { id: 'financials', label: 'Financials' },
  { id: 'filings', label: 'Filings' },
  { id: 'bonds', label: 'Bonds' },
  { id: 'discussion', label: 'Discussion' },
] as const

export const analysisScope = defineScope({
  id: 'analysis',
  label: 'This company',
  screen: 'Analysis',
  hotkeys: [
    ...ANALYSIS_SECTIONS.map((section, index) => ({ id: `section-${section.id}`, keys: String(index + 1), label: `Jump to ${section.label}`, group: 'Sections', role: 'tab' as const })),
    { id: 'discuss', keys: 'd', label: 'Write in the company’s discussion channel', description: 'Screen-specific: discussion, not a download.' },
    { id: 'tag', keys: 't', label: 'Add a tag' },
    { id: 'note', keys: 'n', label: 'Write a research note', role: 'new' },
    { id: 'compare', keys: 'p', label: 'Compare with peers' },
    { id: 'backtest', keys: 'b', label: 'Backtest this ticker' },
  ],
})

export const pricePanelScope = defineScope({
  id: 'analysis.price',
  label: 'Price chart',
  screen: 'Analysis',
  parent: 'analysis',
  hotkeys: [
    { id: 'wider', keys: '-', label: 'Widen the price range' },
    { id: 'narrower', keys: ['=', '+'], label: 'Narrow the price range' },
  ],
})

export const financialsScope = defineScope({
  id: 'analysis.financials',
  label: 'Financial statements',
  screen: 'Analysis',
  parent: 'analysis',
  hotkeys: [
    { id: 'previous-statement', keys: '[', label: 'Previous statement', role: 'previous' },
    { id: 'next-statement', keys: ']', label: 'Next statement', role: 'next' },
    { id: 'view', keys: 'v', label: 'Cycle Values, YoY, Common size' },
    { id: 'filter', keys: 'f', label: 'Filter lines', role: 'filter' },
    { id: 'empty', keys: 'e', label: 'Show or hide empty lines' },
    { id: 'chart-type', keys: 'c', label: 'Switch bars and lines' },
    { id: 'clear-chart', keys: 'x', label: 'Clear the chart', role: 'delete' },
    { id: 'move', keys: ['ArrowDown', 'ArrowUp'], label: 'In the table: move between lines (also J, K)', role: 'next', fixed: true },
    { id: 'chart-line', keys: 'Space', label: 'In the table: chart or un-chart the focused line', fixed: true },
  ],
})

export const filingsPanelScope = defineScope({
  id: 'analysis.filings',
  label: 'Filings',
  screen: 'Analysis',
  parent: 'analysis',
  hotkeys: [
    { id: 'open-latest', keys: 'o', label: 'Open the latest filing', role: 'open' },
  ],
})

export const bondsPanelScope = defineScope({
  id: 'analysis.bonds',
  label: 'Bonds',
  screen: 'Analysis',
  parent: 'analysis',
  hotkeys: [
    { id: 'market', keys: 'm', label: 'Compare the company’s bonds in the bond market' },
    { id: 'redeemed', keys: 'h', label: 'Show or hide matured and redeemed bonds (history)' },
  ],
})
