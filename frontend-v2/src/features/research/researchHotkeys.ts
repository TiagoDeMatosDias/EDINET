import { defineScope } from '../../hotkeys/registry'

export const RESEARCH_TABS = [
  { key: 'companies', label: 'Companies' },
  { key: 'notes', label: 'Notes' },
  { key: 'alerts', label: 'Alerts' },
  { key: 'options', label: 'Options' },
  { key: 'bonds', label: 'Bonds & credit' },
  { key: 'bond-market', label: 'Bond market' },
] as const

export const researchScope = defineScope({
  id: 'research',
  label: 'Research',
  screen: 'Research',
  hotkeys: [
    ...RESEARCH_TABS.map((tab, index) => ({ id: `tab-${tab.key}`, keys: String(index + 1), label: `${tab.label} tab`, group: 'Tabs', role: 'tab' as const })),
    { id: 'tab-step', keys: ['ArrowLeft', 'ArrowRight'], label: 'In the tab list: previous or next tab (also [ ])', group: 'Tabs', fixed: true },
  ],
})

export const bookScope = defineScope({
  id: 'research.companies',
  label: 'Companies',
  screen: 'Research',
  parent: 'research',
  hotkeys: [
    { id: 'next', keys: 'j', label: 'Next company (↓ in the list)', role: 'next' },
    { id: 'previous', keys: 'k', label: 'Previous company (↑ in the list)', role: 'previous' },
    { id: 'open', keys: 'o', label: 'Open the company in Analysis (Enter in the list)', role: 'open' },
    { id: 'add', keys: 'a', label: 'Add a company to research', role: 'add' },
    { id: 'filter', keys: 'f', label: 'Filter companies', role: 'filter' },
    { id: 'previous-tag', keys: '[', label: 'Previous tag', role: 'previous' },
    { id: 'next-tag', keys: ']', label: 'Next tag', role: 'next' },
    { id: 'status', keys: 's', label: 'Set the thesis status' },
    { id: 'thesis', keys: 'e', label: 'Edit the thesis' },
    { id: 'tag', keys: 't', label: 'Add a tag' },
    { id: 'note', keys: 'n', label: 'Write a note', role: 'new' },
    { id: 'compare', keys: 'c', label: 'Compare the companies listed (up to 12)' },
    { id: 'download', keys: 'd', label: 'Download the list as CSV', role: 'download' },
  ],
})

export const notesScope = defineScope({
  id: 'research.notes',
  label: 'Notes',
  screen: 'Research',
  parent: 'research',
  hotkeys: [
    { id: 'next', keys: 'j', label: 'Next note (↓ in the list)', role: 'next' },
    { id: 'previous', keys: 'k', label: 'Previous note (↑ in the list)', role: 'previous' },
    { id: 'new', keys: 'n', label: 'Write a note', role: 'new' },
    { id: 'edit', keys: 'e', label: 'Edit the note' },
    { id: 'delete', keys: 'x', label: 'Delete the note (press twice)', role: 'delete' },
    { id: 'filter', keys: 'f', label: 'Search notes', role: 'filter' },
    { id: 'download', keys: 'd', label: 'Download the notes listed as Markdown', role: 'download' },
  ],
})

export const alertsScope = defineScope({
  id: 'research.alerts',
  label: 'Alerts',
  screen: 'Research',
  parent: 'research',
  hotkeys: [
    { id: 'next', keys: 'j', label: 'Next alert (↓ in the list)', role: 'next' },
    { id: 'previous', keys: 'k', label: 'Previous alert (↑ in the list)', role: 'previous' },
    { id: 'new', keys: 'n', label: 'New alert: Enter picks the company, then Enter adds it', role: 'new' },
    { id: 'triggered', keys: 't', label: 'Show only triggered alerts' },
    { id: 'delete', keys: 'x', label: 'Delete the alert (press twice)', role: 'delete' },
  ],
})

export const optionsScope = defineScope({
  id: 'research.options',
  label: 'Options',
  screen: 'Research',
  parent: 'research',
  hotkeys: [
    { id: 'company', keys: 'a', label: 'Price options on a company', role: 'add' },
    { id: 'manual', keys: 'm', label: 'Manual inputs (no company)' },
    { id: 'strike-down', keys: '[', label: 'Strike down one step', role: 'previous' },
    { id: 'strike-up', keys: ']', label: 'Strike up one step', role: 'next' },
    { id: 'volatility', keys: 'v', label: 'Next volatility estimate' },
    { id: 'days', keys: 't', label: 'Days to expiry' },
    { id: 'market', keys: 'i', label: 'Market price, for implied volatility' },
    { id: 'strategy', keys: 's', label: 'Choose a strategy' },
    { id: 'ladder', keys: 'l', label: 'Strike ladder (↑ ↓ choose a strike)' },
    { id: 'reset', keys: 'r', label: 'Reset to the company’s data', role: 'reset' },
  ],
})

export const bondsScope = defineScope({
  id: 'research.bonds',
  label: 'Bonds & credit',
  screen: 'Research',
  parent: 'research',
  hotkeys: [
    { id: 'company', keys: 'a', label: 'Price a bond issued by a company', role: 'add' },
    { id: 'manual', keys: 'm', label: 'Manual inputs (no company)' },
    { id: 'shorter', keys: '[', label: 'Maturity one year shorter', role: 'previous' },
    { id: 'longer', keys: ']', label: 'Maturity one year longer', role: 'next' },
    { id: 'maturity', keys: 'd', label: 'Maturity date', description: 'Screen-specific: the maturity date, not a download.' },
    { id: 'fee', keys: 'f', label: 'Fee as a percentage or a set amount', description: 'Screen-specific: switches the fee type rather than filtering.' },
    { id: 'coupon', keys: 'c', label: 'Coupon' },
    { id: 'yield', keys: 'y', label: 'Risk-free yield' },
    { id: 'price', keys: 'p', label: 'Market price, for the implied yield and default risk' },
    { id: 'reset', keys: 'r', label: 'Reset to the company’s data', role: 'reset' },
  ],
})

export const bondMarketScope = defineScope({
  id: 'research.bond-market',
  label: 'Bond market',
  screen: 'Research',
  parent: 'research',
  hotkeys: [
    { id: 'next', keys: 'j', label: 'Next bond (↓ in the list)', role: 'next' },
    { id: 'previous', keys: 'k', label: 'Previous bond (↑ in the list)', role: 'previous' },
    { id: 'open', keys: 'o', label: 'Open the issuer in Analysis (Enter in the list)', role: 'open' },
    { id: 'filter', keys: 'f', label: 'Filter bonds', role: 'filter' },
    { id: 'issuer', keys: 'i', label: 'Show only this issuer’s bonds, or all again' },
    { id: 'calculator', keys: 'c', label: 'Price the bond in the calculator' },
    { id: 'previous-rating', keys: '[', label: 'Previous rating group', role: 'previous' },
    { id: 'next-rating', keys: ']', label: 'Next rating group', role: 'next' },
    { id: 'download', keys: 'd', label: 'Download the bonds listed as CSV', role: 'download' },
  ],
})
