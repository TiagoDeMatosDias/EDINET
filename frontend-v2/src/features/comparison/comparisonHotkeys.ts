import { defineScope } from '../../hotkeys/registry'

export const COMPARISON_SECTIONS = ['Companies', 'Metrics', 'Table', 'Charts'] as const

export const comparisonScope = defineScope({
  id: 'comparison',
  label: 'Compare',
  screen: 'Compare',
  hotkeys: [
    ...COMPARISON_SECTIONS.map((section, index) => ({ id: `section-${index + 1}`, keys: String(index + 1), label: `Jump to ${section}`, group: 'Sections', role: 'tab' as const })),
    { id: 'add-company', keys: 'a', label: 'Add a company', group: 'Companies', role: 'add' },
    { id: 'peers', keys: 'p', label: 'Go to the suggested peers (↑/↓, then Enter adds)', group: 'Companies' },
    { id: 'add-closest-peer', keys: 'Shift+P', label: 'Add the closest peer', group: 'Companies', role: 'add' },
    { id: 'move-company-earlier', keys: '[', label: 'Move the company earlier', group: 'Companies', role: 'previous' },
    { id: 'move-company-later', keys: ']', label: 'Move the company later', group: 'Companies', role: 'next' },
    { id: 'remove-company', keys: 'Shift+X', label: 'Remove the company', group: 'Companies', role: 'delete' },
    { id: 'saved', keys: 'o', label: 'Open a saved comparison', group: 'Companies', role: 'open' },
    { id: 'save', keys: ['Ctrl+S', 'Cmd+S'], label: 'Save this comparison', group: 'Companies', whileTyping: true },
    { id: 'next-metric', keys: 'j', label: 'Next metric (also ↓ in the table)', group: 'Table', role: 'next' },
    { id: 'previous-metric', keys: 'k', label: 'Previous metric (also ↑ in the table)', group: 'Table', role: 'previous' },
    { id: 'next-company', keys: 'l', label: 'Next company (also → in the table)', group: 'Table', description: 'Screen-specific: moves right, like Vim.' },
    { id: 'previous-company', keys: 'h', label: 'Previous company (also ← in the table)', group: 'Table' },
    { id: 'open', keys: 'Enter', label: 'Open the company in Analysis (Shift: new tab)', group: 'Table', role: 'open', fixed: true },
    { id: 'sort', keys: 's', label: 'Sort companies by the metric, best first', group: 'Table' },
    { id: 'hide-metric', keys: 'x', label: 'Hide the metric', group: 'Table', role: 'delete' },
    { id: 'add-metric', keys: 'm', label: 'Add a metric', group: 'Table' },
    { id: 'empty', keys: 'e', label: 'Show or hide metrics no company reports', group: 'Table' },
    { id: 'ranks', keys: 'r', label: 'Show or hide ranks', group: 'Table', description: 'Screen-specific: toggles ranks rather than running.' },
    { id: 'indexed', keys: 'i', label: 'Index the trend chart to 100', group: 'Table' },
    { id: 'download', keys: 'd', label: 'Download the table as CSV', group: 'Table', role: 'download' },
  ],
})
