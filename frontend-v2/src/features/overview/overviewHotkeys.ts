import { defineScope } from '../../hotkeys/registry'

export const OVERVIEW_PANELS = ['Portfolio', 'Research', 'Chat', 'Recent work', 'Data', 'Start'] as const

export const overviewScope = defineScope({
  id: 'overview',
  label: 'Overview',
  screen: 'Overview',
  hotkeys: [
    { id: 'next', keys: 'j', label: 'Next item in any panel', role: 'next' },
    { id: 'previous', keys: 'k', label: 'Previous item in any panel', role: 'previous' },
    { id: 'next-panel', keys: ']', label: 'Next panel', role: 'next' },
    { id: 'previous-panel', keys: '[', label: 'Previous panel', role: 'previous' },
    { id: 'open', keys: 'Enter', label: 'Open the item', role: 'open', fixed: true },
    ...OVERVIEW_PANELS.map((panel, index) => ({ id: `panel-${index + 1}`, keys: String(index + 1), label: `Go to ${panel}`, group: 'Panels', role: 'tab' as const })),
    { id: 'new-screen', keys: 'n', label: 'New screen', group: 'Start something', role: 'new' },
    { id: 'backtest', keys: 't', label: 'Test an idea in Backtest', group: 'Start something' },
    { id: 'chat', keys: 'u', label: 'Read unread chat', group: 'Start something' },
  ],
})
