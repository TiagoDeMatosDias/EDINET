import { defineScope } from '../../hotkeys/registry'

export const BACKTEST_MODES = [['manual', 'Portfolio'], ['screen', 'Rolling screen'], ['csv', 'CSV set']] as const

export const backtestScope = defineScope({
  id: 'backtest',
  label: 'Backtest',
  screen: 'Backtest',
  hotkeys: [
    ...BACKTEST_MODES.map(([mode, label], index) => ({ id: `mode-${mode}`, keys: String(index + 1), label: `${label} backtest`, group: 'Set up and run', role: 'tab' as const })),
    { id: 'run', keys: 'r', label: 'Run the backtest', group: 'Set up and run', role: 'run' },
    { id: 'run-from-form', keys: ['Ctrl+Enter', 'Cmd+Enter'], label: 'Run the backtest from the setup form', group: 'Set up and run', role: 'run', fixed: true },
    { id: 'cancel', keys: 'x', label: 'Cancel the running rolling backtest', group: 'Set up and run', description: 'Screen-specific: cancels the run.' },
    { id: 'setup-panel', keys: 'e', label: 'Show or hide the setup panel', group: 'Set up and run' },
    { id: 'setup-focus', keys: 's', label: 'Focus the first setup field', group: 'Set up and run' },
    { id: 'add-holding', keys: 'a', label: 'Add a holding', group: 'Set up and run', role: 'add' },
    { id: 'benchmark', keys: 'b', label: 'Choose the benchmark', group: 'Set up and run' },
    { id: 'previous-view', keys: '[', label: 'Previous results view', group: 'Results', role: 'previous' },
    { id: 'next-view', keys: ']', label: 'Next results view', group: 'Results', role: 'next' },
    { id: 'next-duration', keys: 'd', label: 'Next holding period', group: 'Results', description: 'Screen-specific: the holding period, not a download.' },
    { id: 'previous-duration', keys: 'Shift+D', label: 'Previous holding period', group: 'Results', description: 'Screen-specific: the holding period, not a download.' },
    { id: 'weighting', keys: 'w', label: 'Next weighting', group: 'Results' },
    { id: 'measure', keys: 'm', label: 'Heatmap: return or excess', group: 'Results' },
    { id: 'close-detail', keys: 'Escape', label: 'Close the selected run', group: 'Results', role: 'close' },
    { id: 'rows', keys: ['j', 'k'], label: 'In a table: move through rows; Enter opens', group: 'Results', role: 'next', fixed: true },
    { id: 'saved', keys: 'l', label: 'Focus the saved results', group: 'Saved results' },
    { id: 'open-saved', keys: 'Enter', label: 'In the saved list: open the focused result', group: 'Saved results', role: 'open', fixed: true },
    { id: 'download-saved', keys: 'o', label: 'In the saved list: download the focused result', group: 'Saved results', role: 'download', description: 'Screen-specific: downloads rather than opens.', fixed: true },
  ],
})
