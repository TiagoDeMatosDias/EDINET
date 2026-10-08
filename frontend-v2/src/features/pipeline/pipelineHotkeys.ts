import { defineScope } from '../../hotkeys/registry'

export const PIPELINE_REGIONS = ['Library', 'Sequence', 'Configuration', 'Latest run', 'History'] as const

export const pipelineScope = defineScope({
  id: 'pipeline',
  label: 'Data pipeline',
  screen: 'Data pipeline',
  hotkeys: [
    ...PIPELINE_REGIONS.map((region, index) => ({ id: `region-${index + 1}`, keys: String(index + 1), label: `Go to ${region}`, group: 'Moving around', role: 'tab' as const })),
    { id: 'filter', keys: 'f', label: 'Filter the step library', group: 'Moving around', role: 'filter' },
    { id: 'list-next', keys: ['j', 'ArrowDown'], label: 'Next item in the focused list', group: 'Moving around', role: 'next', fixed: true },
    { id: 'list-previous', keys: ['k', 'ArrowUp'], label: 'Previous item in the focused list', group: 'Moving around', role: 'previous', fixed: true },
    { id: 'add-or-configure', keys: ['Enter', 'Space'], label: 'Library: add the step · Sequence: configure it', group: 'Building a run', role: 'open', fixed: true },
    { id: 'move-step', keys: ['Shift+J', 'Shift+K'], label: 'Sequence: move the step down or up', group: 'Building a run', description: 'Shift with the list keys reorders instead of moving the cursor.', fixed: true },
    { id: 'overwrite', keys: 'o', label: 'Sequence: overwrite on or off', group: 'Building a run', description: 'Toggles the focused step; not "open".', fixed: true },
    { id: 'remove-step', keys: ['x', 'Delete'], label: 'Sequence: remove the step', group: 'Building a run', role: 'delete', fixed: true },
    { id: 'daily', keys: 'd', label: 'Use the daily refresh recipe', group: 'Building a run', description: 'Screen-specific: the daily recipe, not a download.' },
    { id: 'save', keys: 's', label: 'Save the sequence as a setup', group: 'Building a run' },
    { id: 'load', keys: 'l', label: 'Load a saved setup', group: 'Building a run' },
    { id: 'run', keys: 'r', label: 'Run the sequence', group: 'Running', role: 'run' },
    { id: 'cancel', keys: 'c', label: 'Cancel the running job (press twice)', group: 'Running' },
    { id: 'show-run', keys: 'Enter', label: 'History: show that run', group: 'Running', role: 'open', fixed: true },
  ],
})
