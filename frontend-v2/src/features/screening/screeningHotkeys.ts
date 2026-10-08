import { defineScope } from '../../hotkeys/registry'

export const screeningScope = defineScope({
  id: 'screening',
  label: 'Building',
  screen: 'Screen',
  hotkeys: [
    { id: 'add-rule', keys: 'n', label: 'Add a rule and choose its metric', role: 'new' },
    { id: 'add-group', keys: 'Shift+N', label: 'Add a group of alternatives', role: 'new' },
    { id: 'screens', keys: 'o', label: 'Open a saved or starter screen', role: 'open' },
    { id: 'date', keys: 'd', label: 'Change the as-of date (runs the screen)', description: 'Screen-specific: the as-of date, not a download.' },
    { id: 'columns', keys: 'c', label: 'Choose output columns' },
    { id: 'collapse', keys: 'b', label: 'Collapse or expand the rules' },
    { id: 'run', keys: 'r', label: 'Run the screen', role: 'run' },
    { id: 'menu-move', keys: ['ArrowUp', 'ArrowDown'], label: 'In a menu or picker: move through metrics, screens, or dates', group: 'Menus and pickers', role: 'next', fixed: true },
    { id: 'menu-choose', keys: 'Enter', label: 'In a menu or picker: choose', group: 'Menus and pickers', role: 'open', fixed: true },
  ],
})

/** Run and save work from inside fields too, and while a menu is open. */
export const screeningCommandsScope = defineScope({
  id: 'screening.commands',
  label: 'Anywhere on this page',
  screen: 'Screen',
  parent: 'screening',
  hotkeys: [
    { id: 'run', keys: ['Ctrl+Enter', 'Cmd+Enter'], label: 'Run the screen (also while typing)', role: 'run', whileTyping: true },
    { id: 'save', keys: ['Ctrl+S', 'Cmd+S'], label: 'Save the screen (also while typing)', whileTyping: true },
  ],
})

export const resultsScope = defineScope({
  id: 'screening.results',
  label: 'Results',
  screen: 'Screen',
  parent: 'screening',
  hotkeys: [
    { id: 'enter', keys: ['j', 'ArrowDown', 'k', 'ArrowUp'], label: 'Go to the results', role: 'next' },
    { id: 'previous-page', keys: '[', label: 'Previous page of results', role: 'previous' },
    { id: 'next-page', keys: ']', label: 'Next page of results', role: 'next' },
    { id: 'move', keys: ['j', 'k'], label: 'In the results: move down or up (also ↓ ↑)', role: 'next', fixed: true },
    { id: 'ends', keys: ['Home', 'End'], label: 'In the results: first or last company', fixed: true },
    { id: 'open', keys: 'Enter', label: 'Open the company in Analysis', role: 'open', fixed: true },
    { id: 'open-tab', keys: 'Shift+Enter', label: 'Open the company in a new tab', role: 'open', fixed: true },
  ],
})

/** Shown on Analysis after opening a company from screen results. */
export const screenTrailScope = defineScope({
  id: 'screen-trail',
  label: 'Screened companies',
  screen: 'Analysis',
  parent: 'analysis',
  hotkeys: [
    { id: 'next', keys: 'Shift+J', label: 'Next screened company', role: 'next' },
    { id: 'previous', keys: 'Shift+K', label: 'Previous screened company', role: 'previous' },
  ],
})
