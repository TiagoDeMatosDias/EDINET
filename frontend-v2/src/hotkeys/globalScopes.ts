import { PAGE_SHORTCUTS } from '../components/pageShortcuts'
import { defineScope, GLOBAL_SCOPE_ID, GOTO_SCOPE_ID } from './registry'

/** Keys that work on every page; page scopes are checked first. */
export const globalScope = defineScope({
  id: GLOBAL_SCOPE_ID,
  label: 'Anywhere',
  screen: 'Anywhere',
  hotkeys: [
    { id: 'search', keys: '/', label: 'Search companies', role: 'search' },
    { id: 'help', keys: '?', label: 'Show the shortcuts for this screen', role: 'help' },
    { id: 'goto', keys: 'g', label: 'Go to a page (then a letter)', description: 'Starts a two-key sequence; the second key picks the page.' },
    { id: 'leave-field', keys: 'Shift+Tab', label: 'Leave the field you are typing in', fixed: true },
    { id: 'close', keys: 'Escape', label: 'Close a menu or leave a field', role: 'close', fixed: true },
  ],
})

/** The second key of "G then a letter"; only compared with each other. */
export const gotoScope = defineScope({
  id: GOTO_SCOPE_ID,
  label: 'Go to a page (after G)',
  screen: 'Anywhere',
  hotkeys: PAGE_SHORTCUTS.map(page => ({ id: page.id, keys: page.key, label: page.label })),
})
