import type { ShortcutGroup } from '../../components/ShortcutsDialog'

export const BOOK_SHORTCUTS: ShortcutGroup = { title: 'Companies', shortcuts: [
  { keys: ['J', 'K'], label: 'Next or previous company (↓ ↑ in the list)' },
  { keys: ['O'], label: 'Open the company in Analysis (Enter in the list)' },
  { keys: ['A'], label: 'Add a company to research' },
  { keys: ['F'], label: 'Filter companies' },
  { keys: ['[', ']'], label: 'Previous or next tag' },
  { keys: ['S'], label: 'Set the thesis status' },
  { keys: ['E'], label: 'Edit the thesis' },
  { keys: ['T'], label: 'Add a tag' },
  { keys: ['N'], label: 'Write a note' },
  { keys: ['C'], label: 'Compare the companies listed (up to 12)' },
  { keys: ['D'], label: 'Download the list as CSV' },
] }

export const NOTES_SHORTCUTS: ShortcutGroup = { title: 'Notes', shortcuts: [
  { keys: ['J', 'K'], label: 'Next or previous note (↓ ↑ in the list)' },
  { keys: ['N'], label: 'Write a note' },
  { keys: ['E'], label: 'Edit the note' },
  { keys: ['X'], label: 'Delete the note (press twice)' },
  { keys: ['F'], label: 'Search notes' },
  { keys: ['D'], label: 'Download the notes listed as Markdown' },
] }

export const ALERTS_SHORTCUTS: ShortcutGroup = { title: 'Alerts', shortcuts: [
  { keys: ['J', 'K'], label: 'Next or previous alert (↓ ↑ in the list)' },
  { keys: ['N'], label: 'New alert' },
  { keys: ['T'], label: 'Show only triggered alerts' },
  { keys: ['X'], label: 'Delete the alert (press twice)' },
] }

export const OPTIONS_SHORTCUTS: ShortcutGroup = { title: 'Options', shortcuts: [
  { keys: ['A'], label: 'Price options on a company' },
  { keys: ['M'], label: 'Manual inputs (no company)' },
  { keys: ['[', ']'], label: 'Strike down or up one step' },
  { keys: ['V'], label: 'Next volatility estimate' },
  { keys: ['T'], label: 'Days to expiry' },
  { keys: ['I'], label: 'Market price, for implied volatility' },
  { keys: ['S'], label: 'Choose a strategy' },
  { keys: ['L'], label: 'Strike ladder (↑ ↓ choose a strike)' },
  { keys: ['R'], label: 'Reset to the company’s data' },
] }

export const BONDS_SHORTCUTS: ShortcutGroup = { title: 'Bonds', shortcuts: [
  { keys: ['A'], label: 'Price a bond issued by a company' },
  { keys: ['M'], label: 'Manual inputs (no company)' },
  { keys: ['[', ']'], label: 'Maturity one year shorter or longer' },
  { keys: ['C'], label: 'Coupon' },
  { keys: ['Y'], label: 'Risk-free yield' },
  { keys: ['P'], label: 'Market price, for the implied yield and default risk' },
  { keys: ['R'], label: 'Reset to the company’s data' },
] }
