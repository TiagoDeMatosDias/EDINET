import { defineScope } from '../../hotkeys/registry'

export const ACCOUNT_SECTIONS = [
  { id: 'profile', label: 'Public profile' },
  { id: 'account', label: 'Sign-in details' },
  { id: 'password', label: 'Password' },
  { id: 'encryption', label: 'Chat encryption' },
  { id: 'blocks', label: 'Blocked' },
  { id: 'tokens', label: 'API tokens' },
  { id: 'sessions', label: 'Sessions' },
  { id: 'keyboard', label: 'Keyboard shortcuts' },
] as const

export const accountScope = defineScope({
  id: 'account',
  label: 'Account',
  screen: 'Account',
  hotkeys: [
    ...ACCOUNT_SECTIONS.map((section, index) => ({ id: `section-${section.id}`, keys: String(index + 1), label: `Go to ${section.label}`, group: 'Sections', role: 'tab' as const })),
    { id: 'view-profile', keys: 'v', label: 'View your public profile' },
    { id: 'save-field', keys: 'Enter', label: 'In a field: save that section', role: 'open', fixed: true },
    { id: 'list-next', keys: ['j', 'ArrowDown'], label: 'In a list: next entry', role: 'next', fixed: true },
    { id: 'list-previous', keys: ['k', 'ArrowUp'], label: 'In a list: previous entry', role: 'previous', fixed: true },
    { id: 'unblock', keys: 'u', label: 'In the blocked list: unblock the entry', fixed: true },
  ],
})
