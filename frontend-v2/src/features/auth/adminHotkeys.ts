import { defineScope } from '../../hotkeys/registry'

export const ADMIN_SECTIONS = ['Users', 'Invite and reset', 'Access', 'Pipeline schedules', 'Audit log', 'Server settings'] as const

export const adminScope = defineScope({
  id: 'admin',
  label: 'Administration',
  screen: 'Admin',
  hotkeys: [
    ...ADMIN_SECTIONS.map((section, index) => ({ id: `section-${index + 1}`, keys: String(index + 1), label: `Go to ${section}`, group: 'Sections', role: 'tab' as const })),
    { id: 'filter', keys: 'f', label: 'Filter users', role: 'filter' },
    { id: 'invite', keys: 'i', label: 'Create an invitation link' },
    { id: 'row-next', keys: ['j', 'ArrowDown'], label: 'Users or audit log: next row', group: 'In a list', role: 'next', fixed: true },
    { id: 'row-previous', keys: ['k', 'ArrowUp'], label: 'Users or audit log: previous row', group: 'In a list', role: 'previous', fixed: true },
    { id: 'change-role', keys: 'Enter', label: 'Users: change the role', group: 'In a list', role: 'open', fixed: true },
    { id: 'reset-link', keys: 'p', label: 'Users: make a password-reset link', group: 'In a list', fixed: true },
    { id: 'disable', keys: 'x', label: 'Users: disable the account (press twice)', group: 'In a list', role: 'delete', fixed: true },
  ],
})
