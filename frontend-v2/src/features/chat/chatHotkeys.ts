import { defineScope } from '../../hotkeys/registry'

const KINDS = ['All', 'Topics', 'Companies', 'Direct', 'Groups'] as const

export const chatScope = defineScope({
  id: 'chat',
  label: 'Chat',
  screen: 'Chat',
  hotkeys: [
    { id: 'previous-entry', keys: '[', label: 'Previous channel or conversation', group: 'Moving around', role: 'previous' },
    { id: 'next-entry', keys: ']', label: 'Next channel or conversation', group: 'Moving around', role: 'next' },
    { id: 'unread', keys: 'u', label: 'Next one with unread messages', group: 'Moving around' },
    { id: 'feed', keys: 'a', label: 'All messages in one stream', group: 'Moving around', description: 'Screen-specific: shows every message rather than adding.' },
    { id: 'switch', keys: 't', label: 'Jump to a channel, company, or person', group: 'Moving around' },
    { id: 'next', keys: 'j', label: 'Next message (↓ in the list)', group: 'Moving around', role: 'next' },
    { id: 'previous', keys: 'k', label: 'Previous message (↑ in the list)', group: 'Moving around', role: 'previous' },
    { id: 'list', keys: 'l', label: 'Put the keyboard in the message list', group: 'Moving around' },
    { id: 'clear', keys: 'Escape', label: 'Clear the selection or the reply', group: 'Moving around', role: 'close' },
    { id: 'compose', keys: 'Enter', label: 'Write a message', group: 'Writing', role: 'open' },
    { id: 'send', keys: 'Enter', label: 'In the message box: send (Shift+Enter: new line)', group: 'Writing', role: 'open', fixed: true },
    { id: 'company-reference', keys: '$', label: 'In a message: reference a company (links to its analysis)', group: 'Writing', fixed: true },
    { id: 'mention', keys: '@', label: 'In a message: mention a person', group: 'Writing', fixed: true },
    { id: 'reply', keys: 'r', label: 'Reply to the selected message', group: 'Writing', description: 'Screen-specific: replies rather than running.' },
    { id: 'new-dm', keys: 'n', label: 'New direct message (end-to-end encrypted)', group: 'Writing', role: 'new' },
    { id: 'new-group', keys: 'Shift+N', label: 'New group', group: 'Writing', role: 'new' },
    { id: 'filter', keys: 'f', label: 'Filter: words, from:name, in:channel', group: 'Filtering', role: 'filter' },
    ...KINDS.map((kind, index) => ({ id: `kind-${index + 1}`, keys: String(index + 1), label: `Show ${kind}`, group: 'Filtering', role: 'tab' as const })),
    { id: 'mentions', keys: 'm', label: 'Only messages that mention you', group: 'Filtering' },
    { id: 'with-companies', keys: '$', label: 'Only messages that reference a company', group: 'Filtering' },
    { id: 'open-company', keys: 'o', label: 'Open the company it mentions in Analysis', group: 'The selected message', role: 'open' },
    { id: 'place', keys: 'c', label: 'Go to where it was posted, or to the company’s channel', group: 'The selected message' },
    { id: 'profile', keys: 'p', label: 'The author’s profile', group: 'The selected message' },
    { id: 'dm-author', keys: 'd', label: 'Message the author privately', group: 'The selected message', description: 'Screen-specific: a direct message, not a download.' },
    { id: 'delete', keys: 'x', label: 'Delete it, if yours (press twice)', group: 'The selected message', role: 'delete' },
    { id: 'block', keys: 'b', label: 'Block the author (press twice); with nothing selected, block the channel', group: 'The selected message' },
    { id: 'subscribe', keys: 's', label: 'Subscribe or unsubscribe', group: 'Channel' },
    { id: 'details', keys: 'i', label: 'Show or hide details', group: 'Channel' },
  ],
})

export const profileScope = defineScope({
  id: 'profile',
  label: 'Profile',
  screen: 'Profile',
  hotkeys: [
    { id: 'message', keys: 'm', label: 'Message this person' },
    { id: 'block', keys: 'b', label: 'Block or unblock (press twice to block)' },
    { id: 'edit', keys: 'e', label: 'Edit your own profile' },
    { id: 'disarm', keys: 'Escape', label: 'Cancel a pending block', role: 'close' },
  ],
})
