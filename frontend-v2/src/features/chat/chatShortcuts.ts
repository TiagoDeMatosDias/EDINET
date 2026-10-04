import type { ShortcutGroup } from '../../components/ShortcutsDialog'

export const CHAT_SHORTCUTS: ShortcutGroup[] = [
  { title: 'Moving around', shortcuts: [
    { keys: ['[', ']'], label: 'Previous or next channel or conversation' },
    { keys: ['U'], label: 'Next one with unread messages' },
    { keys: ['A'], label: 'All messages in one stream' },
    { keys: ['T'], label: 'Jump to a channel, company, or person' },
    { keys: ['J', 'K'], label: 'Next or previous message (↓ ↑ in the list)' },
    { keys: ['L'], label: 'Put the keyboard in the message list' },
    { keys: ['Esc'], label: 'Clear the selection, the reply, or leave a field' },
  ] },
  { title: 'Writing', shortcuts: [
    { keys: ['Enter'], label: 'Write a message; Enter again sends it' },
    { keys: ['$'], label: 'In a message: reference a company (links to its analysis)' },
    { keys: ['@'], label: 'In a message: mention a person' },
    { keys: ['R'], label: 'Reply to the selected message' },
    { keys: ['N'], label: 'New direct message (end-to-end encrypted)' },
    { keys: ['Shift+N'], label: 'New group' },
  ] },
  { title: 'Filtering', shortcuts: [
    { keys: ['F'], label: 'Filter: words, from:name, in:channel' },
    { keys: ['1', '2', '3', '4', '5'], label: 'All, Topics, Companies, Direct, Groups' },
    { keys: ['M'], label: 'Only messages that mention you' },
    { keys: ['$'], label: 'Only messages that reference a company' },
  ] },
  { title: 'The selected message', shortcuts: [
    { keys: ['O'], label: 'Open the company it mentions in Analysis' },
    { keys: ['C'], label: 'Go to where it was posted, or to the company’s channel' },
    { keys: ['P'], label: 'The author’s profile' },
    { keys: ['D'], label: 'Message the author privately' },
    { keys: ['X'], label: 'Delete it, if yours (press twice)' },
    { keys: ['B'], label: 'Block the author (press twice); with nothing selected, block the channel' },
  ] },
  { title: 'Channel', shortcuts: [
    { keys: ['S'], label: 'Subscribe or unsubscribe' },
    { keys: ['I'], label: 'Show or hide details' },
    { keys: ['?'], label: 'Show or hide this list' },
  ] },
]
