import { defineScope } from '../../hotkeys/registry'

export const LANGUAGES = [
  { id: 'curl', label: 'curl' },
  { id: 'python', label: 'Python' },
  { id: 'javascript', label: 'JavaScript' },
] as const

export const apiDocsScope = defineScope({
  id: 'api-docs',
  label: 'API docs',
  screen: 'API docs',
  hotkeys: [
    { id: 'previous-section', keys: '[', label: 'Previous screen', role: 'previous' },
    { id: 'next-section', keys: ']', label: 'Next screen', role: 'next' },
    { id: 'next', keys: 'j', label: 'Next endpoint', role: 'next' },
    { id: 'previous', keys: 'k', label: 'Previous endpoint', role: 'previous' },
    { id: 'filter', keys: 'f', label: 'Filter endpoints', role: 'filter' },
    ...LANGUAGES.map((language, index) => ({ id: `language-${language.id}`, keys: String(index + 1), label: `Examples in ${language.label}`, group: 'Examples', role: 'tab' as const })),
    { id: 'copy', keys: 'c', label: "Copy the endpoint's example", group: 'Examples' },
    { id: 'send', keys: 'r', label: 'Send the endpoint (reads only)', role: 'run' },
    { id: 'toggle', keys: 'Enter', label: 'Open or close the focused endpoint', role: 'open', fixed: true },
  ],
})
