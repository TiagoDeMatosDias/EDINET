import { useQuery } from '@tanstack/react-query'
import { CornerUpLeft, Lock, SendHorizontal, X } from 'lucide-react'
import { useEffect, useImperativeHandle, useRef, useState, type KeyboardEvent, type ReactNode, type Ref } from 'react'

import { useCompanySearch } from '../../components/CompanyPicker'
import { chatApi } from './chatApi'
import { companyTokens, tseCode } from './chatModel'
import type { CompanyRef, MessageBody } from './chatTypes'

interface Suggestion { key: string; label: string; detail?: string; insert: string; ref?: [string, CompanyRef] }

const TRIGGER = /(^|\s)([$@#])([^\s$@#]{0,30})$/
// Drafts survive switching channels for the rest of the session.
const drafts = new Map<string, string>()

export function Composer({ draftKey, placeholder, disabled, disabledReason, encrypted, replyTo, onCancelReply, onSend, inputRef, target, topics }: {
  draftKey: string
  placeholder: string
  disabled?: boolean
  disabledReason?: ReactNode
  encrypted?: boolean
  replyTo?: string | null
  onCancelReply?: () => void
  onSend: (body: MessageBody) => Promise<unknown>
  inputRef?: Ref<HTMLTextAreaElement>
  target?: ReactNode
  topics: Array<{ slug: string; name: string }>
}) {
  const [text, setText] = useState(() => drafts.get(draftKey) ?? '')
  const [shownKey, setShownKey] = useState(draftKey)
  const [trigger, setTrigger] = useState<{ char: string; query: string; start: number } | null>(null)
  const [active, setActive] = useState(0)
  const [error, setError] = useState<string | null>(null)
  const [sending, setSending] = useState(false)
  const refs = useRef(new Map<string, CompanyRef>())
  const area = useRef<HTMLTextAreaElement | null>(null)
  // Pages focus the composer from a shortcut through ``inputRef``.
  useImperativeHandle(inputRef, () => area.current as HTMLTextAreaElement, [])

  if (shownKey !== draftKey) {
    setShownKey(draftKey)
    setText(drafts.get(draftKey) ?? '')
    setTrigger(null)
  }
  useEffect(() => { drafts.set(draftKey, text) }, [draftKey, text])
  useEffect(() => {
    const element = area.current
    if (!element) return
    element.style.height = 'auto'
    element.style.height = `${Math.min(element.scrollHeight, 160)}px`
  }, [text])

  const companies = useCompanySearch(trigger?.char === '$' ? trigger.query : '', 8)
  const people = useQuery({
    queryKey: ['chat', 'people', trigger?.query ?? ''],
    queryFn: () => chatApi.people(trigger?.query ?? ''),
    enabled: trigger?.char === '@',
    staleTime: 30_000,
  })
  const suggestions: Suggestion[] = !trigger ? []
    : trigger.char === '$' ? (companies.data?.results ?? []).filter(item => item.company_code).map(item => {
      const token = tseCode(item.ticker) ?? item.company_code!
      return { key: item.company_code!, label: item.company_name || token, detail: [tseCode(item.ticker), item.company_code, item.industry].filter(Boolean).join(' · '), insert: `$${token} `, ref: [token, { code: item.company_code!, name: item.company_name || token }] as [string, CompanyRef] }
    })
      : trigger.char === '@' ? (people.data?.people ?? []).filter(person => !person.is_me).slice(0, 8).map(person => ({ key: person.user_id, label: `@${person.username}`, detail: person.display_name ?? undefined, insert: `@${person.username} ` }))
        : topics.filter(item => item.slug.startsWith(trigger.query.toLowerCase())).slice(0, 8).map(item => ({ key: item.slug, label: `#${item.slug}`, detail: item.name, insert: `#${item.slug} ` }))
  const index = Math.min(active, Math.max(0, suggestions.length - 1))

  const detect = (value: string, caret: number) => {
    const match = TRIGGER.exec(value.slice(0, caret))
    if (!match) { setTrigger(null); return }
    setTrigger({ char: match[2], query: match[3], start: caret - match[3].length - 1 })
    setActive(0)
  }

  const insert = (suggestion: Suggestion) => {
    if (!trigger) return
    const element = area.current
    const caret = element?.selectionStart ?? text.length
    const next = text.slice(0, trigger.start) + suggestion.insert + text.slice(caret)
    if (suggestion.ref) refs.current.set(suggestion.ref[0], suggestion.ref[1])
    setText(next)
    setTrigger(null)
    const position = trigger.start + suggestion.insert.length
    requestAnimationFrame(() => { element?.focus(); element?.setSelectionRange(position, position) })
  }

  const send = async () => {
    const value = text.trim()
    if (!value || disabled || sending) return
    setSending(true)
    setError(null)
    try {
      const tokens = companyTokens(value)
      const known = Object.fromEntries(tokens.filter(token => refs.current.has(token)).map(token => [token, refs.current.get(token)!]))
      const unknown = tokens.filter(token => !known[token])
      if (unknown.length) {
        const found = await chatApi.resolve(unknown).catch(() => ({ refs: {} }))
        for (const [token, ref] of Object.entries(found.refs)) known[token] = { code: ref.code, name: ref.name }
      }
      await onSend({ text: value, refs: known })
      setText('')
      drafts.delete(draftKey)
      refs.current.clear()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'The message could not be sent')
    } finally {
      setSending(false)
    }
  }

  const onKeyDown = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.nativeEvent.isComposing) return
    if (trigger && suggestions.length) {
      if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
        event.preventDefault()
        setActive((index + (event.key === 'ArrowDown' ? 1 : -1) + suggestions.length) % suggestions.length)
        return
      }
      if (event.key === 'Enter' || event.key === 'Tab') { event.preventDefault(); insert(suggestions[index]); return }
    }
    if (event.key === 'Escape') {
      event.preventDefault()
      if (trigger) setTrigger(null)
      else if (replyTo) onCancelReply?.()
      else event.currentTarget.blur()
      return
    }
    if (event.key === 'Enter' && !event.shiftKey) { event.preventDefault(); void send() }
  }

  return <div className="chat-composer">
    {replyTo && <div className="chat-composer__reply"><CornerUpLeft aria-hidden="true" /><span>{replyTo}</span><button type="button" className="icon-button" aria-label="Cancel reply" onClick={onCancelReply}><X /></button></div>}
    {trigger && suggestions.length > 0 && <ul className="chat-suggest" role="listbox" aria-label="Suggestions">
      {suggestions.map((item, position) => <li key={item.key} role="option" aria-selected={position === index} className={position === index ? 'is-active' : undefined} onMouseDown={event => { event.preventDefault(); insert(item) }}><strong>{item.label}</strong>{item.detail && <small>{item.detail}</small>}</li>)}
    </ul>}
    <div className="chat-composer__row">
      {target}
      <textarea
        ref={area}
        rows={1}
        className="chat-composer__input"
        aria-label="Message"
        placeholder={disabled && typeof disabledReason === 'string' ? disabledReason : placeholder}
        value={text}
        disabled={disabled}
        maxLength={4000}
        onChange={event => { setText(event.target.value); detect(event.target.value, event.target.selectionStart) }}
        onKeyDown={onKeyDown}
        onBlur={() => setTrigger(null)}
      />
      <button type="button" className="button button--primary button--small" disabled={disabled || sending || !text.trim()} onClick={() => void send()} title="Send (Enter)">{encrypted && <Lock aria-hidden="true" />}<SendHorizontal aria-hidden="true" /><span className="sr-only">Send</span></button>
    </div>
    <div className="chat-composer__hint">
      {error ? <span className="form-error" role="alert">{error}</span>
        : disabled && disabledReason && typeof disabledReason !== 'string' ? disabledReason
          : <><kbd>Enter</kbd> send · <kbd>Shift</kbd>+<kbd>Enter</kbd> new line · <kbd>$</kbd> company · <kbd>@</kbd> person · <kbd>#</kbd> channel · <kbd>Esc</kbd> leave{encrypted && <span className="chat-composer__e2e"><Lock aria-hidden="true" />End-to-end encrypted</span>}</>}
    </div>
  </div>
}
