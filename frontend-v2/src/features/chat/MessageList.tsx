import { AtSign, CornerUpLeft, Lock, MessageSquare, Trash2, UserX } from 'lucide-react'
import { Fragment, useEffect, useLayoutEffect, useRef, type KeyboardEvent, type ReactNode } from 'react'
import { Link } from 'react-router-dom'

import { colorFor, continues, dayKey, dayLabel, fullTime, mentions, messageTime, personName, segments, systemText } from './chatModel'
import type { ChatMessage, MessageBody } from './chatTypes'
import type { Decrypted } from './e2e'

export function MessageText({ body, me, channelSlugs }: { body: MessageBody; me: string; channelSlugs: ReadonlySet<string> }) {
  return <span className="chat-text">{segments(body.text, body.refs, channelSlugs).map((part, index) => {
    switch (part.type) {
      case 'text': return <Fragment key={index}>{part.value}</Fragment>
      case 'company': return part.ref
        ? <Link key={index} className="chat-ref" to={`/analyze/${encodeURIComponent(part.ref.code)}`} title={`${part.ref.name}: open the analysis`}>${part.token}<small>{part.ref.name}</small></Link>
        : <span key={index} className="chat-ref chat-ref--unknown" title="No company has this code">${part.token}</span>
      case 'mention': return <Link key={index} className={part.username === me ? 'chat-mention is-me' : 'chat-mention'} to={`/people/${encodeURIComponent(part.username)}`}>@{part.username}</Link>
      case 'link': return <a key={index} className="chat-link" href={part.url} target="_blank" rel="noopener noreferrer nofollow">{part.url}</a>
      case 'channel': return <Link key={index} className="chat-channel-link" to={`/chat?c=topic:${part.slug}`}>#{part.slug}</Link>
      default: return null
    }
  })}</span>
}

export interface Where { label: string; color: string; href: string; locked?: boolean }

export type MessageAction = 'reply' | 'dm' | 'profile' | 'delete' | 'block' | 'select'

interface ListProps {
  messages: ChatMessage[]
  cursor: number
  onCursor: (index: number) => void
  me: { user_id: string; username: string; role?: string }
  bodyOf: (message: ChatMessage) => MessageBody | Decrypted | undefined
  whereOf?: (message: ChatMessage) => Where | undefined
  names: (userId: string) => string
  channelSlugs: ReadonlySet<string>
  armed?: string | null
  onAction: (action: MessageAction, message: ChatMessage) => void
  hasMore?: boolean
  loadingMore?: boolean
  onLoadMore?: () => void
  empty?: ReactNode
  focusRequest?: number
  label: string
}

function resolved(value: MessageBody | Decrypted | undefined): MessageBody | 'locked' | 'broken' | undefined {
  if (!value) return undefined
  if ('state' in value) return value.state === 'ok' ? value.body : value.state === 'missing' ? 'locked' : 'broken'
  return value
}

/**
 * A dense, keyboard-driven message list: J/K or ↓/↑ move the cursor, older
 * messages load at the top, and the view follows new messages while at the bottom.
 */
export function MessageList(props: ListProps) {
  const { messages, cursor, onCursor, me, bodyOf, whereOf, names, channelSlugs, armed, onAction } = props
  const scroller = useRef<HTMLDivElement>(null)
  const atBottom = useRef(true)
  const firstSeq = messages[0]?.seq
  const previousFirst = useRef(firstSeq)
  const previousHeight = useRef(0)

  // Keep the reader's place when older messages are prepended; follow new ones when at the bottom.
  useLayoutEffect(() => {
    const element = scroller.current
    if (!element) return
    if (previousFirst.current !== undefined && firstSeq !== undefined && firstSeq < previousFirst.current) {
      element.scrollTop += element.scrollHeight - previousHeight.current
    } else if (atBottom.current) {
      element.scrollTop = element.scrollHeight
    }
    previousFirst.current = firstSeq
    previousHeight.current = element.scrollHeight
  }, [messages, firstSeq])

  useEffect(() => {
    const row = scroller.current?.querySelector<HTMLElement>('[data-cursor="true"]')
    row?.scrollIntoView?.({ block: 'nearest' })
  }, [cursor])

  useEffect(() => {
    if (!props.focusRequest) return
    scroller.current?.querySelector<HTMLElement>('[data-cursor="true"]')?.focus({ preventScroll: true })
  }, [props.focusRequest])

  const onScroll = () => {
    const element = scroller.current
    if (!element) return
    atBottom.current = element.scrollHeight - element.scrollTop - element.clientHeight < 40
    if (element.scrollTop < 60 && props.hasMore && !props.loadingMore) props.onLoadMore?.()
  }

  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key !== 'ArrowDown' && event.key !== 'ArrowUp') return
    event.preventDefault()
    onCursor(Math.max(0, Math.min(messages.length - 1, cursor + (event.key === 'ArrowDown' ? 1 : -1))))
  }

  const byId = new Map(messages.map(message => [message.message_id, message]))
  return <div className="chat-list" ref={scroller} onScroll={onScroll} onKeyDown={onKeyDown} role="log" aria-label={props.label} aria-live="polite">
    {props.hasMore && <button type="button" className="chat-list__more" onClick={props.onLoadMore} disabled={props.loadingMore}>{props.loadingMore ? 'Loading…' : 'Load older messages'}</button>}
    {!messages.length && <div className="chat-list__empty">{props.empty ?? 'No messages yet.'}</div>}
    {messages.map((message, index) => {
      const previous = messages[index - 1]
      const newDay = !previous || dayKey(previous.created_at) !== dayKey(message.created_at)
      const isCursor = index === cursor
      const where = whereOf?.(message)
      const common = { 'data-cursor': isCursor, tabIndex: isCursor ? 0 : -1, onClick: () => onCursor(index), 'aria-selected': isCursor }
      const separator = newDay && <div className="chat-day" role="separator"><span>{dayLabel(message.created_at)}</span></div>
      if (message.kind === 'system') {
        return <Fragment key={message.message_id}>{separator}<div className={isCursor ? 'chat-row chat-row--system is-cursor' : 'chat-row chat-row--system'} {...common}>
          <time dateTime={message.created_at} title={fullTime(message.created_at)}>{messageTime(message.created_at)}</time>
          {where && <WhereChip where={where} />}
          <span>{systemText(message, names)}</span>
        </div></Fragment>
      }
      const body = resolved(bodyOf(message))
      const mine = message.sender.user_id === me.user_id
      const mentioned = typeof body === 'object' && mentions(body.text, me.username)
      const joined = !newDay && continues(previous, message) && !where
      const replyTo = message.reply_to ? byId.get(message.reply_to) : undefined
      const replyBody = replyTo ? resolved(bodyOf(replyTo)) : undefined
      const classes = ['chat-row', joined && 'chat-row--joined', isCursor && 'is-cursor', mentioned && 'is-mention', mine && 'is-mine'].filter(Boolean).join(' ')
      return <Fragment key={message.message_id}>{separator}<div className={classes} {...common}>
        <time dateTime={message.created_at} title={fullTime(message.created_at)}>{messageTime(message.created_at)}</time>
        {where && <WhereChip where={where} />}
        {joined ? <span className="chat-row__sender chat-row__sender--joined" aria-hidden="true" /> : <Link className="chat-row__sender" to={`/people/${encodeURIComponent(message.sender.username)}`} style={{ color: colorFor(message.sender.user_id) }} title={`@${message.sender.username}`}>{personName(message.sender)}</Link>}
        <div className="chat-row__body">
          {message.reply_to && <span className="chat-row__reply"><CornerUpLeft aria-hidden="true" />{replyTo ? <>{personName(replyTo.sender)}: {typeof replyBody === 'object' ? replyBody.text.slice(0, 80) : '…'}</> : 'an earlier message'}</span>}
          {message.deleted ? <em className="chat-row__deleted">Message deleted</em>
            : body === undefined ? <em className="chat-row__locked"><Lock aria-hidden="true" />Decrypting…</em>
              : body === 'locked' ? <em className="chat-row__locked" title="Encrypted with a key that has not been shared with you yet"><Lock aria-hidden="true" />Encrypted; waiting for a member to share the key</em>
                : body === 'broken' ? <em className="chat-row__locked"><Lock aria-hidden="true" />This message could not be decrypted</em>
                  : <MessageText body={body} me={me.username} channelSlugs={channelSlugs} />}
        </div>
        {isCursor && !message.deleted && <span className="chat-row__actions">
          <button type="button" className="icon-button" title="Reply (R)" aria-label="Reply" onClick={event => { event.stopPropagation(); onAction('reply', message) }}><CornerUpLeft /></button>
          {!mine && <button type="button" className="icon-button" title="Message privately (D)" aria-label={`Message ${message.sender.username} privately`} onClick={event => { event.stopPropagation(); onAction('dm', message) }}><MessageSquare /></button>}
          <button type="button" className="icon-button" title="Profile (P)" aria-label={`Profile of ${message.sender.username}`} onClick={event => { event.stopPropagation(); onAction('profile', message) }}><AtSign /></button>
          {(mine || (me.role === 'admin' && message.channel_id)) && <button type="button" className={armed === message.message_id ? 'icon-button is-armed' : 'icon-button'} title="Delete (X twice)" aria-label={armed === message.message_id ? 'Confirm delete' : 'Delete message'} onClick={event => { event.stopPropagation(); onAction('delete', message) }}><Trash2 /></button>}
          {!mine && <button type="button" className={armed === `block:${message.sender.user_id}` ? 'icon-button is-armed' : 'icon-button'} title="Block this person (B twice)" aria-label={armed === `block:${message.sender.user_id}` ? `Confirm blocking ${message.sender.username}` : `Block ${message.sender.username}`} onClick={event => { event.stopPropagation(); onAction('block', message) }}><UserX /></button>}
        </span>}
      </div></Fragment>
    })}
  </div>
}

function WhereChip({ where }: { where: Where }) {
  return <Link className="chat-where" to={where.href} style={{ borderLeftColor: where.color }} title={`Open ${where.label}`} onClick={event => event.stopPropagation()}>{where.locked && <Lock aria-hidden="true" />}{where.label}</Link>
}
