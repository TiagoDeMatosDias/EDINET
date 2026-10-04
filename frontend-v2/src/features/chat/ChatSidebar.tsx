import { Hash, Inbox, Lock, MessagesSquare, Plus, Users } from 'lucide-react'
import type { ReactNode } from 'react'

import { channelPrefix, sameTarget, type SidebarEntry } from './chatModel'
import type { ChatTarget } from './chatTypes'

function Section({ title, action, children }: { title: string; action?: ReactNode; children: ReactNode }) {
  return <section className="chat-side__section"><header><h3>{title}</h3>{action}</header><ul>{children}</ul></section>
}

export function ChatSidebar({ entries, current, onSelect, onNewDm, onNewGroup, locked }: {
  entries: SidebarEntry[]
  current: ChatTarget
  onSelect: (target: ChatTarget) => void
  onNewDm: () => void
  onNewGroup: () => void
  locked: boolean
}) {
  const item = (entry: SidebarEntry) => {
    const active = sameTarget(entry.target, current)
    const prefix = entry.icon === 'topic' ? '#' : entry.icon === 'company' ? channelPrefix({ kind: 'company' }) : null
    return <li key={entry.key}>
      <button type="button" className={['chat-side__item', active && 'is-active', entry.muted && 'is-muted', entry.unread > 0 && 'has-unread'].filter(Boolean).join(' ')} aria-current={active ? 'true' : undefined} onClick={() => onSelect(entry.target)} title={entry.muted ? `${entry.label} (not subscribed)` : entry.label}>
        <span className="chat-dot" style={{ background: entry.color }} aria-hidden="true" />
        {entry.icon === 'feed' && <Inbox aria-hidden="true" />}
        {entry.icon === 'invite' && <Users aria-hidden="true" />}
        {(entry.icon === 'dm' || entry.icon === 'group') && <Lock className="chat-side__lock" aria-label="End-to-end encrypted" />}
        <span className="chat-side__label">{prefix && <span className="chat-side__prefix">{prefix}</span>}{entry.label}</span>
        {entry.detail && <small>{entry.detail}</small>}
        {entry.unread > 0 && <span className="chat-badge">{entry.icon === 'invite' ? 'invited' : entry.unread > 99 ? '99+' : entry.unread}</span>}
      </button>
    </li>
  }
  const of = (...icons: SidebarEntry['icon'][]) => entries.filter(entry => icons.includes(entry.icon))
  return <nav className="chat-side" aria-label="Channels and conversations">
    <ul className="chat-side__top">{of('feed').map(item)}</ul>
    {of('invite').length > 0 && <Section title="Invitations">{of('invite').map(item)}</Section>}
    <Section title="Channels" action={<Hash aria-hidden="true" />}>{of('topic').map(item)}</Section>
    <Section title="Companies" action={<span className="chat-side__hint" title="Open any company's channel with T, or from its analysis page">T</span>}>{of('company').length ? of('company').map(item) : <li className="chat-side__empty">Press <kbd>T</kbd> and type a company</li>}</Section>
    <Section title="Direct messages" action={<button type="button" className="icon-button" aria-label="New direct message (N)" title="New direct message (N)" onClick={onNewDm}><Plus /></button>}>
      {of('dm').length ? of('dm').map(item) : <li className="chat-side__empty">{locked ? 'Unlock to read' : 'None yet'} · <kbd>N</kbd></li>}
    </Section>
    <Section title="Groups" action={<button type="button" className="icon-button" aria-label="New group (Shift+N)" title="New group (Shift+N)" onClick={onNewGroup}><MessagesSquare /></button>}>
      {of('group').length ? of('group').map(item) : <li className="chat-side__empty">None yet · <kbd>Shift</kbd>+<kbd>N</kbd></li>}
    </Section>
  </nav>
}
