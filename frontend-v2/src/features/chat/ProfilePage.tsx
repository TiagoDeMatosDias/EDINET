import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Ban, Globe, KeyRound, MapPin, MessageSquare, Pencil } from 'lucide-react'
import { useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'

import { ErrorState, LoadingState } from '../../components/Feedback'
import { useHotkeys } from '../../hooks/useHotkeys'
import { chatApi, chatKeys, invalidateChat } from './chatApi'
import { colorFor, fullTime, personName, relativeTime } from './chatModel'
import { formatFingerprint } from './crypto'
import { MessageText } from './MessageList'
import './chat.css'
import './profile.css'

const shortDate = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })

/** "#stocks" for a topic, "$Toyota…" for a company channel. */
function placeName(channelId: string, name: string) {
  return channelId.startsWith('topic:') ? `#${channelId.slice(6)}` : `$${name}`
}

/** A member's public profile: who they are, what they follow, what they post, and a way to talk to them. */
export default function ProfilePage() {
  const { username = '' } = useParams()
  const navigate = useNavigate()
  const client = useQueryClient()
  const profile = useQuery({ queryKey: chatKeys.profile(username), queryFn: () => chatApi.profile(username), retry: false })
  const [armed, setArmed] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const data = profile.data

  const toggleBlock = async () => {
    if (!data) return
    if (!data.blocked_by_me && !armed) { setArmed(true); return }
    setArmed(false)
    try {
      await chatApi.block('user', data.user_id, !data.blocked_by_me)
      invalidateChat(client)
      void profile.refetch()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'That did not work')
    }
  }
  const message = () => { if (data?.can_message) navigate(`/chat?dm=${encodeURIComponent(data.user_id)}`) }
  useHotkeys({
    m: message,
    b: () => { if (data && !data.is_me) void toggleBlock() },
    e: () => { if (data?.is_me) navigate('/account#profile') },
    Escape: () => setArmed(false),
  })

  if (profile.isLoading) return <LoadingState label="Loading profile" />
  if (profile.isError || !data) return <ErrorState error={profile.error} retry={() => profile.refetch()} />

  return <div className="profile-page dense-page">
    <header className="profile-head">
      <span className="profile-avatar" style={{ background: colorFor(data.user_id) }} aria-hidden="true">{personName(data).slice(0, 1).toUpperCase()}</span>
      <div className="profile-head__name">
        <h1>{personName(data)}</h1>
        <p>@{data.username}{data.role === 'admin' && <span className="badge">admin</span>}{data.member_since && <> · member since {new Date(data.member_since).toLocaleDateString('en-GB', { month: 'short', year: 'numeric' })}</>}</p>
      </div>
      <div className="profile-head__actions">
        {data.is_me
          ? <Link className="button button--secondary button--small" to="/account#profile"><Pencil aria-hidden="true" />Edit profile <kbd>E</kbd></Link>
          : <>
            <button type="button" className="button button--primary button--small" disabled={!data.can_message} onClick={message} title={data.can_message ? 'End-to-end encrypted conversation (M)' : data.blocked_by_me ? 'Unblock to message' : 'Not accepting messages'}><MessageSquare aria-hidden="true" />Message <kbd>M</kbd></button>
            <button type="button" className={armed ? 'button button--danger button--small' : 'button button--ghost button--small'} onClick={() => void toggleBlock()}><Ban aria-hidden="true" />{data.blocked_by_me ? 'Unblock' : armed ? 'Block: sure?' : 'Block'} <kbd>B</kbd></button>
          </>}
      </div>
    </header>
    {error && <p className="form-error" role="alert">{error}</p>}
    {data.blocked_by_me && <p className="callout callout--warning">You blocked {personName(data)}: their messages are hidden from you and they cannot message you.</p>}
    <div className="profile-grid">
      <section className="profile-card" aria-label="About">
        <h2>About</h2>
        {data.bio ? <p className="profile-bio">{data.bio}</p> : <p className="muted">{data.is_me ? 'Add a short bio in Account so others know your focus.' : 'No bio yet.'}</p>}
        <dl className="profile-facts">
          {data.location && <div><dt><MapPin aria-hidden="true" />Location</dt><dd>{data.location}</dd></div>}
          {data.website && <div><dt><Globe aria-hidden="true" />Website</dt><dd><a href={data.website} target="_blank" rel="noopener noreferrer nofollow">{data.website.replace(/^https?:\/\//, '')}</a></dd></div>}
          <div><dt><KeyRound aria-hidden="true" />Encryption key</dt><dd>{data.key_fingerprint ? <code title="Compare with the person to confirm private messages reach them alone">{formatFingerprint(data.key_fingerprint)}</code> : <span className="muted">not set up yet</span>}</dd></div>
          <div><dt>Messages</dt><dd>{data.accepts_messages ? 'Accepts private messages' : 'Not accepting private messages'}</dd></div>
        </dl>
        {data.interests.length > 0 && <><h3>Interests</h3><div className="profile-chips">{data.interests.map(item => <span key={item} className="profile-chip">{item}</span>)}</div></>}
        {data.companies.length > 0 && <><h3>Companies they follow</h3><ul className="profile-companies">{data.companies.map(item => <li key={item.code}>
          <Link to={`/analyze/${encodeURIComponent(item.code)}`}>{item.name}</Link>
          <Link className="profile-companies__channel" to={`/chat?c=company:${encodeURIComponent(item.code)}`} title="The company's channel">#</Link>
        </li>)}</ul></>}
      </section>
      <section className="profile-card" aria-label="Activity">
        <h2>Public activity</h2>
        <dl className="profile-stats">
          <div><dt>Posts</dt><dd>{data.stats.posts}</dd></div>
          <div><dt>First</dt><dd>{data.stats.first_post ? shortDate.format(new Date(data.stats.first_post)) : '—'}</dd></div>
          <div><dt>Latest</dt><dd>{data.stats.last_post ? relativeTime(data.stats.last_post) : '—'}</dd></div>
        </dl>
        {data.stats.top_channels.length > 0 && <div className="profile-chips">{data.stats.top_channels.map(item => <Link key={item.channel_id} className="profile-chip" to={`/chat?c=${encodeURIComponent(item.channel_id)}`} style={{ borderLeftColor: colorFor(item.channel_id) }}>{placeName(item.channel_id, item.name)} <small>{item.posts}</small></Link>)}</div>}
        <h3>Recent posts</h3>
        {data.recent_posts.length ? <ul className="profile-posts">{[...data.recent_posts].reverse().map(post => <li key={post.message_id}>
          <Link className="chat-where" to={`/chat?c=${encodeURIComponent(post.channel_id ?? '')}`} style={{ borderLeftColor: colorFor(post.channel_id ?? '') }}>{placeName(post.channel_id ?? '', post.channel_name)}</Link>
          <time dateTime={post.created_at} title={fullTime(post.created_at)}>{relativeTime(post.created_at)}</time>
          {post.body && <MessageText body={post.body} me="" channelSlugs={new Set()} />}
        </li>)}</ul> : <p className="muted">No public posts yet.</p>}
      </section>
    </div>
  </div>
}
