export interface CompanyRef { code: string; name: string; ticker?: string | null }

export interface MessageBody {
  text: string
  refs: Record<string, CompanyRef>
  unreadable?: boolean
}

export interface ChatSender { user_id: string; username: string; display_name?: string | null }

export interface SystemEvent {
  event: 'created' | 'invited' | 'joined' | 'declined' | 'left' | 'removed' | 'renamed' | 'deleted' | 'key_added' | 'key_changed' | 'keys_shared' | 'rotated'
  key_version?: number
  /** For ``deleted``: the message that was deleted. */
  message_id?: string
  kind?: string
  title?: string | null
  members?: string[]
}

export interface ChatMessage {
  seq: number
  message_id: string
  kind: 'message' | 'system'
  channel_id: string | null
  conversation_id: string | null
  sender: ChatSender
  created_at: string
  reply_to?: string | null
  deleted: boolean
  body?: MessageBody
  encrypted?: { ciphertext: string; nonce: string; key_version: number }
  system?: SystemEvent
}

export interface Channel {
  channel_id: string
  kind: 'topic' | 'company'
  name: string
  description?: string | null
  slug?: string
  company_code?: string
  ticker?: string | null
  industry?: string | null
  subscribers?: number
  message_count?: number
  last_message_at?: string | null
  subscribed?: boolean
  blocked?: boolean
  unread?: number
  last_read_seq?: number | null
}

export interface ConversationMember {
  user_id: string
  username: string
  display_name?: string | null
  role: 'owner' | 'member'
  status: 'active' | 'invited'
  key_id?: string | null
  fingerprint?: string | null
}

export interface Conversation {
  conversation_id: string
  kind: 'dm' | 'group'
  title?: string | null
  key_version: number
  rekey_needed: boolean
  my_status: 'active' | 'invited'
  my_role: 'owner' | 'member'
  hidden: boolean
  last_seq?: number | null
  last_read_seq: number
  last_message_at?: string | null
  unread: number
  /** How many key copies you hold here; it changes when someone shares a key with you. */
  my_wraps: number
  members: ConversationMember[]
}

export interface BlockedUser { user_id: string; username: string; display_name?: string | null; created_at: string }
export interface Blocks { users: BlockedUser[]; channels: Array<Channel & { created_at: string }> }

export interface ChatProfile {
  user_id: string
  username: string
  display_name?: string | null
  bio?: string | null
  location?: string | null
  website?: string | null
  interests: string[]
  companies: string[]
  allow_dms: 'everyone' | 'nobody'
  discoverable: boolean
}

export interface ChatSummary {
  me: { user_id: string; username: string; role: string; profile: ChatProfile | null }
  key: { key_id: string; fingerprint: string; created_at: string } | null
  channels: Channel[]
  conversations: Conversation[]
  blocks: Blocks
  cursor: number
}

export interface OwnKey {
  key_id: string
  user_id: string
  public_key: string
  fingerprint: string
  wrapped_private_key: string
  wrap_salt: string
  wrap_iv: string
  wrap_iterations: number
  created_at: string
}

export interface PublicKey { key_id: string; user_id: string; public_key: string; fingerprint: string; active: boolean }

export interface KeyWrap {
  key_version: number
  recipient_key_id: string
  wrapper_user_id: string
  wrapper_key_id: string
  wrapped_key: string
  iv: string
}

export interface ConversationKeys {
  key_version: number
  rekey_needed: boolean
  wraps: KeyWrap[]
  wrapper_keys: PublicKey[]
  coverage: Record<string, Record<string, number[]>>
}

export interface Person {
  user_id: string
  username: string
  display_name?: string | null
  accepts_messages: boolean
  blocked: boolean
  is_me: boolean
}

export interface PublicProfile {
  user_id: string
  username: string
  display_name?: string | null
  bio?: string | null
  location?: string | null
  website?: string | null
  interests: string[]
  companies: Array<{ code: string; name: string }>
  accepts_messages: boolean
  member_since?: string | null
  role?: string | null
  key_fingerprint?: string | null
  is_me: boolean
  blocked_by_me: boolean
  can_message: boolean
  stats: { posts: number; first_post?: string | null; last_post?: string | null; top_channels: Array<{ channel_id: string; name: string; posts: number }> }
  recent_posts: Array<ChatMessage & { channel_name: string }>
}

/** What the main pane shows: everything, one channel, or one conversation. */
export type ChatTarget =
  | { type: 'feed' }
  | { type: 'channel'; id: string }
  | { type: 'conversation'; id: string }
