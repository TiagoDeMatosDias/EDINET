import { Building2, Landmark, User, UserRound } from 'lucide-react'
import type { ReactNode } from 'react'
import { Link } from 'react-router-dom'

import { Tip } from '../../components/Tooltip'
import { entityHref, filedName, KIND_HINTS, KIND_LABELS } from './ownershipFormat'
import type { EntityKind, EntityNames } from './ownershipTypes'

const KIND_ICONS = { person: UserRound, individual: User, company: Building2, organization: Landmark } as const

export function Kpi({ label, value, detail, tip }: { label: string; value: ReactNode; detail?: ReactNode; tip?: string }) {
  return <div className="ow-kpi"><span>{tip ? <Tip content={tip}>{label}</Tip> : label}</span><strong>{value}</strong>{detail && <small>{detail}</small>}</div>
}

export function KindIcon({ kind }: { kind: EntityKind }) {
  const Icon = KIND_ICONS[kind] ?? Landmark
  return <span className={`ow-kind ow-kind--${kind}`} title={`${KIND_LABELS[kind]}: ${KIND_HINTS[kind]}`}><Icon aria-hidden="true" /><span className="sr-only">{KIND_LABELS[kind]}</span></span>
}

/**
 * A person's or organisation's name, linking to its Research page. The name is
 * in English where a source gives one; hovering shows it as filed.
 */
export function EntityLink({ entity, className }: { entity: Pick<EntityNames, 'entity_id' | 'name' | 'name_ja'>; className?: string }) {
  if (!entity.entity_id) return <span className={className} title={filedName(entity)}>{entity.name}</span>
  return <Link className={className} to={entityHref(entity.entity_id)} title={filedName(entity) ?? 'Open in Research: People & holders'} onClick={event => event.stopPropagation()}>{entity.name}</Link>
}

/** A stake drawn as a bar scaled to the largest stake in the table. */
export function StakeBar({ ratio, max }: { ratio: number | null | undefined; max: number }) {
  if (ratio == null || !(max > 0)) return null
  return <span className="ow-bar" aria-hidden="true"><span style={{ width: `${Math.max(1, Math.min(100, (ratio / max) * 100))}%` }} /></span>
}
