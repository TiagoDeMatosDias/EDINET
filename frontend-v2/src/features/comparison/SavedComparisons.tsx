import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Bookmark, X } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'

import { ApiError, apiPost, apiRequest } from '../../api/client'
import { parseList } from './comparisonModel'
import type { SavedComparison } from './comparisonTypes'

/**
 * Named company sets, kept on the server. O opens the list; Ctrl+S opens it
 * ready to save the current set. An empty metric list means the standard metrics.
 */
export function SavedComparisons({ open, saving, codes, metrics, defaultName, onOpenChange, onLoad }: {
  open: boolean
  /** Opened to save (Ctrl+S): the name field takes focus. */
  saving: boolean
  codes: string[]
  /** The metrics to store, or ``[]`` for the standard set. */
  metrics: string[]
  defaultName: string
  onOpenChange: (open: boolean) => void
  onLoad: (codes: string[], metrics: string[]) => void
}) {
  const client = useQueryClient()
  const wrapper = useRef<HTMLDivElement>(null)
  const nameInput = useRef<HTMLInputElement>(null)
  const list = useRef<HTMLUListElement>(null)
  const [name, setName] = useState('')
  const templates = useQuery({
    queryKey: ['comparison-templates'],
    queryFn: () => apiRequest<{ templates: SavedComparison[] }>('/api/research/comparison-templates'),
    enabled: open,
  })
  const save = useMutation({
    mutationFn: (label: string) => apiPost<SavedComparison>('/api/research/comparison-templates', { name: label, company_codes: codes, metrics }),
    onSuccess: () => { setName(''); void client.invalidateQueries({ queryKey: ['comparison-templates'] }) },
  })
  const remove = useMutation({
    mutationFn: (id: string) => apiRequest(`/api/research/comparison-templates/${encodeURIComponent(id)}`, { method: 'DELETE' }),
    onSuccess: () => void client.invalidateQueries({ queryKey: ['comparison-templates'] }),
  })
  useEffect(() => {
    if (!open) return
    if (saving && codes.length >= 2) nameInput.current?.focus()
    else (list.current?.querySelector<HTMLElement>('button') ?? nameInput.current)?.focus()
    const onPointerDown = (event: MouseEvent) => { if (!wrapper.current?.contains(event.target as Node)) onOpenChange(false) }
    document.addEventListener('mousedown', onPointerDown)
    return () => document.removeEventListener('mousedown', onPointerDown)
  }, [codes.length, onOpenChange, open, saving, templates.data])
  const items = templates.data?.templates ?? []
  const submit = () => {
    const label = (name.trim() || defaultName).slice(0, 100)
    if (label && codes.length >= 2) save.mutate(label)
  }

  return <div ref={wrapper} className="cmp-saved" onKeyDown={event => { if (event.key === 'Escape') { event.stopPropagation(); onOpenChange(false) } }}>
    <button type="button" className="button button--secondary button--small" aria-expanded={open} aria-haspopup="dialog" onClick={() => onOpenChange(!open)} title="Saved comparisons (O); save this one with Ctrl+S"><Bookmark aria-hidden="true" />Saved</button>
    {open && <div className="cmp-saved__popover" role="dialog" aria-label="Saved comparisons">
      <form className="cmp-saved__save" onSubmit={event => { event.preventDefault(); submit() }}>
        <input ref={nameInput} className="input" aria-label="Name for this comparison" placeholder={codes.length >= 2 ? defaultName : 'Add two companies to save'} value={name} maxLength={100} disabled={codes.length < 2} onChange={event => setName(event.target.value)} />
        <button type="submit" className="button button--primary button--small" disabled={codes.length < 2 || save.isPending}>Save</button>
      </form>
      {save.error && <p className="form-error">{save.error instanceof ApiError && save.error.status === 409 ? 'A comparison with this name already exists.' : save.error.message}</p>}
      {templates.isLoading && <p className="cmp-panel__empty">Loading…</p>}
      {templates.error && <p className="form-error">Could not load saved comparisons.</p>}
      {!templates.isLoading && !items.length && !templates.error && <p className="cmp-panel__empty">No saved comparisons yet.</p>}
      {items.length > 0 && <ul ref={list} className="cmp-saved__list" onKeyDown={event => {
        if (event.key !== 'ArrowDown' && event.key !== 'ArrowUp') return
        event.preventDefault()
        const buttons = [...event.currentTarget.querySelectorAll<HTMLButtonElement>('.cmp-saved__open')]
        const index = buttons.indexOf(document.activeElement as HTMLButtonElement)
        buttons[(index + (event.key === 'ArrowDown' ? 1 : -1) + buttons.length) % buttons.length]?.focus()
      }}>
        {items.map(item => {
          const companies = parseList(item.companies_json)
          const chosen = parseList(item.metrics_json)
          return <li key={item.template_id}>
            <button type="button" className="cmp-saved__open" onClick={() => { onLoad(companies, chosen); onOpenChange(false) }}>
              <strong>{item.name}</strong>
              <small>{companies.length} companies · {chosen.length ? `${chosen.length} metrics` : 'standard metrics'}</small>
            </button>
            <button type="button" className="icon-button" aria-label={`Delete ${item.name}`} onClick={() => remove.mutate(item.template_id)}><X /></button>
          </li>
        })}
      </ul>}
    </div>}
  </div>
}
