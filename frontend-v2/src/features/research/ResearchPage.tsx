import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { apiRequest } from '../../api/client'
import type { SecuritySearchResult } from '../../api/types'
import { CompanyPicker } from '../../components/CompanyPicker'
import { EmptyState, LoadingState } from '../../components/Feedback'
import { Card, PageHeader } from '../../components/Page'

interface TagSummary { name: string; member_count: number }
interface TagListResponse { tags: TagSummary[] }
interface TagMembersResponse { tag: string; companies: string[] }
interface Note { note_id: string; title: string; body: string; edinet_code?: string | null; version?: number }
interface Alert { alert_id: string; name: string; metric: string; operator: string; value: number }
interface CompanyResearch { edinet_code?: string; thesis_status?: string | null; target_value?: number | null; target_currency?: string | null; review_on?: string | null; version?: number }
interface ThesisDraft { thesis_status: string; target_value: string; target_currency: string; review_on: string }

function companyReference(code: string | null | undefined): SecuritySearchResult | null {
  return code ? { company_code: code, ticker: '', company_name: code } : null
}

function TagPanel() {
  const client = useQueryClient()
  const [newTag, setNewTag] = useState('')
  const [selectedTag, setSelectedTag] = useState('')
  const [member, setMember] = useState<SecuritySearchResult | null>(null)
  const tags = useQuery({ queryKey: ['research-tags'], queryFn: () => apiRequest<TagListResponse>('/api/tags') })
  const activeTag = selectedTag || tags.data?.tags[0]?.name || ''
  const members = useQuery({
    queryKey: ['research-tag-members', activeTag],
    enabled: Boolean(activeTag),
    queryFn: () => apiRequest<TagMembersResponse>(`/api/tags/${encodeURIComponent(activeTag)}/companies`),
  })

  const createTag = useMutation({
    mutationFn: () => apiRequest('/api/tags', { method: 'POST', body: JSON.stringify({ name: newTag.trim() }) }),
    onSuccess: () => { setSelectedTag(newTag.trim()); setNewTag(''); void client.invalidateQueries({ queryKey: ['research-tags'] }) },
  })
  const deleteTag = useMutation({
    mutationFn: (name: string) => apiRequest(`/api/tags/${encodeURIComponent(name)}`, { method: 'DELETE' }),
    onSuccess: () => { setSelectedTag(''); void client.invalidateQueries({ queryKey: ['research-tags'] }); void client.invalidateQueries({ queryKey: ['research-tag-members'] }) },
  })
  const addMember = useMutation({
    mutationFn: () => apiRequest(`/api/tags/${encodeURIComponent(member?.company_code ?? '')}/${encodeURIComponent(activeTag)}`, { method: 'POST' }),
    onSuccess: () => { setMember(null); void client.invalidateQueries({ queryKey: ['research-tags'] }); void client.invalidateQueries({ queryKey: ['research-tag-members', activeTag] }) },
  })
  const removeMember = useMutation({
    mutationFn: (code: string) => apiRequest(`/api/tags/${encodeURIComponent(code)}/${encodeURIComponent(activeTag)}`, { method: 'DELETE' }),
    onSuccess: () => { void client.invalidateQueries({ queryKey: ['research-tags'] }); void client.invalidateQueries({ queryKey: ['research-tag-members', activeTag] }) },
  })

  return <Card title="Tags, favorites & watchlists" description="Favorites and named watchlists are ordinary private tags, so the same labels work in screening and analysis.">
    <div className="button-row"><input className="input" value={newTag} onChange={event => setNewTag(event.target.value)} placeholder="New tag or watchlist" /><button className="button button--primary" disabled={!newTag.trim() || createTag.isPending} onClick={() => createTag.mutate()}>Create</button></div>
    {tags.isLoading && <LoadingState label="Loading tags" />}
    <div className="tag-collection">{tags.data?.tags.map(tag => <button type="button" className={`tag-collection-item ${activeTag === tag.name ? 'active' : ''}`} key={tag.name} onClick={() => setSelectedTag(tag.name)}><span>{tag.name}</span><small>{tag.member_count} companies</small></button>)}</div>
    {!tags.isLoading && !tags.data?.tags.length && <EmptyState title="No tags yet" description="Create a tag or favorite a company from analysis." />}
    {activeTag && <div className="tag-members"><div className="card-header"><div><h3>{activeTag}</h3><p>Add companies with the shared company search.</p></div>{activeTag !== 'Favorite' && <button className="text-button text-button--danger" onClick={() => deleteTag.mutate(activeTag)}>Delete tag</button>}</div><div className="button-row"><CompanyPicker selected={member} onSelect={setMember} clearOnSelect label="Add company" /><button className="button button--secondary" disabled={!member?.company_code || addMember.isPending} onClick={() => addMember.mutate()}>{addMember.isPending ? 'Adding…' : 'Add'}</button></div>{member && <p className="muted">Selected: {member.company_name || member.ticker || member.company_code}</p>}{addMember.error && <p className="form-error">{(addMember.error as Error).message}</p>}{members.error && <p className="form-error">{(members.error as Error).message}</p>}{members.isLoading && <LoadingState label="Loading members" />}{members.data?.companies.map(code => <div className="research-row" key={code}><strong>{code}</strong><button className="text-button text-button--danger" onClick={() => removeMember.mutate(code)}>Remove</button></div>)}{members.data && !members.data.companies.length && <p className="muted">No companies in this tag yet.</p>}</div>}
  </Card>
}

export default function ResearchPage() {
  const client = useQueryClient()
  const [noteTitle, setNoteTitle] = useState('')
  const [noteBody, setNoteBody] = useState('')
  const [noteCompany, setNoteCompany] = useState<SecuritySearchResult | null>(null)
  const [noteVersion, setNoteVersion] = useState<number | null>(null)
  const [editingNoteId, setEditingNoteId] = useState<string | null>(null)
  const [alertName, setAlertName] = useState('')
  const [alertCompany, setAlertCompany] = useState<SecuritySearchResult | null>(null)
  const [alertValue, setAlertValue] = useState('')
  const [alertOperator, setAlertOperator] = useState('>')
  const [alertMetric, setAlertMetric] = useState('LatestPrice')
  const [thesisCompany, setThesisCompany] = useState<SecuritySearchResult | null>(null)
  const [thesisDraft, setThesisDraft] = useState<ThesisDraft | null>(null)

  const notes = useQuery({ queryKey: ['research-notes'], queryFn: () => apiRequest<{ notes: Note[] }>('/api/research/notes') })
  const alerts = useQuery({ queryKey: ['research-alerts'], queryFn: () => apiRequest<{ alerts: Alert[] }>('/api/research/alerts') })
  const thesisCode = thesisCompany?.company_code ?? ''
  const thesisQuery = useQuery({
    queryKey: ['research-thesis', thesisCode],
    enabled: Boolean(thesisCode),
    queryFn: () => apiRequest<CompanyResearch>(`/api/research/companies/${encodeURIComponent(thesisCode)}`),
  })
  const thesisValues = thesisDraft ?? {
    thesis_status: thesisQuery.data?.thesis_status ?? '',
    target_value: thesisQuery.data?.target_value == null ? '' : String(thesisQuery.data.target_value),
    target_currency: thesisQuery.data?.target_currency ?? 'JPY',
    review_on: thesisQuery.data?.review_on ?? '',
  }

  const saveNote = useMutation({
    mutationFn: () => {
      const body: Record<string, unknown> = { title: noteTitle, body: noteBody, edinet_code: noteCompany?.company_code || undefined }
      if (editingNoteId && noteVersion !== null) body.expected_version = noteVersion
      return editingNoteId ? apiRequest(`/api/research/notes/${editingNoteId}`, { method: 'PATCH', body: JSON.stringify(body) }) : apiRequest('/api/research/notes', { method: 'POST', body: JSON.stringify(body) })
    },
    onSuccess: () => { setNoteTitle(''); setNoteBody(''); setNoteCompany(null); setEditingNoteId(null); setNoteVersion(null); void client.invalidateQueries({ queryKey: ['research-notes'] }) },
    onError: (error: Error) => { if (error.message.includes('version')) setNoteVersion(Number(error.message.match(/\d+/)?.[0])) },
  })
  const deleteNote = useMutation({ mutationFn: (id: string) => apiRequest(`/api/research/notes/${id}`, { method: 'DELETE' }), onSuccess: () => void client.invalidateQueries({ queryKey: ['research-notes'] }) })
  const createAlert = useMutation({
    mutationFn: () => apiRequest('/api/research/alerts', { method: 'POST', body: JSON.stringify({ name: alertName, edinet_code: alertCompany?.company_code || undefined, metric: alertMetric, operator: alertOperator, value: Number(alertValue) }) }),
    onSuccess: () => { setAlertName(''); setAlertValue(''); setAlertCompany(null); void client.invalidateQueries({ queryKey: ['research-alerts'] }) },
  })
  const deleteAlert = useMutation({ mutationFn: (id: string) => apiRequest(`/api/research/alerts/${id}`, { method: 'DELETE' }), onSuccess: () => void client.invalidateQueries({ queryKey: ['research-alerts'] }) })
  const saveThesis = useMutation({
    mutationFn: () => apiRequest(`/api/research/companies/${encodeURIComponent(thesisCode)}`, { method: 'PATCH', body: JSON.stringify({ thesis_status: thesisValues.thesis_status || undefined, target_value: thesisValues.target_value ? Number(thesisValues.target_value) : undefined, target_currency: thesisValues.target_currency || undefined, review_on: thesisValues.review_on || undefined }) }),
    onSuccess: () => { setThesisDraft(null); void client.invalidateQueries({ queryKey: ['research-thesis', thesisCode] }); void thesisQuery.refetch() },
  })

  const startEditNote = (note: Note) => { setEditingNoteId(note.note_id); setNoteTitle(note.title); setNoteBody(note.body); setNoteCompany(companyReference(note.edinet_code)); setNoteVersion(note.version ?? null) }
  const cancelEdit = () => { setEditingNoteId(null); setNoteTitle(''); setNoteBody(''); setNoteCompany(null); setNoteVersion(null) }

  return <div className="stack dense-page"><PageHeader eyebrow="Research state" title="Tags and research" description="Tags, favorites, notes, thesis tracking, and alerts are isolated from other accounts." /><TagPanel /><div className="three-column research-grid"><Card title="Notes"><label className="field-label">Title<input className="input" value={noteTitle} onChange={event => setNoteTitle(event.target.value)} /></label><CompanyPicker selected={noteCompany} onSelect={setNoteCompany} label="Company (optional)" /><label className="field-label">Body<textarea className="input" rows={3} value={noteBody} onChange={event => setNoteBody(event.target.value)} /></label><div className="button-row"><button className="button button--primary" disabled={!noteTitle.trim() || !noteBody.trim() || saveNote.isPending} onClick={() => saveNote.mutate()}>{editingNoteId ? 'Update note' : 'Save note'}</button>{editingNoteId && <button className="text-button" onClick={cancelEdit}>Cancel</button>}</div>{saveNote.error && <p className="form-error">{(saveNote.error as Error).message}</p>}{notes.isLoading && <LoadingState label="Loading notes" />}{notes.data?.notes.slice(0, 15).map(note => <article className="research-note" key={note.note_id}><div><strong>{note.title}</strong><small>{note.edinet_code || 'General'} · v{note.version ?? 1}</small></div><p>{note.body.slice(0, 300)}{note.body.length > 300 ? '…' : ''}</p><div className="button-row"><button className="text-button" onClick={() => startEditNote(note)}>Edit</button><button className="text-button text-button--danger" onClick={() => deleteNote.mutate(note.note_id)}>Delete</button></div></article>)}</Card><Card title="Thesis & targets"><CompanyPicker selected={thesisCompany} onSelect={company => { setThesisCompany(company); setThesisDraft(null) }} label="Company" />{thesisQuery.isLoading && <LoadingState label="Loading thesis" />}{thesisQuery.data && <div className="stack"><label className="field-label">Thesis status<select className="select" value={thesisValues.thesis_status} onChange={event => setThesisDraft({ ...thesisValues, thesis_status: event.target.value })}><option value="">None</option><option value="watch">Watch</option><option value="buy">Buy</option><option value="hold">Hold</option><option value="sell">Sell</option></select></label><label className="field-label">Target value<input className="input" type="number" value={thesisValues.target_value} onChange={event => setThesisDraft({ ...thesisValues, target_value: event.target.value })} /></label><label className="field-label">Currency<select className="select" value={thesisValues.target_currency} onChange={event => setThesisDraft({ ...thesisValues, target_currency: event.target.value })}><option>JPY</option><option>USD</option><option>EUR</option></select></label><label className="field-label">Review on<input className="input" type="date" value={thesisValues.review_on} onChange={event => setThesisDraft({ ...thesisValues, review_on: event.target.value })} /></label><button className="button button--primary" disabled={saveThesis.isPending} onClick={() => saveThesis.mutate()}>Save thesis</button></div>}</Card><Card title="Alerts"><label className="field-label">Name<input className="input" value={alertName} onChange={event => setAlertName(event.target.value)} placeholder="Price alert" /></label><CompanyPicker selected={alertCompany} onSelect={setAlertCompany} label="Company" /><div className="button-row"><select className="select compact" value={alertMetric} onChange={event => setAlertMetric(event.target.value)}><option>LatestPrice</option><option>PERatio</option><option>DividendsYield</option><option>ReturnOnEquity</option></select><select className="select compact" value={alertOperator} onChange={event => setAlertOperator(event.target.value)}><option>{'>'}</option><option>{'>='}</option><option>{'<'}</option><option>{'<='}</option><option>=</option></select></div><label className="field-label">Value<input className="input" type="number" value={alertValue} onChange={event => setAlertValue(event.target.value)} /></label><button className="button button--primary" disabled={!alertCompany?.company_code || !alertValue || createAlert.isPending} onClick={() => createAlert.mutate()}>Create alert</button>{alerts.isLoading && <LoadingState label="Loading alerts" />}{alerts.data?.alerts.map(alert => <div className="research-row" key={alert.alert_id}><div><strong>{alert.name}</strong><small>{alert.metric} {alert.operator} {alert.value}</small></div><button className="text-button text-button--danger" onClick={() => deleteAlert.mutate(alert.alert_id)}>Remove</button></div>)}</Card></div></div>
}
