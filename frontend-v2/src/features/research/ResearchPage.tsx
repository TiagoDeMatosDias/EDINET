import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { apiRequest } from '../../api/client'
import { EmptyState, LoadingState } from '../../components/Feedback'
import { Card, PageHeader } from '../../components/Page'

interface Watchlist { watchlist_id: string; name: string; item_count: number }
interface Note { note_id: string; title: string; body: string; edinet_code?: string | null }
interface Alert { alert_id: string; name: string; metric: string; operator: string; value: number }

export default function ResearchPage() {
  const client = useQueryClient()
  const [watchlistName, setWatchlistName] = useState('My watchlist')
  const [noteTitle, setNoteTitle] = useState('')
  const [noteBody, setNoteBody] = useState('')
  const [noteCode, setNoteCode] = useState('')
  const [alertName, setAlertName] = useState('Price alert')
  const [alertCode, setAlertCode] = useState('')
  const [alertValue, setAlertValue] = useState('')
  const watchlists = useQuery({ queryKey: ['research-watchlists'], queryFn: () => apiRequest<{ watchlists: Watchlist[] }>('/api/research/watchlists') })
  const notes = useQuery({ queryKey: ['research-notes'], queryFn: () => apiRequest<{ notes: Note[] }>('/api/research/notes') })
  const alerts = useQuery({ queryKey: ['research-alerts'], queryFn: () => apiRequest<{ alerts: Alert[] }>('/api/research/alerts') })
  const createWatchlist = useMutation({ mutationFn: () => apiRequest('/api/research/watchlists', { method: 'POST', body: JSON.stringify({ name: watchlistName }) }), onSuccess: () => { setWatchlistName(''); void client.invalidateQueries({ queryKey: ['research-watchlists'] }) } })
  const createNote = useMutation({ mutationFn: () => apiRequest('/api/research/notes', { method: 'POST', body: JSON.stringify({ title: noteTitle, body: noteBody, edinet_code: noteCode || undefined }) }), onSuccess: () => { setNoteTitle(''); setNoteBody(''); void client.invalidateQueries({ queryKey: ['research-notes'] }) } })
  const createAlert = useMutation({ mutationFn: () => apiRequest('/api/research/alerts', { method: 'POST', body: JSON.stringify({ name: alertName, edinet_code: alertCode || undefined, metric: 'LatestPrice', operator: '>', value: Number(alertValue) }) }), onSuccess: () => { setAlertValue(''); void client.invalidateQueries({ queryKey: ['research-alerts'] }) } })
  const deleteNote = useMutation({ mutationFn: (id: string) => apiRequest(`/api/research/notes/${id}`, { method: 'DELETE' }), onSuccess: () => void client.invalidateQueries({ queryKey: ['research-notes'] }) })
  return <div className="stack dense-page"><PageHeader eyebrow="Research state" title="Watchlists and research" description="Your watchlists, notes, and in-app alerts are isolated from other accounts." /><div className="three-column research-grid"><Card title="Watchlists"><div className="button-row"><input className="input" value={watchlistName} onChange={event => setWatchlistName(event.target.value)} placeholder="Watchlist name" /><button className="button button--primary" disabled={!watchlistName.trim() || createWatchlist.isPending} onClick={() => createWatchlist.mutate()}>Create</button></div>{watchlists.isLoading && <LoadingState label="Loading watchlists" />}{watchlists.data?.watchlists.map(item => <div className="research-row" key={item.watchlist_id}><strong>{item.name}</strong><small>{item.item_count} companies</small></div>)}{watchlists.data && !watchlists.data.watchlists.length && <EmptyState title="No watchlists" description="Create one to keep a private company set." />}</Card><Card title="Notes"><label className="field-label">Title<input className="input" value={noteTitle} onChange={event => setNoteTitle(event.target.value)} /></label><label className="field-label">EDINET code (optional)<input className="input" value={noteCode} onChange={event => setNoteCode(event.target.value)} /></label><label className="field-label">Note<textarea className="input" rows={4} value={noteBody} onChange={event => setNoteBody(event.target.value)} /></label><button className="button button--primary" disabled={!noteTitle.trim() || !noteBody.trim() || createNote.isPending} onClick={() => createNote.mutate()}>Save note</button>{notes.data?.notes.map(note => <article className="research-note" key={note.note_id}><div><strong>{note.title}</strong><small>{note.edinet_code || 'General research'}</small></div><p>{note.body}</p><button className="text-button" onClick={() => deleteNote.mutate(note.note_id)}>Delete</button></article>)}</Card><Card title="Alerts"><label className="field-label">Name<input className="input" value={alertName} onChange={event => setAlertName(event.target.value)} /></label><label className="field-label">EDINET code<input className="input" value={alertCode} onChange={event => setAlertCode(event.target.value)} /></label><label className="field-label">Latest price above<input className="input" type="number" value={alertValue} onChange={event => setAlertValue(event.target.value)} /></label><button className="button button--primary" disabled={!alertCode.trim() || !alertValue || createAlert.isPending} onClick={() => createAlert.mutate()}>Create alert</button>{alerts.data?.alerts.map(alert => <div className="research-row" key={alert.alert_id}><strong>{alert.name}</strong><small>{alert.metric} {alert.operator} {alert.value}</small></div>)}</Card></div></div>
}
