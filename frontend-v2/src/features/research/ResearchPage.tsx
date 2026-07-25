import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { apiRequest } from '../../api/client'
import { EmptyState, LoadingState } from '../../components/Feedback'
import { Card, PageHeader } from '../../components/Page'

interface Watchlist { watchlist_id: string; name: string; item_count: number }
interface Note { note_id: string; title: string; body: string; edinet_code?: string | null; version?: number }
interface Alert { alert_id: string; name: string; metric: string; operator: string; value: number }
interface CompanyResearch { edinet_code?: string; thesis_status?: string | null; target_value?: number | null; target_currency?: string | null; review_on?: string | null; version?: number }

export default function ResearchPage() {
  const client = useQueryClient()
  const [watchlistName, setWatchlistName] = useState('')
  const [noteTitle, setNoteTitle] = useState('')
  const [noteBody, setNoteBody] = useState('')
  const [noteCode, setNoteCode] = useState('')
  const [noteVersion, setNoteVersion] = useState<number | null>(null)
  const [editingNoteId, setEditingNoteId] = useState<string | null>(null)
  const [alertName, setAlertName] = useState('')
  const [alertCode, setAlertCode] = useState('')
  const [alertValue, setAlertValue] = useState('')
  const [alertOperator, setAlertOperator] = useState('>')
  const [alertMetric, setAlertMetric] = useState('LatestPrice')
  const [thesisCode, setThesisCode] = useState('')
  const [thesisStatus, setThesisStatus] = useState('')
  const [targetValue, setTargetValue] = useState('')
  const [targetCurrency, setTargetCurrency] = useState('JPY')
  const [reviewOn, setReviewOn] = useState('')

  const watchlists = useQuery({ queryKey: ['research-watchlists'], queryFn: () => apiRequest<{ watchlists: Watchlist[] }>('/api/research/watchlists') })
  const notes = useQuery({ queryKey: ['research-notes'], queryFn: () => apiRequest<{ notes: Note[] }>('/api/research/notes') })
  const alerts = useQuery({ queryKey: ['research-alerts'], queryFn: () => apiRequest<{ alerts: Alert[] }>('/api/research/alerts') })
  const thesisQuery = useQuery({
    queryKey: ['research-thesis', thesisCode],
    enabled: thesisCode.trim().length > 0,
    queryFn: () => apiRequest<CompanyResearch>(`/api/research/companies/${encodeURIComponent(thesisCode.trim())}`),
  })

  const createWatchlist = useMutation({ mutationFn: () => apiRequest('/api/research/watchlists', { method: 'POST', body: JSON.stringify({ name: watchlistName }) }), onSuccess: () => { setWatchlistName(''); void client.invalidateQueries({ queryKey: ['research-watchlists'] }) } })
  const deleteWatchlist = useMutation({ mutationFn: (id: string) => apiRequest(`/api/research/watchlists/${id}`, { method: 'DELETE' }), onSuccess: () => void client.invalidateQueries({ queryKey: ['research-watchlists'] }) })

  const saveNote = useMutation({
    mutationFn: () => {
      const body: Record<string, unknown> = { title: noteTitle, body: noteBody, edinet_code: noteCode || undefined }
      if (editingNoteId && noteVersion !== null) body.expected_version = noteVersion
      return editingNoteId
        ? apiRequest(`/api/research/notes/${editingNoteId}`, { method: 'PATCH', body: JSON.stringify(body) })
        : apiRequest('/api/research/notes', { method: 'POST', body: JSON.stringify(body) })
    },
    onSuccess: () => { setNoteTitle(''); setNoteBody(''); setNoteCode(''); setEditingNoteId(null); setNoteVersion(null); void client.invalidateQueries({ queryKey: ['research-notes'] }) },
    onError: (err: Error) => { if (err.message.includes('version')) setNoteVersion(Number(err.message.match(/\d+/)?.[0])); },
  })
  const deleteNote = useMutation({ mutationFn: (id: string) => apiRequest(`/api/research/notes/${id}`, { method: 'DELETE' }), onSuccess: () => void client.invalidateQueries({ queryKey: ['research-notes'] }) })

  const createAlert = useMutation({
    mutationFn: () => apiRequest('/api/research/alerts', { method: 'POST', body: JSON.stringify({ name: alertName, edinet_code: alertCode || undefined, metric: alertMetric, operator: alertOperator, value: Number(alertValue) }) }),
    onSuccess: () => { setAlertName(''); setAlertValue(''); void client.invalidateQueries({ queryKey: ['research-alerts'] }) },
  })
  const deleteAlert = useMutation({ mutationFn: (id: string) => apiRequest(`/api/research/alerts/${id}`, { method: 'DELETE' }), onSuccess: () => void client.invalidateQueries({ queryKey: ['research-alerts'] }) })

  const saveThesis = useMutation({
    mutationFn: () => apiRequest(`/api/research/companies/${thesisCode.trim()}`, { method: 'PATCH', body: JSON.stringify({ thesis_status: thesisStatus || undefined, target_value: targetValue ? Number(targetValue) : undefined, target_currency: targetCurrency || undefined, review_on: reviewOn || undefined }) }),
    onSuccess: () => { void client.invalidateQueries({ queryKey: ['research-thesis', thesisCode] }); void thesisQuery.refetch() },
  })

  const startEditNote = (note: Note) => { setEditingNoteId(note.note_id); setNoteTitle(note.title); setNoteBody(note.body); setNoteCode(note.edinet_code ?? ''); setNoteVersion(note.version ?? null) }
  const cancelEdit = () => { setEditingNoteId(null); setNoteTitle(''); setNoteBody(''); setNoteCode(''); setNoteVersion(null) }

  return (
    <div className="stack dense-page">
      <PageHeader eyebrow="Research state" title="Watchlists and research" description="Your watchlists, notes, thesis tracking, and alerts are isolated from other accounts." />
      <div className="three-column research-grid">
        <Card title="Watchlists">
          <div className="button-row">
            <input className="input" value={watchlistName} onChange={e => setWatchlistName(e.target.value)} placeholder="Watchlist name" />
            <button className="button button--primary" disabled={!watchlistName.trim() || createWatchlist.isPending} onClick={() => createWatchlist.mutate()}>Create</button>
          </div>
          {watchlists.isLoading && <LoadingState label="Loading" />}
          {watchlists.data?.watchlists.map(item => (
            <div className="research-row" key={item.watchlist_id}>
              <strong>{item.name}</strong>
              <small>{item.item_count} companies</small>
              <button className="text-button text-button--danger" onClick={() => deleteWatchlist.mutate(item.watchlist_id)}>×</button>
            </div>
          ))}
          {watchlists.data && !watchlists.data.watchlists.length && <EmptyState title="No watchlists" description="Create one to keep a private company set." />}
        </Card>

        <Card title="Notes">
          <label className="field-label">Title<input className="input" value={noteTitle} onChange={e => setNoteTitle(e.target.value)} /></label>
          <label className="field-label">EDINET code<input className="input" value={noteCode} onChange={e => setNoteCode(e.target.value)} placeholder="E00000" /></label>
          <label className="field-label">Body<textarea className="input" rows={3} value={noteBody} onChange={e => setNoteBody(e.target.value)} /></label>
          <div className="button-row">
            <button className="button button--primary" disabled={!noteTitle.trim() || !noteBody.trim() || saveNote.isPending} onClick={() => saveNote.mutate()}>
              {editingNoteId ? 'Update note' : 'Save note'}
            </button>
            {editingNoteId && <button className="text-button" onClick={cancelEdit}>Cancel</button>}
          </div>
          {saveNote.error && <p className="form-error">{(saveNote.error as Error).message}</p>}
          {notes.isLoading && <LoadingState label="Loading" />}
          {notes.data?.notes.slice(0, 15).map(note => (
            <article className="research-note" key={note.note_id}>
              <div>
                <strong>{note.title}</strong>
                <small>{note.edinet_code || 'General'} · v{note.version ?? 1}</small>
              </div>
              <p>{note.body.slice(0, 300)}{note.body.length > 300 ? '…' : ''}</p>
              <div className="button-row">
                <button className="text-button" onClick={() => startEditNote(note)}>Edit</button>
                <button className="text-button text-button--danger" onClick={() => deleteNote.mutate(note.note_id)}>Delete</button>
              </div>
            </article>
          ))}
        </Card>

        <Card title="Thesis & targets">
          <label className="field-label">EDINET code<input className="input" value={thesisCode} onChange={e => setThesisCode(e.target.value)} placeholder="E00000" /></label>
          <button className="button button--secondary" disabled={!thesisCode.trim()} onClick={() => thesisQuery.refetch()}>Look up</button>
          {thesisQuery.data && (
            <div className="stack">
              <label className="field-label">
                Thesis status
                <select className="select" value={thesisQuery.data.thesis_status ?? thesisStatus} onChange={e => setThesisStatus(e.target.value)}>
                  <option value="">None</option>
                  <option value="watch">Watch</option>
                  <option value="buy">Buy</option>
                  <option value="hold">Hold</option>
                  <option value="sell">Sell</option>
                </select>
              </label>
              <label className="field-label">Target value<input className="input" type="number" value={(targetValue || thesisQuery.data.target_value) ?? ''} onChange={e => setTargetValue(e.target.value)} /></label>
              <label className="field-label">
                Currency
                <select className="select" value={(targetCurrency || thesisQuery.data.target_currency) ?? 'JPY'} onChange={e => setTargetCurrency(e.target.value)}>
                  <option>JPY</option><option>USD</option><option>EUR</option>
                </select>
              </label>
              <label className="field-label">Review on<input className="input" type="date" value={(reviewOn || thesisQuery.data.review_on) ?? ''} onChange={e => setReviewOn(e.target.value)} /></label>
              <button className="button button--primary" disabled={saveThesis.isPending} onClick={() => saveThesis.mutate()}>Save thesis</button>
            </div>
          )}
        </Card>

        <Card title="Alerts">
          <label className="field-label">Name<input className="input" value={alertName} onChange={e => setAlertName(e.target.value)} placeholder="Price alert" /></label>
          <label className="field-label">EDINET code<input className="input" value={alertCode} onChange={e => setAlertCode(e.target.value)} placeholder="E00000" /></label>
          <div className="button-row">
            <select className="select compact" value={alertMetric} onChange={e => setAlertMetric(e.target.value)}>
              <option>LatestPrice</option><option>PERatio</option><option>DividendsYield</option><option>ReturnOnEquity</option>
            </select>
            <select className="select compact" value={alertOperator} onChange={e => setAlertOperator(e.target.value)}>
              <option>{'>'}</option><option>{'>='}</option><option>{'<'}</option><option>{'<='}</option><option>=</option>
            </select>
          </div>
          <label className="field-label">Value<input className="input" type="number" value={alertValue} onChange={e => setAlertValue(e.target.value)} /></label>
          <button className="button button--primary" disabled={!alertCode.trim() || !alertValue || createAlert.isPending} onClick={() => createAlert.mutate()}>Create alert</button>
          {alerts.isLoading && <LoadingState label="Loading" />}
          {alerts.data?.alerts.map(alert => (
            <div className="research-row" key={alert.alert_id}>
              <div><strong>{alert.name}</strong><small>{alert.metric} {alert.operator} {alert.value}</small></div>
              <button className="text-button text-button--danger" onClick={() => deleteAlert.mutate(alert.alert_id)}>×</button>
            </div>
          ))}
        </Card>
      </div>
    </div>
  )
}
