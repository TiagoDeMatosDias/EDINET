import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useMemo, useRef, useState, type KeyboardEvent } from 'react'
import { Link } from 'react-router-dom'

import { apiPost, apiRequest } from '../../api/client'
import type { SecuritySearchResult } from '../../api/types'
import { CompanyPicker } from '../../components/CompanyPicker'
import { useHotkeys } from '../../hooks/useHotkeys'
import { formatMetricValue } from '../../metrics'
import { ConfirmButton } from './ConfirmButton'
import { invalidateResearch } from './researchQueries'
import { ALERT_OPERATORS, alertCondition, alertDistance, formatSignedPercent, parseThreshold, relativeDay } from './researchModel'
import type { ResearchBook } from './researchTypes'
import { moveCursorKey, useListCursor } from './useListCursor'

export function AlertsView({ book, active, today, onOpenCompany }: {
  book?: ResearchBook
  active: boolean
  today: string
  onOpenCompany: (code: string) => void
}) {
  const client = useQueryClient()
  const definitions = useMemo(() => book?.metric_definitions ?? {}, [book])
  const alerts = useMemo(() => book?.alerts ?? [], [book])
  const [onlyTriggered, setOnlyTriggered] = useState(false)
  const [cursor, setCursor] = useState(0)
  const [focusRequest, setFocusRequest] = useState(0)
  const [armed, setArmed] = useState<string | null>(null)
  const [company, setCompany] = useState<SecuritySearchResult | null>(null)
  const [metric, setMetric] = useState('LatestPrice')
  const [operator, setOperator] = useState('<')
  const [value, setValue] = useState('')
  const pickerRef = useRef<HTMLInputElement>(null)
  const valueRef = useRef<HTMLInputElement>(null)

  // Triggered first, then the ones closest to their threshold.
  const shown = useMemo(() => [...alerts]
    .filter(alert => !onlyTriggered || alert.triggered)
    .sort((a, b) => Number(b.triggered) - Number(a.triggered) || Math.abs(alertDistance(a) ?? Infinity) - Math.abs(alertDistance(b) ?? Infinity)), [alerts, onlyTriggered])
  const index = Math.min(cursor, Math.max(shown.length - 1, 0))
  const body = useListCursor<HTMLTableSectionElement>(index, focusRequest)
  const remove = useMutation({
    mutationFn: (id: string) => apiRequest(`/api/research/alerts/${encodeURIComponent(id)}`, { method: 'DELETE' }),
    onSuccess: () => invalidateResearch(client),
  })
  const percent = definitions[metric]?.format === 'percent'
  const typed = parseThreshold(value)
  const threshold = typed.number / (percent ? 100 : 1)
  const condition = typed.operator ?? operator
  const valid = Boolean(company?.company_code) && typed.text !== '' && Number.isFinite(threshold)
  const create = useMutation({
    mutationFn: () => apiPost('/api/research/alerts', { name: alertCondition({ metric, operator: condition, value: threshold }, definitions), edinet_code: company?.company_code, metric, operator: condition, value: threshold }),
    onSuccess: () => {
      setValue('')
      setOperator(condition)
      setCompany(null)
      invalidateResearch(client)
      // Ready for the next one, as after pressing N.
      pickerRef.current?.focus()
    },
  })
  const step = (delta: number) => { setCursor(Math.max(0, Math.min(shown.length - 1, index + delta))); setFocusRequest(request => request + 1) }
  useHotkeys({
    j: () => step(1),
    k: () => step(-1),
    n: () => pickerRef.current?.focus(),
    t: () => setOnlyTriggered(!onlyTriggered),
    x: () => {
      const current = shown[index]
      if (!current) return
      if (armed === current.alert_id) { setArmed(null); remove.mutate(current.alert_id) } else setArmed(current.alert_id)
    },
  }, active)
  const onBodyKeyDown = (event: KeyboardEvent<HTMLTableSectionElement>) => {
    if (moveCursorKey(event, index, shown.length, setCursor)) return
    const current = shown[index]
    if (event.key === 'Enter' && current?.edinet_code && event.target === event.currentTarget.querySelector('[data-cursor="true"]')) { event.preventDefault(); onOpenCompany(current.edinet_code) }
  }
  const triggered = alerts.filter(alert => alert.triggered).length

  return <div className="rs-alerts-view">
    <section className="panel" aria-label="New alert">
      <form className="rs-alert-form" onSubmit={event => { event.preventDefault(); if (valid) create.mutate() }}>
        <div className="rs-alert-form__company"><CompanyPicker selected={company} onSelect={next => { setCompany(next); if (next) valueRef.current?.focus() }} inputRef={pickerRef} label="Company" /></div>
        <label className="field-label">Metric<select className="select" value={metric} onChange={event => setMetric(event.target.value)}>{Object.entries(definitions).map(([key, definition]) => <option key={key} value={key}>{definition.label}</option>)}</select></label>
        <label className="field-label">Condition<select className="select" value={condition} onChange={event => { setOperator(event.target.value); setValue(typed.text) }}>{ALERT_OPERATORS.map(item => <option key={item}>{item}</option>)}</select></label>
        <label className="field-label">{percent ? 'Value (%)' : 'Value'}<input ref={valueRef} className="input" inputMode="decimal" aria-label="Value" placeholder={`e.g. ${condition} 1500`} value={value} onChange={event => setValue(event.target.value)} onKeyDown={event => { if (event.key === 'Escape') event.currentTarget.blur() }} /></label>
        <button type="submit" className="button button--primary button--small" disabled={!valid || create.isPending}>{create.isPending ? 'Adding…' : 'Add alert'} <kbd aria-hidden="true">Enter</kbd></button>
        {create.error && <span className="form-error">{(create.error as Error).message}</span>}
        {remove.error && <span className="form-error">Could not delete the alert: {(remove.error as Error).message}</span>}
      </form>
      <p className="rs-hint"><kbd>N</kbd> starts a new alert: pick the company with <kbd>Enter</kbd>, type the value (a leading <code>&lt;</code>, <code>&gt;=</code>… sets the condition), and press <kbd>Enter</kbd> to add it. Alerts are checked against the latest stored prices and filings each time this page loads; triggered ones are listed first.</p>
    </section>
    <section className="panel" aria-label="Alerts">
      <div className="rs-toolbar">
        <span className="rs-count">{alerts.length} alerts · {triggered} triggered</span>
        <label className="inline-toggle"><input type="checkbox" checked={onlyTriggered} onChange={event => setOnlyTriggered(event.target.checked)} />Triggered only <kbd aria-hidden="true">T</kbd></label>
      </div>
      {shown.length ? <div className="rs-table-scroll"><table className="rs-table" aria-label="Alerts">
        <thead><tr><th scope="col">Status</th><th scope="col">Company</th><th scope="col">Condition</th><th scope="col" className="num">Now</th><th scope="col" className="num" title="How far the current value is from the threshold">Gap</th><th scope="col">Added</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead>
        <tbody ref={body} onKeyDown={onBodyKeyDown}>
          {shown.map((alert, position) => {
            const isCursor = position === index
            const distance = alertDistance(alert)
            return <tr key={alert.alert_id} data-cursor={isCursor} className={isCursor ? 'is-cursor' : undefined} tabIndex={isCursor ? 0 : -1} aria-selected={isCursor} onClick={() => setCursor(position)}>
              <td>{alert.triggered ? <span className="rs-pill rs-pill--triggered">Triggered</span> : alert.current_value == null ? <span className="rs-dim" title="No current value for this metric">No data</span> : <span className="rs-dim">Waiting</span>}</td>
              <th scope="row">{alert.edinet_code ? <Link to={`/research?company=${encodeURIComponent(alert.edinet_code)}`}>{alert.company_name ?? alert.edinet_code}</Link> : 'Any company'}</th>
              <td>{alertCondition(alert, definitions)}</td>
              <td className="num">{formatMetricValue(definitions[alert.metric], alert.current_value, { price: alert.price_currency })}</td>
              <td className="num">{formatSignedPercent(distance)}</td>
              <td className="rs-dim">{relativeDay(alert.created_at, today)}</td>
              <td><ConfirmButton label={`Delete alert ${alert.name}`} confirmLabel="Delete?" armed={armed === alert.alert_id} onArmedChange={next => setArmed(next ? alert.alert_id : null)} onConfirm={() => remove.mutate(alert.alert_id)}>Delete</ConfirmButton></td>
            </tr>
          })}
        </tbody>
      </table></div> : <p className="rs-empty">{alerts.length ? 'No alert is triggered right now.' : 'No alerts yet. Add one above, or from a company’s research panel.'}</p>}
    </section>
  </div>
}
