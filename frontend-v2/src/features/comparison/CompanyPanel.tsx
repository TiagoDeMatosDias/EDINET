import { ArrowDown, ArrowUp, X } from 'lucide-react'
import type { ReactNode, Ref } from 'react'
import { Link } from 'react-router-dom'

import type { SecuritySearchResult } from '../../api/types'
import { CompanyPicker } from '../../components/CompanyPicker'
import { formatPeriod, MAX_COMPANIES, seriesStyle } from './comparisonModel'
import type { CompanyInfo } from './comparisonTypes'

export function Swatch({ index }: { index: number }) {
  const style = seriesStyle(index)
  return <span className={style.dashed ? 'cmp-swatch cmp-swatch--dashed' : 'cmp-swatch'} style={{ color: style.color }} aria-hidden="true" />
}

/**
 * The selected companies, one dense row each, in comparison order. Each row's
 * colour is the company's colour in every chart.
 */
export function CompanyPanel({ codes, info, periods, cursorCode, inputRef, actions, onAdd, onRemove, onMove, onCursor }: {
  codes: string[]
  info: Record<string, CompanyInfo | undefined>
  periods: Record<string, string | null | undefined>
  cursorCode: string | null
  inputRef: Ref<HTMLInputElement>
  actions?: ReactNode
  onAdd: (company: SecuritySearchResult) => void
  onRemove: (code: string) => void
  onMove: (code: string, offset: number) => void
  onCursor: (code: string) => void
}) {
  const full = codes.length >= MAX_COMPANIES
  return <section className="panel cmp-panel cmp-companies" aria-labelledby="cmp-companies-title">
    <header className="cmp-panel__header">
      <h2 id="cmp-companies-title">Companies <span className="cmp-count">{codes.length}/{MAX_COMPANIES}</span></h2>
      {actions}
    </header>
    <div className="cmp-companies__add">
      <CompanyPicker selected={null} onSelect={company => company && onAdd(company)} clearOnSelect requireCompanyCode disabled={full} label="Add a company" placeholder={full ? 'Twelve companies is the most a comparison holds' : 'Add a company: name, ticker, code…'} inputRef={inputRef} />
      <kbd aria-hidden="true">A</kbd>
    </div>
    {codes.length > 0 ? <ol className="cmp-companies__list">
      {codes.map((code, index) => {
        const company = info[code]
        const name = company?.company_name || company?.ticker || code
        return <li key={code} className={code === cursorCode ? 'is-cursor' : undefined} onClick={() => onCursor(code)}>
          <Swatch index={index} />
          <Link className="cmp-companies__name" to={`/analyze/${encodeURIComponent(code)}`} title={`Open ${name} in Analysis`}>{name}</Link>
          <span className="cmp-companies__meta mono">{company?.ticker}</span>
          <span className="cmp-companies__meta cmp-companies__industry" title={company?.industry}>{company?.industry}</span>
          <span className="cmp-companies__meta">{periods[code] ? `FY ${formatPeriod(periods[code])}` : ''}</span>
          <span className="cmp-companies__actions">
            <button type="button" className="icon-button" disabled={index === 0} onClick={() => onMove(code, -1)} aria-label={`Move ${name} earlier`} title="Move earlier ([)"><ArrowUp /></button>
            <button type="button" className="icon-button" disabled={index === codes.length - 1} onClick={() => onMove(code, 1)} aria-label={`Move ${name} later`} title="Move later (])"><ArrowDown /></button>
            <button type="button" className="icon-button" onClick={() => onRemove(code)} aria-label={`Remove ${name}`} title="Remove (Shift+X)"><X /></button>
          </span>
        </li>
      })}
    </ol> : <p className="cmp-panel__empty">Search for a company, then add its peers from the suggestions. Screening and Analysis can also send companies here.</p>}
  </section>
}
