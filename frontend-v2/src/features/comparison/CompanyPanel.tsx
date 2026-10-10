import { ArrowDown, ArrowUp, Plus, Tag, X } from 'lucide-react'
import type { ReactNode, Ref } from 'react'
import { Link } from 'react-router-dom'

import { CompanyPicker } from '../../components/CompanyPicker'
import { describeTag, formatPeriod, MAX_COMPANIES, seriesStyle, shortName } from './comparisonModel'
import type { CompanyInfo, TagSet } from './comparisonTypes'
import { HotkeyKbd } from '../../hotkeys/HotkeyKbd'
import { comparisonScope } from './comparisonHotkeys'

export function Swatch({ index }: { index: number }) {
  const style = seriesStyle(index)
  return <span className={style.dashed ? 'cmp-swatch cmp-swatch--dashed' : 'cmp-swatch'} style={{ color: style.color }} aria-hidden="true" />
}

// More of a tag's companies than this are counted rather than listed.
const TAG_REST_SHOWN = 12

/**
 * The selected companies, one dense row each, in comparison order. Each row's
 * colour is the company's colour in every chart. The finder also matches the
 * user's tags: choosing one adds its companies, and those that did not fit (or
 * were removed since) stay one click away under the finder.
 */
export function CompanyPanel({ codes, info, periods, cursorCode, inputRef, actions, tags, tagRest, onAdd, onAddTag, onDismissTag, onRemove, onMove, onCursor }: {
  codes: string[]
  info: Record<string, CompanyInfo | undefined>
  periods: Record<string, string | null | undefined>
  cursorCode: string | null
  inputRef: Ref<HTMLInputElement>
  actions?: ReactNode
  tags: TagSet[]
  /** The tag last added from, with its companies that are not in the comparison. */
  tagRest: { tag: string; companies: CompanyInfo[] } | null
  onAdd: (companies: CompanyInfo[]) => void
  onAddTag: (name: string) => void
  onDismissTag: () => void
  onRemove: (code: string) => void
  onMove: (code: string, offset: number) => void
  onCursor: (code: string) => void
}) {
  const full = codes.length >= MAX_COMPANIES
  const pickerTags = tags.map(tag => {
    const { summary, names, disabled } = describeTag(tag, codes)
    return { name: tag.name, detail: [summary, names].filter(Boolean).join(' · '), disabled }
  })
  return <section className="panel cmp-panel cmp-companies" aria-labelledby="cmp-companies-title">
    <header className="cmp-panel__header">
      <h2 id="cmp-companies-title">Companies <span className="cmp-count">{codes.length}/{MAX_COMPANIES}</span></h2>
      {actions}
    </header>
    <div className="cmp-companies__add">
      <CompanyPicker
        selected={null}
        onSelect={company => company?.company_code && onAdd([{ company_code: company.company_code, company_name: company.company_name, ticker: company.ticker, industry: company.industry }])}
        clearOnSelect
        requireCompanyCode
        disabled={full}
        label="Add a company or a tag"
        placeholder={full ? 'Twelve companies is the most a comparison holds' : 'Add a company or a tag: name, ticker, code…'}
        inputRef={inputRef}
        tags={pickerTags}
        onSelectTag={onAddTag}
      />
      <span aria-hidden="true"><HotkeyKbd hotkey={comparisonScope.byId['add-company']} /></span>
    </div>
    {tagRest && tagRest.companies.length > 0 && <div className="cmp-companies__tagged" role="group" aria-label={`Other companies tagged ${tagRest.tag}`}>
      <span className="cmp-companies__tagged-label"><Tag aria-hidden="true" />Also tagged “{tagRest.tag}”</span>
      {tagRest.companies.slice(0, TAG_REST_SHOWN).map(company => {
        const name = company.company_name || company.ticker || company.company_code
        return <button key={company.company_code} type="button" className="cmp-pill" disabled={full} onClick={() => onAdd([company])} title={full ? `Remove a company to make room for ${name}` : `Add ${name} to the comparison`}><Plus aria-hidden="true" />{shortName(name)}</button>
      })}
      {tagRest.companies.length > TAG_REST_SHOWN && <span className="cmp-panel__meta">and {tagRest.companies.length - TAG_REST_SHOWN} more</span>}
      <button type="button" className="icon-button" onClick={onDismissTag} aria-label={`Hide the other companies tagged ${tagRest.tag}`} title="Hide"><X /></button>
    </div>}
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
    </ol> : <p className="cmp-panel__empty">Search for a company or one of your tags, then add peers from the suggestions. Screening and Analysis can also send companies here.</p>}
  </section>
}
