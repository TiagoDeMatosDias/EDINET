import { FileText, Languages } from 'lucide-react'
import { useState } from 'react'
import { Link } from 'react-router-dom'

import { filingHref } from '../filings/filingFormat'
import { fiscalYear, formatMonth } from './ownershipFormat'
import { useCareerTranslation } from './ownershipQueries'
import type { EntityCareer } from './ownershipTypes'

/**
 * A person's career summary as their company's annual report gives it. The
 * report is in Japanese; the English is requested once the career is shown
 * and replaces the filed text when it arrives.
 */
export function CareerList({ careers }: { careers: EntityCareer[] }) {
  const [index, setIndex] = useState(0)
  const [asFiled, setAsFiled] = useState(false)
  const career = careers[Math.min(index, careers.length - 1)]
  const translation = useCareerTranslation(career)
  const english = translation.data ?? {}
  const translated = career.entries.some(entry => english[entry.text])
  const showEnglish = translated && !asFiled
  return <section className="ow-section" aria-label="Career">
    <h3>Career <small>from {career.company_name}’s report for {fiscalYear(career.period_end)}</small></h3>
    <div className="ow-career__bar">
      {careers.length > 1 && <label className="rs-inline-field">Told by
        <select className="select" value={index} onChange={event => setIndex(Number(event.target.value))}>
          {careers.map((item, position) => <option key={item.doc_id} value={position}>{item.company_name} · {fiscalYear(item.period_end)}</option>)}
        </select>
      </label>}
      {translated && <button type="button" className="text-button" aria-pressed={asFiled} onClick={() => setAsFiled(!asFiled)}><Languages aria-hidden="true" />{asFiled ? 'Show in English' : 'Show as filed'}</button>}
      {translation.isLoading && <span className="rs-dim" role="status">Putting the career into English…</span>}
      {translation.isError && <span className="rs-dim">Shown as filed: the translation is unavailable.</span>}
      <Link to={filingHref(career.doc_id, career.edinet_code, 'research')} title="The annual report in the Filing Explorer"><FileText aria-hidden="true" />Report</Link>
    </div>
    <ol className="ow-career">
      {career.entries.map((entry, position) => <li key={position}>
        <time>{formatMonth(entry.date) || '·'}</time>
        <span lang={showEnglish && english[entry.text] ? undefined : 'ja'} title={showEnglish && english[entry.text] ? `Filed as ${entry.text}` : undefined}>{showEnglish ? english[entry.text] ?? entry.text : entry.text}</span>
      </li>)}
    </ol>
    {showEnglish && <p className="rs-hint">Joining a company and titles held at one are rewritten from the filed Japanese by rule; other lines are machine-translated and may render names loosely. Hover a line for it as filed.</p>}
  </section>
}
