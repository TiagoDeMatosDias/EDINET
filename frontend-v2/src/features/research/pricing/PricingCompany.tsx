import { X } from 'lucide-react'
import type { Ref } from 'react'
import { Link } from 'react-router-dom'

import { CompanyPicker } from '../../../components/CompanyPicker'
import { formatMetricValue } from '../../../metrics'
import { analysisHref, formatDay } from '../researchModel'
import { PRICE_FORMAT } from '../researchQueries'
import type { PricingInputs } from '../researchTypes'

/** The company a calculator draws its inputs from, or manual inputs when none is chosen. */
export function PricingCompany({ code, inputs, loading, error, inputRef, onChange, children }: {
  code: string
  inputs?: PricingInputs
  loading: boolean
  error: unknown
  inputRef: Ref<HTMLInputElement>
  onChange: (code: string) => void
  children?: React.ReactNode
}) {
  const company = inputs?.company
  return <div className="rs-underlying">
    <div className="rs-underlying__picker">
      <CompanyPicker selected={null} clearOnSelect inputRef={inputRef} label="Company" placeholder={code ? 'Change company…' : 'Price for a company, or enter inputs by hand…'} onSelect={item => { if (item?.company_code) onChange(item.company_code) }} />
      <kbd aria-hidden="true">A</kbd>
    </div>
    {code ? <div className="rs-underlying__company">
      {loading && <span className="rs-dim">Loading {code}…</span>}
      {error != null && <span className="form-error">{(error as Error).message}</span>}
      {company && <>
        <strong><Link to={analysisHref(company.company_code)}>{company.company_name}</Link></strong>
        <span className="rs-dim">{[company.ticker !== company.company_name && company.ticker, company.industry].filter(Boolean).join(' · ')}</span>
        {inputs?.spot != null && <span className="mono">{formatMetricValue(PRICE_FORMAT, inputs.spot, { price: inputs.currency.price })}{inputs.price_date && <small className="rs-dim"> · {formatDay(inputs.price_date)}</small>}</span>}
        {inputs?.spot == null && <span className="rs-dim">No stored price</span>}
      </>}
      <button type="button" className="icon-button" aria-label="Use manual inputs instead of a company" title="Manual inputs (M)" onClick={() => onChange('')}><X /></button>
    </div> : <span className="rs-dim rs-underlying__manual">Manual inputs <kbd aria-hidden="true">M</kbd></span>}
    {children}
  </div>
}
