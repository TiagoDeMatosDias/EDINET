import { Pencil, Plus, Tag, Trash2, X } from 'lucide-react'
import { useState, type ReactNode } from 'react'

import { apiRequest, queryString } from '../../api/client'
import { ASSET_CATEGORIES, type Draft } from './manualModel'
import { formatDay } from './portfolioFormat'
import { SectionCard } from './PortfolioPrimitives'
import type { ManualAsset, ManualPrice } from './portfolioTypes'

type Rebuilt = { daily_rows: number; holdings_count: number }

const CATEGORY = new Map(ASSET_CATEGORIES.map(item => [item.value, item.label]))
const priceText = (price: ManualPrice) => `${price.price.toLocaleString(undefined, { maximumFractionDigits: 6 })} ${price.currency}`

/**
 * The assets this account defined and the prices it entered. Both value its
 * own portfolio only; the shared market prices never change.
 */
export function ManualAssets({ assets, form, onEdit, onChanged }: {
  assets: ManualAsset[]
  /** The entry form, when it was opened from this card. */
  form?: ReactNode
  onEdit: (preset: Partial<Draft>) => void
  onChanged: (message: string) => void
}) {
  const [confirming, setConfirming] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const run = async (work: () => Promise<Rebuilt>, done: string) => {
    setBusy(true)
    setError('')
    try {
      const result = await work()
      setConfirming(null)
      onChanged(`${done} Rebuilt ${result.holdings_count} holdings over ${result.daily_rows.toLocaleString()} days.`)
    } catch (problem) {
      setError(problem instanceof Error ? problem.message : 'The change failed')
    } finally {
      setBusy(false)
    }
  }
  const deleteAsset = (asset: ManualAsset) => run(
    () => apiRequest<Rebuilt>(`/api/portfolio/assets/${encodeURIComponent(asset.symbol)}`, { method: 'DELETE' }),
    `Deleted ${asset.defined ? 'the asset' : 'the prices of'} ${asset.symbol}${asset.defined && asset.prices.length ? ` and its ${asset.prices.length} price${asset.prices.length === 1 ? '' : 's'}` : ''}; its transactions stay.`,
  )
  const deletePrice = (asset: ManualAsset, price: ManualPrice) => run(
    () => apiRequest<Rebuilt>(`/api/portfolio/prices${queryString({ symbol: asset.symbol, date: price.date })}`, { method: 'DELETE' }),
    `Deleted the price of ${asset.symbol} for ${formatDay(price.date)}.`,
  )

  return <SectionCard
    title="Assets & prices"
    description="Securities you defined and prices you entered. They value your portfolio only; the shared market prices do not change."
    actions={<>
      <button type="button" className="button button--ghost button--small" onClick={() => onEdit({ kind: 'asset' })}><Tag aria-hidden="true" />Define an asset</button>
      <button type="button" className="button button--ghost button--small" onClick={() => onEdit({ kind: 'price' })}><Plus aria-hidden="true" />Enter a price</button>
    </>}
  >
    {form}
    {!assets.length ? <p className="muted pf-assets__empty">None yet. Define a security the market data does not have (a foreign stock, an ETF, a private fund), or enter a price for one you hold.</p> : <ul className="pf-assets">{assets.map(asset => {
      const priced = asset.latest_price
      const details = asset.defined
        ? [CATEGORY.get(asset.asset_category ?? '') ?? asset.asset_category, asset.currency, asset.fetch_prices ? 'market prices too' : 'priced by hand only', asset.isin].filter(Boolean).join(' · ')
        : 'Prices entered for a security in the market data'
      return <li key={asset.symbol}>
        <div className="pf-assets__row">
          <span className="pf-assets__name"><strong>{asset.symbol}{asset.name ? <> · {asset.name}</> : null}</strong><small>{details}{asset.held ? ' · held' : ''}</small></span>
          <span className="pf-assets__price">{priced ? <><strong className="mono">{priceText(priced)}</strong><small>{formatDay(priced.date)}</small></> : <small>No price entered</small>}</span>
          <span className="pf-assets__actions">
            {confirming === asset.symbol ? <>
              <span className="muted">Delete {asset.defined ? 'it' : 'its prices'}{asset.prices.length ? ` and ${asset.prices.length} price${asset.prices.length === 1 ? '' : 's'}` : ''}?</span>
              <button type="button" className="button button--danger button--small" disabled={busy} onClick={() => void deleteAsset(asset)}>Delete</button>
              <button type="button" className="button button--ghost button--small" onClick={() => setConfirming(null)}>Cancel</button>
            </> : <>
              <button type="button" className="button button--ghost button--small" onClick={() => onEdit({ kind: 'price', symbol: asset.symbol, currency: asset.currency ?? priced?.currency ?? undefined })} aria-label={`Enter a price for ${asset.symbol}`}><Plus aria-hidden="true" />Price</button>
              <button type="button" className="button button--ghost button--small" onClick={() => onEdit({ kind: 'asset', symbol: asset.symbol, name: asset.name ?? '', category: asset.asset_category ?? 'STK', currency: asset.currency ?? priced?.currency ?? undefined, isin: asset.isin ?? '', description: asset.notes ?? '', fetchPrices: Boolean(asset.fetch_prices) })} aria-label={`${asset.defined ? 'Edit' : 'Define'} ${asset.symbol}`}><Pencil aria-hidden="true" />{asset.defined ? 'Edit' : 'Define'}</button>
              <button type="button" className="button button--ghost button--small" onClick={() => setConfirming(asset.symbol)} aria-label={`Delete ${asset.symbol}`}><Trash2 aria-hidden="true" />Delete…</button>
            </>}
          </span>
        </div>
        {asset.prices.length > 0 && <details className="pf-assets__prices">
          <summary>{asset.prices.length} price{asset.prices.length === 1 ? '' : 's'} entered</summary>
          <ul>{asset.prices.map(price => <li key={price.date}>
            <span className="mono">{formatDay(price.date)}</span>
            <strong className="mono">{priceText(price)}</strong>
            <small>{price.note ?? ''}</small>
            <button type="button" className="icon-button" disabled={busy} aria-label={`Delete the price of ${asset.symbol} for ${price.date}`} title="Delete this price" onClick={() => void deletePrice(asset, price)}><X /></button>
          </li>)}</ul>
        </details>}
      </li>
    })}</ul>}
    {error && <p className="form-error" role="alert">{error}</p>}
  </SectionCard>
}
