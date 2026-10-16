import { act, cleanup, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'

import { useSelectedSection } from './useSelectedSection'

function Page() {
  const selected = useSelectedSection<'financials' | 'discussion'>(true)
  return <>
    <button type="button">Search</button>
    <section id="financials" className="analysis-section" data-testid="financials" data-selected={selected === 'financials' || undefined}>
      <h2 tabIndex={-1}>Financial statements</h2>
      <button type="button">Bars</button>
      <div className="statement-table-wrap"><table><tbody><tr tabIndex={-1} data-testid="line"><td>Net sales</td></tr></tbody></table></div>
    </section>
    <section id="discussion" className="analysis-section" data-testid="discussion" data-selected={selected === 'discussion' || undefined}>
      <div className="chat-list">A message</div>
    </section>
  </>
}

const selected = (id: string) => screen.getByTestId(id).hasAttribute('data-selected')
const focus = (element: HTMLElement) => act(() => element.focus())

afterEach(cleanup)

describe('selected section', () => {
  it('selects the section clicked in and releases it on a click elsewhere or Escape', () => {
    render(<Page />)
    expect(selected('financials')).toBe(false)
    fireEvent.click(screen.getByText('Net sales'))
    expect(selected('financials')).toBe(true)
    fireEvent.click(screen.getByText('A message'))
    expect(selected('financials')).toBe(false)
    expect(selected('discussion')).toBe(true)
    fireEvent.keyDown(document.body, { key: 'Escape' })
    expect(selected('discussion')).toBe(false)
    fireEvent.click(screen.getByRole('button', { name: 'Bars' }))
    expect(selected('financials')).toBe(true)
    fireEvent.click(screen.getByRole('button', { name: 'Search' }))
    expect(selected('financials')).toBe(false)
  })

  it('selects by keyboard focus inside a scroll area, but not when focus only reaches the heading', () => {
    render(<Page />)
    focus(screen.getByRole('heading', { name: 'Financial statements' }))
    expect(selected('financials')).toBe(false)
    focus(screen.getByTestId('line'))
    expect(selected('financials')).toBe(true)
    // Focus moving within the section keeps it; focus leaving it releases it.
    focus(screen.getByRole('button', { name: 'Bars' }))
    expect(selected('financials')).toBe(true)
    focus(screen.getByRole('button', { name: 'Search' }))
    expect(selected('financials')).toBe(false)
  })
})
