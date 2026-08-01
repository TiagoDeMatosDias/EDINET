import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import { PortfolioDrawer } from './PortfolioDrawer'

describe('PortfolioDrawer', () => {
  it('is a labelled modal, moves focus to close, and closes with Escape', () => {
    const onClose = vi.fn()
    render(<PortfolioDrawer open eyebrow="Position detail" title="VWCE" description="Global equity ETF" onClose={onClose}>
      <p>Position statistics</p>
    </PortfolioDrawer>)

    expect(screen.getByRole('dialog', { name: 'VWCE' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Close details' })).toHaveFocus()

    fireEvent.keyDown(window, { key: 'Escape' })

    expect(onClose).toHaveBeenCalledOnce()
  })
})
