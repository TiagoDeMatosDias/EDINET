import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { readRecentDates } from './asOfDates'
import { AsOfMenu } from './AsOfMenu'

afterEach(() => {
  cleanup()
  localStorage.clear()
})

describe('as-of date menu', () => {
  it('applies a typed date with Enter and remembers it', () => {
    const choose = vi.fn()
    render(<AsOfMenu value="" onChoose={choose} onClose={() => undefined} />)
    const input = screen.getByRole('combobox', { name: 'As-of date' })

    fireEvent.change(input, { target: { value: '2023' } })
    expect(screen.getAllByRole('option')[0]).toHaveTextContent('Use 31 Dec 2023')
    fireEvent.keyDown(input, { key: 'Enter' })

    expect(choose).toHaveBeenCalledWith('2023-12-31')
    expect(readRecentDates()).toEqual(['2023-12-31'])
  })

  it('moves through the presets with the arrow keys', () => {
    const choose = vi.fn()
    render(<AsOfMenu value="2020-01-01" onChoose={choose} onClose={() => undefined} />)
    const input = screen.getByRole('combobox', { name: 'As-of date' })

    expect(screen.getAllByRole('option')[0]).toHaveTextContent('Latest data')
    fireEvent.keyDown(input, { key: 'ArrowDown' })
    fireEvent.keyDown(input, { key: 'Enter' })

    expect(choose).toHaveBeenCalledWith(expect.stringMatching(/^\d{4}-\d{2}-\d{2}$/))
  })

  it('says why a typed date cannot be used, and Esc closes', () => {
    const close = vi.fn()
    render(<AsOfMenu value="" onChoose={() => undefined} onClose={close} />)
    const input = screen.getByRole('combobox', { name: 'As-of date' })

    fireEvent.change(input, { target: { value: 'soon' } })
    expect(screen.getByRole('status')).toHaveTextContent('Try 2023-06-30')
    fireEvent.keyDown(input, { key: 'Escape' })
    expect(close).toHaveBeenCalled()
  })
})
