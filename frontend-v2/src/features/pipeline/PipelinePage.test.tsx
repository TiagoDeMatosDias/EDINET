import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { ConfigField } from './PipelinePage'

afterEach(cleanup)

describe('pipeline file fields', () => {
  it('encodes a selected CSV for the pipeline upload boundary', async () => {
    const onChange = vi.fn()
    render(
      <ConfigField
        field={{
          name: 'csv_file',
          type: 'file',
          label: 'CSV file',
          filetypes: [['CSV files', '*.csv'], ['All files', '*.*']],
        }}
        value=""
        onChange={onChange}
      />,
    )

    const input = screen.getByLabelText('CSV file')
    expect(input).toHaveAttribute('type', 'file')
    expect(input).toHaveAttribute('accept', '.csv')

    const csv = 'Date,Price\n2025-01-01,100\n'
    fireEvent.change(input, {
      target: { files: [new File([csv], 'prices.csv', { type: 'text/csv' })] },
    })

    await waitFor(() => expect(onChange).toHaveBeenCalledWith(expect.any(File)))
    expect(onChange.mock.calls[0][0]).toHaveProperty('name', 'prices.csv')
  })

  it('rejects an oversized file before reading it into memory', async () => {
    const onChange = vi.fn()
    render(
      <ConfigField
        field={{ name: 'csv_file', type: 'file', label: 'CSV file' }}
        value=""
        maxUploadBytes={10}
        onChange={onChange}
      />,
    )

    const input = screen.getByLabelText('CSV file')
    const file = new File([new Uint8Array(11)], 'large.csv', { type: 'text/csv' })
    fireEvent.change(input, { target: { files: [file] } })

    expect(await screen.findByRole('alert')).toHaveTextContent('File is too large')
    expect(onChange).not.toHaveBeenCalled()
  })
})
