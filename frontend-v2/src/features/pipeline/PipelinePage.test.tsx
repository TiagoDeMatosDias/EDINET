import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import { ConfigField } from './PipelinePage'

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

    await waitFor(() => expect(onChange).toHaveBeenCalledWith({
      filename: 'prices.csv',
      content: btoa(csv),
    }))
  })
})
