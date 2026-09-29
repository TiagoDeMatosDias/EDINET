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

describe('pipeline job details', () => {
  it('renders step state as a table and output as labelled values', async () => {
    const { RunOutput, StepStateTable } = await import('./JobDetails')
    render(<>
      <StepStateTable steps={[
        { ordinal: 1, step_name: 'parse_xbrl', overwrite: false, status: 'failed', duration_ms: 65_000, error_message: 'Archive missing' },
        { ordinal: 0, step_name: 'download_xbrl', overwrite: true, status: 'completed', duration_ms: 1_500 },
      ]} />
      <RunOutput output={{ download_xbrl: { files_downloaded: 1234, skipped: [], doc_ids: ['S1', 'S2'] } }} />
    </>)

    const rows = screen.getAllByRole('row').slice(1).map(row => row.textContent)
    expect(rows[0]).toContain('download_xbrl')
    expect(rows[0]).toContain('1.5 s')
    expect(rows[1]).toContain('Archive missing')
    expect(rows[1]).toContain('1 min 5 s')
    expect(screen.getByText('Files downloaded').nextSibling).toHaveTextContent('1,234')
    expect(screen.getByText('Doc ids').nextSibling).toHaveTextContent('S1, S2')
    expect(screen.getByText('Skipped').nextSibling).toHaveTextContent('None')
  })
})
