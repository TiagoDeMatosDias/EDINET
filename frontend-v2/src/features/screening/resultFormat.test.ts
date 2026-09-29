import { describe, expect, it } from 'vitest'

import { formatResultValue } from './resultFormat'

describe('screening result formatting', () => {
  it('rounds numbers and abbreviates large amounts', () => {
    expect(formatResultValue(189.8463396453232)).toBe('189.85')
    expect(formatResultValue(32_022_092_000)).toBe('32.02 Billion')
    expect(formatResultValue(1.00885288829534)).toBe('1.01')
  })

  it('shows columns declared as percent from their stored fractions', () => {
    expect(formatResultValue(0.035129608373361175, 'percent')).toBe('3.5%')
  })

  it('leaves text untouched and marks missing values', () => {
    expect(formatResultValue('72030')).toBe('72030')
    expect(formatResultValue('Alpha Corp')).toBe('Alpha Corp')
    expect(formatResultValue(null, 'percent')).toBe('—')
  })
})
