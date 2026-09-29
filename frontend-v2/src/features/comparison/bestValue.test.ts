import { describe, expect, it } from 'vitest'

import { bestValue } from './bestValue'

describe('comparison best value', () => {
  it('prefers the lowest non-negative value when lower is better', () => {
    expect(bestValue('lower', [18.4, 14.7])).toBe(14.7)
    expect(bestValue('lower', [-5, 14.7, 20])).toBe(14.7)
    expect(bestValue('lower', [0, 0.8])).toBe(0)
  })

  it('prefers the highest value when higher is better', () => {
    expect(bestValue('higher', [0.08, 0.12])).toBe(0.12)
    expect(bestValue('higher', [-0.02, 0.05])).toBe(0.05)
  })

  it('does not highlight metrics without a direction', () => {
    expect(bestValue(undefined, [100, 200])).toBeNull()
  })

  it('needs at least two comparable values', () => {
    expect(bestValue('lower', [14.7, null])).toBeNull()
    expect(bestValue('lower', [14.7, -3])).toBeNull()
  })
})
