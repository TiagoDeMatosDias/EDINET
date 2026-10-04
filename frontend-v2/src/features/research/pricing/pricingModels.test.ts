import { describe, expect, it } from 'vitest'

import { accruedInterest, cashflows, cleanPrice, dirtyPrice, hazardFromProbability, hazardFromSpread, impliedHazard, internalRate, outcomes, priceWithCredit, riskMeasures, riskyDirtyPrice, scenarioChange, yieldFromPrice } from './bondModel'
import { altmanZ, altmanZDoublePrime, defaultPoint, merton, mertonAt } from './creditModel'
import { addDays, binomialPrice, blackScholes, daysBetween, expectedMove, impliedVolatility, normCdf, STRATEGY_PRESETS, strategyCost, strategyFees, strategyGreeks, strikeLadder, strikeStep, summarizeStrategy } from './optionsModel'

const base = { spot: 100, strike: 100, years: 1, rate: 0.05, dividendYield: 0, volatility: 0.2 }

describe('Black–Scholes', () => {
  it('matches textbook prices and Greeks', () => {
    const call = blackScholes('call', base)
    const put = blackScholes('put', base)
    expect(call.price).toBeCloseTo(10.4506, 3)
    expect(put.price).toBeCloseTo(5.5735, 3)
    expect(call.delta).toBeCloseTo(0.6368, 3)
    expect(call.gamma).toBeCloseTo(0.018762, 5)
    expect(call.vega).toBeCloseTo(0.37524, 4)
    expect(call.theta).toBeCloseTo(-6.414 / 365, 4)
    expect(call.rho).toBeCloseTo(0.53232, 4)
    expect(put.delta).toBeCloseTo(call.delta - 1, 6)
    expect(call.probabilityInTheMoney).toBeCloseTo(normCdf(call.d2 as number), 9)
  })

  it('handles dividends (Hull: index option worth 51.83) and keeps put–call parity', () => {
    const input = { spot: 930, strike: 900, years: 2 / 12, rate: 0.08, dividendYield: 0.03, volatility: 0.2 }
    const call = blackScholes('call', input).price
    const put = blackScholes('put', input).price
    expect(call).toBeCloseTo(51.83, 1)
    expect(call - put).toBeCloseTo(930 * Math.exp(-0.03 * 2 / 12) - 900 * Math.exp(-0.08 * 2 / 12), 8)
  })

  it('is worth the discounted intrinsic value at expiry or without volatility', () => {
    expect(blackScholes('call', { ...base, years: 0, spot: 110 }).price).toBeCloseTo(10, 9)
    expect(blackScholes('put', { ...base, years: 0, spot: 110 }).price).toBe(0)
    expect(blackScholes('call', { ...base, volatility: 0 }).price).toBeCloseTo(100 - 100 * Math.exp(-0.05), 9)
  })

  it('recovers the volatility from a price', () => {
    const price = blackScholes('put', { ...base, volatility: 0.37 }).price
    expect(impliedVolatility('put', price, base)).toBeCloseTo(0.37, 5)
    expect(impliedVolatility('call', 0.0001, base)).toBeNull()
    expect(impliedVolatility('call', 150, base)).toBeNull()
  })
})

describe('binomial tree', () => {
  it('converges to Black–Scholes for European exercise and adds an early-exercise premium to puts', () => {
    expect(binomialPrice('call', base, 400, false)).toBeCloseTo(blackScholes('call', base).price, 1)
    // Without dividends an American call is never exercised early.
    expect(binomialPrice('call', base, 400)).toBeCloseTo(binomialPrice('call', base, 400, false), 8)
    expect(binomialPrice('put', base, 400)).toBeGreaterThan(blackScholes('put', base).price + 0.2)
  })
})

describe('strikes', () => {
  it('uses a natural interval about 2% of spot', () => {
    expect(strikeStep(2856.5)).toBe(50)
    expect(strikeStep(281)).toBe(5)
    expect(strikeStep(3753)).toBe(100)
    expect(strikeLadder(2856.5, 5)).toEqual([2750, 2800, 2850, 2900, 2950])
  })
})

describe('strategies', () => {
  const market = { spot: 100, rate: 0.05, dividendYield: 0, volatility: 0.2 }
  const legs = (key: string, strike = 100, step = 5) => STRATEGY_PRESETS.find(item => item.key === key)!.legs(strike, step)

  it('summarises a long call', () => {
    const summary = summarizeStrategy(legs('long-call'), 1, market)
    expect(summary.cost).toBeCloseTo(10.4506, 3)
    expect(summary.maxProfit).toBeNull()
    expect(summary.maxLoss).toBeCloseTo(-10.4506, 3)
    expect(summary.breakevens[0]).toBeCloseTo(110.4506, 3)
    const d2 = (Math.log(100 / 110.4506) + (0.05 - 0.02) * 1) / 0.2
    expect(summary.probabilityOfProfit).toBeCloseTo(normCdf(d2), 6)
  })

  it('finds both breakevens of a straddle and the cap of a spread', () => {
    const straddle = summarizeStrategy(legs('straddle'), 1, market)
    expect(straddle.breakevens.map(value => Math.round(value * 1000) / 1000)).toEqual([100 - straddle.cost, 100 + straddle.cost].map(value => Math.round(value * 1000) / 1000))
    const spread = summarizeStrategy(legs('bull-call'), 1, market)
    expect(spread.maxProfit).toBeCloseTo(20 - spread.cost, 6)
    expect(spread.maxLoss).toBeCloseTo(-spread.cost, 6)
  })

  it('values shares at spot and caps a covered call', () => {
    const covered = legs('covered-call')
    expect(strategyCost(covered, 1, market)).toBeCloseTo(100 - blackScholes('call', { ...base, strike: 110 }).price, 6)
    expect(strategyGreeks(covered, 1, market).delta).toBeCloseTo(1 - blackScholes('call', { ...base, strike: 110 }).delta, 6)
    const summary = summarizeStrategy(covered, 1, market)
    expect(summary.maxProfit).toBeCloseTo(110 - summary.cost, 6)
  })

  it('takes trading fees out of every result', () => {
    const straddle = legs('straddle')
    // ¥500 a contract of 100 shares, charged opening and closing, on two legs: ¥20 a share.
    const fees = strategyFees(straddle, { perContract: 500, contractSize: 100, roundTrip: true })
    expect(fees).toBeCloseTo(20, 9)
    expect(strategyFees(straddle, { perContract: 0, contractSize: 100, roundTrip: true })).toBe(0)
    const plain = summarizeStrategy(straddle, 1, market)
    const charged = summarizeStrategy(straddle, 1, market, fees)
    expect(charged.cost).toBeCloseTo(plain.cost, 9)
    expect(charged.maxLoss).toBeCloseTo((plain.maxLoss as number) - 20, 9)
    expect(charged.breakevens[0]).toBeCloseTo(plain.breakevens[0] - 20, 6)
    expect(charged.breakevens[1]).toBeCloseTo(plain.breakevens[1] + 20, 6)
    expect(charged.probabilityOfProfit).toBeLessThan(plain.probabilityOfProfit)
  })

  it('charges a percentage fee on each leg\'s premium, or on the share price for shares', () => {
    const straddle = legs('straddle')
    const premiums = straddle.map(leg => Math.abs(strategyCost([leg], 1, market)))
    const percent = { mode: 'percent' as const, perContract: 500, percent: 0.01, contractSize: 100, roundTrip: false }
    expect(strategyFees(straddle, percent, 1, market)).toBeCloseTo(0.01 * (premiums[0] + premiums[1]), 9)
    expect(strategyFees(straddle, { ...percent, roundTrip: true }, 1, market)).toBeCloseTo(0.02 * (premiums[0] + premiums[1]), 9)
    expect(strategyFees([{ kind: 'stock', side: 1, quantity: 2, strike: 0 }], percent, 1, market)).toBeCloseTo(0.01 * market.spot * 2, 9)
    // Without prices a percentage cannot be charged; the set amount is ignored in this mode.
    expect(strategyFees(straddle, percent)).toBe(0)
  })

  it('converts between an expiry date and days to expiry', () => {
    expect(addDays('2026-10-03', 90)).toBe('2027-01-01')
    expect(daysBetween('2026-10-03', '2027-01-01')).toBe(90)
    expect(daysBetween('2026-10-03', 'soon')).toBeNull()
  })

  it('gives a one-sigma band', () => {
    const move = expectedMove(100, 0.2, 0.25)
    expect(move.low).toBeCloseTo(100 * Math.exp(-0.1), 9)
    expect(move.high).toBeCloseTo(100 * Math.exp(0.1), 9)
  })
})

describe('bonds', () => {
  const bond = { face: 100, coupon: 0.04, frequency: 2, years: 5 }

  it('prices at par when the yield equals the coupon, and inverts', () => {
    expect(cleanPrice(bond, 0.04)).toBeCloseTo(100, 9)
    expect(yieldFromPrice(bond, 95)).toBeCloseTo(0.04 + 0.0113, 3)
    expect(cleanPrice(bond, yieldFromPrice(bond, 95) as number)).toBeCloseTo(95, 6)
  })

  it('has duration equal to maturity for a zero-coupon bond', () => {
    const zero = { face: 100, coupon: 0, frequency: 1, years: 7 }
    expect(dirtyPrice(zero, 0.03)).toBeCloseTo(100 / 1.03 ** 7, 9)
    const risk = riskMeasures(zero, 0.03)
    expect(risk.macaulay).toBeCloseTo(7, 9)
    expect(risk.modified).toBeCloseTo(7 / 1.03, 9)
    // Duration and convexity predict a 1% yield rise to within a few basis points.
    const predicted = -risk.modified * 0.01 + 0.5 * risk.convexity * 0.0001
    expect(dirtyPrice(zero, 0.04) / dirtyPrice(zero, 0.03) - 1).toBeCloseTo(predicted, 3)
  })

  it('accrues interest over a short first period', () => {
    const odd = { ...bond, years: 2.25 }
    expect(cashflows(odd).map(flow => flow.time)).toEqual([0.25, 0.75, 1.25, 1.75, 2.25])
    expect(accruedInterest(odd)).toBeCloseTo(1, 9)
    expect(cleanPrice(odd, 0.04)).toBeCloseTo(100, 1)
  })

  it('discounts for default risk and recovers the hazard from a price', () => {
    const credit = { riskFree: 0.02, hazard: 0.03, recovery: 0.4 }
    expect(riskyDirtyPrice(bond, { ...credit, hazard: 0 })).toBeCloseTo(dirtyPrice(bond, 0.02), 9)
    // Recovery is on face only, so even full recovery loses the coupons after default.
    expect(riskyDirtyPrice(bond, { ...credit, recovery: 1 })).toBeGreaterThan(riskyDirtyPrice(bond, credit))
    const priced = priceWithCredit(bond, credit)
    expect(priced.price).toBeLessThan(priced.riskFreePrice)
    // The credit triangle: spread ≈ hazard × (1 − recovery).
    expect(priced.spread).toBeCloseTo(0.03 * 0.6, 2)
    expect(priced.cumulativeDefault).toBeCloseTo(1 - Math.exp(-0.15), 9)
    expect(impliedHazard(bond, priced.price, 0.02, 0.4)).toBeCloseTo(0.03, 6)
    expect(impliedHazard(bond, 200, 0.02, 0.4)).toBeNull()
    expect(hazardFromProbability(1 - Math.exp(-0.5), 5)).toBeCloseTo(0.1, 9)
    expect(hazardFromSpread(0.012, 0.4)).toBeCloseTo(0.02, 9)
  })

  it('lists every ending with probabilities that sum to one', () => {
    const credit = { riskFree: 0.02, hazard: 0.05, recovery: 0.4 }
    const price = priceWithCredit(bond, credit)
    const ends = outcomes(bond, price.price, credit)
    expect(ends).toHaveLength(11)
    expect(ends.reduce((sum, item) => sum + item.probability, 0)).toBeCloseTo(1, 9)
    const repaid = ends[ends.length - 1]
    expect(repaid.defaulted).toBe(false)
    expect(repaid.return).toBeCloseTo(price.yield as number, 8)
    expect(repaid.totalReturn).toBeCloseTo(120 / (price.price + accruedInterest(bond)) - 1, 9)
    expect(ends[0].return as number).toBeLessThan(-0.5)
    expect(internalRate(100, [{ time: 1, amount: 105 }], 1)).toBeCloseTo(0.05, 9)
  })

  it('reprices for rate and spread scenarios', () => {
    const credit = { riskFree: 0.02, hazard: 0.02, recovery: 0.4 }
    expect(scenarioChange(bond, credit, 0, 0)).toBeCloseTo(0, 12)
    expect(scenarioChange(bond, credit, 0.01, 0)).toBeLessThan(-0.04)
    expect(scenarioChange(bond, credit, 0, -0.5)).toBeGreaterThan(0)
  })
})

describe('credit models', () => {
  it('recovers the firm behind an equity value (Merton)', () => {
    const firm = mertonAt(150, 0.25, 100, 0.02, 1)
    const root = 1
    const d1 = (Math.log(150 / 100) + (0.02 + 0.25 ** 2 / 2)) / (0.25 * root)
    const equity = 150 * normCdf(d1) - 100 * Math.exp(-0.02) * normCdf(d1 - 0.25)
    const equityVolatility = normCdf(d1) * 0.25 * 150 / equity
    const solved = merton({ equity, equityVolatility, debt: 100, rate: 0.02, years: 1 })!
    expect(solved.converged).toBe(true)
    expect(solved.assets).toBeCloseTo(150, 4)
    expect(solved.assetVolatility).toBeCloseTo(0.25, 6)
    expect(solved.probabilityOfDefault).toBeCloseTo(firm.probabilityOfDefault, 8)
    expect(solved.distanceToDefault).toBeCloseTo((Math.log(1.5) + (0.02 - 0.03125)) / 0.25, 6)
    expect(solved.spread).toBeGreaterThan(0)
    expect(merton({ equity: 0, equityVolatility: 0.3, debt: 1, rate: 0, years: 1 })).toBeNull()
  })

  it('sets the default point between short-term and total liabilities', () => {
    expect(defaultPoint(60, 100)).toBe(80)
    expect(defaultPoint(null, 100)).toBe(100)
    expect(defaultPoint(10, null)).toBeNull()
  })

  it('scores Altman Z and Z″ with their zones', () => {
    const input = { totalAssets: 1000, totalLiabilities: 400, currentAssets: 500, currentLiabilities: 300, retainedEarnings: 300, operatingIncome: 100, revenue: 1200, marketCap: 1500, bookEquity: 600 }
    const z = altmanZ(input)!
    expect(z.score).toBeCloseTo(1.2 * 0.2 + 1.4 * 0.3 + 3.3 * 0.1 + 0.6 * 3.75 + 1.2, 9)
    expect(z.zone).toBe('safe')
    expect(altmanZDoublePrime(input)!.score).toBeCloseTo(6.56 * 0.2 + 3.26 * 0.3 + 6.72 * 0.1 + 1.05 * 1.5, 9)
    expect(altmanZ({ ...input, retainedEarnings: -800, marketCap: 50, operatingIncome: -50 })!.zone).toBe('distress')
    expect(altmanZ({ ...input, retainedEarnings: null })).toBeNull()
  })
})
