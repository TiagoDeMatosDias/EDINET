/**
 * Fixed-coupon bond pricing with default risk. Yields compound at the coupon
 * frequency (the street convention); default follows a constant hazard rate,
 * with a recovery paid on face value at the coupon date after default.
 * Rates and probabilities are fractions; time is in years; prices are per ``face``.
 */

export interface BondTerms {
  face: number
  /** Annual coupon rate. */
  coupon: number
  /** Coupons per year. */
  frequency: number
  /** Years to maturity; fractional maturities have a short first period. */
  years: number
}

export interface Cashflow {
  time: number
  coupon: number
  principal: number
  amount: number
}

export function cashflows({ face, coupon, frequency, years }: BondTerms): Cashflow[] {
  if (!(years > 0) || !(frequency > 0)) return []
  const count = Math.max(1, Math.ceil(years * frequency - 1e-9))
  const payment = face * coupon / frequency
  return Array.from({ length: count }, (_, index) => {
    const time = years - (count - 1 - index) / frequency
    const principal = index === count - 1 ? face : 0
    return { time, coupon: payment, principal, amount: payment + principal }
  })
}

/** Coupon earned since the last payment date, which the buyer pays the seller on top of the clean price. */
export function accruedInterest(terms: BondTerms) {
  const flows = cashflows(terms)
  if (!flows.length) return 0
  const elapsed = 1 - flows[0].time * terms.frequency
  return Math.max(elapsed, 0) * flows[0].coupon
}

function discount(time: number, yieldRate: number, frequency: number) {
  return (1 + yieldRate / frequency) ** (-frequency * time)
}

/** Full (dirty) price at a yield to maturity. */
export function dirtyPrice(terms: BondTerms, yieldRate: number) {
  return cashflows(terms).reduce((sum, flow) => sum + flow.amount * discount(flow.time, yieldRate, terms.frequency), 0)
}

export function cleanPrice(terms: BondTerms, yieldRate: number) {
  return dirtyPrice(terms, yieldRate) - accruedInterest(terms)
}

/** Yield to maturity for a clean price, or ``null`` if no yield between -50% and 200% produces it. */
export function yieldFromPrice(terms: BondTerms, price: number) {
  const target = price + accruedInterest(terms)
  let low = -0.5
  let high = 2
  const at = (rate: number) => dirtyPrice(terms, rate)
  if (!(target > 0) || target > at(low) || target < at(high)) return null
  for (let i = 0; i < 200; i += 1) {
    const mid = (low + high) / 2
    if (at(mid) > target) low = mid
    else high = mid
    if (high - low < 1e-10) break
  }
  return (low + high) / 2
}

export interface RiskMeasures {
  macaulay: number
  modified: number
  convexity: number
  /** Price change for a one basis point fall in yield, per ``face``. */
  dv01: number
}

export function riskMeasures(terms: BondTerms, yieldRate: number): RiskMeasures {
  const flows = cashflows(terms)
  const f = terms.frequency
  const price = dirtyPrice(terms, yieldRate)
  if (!(price > 0)) return { macaulay: 0, modified: 0, convexity: 0, dv01: 0 }
  let weighted = 0
  let curvature = 0
  for (const flow of flows) {
    const pv = flow.amount * discount(flow.time, yieldRate, f)
    weighted += flow.time * pv
    curvature += flow.time * (flow.time + 1 / f) * pv
  }
  const macaulay = weighted / price
  const modified = macaulay / (1 + yieldRate / f)
  const convexity = curvature / (price * (1 + yieldRate / f) ** 2)
  return { macaulay, modified, convexity, dv01: modified * price / 10_000 }
}

/** Constant hazard rate that gives this cumulative default probability by ``years``. */
export function hazardFromProbability(probability: number, years: number) {
  if (!(years > 0) || !(probability > 0)) return 0
  return -Math.log(1 - Math.min(probability, 0.999999)) / years
}

/** The "credit triangle": a spread compensates expected loss, so hazard ≈ spread ÷ (1 − recovery). */
export function hazardFromSpread(spread: number, recovery: number) {
  return Math.max(spread, 0) / Math.max(1 - recovery, 1e-6)
}

export function survival(hazard: number, time: number) {
  return Math.exp(-hazard * Math.max(time, 0))
}

export interface CreditTerms {
  /** Default-free yield for this maturity. */
  riskFree: number
  hazard: number
  /** Share of face recovered after default. */
  recovery: number
}

/** Dirty price with default risk: surviving cash flows plus recovery if default comes first, discounted at the risk-free yield. */
export function riskyDirtyPrice(terms: BondTerms, credit: CreditTerms) {
  let previous = 1
  let price = 0
  for (const flow of cashflows(terms)) {
    const alive = survival(credit.hazard, flow.time)
    const df = discount(flow.time, credit.riskFree, terms.frequency)
    price += df * (flow.amount * alive + credit.recovery * terms.face * (previous - alive))
    previous = alive
  }
  return price
}

export interface CreditPricing {
  riskFreePrice: number
  price: number
  yield: number | null
  spread: number | null
  /** Present value lost to expected defaults. */
  expectedLoss: number
  cumulativeDefault: number
}

export function priceWithCredit(terms: BondTerms, credit: CreditTerms): CreditPricing {
  const accrued = accruedInterest(terms)
  const riskFreePrice = dirtyPrice(terms, credit.riskFree) - accrued
  const price = riskyDirtyPrice(terms, credit) - accrued
  const yieldRate = yieldFromPrice(terms, price)
  return {
    riskFreePrice,
    price,
    yield: yieldRate,
    spread: yieldRate == null ? null : yieldRate - credit.riskFree,
    expectedLoss: riskFreePrice - price,
    cumulativeDefault: 1 - survival(credit.hazard, terms.years),
  }
}

/** The hazard rate at which the default-adjusted price equals a market price, or ``null`` if even no default risk is too cheap. */
export function impliedHazard(terms: BondTerms, price: number, riskFree: number, recovery: number) {
  const target = price + accruedInterest(terms)
  const at = (hazard: number) => riskyDirtyPrice(terms, { riskFree, hazard, recovery })
  if (target > at(0)) return null
  let low = 0
  let high = 5
  if (target < at(high)) return high
  for (let i = 0; i < 200; i += 1) {
    const mid = (low + high) / 2
    if (at(mid) > target) low = mid
    else high = mid
    if (high - low < 1e-10) break
  }
  return (low + high) / 2
}

export interface Outcome {
  /** Coupon date by which default happens, or maturity for full repayment. */
  time: number
  probability: number
  defaulted: boolean
  /** Total cash received per ``face``. */
  received: number
  /** Annualised return on the clean-plus-accrued price paid, at the coupon frequency. */
  return: number | null
  /** Everything received against the price paid, not annualised. */
  totalReturn: number
}

/** Every way the bond can end — default in each coupon period, or repayment — with its chance and the return it gives. */
export function outcomes(terms: BondTerms, price: number, credit: Pick<CreditTerms, 'hazard' | 'recovery'>): Outcome[] {
  const flows = cashflows(terms)
  const paid = price + accruedInterest(terms)
  const result: Outcome[] = []
  let previous = 1
  flows.forEach((flow, index) => {
    const alive = survival(credit.hazard, flow.time)
    const received = flows.slice(0, index).map(item => ({ time: item.time, amount: item.amount }))
    received.push({ time: flow.time, amount: credit.recovery * terms.face })
    const total = received.reduce((sum, item) => sum + item.amount, 0)
    result.push({
      time: flow.time,
      probability: previous - alive,
      defaulted: true,
      received: total,
      return: internalRate(paid, received, terms.frequency),
      totalReturn: total / paid - 1,
    })
    previous = alive
  })
  const full = flows.map(flow => ({ time: flow.time, amount: flow.amount }))
  const total = full.reduce((sum, item) => sum + item.amount, 0)
  result.push({
    time: terms.years,
    probability: previous,
    defaulted: false,
    received: total,
    return: internalRate(paid, full, terms.frequency),
    totalReturn: total / paid - 1,
  })
  return result
}

/** The yield, compounding at ``frequency``, at which these cash flows are worth ``paid`` today. */
export function internalRate(paid: number, flows: Array<{ time: number; amount: number }>, frequency: number) {
  const value = (rate: number) => flows.reduce((sum, flow) => sum + flow.amount * discount(flow.time, rate, frequency), 0)
  let low = -0.99 * frequency
  let high = 5
  if (!(paid > 0) || value(low) < paid || value(high) > paid) return null
  for (let i = 0; i < 200; i += 1) {
    const mid = (low + high) / 2
    if (value(mid) > paid) low = mid
    else high = mid
    if (high - low < 1e-10) break
  }
  return (low + high) / 2
}

/** Price change, as a fraction, when the risk-free yield and the credit spread move by the given amounts. */
export function scenarioChange(terms: BondTerms, credit: CreditTerms, rateShift: number, spreadShift: number) {
  const base = priceWithCredit(terms, credit)
  // A wider spread is a higher hazard by the credit triangle; a tighter one, lower (never below zero).
  const hazard = Math.max(credit.hazard + spreadShift / Math.max(1 - credit.recovery, 1e-6), 0)
  const shifted = priceWithCredit(terms, { ...credit, riskFree: credit.riskFree + rateShift, hazard })
  return base.price ? shifted.price / base.price - 1 : 0
}
