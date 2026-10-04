/**
 * Option pricing: Black–Scholes–Merton with a continuous dividend yield as the
 * baseline, a Cox–Ross–Rubinstein tree for American exercise, and multi-leg
 * strategies. Rates, yields, and volatility are annual fractions; time is in years.
 */

export type OptionKind = 'call' | 'put'
export type LegKind = OptionKind | 'stock'

export interface OptionInputs {
  spot: number
  strike: number
  years: number
  rate: number
  dividendYield: number
  volatility: number
}

export interface OptionValue {
  price: number
  /** Per 1 unit of the underlying. */
  delta: number
  gamma: number
  /** Per 1 percentage point of volatility. */
  vega: number
  /** Per calendar day. */
  theta: number
  /** Per 1 percentage point of the interest rate. */
  rho: number
  /** Risk-neutral probability of finishing in the money. */
  probabilityInTheMoney: number
  d1: number | null
  d2: number | null
}

/** Complementary error function; fractional error below 1.2e-7 everywhere (Numerical Recipes, erfcc). */
function erfc(x: number) {
  const z = Math.abs(x)
  const t = 1 / (1 + 0.5 * z)
  const r = t * Math.exp(-z * z - 1.26551223 + t * (1.00002368 + t * (0.37409196 + t * (0.09678418 + t * (-0.18628806 + t * (0.27886807 + t * (-1.13520398 + t * (1.48851587 + t * (-0.82215223 + t * 0.17087277)))))))))
  return x >= 0 ? r : 2 - r
}

export function normCdf(x: number) {
  return 0.5 * erfc(-x / Math.SQRT2)
}

export function normPdf(x: number) {
  return Math.exp(-0.5 * x * x) / Math.sqrt(2 * Math.PI)
}

export function blackScholes(kind: OptionKind, input: OptionInputs): OptionValue {
  const { spot: S, strike: K, years: T, rate: r, dividendYield: q, volatility: sigma } = input
  const carry = Math.exp(-q * Math.max(T, 0))
  const discount = Math.exp(-r * Math.max(T, 0))
  const sign = kind === 'call' ? 1 : -1
  if (!(T > 0) || !(sigma > 0) || !(S > 0) || !(K > 0)) {
    // At expiry, or with no volatility, the option is worth its discounted forward intrinsic value.
    const forwardGap = sign * (S * carry - K * discount)
    const inTheMoney = forwardGap > 0
    return {
      price: Math.max(forwardGap, 0),
      delta: inTheMoney ? sign * carry : 0,
      gamma: 0,
      vega: 0,
      theta: 0,
      rho: inTheMoney ? sign * K * Math.max(T, 0) * discount / 100 : 0,
      probabilityInTheMoney: inTheMoney ? 1 : 0,
      d1: null,
      d2: null,
    }
  }
  const root = Math.sqrt(T)
  const d1 = (Math.log(S / K) + (r - q + sigma * sigma / 2) * T) / (sigma * root)
  const d2 = d1 - sigma * root
  const nd1 = normCdf(sign * d1)
  const nd2 = normCdf(sign * d2)
  const density = normPdf(d1)
  const price = sign * (S * carry * nd1 - K * discount * nd2)
  const thetaYear = -S * carry * density * sigma / (2 * root) - sign * r * K * discount * nd2 + sign * q * S * carry * nd1
  return {
    price: Math.max(price, 0),
    delta: sign * carry * nd1,
    gamma: carry * density / (S * sigma * root),
    vega: S * carry * density * root / 100,
    theta: thetaYear / 365,
    rho: sign * K * T * discount * nd2 / 100,
    probabilityInTheMoney: nd2,
    d1,
    d2,
  }
}

/** American option value on a Cox–Ross–Rubinstein tree; with ``american: false`` it converges to Black–Scholes. */
export function binomialPrice(kind: OptionKind, input: OptionInputs, steps = 200, american = true) {
  const { spot: S, strike: K, years: T, rate: r, dividendYield: q, volatility: sigma } = input
  if (!(T > 0) || !(sigma > 0)) return blackScholes(kind, input).price
  const dt = T / steps
  const up = Math.exp(sigma * Math.sqrt(dt))
  const down = 1 / up
  const growth = Math.exp((r - q) * dt)
  const p = (growth - down) / (up - down)
  if (!(p > 0 && p < 1)) return blackScholes(kind, input).price
  const discount = Math.exp(-r * dt)
  const sign = kind === 'call' ? 1 : -1
  const values = new Float64Array(steps + 1)
  for (let i = 0; i <= steps; i += 1) values[i] = Math.max(sign * (S * up ** (steps - 2 * i) - K), 0)
  for (let step = steps - 1; step >= 0; step -= 1) {
    for (let i = 0; i <= step; i += 1) {
      const held = discount * (p * values[i] + (1 - p) * values[i + 1])
      values[i] = american ? Math.max(held, sign * (S * up ** (step - 2 * i) - K)) : held
    }
  }
  return values[0]
}

/** The volatility at which Black–Scholes matches a market price, or ``null`` when the price is outside no-arbitrage bounds. */
export function impliedVolatility(kind: OptionKind, price: number, input: Omit<OptionInputs, 'volatility'>) {
  if (!(price > 0) || !(input.years > 0)) return null
  const at = (volatility: number) => blackScholes(kind, { ...input, volatility }).price
  let low = 1e-4
  let high = 5
  if (price < at(low) - 1e-9 || price > at(high)) return null
  for (let i = 0; i < 100; i += 1) {
    const mid = (low + high) / 2
    if (at(mid) > price) high = mid
    else low = mid
    if (high - low < 1e-7) break
  }
  return (low + high) / 2
}

/** A strike interval that reads naturally at this price: 1, 2, 2.5, or 5 times a power of ten, about 2% of spot. */
export function strikeStep(spot: number) {
  if (!(spot > 0)) return 1
  const target = spot / 50
  const power = 10 ** Math.floor(Math.log10(target))
  const candidates = [1, 2, 2.5, 5, 10].map(multiple => multiple * power)
  return candidates.reduce((best, step) => Math.abs(Math.log(step / target)) < Math.abs(Math.log(best / target)) ? step : best)
}

export function roundToStep(value: number, step: number) {
  return Math.round(value / step) * step
}

/** Strikes on the step grid around spot, nearest-the-money in the middle. */
export function strikeLadder(spot: number, count = 13) {
  const step = strikeStep(spot)
  const centre = roundToStep(spot, step)
  const half = Math.floor(count / 2)
  return Array.from({ length: count }, (_, index) => centre + (index - half) * step).filter(strike => strike > 0)
}

export interface Leg {
  kind: LegKind
  /** +1 bought, -1 sold. */
  side: 1 | -1
  quantity: number
  /** Ignored for stock. */
  strike: number
}

export interface StrategyPreset {
  key: string
  label: string
  description: string
  legs: (strike: number, step: number) => Leg[]
}

const option = (kind: OptionKind, side: 1 | -1, strike: number): Leg => ({ kind, side, quantity: 1, strike })
const stock: Leg = { kind: 'stock', side: 1, quantity: 1, strike: 0 }

export const STRATEGY_PRESETS: StrategyPreset[] = [
  { key: 'long-call', label: 'Long call', description: 'Profit if the price rises above strike plus premium.', legs: strike => [option('call', 1, strike)] },
  { key: 'long-put', label: 'Long put', description: 'Profit if the price falls below strike less premium.', legs: strike => [option('put', 1, strike)] },
  { key: 'covered-call', label: 'Covered call', description: 'Own the shares and sell a call: income, capped upside.', legs: (strike, step) => [stock, option('call', -1, strike + 2 * step)] },
  { key: 'protective-put', label: 'Protective put', description: 'Own the shares and buy a put: a floor under losses.', legs: (strike, step) => [stock, option('put', 1, strike - 2 * step)] },
  { key: 'collar', label: 'Collar', description: 'Shares, a bought put, and a sold call: a band of outcomes.', legs: (strike, step) => [stock, option('put', 1, strike - 2 * step), option('call', -1, strike + 2 * step)] },
  { key: 'straddle', label: 'Straddle', description: 'A call and a put at one strike: profit from a large move either way.', legs: strike => [option('call', 1, strike), option('put', 1, strike)] },
  { key: 'strangle', label: 'Strangle', description: 'Out-of-the-money call and put: cheaper, needs a bigger move.', legs: (strike, step) => [option('put', 1, strike - 2 * step), option('call', 1, strike + 2 * step)] },
  { key: 'bull-call', label: 'Bull call spread', description: 'Buy a call, sell a higher one: a cheaper, capped bet on a rise.', legs: (strike, step) => [option('call', 1, strike), option('call', -1, strike + 4 * step)] },
  { key: 'bear-put', label: 'Bear put spread', description: 'Buy a put, sell a lower one: a cheaper, capped bet on a fall.', legs: (strike, step) => [option('put', 1, strike), option('put', -1, strike - 4 * step)] },
  { key: 'iron-condor', label: 'Iron condor', description: 'Sell a strangle and buy wings: income if the price stays in a range.', legs: (strike, step) => [option('put', 1, strike - 6 * step), option('put', -1, strike - 3 * step), option('call', -1, strike + 3 * step), option('call', 1, strike + 6 * step)] },
]

export interface Market {
  spot: number
  rate: number
  dividendYield: number
  volatility: number
}

/** One leg's value per unit at a given spot with ``years`` left. */
function legUnitValue(leg: Leg, spot: number, years: number, market: Market) {
  if (leg.kind === 'stock') return spot
  return blackScholes(leg.kind, { spot, strike: leg.strike, years, rate: market.rate, dividendYield: market.dividendYield, volatility: market.volatility }).price
}

export function legPremium(leg: Leg, years: number, market: Market) {
  return leg.side * leg.quantity * legUnitValue(leg, market.spot, years, market)
}

/** Net cost of opening the position today (negative when it pays a credit). */
export function strategyCost(legs: Leg[], years: number, market: Market) {
  return legs.reduce((sum, leg) => sum + legPremium(leg, years, market), 0)
}

/** Profit or loss at ``spot`` with ``remaining`` years left, against today's cost and any trading fees. */
export function strategyProfit(legs: Leg[], spot: number, remaining: number, years: number, market: Market, fees = 0) {
  const value = legs.reduce((sum, leg) => sum + leg.side * leg.quantity * legUnitValue(leg, spot, remaining, market), 0)
  return value - strategyCost(legs, years, market) - fees
}

export type FeeMode = 'contract' | 'percent'

export interface TradingFees {
  /** A set amount per contract, or a share of each trade's value. Older saved settings have no mode: per contract. */
  mode?: FeeMode
  /** Charged per contract traded, in the price currency. */
  perContract: number
  /** Share of the traded value (premium, or the share price for a share leg), as a fraction. */
  percent?: number
  /** Shares per contract; a share leg counts as one lot of this size. */
  contractSize: number
  /** Charged again when the position is closed or exercised. */
  roundTrip: boolean
}

/**
 * Fees per share of underlying for the whole position: every leg's quantity, once or twice.
 * A percentage fee needs today's prices (``years`` and ``market``); the closing trade is
 * charged on the opening value, since what the position will be worth then is unknown.
 */
export function strategyFees(legs: Leg[], fees: TradingFees, years = 0, market?: Market) {
  const times = fees.roundTrip ? 2 : 1
  if (fees.mode === 'percent') {
    const rate = fees.percent ?? 0
    if (!(rate > 0) || !market) return 0
    return legs.reduce((sum, leg) => sum + Math.abs(leg.quantity) * rate * Math.abs(legUnitValue(leg, market.spot, years, market)) * times, 0)
  }
  if (!(fees.perContract > 0) || !(fees.contractSize > 0)) return 0
  const perShare = fees.perContract / fees.contractSize * times
  return legs.reduce((sum, leg) => sum + Math.abs(leg.quantity) * perShare, 0)
}

export function strategyGreeks(legs: Leg[], years: number, market: Market) {
  const total = { delta: 0, gamma: 0, vega: 0, theta: 0, rho: 0 }
  for (const leg of legs) {
    const weight = leg.side * leg.quantity
    if (leg.kind === 'stock') { total.delta += weight; continue }
    const value = blackScholes(leg.kind, { spot: market.spot, strike: leg.strike, years, rate: market.rate, dividendYield: market.dividendYield, volatility: market.volatility })
    total.delta += weight * value.delta
    total.gamma += weight * value.gamma
    total.vega += weight * value.vega
    total.theta += weight * value.theta
    total.rho += weight * value.rho
  }
  return total
}

/** Expiry profit is piecewise linear in spot; beyond the highest strike its slope is the net number of calls plus shares. */
function upsideSlope(legs: Leg[]) {
  return legs.reduce((sum, leg) => sum + (leg.kind === 'put' ? 0 : leg.side * leg.quantity), 0)
}

export interface StrategySummary {
  /** Premiums and shares bought less those sold, before fees. */
  cost: number
  fees: number
  maxProfit: number | null
  maxLoss: number | null
  breakevens: number[]
  /** Risk-neutral chance the expiry profit is positive. */
  probabilityOfProfit: number
}

/** Kinks of the expiry payoff: 0, each strike, and far above the highest strike. */
function payoffKinks(legs: Leg[], spot: number) {
  const strikes = legs.filter(leg => leg.kind !== 'stock').map(leg => leg.strike)
  const top = Math.max(spot, ...strikes) * 2
  return [...new Set([0, ...strikes, top])].sort((a, b) => a - b)
}

export function summarizeStrategy(legs: Leg[], years: number, market: Market, fees = 0): StrategySummary {
  const cost = strategyCost(legs, years, market)
  const at = (price: number) => strategyProfit(legs, price, 0, years, market, fees)
  const kinks = payoffKinks(legs, market.spot)
  const values = kinks.map(at)
  const up = upsideSlope(legs)
  // A stock or put payoff at zero is finite, so only the upside can be unbounded.
  const maxProfit = up > 1e-9 ? null : Math.max(...values)
  const maxLoss = up < -1e-9 ? null : Math.min(...values)
  const breakevens: number[] = []
  for (let i = 0; i < kinks.length - 1; i += 1) {
    const [a, b] = [values[i], values[i + 1]]
    if (a === 0) breakevens.push(kinks[i])
    else if (a * b < 0) breakevens.push(kinks[i] + (kinks[i + 1] - kinks[i]) * (a / (a - b)))
  }
  const last = kinks[kinks.length - 1]
  if (values[values.length - 1] * (values[values.length - 1] + up * last) < 0) breakevens.push(last - values[values.length - 1] / up)
  const positive = breakevens.filter(value => value > 0)
  return { cost, fees, maxProfit, maxLoss, breakevens: positive, probabilityOfProfit: probabilityOfProfit(legs, years, market, positive, fees) }
}

/**
 * Risk-neutral chance the expiry profit is positive. The payoff is piecewise
 * linear, so its sign only changes at breakevens: sum the lognormal probability
 * of each interval between them where the profit is positive.
 */
function probabilityOfProfit(legs: Leg[], years: number, market: Market, breakevens: number[], fees: number) {
  const profitAt = (price: number) => strategyProfit(legs, price, 0, years, market, fees)
  if (!(years > 0) || !(market.volatility > 0)) return profitAt(market.spot) > 0 ? 1 : 0
  const spread = market.volatility * Math.sqrt(years)
  const drift = Math.log(market.spot) + (market.rate - market.dividendYield - market.volatility ** 2 / 2) * years
  const cdf = (price: number) => price <= 0 ? 0 : price === Infinity ? 1 : normCdf((Math.log(price) - drift) / spread)
  const bounds = [0, ...[...breakevens].sort((a, b) => a - b), Infinity]
  let probability = 0
  for (let i = 0; i < bounds.length - 1; i += 1) {
    const [low, high] = [bounds[i], bounds[i + 1]]
    const inside = high === Infinity ? low * 2 + 1 : (low + high) / 2
    if (profitAt(inside) > 0) probability += cdf(high) - cdf(low)
  }
  return Math.min(Math.max(probability, 0), 1)
}

/** One standard deviation of the price at expiry under the lognormal model, as a low/high band. */
export function expectedMove(spot: number, volatility: number, years: number) {
  const spread = volatility * Math.sqrt(Math.max(years, 0))
  return { low: spot * Math.exp(-spread), high: spot * Math.exp(spread) }
}

const DAY_MS = 86_400_000

/** A date ``days`` after ``from`` (both ISO dates). */
export function addDays(from: string, days: number) {
  return new Date(Date.parse(`${from}T00:00:00Z`) + Math.round(days) * DAY_MS).toISOString().slice(0, 10)
}

/** Whole calendar days from ``from`` to ``to``, or ``null`` for an unreadable date. */
export function daysBetween(from: string, to: string) {
  const [start, end] = [Date.parse(`${from}T00:00:00Z`), Date.parse(`${to}T00:00:00Z`)]
  return Number.isFinite(start) && Number.isFinite(end) ? Math.round((end - start) / DAY_MS) : null
}
