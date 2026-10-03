/**
 * Default-risk estimates from a company's own data: the Merton structural model
 * (equity as a call on the firm's assets) and Altman's Z-scores.
 */

import type { PricingInputs } from '../researchTypes'
import { normCdf } from './optionsModel'

export interface MertonInputs {
  /** Market value of equity. */
  equity: number
  /** Annualised equity volatility. */
  equityVolatility: number
  /** Debt due at the horizon (the default point). */
  debt: number
  rate: number
  years: number
  /** Expected asset return; the risk-free rate gives risk-neutral probabilities. */
  drift?: number
}

export interface MertonResult {
  assets: number
  assetVolatility: number
  distanceToDefault: number
  probabilityOfDefault: number
  /** Yield over the risk-free rate on the firm's debt, treated as one zero-coupon bond. */
  spread: number
  leverage: number
  converged: boolean
}

function callOnAssets(assets: number, volatility: number, debt: number, rate: number, years: number) {
  const root = Math.sqrt(years)
  const d1 = (Math.log(assets / debt) + (rate + volatility * volatility / 2) * years) / (volatility * root)
  const d2 = d1 - volatility * root
  return { value: assets * normCdf(d1) - debt * Math.exp(-rate * years) * normCdf(d2), d1, d2 }
}

/**
 * Solve for the asset value and volatility that reproduce the equity's market
 * value and volatility (E = V·N(d1) − D·e^(−rT)·N(d2) and σE·E = N(d1)·σV·V),
 * by alternating the two equations until they agree.
 */
export function merton(input: MertonInputs): MertonResult | null {
  const { equity: E, equityVolatility: sigmaE, debt: D, rate: r, years: T } = input
  if (!(E > 0) || !(sigmaE > 0) || !(D > 0) || !(T > 0)) return null
  let assets = E + D * Math.exp(-r * T)
  let volatility = sigmaE * E / assets
  let converged = false
  for (let iteration = 0; iteration < 200; iteration += 1) {
    // Newton on the first equation for V, holding σV: dE/dV = N(d1).
    for (let step = 0; step < 50; step += 1) {
      const { value, d1 } = callOnAssets(assets, volatility, D, r, T)
      const slope = Math.max(normCdf(d1), 1e-12)
      const next = Math.max(assets - (value - E) / slope, E)
      if (Math.abs(next - assets) < 1e-10 * assets) { assets = next; break }
      assets = next
    }
    const { d1 } = callOnAssets(assets, volatility, D, r, T)
    const nextVolatility = sigmaE * E / (Math.max(normCdf(d1), 1e-12) * assets)
    if (Math.abs(nextVolatility - volatility) < 1e-9) { volatility = nextVolatility; converged = true; break }
    volatility = nextVolatility
  }
  return mertonAt(assets, volatility, D, r, T, input.drift ?? r, converged)
}

/** Default probability and spread at another horizon, for the same firm. */
export function mertonAt(assets: number, volatility: number, debt: number, rate: number, years: number, drift = rate, converged = true): MertonResult {
  const root = Math.sqrt(years)
  const distance = (Math.log(assets / debt) + (drift - volatility * volatility / 2) * years) / (volatility * root)
  const { d1, d2 } = callOnAssets(assets, volatility, debt, rate, years)
  // Debt is worth the assets less the equity call: D·e^(−rT)·N(d2) + V·N(−d1).
  const debtValue = debt * Math.exp(-rate * years) * normCdf(d2) + assets * normCdf(-d1)
  const debtYield = -Math.log(debtValue / debt) / years
  return {
    assets,
    assetVolatility: volatility,
    distanceToDefault: distance,
    probabilityOfDefault: normCdf(-distance),
    spread: Math.max(debtYield - rate, 0),
    leverage: debt / assets,
    converged,
  }
}

/** The KMV default point: short-term liabilities plus half the long-term ones. */
export function defaultPoint(currentLiabilities: number | null | undefined, totalLiabilities: number | null | undefined) {
  if (totalLiabilities == null || !(totalLiabilities > 0)) return null
  if (currentLiabilities == null) return totalLiabilities
  return currentLiabilities + 0.5 * Math.max(totalLiabilities - currentLiabilities, 0)
}

export type Zone = 'safe' | 'grey' | 'distress'

export interface ZScore {
  score: number
  zone: Zone
  components: Array<{ key: string; label: string; ratio: number; weight: number }>
}

export interface ZInputs {
  totalAssets?: number | null
  totalLiabilities?: number | null
  currentAssets?: number | null
  currentLiabilities?: number | null
  retainedEarnings?: number | null
  operatingIncome?: number | null
  revenue?: number | null
  marketCap?: number | null
  bookEquity?: number | null
}

function score(parts: Array<[string, string, number | null, number]>, safe: number, distress: number): ZScore | null {
  if (parts.some(([, , ratio]) => ratio == null || !Number.isFinite(ratio))) return null
  const components = parts.map(([key, label, ratio, weight]) => ({ key, label, ratio: ratio as number, weight }))
  const total = components.reduce((sum, part) => sum + part.ratio * part.weight, 0)
  return { score: total, zone: total > safe ? 'safe' : total < distress ? 'distress' : 'grey', components }
}

const ratio = (numerator: number | null | undefined, denominator: number | null | undefined) => numerator != null && denominator ? numerator / denominator : null

/** Altman (1968) for listed manufacturers: above 2.99 is safe, below 1.81 is distress. */
export function altmanZ(input: ZInputs) {
  const workingCapital = input.currentAssets != null && input.currentLiabilities != null ? input.currentAssets - input.currentLiabilities : null
  return score([
    ['X1', 'Working capital ÷ assets', ratio(workingCapital, input.totalAssets), 1.2],
    ['X2', 'Retained earnings ÷ assets', ratio(input.retainedEarnings, input.totalAssets), 1.4],
    ['X3', 'Operating income ÷ assets', ratio(input.operatingIncome, input.totalAssets), 3.3],
    ['X4', 'Market cap ÷ liabilities', ratio(input.marketCap, input.totalLiabilities), 0.6],
    ['X5', 'Sales ÷ assets', ratio(input.revenue, input.totalAssets), 1.0],
  ], 2.99, 1.81)
}

/** Altman Z″ for non-manufacturers, which leaves out asset turnover: above 2.6 is safe, below 1.1 is distress. */
export function altmanZDoublePrime(input: ZInputs) {
  const workingCapital = input.currentAssets != null && input.currentLiabilities != null ? input.currentAssets - input.currentLiabilities : null
  return score([
    ['X1', 'Working capital ÷ assets', ratio(workingCapital, input.totalAssets), 6.56],
    ['X2', 'Retained earnings ÷ assets', ratio(input.retainedEarnings, input.totalAssets), 3.26],
    ['X3', 'Operating income ÷ assets', ratio(input.operatingIncome, input.totalAssets), 6.72],
    ['X4', 'Book equity ÷ liabilities', ratio(input.bookEquity, input.totalLiabilities), 1.05],
  ], 2.6, 1.1)
}

/** What a company's statements and share price say about its default risk. */
export function creditProfile(inputs: PricingInputs, state: { riskFree: number; years: number; equityWindow: string }) {
  const lines = inputs.credit.lines
  const equityVolatility = inputs.volatility.estimates.find(item => item.window === state.equityWindow)?.value ?? null
  const debt = defaultPoint(lines.CurrentLiabilities, lines.TotalLiabilities)
  const firm = inputs.market_cap && equityVolatility && debt ? merton({ equity: inputs.market_cap, equityVolatility, debt, rate: state.riskFree, years: 1 }) : null
  const atMaturity = firm && debt ? mertonAt(firm.assets, firm.assetVolatility, debt, state.riskFree, Math.max(state.years, 0.25)) : null
  const zInput = {
    totalAssets: lines.TotalAssets, totalLiabilities: lines.TotalLiabilities, currentAssets: lines.CurrentAssets, currentLiabilities: lines.CurrentLiabilities,
    retainedEarnings: lines.RetainedEarnings, operatingIncome: lines.OperatingIncome, revenue: lines.Revenue, marketCap: inputs.market_cap, bookEquity: lines.TotalEquity,
  }
  return { equityVolatility, debt, firm, atMaturity, z: altmanZ(zInput), zDouble: altmanZDoublePrime(zInput) }
}

export type CreditProfile = ReturnType<typeof creditProfile>
