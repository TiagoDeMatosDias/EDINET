import { fxLegs } from './activityModel'
import { money, transactionCashEffect } from './portfolioFormat'
import type { Transaction } from './portfolioTypes'

function signed(amount: number, currency: string) {
  return `${amount > 0 ? '+' : ''}${money(amount, currency, 2)}`
}

/** Cash in (+) or out (−) in the record's currency; a conversion shows both currencies. */
export function CashEffect({ row }: { row: Transaction }) {
  const legs = fxLegs(row)
  if (legs) return <span className="pf-fx-legs" title="A currency conversion: one balance goes down, the other up. Its commission is charged separately.">
    {legs.map(leg => <span key={leg.currency} className={leg.amount < 0 ? 'number-negative' : undefined}>{signed(leg.amount, leg.currency)}</span>)}
  </span>
  const effect = transactionCashEffect(row)
  return <span className={Number(effect) < 0 ? 'number-negative' : undefined}>{money(effect, row.currency ?? 'EUR', 2)}</span>
}
