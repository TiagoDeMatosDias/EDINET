import { ArrowRight, Check } from 'lucide-react'
import { Link } from 'react-router-dom'

import { MarketingLayout } from './MarketingLayout'

const included = [
  'Company search and analysis',
  'EDINET filing and XBRL research',
  'Financial statements, ratios, and rolling metrics',
  'Flexible multi-company comparisons',
  'Screening and backtesting tools',
  'Tags, watchlists, notes, and portfolio workspace',
]

export default function PricingPage() {
  return (
    <MarketingLayout>
      <section className="pricing-hero">
        <div className="marketing-container">
          <div className="marketing-section__heading marketing-section__heading--centered">
            <span className="marketing-kicker">Simple pricing</span>
            <h1>One plan. The entire research workspace.</h1>
            <p>Choose monthly flexibility or pay for the year upfront and save two months.</p>
          </div>

          <article className="pricing-card">
            <div className="pricing-card__summary">
              <span className="pricing-card__badge">Shade Research</span>
              <h2>Full access</h2>
              <p>Everything you need to research, compare, and track public companies across markets and sources.</p>
              <div className="pricing-amount">
                <strong>€10</strong>
                <span>per month</span>
              </div>
              <div className="pricing-annual">
                <strong>€100 per year</strong>
                <span>when paid upfront · save €20</span>
              </div>
              <Link className="button button--primary marketing-button pricing-card__button" to="/register">
                Create your account <ArrowRight aria-hidden="true" />
              </Link>
              <p className="pricing-card__signin">Already registered? <Link to="/login">Sign in</Link></p>
            </div>

            <div className="pricing-card__included">
              <h3>Everything is included</h3>
              <ul>
                {included.map(item => (
                  <li key={item}><span><Check aria-hidden="true" /></span>{item}</li>
                ))}
              </ul>
              <div className="pricing-card__note">
                <strong>No feature tiers.</strong>
                <p>Monthly and annual billing provide the same complete workspace.</p>
              </div>
            </div>
          </article>
        </div>
      </section>
    </MarketingLayout>
  )
}
