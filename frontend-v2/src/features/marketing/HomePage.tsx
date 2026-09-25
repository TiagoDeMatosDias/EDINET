import { ArrowRight } from 'lucide-react'
import { Link } from 'react-router-dom'

import { BRAND_MARK_URL } from '../../components/Brand'
import { MarketingLayout } from './MarketingLayout'

const features = [
  {
    title: 'Find any listed company',
    copy: 'Search by company name, ticker, EDINET code, and other identifiers from one consistent company finder.',
  },
  {
    title: 'Read the source filings',
    copy: 'Explore retained EDINET filings, XBRL facts, and Japanese and English narrative views without losing context.',
  },
  {
    title: 'Understand the financials',
    copy: 'Review standardized statements, company snapshots, historical metrics, ratios, prices, and reporting trends.',
  },
  {
    title: 'Compare what matters',
    copy: 'Place companies side by side and choose the financial or analytical metrics that fit your research question.',
  },
  {
    title: 'Test investment ideas',
    copy: 'Screen the market, build rules, and run point-in-time backtests before turning a thesis into a decision.',
  },
  {
    title: 'Keep research organized',
    copy: 'Use tags, watchlists, notes, and portfolio views to keep companies and follow-up work in one place.',
  },
]

export default function HomePage() {
  return (
    <MarketingLayout>
      <section className="marketing-hero">
        <div className="marketing-container marketing-hero__grid">
          <div className="marketing-hero__copy">
            <span className="marketing-kicker">Value in context</span>
            <h1>Research companies with the evidence still attached.</h1>
            <p>
              Shade Research brings company discovery, source filings, standardized financials,
              comparisons, screening, and portfolio research into one focused workspace.
            </p>
            <div className="marketing-hero__actions">
              <Link className="button button--primary marketing-button" to="/register">
                Create your account <ArrowRight aria-hidden="true" />
              </Link>
              <Link className="button button--secondary marketing-button" to="/login">Sign in</Link>
              <Link className="marketing-inline-link" to="/pricing">View pricing</Link>
            </div>
            <ul className="marketing-proof" aria-label="Product highlights">
              <li>One research workspace</li>
              <li>Source-linked company data</li>
              <li>€10 per month</li>
            </ul>
          </div>

          <div className="marketing-hero__art" aria-hidden="true">
            <img src={BRAND_MARK_URL} alt="" />
            <span className="marketing-hero__horizon" />
          </div>
        </div>
      </section>

      <section className="marketing-section" aria-labelledby="research-workflow-heading">
        <div className="marketing-container">
          <div className="marketing-section__heading">
            <span className="marketing-kicker">From filing to decision</span>
            <h2 id="research-workflow-heading">The core tools for a complete company research workflow.</h2>
            <p>Start broad, inspect the evidence, compare alternatives, and keep the work attached to the company.</p>
          </div>
          <div className="marketing-feature-grid">
            {features.map(({ title, copy }, index) => (
              <article className="marketing-feature-card" key={title}>
                <span className="marketing-feature-card__index">{String(index + 1).padStart(2, '0')}</span>
                <h3>{title}</h3>
                <p>{copy}</p>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section className="marketing-cta">
        <div className="marketing-container marketing-cta__card">
          <div>
            <span className="marketing-kicker">Start your research</span>
            <h2>One place to move from raw disclosure to a clearer investment view.</h2>
          </div>
          <div className="marketing-cta__actions">
            <Link className="button button--primary marketing-button" to="/register">Create account</Link>
            <Link className="marketing-inline-link" to="/pricing">See the plan <ArrowRight aria-hidden="true" /></Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  )
}
