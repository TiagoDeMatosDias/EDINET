import { ArrowRight, BarChart3, Building2, Check, FileText, GitCompare, Search, StickyNote } from 'lucide-react'
import { Link } from 'react-router-dom'

import { BrandLockup } from '../../components/Brand'
import { MarketingLayout } from './MarketingLayout'

const features = [
  {
    icon: Search,
    title: 'Find any listed company',
    copy: 'Search by company name, ticker, EDINET code, and other identifiers from one consistent company finder.',
  },
  {
    icon: FileText,
    title: 'Read the source filings',
    copy: 'Explore retained EDINET filings, XBRL facts, and Japanese and English narrative views without losing context.',
  },
  {
    icon: Building2,
    title: 'Understand the financials',
    copy: 'Review standardized statements, company snapshots, historical metrics, ratios, prices, and reporting trends.',
  },
  {
    icon: GitCompare,
    title: 'Compare what matters',
    copy: 'Place companies side by side and choose the financial or analytical metrics that fit your research question.',
  },
  {
    icon: BarChart3,
    title: 'Test investment ideas',
    copy: 'Screen the market, build rules, and run point-in-time backtests before turning a thesis into a decision.',
  },
  {
    icon: StickyNote,
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
            <div className="marketing-proof" aria-label="Product highlights">
              <span><Check aria-hidden="true" /> One research workspace</span>
              <span><Check aria-hidden="true" /> Source-linked company data</span>
              <span><Check aria-hidden="true" /> €10 per month</span>
            </div>
          </div>

          <div className="product-preview" aria-label="Example company research snapshot">
            <div className="product-preview__bar">
              <BrandLockup className="product-preview__brand" compact />
              <span className="product-preview__status">Data ready</span>
            </div>
            <div className="product-preview__search"><Search aria-hidden="true" /> Search name, ticker, or EDINET code</div>
            <div className="product-preview__company">
              <div>
                <span className="preview-label">Selected company</span>
                <strong>Example Industries</strong>
                <small>TYO: 0000 · E00000</small>
              </div>
              <span className="preview-chip">FY 2026</span>
            </div>
            <div className="product-preview__metrics">
              <div><span>Revenue</span><strong>¥842.6B</strong><small className="positive">+8.4%</small></div>
              <div><span>Operating margin</span><strong>14.2%</strong><small className="positive">+1.1 pts</small></div>
              <div><span>ROE</span><strong>12.8%</strong><small>5Y: 10.9%</small></div>
            </div>
            <div className="product-preview__chart">
              <div className="chart-heading"><span>Revenue history</span><small>5 fiscal years</small></div>
              <svg viewBox="0 0 520 150" role="img" aria-label="Illustrative rising revenue chart">
                <defs>
                  <linearGradient id="preview-fill" x1="0" x2="0" y1="0" y2="1">
                    <stop offset="0%" stopColor="#D88373" stopOpacity=".32" />
                    <stop offset="100%" stopColor="#D88373" stopOpacity="0" />
                  </linearGradient>
                </defs>
                <path className="chart-gridline" d="M0 30H520M0 75H520M0 120H520" />
                <path className="chart-area" d="M0 130 C60 122 75 112 125 110 S200 82 260 88 S350 70 390 54 S470 38 520 18 V150 H0Z" />
                <path className="chart-line" d="M0 130 C60 122 75 112 125 110 S200 82 260 88 S350 70 390 54 S470 38 520 18" />
              </svg>
            </div>
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
            {features.map(({ icon: Icon, title, copy }) => (
              <article className="marketing-feature-card" key={title}>
                <span className="marketing-feature-card__icon"><Icon aria-hidden="true" /></span>
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
