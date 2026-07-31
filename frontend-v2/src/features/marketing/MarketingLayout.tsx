import type { ReactNode } from 'react'
import { Link, NavLink } from 'react-router-dom'

import { BrandLockup } from '../../components/Brand'
import './marketing.css'

export function MarketingLayout({ children }: { children: ReactNode }) {
  return (
    <div className="marketing-page">
      <header className="marketing-header">
        <div className="marketing-container marketing-header__inner">
          <Link className="marketing-brand" to="/" aria-label="Shade Research home">
            <BrandLockup showTagline />
          </Link>

          <nav className="marketing-nav" aria-label="Public navigation">
            <NavLink to="/" end>Home</NavLink>
            <NavLink to="/pricing">Pricing</NavLink>
          </nav>

          <div className="marketing-header__actions">
            <Link className="marketing-link" to="/login">Sign in</Link>
            <Link className="button button--primary" to="/register">Create account</Link>
          </div>
        </div>
      </header>

      <main>{children}</main>

      <footer className="marketing-footer">
        <div className="marketing-container marketing-footer__inner">
          <div>
            <Link className="marketing-brand marketing-brand--footer" to="/" aria-label="Shade Research home">
              <BrandLockup compact />
            </Link>
            <p>Research companies with the evidence attached and the value placed in context.</p>
          </div>
          <nav aria-label="Footer navigation">
            <Link to="/pricing">Pricing</Link>
            <Link to="/login">Sign in</Link>
            <Link to="/register">Register</Link>
          </nav>
        </div>
      </footer>
    </div>
  )
}
