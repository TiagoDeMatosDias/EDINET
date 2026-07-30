import { Activity } from 'lucide-react'
import type { ReactNode } from 'react'
import { Link, NavLink } from 'react-router-dom'

import './marketing.css'

export function MarketingLayout({ children }: { children: ReactNode }) {
  return (
    <div className="marketing-page">
      <header className="marketing-header">
        <div className="marketing-container marketing-header__inner">
          <Link className="marketing-brand" to="/" aria-label="Shade home">
            <span className="marketing-brand__mark"><Activity aria-hidden="true" /></span>
            <span><strong>Shade</strong><small>Japanese equity research</small></span>
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
            <Link className="marketing-brand marketing-brand--footer" to="/">
              <span className="marketing-brand__mark"><Activity aria-hidden="true" /></span>
              <strong>Shade</strong>
            </Link>
            <p>Research Japanese public companies from source filing to investment view.</p>
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
