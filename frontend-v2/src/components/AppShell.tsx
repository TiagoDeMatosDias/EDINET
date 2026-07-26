import { Activity, BarChart3, BriefcaseBusiness, Building2, CircleCheck, CircleX, FileText, GitCompare, Home, LogIn, Menu, PanelLeftClose, Search, Settings, Shield, StickyNote, UserCircle, Workflow, X } from 'lucide-react'
import { useState, type ReactNode } from 'react'
import { NavLink, useLocation, useNavigate } from 'react-router-dom'

import { useHealth } from '../hooks/useHealth'
import { GlobalCompanySearch } from './GlobalCompanySearch'
import { useAuth } from '../features/auth/AuthProvider'

const navigation = [
  { to: '/', label: 'Overview', icon: Home },
  { to: '/screen', label: 'Screen', icon: Search },
  { to: '/analyze', label: 'Analyze', icon: Building2 },
  { to: '/backtest', label: 'Backtest', icon: BarChart3 },
  { to: '/portfolio', label: 'Portfolio', icon: BriefcaseBusiness },
  { to: '/pipeline', label: 'Data pipeline', icon: Workflow },
  { to: '/filings', label: 'Filings', icon: FileText },
  { to: '/compare', label: 'Compare', icon: GitCompare },
  { to: '/research', label: 'Research', icon: StickyNote },
]

function Navigation({ onNavigate }: { onNavigate?: () => void }) {
  const auth = useAuth()
  return <nav className="primary-nav" aria-label="Primary navigation">
    {navigation.map(item => { const Icon = item.icon; return <NavLink key={item.to} to={item.to} end={item.to === '/'} onClick={onNavigate}><Icon aria-hidden="true" /><span>{item.label}</span></NavLink> })}
    {auth.user && <NavLink to="/account" end onClick={onNavigate}><Settings aria-hidden="true" /><span>Account</span></NavLink>}
    {auth.user?.role === 'admin' && <NavLink to="/admin" end onClick={onNavigate}><Shield aria-hidden="true" /><span>Admin</span></NavLink>}
  </nav>
}

function AuthSection() {
  const auth = useAuth()
  const navigate = useNavigate()
  const [menuOpen, setMenuOpen] = useState(false)

  if (auth.loading) {
    return <div className="auth-status"><small>Checking session…</small></div>
  }

  if (!auth.user) {
    if (auth.status?.mode === 'accounts') {
      return (
        <button className="button button--small button--primary" onClick={() => navigate('/login')}>
          <LogIn aria-hidden="true" size={14} />
          <span>Sign in</span>
        </button>
      )
    }
    return (
      <div className="auth-status auth-status--disabled">
        <small>Auth disabled</small>
      </div>
    )
  }

  return (
    <div className="auth-status auth-status--user">
      <button
        className="auth-user-button"
        onClick={() => setMenuOpen(v => !v)}
        onBlur={() => setTimeout(() => setMenuOpen(false), 200)}
      >
        <UserCircle aria-hidden="true" size={16} />
        <span>{auth.user.username}</span>
        <small>{auth.user.role}</small>
      </button>
      {menuOpen && (
        <div className="auth-dropdown">
          <button onClick={() => { navigate('/account'); setMenuOpen(false) }}>
            <Settings size={14} /> Account settings
          </button>
          {auth.user.role === 'admin' && (
            <button onClick={() => { navigate('/admin'); setMenuOpen(false) }}>
              <Shield size={14} /> Administration
            </button>
          )}
          <hr />
          <button onClick={() => { void auth.logout(); setMenuOpen(false) }}>
            Sign out
          </button>
        </div>
      )}
    </div>
  )
}

export function AppShell({ children }: { children: ReactNode }) {
  const [mobileOpen, setMobileOpen] = useState(false)
  const [collapsed, setCollapsed] = useState(false)
  const health = useHealth()
  const location = useLocation()

  return <div className={collapsed ? 'app-shell app-shell--collapsed' : 'app-shell'}>
    <aside className={mobileOpen ? 'sidebar sidebar--open' : 'sidebar'}>
      <div className="brand"><span className="brand-mark"><Activity aria-hidden="true" /></span><span className="brand-copy"><strong>Shade</strong><small>Research workspace</small></span><button className="icon-button mobile-only" aria-label="Close navigation" onClick={() => setMobileOpen(false)}><X /></button></div>
      <Navigation onNavigate={() => setMobileOpen(false)} />
      <button className="sidebar-collapse desktop-only" onClick={() => setCollapsed(value => !value)}><PanelLeftClose aria-hidden="true" /><span>{collapsed ? 'Expand' : 'Collapse'}</span></button>
    </aside>
    {mobileOpen && <button className="backdrop" aria-label="Close navigation" onClick={() => setMobileOpen(false)} />}
    <div className="app-content">
      <header className="topbar"><button className="icon-button mobile-only" aria-label="Open navigation" onClick={() => setMobileOpen(true)}><Menu /></button><GlobalCompanySearch /><div className="topbar-actions"><div className={health.isError ? 'health health--error' : 'health'}>{health.isError ? <CircleX /> : <CircleCheck />}<span>{health.isError ? 'Backend unavailable' : health.data?.jobs_active ? `${health.data.jobs_active} job active` : 'Data service ready'}</span></div><AuthSection /></div></header>
      <main id="main-content" key={location.pathname}>{children}</main>
      <nav className="mobile-nav" aria-label="Mobile primary navigation">{navigation.slice(0, 5).map(item => { const Icon = item.icon; return <NavLink key={item.to} to={item.to} end={item.to === '/'}><Icon /><span>{item.label}</span></NavLink> })}</nav>
    </div>
  </div>
}
