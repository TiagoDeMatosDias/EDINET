import { Activity, BarChart3, BriefcaseBusiness, Building2, CircleCheck, CircleX, Home, Menu, PanelLeftClose, Search, Workflow, X } from 'lucide-react'
import { useState, type ReactNode } from 'react'
import { NavLink, useLocation } from 'react-router-dom'

import { useHealth } from '../hooks/useHealth'
import { GlobalCompanySearch } from './GlobalCompanySearch'

const navigation = [
  { to: '/', label: 'Overview', icon: Home },
  { to: '/screen', label: 'Screen', icon: Search },
  { to: '/analyze', label: 'Analyze', icon: Building2 },
  { to: '/backtest', label: 'Backtest', icon: BarChart3 },
  { to: '/portfolio', label: 'Portfolio', icon: BriefcaseBusiness },
  { to: '/pipeline', label: 'Data pipeline', icon: Workflow },
]

function Navigation({ onNavigate }: { onNavigate?: () => void }) {
  return <nav className="primary-nav" aria-label="Primary navigation">{navigation.map(item => { const Icon = item.icon; return <NavLink key={item.to} to={item.to} end={item.to === '/'} onClick={onNavigate}><Icon aria-hidden="true" /><span>{item.label}</span></NavLink> })}</nav>
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
      <header className="topbar"><button className="icon-button mobile-only" aria-label="Open navigation" onClick={() => setMobileOpen(true)}><Menu /></button><GlobalCompanySearch /><div className={health.isError ? 'health health--error' : 'health'}>{health.isError ? <CircleX /> : <CircleCheck />}<span>{health.isError ? 'Backend unavailable' : health.data?.jobs_active ? `${health.data.jobs_active} job active` : 'Data service ready'}</span></div></header>
      <main id="main-content" key={location.pathname}>{children}</main>
      <nav className="mobile-nav" aria-label="Mobile primary navigation">{navigation.slice(0, 5).map(item => { const Icon = item.icon; return <NavLink key={item.to} to={item.to} end={item.to === '/'}><Icon /><span>{item.label}</span></NavLink> })}</nav>
    </div>
  </div>
}
