import { BarChart3, BriefcaseBusiness, Building2, CircleCheck, CircleX, FileText, GitCompare, Home, LogIn, Menu, MessagesSquare, PanelLeftClose, Search, Settings, Shield, StickyNote, UserCircle, Workflow, X } from 'lucide-react'
import { useEffect, useState, type ReactNode } from 'react'
import { NavLink, useLocation, useNavigate } from 'react-router-dom'

import { useHealth, useSystemStatus } from '../hooks/useHealth'
import { BrandLockup } from './Brand'
import { GlobalCompanySearch } from './GlobalCompanySearch'
import { GlobalHotkeys } from './GlobalHotkeys'
import { globalScope } from '../hotkeys/globalScopes'
import { useGotoPages } from '../hotkeys/goto'
import { useHotkeyText } from '../hotkeys/useHotkeyText'
import { useAuth } from '../features/auth/authContext'
import { useChatUnread } from '../features/chat/chatApi'

const navigation = [
  { to: '/overview', label: 'Overview', icon: Home },
  { to: '/screen', label: 'Screen', icon: Search },
  { to: '/analyze', label: 'Analyze', icon: Building2 },
  { to: '/backtest', label: 'Backtest', icon: BarChart3 },
  { to: '/portfolio', label: 'Portfolio', icon: BriefcaseBusiness },
  { to: '/filings', label: 'Filings', icon: FileText },
  { to: '/compare', label: 'Compare', icon: GitCompare },
  { to: '/research', label: 'Research', icon: StickyNote },
  { to: '/chat', label: 'Chat', icon: MessagesSquare },
]
const pipelineNavigation = { to: '/pipeline', label: 'Data pipeline', icon: Workflow }
const accountNavigation = { to: '/account', label: 'Account', icon: Settings }
const adminNavigation = { to: '/admin', label: 'Admin', icon: Shield }

/** The sidebar links, each with its "G then a letter" shortcut; the letters light up while G waits for one. */
function Navigation({ onNavigate, keysActive = false, unreadChat = 0 }: { onNavigate?: () => void; keysActive?: boolean; unreadChat?: number }) {
  const auth = useAuth()
  const isAdmin = auth.user?.role === 'admin'
  const gotoPages = useGotoPages(isAdmin, Boolean(auth.user))
  const leader = useHotkeyText(globalScope.byId.goto)
  const items = [
    ...navigation.slice(0, 5),
    ...(isAdmin ? [pipelineNavigation] : []),
    ...navigation.slice(5),
    ...(auth.user ? [accountNavigation] : []),
    ...(isAdmin ? [adminNavigation] : []),
  ]
  return <nav className={keysActive ? 'primary-nav primary-nav--keys' : 'primary-nav'} aria-label="Primary navigation">
    {items.map(item => {
      const Icon = item.icon
      const key = gotoPages.find(page => page.to === item.to)?.hint
      const unread = item.to === '/chat' && unreadChat > 0 ? unreadChat : 0
      return <NavLink key={item.to} to={item.to} end={item.to === '/overview'} onClick={onNavigate} title={key ? `${item.label} (${leader} then ${key})` : undefined}><Icon aria-hidden="true" /><span>{item.label}</span>{unread > 0 && <span className="primary-nav__badge" aria-label={`${unread} unread`}>{unread > 99 ? '99+' : unread}</span>}{key && <kbd className="primary-nav__key" aria-hidden="true">{key}</kbd>}</NavLink>
    })}
    <p className="primary-nav__hint">Press <kbd>{leader}</kbd> then a letter</p>
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
  const [keysActive, setKeysActive] = useState(false)
  const health = useHealth()
  const location = useLocation()
  const auth = useAuth()
  const unread = useChatUnread(Boolean(auth.user) || auth.status?.mode === 'disabled')
  const operator = auth.user?.role === 'admin' || auth.user?.role === 'operator'
  const system = useSystemStatus(operator)
  const activeJobs = operator ? system.data?.jobs.active ?? 0 : 0
  const mobileNavigation = navigation.slice(0, 5)
  // Pages remount per path; start each one at the top instead of the previous page's scroll position.
  useEffect(() => { window.scrollTo(0, 0) }, [location.pathname])

  return <div className={collapsed ? 'app-shell app-shell--collapsed' : 'app-shell'}>
    <aside className={mobileOpen ? 'sidebar sidebar--open' : 'sidebar'}>
      <div className="brand"><BrandLockup showTagline /><button className="icon-button mobile-only" aria-label="Close navigation" onClick={() => setMobileOpen(false)}><X /></button></div>
      <Navigation onNavigate={() => setMobileOpen(false)} keysActive={keysActive} unreadChat={unread.data?.total ?? 0} />
      <button className="sidebar-collapse desktop-only" onClick={() => setCollapsed(value => !value)}><PanelLeftClose aria-hidden="true" /><span>{collapsed ? 'Expand' : 'Collapse'}</span></button>
    </aside>
    {mobileOpen && <button className="backdrop" aria-label="Close navigation" onClick={() => setMobileOpen(false)} />}
    <div className="app-content">
      <header className="topbar"><button className="icon-button mobile-only" aria-label="Open navigation" onClick={() => setMobileOpen(true)}><Menu /></button><GlobalCompanySearch /><div className="topbar-actions"><div className={health.isError ? 'health health--error' : 'health'}>{health.isError ? <CircleX /> : <CircleCheck />}<span>{health.isError ? 'Backend unavailable' : activeJobs ? `${activeJobs} job${activeJobs === 1 ? '' : 's'} active` : 'Data service ready'}</span></div><AuthSection /></div></header>
      <main id="main-content" key={location.pathname}>{children}</main>
      <GlobalHotkeys isAdmin={auth.user?.role === 'admin'} signedIn={Boolean(auth.user)} onPendingChange={setKeysActive} />
      <nav className="mobile-nav" aria-label="Mobile primary navigation">{mobileNavigation.map(item => { const Icon = item.icon; return <NavLink key={item.to} to={item.to} end={item.to === '/overview'}><Icon /><span>{item.label}</span></NavLink> })}</nav>
    </div>
  </div>
}
