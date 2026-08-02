import { lazy, Suspense, type ReactNode } from 'react'
import { Navigate, Outlet, Route, Routes } from 'react-router-dom'

import { AppShell } from './components/AppShell'
import { LoadingState } from './components/Feedback'
import { AuthProvider, useAuth } from './features/auth/AuthProvider'

const OverviewPage = lazy(() => import('./features/overview/OverviewPage'))
const HomePage = lazy(() => import('./features/marketing/HomePage'))
const PricingPage = lazy(() => import('./features/marketing/PricingPage'))
const ScreeningPage = lazy(() => import('./features/screening/ScreeningPage'))
const AnalysisPage = lazy(() => import('./features/analysis/AnalysisPage'))
const BacktestingPage = lazy(() => import('./features/backtesting/BacktestingPage'))
const PortfolioPage = lazy(() => import('./features/portfolio/PortfolioPage'))
const PipelinePage = lazy(() => import('./features/pipeline/PipelinePage'))
const FilingsPage = lazy(() => import('./features/filings/FilingsPage'))
const FilingViewerPage = lazy(() => import('./features/filings/FilingViewerPage'))
const ComparisonPage = lazy(() => import('./features/comparison/ComparisonPage'))
const ResearchPage = lazy(() => import('./features/research/ResearchPage'))
const LoginPage = lazy(() => import('./features/auth/LoginPage'))
const AccountPage = lazy(() => import('./features/auth/AccountPage'))
const AdminPage = lazy(() => import('./features/auth/AdminPage'))

function WorkspaceLayout() {
  return (
    <AppShell>
      <Outlet />
    </AppShell>
  )
}

function AdminOnlyPage({ children }: { children: ReactNode }) {
  const auth = useAuth()
  if (auth.loading) return <LoadingState label="Checking admin access" />
  if (auth.user?.role !== 'admin') return <Navigate replace to="/overview" />
  return children
}

export function App() {
  return (
    <AuthProvider>
      <Suspense fallback={<LoadingState label="Loading page" />}>
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/pricing" element={<PricingPage />} />
          <Route path="/login" element={<LoginPage key="login" initialMode="login" />} />
          <Route path="/register" element={<LoginPage key="register" initialMode="register" />} />
          <Route path="/security" element={<Navigate replace to="/analyze" />} />
          <Route path="/backtesting" element={<Navigate replace to="/backtest" />} />

          <Route element={<WorkspaceLayout />}>
            <Route path="/overview" element={<OverviewPage />} />
            <Route path="/screen" element={<ScreeningPage />} />
            <Route path="/analyze" element={<AnalysisPage />} />
            <Route path="/analyze/:companyCode" element={<AnalysisPage />} />
            <Route path="/backtest" element={<BacktestingPage />} />
            <Route path="/portfolio" element={<PortfolioPage />} />
            <Route path="/pipeline" element={<AdminOnlyPage><PipelinePage /></AdminOnlyPage>} />
            <Route path="/filings" element={<FilingsPage />} />
            <Route path="/filings/:docId" element={<FilingViewerPage />} />
            <Route path="/compare" element={<ComparisonPage />} />
            <Route path="/research" element={<ResearchPage />} />
            <Route path="/account" element={<AccountPage />} />
            <Route path="/admin" element={<AdminPage />} />
          </Route>

          <Route path="*" element={<Navigate replace to="/" />} />
        </Routes>
      </Suspense>
    </AuthProvider>
  )
}
