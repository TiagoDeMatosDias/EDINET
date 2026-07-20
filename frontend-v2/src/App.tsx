import { lazy, Suspense } from 'react'
import { Navigate, Route, Routes } from 'react-router-dom'

import { AppShell } from './components/AppShell'
import { LoadingState } from './components/Feedback'

const OverviewPage = lazy(() => import('./features/overview/OverviewPage'))
const ScreeningPage = lazy(() => import('./features/screening/ScreeningPage'))
const AnalysisPage = lazy(() => import('./features/analysis/AnalysisPage'))
const BacktestingPage = lazy(() => import('./features/backtesting/BacktestingPage'))
const PortfolioPage = lazy(() => import('./features/portfolio/PortfolioPage'))
const PipelinePage = lazy(() => import('./features/pipeline/PipelinePage'))

export function App() {
  return (
    <AppShell>
      <Suspense fallback={<LoadingState label="Loading workspace" />}>
        <Routes>
          <Route path="/" element={<OverviewPage />} />
          <Route path="/screen" element={<ScreeningPage />} />
          <Route path="/analyze" element={<AnalysisPage />} />
          <Route path="/analyze/:companyCode" element={<AnalysisPage />} />
          <Route path="/backtest" element={<BacktestingPage />} />
          <Route path="/portfolio" element={<PortfolioPage />} />
          <Route path="/pipeline" element={<PipelinePage />} />
          <Route path="*" element={<Navigate replace to="/" />} />
        </Routes>
      </Suspense>
    </AppShell>
  )
}
