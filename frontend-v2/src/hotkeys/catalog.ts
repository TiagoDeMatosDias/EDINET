/**
 * Imports every screen's hotkey declarations so the settings panel lists all
 * of them, including screens whose (lazily loaded) pages were never opened.
 * Add a screen's ``*Hotkeys.ts`` here when you create it; order is the order
 * screens appear in settings.
 */
import './globalScopes'
import '../features/overview/overviewHotkeys'
import '../features/pipeline/pipelineHotkeys'
import '../features/auth/accountHotkeys'
import '../features/auth/adminHotkeys'
import '../features/filings/filingsHotkeys'
import '../features/screening/screeningHotkeys'
import '../features/analysis/analysisHotkeys'
import '../features/comparison/comparisonHotkeys'
import '../features/backtesting/backtestHotkeys'
import '../features/portfolio/portfolioHotkeys'
import '../features/research/researchHotkeys'
import '../features/chat/chatHotkeys'
