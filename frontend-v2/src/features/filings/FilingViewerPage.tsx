import { useEffect, useMemo, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { ArrowLeft, Code, Download, FileText, Globe, ShieldAlert, Table2, Tags } from 'lucide-react'

import { apiRequest, queryString } from '../../api/client'
import { EmptyState, LoadingState } from '../../components/Feedback'

interface Filing {
  doc_id: string; edinet_code?: string | null; submitter_name?: string | null
  submitted_at?: string | null; period_start?: string | null; period_end?: string | null
  status: string; form_code?: string | null; archive_sha256?: string | null
}
interface Fact {
  fact_id: string; concept: string; concept_en?: string; context_id?: string
  value_text?: string; numeric_value?: number | null; unit_id?: string
  namespace_uri?: string
}
interface Section {
  section_id: string; title?: string; title_en?: string; text: string; text_en?: string; ordinal: number
}
interface FilingDetail {
  filing: Filing
  artifacts: Array<{ artifact_id: string; member_path: string; kind: string; size_bytes: number }>
}
interface QualityIssue { issue_id: string; severity: string; code: string; message: string }
interface TaxonomyEntry { namespace_uri?: string; concept?: string }

type Tab = 'original' | 'document' | 'facts' | 'audit' | 'taxonomy' | 'quality'

const TABS: { key: Tab; label: string; icon: typeof FileText }[] = [
  { key: 'original', label: 'Report', icon: Globe },
  { key: 'document', label: 'Sections', icon: FileText },
  { key: 'facts', label: 'Statements', icon: Table2 },
  { key: 'audit', label: 'Audit', icon: ShieldAlert },
  { key: 'taxonomy', label: 'Taxonomy', icon: Tags },
  { key: 'quality', label: 'Quality', icon: ShieldAlert },
]

const SEVERITY_CLASS: Record<string, string> = { error: 'status-pill--warn', warning: 'status-pill--warn', info: 'status-pill--ok' }

function fmtNum(n: number): string {
  if (Math.abs(n) >= 1e12) return (n / 1e12).toFixed(2) + 'T'
  if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(2) + 'B'
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(2) + 'M'
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(1) + 'K'
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 })
}

function conceptDisplay(concept: string): string {
  return concept.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
    .replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()).replace(/Net Income/i, 'Net Income').replace(/Gross Profit/i, 'Gross Profit').trim()
}

function extractPeriod(fact: Fact): string {
  const ctx = fact.context_id ?? ''
  const m = ctx.match(/(FY|Q[1-4]|Annual|Interim|SemiAnnual)/i)
  if (m) {
    const rest = ctx.slice(ctx.indexOf(m[1]) + m[1].length)
    const yr = rest.match(/(\d{4})/)
    return yr ? `${m[1]} ${yr[1]}` : m[1]
  }
  const yr = ctx.match(/(\d{4})/)
  return yr ? yr[1] : ctx.slice(-12)
}

// --- Statement structure ---
interface StmtSection { label: string; labelEn?: string; concepts: string[]; isSubtotal?: boolean }
interface StmtDefinition { name: string; sections: StmtSection[] }
interface PivotCell { period: string; value: number | null; fact: Fact }
interface StmtRow { kind: 'section' | 'row' | 'subtotal'; label: string; labelEn?: string; concept?: string; unit?: string; cells: Map<string, PivotCell>; indent: boolean }

// EDGAR-style statement definitions with proper ordering and subtotals
const STATEMENTS: StmtDefinition[] = [
  {
    name: 'Balance Sheet',
    sections: [
      { label: 'Assets', concepts: [] },
      { label: 'Current assets', concepts: ['CurrentAssets', 'CashAndDeposits', 'CashAndCashEquivalents', 'NotesAndAccountsReceivableTrade', 'AccountsReceivableTrade', 'AccountsReceivable', 'NotesReceivable', 'Inventories', 'MerchandiseAndFinishedGoods', 'WorkInProcess', 'RawMaterials', 'Supplies', 'ShortTermLoansReceivable', 'DeferredTaxAssets', 'AllowanceForDoubtfulAccounts', 'AllowanceForDoubtfulAccountsIOAByGroup', 'AccountsPayableOther', 'OtherCurrentAssets', 'Other'] },
      { label: 'Non-current assets', concepts: ['NonCurrentAssets', 'PropertyPlantAndEquipment', 'BuildingsAndStructures', 'MachineryEquipmentAndVehicles', 'Land', 'ConstructionInProgress', 'IntangibleAssets', 'Goodwill', 'Software', 'InvestmentsAndOtherAssets', 'InvestmentSecurities', 'InvestmentsInSubsidiaries', 'LongTermLoansReceivable', 'DeferredTaxAssetsNonCurrent', 'OtherNonCurrentAssets'] },
      { label: 'Total assets', concepts: ['TotalAssets', 'Assets'], isSubtotal: true },
      { label: 'Liabilities', concepts: [] },
      { label: 'Current liabilities', concepts: ['CurrentLiabilities', 'NotesAndAccountsPayableTrade', 'AccountsPayableTrade', 'AccountsPayable', 'ShortTermBorrowings', 'ShortTermLoansPayable', 'CurrentPortionOfLongTermDebt', 'CommercialPapers', 'IncomeTaxesPayable', 'AccruedExpenses', 'DepositsReceived', 'ProvisionForBonuses', 'OtherCurrentLiabilities'] },
      { label: 'Non-current liabilities', concepts: ['NonCurrentLiabilities', 'LongTermBorrowings', 'LongTermLoansPayable', 'Bonds', 'CorporateBonds', 'ConvertibleBonds', 'DeferredTaxLiabilities', 'ProvisionForRetirementBenefits', 'ProvisionForDirectorsRetirementBenefits', 'AssetRetirementObligations', 'OtherNonCurrentLiabilities'] },
      { label: 'Total liabilities', concepts: ['TotalLiabilities', 'Liabilities'], isSubtotal: true },
      { label: 'Net assets', concepts: [] },
      { label: 'Shareholders equity', concepts: ['NetAssets', 'ShareholdersEquity', 'CapitalStock', 'ShareCapital', 'CapitalSurplus', 'AdditionalPaidInCapital', 'RetainedEarnings', 'TreasuryShares', 'TreasuryStock', 'AccumulatedOtherComprehensiveIncome', 'ValuationDifferenceOnAvailableForSaleSecurities', 'DeferredGainsOrLossesOnHedges', 'RevaluationReserve', 'ForeignCurrencyTranslationAdjustment', 'RemeasurementsOfDefinedBenefitPlans', 'StockAcquisitionRights', 'ShareOptions', 'NonControllingInterests', 'MinorityInterests', 'NetChangesOfItemsOtherThanShareholdersEquity', 'TotalChangesOfItemsDuringThePeriod', 'DisposalOfTreasuryStock', 'PurchaseOfTreasuryStock', 'RetirementOfTreasuryStock', 'DividendsFromSurplus', 'ReversalOfReserveForAdvancedDepreciationOfNoncurrentAssets'] },
      { label: 'Total net assets', concepts: ['TotalNetAssets', 'TotalEquity', 'Equity'], isSubtotal: true },
      { label: 'Total liabilities and net assets', concepts: ['TotalLiabilitiesAndNetAssets', 'TotalLiabilitiesAndEquity'], isSubtotal: true },
    ],
  },
  {
    name: 'Income Statement',
    sections: [
      { label: 'Revenue', concepts: ['NetSales', 'Revenue', 'OperatingRevenue', 'SalesRevenue', 'OtherRevenue'] },
      { label: 'Cost of sales', concepts: ['CostOfSales', 'CostOfGoodsSold', 'CostOfRevenue'] },
      { label: 'Gross profit', concepts: ['GrossProfit', 'GrossMargin'], isSubtotal: true },
      { label: 'Operating expenses', concepts: ['SellingGeneralAndAdministrativeExpenses', 'SGA', 'SellingExpenses', 'GeneralAndAdministrativeExpenses', 'SalariesAndWages', 'DepreciationAndAmortizationSGA', 'ResearchAndDevelopment', 'OtherOperatingExpenses'] },
      { label: 'Operating income', concepts: ['OperatingIncome', 'OperatingProfit', 'OperatingLoss', 'BusinessProfit'], isSubtotal: true },
      { label: 'Non-operating income / expenses', concepts: ['NonOperatingIncome', 'NonOperatingExpenses', 'InterestIncome', 'InterestExpense', 'DividendIncome', 'ForeignExchangeGains', 'ForeignExchangeLosses', 'GainOnSalesOfSecurities', 'LossOnSalesOfSecurities', 'EquityInEarningsOfAffiliates', 'OtherNonOperatingItems'] },
      { label: 'Ordinary income', concepts: ['OrdinaryIncome', 'OrdinaryProfit', 'OrdinaryLoss', 'IncomeBeforeIncomeTaxes'], isSubtotal: true },
      { label: 'Extraordinary items', concepts: ['ExtraordinaryIncome', 'ExtraordinaryLoss', 'ExtraordinaryItems', 'GainOnSalesOfFixedAssets', 'LossOnSalesOfFixedAssets', 'ImpairmentLoss', 'LossOnDisaster', 'RestructuringCharges'] },
      { label: 'Income before tax', concepts: ['IncomeBeforeTax', 'IncomeBeforeIncomeTaxesAndMinorityInterests'], isSubtotal: true },
      { label: 'Income taxes', concepts: ['IncomeTaxes', 'IncomeTaxExpense', 'IncomeTaxesCurrent', 'IncomeTaxesDeferred', 'CorporationTax', 'InhabitantTax', 'EnterpriseTax', 'AdjustmentForIncomeTaxes'] },
      { label: 'Net income', concepts: ['NetIncome', 'NetLoss', 'ProfitLoss', 'ProfitLossAttributableToOwnersOfParent', 'IncomeAttributableToOwnersOfParent'], isSubtotal: true },
      { label: 'Earnings per share', concepts: ['BasicEarningsPerShare', 'BasicEPS', 'DilutedEarningsPerShare', 'DilutedEPS', 'BasicEarningsLossPerShare', 'DilutedEarningsLossPerShare'] },
      { label: 'Key metrics (summary)', concepts: ['NumberOfEmployees', 'AverageNumberOfTemporaryWorkers', 'TotalNumberOfIssuedShares', 'NumberOfSharesIssuedSharesVotingRights', 'NumberOfSharesHeld', 'ShareholdingRatio', 'TotalShareholderReturn', 'TotalReturnOnSharePriceIndex'] },
    ],
  },
  {
    name: 'Cash Flow',
    sections: [
      { label: 'Operating activities', concepts: ['NetCashProvidedByUsedInOperatingActivities', 'OperatingActivities', 'CashFlowsFromOperatingActivities', 'IncomeBeforeIncomeTaxes', 'DepreciationAndAmortization', 'Depreciation', 'AmortizationOfGoodwill', 'ImpairmentLoss', 'InterestAndDividendsIncome', 'InterestExpense', 'ForeignExchangeLossesGains', 'DecreaseIncreaseInTradeReceivables', 'DecreaseIncreaseInInventories', 'IncreaseDecreaseInTradePayables', 'IncreaseDecreaseInAccruedExpenses', 'OtherOperatingCF', 'SubtotalOperatingCF'], isSubtotal: true },
      { label: 'Investing activities', concepts: ['NetCashProvidedByUsedInInvestingActivities', 'InvestingActivities', 'CashFlowsFromInvestingActivities', 'PurchaseOfPropertyPlantAndEquipment', 'ProceedsFromSalesOfPropertyPlantAndEquipment', 'PurchaseOfIntangibleAssets', 'PurchaseOfInvestmentSecurities', 'ProceedsFromSalesOfInvestmentSecurities', 'PaymentsForAcquisitions', 'OtherInvestingCF'] },
      { label: 'Financing activities', concepts: ['NetCashProvidedByUsedInFinancingActivities', 'FinancingActivities', 'CashFlowsFromFinancingActivities', 'ProceedsFromShortTermBorrowings', 'RepaymentsOfShortTermBorrowings', 'ProceedsFromLongTermBorrowings', 'RepaymentsOfLongTermBorrowings', 'ProceedsFromIssuanceOfBonds', 'RedemptionOfBonds', 'ProceedsFromIssuanceOfShares', 'DividendsPaid', 'CashDividendsPaid', 'OtherFinancingCF'] },
      { label: 'Net change in cash', concepts: ['NetIncreaseDecreaseInCashAndCashEquivalents', 'NetChangeInCash', 'EffectOfExchangeRateChangesOnCash'], isSubtotal: true },
      { label: 'Cash at beginning', concepts: ['CashAndCashEquivalentsAtBeginningOfPeriod', 'BeginningCash', 'CashAtBeginningOfPeriod', 'BeginningBalance'] },
      { label: 'Cash at end', concepts: ['CashAndCashEquivalentsAtEndOfPeriod', 'EndingCash', 'CashAtEndOfPeriod', 'EndingBalance'], isSubtotal: true },
    ],
  },
]

function matchConcept(concept: string, patterns: string[]): boolean {
  const upper = concept.toUpperCase()
  for (const p of patterns) {
    const pUpper = p.toUpperCase()
    // Match if concept contains the pattern, OR if the pattern starts the concept
    // (handles suffixes like SummaryOfBusinessResults)
    if (upper.includes(pUpper)) return true
    // Also match prefix: 'NetSales' matches 'NetSalesSummaryOfBusinessResults'
    if (upper.startsWith(pUpper)) return true
  }
  return false
}

// Also try conceptDisplay-based matching for common Japanese-labeled concepts
function conceptLabelToEnglish(label: string): string | null {
  const m: Record<string, string> = {
    'Net Sales Summary Of Business Results': 'NetSales',
    'Total Assets Summary Of Business Results': 'TotalAssets',
    'Ordinary Income Loss Summary Of Business Results': 'OrdinaryIncome',
    'Net Income Loss Summary Of Business Results': 'NetIncome',
    'Profit Loss Attributable To Owners Of Parent Summary Of Business Results': 'NetIncome',
    'Basic Earnings Loss Per Share Summary Of Business Results': 'BasicEPS',
    'Diluted Earnings Per Share Summary Of Business Results': 'DilutedEPS',
    'Equity To Asset Ratio Summary Of Business Results': 'Equity',
    'Net Assets Per Share Summary Of Business Results': 'NetAssets',
    'Net Assets Summary Of Business Results': 'NetAssets',
    'Rate Of Return On Equity Summary Of Business Results': 'ReturnOnEquity',
    'Price Earnings Ratio Summary Of Business Results': 'PERatio',
    'Cash And Cash Equivalents Summary Of Business Results': 'Cash',
    'Capital Stock Summary Of Business Results': 'ShareCapital',
    'Comprehensive Income Summary Of Business Results': 'ComprehensiveIncome',
    'Dividend Paid Per Share Summary Of Business Results': 'Dividends',
    'Interim Dividend Paid Per Share Summary Of Business Results': 'Dividends',
    'Payout Ratio Summary Of Business Results': 'PayoutRatio',
    'Net Cash Provided By Used In Operating Activities Summary Of Business Results': 'OperatingActivities',
    'Net Cash Provided By Used In Investing Activities Summary Of Business Results': 'InvestingActivities',
    'Net Cash Provided By Used In Financing Activities Summary Of Business Results': 'FinancingActivities',
  }
  return m[label] || null
}

function buildStatementTable(facts: Fact[], stmtDef: StmtDefinition): StmtRow[] {
  // Collect all facts into a concept-indexed map
  const factMap = new Map<string, Fact[]>()
  for (const f of facts) factMap.set(f.concept, [...(factMap.get(f.concept) || []), f])

  // Build rows following the statement structure
  const rows: StmtRow[] = []
  const usedConcepts = new Set<string>()

  for (const section of stmtDef.sections) {
    // Section header
    rows.push({ kind: 'section', label: section.label, indent: false, cells: new Map() })

    // Find matching facts
    let matchedCount = 0
    for (const [concept, factList] of factMap) {
      if (usedConcepts.has(concept)) continue
      if (section.concepts.length === 0) continue // Skip empty-concept sections (pure headers)
      if (matchConcept(concept, section.concepts)) {
        usedConcepts.add(concept)
        const row: StmtRow = { kind: section.isSubtotal ? 'subtotal' : 'row', label: conceptDisplay(concept), concept, indent: !section.isSubtotal, cells: new Map() }
        if (factList[0].concept_en) row.labelEn = factList[0].concept_en
        if (factList[0].unit_id) row.unit = factList[0].unit_id
        for (const f of factList) {
          const period = extractPeriod(f)
          row.cells.set(period, { period, value: f.numeric_value ?? null, fact: f })
        }
        rows.push(row)
        matchedCount++
      }
    }
  }

  // Remaining unmatched concepts go to "Other"
  const otherRows: StmtRow[] = []
  for (const [concept, factList] of factMap) {
    if (usedConcepts.has(concept)) continue
    const row: StmtRow = { kind: 'row', label: conceptDisplay(concept), concept, indent: true, cells: new Map() }
    if (factList[0].concept_en) row.labelEn = factList[0].concept_en
    if (factList[0].unit_id) row.unit = factList[0].unit_id
    for (const f of factList) {
      row.cells.set(extractPeriod(f), { period: extractPeriod(f), value: f.numeric_value ?? null, fact: f })
    }
    otherRows.push(row)
  }
  if (otherRows.length > 0) {
    rows.push({ kind: 'section', label: 'Other', indent: false, cells: new Map() })
    rows.push(...otherRows)
  }

  return rows
}

function collectPeriodsFromRows(stmts: Map<string, StmtRow[]>): string[] {
  const seen = new Set<string>()
  for (const rows of stmts.values()) {
    for (const row of rows) {
      for (const key of row.cells.keys()) seen.add(key)
    }
  }
  return Array.from(seen).sort()
}

export default function FilingViewerPage() {
  const { docId } = useParams<{ docId: string }>()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [tab, setTab] = useState<Tab>('original')
  const [selectedHtm, setSelectedHtm] = useState<string | null>(null)
  const [htmContent, setHtmContent] = useState('')
  const [htmCache, setHtmCache] = useState<Map<string, { jp: string; en: string }>>(new Map())
  const htmContentEn = selectedHtm ? htmCache.get(selectedHtm)?.en || '' : ''
  const [showEn, setShowEn] = useState(false)
  const [translatingHtm, setTranslatingHtm] = useState(false)
  const [htmError, setHtmError] = useState('')
  const [conceptFilter, setConceptFilter] = useState('')
  const [sideBySide, setSideBySide] = useState(true)

  const detail = useQuery({
    queryKey: ['filing', docId],
    enabled: Boolean(docId),
    queryFn: () => apiRequest<FilingDetail>(`/api/filings/${encodeURIComponent(docId ?? '')}`),
  })
  const facts = useQuery({
    queryKey: ['filing-facts-tr', docId],
    enabled: Boolean(docId),
    queryFn: () => apiRequest<{ facts: Fact[]; count: number }>(`/api/filings/${encodeURIComponent(docId ?? '')}/facts-translated${queryString({ limit: 3000 })}`),
  })
  const [translatingSections, setTranslatingSections] = useState<Set<string>>(new Set())
  const sections = useQuery({
    queryKey: ['filing-sections-tr', docId],
    enabled: Boolean(docId),
    queryFn: () => apiRequest<{ sections: Section[]; count: number }>(`/api/filings/${encodeURIComponent(docId ?? '')}/sections-translated${queryString({ limit: 500, bodies: 'true' })}`),
  })
  // When side-by-side is enabled, fetch body translations for sections that lack them
  const fetchBodyTranslation = async (sectionId: string, force = false) => {
    if (translatingSections.has(sectionId)) return
    setTranslatingSections(prev => new Set(prev).add(sectionId))
    try {
      const params = force ? { section_id: sectionId, force: 'true' } : { section_id: sectionId }
      const data = await apiRequest<{ section: Section }>(`/api/filings/${encodeURIComponent(docId ?? '')}/translate-body${queryString(params)}`)
      if (data.section?.text_en) {
        queryClient.setQueryData(['filing-sections-tr', docId], (old: { sections: Section[]; count: number } | undefined) => {
          if (!old) return old
          return { ...old, sections: old.sections.map(s => s.section_id === sectionId ? { ...s, text_en: data.section.text_en, title_en: data.section.title_en } : s) }
        })
      }
    } catch { /* ignore */ }
    setTranslatingSections(prev => { const next = new Set(prev); next.delete(sectionId); return next })
  }
  // Trigger body translation fetch when sideBySide is toggled on
  useEffect(() => {
    if (sideBySide && sections.data?.sections) {
      for (const s of sections.data.sections) {
        if (!s.text_en && s.text && s.text.length > 0) {
          void fetchBodyTranslation(s.section_id)
        }
      }
    }
  }, [sideBySide, sections.data])
  const taxonomy = useQuery({
    queryKey: ['filing-taxonomy', docId],
    enabled: Boolean(docId) && tab === 'taxonomy',
    queryFn: () => apiRequest<{ taxonomy: TaxonomyEntry[] }>(`/api/filings/${encodeURIComponent(docId ?? '')}/taxonomy`),
  })
  const quality = useQuery({
    queryKey: ['filing-quality', docId],
    enabled: Boolean(docId),
    queryFn: () => apiRequest<{ issues: QualityIssue[] }>(`/api/filings/${encodeURIComponent(docId ?? '')}/quality`),
  })
  const htmFiles = useQuery({
    queryKey: ['filing-htm', docId],
    enabled: Boolean(docId),
    queryFn: () => apiRequest<{ files: Array<{ artifact_id: string; member_path: string; label: string; filename: string; size_bytes: number }> }>(`/api/filings/${encodeURIComponent(docId ?? '')}/htm-files`),
  })

  const loadHtm = async (artifactId: string) => {
    setSelectedHtm(artifactId)
    setHtmError('')
    const cached = htmCache.get(artifactId)
    if (cached) {
      setHtmContent(cached.jp)
      if (cached.en) setShowEn(true)
      return
    }
    setHtmContent('')
    setShowEn(false)
    try {
      const data = await apiRequest<{ html: string }>(`/api/filings/${encodeURIComponent(docId ?? '')}/html/${encodeURIComponent(artifactId)}`)
      setHtmContent(data.html)
      setHtmCache(prev => new Map(prev).set(artifactId, { jp: data.html, en: '' }))
    } catch { setHtmContent('<p>Failed to load report</p>'); return }
    // Start English in background
    setTranslatingHtm(true)
    try {
      const enData = await apiRequest<{ html: string; html_en?: string }>(`/api/filings/${encodeURIComponent(docId ?? '')}/html/${encodeURIComponent(artifactId)}?translate=true`)
      if (enData.html_en) {
        setHtmCache(prev => { const m = new Map(prev); const entry = m.get(artifactId) || { jp: '', en: '' }; m.set(artifactId, { jp: entry.jp || htmContent, en: enData.html_en! }); return m })
        setShowEn(true)
        setHtmError('')
      } else { setHtmError('Translation returned empty') }
    } catch { setHtmError('Translation request failed') }
    setTranslatingHtm(false)
  }

  const toggleTranslation = async () => {
    if (!selectedHtm) return
    const currentEn = htmCache.get(selectedHtm)?.en
    if (currentEn) { setShowEn(!showEn); return }
    setTranslatingHtm(true)
    try {
      const enData = await apiRequest<{ html: string; html_en?: string }>(`/api/filings/${encodeURIComponent(docId ?? '')}/html/${encodeURIComponent(selectedHtm)}?translate=true`)
      if (enData.html_en) {
        setHtmCache(prev => { const m = new Map(prev); const entry = m.get(selectedHtm) || { jp: '', en: '' }; m.set(selectedHtm, { jp: entry.jp || htmContent, en: enData.html_en! }); return m })
        setShowEn(true)
        setHtmError('')
      } else { setHtmError('Translation returned empty') }
    } catch { setHtmError('Translation request failed') }
    setTranslatingHtm(false)
  }

  const filing = detail.data?.filing

  // Build EDGAR-style statement tables from facts
  const statements = useMemo(() => {
    if (!facts.data?.facts) return new Map<string, StmtRow[]>()
    const result = new Map<string, StmtRow[]>()
    for (const stmtDef of STATEMENTS) {
      const rows = buildStatementTable(facts.data.facts, stmtDef)
      if (rows.some(r => r.kind !== 'section')) result.set(stmtDef.name, rows)
    }
    return result
  }, [facts.data])
  const periods = useMemo(() => collectPeriodsFromRows(statements), [statements])

  const stmtOrder = ['Balance Sheet', 'Income Statement', 'Cash Flow']
  const orderedStatements = stmtOrder.filter(s => statements.has(s))
  for (const s of statements.keys()) { if (!orderedStatements.includes(s)) orderedStatements.push(s) }

  return (
    <div className="filing-viewer-page">
      <header className="filing-viewer-header">
        <button className="button button--ghost" onClick={() => navigate('/filings')}><ArrowLeft size={16} /> Back</button>
        <div className="filing-viewer-title">
          <h1>{filing?.submitter_name || filing?.edinet_code || docId}</h1>
          <div className="filing-meta">
            <span className="status-pill">{filing?.status ?? 'Loading'}</span>
            <span>{docId}</span>
            {filing?.edinet_code && <span>{filing.edinet_code}</span>}
            {filing?.form_code && <span>{filing.form_code}</span>}
            {filing?.period_end && <span>Period: {filing.period_end}</span>}
            {filing?.submitted_at && <span>Filed: {filing.submitted_at}</span>}
          </div>
        </div>
        {filing?.archive_sha256 && (
          <a className="button button--secondary" href={`/api/filings/${docId}/artifact`} download><Download size={14} /> ZIP</a>
        )}
      </header>

      <div className="tabs-bar">
        {TABS.map(t => (
          <button key={t.key} className={tab === t.key ? 'tab tab--active' : 'tab'} onClick={() => setTab(t.key)}>
            <t.icon size={14} /> {t.label}
          </button>
        ))}
      </div>

      <div className="filing-viewer-body">
        {detail.isLoading && <LoadingState label="Loading" />}

        {/* Original HTM report */}
        {tab === 'original' && (
          <div style={{ display: 'grid', gridTemplateColumns: '260px 1fr', gap: 20, paddingTop: 20, minHeight: '60vh' }}>
            <div className="card" style={{ maxHeight: '70vh', overflowY: 'auto' }}>
              <div className="card-header"><h2>Report files</h2></div>
              <div className="card-body">
                {htmFiles.isLoading ? <LoadingState label="Loading" /> : (
                  htmFiles.data?.files.map(f => (
                    <button
                      key={f.artifact_id}
                      className={selectedHtm === f.artifact_id ? 'outline-item' : 'outline-item'}
                      style={selectedHtm === f.artifact_id ? { background: 'var(--primary-soft)', fontWeight: 600 } : {}}
                      onClick={() => { void loadHtm(f.artifact_id) }}
                    >
                      <strong>{f.label}</strong>
                      <small style={{ display: 'block', fontSize: '.68rem', color: 'var(--muted)' }}>{(f.size_bytes / 1024).toFixed(0)} KB</small>
                    </button>
                  ))
                )}
                {htmFiles.data && !htmFiles.data.files.length && <EmptyState title="No HTML files" description="This archive has no inline XBRL report files." />}
              </div>
            </div>
            <div>
              {!selectedHtm ? (
                <EmptyState title="Select a report file" description="Click a file in the sidebar to view the original EDINET report." />
              ) : !htmContent ? (
                <LoadingState label="Loading report" />
              ) : (
                <div className="card">
                  <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <h2>{htmFiles.data?.files.find(f => f.artifact_id === selectedHtm)?.label || 'Report'}</h2>
                    <button
                      className={showEn ? 'button button--primary button--small' : 'button button--secondary button--small'}
                      disabled={translatingHtm || (!htmContentEn && showEn)}
                      onClick={() => toggleTranslation()}
                    >
                      {translatingHtm ? 'Translating with Argos…' : showEn ? 'Show 日本語' : htmContentEn ? 'Show English' : 'Translate to English'}
                    </button>
                    <button
                      className="button button--small button--ghost"
                      disabled={translatingHtm || !selectedHtm}
                      onClick={async () => {
                        if (!selectedHtm) return
                        setHtmCache(prev => { const m = new Map(prev); m.delete(selectedHtm); return m })
                        setTranslatingHtm(true)
                        setHtmError('')
                        try {
                          const enData = await apiRequest<{ html: string; html_en?: string }>(`/api/filings/${encodeURIComponent(docId ?? '')}/html/${encodeURIComponent(selectedHtm)}?translate=true&force=true`)
                          if (enData.html_en) {
                            setHtmCache(prev => { const m = new Map(prev); const entry = m.get(selectedHtm) || { jp: '', en: '' }; m.set(selectedHtm, { jp: entry.jp || htmContent, en: enData.html_en! }); return m })
                            setShowEn(true)
                          }
                        } catch { setHtmError('Refresh failed') }
                        setTranslatingHtm(false)
                      }}
                    >
                      ↻ Retranslate
                    </button>
                    {htmError && <small style={{ color: 'var(--danger)', marginLeft: 8 }}>{htmError}</small>}
                  </div>
                  <div className="card-body" style={{ padding: 0, position: 'relative' }}>
                    {translatingHtm && (
                      <div style={{ position: 'absolute', top: 8, right: 12, zIndex: 1, padding: '4px 10px', borderRadius: 6, background: 'var(--primary-soft)', color: 'var(--primary)', fontSize: '.78rem', fontWeight: 600 }}>
                        Translating with Argos…
                      </div>
                    )}
                    <iframe
                      srcDoc={showEn && htmContentEn ? htmContentEn : htmContent}
                      sandbox="allow-same-origin"
                      style={{ width: '100%', height: '75vh', border: '0', background: '#fff' }}
                      title="EDINET report"
                    />
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Document */}
        {tab === 'document' && (
          <div className="filing-document">
            {sections.isLoading ? <LoadingState label="Loading document" /> : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                {sections.data?.sections.map(s => (
                  <article key={s.section_id} className="filing-section">
                    <h2>{s.title || `Section ${s.ordinal}`}</h2>
                    {sideBySide && s.title_en && (
                      <h3 className="en-heading">{s.title_en}</h3>
                    )}
                    {sideBySide ? (
                      <div className="side-by-side">
                        <div className="side-panel jp-panel">
                          <span className="panel-label">日本語</span>
                          <p>{s.text}</p>
                        </div>
                        <div className="side-panel en-panel">
                          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
                            <span className="panel-label" style={{ marginBottom: 0 }}>English</span>
                            <button
                              className="text-button"
                              style={{ fontSize: '.7rem' }}
                              disabled={translatingSections.has(s.section_id)}
                              onClick={() => { void fetchBodyTranslation(s.section_id, true) }}
                            >
                              {translatingSections.has(s.section_id) ? 'Translating…' : 'Retranslate'}
                            </button>
                          </div>
                          {s.text_en ? (
                            <p>{s.text_en}</p>
                          ) : translatingSections.has(s.section_id) ? (
                            <p className="text-muted" style={{ fontStyle: 'italic' }}>Translating…</p>
                          ) : (
                            <p className="text-muted" style={{ fontStyle: 'italic', cursor: 'pointer', textDecoration: 'underline' }}
                               onClick={() => { void fetchBodyTranslation(s.section_id) }}>
                              Click to translate
                            </p>
                          )}
                        </div>
                      </div>
                    ) : (
                      <p>{s.text}</p>
                    )}
                  </article>
                ))}
              </div>
            )}
            {sections.data && !sections.data.sections.length && <EmptyState title="No narrative" />}
          </div>
        )}

        {/* Facts / Statements */}
        {tab === 'facts' && (
          <div>
            <div className="facts-toolbar">
              <input className="input" placeholder="Filter concepts…" value={conceptFilter} onChange={e => setConceptFilter(e.target.value)} />
              <span className="text-muted">{facts.data?.count ?? 0} facts</span>
              <label className="inline-toggle">
                <input type="checkbox" checked={sideBySide} onChange={e => setSideBySide(e.target.checked)} /> Side-by-side EN
              </label>
            </div>
            {facts.isLoading ? <LoadingState label="Loading facts" /> : !facts.data?.facts.length ? (
              <EmptyState title="No facts" />
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 32 }}>
                {orderedStatements.map(stmtName => {
                  const rows = statements.get(stmtName)!.filter(r => {
                    if (r.kind === 'section') return true
                    if (!conceptFilter) return true
                    return (r.concept || '').toLowerCase().includes(conceptFilter.toLowerCase())
                  })
                  // Hide statement if no data rows
                  if (!rows.some(r => r.kind !== 'section')) return null
                  return (
                    <div key={stmtName} className="statement-block">
                      <h2 className="statement-title">{stmtName}</h2>
                      <div className="table-scroll">
                        <table className="facts-table statement-table">
                          <thead>
                            <tr>
                              <th style={{ minWidth: 220 }}></th>
                              {sideBySide && <th style={{ minWidth: 180 }}>English</th>}
                              <th style={{ width: 60 }}>Unit</th>
                              {periods.map(p => <th key={p} className="num-col">{p}</th>)}
                            </tr>
                          </thead>
                          <tbody>
                            {rows.map((row, i) => {
                              if (row.kind === 'section') {
                                return (
                                  <tr key={`sec-${i}`} className="section-row">
                                    <td colSpan={sideBySide ? 3 + periods.length : 2 + periods.length}>
                                      <strong>{row.label}</strong>
                                    </td>
                                  </tr>
                                )
                              }
                              const isSub = row.kind === 'subtotal'
                              return (
                                <tr key={row.concept || i} className={isSub ? 'subtotal-row' : ''}>
                                  <td className={`concept-cell${row.indent ? ' indent' : ''}`}>
                                    {isSub ? <strong>{row.label}</strong> : row.label}
                                  </td>
                                  {sideBySide && <td className="en-cell">{isSub ? '' : (row.labelEn || '')}</td>}
                                  <td className="unit-cell">{row.unit || ''}</td>
                                  {periods.map(p => {
                                    const cell = row.cells.get(p)
                                    return (
                                      <td key={p} className="num-col">
                                        {cell?.value != null ? (isSub ? <strong>{fmtNum(cell.value)}</strong> : fmtNum(cell.value)) : '—'}
                                      </td>
                                    )
                                  })}
                                </tr>
                              )
                            })}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        )}

        {/* Audit */}
        {tab === 'audit' && (
          <div style={{ paddingTop: 20 }}>
            {detail.data?.artifacts.filter(a => a.member_path.includes('AuditDoc')).map(a => (
              <div key={a.artifact_id} style={{ display: 'flex', justifyContent: 'space-between', padding: '8px 0', borderBottom: '1px solid var(--border)' }}>
                <span>{a.member_path}</span>
                <small>{a.kind} · {a.size_bytes.toLocaleString()} bytes</small>
              </div>
            ))}
            {!detail.data?.artifacts.some(a => a.member_path.includes('AuditDoc')) && <EmptyState title="No audit reports" />}
          </div>
        )}

        {/* Taxonomy */}
        {tab === 'taxonomy' && (
          <div style={{ paddingTop: 20 }}>
            {taxonomy.isLoading ? <LoadingState label="Loading" /> : taxonomy.data?.taxonomy.length ? (
              <table className="facts-table"><thead><tr><th>Concept</th><th>Namespace</th></tr></thead><tbody>{taxonomy.data.taxonomy.map((t, i) => <tr key={i}><td>{t.concept}</td><td style={{ fontFamily: 'monospace', fontSize: '.72rem' }}>{t.namespace_uri || '—'}</td></tr>)}</tbody></table>
            ) : <EmptyState title="No taxonomy" />}
          </div>
        )}

        {/* Quality */}
        {tab === 'quality' && (
          <div style={{ paddingTop: 20 }}>
            {quality.isLoading ? <LoadingState label="Loading" /> : quality.data?.issues.length ? quality.data.issues.map(i => (
              <div key={i.issue_id} className="quality-row">
                <span className={`status-pill ${SEVERITY_CLASS[i.severity] ?? ''}`}>{i.severity}</span>
                <strong>{i.code}</strong>
                <small>{i.message}</small>
              </div>
            )) : <p className="text-muted">No quality issues detected.</p>}
          </div>
        )}
      </div>
    </div>
  )
}
