import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { ColumnDef } from '@tanstack/react-table';
import { ChevronDown, ChevronUp, Download, FlaskConical, Plus, Save, Trash2 } from 'lucide-react';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { apiPost, apiRequest, queryString } from '../../api/client';
import type { ScreeningResult } from '../../api/types';
import { DataTable } from '../../components/DataTable';
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback';
import { Card, Field, PageHeader } from '../../components/Page';
import { serializeComputedColumn, serializeCriterion } from './criterion-values';
import { CriterionEditor, ExpressionTokenList } from './ExpressionEditorDense';
import { newExpressionCriterion, normalizeCriterion } from './expression-model';

import type { ComputedColumn, Criterion, ExpressionToken, MetricCatalog, SavedScreen } from './types';
type ResultRow = Record<string, unknown>;
const DEFAULT_COLUMNS = ['CompanyInfo.EdinetCode', 'CompanyInfo.Company_Ticker', 'CompanyInfo.Company_Name', 'CompanyInfo.Company_Industry'];
const DRAFT_KEY = 'shade.screening.draft';
const RULES_COLLAPSED_KEY = 'shade.screening.rules-collapsed';
function readDraft(): SavedScreen | null {
    try {
        return JSON.parse(localStorage.getItem(DRAFT_KEY) ?? 'null') as SavedScreen | null;
    }
    catch {
        return null;
    }
}
function resultRows(result?: ScreeningResult): ResultRow[] {
    return result?.rows.map(row => Object.fromEntries(result.columns.map((column, index) => [column, row[index]]))) ?? [];
}
function ResultColumnPicker({ catalog, selected, onChange }: {
    catalog: MetricCatalog;
    selected: string[];
    onChange: (next: string[]) => void;
}) {
    const tables = Object.keys(catalog).sort((a, b) => a.localeCompare(b));
    const [table, setTable] = useState(tables.includes('CompanyInfo') ? 'CompanyInfo' : tables[0] ?? '');
    const [search, setSearch] = useState('');
    const columns = (catalog[table] ?? []).filter(column => column.toLowerCase().includes(search.toLowerCase()));
    return <div className="result-column-picker"><div className="column-picker-controls"><select className="select" value={table} onChange={event => { setTable(event.target.value); setSearch(''); }} aria-label="Result column table">{tables.map(item => <option key={item}>{item}</option>)}</select><input className="input" value={search} onChange={event => setSearch(event.target.value)} placeholder="Filter columns"/></div><div className="column-picker column-picker--dense">{columns.map(column => { const ref = `${table}.${column}`; return <label className="check" key={ref}><input type="checkbox" checked={selected.includes(ref)} onChange={event => onChange(event.target.checked ? [...selected, ref] : selected.filter(item => item !== ref))}/>{column}</label>; })}</div><div className="selected-columns"><strong>{selected.length} selected</strong>{selected.slice(0, 8).map(ref => <button key={ref} title="Remove" onClick={() => onChange(selected.filter(item => item !== ref))}>{ref.split('.').at(-1)}</button>)}{selected.length > 8 && <span>+{selected.length - 8}</span>}</div></div>;
}
function computedExpressionTokens(column: ComputedColumn): ExpressionToken[] {
    if (column.expression_tokens) return column.expression_tokens;
    if (column.formula_type === 'price_ratio' && column.numerator_table && column.numerator_column && column.denominator_table && column.denominator_column) {
        return [
            { type: 'column', table: column.numerator_table, column: column.numerator_column },
            { type: 'op', op: '/' },
            { type: 'column', table: column.denominator_table, column: column.denominator_column },
        ];
    }
    return [];
}
function defaultComputed(catalog: MetricCatalog): ComputedColumn {
    const table = catalog.ShareMetrics ? 'ShareMetrics' : Object.keys(catalog)[0] ?? '';
    return {
        name: 'Derived metric',
        formula_type: 'expression',
        expression_tokens: [
            { type: 'column', table: 'Stock_Prices', column: 'Price' },
            { type: 'op', op: '/' },
            { type: 'column', table, column: catalog[table]?.[0] ?? '' },
        ],
    };
}
export function DerivedColumns({ value, catalog, onChange }: {
    value: ComputedColumn[];
    catalog: MetricCatalog;
    onChange: (next: ComputedColumn[]) => void;
}) {
    const update = (index: number, patch: Partial<ComputedColumn>) => onChange(value.map((item, itemIndex) => itemIndex === index ? { ...item, ...patch } : item));
    const updateFormula = (index: number, expression_tokens: ExpressionToken[]) => update(index, { formula_type: 'expression', expression_tokens, formula: null });
    return <div className="derived-list">
        {value.map((column, index) => <div className="derived-column" key={`derived-${index}`}>
            <div className="derived-column-head">
                <input className="input" aria-label="Derived column name" value={column.name} onChange={event => update(index, { name: event.target.value })}/>
                <button className="icon-button" type="button" aria-label={`Remove derived column ${column.name}`} onClick={() => onChange(value.filter((_, itemIndex) => itemIndex !== index))}><Trash2 /></button>
            </div>
            <ExpressionTokenList label="Formula" value={computedExpressionTokens(column)} catalog={catalog} tagNames={[]} onChange={tokens => updateFormula(index, tokens)}/>
        </div>)}
        <button className="button button--ghost" type="button" onClick={() => onChange([...value, defaultComputed(catalog)])}><Plus />Derived field</button>
    </div>;
}
function buildColumns(result: ScreeningResult | undefined, navigate: ReturnType<typeof useNavigate>) {
    const columns: ColumnDef<ResultRow>[] = (result?.columns ?? []).map(column => ({ accessorKey: column, header: column.split('.').at(-1) ?? column, cell: info => String(info.getValue() ?? '—') }));
    columns.push({ id: 'action', header: '', cell: ({ row }) => { const code = row.original.EdinetCode ?? row.original['CompanyInfo.EdinetCode']; return <button className="button button--ghost" disabled={!code} onClick={() => navigate(`/analyze/${encodeURIComponent(String(code))}?from=screen`)}>Analyze</button>; } });
    return columns;
}
export default function ScreeningWorkspaceDense() {
    const draft = useMemo(() => readDraft(), []);
    const [criteria, setCriteria] = useState<Criterion[]>(() => (draft?.criteria?.length ? draft.criteria : [newExpressionCriterion()]).map(normalizeCriterion));
    const [columns, setColumns] = useState(draft?.columns?.length ? draft.columns : DEFAULT_COLUMNS);
    const [computed, setComputed] = useState<ComputedColumn[]>(draft?.computed_columns ?? []);
    const [screeningDate, setScreeningDate] = useState(draft?.screening_date ?? '');
    const [rankingAlgorithm, setRankingAlgorithm] = useState(draft?.ranking_algorithm ?? 'none');
    const [rankingRules, setRankingRules] = useState<Array<Record<string, unknown>>>(draft?.ranking_rules ?? []);
    const [saveName, setSaveName] = useState('');
    const [selectedSaved, setSelectedSaved] = useState('');
    const [result, setResult] = useState<ScreeningResult>();
    const [optionsTab, setOptionsTab] = useState<'columns' | 'derived'>('columns');
    const [rulesCollapsed, setRulesCollapsed] = useState(() => localStorage.getItem(RULES_COLLAPSED_KEY) === 'true');
    const navigate = useNavigate();
    const queryClient = useQueryClient();
    const db = useQuery({ queryKey: ['screening-db'], queryFn: () => apiRequest<{
            db_path: string;
        }>('/api/screening/db-path') });
    const metrics = useQuery({ queryKey: ['screening-metrics', db.data?.db_path], enabled: Boolean(db.data?.db_path), queryFn: () => apiRequest<{
            tables: MetricCatalog;
        }>(`/api/screening/metrics${queryString({ db_path: db.data!.db_path })}`) });
    const saved = useQuery({ queryKey: ['saved-screenings'], queryFn: () => apiRequest<{
            screenings: string[];
        }>('/api/screening/saved') });
    const tags = useQuery({
        queryKey: ['tags'],
        queryFn: () => apiRequest<{ tags: Array<{ name: string; member_count: number }> }>('/api/tags'),
    });
    const tagNames = useMemo(() => (tags.data?.tags ?? []).map(t => t.name), [tags.data]);
    const catalog = metrics.data?.tables ?? {};
    const payload = useCallback(() => ({ criteria: criteria.map(serializeCriterion), columns, computed_columns: computed.map(serializeComputedColumn), screening_date: screeningDate || null, ranking_algorithm: rankingAlgorithm, ranking_rules: rankingRules }), [criteria, columns, computed, screeningDate, rankingAlgorithm, rankingRules]);
    const run = useMutation({ mutationFn: () => apiPost<ScreeningResult>('/api/screening/run', { db_path: db.data!.db_path, ...payload(), sort_order: 'DESC' }), onSuccess: setResult });
    const save = useMutation({ mutationFn: () => apiPost('/api/screening/save', { name: saveName.trim(), ...payload() }), onSuccess: () => { setSelectedSaved(saveName.trim()); setSaveName(''); void queryClient.invalidateQueries({ queryKey: ['saved-screenings'] }); } });
    const removeSaved = useMutation({ mutationFn: () => apiRequest(`/api/screening/saved/${encodeURIComponent(selectedSaved)}`, { method: 'DELETE' }), onSuccess: () => { setSelectedSaved(''); void queryClient.invalidateQueries({ queryKey: ['saved-screenings'] }); } });
    useEffect(() => localStorage.setItem(DRAFT_KEY, JSON.stringify(payload())), [payload]);
    useEffect(() => localStorage.setItem(RULES_COLLAPSED_KEY, String(rulesCollapsed)), [rulesCollapsed]);
    const loadSaved = async (name: string) => { setSelectedSaved(name); if (!name)
        return; const data = await apiRequest<SavedScreen>(`/api/screening/saved/${encodeURIComponent(name)}`); setCriteria((data.criteria ?? []).map(normalizeCriterion)); setColumns(data.columns?.length ? data.columns : DEFAULT_COLUMNS); setComputed(data.computed_columns ?? []); setScreeningDate(data.screening_date ?? ''); setRankingAlgorithm(data.ranking_algorithm ?? 'none'); setRankingRules(data.ranking_rules ?? []); };
    const exportResults = async () => { if (!db.data)
        return; const response = await fetch('/api/screening/export', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ db_path: db.data.db_path, ...payload(), format: 'csv' }) }); if (!response.ok)
        throw new Error('Export failed'); const blob = await response.blob(); const link = document.createElement('a'); link.href = URL.createObjectURL(blob); link.download = 'screening.csv'; link.click(); URL.revokeObjectURL(link.href); };
    const deleteSavedScreen = () => { if (!selectedSaved || !window.confirm(`Delete saved screen "${selectedSaved}"?`))
        return; removeSaved.mutate(); };
    const tableColumns = useMemo(() => buildColumns(result, navigate), [result, navigate]);
    if (db.isLoading || metrics.isLoading)
        return <LoadingState label="Preparing screening data"/>;
    if (db.isError || metrics.isError)
        return <ErrorState error={db.error ?? metrics.error}/>;
    return <div className="stack dense-page screening-workspace screening-workspace--max"><PageHeader eyebrow="Company discovery" title="Screen companies" description="Build full expressions from table-first metric selectors." actions={<button className="button button--primary" onClick={() => run.mutate()} disabled={run.isPending}><FlaskConical />{run.isPending ? 'Running…' : 'Run screen'}</button>}/><div className="screen-toolbar"><Field label="Saved screen"><div className="inline-control"><select className="select" value={selectedSaved} onChange={event => void loadSaved(event.target.value)}><option value="">New screen</option>{saved.data?.screenings.map(name => <option key={name}>{name}</option>)}</select><button className="button button--danger" type="button" disabled={!selectedSaved || removeSaved.isPending} onClick={deleteSavedScreen}><Trash2 />Delete</button></div></Field><Field label="Save as"><div className="inline-control"><input className="input" value={saveName} onChange={event => setSaveName(event.target.value)} placeholder="Screen name"/><button className="button button--secondary" disabled={!saveName.trim()} onClick={() => save.mutate()}><Save />Save</button></div></Field><Field label="As-of date"><input className="input" type="date" value={screeningDate} onChange={event => setScreeningDate(event.target.value)}/></Field><span className="toolbar-summary">{criteria.length} rules · {columns.length} columns · {computed.length} derived</span></div><div className={'screen-builder-grid screen-builder-grid--dense' + (rulesCollapsed ? ' is-rules-collapsed' : '')}>
<Card title={'Rules (' + criteria.length + ')'} actions={<div className="button-row"><button className="button button--secondary rules-collapse" aria-expanded={!rulesCollapsed} aria-controls="screening-rules" onClick={() => setRulesCollapsed(value => !value)}>{rulesCollapsed ? <ChevronDown /> : <ChevronUp />}{rulesCollapsed ? 'Show rules' : 'Minimize'}</button>{!rulesCollapsed && <button className="button button--secondary" onClick={() => setCriteria(items => [...items, newExpressionCriterion()])}><Plus />Rule</button>}</div>}><div id="screening-rules" className="criteria-list">{criteria.map((criterion, index) => <CriterionEditor key={criterion.id} criterion={criterion} catalog={catalog} tagNames={tagNames} index={index} onChange={next => setCriteria(items => items.map((item, itemIndex) => itemIndex === index ? next : item))} onRemove={() => setCriteria(items => items.filter(item => item.id !== criterion.id))}/>)}</div></Card>
<div className="screen-side"><Card title="Screen output" actions={<div className="segmented"><button className={optionsTab === 'columns' ? 'active' : ''} onClick={() => setOptionsTab('columns')}>Columns ({columns.length})</button><button className={optionsTab === 'derived' ? 'active' : ''} onClick={() => setOptionsTab('derived')}>Derived ({computed.length})</button></div>}>{optionsTab === 'columns' ? <ResultColumnPicker catalog={catalog} selected={columns} onChange={setColumns}/> : <DerivedColumns value={computed} catalog={catalog} onChange={setComputed}/>}</Card></div>
</div><Card className="results-card results-card--large" title={result ? `${result.row_count.toLocaleString()} matching companies` : 'Results'} actions={result && <div className="button-row"><button className="button button--secondary" onClick={() => void exportResults()}><Download />CSV</button><button className="button button--primary" onClick={() => navigate('/backtest?source=screen')}><FlaskConical />Rolling backtest</button></div>}>{run.isPending ? <LoadingState label="Running screen"/> : run.isError ? <ErrorState error={run.error}/> : !result ? <EmptyState title="No results yet" description="Load or build a screen, then run it."/> : <DataTable data={resultRows(result)} columns={tableColumns} dense/>}</Card></div>;
}
