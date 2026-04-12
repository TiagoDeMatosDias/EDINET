"""Screening page: filter and view corporate financial data."""

import logging
import tkinter as tk
from datetime import datetime
from tkinter import ttk, filedialog, simpledialog, messagebox

from ui_tk import controllers as ctrl
from ui_tk.shared.widgets import (
    DatabasePickerEntry,
    EmptyState,
    PageHeader,
    RoundedButton,
    SearchableCombobox,
    ScrollableFrame,
    SectionCard,
    reapply_widget_tree,
)
from ui_tk.style import COLORS, FONT_SMALL, FONT_UI, FONT_UI_BOLD, PAD
from ui_tk.utils import run_in_background

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_OPERATORS = [">", ">=", "<", "<=", "=", "!=", "BETWEEN"]
_COMPARISON_MODE_LABELS = {
    "Fixed Value": "fixed",
    "Compare to Column": "column",
}
_RANKING_ALGORITHM_LABELS = {
    "None": "none",
    "Weighted Min-Max": "weighted_minmax",
    "Weighted Percentile": "weighted_percentile",
}
_RANKING_DIRECTION_LABELS = {
    "Higher is Better": "higher",
    "Lower is Better": "lower",
}

# Left panel fixed width
_LEFT_WIDTH = 340


class ScreeningPage(ttk.Frame):
    """Screening view for filtering companies by financial criteria."""

    def __init__(self, parent, app=None, **kw):
        super().__init__(parent, **kw)
        self.app = app

        # ── State ───────────────────────────────────────────────────────
        self._db_path: str = ""
        self._available_metrics: dict[str, list[str]] = {}
        self._available_periods: list[str] = []
        self._criteria_rows: list[dict] = []
        self._ranking_rows: list[dict] = []
        self._results_df = None
        self._result_records_by_item: dict[str, dict] = {}
        self._sort_column: str | None = None
        self._sort_ascending: bool = True
        self._display_columns: dict[str, tk.BooleanVar] = {}
        self._status_var = tk.StringVar(value="Select a database and add criteria to start screening")
        self._builder_summary_var = tk.StringVar(value="0 criteria • 0 ranking rules")
        self._column_summary_var = tk.StringVar(value="Default columns only")
        self._results_summary_var = tk.StringVar(value="No screening results yet")
        self._query_summary_var = tk.StringVar(value="No active filters")
        self._sort_summary_var = tk.StringVar(value="Sort: none")
        self._empty_results = None

        # ── Layout ──────────────────────────────────────────────────────
        self._build_toolbar()

        # Main body: left panel + right panel using grid for precise control
        self._body = ttk.Frame(self)
        self._body.pack(fill="both", expand=True)
        self._body.grid_columnconfigure(0, weight=0, minsize=_LEFT_WIDTH)
        self._body.grid_columnconfigure(1, weight=1)
        self._body.grid_rowconfigure(0, weight=1)

        self._build_left_panel()
        self._build_right_panel()

    # ── Toolbar ─────────────────────────────────────────────────────────

    def _build_toolbar(self):
        self._header = PageHeader(
            self,
            title="Screening",
            subtitle="Filter and rank companies based on financial metrics. Double-click results to open Security Analysis."
        )
        self._header.pack(fill="x", padx=PAD * 2, pady=(PAD * 2, PAD))

        toolbar = self._header.actions
        self._export_btn = RoundedButton(
            toolbar, text="Export CSV", style="Ghost.TButton",
            command=self._export_results,
        )
        self._export_btn.pack(side="right")

        self._export_backtest_btn = RoundedButton(
            toolbar, text="Backtest CSV", style="Ghost.TButton",
            command=self._export_backtest_results,
        )
        self._export_backtest_btn.pack(side="right", padx=(0, 6))

        self._history_btn = RoundedButton(
            toolbar, text="History", style="Ghost.TButton",
            command=self._show_history,
        )
        self._history_btn.pack(side="right", padx=(0, 6))

        self._save_btn = RoundedButton(
            toolbar, text="Save", style="Ghost.TButton",
            command=self._save_screening,
        )
        self._save_btn.pack(side="right", padx=(0, 6))

        self._load_btn = RoundedButton(
            toolbar, text="Load", style="Ghost.TButton",
            command=self._load_screening,
        )
        self._load_btn.pack(side="right", padx=(0, 6))

        self._run_btn = RoundedButton(
            toolbar, text="Run Screening", style="Accent.TButton",
            command=self._run_screening,
        )
        self._run_btn.pack(side="right", padx=(0, 6))

    # ── Left Panel (criteria builder) ───────────────────────────────────

    def _build_left_panel(self):
        self._left = ttk.Frame(self._body, style="App.TFrame", width=_LEFT_WIDTH)
        self._left.grid(row=0, column=0, sticky="nsew")
        self._left.grid_propagate(False)
        self._left_scroll = ScrollableFrame(self._left, bg_key="surface", width=_LEFT_WIDTH)
        self._left_scroll.pack(fill="both", expand=True)
        inner = self._left_scroll.interior
        inner.configure(style="Surface.TFrame")

        data_card = SectionCard(inner, "Data Context", "Database scope and screening period.", style="Panel.TFrame")
        data_card.pack(fill="x", padx=PAD // 2, pady=(PAD // 2, PAD // 2))
        self._db_picker = DatabasePickerEntry(data_card.body, label="Database", label_style="Panel.TLabel")
        self._db_picker.pack(fill="x", pady=(0, PAD))
        self._db_picker._var.trace_add("write", lambda *_: self._on_db_changed())

        period_frame = ttk.Frame(data_card.body, style="Panel.TFrame")
        period_frame.pack(fill="x")
        ttk.Label(period_frame, text="Period", style="Panel.TLabel", font=FONT_SMALL).pack(anchor="w")
        self._period_var = tk.StringVar()
        self._period_combo = SearchableCombobox(period_frame, textvariable=self._period_var, width=8)
        self._period_combo.pack(fill="x", pady=(4, 0))

        criteria_card = SectionCard(inner, "Criteria Builder", "Compose filters against one or more tables.", style="Panel.TFrame")
        criteria_card.pack(fill="x", padx=PAD // 2, pady=(0, PAD // 2))
        ttk.Label(criteria_card.body, textvariable=self._builder_summary_var, style="Panel.TLabel", font=FONT_SMALL).pack(anchor="w", pady=(0, 6))
        self._criteria_frame = ttk.Frame(criteria_card.body, style="Panel.TFrame")
        self._criteria_frame.pack(fill="x")
        self._add_criterion_btn = RoundedButton(
            criteria_card.body,
            text="Add Criterion",
            style="Ghost.TButton",
            command=self._add_criterion,
        )
        self._add_criterion_btn.pack(fill="x", pady=(8, 0))

        ranking_card = SectionCard(inner, "Ranking", "Order results by weighted metrics when needed.", style="Panel.TFrame")
        ranking_card.pack(fill="x", padx=PAD // 2, pady=(0, PAD // 2))
        ranking_algo_row = ttk.Frame(ranking_card.body, style="Panel.TFrame")
        ranking_algo_row.pack(fill="x", pady=(0, PAD))
        ttk.Label(ranking_algo_row, text="Algorithm", style="Panel.TLabel", font=FONT_SMALL).pack(anchor="w")
        self._ranking_algorithm_var = tk.StringVar(value="None")
        self._ranking_algorithm_combo = SearchableCombobox(
            ranking_algo_row,
            textvariable=self._ranking_algorithm_var,
            values=list(_RANKING_ALGORITHM_LABELS.keys()),
            width=18,
        )
        self._ranking_algorithm_combo.pack(fill="x", pady=(4, 0))
        self._ranking_frame = ttk.Frame(ranking_card.body, style="Panel.TFrame")
        self._ranking_frame.pack(fill="x")
        self._add_ranking_btn = RoundedButton(
            ranking_card.body,
            text="Add Ranking Rule",
            style="Ghost.TButton",
            command=self._add_ranking_rule,
        )
        self._add_ranking_btn.pack(fill="x", pady=(8, 0))

        columns_card = SectionCard(inner, "Output Columns", "Control what appears in the result grid.", style="Panel.TFrame")
        columns_card.pack(fill="x", padx=PAD // 2, pady=(0, PAD))
        ttk.Label(columns_card.body, textvariable=self._column_summary_var, style="Panel.TLabel", font=FONT_SMALL).pack(anchor="w", pady=(0, 6))
        self._columns_frame = ttk.Frame(columns_card.body, style="Panel.TFrame")
        self._columns_frame.pack(fill="x")

    def _bind_left_scroll(self, _event=None):
        self._left_canvas.bind_all("<MouseWheel>", self._on_left_mousewheel)

    def _unbind_left_scroll(self, _event=None):
        self._left_canvas.unbind_all("<MouseWheel>")

    def _on_left_mousewheel(self, event):
        self._left_canvas.yview_scroll(-1 * (event.delta // 120), "units")

    # ── Right Panel (results) ───────────────────────────────────────────

    def _build_right_panel(self):
        self._right = ttk.Frame(self._body, style="App.TFrame")
        self._right.grid(row=0, column=1, sticky="nsew", padx=(2, 0))

        workbench = SectionCard(
            self._right,
            "Results Workbench",
            "Ranked output ready for export or deeper single-security analysis.",
            style="Panel.TFrame",
        )
        workbench.pack(fill="both", expand=True, padx=PAD // 2, pady=(PAD // 2, 0))

        summary = ttk.Frame(workbench.body, style="Panel.TFrame")
        summary.pack(fill="x", pady=(0, PAD))
        left = ttk.Frame(summary, style="Panel.TFrame")
        left.pack(side="left", fill="x", expand=True)
        ttk.Label(left, textvariable=self._results_summary_var, style="Panel.TLabel", font=FONT_UI_BOLD).pack(anchor="w")
        ttk.Label(left, textvariable=self._status_var, style="Panel.TLabel", font=FONT_SMALL).pack(anchor="w", pady=(2, 0))
        ttk.Label(left, textvariable=self._query_summary_var, style="Panel.TLabel", font=FONT_SMALL, wraplength=760, justify="left").pack(anchor="w", pady=(8, 0))
        ttk.Label(left, textvariable=self._sort_summary_var, style="Panel.TLabel", font=FONT_SMALL).pack(anchor="w", pady=(6, 0))

        self._table_area = ttk.Frame(workbench.body, style="Panel.TFrame")
        self._table_area.pack(fill="both", expand=True)

        self._empty_results = EmptyState(
            self._table_area,
            "No Results Yet",
            "Choose a database, add screening criteria, and run the screen. Result rows can be double-clicked to open Security Analysis.",
            style="Panel.TFrame",
        )
        self._empty_results.pack(fill="both", expand=True)

        tree_frame = ttk.Frame(self._table_area, style="Panel.TFrame")
        self._tree_frame = tree_frame
        self._tree = ttk.Treeview(tree_frame, show="headings")
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self._tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self._tree.xview)
        self._tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self._tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

        # Alternating row colours
        self._tree.tag_configure("even", background=COLORS["surface"])
        self._tree.tag_configure("odd", background=COLORS["surface_alt"])

        # Double-click for company detail stub
        self._tree.bind("<Double-1>", self._on_company_click)

        self._set_results_empty(True)

    def _set_results_empty(self, show: bool):
        if show:
            self._tree_frame.pack_forget()
            self._empty_results.pack(fill="both", expand=True)
        else:
            self._empty_results.pack_forget()
            self._tree_frame.pack(fill="both", expand=True)

    def _update_builder_summaries(self):
        criteria_count = len(self._criteria_rows)
        ranking_count = len(self._ranking_rows)
        self._builder_summary_var.set(
            f"{criteria_count} criter{'ion' if criteria_count == 1 else 'ia'} • {ranking_count} ranking rule{'s' if ranking_count != 1 else ''}"
        )

        selected_columns = sum(1 for var in self._display_columns.values() if var.get())
        self._column_summary_var.set(
            f"{selected_columns} explicit column{'s' if selected_columns != 1 else ''} selected"
            if selected_columns
            else "Default output columns only"
        )

        criteria = self._collect_criteria()
        ranking_rules = self._collect_ranking_rules()
        query_bits: list[str] = []
        if criteria:
            query_bits.extend(
                f"{crit['column']} {crit['operator']} {crit.get('value', crit.get('compare_column', '?'))}"
                for crit in criteria[:4]
            )
        if ranking_rules:
            query_bits.append(f"ranking={self._get_ranking_algorithm()}")
        if self._period_var.get().strip():
            query_bits.append(f"period={self._period_var.get().strip()}")
        self._query_summary_var.set(" • ".join(query_bits) if query_bits else "No active filters")

        if self.app is not None:
            self.app.set_context(
                "Screening",
                f"{criteria_count} criteria • {ranking_count} ranking rules • {self._results_summary_var.get()}",
            )

    # ── Database change handler ─────────────────────────────────────────

    def _on_db_changed(self):
        db = self._db_picker.get().strip()
        if not db or db == self._db_path:
            return
        self._db_path = db
        self._status_var.set("Loading screening metadata...")

        def _load():
            metrics = ctrl.screening_get_metrics(db)
            periods = ctrl.screening_get_periods(db)
            return metrics, periods

        def _on_done(result):
            metrics, periods = result
            self._available_metrics = metrics
            self._available_periods = periods

            # Update period combo
            self._period_combo["values"] = periods
            if periods:
                self._period_var.set(periods[-1])

            # Update criteria combos
            self._refresh_metric_options()

            # Update column checkboxes
            self._rebuild_column_checkboxes()
            self._update_builder_summaries()
            self._status_var.set(f"Loaded screening schema from {db}")
            self._results_summary_var.set("Ready to run screening")

            logger.info("Loaded metrics from %s: %d tables", db, len(metrics))

        def _on_error(exc):
            logger.error("Failed to load database: %s", exc)
            self._status_var.set(f"Failed to load database: {exc}")

        run_in_background(_load, on_done=_on_done, on_error=_on_error)

    def _get_table_names(self) -> list[str]:
        """Return sorted list of available screening table names."""
        return sorted(self._available_metrics.keys())

    def _get_columns_for_table(self, table: str) -> list[str]:
        """Return column names for the given table."""
        return self._available_metrics.get(table, [])

    def _refresh_metric_options(self):
        """Update existing criteria row table combo values."""
        tables = self._get_table_names()
        for row in self._criteria_rows:
            row["table_combo"].set_source_values(tables)
            row["target_table_combo"].set_source_values(tables)
        for row in self._ranking_rows:
            row["table_combo"].set_source_values(tables)

    # ── Criteria builder ────────────────────────────────────────────────

    def _add_criterion(self):
        row_frame = ttk.Frame(self._criteria_frame, style="PanelAlt.TFrame")
        row_frame.pack(fill="x", pady=(0, PAD // 2))

        tables = self._get_table_names()

        row_header = ttk.Frame(row_frame, style="PanelAlt.TFrame")
        row_header.pack(fill="x", padx=PAD, pady=(PAD, 0))
        ttk.Label(
            row_header,
            text=f"Criterion {len(self._criteria_rows) + 1}",
            style="PanelAlt.TLabel",
            font=FONT_UI_BOLD,
        ).pack(side="left")
        remove_btn = RoundedButton(
            row_header, text="✕", style="Danger.TButton",
            width=2,
        )
        remove_btn.pack(side="right")

        table_field = ttk.Frame(row_frame, style="PanelAlt.TFrame")
        table_field.pack(fill="x", padx=PAD, pady=(8, 0))
        ttk.Label(
            table_field,
            text="Source Table",
            style="PanelAlt.TLabel",
            font=FONT_SMALL,
        ).pack(anchor="w")

        table_var = tk.StringVar()
        table_combo = SearchableCombobox(
            table_field, textvariable=table_var,
            values=tables,
        )
        table_combo.pack(fill="x", pady=(4, 0))

        column_field = ttk.Frame(row_frame, style="PanelAlt.TFrame")
        column_field.pack(fill="x", padx=PAD, pady=(8, 0))
        ttk.Label(
            column_field,
            text="Source Metric",
            style="PanelAlt.TLabel",
            font=FONT_SMALL,
        ).pack(anchor="w")

        column_var = tk.StringVar()
        column_combo = SearchableCombobox(
            column_field, textvariable=column_var,
            values=[],
        )
        column_combo.pack(fill="x", pady=(4, 0))

        def _on_table_change(*_):
            tbl = table_var.get()
            cols = self._get_columns_for_table(tbl)
            column_combo.set_source_values(cols)
            if column_var.get() not in cols:
                column_var.set("")

        table_var.trace_add("write", _on_table_change)

        settings_row = ttk.Frame(row_frame, style="PanelAlt.TFrame")
        settings_row.pack(fill="x", padx=PAD, pady=(8, 0))

        comparison_mode_field = ttk.Frame(settings_row, style="PanelAlt.TFrame")
        comparison_mode_field.pack(side="left", fill="x", expand=True, padx=(0, 6))
        ttk.Label(
            comparison_mode_field,
            text="Comparison",
            style="PanelAlt.TLabel",
            font=FONT_SMALL,
        ).pack(anchor="w")

        comparison_mode_var = tk.StringVar(value="Fixed Value")
        comparison_mode_combo = SearchableCombobox(
            comparison_mode_field,
            textvariable=comparison_mode_var,
            values=list(_COMPARISON_MODE_LABELS.keys()),
        )
        comparison_mode_combo.pack(fill="x", pady=(4, 0))

        operator_field = ttk.Frame(settings_row, style="PanelAlt.TFrame")
        operator_field.pack(side="left", fill="x")
        ttk.Label(
            operator_field,
            text="Operator",
            style="PanelAlt.TLabel",
            font=FONT_SMALL,
        ).pack(anchor="w")

        op_var = tk.StringVar(value=">")
        op_combo = SearchableCombobox(
            operator_field, textvariable=op_var,
            values=_OPERATORS, width=10,
        )
        op_combo.pack(fill="x", pady=(4, 0))

        compare_row = ttk.Frame(row_frame, style="PanelAlt.TFrame")
        compare_row.pack(fill="x", padx=PAD, pady=(8, PAD))

        fixed_frame = ttk.Frame(compare_row, style="PanelAlt.TFrame")
        dynamic_frame = ttk.Frame(compare_row, style="PanelAlt.TFrame")

        ttk.Label(
            fixed_frame,
            text="Value",
            style="PanelAlt.TLabel",
            font=FONT_SMALL,
        ).pack(anchor="w")
        fixed_inputs = ttk.Frame(fixed_frame, style="PanelAlt.TFrame")
        fixed_inputs.pack(fill="x", pady=(4, 0))

        val_var = tk.StringVar()
        val_entry = ttk.Entry(fixed_inputs, textvariable=val_var)
        val_entry.pack(side="left", fill="x", expand=True, padx=(0, 4))

        val2_var = tk.StringVar()
        val2_entry = ttk.Entry(fixed_inputs, textvariable=val2_var)

        target_table_field = ttk.Frame(dynamic_frame, style="PanelAlt.TFrame")
        target_table_field.pack(fill="x")
        ttk.Label(
            target_table_field,
            text="Target Table",
            style="PanelAlt.TLabel",
            font=FONT_SMALL,
        ).pack(anchor="w")

        target_table_var = tk.StringVar()
        target_table_combo = SearchableCombobox(
            target_table_field,
            textvariable=target_table_var,
            values=tables,
        )
        target_table_combo.pack(fill="x", pady=(4, 0))

        target_column_field = ttk.Frame(dynamic_frame, style="PanelAlt.TFrame")
        target_column_field.pack(fill="x", pady=(8, 0))
        ttk.Label(
            target_column_field,
            text="Target Metric",
            style="PanelAlt.TLabel",
            font=FONT_SMALL,
        ).pack(anchor="w")

        target_column_var = tk.StringVar()
        target_column_combo = SearchableCombobox(
            target_column_field,
            textvariable=target_column_var,
            values=[],
        )
        target_column_combo.pack(fill="x", pady=(4, 0))

        def _on_target_table_change(*_):
            tbl = target_table_var.get()
            cols = self._get_columns_for_table(tbl)
            target_column_combo.set_source_values(cols)
            if target_column_var.get() not in cols:
                target_column_var.set("")

        target_table_var.trace_add("write", _on_target_table_change)

        def _on_op_change(*_):
            if (
                comparison_mode_var.get() == "Compare to Column"
                and op_var.get() == "BETWEEN"
            ):
                comparison_mode_var.set("Fixed Value")
            if op_var.get() == "BETWEEN":
                val2_entry.pack(side="left", fill="x", expand=True)
            else:
                val2_entry.pack_forget()

        op_var.trace_add("write", _on_op_change)

        def _on_mode_change(*_):
            if (
                comparison_mode_var.get() == "Compare to Column"
                and op_var.get() == "BETWEEN"
            ):
                op_var.set(">")
            fixed_frame.pack_forget()
            dynamic_frame.pack_forget()
            if comparison_mode_var.get() == "Compare to Column":
                dynamic_frame.pack(fill="x")
            else:
                fixed_frame.pack(fill="x")
                _on_op_change()

        comparison_mode_var.trace_add("write", _on_mode_change)
        _on_mode_change()

        row_data = {
            "frame": row_frame,
            "table_var": table_var,
            "table_combo": table_combo,
            "column_var": column_var,
            "column_combo": column_combo,
            "comparison_mode_var": comparison_mode_var,
            "comparison_mode_combo": comparison_mode_combo,
            "op_var": op_var,
            "val_var": val_var,
            "val2_var": val2_var,
            "val2_entry": val2_entry,
            "target_table_var": target_table_var,
            "target_table_combo": target_table_combo,
            "target_column_var": target_column_var,
            "target_column_combo": target_column_combo,
            "remove_btn": remove_btn,
        }

        remove_btn.configure(command=lambda: self._remove_criterion(row_data))
        self._criteria_rows.append(row_data)
        self._update_builder_summaries()

    def _remove_criterion(self, row_data):
        row_data["frame"].destroy()
        self._criteria_rows.remove(row_data)
        self._update_builder_summaries()

    def _add_ranking_rule(self):
        row_frame = ttk.Frame(self._ranking_frame, style="PanelAlt.TFrame")
        row_frame.pack(fill="x", pady=(0, PAD // 2))

        tables = self._get_table_names()

        row_header = ttk.Frame(row_frame, style="PanelAlt.TFrame")
        row_header.pack(fill="x", padx=PAD, pady=(PAD, 0))
        ttk.Label(
            row_header,
            text=f"Ranking Rule {len(self._ranking_rows) + 1}",
            style="PanelAlt.TLabel",
            font=FONT_UI_BOLD,
        ).pack(side="left")
        remove_btn = RoundedButton(
            row_header, text="✕", style="Danger.TButton", width=2,
        )
        remove_btn.pack(side="right")

        table_field = ttk.Frame(row_frame, style="PanelAlt.TFrame")
        table_field.pack(fill="x", padx=PAD, pady=(8, 0))
        ttk.Label(
            table_field,
            text="Ranking Table",
            style="PanelAlt.TLabel",
            font=FONT_SMALL,
        ).pack(anchor="w")

        table_var = tk.StringVar()
        table_combo = SearchableCombobox(
            table_field, textvariable=table_var, values=tables,
        )
        table_combo.pack(fill="x", pady=(4, 0))

        column_field = ttk.Frame(row_frame, style="PanelAlt.TFrame")
        column_field.pack(fill="x", padx=PAD, pady=(8, 0))
        ttk.Label(
            column_field,
            text="Ranking Metric",
            style="PanelAlt.TLabel",
            font=FONT_SMALL,
        ).pack(anchor="w")

        column_var = tk.StringVar()
        column_combo = SearchableCombobox(
            column_field, textvariable=column_var, values=[],
        )
        column_combo.pack(fill="x", pady=(4, 0))

        def _on_table_change(*_):
            cols = self._get_columns_for_table(table_var.get())
            column_combo.set_source_values(cols)
            if column_var.get() not in cols:
                column_var.set("")

        table_var.trace_add("write", _on_table_change)

        bottom_row = ttk.Frame(row_frame, style="PanelAlt.TFrame")
        bottom_row.pack(fill="x", padx=PAD, pady=(8, PAD))

        direction_field = ttk.Frame(bottom_row, style="PanelAlt.TFrame")
        direction_field.pack(side="left", fill="x", expand=True, padx=(0, 6))
        ttk.Label(
            direction_field,
            text="Direction",
            style="PanelAlt.TLabel",
            font=FONT_SMALL,
        ).pack(anchor="w")

        direction_var = tk.StringVar(value="Higher is Better")
        direction_combo = SearchableCombobox(
            direction_field,
            textvariable=direction_var,
            values=list(_RANKING_DIRECTION_LABELS.keys()),
        )
        direction_combo.pack(fill="x", pady=(4, 0))

        weight_field = ttk.Frame(bottom_row, style="PanelAlt.TFrame")
        weight_field.pack(side="left", fill="x")
        ttk.Label(
            weight_field,
            text="Weight",
            style="PanelAlt.TLabel",
            font=FONT_SMALL,
        ).pack(anchor="w")

        weight_var = tk.StringVar(value="1.0")
        ttk.Entry(weight_field, textvariable=weight_var, width=10).pack(
            fill="x", pady=(4, 0)
        )

        row_data = {
            "frame": row_frame,
            "table_var": table_var,
            "table_combo": table_combo,
            "column_var": column_var,
            "column_combo": column_combo,
            "direction_var": direction_var,
            "weight_var": weight_var,
            "remove_btn": remove_btn,
        }
        remove_btn.configure(command=lambda: self._remove_ranking_rule(row_data))
        self._ranking_rows.append(row_data)
        self._update_builder_summaries()

    def _remove_ranking_rule(self, row_data):
        row_data["frame"].destroy()
        self._ranking_rows.remove(row_data)
        self._update_builder_summaries()

    # ── Column selector ─────────────────────────────────────────────────

    def _rebuild_column_checkboxes(self):
        for child in self._columns_frame.winfo_children():
            child.destroy()
        self._display_columns.clear()

        tables = self._get_table_names()
        if not tables:
            return

        # Table selector dropdown
        selector_frame = ttk.Frame(self._columns_frame, style="Panel.TFrame")
        selector_frame.pack(fill="x", pady=(2, 4))

        ttk.Label(selector_frame, text="Table:", style="Panel.TLabel").pack(
            side="left",
        )
        self._col_table_var = tk.StringVar()
        self._col_table_combo = SearchableCombobox(
            selector_frame, textvariable=self._col_table_var,
            values=tables,
        )
        self._col_table_combo.pack(side="left", fill="x", expand=True, padx=(4, 0))

        search_row = ttk.Frame(self._columns_frame, style="Panel.TFrame")
        search_row.pack(fill="x", pady=(0, 4))
        ttk.Label(search_row, text="Find:", style="Panel.TLabel").pack(
            side="left",
        )
        self._col_search_var = tk.StringVar()
        ttk.Entry(
            search_row, textvariable=self._col_search_var,
        ).pack(side="left", fill="x", expand=True, padx=(4, 0))

        # Container for column checkboxes (populated on table change)
        self._col_checks_frame = ttk.Frame(self._columns_frame, style="Panel.TFrame")
        self._col_checks_frame.pack(fill="x")

        # Pre-register BooleanVars for all columns across all tables
        for table, cols in self._available_metrics.items():
            for col in cols:
                key = f"{table}.{col}"
                var = tk.BooleanVar(value=False)
                var.trace_add("write", lambda *_: self._update_builder_summaries())
                self._display_columns[key] = var

        def _render_column_checks(*_):
            # Clear current checkboxes
            for child in self._col_checks_frame.winfo_children():
                child.destroy()
            tbl = self._col_table_var.get()
            cols = self._get_columns_for_table(tbl)
            search_text = self._col_search_var.get().strip().lower()
            for col in cols:
                if search_text and search_text not in col.lower():
                    continue
                key = f"{tbl}.{col}"
                var = self._display_columns.get(key)
                if var is None:
                    var = tk.BooleanVar(value=False)
                    self._display_columns[key] = var
                cb = ttk.Checkbutton(
                    self._col_checks_frame, text=col, variable=var,
                    style="Panel.TCheckbutton",
                )
                cb.pack(anchor="w")

        self._col_table_var.trace_add("write", _render_column_checks)
        self._col_search_var.trace_add("write", _render_column_checks)

        # Select first table by default
        if tables:
            self._col_table_var.set(tables[0])
        self._update_builder_summaries()

    # ── Collect criteria from UI ────────────────────────────────────────

    def _collect_criteria(self) -> list[dict]:
        criteria = []
        for row in self._criteria_rows:
            table = row["table_var"].get()
            column = row["column_var"].get()
            if not table or not column:
                continue

            op = row["op_var"].get()
            comparison_mode_label = row["comparison_mode_var"].get()
            comparison_mode = _COMPARISON_MODE_LABELS.get(
                comparison_mode_label, "fixed"
            )

            crit = {
                "table": table,
                "column": column,
                "operator": op,
                "comparison_mode": comparison_mode,
            }

            if comparison_mode == "column":
                compare_table = row["target_table_var"].get()
                compare_column = row["target_column_var"].get()
                if not compare_table or not compare_column:
                    continue
                crit["compare_table"] = compare_table
                crit["compare_column"] = compare_column
            else:
                raw_value = row["val_var"].get().strip()
                if not raw_value:
                    continue
                crit["value"] = self._coerce_criterion_value(table, raw_value)

            if comparison_mode == "fixed" and op == "BETWEEN":
                raw_value2 = row["val2_var"].get().strip()
                if not raw_value2:
                    continue
                crit["value2"] = self._coerce_criterion_value(table, raw_value2)

            criteria.append(crit)
        return criteria

    def _collect_ranking_rules(self) -> list[dict]:
        ranking_rules = []
        for row in self._ranking_rows:
            table = row["table_var"].get()
            column = row["column_var"].get()
            if not table or not column:
                continue
            try:
                weight = float(row["weight_var"].get().strip())
            except (TypeError, ValueError):
                continue
            if weight <= 0:
                continue
            direction = _RANKING_DIRECTION_LABELS.get(
                row["direction_var"].get(), "higher"
            )
            ranking_rules.append(
                {
                    "table": table,
                    "column": column,
                    "weight": weight,
                    "direction": direction,
                }
            )
        return ranking_rules

    def _get_ranking_algorithm(self) -> str:
        return _RANKING_ALGORITHM_LABELS.get(
            self._ranking_algorithm_var.get(), "none"
        )

    def _collect_columns(self) -> list[str]:
        from src.screening import get_default_columns

        cols = get_default_columns(self._available_metrics)
        for key, var in self._display_columns.items():
            if var.get() and key not in cols:
                cols.append(key)
        # Also include criteria columns
        for crit in self._collect_criteria():
            col_ref = f"{crit['table']}.{crit['column']}"
            if col_ref not in cols:
                cols.append(col_ref)
        for rule in self._collect_ranking_rules():
            col_ref = f"{rule['table']}.{rule['column']}"
            if col_ref not in cols:
                cols.append(col_ref)
        return cols

    def _coerce_criterion_value(self, table: str, raw_value: str):
        """Convert numeric inputs when appropriate, otherwise preserve text."""
        if table == "CompanyInfo":
            return raw_value

        try:
            if any(char in raw_value for char in (".", "e", "E")):
                return float(raw_value)
            return int(raw_value)
        except ValueError:
            return raw_value

    # ── Run screening ───────────────────────────────────────────────────

    def _run_screening(self):
        if not self._db_path:
            logger.warning("No database selected")
            return

        criteria = self._collect_criteria()
        columns = self._collect_columns()
        period = self._period_var.get() or None
        ranking_algorithm = self._get_ranking_algorithm()
        ranking_rules = self._collect_ranking_rules()

        self._status_var.set("Running screening...")
        self._results_summary_var.set("Running screening")
        self._run_btn.state(["disabled"])
        self._update_builder_summaries()

        def _do():
            return ctrl.screening_run(
                self._db_path,
                criteria,
                columns,
                period,
                self._sort_column,
                "ASC" if self._sort_ascending else "DESC",
                ranking_algorithm=ranking_algorithm,
                ranking_rules=ranking_rules,
            )

        def _on_done(df):
            self._results_df = df
            self._populate_results(df)
            count = len(df)
            self._results_summary_var.set(
                f"{count} {'company' if count == 1 else 'companies'} returned"
            )
            self._status_var.set(
                f"{count} {'company' if count == 1 else 'companies'} found"
            )
            self._run_btn.state(["!disabled"])
            self._update_builder_summaries()

            # Save history entry
            try:
                ctrl.screening_save_history({
                    "criteria": criteria,
                    "columns": columns,
                    "period": period,
                    "ranking_algorithm": ranking_algorithm,
                    "ranking_rules": ranking_rules,
                    "result_count": count,
                })
            except Exception:
                pass

        def _on_error(exc):
            logger.error("Screening failed: %s", exc)
            self._status_var.set(f"Error: {exc}")
            self._results_summary_var.set("Screening failed")
            self._run_btn.state(["!disabled"])

        run_in_background(_do, on_done=_on_done, on_error=_on_error)

    # ── Results display ─────────────────────────────────────────────────

    def _populate_results(self, df):
        """Clear and populate the Treeview with DataFrame contents."""
        from src.screening import format_financial_value

        # Clear
        self._tree.delete(*self._tree.get_children())
        self._result_records_by_item = {}

        if df is None or df.empty:
            self._tree["columns"] = ()
            self._set_results_empty(True)
            return

        self._set_results_empty(False)

        cols = list(df.columns)
        self._tree["columns"] = cols

        for col in cols:
            self._tree.heading(
                col, text=col,
                command=lambda c=col: self._sort_by_column(c),
            )
            self._tree.column(col, width=110, minwidth=60, stretch=True)

        for i, (_, row) in enumerate(df.iterrows()):
            values = []
            for col in cols:
                val = row[col]
                values.append(format_financial_value(val, col))
            tag = "even" if i % 2 == 0 else "odd"
            item_id = self._tree.insert("", "end", values=values, tags=(tag,))
            self._result_records_by_item[item_id] = row.to_dict()

        logger.info("Populated results table with %d rows, %d columns",
                     len(df), len(cols))

    @staticmethod
    def _coalesce_record_value(record: dict, keys: tuple[str, ...]) -> str:
        for key in keys:
            value = record.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return ""

    def _build_security_record(self, record: dict) -> dict | None:
        edinet_code = self._coalesce_record_value(
            record,
            ("edinet_code", "edinetCode", "EdinetCode"),
        )
        if not edinet_code:
            return None
        return {
            "edinet_code": edinet_code,
            "ticker": self._coalesce_record_value(
                record,
                ("ticker", "Ticker", "Company_Ticker", "company_ticker"),
            ),
            "company_name": self._coalesce_record_value(
                record,
                (
                    "company_name",
                    "Company_Name",
                    "CompanyName",
                    "Submitter Name",
                ),
            ),
            "industry": self._coalesce_record_value(
                record,
                ("industry", "Industry", "Company_Industry"),
            ),
            "market": self._coalesce_record_value(
                record,
                ("market", "Market", "Listed"),
            ),
        }

    def _sort_by_column(self, col: str):
        """Sort the Treeview by a column header click."""
        if self._results_df is None or self._results_df.empty:
            return

        if self._sort_column == col:
            self._sort_ascending = not self._sort_ascending
        else:
            self._sort_column = col
            self._sort_ascending = True

        try:
            df = self._results_df.sort_values(
                by=col, ascending=self._sort_ascending, na_position="last",
            )
            df = df.reset_index(drop=True)
            self._results_df = df
            self._populate_results(df)
            direction = "ascending" if self._sort_ascending else "descending"
            self._sort_summary_var.set(f"Sort: {col} ({direction})")
        except KeyError:
            pass

        # Update header text with sort indicator
        for c in self._tree["columns"]:
            indicator = ""
            if c == col:
                indicator = " ▲" if self._sort_ascending else " ▼"
            self._tree.heading(
                c, text=f"{c}{indicator}",
                command=lambda cc=c: self._sort_by_column(cc),
            )

    def _on_company_click(self, event):
        """Handle double-click on a result row."""
        item = self._tree.identify_row(event.y)
        if not item:
            return
        record = self._result_records_by_item.get(item)
        if not record:
            return
        security_record = self._build_security_record(record)
        if security_record is None:
            self._status_var.set("Selected result cannot be opened in Security Analysis")
            return
        if self.app and hasattr(self.app, "show_security_analysis"):
            self._status_var.set(
                f"Opening {security_record.get('company_name') or security_record.get('ticker') or security_record['edinet_code']}..."
            )
            self.app.show_security_analysis(security_record, db_path=self._db_path)
            return
        logger.warning("Screening row double-click ignored because the app context is unavailable")

    # ── Save / Load / History / Export ──────────────────────────────────

    def _save_screening(self):
        name = simpledialog.askstring(
            "Save Screening", "Enter a name for this screening:",
            parent=self,
        )
        if not name:
            return
        criteria = self._collect_criteria()
        columns = self._collect_columns()
        period = self._period_var.get() or None
        ranking_algorithm = self._get_ranking_algorithm()
        ranking_rules = self._collect_ranking_rules()

        try:
            ctrl.screening_save(
                name,
                criteria,
                columns,
                period,
                ranking_algorithm=ranking_algorithm,
                ranking_rules=ranking_rules,
            )
            logger.info("Saved screening '%s'", name)
        except Exception as exc:
            logger.error("Failed to save screening: %s", exc)

    def _load_screening(self):
        names = ctrl.screening_list()
        if not names:
            logger.info("No saved screenings found")
            return

        win = tk.Toplevel(self)
        win.title("Load Screening")
        win.geometry("320x400")
        win.configure(bg=COLORS["surface"])
        win.transient(self.winfo_toplevel())
        win.grab_set()

        ttk.Label(win, text="Saved Screenings:", style="Surface.TLabel").pack(
            anchor="w", padx=PAD, pady=(PAD, 4),
        )

        listbox = tk.Listbox(
            win, bg=COLORS["input_bg"], fg=COLORS["text"],
            font=FONT_UI, selectbackground=COLORS["highlight"],
            relief="flat", borderwidth=0,
        )
        listbox.pack(fill="both", expand=True, padx=PAD, pady=4)

        for name in names:
            listbox.insert("end", name)

        btn_frame = ttk.Frame(win, style="Surface.TFrame")
        btn_frame.pack(fill="x", padx=PAD, pady=PAD)

        def _do_load():
            sel = listbox.curselection()
            if not sel:
                return
            name = names[sel[0]]
            try:
                data = ctrl.screening_load(name)
                self._apply_loaded_screening(data)
                logger.info("Loaded screening '%s'", name)
            except Exception as exc:
                logger.error("Failed to load screening: %s", exc)
            win.destroy()

        def _do_delete():
            sel = listbox.curselection()
            if not sel:
                return
            name = names[sel[0]]
            try:
                ctrl.screening_delete(name)
                listbox.delete(sel[0])
                names.pop(sel[0])
                logger.info("Deleted screening '%s'", name)
            except Exception as exc:
                logger.error("Failed to delete screening: %s", exc)

        RoundedButton(
            btn_frame, text="Load", style="Accent.TButton",
            command=_do_load,
        ).pack(side="left", padx=(0, 4))
        RoundedButton(
            btn_frame, text="Delete", style="Danger.TButton",
            command=_do_delete,
        ).pack(side="left", padx=4)
        RoundedButton(
            btn_frame, text="Cancel", style="Ghost.TButton",
            command=win.destroy,
        ).pack(side="right")

    def _apply_loaded_screening(self, data: dict):
        """Apply loaded screening criteria to the UI."""
        # Clear existing criteria rows
        for row in list(self._criteria_rows):
            row["frame"].destroy()
        self._criteria_rows.clear()
        for row in list(self._ranking_rows):
            row["frame"].destroy()
        self._ranking_rows.clear()

        # Set period
        period = data.get("period")
        if period and period in self._available_periods:
            self._period_var.set(period)

        # Rebuild criteria rows
        for crit in data.get("criteria", []):
            self._add_criterion()
            row = self._criteria_rows[-1]
            row["table_var"].set(crit["table"])
            row["column_var"].set(crit["column"])
            comparison_mode = crit.get("comparison_mode", "fixed")
            comparison_label = next(
                (
                    label for label, value in _COMPARISON_MODE_LABELS.items()
                    if value == comparison_mode
                ),
                "Fixed Value",
            )
            row["comparison_mode_var"].set(comparison_label)
            row["op_var"].set(crit.get("operator", ">"))
            if comparison_mode == "column":
                row["target_table_var"].set(crit.get("compare_table", ""))
                row["target_column_var"].set(crit.get("compare_column", ""))
            else:
                row["val_var"].set(str(crit.get("value", "")))
            if comparison_mode == "fixed" and "value2" in crit:
                row["val2_var"].set(str(crit["value2"]))

        ranking_label = next(
            (
                label for label, value in _RANKING_ALGORITHM_LABELS.items()
                if value == data.get("ranking_algorithm", "none")
            ),
            "None",
        )
        self._ranking_algorithm_var.set(ranking_label)

        for rule in data.get("ranking_rules", []):
            self._add_ranking_rule()
            row = self._ranking_rows[-1]
            row["table_var"].set(rule.get("table", ""))
            row["column_var"].set(rule.get("column", ""))
            direction_label = next(
                (
                    label for label, value in _RANKING_DIRECTION_LABELS.items()
                    if value == rule.get("direction", "higher")
                ),
                "Higher is Better",
            )
            row["direction_var"].set(direction_label)
            row["weight_var"].set(str(rule.get("weight", 1.0)))

        # Set display columns
        for key, var in self._display_columns.items():
            var.set(key in data.get("columns", []))
        self._update_builder_summaries()

    def _show_history(self):
        history = ctrl.screening_load_history()
        if not history:
            logger.info("No screening history found")
            return

        win = tk.Toplevel(self)
        win.title("Screening History")
        win.geometry("500x400")
        win.configure(bg=COLORS["surface"])
        win.transient(self.winfo_toplevel())
        win.grab_set()

        ttk.Label(win, text="Past Screenings:", style="Surface.TLabel").pack(
            anchor="w", padx=PAD, pady=(PAD, 4),
        )

        tree = ttk.Treeview(
            win, columns=("time", "criteria", "period", "count"),
            show="headings", height=12,
        )
        tree.heading("time", text="Time")
        tree.heading("criteria", text="Criteria")
        tree.heading("period", text="Period")
        tree.heading("count", text="Results")
        tree.column("time", width=140)
        tree.column("criteria", width=180)
        tree.column("period", width=60)
        tree.column("count", width=60)
        tree.pack(fill="both", expand=True, padx=PAD, pady=4)

        for entry in history:
            ts = entry.get("timestamp", "?")
            crit_summary = ", ".join(
                f"{c.get('column', '?')} {c.get('operator', '?')} {c.get('value', '?')}"
                for c in entry.get("criteria", [])
            ) or "—"
            period = entry.get("period", "—") or "—"
            count = entry.get("result_count", "?")
            tree.insert("", "end", values=(ts, crit_summary, period, count))

        def _on_rerun():
            sel = tree.selection()
            if not sel:
                return
            idx = tree.index(sel[0])
            if 0 <= idx < len(history):
                self._apply_loaded_screening(history[idx])
                win.destroy()

        btn_frame = ttk.Frame(win, style="Surface.TFrame")
        btn_frame.pack(fill="x", padx=PAD, pady=PAD)
        RoundedButton(
            btn_frame, text="Re-run Selected", style="Accent.TButton",
            command=_on_rerun,
        ).pack(side="left")
        RoundedButton(
            btn_frame, text="Close", style="Ghost.TButton",
            command=win.destroy,
        ).pack(side="right")

    def _export_results(self):
        if self._results_df is None or self._results_df.empty:
            logger.warning("No results to export")
            return

        path = filedialog.asksaveasfilename(
            title="Export Screening Results",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            parent=self,
        )
        if not path:
            return

        try:
            result = ctrl.screening_export(self._results_df, path)
            logger.info("Exported results to %s", result)
        except Exception as exc:
            logger.error("Export failed: %s", exc)

    def _export_backtest_results(self):
        if not self._db_path:
            logger.warning("No database selected")
            return

        max_companies = simpledialog.askinteger(
            "Backtest Export",
            "Maximum companies to export per year:",
            parent=self,
            minvalue=1,
            initialvalue=25,
        )
        if max_companies is None:
            return

        historical = messagebox.askyesno(
            "Backtest Export",
            "Export a historical company list for every year in the database?",
            parent=self,
        )

        path = filedialog.asksaveasfilename(
            title="Export Backtest Company List",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            parent=self,
        )
        if not path:
            return

        criteria = self._collect_criteria()
        columns = self._collect_columns()
        period = self._period_var.get() or None
        ranking_algorithm = self._get_ranking_algorithm()
        ranking_rules = self._collect_ranking_rules()

        self._status_var.set("Exporting backtest company list...")

        def _do():
            return ctrl.screening_export_backtest(
                self._db_path,
                criteria,
                columns,
                path,
                period,
                max_companies,
                ranking_algorithm=ranking_algorithm,
                ranking_rules=ranking_rules,
                historical=historical,
            )

        def _on_done(result_path):
            self._status_var.set(f"Backtest export saved to {result_path}")
            logger.info("Exported backtest company list to %s", result_path)

        def _on_error(exc):
            self._status_var.set(f"Backtest export failed: {exc}")
            logger.error("Backtest export failed: %s", exc)

        run_in_background(_do, on_done=_on_done, on_error=_on_error)

    # ── Theme toggle support ────────────────────────────────────────────

    def reapply_colors(self):
        """Re-apply theme colours after a theme toggle."""
        t = COLORS
        self._tree.tag_configure("even", background=t["surface"])
        self._tree.tag_configure("odd", background=t["surface_alt"])
        if hasattr(self, "_left_scroll"):
            self._left_scroll.reapply_colors()

        for btn in (
            self._export_btn,
            self._export_backtest_btn,
            self._history_btn,
            self._save_btn,
            self._load_btn,
            self._add_criterion_btn,
            self._add_ranking_btn,
            self._run_btn,
        ):
            btn.reapply_colors()
        for row in self._criteria_rows:
            if "remove_btn" in row:
                row["remove_btn"].reapply_colors()
        for row in self._ranking_rows:
            if "remove_btn" in row:
                row["remove_btn"].reapply_colors()
