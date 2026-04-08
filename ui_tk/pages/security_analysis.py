"""Security Analysis page for single-company research and comparison."""

from __future__ import annotations

import logging
import os
import tkinter as tk
from tkinter import messagebox, simpledialog, ttk

import pandas as pd

from ui_tk import controllers as ctrl
from ui_tk.shared.widgets import DatabasePickerEntry, PageHeader, RoundedButton, SectionCard, StatTile, TabBar
from ui_tk.style import COLORS, FONT_SMALL, FONT_UI_BOLD, PAD
from ui_tk.utils import run_in_background

try:
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    from matplotlib.figure import Figure
except ImportError:  # pragma: no cover - exercised only when matplotlib missing
    FigureCanvasTkAgg = None
    Figure = None

logger = logging.getLogger(__name__)

_STATEMENT_KEY_MAP = {
    "Income Statement": "income_statement",
    "Balance Sheet": "balance_sheet",
    "Cashflow Statement": "cashflow_statement",
}

_CHART_METRIC_OPTIONS = [
    "Stock Price",
    "Revenue",
    "Operating Income",
    "Net Income",
    "Shareholders' Equity",
    "P/E",
    "P/B",
    "Dividend Yield",
    "ROE",
]

_CHART_TIMEFRAMES = ["1Y", "3Y", "5Y", "Max"]


def _is_missing(value) -> bool:
    try:
        return value is None or pd.isna(value)
    except TypeError:
        return value is None


def _safe_float(value) -> float | None:
    if _is_missing(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_str(value) -> str:
    if _is_missing(value):
        return ""
    return str(value).strip()


def _short_date(value) -> str:
    text = "" if value is None else str(value).strip()
    return text[:10] if text else "N/A"


def _format_short_number(value: float | None) -> str:
    if value is None:
        return "N/A"
    abs_value = abs(value)
    if abs_value >= 1_000_000_000_000:
        return f"{value / 1_000_000_000_000:.2f}T"
    if abs_value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:,.2f}" if abs_value < 100 else f"{value:,.0f}"


def _format_currency(value: float | None, prefix: str = "JPY") -> str:
    if value is None:
        return "N/A"
    return f"{prefix} {_format_short_number(value)}"


def _format_percent(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value * 100:.2f}%"


def _format_ratio(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:.2f}x"


def _format_statement_value(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value / 1_000_000:,.1f}"


def _one_year_return_from_history(price_history: list[dict]) -> float | None:
    if len(price_history) < 2:
        return None
    df = pd.DataFrame(price_history)
    if df.empty:
        return None
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["trade_date", "price"]).sort_values("trade_date")
    if len(df) < 2:
        return None
    latest = df.iloc[-1]
    target_date = latest["trade_date"] - pd.Timedelta(days=365)
    prior_df = df[df["trade_date"] <= target_date]
    if prior_df.empty:
        return None
    prior = prior_df.iloc[-1]
    if prior["price"] in (None, 0.0):
        return None
    return (float(latest["price"]) - float(prior["price"])) / float(prior["price"])


class SecurityAnalysisPage(ttk.Frame):
    """Interactive page for researching a single security."""

    def __init__(self, parent, app=None, **kw):
        super().__init__(parent, **kw)
        self.app = app

        # --- State ---
        self._db_path = ctrl.get_default_database_path()
        self._selected_security: dict | None = None
        self._overview: dict = {}
        self._ratios: dict = {}
        self._statements: dict = {}
        self._price_history: list[dict] = []
        self._peers: list[dict] = []
        self._manual_peer_codes: list[str] = []
        self._search_results: list[dict] = []
        self._search_after_id: str | None = None
        self._search_request_id: int = 0
        self._load_request_id: int = 0
        self._is_loading = False
        self._suspend_search = False
        self._price_history_request_id: int = 0
        self._peers_request_id: int = 0
        self._loading_price_history = False
        self._loading_peers = False
        self._price_history_loaded_for: str | None = None
        self._peers_loaded_for: str | None = None
        self._db_optimize_request_id: int = 0
        self._db_optimized_path: str | None = None
        self._active_tab: str = "Overview"

        # --- Tk variables ---
        self._status_var = tk.StringVar(value="Select a database and search for a security.")
        self._search_var = tk.StringVar()
        self._hero_company_name_var = tk.StringVar(value="No security selected")
        self._company_card_var = tk.StringVar(value="Search for a company to load its identity, ticker, EDINET code, and industry context.")
        self._market_card_var = tk.StringVar(value="Price data will appear here")
        self._valuation_card_var = tk.StringVar(value="Valuation data will appear here")
        self._overview_company_var = tk.StringVar(value="Select a security to load company details.")
        self._overview_meta_var = tk.StringVar(value="Data quality and metadata will appear here.")
        self._peer_summary_var = tk.StringVar(value="Peers load after a company is selected.")
        self._period_count_var = tk.StringVar(value="8")
        self._statement_kind_var = tk.StringVar(value="Income Statement")
        self._chart_metric_var = tk.StringVar(value="Stock Price")
        self._chart_range_var = tk.StringVar(value="5Y")
        self._chart_show_peers_var = tk.BooleanVar(value=True)

        # --- Layout ---
        self._build_toolbar()
        self._build_controls()
        self._build_summary_cards()
        self._build_tabs()

        if self._db_path:
            self._db_picker.set(self._db_path)
            self._status_var.set("Database loaded. Start typing to search.")
            self._start_database_optimization(self._db_path)

        self._search_var.trace_add("write", lambda *_: self._schedule_search())
        self._db_picker._var.trace_add("write", lambda *_: self._on_db_changed())

    # ------------------------------------------------------------------
    # Layout
    # ------------------------------------------------------------------

    def _build_toolbar(self):
        self._header = PageHeader(
            self,
            title="Security Analysis",
            subtitle="Search a company, inspect its current market snapshot, and drill into statements, charts, and peers.",
            context="The selected company becomes the anchor for the whole workspace.",
        )
        self._header.pack(fill="x", padx=PAD * 2, pady=(PAD * 2, PAD))

        toolbar = self._header.actions

        self._refresh_btn = RoundedButton(
            toolbar,
            text="Refresh",
            style="Ghost.TButton",
            command=self._refresh_selected_security,
        )
        self._refresh_btn.pack(side="right")

        self._update_price_btn = RoundedButton(
            toolbar,
            text="Update Price",
            style="Accent.TButton",
            command=self._update_selected_price,
        )
        self._update_price_btn.pack(side="right", padx=(0, 6))
        self._update_price_btn.state(["disabled"])
        self._refresh_btn.state(["disabled"])

    def _build_controls(self):
        controls = SectionCard(
            self,
            "Search & Context",
            "Keep database scope compact and let search dominate the workflow.",
            style="Panel.TFrame",
        )
        controls.pack(fill="x", padx=PAD * 2, pady=(0, PAD))
        controls.body.grid_columnconfigure(1, weight=1)

        self._db_picker = DatabasePickerEntry(
            controls.body,
            label="Database",
            value=self._db_path,
            label_style="Panel.TLabel",
        )
        self._db_picker.grid(row=0, column=0, sticky="ew", padx=(0, PAD), pady=(0, PAD))

        search_wrap = ttk.Frame(controls.body, style="Panel.TFrame")
        search_wrap.grid(row=0, column=1, sticky="nsew", pady=(0, PAD))
        search_wrap.grid_columnconfigure(0, weight=1)

        ttk.Label(search_wrap, text="Search Security", style="Panel.TLabel", font=FONT_SMALL).grid(row=0, column=0, sticky="w")
        self._search_entry = ttk.Entry(search_wrap, textvariable=self._search_var)
        self._search_entry.grid(row=1, column=0, sticky="ew", pady=(2, 0))
        self._search_entry.bind("<Down>", self._focus_suggestions)
        self._search_entry.bind("<Escape>", lambda _e: self._hide_suggestions())

        self._suggestions_frame = ttk.Frame(search_wrap, style="Panel.TFrame")
        self._suggestions_frame.grid(row=2, column=0, sticky="ew", pady=(4, 0))
        self._suggestions_frame.grid_columnconfigure(0, weight=1)

        self._suggestions_list = tk.Listbox(
            self._suggestions_frame,
            height=5,
            bg=COLORS["input_bg"],
            fg=COLORS["text"],
            selectbackground=COLORS["highlight"],
            selectforeground="#ffffff",
            relief="flat",
            borderwidth=1,
            highlightbackground=COLORS["border"],
            highlightthickness=1,
            activestyle="none",
        )
        self._suggestions_list.grid(row=0, column=0, sticky="ew")
        self._suggestions_list.bind("<Double-1>", self._on_suggestion_confirm)
        self._suggestions_list.bind("<Return>", self._on_suggestion_confirm)
        self._suggestions_list.bind("<Escape>", lambda _e: self._hide_suggestions())
        self._suggestions_frame.grid_remove()

        ttk.Label(controls.body, textvariable=self._status_var, style="Panel.TLabel", font=FONT_SMALL).grid(row=1, column=0, columnspan=2, sticky="w")

    def _build_summary_cards(self):
        summary = ttk.Frame(self, style="App.TFrame")
        summary.pack(fill="x", padx=PAD * 2, pady=(0, PAD))
        for col in range(3):
            summary.grid_columnconfigure(col, weight=1)

        self._company_card = SectionCard(summary, "Selected Company", "Identity and listing context.", style="Hero.TFrame")
        self._company_card.grid(row=0, column=0, sticky="nsew", padx=(0, PAD // 2))
        ttk.Label(self._company_card.body, textvariable=self._hero_company_name_var, style="Hero.TLabel", font=("Cascadia Mono", 16, "bold")).pack(anchor="w")
        ttk.Label(self._company_card.body, textvariable=self._company_card_var, style="Hero.TLabel", justify="left", wraplength=360).pack(anchor="w", pady=(6, 0))

        self._market_card = SectionCard(summary, "Market Snapshot", "Price action and freshness.", style="Panel.TFrame")
        self._market_card.grid(row=0, column=1, sticky="nsew", padx=PAD // 2)
        ttk.Label(self._market_card.body, textvariable=self._market_card_var, style="Panel.TLabel", justify="left", wraplength=300).pack(anchor="w")

        self._valuation_card = SectionCard(summary, "Valuation Snapshot", "Headline market multiples and yield.", style="Panel.TFrame")
        self._valuation_card.grid(row=0, column=2, sticky="nsew", padx=(PAD // 2, 0))
        ttk.Label(self._valuation_card.body, textvariable=self._valuation_card_var, style="Panel.TLabel", justify="left", wraplength=300).pack(anchor="w")

    def _make_summary_card(self, parent, title: str, text_var: tk.StringVar) -> ttk.Frame:
        frame = ttk.Frame(parent, style="Surface.TFrame")
        ttk.Label(frame, text=title, style="Surface.TLabel", font=FONT_UI_BOLD).pack(
            anchor="w", padx=PAD, pady=(PAD, 0)
        )
        ttk.Label(
            frame,
            textvariable=text_var,
            style="Surface.TLabel",
            justify="left",
            wraplength=280,
        ).pack(anchor="w", fill="x", padx=PAD, pady=(6, PAD))
        return frame

    def _build_tabs(self):
        tabs_wrap = ttk.Frame(self, style="App.TFrame")
        tabs_wrap.pack(fill="both", expand=True, padx=PAD * 2, pady=(0, PAD * 2))

        self._tab_container = ttk.Frame(tabs_wrap)
        self._tab_container.pack(fill="both", expand=True, pady=(PAD // 2, 0))

        self._tab_frames: dict[str, ttk.Frame] = {
            "Overview": ttk.Frame(self._tab_container, style="Surface.TFrame"),
            "Statements": ttk.Frame(self._tab_container, style="Surface.TFrame"),
            "Charts": ttk.Frame(self._tab_container, style="Surface.TFrame"),
            "Peers": ttk.Frame(self._tab_container, style="Surface.TFrame"),
        }

        self._build_overview_tab(self._tab_frames["Overview"])
        self._build_statements_tab(self._tab_frames["Statements"])
        self._build_charts_tab(self._tab_frames["Charts"])
        self._build_peers_tab(self._tab_frames["Peers"])

        self._tab_bar = TabBar(
            tabs_wrap,
            ["Overview", "Statements", "Charts", "Peers"],
            on_tab_changed=self._show_tab,
        )
        self._tab_bar.pack(fill="x", before=self._tab_container)
        self._show_tab("Overview")

    def _build_overview_tab(self, parent):
        parent.grid_columnconfigure(0, weight=1)
        parent.grid_columnconfigure(1, weight=1)
        parent.grid_rowconfigure(0, weight=1)
        parent.grid_rowconfigure(1, weight=1)

        profile = SectionCard(parent, "Company Profile", "Identity, listing details, and descriptive context.", style="Panel.TFrame")
        profile.grid(row=0, column=0, sticky="nsew", padx=(0, PAD // 2), pady=(0, PAD // 2))
        ttk.Label(
            profile.body,
            textvariable=self._overview_company_var,
            style="Panel.TLabel",
            justify="left",
            wraplength=420,
        ).pack(anchor="w", fill="x")

        fundamentals = SectionCard(parent, "Fundamentals", "Latest operating and balance sheet anchors.", style="Panel.TFrame")
        fundamentals.grid(row=0, column=1, sticky="nsew", padx=(PAD // 2, 0), pady=(0, PAD // 2))
        self._fundamentals_tree = self._build_key_value_tree(fundamentals.body)

        ratios = SectionCard(parent, "Ratios", "Valuation and quality metrics used for quick comparison.", style="Panel.TFrame")
        ratios.grid(row=1, column=0, sticky="nsew", padx=(0, PAD // 2), pady=(PAD // 2, 0))
        self._ratios_tree = self._build_key_value_tree(ratios.body)

        metadata = SectionCard(parent, "Metadata", "Freshness, source identifiers, and data quality notes.", style="Panel.TFrame")
        metadata.grid(row=1, column=1, sticky="nsew", padx=(PAD // 2, 0), pady=(PAD // 2, 0))
        ttk.Label(
            metadata.body,
            textvariable=self._overview_meta_var,
            style="Panel.TLabel",
            justify="left",
            wraplength=420,
        ).pack(anchor="w", fill="x")

    def _build_key_value_tree(self, parent) -> ttk.Treeview:
        frame = ttk.Frame(parent, style="Panel.TFrame")
        frame.pack(fill="both", expand=True)
        tree = ttk.Treeview(frame, columns=("metric", "value"), show="headings", height=7)
        tree.heading("metric", text="Metric", anchor="w")
        tree.heading("value", text="Value", anchor="w")
        tree.column("metric", width=180, anchor="w")
        tree.column("value", width=180, anchor="w")
        ybar = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=ybar.set)
        tree.pack(side="left", fill="both", expand=True)
        ybar.pack(side="right", fill="y")
        return tree

    def _build_statements_tab(self, parent):
        controls = SectionCard(parent, "Statement View", "Switch statement type and period count without leaving the data surface.", style="Panel.TFrame")
        controls.pack(fill="x", padx=PAD, pady=PAD)

        control_row = ttk.Frame(controls.body, style="Panel.TFrame")
        control_row.pack(fill="x")
        ttk.Label(control_row, text="Statement", style="Panel.TLabel").pack(side="left")
        statement_combo = ttk.Combobox(
            control_row,
            textvariable=self._statement_kind_var,
            values=list(_STATEMENT_KEY_MAP.keys()),
            state="readonly",
            width=18,
        )
        statement_combo.pack(side="left", padx=(6, PAD))
        statement_combo.bind("<<ComboboxSelected>>", lambda _e: self._render_statement_table())

        ttk.Label(control_row, text="Periods", style="Panel.TLabel").pack(side="left")
        period_combo = ttk.Combobox(
            control_row,
            textvariable=self._period_count_var,
            values=["4", "8", "12"],
            state="readonly",
            width=6,
        )
        period_combo.pack(side="left", padx=(6, 0))
        period_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_selected_security())

        tree_frame = ttk.Frame(parent, style="Panel.TFrame")
        tree_frame.pack(fill="both", expand=True, padx=PAD, pady=(0, PAD))
        self._statement_tree = ttk.Treeview(tree_frame, show="headings")
        ybar = ttk.Scrollbar(tree_frame, orient="vertical", command=self._statement_tree.yview)
        xbar = ttk.Scrollbar(tree_frame, orient="horizontal", command=self._statement_tree.xview)
        self._statement_tree.configure(yscrollcommand=ybar.set, xscrollcommand=xbar.set)
        self._statement_tree.grid(row=0, column=0, sticky="nsew")
        ybar.grid(row=0, column=1, sticky="ns")
        xbar.grid(row=1, column=0, sticky="ew")
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

    def _build_charts_tab(self, parent):
        controls = SectionCard(parent, "Chart Controls", "Adjust metric, range, and comparison overlay while keeping the chart dominant.", style="Panel.TFrame")
        controls.pack(fill="x", padx=PAD, pady=PAD)
        control_row = ttk.Frame(controls.body, style="Panel.TFrame")
        control_row.pack(fill="x")

        ttk.Label(control_row, text="Metric", style="Panel.TLabel").pack(side="left")
        metric_combo = ttk.Combobox(
            control_row,
            textvariable=self._chart_metric_var,
            values=_CHART_METRIC_OPTIONS,
            state="readonly",
            width=20,
        )
        metric_combo.pack(side="left", padx=(6, PAD))
        metric_combo.bind("<<ComboboxSelected>>", lambda _e: self._redraw_chart())

        ttk.Label(control_row, text="Range", style="Panel.TLabel").pack(side="left")
        range_combo = ttk.Combobox(
            control_row,
            textvariable=self._chart_range_var,
            values=_CHART_TIMEFRAMES,
            state="readonly",
            width=8,
        )
        range_combo.pack(side="left", padx=(6, PAD))
        range_combo.bind("<<ComboboxSelected>>", lambda _e: self._redraw_chart())

        ttk.Checkbutton(
            control_row,
            text="Show peers",
            variable=self._chart_show_peers_var,
            style="Panel.TCheckbutton",
            command=self._redraw_chart,
        ).pack(side="left")

        self._chart_frame = ttk.Frame(parent, style="Panel.TFrame")
        self._chart_frame.pack(fill="both", expand=True, padx=PAD, pady=(0, PAD))

        if FigureCanvasTkAgg and Figure:
            self._chart_figure = Figure(figsize=(7.2, 4.8), dpi=100)
            self._chart_canvas = FigureCanvasTkAgg(self._chart_figure, master=self._chart_frame)
            self._chart_canvas.get_tk_widget().pack(fill="both", expand=True)
            self._chart_empty_label = None
        else:
            self._chart_figure = None
            self._chart_canvas = None
            self._chart_empty_label = ttk.Label(
                self._chart_frame,
                text="matplotlib is not installed. Charts are unavailable.",
                style="Dim.TLabel",
            )
            self._chart_empty_label.pack(expand=True)

    def _build_peers_tab(self, parent):
        toolbar = SectionCard(parent, "Peer Comparison", "Compare the selected security against industry peers and manual additions.", style="Panel.TFrame")
        toolbar.pack(fill="x", padx=PAD, pady=PAD)
        top = ttk.Frame(toolbar.body, style="Panel.TFrame")
        top.pack(fill="x", pady=(0, PAD))
        ttk.Label(top, textvariable=self._peer_summary_var, style="Panel.TLabel", font=FONT_SMALL, wraplength=640, justify="left").pack(side="left", fill="x", expand=True)

        self._add_peer_btn = RoundedButton(
            top,
            text="Add Peer",
            style="Ghost.TButton",
            command=self._add_manual_peer,
        )
        self._add_peer_btn.pack(side="right", padx=2)
        self._add_peer_btn.state(["disabled"])

        self._reset_peers_btn = RoundedButton(
            top,
            text="Reset Peers",
            style="Ghost.TButton",
            command=self._reset_manual_peers,
        )
        self._reset_peers_btn.pack(side="right", padx=2)
        self._reset_peers_btn.state(["disabled"])

        table_frame = ttk.Frame(parent, style="Panel.TFrame")
        table_frame.pack(fill="both", expand=True, padx=PAD, pady=(0, PAD))
        cols = (
            "role",
            "company_name",
            "ticker",
            "PERatio",
            "PriceToBook",
            "ReturnOnEquity",
            "DividendsYield",
            "latest_price",
            "one_year_return",
        )
        self._peers_tree = ttk.Treeview(table_frame, columns=cols, show="headings")
        headings = {
            "role": "Role",
            "company_name": "Company",
            "ticker": "Ticker",
            "PERatio": "P/E",
            "PriceToBook": "P/B",
            "ReturnOnEquity": "ROE",
            "DividendsYield": "Div Yield",
            "latest_price": "Latest Price",
            "one_year_return": "1Y Return",
        }
        widths = {
            "role": 90,
            "company_name": 220,
            "ticker": 80,
            "PERatio": 80,
            "PriceToBook": 80,
            "ReturnOnEquity": 90,
            "DividendsYield": 90,
            "latest_price": 110,
            "one_year_return": 90,
        }
        for col in cols:
            self._peers_tree.heading(col, text=headings[col], anchor="w")
            self._peers_tree.column(col, width=widths[col], anchor="w", stretch=(col == "company_name"))
        ybar = ttk.Scrollbar(table_frame, orient="vertical", command=self._peers_tree.yview)
        xbar = ttk.Scrollbar(table_frame, orient="horizontal", command=self._peers_tree.xview)
        self._peers_tree.configure(yscrollcommand=ybar.set, xscrollcommand=xbar.set)
        self._peers_tree.grid(row=0, column=0, sticky="nsew")
        ybar.grid(row=0, column=1, sticky="ns")
        xbar.grid(row=1, column=0, sticky="ew")
        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)

    # ------------------------------------------------------------------
    # Search and selection
    # ------------------------------------------------------------------

    def _on_db_changed(self):
        new_path = self._db_picker.get().strip()
        if new_path == self._db_path:
            return
        self._db_path = new_path
        self._db_optimized_path = None
        if self._db_path:
            ctrl.remember_database_path(self._db_path)
            self._status_var.set("Database updated. Start typing to search.")
            self._start_database_optimization(self._db_path)
        else:
            self._status_var.set("Select a database to begin.")
        self._selected_security = None
        self._manual_peer_codes = []
        self._search_results = []
        self._hide_suggestions()
        self._clear_rendered_data()

    def _start_database_optimization(self, db_path: str):
        clean_db_path = _safe_str(db_path)
        if not clean_db_path or clean_db_path == self._db_optimized_path:
            return

        self._db_optimize_request_id += 1
        request_id = self._db_optimize_request_id

        def _load():
            return ctrl.security_optimize_database(clean_db_path)

        def _on_done(_result):
            if request_id != self._db_optimize_request_id:
                return
            self._db_optimized_path = clean_db_path
            logger.info("Security Analysis database optimization completed for %s", clean_db_path)

        def _on_error(exc):
            if request_id != self._db_optimize_request_id:
                return
            logger.warning("Security Analysis database optimization failed: %s", exc)

        run_in_background(_load, on_done=_on_done, on_error=_on_error)

    def _schedule_search(self):
        if self._suspend_search:
            return
        if self._search_after_id:
            self.after_cancel(self._search_after_id)
            self._search_after_id = None
        query = self._search_var.get().strip()
        if not self._db_path or not query:
            self._hide_suggestions()
            return
        self._search_after_id = self.after(250, self._run_search)

    def _run_search(self):
        self._search_after_id = None
        query = self._search_var.get().strip()
        if not self._db_path or not query:
            self._hide_suggestions()
            return

        request_id = self._search_request_id + 1
        self._search_request_id = request_id

        def _load():
            return ctrl.security_search(self._db_path, query, limit=25)

        def _on_done(results):
            if request_id != self._search_request_id:
                return
            self._search_results = results
            self._render_search_results(results)

        def _on_error(exc):
            if request_id != self._search_request_id:
                return
            logger.error("Security search failed: %s", exc, exc_info=True)
            self._status_var.set(f"Search failed: {exc}")
            self._hide_suggestions()

        run_in_background(_load, on_done=_on_done, on_error=_on_error)

    def _render_search_results(self, results: list[dict]):
        self._suggestions_list.delete(0, "end")
        if not results:
            self._hide_suggestions()
            return
        for record in results:
            parts = [
                _safe_str(record.get("ticker")),
                _safe_str(record.get("company_name")),
                _safe_str(record.get("industry")),
            ]
            if record.get("latest_price") is not None:
                parts.append(_format_currency(_safe_float(record.get("latest_price"))))
            line = "  ".join(part for part in parts if part)
            self._suggestions_list.insert("end", line.strip())
        self._suggestions_list.selection_clear(0, "end")
        self._suggestions_list.selection_set(0)
        self._suggestions_frame.grid()

    def _hide_suggestions(self):
        self._suggestions_frame.grid_remove()

    def _focus_suggestions(self, _event=None):
        if self._search_results:
            self._suggestions_list.focus_set()
            self._suggestions_list.selection_clear(0, "end")
            self._suggestions_list.selection_set(0)
        return "break"

    def _on_suggestion_confirm(self, _event=None):
        selection = self._suggestions_list.curselection()
        if not selection:
            return "break"
        index = int(selection[0])
        if not (0 <= index < len(self._search_results)):
            return "break"
        self._select_security(self._search_results[index])
        return "break"

    def open_security(self, record: dict, db_path: str | None = None):
        """Select and load a security from another view."""
        clean_db_path = _safe_str(db_path)
        if clean_db_path and clean_db_path != self._db_path:
            self._db_picker.set(clean_db_path)

        normalized = {
            "edinet_code": _safe_str(
                record.get("edinet_code")
                or record.get("edinetCode")
                or record.get("EdinetCode")
            ),
            "ticker": _safe_str(
                record.get("ticker")
                or record.get("Company_Ticker")
                or record.get("company_ticker")
                or record.get("Ticker")
            ),
            "company_name": _safe_str(
                record.get("company_name")
                or record.get("Company_Name")
                or record.get("Submitter Name")
            ),
            "industry": _safe_str(
                record.get("industry")
                or record.get("Company_Industry")
                or record.get("Industry")
            ),
            "market": _safe_str(
                record.get("market")
                or record.get("Listed")
                or record.get("Market")
            ),
        }
        if not normalized["edinet_code"]:
            raise ValueError("Selected record is missing an EDINET code")
        self._select_security(normalized)

    def _select_security(self, record: dict):
        self._selected_security = record
        self._suspend_search = True
        try:
            self._search_var.set(record.get("company_name") or record.get("ticker") or "")
        finally:
            self._suspend_search = False
        self._manual_peer_codes = []
        self._hide_suggestions()
        self._load_selected_security()

    @staticmethod
    def _build_ratios_from_overview(overview: dict) -> dict:
        ratios = dict(overview.get("valuation_latest", {}))
        ratios.update(overview.get("quality_latest", {}))
        metadata = overview.get("metadata", {})
        ratios["period_end"] = metadata.get("last_financial_period_end")
        ratios["latest_price_date"] = metadata.get("last_price_date")
        return ratios

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _set_loading(self, is_loading: bool, message: str | None = None):
        self._is_loading = is_loading
        if message:
            self._status_var.set(message)
        state = ["disabled"] if is_loading or not self._selected_security else ["!disabled"]
        self._update_price_btn.state(state)
        self._refresh_btn.state(state)
        self._add_peer_btn.state(state)
        self._reset_peers_btn.state(["!disabled"] if self._manual_peer_codes and not is_loading else ["disabled"])

    def _load_selected_security(self):
        if not self._selected_security or not self._db_path:
            return

        edinet_code = self._selected_security.get("edinet_code")
        ticker = self._selected_security.get("ticker")
        periods = int(self._period_count_var.get())
        request_id = self._load_request_id + 1
        self._load_request_id = request_id
        self._set_loading(True, f"Loading security {edinet_code}...")
        self._price_history = []
        self._peers = []
        self._price_history_loaded_for = None
        self._peers_loaded_for = None
        self._loading_price_history = False
        self._loading_peers = False

        def _load():
            overview = ctrl.security_get_overview(self._db_path, edinet_code)
            statements = ctrl.security_get_statements(self._db_path, edinet_code, periods=periods)
            return {
                "overview": overview,
                "statements": statements,
            }

        def _on_done(payload):
            if request_id != self._load_request_id:
                return
            self._overview = payload["overview"]
            self._ratios = self._build_ratios_from_overview(self._overview)
            self._statements = payload["statements"]
            self._render_all()
            company_name = self._overview.get("company", {}).get("company_name") or edinet_code
            self._set_loading(False, f"Loaded {company_name}.")

        def _on_error(exc):
            if request_id != self._load_request_id:
                return
            logger.error("Failed to load security: %s", exc, exc_info=True)
            self._set_loading(False, f"Failed to load security: {exc}")
            messagebox.showerror("Security Analysis", str(exc), parent=self.winfo_toplevel())

        run_in_background(_load, on_done=_on_done, on_error=_on_error)

    def _ensure_price_history_loaded(self):
        if not self._selected_security or not self._db_path:
            return
        edinet_code = self._selected_security.get("edinet_code")
        ticker = self._selected_security.get("ticker")
        if not ticker or self._loading_price_history or self._price_history_loaded_for == edinet_code:
            return

        request_id = self._load_request_id
        self._price_history_request_id += 1
        background_request_id = self._price_history_request_id
        self._loading_price_history = True

        def _load():
            return ctrl.security_get_price_history(self._db_path, ticker)

        def _on_done(price_history):
            if request_id != self._load_request_id or background_request_id != self._price_history_request_id:
                return
            self._loading_price_history = False
            self._price_history = price_history
            self._price_history_loaded_for = edinet_code
            self._render_peers()
            self._redraw_chart()

        def _on_error(exc):
            if request_id != self._load_request_id or background_request_id != self._price_history_request_id:
                return
            self._loading_price_history = False
            logger.error("Failed to load price history: %s", exc, exc_info=True)

        run_in_background(_load, on_done=_on_done, on_error=_on_error)

    def _ensure_peers_loaded(self):
        if not self._selected_security or not self._db_path:
            return
        edinet_code = self._selected_security.get("edinet_code")
        if self._loading_peers or self._peers_loaded_for == edinet_code:
            return

        request_id = self._load_request_id
        self._peers_request_id += 1
        background_request_id = self._peers_request_id
        self._loading_peers = True
        company = self._overview.get("company", {})

        def _load():
            return self._build_peer_payload(edinet_code, {"company": company})

        def _on_done(peers):
            if request_id != self._load_request_id or background_request_id != self._peers_request_id:
                return
            self._loading_peers = False
            self._peers = peers
            self._peers_loaded_for = edinet_code
            self._render_peers()
            self._redraw_chart()

        def _on_error(exc):
            if request_id != self._load_request_id or background_request_id != self._peers_request_id:
                return
            self._loading_peers = False
            logger.error("Failed to load peers: %s", exc, exc_info=True)

        run_in_background(_load, on_done=_on_done, on_error=_on_error)

    def _build_peer_payload(self, edinet_code: str, overview: dict) -> list[dict]:
        company = overview.get("company", {})
        peers = ctrl.security_get_peers(
            self._db_path,
            edinet_code,
            industry=company.get("industry"),
            limit=8,
        )
        peer_map = {peer.get("edinet_code"): peer for peer in peers if peer.get("edinet_code")}

        for manual_code in self._manual_peer_codes:
            if manual_code == edinet_code or manual_code in peer_map:
                continue
            try:
                peer_overview = ctrl.security_get_overview(self._db_path, manual_code)
                peer_ratios = ctrl.security_get_ratios(self._db_path, manual_code)
                peer_history = ctrl.security_get_price_history(
                    self._db_path,
                    peer_overview.get("company", {}).get("ticker", ""),
                )
            except Exception as exc:  # pragma: no cover - recoverable UI path
                logger.warning("Skipping manual peer %s: %s", manual_code, exc)
                continue
            peer_map[manual_code] = {
                "edinet_code": manual_code,
                "ticker": peer_overview.get("company", {}).get("ticker"),
                "company_name": peer_overview.get("company", {}).get("company_name"),
                "industry": peer_overview.get("company", {}).get("industry"),
                "latest_price": peer_overview.get("market", {}).get("latest_price"),
                "latest_price_date": peer_overview.get("market", {}).get("latest_price_date"),
                "PERatio": peer_ratios.get("PERatio"),
                "PriceToBook": peer_ratios.get("PriceToBook"),
                "DividendsYield": peer_ratios.get("DividendsYield"),
                "ReturnOnEquity": peer_ratios.get("ReturnOnEquity"),
                "MarketCap": peer_ratios.get("MarketCap"),
                "one_year_return": _one_year_return_from_history(peer_history),
                "role": "Manual Peer",
            }

        out = list(peer_map.values())
        for row in out:
            row.setdefault("role", "Industry Peer")
        return out

    def _refresh_selected_security(self):
        if self._selected_security:
            self._load_selected_security()

    def _update_selected_price(self):
        if not self._selected_security or not self._db_path:
            return
        ticker = self._selected_security.get("ticker")
        company_name = self._selected_security.get("company_name") or ticker
        if not ticker:
            messagebox.showwarning(
                "Security Analysis",
                "Selected security does not have a ticker.",
                parent=self.winfo_toplevel(),
            )
            return
        if not messagebox.askyesno(
            "Update Price",
            f"Update price history for {company_name} ({ticker})?",
            parent=self.winfo_toplevel(),
        ):
            return

        self._set_loading(True, f"Updating price history for {ticker}...")

        def _load():
            return ctrl.security_update_price(self._db_path, ticker)

        def _on_done(result):
            self._set_loading(False, result.get("message", "Price update finished."))
            if result.get("ok"):
                self._load_selected_security()
            else:
                messagebox.showwarning(
                    "Price Update",
                    result.get("message", "Price update did not complete successfully."),
                    parent=self.winfo_toplevel(),
                )

        def _on_error(exc):
            logger.error("Price update failed: %s", exc, exc_info=True)
            self._set_loading(False, f"Price update failed: {exc}")
            messagebox.showerror("Price Update", str(exc), parent=self.winfo_toplevel())

        run_in_background(_load, on_done=_on_done, on_error=_on_error)

    def _add_manual_peer(self):
        if not self._selected_security or not self._db_path:
            return
        query = simpledialog.askstring(
            "Add Peer",
            "Enter a ticker, company name, or EDINET code:",
            parent=self.winfo_toplevel(),
        )
        if not query:
            return

        self._set_loading(True, f"Searching for peer '{query}'...")

        def _load():
            return ctrl.security_search(self._db_path, query, limit=10)

        def _on_done(results):
            self._set_loading(False)
            selected_code = self._selected_security.get("edinet_code")
            candidates = [r for r in results if r.get("edinet_code") != selected_code]
            if not candidates:
                messagebox.showinfo(
                    "Add Peer",
                    "No matching peer found.",
                    parent=self.winfo_toplevel(),
                )
                return
            chosen = candidates[0]
            code = chosen.get("edinet_code")
            if not code:
                return
            if code not in self._manual_peer_codes:
                self._manual_peer_codes.append(code)
            self._reset_peers_btn.state(["!disabled"])
            self._load_selected_security()

        def _on_error(exc):
            logger.error("Manual peer search failed: %s", exc, exc_info=True)
            self._set_loading(False, f"Peer search failed: {exc}")
            messagebox.showerror("Add Peer", str(exc), parent=self.winfo_toplevel())

        run_in_background(_load, on_done=_on_done, on_error=_on_error)

    def _reset_manual_peers(self):
        self._manual_peer_codes = []
        self._reset_peers_btn.state(["disabled"])
        self._refresh_selected_security()

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def _clear_rendered_data(self):
        self._overview = {}
        self._ratios = {}
        self._statements = {}
        self._price_history = []
        self._peers = []
        self._price_history_loaded_for = None
        self._peers_loaded_for = None
        self._loading_price_history = False
        self._loading_peers = False
        self._hero_company_name_var.set("No security selected")
        self._company_card_var.set("Search for a company to load its identity, ticker, EDINET code, and industry context.")
        self._market_card_var.set("Price data will appear here")
        self._valuation_card_var.set("Valuation data will appear here")
        self._overview_company_var.set("Select a security to load company details.")
        self._overview_meta_var.set("Data quality and metadata will appear here.")
        self._peer_summary_var.set("Peers load after a company is selected.")
        self._populate_key_value_tree(self._fundamentals_tree, [])
        self._populate_key_value_tree(self._ratios_tree, [])
        self._render_statement_table()
        if self._active_tab == "Peers":
            self._render_peers()
        if self._active_tab == "Charts":
            self._redraw_chart()

    def _render_all(self):
        company = self._overview.get("company", {})
        market = self._overview.get("market", {})
        fundamentals = self._overview.get("fundamentals_latest", {})
        valuation = self._overview.get("valuation_latest", {})
        metadata = self._overview.get("metadata", {})
        quality = self._overview.get("quality_latest", {})

        self._hero_company_name_var.set(company.get("company_name") or "N/A")
        self._company_card_var.set(
            "\n".join(
                [
                    f"Ticker: {company.get('ticker') or 'N/A'}",
                    f"EDINET: {company.get('edinet_code') or 'N/A'}",
                    f"Industry: {company.get('industry') or 'N/A'}",
                    f"Market: {company.get('market') or 'N/A'}",
                ]
            )
        )
        self._market_card_var.set(
            "\n".join(
                [
                    f"Last: {_format_currency(_safe_float(market.get('latest_price')))}",
                    f"1D: {_format_percent(_safe_float(market.get('change_pct_1d')))}",
                    (
                        "52W: "
                        f"{_format_currency(_safe_float(market.get('range_52w_low')))} - "
                        f"{_format_currency(_safe_float(market.get('range_52w_high')))}"
                    ),
                    f"Updated: {_short_date(market.get('latest_price_date'))}",
                ]
            )
        )
        self._valuation_card_var.set(
            "\n".join(
                [
                    f"P/E: {_format_ratio(_safe_float(valuation.get('PERatio')))}",
                    f"P/B: {_format_ratio(_safe_float(valuation.get('PriceToBook')))}",
                    f"Div Yield: {_format_percent(_safe_float(valuation.get('DividendsYield')))}",
                    f"Market Cap: {_format_currency(_safe_float(valuation.get('MarketCap')))}",
                ]
            )
        )

        self._overview_company_var.set(
            "\n".join(
                [
                    f"Company: {company.get('company_name') or 'N/A'}",
                    f"Ticker: {company.get('ticker') or 'N/A'}",
                    f"EDINET Code: {company.get('edinet_code') or 'N/A'}",
                    f"Industry: {company.get('industry') or 'N/A'}",
                    f"Market: {company.get('market') or 'N/A'}",
                    "",
                    company.get("description") or "No company description available.",
                ]
            )
        )
        self._overview_meta_var.set(
            "\n".join(
                [
                    f"Last financial period: {_short_date(metadata.get('last_financial_period_end'))}",
                    f"Last price date: {_short_date(metadata.get('last_price_date'))}",
                    f"Latest docID: {metadata.get('doc_id') or 'N/A'}",
                    "Flags: "
                    + (", ".join(metadata.get("data_quality_flags", [])) or "None"),
                ]
            )
        )

        fundamentals_rows = [
            ("Revenue", _format_currency(_safe_float(fundamentals.get("Revenue")))),
            ("Operating Income", _format_currency(_safe_float(fundamentals.get("OperatingIncome")))),
            ("Net Income", _format_currency(_safe_float(fundamentals.get("NetIncome")))),
            ("Total Assets", _format_currency(_safe_float(fundamentals.get("TotalAssets")))),
            ("Shareholders' Equity", _format_currency(_safe_float(fundamentals.get("ShareholdersEquity")))),
            ("Shares Outstanding", _format_short_number(_safe_float(fundamentals.get("SharesOutstanding")))),
        ]
        ratio_rows = [
            ("P/E", _format_ratio(_safe_float(valuation.get("PERatio")))),
            ("P/B", _format_ratio(_safe_float(valuation.get("PriceToBook")))),
            ("Dividend Yield", _format_percent(_safe_float(valuation.get("DividendsYield")))),
            ("ROE", _format_percent(_safe_float(quality.get("ReturnOnEquity")))),
            ("Current Ratio", _format_ratio(_safe_float(quality.get("CurrentRatio")))),
            ("Debt/Equity", _format_ratio(_safe_float(quality.get("DebtToEquity")))),
        ]
        self._populate_key_value_tree(self._fundamentals_tree, fundamentals_rows)
        self._populate_key_value_tree(self._ratios_tree, ratio_rows)
        if self.app is not None:
            selected_name = company.get("company_name") or company.get("ticker") or company.get("edinet_code") or "Security Analysis"
            self.app.set_context(
                "Security Analysis",
                f"{selected_name} • last price {_short_date(market.get('latest_price_date'))} • last filing {_short_date(metadata.get('last_financial_period_end'))}",
            )
        self._render_statement_table()
        if self._active_tab == "Peers":
            self._render_peers()
        if self._active_tab == "Charts":
            self._redraw_chart()

    def _populate_key_value_tree(self, tree: ttk.Treeview, rows: list[tuple[str, str]]):
        tree.delete(*tree.get_children())
        for key, value in rows:
            tree.insert("", "end", values=(key, value))

    def _render_statement_table(self):
        rows = self._statements.get(_STATEMENT_KEY_MAP[self._statement_kind_var.get()], [])
        periods = self._statements.get("periods", [])
        columns = ["metric"] + periods
        self._statement_tree.delete(*self._statement_tree.get_children())
        self._statement_tree.configure(columns=columns)

        self._statement_tree.heading("metric", text="Metric", anchor="w")
        self._statement_tree.column("metric", width=220, anchor="w", stretch=True)
        for period in periods:
            self._statement_tree.heading(period, text=period, anchor="e")
            self._statement_tree.column(period, width=120, anchor="e", stretch=False)

        if not periods:
            return

        for row in rows:
            values = [row.get("metric", "")]
            values.extend(_format_statement_value(_safe_float(value)) for value in row.get("values", []))
            self._statement_tree.insert("", "end", values=values)

    def _selected_peer_row(self) -> dict | None:
        if not self._selected_security:
            return None
        company = self._overview.get("company", {})
        market = self._overview.get("market", {})
        valuation = self._overview.get("valuation_latest", {})
        quality = self._overview.get("quality_latest", {})
        return {
            "role": "Selected",
            "company_name": company.get("company_name"),
            "ticker": company.get("ticker"),
            "PERatio": valuation.get("PERatio"),
            "PriceToBook": valuation.get("PriceToBook"),
            "ReturnOnEquity": quality.get("ReturnOnEquity"),
            "DividendsYield": valuation.get("DividendsYield"),
            "latest_price": market.get("latest_price"),
            "one_year_return": _one_year_return_from_history(self._price_history),
        }

    def _render_peers(self):
        self._peers_tree.delete(*self._peers_tree.get_children())
        selected_row = self._selected_peer_row()
        all_rows = [selected_row] if selected_row else []
        all_rows.extend(self._peers)
        peer_count = max(0, len(all_rows) - (1 if selected_row else 0))
        self._peer_summary_var.set(
            f"Selected company plus {peer_count} peer{'s' if peer_count != 1 else ''}. Manual peers remain merged into the comparison set."
            if selected_row
            else "Peers load after a company is selected."
        )
        for row in all_rows:
            if not row:
                continue
            self._peers_tree.insert(
                "",
                "end",
                values=(
                    row.get("role", "Industry Peer"),
                    row.get("company_name") or "N/A",
                    row.get("ticker") or "N/A",
                    _format_ratio(_safe_float(row.get("PERatio"))),
                    _format_ratio(_safe_float(row.get("PriceToBook"))),
                    _format_percent(_safe_float(row.get("ReturnOnEquity"))),
                    _format_percent(_safe_float(row.get("DividendsYield"))),
                    _format_currency(_safe_float(row.get("latest_price"))),
                    _format_percent(_safe_float(row.get("one_year_return"))),
                ),
            )

    def _show_tab(self, name: str):
        self._active_tab = name
        for frame in self._tab_frames.values():
            frame.pack_forget()
        self._tab_frames[name].pack(fill="both", expand=True)
        if name == "Charts":
            self._ensure_price_history_loaded()
            if self._chart_metric_var.get() in {"P/E", "P/B", "Dividend Yield", "ROE"} and self._chart_show_peers_var.get():
                self._ensure_peers_loaded()
            self._redraw_chart()
        elif name == "Peers":
            self._ensure_price_history_loaded()
            self._ensure_peers_loaded()
            self._render_peers()
        elif name == "Statements":
            self._render_statement_table()

    # ------------------------------------------------------------------
    # Charting
    # ------------------------------------------------------------------

    def _redraw_chart(self):
        if self._chart_canvas is None or self._chart_figure is None:
            return

        t = COLORS
        fig = self._chart_figure
        fig.clear()
        fig.patch.set_facecolor(t["surface"])
        ax = fig.add_subplot(111)
        ax.set_facecolor(t["surface"])
        ax.tick_params(colors=t["text"])
        for spine in ax.spines.values():
            spine.set_color(t["border"])
        ax.xaxis.label.set_color(t["text"])
        ax.yaxis.label.set_color(t["text"])
        ax.title.set_color(t["text"])
        ax.grid(color=t["border"], alpha=0.35)

        metric = self._chart_metric_var.get()

        if self._active_tab == "Charts":
            if metric == "Stock Price":
                self._ensure_price_history_loaded()
            elif metric in {"P/E", "P/B", "Dividend Yield", "ROE"} and self._chart_show_peers_var.get():
                self._ensure_peers_loaded()

        if metric == "Stock Price":
            self._draw_price_chart(ax)
        elif metric in {"Revenue", "Operating Income", "Net Income", "Shareholders' Equity"}:
            self._draw_statement_chart(ax, metric)
        else:
            self._draw_peer_metric_chart(ax, metric)

        fig.tight_layout()
        self._chart_canvas.draw_idle()

    def _draw_price_chart(self, ax):
        history = pd.DataFrame(self._price_history)
        if history.empty:
            message = "Loading price history..." if self._loading_price_history else "No price history available"
            ax.text(0.5, 0.5, message, ha="center", va="center", color=COLORS["text_dim"])
            return
        history["trade_date"] = pd.to_datetime(history["trade_date"], errors="coerce")
        history["price"] = pd.to_numeric(history["price"], errors="coerce")
        history = history.dropna(subset=["trade_date", "price"]).sort_values("trade_date")
        if history.empty:
            ax.text(0.5, 0.5, "No price history available", ha="center", va="center", color=COLORS["text_dim"])
            return

        timeframe = self._chart_range_var.get()
        if timeframe != "Max":
            years = int(timeframe.rstrip("Y"))
            cutoff = history["trade_date"].max() - pd.Timedelta(days=365 * years)
            history = history[history["trade_date"] >= cutoff]

        label = self._overview.get("company", {}).get("ticker") or "Price"
        ax.plot(history["trade_date"], history["price"], color=COLORS["accent"], linewidth=2.0, label=label)
        ax.set_title("Stock Price History")
        ax.set_ylabel("Price")
        ax.legend(facecolor=COLORS["surface"], edgecolor=COLORS["border"], labelcolor=COLORS["text"])

    def _draw_statement_chart(self, ax, metric: str):
        field_map = {
            "Revenue": "netSales",
            "Operating Income": "operatingIncome",
            "Net Income": "netIncome",
            "Shareholders' Equity": "shareholdersEquity",
        }
        field = field_map[metric]
        records = self._statements.get("records", [])
        if not records:
            ax.text(0.5, 0.5, "No statement history available", ha="center", va="center", color=COLORS["text_dim"])
            return
        labels = [record.get("period_end", "") for record in records]
        values = [_safe_float(record.get(field)) for record in records]
        if all(value is None for value in values):
            ax.text(0.5, 0.5, f"No {metric.lower()} data available", ha="center", va="center", color=COLORS["text_dim"])
            return
        plotted_values = [0 if value is None else value / 1_000_000 for value in values]
        ax.bar(labels, plotted_values, color=COLORS["accent"])
        ax.set_title(f"{metric} by Period")
        ax.set_ylabel("JPY mn")
        ax.tick_params(axis="x", rotation=30)

    def _draw_peer_metric_chart(self, ax, metric: str):
        metric_map = {
            "P/E": ("PERatio", _format_ratio),
            "P/B": ("PriceToBook", _format_ratio),
            "Dividend Yield": ("DividendsYield", _format_percent),
            "ROE": ("ReturnOnEquity", _format_percent),
        }
        field, _formatter = metric_map[metric]
        rows = []
        selected = self._selected_peer_row()
        if selected:
            rows.append(selected)
        if self._chart_show_peers_var.get():
            rows.extend(self._peers)
        if not rows:
            message = "Loading peer data..." if self._loading_peers else "No peer data available"
            ax.text(0.5, 0.5, message, ha="center", va="center", color=COLORS["text_dim"])
            return
        names = [row.get("ticker") or row.get("company_name") or "N/A" for row in rows]
        values = [_safe_float(row.get(field)) for row in rows]
        if all(value is None for value in values):
            ax.text(0.5, 0.5, f"No {metric.lower()} data available", ha="center", va="center", color=COLORS["text_dim"])
            return
        plotted_values = [0 if value is None else value * 100 for value in values] if field in {"DividendsYield", "ReturnOnEquity"} else [0 if value is None else value for value in values]
        colors = [COLORS["accent"]] + [COLORS["success"] for _ in rows[1:]]
        ax.bar(names, plotted_values, color=colors)
        ax.set_title(f"{metric} Comparison")
        ax.set_ylabel("Percent" if field in {"DividendsYield", "ReturnOnEquity"} else "Multiple")
        ax.tick_params(axis="x", rotation=30)

    # ------------------------------------------------------------------
    # Theme integration
    # ------------------------------------------------------------------

    def reapply_colors(self):
        """Re-apply colours for raw Tk widgets after a theme toggle."""
        self._suggestions_list.configure(
            bg=COLORS["input_bg"],
            fg=COLORS["text"],
            selectbackground=COLORS["highlight"],
            highlightbackground=COLORS["border"],
        )
        self._db_picker.reapply_colors()
        self._refresh_btn.reapply_colors()
        self._update_price_btn.reapply_colors()
        self._add_peer_btn.reapply_colors()
        self._reset_peers_btn.reapply_colors()
        self._redraw_chart()