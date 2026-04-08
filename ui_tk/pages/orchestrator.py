"""Orchestrator page: pipeline workbench with sequence cards and inspector."""

import json
import logging
import threading
import time
import tkinter as tk
from tkinter import filedialog, simpledialog, ttk

from ui_tk import controllers as ctrl
from ui_tk.shared.widgets import (
    DatabasePickerEntry,
    EmptyState,
    FilePickerEntry,
    LabeledEntry,
    LabeledText,
    PageHeader,
    PortfolioGrid,
    RoundedButton,
    ScrollableFrame,
    SectionCard,
    reapply_widget_tree,
)
from ui_tk.style import COLORS, FONT_MONO, FONT_SMALL, FONT_UI_BOLD, PAD
from ui_tk.utils import run_in_background

logger = logging.getLogger(__name__)

_STEP_GROUPS = {
    "Ingest": [
        "get_documents",
        "download_documents",
        "populate_company_info",
        "import_stock_prices_csv",
        "update_stock_prices",
    ],
    "Transform": [
        "parse_taxonomy",
        "generate_financial_statements",
        "generate_ratios",
        "generate_historical_ratios",
    ],
    "Analyze": ["Multivariate_Regression", "backtest", "backtest_set"],
}

_STATUS_TEXT = {
    "idle": "Ready",
    "pending": "Queued",
    "running": "Running",
    "done": "Done",
    "failed": "Failed",
    "cancelled": "Cancelled",
}

_CARD_BG_KEY = {
    "Panel.TFrame": "panel",
    "Hero.TFrame": "hero",
}


class OrchestratorPage(ttk.Frame):
    """Pipeline workbench for building, configuring, and running workflows."""

    def __init__(self, parent, app, **kw):
        super().__init__(parent, style="App.TFrame", **kw)
        self.app = app
        self._setup_name: str = ""
        self._steps: list[list] = []
        self._step_configs: dict[str, dict] = {}
        self._selected_idx: int | None = None
        self._config_widgets: dict[str, tuple[str, object]] = {}
        self._is_running = False
        self._cancel_event = threading.Event()
        self._step_runtime_status: list[str] = []
        self._sequence_buttons: list[RoundedButton] = []
        self._library_buttons: list[RoundedButton] = []

        self._setup_var = tk.StringVar(value="No active setup")
        self._summary_var = tk.StringVar(value="No steps yet")
        self._selection_var = tk.StringVar(value="No step selected")
        self._pipeline_status_var = tk.StringVar(value="Idle")
        self._inspector_title_var = tk.StringVar(value="Inspector")
        self._inspector_meta_var = tk.StringVar(value="Select a step to configure it.")

        self._build_layout()

        cfg = ctrl.load_ui_pipeline()
        self.load_config(cfg, name="(active)")

        self.bind_all("<Control-s>", lambda _event: self._save_setup(), add="+")
        self.bind_all("<F5>", lambda _event: self._on_run(), add="+")
        self.bind_all("<Control-r>", lambda _event: self._on_run(), add="+")
        self.bind_all("<Delete>", self._delete_selected_shortcut, add="+")

    # ------------------------------------------------------------------
    # Layout
    # ------------------------------------------------------------------

    def _build_layout(self):
        outer = ttk.Frame(self, style="App.TFrame")
        outer.pack(fill="both", expand=True, padx=PAD * 2, pady=PAD * 2)
        outer.grid_columnconfigure(0, weight=0, minsize=280)
        outer.grid_columnconfigure(1, weight=3)
        outer.grid_columnconfigure(2, weight=2, minsize=360)
        outer.grid_rowconfigure(1, weight=1)

        self._header = PageHeader(
            outer,
            title="Pipeline Builder",
            subtitle="Build an ordered execution plan, inspect step configuration, and run it with visible status.",
            context="The pipeline view is now a workbench: library, sequence, inspector.",
        )
        self._header.grid(row=0, column=0, columnspan=3, sticky="ew", pady=(0, PAD * 2))

        self._export_tpl_btn = RoundedButton(
            self._header.actions,
            text="Export Template",
            style="Ghost.TButton",
            command=self._export_template,
        )
        self._export_tpl_btn.pack(side="right")
        self._new_btn = RoundedButton(self._header.actions, text="New", style="Ghost.TButton", command=self._new_setup)
        self._new_btn.pack(side="right", padx=(0, 6))
        self._load_btn = RoundedButton(self._header.actions, text="Load", style="Ghost.TButton", command=self._load_setup)
        self._load_btn.pack(side="right", padx=(0, 6))
        self._save_btn = RoundedButton(self._header.actions, text="Save", style="Ghost.TButton", command=self._save_setup)
        self._save_btn.pack(side="right", padx=(0, 6))
        self._stop_btn = RoundedButton(self._header.actions, text="Stop", style="Danger.TButton", command=self._on_stop)
        self._stop_btn.pack(side="right", padx=(0, 6))
        self._stop_btn.state(["disabled"])
        self._run_btn = RoundedButton(self._header.actions, text="Run", style="Accent.TButton", command=self._on_run)
        self._run_btn.pack(side="right", padx=(0, 6))

        self._left_col = ttk.Frame(outer, style="App.TFrame")
        self._left_col.grid(row=1, column=0, sticky="nsew", padx=(0, PAD))
        self._left_col.grid_rowconfigure(1, weight=1)

        self._setup_card = SectionCard(self._left_col, "Active Setup", "Saved pipeline state and quick actions.")
        self._setup_card.grid(row=0, column=0, sticky="ew", pady=(0, PAD))
        self._build_setup_card(self._setup_card.body)

        self._library_card = SectionCard(self._left_col, "Step Library", "Add pipeline steps grouped by function.")
        self._library_card.grid(row=1, column=0, sticky="nsew")
        self._build_step_library(self._library_card.body)

        self._center_col = ttk.Frame(outer, style="App.TFrame")
        self._center_col.grid(row=1, column=1, sticky="nsew", padx=(0, PAD))
        self._center_col.grid_rowconfigure(0, weight=1)

        self._sequence_card = SectionCard(
            self._center_col,
            "Pipeline Sequence",
            "Ordered execution plan with per-step status and summary.",
        )
        self._sequence_card.grid(row=0, column=0, sticky="nsew")
        self._build_sequence_surface(self._sequence_card.body)

        self._right_col = ttk.Frame(outer, style="App.TFrame")
        self._right_col.grid(row=1, column=2, sticky="nsew")
        self._right_col.grid_rowconfigure(0, weight=1)

        self._inspector_card = SectionCard(self._right_col, "Inspector", "Selected step configuration.")
        self._inspector_card.grid(row=0, column=0, sticky="nsew")
        self._build_inspector(self._inspector_card.body)

    def _build_setup_card(self, parent):
        ttk.Label(parent, textvariable=self._setup_var, style="Panel.TLabel", font=FONT_UI_BOLD).pack(anchor="w")
        ttk.Label(
            parent,
            textvariable=self._summary_var,
            style="Panel.TLabel",
            font=FONT_SMALL,
            wraplength=250,
            justify="left",
        ).pack(anchor="w", pady=(4, 0))
        ttk.Label(parent, textvariable=self._pipeline_status_var, style="Panel.TLabel", font=FONT_SMALL).pack(anchor="w", pady=(8, 0))

    def _build_step_library(self, parent):
        self._add_step_btn = RoundedButton(parent, text="+ Add Step Menu", style="Ghost.TButton", command=self._add_step)
        self._add_step_btn.pack(fill="x", pady=(0, PAD))
        for group_name, step_names in _STEP_GROUPS.items():
            block = ttk.Frame(parent, style="Panel.TFrame")
            block.pack(fill="x", pady=(0, PAD))
            ttk.Label(block, text=group_name, style="Panel.TLabel", font=FONT_UI_BOLD).pack(anchor="w")
            for step_name in step_names:
                if step_name not in ctrl.STEP_DISPLAY:
                    continue
                button = RoundedButton(
                    block,
                    text=ctrl.STEP_DISPLAY[step_name],
                    style="Small.TButton",
                    command=lambda name=step_name: self._do_add_step(name),
                )
                button.pack(fill="x", pady=(6, 0))
                self._library_buttons.append(button)

    def _build_sequence_surface(self, parent):
        meta = ttk.Frame(parent, style="Panel.TFrame")
        meta.pack(fill="x", pady=(0, PAD))
        left = ttk.Frame(meta, style="Panel.TFrame")
        left.pack(side="left", fill="x", expand=True)
        ttk.Label(left, textvariable=self._selection_var, style="Panel.TLabel", font=FONT_UI_BOLD).pack(anchor="w")
        ttk.Label(
            left,
            text="Select a step card to edit its configuration in the inspector.",
            style="Panel.TLabel",
            font=FONT_SMALL,
        ).pack(anchor="w", pady=(2, 0))

        self._sequence_scroll = ScrollableFrame(parent, bg_key="panel")
        self._sequence_scroll.pack(fill="both", expand=True)
        self._sequence_inner = self._sequence_scroll.interior
        self._sequence_inner.configure(style="Panel.TFrame")

    def _build_inspector(self, parent):
        header = ttk.Frame(parent, style="Panel.TFrame")
        header.pack(fill="x")
        left = ttk.Frame(header, style="Panel.TFrame")
        left.pack(side="left", fill="x", expand=True)
        ttk.Label(left, textvariable=self._inspector_title_var, style="Panel.TLabel", font=FONT_UI_BOLD).pack(anchor="w")
        ttk.Label(
            left,
            textvariable=self._inspector_meta_var,
            style="Panel.TLabel",
            font=FONT_SMALL,
            wraplength=320,
            justify="left",
        ).pack(anchor="w", pady=(2, 0))
        self._close_inspector_btn = RoundedButton(header, text="Clear", style="Ghost.TButton", command=self._close_config_panel)
        self._close_inspector_btn.pack(side="right")

        self._inspector_scroll = ScrollableFrame(parent, bg_key="panel")
        self._inspector_scroll.pack(fill="both", expand=True, pady=(PAD, 0))
        self._config_frame = self._inspector_scroll.interior
        self._config_frame.configure(style="Panel.TFrame")
        self._show_empty_inspector()

    # ------------------------------------------------------------------
    # Sequence rendering and selection
    # ------------------------------------------------------------------

    def _show_empty_inspector(self):
        for child in self._config_frame.winfo_children():
            child.destroy()
        EmptyState(
            self._config_frame,
            "No Step Selected",
            "Select or add a step to inspect its configuration fields here.",
            style="Panel.TFrame",
        ).pack(fill="both", expand=True)

    def _ensure_status_length(self):
        target = len(self._steps)
        current = len(self._step_runtime_status)
        if current < target:
            self._step_runtime_status.extend(["idle"] * (target - current))
        elif current > target:
            self._step_runtime_status = self._step_runtime_status[:target]

    def _refresh_sequence_cards(self):
        self._ensure_status_length()
        self._sequence_buttons.clear()
        for child in self._sequence_inner.winfo_children():
            child.destroy()

        if not self._steps:
            EmptyState(
                self._sequence_inner,
                "No Pipeline Steps",
                "Add a step from the library to start building a workflow. Sequence cards will appear here in execution order.",
                style="Panel.TFrame",
            ).pack(fill="x", pady=(PAD, 0))
            return

        for idx, (step_name, overwrite) in enumerate(self._steps):
            is_selected = idx == self._selected_idx
            frame_style = "Hero.TFrame" if is_selected else "Panel.TFrame"
            label_style = "Hero.TLabel" if is_selected else "Panel.TLabel"
            card = ttk.Frame(self._sequence_inner, style=frame_style)
            card.pack(fill="x", pady=(0, PAD))

            top = ttk.Frame(card, style=frame_style)
            top.pack(fill="x", padx=PAD, pady=(PAD, 0))

            tk.Label(
                top,
                text=f"{idx + 1:02d}",
                bg=COLORS["accent_soft"],
                fg=COLORS["accent"],
                font=FONT_SMALL,
                padx=8,
                pady=3,
            ).pack(side="left")

            title_wrap = ttk.Frame(top, style=frame_style)
            title_wrap.pack(side="left", fill="x", expand=True, padx=(10, 0))
            ttk.Label(title_wrap, text=ctrl.STEP_DISPLAY.get(step_name, step_name), style=label_style, font=FONT_UI_BOLD).pack(anchor="w")
            ttk.Label(
                title_wrap,
                text=self._summarize_step(step_name),
                style=label_style,
                font=FONT_SMALL,
                wraplength=430,
                justify="left",
            ).pack(anchor="w", pady=(4, 0))

            actions = ttk.Frame(top, style=frame_style)
            actions.pack(side="right")
            select_btn = RoundedButton(actions, text="Inspect", style="Ghost.TButton", command=lambda i=idx: self._select_step(i))
            select_btn.pack(side="left", padx=(0, 4))
            move_up_btn = RoundedButton(actions, text="↑", style="Small.TButton", width=3, command=lambda i=idx: self._move_step_to(i, i - 1))
            move_up_btn.pack(side="left", padx=(0, 4))
            move_down_btn = RoundedButton(actions, text="↓", style="Small.TButton", width=3, command=lambda i=idx: self._move_step_to(i, i + 1))
            move_down_btn.pack(side="left", padx=(0, 4))
            remove_btn = RoundedButton(actions, text="Remove", style="Danger.TButton", command=lambda i=idx: self._remove_step_by_index(i))
            remove_btn.pack(side="left")
            self._sequence_buttons.extend([select_btn, move_up_btn, move_down_btn, remove_btn])

            bottom = ttk.Frame(card, style=frame_style)
            bottom.pack(fill="x", padx=PAD, pady=(8, PAD))
            status_text = _STATUS_TEXT.get(self._step_runtime_status[idx], "Ready")
            status_bg = COLORS[_CARD_BG_KEY.get(frame_style, "panel")]
            tk.Label(bottom, text=status_text, bg=status_bg, fg=self._status_color(self._step_runtime_status[idx]), font=FONT_SMALL).pack(side="left")
            if overwrite:
                tk.Label(
                    bottom,
                    text="overwrite",
                    bg=COLORS["surface_alt"],
                    fg=COLORS["text_dim"],
                    font=FONT_SMALL,
                    padx=8,
                    pady=3,
                ).pack(side="left", padx=(8, 0))

            for widget in (card, top, title_wrap, bottom):
                widget.bind("<Button-1>", lambda _event, i=idx: self._select_step(i))

    def _status_color(self, status: str) -> str:
        if status == "done":
            return COLORS["success"]
        if status == "failed":
            return COLORS["error"]
        if status == "running":
            return COLORS["accent"]
        if status == "pending":
            return COLORS["warning"]
        return COLORS["text_dim"]

    def _summarize_step(self, step_name: str) -> str:
        cfg = self._step_configs.get(step_name, {})
        if not cfg:
            return "No configuration captured yet."

        parts: list[str] = []
        for field in ctrl.STEP_FIELD_DEFINITIONS.get(step_name, []):
            value = cfg.get(field.key)
            if value in (None, "", [], {}):
                continue
            if isinstance(value, (dict, list)):
                rendered = f"{len(value)} item{'s' if len(value) != 1 else ''}"
            else:
                rendered = str(value)
            if "/" in rendered or "\\" in rendered:
                rendered = rendered.replace("\\", "/").split("/")[-1]
            parts.append(f"{field.key}={rendered}")
            if len(parts) == 3:
                break
        return " • ".join(parts) if parts else "Configuration exists but has no prominent values yet."

    def _select_step(self, idx: int):
        if not (0 <= idx < len(self._steps)):
            return
        self._save_current_config_fields()
        self._selected_idx = idx
        step_name = self._steps[idx][0]
        self._selection_var.set(f"Selected: {ctrl.STEP_DISPLAY.get(step_name, step_name)}")
        self._open_config_panel()
        self._refresh_sequence_cards()
        self._update_overview()

    def _move_step_to(self, current_idx: int, new_idx: int):
        if new_idx < 0 or new_idx >= len(self._steps):
            return
        self._save_current_config_fields()
        self._steps.insert(new_idx, self._steps.pop(current_idx))
        if current_idx < len(self._step_runtime_status):
            status = self._step_runtime_status.pop(current_idx)
            self._step_runtime_status.insert(new_idx, status)
        if self._selected_idx == current_idx:
            self._selected_idx = new_idx
        elif self._selected_idx == new_idx:
            self._selected_idx = current_idx
        self._refresh_sequence_cards()
        self._update_overview()

    def _remove_step_by_index(self, idx: int):
        if not (0 <= idx < len(self._steps)):
            return
        removed = self._steps.pop(idx)
        if idx < len(self._step_runtime_status):
            self._step_runtime_status.pop(idx)
        if self._selected_idx == idx:
            self._selected_idx = None
            self._close_config_panel()
        elif self._selected_idx is not None and idx < self._selected_idx:
            self._selected_idx -= 1
        logger.info("Removed step: %s", ctrl.STEP_DISPLAY.get(removed[0], removed[0]))
        self._refresh_sequence_cards()
        self._update_overview()

    def _delete_selected_shortcut(self, _event=None):
        focus = self.focus_get()
        if focus and focus.winfo_toplevel() != self.winfo_toplevel():
            return
        if self._selected_idx is not None:
            self._remove_step_by_index(self._selected_idx)

    # ------------------------------------------------------------------
    # Inspector
    # ------------------------------------------------------------------

    def _open_config_panel(self, _event=None):
        idx = self._selected_idx
        if idx is None or idx >= len(self._steps):
            self._show_empty_inspector()
            return
        step_name = self._steps[idx][0]
        self._build_config_fields(step_name)

    def _close_config_panel(self, _event=None):
        self._save_current_config_fields()
        self._selected_idx = None
        self._selection_var.set("No step selected")
        self._inspector_title_var.set("Inspector")
        self._inspector_meta_var.set("Select a step to configure it.")
        self._show_empty_inspector()
        self._refresh_sequence_cards()
        self._update_overview()

    def _build_config_fields(self, step_name: str):
        for child in self._config_frame.winfo_children():
            child.destroy()

        display = ctrl.STEP_DISPLAY.get(step_name, step_name)
        self._inspector_title_var.set(display)
        self._inspector_meta_var.set("Edit values here. The current page uses explicit apply into the active setup state.")

        top = ttk.Frame(self._config_frame, style="Panel.TFrame")
        top.pack(fill="x", pady=(0, PAD))
        ttk.Label(top, text="Step Configuration", style="Panel.TLabel", font=FONT_UI_BOLD).pack(anchor="w")
        ttk.Label(
            top,
            text=self._summarize_step(step_name),
            style="Panel.TLabel",
            font=FONT_SMALL,
            wraplength=320,
            justify="left",
        ).pack(anchor="w", pady=(4, 0))

        cfg = self._step_configs.get(step_name, {})
        self._config_widgets = {}

        if step_name in ctrl.STEPS_WITH_OVERWRITE and self._selected_idx is not None:
            overwrite_var = tk.BooleanVar(value=self._steps[self._selected_idx][1])
            ttk.Checkbutton(
                self._config_frame,
                text="Overwrite existing generated data",
                variable=overwrite_var,
                style="Panel.TCheckbutton",
            ).pack(anchor="w", pady=(0, PAD))
            self._config_widgets["__overwrite__"] = ("bool", overwrite_var)

        self._build_step_fields(self._config_frame, step_name, cfg)
        RoundedButton(
            self._config_frame,
            text="Apply Step Config",
            style="Accent.TButton",
            command=self._save_current_config_fields,
        ).pack(fill="x", pady=(PAD, 0))

    def _build_step_fields(self, parent, step_name: str, cfg: dict):
        field_defs = ctrl.STEP_FIELD_DEFINITIONS.get(step_name, [])
        for field in field_defs:
            value = cfg.get(field.key, field.default)
            label = field.display_label

            if field.field_type == "database":
                widget = DatabasePickerEntry(parent, label=label, value=str(value), label_style="Panel.TLabel")
                widget.pack(fill="x", pady=(0, PAD))
                self._config_widgets[field.key] = ("str", widget)
            elif field.field_type == "file":
                kwargs = {"filetypes": field.filetypes} if field.filetypes else {}
                widget = FilePickerEntry(parent, label=label, value=str(value), label_style="Panel.TLabel", **kwargs)
                widget.pack(fill="x", pady=(0, PAD))
                self._config_widgets[field.key] = ("str", widget)
            elif field.field_type == "json":
                text = json.dumps(value, indent=2) if isinstance(value, (dict, list)) else str(value)
                widget = LabeledText(parent, label=label, value=text, height=field.height, label_style="Panel.TLabel")
                widget.pack(fill="x", pady=(0, PAD))
                self._config_widgets[field.key] = ("json", widget)
            elif field.field_type == "text":
                widget = LabeledText(parent, label=label, value=str(value), height=field.height, label_style="Panel.TLabel")
                widget.pack(fill="x", pady=(0, PAD))
                self._config_widgets[field.key] = ("text", widget)
            elif field.field_type == "num":
                widget = LabeledEntry(parent, label=label, value=str(value), label_style="Panel.TLabel")
                widget.pack(fill="x", pady=(0, PAD))
                self._config_widgets[field.key] = ("num", widget)
            elif field.field_type == "portfolio":
                ttk.Label(parent, text=label, style="Panel.TLabel", font=FONT_UI_BOLD).pack(anchor="w", pady=(0, 4))
                widget = PortfolioGrid(parent, portfolio=value if isinstance(value, dict) else {})
                widget.pack(fill="x", pady=(0, PAD))
                self._config_widgets[field.key] = ("portfolio", widget)
            else:
                widget = LabeledEntry(parent, label=label, value=str(value), label_style="Panel.TLabel")
                widget.pack(fill="x", pady=(0, PAD))
                self._config_widgets[field.key] = ("str", widget)

    def _save_current_config_fields(self):
        if self._selected_idx is None or self._selected_idx >= len(self._steps):
            return
        step_name = self._steps[self._selected_idx][0]
        cfg = self._step_configs.setdefault(step_name, {})

        for key, (kind, widget) in self._config_widgets.items():
            if key == "__overwrite__":
                self._steps[self._selected_idx][1] = widget.get()
                continue
            if kind == "str":
                cfg[key] = widget.get()
            elif kind == "text":
                cfg[key] = widget.get()
            elif kind == "num":
                raw = widget.get()
                try:
                    cfg[key] = int(raw) if "." not in raw else float(raw)
                except ValueError:
                    cfg[key] = raw
            elif kind == "json":
                try:
                    cfg[key] = json.loads(widget.get())
                except json.JSONDecodeError:
                    cfg[key] = widget.get()
            elif kind == "portfolio":
                cfg[key] = widget.get_portfolio()

        self._step_configs[step_name] = cfg
        self._refresh_sequence_cards()
        self._update_overview()
        logger.info("Config saved for: %s", step_name)

    # ------------------------------------------------------------------
    # Step management
    # ------------------------------------------------------------------

    def _add_step(self):
        menu = tk.Menu(
            self,
            tearoff=0,
            bg=COLORS["surface"],
            fg=COLORS["text"],
            font=FONT_MONO,
            activebackground=COLORS["highlight"],
        )
        for step_name in ctrl.ALL_STEP_NAMES:
            menu.add_command(label=ctrl.STEP_DISPLAY.get(step_name, step_name), command=lambda name=step_name: self._do_add_step(name))
        menu.tk_popup(self._add_step_btn.winfo_rootx(), self._add_step_btn.winfo_rooty() + self._add_step_btn.winfo_height())

    def _do_add_step(self, step_name: str):
        self._save_current_config_fields()
        self._steps.append([step_name, False])
        self._step_runtime_status.append("idle")
        if step_name not in self._step_configs:
            self._step_configs[step_name] = ctrl.get_default_config_for_step(step_name)
        self._selected_idx = len(self._steps) - 1
        self._refresh_sequence_cards()
        self._open_config_panel()
        self._update_overview()
        logger.info("Added step: %s", ctrl.STEP_DISPLAY.get(step_name, step_name))

    # ------------------------------------------------------------------
    # Setup persistence
    # ------------------------------------------------------------------

    def load_config(self, cfg: dict, name: str = ""):
        self._setup_name = name
        self._steps = ctrl.build_steps_from_config(cfg)
        self._step_configs = ctrl.build_step_configs_from_config(cfg)
        self._selected_idx = 0 if self._steps else None
        self._step_runtime_status = ["idle"] * len(self._steps)
        self._refresh_sequence_cards()
        self._update_overview()
        if self._selected_idx is not None:
            self._select_step(self._selected_idx)
        else:
            self._show_empty_inspector()

    def new_setup(self, name: str):
        self._setup_name = name
        self._steps = []
        self._step_configs = {}
        self._selected_idx = None
        self._step_runtime_status = []
        self._show_empty_inspector()
        self._refresh_sequence_cards()
        self._update_overview()
        logger.info("New setup: %s", name)

    def _save_setup(self):
        self._save_current_config_fields()
        name = self._setup_name or "(active)"
        if name == "(active)":
            name = simpledialog.askstring("Save Setup", "Setup name:", parent=self)
            if not name:
                return
            self._setup_name = name
        cfg = ctrl.build_config_dict(self._steps, self._step_configs)
        ctrl.save_setup(name, cfg)
        ctrl.save_ui_pipeline(cfg)
        self._update_overview()
        logger.info("Setup saved: %s", name)

    def _load_setup(self):
        setups = ctrl.list_setups()
        if not setups:
            logger.info("No saved setups found")
            return

        win = tk.Toplevel(self.winfo_toplevel())
        win.title("Load Setup")
        win.geometry("360x320")
        win.configure(bg=COLORS["surface"])
        win.transient(self.winfo_toplevel())
        win.grab_set()

        ttk.Label(win, text="Select a saved setup", style="Surface.TLabel").pack(anchor="w", padx=PAD * 2, pady=(PAD * 2, 0))
        listbox = tk.Listbox(
            win,
            bg=COLORS["input_bg"],
            fg=COLORS["text"],
            font=FONT_MONO,
            selectbackground=COLORS["highlight"],
            relief="flat",
            borderwidth=0,
            activestyle="none",
        )
        listbox.pack(fill="both", expand=True, padx=PAD * 2, pady=PAD)
        for setup in setups:
            listbox.insert("end", setup)
        if setups:
            listbox.selection_set(0)

        def _load():
            selection = listbox.curselection()
            if not selection:
                return
            name = setups[selection[0]]
            cfg = ctrl.load_setup(name)
            win.destroy()
            self.load_config(cfg, name=name)
            logger.info("Loaded setup: %s", name)

        listbox.bind("<Double-1>", lambda _event: _load())
        buttons = ttk.Frame(win, style="Surface.TFrame")
        buttons.pack(fill="x", padx=PAD * 2, pady=(0, PAD))
        RoundedButton(buttons, text="Open", style="Accent.TButton", command=_load).pack(side="right")
        RoundedButton(buttons, text="Cancel", style="Ghost.TButton", command=win.destroy).pack(side="right", padx=(0, PAD))

    def _new_setup(self):
        name = simpledialog.askstring("New Setup", "Setup name:", parent=self)
        if name:
            self.new_setup(name)

    # ------------------------------------------------------------------
    # Run / stop
    # ------------------------------------------------------------------

    def _get_enabled_steps(self) -> list[dict]:
        return [{"name": name, "overwrite": overwrite} for name, overwrite in self._steps]

    def _set_step_status(self, step_name: str, new_status: str, source_statuses: tuple[str, ...]):
        for idx, (name, _overwrite) in enumerate(self._steps):
            if name == step_name and self._step_runtime_status[idx] in source_statuses:
                self._step_runtime_status[idx] = new_status
                break
        self._refresh_sequence_cards()

    def _on_run(self):
        enabled = self._get_enabled_steps()
        if not enabled:
            logger.warning("No steps enabled — nothing to run")
            return

        self._save_current_config_fields()
        cfg_dict = ctrl.build_config_dict(self._steps, self._step_configs)
        ctrl.save_ui_pipeline(cfg_dict)

        self._is_running = True
        self._cancel_event.clear()
        self._step_runtime_status = ["pending"] * len(self._steps)
        self._pipeline_status_var.set("Pipeline running")
        self._run_btn.state(["disabled"])
        self._stop_btn.state(["!disabled"])
        for button in self._library_buttons:
            button.state(["disabled"])
        self._add_step_btn.state(["disabled"])
        self._refresh_sequence_cards()

        start_time = time.time()
        steps_done: list[str] = []

        def _on_step_start(name):
            self.after(0, lambda n=name: self._handle_step_start(n))

        def _on_step_done(name):
            self.after(0, lambda n=name: self._handle_step_done(n, steps_done))

        def _on_step_error(name, exc):
            self.after(0, lambda n=name, e=exc: self._handle_step_error(n, e))

        def _do_run():
            ctrl.run_pipeline(
                steps=enabled,
                config_dict=cfg_dict,
                on_step_start=_on_step_start,
                on_step_done=_on_step_done,
                on_step_error=_on_step_error,
                cancel_event=self._cancel_event,
            )

        def _on_done(_result):
            elapsed = time.time() - start_time
            self._is_running = False
            self._run_btn.state(["!disabled"])
            self._stop_btn.state(["disabled"])
            for button in self._library_buttons:
                button.state(["!disabled"])
            self._add_step_btn.state(["!disabled"])
            if self._cancel_event.is_set():
                self._pipeline_status_var.set(f"Cancelled after {elapsed:.1f}s")
                self._step_runtime_status = [
                    status if status in {"done", "failed"} else "cancelled"
                    for status in self._step_runtime_status
                ]
            else:
                self._pipeline_status_var.set(f"Completed in {elapsed:.1f}s")
            self._refresh_sequence_cards()
            self._update_overview()
            logger.info("Pipeline completed (%d steps, %.1fs)", len(steps_done), elapsed)

        def _on_error(exc):
            elapsed = time.time() - start_time
            self._is_running = False
            self._run_btn.state(["!disabled"])
            self._stop_btn.state(["disabled"])
            for button in self._library_buttons:
                button.state(["!disabled"])
            self._add_step_btn.state(["!disabled"])
            self._pipeline_status_var.set(f"Failed after {elapsed:.1f}s")
            self._refresh_sequence_cards()
            self._update_overview()
            logger.error("Pipeline failed after %.1fs: %s", elapsed, exc)

        run_in_background(_do_run, on_done=_on_done, on_error=_on_error)

    def _handle_step_start(self, step_name: str):
        self._set_step_status(step_name, "running", ("pending", "idle"))
        self.app.log_panel.append("INFO", f"▶ Starting step: {ctrl.STEP_DISPLAY.get(step_name, step_name)}")

    def _handle_step_done(self, step_name: str, steps_done: list[str]):
        steps_done.append(step_name)
        self._set_step_status(step_name, "done", ("running",))

    def _handle_step_error(self, step_name: str, exc: Exception):
        self._set_step_status(step_name, "failed", ("running", "pending"))
        logger.error("Step failed: %s (%s)", step_name, exc)

    def _on_stop(self):
        if self._is_running:
            self._cancel_event.set()
            self._pipeline_status_var.set("Stop requested")
            logger.info("Stop requested — will halt after current step")

    def _export_template(self):
        dest = filedialog.asksaveasfilename(
            parent=self,
            title="Export run_config template",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            initialfile="run_config.template.json",
        )
        if not dest:
            return

        from pathlib import Path

        path = ctrl.generate_template_run_config(dest=Path(dest))
        logger.info("Template exported: %s", path)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _update_overview(self):
        step_count = len(self._steps)
        self._setup_var.set(self._setup_name or "(active)")
        if self._selected_idx is not None and self._selected_idx < len(self._steps):
            selected_raw = self._steps[self._selected_idx][0]
            selected_name = ctrl.STEP_DISPLAY.get(selected_raw, selected_raw)
        else:
            selected_name = "None"
        self._summary_var.set(f"{step_count} step{'s' if step_count != 1 else ''} in sequence • selected: {selected_name}")
        if self.app is not None:
            self.app.set_context(
                "Pipeline Builder",
                f"{self._setup_name or '(active)'} • {step_count} step{'s' if step_count != 1 else ''} • {self._pipeline_status_var.get()}",
            )

    # ------------------------------------------------------------------
    # Theme
    # ------------------------------------------------------------------

    def reapply_colors(self):
        self._run_btn.reapply_colors()
        self._stop_btn.reapply_colors()
        self._save_btn.reapply_colors()
        self._load_btn.reapply_colors()
        self._new_btn.reapply_colors()
        self._export_tpl_btn.reapply_colors()
        self._add_step_btn.reapply_colors()
        self._close_inspector_btn.reapply_colors()
        for button in self._library_buttons + self._sequence_buttons:
            button.reapply_colors()
        self._sequence_scroll.reapply_colors()
        self._inspector_scroll.reapply_colors()
        reapply_widget_tree(self._config_frame)
        self._refresh_sequence_cards()
        if self._selected_idx is not None:
            self._open_config_panel()