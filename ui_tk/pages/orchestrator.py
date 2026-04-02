"""Orchestrator page: step list, config panel, run controls."""

import copy
import json
import logging
import queue
import threading
import time
import tkinter as tk
from tkinter import ttk, filedialog, simpledialog

from ui_tk import controllers as ctrl
from ui_tk.style import COLORS, FONT_UI, FONT_UI_BOLD, FONT_MONO, PAD
from ui_tk.utils import run_in_background
from ui_tk.shared.widgets import (
    LabeledEntry, LabeledText, FilePickerEntry, DatabasePickerEntry,
    PortfolioGrid,
)

logger = logging.getLogger(__name__)


class OrchestratorPage(ttk.Frame):
    """Main orchestrator view with step list, config panel and run controls."""

    def __init__(self, parent, app, **kw):
        super().__init__(parent, **kw)
        self.app = app
        self._setup_name: str = ""
        self._steps: list = []          # [[name, overwrite], ...]
        self._step_configs: dict = {}   # {step_name: {field: value, ...}}
        self._selected_idx: int | None = None
        self._is_running = False
        self._cancel_event = threading.Event()
        self._config_panel_visible = False

        # ── layout: left (step list) + centre (paned: main + config) ───
        self._body = ttk.PanedWindow(self, orient="horizontal")
        self._body.pack(fill="both", expand=True)

        # left panel: step list
        self._left = ttk.Frame(self._body, width=240)
        self._body.add(self._left, weight=0)

        # right area: main + optional config
        self._right_pane = ttk.PanedWindow(self._body, orient="horizontal")
        self._body.add(self._right_pane, weight=1)

        self._main_area = ttk.Frame(self._right_pane)
        self._right_pane.add(self._main_area, weight=1)

        self._config_frame = ttk.Frame(self._right_pane, style="Surface.TFrame")
        # not added to pane until user opens it

        # ── build sub-sections ──────────────────────────────────────────
        self._build_step_list()
        self._build_main_area()
        self._build_run_controls()

        # ── load current run_config as default ──────────────────────────
        cfg = ctrl.load_run_config()
        self.load_config(cfg, name="(active)")

        # ── keyboard shortcuts ──────────────────────────────────────────
        self.bind_all("<Control-s>", lambda _: self._save_setup(), add="+")
        self.bind_all("<F5>", lambda _: self._on_run(), add="+")
        self.bind_all("<Control-r>", lambda _: self._on_run(), add="+")

    # ── step list (left panel) ──────────────────────────────────────────

    def _build_step_list(self):
        header = ttk.Frame(self._left)
        header.pack(fill="x", padx=PAD, pady=(PAD, 0))
        self._setup_label = ttk.Label(header, text="Pipeline:",
                                      style="Accent.TLabel")
        self._setup_label.pack(anchor="w")
        ttk.Separator(self._left, orient="horizontal").pack(fill="x",
                                                            padx=PAD, pady=2)

        # listbox for steps
        self._step_listbox = tk.Listbox(
            self._left, bg=COLORS["surface"], fg=COLORS["text"],
            font=FONT_UI, selectbackground=COLORS["highlight"],
            selectforeground="#ffffff", relief="flat", borderwidth=0,
            highlightthickness=0,
            activestyle="none",
        )
        self._step_listbox.pack(fill="both", expand=True, padx=PAD, pady=2)
        self._step_listbox.bind("<<ListboxSelect>>", self._on_step_select)
        self._step_listbox.bind("<Return>", self._open_config_panel)
        self._step_listbox.bind("<Delete>", self._remove_selected_step)
        self._step_listbox.bind("<Alt-Up>", self._move_step_up)
        self._step_listbox.bind("<Alt-Down>", self._move_step_down)


        # context menu
        self._step_menu = tk.Menu(self._step_listbox, tearoff=0,
                                  bg=COLORS["surface"], fg=COLORS["text"],
                                  font=FONT_UI,
                                  activebackground=COLORS["highlight"])
        self._step_menu.add_command(label="Configure",
                                    command=self._open_config_panel)
        self._step_menu.add_command(label="Move Up",
                                    command=lambda: self._move_step_up(None))
        self._step_menu.add_command(label="Move Down",
                                    command=lambda: self._move_step_down(None))
        self._step_menu.add_separator()
        self._step_menu.add_command(label="Remove",
                                    command=lambda: self._remove_selected_step(None))
        self._step_listbox.bind("<Button-3>", self._show_step_menu)

        # add step button
        add_frame = ttk.Frame(self._left)
        add_frame.pack(fill="x", padx=PAD, pady=(2, 0))
        ttk.Button(add_frame, text="+ Add Step",
                   command=self._add_step, style="Small.TButton").pack(fill="x")

        ttk.Separator(self._left, orient="horizontal").pack(fill="x",
                                                            padx=PAD, pady=4)

        # setup buttons
        btn_frame = ttk.Frame(self._left)
        btn_frame.pack(fill="x", padx=PAD, pady=(0, PAD))
        ttk.Button(btn_frame, text="Save", style="Ghost.TButton",
                   command=self._save_setup).pack(side="left", expand=True,
                                                  fill="x", padx=(0, 2))
        ttk.Button(btn_frame, text="Load", style="Ghost.TButton",
                   command=self._load_setup).pack(side="left", expand=True,
                                                  fill="x", padx=2)
        ttk.Button(btn_frame, text="New", style="Ghost.TButton",
                   command=self._new_setup).pack(side="left", expand=True,
                                                 fill="x", padx=(2, 0))

    def _refresh_step_listbox(self):
        sel = self._step_listbox.curselection()
        self._step_listbox.delete(0, "end")
        for name, _ow in self._steps:
            display_name = ctrl.STEP_DISPLAY.get(name, name)
            self._step_listbox.insert("end", f" ≡  {display_name}")
        if sel and sel[0] < self._step_listbox.size():
            self._step_listbox.selection_set(sel[0])
            self._step_listbox.see(sel[0])

    # ── main area (centre) ──────────────────────────────────────────────

    def _build_main_area(self):
        self._main_info = ttk.Frame(self._main_area)
        self._main_info.pack(fill="both", expand=True, padx=PAD * 2,
                             pady=PAD * 2)
        self._info_label = ttk.Label(self._main_info, text="",
                                     style="Heading.TLabel")
        self._info_label.pack(anchor="w")
        self._info_details = ttk.Label(self._main_info, text="",
                                       wraplength=500)
        self._info_details.pack(anchor="w", pady=(PAD, 0))

    def _update_main_info(self):
        total = len(self._steps)
        self._info_label.configure(
            text=f"Setup: {self._setup_name}")
        self._info_details.configure(
            text=(f"Steps: {total}\n\n"
                  "Select a step to configure.\n"
                  "Alt+↑/↓ to reorder.  [+] to add steps."))

    # ── run controls (bottom) ───────────────────────────────────────────

    def _build_run_controls(self):
        bar = ttk.Frame(self)
        bar.pack(fill="x", padx=PAD, pady=(0, PAD))

        self._run_btn = ttk.Button(bar, text="▶ Run",
                                   command=self._on_run,
                                   style="Accent.TButton")
        self._run_btn.pack(side="right", padx=PAD)

        self._stop_btn = ttk.Button(bar, text="◀ Stop",
                                    command=self._on_stop,
                                    style="Danger.TButton")
        self._stop_btn.pack(side="right")
        self._stop_btn.state(["disabled"])

    # ── config panel (right, on demand) ────────────────────────────────

    def _open_config_panel(self, _event=None):
        idx = self._get_selected_index()
        if idx is None:
            return
        step_name = self._steps[idx][0]
        self._selected_idx = idx

        # clear old config widgets
        for w in self._config_frame.winfo_children():
            w.destroy()

        # show panel if hidden
        if not self._config_panel_visible:
            self._right_pane.add(self._config_frame, weight=0)
            self._config_panel_visible = True

        self._build_config_fields(step_name)

    def _close_config_panel(self, _event=None):
        if self._config_panel_visible:
            self._save_current_config_fields()
            self._right_pane.forget(self._config_frame)
            self._config_panel_visible = False

    def _build_config_fields(self, step_name: str):
        """Build the config panel fields for *step_name*."""
        frame = self._config_frame
        frame.configure(width=280)

        # header
        header = ttk.Frame(frame, style="Surface.TFrame")
        header.pack(fill="x", padx=PAD, pady=(PAD, 0))
        ttk.Label(header, text="CONFIG", style="Surface.TLabel",
                  font=FONT_UI_BOLD).pack(side="left")
        close_btn = ttk.Button(header, text="✕", width=3,
                               command=self._close_config_panel)
        close_btn.pack(side="right")

        ttk.Separator(frame, orient="horizontal").pack(fill="x", padx=PAD,
                                                       pady=4)

        display = ctrl.STEP_DISPLAY.get(step_name, step_name)
        ttk.Label(frame, text=display, style="Surface.TLabel",
                  font=FONT_UI_BOLD).pack(anchor="w", padx=PAD)

        # scrollable area
        canvas = tk.Canvas(frame, bg=COLORS["surface"], highlightthickness=0)
        scrollbar = ttk.Scrollbar(frame, orient="vertical",
                                  command=canvas.yview)
        scroll_frame = ttk.Frame(canvas, style="Surface.TFrame")
        scroll_frame.bind("<Configure>",
                          lambda e: canvas.configure(
                              scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        canvas.pack(fill="both", expand=True, padx=PAD, pady=PAD)

        # enable mousewheel scrolling (scoped to canvas hover)
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        def _bind_wheel(_e):
            canvas.bind_all("<MouseWheel>", _on_mousewheel)
        def _unbind_wheel(_e):
            canvas.unbind_all("<MouseWheel>")
        canvas.bind("<Enter>", _bind_wheel)
        canvas.bind("<Leave>", _unbind_wheel)
        canvas.bind("<Destroy>", lambda _e: canvas.unbind_all("<MouseWheel>"))

        cfg = self._step_configs.get(step_name, {})
        self._config_widgets: dict[str, object] = {}

        # overwrite toggle (for applicable steps)
        if step_name in ctrl.STEPS_WITH_OVERWRITE:
            idx = self._selected_idx
            if idx is not None:
                ow_var = tk.BooleanVar(value=self._steps[idx][1])
                ow_cb = ttk.Checkbutton(scroll_frame, text="Overwrite",
                                        variable=ow_var,
                                        style="Surface.TCheckbutton")
                ow_cb.pack(anchor="w", pady=(0, PAD))
                self._config_widgets["__overwrite__"] = ow_var

        # Build fields based on step type
        if step_name == "backtest":
            self._build_backtest_config(scroll_frame, cfg)
        elif step_name == "backtest_set":
            self._build_backtest_set_config(scroll_frame, cfg)
        elif step_name == "Multivariate_Regression":
            self._build_regression_config(scroll_frame, cfg)
        else:
            self._build_generic_config(scroll_frame, cfg)

        # save button
        ttk.Button(frame, text="Save Config",
                   style="Accent.TButton",
                   command=self._save_current_config_fields
                   ).pack(padx=PAD, pady=(0, PAD))

        # bind Esc to close
        frame.bind_all("<Escape>", self._close_config_panel)

    def _build_generic_config(self, parent, cfg: dict):
        """Build entry fields for a flat config dict."""
        for key, value in cfg.items():
            if isinstance(value, dict):
                # nested dict — show as JSON text
                w = LabeledText(parent, label=key,
                                value=json.dumps(value, indent=2), height=3)
                w.pack(fill="x", pady=(0, PAD))
                self._config_widgets[key] = ("json", w)
            elif key.endswith("Database") or key == "Target_Database":
                w = DatabasePickerEntry(parent, label=key, value=str(value))
                w.pack(fill="x", pady=(0, PAD))
                self._config_widgets[key] = ("str", w)
            elif key.endswith("_file") or key.endswith("_Config") or key == "csv_file":
                w = FilePickerEntry(parent, label=key, value=str(value))
                w.pack(fill="x", pady=(0, PAD))
                self._config_widgets[key] = ("str", w)
            elif key == "xsd_file":
                w = FilePickerEntry(parent, label=key, value=str(value),
                                    filetypes=[("XSD files", "*.xsd"),
                                               ("All files", "*.*")])
                w.pack(fill="x", pady=(0, PAD))
                self._config_widgets[key] = ("str", w)
            else:
                w = LabeledEntry(parent, label=key, value=str(value))
                w.pack(fill="x", pady=(0, PAD))
                # detect numeric
                if isinstance(value, (int, float)):
                    self._config_widgets[key] = ("num", w)
                else:
                    self._config_widgets[key] = ("str", w)

    def _build_backtest_config(self, parent, cfg: dict):
        """Build backtest-specific config with portfolio grid."""
        w = DatabasePickerEntry(parent, label="Source_Database",
                                value=str(cfg.get("Source_Database", "")))
        w.pack(fill="x", pady=(0, PAD))
        self._config_widgets["Source_Database"] = ("str", w)

        for key in ("PerShare_Table", "Financial_Statements_Table",
                     "start_date", "end_date", "benchmark_ticker",
                     "output_file"):
            w = LabeledEntry(parent, label=key, value=str(cfg.get(key, "")))
            w.pack(fill="x", pady=(0, PAD))
            self._config_widgets[key] = ("str", w)

        w = LabeledEntry(parent, label="risk_free_rate",
                         value=str(cfg.get("risk_free_rate", 0.0)))
        w.pack(fill="x", pady=(0, PAD))
        self._config_widgets["risk_free_rate"] = ("num", w)

        # portfolio grid
        ttk.Label(parent, text="Portfolio:", style="Surface.TLabel",
                  font=FONT_UI_BOLD).pack(anchor="w", pady=(PAD, 2))
        portfolio = cfg.get("portfolio", {})
        self._portfolio_grid = PortfolioGrid(parent, portfolio=portfolio)
        self._portfolio_grid.pack(fill="x", pady=(0, PAD))
        self._config_widgets["portfolio"] = ("portfolio", self._portfolio_grid)

    def _build_backtest_set_config(self, parent, cfg: dict):
        """Build backtest-set config (CSV-driven, no portfolio)."""
        w = DatabasePickerEntry(parent, label="Source_Database",
                                value=str(cfg.get("Source_Database", "")))
        w.pack(fill="x", pady=(0, PAD))
        self._config_widgets["Source_Database"] = ("str", w)

        for key in ("PerShare_Table", "Financial_Statements_Table",
                     "benchmark_ticker", "output_dir"):
            w = LabeledEntry(parent, label=key, value=str(cfg.get(key, "")))
            w.pack(fill="x", pady=(0, PAD))
            self._config_widgets[key] = ("str", w)

        w = FilePickerEntry(parent, label="csv_file",
                            value=str(cfg.get("csv_file", "")),
                            filetypes=[("CSV files", "*.csv"),
                                       ("All files", "*.*")])
        w.pack(fill="x", pady=(0, PAD))
        self._config_widgets["csv_file"] = ("str", w)

        for key in ("risk_free_rate", "initial_capital"):
            val = cfg.get(key, 0.0)
            w = LabeledEntry(parent, label=key, value=str(val))
            w.pack(fill="x", pady=(0, PAD))
            self._config_widgets[key] = ("num", w)

    def _build_regression_config(self, parent, cfg: dict):
        """Build regression-specific config with SQL text area."""
        w = DatabasePickerEntry(parent, label="Source_Database",
                                value=str(cfg.get("Source_Database", "")))
        w.pack(fill="x", pady=(0, PAD))
        self._config_widgets["Source_Database"] = ("str", w)

        w = FilePickerEntry(parent, label="Output",
                            value=str(cfg.get("Output", "")))
        w.pack(fill="x", pady=(0, PAD))
        self._config_widgets["Output"] = ("str", w)

        # winsorize thresholds
        wt = cfg.get("winsorize_thresholds", {"lower": 0.05, "upper": 0.95})
        w = LabeledText(parent, label="winsorize_thresholds (JSON)",
                        value=json.dumps(wt, indent=2), height=3)
        w.pack(fill="x", pady=(0, PAD))
        self._config_widgets["winsorize_thresholds"] = ("json", w)

        # SQL query
        w = LabeledText(parent, label="SQL_Query",
                        value=str(cfg.get("SQL_Query", "")), height=6)
        w.pack(fill="x", pady=(0, PAD))
        self._config_widgets["SQL_Query"] = ("text", w)

    def _save_current_config_fields(self):
        """Read widget values back into ``_step_configs``."""
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
        logger.info(f"Config saved for: {step_name}")

    # ── step list actions ───────────────────────────────────────────────

    def _get_selected_index(self) -> int | None:
        sel = self._step_listbox.curselection()
        return sel[0] if sel else None

    def _on_step_select(self, _event=None):
        idx = self._get_selected_index()
        if idx is not None:
            self._selected_idx = idx
            self._open_config_panel()

    def _show_step_menu(self, event):
        try:
            self._step_listbox.selection_clear(0, "end")
            idx = self._step_listbox.nearest(event.y)
            self._step_listbox.selection_set(idx)
            self._selected_idx = idx
            self._step_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self._step_menu.grab_release()

    def _add_step(self):
        """Show a menu of available step types to add."""
        menu = tk.Menu(self._step_listbox, tearoff=0,
                       bg=COLORS["surface"], fg=COLORS["text"],
                       font=FONT_MONO,
                       activebackground=COLORS["highlight"])
        for sname in ctrl.ALL_STEP_NAMES:
            display = ctrl.STEP_DISPLAY.get(sname, sname)
            menu.add_command(label=display,
                             command=lambda s=sname: self._do_add_step(s))
        # show at button location
        btn = self._left.winfo_children()[-2]  # add_frame
        menu.tk_popup(btn.winfo_rootx(), btn.winfo_rooty() + btn.winfo_height())

    def _do_add_step(self, step_name: str):
        self._steps.append([step_name, False])
        if step_name not in self._step_configs:
            self._step_configs[step_name] = ctrl.get_default_config_for_step(
                step_name)
        self._refresh_step_listbox()
        self._update_main_info()
        logger.info(f"Added step: {ctrl.STEP_DISPLAY.get(step_name, step_name)}")

    def _remove_selected_step(self, _event=None):
        idx = self._get_selected_index()
        if idx is None:
            return
        removed = self._steps.pop(idx)
        self._close_config_panel()
        self._refresh_step_listbox()
        self._update_main_info()
        logger.info(f"Removed step: {removed[0]}")

    def _move_step_up(self, _event=None):
        idx = self._get_selected_index()
        if idx is None or idx == 0:
            return
        self._steps[idx], self._steps[idx - 1] = (
            self._steps[idx - 1], self._steps[idx])
        self._refresh_step_listbox()
        self._step_listbox.selection_set(idx - 1)
        return "break"

    def _move_step_down(self, _event=None):
        idx = self._get_selected_index()
        if idx is None or idx >= len(self._steps) - 1:
            return
        self._steps[idx], self._steps[idx + 1] = (
            self._steps[idx + 1], self._steps[idx])
        self._refresh_step_listbox()
        self._step_listbox.selection_set(idx + 1)
        return "break"

    # ── setup persistence ───────────────────────────────────────────────

    def load_config(self, cfg: dict, name: str = ""):
        self._setup_name = name
        self._steps = ctrl.build_steps_from_config(cfg)
        self._step_configs = ctrl.build_step_configs_from_config(cfg)
        self._close_config_panel()
        self._refresh_step_listbox()
        self._update_main_info()
        self._setup_label.configure(text=f"Pipeline: {name}")

    def new_setup(self, name: str):
        self._setup_name = name
        self._steps = []
        self._step_configs = {}
        self._close_config_panel()
        self._refresh_step_listbox()
        self._update_main_info()
        self._setup_label.configure(text=f"Pipeline: {name}")
        logger.info(f"New setup: {name}")

    def _save_setup(self):
        name = self._setup_name or "(active)"
        if name == "(active)":
            name = simpledialog.askstring("Save Setup", "Setup name:",
                                         parent=self)
            if not name:
                return
            self._setup_name = name
        cfg = ctrl.build_config_dict(self._steps, self._step_configs)
        ctrl.save_setup(name, cfg)
        ctrl.save_run_config(cfg)
        self._setup_label.configure(text=f"Pipeline: {name}")
        logger.info(f"Setup saved: {name}")

    def _load_setup(self):
        setups = ctrl.list_setups()
        if not setups:
            logger.info("No saved setups found")
            return

        win = tk.Toplevel(self.winfo_toplevel())
        win.title("Load Setup")
        win.geometry("350x300")
        win.configure(bg=COLORS["surface"])
        win.transient(self.winfo_toplevel())
        win.grab_set()

        ttk.Label(win, text="Select a setup:", style="Surface.TLabel"
                  ).pack(anchor="w", padx=PAD * 2, pady=(PAD * 2, 0))

        lb = tk.Listbox(win, bg=COLORS["surface"], fg=COLORS["text"],
                        font=FONT_MONO, selectbackground=COLORS["highlight"],
                        relief="flat", borderwidth=0, activestyle="none")
        lb.pack(fill="both", expand=True, padx=PAD * 2, pady=PAD)
        for s in setups:
            lb.insert("end", s)
        if setups:
            lb.selection_set(0)

        def _load():
            sel = lb.curselection()
            if not sel:
                return
            name = setups[sel[0]]
            win.destroy()
            cfg = ctrl.load_setup(name)
            self.load_config(cfg, name=name)
            logger.info(f"Loaded setup: {name}")

        lb.bind("<Double-1>", lambda _: _load())
        ttk.Button(win, text="Open", command=_load,
                   style="Accent.TButton").pack(pady=(0, PAD))

    def _new_setup(self):
        name = simpledialog.askstring("New Setup", "Setup name:", parent=self)
        if name:
            self.new_setup(name)

    # ── run / stop ──────────────────────────────────────────────────────

    def _get_enabled_steps(self) -> list[dict]:
        return [{"name": name, "overwrite": ow}
                for name, ow in self._steps]

    def _on_run(self):
        enabled = self._get_enabled_steps()
        if not enabled:
            logger.warning("No steps enabled — nothing to run")
            return

        # save config before running (so CLI stays compatible)
        cfg_dict = ctrl.build_config_dict(self._steps, self._step_configs)
        ctrl.save_run_config(cfg_dict)

        self._is_running = True
        self._cancel_event.clear()
        self._run_btn.state(["disabled"])
        self._stop_btn.state(["!disabled"])
        self._step_listbox.configure(state="disabled")

        start_time = time.time()
        steps_done = []
        steps_failed = []

        def _on_step_start(name):
            self.app.log_panel.append("INFO",
                f"▶ Starting step: {ctrl.STEP_DISPLAY.get(name, name)}")

        def _on_step_done(name):
            steps_done.append(name)

        def _on_step_error(name, exc):
            steps_failed.append(name)

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
            self._step_listbox.configure(state="normal")
            logger.info(
                f"Pipeline completed ({len(steps_done)} steps, "
                f"{elapsed:.1f}s)")

        def _on_error(exc):
            elapsed = time.time() - start_time
            self._is_running = False
            self._run_btn.state(["!disabled"])
            self._stop_btn.state(["disabled"])
            self._step_listbox.configure(state="normal")
            logger.error(
                f"Pipeline failed after {elapsed:.1f}s: {exc}")

        run_in_background(_do_run, on_done=_on_done, on_error=_on_error)

    def _on_stop(self):
        if self._is_running:
            self._cancel_event.set()
            logger.info("Stop requested — will halt after current step")

    def reapply_colors(self):
        """Re-apply theme colours to raw tk widgets."""
        t = COLORS
        self._step_listbox.configure(
            bg=t["surface"], fg=t["text"],
            selectbackground=t["highlight"],
        )
        self._step_menu.configure(
            bg=t["surface"], fg=t["text"],
            activebackground=t["highlight"],
        )
