"""Home page: saved setups, workflow entry points, and project summary."""

import logging
import os
from datetime import datetime
import tkinter as tk
from tkinter import ttk

from ui_tk import controllers as ctrl
from ui_tk.shared.widgets import EmptyState, PageHeader, RoundedButton, SectionCard, StatTile
from ui_tk.style import COLORS, FONT_SMALL, PAD

logger = logging.getLogger(__name__)


class HomePage(ttk.Frame):
    def __init__(self, parent, app, **kw):
        super().__init__(parent, style="App.TFrame", **kw)
        self.app = app
        self._setups: list[str] = []
        self._latest_modified_var = tk.StringVar(value="No saved setups yet")

        body = ttk.Frame(self, style="App.TFrame")
        body.pack(fill="both", expand=True, padx=PAD * 2, pady=PAD * 2)
        body.grid_columnconfigure(0, weight=3)
        body.grid_columnconfigure(1, weight=2)
        body.grid_rowconfigure(2, weight=1)

        self._header = PageHeader(
            body,
            title="Research Workspace",
            subtitle="Open a saved pipeline, create a new one, or jump directly into a research surface.",
            context="The home view is now a launchpad rather than a blank list.",
        )
        self._header.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, PAD * 2))

        hero = ttk.Frame(body, style="Hero.TFrame")
        hero.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, PAD * 2))
        for column in range(3):
            hero.grid_columnconfigure(column, weight=1)

        self._saved_count_tile = StatTile(hero, "Saved Pipelines", "0", "Workflows ready to load", style="Hero.TFrame")
        self._saved_count_tile.grid(row=0, column=0, sticky="nsew", padx=(0, PAD // 2), pady=PAD // 2)
        self._latest_tile = StatTile(hero, "Latest Activity", "No recent saves", "Most recent setup timestamp", style="Hero.TFrame")
        self._latest_tile.grid(row=0, column=1, sticky="nsew", padx=PAD // 2, pady=PAD // 2)
        self._workspace_tile = StatTile(hero, "Workspace", "EDINET", "Desktop research shell", style="Hero.TFrame")
        self._workspace_tile.grid(row=0, column=2, sticky="nsew", padx=(PAD // 2, 0), pady=PAD // 2)

        self._setups_card = SectionCard(
            body,
            "Saved Setups",
            "Ordered workflows you can load into the pipeline builder.",
        )
        self._setups_card.grid(row=2, column=0, sticky="nsew", padx=(0, PAD))
        self._build_setups_surface(self._setups_card.body)

        right_col = ttk.Frame(body, style="App.TFrame")
        right_col.grid(row=2, column=1, sticky="nsew")
        right_col.grid_rowconfigure(1, weight=1)

        self._quick_card = SectionCard(
            right_col,
            "Workflow Surfaces",
            "Move into the task area you need without opening a setup first.",
        )
        self._quick_card.grid(row=0, column=0, sticky="ew", pady=(0, PAD))
        self._build_quick_actions(self._quick_card.body)

        self._notes_card = SectionCard(
            right_col,
            "Working Notes",
            "Keyboard hints and the intended flow through the application.",
        )
        self._notes_card.grid(row=1, column=0, sticky="nsew")
        self._build_notes_panel(self._notes_card.body)

        self.bind_all("<Control-n>", lambda _event: self._new_setup(), add="+")
        self._refresh_list()

    def _build_setups_surface(self, parent):
        action_row = ttk.Frame(parent, style="Panel.TFrame")
        action_row.pack(fill="x", pady=(0, PAD))
        self._new_btn = RoundedButton(action_row, text="New Setup", style="Ghost.TButton", command=self._new_setup)
        self._new_btn.pack(side="left")
        self._open_btn = RoundedButton(action_row, text="Open Selected", style="Accent.TButton", command=self._open_selected)
        self._open_btn.pack(side="right")

        self._tree_wrap = ttk.Frame(parent, style="Panel.TFrame")
        self._tree_wrap.pack(fill="both", expand=True)
        self._empty_state = EmptyState(
            self._tree_wrap,
            "No Saved Pipelines",
            "Create a setup to start building orchestrated EDINET workflows. Saved setups will appear here for quick reopening.",
        )

        self._tree = ttk.Treeview(self._tree_wrap, columns=("name", "modified"), show="headings", selectmode="browse")
        self._tree.heading("name", text="Setup Name", anchor="w")
        self._tree.heading("modified", text="Last Modified", anchor="w")
        self._tree.column("name", stretch=True, minwidth=220, anchor="w")
        self._tree.column("modified", stretch=False, width=140, anchor="w")
        scroll = ttk.Scrollbar(self._tree_wrap, orient="vertical", command=self._tree.yview)
        self._tree.configure(yscrollcommand=scroll.set)
        self._tree.bind("<Double-1>", lambda _event: self._open_selected())
        self._tree.bind("<Return>", lambda _event: self._open_selected())
        scroll.pack(side="right", fill="y")
        self._tree.pack(side="left", fill="both", expand=True)

    def _build_quick_actions(self, parent):
        actions = [
            ("Open Pipeline Builder", "Orchestrator", "Configure ordered workflows and run them."),
            ("Start Screening", "Screening", "Build a ranked query against financial data."),
            ("Open Security Analysis", "Security Analysis", "Inspect one company in detail."),
            ("Review Data Workspace", "Data", "Check project resources, data folders, and references."),
        ]
        self._quick_buttons: list[RoundedButton] = []
        for text, target, blurb in actions:
            row = ttk.Frame(parent, style="Panel.TFrame")
            row.pack(fill="x", pady=(0, PAD))
            info = ttk.Frame(row, style="Panel.TFrame")
            info.pack(side="left", fill="x", expand=True)
            ttk.Label(info, text=text, style="Panel.TLabel").pack(anchor="w")
            ttk.Label(info, text=blurb, style="Panel.TLabel", font=FONT_SMALL).pack(anchor="w", pady=(2, 0))
            button = RoundedButton(row, text="Open", style="Ghost.TButton", command=lambda name=target: self.app.switch_view(name))
            button.pack(side="right")
            self._quick_buttons.append(button)

    def _build_notes_panel(self, parent):
        lines = [
            "1. Build or load a pipeline in Orchestrator.",
            "2. Use Screening to generate candidate sets.",
            "3. Jump into Security Analysis from any screening result.",
            "4. Keep the console open while running data-heavy operations.",
            "",
            "Shortcuts",
            "Ctrl+1..5 switch top-level views.",
            "Ctrl+N creates a new setup from Home.",
            "F5 runs the active pipeline inside Orchestrator.",
        ]
        ttk.Label(parent, text="\n".join(lines), style="Panel.TLabel", justify="left").pack(anchor="w")

    def _refresh_list(self):
        self._tree.delete(*self._tree.get_children())
        self._setups = ctrl.list_setups()
        latest_mtime = None
        latest_name = ""
        for name in self._setups:
            path = ctrl.SAVED_SETUPS_DIR / f"{name}.json"
            try:
                mtime = os.path.getmtime(path)
                date_str = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")
                if latest_mtime is None or mtime > latest_mtime:
                    latest_mtime = mtime
                    latest_name = name
            except OSError:
                date_str = ""
            self._tree.insert("", "end", values=(name, date_str))

        if self._setups:
            first = self._tree.get_children()[0]
            self._tree.selection_set(first)
            self._tree.focus(first)
            self._empty_state.pack_forget()
            self._tree.pack(side="left", fill="both", expand=True)
        else:
            self._tree.pack_forget()
            self._empty_state.pack(fill="both", expand=True)

        latest_text = "No recent saves" if latest_mtime is None else latest_name
        latest_meta = "Most recent setup timestamp" if latest_mtime is None else datetime.fromtimestamp(latest_mtime).strftime("%Y-%m-%d %H:%M")
        self._saved_count_tile.set(str(len(self._setups)), "Saved workflows available")
        self._latest_tile.set(latest_text, latest_meta)
        self.app.set_context("Home", f"{len(self._setups)} saved setup{'s' if len(self._setups) != 1 else ''} available.")
        logger.info("Loaded %d saved setups", len(self._setups))

    def _get_selected_name(self) -> str | None:
        selection = self._tree.selection()
        if not selection:
            return None
        values = self._tree.item(selection[0], "values")
        return values[0] if values else None

    def _open_selected(self):
        name = self._get_selected_name()
        if not name:
            return
        cfg = ctrl.load_setup(name)
        self.app.switch_view("Orchestrator")
        orch = self.app._views.get("Orchestrator")
        if orch:
            orch.load_config(cfg, name=name)

    def _new_setup(self):
        win = tk.Toplevel(self.winfo_toplevel())
        win.title("New Setup")
        win.geometry("380x150")
        win.configure(bg=COLORS["surface"])
        win.transient(self.winfo_toplevel())
        win.grab_set()

        ttk.Label(win, text="Setup name", style="Surface.TLabel").pack(anchor="w", padx=PAD * 2, pady=(PAD * 2, 0))
        var = tk.StringVar()
        entry = ttk.Entry(win, textvariable=var, width=42)
        entry.pack(padx=PAD * 2, pady=PAD, fill="x")
        entry.focus_set()

        def _create(_event=None):
            name = var.get().strip()
            if not name:
                return
            win.destroy()
            self.app.switch_view("Orchestrator")
            orch = self.app._views.get("Orchestrator")
            if orch:
                orch.new_setup(name)

        entry.bind("<Return>", _create)
        win.bind("<Escape>", lambda _event: win.destroy())
        buttons = ttk.Frame(win, style="Surface.TFrame")
        buttons.pack(fill="x", padx=PAD * 2, pady=(0, PAD))
        RoundedButton(buttons, text="Create", command=_create, style="Accent.TButton").pack(side="right")
        RoundedButton(buttons, text="Cancel", command=win.destroy, style="Ghost.TButton").pack(side="right", padx=(0, PAD))

    def reapply_colors(self):
        self._new_btn.reapply_colors()
        self._open_btn.reapply_colors()
        for button in getattr(self, "_quick_buttons", []):
            button.reapply_colors()
