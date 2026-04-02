"""Home page: saved setups list, New/Open buttons."""

import logging
import os
from datetime import datetime
import tkinter as tk
from tkinter import ttk

from ui_tk import controllers as ctrl
from ui_tk.style import COLORS, FONT_UI, FONT_UI_BOLD, FONT_HEADING, FONT_SUBHEAD, PAD

logger = logging.getLogger(__name__)


class HomePage(ttk.Frame):
    def __init__(self, parent, app, **kw):
        super().__init__(parent, **kw)
        self.app = app
        self._setups: list[str] = []

        # ── outer padding container ─────────────────────────────────────
        body = ttk.Frame(self)
        body.pack(fill="both", expand=True, padx=PAD * 5, pady=PAD * 3)

        # ── page header ─────────────────────────────────────────────────
        header = ttk.Frame(body)
        header.pack(fill="x", pady=(0, PAD * 2))
        ttk.Label(header, text="EDINET Pipeline Manager",
                  style="Heading.TLabel").pack(anchor="w")
        ttk.Label(header, text="Select a saved setup to load it, or create a new one.",
                  style="Subhead.TLabel").pack(anchor="w", pady=(4, 0))

        # ── section label ────────────────────────────────────────────────
        ttk.Label(body, text="SAVED SETUPS",
                  style="SectionHead.TLabel").pack(anchor="w", pady=(0, 4))

        # ── treeview card ────────────────────────────────────────────────
        card = ttk.Frame(body, style="Surface.TFrame")
        card.pack(fill="both", expand=True)

        self._tree = ttk.Treeview(
            card,
            columns=("name", "modified"),
            show="headings",
            selectmode="browse",
        )
        self._tree.heading("name",     text="Setup Name",     anchor="w")
        self._tree.heading("modified", text="Last Modified",  anchor="w")
        self._tree.column("name",     stretch=True,  minwidth=200, anchor="w")
        self._tree.column("modified", stretch=False, width=120,    anchor="w")

        _scroll = ttk.Scrollbar(card, orient="vertical",
                                command=self._tree.yview)
        self._tree.configure(yscrollcommand=_scroll.set)
        _scroll.pack(side="right", fill="y")
        self._tree.pack(fill="both", expand=True)

        self._tree.bind("<Double-1>",    lambda _: self._open_selected())
        self._tree.bind("<Return>",      lambda _: self._open_selected())

        # ── action row ───────────────────────────────────────────────────
        btn_row = ttk.Frame(body)
        btn_row.pack(fill="x", pady=(PAD * 2, 0))
        self._open_btn = ttk.Button(btn_row, text="Open Selected",
                                    command=self._open_selected,
                                    style="Accent.TButton")
        self._open_btn.pack(side="right")
        ttk.Button(btn_row, text="New Setup", style="Ghost.TButton",
                   command=self._new_setup).pack(side="right", padx=(0, PAD))

        self.bind_all("<Control-n>", lambda _: self._new_setup(), add="+")

        self._refresh_list()

    # ── list management ─────────────────────────────────────────────────

    def _refresh_list(self):
        self._tree.delete(*self._tree.get_children())
        self._setups = ctrl.list_setups()
        for name in self._setups:
            path = ctrl.SAVED_SETUPS_DIR / f"{name}.json"
            try:
                mtime = os.path.getmtime(path)
                date_str = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d")
            except OSError:
                date_str = ""
            self._tree.insert("", "end", values=(name, date_str))
        if self._setups:
            first = self._tree.get_children()[0]
            self._tree.selection_set(first)
            self._tree.focus(first)
        logger.info(f"Loaded {len(self._setups)} saved setups")

    def _get_selected_name(self) -> str | None:
        sel = self._tree.selection()
        if not sel:
            return None
        vals = self._tree.item(sel[0], "values")
        return vals[0] if vals else None

    # ── actions ──────────────────────────────────────────────────────────

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
        win.geometry("360x130")
        win.configure(bg=COLORS["surface"])
        win.transient(self.winfo_toplevel())
        win.grab_set()

        ttk.Label(win, text="Setup name:", style="Surface.TLabel"
                  ).pack(anchor="w", padx=PAD * 2, pady=(PAD * 2, 0))
        var = tk.StringVar()
        ent = ttk.Entry(win, textvariable=var, width=42)
        ent.pack(padx=PAD * 2, pady=PAD, fill="x")
        ent.focus_set()

        def _create(_e=None):
            name = var.get().strip()
            if not name:
                return
            win.destroy()
            self.app.switch_view("Orchestrator")
            orch = self.app._views.get("Orchestrator")
            if orch:
                orch.new_setup(name)

        ent.bind("<Return>", _create)
        win.bind("<Escape>", lambda _: win.destroy())
        btn_row = ttk.Frame(win, style="Surface.TFrame")
        btn_row.pack(fill="x", padx=PAD * 2, pady=(0, PAD))
        ttk.Button(btn_row, text="Create", command=_create,
                   style="Accent.TButton").pack(side="right")
        ttk.Button(btn_row, text="Cancel", style="Ghost.TButton",
                   command=win.destroy).pack(side="right", padx=(0, PAD))

    def reapply_colors(self):
        """Treeview is ttk — theme is applied globally via apply_theme."""
        pass
