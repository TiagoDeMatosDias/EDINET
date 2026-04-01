"""Home page: saved setups list, New/Open buttons."""

import logging
import os
import tkinter as tk
from tkinter import ttk

from ui_tk import controllers as ctrl
from ui_tk.style import COLORS, FONT_UI, FONT_UI_BOLD, FONT_HEADING, PAD

logger = logging.getLogger(__name__)


class HomePage(ttk.Frame):
    def __init__(self, parent, app, **kw):
        super().__init__(parent, **kw)
        self.app = app

        # ── title ───────────────────────────────────────────────────────
        ttk.Label(self, text="EDINET Pipeline Manager",
                  style="Heading.TLabel").pack(pady=(PAD * 3, PAD * 2))

        # ── setups list ─────────────────────────────────────────────────
        ttk.Label(self, text="Saved Setups:",
                  style="Accent.TLabel").pack(anchor="w", padx=PAD * 4)

        list_frame = ttk.Frame(self, style="Surface.TFrame")
        list_frame.pack(fill="both", expand=True,
                        padx=PAD * 4, pady=(PAD // 2, PAD))

        self.setup_list = tk.Listbox(
            list_frame, bg=COLORS["surface"], fg=COLORS["text"],
            font=FONT_UI, selectbackground=COLORS["highlight"],
            selectforeground="#ffffff", relief="flat", borderwidth=0,
            highlightthickness=1, highlightbackground=COLORS["border"],
            highlightcolor=COLORS["accent"],
            activestyle="none",
        )
        self.setup_list.pack(fill="both", expand=True, padx=2, pady=2)
        self.setup_list.bind("<Double-1>", lambda _: self._open_selected())
        self.setup_list.bind("<Return>", lambda _: self._open_selected())

        # ── buttons ─────────────────────────────────────────────────────
        btn_row = ttk.Frame(self)
        btn_row.pack(pady=PAD * 2)
        self._new_btn = ttk.Button(btn_row, text="New Setup",
                                   command=self._new_setup)
        self._new_btn.pack(side="left", padx=PAD)
        self._open_btn = ttk.Button(btn_row, text="Open Selected",
                                    command=self._open_selected,
                                    style="Accent.TButton")
        self._open_btn.pack(side="left", padx=PAD)

        # keyboard: Ctrl+N for new
        self.bind_all("<Control-n>", lambda _: self._new_setup(), add="+")

        self._refresh_list()

    def _refresh_list(self):
        self.setup_list.delete(0, "end")
        setups = ctrl.list_setups()
        for name in setups:
            # try to get file modification date
            path = ctrl.SAVED_SETUPS_DIR / f"{name}.json"
            try:
                mtime = os.path.getmtime(path)
                from datetime import datetime
                date_str = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d")
            except OSError:
                date_str = ""
            display = f"  {name} {'·' * max(1, 40 - len(name))} {date_str}"
            self.setup_list.insert("end", display)
        if setups:
            self.setup_list.selection_set(0)
        logger.info(f"Loaded {len(setups)} saved setups")

    def _get_selected_name(self) -> str | None:
        sel = self.setup_list.curselection()
        if not sel:
            return None
        setups = ctrl.list_setups()
        idx = sel[0]
        if idx < len(setups):
            return setups[idx]
        return None

    def _open_selected(self):
        name = self._get_selected_name()
        if not name:
            return
        # load setup and switch to orchestrator
        cfg = ctrl.load_setup(name)
        self.app.switch_view("Orchestrator")
        orch = self.app._views.get("Orchestrator")
        if orch:
            orch.load_config(cfg, name=name)

    def _new_setup(self):
        win = tk.Toplevel(self.winfo_toplevel())
        win.title("New Setup")
        win.geometry("350x120")
        win.configure(bg=COLORS["surface"])
        win.transient(self.winfo_toplevel())
        win.grab_set()

        ttk.Label(win, text="Setup name:", style="Surface.TLabel"
                  ).pack(anchor="w", padx=PAD * 2, pady=(PAD * 2, 0))
        var = tk.StringVar()
        ent = ttk.Entry(win, textvariable=var, width=40)
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
        ttk.Button(win, text="Create", command=_create,
                   style="Accent.TButton").pack(pady=(0, PAD))

    def reapply_colors(self):
        """Re-apply theme colours to raw tk widgets."""
        t = COLORS
        self.setup_list.configure(
            bg=t["surface"], fg=t["text"],
            selectbackground=t["highlight"],
            highlightbackground=t["border"],
            highlightcolor=t["accent"],
        )
