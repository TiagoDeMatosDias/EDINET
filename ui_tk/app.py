"""EDINET Tk application bootstrap: root window, view switching, event loop."""

import logging
import queue
import sys
import tkinter as tk
from tkinter import ttk

from ui_tk.style import (
    COLORS, FONT_UI, FONT_UI_BOLD, FONT_HEADING, FONT_MONO, PAD,
    apply_theme, toggle_theme, is_dark,
)
from ui_tk.utils import QueueLogHandler, poll_events
from ui_tk.shared.widgets import LogPanel, RoundedButton, reapply_widget_tree

logger = logging.getLogger(__name__)

VIEW_NAMES = ["Home", "Orchestrator", "Data", "Screening", "Security Analysis"]


class App:
    """Top-level application controller.

    Owns the Tk root, the top bar, the log panel, and view switching.
    """

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("SHADE Research — EDINET")
        self.root.geometry("1100x750")
        self.root.minsize(900, 550)

        apply_theme(self.root)

        # ── state (must be set before any widget that triggers callbacks) ─
        self._views: dict[str, ttk.Frame] = {}
        self._active_view: str | None = None
        self._tab_buttons: dict[str, RoundedButton] = {}
        self._tab_indicators: dict[str, ttk.Frame] = {}

        # ── log queue (fed by QueueLogHandler) ──────────────────────────
        self.log_queue: queue.Queue = queue.Queue()
        self._setup_log_handler()

        # ── layout skeleton ─────────────────────────────────────────────
        # top bar (packed first so it sits at the top)
        self._build_top_bar()

        # bottom: log panel (always visible)
        self.log_panel = LogPanel(self.root)
        self.log_panel.pack(side="bottom", fill="x")

        # separator above log
        ttk.Separator(self.root, orient="horizontal").pack(side="bottom",
                                                           fill="x")

        # centre: view container (fill remaining space)
        self._view_container = ttk.Frame(self.root)
        self._view_container.pack(fill="both", expand=True)

        # ── keyboard shortcuts ──────────────────────────────────────────
        self.root.bind_all("<Control-Key-1>",
                           lambda _: self.switch_view("Home"))
        self.root.bind_all("<Control-Key-2>",
                           lambda _: self.switch_view("Orchestrator"))
        self.root.bind_all("<Control-Key-3>",
                           lambda _: self.switch_view("Data"))
        self.root.bind_all("<Control-Key-4>",
                           lambda _: self.switch_view("Screening"))
        self.root.bind_all("<Control-Key-5>",
                   lambda _: self.switch_view("Security Analysis"))

        # ── start polling ───────────────────────────────────────────────
        self.root.after(100, self._poll_logs)
        poll_events(self.root)

        # ── show home ──────────────────────────────────────────────────
        self.switch_view("Home")

        logger.info("Application started")

    # ── top bar ─────────────────────────────────────────────────────────

    def _build_top_bar(self):
        self._top_bar = ttk.Frame(self.root, style="TopBar.TFrame")
        self._top_bar.pack(side="top", fill="x")

        # branding
        brand = ttk.Label(self._top_bar, text="SHADE Research",
                          style="TopBar.Brand.TLabel")
        brand.pack(side="left", padx=(PAD * 2, PAD * 3), pady=(PAD + 2, PAD))

        # navigation tabs
        nav = ttk.Frame(self._top_bar, style="TopBar.TFrame")
        nav.pack(side="left", padx=PAD)

        for name in VIEW_NAMES:
            tab_frame = ttk.Frame(nav, style="TopBar.TFrame")
            tab_frame.pack(side="left", padx=2)

            btn = RoundedButton(
                tab_frame, text=name, style="TopBar.Tab.TButton",
                command=lambda n=name: self.switch_view(n),
            )
            btn.pack(side="top")

            # 2-pixel underline indicator
            indicator = ttk.Frame(tab_frame, height=2,
                                  style="TopBar.TFrame")
            indicator.pack(side="top", fill="x", padx=6)

            self._tab_buttons[name] = btn
            self._tab_indicators[name] = indicator

        # right side controls
        right = ttk.Frame(self._top_bar, style="TopBar.TFrame")
        right.pack(side="right", padx=PAD)

        self._theme_btn = RoundedButton(
            right, text="◑ Dark" if is_dark() else "◑ Light",
            style="TopBar.Icon.TButton", command=self._toggle_theme,
        )
        self._theme_btn.pack(side="right", padx=4, pady=4)

        self._api_btn = RoundedButton(right, text="⚿ API Key",
                                      style="TopBar.Icon.TButton",
                                      command=self._open_api_key)
        self._api_btn.pack(side="right", padx=4, pady=4)

        # 1-pixel bottom border
        tk.Frame(self.root, bg=COLORS["border"], height=1).pack(
            side="top", fill="x")

    def _update_tab_visuals(self):
        """Update tab button styles and underline indicators."""
        t = COLORS
        for name in VIEW_NAMES:
            btn = self._tab_buttons[name]
            ind = self._tab_indicators[name]
            if name == self._active_view:
                btn.configure(style="TopBar.TabActive.TButton")
                ind.configure(style="Accent.TFrame")
                # Ensure accent frame style exists
                ttk.Style(self.root).configure(
                    "Accent.TFrame", background=t["accent"])
            else:
                btn.configure(style="TopBar.Tab.TButton")
                ind.configure(style="TopBar.TFrame")

    # ── view switching ──────────────────────────────────────────────────

    def switch_view(self, name: str):
        if name == self._active_view:
            return

        # hide current
        if self._active_view and self._active_view in self._views:
            self._views[self._active_view].pack_forget()

        # create lazily
        if name not in self._views:
            self._views[name] = self._create_view(name)

        self._views[name].pack(in_=self._view_container,
                               fill="both", expand=True)
        self._active_view = name
        self._update_tab_visuals()

    def show_security_analysis(self, record: dict, db_path: str | None = None):
        """Switch to Security Analysis and open the selected company."""
        self.switch_view("Security Analysis")
        view = self._views.get("Security Analysis")
        if view is None:
            return
        open_security = getattr(view, "open_security", None)
        if callable(open_security):
            open_security(record, db_path=db_path)

    def _create_view(self, name: str) -> ttk.Frame:
        if name == "Home":
            from ui_tk.pages.home import HomePage
            return HomePage(self._view_container, self)
        elif name == "Orchestrator":
            from ui_tk.pages.orchestrator import OrchestratorPage
            return OrchestratorPage(self._view_container, self)
        elif name == "Screening":
            from ui_tk.pages.screening import ScreeningPage
            return ScreeningPage(self._view_container, self)
        elif name == "Security Analysis":
            from ui_tk.pages.security_analysis import SecurityAnalysisPage
            return SecurityAnalysisPage(self._view_container, self)
        else:
            from ui_tk.pages.data import DataPage
            return DataPage(self._view_container)

    # ── theme toggle ────────────────────────────────────────────────────

    def _toggle_theme(self):
        mode = toggle_theme(self.root)
        self._theme_btn.configure(
            text=f"◑ {'Dark' if mode == 'dark' else 'Light'}")
        self._rebuild_dynamic_widgets()

    def _rebuild_dynamic_widgets(self):
        """Re-apply colours to widgets that use raw tk (not ttk styles)."""
        t = COLORS
        # top bar children are ttk — handled by apply_theme
        # log panel text area
        if hasattr(self, 'log_panel'):
            self.log_panel.reapply_colors()
        self._theme_btn.reapply_colors()
        self._api_btn.reapply_colors()
        for button in self._tab_buttons.values():
            button.reapply_colors()
        # update tab visuals
        self._update_tab_visuals()
        # propagate to views
        for view in self._views.values():
            if hasattr(view, 'reapply_colors'):
                view.reapply_colors()
            reapply_widget_tree(view)

    # ── API Key dialog ──────────────────────────────────────────────────

    def _open_api_key(self):
        from ui_tk import controllers as ctrl
        t = COLORS
        win = tk.Toplevel(self.root)
        win.title("API Key")
        win.geometry("420x160")
        win.configure(bg=t["surface"])
        win.transient(self.root)
        win.grab_set()

        ttk.Label(win, text="EDINET API Key:", style="Surface.TLabel"
                  ).pack(anchor="w", padx=PAD * 2, pady=(PAD * 2, 0))
        var = tk.StringVar(value=ctrl.get_api_key())
        ent = ttk.Entry(win, textvariable=var, width=50, show="•")
        ent.pack(padx=PAD * 2, pady=PAD, fill="x")
        ent.focus_set()

        def _save():
            ctrl.save_api_key(var.get())
            logger.info("API key saved")
            win.destroy()

        btn_row = ttk.Frame(win, style="Surface.TFrame")
        btn_row.pack(fill="x", padx=PAD * 2, pady=PAD)
        RoundedButton(btn_row, text="Save", command=_save,
                  style="Accent.TButton").pack(side="right")
        RoundedButton(btn_row, text="Cancel", style="Ghost.TButton",
                  command=win.destroy).pack(side="right", padx=(0, PAD))

        win.bind("<Return>", lambda _: _save())
        win.bind("<Escape>", lambda _: win.destroy())

    # ── log polling ─────────────────────────────────────────────────────

    def _poll_logs(self):
        try:
            while True:
                kind, level, msg = self.log_queue.get_nowait()
                if kind == "log":
                    self.log_panel.append(level, msg)
        except queue.Empty:
            pass
        self.root.after(100, self._poll_logs)

    # ── logging handler ─────────────────────────────────────────────────

    def _setup_log_handler(self):
        handler = QueueLogHandler(self.log_queue)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            fmt="%(asctime)s  %(levelname)-5s  %(message)s",
            datefmt="%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)

        # Redirect print() calls (used by legacy backend modules) into the
        # logging system so they appear in both the log file and the UI panel.
        self._orig_stdout = sys.stdout
        sys.stdout = _PrintToLogger(logging.getLogger("stdout"))


class _PrintToLogger:
    """File-like wrapper that forwards ``print()`` output to a logger."""

    def __init__(self, logger: logging.Logger, *, orig=None):
        self._logger = logger
        self._orig = orig or sys.__stdout__
        self._buf = ""

    def write(self, text: str):
        self._orig.write(text)          # keep normal console output
        if not text:
            return
        self._buf += text
        # Emit a log record for every complete line in the buffer
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            line = line.strip()
            if line:
                self._logger.info(line)

    def flush(self):
        if self._buf:
            line = self._buf.strip()
            if line:
                self._logger.info(line)
            self._buf = ""
        self._orig.flush()


def run_tk_app():
    """Entry point: create the Tk root and start the mainloop."""
    root = tk.Tk()
    _app = App(root)
    root.mainloop()
