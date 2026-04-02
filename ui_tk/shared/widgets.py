"""Reusable widgets for the EDINET Tk UI."""

import os
import re
import subprocess
import tkinter as tk
from tkinter import ttk

try:
    import customtkinter as ctk
except ImportError:
    ctk = None

from ui_tk.style import (
    BUTTON_RADIUS,
    BUTTON_RADIUS_SMALL,
    COLORS,
    FONT_UI,
    FONT_UI_BOLD,
    FONT_TOPBAR_ACTION,
    FONT_TOPBAR_NAV,
    FONT_MONO,
    FONT_SMALL,
    PAD,
)

# Matches file/folder paths such as:
#   data/results\summary.txt   C:\foo\bar.csv   ./output/file.log
# Requires the path to start with a drive letter (C:\), a dot-slash (./),
# or a directory-style word immediately followed by / or \.
_PATH_RE = re.compile(
    r'(?:[A-Za-z]:[\\\/]|\.{1,2}[\\\/])?'   # optional drive or dot-slash prefix
    r'[A-Za-z0-9_.\\-]+'                      # first directory component (no spaces)
    r'(?:[\\\/][A-Za-z0-9_.\\-]+)+'           # one or more / or \ separated components
)


def _button_tokens(style_name: str) -> dict[str, object]:
    t = COLORS
    tokens = {
        "fg_color": t["surface_alt"],
        "hover_color": t["border"],
        "text_color": t["text"],
        "border_color": t["surface_alt"],
        "border_width": 0,
        "corner_radius": BUTTON_RADIUS,
        "height": 36,
        "font": FONT_UI,
    }
    if style_name == "Accent.TButton":
        tokens.update({
            "fg_color": t["accent"],
            "hover_color": t["accent_hover"],
            "text_color": "#ffffff",
            "border_color": t["accent"],
            "font": FONT_UI_BOLD,
        })
    elif style_name == "Danger.TButton":
        tokens.update({
            "fg_color": t["error"],
            "hover_color": "#E06480",
            "text_color": "#ffffff",
            "border_color": t["error"],
            "font": FONT_UI_BOLD,
        })
    elif style_name == "Ghost.TButton":
        tokens.update({
            "fg_color": "transparent",
            "hover_color": t["surface_alt"],
            "text_color": t["text"],
            "border_color": t["border"],
            "border_width": 1,
        })
    elif style_name == "Icon.TButton":
        tokens.update({
            "fg_color": "transparent",
            "hover_color": t["surface_alt"],
            "text_color": t["text"],
            "border_color": t["surface"],
            "border_width": 0,
            "corner_radius": BUTTON_RADIUS_SMALL,
            "height": 34,
            "font": FONT_UI,
        })
    elif style_name == "TopBar.Icon.TButton":
        tokens.update({
            "fg_color": "transparent",
            "hover_color": t["surface_alt"],
            "text_color": t["text"],
            "border_color": t["surface"],
            "border_width": 0,
            "corner_radius": BUTTON_RADIUS_SMALL,
            "height": 38,
            "font": FONT_TOPBAR_ACTION,
        })
    elif style_name == "Small.TButton":
        tokens.update({
            "fg_color": t["surface_alt"],
            "hover_color": t["border"],
            "text_color": t["text"],
            "border_color": t["surface_alt"],
            "corner_radius": BUTTON_RADIUS_SMALL,
            "height": 30,
            "font": FONT_SMALL,
        })
    elif style_name == "Tab.TButton":
        tokens.update({
            "fg_color": "transparent",
            "hover_color": t["surface_alt"],
            "text_color": t["tab_inactive"],
            "border_color": t["surface"],
            "border_width": 0,
            "corner_radius": BUTTON_RADIUS_SMALL,
            "height": 34,
            "font": FONT_UI,
        })
    elif style_name == "TabActive.TButton":
        tokens.update({
            "fg_color": t["surface_alt"],
            "hover_color": t["surface_alt"],
            "text_color": t["tab_active"],
            "border_color": t["surface_alt"],
            "corner_radius": BUTTON_RADIUS_SMALL,
            "height": 34,
            "font": FONT_UI_BOLD,
        })
    elif style_name == "TopBar.Tab.TButton":
        tokens.update({
            "fg_color": "transparent",
            "hover_color": t["surface_alt"],
            "text_color": t["text"],
            "border_color": t["surface"],
            "border_width": 0,
            "corner_radius": BUTTON_RADIUS_SMALL,
            "height": 40,
            "font": FONT_TOPBAR_NAV,
        })
    elif style_name == "TopBar.TabActive.TButton":
        tokens.update({
            "fg_color": t["surface_alt"],
            "hover_color": t["surface_alt"],
            "text_color": t["tab_active"],
            "border_color": t["surface_alt"],
            "corner_radius": BUTTON_RADIUS_SMALL,
            "height": 40,
            "font": FONT_TOPBAR_NAV,
        })
    return tokens


def _coerce_button_width(width: object) -> object:
    if isinstance(width, int) and width <= 10:
        return max(36, width * 12)
    return width


def _detect_bg(widget) -> str:
    """Walk up the widget tree to find the actual background color.

    Handles both tk and ttk widgets.  Falls back to COLORS["bg"].
    """
    w = widget
    while w is not None:
        # tk widgets expose bg directly
        try:
            bg = w.cget("bg")
            if bg and bg != "SystemButtonFace":
                return str(bg)
        except (tk.TclError, AttributeError):
            pass
        # ttk widgets need style lookup
        try:
            style_name = str(w.cget("style") or "")
            if not style_name:
                style_name = w.winfo_class()
            s = ttk.Style()
            bg = s.lookup(style_name, "background")
            if bg:
                return str(bg)
        except (tk.TclError, AttributeError):
            pass
        w = w.master
    return COLORS["bg"]


class RoundedButton(tk.Frame):
    """Rounded button using CTkButton when available, ttk fallback otherwise.

    Uses a plain tk.Frame (not ttk.Frame) so that CTkButton's
    ``bg_color="transparent"`` background detection works correctly.
    """

    def __init__(self, parent, text="", command=None, style="TButton", **kw):
        parent_bg = _detect_bg(parent)
        super().__init__(parent, bg=parent_bg, highlightthickness=0, bd=0)
        self._style_name = style
        self._is_disabled = False
        self._parent_bg = parent_bg
        width = kw.pop("width", None)
        self._inner = None

        if ctk is None:
            self._inner = ttk.Button(self, text=text, command=command,
                                     style=style, width=width, **kw)
            self._inner.pack(fill="both", expand=True)
            return

        tokens = _button_tokens(style)
        ctk_width = _coerce_button_width(width)
        self._inner = ctk.CTkButton(
            self,
            text=text,
            command=command,
            width=ctk_width or 0,
            height=tokens["height"],
            corner_radius=tokens["corner_radius"],
            fg_color=tokens["fg_color"],
            hover_color=tokens["hover_color"],
            text_color=tokens["text_color"],
            border_color=tokens["border_color"],
            border_width=tokens["border_width"],
            bg_color=parent_bg,
            font=tokens["font"],
            textvariable=kw.pop("textvariable", None),
            anchor=kw.pop("anchor", "center"),
        )
        self._inner.pack(fill="both", expand=True)

    def configure(self, cnf=None, **kw):
        if cnf:
            kw.update(cnf)
        style = kw.pop("style", None)
        if style is not None:
            self._style_name = style
        if ctk is None:
            if style is not None:
                kw["style"] = style
            self._inner.configure(**kw)
            return

        if "width" in kw:
            kw["width"] = _coerce_button_width(kw["width"])

        if style is not None:
            self.reapply_colors()

        if "state" in kw:
            self._is_disabled = kw["state"] == "disabled"
        self._inner.configure(**kw)

    config = configure

    def state(self, statespec=None):
        if statespec is None:
            return ("disabled",) if self._is_disabled else ("!disabled",)
        disabled = any(state == "disabled" for state in statespec)
        enabled = any(state == "!disabled" for state in statespec)
        if disabled:
            self._is_disabled = True
        elif enabled:
            self._is_disabled = False

        if ctk is None:
            self._inner.state(statespec)
            return self._inner.state()

        self._inner.configure(state="disabled" if self._is_disabled else "normal")
        return self.state()

    def reapply_colors(self):
        parent_bg = _detect_bg(self.master)
        self._parent_bg = parent_bg
        self.configure_frame(bg=parent_bg)
        if ctk is None:
            self._inner.configure(style=self._style_name)
            return
        tokens = _button_tokens(self._style_name)
        self._inner.configure(
            height=tokens["height"],
            corner_radius=tokens["corner_radius"],
            fg_color=tokens["fg_color"],
            hover_color=tokens["hover_color"],
            text_color=tokens["text_color"],
            border_color=tokens["border_color"],
            border_width=tokens["border_width"],
            bg_color=parent_bg,
            font=tokens["font"],
        )
        self._inner.configure(state="disabled" if self._is_disabled else "normal")

    def configure_frame(self, **kw):
        """Configure the underlying tk.Frame itself (not the inner button)."""
        super().configure(**kw)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def reapply_widget_tree(widget):
    for child in widget.winfo_children():
        if hasattr(child, "reapply_colors"):
            child.reapply_colors()
        reapply_widget_tree(child)


class LogPanel(ttk.Frame):
    """Full-width log output panel.

    Displays colour-coded log lines by level with auto-scroll, clear,
    export, and filter controls.
    """

    LEVEL_TAG = {
        "DEBUG":    "debug",
        "INFO":     "info",
        "WARNING":  "warning",
        "ERROR":    "error",
        "CRITICAL": "error",
    }

    def __init__(self, parent, **kw):
        super().__init__(parent, **kw)
        self._auto_scroll = True
        self._filter_level = "ALL"
        self._all_records: list[tuple[str, str]] = []  # (level, text)

        # ── toolbar ─────────────────────────────────────────────────────
        toolbar = ttk.Frame(self)
        toolbar.pack(fill="x", padx=PAD, pady=(PAD // 2, 0))

        ttk.Label(toolbar, text="Log", style="Accent.TLabel").pack(side="left")
        sep = ttk.Separator(toolbar, orient="horizontal")
        sep.pack(side="left", fill="x", expand=True, padx=PAD)

        self._filter_var = tk.StringVar(value="All")
        filt = ttk.Combobox(toolbar, textvariable=self._filter_var,
                            values=["All", "Info", "Warning", "Error"],
                            width=8, state="readonly")
        filt.pack(side="right", padx=(PAD // 2, 0))
        filt.bind("<<ComboboxSelected>>", self._on_filter_changed)
        ttk.Label(toolbar, text="Filter:").pack(side="right")

        self._autoscroll_var = tk.BooleanVar(value=True)
        cb = ttk.Checkbutton(toolbar, text="Auto-scroll",
                             variable=self._autoscroll_var,
                             command=self._on_autoscroll_toggle)
        cb.pack(side="right", padx=PAD)

        self._export_btn = RoundedButton(toolbar, text="Export",
                         style="Small.TButton",
                         command=self._on_export)
        self._export_btn.pack(side="right", padx=2)
        self._clear_btn = RoundedButton(toolbar, text="Clear",
                        style="Small.TButton",
                        command=self.clear)
        self._clear_btn.pack(side="right", padx=2)

        # ── text area ──────────────────────────────────────────────────
        text_frame = ttk.Frame(self)
        text_frame.pack(fill="both", expand=True, padx=PAD, pady=(2, PAD))

        self.text = tk.Text(text_frame, wrap="word", state="disabled",
                            bg=COLORS["log_bg"], fg=COLORS["text"],
                            font=FONT_MONO, insertbackground=COLORS["text"],
                            selectbackground=COLORS["highlight"],
                            relief="flat", borderwidth=0, padx=6, pady=4,
                            height=8)
        scrollbar = ttk.Scrollbar(text_frame, orient="vertical",
                                  command=self.text.yview)
        self.text.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        self.text.pack(side="left", fill="both", expand=True)

        self._apply_text_tags()

        # track user scroll
        self.text.bind("<MouseWheel>", self._on_user_scroll)
        self.text.bind("<Button-4>", self._on_user_scroll)
        self.text.bind("<Button-5>", self._on_user_scroll)

    # ── public API ──────────────────────────────────────────────────────

    def append(self, level: str, text: str):
        """Append a log line. Safe to call from any thread via the event queue."""
        self._all_records.append((level, text))
        if self._matches_filter(level):
            self._insert_line(level, text)

    def clear(self):
        self._all_records.clear()
        self.text.configure(state="normal")
        self.text.delete("1.0", "end")
        self.text.configure(state="disabled")

    def reapply_colors(self):
        """Re-apply theme colours after a theme toggle."""
        t = COLORS
        self.text.configure(bg=t["log_bg"], fg=t["text"],
                            insertbackground=t["text"],
                            selectbackground=t["highlight"])
        self._export_btn.reapply_colors()
        self._clear_btn.reapply_colors()
        self._apply_text_tags()

    # ── internals ───────────────────────────────────────────────────────

    def _apply_text_tags(self):
        t = COLORS
        self.text.tag_configure("info",    foreground=t["log_info"])
        self.text.tag_configure("debug",   foreground=t["text_dim"])
        self.text.tag_configure("warning", foreground=t["warning"])
        self.text.tag_configure("error",   foreground=t["error"])
        self.text.tag_configure("link", foreground=t["accent"],
                                underline=True)
        self.text.tag_bind("link", "<Enter>",
                           lambda _: self.text.configure(cursor="hand2"))
        self.text.tag_bind("link", "<Leave>",
                           lambda _: self.text.configure(cursor=""))
        self.text.tag_bind("link", "<Button-1>", self._on_link_click)
        # Ensure link tag draws on top of level tags
        self.text.tag_raise("link")

    def _matches_filter(self, level: str) -> bool:
        f = self._filter_var.get()
        if f == "All":
            return True
        return level.upper().startswith(f.upper())

    def _insert_line(self, level: str, text: str):
        tag = self.LEVEL_TAG.get(level.upper(), "info")
        self.text.configure(state="normal")

        # Scan for file paths and insert them with the "link" tag
        last = 0
        for m in _PATH_RE.finditer(text):
            candidate = m.group(0).rstrip(".,;:!?")
            if not candidate:
                continue
            # Resolve relative paths from the working directory
            resolved = os.path.abspath(candidate)
            if not (os.path.exists(resolved)
                    or os.path.exists(os.path.dirname(resolved))):
                continue
            # Insert text before the path
            if m.start() > last:
                self.text.insert("end", text[last:m.start()], tag)
            self.text.insert("end", candidate, (tag, "link"))
            last = m.start() + len(candidate)

        # Insert remaining text (or the whole line if no paths found)
        if last < len(text):
            self.text.insert("end", text[last:], tag)
        self.text.insert("end", "\n", tag)

        self.text.configure(state="disabled")
        if self._auto_scroll:
            self.text.see("end")

    def _on_link_click(self, event):
        """Open the clicked path in Windows Explorer."""
        idx = self.text.index(f"@{event.x},{event.y}")
        # Get the full extent of the link tag at the click position
        try:
            rng = self.text.tag_prevrange("link", f"{idx}+1c")
        except tk.TclError:
            return
        if not rng:
            return
        path = self.text.get(*rng).strip()
        resolved = os.path.abspath(path)
        # Open the containing folder (selecting the file) or the folder itself
        try:
            if os.path.isfile(resolved):
                subprocess.Popen(["explorer", "/select,", resolved])
            elif os.path.isdir(resolved):
                subprocess.Popen(["explorer", resolved])
            else:
                # Try opening the parent directory
                parent = os.path.dirname(resolved)
                if os.path.isdir(parent):
                    subprocess.Popen(["explorer", parent])
        except OSError:
            pass

    def _on_filter_changed(self, _event=None):
        self.text.configure(state="normal")
        self.text.delete("1.0", "end")
        self.text.configure(state="disabled")
        for lvl, txt in self._all_records:
            if self._matches_filter(lvl):
                self._insert_line(lvl, txt)

    def _on_autoscroll_toggle(self):
        self._auto_scroll = self._autoscroll_var.get()
        if self._auto_scroll:
            self.text.see("end")

    def _on_user_scroll(self, _event=None):
        # if user scrolls up, disable auto-scroll
        if self.text.yview()[1] < 1.0:
            self._auto_scroll = False
            self._autoscroll_var.set(False)
        else:
            self._auto_scroll = True
            self._autoscroll_var.set(True)

    def _on_export(self):
        from tkinter import filedialog
        path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            title="Export log",
        )
        if path:
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.text.get("1.0", "end"))


class TabBar(ttk.Frame):
    """Horizontal text tab bar using ttk.Buttons with underline indicators."""

    def __init__(self, parent, tabs: list[str], on_tab_changed=None, **kw):
        super().__init__(parent, **kw)
        self._tabs = tabs
        self._on_changed = on_tab_changed
        self._buttons: list[RoundedButton] = []
        self._indicators: list[ttk.Frame] = []
        self._active = 0

        for i, name in enumerate(tabs):
            frame = ttk.Frame(self)
            frame.pack(side="left", padx=2)

            btn = RoundedButton(frame, text=name, style="Tab.TButton",
                                command=lambda idx=i: self.select(idx))
            btn.pack(side="top")

            indicator = ttk.Frame(frame, height=2)
            indicator.pack(side="top", fill="x", padx=4)

            self._buttons.append(btn)
            self._indicators.append(indicator)

        self.select(0)

    def select(self, index: int):
        self._active = index
        for i, (btn, ind) in enumerate(zip(self._buttons, self._indicators)):
            if i == index:
                btn.configure(style="TabActive.TButton")
                ttk.Style().configure("Accent.TFrame",
                                      background=COLORS["accent"])
                ind.configure(style="Accent.TFrame")
            else:
                btn.configure(style="Tab.TButton")
                ind.configure(style="TFrame")
        if self._on_changed:
            self._on_changed(self._tabs[index])

    @property
    def active_tab(self) -> str:
        return self._tabs[self._active]


class LabeledEntry(ttk.Frame):
    """A label above a ttk.Entry, with ``get`` / ``set`` convenience."""

    def __init__(self, parent, label: str, value: str = "", **kw):
        super().__init__(parent, **kw)
        ttk.Label(self, text=label, style="Surface.TLabel").pack(anchor="w")
        self._var = tk.StringVar(value=value)
        self._entry = ttk.Entry(self, textvariable=self._var, width=32)
        self._entry.pack(fill="x", pady=(2, 0))

    def get(self) -> str:
        return self._var.get()

    def set(self, value: str):
        self._var.set(value)

    @property
    def entry(self):
        return self._entry


class LabeledText(ttk.Frame):
    """A label above a multi-line tk.Text for larger inputs (e.g. SQL)."""

    def __init__(self, parent, label: str, value: str = "", height: int = 4,
                 **kw):
        super().__init__(parent, **kw)
        ttk.Label(self, text=label, style="Surface.TLabel").pack(anchor="w")
        self._text = tk.Text(self, height=height, wrap="word",
                             bg=COLORS["input_bg"], fg=COLORS["text"],
                             font=FONT_MONO, insertbackground=COLORS["text"],
                             relief="flat", borderwidth=1,
                             highlightbackground=COLORS["border"],
                             highlightthickness=1)
        self._text.pack(fill="x", pady=(2, 0))
        if value:
            self._text.insert("1.0", value)

    def get(self) -> str:
        return self._text.get("1.0", "end-1c")

    def set(self, value: str):
        self._text.delete("1.0", "end")
        self._text.insert("1.0", value)


class FilePickerEntry(ttk.Frame):
    """Entry with a Browse button for file / database selection."""

    def __init__(self, parent, label: str, value: str = "",
                 filetypes=None, dialog_title: str = "Select file", **kw):
        super().__init__(parent, **kw)
        self._filetypes = filetypes or [("All files", "*.*")]
        self._dialog_title = dialog_title
        ttk.Label(self, text=label, style="Surface.TLabel").pack(anchor="w")
        row = ttk.Frame(self)
        row.pack(fill="x", pady=(2, 0))
        self._var = tk.StringVar(value=value)
        self._entry = ttk.Entry(row, textvariable=self._var)
        self._entry.pack(side="left", fill="x", expand=True)
        self._browse_btn = RoundedButton(row, text="Browse...",
                         style="Ghost.TButton",
                         command=self._browse)
        self._browse_btn.pack(side="right", padx=(4, 0))

    def _browse(self):
        from tkinter import filedialog
        path = filedialog.askopenfilename(title=self._dialog_title,
                                          filetypes=self._filetypes)
        if path:
            self._var.set(path)

    def get(self) -> str:
        return self._var.get()

    def set(self, value: str):
        self._var.set(value)

    def reapply_colors(self):
        self._browse_btn.reapply_colors()


class DatabasePickerEntry(FilePickerEntry):
    """Convenience subclass pre-configured for .db files."""

    def __init__(self, parent, label: str = "Database", value: str = "",
                 **kw):
        super().__init__(parent, label=label, value=value,
                         filetypes=[("SQLite DB", "*.db"),
                                    ("All files", "*.*")],
                         dialog_title="Select database", **kw)


class PortfolioGrid(ttk.Frame):
    """Editable portfolio table with add/delete rows and clipboard support.

    Each row has: Ticker (secCode), Mode (weight/shares/value), Amount.
    """

    def __init__(self, parent, portfolio: dict | None = None, **kw):
        super().__init__(parent, **kw)

        cols = ("ticker", "mode", "amount")
        self.tree = ttk.Treeview(self, columns=cols, show="headings",
                                 height=6)
        self.tree.heading("ticker", text="Ticker")
        self.tree.heading("mode", text="Mode")
        self.tree.heading("amount", text="Amount")
        self.tree.column("ticker", width=100)
        self.tree.column("mode", width=80)
        self.tree.column("amount", width=80)
        self.tree.pack(fill="both", expand=True)

        btn_row = ttk.Frame(self)
        btn_row.pack(fill="x", pady=(4, 0))
        self._add_btn = RoundedButton(btn_row, text="+ Add Row",
                          style="Small.TButton",
                          command=self._add_row)
        self._add_btn.pack(side="left", padx=2)
        self._del_btn = RoundedButton(btn_row, text="- Delete Row",
                          style="Ghost.TButton",
                          command=self._del_row)
        self._del_btn.pack(side="left", padx=2)

        # double-click to edit
        self.tree.bind("<Double-1>", self._on_double_click)

        if portfolio:
            self._load(portfolio)

    def _load(self, portfolio: dict):
        for item in self.tree.get_children():
            self.tree.delete(item)
        for ticker, info in portfolio.items():
            mode = info.get("mode", "weight")
            amount = info.get("value", 0.0)
            self.tree.insert("", "end", values=(ticker, mode, amount))

    def _add_row(self):
        self.tree.insert("", "end", values=("", "weight", "0.0"))

    def _del_row(self):
        sel = self.tree.selection()
        for item in sel:
            self.tree.delete(item)

    def _on_double_click(self, event):
        region = self.tree.identify_region(event.x, event.y)
        if region != "cell":
            return
        col = self.tree.identify_column(event.x)
        item = self.tree.identify_row(event.y)
        if not item:
            return
        col_idx = int(col.replace("#", "")) - 1
        col_names = ("ticker", "mode", "amount")
        current = self.tree.item(item, "values")[col_idx]

        # inline edit
        bbox = self.tree.bbox(item, col)
        if not bbox:
            return
        entry = ttk.Entry(self.tree, width=bbox[2])
        entry.place(x=bbox[0], y=bbox[1], width=bbox[2], height=bbox[3])
        entry.insert(0, current)
        entry.focus_set()

        def _commit(_e=None):
            vals = list(self.tree.item(item, "values"))
            vals[col_idx] = entry.get()
            self.tree.item(item, values=vals)
            entry.destroy()

        entry.bind("<Return>", _commit)
        entry.bind("<FocusOut>", _commit)
        entry.bind("<Escape>", lambda _: entry.destroy())

    def reapply_colors(self):
        self._add_btn.reapply_colors()
        self._del_btn.reapply_colors()

    def get_portfolio(self) -> dict:
        """Return portfolio dict in the same format as run_config.json."""
        portfolio = {}
        for item in self.tree.get_children():
            vals = self.tree.item(item, "values")
            ticker, mode, amount = vals[0], vals[1], vals[2]
            if ticker:
                try:
                    amount = float(amount)
                except ValueError:
                    amount = 0.0
                portfolio[ticker] = {"mode": mode, "value": amount}
        return portfolio

    def set_portfolio(self, portfolio: dict):
        self._load(portfolio)
