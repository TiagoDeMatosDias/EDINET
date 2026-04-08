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
    FONT_MONO,
    FONT_SMALL,
    FONT_TITLE,
    FONT_TOPBAR_ACTION,
    FONT_TOPBAR_NAV,
    FONT_UI,
    FONT_UI_BOLD,
    PAD,
)

_PATH_RE = re.compile(
    r"(?:[A-Za-z]:[\\/]|\.{1,2}[\\/])?"
    r"[A-Za-z0-9_.\\-]+"
    r"(?:[\\/][A-Za-z0-9_.\\-]+)+"
)


def _button_tokens(style_name: str) -> dict[str, object]:
    t = COLORS
    tokens = {
        "fg_color": t["surface_alt"],
        "hover_color": t["overlay"],
        "text_color": t["text"],
        "border_color": t["surface_alt"],
        "border_width": 0,
        "corner_radius": BUTTON_RADIUS,
        "height": 36,
        "font": FONT_UI,
    }
    if style_name == "Accent.TButton":
        tokens.update(
            {
                "fg_color": t["accent"],
                "hover_color": t["accent_hover"],
                "text_color": "#ffffff",
                "border_color": t["accent"],
                "font": FONT_UI_BOLD,
            }
        )
    elif style_name == "Danger.TButton":
        tokens.update(
            {
                "fg_color": t["error"],
                "hover_color": t["accent_hover"],
                "text_color": "#ffffff",
                "border_color": t["error"],
                "font": FONT_UI_BOLD,
            }
        )
    elif style_name == "Ghost.TButton":
        tokens.update(
            {
                "fg_color": "transparent",
                "hover_color": t["surface_alt"],
                "text_color": t["text"],
                "border_color": t["border"],
                "border_width": 1,
            }
        )
    elif style_name in {"Icon.TButton", "TopBar.Icon.TButton"}:
        tokens.update(
            {
                "fg_color": "transparent",
                "hover_color": t["surface_alt"],
                "text_color": t["text"],
                "border_color": t["surface"],
                "border_width": 0,
                "corner_radius": BUTTON_RADIUS_SMALL,
                "height": 34,
                "font": FONT_TOPBAR_ACTION if style_name == "TopBar.Icon.TButton" else FONT_UI,
            }
        )
    elif style_name == "Small.TButton":
        tokens.update(
            {
                "fg_color": t["surface_alt"],
                "hover_color": t["overlay"],
                "text_color": t["text"],
                "border_color": t["surface_alt"],
                "corner_radius": BUTTON_RADIUS_SMALL,
                "height": 30,
                "font": FONT_SMALL,
            }
        )
    elif style_name in {"Tab.TButton", "TopBar.Tab.TButton"}:
        tokens.update(
            {
                "fg_color": "transparent",
                "hover_color": t["surface_alt"],
                "text_color": t["tab_inactive"],
                "border_color": t["surface"],
                "border_width": 0,
                "corner_radius": BUTTON_RADIUS_SMALL,
                "height": 36,
                "font": FONT_TOPBAR_NAV if style_name.startswith("TopBar") else FONT_UI,
            }
        )
    elif style_name in {"TabActive.TButton", "TopBar.TabActive.TButton"}:
        tokens.update(
            {
                "fg_color": t["surface_alt"],
                "hover_color": t["surface_alt"],
                "text_color": t["tab_active"],
                "border_color": t["surface_alt"],
                "corner_radius": BUTTON_RADIUS_SMALL,
                "height": 36,
                "font": FONT_TOPBAR_NAV if style_name.startswith("TopBar") else FONT_UI_BOLD,
            }
        )
    return tokens


def _coerce_button_width(width: object) -> object:
    if isinstance(width, int) and width <= 10:
        return max(36, width * 12)
    return width


def _detect_bg(widget) -> str:
    """Walk up the widget tree to find the effective background color."""

    current = widget
    while current is not None:
        try:
            bg = current.cget("bg")
            if bg and bg != "SystemButtonFace":
                return str(bg)
        except (tk.TclError, AttributeError):
            pass
        try:
            style_name = str(current.cget("style") or "") or current.winfo_class()
            style = ttk.Style()
            bg = style.lookup(style_name, "background")
            if bg:
                return str(bg)
        except (tk.TclError, AttributeError):
            pass
        current = current.master
    return COLORS["bg"]


class RoundedButton(tk.Frame):
    """Rounded button using CTkButton when available, ttk fallback otherwise."""

    def __init__(self, parent, text="", command=None, style="TButton", **kw):
        parent_bg = _detect_bg(parent)
        super().__init__(parent, bg=parent_bg, highlightthickness=0, bd=0)
        self._style_name = style
        self._is_disabled = False
        width = kw.pop("width", None)
        self._inner = None

        if ctk is None:
            self._inner = ttk.Button(self, text=text, command=command, style=style, width=width, **kw)
            self._inner.pack(fill="both", expand=True)
            return

        tokens = _button_tokens(style)
        self._inner = ctk.CTkButton(
            self,
            text=text,
            command=command,
            width=_coerce_button_width(width) or 0,
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
        if any(state == "disabled" for state in statespec):
            self._is_disabled = True
        elif any(state == "!disabled" for state in statespec):
            self._is_disabled = False
        if ctk is None:
            self._inner.state(statespec)
            return self._inner.state()
        self._inner.configure(state="disabled" if self._is_disabled else "normal")
        return self.state()

    def reapply_colors(self):
        parent_bg = _detect_bg(self.master)
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
            state="disabled" if self._is_disabled else "normal",
        )

    def configure_frame(self, **kw):
        super().configure(**kw)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def reapply_widget_tree(widget):
    for child in widget.winfo_children():
        if hasattr(child, "reapply_colors"):
            child.reapply_colors()
        reapply_widget_tree(child)


class ScrollableFrame(ttk.Frame):
    """Canvas-backed scrollable frame for dense panels and card stacks."""

    def __init__(self, parent, *, bg_key: str = "surface", width: int | None = None, **kw):
        super().__init__(parent, **kw)
        self._bg_key = bg_key
        self.canvas = tk.Canvas(self, bg=COLORS[bg_key], highlightthickness=0, bd=0)
        self.scrollbar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.interior = ttk.Frame(self.canvas, style="Surface.TFrame")
        self._window_id = self.canvas.create_window((0, 0), window=self.interior, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")

        self.interior.bind("<Configure>", self._sync_scrollregion)
        self.canvas.bind("<Configure>", self._sync_width)
        self.canvas.bind("<Enter>", self._bind_wheel)
        self.canvas.bind("<Leave>", self._unbind_wheel)

        if width is not None:
            self.canvas.configure(width=width)

    def _sync_scrollregion(self, _event=None):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _sync_width(self, event=None):
        if event is None:
            return
        self.canvas.itemconfigure(self._window_id, width=event.width)

    def _on_mousewheel(self, event):
        delta = event.delta // 120 if event.delta else 0
        if delta:
            self.canvas.yview_scroll(-delta, "units")

    def _bind_wheel(self, _event=None):
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)

    def _unbind_wheel(self, _event=None):
        self.canvas.unbind_all("<MouseWheel>")

    def reapply_colors(self):
        self.canvas.configure(bg=COLORS[self._bg_key])


class SectionCard(ttk.Frame):
    """Reusable panel with a header row and body container."""

    def __init__(self, parent, title: str, subtitle: str | None = None, *, style: str = "Panel.TFrame", **kw):
        super().__init__(parent, style=style, **kw)
        label_style = {
            "Panel.TFrame": "Panel.TLabel",
            "PanelAlt.TFrame": "PanelAlt.TLabel",
            "Hero.TFrame": "Hero.TLabel",
        }.get(style, "Surface.TLabel")
        self.header = ttk.Frame(self, style=style)
        self.header.pack(fill="x", padx=PAD, pady=(PAD, 0))
        title_wrap = ttk.Frame(self.header, style=style)
        title_wrap.pack(side="left", fill="x", expand=True)
        ttk.Label(title_wrap, text=title, style=label_style, font=FONT_UI_BOLD).pack(anchor="w")
        if subtitle:
            ttk.Label(title_wrap, text=subtitle, style=label_style, font=FONT_SMALL).pack(anchor="w", pady=(2, 0))
        self.actions = ttk.Frame(self.header, style=style)
        self.actions.pack(side="right")
        self.body = ttk.Frame(self, style=style)
        self.body.pack(fill="both", expand=True, padx=PAD, pady=(8, PAD))


class PageHeader(ttk.Frame):
    """Standard page header with title, subtitle, context, and actions."""

    def __init__(self, parent, title: str, subtitle: str = "", context: str = "", **kw):
        super().__init__(parent, style="App.TFrame", **kw)
        left = ttk.Frame(self, style="App.TFrame")
        left.pack(side="left", fill="x", expand=True)
        ttk.Label(left, text=title, style="Heading.TLabel").pack(anchor="w")
        self._subtitle_var = tk.StringVar(value=subtitle)
        self._context_var = tk.StringVar(value=context)
        ttk.Label(left, textvariable=self._subtitle_var, style="Subhead.TLabel").pack(anchor="w", pady=(2, 0))
        ttk.Label(left, textvariable=self._context_var, style="Meta.TLabel").pack(anchor="w", pady=(4, 0))
        self.actions = ttk.Frame(self, style="App.TFrame")
        self.actions.pack(side="right", anchor="n")

    def set_subtitle(self, text: str):
        self._subtitle_var.set(text)

    def set_context(self, text: str):
        self._context_var.set(text)


class StatTile(ttk.Frame):
    """Compact metric tile used in headers and dashboards."""

    def __init__(self, parent, label: str, value: str = "", meta: str = "", *, style: str = "Panel.TFrame", **kw):
        super().__init__(parent, style=style, **kw)
        label_style = "Panel.TLabel" if style == "Panel.TFrame" else "Hero.TLabel"
        self._value_var = tk.StringVar(value=value)
        self._meta_var = tk.StringVar(value=meta)
        ttk.Label(self, text=label, style=label_style, font=FONT_SMALL).pack(anchor="w", padx=PAD, pady=(PAD, 0))
        ttk.Label(self, textvariable=self._value_var, style=label_style, font=FONT_TITLE).pack(anchor="w", padx=PAD, pady=(4, 0))
        ttk.Label(self, textvariable=self._meta_var, style=label_style, font=FONT_SMALL).pack(anchor="w", padx=PAD, pady=(4, PAD))

    def set(self, value: str = "", meta: str = ""):
        self._value_var.set(value)
        self._meta_var.set(meta)


class EmptyState(ttk.Frame):
    """Simple empty-state panel for major work surfaces."""

    def __init__(self, parent, title: str, message: str, *, style: str = "Panel.TFrame", **kw):
        super().__init__(parent, style=style, **kw)
        label_style = "Panel.TLabel" if style == "Panel.TFrame" else "Surface.TLabel"
        ttk.Label(self, text=title, style=label_style, font=FONT_UI_BOLD).pack(anchor="center", pady=(PAD * 2, 4))
        ttk.Label(self, text=message, style=label_style, font=FONT_SMALL, justify="center", wraplength=420).pack(anchor="center", padx=PAD * 2, pady=(0, PAD * 2))


class LogPanel(ttk.Frame):
    """Console drawer with filtering, search, and collapse support."""

    LEVEL_TAG = {
        "DEBUG": "debug",
        "INFO": "info",
        "WARNING": "warning",
        "ERROR": "error",
        "CRITICAL": "error",
    }

    def __init__(self, parent, **kw):
        super().__init__(parent, style="Console.TFrame", **kw)
        self._auto_scroll = True
        self._collapsed = False
        self._all_records: list[tuple[str, str]] = []
        self._query_var = tk.StringVar()
        self._filter_var = tk.StringVar(value="All")
        self._count_var = tk.StringVar(value="0 lines")

        toolbar = ttk.Frame(self, style="Console.TFrame")
        toolbar.pack(fill="x", padx=PAD, pady=(PAD // 2, 0))

        left = ttk.Frame(toolbar, style="Console.TFrame")
        left.pack(side="left", fill="x", expand=True)
        ttk.Label(left, text="Console", style="Accent.TLabel").pack(side="left")
        ttk.Label(left, textvariable=self._count_var, style="Console.TLabel", font=FONT_SMALL).pack(side="left", padx=(8, 0))
        ttk.Separator(left, orient="horizontal").pack(side="left", fill="x", expand=True, padx=PAD)

        self._collapse_btn = RoundedButton(toolbar, text="Hide", style="Small.TButton", command=self._toggle_collapsed)
        self._collapse_btn.pack(side="right", padx=2)

        self._filter_combo = ttk.Combobox(toolbar, textvariable=self._filter_var, values=["All", "Info", "Warning", "Error"], width=8, state="readonly")
        self._filter_combo.pack(side="right", padx=(PAD // 2, 0))
        self._filter_combo.bind("<<ComboboxSelected>>", self._on_filter_changed)
        ttk.Label(toolbar, text="Level", style="Console.TLabel", font=FONT_SMALL).pack(side="right", padx=(PAD, 0))

        self._search_entry = ttk.Entry(toolbar, textvariable=self._query_var, width=24)
        self._search_entry.pack(side="right", padx=(PAD // 2, 0))
        self._search_entry.bind("<KeyRelease>", self._on_filter_changed)
        ttk.Label(toolbar, text="Search", style="Console.TLabel", font=FONT_SMALL).pack(side="right", padx=(PAD, 0))

        self._autoscroll_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(toolbar, text="Auto-scroll", style="Panel.TCheckbutton", variable=self._autoscroll_var, command=self._on_autoscroll_toggle).pack(side="right", padx=PAD)

        self._export_btn = RoundedButton(toolbar, text="Export", style="Small.TButton", command=self._on_export)
        self._export_btn.pack(side="right", padx=2)
        self._clear_btn = RoundedButton(toolbar, text="Clear", style="Small.TButton", command=self.clear)
        self._clear_btn.pack(side="right", padx=2)

        self._body = ttk.Frame(self, style="Console.TFrame")
        self._body.pack(fill="both", expand=True, padx=PAD, pady=(6, PAD))

        text_frame = ttk.Frame(self._body, style="Console.TFrame")
        text_frame.pack(fill="both", expand=True)
        self.text = tk.Text(
            text_frame,
            wrap="word",
            state="disabled",
            bg=COLORS["log_bg"],
            fg=COLORS["text"],
            font=FONT_MONO,
            insertbackground=COLORS["text"],
            selectbackground=COLORS["highlight"],
            relief="flat",
            borderwidth=0,
            padx=8,
            pady=6,
            height=9,
        )
        scrollbar = ttk.Scrollbar(text_frame, orient="vertical", command=self.text.yview)
        self.text.configure(yscrollcommand=scrollbar.set)
        self.text.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        self._apply_text_tags()
        self.text.bind("<MouseWheel>", self._on_user_scroll)
        self.text.bind("<Button-4>", self._on_user_scroll)
        self.text.bind("<Button-5>", self._on_user_scroll)

    def append(self, level: str, text: str):
        self._all_records.append((level, text))
        self._count_var.set(f"{len(self._all_records)} lines")
        if self._matches_filter(level, text):
            self._insert_line(level, text)

    def clear(self):
        self._all_records.clear()
        self._count_var.set("0 lines")
        self.text.configure(state="normal")
        self.text.delete("1.0", "end")
        self.text.configure(state="disabled")

    def reapply_colors(self):
        t = COLORS
        self.text.configure(bg=t["log_bg"], fg=t["text"], insertbackground=t["text"], selectbackground=t["highlight"])
        self._collapse_btn.reapply_colors()
        self._export_btn.reapply_colors()
        self._clear_btn.reapply_colors()
        self._apply_text_tags()

    def _apply_text_tags(self):
        t = COLORS
        self.text.tag_configure("info", foreground=t["log_info"])
        self.text.tag_configure("debug", foreground=t["text_dim"])
        self.text.tag_configure("warning", foreground=t["warning"])
        self.text.tag_configure("error", foreground=t["error"])
        self.text.tag_configure("link", foreground=t["accent"], underline=True)
        self.text.tag_bind("link", "<Enter>", lambda _e: self.text.configure(cursor="hand2"))
        self.text.tag_bind("link", "<Leave>", lambda _e: self.text.configure(cursor=""))
        self.text.tag_bind("link", "<Button-1>", self._on_link_click)
        self.text.tag_raise("link")

    def _toggle_collapsed(self):
        self._collapsed = not self._collapsed
        if self._collapsed:
            self._body.pack_forget()
            self._collapse_btn.configure(text="Show")
        else:
            self._body.pack(fill="both", expand=True, padx=PAD, pady=(6, PAD))
            self._collapse_btn.configure(text="Hide")

    def _matches_filter(self, level: str, text: str) -> bool:
        level_filter = self._filter_var.get()
        if level_filter != "All" and not level.upper().startswith(level_filter.upper()):
            return False
        query = self._query_var.get().strip().lower()
        return not query or query in text.lower() or query in level.lower()

    def _insert_line(self, level: str, text: str):
        tag = self.LEVEL_TAG.get(level.upper(), "info")
        self.text.configure(state="normal")
        last = 0
        for match in _PATH_RE.finditer(text):
            candidate = match.group(0).rstrip(".,;:!?")
            if not candidate:
                continue
            resolved = os.path.abspath(candidate)
            if not (os.path.exists(resolved) or os.path.exists(os.path.dirname(resolved))):
                continue
            if match.start() > last:
                self.text.insert("end", text[last:match.start()], tag)
            self.text.insert("end", candidate, (tag, "link"))
            last = match.start() + len(candidate)
        if last < len(text):
            self.text.insert("end", text[last:], tag)
        self.text.insert("end", "\n", tag)
        self.text.configure(state="disabled")
        if self._auto_scroll:
            self.text.see("end")

    def _on_link_click(self, event):
        idx = self.text.index(f"@{event.x},{event.y}")
        try:
            rng = self.text.tag_prevrange("link", f"{idx}+1c")
        except tk.TclError:
            return
        if not rng:
            return
        path = self.text.get(*rng).strip()
        resolved = os.path.abspath(path)
        try:
            if os.path.isfile(resolved):
                subprocess.Popen(["explorer", "/select,", resolved])
            elif os.path.isdir(resolved):
                subprocess.Popen(["explorer", resolved])
            else:
                parent = os.path.dirname(resolved)
                if os.path.isdir(parent):
                    subprocess.Popen(["explorer", parent])
        except OSError:
            pass

    def _on_filter_changed(self, _event=None):
        self.text.configure(state="normal")
        self.text.delete("1.0", "end")
        self.text.configure(state="disabled")
        for level, text in self._all_records:
            if self._matches_filter(level, text):
                self._insert_line(level, text)

    def _on_autoscroll_toggle(self):
        self._auto_scroll = self._autoscroll_var.get()
        if self._auto_scroll:
            self.text.see("end")

    def _on_user_scroll(self, _event=None):
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
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(self.text.get("1.0", "end"))


class TabBar(ttk.Frame):
    """Horizontal text tab bar using rounded buttons and underline indicators."""

    def __init__(self, parent, tabs: list[str], on_tab_changed=None, **kw):
        super().__init__(parent, style="App.TFrame", **kw)
        self._tabs = tabs
        self._on_changed = on_tab_changed
        self._buttons: list[RoundedButton] = []
        self._indicators: list[ttk.Frame] = []
        self._active = 0

        for index, name in enumerate(tabs):
            frame = ttk.Frame(self, style="App.TFrame")
            frame.pack(side="left", padx=(0, 6))
            button = RoundedButton(frame, text=name, style="Tab.TButton", command=lambda idx=index: self.select(idx))
            button.pack(side="top")
            indicator = ttk.Frame(frame, height=2, style="App.TFrame")
            indicator.pack(side="top", fill="x", padx=4, pady=(3, 0))
            self._buttons.append(button)
            self._indicators.append(indicator)

        self.select(0)

    def select(self, index: int):
        self._active = index
        style = ttk.Style()
        style.configure("Accent.TFrame", background=COLORS["accent"])
        for idx, (button, indicator) in enumerate(zip(self._buttons, self._indicators)):
            if idx == index:
                button.configure(style="TabActive.TButton")
                indicator.configure(style="Accent.TFrame")
            else:
                button.configure(style="Tab.TButton")
                indicator.configure(style="App.TFrame")
        if self._on_changed:
            self._on_changed(self._tabs[index])

    @property
    def active_tab(self) -> str:
        return self._tabs[self._active]


class LabeledEntry(ttk.Frame):
    """A label above a ttk.Entry, with ``get`` / ``set`` convenience."""

    def __init__(self, parent, label: str, value: str = "", *, label_style: str = "Panel.TLabel", **kw):
        super().__init__(parent, **kw)
        ttk.Label(self, text=label, style=label_style, font=FONT_SMALL).pack(anchor="w")
        self._var = tk.StringVar(value=value)
        self._entry = ttk.Entry(self, textvariable=self._var, width=32)
        self._entry.pack(fill="x", pady=(4, 0))

    def get(self) -> str:
        return self._var.get()

    def set(self, value: str):
        self._var.set(value)

    @property
    def entry(self):
        return self._entry


class LabeledText(ttk.Frame):
    """A label above a multi-line tk.Text for larger inputs (e.g. SQL)."""

    def __init__(self, parent, label: str, value: str = "", height: int = 4, *, label_style: str = "Panel.TLabel", **kw):
        super().__init__(parent, **kw)
        ttk.Label(self, text=label, style=label_style, font=FONT_SMALL).pack(anchor="w")
        self._text = tk.Text(
            self,
            height=height,
            wrap="word",
            bg=COLORS["input_bg"],
            fg=COLORS["text"],
            font=FONT_MONO,
            insertbackground=COLORS["text"],
            relief="flat",
            borderwidth=1,
            highlightbackground=COLORS["border"],
            highlightthickness=1,
            padx=8,
            pady=6,
        )
        self._text.pack(fill="x", pady=(4, 0))
        if value:
            self._text.insert("1.0", value)

    def get(self) -> str:
        return self._text.get("1.0", "end-1c")

    def set(self, value: str):
        self._text.delete("1.0", "end")
        self._text.insert("1.0", value)


class FilePickerEntry(ttk.Frame):
    """Entry with a Browse button for file or database selection."""

    def __init__(self, parent, label: str, value: str = "", filetypes=None, dialog_title: str = "Select file", *, label_style: str = "Panel.TLabel", **kw):
        super().__init__(parent, **kw)
        self._filetypes = filetypes or [("All files", "*.*")]
        self._dialog_title = dialog_title
        ttk.Label(self, text=label, style=label_style, font=FONT_SMALL).pack(anchor="w")
        row = ttk.Frame(self)
        row.pack(fill="x", pady=(4, 0))
        self._var = tk.StringVar(value=value)
        self._entry = ttk.Entry(row, textvariable=self._var)
        self._entry.pack(side="left", fill="x", expand=True)
        self._browse_btn = RoundedButton(row, text="Browse", style="Ghost.TButton", command=self._browse)
        self._browse_btn.pack(side="right", padx=(6, 0))

    def _browse(self):
        from tkinter import filedialog

        path = filedialog.askopenfilename(title=self._dialog_title, filetypes=self._filetypes)
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

    def __init__(self, parent, label: str = "Database", value: str = "", **kw):
        super().__init__(
            parent,
            label=label,
            value=value,
            filetypes=[("SQLite DB", "*.db"), ("All files", "*.*")],
            dialog_title="Select database",
            **kw,
        )


class PortfolioGrid(ttk.Frame):
    """Editable portfolio table with add/delete rows and inline edits."""

    def __init__(self, parent, portfolio: dict | None = None, **kw):
        super().__init__(parent, **kw)
        cols = ("ticker", "mode", "amount")
        self.tree = ttk.Treeview(self, columns=cols, show="headings", height=6)
        self.tree.heading("ticker", text="Ticker")
        self.tree.heading("mode", text="Mode")
        self.tree.heading("amount", text="Amount")
        self.tree.column("ticker", width=120)
        self.tree.column("mode", width=110)
        self.tree.column("amount", width=100, anchor="e")
        self.tree.pack(fill="both", expand=True)

        btn_row = ttk.Frame(self)
        btn_row.pack(fill="x", pady=(6, 0))
        self._add_btn = RoundedButton(btn_row, text="+ Add Row", style="Small.TButton", command=self._add_row)
        self._add_btn.pack(side="left", padx=(0, 4))
        self._del_btn = RoundedButton(btn_row, text="Delete Row", style="Ghost.TButton", command=self._del_row)
        self._del_btn.pack(side="left")

        self.tree.bind("<Double-1>", self._on_double_click)
        if portfolio:
            self._load(portfolio)

    def _load(self, portfolio: dict):
        for item in self.tree.get_children():
            self.tree.delete(item)
        for ticker, info in portfolio.items():
            self.tree.insert("", "end", values=(ticker, info.get("mode", "weight"), info.get("value", 0.0)))

    def _add_row(self):
        self.tree.insert("", "end", values=("", "weight", "0.0"))

    def _del_row(self):
        for item in self.tree.selection():
            self.tree.delete(item)

    def _on_double_click(self, event):
        if self.tree.identify_region(event.x, event.y) != "cell":
            return
        column = self.tree.identify_column(event.x)
        item = self.tree.identify_row(event.y)
        if not item:
            return
        col_idx = int(column.replace("#", "")) - 1
        current = self.tree.item(item, "values")[col_idx]
        bbox = self.tree.bbox(item, column)
        if not bbox:
            return
        entry = ttk.Entry(self.tree)
        entry.place(x=bbox[0], y=bbox[1], width=bbox[2], height=bbox[3])
        entry.insert(0, current)
        entry.focus_set()

        def _commit(_event=None):
            values = list(self.tree.item(item, "values"))
            values[col_idx] = entry.get()
            self.tree.item(item, values=values)
            entry.destroy()

        entry.bind("<Return>", _commit)
        entry.bind("<FocusOut>", _commit)
        entry.bind("<Escape>", lambda _event: entry.destroy())

    def reapply_colors(self):
        self._add_btn.reapply_colors()
        self._del_btn.reapply_colors()

    def get_portfolio(self) -> dict:
        portfolio: dict[str, dict[str, float | str]] = {}
        for item in self.tree.get_children():
            ticker, mode, amount = self.tree.item(item, "values")
            if not ticker:
                continue
            try:
                amount_value = float(amount)
            except ValueError:
                amount_value = 0.0
            portfolio[ticker] = {"mode": mode, "value": amount_value}
        return portfolio

    def set_portfolio(self, portfolio: dict):
        self._load(portfolio)
