"""Modern dual-theme style system for the EDINET UI.

Provides a dark and light palette with clean financial-app aesthetics.
The module exposes a mutable ``theme`` dict that all widgets should read
from so that switching between light and dark is a single operation.
"""

import tkinter as tk
from tkinter import ttk

# ── Palettes ────────────────────────────────────────────────────────────

_DARK = {
    "bg":          "#111118",
    "surface":     "#1a1a28",
    "surface_alt": "#222234",
    "border":      "#2c2c44",
    "text":        "#dde1f5",
    "text_dim":    "#7878a0",
    "accent":      "#7c9df5",
    "accent_hover":"#5b7de0",
    "success":     "#78dba9",
    "warning":     "#f5c862",
    "error":       "#f56c6c",
    "highlight":   "#3d5afe",
    "input_bg":    "#16161f",
    "log_bg":      "#0c0c14",
    "tab_active":  "#7c9df5",
    "tab_inactive":"#7878a0",
    "card":        "#1a1a28",
}

_LIGHT = {
    "bg":          "#f2f3f7",
    "surface":     "#ffffff",
    "surface_alt": "#eaecf2",
    "border":      "#cdd0da",
    "text":        "#111118",
    "text_dim":    "#606880",
    "accent":      "#3d5afe",
    "accent_hover":"#2e46d1",
    "success":     "#16a34a",
    "warning":     "#ca8a04",
    "error":       "#dc2626",
    "highlight":   "#3d5afe",
    "input_bg":    "#ffffff",
    "log_bg":      "#f7f8fc",
    "tab_active":  "#3d5afe",
    "tab_inactive":"#606880",
    "card":        "#ffffff",
}

# The live palette — start with dark.  ``toggle_theme()`` swaps it.
theme: dict[str, str] = dict(_DARK)
_current_mode: str = "dark"

# ── Font stacks ─────────────────────────────────────────────────────────
_SANS = ("Segoe UI", "Helvetica Neue", "Arial")
_MONO = ("Cascadia Mono", "Consolas", "Courier New")

FONT_UI        = (_SANS[0], 10)
FONT_UI_BOLD   = (_SANS[0], 10, "bold")
FONT_HEADING   = (_SANS[0], 16, "bold")
FONT_SUBHEAD   = (_SANS[0], 11)
FONT_LABEL     = (_SANS[0],  9, "bold")   # uppercase section labels
FONT_SMALL     = (_SANS[0],  9)
FONT_MONO      = (_MONO[0], 10)
FONT_MONO_BOLD = (_MONO[0], 10, "bold")

# ── Spacing ─────────────────────────────────────────────────────────────
PAD = 8

# ── Legacy aliases (kept for callers that import COLORS) ────────────────
COLORS = theme            # same dict object — always in sync


def is_dark() -> bool:
    return _current_mode == "dark"


def toggle_theme(root: tk.Tk) -> str:
    """Switch between dark and light. Returns the new mode string."""
    global _current_mode
    src = _LIGHT if _current_mode == "dark" else _DARK
    _current_mode = "light" if _current_mode == "dark" else "dark"
    theme.update(src)
    apply_theme(root)
    return _current_mode


def apply_theme(root: tk.Tk):
    """(Re-)apply the current palette to *root* and all ttk widgets."""
    t = theme
    root.configure(bg=t["bg"])
    root.option_add("*Font", FONT_UI)
    root.option_add("*Background", t["bg"])
    root.option_add("*Foreground", t["text"])
    root.option_add("*HighlightThickness", 0)

    s = ttk.Style(root)
    s.theme_use("clam")

    # ── base ────────────────────────────────────────────────────────────
    s.configure(".", background=t["bg"], foreground=t["text"],
                font=FONT_UI, borderwidth=0, focuscolor="")

    # ── frames ──────────────────────────────────────────────────────────
    s.configure("TFrame",         background=t["bg"])
    s.configure("Surface.TFrame", background=t["surface"])
    s.configure("Card.TFrame",    background=t["card"], relief="flat")
    s.configure("TopBar.TFrame",  background=t["surface"])

    # ── labels ──────────────────────────────────────────────────────────
    s.configure("TLabel",          background=t["bg"], foreground=t["text"],
                font=FONT_UI)
    s.configure("Heading.TLabel",  font=FONT_HEADING,  foreground=t["text"])
    s.configure("Subhead.TLabel",  font=FONT_SUBHEAD,  foreground=t["text_dim"])
    s.configure("SectionHead.TLabel", font=FONT_LABEL, foreground=t["text_dim"],
                background=t["bg"])
    s.configure("Dim.TLabel",      foreground=t["text_dim"])
    s.configure("Accent.TLabel",   foreground=t["accent"])
    s.configure("Success.TLabel",  foreground=t["success"])
    s.configure("Warning.TLabel",  foreground=t["warning"])
    s.configure("Error.TLabel",    foreground=t["error"])
    s.configure("Surface.TLabel",  background=t["surface"], foreground=t["text"])
    s.configure("TopBar.TLabel",   background=t["surface"], foreground=t["text"],
                font=FONT_UI_BOLD)

    # tab labels
    s.configure("Tab.TLabel", background=t["surface"],
                foreground=t["tab_inactive"], font=FONT_UI, padding=(12, 6))
    s.configure("TabActive.TLabel", background=t["surface"],
                foreground=t["tab_active"], font=FONT_UI_BOLD, padding=(12, 6))

    # ── buttons — truly flat, hover via background only ─────────────────
    _btn_pad = (PAD * 2, 7)
    s.configure("TButton",
                background=t["surface_alt"], foreground=t["text"],
                font=FONT_UI, borderwidth=0, relief="flat",
                padding=_btn_pad, focuscolor="")
    s.map("TButton",
          background=[("active",   t["border"]),
                      ("disabled", t["bg"])],
          foreground=[("disabled", t["text_dim"])])

    s.configure("Accent.TButton",
                background=t["accent"], foreground="#ffffff",
                font=FONT_UI_BOLD, borderwidth=0, relief="flat",
                padding=_btn_pad, focuscolor="")
    s.map("Accent.TButton",
          background=[("active",   t["accent_hover"]),
                      ("disabled", t["border"])])

    s.configure("Danger.TButton",
                background=t["error"], foreground="#ffffff",
                font=FONT_UI_BOLD, borderwidth=0, relief="flat",
                padding=_btn_pad, focuscolor="")
    s.map("Danger.TButton",
          background=[("active", "#b91c1c"), ("disabled", t["border"])])

    s.configure("Small.TButton",
                font=FONT_SMALL, padding=(PAD, 4),
                borderwidth=0, relief="flat", focuscolor="")

    s.configure("Tab.TButton",
                background=t["surface"], foreground=t["tab_inactive"],
                font=FONT_UI, borderwidth=0, padding=(16, 8),
                relief="flat", focuscolor="")
    s.map("Tab.TButton",
          background=[("active", t["surface_alt"])],
          foreground=[("active", t["text"])])

    s.configure("TabActive.TButton",
                background=t["surface"], foreground=t["tab_active"],
                font=FONT_UI_BOLD, borderwidth=0, padding=(16, 8),
                relief="flat", focuscolor="")

    # icon buttons (theme toggle, close)
    s.configure("Icon.TButton",
                background=t["surface"], foreground=t["text_dim"],
                font=FONT_UI, borderwidth=0, padding=(8, 5),
                relief="flat", focuscolor="")
    s.map("Icon.TButton",
          background=[("active", t["surface_alt"])],
          foreground=[("active", t["text"])])

    # ── entries ─────────────────────────────────────────────────────────
    s.configure("TEntry",
                fieldbackground=t["input_bg"], foreground=t["text"],
                insertcolor=t["text"], borderwidth=1, relief="solid",
                padding=6)
    s.map("TEntry",
          fieldbackground=[("focus", t["surface"])],
          bordercolor=[("focus", t["accent"])])

    # ── combobox ────────────────────────────────────────────────────────
    s.configure("TCombobox",
                fieldbackground=t["input_bg"], foreground=t["text"],
                borderwidth=1, padding=5, arrowcolor=t["text"])
    s.map("TCombobox",
          fieldbackground=[("focus", t["surface"])],
          bordercolor=[("focus", t["accent"])])
    root.option_add("*TCombobox*Listbox.Background",      t["surface"])
    root.option_add("*TCombobox*Listbox.Foreground",      t["text"])
    root.option_add("*TCombobox*Listbox.selectBackground", t["highlight"])

    # ── checkbutton ─────────────────────────────────────────────────────
    s.configure("TCheckbutton",
                background=t["bg"], foreground=t["text"],
                font=FONT_UI, focuscolor="")
    s.map("TCheckbutton",
          background=[("active", t["bg"])],
          indicatorcolor=[("selected",  t["accent"]),
                          ("!selected", t["border"])])
    # checkbutton inside Surface panels
    s.configure("Surface.TCheckbutton",
                background=t["surface"], foreground=t["text"],
                font=FONT_UI, focuscolor="")
    s.map("Surface.TCheckbutton",
          background=[("active", t["surface"])],
          indicatorcolor=[("selected",  t["accent"]),
                          ("!selected", t["border"])])

    # ── separator ───────────────────────────────────────────────────────
    s.configure("TSeparator",       background=t["border"])
    s.configure("TopBar.TSeparator",background=t["border"])
    s.configure("TPanedwindow",     background=t["border"])
    s.configure("Sash", sashthickness=4, gripcount=0, background=t["border"])

    # ── scrollbar — thin, minimal ────────────────────────────────────────
    s.configure("Vertical.TScrollbar",
                background=t["surface_alt"], troughcolor=t["bg"],
                borderwidth=0, arrowsize=0, width=8)
    s.map("Vertical.TScrollbar",
          background=[("active", t["text_dim"])])

    # ── notebook ────────────────────────────────────────────────────────
    s.configure("TNotebook", background=t["bg"], borderwidth=0)
    s.configure("TNotebook.Tab",
                background=t["surface"], foreground=t["text_dim"],
                font=FONT_UI, padding=(PAD * 2, PAD // 2), borderwidth=0)
    s.map("TNotebook.Tab",
          background=[("selected", t["bg"])],
          foreground=[("selected", t["accent"])])

    # ── treeview ─────────────────────────────────────────────────────────
    s.configure("Treeview",
                background=t["surface"], foreground=t["text"],
                fieldbackground=t["surface"], font=FONT_UI,
                rowheight=30, borderwidth=0)
    s.configure("Treeview.Heading",
                background=t["surface_alt"], foreground=t["text_dim"],
                font=FONT_LABEL, relief="flat", padding=(PAD, 6))
    s.map("Treeview",
          background=[("selected", t["highlight"])],
          foreground=[("selected", "#ffffff")])
    s.map("Treeview.Heading",
          background=[("active", t["border"])])

    # ── labelframe ───────────────────────────────────────────────────────
    s.configure("TLabelframe",
                background=t["surface"], foreground=t["text"],
                borderwidth=1, relief="solid")
    s.configure("TLabelframe.Label",
                background=t["surface"], foreground=t["accent"],
                font=FONT_UI_BOLD)

    # ── progressbar ──────────────────────────────────────────────────────
    s.configure("Horizontal.TProgressbar",
                troughcolor=t["surface_alt"], background=t["accent"],
                borderwidth=0, thickness=3)

    return s
