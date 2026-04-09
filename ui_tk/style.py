"""Design tokens and ttk styling for the EDINET Tk UI.

The UI is intentionally desktop-tool oriented: dense, calm, and highly
readable for long research sessions. The theme system exposes a mutable
``theme`` dict so all pages can read live colors after a theme toggle.
"""

import tkinter as tk
from tkinter import ttk

# ---------------------------------------------------------------------------
# Palette
# ---------------------------------------------------------------------------

_DARK = {
      "bg": "#171B22",
      "surface": "#1F2430",
      "surface_alt": "#262C39",
      "surface_soft": "#202633",
      "panel": "#202635",
      "panel_alt": "#2B3242",
      "hero": "#222A3A",
      "overlay": "#30384A",
      "border": "#3F485C",
      "text": "#D8E0EF",
      "text_dim": "#97A4BC",
      "text_muted": "#6E7990",
      "accent": "#5B7CFA",
      "accent_hover": "#7894FF",
      "accent_soft": "#2B3A67",
      "success": "#8FCB93",
      "warning": "#D7B36A",
      "error": "#D97E7E",
      "highlight": "#5B7CFA",
      "input_bg": "#151A23",
      "log_bg": "#121721",
      "tab_active": "#D8E0EF",
      "tab_inactive": "#8A97B2",
      "card": "#202635",
      "log_info": "#88B2FF",
}

_LIGHT = {
      "bg": "#EEF2F7",
      "surface": "#FFFFFF",
      "surface_alt": "#E8EDF5",
      "surface_soft": "#F5F7FB",
      "panel": "#FFFFFF",
      "panel_alt": "#E5EBF6",
      "hero": "#EFF4FF",
      "overlay": "#D9E2F2",
      "border": "#C2CBDC",
      "text": "#172033",
      "text_dim": "#56627A",
      "text_muted": "#7B879D",
      "accent": "#3F5DE0",
      "accent_hover": "#2E4BC9",
      "accent_soft": "#D8E3FF",
      "success": "#2E8B57",
      "warning": "#B7811F",
      "error": "#C65353",
      "highlight": "#3F5DE0",
      "input_bg": "#F7F9FD",
      "log_bg": "#F4F7FC",
      "tab_active": "#172033",
      "tab_inactive": "#5D6880",
      "card": "#FFFFFF",
      "log_info": "#3451D1",
}

theme: dict[str, str] = dict(_DARK)
_current_mode = "dark"

# ---------------------------------------------------------------------------
# Typography
# ---------------------------------------------------------------------------

_MONO = ( "Consolas", "Courier New","Cascadia Mono")

FONT_UI = (_MONO[0], 10)
FONT_UI_BOLD = (_MONO[0], 10, "bold")
FONT_HEADING = (_MONO[0], 18, "bold")
FONT_TITLE = (_MONO[0], 14, "bold")
FONT_SUBHEAD = (_MONO[0], 11)
FONT_TOPBAR_BRAND = (_MONO[0], 18, "bold")
FONT_TOPBAR_NAV = (_MONO[0], 15, "bold")
FONT_TOPBAR_ACTION = (_MONO[0], 12, "bold")
FONT_LABEL = (_MONO[0], 9, "bold")
FONT_SMALL = (_MONO[0], 9)
FONT_MONO = (_MONO[0], 10)
FONT_MONO_BOLD = (_MONO[0], 10, "bold")

# ---------------------------------------------------------------------------
# Geometry
# ---------------------------------------------------------------------------

PAD = 12
SHELL_PAD = 16
BUTTON_RADIUS = 10
BUTTON_RADIUS_SMALL = 8

COLORS = theme


def is_dark() -> bool:
      return _current_mode == "dark"


def toggle_theme(root: tk.Tk) -> str:
      """Switch between dark and light mode and re-apply widget styles."""

      global _current_mode
      src = _LIGHT if _current_mode == "dark" else _DARK
      _current_mode = "light" if _current_mode == "dark" else "dark"
      theme.update(src)
      apply_theme(root)
      return _current_mode


def apply_theme(root: tk.Tk):
      """Apply the active palette to the root window and ttk widgets."""

      t = theme
      root.configure(bg=t["bg"])
      root.option_add("*Font", FONT_UI)
      root.option_add("*Background", t["bg"])
      root.option_add("*Foreground", t["text"])
      root.option_add("*HighlightThickness", 0)

      style = ttk.Style(root)
      style.theme_use("clam")

      # Base
      style.configure(
            ".",
            background=t["bg"],
            foreground=t["text"],
            font=FONT_UI,
            borderwidth=0,
            focuscolor="",
            relief="flat",
      )

      # Frames
      style.configure("TFrame", background=t["bg"])
      style.configure("App.TFrame", background=t["bg"])
      style.configure("Surface.TFrame", background=t["surface"])
      style.configure("Card.TFrame", background=t["card"])
      style.configure("Panel.TFrame", background=t["panel"])
      style.configure("PanelAlt.TFrame", background=t["panel_alt"])
      style.configure("Hero.TFrame", background=t["hero"])
      style.configure("Inset.TFrame", background=t["input_bg"])
      style.configure("Toolbar.TFrame", background=t["surface"])
      style.configure("Console.TFrame", background=t["log_bg"])
      style.configure("Accent.TFrame", background=t["accent"])

      # Labels
      style.configure("TLabel", background=t["bg"], foreground=t["text"], font=FONT_UI)
      style.configure("Heading.TLabel", background=t["bg"], foreground=t["text"], font=FONT_HEADING)
      style.configure("Title.TLabel", background=t["bg"], foreground=t["text"], font=FONT_TITLE)
      style.configure("Subhead.TLabel", background=t["bg"], foreground=t["text_dim"], font=FONT_SUBHEAD)
      style.configure("SectionHead.TLabel", background=t["bg"], foreground=t["text_dim"], font=FONT_LABEL)
      style.configure("Dim.TLabel", background=t["bg"], foreground=t["text_dim"], font=FONT_UI)
      style.configure("Meta.TLabel", background=t["bg"], foreground=t["text_muted"], font=FONT_SMALL)
      style.configure("Accent.TLabel", background=t["bg"], foreground=t["accent"], font=FONT_UI_BOLD)
      style.configure("Success.TLabel", background=t["bg"], foreground=t["success"], font=FONT_UI_BOLD)
      style.configure("Warning.TLabel", background=t["bg"], foreground=t["warning"], font=FONT_UI_BOLD)
      style.configure("Error.TLabel", background=t["bg"], foreground=t["error"], font=FONT_UI_BOLD)

      for style_name, bg in {
            "Surface.TLabel": t["surface"],
            "Card.TLabel": t["card"],
            "Panel.TLabel": t["panel"],
            "PanelAlt.TLabel": t["panel_alt"],
            "Hero.TLabel": t["hero"],
            "Inset.TLabel": t["input_bg"],
            "Console.TLabel": t["log_bg"],
            "Toolbar.TLabel": t["surface"],
      }.items():
            style.configure(style_name, background=bg, foreground=t["text"], font=FONT_UI)

      style.configure("CardTitle.TLabel", background=t["panel"], foreground=t["text"], font=FONT_UI_BOLD)
      style.configure("MetricName.TLabel", background=t["panel"], foreground=t["text_dim"], font=FONT_SMALL)
      style.configure("MetricValue.TLabel", background=t["panel"], foreground=t["text"], font=FONT_TITLE)
      style.configure("Badge.TLabel", background=t["accent_soft"], foreground=t["accent"], font=FONT_SMALL, padding=(8, 3))

      style.configure("TopBar.TLabel", background=t["surface"], foreground=t["text"], font=FONT_UI_BOLD)
      style.configure("TopBar.Brand.TLabel", background=t["surface"], foreground=t["text"], font=FONT_TOPBAR_BRAND)
      style.configure("TopBar.Meta.TLabel", background=t["surface"], foreground=t["text_dim"], font=FONT_SMALL)

      # Buttons
      button_pad = (16, 8)
      style.configure(
            "TButton",
            background=t["surface_alt"],
            foreground=t["text"],
            font=FONT_UI,
            padding=button_pad,
            borderwidth=0,
            relief="flat",
            focuscolor="",
      )
      style.map(
            "TButton",
            background=[("active", t["overlay"]), ("disabled", t["surface_soft"])],
            foreground=[("disabled", t["text_muted"])],
      )

      style.configure(
            "Accent.TButton",
            background=t["accent"],
            foreground="#ffffff",
            font=FONT_UI_BOLD,
            padding=button_pad,
            borderwidth=0,
            relief="flat",
      )
      style.map("Accent.TButton", background=[("active", t["accent_hover"]), ("disabled", t["border"])])

      style.configure(
            "Danger.TButton",
            background=t["error"],
            foreground="#ffffff",
            font=FONT_UI_BOLD,
            padding=button_pad,
            borderwidth=0,
            relief="flat",
      )
      style.map("Danger.TButton", background=[("active", t["accent_hover"]), ("disabled", t["border"])])

      style.configure(
            "Ghost.TButton",
            background=t["surface"],
            foreground=t["text"],
            font=FONT_UI,
            padding=button_pad,
            borderwidth=1,
            relief="solid",
            bordercolor=t["border"],
      )
      style.map(
            "Ghost.TButton",
            background=[("active", t["surface_alt"]), ("disabled", t["surface"])],
            foreground=[("disabled", t["text_muted"])],
      )

      style.configure("Small.TButton", font=FONT_SMALL, padding=(12, 6), borderwidth=0, relief="flat")
      style.configure("Chip.TButton", background=t["surface_alt"], foreground=t["text_dim"], font=FONT_SMALL, padding=(10, 5), borderwidth=0)
      style.map("Chip.TButton", background=[("active", t["overlay"])], foreground=[("active", t["text"])])

      style.configure("Tab.TButton", background=t["surface"], foreground=t["tab_inactive"], font=FONT_UI, padding=(12, 8), borderwidth=0)
      style.configure("TabActive.TButton", background=t["surface"], foreground=t["tab_active"], font=FONT_UI_BOLD, padding=(12, 8), borderwidth=0)
      style.map("Tab.TButton", background=[("active", t["surface_alt"])], foreground=[("active", t["text"])])

      style.configure("TopBar.Tab.TButton", background=t["surface"], foreground=t["tab_inactive"], font=FONT_TOPBAR_NAV, padding=(18, 12), borderwidth=0)
      style.configure("TopBar.TabActive.TButton", background=t["surface"], foreground=t["tab_active"], font=FONT_TOPBAR_NAV, padding=(18, 12), borderwidth=0)
      style.map("TopBar.Tab.TButton", background=[("active", t["surface_alt"])], foreground=[("active", t["text"])])

      style.configure("Icon.TButton", background=t["surface"], foreground=t["text"], font=FONT_UI, padding=(8, 7), borderwidth=0)
      style.configure("TopBar.Icon.TButton", background=t["surface"], foreground=t["text"], font=FONT_TOPBAR_ACTION, padding=(14, 10), borderwidth=0)
      style.map("Icon.TButton", background=[("active", t["surface_alt"])])
      style.map("TopBar.Icon.TButton", background=[("active", t["surface_alt"])])

      # Entries / Combo
      style.configure(
            "TEntry",
            fieldbackground=t["input_bg"],
            foreground=t["text"],
            insertcolor=t["text"],
            borderwidth=1,
            relief="solid",
            bordercolor=t["border"],
            padding=7,
      )
      style.map("TEntry", bordercolor=[("focus", t["accent"])], fieldbackground=[("focus", t["input_bg"])])

      style.configure(
            "TCombobox",
            fieldbackground=t["input_bg"],
            foreground=t["text"],
            padding=6,
            borderwidth=1,
            bordercolor=t["border"],
            arrowcolor=t["text_dim"],
      )
      style.map("TCombobox", bordercolor=[("focus", t["accent"])], fieldbackground=[("readonly", t["input_bg"]), ("focus", t["input_bg"])])

      root.option_add("*TCombobox*Listbox.Background", t["surface"])
      root.option_add("*TCombobox*Listbox.Foreground", t["text"])
      root.option_add("*TCombobox*Listbox.selectBackground", t["highlight"])

      # Checkbuttons
      style.configure("TCheckbutton", background=t["bg"], foreground=t["text"], font=FONT_UI, focuscolor="")
      style.configure("Surface.TCheckbutton", background=t["surface"], foreground=t["text"], font=FONT_UI, focuscolor="")
      style.configure("Panel.TCheckbutton", background=t["panel"], foreground=t["text"], font=FONT_UI, focuscolor="")

      # Separators / panes / scrollbars
      style.configure("TSeparator", background=t["border"])
      style.configure("TPanedwindow", background=t["border"])
      style.configure("Sash", sashthickness=4, gripcount=0, background=t["border"])
      style.configure("Vertical.TScrollbar", background=t["surface_alt"], troughcolor=t["bg"], borderwidth=0, arrowsize=0, width=10)
      style.configure("Horizontal.TScrollbar", background=t["surface_alt"], troughcolor=t["bg"], borderwidth=0, arrowsize=0, width=10)
      style.map("Vertical.TScrollbar", background=[("active", t["overlay"])])
      style.map("Horizontal.TScrollbar", background=[("active", t["overlay"])])

      # Treeview
      style.configure(
            "Treeview",
            background=t["surface"],
            foreground=t["text"],
            fieldbackground=t["surface"],
            font=FONT_UI,
            rowheight=32,
            borderwidth=0,
            relief="flat",
      )
      style.map("Treeview", background=[("selected", t["accent_soft"])], foreground=[("selected", t["text"])])
      style.configure(
            "Treeview.Heading",
            background=t["panel_alt"],
            foreground=t["text_dim"],
            font=FONT_LABEL,
            relief="flat",
            padding=(10, 6),
      )
      style.map("Treeview.Heading", background=[("active", t["overlay"])], foreground=[("active", t["text"])])

      # Notebook fallback
      style.configure("TNotebook", background=t["bg"], borderwidth=0)
      style.configure("TNotebook.Tab", background=t["surface"], foreground=t["text_dim"], font=FONT_UI, padding=(14, 8), borderwidth=0)
      style.map("TNotebook.Tab", background=[("selected", t["surface"])], foreground=[("selected", t["accent"])])

      # Misc
      style.configure("TLabelframe", background=t["panel"], borderwidth=1, relief="solid", bordercolor=t["border"])
      style.configure("TLabelframe.Label", background=t["panel"], foreground=t["text_dim"], font=FONT_LABEL)
      style.configure("Horizontal.TProgressbar", troughcolor=t["surface_alt"], background=t["accent"], borderwidth=0, thickness=4)

      return style

