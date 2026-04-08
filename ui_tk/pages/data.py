"""Data workspace view with project resources and quick navigation."""

import os
from pathlib import Path
from tkinter import ttk

from ui_tk import controllers as ctrl
from ui_tk.shared.widgets import PageHeader, RoundedButton, SectionCard, StatTile
from ui_tk.style import FONT_SMALL, PAD


class DataPage(ttk.Frame):
    def __init__(self, parent, app=None, **kw):
        super().__init__(parent, style="App.TFrame", **kw)
        self.app = app

        body = ttk.Frame(self, style="App.TFrame")
        body.pack(fill="both", expand=True, padx=PAD * 2, pady=PAD * 2)
        body.grid_columnconfigure(0, weight=1)
        body.grid_columnconfigure(1, weight=1)
        body.grid_rowconfigure(2, weight=1)

        self._header = PageHeader(
            body,
            title="Data Workspace",
            subtitle="Project paths, reference assets, and quick links into downstream tasks.",
            context="This replaces the old placeholder with an operational landing surface.",
        )
        self._header.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, PAD * 2))

        hero = ttk.Frame(body, style="Hero.TFrame")
        hero.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, PAD * 2))
        for column in range(3):
            hero.grid_columnconfigure(column, weight=1)

        db_path = self._database_path()
        db_name = Path(db_path).name if db_path else "No default database"
        self._database_tile = StatTile(hero, "Default Database", db_name, db_path or "Configure in the active workflow", style="Hero.TFrame")
        self._database_tile.grid(row=0, column=0, sticky="nsew", padx=(0, PAD // 2), pady=PAD // 2)
        self._reference_tile = StatTile(hero, "Reference Assets", str(self._count_files(ctrl.CONFIG_DIR / "reference")), "Files in config/reference", style="Hero.TFrame")
        self._reference_tile.grid(row=0, column=1, sticky="nsew", padx=PAD // 2, pady=PAD // 2)
        self._output_tile = StatTile(hero, "Output Folders", str(self._count_directories(ctrl.BASE_DIR / "data")), "Primary directories under data/", style="Hero.TFrame")
        self._output_tile.grid(row=0, column=2, sticky="nsew", padx=(PAD // 2, 0), pady=PAD // 2)

        self._resource_card = SectionCard(body, "Project Resources", "Stable locations used by the UI and pipeline workflows.")
        self._resource_card.grid(row=2, column=0, sticky="nsew", padx=(0, PAD))
        self._build_resource_list(self._resource_card.body)

        self._action_card = SectionCard(body, "Next Actions", "Jump from project resources into the tool surface that uses them.")
        self._action_card.grid(row=2, column=1, sticky="nsew")
        self._build_action_panel(self._action_card.body)

        if self.app is not None:
            self.app.set_context("Data Workspace", "Reference files, state files, and data outputs are summarized here.")

    def _database_path(self) -> str:
        getter = getattr(ctrl, "get_default_database_path", None)
        if callable(getter):
            try:
                return getter() or ""
            except Exception:
                return ""
        return ""

    @staticmethod
    def _count_files(path: Path) -> int:
        try:
            return sum(1 for child in path.iterdir() if child.is_file())
        except OSError:
            return 0

    @staticmethod
    def _count_directories(path: Path) -> int:
        try:
            return sum(1 for child in path.iterdir() if child.is_dir())
        except OSError:
            return 0

    def _build_resource_list(self, parent):
        rows = [
            ("State", ctrl.STATE_DIR),
            ("Examples", ctrl.EXAMPLES_DIR),
            ("Reference", ctrl.CONFIG_DIR / "reference"),
            ("Backtests", ctrl.BASE_DIR / "data" / "backtest_results"),
            ("Backtest Sets", ctrl.BASE_DIR / "data" / "backtest_set_results"),
            ("Logs", ctrl.BASE_DIR / "logs"),
        ]
        for label, path in rows:
            row = ttk.Frame(parent, style="Panel.TFrame")
            row.pack(fill="x", pady=(0, PAD))
            left = ttk.Frame(row, style="Panel.TFrame")
            left.pack(side="left", fill="x", expand=True)
            ttk.Label(left, text=label, style="Panel.TLabel").pack(anchor="w")
            ttk.Label(left, text=str(path), style="Panel.TLabel", font=FONT_SMALL).pack(anchor="w", pady=(2, 0))
            ttk.Label(left, text=self._describe_path(path), style="Panel.TLabel", font=FONT_SMALL).pack(anchor="w", pady=(4, 0))

    def _build_action_panel(self, parent):
        actions = [
            ("Open Orchestrator", "Orchestrator", "Configure source and target paths for pipeline steps."),
            ("Open Screening", "Screening", "Use the active database to search for candidate companies."),
            ("Open Security Analysis", "Security Analysis", "Inspect an individual company after ingest or screening."),
        ]
        self._action_buttons: list[RoundedButton] = []
        for text, target, desc in actions:
            row = ttk.Frame(parent, style="Panel.TFrame")
            row.pack(fill="x", pady=(0, PAD))
            info = ttk.Frame(row, style="Panel.TFrame")
            info.pack(side="left", fill="x", expand=True)
            ttk.Label(info, text=text, style="Panel.TLabel").pack(anchor="w")
            ttk.Label(info, text=desc, style="Panel.TLabel", font=FONT_SMALL, wraplength=300, justify="left").pack(anchor="w", pady=(2, 0))
            button = RoundedButton(row, text="Open", style="Ghost.TButton", command=lambda name=target: self.app.switch_view(name) if self.app else None)
            button.pack(side="right")
            self._action_buttons.append(button)

        note = ttk.Frame(parent, style="Panel.TFrame")
        note.pack(fill="x", pady=(PAD, 0))
        ttk.Label(note, text="What belongs here", style="Panel.TLabel").pack(anchor="w")
        ttk.Label(
            note,
            text="The Data view is intentionally operational rather than analytical. It should help users verify where inputs, references, state, and outputs live before they move into a specific workflow.",
            style="Panel.TLabel",
            font=FONT_SMALL,
            wraplength=340,
            justify="left",
        ).pack(anchor="w", pady=(4, 0))

    @staticmethod
    def _describe_path(path: Path) -> str:
        try:
            if path.is_dir():
                count = sum(1 for _ in path.iterdir())
                return f"Directory available, {count} immediate item{'s' if count != 1 else ''}."
            if path.is_file():
                size = os.path.getsize(path)
                return f"File available, {size:,} bytes."
        except OSError:
            pass
        return "Path not found in the current workspace state."

    def reapply_colors(self):
        for button in getattr(self, "_action_buttons", []):
            button.reapply_colors()
