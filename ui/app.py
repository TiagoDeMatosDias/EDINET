"""
EDINET – Flet GUI Application  (Flet ≥ 0.80)

A modern, Material-Design GUI for configuring and running the EDINET
financial-data pipeline.  Supports light / dark mode, drag-and-drop
step reordering, per-step config dialogs, and persistent setups.
"""

import copy
import json
import logging
import os
import sys
import threading
from pathlib import Path

import flet as ft
from dotenv import dotenv_values, set_key

# ── Path helpers ──────────────────────────────────────────────────────────────

def _base_dir() -> Path:
    """Project root: next to the .exe when frozen, else the repo root."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    return Path(__file__).resolve().parent.parent


BASE_DIR = _base_dir()
ENV_PATH = BASE_DIR / ".env"
CONFIG_DIR = BASE_DIR / "config"
STATE_DIR = CONFIG_DIR / "state"
REFERENCE_DIR = CONFIG_DIR / "reference"
RUN_CONFIG_PATH = STATE_DIR / "run_config.json"
SAVED_SETUPS_DIR = STATE_DIR / "saved_setups"
APP_STATE_PATH = STATE_DIR / "app_state.json"
ASSETS_DIR = BASE_DIR / "assets"

# ── Step metadata ─────────────────────────────────────────────────────────────

# Maps step name → its config key in run_config.json (steps without a config
# are intentionally absent).
STEP_CONFIG_KEY: dict[str, str] = {
    "get_documents":              "get_documents_config",
    "download_documents":         "download_documents_config",
    "populate_company_info":      "populate_company_info_config",
    "import_stock_prices_csv":    "import_stock_prices_csv_config",
    "parse_taxonomy":             "parse_taxonomy_config",
    "find_significant_predictors": "find_significant_predictors_config",
    "Multivariate_Regression":    "Multivariate_Regression_config",
    "backtest":                   "backtesting_config",
}

STEP_DISPLAY: dict[str, str] = {
    "get_documents":              "Get Documents",
    "download_documents":         "Download Documents",
    "standardize_data":           "Standardize Data",
    "populate_company_info":      "Populate Company Info",
    "import_stock_prices_csv":    "Import Stock Prices (CSV)",
    "update_stock_prices":        "Update Stock Prices",
    "parse_taxonomy":             "Parse Taxonomy",
    "generate_financial_ratios":  "Generate Financial Ratios",
    "find_significant_predictors": "Find Significant Predictors",
    "Multivariate_Regression":    "Multivariate Regression",
    "backtest":                   "Backtest Portfolio",
}

DEFAULT_STEPS = list(STEP_DISPLAY.keys())

STEPS_WITH_OVERWRITE: set[str] = {
    "standardize_data",
    "generate_financial_ratios",
    "find_significant_predictors",
}

# Default config templates so the ⚙ dialog is never empty for a
# configurable step, even if the run_config.json hasn't been set up yet.
DEFAULT_STEP_CONFIGS: dict[str, dict] = {
    "get_documents": {
        "startDate": "",
        "endDate": "",
    },
    "download_documents": {
        "docTypeCode": "120",
        "csvFlag": "1",
        "secCode": "",
        "Downloaded": "False",
    },
    "populate_company_info": {
        "csv_file": "config/reference/EdinetcodeDlInfo.csv",
    },
    "import_stock_prices_csv": {
        "csv_file": "",
        "ticker": "",
        "currency": "JPY",
        "date_column": "Date",
        "price_column": "Close",
    },
    "parse_taxonomy": {
        "xsd_file": "config/reference/jppfs_cor_2013-08-31.xsd",
    },
    "find_significant_predictors": {
        "output_file": "data/ols_results/predictor_search_results.txt",
        "winsorize_thresholds": {"lower": 0.05, "upper": 0.95},
        "alpha": 0.05,
        "dependent_variables": [],
    },
    "Multivariate_Regression": {
        "Output": "data/ols_results/ols_results_summary.txt",
        "winsorize_thresholds": {"lower": 0.05, "upper": 0.95},
        "SQL_Query": "",
    },
    "backtest": {
        "start_date": "2023-01-01",
        "end_date": "2025-12-31",
        "portfolio": {},
        "benchmark_ticker": "",
        "output_file": "data/backtest_results/backtest_report.txt",
        "risk_free_rate": 0.0,
    },
}

# ── Persistence helpers ───────────────────────────────────────────────────────

def _read_env() -> dict[str, str]:
    if ENV_PATH.exists():
        return dict(dotenv_values(str(ENV_PATH)))
    return {}


def _write_env(key: str, value: str):
    if not ENV_PATH.exists():
        ENV_PATH.touch()
    set_key(str(ENV_PATH), key, value)


def _load_app_state() -> dict:
    try:
        if APP_STATE_PATH.exists():
            with open(APP_STATE_PATH) as f:
                return json.load(f)
    except (json.JSONDecodeError, OSError):
        pass
    return {"recent_databases": []}


def _save_app_state(state: dict):
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        with open(APP_STATE_PATH, "w") as f:
            json.dump(state, f, indent=2)
    except OSError:
        pass


def _load_run_config() -> dict:
    try:
        if RUN_CONFIG_PATH.exists():
            with open(RUN_CONFIG_PATH) as f:
                return json.load(f)
    except (json.JSONDecodeError, OSError):
        pass
    return {"run_steps": {s: False for s in DEFAULT_STEPS}}


def _save_run_config(cfg: dict):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with open(RUN_CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


def _list_saved_setups() -> list[str]:
    SAVED_SETUPS_DIR.mkdir(parents=True, exist_ok=True)
    return sorted(f.stem for f in SAVED_SETUPS_DIR.glob("*.json"))


def _save_named_setup(name: str, cfg: dict) -> Path:
    """Save a named setup and return the file path."""
    SAVED_SETUPS_DIR.mkdir(parents=True, exist_ok=True)
    path = SAVED_SETUPS_DIR / f"{name}.json"
    with open(path, "w") as f:
        json.dump(cfg, f, indent=2)
    return path


def _load_named_setup(name: str) -> dict:
    with open(SAVED_SETUPS_DIR / f"{name}.json") as f:
        return json.load(f)


# ═══════════════════════════════════════════════════════════════════════════════
#  Flet Application
# ═══════════════════════════════════════════════════════════════════════════════

def main(page: ft.Page):
    # ── Page setup ────────────────────────────────────────────────────────
    page.title = "EDINET – Financial Data Pipeline"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.padding = 0
    page.theme = ft.Theme(color_scheme_seed=ft.Colors.BLUE)
    page.window.icon = str(ASSETS_DIR / "icon.ico")

    # ── Mutable state ─────────────────────────────────────────────────────
    env = _read_env()
    app_state = _load_app_state()
    run_cfg = _load_run_config()

    # Seed the recent-databases list with the current DB_PATH from .env
    # so the dropdown always shows the active database on first launch.
    current_db = env.get("DB_PATH", "")
    if current_db:
        dbs = app_state.setdefault("recent_databases", [])
        if current_db not in dbs:
            dbs.insert(0, current_db)
            _save_app_state(app_state)

    # Ordered list of [step_name, enabled, overwrite]
    steps: list[list] = []
    for name, val in run_cfg.get("run_steps", {}).items():
        if isinstance(val, dict):
            steps.append([name, bool(val.get("enabled", False)), bool(val.get("overwrite", False))])
        else:
            steps.append([name, bool(val), False])
    if not steps:
        steps = [[s, False, False] for s in DEFAULT_STEPS]

    # Per-step configuration dicts
    step_configs: dict[str, dict] = {}
    for sname in STEP_CONFIG_KEY:
        cfg_key = STEP_CONFIG_KEY[sname]
        loaded = run_cfg.get(cfg_key, {})
        step_configs[sname] = loaded if loaded else copy.deepcopy(
            DEFAULT_STEP_CONFIGS.get(sname, {})
        )

    is_running = [False]

    # ── File picker (Service in Flet ≥ 0.80) ─────────────────────────────
    fp = ft.FilePicker()
    page.services.append(fp)

    # ── SnackBar ──────────────────────────────────────────────────────────
    snack = ft.SnackBar(content=ft.Text(""), open=False)
    page.overlay.append(snack)

    def _snack(msg: str):
        snack.content = ft.Text(msg)
        snack.open = True
        page.update()

    # ── Dialog helpers ────────────────────────────────────────────────────

    def _show(dlg: ft.AlertDialog):
        page.show_dialog(dlg)

    def _pop():
        page.pop_dialog()

    # ── Recent databases ──────────────────────────────────────────────────

    def _add_recent_db(path: str):
        dbs = app_state.setdefault("recent_databases", [])
        if path in dbs:
            dbs.remove(path)
        dbs.insert(0, path)
        app_state["recent_databases"] = dbs[:20]
        _save_app_state(app_state)

    def _refresh_db_dropdown():
        current = env.get("DB_PATH", "")
        dbs = app_state.get("recent_databases", [])
        db_dropdown.options = [
            ft.dropdown.Option(key=d, text=os.path.basename(d)) for d in dbs
        ]
        db_dropdown.value = current if current in dbs else None
        page.update()

    # ── Assemble current run config ───────────────────────────────────────

    def _current_config() -> dict:
        cfg: dict = {}
        cfg["run_steps"] = {
            name: {"enabled": enabled, "overwrite": overwrite}
            for name, enabled, overwrite in steps
        }
        for sname, cfg_key in STEP_CONFIG_KEY.items():
            scfg = step_configs.get(sname)
            if scfg:
                cfg[cfg_key] = scfg
        # Persist the financial-ratios config path
        ratios = env.get("FINANCIAL_RATIOS_CONFIG_PATH", "")
        if ratios:
            cfg["financial_ratios_config_path"] = ratios
        return cfg

    # ══════════════════════════════════════════════════════════════════════
    #  Top-bar callbacks  (async where FilePicker is used)
    # ══════════════════════════════════════════════════════════════════════

    def on_db_change(e):
        path = db_dropdown.value
        if path:
            _write_env("DB_PATH", path)
            env["DB_PATH"] = path

    async def on_add_db(e):
        result = await fp.save_file(
            dialog_title="Create new database",
            file_name="edinet.db",
            allowed_extensions=["db"],
        )
        if result:
            db_path = result if result.endswith(".db") else result + ".db"
            _write_env("DB_PATH", db_path)
            env["DB_PATH"] = db_path
            _add_recent_db(db_path)
            _refresh_db_dropdown()
            _snack(f"Database set to {os.path.basename(db_path)}")

    async def on_open_db(e):
        files = await fp.pick_files(
            dialog_title="Select existing database",
            file_type=ft.FilePickerFileType.CUSTOM,
            allowed_extensions=["db"],
            allow_multiple=False,
        )
        if files:
            db_path = files[0].path
            _write_env("DB_PATH", db_path)
            env["DB_PATH"] = db_path
            _add_recent_db(db_path)
            _refresh_db_dropdown()
            _snack(f"Database loaded: {os.path.basename(db_path)}")

    def on_api_key(e):
        tf = ft.TextField(
            value=env.get("API_KEY", ""),
            label="EDINET API Key",
            password=True,
            can_reveal_password=True,
            width=420,
        )

        def save(_):
            _write_env("API_KEY", tf.value.strip())
            env["API_KEY"] = tf.value.strip()
            _pop()
            _snack("API key saved")

        _show(ft.AlertDialog(
            modal=True,
            title=ft.Text("Set API Key"),
            content=tf,
            actions=[
                ft.TextButton("Cancel", on_click=lambda _: _pop()),
                ft.Button("Save", on_click=save),
            ],
        ))

    def toggle_theme(e):
        if page.theme_mode == ft.ThemeMode.LIGHT:
            page.theme_mode = ft.ThemeMode.DARK
            theme_btn.icon = ft.Icons.LIGHT_MODE
            theme_btn.tooltip = "Switch to light mode"
        else:
            page.theme_mode = ft.ThemeMode.LIGHT
            theme_btn.icon = ft.Icons.DARK_MODE
            theme_btn.tooltip = "Switch to dark mode"
        page.update()

    # ══════════════════════════════════════════════════════════════════════
    #  Step config dialogs
    # ══════════════════════════════════════════════════════════════════════

    def _build_fields(cfg: dict, prefix: str = "") -> list[tuple]:
        """Return [(dotted_key | None, Control), …] for a config dict."""
        fields: list[tuple] = []
        for key, val in cfg.items():
            fk = f"{prefix}.{key}" if prefix else key
            if isinstance(val, dict):
                fields.append(
                    (None, ft.Text(key, weight=ft.FontWeight.BOLD, size=13))
                )
                fields.extend(_build_fields(val, fk))
            elif isinstance(val, list):
                text = "\n".join(str(v) for v in val)
                fields.append((
                    fk,
                    ft.TextField(
                        label=key, value=text, dense=True,
                        multiline=True, min_lines=2, max_lines=6,
                    ),
                ))
            else:
                fields.append((
                    fk,
                    ft.TextField(label=key, value=str(val), dense=True),
                ))
        return fields

    def _read_fields(fields: list[tuple], original: dict) -> dict:
        """Read edited values from form fields back into a config dict."""
        result = copy.deepcopy(original)
        for key_path, ctrl in fields:
            if key_path is None or not isinstance(ctrl, ft.TextField):
                continue
            raw = ctrl.value
            parts = key_path.split(".")
            target = result
            for p in parts[:-1]:
                target = target.setdefault(p, {})
            last = parts[-1]
            orig_val = target.get(last)

            if isinstance(orig_val, bool):
                target[last] = raw.strip().lower() in ("true", "1", "yes")
            elif isinstance(orig_val, float):
                try:
                    target[last] = float(raw)
                except ValueError:
                    target[last] = raw
            elif isinstance(orig_val, int):
                try:
                    target[last] = int(raw)
                except ValueError:
                    target[last] = raw
            elif isinstance(orig_val, list):
                try:
                    target[last] = json.loads(raw)
                except json.JSONDecodeError:
                    target[last] = [
                        ln.strip() for ln in raw.splitlines() if ln.strip()
                    ]
            else:
                target[last] = raw
        return result

    def open_step_config(step_name: str):
        if step_name == "backtest":
            _open_backtest_config()
            return
        if step_name == "import_stock_prices_csv":
            _open_import_csv_config()
            return

        current = step_configs.get(step_name, {})
        if not current:
            current = copy.deepcopy(DEFAULT_STEP_CONFIGS.get(step_name, {}))
        if not current:
            _snack(f"No configuration for {STEP_DISPLAY.get(step_name, step_name)}")
            return
        fields = _build_fields(current)

        def save(_):
            step_configs[step_name] = _read_fields(fields, current)
            _pop()
            _snack(
                f"Config for '{STEP_DISPLAY.get(step_name, step_name)}' updated"
            )

        _show(ft.AlertDialog(
            modal=True,
            title=ft.Text(
                f"Configure: {STEP_DISPLAY.get(step_name, step_name)}"
            ),
            content=ft.Column(
                [ctrl for _, ctrl in fields],
                scroll=ft.ScrollMode.AUTO,
                width=500,
                height=400,
            ),
            actions=[
                ft.TextButton("Cancel", on_click=lambda _: _pop()),
                ft.Button("Save", on_click=save),
            ],
        ))

    # ── Custom CSV stock-price import config dialog ─────────────────────

    def _open_import_csv_config():
        """Open a dedicated dialog for configuring CSV stock-price import."""
        current = step_configs.get("import_stock_prices_csv", {})
        if not current:
            current = copy.deepcopy(DEFAULT_STEP_CONFIGS.get("import_stock_prices_csv", {}))

        csv_path_tf = ft.TextField(
            label="CSV File Path",
            value=current.get("csv_file", ""),
            dense=True,
            width=380,
            read_only=True,
        )

        async def _pick_csv(_):
            files = await fp.pick_files(
                dialog_title="Select stock-price CSV file",
                file_type=ft.FilePickerFileType.CUSTOM,
                allowed_extensions=["csv"],
                allow_multiple=False,
            )
            if files:
                csv_path_tf.value = files[0].path
                page.update()

        browse_btn = ft.IconButton(
            icon=ft.Icons.FOLDER_OPEN,
            tooltip="Browse for CSV file",
            on_click=_pick_csv,
        )

        ticker_tf = ft.TextField(
            label="Ticker",
            value=current.get("ticker", ""),
            dense=True,
            width=200,
            hint_text="e.g. 7203",
        )
        currency_tf = ft.TextField(
            label="Currency",
            value=current.get("currency", "JPY"),
            dense=True,
            width=200,
            hint_text="e.g. JPY, USD",
        )
        date_col_tf = ft.TextField(
            label="Date Column",
            value=current.get("date_column", "Date"),
            dense=True,
            width=200,
            hint_text="CSV column for date",
        )
        price_col_tf = ft.TextField(
            label="Price Column",
            value=current.get("price_column", "Close"),
            dense=True,
            width=200,
            hint_text="CSV column for price",
        )

        def save(_):
            if not csv_path_tf.value.strip():
                _snack("Please select a CSV file")
                return
            if not ticker_tf.value.strip():
                _snack("Please enter a ticker symbol")
                return
            step_configs["import_stock_prices_csv"] = {
                "csv_file": csv_path_tf.value.strip(),
                "ticker": ticker_tf.value.strip(),
                "currency": currency_tf.value.strip() or "JPY",
                "date_column": date_col_tf.value.strip() or "Date",
                "price_column": price_col_tf.value.strip() or "Close",
            }
            _pop()
            _snack("Import CSV config updated")

        _show(ft.AlertDialog(
            modal=True,
            title=ft.Text("Configure: Import Stock Prices (CSV)"),
            content=ft.Column(
                [
                    ft.Text(
                        "Select a CSV file and map its columns to the database fields.",
                        size=12, color=ft.Colors.GREY_500,
                    ),
                    ft.Row([csv_path_tf, browse_btn], spacing=4),
                    ft.Divider(height=1),
                    ft.Row([ticker_tf, currency_tf], spacing=16),
                    ft.Divider(height=1),
                    ft.Text("Column Mapping", weight=ft.FontWeight.BOLD, size=13),
                    ft.Text(
                        "Specify which columns in the CSV correspond to Date and Price.",
                        size=11, color=ft.Colors.GREY_500,
                    ),
                    ft.Row([date_col_tf, price_col_tf], spacing=16),
                ],
                scroll=ft.ScrollMode.AUTO,
                width=500,
                height=320,
                spacing=8,
            ),
            actions=[
                ft.TextButton("Cancel", on_click=lambda _: _pop()),
                ft.Button("Save", on_click=save),
            ],
        ))

    # ── Custom backtest config dialog ─────────────────────────────────

    def _open_backtest_config():
        """Open a dedicated dialog for backtesting configuration."""
        current = step_configs.get("backtest", {})
        if not current:
            current = copy.deepcopy(DEFAULT_STEP_CONFIGS.get("backtest", {}))
        raw_portfolio = current.get("portfolio", {})

        # Normalise legacy plain-float entries to the new dict format
        portfolio: dict[str, dict] = {}
        for tk, spec in raw_portfolio.items():
            if isinstance(spec, (int, float)):
                portfolio[tk] = {"mode": "weight", "value": spec * 100}
            elif isinstance(spec, dict):
                mode = spec.get("mode", "weight")
                val = spec.get("value", 0)
                # Store weight values as percentages for display
                if mode == "weight":
                    portfolio[tk] = {"mode": "weight", "value": val * 100}
                else:
                    portfolio[tk] = {"mode": mode, "value": val}
            else:
                portfolio[tk] = {"mode": "weight", "value": 0}

        start_tf = ft.TextField(
            label="Start Date (YYYY-MM-DD)",
            value=current.get("start_date", ""),
            dense=True,
            width=220,
        )
        end_tf = ft.TextField(
            label="End Date (YYYY-MM-DD)",
            value=current.get("end_date", ""),
            dense=True,
            width=220,
        )
        bench_tf = ft.TextField(
            label="Benchmark Ticker (optional)",
            value=current.get("benchmark_ticker", ""),
            dense=True,
            width=220,
        )
        output_tf = ft.TextField(
            label="Output File",
            value=current.get("output_file", "data/backtest_results/backtest_report.txt"),
            dense=True,
            width=460,
        )
        risk_free_tf = ft.TextField(
            label="Risk-Free Rate (%)",
            value=str(current.get("risk_free_rate", 0.0) * 100),
            dense=True,
            width=220,
            hint_text="e.g. 2.5 for 2.5%",
        )
        capital_tf = ft.TextField(
            label="Initial Capital (0 = omit)",
            value=str(int(current.get("initial_capital", 0))),
            dense=True,
            width=220,
            hint_text="e.g. 1000000",
        )

        # Portfolio management
        ticker_tf = ft.TextField(label="Ticker", dense=True, width=130)
        mode_dd = ft.Dropdown(
            label="Mode",
            value="weight",
            options=[
                ft.dropdown.Option("weight", "Weight %"),
                ft.dropdown.Option("shares", "Shares"),
                ft.dropdown.Option("value", "Value (¥)"),
            ],
            dense=True,
            width=130,
        )
        alloc_tf = ft.TextField(label="Amount", dense=True, width=110)
        portfolio_list = ft.Column(spacing=2, scroll=ft.ScrollMode.AUTO, height=180)
        weight_total_text = ft.Text("", size=12)

        _MODE_LABELS = {"weight": "%", "shares": "shares", "value": "¥"}

        def _update_weight_total():
            weight_sum = sum(
                e["value"] for e in portfolio.values() if e["mode"] == "weight"
            )
            has_fixed = any(
                e["mode"] in ("shares", "value") for e in portfolio.values()
            )
            if not portfolio:
                weight_total_text.value = ""
                weight_total_text.color = None
                return
            parts = []
            if weight_sum > 0:
                if abs(weight_sum - 100.0) < 0.01 and not has_fixed:
                    parts.append(f"Weight total: {weight_sum:.1f}% ✓")
                    weight_total_text.color = ft.Colors.GREEN_700
                elif has_fixed:
                    parts.append(f"Weight total: {weight_sum:.1f}%")
                    weight_total_text.color = ft.Colors.BLUE_400
                else:
                    parts.append(
                        f"Weight total: {weight_sum:.1f}% "
                        f"(will be normalised)"
                    )
                    weight_total_text.color = ft.Colors.ORANGE_400
            if has_fixed:
                n_shares = sum(
                    1 for e in portfolio.values() if e["mode"] == "shares"
                )
                n_value = sum(
                    1 for e in portfolio.values() if e["mode"] == "value"
                )
                fixed_parts = []
                if n_shares:
                    fixed_parts.append(f"{n_shares} by shares")
                if n_value:
                    fixed_parts.append(f"{n_value} by value")
                parts.append("Fixed: " + ", ".join(fixed_parts))
                if not weight_sum:
                    weight_total_text.color = ft.Colors.GREEN_700
            weight_total_text.value = "  |  ".join(parts)

        def _rebuild_portfolio_list():
            portfolio_list.controls.clear()
            for tk, entry in portfolio.items():
                mode = entry["mode"]
                val = entry["value"]
                lbl = _MODE_LABELS.get(mode, "")
                if mode == "weight":
                    display = f"{val:.1f}{lbl}"
                elif mode == "shares":
                    display = f"{val:g} {lbl}"
                else:
                    display = f"{lbl}{val:,.0f}"
                portfolio_list.controls.append(
                    ft.Row(
                        [
                            ft.Text(tk, width=100, size=13),
                            ft.Text(
                                mode.capitalize(),
                                width=60,
                                size=11,
                                color=ft.Colors.BLUE_400,
                            ),
                            ft.Text(display, width=100, size=13),
                            ft.IconButton(
                                icon=ft.Icons.DELETE,
                                icon_size=16,
                                icon_color=ft.Colors.RED_400,
                                tooltip="Remove",
                                on_click=lambda _, t=tk: _remove_ticker(t),
                            ),
                        ],
                        spacing=4,
                    )
                )
            _update_weight_total()
            page.update()

        def _add_ticker(_):
            tk = ticker_tf.value.strip()
            mode = mode_dd.value or "weight"
            raw = alloc_tf.value.strip()
            if not tk:
                _snack("Enter a ticker symbol")
                return
            try:
                val = float(raw)
            except ValueError:
                _snack("Amount must be a number")
                return
            if val <= 0:
                _snack("Amount must be positive")
                return
            if mode == "weight" and val > 100:
                _snack("Weight cannot exceed 100%")
                return
            portfolio[tk] = {"mode": mode, "value": val}
            ticker_tf.value = ""
            alloc_tf.value = ""
            _rebuild_portfolio_list()

        def _remove_ticker(tk: str):
            portfolio.pop(tk, None)
            _rebuild_portfolio_list()

        add_btn = ft.IconButton(
            icon=ft.Icons.ADD_CIRCLE,
            icon_color=ft.Colors.GREEN_700,
            tooltip="Add ticker",
            on_click=_add_ticker,
        )

        _rebuild_portfolio_list()

        def save(_):
            # Validate
            if not portfolio:
                _snack("Portfolio is empty — add at least one ticker")
                return

            # Check weight-only portfolios for sum, but only warn
            weight_sum = sum(
                e["value"] for e in portfolio.values() if e["mode"] == "weight"
            )
            has_fixed = any(
                e["mode"] in ("shares", "value") for e in portfolio.values()
            )
            if not has_fixed and abs(weight_sum - 100.0) > 0.01:
                _snack(
                    f"⚠ Weights sum to {weight_sum:.1f}% (not 100%). "
                    f"They will be normalised at run time."
                )
                # Still allow saving — just a warning

            try:
                rf = float(risk_free_tf.value.strip()) / 100.0
            except ValueError:
                rf = 0.0
            try:
                cap = float(capital_tf.value.strip())
            except ValueError:
                cap = 0.0

            # Convert portfolio to the storage format expected by backtesting
            saved_portfolio: dict = {}
            for tk, entry in portfolio.items():
                mode = entry["mode"]
                val = entry["value"]
                if mode == "weight":
                    # Store as fraction (0.0–1.0) for weight mode
                    saved_portfolio[tk] = {
                        "mode": "weight",
                        "value": val / 100.0,
                    }
                else:
                    saved_portfolio[tk] = {
                        "mode": mode,
                        "value": val,
                    }

            step_configs["backtest"] = {
                "start_date": start_tf.value.strip(),
                "end_date": end_tf.value.strip(),
                "portfolio": saved_portfolio,
                "benchmark_ticker": bench_tf.value.strip(),
                "output_file": output_tf.value.strip(),
                "risk_free_rate": rf,
                "initial_capital": cap,
            }
            _pop()
            _snack("Backtest config updated")

        _show(ft.AlertDialog(
            modal=True,
            title=ft.Text("Configure: Backtest Portfolio"),
            content=ft.Column(
                [
                    ft.Row([start_tf, end_tf], spacing=16),
                    ft.Row([bench_tf, risk_free_tf], spacing=16),
                    capital_tf,
                    output_tf,
                    ft.Divider(height=1),
                    ft.Text("Portfolio", weight=ft.FontWeight.BOLD, size=14),
                    ft.Row(
                        [ticker_tf, mode_dd, alloc_tf, add_btn],
                        spacing=8,
                    ),
                    portfolio_list,
                    weight_total_text,
                ],
                scroll=ft.ScrollMode.AUTO,
                width=520,
                height=500,
                spacing=8,
            ),
            actions=[
                ft.TextButton("Cancel", on_click=lambda _: _pop()),
                ft.Button("Save", on_click=save),
            ],
        ))

    # ══════════════════════════════════════════════════════════════════════
    #  Drag-and-drop step list  (compact design)
    # ══════════════════════════════════════════════════════════════════════

    steps_column = ft.Column(spacing=2, scroll=ft.ScrollMode.AUTO, expand=True)

    def _make_drop_handler(target_idx: int):
        def handler(e):
            try:
                src_idx = int(e.src.data)
            except (ValueError, TypeError, AttributeError):
                return
            if src_idx != target_idx:
                item = steps.pop(src_idx)
                steps.insert(target_idx, item)
                _rebuild_steps()
        return handler

    def _toggle_step(idx: int, value: bool):
        steps[idx][1] = value
        _rebuild_steps()

    def _toggle_overwrite(idx: int, value: bool):
        steps[idx][2] = value

    def _rebuild_steps():
        """Rebuild the compact step list UI."""
        steps_column.controls.clear()

        for idx, (sname, enabled, overwrite) in enumerate(steps):
            has_cfg = sname in STEP_CONFIG_KEY
            display = STEP_DISPLAY.get(sname, sname)
            accent = ft.Colors.GREEN_700 if enabled else ft.Colors.RED_400

            cb = ft.Checkbox(
                value=enabled,
                active_color=ft.Colors.GREEN_700,
                on_change=lambda e, i=idx: _toggle_step(i, e.control.value),
            )

            row_items: list[ft.Control] = [
                ft.Icon(ft.Icons.DRAG_HANDLE, color=ft.Colors.GREY_400, size=16),
                cb,
                ft.Text(display, expand=True, size=13),
            ]

            if has_cfg and enabled:
                row_items.append(
                    ft.IconButton(
                        icon=ft.Icons.SETTINGS,
                        icon_size=16,
                        icon_color=ft.Colors.BLUE_400,
                        tooltip="Configure step",
                        style=ft.ButtonStyle(padding=4),
                        on_click=lambda _, sn=sname: open_step_config(sn),
                    )
                )

            if sname in STEPS_WITH_OVERWRITE and enabled:
                row_items.append(
                    ft.Checkbox(
                        label="Overwrite",
                        value=overwrite,
                        active_color=ft.Colors.ORANGE_700,
                        on_change=lambda e, i=idx: _toggle_overwrite(i, e.control.value),
                    )
                )

            row_container = ft.Container(
                content=ft.Row(
                    row_items,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                    spacing=4,
                ),
                padding=ft.Padding(left=8, right=8, top=2, bottom=2),
                border_radius=6,
                border=ft.Border.only(left=ft.BorderSide(3, accent)),
                bgcolor=ft.Colors.SURFACE_CONTAINER_LOW,
            )

            ghost = ft.Container(
                content=ft.Text(display, color=ft.Colors.GREY_500, size=12),
                padding=ft.Padding(left=8, right=8, top=2, bottom=2),
                border_radius=6,
                border=ft.Border.all(1, ft.Colors.GREY_300),
                opacity=0.4,
            )

            steps_column.controls.append(
                ft.DragTarget(
                    group="steps",
                    content=ft.Draggable(
                        group="steps",
                        data=str(idx),
                        content=row_container,
                        content_when_dragging=ghost,
                    ),
                    on_accept=_make_drop_handler(idx),
                )
            )

        page.update()

    # ══════════════════════════════════════════════════════════════════════
    #  Bottom panel callbacks
    # ══════════════════════════════════════════════════════════════════════

    ratios_path_text = ft.Text(
        f"Ratios config: {env.get('FINANCIAL_RATIOS_CONFIG_PATH', 'Not set')}",
        size=11,
        color=ft.Colors.GREY_500,
        italic=True,
    )

    async def on_ratios_btn(e):
        files = await fp.pick_files(
            dialog_title="Select financial_ratios_config.json",
            file_type=ft.FilePickerFileType.CUSTOM,
            allowed_extensions=["json"],
            allow_multiple=False,
        )
        if files:
            path = files[0].path
            _write_env("FINANCIAL_RATIOS_CONFIG_PATH", path)
            env["FINANCIAL_RATIOS_CONFIG_PATH"] = path
            ratios_path_text.value = f"Ratios config: {path}"
            _snack(f"Ratios config set to {os.path.basename(path)}")

    def on_save_setup(e):
        tf = ft.TextField(label="Setup name", autofocus=True, width=320)

        def save(_):
            name = tf.value.strip()
            if not name:
                return
            saved_path = _save_named_setup(name, _current_config())
            _pop()
            _snack(f"Setup '{name}' saved → {saved_path}")

        _show(ft.AlertDialog(
            modal=True,
            title=ft.Text("Save Setup"),
            content=ft.Column(
                [
                    tf,
                    ft.Text(
                        f"Setups are stored in:\n{SAVED_SETUPS_DIR}",
                        size=11, color=ft.Colors.GREY_500, italic=True,
                    ),
                ],
                tight=True,
                spacing=8,
            ),
            actions=[
                ft.TextButton("Cancel", on_click=lambda _: _pop()),
                ft.Button("Save", on_click=save),
            ],
        ))

    def on_load_setup(e):
        setups = _list_saved_setups()
        if not setups:
            _snack(f"No saved setups found in {SAVED_SETUPS_DIR}")
            return

        dd = ft.Dropdown(
            label="Select setup",
            options=[ft.dropdown.Option(s) for s in setups],
            width=320,
        )

        def load(_):
            name = dd.value
            if not name:
                return
            loaded = _load_named_setup(name)
            # Update steps
            steps.clear()
            for sn, val in loaded.get("run_steps", {}).items():
                if isinstance(val, dict):
                    steps.append([sn, bool(val.get("enabled", False)), bool(val.get("overwrite", False))])
                else:
                    steps.append([sn, bool(val), False])
            # Update step configs
            for sn, cfg_key in STEP_CONFIG_KEY.items():
                step_configs[sn] = loaded.get(cfg_key, copy.deepcopy(
                    DEFAULT_STEP_CONFIGS.get(sn, {})
                ))
            # Restore financial-ratios config path
            ratios = loaded.get("financial_ratios_config_path", "")
            if ratios:
                _write_env("FINANCIAL_RATIOS_CONFIG_PATH", ratios)
                env["FINANCIAL_RATIOS_CONFIG_PATH"] = ratios
                ratios_path_text.value = f"Ratios config: {ratios}"
            _pop()
            _rebuild_steps()
            _snack(f"Setup '{name}' loaded from {SAVED_SETUPS_DIR / (name + '.json')}")

        _show(ft.AlertDialog(
            modal=True,
            title=ft.Text("Load Setup"),
            content=ft.Column(
                [
                    dd,
                    ft.Text(
                        f"Loading from: {SAVED_SETUPS_DIR}",
                        size=11, color=ft.Colors.GREY_500, italic=True,
                    ),
                ],
                tight=True,
                spacing=8,
            ),
            actions=[
                ft.TextButton("Cancel", on_click=lambda _: _pop()),
                ft.Button("Load", on_click=load),
            ],
        ))

    # ══════════════════════════════════════════════════════════════════════
    #  Log output & Run
    # ══════════════════════════════════════════════════════════════════════

    log_output = ft.TextField(
        value="",
        multiline=True,
        read_only=True,
        min_lines=4,
        max_lines=8,
        expand=True,
        text_size=11,
        dense=True,
        border_color=ft.Colors.TRANSPARENT,
        filled=True,
    )

    progress = ft.ProgressBar(visible=False)

    class _UILogHandler(logging.Handler):
        """Forward log records into the Flet text field."""

        def emit(self, record):
            try:
                msg = self.format(record)
                log_output.value = (log_output.value or "") + msg + "\n"
                page.update()
            except Exception:
                pass

    def on_run(e):
        if is_running[0]:
            return
        is_running[0] = True
        run_btn.disabled = True
        progress.visible = True
        log_output.value = ""
        page.update()

        # Persist the current config before running
        _save_run_config(_current_config())

        def _do_run():
            handler = _UILogHandler()
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s  %(levelname)s  %(message)s",
                    datefmt="%H:%M:%S",
                )
            )
            try:
                proj = str(BASE_DIR)
                if proj not in sys.path:
                    sys.path.insert(0, proj)

                from config import Config
                Config._instance = None

                from src.logger import setup_logging
                setup_logging()

                # Add UI handler AFTER setup_logging so it isn't
                # removed by the handler cleanup in setup_logging().
                root_logger = logging.getLogger()
                root_logger.addHandler(handler)
                handler.setLevel(logging.INFO)

                import src.orchestrator as orchestrator
                orchestrator.run()

                log_output.value = (
                    (log_output.value or "")
                    + "\n✅ Run completed successfully!\n"
                )
            except Exception as ex:
                log_output.value = (
                    (log_output.value or "")
                    + f"\n❌ Error: {ex}\n"
                )
            finally:
                # root_logger may not be bound if setup_logging() failed
                try:
                    logging.getLogger().removeHandler(handler)
                except Exception:
                    pass
                is_running[0] = False
                run_btn.disabled = False
                progress.visible = False
                page.update()

        threading.Thread(target=_do_run, daemon=True).start()

    run_btn = ft.Button(
        "Run",
        icon=ft.Icons.PLAY_ARROW,
        color=ft.Colors.WHITE,
        bgcolor=ft.Colors.GREEN_700,
        style=ft.ButtonStyle(
            shape=ft.RoundedRectangleBorder(radius=8),
        ),
        height=44,
        on_click=on_run,
    )

    # ══════════════════════════════════════════════════════════════════════
    #  Top-bar controls
    # ══════════════════════════════════════════════════════════════════════

    db_dropdown = ft.Dropdown(
        label="Database",
        width=260,
        dense=True,
        text_size=13,
        on_select=on_db_change,
    )
    # Populate dropdown (without page.update — page not fully built yet)
    dbs = app_state.get("recent_databases", [])
    db_dropdown.options = [
        ft.dropdown.Option(key=d, text=os.path.basename(d)) for d in dbs
    ]
    db_dropdown.value = current_db if current_db in dbs else None

    theme_btn = ft.IconButton(
        icon=ft.Icons.DARK_MODE,
        tooltip="Switch to dark mode",
        on_click=toggle_theme,
    )

    app_bar = ft.AppBar(
        leading=ft.Image(src="icon.svg", width=28, height=28),
        leading_width=40,
        title=ft.Text("EDINET", weight=ft.FontWeight.BOLD),
        center_title=False,
        actions=[
            db_dropdown,
            ft.IconButton(
                icon=ft.Icons.ADD,
                tooltip="Create new database",
                on_click=on_add_db,
            ),
            ft.IconButton(
                icon=ft.Icons.FOLDER_OPEN,
                tooltip="Load existing database",
                on_click=on_open_db,
            ),
            ft.VerticalDivider(width=1),
            ft.Button(
                "API Key",
                icon=ft.Icons.KEY,
                on_click=on_api_key,
            ),
            ft.VerticalDivider(width=1),
            theme_btn,
        ],
    )

    # ══════════════════════════════════════════════════════════════════════
    #  Page layout — two-panel design
    # ══════════════════════════════════════════════════════════════════════

    # ── Upper panel: Run Steps ────────────────────────────────────────────
    top_panel = ft.Container(
        content=ft.Column(
            [
                ft.Row(
                    [
                        ft.Text("Run Steps", size=16, weight=ft.FontWeight.W_600),
                        ft.Container(expand=True),
                        ft.OutlinedButton(
                            "Financial Ratios Config",
                            icon=ft.Icons.DESCRIPTION,
                            on_click=on_ratios_btn,
                            style=ft.ButtonStyle(padding=ft.Padding(8, 4, 8, 4)),
                        ),
                    ],
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                ),
                ft.Text(
                    "Drag to reorder  •  Check to enable  •  ⚙ to configure",
                    size=11, color=ft.Colors.GREY_500,
                ),
                ratios_path_text,
                ft.Divider(height=1),
                steps_column,
            ],
            spacing=4,
            expand=True,
        ),
        padding=ft.Padding(left=20, right=20, top=12, bottom=8),
        expand=True,
    )

    # ── Lower panel: Actions + Output ─────────────────────────────────────
    bottom_panel = ft.Container(
        content=ft.Column(
            [
                ft.Row(
                    [
                        ft.OutlinedButton(
                            "Save Setup",
                            icon=ft.Icons.SAVE,
                            on_click=on_save_setup,
                        ),
                        ft.OutlinedButton(
                            "Load Setup",
                            icon=ft.Icons.FOLDER_OPEN,
                            on_click=on_load_setup,
                        ),
                        ft.Container(expand=True),
                        run_btn,
                    ],
                ),
                progress,
                ft.Text("Output", size=12, weight=ft.FontWeight.W_500),
                log_output,
            ],
            spacing=6,
        ),
        padding=ft.Padding(left=20, right=20, top=8, bottom=12),
        bgcolor=ft.Colors.SURFACE_CONTAINER,
        border=ft.Border.only(top=ft.BorderSide(1, ft.Colors.OUTLINE_VARIANT)),
    )

    page.appbar = app_bar
    page.add(
        ft.Column(
            [top_panel, bottom_panel],
            spacing=0,
            expand=True,
        )
    )

    # Initial render of step list
    _rebuild_steps()


# ── Entry point ───────────────────────────────────────────────────────────────

def launch():
    """Start the EDINET GUI."""
    ft.run(main, assets_dir=str(ASSETS_DIR))


if __name__ == "__main__":
    launch()
