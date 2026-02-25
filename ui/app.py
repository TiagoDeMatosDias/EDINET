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
RUN_CONFIG_PATH = CONFIG_DIR / "run_config.json"
SAVED_SETUPS_DIR = CONFIG_DIR / "saved_setups"
APP_STATE_PATH = CONFIG_DIR / "app_state.json"
ASSETS_DIR = BASE_DIR / "assets"

# ── Step metadata ─────────────────────────────────────────────────────────────

# Maps step name → its config key in run_config.json (steps without a config
# are intentionally absent).
STEP_CONFIG_KEY: dict[str, str] = {
    "get_documents":              "get_documents_config",
    "download_documents":         "download_documents_config",
    "populate_company_info":      "populate_company_info_config",
    "parse_taxonomy":             "parse_taxonomy_config",
    "find_significant_predictors": "find_significant_predictors_config",
    "Multivariate_Regression":    "Multivariate_Regression_config",
}

STEP_DISPLAY: dict[str, str] = {
    "get_documents":              "Get Documents",
    "download_documents":         "Download Documents",
    "standardize_data":           "Standardize Data",
    "populate_company_info":      "Populate Company Info",
    "update_stock_prices":        "Update Stock Prices",
    "parse_taxonomy":             "Parse Taxonomy",
    "generate_financial_ratios":  "Generate Financial Ratios",
    "find_significant_predictors": "Find Significant Predictors",
    "Multivariate_Regression":    "Multivariate Regression",
}

DEFAULT_STEPS = list(STEP_DISPLAY.keys())

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
        "csv_file": "config/EdinetcodeDlInfo.csv",
    },
    "parse_taxonomy": {
        "xsd_file": "config/jppfs_cor_2013-08-31.xsd",
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
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
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
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
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

    # Ordered list of [step_name, enabled]
    steps: list[list] = [
        [name, bool(enabled)]
        for name, enabled in run_cfg.get("run_steps", {}).items()
    ]
    if not steps:
        steps = [[s, False] for s in DEFAULT_STEPS]

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
        cfg["run_steps"] = {name: enabled for name, enabled in steps}
        for sname, cfg_key in STEP_CONFIG_KEY.items():
            if step_configs.get(sname):
                cfg[cfg_key] = step_configs[sname]
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

    def _rebuild_steps():
        """Rebuild the compact step list UI."""
        steps_column.controls.clear()

        for idx, (sname, enabled) in enumerate(steps):
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
            for sn, en in loaded.get("run_steps", {}).items():
                steps.append([sn, bool(en)])
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
            root_logger = logging.getLogger()
            root_logger.addHandler(handler)
            try:
                proj = str(BASE_DIR)
                if proj not in sys.path:
                    sys.path.insert(0, proj)

                from config import Config
                Config._instance = None

                from src.logger import setup_logging
                setup_logging()

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
                root_logger.removeHandler(handler)
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
