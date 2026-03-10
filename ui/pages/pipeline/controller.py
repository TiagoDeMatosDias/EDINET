import os
from typing import Callable

import flet as ft

from ui.pages.pipeline.persistence import (
    SAVED_SETUPS_DIR,
    build_step_configs,
    build_steps,
    list_saved_setups,
    load_named_setup,
    save_app_state,
    save_named_setup,
    write_env,
)
from ui.pages.pipeline.step_dialogs import (
    open_backtest_config,
    open_generic_step_config,
    open_import_csv_config,
)


def seed_recent_database(env: dict[str, str], app_state: dict) -> str:
    current_db = env.get("DB_PATH", "")
    if current_db:
        dbs = app_state.setdefault("recent_databases", [])
        if current_db not in dbs:
            dbs.insert(0, current_db)
            save_app_state(app_state)
    return current_db


class AppController:
    def __init__(
        self,
        *,
        page: ft.Page,
        fp: ft.FilePicker,
        env: dict[str, str],
        app_state: dict,
        steps: list[list],
        step_configs: dict[str, dict],
        snack: Callable[[str], None],
        show: Callable[[ft.AlertDialog], None],
        pop: Callable[[], None],
        current_config: Callable[[], dict],
        ratios_path_text: ft.Text,
    ):
        self.page = page
        self.fp = fp
        self.env = env
        self.app_state = app_state
        self.steps = steps
        self.step_configs = step_configs
        self.snack = snack
        self.show = show
        self.pop = pop
        self.current_config = current_config
        self.ratios_path_text = ratios_path_text
        self.db_dropdown: ft.Dropdown | None = None
        self.theme_btn: ft.IconButton | None = None
        self._rebuild_steps: Callable[[], None] = lambda: None

    def bind_controls(self, db_dropdown: ft.Dropdown, theme_btn: ft.IconButton):
        self.db_dropdown = db_dropdown
        self.theme_btn = theme_btn

    def set_rebuild_steps(self, rebuild_steps: Callable[[], None]):
        self._rebuild_steps = rebuild_steps

    def _add_recent_db(self, path: str):
        dbs = self.app_state.setdefault("recent_databases", [])
        if path in dbs:
            dbs.remove(path)
        dbs.insert(0, path)
        self.app_state["recent_databases"] = dbs[:20]
        save_app_state(self.app_state)

    def refresh_db_dropdown(self, *, update: bool = True):
        if self.db_dropdown is None:
            return
        current = self.env.get("DB_PATH", "")
        dbs = self.app_state.get("recent_databases", [])
        self.db_dropdown.options = [ft.dropdown.Option(key=d, text=os.path.basename(d)) for d in dbs]
        self.db_dropdown.value = current if current in dbs else None
        if update:
            self.page.update()

    def on_db_change(self, _):
        if self.db_dropdown is None:
            return
        path = self.db_dropdown.value
        if path:
            write_env("DB_PATH", path)
            self.env["DB_PATH"] = path

    async def on_add_db(self, _):
        result = await self.fp.save_file(
            dialog_title="Create new database",
            file_name="edinet.db",
            allowed_extensions=["db"],
        )
        if result:
            db_path = result if result.endswith(".db") else result + ".db"
            write_env("DB_PATH", db_path)
            self.env["DB_PATH"] = db_path
            self._add_recent_db(db_path)
            self.refresh_db_dropdown()
            self.snack(f"Database set to {os.path.basename(db_path)}")

    async def on_open_db(self, _):
        files = await self.fp.pick_files(
            dialog_title="Select existing database",
            file_type=ft.FilePickerFileType.CUSTOM,
            allowed_extensions=["db"],
            allow_multiple=False,
        )
        if files:
            db_path = files[0].path
            write_env("DB_PATH", db_path)
            self.env["DB_PATH"] = db_path
            self._add_recent_db(db_path)
            self.refresh_db_dropdown()
            self.snack(f"Database loaded: {os.path.basename(db_path)}")

    def on_api_key(self, _):
        tf = ft.TextField(
            value=self.env.get("API_KEY", ""),
            label="EDINET API Key",
            password=True,
            can_reveal_password=True,
            width=420,
        )

        def save(__):
            write_env("API_KEY", tf.value.strip())
            self.env["API_KEY"] = tf.value.strip()
            self.pop()
            self.snack("API key saved")

        self.show(ft.AlertDialog(
            modal=True,
            title=ft.Text("Set API Key"),
            content=tf,
            actions=[
                ft.TextButton("Cancel", on_click=lambda __: self.pop()),
                ft.Button("Save", on_click=save),
            ],
        ))

    def toggle_theme(self, _):
        if self.theme_btn is None:
            return
        if self.page.theme_mode == ft.ThemeMode.LIGHT:
            self.page.theme_mode = ft.ThemeMode.DARK
            self.theme_btn.icon = ft.Icons.LIGHT_MODE
            self.theme_btn.tooltip = "Switch to light mode"
        else:
            self.page.theme_mode = ft.ThemeMode.LIGHT
            self.theme_btn.icon = ft.Icons.DARK_MODE
            self.theme_btn.tooltip = "Switch to dark mode"
        self.page.update()

    def open_step_config(self, step_name: str):
        if step_name == "backtest":
            open_backtest_config(self.page, self.step_configs, self.snack, self.show, self.pop)
            return
        if step_name == "import_stock_prices_csv":
            open_import_csv_config(self.page, self.fp, self.step_configs, self.snack, self.show, self.pop)
            return
        open_generic_step_config(self.page, step_name, self.step_configs, self.snack, self.show, self.pop)

    async def on_ratios_btn(self, _):
        files = await self.fp.pick_files(
            dialog_title="Select financial_ratios_config.json",
            file_type=ft.FilePickerFileType.CUSTOM,
            allowed_extensions=["json"],
            allow_multiple=False,
        )
        if files:
            path = files[0].path
            write_env("FINANCIAL_RATIOS_CONFIG_PATH", path)
            self.env["FINANCIAL_RATIOS_CONFIG_PATH"] = path
            self.ratios_path_text.value = f"Ratios config: {path}"
            self.snack(f"Ratios config set to {os.path.basename(path)}")

    def on_save_setup(self, _):
        tf = ft.TextField(label="Setup name", autofocus=True, width=320)

        def save(__):
            name = tf.value.strip()
            if not name:
                return
            saved_path = save_named_setup(name, self.current_config())
            self.pop()
            self.snack(f"Setup '{name}' saved → {saved_path}")

        self.show(ft.AlertDialog(
            modal=True,
            title=ft.Text("Save Setup"),
            content=ft.Column(
                [
                    tf,
                    ft.Text(
                        f"Setups are stored in:\n{SAVED_SETUPS_DIR}",
                        size=11,
                        color=ft.Colors.GREY_500,
                        italic=True,
                    ),
                ],
                tight=True,
                spacing=8,
            ),
            actions=[
                ft.TextButton("Cancel", on_click=lambda __: self.pop()),
                ft.Button("Save", on_click=save),
            ],
        ))

    def on_load_setup(self, _):
        setups = list_saved_setups()
        if not setups:
            self.snack(f"No saved setups found in {SAVED_SETUPS_DIR}")
            return

        dd = ft.Dropdown(
            label="Select setup",
            options=[ft.dropdown.Option(s) for s in setups],
            width=320,
        )

        def load(__):
            name = dd.value
            if not name:
                return
            loaded = load_named_setup(name)
            self.steps.clear()
            self.steps.extend(build_steps(loaded))
            self.step_configs.clear()
            self.step_configs.update(build_step_configs(loaded))

            ratios = loaded.get("financial_ratios_config_path", "")
            if ratios:
                write_env("FINANCIAL_RATIOS_CONFIG_PATH", ratios)
                self.env["FINANCIAL_RATIOS_CONFIG_PATH"] = ratios
                self.ratios_path_text.value = f"Ratios config: {ratios}"
            self.pop()
            self._rebuild_steps()
            self.snack(f"Setup '{name}' loaded from {SAVED_SETUPS_DIR / (name + '.json')}")

        self.show(ft.AlertDialog(
            modal=True,
            title=ft.Text("Load Setup"),
            content=ft.Column(
                [
                    dd,
                    ft.Text(
                        f"Loading from: {SAVED_SETUPS_DIR}",
                        size=11,
                        color=ft.Colors.GREY_500,
                        italic=True,
                    ),
                ],
                tight=True,
                spacing=8,
            ),
            actions=[
                ft.TextButton("Cancel", on_click=lambda __: self.pop()),
                ft.Button("Load", on_click=load),
            ],
        ))
