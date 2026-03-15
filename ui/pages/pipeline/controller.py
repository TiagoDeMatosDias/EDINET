import os
from typing import Callable

import flet as ft

from ui.pages.pipeline.persistence import (
    SAVED_SETUPS_DIR,
    build_step_configs,
    build_steps,
    list_saved_setups,
    load_named_setup,
    save_named_setup,
    write_env,
)
from ui.pages.pipeline.step_dialogs import (
    open_backtest_config,
    open_backtest_set_config,
    open_download_documents_config,
    open_generate_financial_statements_config,
    open_get_documents_config,
    open_generate_historical_ratios_config,
    open_generate_ratios_config,
    open_generic_step_config,
    open_import_csv_config,
    open_multivariate_regression_config,
    open_parse_taxonomy_config,
    open_populate_company_info_config,
    open_update_stock_prices_config,
)


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
        self.theme_btn: ft.IconButton | None = None
        self._rebuild_steps: Callable[[], None] = lambda: None

    def bind_controls(self, theme_btn: ft.IconButton):
        self.theme_btn = theme_btn

    def set_rebuild_steps(self, rebuild_steps: Callable[[], None]):
        self._rebuild_steps = rebuild_steps

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
        if step_name == "get_documents":
            open_get_documents_config(self.page, self.fp, self.step_configs, self.snack, self.show, self.pop)
            return
        if step_name == "download_documents":
            open_download_documents_config(self.page, self.fp, self.step_configs, self.snack, self.show, self.pop)
            return
        if step_name == "populate_company_info":
            open_populate_company_info_config(self.page, self.fp, self.step_configs, self.snack, self.show, self.pop)
            return
        if step_name == "backtest":
            open_backtest_config(self.page, self.fp, self.step_configs, self.snack, self.show, self.pop)
            return
        if step_name == "backtest_set":
            open_backtest_set_config(self.page, self.fp, self.step_configs, self.snack, self.show, self.pop)
            return
        if step_name == "import_stock_prices_csv":
            open_import_csv_config(self.page, self.fp, self.step_configs, self.snack, self.show, self.pop)
            return
        if step_name == "parse_taxonomy":
            open_parse_taxonomy_config(self.page, self.fp, self.step_configs, self.snack, self.show, self.pop)
            return
        if step_name == "update_stock_prices":
            open_update_stock_prices_config(self.page, self.fp, self.step_configs, self.snack, self.show, self.pop)
            return
        if step_name == "generate_financial_statements":
            open_generate_financial_statements_config(
                self.page, self.fp, self.step_configs, self.snack, self.show, self.pop,
            )
            return
        if step_name == "generate_ratios":
            open_generate_ratios_config(
                self.page, self.fp, self.step_configs, self.snack, self.show, self.pop,
            )
            return
        if step_name == "generate_historical_ratios":
            open_generate_historical_ratios_config(
                self.page, self.fp, self.step_configs, self.snack, self.show, self.pop,
            )
            return
        if step_name == "Multivariate_Regression":
            open_multivariate_regression_config(
                self.page, self.fp, self.step_configs, self.snack, self.show, self.pop,
            )
            return
        open_generic_step_config(self.page, step_name, self.step_configs, self.snack, self.show, self.pop)

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
