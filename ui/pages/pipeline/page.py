import flet as ft

from ui.pages.pipeline.controller import AppController
from ui.pages.pipeline.layout import build_app_bar, build_bottom_panel, build_top_panel
from ui.pages.pipeline.models import PipelinePageState
from ui.pages.pipeline.persistence import (
    ASSETS_DIR,
    BASE_DIR,
    build_current_config,
    build_step_configs,
    build_steps,
    load_app_state,
    load_run_config,
    read_env,
    save_run_config,
)
from ui.pages.pipeline.run_controls import create_run_controls
from ui.pages.pipeline.step_list import create_steps_column
from ui.shared.page_services import create_page_services


def build_pipeline_page(page: ft.Page):
    page.title = "EDINET - Financial Data Pipeline"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.padding = 0
    page.theme = ft.Theme(color_scheme_seed=ft.Colors.BLUE)
    page.window.icon = str(ASSETS_DIR / "icon.ico")

    state = PipelinePageState(
        env=read_env(),
        app_state=load_app_state(),
        run_cfg=load_run_config(),
        steps=[],
        step_configs={},
        is_running=[False],
    )
    state.steps = build_steps(state.run_cfg)
    state.step_configs = build_step_configs(state.run_cfg)

    env = state.env
    app_state = state.app_state
    steps = state.steps
    step_configs = state.step_configs
    is_running = state.is_running

    fp = ft.FilePicker()
    page.services.append(fp)
    services = create_page_services(page)

    def current_config() -> dict:
        return build_current_config(steps, step_configs, env)

    controller = AppController(
        page=page,
        fp=fp,
        env=env,
        app_state=app_state,
        steps=steps,
        step_configs=step_configs,
        snack=services.snack,
        show=services.show,
        pop=services.pop,
        current_config=current_config,
    )

    steps_column, rebuild_steps = create_steps_column(page, steps, controller.open_step_config)
    controller.set_rebuild_steps(rebuild_steps)

    log_output, progress, run_btn = create_run_controls(
        page,
        is_running=is_running,
        base_dir=BASE_DIR,
        current_config=current_config,
        save_run_config=save_run_config,
    )

    theme_btn = ft.IconButton(
        icon=ft.Icons.DARK_MODE,
        tooltip="Switch to dark mode",
        on_click=controller.toggle_theme,
    )

    controller.bind_controls(theme_btn)

    page.appbar = build_app_bar(
        controller.on_api_key,
        theme_btn,
    )
    page.add(
        ft.Column(
            [
                build_top_panel(steps_column),
                build_bottom_panel(run_btn, progress, log_output, controller.on_save_setup, controller.on_load_setup),
            ],
            spacing=0,
            expand=True,
        )
    )

    rebuild_steps()
