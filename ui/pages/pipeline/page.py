import flet as ft

from ui.pages.pipeline.controller import AppController, seed_recent_database
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
    seed_recent_database(env, app_state)

    fp = ft.FilePicker()
    page.services.append(fp)
    services = create_page_services(page)

    def current_config() -> dict:
        return build_current_config(steps, step_configs, env)

    ratios_path_text = ft.Text(
        f"Ratios config: {env.get('FINANCIAL_RATIOS_CONFIG_PATH', 'Not set')}",
        size=11,
        color=ft.Colors.GREY_500,
        italic=True,
    )

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
        ratios_path_text=ratios_path_text,
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

    db_dropdown = ft.Dropdown(
        label="Database",
        width=260,
        dense=True,
        text_size=13,
        on_select=controller.on_db_change,
    )
    theme_btn = ft.IconButton(
        icon=ft.Icons.DARK_MODE,
        tooltip="Switch to dark mode",
        on_click=controller.toggle_theme,
    )

    controller.bind_controls(db_dropdown, theme_btn)
    controller.refresh_db_dropdown(update=False)

    page.appbar = build_app_bar(
        db_dropdown,
        controller.on_add_db,
        controller.on_open_db,
        controller.on_api_key,
        theme_btn,
    )
    page.add(
        ft.Column(
            [
                build_top_panel(steps_column, ratios_path_text, controller.on_ratios_btn),
                build_bottom_panel(run_btn, progress, log_output, controller.on_save_setup, controller.on_load_setup),
            ],
            spacing=0,
            expand=True,
        )
    )

    rebuild_steps()
