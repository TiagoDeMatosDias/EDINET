import flet as ft


def build_app_bar(
    on_api_key,
    theme_btn: ft.IconButton,
    on_tab_change=None,
    active_tab: int = 0,
) -> ft.AppBar:
    """App bar matching mockup: logo + SHADE Research, Orchestrator/Data tabs,
    red API Key button, dark-mode toggle."""
    return ft.AppBar(
        leading=ft.Container(
            content=ft.Row(
                [
                    ft.Image(
                        src="icon_hexagon.svg",
                        height=32,
                        width=32,
                    ),
                    ft.Column(
                        [
                            ft.Text(
                                "SHADE",
                                size=14,
                                weight=ft.FontWeight.BOLD,
                                color="#2c3e50",
                            ),
                            ft.Text(
                                "Research",
                                size=10,
                                color="#e74c3c",
                            ),
                        ],
                        spacing=0,
                        alignment=ft.MainAxisAlignment.CENTER,
                    ),
                ],
                spacing=6,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
            ),
            padding=ft.Padding(left=14, top=0, right=0, bottom=0),
        ),
        leading_width=160,
        title=ft.Row(
            [
                _tab_pill("Orchestrator", selected=(active_tab == 0),
                          on_click=lambda _: on_tab_change(0) if on_tab_change else None),
                _tab_pill("Data", selected=(active_tab == 1),
                          on_click=lambda _: on_tab_change(1) if on_tab_change else None),
            ],
            spacing=8,
        ),
        center_title=False,
        bgcolor=ft.Colors.SURFACE_CONTAINER,
        actions=[
            ft.OutlinedButton(
                "API Key",
                on_click=on_api_key,
                style=ft.ButtonStyle(
                    color=ft.Colors.RED_700,
                    side=ft.BorderSide(1, ft.Colors.RED_700),
                    shape=ft.RoundedRectangleBorder(radius=20),
                    padding=ft.Padding(left=16, right=16, top=6, bottom=6),
                ),
            ),
            theme_btn,
            ft.Container(width=8),
        ],
    )


def _tab_pill(label: str, *, selected: bool, on_click) -> ft.Container:
    return ft.Container(
        content=ft.Text(
            label,
            size=13,
            weight=ft.FontWeight.W_600 if selected else ft.FontWeight.W_400,
            color=None if selected else ft.Colors.GREY_500,
        ),
        border=ft.Border.all(
            1,
            ft.Colors.ON_SURFACE if selected else ft.Colors.OUTLINE_VARIANT,
        ),
        border_radius=20,
        padding=ft.Padding(left=16, right=16, top=6, bottom=6),
        on_click=on_click,
        ink=True,
    )


def build_main_content(
    steps_column: ft.Column,
    log_output: ft.TextField,
    progress: ft.ProgressBar,
    run_btn: ft.Control,
    on_save_setup,
    on_load_setup,
) -> ft.Container:
    """Two-column layout inside a rounded card: steps left, log right,
    buttons along the bottom — matching the mockup."""

    # ---- left: step list (scrollable) ----
    left_panel = ft.Container(
        content=steps_column,
        expand=2,
        padding=ft.Padding(left=2, right=6, top=0, bottom=0),
    )

    # ---- right: log output ----
    right_panel = ft.Container(
        content=ft.Column(
            [
                ft.Text("Log Output", size=14, weight=ft.FontWeight.W_500,
                         text_align=ft.TextAlign.CENTER),
                ft.Container(
                    content=log_output,
                    bgcolor=ft.Colors.SURFACE_CONTAINER_LOW,
                    border_radius=12,
                    border=ft.Border.all(1, ft.Colors.OUTLINE_VARIANT),
                    padding=8,
                    expand=True,
                ),
            ],
            spacing=4,
            expand=True,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        ),
        expand=3,
        padding=ft.Padding(left=8, right=4, top=0, bottom=0),
    )

    # ---- bottom row: Save / Load / progress / Run ----
    bottom_row = ft.Container(
        content=ft.Row(
            [
                ft.OutlinedButton(
                    "Save Setup",
                    on_click=on_save_setup,
                    style=ft.ButtonStyle(
                        shape=ft.RoundedRectangleBorder(radius=20),
                        padding=ft.Padding(left=16, right=16, top=6, bottom=6),
                    ),
                ),
                ft.OutlinedButton(
                    "Load Setup",
                    on_click=on_load_setup,
                    style=ft.ButtonStyle(
                        shape=ft.RoundedRectangleBorder(radius=20),
                        padding=ft.Padding(left=16, right=16, top=6, bottom=6),
                    ),
                ),
                ft.Container(expand=True),
                progress,
                run_btn,
            ],
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
        ),
        padding=ft.Padding(left=0, right=0, top=2, bottom=0),
    )

    # ---- outer rounded card ----
    return ft.Container(
        content=ft.Column(
            [
                ft.Row(
                    [left_panel, right_panel],
                    expand=True,
                    vertical_alignment=ft.CrossAxisAlignment.STRETCH,
                ),
                bottom_row,
            ],
            spacing=0,
            expand=True,
        ),
        border_radius=16,
        border=ft.Border.all(1, ft.Colors.OUTLINE_VARIANT),
        bgcolor=ft.Colors.SURFACE,
        margin=ft.Margin(left=12, right=12, top=8, bottom=8),
        padding=ft.Padding(left=10, right=10, top=8, bottom=6),
        expand=True,
    )
