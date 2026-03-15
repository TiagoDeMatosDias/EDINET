import flet as ft


def build_app_bar(
    db_dropdown: ft.Dropdown,
    on_add_db,
    on_open_db,
    on_api_key,
    theme_btn: ft.IconButton,
) -> ft.AppBar:
    return ft.AppBar(
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


def build_top_panel(steps_column: ft.Column) -> ft.Container:
    return ft.Container(
        content=ft.Column(
            [
                ft.Row(
                    [
                        ft.Text("Run Steps", size=16, weight=ft.FontWeight.W_600),
                    ],
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                ),
                ft.Text(
                    "Drag to reorder  •  Check to enable  •  ⚙ to configure",
                    size=11,
                    color=ft.Colors.GREY_500,
                ),
                ft.Divider(height=1),
                steps_column,
            ],
            spacing=4,
            expand=True,
        ),
        padding=ft.Padding(left=20, right=20, top=12, bottom=8),
        expand=True,
    )


def build_bottom_panel(
    run_btn: ft.Button,
    progress: ft.ProgressBar,
    log_output: ft.TextField,
    on_save_setup,
    on_load_setup,
) -> ft.Container:
    return ft.Container(
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
