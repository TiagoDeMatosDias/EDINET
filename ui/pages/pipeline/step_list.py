import flet as ft

from ui.pages.pipeline.persistence import STEP_CONFIG_KEY, STEP_DISPLAY, STEPS_WITH_OVERWRITE


def create_steps_column(page: ft.Page, steps: list[list], open_step_config):
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
                rebuild_steps()
        return handler

    def _toggle_step(idx: int, value: bool):
        steps[idx][1] = value
        rebuild_steps()

    def _toggle_overwrite(idx: int, value: bool):
        steps[idx][2] = value

    def rebuild_steps():
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

    return steps_column, rebuild_steps
