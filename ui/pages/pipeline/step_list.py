import flet as ft

from ui.pages.pipeline.persistence import STEP_CONFIG_KEY, STEP_DISPLAY, STEPS_WITH_OVERWRITE


def create_steps_column(page: ft.Page, steps: list[list], open_step_config):
    steps_column = ft.Column(spacing=0, expand=True, scroll=ft.ScrollMode.AUTO)

    def _toggle_step(idx: int, value: bool):
        steps[idx][1] = value
        rebuild_steps()

    def _toggle_overwrite(idx: int, value: bool):
        steps[idx][2] = value

    def _on_drag_accept(target_idx: int, e):
        """Move the dragged step to *target_idx*."""
        try:
            src_idx = int(e.src.data)
        except (ValueError, TypeError, AttributeError):
            return
        if src_idx == target_idx:
            return
        item = steps.pop(src_idx)
        steps.insert(target_idx, item)
        rebuild_steps()

    # ------------------------------------------------------------------ #
    # Visual builders
    # ------------------------------------------------------------------ #

    def _build_step_row(idx: int, sname: str, enabled: bool, overwrite: bool):
        """Build one step: rounded pill + gear icon + optional Overwrite."""
        has_cfg = sname in STEP_CONFIG_KEY
        display = STEP_DISPLAY.get(sname, sname)

        cb_color = ft.Colors.BLUE_600 if enabled else ft.Colors.RED_400

        # ---- inner pill: drag handle ≡ + checkbox + label ----
        pill = ft.Container(
            content=ft.Row(
                [
                    ft.Icon(ft.Icons.DRAG_HANDLE, color=ft.Colors.GREY_500, size=16),
                    ft.Checkbox(
                        value=enabled,
                        fill_color={
                            ft.ControlState.SELECTED: cb_color,
                            ft.ControlState.DEFAULT: ft.Colors.RED_400,
                        },
                        scale=0.85,
                        on_change=lambda e, i=idx: _toggle_step(i, e.control.value),
                    ),
                    ft.Text(display, size=13, weight=ft.FontWeight.W_500),
                ],
                spacing=0,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                tight=True,
            ),
            bgcolor=ft.Colors.SURFACE_CONTAINER_LOW,
            border_radius=24,
            border=ft.Border.all(1, ft.Colors.OUTLINE_VARIANT),
            padding=ft.Padding(left=8, right=12, top=0, bottom=0),
            height=36,
        )

        # ---- items outside the pill ----
        outer: list[ft.Control] = [pill]

        if has_cfg:
            outer.append(
                ft.IconButton(
                    icon=ft.Icons.SETTINGS,
                    icon_size=16,
                    icon_color=ft.Colors.GREY_600,
                    tooltip="Configure",
                    style=ft.ButtonStyle(padding=0),
                    width=32,
                    height=32,
                    on_click=lambda _, sn=sname: open_step_config(sn),
                )
            )

        if sname in STEPS_WITH_OVERWRITE:
            outer.append(
                ft.Checkbox(
                    label="Overwrite",
                    value=overwrite,
                    fill_color={
                        ft.ControlState.SELECTED: ft.Colors.BLUE_600,
                        ft.ControlState.DEFAULT: ft.Colors.GREY_400,
                    },
                    label_style=ft.TextStyle(size=12),
                    scale=0.85,
                    on_change=lambda e, i=idx: _toggle_overwrite(i, e.control.value),
                )
            )

        return ft.Row(outer, spacing=2, vertical_alignment=ft.CrossAxisAlignment.CENTER)

    def _build_drop_zone(target_idx: int) -> ft.DragTarget:
        """Invisible zone between steps; expands + highlights on hover."""
        zone = ft.Container(height=2, border_radius=4)

        def _will_accept(e):
            zone.bgcolor = ft.Colors.BLUE_100
            zone.height = 8
            try:
                e.control.update()
            except Exception:
                pass

        def _leave(e):
            zone.bgcolor = None
            zone.height = 2
            try:
                e.control.update()
            except Exception:
                pass

        return ft.DragTarget(
            group="steps",
            content=zone,
            on_accept=lambda e, ti=target_idx: _on_drag_accept(ti, e),
            on_will_accept=_will_accept,
            on_leave=_leave,
        )

    # ------------------------------------------------------------------ #

    def rebuild_steps():
        steps_column.controls.clear()

        # top drop zone
        steps_column.controls.append(_build_drop_zone(0))

        for idx, (sname, enabled, overwrite) in enumerate(steps):
            row = _build_step_row(idx, sname, enabled, overwrite)
            display = STEP_DISPLAY.get(sname, sname)

            ghost = ft.Container(
                content=ft.Text(display, color=ft.Colors.GREY_500, size=12),
                border_radius=24,
                border=ft.Border.all(1, ft.Colors.GREY_300),
                padding=ft.Padding(left=10, right=10, top=4, bottom=4),
                opacity=0.4,
            )

            draggable = ft.Draggable(
                group="steps",
                data=str(idx),
                content=row,
                content_when_dragging=ghost,
            )

            # row itself is also a drop target so dropping ON a row works
            row_target = ft.DragTarget(
                group="steps",
                content=draggable,
                on_accept=lambda e, ti=idx: _on_drag_accept(ti, e),
            )

            steps_column.controls.append(row_target)

            # drop zone after each step
            steps_column.controls.append(_build_drop_zone(idx + 1))

        page.update()

    return steps_column, rebuild_steps
