"""Tests for the UI layer — layout, step_list, run_controls, persistence helpers."""

import unittest
from unittest.mock import MagicMock, patch


# ===================================================================== #
# layout.py
# ===================================================================== #

class TestBuildAppBar(unittest.TestCase):
    """Verify the app bar matches the mockup structure."""

    def _bar(self, **kw):
        import flet as ft
        from ui.pages.pipeline.layout import build_app_bar
        return build_app_bar(
            on_api_key=MagicMock(),
            theme_btn=ft.IconButton(icon=ft.Icons.DARK_MODE),
            **kw,
        )

    def test_returns_appbar(self):
        import flet as ft
        self.assertIsInstance(self._bar(), ft.AppBar)

    def test_leading_has_shade_branding(self):
        import flet as ft
        bar = self._bar()
        # leading → Container → Row(Image, Column(SHADE, Research))
        leading_row = bar.leading.content
        self.assertIsInstance(leading_row, ft.Row)
        # First control is the hex icon image
        self.assertIsInstance(leading_row.controls[0], ft.Image)
        self.assertIn("icon_hexagon", leading_row.controls[0].src)
        # Second control is Column with SHADE / Research text
        text_col = leading_row.controls[1]
        self.assertIsInstance(text_col, ft.Column)
        self.assertEqual(text_col.controls[0].value, "SHADE")
        self.assertEqual(text_col.controls[1].value, "Research")

    def test_leading_has_logo_image(self):
        import flet as ft
        bar = self._bar()
        leading_row = bar.leading.content
        self.assertIsInstance(leading_row.controls[0], ft.Image)

    def test_title_has_orchestrator_and_data_tabs(self):
        import flet as ft
        bar = self._bar()
        tab_labels = []
        for c in bar.title.controls:
            if isinstance(c, ft.Container) and isinstance(c.content, ft.Text):
                tab_labels.append(c.content.value)
        self.assertIn("Orchestrator", tab_labels)
        self.assertIn("Data", tab_labels)

    def test_orchestrator_tab_selected_by_default(self):
        import flet as ft
        bar = self._bar(active_tab=0)
        orch = bar.title.controls[0]  # first tab
        self.assertEqual(orch.content.weight, ft.FontWeight.W_600)

    def test_data_tab_greyed_by_default(self):
        import flet as ft
        bar = self._bar(active_tab=0)
        data_tab = bar.title.controls[1]
        self.assertEqual(data_tab.content.color, ft.Colors.GREY_500)

    def test_actions_contain_api_key_outlined_button(self):
        import flet as ft
        bar = self._bar()
        outlined = [a for a in bar.actions if isinstance(a, ft.OutlinedButton)]
        self.assertEqual(len(outlined), 1)
        self.assertEqual(outlined[0].content, "API Key")

    def test_api_key_button_is_red(self):
        import flet as ft
        bar = self._bar()
        btn = [a for a in bar.actions if isinstance(a, ft.OutlinedButton)][0]
        self.assertEqual(btn.style.color, ft.Colors.RED_700)


class TestBuildMainContent(unittest.TestCase):
    """Verify the two-column layout inside a rounded container."""

    def _content(self):
        import flet as ft
        from ui.pages.pipeline.layout import build_main_content
        return build_main_content(
            steps_column=ft.Column(),
            log_output=ft.TextField(),
            progress=ft.ProgressBar(),
            run_btn=ft.Button("Run"),
            on_save_setup=MagicMock(),
            on_load_setup=MagicMock(),
        )

    def test_returns_container(self):
        import flet as ft
        self.assertIsInstance(self._content(), ft.Container)

    def test_outer_container_has_rounded_border(self):
        c = self._content()
        self.assertEqual(c.border_radius, 16)

    def test_outer_container_has_border(self):
        c = self._content()
        self.assertIsNotNone(c.border)

    def test_inner_column_has_row_and_bottom(self):
        import flet as ft
        inner = self._content().content
        self.assertIsInstance(inner, ft.Column)
        # first child should be the Row (left+right), second should be bottom row
        self.assertEqual(len(inner.controls), 2)
        self.assertIsInstance(inner.controls[0], ft.Row)         # left+right panels
        self.assertIsInstance(inner.controls[1], ft.Container)   # bottom row

    def test_left_right_panels(self):
        import flet as ft
        inner = self._content().content
        panels_row = inner.controls[0]
        self.assertEqual(len(panels_row.controls), 2)  # left + right

    def test_log_output_title_present(self):
        """The right panel should contain a 'Log Output' heading."""
        import flet as ft
        inner = self._content().content
        right = inner.controls[0].controls[1]  # second panel in the row

        def _find(ctrl, target):
            if isinstance(ctrl, ft.Text) and ctrl.value == target:
                return True
            for attr in ("content", "controls"):
                child = getattr(ctrl, attr, None)
                if child is None:
                    continue
                if isinstance(child, list):
                    if any(_find(c, target) for c in child):
                        return True
                elif _find(child, target):
                    return True
            return False

        self.assertTrue(_find(right, "Log Output"))

    def test_bottom_row_has_save_load_buttons(self):
        import flet as ft
        inner = self._content().content
        bottom = inner.controls[1].content  # the Row inside the bottom Container
        outlined = [c for c in bottom.controls if isinstance(c, ft.OutlinedButton)]
        labels = {b.content for b in outlined}
        self.assertIn("Save Setup", labels)
        self.assertIn("Load Setup", labels)

    def test_bottom_row_save_load_are_pill_shaped(self):
        import flet as ft
        inner = self._content().content
        bottom = inner.controls[1].content
        for btn in bottom.controls:
            if isinstance(btn, ft.OutlinedButton):
                self.assertIsInstance(btn.style.shape, ft.RoundedRectangleBorder)
                self.assertEqual(btn.style.shape.radius, 20)


# ===================================================================== #
# step_list.py
# ===================================================================== #

class TestStepListRebuild(unittest.TestCase):
    """Verify rebuild_steps produces the right structure."""

    def _make(self, steps):
        from ui.pages.pipeline.step_list import create_steps_column
        page = MagicMock()
        page.update = MagicMock()
        col, rebuild = create_steps_column(page, steps, MagicMock())
        rebuild()
        return col

    def test_control_count_three_steps(self):
        """N steps → 1 top drop zone + N*(row_target + drop_zone) = 2N+1."""
        steps = [
            ["get_documents", True, False],
            ["backtest", True, False],
            ["generate_ratios", False, False],
        ]
        col = self._make(steps)
        self.assertEqual(len(col.controls), 7)  # 2*3 + 1

    def test_control_count_empty(self):
        col = self._make([])
        self.assertEqual(len(col.controls), 1)  # only top drop zone

    def test_control_count_one_step(self):
        col = self._make([["get_documents", True, False]])
        self.assertEqual(len(col.controls), 3)  # drop + row + drop

    def test_first_control_is_drop_zone(self):
        import flet as ft
        col = self._make([["get_documents", True, False]])
        self.assertIsInstance(col.controls[0], ft.DragTarget)

    def test_second_control_is_row_target(self):
        import flet as ft
        col = self._make([["get_documents", True, False]])
        self.assertIsInstance(col.controls[1], ft.DragTarget)  # row target
        self.assertIsInstance(col.controls[1].content, ft.Draggable)

    def test_draggable_data_matches_index(self):
        col = self._make([
            ["get_documents", True, False],
            ["backtest", False, False],
        ])
        # controls: [drop0, row0, drop1, row1, drop2]
        draggable_0 = col.controls[1].content  # Draggable for idx 0
        draggable_1 = col.controls[3].content  # Draggable for idx 1
        self.assertEqual(draggable_0.data, "0")
        self.assertEqual(draggable_1.data, "1")

    def test_pill_has_fixed_height(self):
        """Step pill should have height=36 for compact display."""
        import flet as ft
        col = self._make([["get_documents", True, False]])
        draggable = col.controls[1].content
        row = draggable.content  # ft.Row of outer items
        pill = row.controls[0]  # first item = pill Container
        self.assertIsInstance(pill, ft.Container)
        self.assertEqual(pill.height, 36)

    def test_overwrite_shown_for_overwrite_steps(self):
        """Steps in STEPS_WITH_OVERWRITE should have an Overwrite checkbox."""
        import flet as ft
        col = self._make([["generate_ratios", True, False]])
        draggable = col.controls[1].content
        row = draggable.content
        checkboxes = [c for c in row.controls if isinstance(c, ft.Checkbox)]
        labels = [cb.label for cb in checkboxes if cb.label]
        self.assertIn("Overwrite", labels)

    def test_no_overwrite_for_normal_steps(self):
        import flet as ft
        col = self._make([["get_documents", True, False]])
        draggable = col.controls[1].content
        row = draggable.content
        checkboxes = [c for c in row.controls if isinstance(c, ft.Checkbox)]
        labels = [cb.label for cb in checkboxes if cb.label]
        self.assertNotIn("Overwrite", labels)

    def test_gear_icon_present_for_configurable_steps(self):
        import flet as ft
        col = self._make([["get_documents", True, False]])
        draggable = col.controls[1].content
        row = draggable.content
        icon_btns = [c for c in row.controls if isinstance(c, ft.IconButton)]
        self.assertEqual(len(icon_btns), 1)
        self.assertEqual(icon_btns[0].icon, ft.Icons.SETTINGS)

    def test_ghost_has_pill_shape(self):
        """The content_when_dragging ghost should be pill-shaped."""
        col = self._make([["get_documents", True, False]])
        draggable = col.controls[1].content
        ghost = draggable.content_when_dragging
        self.assertEqual(ghost.border_radius, 24)


# ===================================================================== #
# Drag-and-drop reorder logic
# ===================================================================== #

class TestDragReorder(unittest.TestCase):
    """Test the pop/insert reorder pattern used by drag-and-drop."""

    @staticmethod
    def _reorder(items, src, dst):
        if src == dst:
            return
        item = items.pop(src)
        items.insert(dst, item)

    def test_move_last_to_first(self):
        items = ["a", "b", "c"]
        self._reorder(items, 2, 0)
        self.assertEqual(items, ["c", "a", "b"])

    def test_move_first_to_last(self):
        items = ["a", "b", "c"]
        self._reorder(items, 0, 2)
        self.assertEqual(items, ["b", "c", "a"])

    def test_adjacent_swap(self):
        items = ["a", "b", "c"]
        self._reorder(items, 0, 1)
        self.assertEqual(items, ["b", "a", "c"])

    def test_same_position_noop(self):
        items = ["a", "b", "c"]
        self._reorder(items, 1, 1)
        self.assertEqual(items, ["a", "b", "c"])

    def test_middle_forward(self):
        items = ["a", "b", "c", "d", "e"]
        self._reorder(items, 1, 3)
        self.assertEqual(items, ["a", "c", "d", "b", "e"])

    def test_middle_backward(self):
        items = ["a", "b", "c", "d", "e"]
        self._reorder(items, 3, 1)
        self.assertEqual(items, ["a", "d", "b", "c", "e"])

    def test_preserves_all_elements(self):
        items = [["s1", True, False], ["s2", False, True], ["s3", True, True]]
        names_before = {s[0] for s in items}
        self._reorder(items, 2, 0)
        self.assertEqual(names_before, {s[0] for s in items})

    def test_preserves_flags(self):
        items = [["s1", True, False], ["s2", False, True], ["s3", True, True]]
        self._reorder(items, 2, 0)
        s3 = next(s for s in items if s[0] == "s3")
        self.assertTrue(s3[1])
        self.assertTrue(s3[2])

    def test_drop_between_first_and_second(self):
        items = ["a", "b", "c", "d"]
        self._reorder(items, 3, 1)
        self.assertEqual(items, ["a", "d", "b", "c"])

    def test_drop_at_end(self):
        items = ["a", "b", "c"]
        self._reorder(items, 0, 3)
        self.assertEqual(items, ["b", "c", "a"])

    def test_single_element(self):
        items = ["a"]
        self._reorder(items, 0, 0)
        self.assertEqual(items, ["a"])


# ===================================================================== #
# run_controls.py
# ===================================================================== #

class TestRunControls(unittest.TestCase):

    def _create(self):
        from ui.pages.pipeline.run_controls import create_run_controls
        page = MagicMock()
        return create_run_controls(
            page,
            is_running=[False],
            base_dir=".",
            current_config=lambda: {},
            save_run_config=MagicMock(),
        )

    def test_run_button_text(self):
        _, _, btn = self._create()
        self.assertEqual(btn.content, "Run")

    def test_run_button_red(self):
        import flet as ft
        _, _, btn = self._create()
        self.assertEqual(btn.bgcolor, ft.Colors.RED_700)

    def test_run_button_white_text(self):
        import flet as ft
        _, _, btn = self._create()
        self.assertEqual(btn.color, ft.Colors.WHITE)

    def test_run_button_pill_shape(self):
        import flet as ft
        _, _, btn = self._create()
        self.assertIsInstance(btn.style.shape, ft.RoundedRectangleBorder)
        self.assertEqual(btn.style.shape.radius, 24)

    def test_run_button_is_button(self):
        import flet as ft
        _, _, btn = self._create()
        self.assertIsInstance(btn, ft.Button)

    def test_progress_hidden(self):
        _, progress, _ = self._create()
        self.assertFalse(progress.visible)

    def test_log_empty_and_readonly(self):
        log, _, _ = self._create()
        self.assertEqual(log.value, "")
        self.assertTrue(log.read_only)

    def test_log_multiline(self):
        log, _, _ = self._create()
        self.assertTrue(log.multiline)


# ===================================================================== #
# persistence helpers
# ===================================================================== #

class TestBuildSteps(unittest.TestCase):

    def test_dict_format(self):
        from ui.pages.pipeline.persistence import build_steps
        cfg = {"run_steps": {
            "get_documents": {"enabled": True, "overwrite": False},
            "backtest": {"enabled": False, "overwrite": True},
        }}
        steps = build_steps(cfg)
        gd = next(s for s in steps if s[0] == "get_documents")
        self.assertTrue(gd[1])
        self.assertFalse(gd[2])
        bt = next(s for s in steps if s[0] == "backtest")
        self.assertFalse(bt[1])
        self.assertTrue(bt[2])

    def test_bool_format(self):
        from ui.pages.pipeline.persistence import build_steps
        cfg = {"run_steps": {"get_documents": True}}
        steps = build_steps(cfg)
        gd = next(s for s in steps if s[0] == "get_documents")
        self.assertTrue(gd[1])
        self.assertFalse(gd[2])

    def test_empty_config_gives_defaults(self):
        from ui.pages.pipeline.persistence import build_steps, DEFAULT_STEPS
        steps = build_steps({})
        self.assertEqual(len(steps), len(DEFAULT_STEPS))

    def test_all_default_steps_present(self):
        from ui.pages.pipeline.persistence import build_steps, DEFAULT_STEPS
        steps = build_steps({"run_steps": {"get_documents": True}})
        names = {s[0] for s in steps}
        for d in DEFAULT_STEPS:
            self.assertIn(d, names)


class TestBuildCurrentConfig(unittest.TestCase):

    def test_run_steps_present(self):
        from ui.pages.pipeline.persistence import build_current_config
        steps = [["get_documents", True, False], ["backtest", False, True]]
        cfg = build_current_config(steps, {"get_documents": {}, "backtest": {}}, {})
        self.assertIn("run_steps", cfg)
        self.assertTrue(cfg["run_steps"]["get_documents"]["enabled"])
        self.assertTrue(cfg["run_steps"]["backtest"]["overwrite"])

    def test_step_config_included(self):
        from ui.pages.pipeline.persistence import build_current_config, STEP_CONFIG_KEY
        steps = [["backtest", True, False]]
        cfgs = {"backtest": {"start_date": "2024-01-01"}}
        cfg = build_current_config(steps, cfgs, {})
        key = STEP_CONFIG_KEY["backtest"]
        self.assertIn(key, cfg)
        self.assertEqual(cfg[key]["start_date"], "2024-01-01")


class TestBuildStepConfigs(unittest.TestCase):

    def test_loads_from_cfg(self):
        from ui.pages.pipeline.persistence import build_step_configs
        cfg = {"backtesting_config": {"start_date": "2020-01-01"}}
        sc = build_step_configs(cfg)
        self.assertIn("backtest", sc)
        self.assertEqual(sc["backtest"]["start_date"], "2020-01-01")

    def test_defaults_when_empty(self):
        from ui.pages.pipeline.persistence import build_step_configs
        sc = build_step_configs({})
        self.assertIn("backtest", sc)
        self.assertIn("get_documents", sc)


if __name__ == "__main__":
    unittest.main()
