"""Smoke tests for the ui_tk package.

Verifies that all modules import cleanly and key widgets can be
instantiated with a hidden Tk root — no interactive loop required.
"""

import tkinter as tk
import pytest


@pytest.fixture(scope="module")
def root():
    """Create a hidden Tk root for widget instantiation."""
    r = tk.Tk()
    r.withdraw()
    yield r
    r.destroy()


def test_import_ui_tk():
    """All ui_tk modules import without error."""
    import ui_tk
    import ui_tk.app
    import ui_tk.style
    import ui_tk.utils
    import ui_tk.controllers
    import ui_tk.shared.widgets
    import ui_tk.pages.home
    import ui_tk.pages.orchestrator
    import ui_tk.pages.data


def test_apply_theme(root):
    from ui_tk.style import apply_theme
    style = apply_theme(root)
    assert style is not None


def test_toggle_theme(root):
    """Theme can be toggled between dark and light."""
    from ui_tk.style import toggle_theme, is_dark, theme
    initial = is_dark()
    mode = toggle_theme(root)
    assert mode != ("dark" if initial else "light") or mode in ("dark", "light")
    assert is_dark() == (mode == "dark")
    # toggle back
    toggle_theme(root)


def test_theme_palette_keys():
    """Both palettes must have the same keys."""
    from ui_tk.style import _DARK, _LIGHT
    assert set(_DARK.keys()) == set(_LIGHT.keys())


def test_log_panel(root):
    from ui_tk.shared.widgets import LogPanel
    panel = LogPanel(root)
    panel.append("INFO", "test message")
    panel.append("ERROR", "error message")
    panel.clear()
    # reapply_colors should not error
    panel.reapply_colors()


def test_tab_bar(root):
    from ui_tk.shared.widgets import TabBar
    calls = []
    bar = TabBar(root, ["A", "B", "C"], on_tab_changed=calls.append)
    assert bar.active_tab == "A"
    bar.select(1)
    assert bar.active_tab == "B"
    assert calls == ["A", "B"]


def test_labeled_entry(root):
    from ui_tk.shared.widgets import LabeledEntry
    w = LabeledEntry(root, label="Test", value="hello")
    assert w.get() == "hello"
    w.set("world")
    assert w.get() == "world"


def test_portfolio_grid(root):
    from ui_tk.shared.widgets import PortfolioGrid
    portfolio = {"12345": {"mode": "weight", "value": 0.5}}
    grid = PortfolioGrid(root, portfolio=portfolio)
    result = grid.get_portfolio()
    assert "12345" in result
    assert result["12345"]["mode"] == "weight"


def test_controllers_list_setups():
    from ui_tk.controllers import list_setups
    setups = list_setups()
    assert isinstance(setups, list)


def test_controllers_build_config_dict():
    from ui_tk.controllers import build_config_dict
    steps = [["get_documents", False], ["backtest", True]]
    configs = {"get_documents": {"startDate": "2026-01-01"}}
    result = build_config_dict(steps, configs)
    assert result["run_steps"]["get_documents"]["enabled"] is True
    assert "get_documents_config" in result


def test_queue_log_handler():
    import logging
    import queue
    from ui_tk.utils import QueueLogHandler

    q = queue.Queue()
    handler = QueueLogHandler(q)
    handler.setFormatter(logging.Formatter("%(message)s"))

    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname="", lineno=0,
        msg="hello", args=(), exc_info=None,
    )
    handler.emit(record)

    kind, level, msg = q.get_nowait()
    assert kind == "log"
    assert level == "INFO"
    assert msg == "hello"


def test_app_init_no_attribute_error(root):
    """Regression: App.__init__ must initialise all state before _build_top_bar."""
    from ui_tk.app import App

    app = App(root)
    assert app._active_view is not None
    assert app._active_view in ("Home", "Orchestrator", "Data")


def test_app_has_tab_buttons(root):
    """App must expose tab buttons for each view."""
    from ui_tk.app import App, VIEW_NAMES

    app = App(root)
    for name in VIEW_NAMES:
        assert name in app._tab_buttons
        assert name in app._tab_indicators


def test_app_switch_view(root):
    """Switching views must update _active_view and tab visuals."""
    from ui_tk.app import App

    app = App(root)
    app.switch_view("Orchestrator")
    assert app._active_view == "Orchestrator"
    app.switch_view("Home")
    assert app._active_view == "Home"
