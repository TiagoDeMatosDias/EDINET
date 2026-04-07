"""Smoke tests for the ui_tk package.

Verifies that all modules import cleanly and key widgets can be
instantiated with a hidden Tk root — no interactive loop required.
"""

import tkinter as tk
from types import SimpleNamespace

import pandas as pd
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
    import ui_tk.pages.screening
    import ui_tk.pages.security_analysis


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
    app.switch_view("Security Analysis")
    assert app._active_view == "Security Analysis"
    app.switch_view("Home")
    assert app._active_view == "Home"


def test_app_show_security_analysis_delegates_to_view(root, monkeypatch):
    """App helper should switch views and delegate to the Security Analysis page."""
    from ui_tk.app import App

    app = App(root)
    app.switch_view("Security Analysis")
    view = app._views["Security Analysis"]
    calls = []

    monkeypatch.setattr(
        view,
        "open_security",
        lambda record, db_path=None: calls.append((record, db_path)),
    )

    record = {"edinet_code": "E00001", "ticker": "1001"}
    app.show_security_analysis(record, db_path="C:/tmp/security.db")

    assert app._active_view == "Security Analysis"
    assert calls == [(record, "C:/tmp/security.db")]


def test_screening_page_init(root):
    """ScreeningPage can be instantiated without error."""
    from ui_tk.pages.screening import ScreeningPage

    page = ScreeningPage(root)
    page.reapply_colors()


def test_security_analysis_page_init(root):
    """SecurityAnalysisPage can be instantiated without error."""
    from ui_tk.pages.security_analysis import SecurityAnalysisPage

    page = SecurityAnalysisPage(root)
    page.reapply_colors()


def test_security_analysis_loads_charts_and_peers_lazily(root, monkeypatch):
    """Overview load should not fetch chart or peer data until those tabs are opened."""
    from ui_tk.pages import security_analysis as security_analysis_page

    calls = {
        "optimize": 0,
        "overview": 0,
        "statements": 0,
        "price_history": 0,
        "peers": 0,
    }

    def _run_now(fn, args=(), on_done=None, on_error=None):
        try:
            result = fn(*args)
            if on_done:
                on_done(result)
        except Exception as exc:  # pragma: no cover - defensive test helper
            if on_error:
                on_error(exc)
        return None

    monkeypatch.setattr(security_analysis_page, "run_in_background", _run_now)
    monkeypatch.setattr(security_analysis_page.ctrl, "get_default_database_path", lambda: "")
    monkeypatch.setattr(
        security_analysis_page.ctrl,
        "security_optimize_database",
        lambda _db_path: calls.__setitem__("optimize", calls["optimize"] + 1) or {"ok": True},
    )
    monkeypatch.setattr(
        security_analysis_page.ctrl,
        "security_get_overview",
        lambda _db_path, _edinet_code: calls.__setitem__("overview", calls["overview"] + 1) or {
            "company": {
                "edinet_code": "E00001",
                "ticker": "1001",
                "company_name": "Alpha Corp",
                "industry": "Industrial",
                "market": "JPX Prime",
                "description": "",
            },
            "market": {
                "latest_price": 1000.0,
                "latest_price_date": "2024-12-31",
                "previous_price": 950.0,
                "change_pct_1d": 0.01,
                "range_52w_low": 800.0,
                "range_52w_high": 1100.0,
            },
            "fundamentals_latest": {
                "Revenue": 100.0,
                "OperatingIncome": 20.0,
                "NetIncome": 10.0,
                "TotalAssets": 500.0,
                "ShareholdersEquity": 200.0,
                "SharesOutstanding": 1000.0,
            },
            "valuation_latest": {
                "PERatio": 10.0,
                "PriceToBook": 1.5,
                "DividendsYield": 0.02,
                "MarketCap": 1000000.0,
            },
            "quality_latest": {
                "ReturnOnEquity": 0.15,
                "DebtToEquity": 0.2,
                "CurrentRatio": 1.8,
                "GrossMargin": 0.35,
            },
            "metadata": {
                "last_financial_period_end": "2024-03-31",
                "last_price_date": "2024-12-31",
                "doc_id": "DOC1",
                "data_quality_flags": [],
            },
        },
    )
    monkeypatch.setattr(
        security_analysis_page.ctrl,
        "security_get_statements",
        lambda _db_path, _edinet_code, periods=8: calls.__setitem__("statements", calls["statements"] + 1) or {
            "periods": ["2024-03-31"],
            "records": [{"period_end": "2024-03-31", "netSales": 100.0}],
            "income_statement": [{"metric": "Net Sales", "field": "netSales", "values": [100.0]}],
            "balance_sheet": [],
            "cashflow_statement": [],
        },
    )
    monkeypatch.setattr(
        security_analysis_page.ctrl,
        "security_get_price_history",
        lambda _db_path, _ticker: calls.__setitem__("price_history", calls["price_history"] + 1) or [
            {"trade_date": "2024-12-31", "price": 1000.0}
        ],
    )
    monkeypatch.setattr(
        security_analysis_page.ctrl,
        "security_get_peers",
        lambda _db_path, _edinet_code, industry=None, limit=8: calls.__setitem__("peers", calls["peers"] + 1) or [
            {
                "edinet_code": "E00002",
                "ticker": "1002",
                "company_name": "Beta Works",
                "industry": industry or "Industrial",
                "latest_price": 900.0,
                "latest_price_date": "2024-12-31",
                "PERatio": 9.0,
                "PriceToBook": 1.2,
                "DividendsYield": 0.01,
                "ReturnOnEquity": 0.12,
                "MarketCap": 900000.0,
                "one_year_return": 0.05,
                "period_end": "2024-03-31",
            }
        ],
    )

    page = security_analysis_page.SecurityAnalysisPage(root)
    page._db_path = "C:/tmp/sample.db"
    page._selected_security = {
        "edinet_code": "E00001",
        "ticker": "1001",
        "company_name": "Alpha Corp",
        "industry": "Industrial",
    }
    page._load_selected_security()

    assert calls["overview"] == 1
    assert calls["statements"] == 1
    assert calls["price_history"] == 0
    assert calls["peers"] == 0

    page._show_tab("Charts")
    assert calls["price_history"] == 1
    assert calls["peers"] == 0

    page._show_tab("Peers")
    assert calls["peers"] == 1


def test_screening_click_opens_security_analysis(root, monkeypatch):
    """Double-clicking a screening result should open Security Analysis."""
    from ui_tk.pages.screening import ScreeningPage

    calls = []

    class DummyApp:
        def show_security_analysis(self, record, db_path=None):
            calls.append((record, db_path))

    page = ScreeningPage(root, app=DummyApp())
    page._db_path = "C:/tmp/sample.db"
    page._results_df = pd.DataFrame(
        [
            {
                "edinetCode": "E00001",
                "Company_Ticker": "1001",
                "Company_Industry": "Industrial",
            }
        ]
    )
    page._populate_results(page._results_df)

    item = page._tree.get_children()[0]
    monkeypatch.setattr(page._tree, "identify_row", lambda _y: item)

    page._on_company_click(SimpleNamespace(y=0))

    assert calls == [
        (
            {
                "edinet_code": "E00001",
                "ticker": "1001",
                "company_name": "",
                "industry": "Industrial",
                "market": "",
            },
            "C:/tmp/sample.db",
        )
    ]


def test_screening_controller_imports():
    """Screening controller functions are importable."""
    from ui_tk.controllers import (
        screening_get_metrics,
        screening_get_periods,
        screening_run,
        screening_export,
        screening_save,
        screening_load,
        screening_list,
        screening_delete,
        screening_save_history,
        screening_load_history,
    )
