"""Smoke tests for the ui_tk package.

Verifies that all modules import cleanly and key widgets can be
instantiated with a hidden Tk root — no interactive loop required.
"""

import copy
import tkinter as tk
from types import SimpleNamespace

import pandas as pd
import pytest


def _run_background_now(fn, args=(), on_done=None, on_error=None):
    try:
        result = fn(*args)
    except Exception as exc:  # pragma: no cover - defensive test helper
        if on_error:
            on_error(exc)
            return None
        raise
    if on_done:
        on_done(result)
    return None


def _fail_on_popup(monkeypatch, module):
    def _unexpected_popup(*args, **kwargs):
        if len(args) >= 2:
            detail = args[1]
        elif args:
            detail = args[0]
        else:
            detail = "popup"
        raise AssertionError(f"Unexpected popup: {detail}")

    monkeypatch.setattr(module.messagebox, "showerror", _unexpected_popup)
    monkeypatch.setattr(module.messagebox, "showwarning", _unexpected_popup)
    monkeypatch.setattr(module.messagebox, "showinfo", _unexpected_popup)


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


def test_searchable_combobox_filters_values(root):
    from ui_tk.shared.widgets import SearchableCombobox

    combo = SearchableCombobox(root, values=["Alpha Corp", "Beta Works", "Gamma Holdings"])
    combo.insert(0, "beta")
    combo._on_key_release(SimpleNamespace(keysym="a"))
    assert list(combo["values"]) == ["Beta Works"]

    combo.delete(0, tk.END)
    combo.insert(0, "or")
    combo._on_key_release(SimpleNamespace(keysym="o"))
    assert list(combo["values"]) == ["Alpha Corp", "Beta Works"]

    combo.delete(0, tk.END)
    combo._on_key_release(SimpleNamespace(keysym="BackSpace"))
    assert list(combo["values"]) == ["Alpha Corp", "Beta Works", "Gamma Holdings"]


def test_searchable_combobox_tracks_values_item_assignment(root):
    from ui_tk.shared.widgets import SearchableCombobox

    combo = SearchableCombobox(root, values=["One", "Two"])
    combo["values"] = ["Tokyo", "Toyota", "Nissan"]
    combo.insert(0, "to")
    combo._on_key_release(SimpleNamespace(keysym="o"))
    assert list(combo["values"]) == ["Tokyo", "Toyota"]


def test_searchable_combobox_set_source_values_keeps_full_list(root):
    from ui_tk.shared.widgets import SearchableCombobox

    combo = SearchableCombobox(root, values=["Stock Price", "Income Statement"])
    combo.set("Stock Price")
    combo.set_source_values(["Stock Price", "Income Statement", "Balance Sheet"])
    assert list(combo["values"]) == ["Stock Price", "Income Statement", "Balance Sheet"]


def test_searchable_combobox_pointer_open_shows_full_list_for_selected_value(root):
    from ui_tk.shared.widgets import SearchableCombobox

    combo = SearchableCombobox(root, values=["Stock Price", "Income Statement", "Balance Sheet"])
    combo.set("Stock Price")
    combo._on_pointer_open()
    assert list(combo["values"]) == ["Stock Price", "Income Statement", "Balance Sheet"]


def test_searchable_combobox_reverts_invalid_typed_value(root):
    from ui_tk.shared.widgets import SearchableCombobox

    combo = SearchableCombobox(root, values=["4", "8", "12"])
    combo.set("12")
    combo._on_combobox_selected()

    combo.delete(0, tk.END)
    combo.insert(0, "bad")
    combo._commit_typed_value()
    assert combo.get() == "12"

    combo.delete(0, tk.END)
    combo.insert(0, "8")
    combo._commit_typed_value()
    assert combo.get() == "8"


def test_searchable_combobox_replaces_selected_value_on_first_key(root):
    from ui_tk.shared.widgets import SearchableCombobox

    combo = SearchableCombobox(root, values=["Tokyo", "Toyota", "Nissan"])
    combo.set("Tokyo")
    combo._on_combobox_selected()

    combo._on_focus_in()
    root.update_idletasks()
    assert combo.selection_present()

    combo._on_key_press(SimpleNamespace(keysym="n", char="n"))
    combo.insert(0, "n")
    combo._on_key_release(SimpleNamespace(keysym="n", char="n"))

    assert list(combo["values"]) == ["Nissan"]


def test_rounded_button_tracks_textvariable(root):
    from ui_tk.shared.widgets import RoundedButton

    label_var = tk.StringVar(value="Line Only")
    button = RoundedButton(root, textvariable=label_var)
    assert button._inner.cget("text") == "Line Only"

    label_var.set("Switch to Line")
    root.update_idletasks()
    assert button._inner.cget("text") == "Switch to Line"


def test_security_analysis_invalid_period_and_range_values_fallback(root):
    from ui_tk.pages.security_analysis import SecurityAnalysisPage

    page = SecurityAnalysisPage(root)
    page._period_count_var.set("bad")
    assert page._selected_period_count() == 12
    assert page._period_count_var.get() == "12"

    page._chart_range_var.set("nonsense")
    assert page._selected_chart_year_window() is None
    assert page._chart_range_var.get() == "Max"


def test_orchestrator_remove_persists_active_pipeline(root, monkeypatch):
    from ui_tk.pages import orchestrator as orchestrator_page

    saved_cfgs = []

    monkeypatch.setattr(orchestrator_page.ctrl, "load_ui_pipeline", lambda: {"run_steps": {}})
    monkeypatch.setattr(
        orchestrator_page.ctrl,
        "save_ui_pipeline",
        lambda cfg: saved_cfgs.append(copy.deepcopy(cfg)),
    )

    page = orchestrator_page.OrchestratorPage(root, app=None)
    page.new_setup("Test Setup")
    saved_cfgs.clear()

    page._do_add_step("get_documents")
    page._do_add_step("download_documents")
    saved_cfgs.clear()

    page._remove_step_by_index(0)

    assert page._steps == [["download_documents", False]]
    assert saved_cfgs
    assert saved_cfgs[-1]["run_steps"] == {
        "download_documents": {"enabled": True, "overwrite": False}
    }


def test_orchestrator_prevents_duplicate_steps(root, monkeypatch):
    from ui_tk.pages import orchestrator as orchestrator_page

    monkeypatch.setattr(orchestrator_page.ctrl, "load_ui_pipeline", lambda: {"run_steps": {}})
    monkeypatch.setattr(orchestrator_page.ctrl, "save_ui_pipeline", lambda _cfg: None)

    page = orchestrator_page.OrchestratorPage(root, app=None)
    page.new_setup("Test Setup")

    page._do_add_step("get_documents")
    page._do_add_step("get_documents")

    assert page._steps == [["get_documents", False]]
    assert page._selected_idx == 0


def test_orchestrator_long_step_cards_keep_action_buttons_visible(root, monkeypatch):
    from ui_tk.pages import orchestrator as orchestrator_page

    monkeypatch.setattr(orchestrator_page.ctrl, "load_ui_pipeline", lambda: {"run_steps": {}})
    monkeypatch.setattr(orchestrator_page.ctrl, "save_ui_pipeline", lambda _cfg: None)

    root.geometry("1100x750+0+0")
    page = orchestrator_page.OrchestratorPage(root, app=None)
    page.pack(fill="both", expand=True)
    page.new_setup("Test Setup")

    steps = [
        "populate_company_info",
        "generate_financial_statements",
        "generate_ratios",
        "generate_historical_ratios",
        "Multivariate_Regression",
        "backtest_set",
    ]
    for step_name in steps:
        page._do_add_step(step_name)

    root.update_idletasks()

    assert len(page._sequence_buttons) == len(steps) * 4
    assert all(button.winfo_ismapped() for button in page._sequence_buttons)


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


def test_controllers_build_config_dict_infers_populate_business_descriptions_database():
    from ui_tk.controllers import build_config_dict

    steps = [
        ["generate_financial_statements", True],
        ["populate_business_descriptions_en", False],
    ]
    configs = {
        "generate_financial_statements": {
            "Source_Database": "base.db",
            "Target_Database": "standardized.db",
        },
        "populate_business_descriptions_en": {
            "Target_Database": "",
            "Providers_Config": "config/reference/business_description_translation_providers.example.json",
        },
    }

    result = build_config_dict(steps, configs)

    assert result["populate_business_descriptions_en_config"]["Target_Database"] == "standardized.db"


def test_controllers_build_step_configs_infers_populate_business_descriptions_database():
    from ui_tk.controllers import build_step_configs_from_config

    run_cfg = {
        "generate_financial_statements_config": {
            "Source_Database": "base.db",
            "Target_Database": "standardized.db",
        },
        "populate_business_descriptions_en_config": {
            "Target_Database": "",
            "Providers_Config": "config/reference/business_description_translation_providers.example.json",
        },
    }

    result = build_step_configs_from_config(run_cfg)

    assert result["populate_business_descriptions_en"]["Target_Database"] == "standardized.db"


def test_orchestrator_add_step_infers_populate_business_descriptions_database(root, monkeypatch):
    from ui_tk.pages import orchestrator as orchestrator_page

    monkeypatch.setattr(orchestrator_page.ctrl, "load_ui_pipeline", lambda: {"run_steps": {}})
    monkeypatch.setattr(orchestrator_page.ctrl, "save_ui_pipeline", lambda _cfg: None)

    page = orchestrator_page.OrchestratorPage(root, app=None)
    page.new_setup("Test Setup")
    page._step_configs["generate_financial_statements"] = {
        "Source_Database": "base.db",
        "Target_Database": "standardized.db",
    }

    page._do_add_step("populate_business_descriptions_en")

    assert page._step_configs["populate_business_descriptions_en"]["Target_Database"] == "standardized.db"


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


def test_screening_add_criterion_button_adds_row(root):
    """The screening builder exposes a visible add button that appends a row."""
    from ui_tk.pages.screening import ScreeningPage

    page = ScreeningPage(root)
    root.update_idletasks()

    assert page._add_criterion_btn._inner.cget("text") == "Add Criterion"
    assert len(page._criteria_rows) == 0

    page._add_criterion_btn.invoke()
    root.update_idletasks()

    assert len(page._criteria_rows) == 1


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

    _fail_on_popup(monkeypatch, security_analysis_page)
    monkeypatch.setattr(security_analysis_page, "run_in_background", _run_background_now)
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
        lambda _db_path, _edinet_code, periods=8, statement_sources=None: calls.__setitem__("statements", calls["statements"] + 1) or {
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


def test_security_analysis_company_description_uses_scrollable_text(root, monkeypatch):
    from ui_tk.pages import security_analysis as security_analysis_page

    long_description_en = "DescriptionWithoutSpaces" * 40
    long_description_ja = "日本語の会社概要" * 30
    summary_text = "Short summary paragraph one.\n\nShort summary paragraph two."

    _fail_on_popup(monkeypatch, security_analysis_page)
    monkeypatch.setattr(security_analysis_page, "run_in_background", _run_background_now)
    monkeypatch.setattr(security_analysis_page.ctrl, "get_default_database_path", lambda: "")
    monkeypatch.setattr(security_analysis_page.ctrl, "security_optimize_database", lambda _db_path: {"ok": True})
    monkeypatch.setattr(
        security_analysis_page.ctrl,
        "security_get_overview",
        lambda _db_path, _edinet_code: {
            "company": {
                "edinet_code": "E00001",
                "ticker": "1001",
                "company_name": "Alpha Corp",
                "industry": "Industrial",
                "market": "JPX Prime",
                "description": long_description_en,
                "filing_description": long_description_ja,
                "filing_description_en": long_description_en,
                "description_summary": summary_text,
            },
            "market": {
                "latest_price": 1000.0,
                "latest_price_date": "2024-12-31",
            },
            "fundamentals_latest": {},
            "valuation_latest": {},
            "quality_latest": {},
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
        lambda _db_path, _edinet_code, periods=8, statement_sources=None: {"periods": [], "records": []},
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

    assert page._company_description_text is not None
    assert page._company_description_text.get("1.0", "end-1c") == summary_text
    assert str(page._company_description_text.cget("wrap")) == "char"
    assert int(page._company_description_text.cget("height")) == 7
    assert str(page._company_description_text.cget("state")) == "disabled"
    assert page._company_description_text.cget("yscrollcommand")
    assert page._company_description_heading_var.get() == "Business Summary"
    assert page._company_description_toggle_btn is not None
    assert "!disabled" in page._company_description_toggle_btn.state()

    page._toggle_company_description_mode()

    assert page._company_description_text.get("1.0", "end-1c") == long_description_en
    assert page._company_description_heading_var.get() == "Business Description"
    assert page._company_description_toggle_var.get() == "Show Summary"


def test_security_analysis_statement_chart_uses_peer_statement_history(root, monkeypatch):
    """Statement charts should be populated from statement rows, including peer histories when enabled."""
    from ui_tk.pages import security_analysis as security_analysis_page

    calls = {
        "overview": 0,
        "statements": [],
        "peers": 0,
        "price_history": 0,
    }

    def _statement_payload(edinet_code):
        values = [100.0, 120.0] if edinet_code == "E00001" else [90.0, 110.0]
        periods = ["2023-03-31", "2024-03-31"] if edinet_code == "E00001" else ["2023-12-31", "2024-12-31"]
        return {
            "periods": periods,
            "records": [{"period_end": periods[0]}, {"period_end": periods[1]}],
            "income_statement": [
                {
                    "metric": "Net Sales",
                    "field": "netSales",
                    "record_field": "netSales",
                    "source": "income_statement",
                    "values": values,
                }
            ],
        }

    _fail_on_popup(monkeypatch, security_analysis_page)
    monkeypatch.setattr(security_analysis_page, "run_in_background", _run_background_now)
    monkeypatch.setattr(security_analysis_page.ctrl, "get_default_database_path", lambda: "")
    monkeypatch.setattr(security_analysis_page.ctrl, "security_optimize_database", lambda _db_path: {"ok": True})
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
                "Revenue": 120.0,
                "OperatingIncome": 20.0,
                "NetIncome": 10.0,
                "TotalAssets": 500.0,
                "ShareholdersEquity": 200.0,
                "SharesOutstanding": 1000.0,
            },
            "valuation_latest": {},
            "quality_latest": {},
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
        lambda _db_path, edinet_code, periods=8, statement_sources=None: calls["statements"].append(edinet_code) or _statement_payload(edinet_code),
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
            }
        ],
    )
    monkeypatch.setattr(
        security_analysis_page.ctrl,
        "security_get_price_history",
        lambda _db_path, _ticker: calls.__setitem__("price_history", calls["price_history"] + 1) or [
            {"trade_date": "2024-12-31", "price": 1000.0}
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

    assert calls["statements"] == ["E00001"]

    page._chart_table_var.set("Income Statement")
    page._on_chart_table_changed()
    page._chart_column_var.set("Net Sales")
    page._show_tab("Charts")

    assert calls["peers"] == 1
    assert calls["statements"] == ["E00001", "E00002"]
    assert page._chart_style_button is not None
    assert "!disabled" in page._chart_style_button.state()
    assert page._chart_style_label_var.get() == "Switch to Line"
    assert page._chart_style_button._inner.cget("text") == "Switch to Line"

    axis = page._chart_figure.axes[0]
    labels = [tick.get_text() for tick in axis.get_xticklabels() if tick.get_text()]
    assert labels == ["2023", "2024"]

    legend = axis.get_legend()
    peer_legend_text = next(text for text in legend.get_texts() if text.get_text() == "1002")
    peer_container = next(container for container in axis.containers if container.get_label() == "1002")
    assert all(patch.get_visible() for patch in peer_container.patches)

    page._on_chart_legend_pick(SimpleNamespace(artist=peer_legend_text))
    assert all(not patch.get_visible() for patch in peer_container.patches)

    page._on_chart_legend_pick(SimpleNamespace(artist=peer_legend_text))
    assert all(patch.get_visible() for patch in peer_container.patches)

    before = page._chart_style_var.get()
    page._toggle_chart_style()
    assert page._chart_style_var.get() != before
    assert page._chart_style_label_var.get() == "Switch to Column"
    assert page._chart_style_button._inner.cget("text") == "Switch to Column"


def test_security_analysis_peer_double_click_opens_selected_peer(root, monkeypatch):
    """Double-clicking a peer row should open that company in Security Analysis."""
    from ui_tk.pages import security_analysis as security_analysis_page

    calls = []

    class DummyApp:
        def set_context(self, _title, _text):
            return None

        def show_security_analysis(self, record, db_path=None):
            calls.append((record, db_path))

    _fail_on_popup(monkeypatch, security_analysis_page)
    monkeypatch.setattr(security_analysis_page, "run_in_background", _run_background_now)
    monkeypatch.setattr(security_analysis_page.ctrl, "get_default_database_path", lambda: "")
    monkeypatch.setattr(security_analysis_page.ctrl, "security_optimize_database", lambda _db_path: {"ok": True})
    monkeypatch.setattr(
        security_analysis_page.ctrl,
        "security_get_overview",
        lambda _db_path, _edinet_code: {
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
            },
            "fundamentals_latest": {},
            "valuation_latest": {},
            "quality_latest": {},
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
        lambda _db_path, _edinet_code, periods=8, statement_sources=None: {"periods": [], "records": []},
    )
    monkeypatch.setattr(
        security_analysis_page.ctrl,
        "security_get_peers",
        lambda _db_path, _edinet_code, industry=None, limit=8: [
            {
                "edinet_code": "E00002",
                "ticker": "1002",
                "company_name": "Beta Works",
                "industry": industry or "Industrial",
                "market": "JPX Growth",
                "latest_price": 900.0,
                "latest_price_date": "2024-12-31",
            }
        ],
    )
    monkeypatch.setattr(
        security_analysis_page.ctrl,
        "security_get_price_history",
        lambda _db_path, _ticker: [{"trade_date": "2024-12-31", "price": 1000.0}],
    )

    page = security_analysis_page.SecurityAnalysisPage(root, app=DummyApp())
    page._db_path = "C:/tmp/sample.db"
    page._selected_security = {
        "edinet_code": "E00001",
        "ticker": "1001",
        "company_name": "Alpha Corp",
        "industry": "Industrial",
        "market": "JPX Prime",
    }
    page._load_selected_security()
    page._show_tab("Peers")

    items = page._peers_tree.get_children()
    peer_item = items[-1]
    monkeypatch.setattr(page._peers_tree, "identify_row", lambda _y: peer_item)

    page._on_peer_double_click(SimpleNamespace(y=0))

    assert calls == [
        (
            {
                "edinet_code": "E00002",
                "ticker": "1002",
                "company_name": "Beta Works",
                "industry": "Industrial",
                "market": "JPX Growth",
            },
            "C:/tmp/sample.db",
        )
    ]


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
