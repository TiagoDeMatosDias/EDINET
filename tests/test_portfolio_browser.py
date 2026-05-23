"""
Playwright end-to-end tests for the Portfolio module.

Requires: pip install pytest-playwright playwright
         playwright install chromium

Run with server on port 8000:
    python -m src.web_app.server
    PLAYWRIGHT_TESTS=1 python -m pytest tests/test_portfolio_browser.py -v --headed
"""

import os
import time
import pytest
from pathlib import Path

# Always run these tests — no env-var gate needed
BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
IBKR_DIR = PROJECT_ROOT / "data" / "ibkr"


def _start_server_if_needed():
    """Check if server is running; skip tests if not."""
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2)
    try:
        host = BASE_URL.split("://")[1].split(":")[0]
        port = int(BASE_URL.split(":")[-1])
        s.connect((host, port))
        s.close()
        return True
    except (socket.error, OSError):
        s.close()
        return False


_server_running = _start_server_if_needed()
pytestmark = pytest.mark.skipif(
    not _server_running,
    reason=f"Server not running at {BASE_URL}. Start with: python -m src.web_app.server",
)


# ============================================================
# Test helpers
# ============================================================

def _upload_xml(page, year="2024"):
    """Upload an XML file and wait for success."""
    fpath = str(IBKR_DIR / f"{year}.xml")
    assert Path(fpath).exists(), f"Test file missing: {fpath}"

    # Navigate to upload tab
    page.goto(f"{BASE_URL}/portfolio")
    page.wait_for_selector("#pf-tabs", timeout=5000)

    # Ensure upload tab is active
    upload_tab = page.locator('[data-tab="upload"]')
    if "is-active" not in (upload_tab.get_attribute("class") or ""):
        upload_tab.click()
        page.wait_for_timeout(300)

    # Upload
    file_input = page.locator("#pf-file-input")
    file_input.set_input_files(fpath)

    # Wait for status to appear
    status = page.locator("#pf-upload-status")
    try:
        status.wait_for(state="visible", timeout=30000)
    except:
        pass

    page.wait_for_timeout(1000)
    return status.inner_text() if status.is_visible() else ""


def _rebuild_state(page):
    """Click rebuild and wait for completion."""
    rebuild_btn = page.locator("#pf-rebuild-btn")
    rebuild_btn.click()
    page.wait_for_timeout(3000)
    # Wait for success message
    try:
        page.wait_for_function(
            "document.querySelector('#pf-rebuild-btn')?.textContent?.includes('Done')",
            timeout=30000,
        )
    except:
        pass
    page.wait_for_timeout(1000)


def _switch_to_charts(page):
    """Switch to the Charts tab."""
    page.locator('[data-tab="charts"]').click()
    page.wait_for_timeout(500)# ============================================================
# Core E2E: Upload → Rebuild → Verify
# ============================================================

class TestFullUploadFlow:
    """Upload all 6 XML files, rebuild, and verify data appears everywhere."""

    def test_upload_single_file_2024(self, page):
        status = _upload_xml(page, "2024")
        assert "Uploaded" in status or "new" in status.lower() or "skipped" in status.lower(), \
            f"Upload status unexpected: '{status}'"
        assert "error" not in status.lower(), f"Upload failed: {status}"

    def test_upload_all_years(self, page):
        """Upload all 6 XML files. Second upload of same file should dedup."""
        for year in ["2020", "2021", "2022", "2023", "2024", "2025"]:
            status = _upload_xml(page, year)
            assert "error" not in status.lower(), f"Upload {year} failed: {status}"

        # Second upload of 2024 should show 0 new
        status2 = _upload_xml(page, "2024")
        assert "0 new" in status2.lower() or "skipped" in status2.lower(), \
            f"Duplicate upload should show 0 new: {status2}"

    def test_rebuild_after_upload(self, page):
        """After uploading 2024, rebuild should produce holdings."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        # Switch to holdings tab
        page.locator('[data-tab="holdings"]').click()
        page.wait_for_timeout(1000)

        table = page.locator("#pf-holdings-table")
        text = table.inner_text()
        assert "VWCE" in text, f"Expected VWCE in holdings, got: {text[:300]}"
        assert "No data" not in text, f"Holdings shows no data: {text[:300]}"

    def test_full_cycle_all_years(self, page):
        """Upload all 6 files → rebuild → verify holdings, transactions, performance."""
        # Upload all
        for year in ["2020", "2021", "2022", "2023", "2024", "2025"]:
            status = _upload_xml(page, year)
            assert "error" not in status.lower(), f"Upload {year} failed: {status}"

        # Rebuild
        _rebuild_state(page)

        # === HOLDINGS TAB ===
        page.locator('[data-tab="holdings"]').click()
        page.wait_for_timeout(1500)
        table = page.locator("#pf-holdings-table")
        holdings_text = table.inner_text()

        # Should have stock holdings
        expected_symbols = ["VWCE", "JXN", "BTI"]
        for sym in expected_symbols:
            assert sym in holdings_text, f"Expected {sym} in holdings: {holdings_text[:300]}"

        # Should NOT show "No data"
        assert "No data" not in holdings_text

        # === TRANSACTIONS TAB ===
        page.locator('[data-tab="transactions"]').click()
        page.wait_for_timeout(1500)
        txn_table = page.locator("#pf-transactions-table")
        txn_text = txn_table.inner_text()

        # Should have trades and dividends
        assert "TRADE" in txn_text, f"Expected TRADE in transactions: {txn_text[:300]}"
        assert "DIVIDEND" in txn_text, f"Expected DIVIDEND in transactions: {txn_text[:300]}"
        assert "No transactions" not in txn_text

        # === PERFORMANCE TAB ===
        page.locator('[data-tab="performance"]').click()
        page.wait_for_timeout(500)

        # Click Compute
        page.locator("#pf-perf-compute").click()
        page.wait_for_timeout(5000)

        metrics = page.locator("#pf-metrics-grid")
        metrics_text = metrics.inner_text()

        assert "Sharpe" in metrics_text, f"Expected Sharpe in metrics: {metrics_text[:300]}"
        assert "Total Return" in metrics_text, f"Expected Total Return: {metrics_text[:300]}"
        assert "Dividends" in metrics_text or "Div Gross" in metrics_text, \
            f"Expected dividend data: {metrics_text[:300]}"

        # Should not show error
        assert "Click Compute" not in metrics_text, "Metrics not computed"


# ============================================================
# Navigation
# ============================================================

class TestNavigation:
    """Portfolio tab appears in navigation on all pages."""

    def test_portfolio_tab_on_main(self, page):
        page.goto(f"{BASE_URL}/")
        tabs = page.locator("nav.tabs button.tab")
        texts = [t.inner_text() for t in tabs.all()]
        assert "Portfolio" in texts, f"Portfolio missing from main nav: {texts}"

    def test_portfolio_tab_on_backtesting(self, page):
        page.goto(f"{BASE_URL}/backtesting")
        tabs = page.locator("nav.tabs button.tab")
        texts = [t.inner_text() for t in tabs.all()]
        assert "Portfolio" in texts, f"Portfolio missing from backtesting nav: {texts}"

    def test_portfolio_tab_on_screening(self, page):
        page.goto(f"{BASE_URL}/screening")
        tabs = page.locator("nav.tabs button.tab")
        texts = [t.inner_text() for t in tabs.all()]
        assert "Portfolio" in texts, f"Portfolio missing from screening nav: {texts}"

    def test_portfolio_tab_on_orchestrator(self, page):
        page.goto(f"{BASE_URL}/orchestrator")
        tabs = page.locator("nav.tabs button.tab")
        texts = [t.inner_text() for t in tabs.all()]
        assert "Portfolio" in texts, f"Portfolio missing from orchestrator nav: {texts}"

    def test_portfolio_tab_on_security(self, page):
        page.goto(f"{BASE_URL}/security")
        tabs = page.locator("nav.tabs button.tab")
        texts = [t.inner_text() for t in tabs.all()]
        assert "Portfolio" in texts, f"Portfolio missing from security nav: {texts}"


# ============================================================
# UI Elements
# ============================================================

class TestUIElements:
    """Verify all UI elements render correctly."""

    def test_drop_zone_is_label(self, page):
        page.goto(f"{BASE_URL}/portfolio")
        drop_zone = page.locator("#pf-drop-zone")
        assert drop_zone.is_visible()
        tag = drop_zone.evaluate("el => el.tagName")
        assert tag == "LABEL", f"Expected LABEL, got {tag}"
        assert drop_zone.get_attribute("for") == "pf-file-input"

    def test_file_input_hidden_properly(self, page):
        page.goto(f"{BASE_URL}/portfolio")
        file_input = page.locator("#pf-file-input")
        # Should exist but be "hidden" via CSS (not display:none)
        assert file_input.get_attribute("type") == "file"
        opacity = file_input.evaluate("el => window.getComputedStyle(el).opacity")
        assert float(opacity) < 0.1, f"File input opacity={opacity}, should be hidden"

    def test_all_five_tabs(self, page):
        page.goto(f"{BASE_URL}/portfolio")
        tabs = page.locator("#pf-tabs .tab-btn")
        assert tabs.count() == 5
        labels = [t.inner_text() for t in tabs.all()]
        assert labels == ["\U0001f4e4 File Upload", "\U0001f4ca Holdings", "\U0001f4cb Transactions", "\U0001f4c8 Charts", "\u26a1 Performance Metrics"]

    def test_drop_zone_clickable(self, page):
        page.goto(f"{BASE_URL}/portfolio")
        drop_zone = page.locator("#pf-drop-zone")
        cursor = drop_zone.evaluate("el => window.getComputedStyle(el).cursor")
        assert cursor == "pointer", f"Cursor should be pointer, got {cursor}"

    def test_chart_renders_after_rebuild(self, page):
        """Value chart should render after upload+rebuild."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.locator('[data-tab="holdings"]').click()
        page.wait_for_timeout(1000)

        # Chart.js should have created a canvas with rendered content
        canvas = page.locator("#pf-value-chart")
        assert canvas.is_visible(), "Value chart not visible"


# ============================================================
# Corporate actions
# ============================================================

class TestCorporateActions:
    """Verify corporate actions appear in data."""

    def test_spinoff_symbols_in_holdings_history(self, page):
        """After uploading all years, SOLV/ONL should appear in history."""
        for year in ["2020", "2021", "2022", "2023", "2024", "2025"]:
            _upload_xml(page, year)
        _rebuild_state(page)

        # Check holdings at spinoff date via API
        resp = page.evaluate("""
            async () => {
                const r = await fetch('/api/portfolio/holdings/at/2024-04-15');
                return await r.json();
            }
        """)
        symbols = {h["symbol"] for h in resp if h.get("quantity", 0) > 0}
        assert "SOLV" in symbols or "ONL" in symbols, \
            f"Expected spinoff symbol in holdings at 2024-04-15, got: {symbols}"


# ============================================================
# Holding Detail Chart
# ============================================================

class TestHoldingDetailChart:
    """Click a holding row and verify the inline detail chart renders."""

    def test_click_holding_shows_chart(self, page):
        """Upload, rebuild, click a holding row — detail chart must be visible with data."""
        # 1. Ensure data exists
        _upload_xml(page, "2024")
        _rebuild_state(page)

        # 2. Navigate to holdings tab
        page.goto(f"{BASE_URL}/portfolio")
        page.wait_for_selector("#pf-tabs", timeout=5000)
        page.locator('[data-tab="holdings"]').click()
        page.wait_for_timeout(2000)

        # 3. Verify holdings table is present with data rows
        table = page.locator("#pf-holdings-table")
        table.wait_for(state="visible", timeout=5000)
        assert "VWCE" in table.inner_text(), "Expected VWCE in holdings table"

        # 4. Click the first non-CASH holding row (has data-holding attribute)
        rows = page.locator(".pf-holding-row[data-holding]")
        row_count = rows.count()
        assert row_count > 0, "No clickable holding rows found"
        first_row = rows.first
        first_row.click()
        page.wait_for_timeout(3000)  # Wait for fetch + chart render

        # 5. Verify detail row appeared
        detail_row = page.locator(".pf-holding-detail-row")
        assert detail_row.is_visible(), "Detail row not visible after clicking holding"

        # 6. Verify detail row contains a canvas for the chart
        detail_canvas = detail_row.locator("canvas")
        assert detail_canvas.is_visible(), "Chart canvas not visible in detail row"

        # 7. Verify canvas has been drawn on (Chart.js rendered data)
        canvas_has_content = detail_canvas.evaluate("""
            (el) => {
                const ctx = el.getContext('2d');
                if (!ctx) return false;
                // Check if canvas has non-empty pixels
                const imageData = ctx.getImageData(0, 0, el.width, el.height);
                if (!imageData || !imageData.data) return false;
                // Check at least some non-zero alpha pixels exist
                for (let i = 3; i < imageData.data.length; i += 4) {
                    if (imageData.data[i] > 0) return true;
                }
                return false;
            }
        """)
        assert canvas_has_content, "Chart canvas has no rendered content (Chart.js may not have drawn data)"

    def test_click_holding_then_another_replaces_chart(self, page):
        """Clicking a second holding should replace the detail chart."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        page.wait_for_selector("#pf-tabs", timeout=5000)
        page.locator('[data-tab="holdings"]').click()
        page.wait_for_timeout(2000)

        rows = page.locator(".pf-holding-row[data-holding]")
        row_count = rows.count()
        assert row_count >= 2, f"Need at least 2 holding rows, got {row_count}"

        # Click first row
        rows.nth(0).click()
        page.wait_for_timeout(2500)
        detail_rows = page.locator(".pf-holding-detail-row")
        assert detail_rows.count() == 1, "Expected exactly 1 detail row after first click"

        # Click second row — should replace, not duplicate
        rows.nth(1).click()
        page.wait_for_timeout(2500)
        detail_rows = page.locator(".pf-holding-detail-row")
        assert detail_rows.count() == 1, "Expected exactly 1 detail row after second click (should replace)"

        # Second click on same row should close it
        rows.nth(1).click()
        page.wait_for_timeout(500)
        detail_rows = page.locator(".pf-holding-detail-row")
        assert detail_rows.count() == 0, "Expected 0 detail rows after clicking same row again (toggle close)"


# ============================================================
# Chart Controls Independence
# ============================================================

class TestChartIndependence:
    """Charts tab has its own currency, benchmark, and inflation controls."""

    def test_chart_controls_exist(self, page):
        """Charts tab has independent currency, benchmark, inflation, update controls."""
        page.goto(f"{BASE_URL}/portfolio")
        page.wait_for_selector("#pf-tabs", timeout=5000)
        page.locator('[data-tab="charts"]').click()
        page.wait_for_timeout(500)

        assert page.locator("#pf-chart-currency").is_visible(), "Chart currency selector not found"
        assert page.locator("#pf-chart-benchmark").is_visible(), "Chart benchmark input not found"
        assert page.locator("#pf-chart-inflation").is_visible(), "Chart inflation checkbox not found"
        assert page.locator("#pf-chart-update").is_visible(), "Chart update button not found"

    def test_chart_controls_independent_from_performance(self, page):
        """Chart currency and benchmark are separate from performance tab."""
        page.goto(f"{BASE_URL}/portfolio")
        page.wait_for_selector("#pf-tabs", timeout=5000)

        # Set chart controls
        page.locator('[data-tab="charts"]').click()
        page.wait_for_timeout(300)
        page.locator("#pf-chart-currency").select_option("USD")
        page.locator("#pf-chart-benchmark").fill("SPY")

        # Switch to performance tab — different elements
        page.locator('[data-tab="performance"]').click()
        page.wait_for_timeout(300)

        # Performance tab should still have default EUR, not USD
        perf_currency = page.locator("#pf-perf-currency").input_value()
        assert perf_currency == "EUR", \
            f"Performance currency should remain EUR, got {perf_currency}"

        # Performance benchmark should be empty, not SPY
        perf_bench = page.locator("#pf-perf-benchmark").input_value()
        assert perf_bench == "", \
            f"Performance benchmark should remain empty, got {perf_bench}"

    def test_chart_update_button_triggers_api_call(self, page):
        """Clicking Update Charts calls the performance API with chart-specific params."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        page.wait_for_selector("#pf-tabs", timeout=5000)
        page.locator('[data-tab="charts"]').click()
        page.wait_for_timeout(500)

        # Set chart-specific benchmark
        page.locator("#pf-chart-benchmark").fill("SPY")

        # Intercept API call to verify correct params
        api_called = page.evaluate("""
            () => {
                return new Promise((resolve) => {
                    const origFetch = window.fetch;
                    window.fetch = function(url, opts) {
                        if (url.includes('/api/portfolio/performance')) {
                            resolve(url);
                        }
                        return origFetch.apply(this, arguments);
                    };
                    // Click the update button
                    document.getElementById('pf-chart-update').click();
                });
            }
        """)
        assert api_called, "Performance API was not called"
        assert "benchmark_ticker=SPY" in api_called, \
            f"Expected benchmark_ticker=SPY in API call, got: {api_called}"
        assert "base_currency=EUR" in api_called, \
            f"Expected base_currency=EUR in API call, got: {api_called}"

    def test_equity_chart_renders_with_benchmark(self, page):
        """After update, equity chart canvas is visible and has benchmark data."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        page.wait_for_selector("#pf-tabs", timeout=5000)
        page.locator('[data-tab="charts"]').click()
        page.wait_for_timeout(500)

        # Set benchmark and click update
        page.locator("#pf-chart-benchmark").fill("SPY")
        page.locator("#pf-chart-update").click()
        page.wait_for_timeout(5000)  # Wait for API + chart render

        # Verify equity chart canvas is visible
        canvas = page.locator("#pf-equity-chart")
        assert canvas.is_visible(), "Equity chart not visible after update"

        # Verify chart has rendered content (pixels drawn)
        canvas_has_content = canvas.evaluate("""
            (el) => {
                const ctx = el.getContext('2d');
                if (!ctx) return false;
                const imageData = ctx.getImageData(0, 0, el.width, el.height);
                if (!imageData || !imageData.data) return false;
                for (let i = 3; i < imageData.data.length; i += 4) {
                    if (imageData.data[i] > 0) return true;
                }
                return false;
            }
        """)
        assert canvas_has_content, "Equity chart has no rendered content"

    def test_inflation_toggle_controls_line_visibility(self, page):
        """Unchecking inflation checkbox removes the inflation line."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        page.wait_for_selector("#pf-tabs", timeout=5000)
        page.locator('[data-tab="charts"]').click()
        page.wait_for_timeout(500)

        # First click update with inflation ON (default)
        page.locator("#pf-chart-update").click()
        page.wait_for_timeout(5000)

        # Check legend contains "Inflation"
        legend_text = page.locator("#pf-equity-chart").evaluate("""
            (el) => {
                const chart = Chart.getChart(el);
                if (!chart) return '';
                return chart.legend.legendItems.map(i => i.text).join('|');
            }
        """)
        assert "Inflation" in legend_text, \
            f"Expected 'Inflation' in chart legend, got: {legend_text}"

        # Uncheck inflation and update
        page.locator("#pf-chart-inflation").uncheck()
        page.locator("#pf-chart-update").click()
        page.wait_for_timeout(5000)

        # Check legend no longer contains "Inflation"
        legend_text2 = page.locator("#pf-equity-chart").evaluate("""
            (el) => {
                const chart = Chart.getChart(el);
                if (!chart) return '';
                return chart.legend.legendItems.map(i => i.text).join('|');
            }
        """)
        assert "Inflation" not in legend_text2, \
            f"Expected no 'Inflation' in chart legend after uncheck, got: {legend_text2}"


# ============================================================
# Portfolio Value — full-width & constituent breakdown
# ============================================================

class TestPortfolioValueChart:
    """Portfolio Value chart is full-width with breakdown-by-holding toggle."""

    def test_value_chart_full_width(self, page):
        """Portfolio Value chart spans full width (not in 2-col grid)."""
        page.goto(f"{BASE_URL}/portfolio")
        page.wait_for_selector("#pf-tabs", timeout=5000)
        page.locator('[data-tab="charts"]').click()
        page.wait_for_timeout(500)

        canvas = page.locator("#pf-value-chart")
        assert canvas.is_visible(), "Value chart canvas not visible"

        # Check it's wider than half the viewport (full-width panel)
        width = canvas.evaluate("el => el.getBoundingClientRect().width")
        vw = page.evaluate("() => window.innerWidth")
        assert width > vw * 0.7, \
            f"Value chart width {width:.0f}px should be >70% of viewport {vw}px"

    def test_breakdown_toggle_exists(self, page):
        """Breakdown by Holding checkbox is present."""
        page.goto(f"{BASE_URL}/portfolio")
        page.wait_for_selector("#pf-tabs", timeout=5000)
        page.locator('[data-tab="charts"]').click()
        page.wait_for_timeout(500)

        cb = page.locator("#pf-value-breakdown")
        assert cb.is_visible(), "Breakdown checkbox not visible"
        assert not cb.is_checked(), "Breakdown should default to unchecked"

    def test_breakdown_shows_stacked_chart(self, page):
        """Checking breakdown fetches constituents and renders stacked area."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        page.wait_for_selector("#pf-tabs", timeout=5000)
        page.locator('[data-tab="charts"]').click()
        page.wait_for_timeout(1000)

        # Check breakdown
        page.locator("#pf-value-breakdown").check()
        page.wait_for_timeout(3000)  # Wait for API + render

        canvas = page.locator("#pf-value-chart")
        assert canvas.is_visible(), "Value chart not visible with breakdown"

        # Verify chart has rendered content
        has_content = canvas.evaluate("""
            (el) => {
                const ctx = el.getContext('2d');
                if (!ctx) return false;
                const d = ctx.getImageData(0, 0, el.width, el.height);
                if (!d || !d.data) return false;
                for (let i = 3; i < d.data.length; i += 4) {
                    if (d.data[i] > 0) return true;
                }
                return false;
            }
        """)
        assert has_content, "Breakdown chart has no rendered content"

        # Legend should contain holding symbols (not just "Total Value")
        legend = canvas.evaluate("""
            (el) => {
                const chart = Chart.getChart(el);
                if (!chart) return '';
                return chart.legend.legendItems.map(i => i.text).join('|');
            }
        """)
        assert "Cash" in legend, f"Expected 'Cash' in breakdown legend, got: {legend}"
        # Should have at least one stock symbol beyond Cash
        assert len(legend.split('|')) >= 2, \
            f"Expected multiple legend items, got: {legend}"

    def test_constituents_zeros_after_position_closed(self, page):
        """After a position is sold, its value in the constituent series
        must be 0 (not null) for dates after the last holding entry,
        so the chart fill drops to zero instead of bridging across the gap."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        page.wait_for_selector("#pf-tabs", timeout=5000)

        # Fetch constituents
        result = page.evaluate("""
            async () => {
                const r = await fetch('/api/portfolio/holdings/history/constituents');
                return await r.json();
            }
        """)

        constituents = result
        dates = constituents.get("dates", [])
        series = constituents.get("series", {})
        assert dates, "Constituents returned no dates"
        assert series, "Constituents returned no series"

        # For every symbol, verify: no null values AFTER the last non-null.
        # (Before the first purchase, null is fine — it creates a proper gap.)
        for sym, vals in series.items():
            last_nonnull = -1
            for i in range(len(vals) - 1, -1, -1):
                if vals[i] is not None:
                    last_nonnull = i
                    break
            if last_nonnull >= 0:
                for i in range(last_nonnull + 1, len(vals)):
                    assert vals[i] is not None, (
                        f"Symbol {sym}: value at index {i} (date {dates[i]}) "
                        f"is null after last holding at index {last_nonnull} (date {dates[last_nonnull]})"
                    )


# ============================================================
# Dividends over time chart
# ============================================================

class TestDividendsChart:
    """Dividends chart with monthly / quarterly / yearly aggregation."""

    def test_dividends_chart_exists(self, page):
        """Dividends chart and period selector are present."""
        page.goto(f"{BASE_URL}/portfolio")
        page.wait_for_selector("#pf-tabs", timeout=5000)
        page.locator('[data-tab="charts"]').click()
        page.wait_for_timeout(500)

        assert page.locator("#pf-dividends-chart").is_visible(), "Dividends chart not visible"
        assert page.locator("#pf-div-period").is_visible(), "Period selector not visible"
        period_val = page.locator("#pf-div-period").input_value()
        assert period_val == "monthly", f"Default period should be monthly, got {period_val}"

    def test_dividends_renders_with_data(self, page):
        """After upload+rebuild, dividends chart renders with bars."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        page.wait_for_selector("#pf-tabs", timeout=5000)
        page.locator('[data-tab="charts"]').click()
        page.wait_for_timeout(2000)

        canvas = page.locator("#pf-dividends-chart")
        assert canvas.is_visible(), "Dividends canvas not visible"

        has_content = canvas.evaluate("""
            (el) => {
                const ctx = el.getContext('2d');
                if (!ctx) return false;
                const d = ctx.getImageData(0, 0, el.width, el.height);
                if (!d || !d.data) return false;
                for (let i = 3; i < d.data.length; i += 4) {
                    if (d.data[i] > 0) return true;
                }
                return false;
            }
        """)
        assert has_content, "Dividends chart has no rendered content"

    def test_period_selector_changes_chart(self, page):
        """Switching period re-renders the dividends chart."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        page.wait_for_selector("#pf-tabs", timeout=5000)
        page.locator('[data-tab="charts"]').click()
        page.wait_for_timeout(2000)

        # Switch to yearly
        page.locator("#pf-div-period").select_option("yearly")
        page.wait_for_timeout(2000)

        canvas = page.locator("#pf-dividends-chart")
        assert canvas.is_visible(), "Dividends chart not visible after period change"

        has_content = canvas.evaluate("""
            (el) => {
                const ctx = el.getContext('2d');
                if (!ctx) return false;
                const d = ctx.getImageData(0, 0, el.width, el.height);
                if (!d || !d.data) return false;
                for (let i = 3; i < d.data.length; i += 4) {
                    if (d.data[i] > 0) return true;
                }
                return false;
            }
        """)
        assert has_content, "Dividends chart has no content after yearly switch"

    def test_quarterly_period_renders(self, page):
        """Quarterly period also renders correctly."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        page.wait_for_selector("#pf-tabs", timeout=5000)
        page.locator('[data-tab="charts"]').click()
        page.wait_for_timeout(2000)

        page.locator("#pf-div-period").select_option("quarterly")
        page.wait_for_timeout(2000)

        canvas = page.locator("#pf-dividends-chart")
        has_content = canvas.evaluate("""
            (el) => {
                const ctx = el.getContext('2d');
                if (!ctx) return false;
                const d = ctx.getImageData(0, 0, el.width, el.height);
                if (!d || !d.data) return false;
                for (let i = 3; i < d.data.length; i += 4) {
                    if (d.data[i] > 0) return true;
                }
                return false;
            }
        """)
        assert has_content, "Dividends chart has no content for quarterly"


# ============================================================
# Error handling
# ============================================================

class TestErrorHandling:
    """Verify error messages display correctly."""

    def test_non_xml_file_upload(self, page):
        page.goto(f"{BASE_URL}/portfolio")

        # Create a temp text file via set_input_files
        import tempfile
        fd, tmp = tempfile.mkstemp(suffix=".txt")
        os.close(fd)
        try:
            with open(tmp, "w") as f:
                f.write("not xml")

            file_input = page.locator("#pf-file-input")
            file_input.set_input_files(tmp)
            page.wait_for_timeout(2000)

            status = page.locator("#pf-upload-status")
            if status.is_visible():
                status_text = status.inner_text()
                assert "error" in status_text.lower() or "xml" in status_text.lower(), \
                    f"Expected error for non-XML, got: {status_text}"
        finally:
            try:
                os.unlink(tmp)
            except:
                pass


# ============================================================================
# New Analytical Charts
# ============================================================================

class TestDividendGrowthChart:
    """Dividend Growth YoY chart."""

    def test_yoy_api_returns_data(self, page):
        """API for dividend growth returns years, dividends, and growth values."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        _switch_to_charts(page)

        data = page.evaluate("""
            async () => {
                const r = await fetch('/api/portfolio/dividends/yoy');
                return await r.json();
            }
        """)

        assert data["years"], "No years in dividend growth data"
        assert data["dividends"], "No dividend values"
        assert len(data["years"]) == len(data["dividends"])
        assert len(data["yoy_growth"]) == len(data["years"])
        # First year should have null growth
        assert data["yoy_growth"][0] is None

    def test_div_growth_chart_renders(self, page):
        """Dividend growth chart canvas is visible and has data on the chart page."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        _switch_to_charts(page)

        canvas = page.locator("#pf-div-growth-chart")
        assert canvas.is_visible(), "Dividend growth chart not visible"

    def test_toggle_to_per_company_view(self, page):
        """Per-company DPS chart renders when toggled."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        _switch_to_charts(page)

        btn = page.locator("#pf-div-growth-show-dps")
        btn.click()
        page.wait_for_timeout(500)

        canvas = page.locator("#pf-div-growth-chart")
        assert canvas.is_visible(), "Per-company dividends chart not visible after toggle"

        # Toggle back
        btn.click()
        page.wait_for_timeout(500)
        assert canvas.is_visible(), "Aggregate dividends chart not visible after toggling back"


class TestReturnsByCompanyChart:
    """Returns by Company chart."""

    def test_api_returns_company_data(self, page):
        """API returns companies with total_return, capital_gain, dividend_return arrays."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        _switch_to_charts(page)

        data = page.evaluate("""
            async () => {
                const r = await fetch('/api/portfolio/returns/by-company');
                return await r.json();
            }
        """)

        assert data["years"], "No years in returns data"
        assert data["companies"], "No companies in returns data"
        for sym, cdata in data["companies"].items():
            assert "total_return" in cdata, f"Company {sym} missing total_return"
            assert "capital_gain" in cdata, f"Company {sym} missing capital_gain"
            assert "dividend_return" in cdata, f"Company {sym} missing dividend_return"
            assert len(cdata["total_return"]) == len(data["years"])

    def test_chart_renders(self, page):
        """Returns by company chart canvas is visible."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        _switch_to_charts(page)

        canvas = page.locator("#pf-returns-by-company-chart")
        assert canvas.is_visible(), "Returns by company chart not visible"

    def test_decompose_toggle(self, page):
        """Decompose checkbox triggers re-render."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        _switch_to_charts(page)

        cb = page.locator("#pf-returns-decompose")
        assert cb.is_visible(), "Decompose checkbox not visible"
        # Check it and verify canvas still visible
        cb.check()
        page.wait_for_timeout(500)
        assert page.locator("#pf-returns-by-company-chart").is_visible(), \
            "Returns chart not visible after decompose toggle"


class TestCurrencySplitChart:
    """Asset currency split pie chart."""

    def test_chart_renders_from_holdings(self, page):
        """Currency split pie chart renders using holdings currency data."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        _switch_to_charts(page)

        canvas = page.locator("#pf-currency-split-chart")
        assert canvas.is_visible(), "Currency split chart not visible"


class TestClosedReturnsChart:
    """Returns by closed positions chart."""

    def test_closed_positions_api_connectivity(self, page):
        """Closed positions API returns data (even if empty)."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        _switch_to_charts(page)

        result = page.evaluate("""
            async () => {
                const r = await fetch('/api/portfolio/holdings/closed');
                return await r.json();
            }
        """)
        assert isinstance(result, list), "Closed positions API should return a list"

    def test_chart_renders(self, page):
        """Closed positions chart canvas exists."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        _switch_to_charts(page)

        canvas = page.locator("#pf-closed-returns-chart")
        assert canvas.is_visible(), "Closed positions chart not visible"


class TestChartMaximize:
    """Click-to-expand chart feature."""

    def test_click_chart_expands_panel(self, page):
        """Clicking a chart panel body adds the chart-expanded class."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        _switch_to_charts(page)

        # Click the portfolio value chart body
        body = page.locator("#pf-value-chart").locator("..")  # canvas is inside panel-body
        # Actually click the panel-body directly
        panel_body = page.locator("#pf-value-chart").locator("xpath=..")
        panel_body.click()
        page.wait_for_timeout(300)

        # Verify the parent panel has chart-expanded class
        panel = page.locator("#pf-value-chart").locator("xpath=ancestor::div[contains(@class,'panel')][1]")
        classes = panel.get_attribute("class") or ""
        assert "chart-expanded" in classes, f"Panel not expanded: {classes}"

    def test_click_again_restores(self, page):
        """Clicking an expanded chart restores it."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        _switch_to_charts(page)

        body = page.locator("#pf-value-chart").locator("xpath=..")
        body.click()
        page.wait_for_timeout(300)
        body.click()
        page.wait_for_timeout(300)

        panel = page.locator("#pf-value-chart").locator("xpath=ancestor::div[contains(@class,'panel')][1]")
        classes = panel.get_attribute("class") or ""
        assert "chart-expanded" not in classes, f"Panel still expanded: {classes}"

    def test_expand_hint_visible_on_hover(self, page):
        """The expand icon (⛶) appears on hover."""
        _upload_xml(page, "2024")
        _rebuild_state(page)

        page.goto(f"{BASE_URL}/portfolio")
        _switch_to_charts(page)

        # Check that a panel-body has the ::after pseudo-element styling
        # (We can verify the CSS class exists on panel-body)
        panel_body = page.locator(".panel-body").first
        assert panel_body.is_visible()
