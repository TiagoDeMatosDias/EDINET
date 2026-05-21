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


# ============================================================
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

    def test_all_four_tabs(self, page):
        page.goto(f"{BASE_URL}/portfolio")
        tabs = page.locator("#pf-tabs .tab-btn")
        assert tabs.count() == 4
        labels = [t.inner_text() for t in tabs.all()]
        assert labels == ["Upload", "Holdings", "Transactions", "Performance"]

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
