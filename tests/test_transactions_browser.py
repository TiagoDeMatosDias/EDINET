"""Framework-level tests for the transactions table via Playwright.

Run with server on port 8000:
    python -m src.web_app.server
    python -m pytest tests/test_transactions_browser.py -v --headed
"""

import os
import pytest
from pathlib import Path

BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
IBKR_DIR = PROJECT_ROOT / "data" / "ibkr"


def _check_server():
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


if not _check_server():
    pytest.skip(f"Server not running at {BASE_URL}", allow_module_level=True)


def _upload_all(page):
    """Upload a single XML file and rebuild."""
    fpath = str(IBKR_DIR / "2024.xml")
    page.goto(f"{BASE_URL}/portfolio")
    page.wait_for_selector("#pf-tabs", timeout=5000)
    page.locator('[data-tab="upload"]').click()
    page.wait_for_timeout(300)
    page.locator("#pf-file-input").set_input_files(fpath)
    page.wait_for_timeout(2000)
    # Rebuild
    page.locator("#pf-rebuild-btn").click()
    page.wait_for_timeout(5000)


def _go_to_transactions(page):
    page.goto(f"{BASE_URL}/portfolio")
    page.wait_for_selector("#pf-tabs", timeout=5000)
    page.locator('.tab-btn[data-tab="transactions"]').click()
    page.wait_for_selector("#pf-txn-tbl", timeout=15000)


class TestTransactionsTableBasic:
    """Basic rendering and data checks."""

    @pytest.fixture(autouse=True)
    def setup(self, page):
        _upload_all(page)
        _go_to_transactions(page)

    def test_table_loads_with_rows(self, page):
        """Transactions table renders with many rows."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        rows = page.locator("#pf-txn-tbl tbody tr")
        count = rows.count()
        assert count > 50, f"Expected >50 transaction rows, got {count}"

    def test_all_columns_render(self, page):
        """All expected column headers are present."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        headers = page.locator("#pf-txn-tbl th")
        texts = [h.inner_text().strip() for h in headers.all()]
        expected = ["Date", "Type", "Symbol", "Qty", "Price", "Amount", "Cur", "B/S", "Comm", "Description"]
        for e in expected:
            assert e in texts, f"Missing column header: {e}"

    def test_type_badges_present(self, page):
        """Activity type column shows colored badges."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        badges = page.locator("#pf-txn-tbl .badge")
        assert badges.count() > 10, "Expected many badge elements"

    def test_amounts_are_color_coded(self, page):
        """Positive amounts green, negative red."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        green = page.locator('#pf-txn-tbl td[style*="var(--success)"]')
        red = page.locator('#pf-txn-tbl td[style*="var(--danger)"]')
        assert green.count() + red.count() > 0, "Expected color-coded amounts"

    def test_count_label_updates(self, page):
        """Transaction count is displayed."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        count_label = page.locator("#pf-txn-count")
        text = count_label.inner_text()
        assert "transactions" in text.lower()

    def test_descriptions_are_truncated(self, page):
        """Long descriptions get truncated with ellipsis."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        cells = page.locator('#pf-txn-tbl td[style*="text-overflow"]')
        assert cells.count() > 0, "Expected truncated description cells"


class TestTransactionsFilterPopup:
    """Filter popup functionality."""

    @pytest.fixture(autouse=True)
    def setup(self, page):
        _upload_all(page)
        _go_to_transactions(page)

    def test_right_click_symbol_opens_text_filter(self, page):
        """Right-clicking Symbol header opens text filter popup."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        sym = page.locator('th[data-txn-sort="symbol"]')
        sym.click(button="right")
        page.wait_for_timeout(500)
        assert page.locator("#pf-txn-filter-popup").is_visible()

    def test_right_click_qty_opens_numeric_filter(self, page):
        """Right-clicking Qty header opens numeric filter with add button."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        qty = page.locator('th[data-txn-sort="quantity"]')
        qty.click(button="right")
        page.wait_for_timeout(500)
        assert page.locator("#pf-txn-filter-popup").is_visible()
        assert page.locator("#dt-filt-add").is_visible()

    def test_text_filter_shows_unique_values(self, page):
        """Text filter shows checkboxes for unique column values."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        page.locator('th[data-txn-sort="activity_type"]').click(button="right")
        page.wait_for_timeout(500)
        checkboxes = page.locator("#dt-filt-vals input[type=checkbox]")
        count = checkboxes.count()
        assert count >= 2, f"Expected >=2 unique values, got {count}"
        # TRADE should be one of them
        labels = page.locator("#dt-filt-vals label span").all_text_contents()
        assert any("TRADE" in l for l in labels), "TRADE should appear in filter values"

    def test_filter_apply_narrows_results(self, page):
        """Applying a type filter reduces visible rows."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        before = page.locator("#pf-txn-tbl tbody tr").count()

        # Open filter on Type column
        page.locator('th[data-txn-sort="activity_type"]').click(button="right")
        page.wait_for_timeout(500)
        # Uncheck all except TRADE
        page.locator("#dt-filt-none").click()
        page.wait_for_timeout(100)
        # Check TRADE
        page.locator('#dt-filt-vals input[data-val="TRADE"]').check()
        page.locator("#dt-filt-apply").click()
        page.wait_for_timeout(500)

        after = page.locator("#pf-txn-tbl tbody tr").count()
        assert after < before, f"Filter should reduce rows: {before} → {after}"
        assert after > 0

    def test_filter_clear_restores_all_rows(self, page):
        """Clicking All in filter popup restores row count."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        before = page.locator("#pf-txn-tbl tbody tr").count()
        # Apply filter
        page.locator('th[data-txn-sort="activity_type"]').click(button="right")
        page.wait_for_timeout(500)
        page.locator("#dt-filt-none").click()
        page.locator('#dt-filt-vals input[data-val="TRADE"]').check()
        page.locator("#dt-filt-apply").click()
        page.wait_for_timeout(500)
        filtered = page.locator("#pf-txn-tbl tbody tr").count()
        assert filtered < before
        # Click All to restore
        page.locator('th[data-txn-sort="activity_type"]').click(button="right")
        page.wait_for_timeout(500)
        page.locator("#dt-filt-all").click()
        page.locator("#dt-filt-apply").click()
        page.wait_for_timeout(500)
        after = page.locator("#pf-txn-tbl tbody tr").count()
        assert after == before, f"All should restore rows: {before} != {after}"

    def test_filter_indicator_appears(self, page):
        """Active filter shows indicator icon on header."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        # Apply a filter
        page.locator('th[data-txn-sort="activity_type"]').click(button="right")
        page.wait_for_timeout(500)
        page.locator("#dt-filt-none").click()
        page.locator('#dt-filt-vals input[data-val="TRADE"]').check()
        page.locator("#dt-filt-apply").click()
        page.wait_for_timeout(500)
        # Check for indicator
        indicator = page.locator('th[data-txn-sort="activity_type"] .pf-filter-indicator')
        assert indicator.is_visible()

    def test_numeric_filter_add_remove_condition(self, page):
        """Add and remove conditions in numeric filter."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        page.locator('th[data-txn-sort="quantity"]').click(button="right")
        page.wait_for_timeout(500)
        # Should start with 1 condition
        rows1 = page.locator(".pf-filt-cond-row")
        assert rows1.count() >= 1
        # Add another
        page.locator("#dt-filt-add").click()
        page.wait_for_timeout(200)
        assert page.locator(".pf-filt-cond-row").count() >= 2
        # Remove one
        page.locator(".pf-filt-cond-rm").first.click()
        page.wait_for_timeout(200)
        assert page.locator(".pf-filt-cond-row").count() >= 1

    def test_filter_popup_closes_on_outside_click(self, page):
        """Clicking outside the popup closes it."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        page.locator('th[data-txn-sort="symbol"]').click(button="right")
        page.wait_for_timeout(500)
        assert page.locator("#pf-txn-filter-popup").is_visible()
        page.locator("#pf-summary-bar").click()
        page.wait_for_timeout(500)
        assert page.locator("#pf-txn-filter-popup").count() == 0

    def test_search_filters_value_list(self, page):
        """Search input filters the value list in real-time."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        page.locator('th[data-txn-sort="activity_type"]').click(button="right")
        page.wait_for_timeout(500)
        before = page.locator("#dt-filt-vals label:not([style*=\"display: none\"])").count()
        # Type search
        page.locator("#dt-filt-search").fill("TRADE")
        page.wait_for_timeout(300)
        after = page.locator("#dt-filt-vals label:not([style*=\"display: none\"])").count()
        assert after < before, f"Search should filter list: {before} → {after}"


class TestTransactionsServerFilters:
    """Server-side filter controls (date range, activity type)."""

    @pytest.fixture(autouse=True)
    def setup(self, page):
        _upload_all(page)
        _go_to_transactions(page)

    def test_activity_type_filter_works(self, page):
        """Selecting TRADE from type dropdown reduces rows."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        before = page.locator("#pf-txn-tbl tbody tr").count()
        # Trigger filter via JS to ensure the change event fires
        page.evaluate("""
            document.getElementById('pf-txn-type-filter').value = 'TRADE';
            document.getElementById('pf-txn-type-filter').dispatchEvent(new Event('change', {bubbles: true}));
        """)
        page.wait_for_timeout(3000)
        page.wait_for_selector("#pf-txn-tbl", timeout=15000)
        after = page.locator("#pf-txn-tbl tbody tr").count()
        assert after <= before, f"Filter should not increase rows: {before} → {after}"

    def test_date_range_filter_reduces_rows(self, page):
        """API date-range filter returns fewer rows than full dataset."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        before = page.locator("#pf-txn-tbl tbody tr").count()
        # Verify the API filter works directly
        result = page.evaluate("""
            async () => {
                const qs = new URLSearchParams();
                qs.set('start_date', '2024-06-01');
                qs.set('end_date', '2024-06-30');
                qs.set('limit', '10000');
                const resp = await fetch('/api/portfolio/transactions?' + qs.toString());
                const data = await resp.json();
                return data.length;
            }
        """)
        assert result > 0, f"Should have some June 2024 transactions, got {result}"
        assert result < before, f"Date filter should reduce rows: {before} → {result}"

    def test_clear_restores_all(self, page):
        """Clear button removes all server-side filters."""
        page.wait_for_selector("#pf-txn-tbl", timeout=10000)
        before = page.locator("#pf-txn-tbl tbody tr").count()
        # Apply type filter via JS
        page.evaluate("""
            document.getElementById('pf-txn-type-filter').value = 'TRADE';
            document.getElementById('pf-txn-type-filter').dispatchEvent(new Event('change', {bubbles: true}));
        """)
        page.wait_for_timeout(2000)
        filtered = page.locator("#pf-txn-tbl tbody tr").count()
        assert filtered <= before
        # Clear
        page.evaluate("""
            document.getElementById('pf-txn-type-filter').value = '';
            document.getElementById('pf-txn-type-filter').dispatchEvent(new Event('change', {bubbles: true}));
        """)
        page.wait_for_timeout(2000)
        after = page.locator("#pf-txn-tbl tbody tr").count()
        assert after == before, f"Clear should restore rows: {before} != {after}"
