"""
Playwright browser tests for the Screening view.

Coverage:
- Computed Column builder: adding a token-based custom computed column
- Computed Column builder: dialog doesn't close when using sub-popups
- Computed Column builder: pre-built formula adds correctly
"""

import threading
import time

import pytest
import uvicorn

from src.web_app.server import app

SERVER_PORT = 8766
BASE_URL = f"http://localhost:{SERVER_PORT}"


@pytest.fixture(scope="session")
def server():
    """Start the FastAPI server in a background thread for the test session."""
    config = uvicorn.Config(app, host="127.0.0.1", port=SERVER_PORT, log_level="error")
    server_instance = uvicorn.Server(config)

    thread = threading.Thread(target=server_instance.run, daemon=True)
    thread.start()

    for _ in range(30):
        try:
            import urllib.request
            urllib.request.urlopen(f"{BASE_URL}/health", timeout=0.5)
            break
        except Exception:
            time.sleep(0.3)
    else:
        raise RuntimeError("Server did not start in time")

    yield

    server_instance.should_exit = True
    thread.join(timeout=5)


@pytest.fixture(scope="function")
def page(context):
    """Create a fresh page for each test."""
    pg = context.new_page()
    yield pg
    pg.close()


# ---------------------------------------------------------------------------
# Computed Column Builder tests
# ---------------------------------------------------------------------------


def test_computed_column_custom_expression_builder(page, server):
    """
    Verify the Custom tab of the computed column builder works:
    1. Dialog opens
    2. Can switch to Custom tab
    3. Can type a name
    4. Can open the token "+" menu and add a column token
    5. The main dialog stays open during sub-popup interaction
    6. Token chip appears, preview updates
    7. Clicking "Add Computed Column" adds it to the column list
    """
    page.goto(f"{BASE_URL}/screening")
    page.wait_for_selector("#scr-cfg", state="attached", timeout=10000)
    # Wait for DB to load (status shows tables loaded or No database found)
    page.wait_for_timeout(2000)

    # If no DB is found, skip the test gracefully
    status = page.locator("#scr-status").text_content() or ""
    if "No database" in status or "Error" in status:
        pytest.skip(f"Screening page has no DB: {status}")

    # Click "+ Add Computed"
    add_comp_btn = page.locator("#scr-add-comp")
    assert add_comp_btn.is_visible(), "+ Add Computed button should be visible"
    add_comp_btn.click()
    page.wait_for_timeout(500)

    # The computed column popup should be visible
    popup = page.locator(".scr-pop-comp")
    assert popup.is_visible(), "Computed column builder popup should be visible"

    # Switch to Custom tab
    custom_tab = popup.locator(".scr-comp-tab", has_text="Custom")
    custom_tab.click()
    page.wait_for_timeout(300)

    # Type a column name
    name_input = popup.locator('input[placeholder="Column name (e.g. My Ratio)"]')
    assert name_input.is_visible(), "Name input should be visible in Custom tab"
    name_input.fill("TestRatio")
    page.wait_for_timeout(200)

    # Click the "+" button to add a token
    add_token_btn = popup.locator(".scr-chp-add")
    assert add_token_btn.is_visible(), "+ token button should be visible"
    add_token_btn.click()
    page.wait_for_timeout(300)

    # Token menu should appear AND the main popup should still be visible
    token_menu = page.locator(".scr-pop-ops")
    assert token_menu.count() > 0, "Token menu should be visible"
    assert popup.is_visible(), "Main builder popup should still be visible"

    # Click "Add Column…" in the token menu
    add_col_btn = token_menu.locator("button", has_text="Add Column")
    assert add_col_btn.is_visible(), "'Add Column…' button should be in token menu"
    add_col_btn.click()
    page.wait_for_timeout(300)

    # Token menu should close, column picker should appear, main popup still visible
    assert token_menu.count() == 0 or not token_menu.first.is_visible(), "Token menu should close"
    assert popup.is_visible(), "Main builder popup should still be visible after column picker opens"

    # Column picker should have table and column selects
    col_picker = page.locator(".scr-pop").filter(has=page.locator(".scr-pop-sel")).last
    table_select = col_picker.locator(".scr-pop-sel").first
    column_select = col_picker.locator(".scr-pop-sel").last

    # Select a table
    table_select.select_option(index=1)  # first real table
    page.wait_for_timeout(300)

    # Select a column
    column_select.select_option(index=1)  # first real column
    page.wait_for_timeout(200)

    # Click OK
    ok_btn = col_picker.locator(".scr-bld-add")
    assert ok_btn.is_visible(), "OK button should be visible in column picker"
    ok_btn.click()
    page.wait_for_timeout(300)

    # Token chip should appear in the builder, preview should be non-empty
    token_chips = popup.locator(".scr-chp-col")
    assert token_chips.count() > 0, "A column token chip should appear after adding a column"

    preview = popup.locator(".scr-bld-preview")
    preview_text = preview.text_content() or ""
    assert preview_text != "(empty expression)", f"Preview should show compiled SQL, got: {preview_text}"

    # Now click "Add Computed Column"
    add_final_btn = popup.locator(".scr-bld-add", has_text="Add Computed Column")
    assert add_final_btn.is_visible()
    add_final_btn.click()
    page.wait_for_timeout(500)

    # Popup should close
    assert popup.count() == 0 or not popup.is_visible(), "Builder popup should close after adding"

    # A computed column bar should appear in the columns section
    comp_bars = page.locator(".scr-col-comp")
    assert comp_bars.count() > 0, "Computed column should appear in the columns section"
    assert "TestRatio" in (comp_bars.text_content() or ""), "Computed column should show the name"

    # The preview/hint should show the compiled SQL
    hints = page.locator(".scr-col-comp .scr-hint")
    if hints.count() > 0:
        hint_text = hints.first.text_content() or ""
        assert hint_text, "Computed column hint should not be empty"


def test_computed_column_prebuilt_formula(page, server):
    """Verify that adding a pre-built computed column formula works."""
    page.goto(f"{BASE_URL}/screening")
    page.wait_for_selector("#scr-cfg", state="attached", timeout=10000)
    page.wait_for_timeout(2000)

    status = page.locator("#scr-status").text_content() or ""
    if "No database" in status or "Error" in status:
        pytest.skip(f"Screening page has no DB: {status}")

    # Click "+ Add Computed"
    page.locator("#scr-add-comp").click()
    page.wait_for_timeout(500)

    popup = page.locator(".scr-pop-comp")
    assert popup.is_visible()

    # Pre-built tab should be active by default — click the first formula
    formula_btns = popup.locator(".scr-comp-btn")
    if formula_btns.count() == 0:
        # No pre-built formulas loaded — skip
        pytest.skip("No pre-built formulas available")

    first_formula = formula_btns.first
    formula_name = first_formula.text_content() or ""
    first_formula.click()
    page.wait_for_timeout(500)

    # Popup should close
    assert popup.count() == 0 or not popup.is_visible()

    # Check the computed column bar appeared
    comp_bars = page.locator(".scr-col-comp")
    assert comp_bars.count() > 0, "Computed column should appear"
    assert formula_name in (comp_bars.text_content() or ""), \
        f"Computed column name '{formula_name}' should be visible"


def test_computed_column_builder_subpopup_does_not_close_dialog(page, server):
    """
    Regression test: clicking through the token "+" menu → column picker
    flow should NOT close the main computed column dialog at any step.
    """
    page.goto(f"{BASE_URL}/screening")
    page.wait_for_selector("#scr-cfg", state="attached", timeout=10000)
    page.wait_for_timeout(2000)

    status = page.locator("#scr-status").text_content() or ""
    if "No database" in status or "Error" in status:
        pytest.skip(f"Screening page has no DB: {status}")

    page.locator("#scr-add-comp").click()
    page.wait_for_timeout(500)

    popup = page.locator(".scr-pop-comp")
    assert popup.is_visible()

    # Switch to Custom
    popup.locator(".scr-comp-tab", has_text="Custom").click()
    page.wait_for_timeout(300)

    # Click "+"
    popup.locator(".scr-chp-add").click()
    page.wait_for_timeout(300)
    assert popup.is_visible(), "Dialog should still be open after clicking +"

    # Click "Add Value…"
    token_menu = page.locator(".scr-pop-ops")
    token_menu.locator("button", has_text="Add Value").click()
    page.wait_for_timeout(300)
    assert popup.is_visible(), "Dialog should still be open after Add Value"

    # Type a value and click OK
    value_input = page.locator('.scr-pop input[placeholder="Number value…"]')
    if value_input.count() > 0:
        value_input.fill("42")
        page.locator(".scr-pop .scr-bld-add").last.click()
        page.wait_for_timeout(300)

    assert popup.is_visible(), "Dialog should still be open after adding a value"

    # Now add a column token too, just to verify multiple sub-popup interactions work
    popup.locator(".scr-chp-add").click()
    page.wait_for_timeout(200)
    assert popup.is_visible(), "Dialog should still be open after clicking + again"

    page.locator(".scr-pop-ops button", has_text="Add Column").click()
    page.wait_for_timeout(200)
    assert popup.is_visible(), "Dialog should still be open after Add Column…"

    # Pick a table and column
    col_picker = page.locator(".scr-pop").filter(has=page.locator(".scr-pop-sel")).last
    col_picker.locator(".scr-pop-sel").first.select_option(index=1)
    page.wait_for_timeout(200)
    col_picker.locator(".scr-pop-sel").last.select_option(index=1)
    page.wait_for_timeout(200)
    col_picker.locator(".scr-bld-add").click()
    page.wait_for_timeout(300)

    assert popup.is_visible(), "Dialog should still be open after picking a column"

    # Add an operator token
    popup.locator(".scr-chp-add").click()
    page.wait_for_timeout(200)
    page.locator(".scr-pop-ops button", has_text="/").click()
    page.wait_for_timeout(200)

    assert popup.is_visible(), "Dialog should still be open after adding operator"

    # Verify we have tokens
    token_chips = popup.locator(".scr-chp")
    assert token_chips.count() > 1, f"Should have multiple token chips, got {token_chips.count()}"
