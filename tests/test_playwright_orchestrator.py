"""
Playwright browser tests for the Orchestrator view.

Coverage:
- Console footer exists only on /orchestrator, not on other pages
- Console Hide/Show toggle works
- Setup save → reload page → setup is restored (no reset)
"""

import threading
import time

import pytest
import uvicorn

from src.web_app.server import app

SERVER_PORT = 8765
BASE_URL = f"http://localhost:{SERVER_PORT}"


@pytest.fixture(scope="session")
def server():
    """Start the FastAPI server in a background thread for the test session."""

    config = uvicorn.Config(app, host="127.0.0.1", port=SERVER_PORT, log_level="error")
    server_instance = uvicorn.Server(config)

    thread = threading.Thread(target=server_instance.run, daemon=True)
    thread.start()

    # Wait for the server to be ready
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
def page_with_state(context):
    """Create a fresh page with localStorage isolated for each test."""
    page = context.new_page()
    yield page
    page.close()


# ---------------------------------------------------------------------------
# Console visibility tests
# ---------------------------------------------------------------------------


def test_console_only_on_orchestrator(page, server):
    """Console footer element exists only on /orchestrator."""
    page.goto(f"{BASE_URL}/orchestrator")
    page.wait_for_selector("#console-log", state="attached", timeout=5000)
    assert page.locator("#console-log").is_visible()
    assert page.locator("#console-toggle").is_visible()

    # Other pages must not have the console log element
    for path in ["/", "/screening", "/security"]:
        page.goto(f"{BASE_URL}{path}")
        assert page.locator("#console-log").count() == 0, \
            f"{path} should not have #console-log"
        assert page.locator("#console-toggle").count() == 0, \
            f"{path} should not have #console-toggle"


def test_console_hide_button_toggles_console(page, server):
    """Clicking 'Hide' in the console footer collapses it; clicking 'Show' restores it."""
    page.goto(f"{BASE_URL}/orchestrator")
    page.wait_for_selector("#console-toggle", state="attached", timeout=5000)

    # Initially the console should be visible (not collapsed)
    assert not page.locator("body").evaluate(
        "el => el.classList.contains('console-collapsed')"
    )
    toggle_btn = page.locator("#console-toggle")
    console_log = page.locator("#console-log")

    # Console log should be visible
    assert console_log.is_visible()

    # Click Hide
    toggle_btn.click()
    page.wait_for_timeout(300)

    # Body should have console-collapsed class, console-log hidden
    assert page.locator("body").evaluate(
        "el => el.classList.contains('console-collapsed')"
    )
    assert not console_log.is_visible()
    assert toggle_btn.text_content().strip() == "Show"

    # Click Show
    toggle_btn.click()
    page.wait_for_timeout(300)

    # Body should NOT have console-collapsed class, console-log visible again
    assert not page.locator("body").evaluate(
        "el => el.classList.contains('console-collapsed')"
    )
    assert console_log.is_visible()
    assert toggle_btn.text_content().strip() == "Hide"


# ---------------------------------------------------------------------------
# Setup save/load tests
# ---------------------------------------------------------------------------


def test_setup_save_and_restore_on_reload(page, page_with_state, server):
    """
    Save a setup via the orchestrator, then reload the page and verify the
    pipeline is restored (not reset to empty).
    """
    page.goto(f"{BASE_URL}/orchestrator")
    page.wait_for_selector("#step-library", state="attached", timeout=5000)
    page.wait_for_selector("#pipeline-list", state="attached", timeout=5000)

    # Give JS a moment to bootstrap fully and load steps from /api/steps
    page.wait_for_timeout(1000)

    # Name the setup uniquely so we can find it later
    setup_name = "playwright-test-setup"
    page.locator("#setup-name").fill(setup_name)

    # Add a step to the pipeline by clicking a step in the library
    step_items = page.locator(".step-item")
    step_count = step_items.count()
    if step_count > 0:
        step_items.first.click()
        page.wait_for_timeout(300)

    # Verify pipeline list has at least one step
    pipeline_steps = page.locator(".pipeline-step")
    assert pipeline_steps.count() > 0, "Pipeline should have at least one step after adding"

    # Save the setup
    page.locator("#save-setup-btn").click()
    page.wait_for_timeout(500)

    # Reload the page
    page.goto(f"{BASE_URL}/orchestrator")
    page.wait_for_selector("#pipeline-list", state="attached", timeout=5000)
    page.wait_for_timeout(1500)

    # Verify the pipeline is restored (not empty)
    pipeline_steps = page.locator(".pipeline-step")
    assert pipeline_steps.count() > 0, \
        "Pipeline should be restored after page reload, not reset to empty"

    # Verify setup name is restored
    setup_input = page.locator("#setup-name")
    assert setup_input.input_value() == setup_name, \
        f"Setup name should be '{setup_name}' after reload"


def test_new_setup_clears_pipeline(page, server):
    """Clicking 'New' should clear the pipeline."""
    page.goto(f"{BASE_URL}/orchestrator")
    page.wait_for_selector("#step-library", state="attached", timeout=5000)
    page.wait_for_timeout(1000)

    # Add a step
    step_items = page.locator(".step-item")
    if step_items.count() > 0:
        step_items.first.click()
        page.wait_for_timeout(300)

    # Verify something is in the pipeline
    assert page.locator(".pipeline-step").count() > 0

    # Click New
    page.locator("#new-setup-btn").click()
    page.wait_for_timeout(500)

    # Pipeline should be empty
    assert page.locator(".pipeline-step").count() == 0, \
        "Pipeline should be empty after clicking New"


def test_load_menu_shows_saved_setups(page, server):
    """The Load popup menu should list saved setups and loading one should populate the pipeline."""
    page.goto(f"{BASE_URL}/orchestrator")
    page.wait_for_selector("#step-library", state="attached", timeout=5000)
    page.wait_for_timeout(1000)

    # First, save a setup so we have something to load
    setup_name_load = "load-menu-test"
    page.locator("#setup-name").fill(setup_name_load)

    step_items = page.locator(".step-item")
    if step_items.count() > 0:
        step_items.first.click()
        page.wait_for_timeout(300)

    page.locator("#save-setup-btn").click()
    page.wait_for_timeout(500)

    # Now create a New setup (empty pipeline)
    page.locator("#new-setup-btn").click()
    page.wait_for_timeout(500)
    assert page.locator(".pipeline-step").count() == 0

    # Open the Load menu
    page.locator("#load-setup-btn").click()
    page.wait_for_selector(".popup-menu", state="attached", timeout=3000)

    # Click the saved setup in the popup
    popup_items = page.locator(".popup-menu button")
    popup_items.first.click()
    page.wait_for_timeout(500)

    # Pipeline should be restored
    assert page.locator(".pipeline-step").count() > 0, \
        "Pipeline should be populated after loading from Load menu"
