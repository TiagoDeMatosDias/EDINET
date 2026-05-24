"""Playwright tests for the Portfolio view tab navigation.

Verifies that clicking each tab activates the corresponding panel.
"""

import threading, time
import pytest, uvicorn
from src.web_app.server import app

SERVER_PORT = 8770
BASE_URL = f"http://localhost:{SERVER_PORT}"


@pytest.fixture(scope="session")
def server():
    config = uvicorn.Config(app, host="127.0.0.1", port=SERVER_PORT, log_level="error")
    srv = uvicorn.Server(config)
    t = threading.Thread(target=srv.run, daemon=True)
    t.start()
    for _ in range(30):
        try:
            import urllib.request
            urllib.request.urlopen(f"{BASE_URL}/health", timeout=0.5)
            break
        except Exception:
            time.sleep(0.3)
    else:
        raise RuntimeError("Server did not start")
    yield
    srv.should_exit = True
    t.join(timeout=5)


@pytest.fixture(scope="function")
def page(context):
    pg = context.new_page()
    errs = []
    pg.on("pageerror", lambda e: errs.append(str(e)))
    pg.on("console", lambda m: errs.append(f"console.{m.type}: {m.text}") if m.type == "error" else None)
    pg._pf_errs = errs
    yield pg
    pg.close()


def _go(page):
    page.goto(f"{BASE_URL}/portfolio")
    page.wait_for_selector("#pf-tabs", state="attached", timeout=10000)
    page.wait_for_timeout(500)
    return page


def _assert_no_js_errors(page):
    errs = getattr(page, '_pf_errs', [])
    if errs:
        pytest.fail(f"JS errors: {'; '.join(errs[:5])}")


def test_tab_clicks_activate_panels(page, server):
    page = _go(page)
    # Ensure initial active tab is upload
    active = page.locator('#pf-tabs .tab-btn.is-active')
    assert active.count() == 1
    assert active.get_attribute('data-tab') == 'upload'

    tabs = ['holdings', 'transactions', 'charts', 'performance', 'upload']
    for tab in tabs:
        btn = page.locator(f'#pf-tabs .tab-btn[data-tab="{tab}"]')
        assert btn.count() == 1, f"Tab button for {tab} missing"
        btn.click()
        page.wait_for_timeout(250)
        # active button should now match
        act = page.locator('#pf-tabs .tab-btn.is-active')
        assert act.get_attribute('data-tab') == tab
        # corresponding panel should be active
        panel = page.locator(f'.tab-panel[data-panel="{tab}"]')
        assert panel.count() == 1
        assert panel.evaluate('el => el.classList.contains("is-active")')

    _assert_no_js_errors(page)
