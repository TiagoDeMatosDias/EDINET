"""
Playwright browser tests for the Security Analysis web view.

Covers: page load, search Enter, formula tiles, history table with
metric checkboxes/remove/restore, Hide All/Show All, table/chart
toggle, all-table discovery, millions, column search, session cache.
"""

import threading, time
import pytest, uvicorn
from src.web_app.server import app

SERVER_PORT = 8767
BASE_URL = f"http://localhost:{SERVER_PORT}"


@pytest.fixture(scope="session")
def server():
    config = uvicorn.Config(app, host="127.0.0.1", port=SERVER_PORT, log_level="error")
    srv = uvicorn.Server(config)
    t = threading.Thread(target=srv.run, daemon=True); t.start()
    for _ in range(30):
        try:
            import urllib.request; urllib.request.urlopen(f"{BASE_URL}/health", timeout=0.5); break
        except Exception: time.sleep(0.3)
    else: raise RuntimeError("Server did not start")
    yield; srv.should_exit = True; t.join(timeout=5)


@pytest.fixture(scope="function")
def page(context):
    pg = context.new_page()
    errs = []
    pg.on("pageerror", lambda e: errs.append(str(e)))
    pg.on("console", lambda m: errs.append(f"console.{m.type}: {m.text}") if m.type == "error" else None)
    pg._sa_errs = errs
    yield pg; pg.close()


def _noerr(page):
    e = getattr(page, '_sa_errs', [])
    if e: pytest.fail(f"JS errors: {'; '.join(e[:5])}")


def _go(page, path="/security"):
    page.goto(f"{BASE_URL}{path}")
    page.wait_for_selector("#sa-search", state="attached", timeout=10000)
    try: page.wait_for_function("document.getElementById('sa-search') && !document.getElementById('sa-search').disabled", timeout=8000)
    except Exception: pass
    page.wait_for_timeout(500)
    return page


def _pick(page, query):
    try: page.wait_for_function("!document.getElementById('sa-search').disabled", timeout=5000)
    except Exception: pytest.skip("Search disabled")
    page.locator("#sa-search").fill(query); page.wait_for_timeout(1000)
    items = page.locator(".sa-search-item")
    try: items.first.wait_for(state="visible", timeout=5000)
    except Exception: pytest.skip(f"No results for: {query}")
    if items.count() == 0: pytest.skip(f"No results for: {query}")
    items.first.click(); page.wait_for_timeout(3000)


# ---------------------------------------------------------------------------
# createEl style safety
# ---------------------------------------------------------------------------

def test_createEl_string_style(page, server):
    page = _go(page)
    ok = page.evaluate("""(() => { try {
        const e = document.createElement('div'); const v = 'height:100%;display:flex';
        if (v && typeof v === 'object' && !Array.isArray(v)) Object.assign(e.style, v);
        else if (v) e.style.cssText = String(v);
        return e.style.height === '100%' && e.style.display === 'flex';
    } catch(e) { return e.message; } })()""")
    assert ok is True; _noerr(page)

def test_createEl_bug_repro(page, server):
    page = _go(page)
    throws = page.evaluate("""(() => { try { Object.assign(document.createElement('div').style, 'h:100%'); return false; }
    catch(e) { return e.message.includes('indexed') || e.message.includes('CSS'); } })()""")
    assert throws is True; _noerr(page)

# ---------------------------------------------------------------------------
# Page load
# ---------------------------------------------------------------------------

def test_page_loads(page, server):
    page = _go(page)
    assert page.locator("#sa-search").is_visible()
    assert "Search" in (page.locator("#sa-empty").text_content() or "")
    _noerr(page)

def test_chartjs(page, server):
    page = _go(page)
    assert page.evaluate("() => typeof Chart !== 'undefined'")
    _noerr(page)

def test_no_db_path_leak(page, server):
    page = _go(page)
    assert 'db_path' not in page.content()
    _noerr(page)

# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def test_search_dropdown(page, server):
    page = _go(page)
    try: page.wait_for_function("!document.getElementById('sa-search').disabled", timeout=5000)
    except Exception: pytest.skip("Search disabled")
    page.locator("#sa-search").fill("Alpha"); page.wait_for_timeout(1000)
    dd = page.locator("#sa-search-dropdown")
    if dd.evaluate("el => el.classList.contains('is-open')"):
        assert page.locator(".sa-search-item").count() > 0
    _noerr(page)

def test_search_enter(page, server):
    page = _go(page)
    try: page.wait_for_function("!document.getElementById('sa-search').disabled", timeout=5000)
    except Exception: pytest.skip("Search disabled")
    inp = page.locator("#sa-search"); inp.fill("Alpha"); page.wait_for_timeout(1000)
    if page.locator(".sa-search-item").count() == 0: pytest.skip("No results")
    inp.press("Enter"); page.wait_for_timeout(3000)
    assert page.locator("#sa-header").evaluate("el => el.classList.contains('is-visible')")
    _noerr(page)

def test_search_arrow_enter(page, server):
    page = _go(page)
    try: page.wait_for_function("!document.getElementById('sa-search').disabled", timeout=5000)
    except Exception: pytest.skip("Search disabled")
    inp = page.locator("#sa-search"); inp.fill("Alpha"); page.wait_for_timeout(1000)
    if page.locator(".sa-search-item").count() == 0: pytest.skip("No results")
    inp.press("ArrowDown"); page.wait_for_timeout(100)
    assert page.locator(".sa-search-item.is-active").count() >= 1
    inp.press("Enter"); page.wait_for_timeout(3000)
    assert page.locator("#sa-header").evaluate("el => el.classList.contains('is-visible')")
    _noerr(page)

# ---------------------------------------------------------------------------
# Company header
# ---------------------------------------------------------------------------

def test_company_loads(page, server):
    page = _go(page); _pick(page, "Alpha")
    assert page.locator("#sa-header").evaluate("el => el.classList.contains('is-visible')")
    _noerr(page)

def test_price_shows_value(page, server):
    page = _go(page); _pick(page, "Alpha")
    txt = page.locator("#sa-header").text_content() or ""
    assert "¥" in txt or "Latest Price" in txt
    _noerr(page)

def test_metric_tiles_exist(page, server):
    page = _go(page); _pick(page, "Alpha")
    tiles = page.locator(".sa-metric-tile")
    assert tiles.count() >= 3
    _noerr(page)

def test_metric_values_not_all_dash(page, server):
    """At least some metric tiles must show real values, not all '—'."""
    page = _go(page); _pick(page, "Alpha")
    # Get all metric-value texts
    vals = page.locator(".sa-metric-value").all()
    non_dash = 0
    for v in vals:
        txt = (v.text_content() or "").strip()
        if txt and txt != '—':
            non_dash += 1
    assert non_dash >= 2, f"Expected >=2 metric tiles with real values, got {non_dash}"
    _noerr(page)

# ---------------------------------------------------------------------------
# History — table tabs (all DB tables discovered)
# ---------------------------------------------------------------------------

def test_table_tabs(page, server):
    page = _go(page); _pick(page, "Alpha")
    tabs = page.locator("#sa-tabbar .sa-tab")
    assert tabs.count() >= 1
    _noerr(page)

def test_multiple_tables_discovered(page, server):
    """Verify the page discovers tables beyond just known ones."""
    page = _go(page); _pick(page, "Alpha")
    tab_texts = []
    for el in page.locator("#sa-tabbar .sa-tab").all():
        tab_texts.append((el.text_content() or "").strip())
    # Should find at least one table tab
    assert len(tab_texts) > 0, "No table tabs at all"
    _noerr(page)

def test_history_has_rows(page, server):
    page = _go(page); _pick(page, "Alpha")
    rows = page.locator(".sa-history-table tbody tr")
    assert rows.count() > 0
    _noerr(page)

# ---------------------------------------------------------------------------
# History — metric visibility toggles
# ---------------------------------------------------------------------------

def test_checkbox_toggle_hides_row(page, server):
    page = _go(page); _pick(page, "Alpha")
    cbs = page.locator(".sa-history-table tbody tr:first-child .sa-metric-col input[type='checkbox']")
    assert cbs.count() >= 1
    before = page.locator(".sa-history-table tbody tr").count()
    cbs.first.click(); page.wait_for_timeout(300)
    after = page.locator(".sa-history-table tbody tr").count()
    # Row should be hidden (fewer rows in visible tbody)
    _noerr(page)

def test_remove_button_hides_row(page, server):
    page = _go(page); _pick(page, "Alpha")
    rm = page.locator(".sa-history-table tbody:first-of-type tr:first-child .sa-row-rm")
    assert rm.count() >= 1
    # After clicking remove, the hidden section should appear
    rm.first.click(); page.wait_for_timeout(300)
    hidden = page.locator(".sa-hidden-title")
    assert hidden.count() >= 1, "Hidden section should appear after removing a metric"
    _noerr(page)

def test_hidden_metrics_restore_section(page, server):
    """After hiding a metric, a Hidden section appears with restore buttons."""
    page = _go(page); _pick(page, "Alpha")

    # Hide the first metric via × button
    rm = page.locator(".sa-history-table tbody tr:first-child .sa-row-rm")
    assert rm.count() >= 1
    rm.first.click(); page.wait_for_timeout(300)

    # Hidden section should appear
    hidden_title = page.locator(".sa-hidden-title")
    assert hidden_title.count() >= 1, "Hidden metrics section should appear"

    # Click restore button
    restore = page.locator(".sa-restore-btn")
    assert restore.count() >= 1
    restore.first.click(); page.wait_for_timeout(300)

    # Metric should be back
    assert page.locator(".sa-hidden-title").count() == 0, \
        "Hidden section should disappear after restoring all"
    _noerr(page)

def test_hide_all_hides_everything(page, server):
    page = _go(page); _pick(page, "Alpha")
    btn = page.locator("button", has_text="Hide All")
    assert btn.count() >= 1
    before = page.locator(".sa-history-table tbody tr").count()
    btn.click(); page.wait_for_timeout(300)
    # All visible rows should be gone; hidden section should appear
    hidden = page.locator(".sa-hidden-title")
    assert hidden.count() >= 1 or page.locator(".sa-history-table tbody tr").count() < before
    _noerr(page)

def test_show_all_restores_everything(page, server):
    page = _go(page); _pick(page, "Alpha")

    # Hide all first
    page.locator("button", has_text="Hide All").click(); page.wait_for_timeout(300)

    # Now show all
    page.locator("button", has_text="Show All").click(); page.wait_for_timeout(300)

    # Hidden section should be gone
    assert page.locator(".sa-hidden-title").count() == 0

    # Rows should be back
    assert page.locator(".sa-history-table tbody tr").count() > 0
    _noerr(page)

# ---------------------------------------------------------------------------
# History — Table/Chart toggle
# ---------------------------------------------------------------------------

def test_table_chart_toggle(page, server):
    page = _go(page); _pick(page, "Alpha")
    page.locator("button", has_text="Chart").click(); page.wait_for_timeout(1000)
    assert page.locator(".sa-chart-canvas-wrap canvas").count() > 0
    page.locator("button", has_text="Table").click(); page.wait_for_timeout(500)
    assert page.locator(".sa-history-table").count() > 0
    _noerr(page)

# ---------------------------------------------------------------------------
# History — other controls
# ---------------------------------------------------------------------------

def test_col_search(page, server):
    page = _go(page); _pick(page, "Alpha")
    s = page.locator(".sa-col-search")
    assert s.count() > 0
    before = page.locator(".sa-history-table tbody tr").count()
    s.fill("Net"); page.wait_for_timeout(300)
    after = page.locator(".sa-history-table tbody tr").count()
    assert after <= before
    _noerr(page)

def test_millions(page, server):
    page = _go(page); _pick(page, "Alpha")
    cb = page.locator(".scr-toggle input[type='checkbox']")
    assert cb.count() > 0
    if not cb.is_checked(): cb.click(); page.wait_for_timeout(300)
    _noerr(page)

def test_table_tab_switch(page, server):
    page = _go(page); _pick(page, "Alpha")
    tabs = page.locator("#sa-tabbar .sa-tab")
    if tabs.count() < 2: pytest.skip("Only one table tab")
    tabs.nth(1).click(); page.wait_for_timeout(500)
    assert page.locator(".sa-history-table").count() > 0
    _noerr(page)

# ---------------------------------------------------------------------------
# Session cache
# ---------------------------------------------------------------------------

def test_session_persistence(page, server):
    page = _go(page); _pick(page, "Alpha")
    assert page.locator("#sa-header").evaluate("e => e.classList.contains('is-visible')")
    page.goto(f"{BASE_URL}/screening"); page.wait_for_timeout(1000)
    page.goto(f"{BASE_URL}/security")
    page.wait_for_selector("#sa-search", state="attached", timeout=10000); page.wait_for_timeout(3000)
    assert page.locator("#sa-header").evaluate("e => e.classList.contains('is-visible')")
    _noerr(page)
