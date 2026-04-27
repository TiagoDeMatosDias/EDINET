from bs4 import BeautifulSoup
from fastapi.testclient import TestClient

from src.web_app.server import app


client = TestClient(app)


def _soup_for(path: str) -> BeautifulSoup:
    response = client.get(path)
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")
    return BeautifulSoup(response.text, "html.parser")


def test_main_page_has_topbar_with_all_tabs() -> None:
    soup = _soup_for("/")
    # Topbar is static HTML; tabs and brand are present before JS runs.
    assert soup.select_one("header.topbar") is not None
    tab_labels = [btn.get_text(strip=True) for btn in soup.select("nav.tabs button.tab")]
    assert tab_labels == ["Main", "Orchestrator", "Screening", "Security Analysis"]
    assert soup.select_one("script[src*='pages/main.js']") is not None


def test_main_page_contains_jobs_region() -> None:
    soup = _soup_for("/")
    main_panel = soup.select_one("section[data-view-panel='main']")
    assert main_panel is not None
    assert main_panel.select_one("#jobs-table") is not None
    assert main_panel.select_one("#main-metrics") is not None


def test_orchestrator_page_contains_builder_regions() -> None:
    soup = _soup_for("/orchestrator")
    orch_panel = soup.select_one("section[data-view-panel='orchestrator']")
    assert orch_panel is not None
    assert orch_panel.select_one("#step-library") is not None
    assert orch_panel.select_one("#pipeline-list") is not None
    assert orch_panel.select_one("#inspector-body") is not None


def test_screening_page_exists_with_correct_panel() -> None:
    soup = _soup_for("/screening")
    assert soup.select_one("section[data-view-panel='screening']") is not None


def test_security_page_exists_with_correct_panel() -> None:
    soup = _soup_for("/security")
    assert soup.select_one("section[data-view-panel='security']") is not None


def test_each_page_loads_correct_js_module() -> None:
    cases = [
        ("/",             "pages/main.js"),
        ("/orchestrator", "pages/orchestrator.js"),
        ("/screening",    "pages/screening.js"),
        ("/security",     "pages/security.js"),
    ]
    for path, expected_script in cases:
        soup = _soup_for(path)
        scripts = [s.get("src", "") for s in soup.find_all("script")]
        assert any(expected_script in s for s in scripts), \
            f"{path} does not load {expected_script}. Scripts found: {scripts}"


def test_each_page_has_console_log() -> None:
    for path in ("/", "/orchestrator", "/screening", "/security"):
        soup = _soup_for(path)
        assert soup.find(id="console-log") is not None, \
            f"{path} missing #console-log"

