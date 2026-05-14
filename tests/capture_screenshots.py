"""Capture web UI screenshots for README.

Captures all views with live data where possible:
  - Dashboard: empty state (pipeline summary)
  - Orchestrator: empty pipeline builder
  - Screening: with criteria and results (Transportation Equipments companies)
  - Backtesting: empty state (mode selector)
  - Security Analysis: with a loaded company (TOYOTA INDUSTRIES)
"""

import json
import os
import sys
import threading
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import uvicorn
from src.web_app.server import app

PORT = 8765
BASE = f"http://127.0.0.1:{PORT}"

# ---------------------------------------------------------------------------
# Start server
# ---------------------------------------------------------------------------
config = uvicorn.Config(app, host="127.0.0.1", port=PORT, log_level="error")
server = uvicorn.Server(config)
t = threading.Thread(target=server.run, daemon=True)
t.start()

for _ in range(30):
    try:
        urllib.request.urlopen(f"{BASE}/health", timeout=0.5)
        break
    except Exception:
        time.sleep(0.3)
else:
    print("Server did not start")
    sys.exit(1)

print(f"Server running at {BASE}")

from playwright.sync_api import sync_playwright

OUT = os.path.join(ROOT, "docs", "images")
os.makedirs(OUT, exist_ok=True)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def api_json(path):
    """Call the local server API and return parsed JSON."""
    r = urllib.request.urlopen(f"{BASE}{path}", timeout=10)
    return json.loads(r.read())


def capture(page, url, fname, *, wait_ms=1500):
    """Navigate to url, wait, screenshot."""
    filepath = os.path.join(OUT, fname)
    print(f"  Capturing {url} -> {fname}")
    page.goto(url, wait_until="networkidle")
    page.wait_for_timeout(wait_ms)
    page.screenshot(path=filepath)
    size = os.path.getsize(filepath)
    print(f"    saved ({size} bytes)")
    return filepath


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1280, "height": 800})

    # ---- Dashboard (empty state) ----
    capture(page, f"{BASE}/", "web-dashboard.png")

    # ---- Orchestrator (empty pipeline) ----
    capture(page, f"{BASE}/orchestrator", "web-orchestrator.png")

    # ---- Backtesting (empty state) ----
    capture(page, f"{BASE}/backtesting", "web-backtesting.png")

    # ----
    # Screening — run a screening with criteria and capture results
    # ----
    print("\n--- Screening (with results) ---")

    # Get db path and metrics from server
    db_path = api_json("/api/screening/db-path")["db_path"]
    metrics = api_json(f"/api/screening/metrics?db_path={db_path}")["tables"]
    formulas = api_json("/api/screening/formulas")["formulas"]
    print(f"  DB: {db_path}, {len(metrics)} tables, {len(formulas)} formulas")

    # Build a screening request: filter by industry, show key columns
    screening_body = {
        "db_path": db_path,
        "criteria": [
            {
                "table": "CompanyInfo",
                "column": "Company_Industry",
                "operator": "=",
                "value": "Transportation Equipments",
                "field_type": "text",
                "comparison_mode": "fixed",
            }
        ],
        "columns": [
            "CompanyInfo.EdinetCode",
            "CompanyInfo.Company_Ticker",
            "CompanyInfo.Company_Name",
            "CompanyInfo.Company_Industry",
        ],
        "computed_columns": [],
        "sort_by": "CompanyInfo.Company_Ticker",
        "sort_order": "ASC",
    }

    req = urllib.request.Request(
        f"{BASE}/api/screening/run",
        data=json.dumps(screening_body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    resp = json.loads(urllib.request.urlopen(req, timeout=30).read())
    print(f"  Screening returned {resp.get('row_count', 0)} results")

    # Navigate to screening, inject state into sessionStorage, reload
    page.goto(f"{BASE}/screening", wait_until="networkidle")
    page.wait_for_timeout(1000)

    # Build sessionStorage payload matching restoreCachedState() format
    cache_payload = {
        "dbPath": db_path,
        "availableMetrics": metrics,
        "screeningDate": "",
        "criteria": [
            {
                "id": "auto-1",
                "table": "CompanyInfo",
                "column": "Company_Industry",
                "operator": "=",
                "value": "Transportation Equipments",
                "field_type": "text",
                "comparison_mode": "fixed",
            }
        ],
        "columns": [
            {"id": "auto-col-1", "kind": "col", "ref": "CompanyInfo.EdinetCode"},
            {"id": "auto-col-2", "kind": "col", "ref": "CompanyInfo.Company_Ticker"},
            {"id": "auto-col-3", "kind": "col", "ref": "CompanyInfo.Company_Name"},
            {"id": "auto-col-4", "kind": "col", "ref": "CompanyInfo.Company_Industry"},
        ],
        "sortBy": "CompanyInfo.Company_Ticker",
        "sortOrder": "ASC",
        "formattedValues": True,
        "results": {
            "columns": resp.get("columns", []),
            "rows": resp.get("rows", []),
            "row_count": resp.get("row_count", 0),
        },
        "sqlDisplay": resp.get("sql_display", ""),
        "_nextId": 100,
        "prebuiltFormulas": formulas,
    }

    page.evaluate(
        """(payload) => {
        sessionStorage.setItem('screening_state', JSON.stringify(payload));
    }""",
        cache_payload,
    )

    # Reload so the page picks up the cached state
    page.goto(f"{BASE}/screening", wait_until="networkidle")
    page.wait_for_timeout(2500)  # Wait for restore + render

    filepath = os.path.join(OUT, "web-screening.png")
    page.screenshot(path=filepath)
    print(f"    saved ({os.path.getsize(filepath)} bytes)")

    # ----
    # Security Analysis — load a company and capture results
    # ----
    print("\n--- Security Analysis (with company data) ---")

    # Find a company with financial data (try TOYOTA INDUSTRIES first)
    search = api_json("/api/security/search?q=TOYOTA%20INDUSTRIES&limit=3")
    if not search.get("results"):
        # Fallback: search for any company
        search = api_json("/api/security/search?q=TOYOTA&limit=3")

    if search.get("results"):
        code = search["results"][0]["edinet_code"]
        name = search["results"][0].get("company_name", code)
        print(f"  Loading company: {name} ({code})")

        # Navigate with edinet_code param — security.js auto-loads it
        page.goto(
            f"{BASE}/security?edinet_code={code}",
            wait_until="networkidle",
        )
        # Wait for company header to appear (the loading state resolves)
        page.wait_for_timeout(3000)

        # Wait for the header to become visible
        try:
            page.wait_for_selector("#sa-header.is-visible", timeout=8000)
            page.wait_for_timeout(1000)  # Let charts/tables render
        except Exception:
            print("  Warning: company header not visible, capturing anyway")

        filepath = os.path.join(OUT, "web-security-analysis.png")
        page.screenshot(path=filepath)
        print(f"    saved ({os.path.getsize(filepath)} bytes)")
    else:
        print("  No results from search, capturing empty state")
        capture(
            page, f"{BASE}/security", "web-security-analysis.png", wait_ms=2000
        )

    browser.close()

print("\nAll screenshots captured")
server.should_exit = True
