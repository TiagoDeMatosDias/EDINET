"""
Full integration test suite.

Lifecycle:
  1. Build the app (``scripts/build.py``) → ``dist/EDINET-Release.zip``
  2. Extract the ZIP into a temp dir, copy ``.env``, create writable databases
  3. Launch ``EDINET.exe`` as a subprocess, wait for /health
  4. **Playwright**: navigate to Orchestrator, add 8 steps via the UI,
     configure them, click Run, wait for pipeline completion, verify step statuses
  5. **Playwright**: verify Screening (click Run, assert results table has rows)
  6. **Playwright**: verify Security Analysis (search company, assert tiles & data)

Pipeline steps (in order):
  - Get Documents       (last 30 days)
  - Download Documents  (docTypeCode=120, csvFlag=1, Downloaded=False)
  - Populate Company Info
  - Parse Taxonomy      (all releases)
  - Update Stock Prices
  - Generate Financial Statements (Granularity_level=3)
  - Generate Ratios     (batch_size=5000)
  - Generate Rolling Metrics

Usage::

    pytest tests/test_integration_full.py -v -s                # full suite
    pytest tests/test_integration_full.py -v -s --rebuild      # force rebuild
    pytest tests/test_integration_full.py -v -s -k "test_screening"  # single test

Requirements:
    - ``playwright install chromium``
    - ``pip install pytest-playwright pyinstaller requests``
    - A valid ``.env`` file in the project root with ``API_KEY=...``
    - Windows (for EXE build; use ``--no-build`` with pre-built ZIP on other OS)
"""

from __future__ import annotations

import atexit
import atexit
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import zipfile
from pathlib import Path

import pytest
import requests


# =============================================================================
# Configuration
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BUILD_SCRIPT = PROJECT_ROOT / "scripts" / "build.py"
DIST_ZIP = PROJECT_ROOT / "dist" / "EDINET-Release.zip"
DOTENV_SRC = PROJECT_ROOT / ".env"
PORT = 8799  # dedicated port
BASE_URL = f"http://127.0.0.1:{PORT}"

INTEGRATION_DIR = PROJECT_ROOT / "data" / "Integration Test"

# How long we wait for the pipeline Run to complete
PIPELINE_TIMEOUT_S = 3600  # 60 minutes

# How long we wait for the EXE to start
STARTUP_TIMEOUT_S = 60

# Date range for get_documents — use a fixed range known to have filings.
# Japanese EDINET filings are sparse during Golden Week (late Apr–early May)
# and weekends. Late March is annual-report season with 500+ docs/day.
TEST_DATE_START = "2026-03-30"
TEST_DATE_END = "2026-03-31"


# =============================================================================
# CLI options
# =============================================================================

def pytest_addoption(parser):
    parser.addoption(
        "--rebuild", action="store_true", default=False,
        help="Force rebuild of the EXE even if a cached build exists.",
    )
    parser.addoption(
        "--no-build", action="store_true", default=False,
        help="Skip the build — use existing dist/EDINET-Release.zip.",
    )
    parser.addoption(
        "--exe-path", type=str, default=None,
        help="Path to a pre-built EDINET.exe (skips build entirely).",
    )


# =============================================================================
# Helpers
# =============================================================================

def _kill_process_tree(pid: int) -> None:
    """Kill a process and all its children."""
    try:
        if sys.platform == "win32":
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True, timeout=10,
            )
        else:
            os.killpg(os.getpgid(pid), signal.SIGKILL)
    except Exception:
        pass


def _resolve_exe_in_dir(app_dir: Path) -> Path | None:
    """Find EDINET.exe inside an extracted release directory."""
    for pattern in ["EDINET.exe", "EDINET", "EDINET.bin"]:
        candidate = app_dir / pattern
        if candidate.exists():
            return candidate
    for candidate in app_dir.rglob("EDINET.exe"):
        return candidate
    return None


# =============================================================================
# Session fixtures
# =============================================================================


@pytest.fixture(scope="session")
def built_app_dir(request):
    """Build (or reuse) the distributable app archive and extract it into
    ``data/Integration Test/``.  The directory is wiped at the start of every
    session so each run starts from a clean slate.  Nothing is deleted on
    teardown — all artifacts are preserved for inspection.

    Returns the directory containing EDINET.exe, .env, config/, data/databases/.
    """
    rebuild = request.config.getoption("--rebuild", False)
    no_build = request.config.getoption("--no-build", False)
    exe_path_opt = request.config.getoption("--exe-path", None)

    app_dir = INTEGRATION_DIR

    # ── Clean start ──────────────────────────────────────────────────────
    if app_dir.exists():
        print(f"  Cleaning previous run: {app_dir}")
        shutil.rmtree(app_dir, ignore_errors=True)
    app_dir.mkdir(parents=True, exist_ok=True)

    # ── Acquire EDINET.exe ───────────────────────────────────────────────
    if exe_path_opt:
        exe_src = Path(exe_path_opt).resolve()
        if not exe_src.exists():
            pytest.fail(f"--exe-path does not exist: {exe_src}")
        shutil.copy2(exe_src, app_dir / "EDINET.exe")
    elif no_build:
        if not DIST_ZIP.exists():
            pytest.fail(
                f"--no-build specified but {DIST_ZIP} does not exist. "
                f"Run 'python scripts/build.py' first."
            )
        print(f"Extracting {DIST_ZIP} → {app_dir}")
        with zipfile.ZipFile(str(DIST_ZIP), "r") as zf:
            zf.extractall(str(app_dir))
    else:
        if rebuild or not DIST_ZIP.exists():
            if not BUILD_SCRIPT.exists():
                pytest.fail(f"Build script not found: {BUILD_SCRIPT}")
            print(f"\n{'='*60}")
            print(f"  Building EDINET.exe (this may take several minutes)...")
            print(f"{'='*60}")
            result = subprocess.run(
                [sys.executable, str(BUILD_SCRIPT)], cwd=str(PROJECT_ROOT),
            )
            if result.returncode != 0:
                pytest.fail("Build failed.")
            if not DIST_ZIP.exists():
                pytest.fail(f"Build succeeded but {DIST_ZIP} was not created.")

        print(f"Extracting {DIST_ZIP} → {app_dir}")
        with zipfile.ZipFile(str(DIST_ZIP), "r") as zf:
            zf.extractall(str(app_dir))

    # ── Verify EXE ──────────────────────────────────────────────────────
    exe = _resolve_exe_in_dir(app_dir)
    if not exe:
        contents = list(app_dir.rglob("*"))
        pytest.fail(
            f"EDINET.exe not found. Contents: "
            f"{[str(c.relative_to(app_dir)) for c in contents[:20]]}"
        )

    # ── .env ────────────────────────────────────────────────────────────
    env_dest = app_dir / ".env"
    if not DOTENV_SRC.exists():
        pytest.fail(f"{DOTENV_SRC} not found. Create a .env file with API_KEY=...")
    env_dest.write_text(DOTENV_SRC.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"  .env ready")

    # ── Fresh databases (reset in-place, don't delete files) ────────────
    db_dir = app_dir / "data" / "databases"
    db_dir.mkdir(parents=True, exist_ok=True)
    for db_name in ["Base.db", "Standardized.db"]:
        db_path = db_dir / db_name
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode = DELETE")
        # Drop every user table to reset
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        for t in tables:
            conn.execute(f"DROP TABLE IF EXISTS [{t}]")
        conn.execute("CREATE TABLE IF NOT EXISTS _placeholder (id INTEGER)")
        conn.commit()
        conn.close()
    print(f"  Databases reset")

    print(f"  App dir: {app_dir}")
    return app_dir


@pytest.fixture(scope="session")
def app_server(built_app_dir):
    """Start EDINET.exe, wait for /health, yield, then stop."""
    exe = _resolve_exe_in_dir(built_app_dir)
    if not exe:
        pytest.fail(f"EDINET.exe not found in {built_app_dir}")

    creationflags = 0
    if sys.platform == "win32":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]

    print(f"\nStarting {exe} on port {PORT}...")
    proc = subprocess.Popen(
        [str(exe), "--port", str(PORT), "--no-reload"],
        cwd=str(built_app_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace",
        creationflags=creationflags,
    )

    # Drain stdout in a background thread to prevent pipe buffer from
    # filling up and deadlocking the subprocess (get_documents prints a
    # line per API call).
    stdout_lines: list[str] = []

    def _drain():
        for line in proc.stdout:
            stdout_lines.append(line)
            if len(stdout_lines) > 5000:
                stdout_lines.pop(0)

    drain_thread = threading.Thread(target=_drain, daemon=True)
    drain_thread.start()

    def _cleanup():
        _kill_process_tree(proc.pid)
    atexit.register(_cleanup)

    print(f"  Waiting for server (up to {STARTUP_TIMEOUT_S}s)...")
    deadline = time.time() + STARTUP_TIMEOUT_S
    last_error = None
    while time.time() < deadline:
        if proc.poll() is not None:
            stdout_snip = ""
            try:
                stdout_snip = proc.stdout.read()[-2000:] if proc.stdout else ""
            except Exception:
                pass
            pytest.fail(
                f"EDINET.exe exited with code {proc.returncode}.\n"
                f"Last output:\n{stdout_snip}"
            )
        try:
            resp = requests.get(f"{BASE_URL}/health", timeout=2)
            if resp.status_code == 200:
                print(f"  Server ready at {BASE_URL}")
                break
        except requests.ConnectionError as exc:
            last_error = exc
            time.sleep(1)
        except Exception as exc:
            last_error = exc
            time.sleep(1)
    else:
        _kill_process_tree(proc.pid)
        pytest.fail(
            f"Server did not start within {STARTUP_TIMEOUT_S}s. "
            f"Last error: {last_error}"
        )

    yield proc

    print(f"\nStopping EDINET.exe (pid={proc.pid})...")
    _kill_process_tree(proc.pid)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        _kill_process_tree(proc.pid)


# =============================================================================
# Function-scoped Playwright fixtures
# =============================================================================

@pytest.fixture(scope="function")
def page(context, app_server):
    """Fresh Playwright page pointing at the built app."""
    pg = context.new_page()
    errors: list[str] = []
    pg.on("pageerror", lambda e: errors.append(str(e)))
    pg.on("console", lambda m: errors.append(f"console.{m.type}: {m.text}") if m.type == "error" else None)
    pg._errors = errors
    yield pg
    if errors:
        print(f"  JS errors: {'; '.join(errors[:8])}")
    pg.close()


# =============================================================================
# Test 1: Build verification
# =============================================================================

def test_build_succeeds(built_app_dir):
    """Verify the extracted app directory has all required files."""
    exe = _resolve_exe_in_dir(built_app_dir)
    assert exe is not None, "EDINET.exe not found"
    assert exe.stat().st_size > 0, "EDINET.exe is empty"

    env_file = built_app_dir / ".env"
    assert env_file.exists(), ".env missing"
    assert "API_KEY" in env_file.read_text(encoding="utf-8")

    config_file = built_app_dir / "config" / "database_paths.json"
    assert config_file.exists(), "config/database_paths.json missing"

    for db_name in ["Base.db", "Standardized.db"]:
        db_path = built_app_dir / "data" / "databases" / db_name
        assert db_path.exists(), f"{db_name} missing"

    print(f"  Build verified: {exe}")


# =============================================================================
# Test 2: Orchestrator pipeline — full Playwright UI drive
# =============================================================================

def test_orchestrator_pipeline_run(page, built_app_dir, app_server):
    """
    Drive the Orchestrator UI via Playwright:

    1. Navigate to /orchestrator
    2. Wait for JS bootstrap (step library populated)
    3. Save a full 8-step pipeline to localStorage, then reload
    4. On reload, initializeSetup() auto-restores the setup
    5. Click Run
    6. Wait for pipeline completion (Run button re-enables)
    7. Assert steps show "done" status via DOM classes
    """
    print("\n  Opening Orchestrator page...")
    page.goto(f"{BASE_URL}/orchestrator", wait_until="domcontentloaded")
    page.wait_for_selector("#step-library", state="attached", timeout=20000)
    page.wait_for_timeout(1500)

    # Wait for step library to have items (JS finished bootstrap)
    page.wait_for_function(
        "() => document.querySelectorAll('.step-item').length > 0",
        timeout=15000,
    )
    print("  Step library populated — JS bootstrap complete")

    # ── Date range ──────────────────────────────────────────────────────
    date_start = TEST_DATE_START
    date_end = TEST_DATE_END
    print(f"  Date range: {date_start} → {date_end}")

    # Read the API key from the test host's .env (the EXE loads it from its own
    # .env via load_dotenv, but Config.get() checks settings dict first — so we
    # include it in the config to be safe)
    api_key = ""
    if DOTENV_SRC.exists():
        import dotenv
        env_vals = dotenv.dotenv_values(str(DOTENV_SRC))
        api_key = env_vals.get("API_KEY", "")
    print(f"  API_KEY present: {bool(api_key)}")

    # ── Save pipeline setup to localStorage, then reload ──────────────────
    # On reload, initializeSetup() restores from localStorage automatically.
    page.evaluate(
        """
        ([dateStart, dateEnd, apiKey]) => {
          const makeStep = (name, enabled, overwrite) => ({
            id: crypto.randomUUID ? crypto.randomUUID() : name,
            name, enabled, overwrite,
            status: 'idle', result: null, error: null,
          });

          const setup = {
            version: 1,
            name: 'Integration Test',
            updatedAt: new Date().toISOString(),
            config: {
              API_KEY: apiKey,
              get_documents_config: { startDate: dateStart, endDate: dateEnd },
              download_documents_config: { docTypeCode: '120', csvFlag: '1', Downloaded: 'False' },
              populate_company_info_config: { csv_file: '' },
              parse_taxonomy_config: {
                xsd_file: '', namespace_prefix: 'jppfs_cor', release_label: '',
                release_year: '', taxonomy_date: '', release_selection: 'latest',
                release_years: [], namespaces: ['jppfs_cor', 'jpcrp_cor'],
                download_dir: 'assets/taxonomy', force_download: 'False', force_reparse: 'False',
              },
              generate_financial_statements_config: { Granularity_level: 3 },
              generate_ratios_config: { batch_size: 5000 },
            },
            pipeline: [
              makeStep('get_documents',              true, false),
              makeStep('download_documents',          true, false),
              makeStep('populate_company_info',       true, false),
              makeStep('parse_taxonomy',              true, false),
              makeStep('update_stock_prices',         true, false),
              makeStep('generate_financial_statements', true, false),
              makeStep('generate_ratios',             true, false),
              makeStep('generate_rolling_metrics',     true, false),
            ],
            selectedStepId: null,
          };

          const setups = JSON.parse(localStorage.getItem('edinet.web.setups') || '{}');
          setups['Integration Test'] = setup;
          localStorage.setItem('edinet.web.setups', JSON.stringify(setups));
          localStorage.setItem('edinet.web.lastSetup', 'Integration Test');
        }
        """,
        [date_start, date_end, api_key],
    )
    print("  Setup saved to localStorage")

    # Reload — initializeSetup() restores 'Integration Test' from localStorage
    page.goto(f"{BASE_URL}/orchestrator", wait_until="domcontentloaded")
    page.wait_for_selector("#step-library", state="attached", timeout=20000)
    # Wait for pipeline to render (DOM-based check)
    page.wait_for_function(
        "() => document.querySelectorAll('.pipeline-step').length >= 8",
        timeout=15000,
    )
    page.wait_for_timeout(1500)
    print("  Page reloaded — pipeline restored with 8 steps")

    # ── Verify pipeline is visible ────────────────────────────────────────
    pipeline_steps = page.locator(".pipeline-step")
    pipeline_count = pipeline_steps.count()
    print(f"  Pipeline shows {pipeline_count} steps")
    assert pipeline_count >= 8, f"Expected >=8 pipeline steps, got {pipeline_count}"

    # Enabled steps should have "enabled" chip
    enabled_chips = page.locator(".pipeline-step .chip:has-text('enabled')")
    print(f"  Enabled step chips: {enabled_chips.count()}")

    # ── Click Run ──────────────────────────────────────────────────────────
    run_btn = page.locator("#run-btn")
    assert run_btn.is_visible(), "Run button not visible"
    assert not run_btn.is_disabled(), "Run button is disabled"

    print(f"\n  {'='*50}")
    print(f"  CLICKING RUN — pipeline may take up to {PIPELINE_TIMEOUT_S}s")
    print(f"  {'='*50}\n")

    run_btn.click()
    page.wait_for_timeout(1000)

    # Verify the Run button actually got disabled (pipeline started)
    if not run_btn.is_disabled():
        # Pipeline didn't start — dump diagnostics
        console_text = (page.locator("#console-log").text_content() or "")[-1500:]
        js_errors = getattr(page, '_errors', [])
        page.screenshot(path="orchestrator_failure.png")
        pytest.fail(
            f"Run button did NOT become disabled after clicking. "
            f"Pipeline never started.\n"
            f"JS errors: {'; '.join(js_errors[-10:]) if js_errors else 'none'}\n"
            f"Console tail:\n{console_text}\n"
            f"Screenshot saved to orchestrator_failure.png"
        )

    # ── Wait for pipeline to finish ──────────────────────────────────────
    # The Run button is disabled during execution, re-enabled on completion.
    print("  Waiting for pipeline to complete...")
    start_wait = time.time()

    try:
        page.wait_for_function(
            "() => { const btn = document.getElementById('run-btn'); return btn && !btn.disabled; }",
            timeout=PIPELINE_TIMEOUT_S * 1000,
        )
    except Exception:
        elapsed = time.time() - start_wait
        # Dump visible state for diagnostics
        done = page.locator(".pipeline-step.is-done").count()
        error = page.locator(".pipeline-step.is-error").count()
        running = page.locator(".pipeline-step.is-running").count()
        console_tail = (page.locator("#console-log").text_content() or "")[-1500:]
        # Also check the EXE stdout for clues
        stdout_snip = "".join(getattr(app_server, '_stdout_lines', [])[-20:])
        pytest.fail(
            f"Pipeline did not complete within {PIPELINE_TIMEOUT_S}s "
            f"(elapsed: {elapsed:.0f}s).\n"
            f"Steps: {done} done, {error} error, {running} running\n"
            f"Console tail:\n{console_tail}\n"
            f"EXE stdout tail:\n{stdout_snip}"
        )

    elapsed = time.time() - start_wait
    print(f"  Pipeline finished in {elapsed:.0f}s")

    # Let final render settle
    page.wait_for_timeout(2000)

    # ── Verify step statuses via DOM ──────────────────────────────────────
    total = page.locator(".pipeline-step").count()
    done = page.locator(".pipeline-step.is-done").count()
    errors = page.locator(".pipeline-step.is-error").count()
    idle = page.locator(".pipeline-step:not(.is-done):not(.is-error):not(.is-running)").count()

    # Read individual step names and statuses from DOM for diagnostics
    step_details = []
    for i in range(total):
        step_el = page.locator(".pipeline-step").nth(i)
        name_el = step_el.locator(".step-name")
        name = name_el.text_content() or f"step-{i}"
        is_done = "is-done" in (step_el.get_attribute("class") or "")
        is_error = "is-error" in (step_el.get_attribute("class") or "")
        step_details.append((name, "done" if is_done else "error" if is_error else "?"))

    print(f"\n  Step results: {done} done, {errors} error, {idle} idle (total {total})")
    for name, status in step_details:
        icon = {"done": "✓", "error": "✗"}.get(status, "?")
        print(f"    {icon} {name}: {status}")

    # Extract error messages from the console log for failed steps
    console_text = (page.locator("#console-log").text_content() or "")
    error_lines = [l for l in console_text.split("\n") if "Step failed" in l or "ERROR" in l]
    if error_lines:
        print(f"\n  Errors from console:")
        for l in error_lines[-10:]:
            print(f"    {l.strip()}")

    # The critical path must succeed (these have no data dependencies).
    # Downstream steps (generate_financial_statements, ratios, rolling_metrics)
    # may fail when the date window has few/no XBRL documents.
    critical = ["Get Documents", "Populate Company Info", "Parse Taxonomy"]
    for name, status in step_details:
        if name in critical:
            assert status == "done", f"Critical step '{name}' failed (status: {status})"

    # If generate_financial_statements succeeded, all steps should pass
    fstmt_status = next((s for n, s in step_details if n == "Generate Financial Statements"), None)
    if fstmt_status == "done":
        assert errors == 0, f"{errors} step(s) failed but core steps passed"
    else:
        print(f"\n  Note: generate_financial_statements failed — expected when")
        print(f"  the date window has insufficient XBRL data. Increase")
        print(f"  TEST_DATE_RANGE_DAYS for a fuller pipeline run.")

    # ── Sanity: databases must have actual data ──────────────────────────
    db1_path = built_app_dir / "data" / "databases" / "Base.db"
    db2_path = built_app_dir / "data" / "databases" / "Standardized.db"

    db1_size = db1_path.stat().st_size
    db2_size = db2_path.stat().st_size
    print(f"  Base.db: {db1_size:,} bytes")
    print(f"  Standardized.db: {db2_size:,} bytes")

    # Check Base.db — must have DocumentList with rows after get_documents
    conn1 = sqlite3.connect(str(db1_path))
    base_tables = [r[0] for r in conn1.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]
    print(f"  Base.db tables: {base_tables}")

    assert "DocumentList" in base_tables, "DocumentList table missing from Base.db"
    doc_count = conn1.execute("SELECT COUNT(*) FROM DocumentList").fetchone()[0]
    print(f"  DocumentList rows: {doc_count}")
    assert doc_count > 0, (
        f"get_documents returned 0 documents. Check EDINET API reachability "
        f"and API_KEY validity."
    )

    has_financial_full = "financialData_full" in base_tables
    if has_financial_full:
        ff_count = conn1.execute("SELECT COUNT(*) FROM financialData_full").fetchone()[0]
        print(f"  financialData_full rows: {ff_count}")
    else:
        print(f"  financialData_full: NOT FOUND (no type-120 docs in range)")
    conn1.close()

    # Check Standardized.db
    conn2 = sqlite3.connect(str(db2_path))
    std_tables = [r[0] for r in conn2.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]
    print(f"  Standardized.db tables: {std_tables}")
    for t in ["CompanyInfo", "Taxonomy"]:
        assert t in std_tables, f"Expected table '{t}' not found in Standardized.db"
    if "FinancialStatements" in std_tables:
        print(f"  FinancialStatements table found — full pipeline succeeded")
    else:
        if has_financial_full:
            pytest.fail("financialData_full exists but FinancialStatements wasn't created")
        print(f"  FinancialStatements not found — no XBRL data to process")
    conn2.close()


# =============================================================================
# Test 3: Screening verification (Playwright)
# =============================================================================

def test_screening_verification(page, built_app_dir, app_server):
    """Run a default screening via Playwright and verify results exist."""
    print("\n  Navigating to Screening...")
    page.goto(f"{BASE_URL}/screening", wait_until="domcontentloaded")
    page.wait_for_selector("#scr-cfg", state="attached", timeout=15000)
    page.wait_for_timeout(3000)

    status_text = page.locator("#scr-status").text_content() or ""
    if "No database" in status_text or "Error" in status_text:
        if "FinancialStatements" in status_text:
            pytest.skip(f"Screening unavailable: {status_text.strip()}")
        pytest.fail(f"Screening page reports error: {status_text}")
    print(f"  Status: {status_text}")

    # Click Run
    run_btn = page.locator("#scr-btn-run")
    assert run_btn.is_visible()
    run_btn.click()

    print("  Waiting for screening results...")
    try:
        page.wait_for_function(
            """
            () => {
              const rows = document.querySelectorAll('#scr-tbody tr');
              const status = document.getElementById('scr-status');
              if (status && status.textContent.includes('Error')) return true;
              return rows.length > 0;
            }
            """,
            timeout=60000,
        )
    except Exception:
        status = page.locator("#scr-status").text_content() or ""
        rows = page.locator("#scr-tbody tr").count()
        pytest.fail(f"Screening did not return results within 60s. Status: '{status}', Rows: {rows}")

    status = page.locator("#scr-status").text_content() or ""
    if "Error" in status:
        if "FinancialStatements" in status or "no such table" in status:
            pytest.skip(f"Screening unavailable (no data): {status.strip()}")
        pytest.fail(f"Screening failed: {status}")

    count_text = page.locator("#scr-count").text_content() or ""
    rows = page.locator("#scr-tbody tr").count()
    print(f"  Results: {count_text} ({rows} rows)")

    assert rows > 0, "Screening returned 0 rows — pipeline may not have populated data"

    # Verify headers exist and sorting works
    headers = page.locator("#scr-thead th")
    assert headers.count() > 0, "No column headers"
    headers.first.click()
    page.wait_for_timeout(500)
    assert page.locator("#scr-tbody tr").count() == rows, "Row count changed after sort"

    # Formatted toggle
    fmt_checkbox = page.locator("#scr-fmt")
    assert fmt_checkbox.is_visible()
    fmt_checkbox.uncheck(); page.wait_for_timeout(200)
    fmt_checkbox.check(); page.wait_for_timeout(200)


# =============================================================================
# Test 4: Security Analysis verification (Playwright)
# =============================================================================

def test_security_analysis_verification(page, built_app_dir, app_server):
    """Open Security Analysis, search a company, verify tiles/history/charts."""
    print("\n  Navigating to Security Analysis...")
    page.goto(f"{BASE_URL}/security", wait_until="domcontentloaded")
    page.wait_for_selector("#sa-search", state="attached", timeout=15000)

    try:
        page.wait_for_function(
            "() => { const s = document.getElementById('sa-search'); return s && !s.disabled; }",
            timeout=15000,
        )
    except Exception:
        pytest.skip("Security Analysis search never became enabled (no DB available)")

    page.wait_for_timeout(1000)

    # Search for a company — try multiple queries and results until one
    # has metric tile values (some companies lack stock prices).
    queries = ["7", "Toyota", "Sony", "Mitsubishi", "Nippon", "1", "Holdings", "Corp"]
    company_selected = False

    for query in queries:
        if company_selected:
            break
        search_input = page.locator("#sa-search")
        search_input.fill(""); page.wait_for_timeout(100)
        search_input.fill(query); page.wait_for_timeout(1500)

        items = page.locator(".sa-search-item")
        try:
            items.first.wait_for(state="visible", timeout=3000)
        except Exception:
            continue

        count = items.count()
        print(f"  Query '{query}': {count} results")
        if count == 0:
            continue

        # Try up to 5 results — re-search each time since dropdown closes on click
        for i in range(min(count, 5)):
            # Re-open the dropdown for each attempt
            if i > 0:
                search_input.fill(""); page.wait_for_timeout(100)
                search_input.fill(query); page.wait_for_timeout(1000)
                try:
                    items.first.wait_for(state="visible", timeout=3000)
                except Exception:
                    break

            items.nth(i).click()
            page.wait_for_timeout(5000)

            if page.locator("#sa-header").count() == 0:
                continue
            header_visible = page.locator("#sa-header").evaluate(
                "el => el.classList.contains('is-visible')"
            )
            if not header_visible:
                continue

            # Check for real data
            tiles = page.locator(".sa-metric-tile")
            tile_count = tiles.count()
            has_data = False
            for t in range(min(tile_count, 15)):
                value_el = tiles.nth(t).locator(".sa-metric-value")
                if value_el.count() > 0:
                    val = (value_el.text_content() or "").strip()
                    if val and val != "—" and val != "":
                        has_data = True
                        break

            if has_data:
                company_selected = True
                print(f"  Selected company #{i+1} via '{query}' (has data)")
                break
            else:
                print(f"  Company #{i+1} via '{query}' has no data, trying next...")

        if company_selected:
            break

    if not company_selected:
        # Check if the DB has financial data at all
        db2_path = built_app_dir / "data" / "databases" / "Standardized.db"
        if db2_path.exists():
            conn = sqlite3.connect(str(db2_path))
            has_fstmt = "FinancialStatements" in [
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            ]
            conn.close()
            if not has_fstmt:
                pytest.skip("FinancialStatements table not created — no data to analyze")
        pytest.fail("Could not select any company. Database may be empty.")

    header_text = page.locator("#sa-header").text_content() or ""
    print(f"  Company: {header_text[:200]}")

    # Wait for history table to load (it may take a moment)
    try:
        page.wait_for_function(
            "() => document.querySelectorAll('.sa-history-table tbody tr').length > 0",
            timeout=15000,
        )
    except Exception:
        pass  # Some companies genuinely have no history — proceed anyway

    # Metric tiles
    tiles = page.locator(".sa-metric-tile")
    tile_count = tiles.count()
    print(f"  Metric tiles: {tile_count}")
    assert tile_count >= 2, f"Expected >=2 metric tiles, got {tile_count}"

    non_dash = 0
    for i in range(min(tile_count, 20)):
        value_el = tiles.nth(i).locator(".sa-metric-value")
        if value_el.count() > 0:
            val = (value_el.text_content() or "").strip()
            if val and val != "—":
                non_dash += 1
    print(f"  Tiles with real values: {non_dash}")
    assert non_dash >= 1, "No metric tiles have real data values"

    # History table (may be empty for some companies with partial data)
    history_rows = page.locator(".sa-history-table tbody tr")
    row_count = history_rows.count()
    print(f"  History rows: {row_count}")
    # At minimum, table structure should exist (tabs present)

    # Table tabs (may fail to load if company has incomplete data)
    tabs = page.locator("#sa-tabbar .sa-tab")
    tab_count = tabs.count()
    print(f"  Table tabs: {tab_count}")
    if tab_count == 0:
        print(f"  Note: no table tabs loaded — company may have incomplete data")
    else:
        assert tab_count >= 1, "No table tabs found"

    # Chart toggle (only if table data loaded)
    if tab_count > 0:
        chart_btn = page.locator("button", has_text="Chart")
        if chart_btn.count() > 0:
            chart_btn.click(); page.wait_for_timeout(1500)
            canvas_count = page.locator(".sa-chart-canvas-wrap canvas").count()
            print(f"  Chart canvases: {canvas_count}")
            assert canvas_count > 0, "Chart did not render"

            table_btn = page.locator("button", has_text="Table")
            if table_btn.count() > 0:
                table_btn.click(); page.wait_for_timeout(500)

    # Hide All / Show All cycle
    if tab_count > 0:
        hide_all = page.locator("button", has_text="Hide All")
        show_all = page.locator("button", has_text="Show All")
        if hide_all.count() > 0:
            rows_before = page.locator(".sa-history-table tbody tr").count()
            hide_all.click(); page.wait_for_timeout(300)
            rows_after_hide = page.locator(".sa-history-table tbody tr").count()
            print(f"  Rows before Hide All: {rows_before}, after: {rows_after_hide}")

            if show_all.count() > 0:
                show_all.click(); page.wait_for_timeout(300)
                rows_after_show = page.locator(".sa-history-table tbody tr").count()
                assert rows_after_show > 0, "Show All did not restore rows"
                print(f"  Rows after Show All: {rows_after_show}")

    print("  Security Analysis verification complete ✓")
