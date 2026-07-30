"""Serve deterministic demo data and refresh documentation screenshots.

The capture environment never opens databases from ``data/`` or the operator's
configured paths.  Use ``--serve-only`` when capturing through another browser
client, or run the script directly to use the declared Playwright dependency.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.request
import zipfile
from pathlib import Path

import uvicorn

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests.factories import (  # noqa: E402
    create_docs_market_database,
    sample_ibkr_xml,
)


def _base_database(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE DocumentList (
                docID TEXT PRIMARY KEY,
                edinetCode TEXT,
                filerName TEXT,
                submitDateTime TEXT,
                periodStart TEXT,
                periodEnd TEXT,
                formCode TEXT,
                docTypeCode TEXT,
                xbrlFlag TEXT,
                csvFlag TEXT,
                xbrlDownloaded INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX idx_document_list_download
                ON DocumentList(xbrlDownloaded, docTypeCode, xbrlFlag, docID);
            CREATE INDEX idx_document_list_company_date
                ON DocumentList(edinetCode, submitDateTime DESC);
            """
        )
        conn.executemany(
            "INSERT INTO DocumentList VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "S100DEMO",
                    "E00001",
                    "Alpha Test Company",
                    "2026-06-20T09:00:00+09:00",
                    "2025-04-01",
                    "2026-03-31",
                    "030000",
                    "120",
                    "1",
                    "0",
                    1,
                ),
                (
                    "S100BETA",
                    "E00002",
                    "Beta Test Company",
                    "2026-06-18T09:00:00+09:00",
                    "2025-04-01",
                    "2026-03-31",
                    "030000",
                    "120",
                    "1",
                    "0",
                    1,
                ),
                (
                    "S100ALPHA25",
                    "E00001",
                    "Alpha Test Company",
                    "2025-06-19T09:00:00+09:00",
                    "2024-04-01",
                    "2025-03-31",
                    "030000",
                    "120",
                    "1",
                    "0",
                    1,
                ),
            ],
        )


def _filing_zip(*, revenue: int, operating_income: int, net_income: int) -> bytes:
    xbrl = f"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"
 xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
 xmlns:jp="https://example.test/jp">
  <xbrli:context id="FY2026">
    <xbrli:entity><xbrli:identifier scheme="demo">E00001</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2025-04-01</xbrli:startDate><xbrli:endDate>2026-03-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:context id="FY2026Instant">
    <xbrli:entity><xbrli:identifier scheme="demo">E00001</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:instant>2026-03-31</xbrli:instant></xbrli:period>
  </xbrli:context>
  <xbrli:unit id="JPY"><xbrli:measure>iso4217:JPY</xbrli:measure></xbrli:unit>
  <jp:NetSales contextRef="FY2026" unitRef="JPY" decimals="0">{revenue}</jp:NetSales>
  <jp:OperatingIncome contextRef="FY2026" unitRef="JPY" decimals="0">{operating_income}</jp:OperatingIncome>
  <jp:NetIncome contextRef="FY2026" unitRef="JPY" decimals="0">{net_income}</jp:NetIncome>
  <jp:TotalAssets contextRef="FY2026Instant" unitRef="JPY" decimals="0">14500000000</jp:TotalAssets>
  <jp:ShareholdersEquity contextRef="FY2026Instant" unitRef="JPY" decimals="0">8265000000</jp:ShareholdersEquity>
</xbrli:xbrl>""".encode()
    html = """<!doctype html><html lang="ja"><head><meta charset="utf-8">
<style>body{font-family:system-ui,sans-serif;padding:24px;color:#172033}h1{font-size:24px}
h2{font-size:18px;margin-top:26px;border-bottom:1px solid #ccd4e0;padding-bottom:6px}
p{line-height:1.75}table{border-collapse:collapse;width:100%}td,th{border:1px solid #ccd4e0;padding:8px}</style>
</head><body>
<h1>有価証券報告書</h1><p>提出会社 概要</p>
<h2>経営成績</h2><p>当期 売上高 増加</p><p>営業利益 増加</p>
<h2>財政状態</h2><p>資産 合計 増加</p><p>純資産 増加</p>
<h2>キャッシュ・フロー</h2><p>営業活動によるキャッシュ・フロー 増加</p>
<h2>リスク</h2><p>重要な 事業 リスク</p>
</body></html>""".encode("utf-8")
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("XBRL/PublicDoc/report.xbrl", xbrl)
        archive.writestr("XBRL/PublicDoc/report.htm", html)
    return output.getvalue()


def _seed_filings(path: Path) -> None:
    from src.filings.catalog import FilingCatalog
    from src.filings.ingest import ingest_content

    catalog = FilingCatalog(path)
    filings = [
        ("S100DEMO", "E00001", "Alpha Test Company", "2026-06-20", 12_200_000_000),
        ("S100BETA", "E00002", "Beta Test Company", "2026-06-18", 9_400_000_000),
        ("S100ALPHA25", "E00001", "Alpha Test Company", "2025-06-19", 11_300_000_000),
    ]
    for doc_id, code, name, submitted_at, revenue in filings:
        ingest_content(
            _filing_zip(
                revenue=revenue,
                operating_income=int(revenue * 0.16),
                net_income=int(revenue * 0.115),
            ),
            doc_id,
            catalog,
            {
                "edinet_code": code,
                "submitter_name": name,
                "period_start": "2025-04-01",
                "period_end": "2026-03-31",
                "submitted_at": f"{submitted_at}T09:00:00+09:00",
                "form_code": "030000",
                "doc_type_code": "120",
            },
        )


def _seed_research(path: Path) -> None:
    from src.research.storage import ResearchStore

    store = ResearchStore(path)
    store.set_company_tags("local", "E00001", ["Favorite", "Quality compounders"])
    store.set_company_tags("local", "E00002", ["Industrial watchlist"])
    store.create_note(
        "local",
        "Margin durability",
        "Review the automation segment and recurring service revenue after the next filing.",
        "E00001",
    )
    store.upsert_company_research(
        "local",
        "E00001",
        thesis_status="watch",
        target_value=2_450,
        target_currency="JPY",
        review_on="2026-09-30",
    )
    store.create_alert(
        "local",
        "Alpha price review",
        "E00001",
        json.dumps({"metric": "LatestPrice", "operator": ">", "value": 130}),
    )


def _seed_portfolio(path: Path, market_path: Path) -> None:
    from src.portfolio.ibkr_parser import normalize_entries, parse_ibkr_xml
    from src.portfolio.portfolio_state import build_portfolio_state
    from src.portfolio.schema import create_tables
    from src.portfolio.transactions import insert_entries

    create_tables(str(path))
    entries = normalize_entries(parse_ibkr_xml(sample_ibkr_xml()))
    insert_entries(
        str(path),
        entries,
        source_file="documentation-demo.xml",
        owner_user_id="local",
    )
    build_portfolio_state(
        str(path),
        db2_path=str(market_path),
        end_date="2026-07-29",
        base_currency="EUR",
        owner_user_id="local",
    )


def create_demo_runtime(*, seed_research: bool = False) -> tuple[Path, dict[str, str]]:
    runtime = Path(tempfile.mkdtemp(prefix="edinet-docs-demo-")).resolve()
    database_dir = runtime / "databases"
    database_dir.mkdir(parents=True)
    paths = {
        "db1": str(database_dir / "Base.db"),
        "db2": str(database_dir / "Standardized.db"),
        "db3": str(database_dir / "Portfolio.db"),
        "auth_db": str(database_dir / "auth.db"),
        "research_db": str(database_dir / "research.db"),
        "pipeline_jobs_db": str(database_dir / "pipeline_jobs.db"),
        "filings_db": str(database_dir / "Filings.db"),
    }
    _base_database(Path(paths["db1"]))
    market_path = create_docs_market_database(paths["db2"])

    os.environ["EDINET_AUTH_MODE"] = "disabled"
    os.environ["EDINET_AUTH_DB"] = paths["auth_db"]
    os.environ["EDINET_ALLOWED_DATA_ROOTS"] = str(runtime)
    os.environ["EDINET_FRONTEND_DIST"] = str((ROOT / "frontend-v2" / "dist").resolve())
    from src.orchestrator.common import db_config

    db_config._cache = dict(paths)
    _seed_portfolio(Path(paths["db3"]), market_path)
    if seed_research:
        _seed_research(Path(paths["research_db"]))
    _seed_filings(Path(paths["filings_db"]))
    return runtime, paths


def _wait_for_server(base_url: str) -> None:
    for _ in range(80):
        try:
            with urllib.request.urlopen(f"{base_url}/health", timeout=0.5):
                return
        except Exception:
            time.sleep(0.25)
    raise RuntimeError("Documentation server did not start")


def _capture_with_playwright(
    base_url: str,
    output_dir: Path,
    research_db: Path,
) -> None:
    from playwright.sync_api import Page, sync_playwright

    output_dir.mkdir(parents=True, exist_ok=True)

    def dismiss_local_warning(page: Page) -> None:
        dismiss = page.get_by_role("button", name="Dismiss")
        if dismiss.count() and dismiss.is_visible():
            dismiss.click()

    def capture(page: Page, route: str, filename: str, *, wait_ms: int = 800) -> None:
        page.goto(f"{base_url}{route}", wait_until="networkidle")
        dismiss_local_warning(page)
        page.wait_for_timeout(wait_ms)
        page.screenshot(path=str(output_dir / filename))

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 720})
        capture(page, "/", "web-home.png")
        capture(page, "/pricing", "web-pricing.png")
        capture(page, "/overview", "web-dashboard.png")
        capture(page, "/pipeline", "web-pipeline.png")

        page.goto(f"{base_url}/screen", wait_until="networkidle")
        dismiss_local_warning(page)
        page.get_by_role("button", name="Run screen").click()
        page.get_by_text("Alpha Test Company").wait_for(timeout=10_000)
        page.screenshot(path=str(output_dir / "web-screening.png"))

        capture(page, "/analyze/E00001", "web-security-analysis.png", wait_ms=1_500)
        capture(page, "/backtest", "web-backtesting.png")
        capture(page, "/portfolio", "web-portfolio.png", wait_ms=1_500)
        capture(page, "/filings", "web-filings.png")

        page.goto(f"{base_url}/filings/S100DEMO", wait_until="networkidle")
        dismiss_local_warning(page)
        page.get_by_role("button", name="Sections", exact=True).click()
        page.get_by_text("English", exact=True).first.wait_for(timeout=10_000)
        page.screenshot(path=str(output_dir / "web-filing-translation.png"))

        _seed_research(research_db)
        capture(page, "/research", "web-research.png")

        page.goto(f"{base_url}/compare", wait_until="networkidle")
        dismiss_local_warning(page)
        picker = page.get_by_label("Add company")
        picker.fill("Alpha")
        page.get_by_role(
            "option",
            name="Alpha Test Company AAA · E00001 · Technology · JPX Prime",
        ).click()
        picker.fill("Beta")
        page.get_by_role(
            "option",
            name="Beta Test Company BBB · E00002 · Industrials · JPX Prime",
        ).click()
        page.get_by_role("button", name="Compare", exact=True).click()
        page.get_by_text("Financial comparison").wait_for(timeout=10_000)
        page.get_by_text("Financial comparison").scroll_into_view_if_needed()
        page.screenshot(path=str(output_dir / "web-comparison.png"))
        page.get_by_text("Metrics", exact=True).scroll_into_view_if_needed()
        page.get_by_role("button", name="Add metric").click()
        page.evaluate("window.scrollBy(0, 150)")
        page.screenshot(path=str(output_dir / "web-comparison-metrics.png"))
        browser.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--serve-only", action="store_true")
    parser.add_argument(
        "--seed-research",
        action="store_true",
        help="Seed tags/notes immediately when using --serve-only.",
    )
    parser.add_argument("--keep-runtime", action="store_true")
    parser.add_argument("--output", type=Path, default=ROOT / "docs" / "images")
    args = parser.parse_args()

    runtime, paths = create_demo_runtime(seed_research=args.seed_research)
    from src.web_app.server import app

    config = uvicorn.Config(app, host="127.0.0.1", port=args.port, log_level="warning")
    server = uvicorn.Server(config)
    base_url = f"http://127.0.0.1:{args.port}"
    try:
        if args.serve_only:
            print(f"Documentation demo: {base_url}", flush=True)
            print(f"Temporary runtime: {runtime}", flush=True)
            server.run()
            return 0

        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        _wait_for_server(base_url)
        _capture_with_playwright(
            base_url,
            args.output.resolve(),
            Path(paths["research_db"]),
        )
        print(f"Captured documentation screenshots in {args.output.resolve()}")
        return 0
    finally:
        server.should_exit = True
        if args.keep_runtime:
            print(f"Kept temporary runtime: {runtime}")
        else:
            shutil.rmtree(runtime, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
