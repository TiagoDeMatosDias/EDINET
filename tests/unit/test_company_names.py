"""English company names read from annual reports for the companies EDINET lists only in Japanese."""

from __future__ import annotations

import io
import sqlite3
import zipfile
from pathlib import Path

import pytest

from src.filings.catalog import FilingCatalog
from src.orchestrator.common import company_names
from src.orchestrator.common.sqlite import connect_write
from tests.filing_fixtures import create_legacy_filings_db, filing_row

HEADER = """<html><body><div style="display:none"><ix:header><ix:hidden>
<ix:nonNumeric name="jpdei_cor:EDINETCodeDEI" contextRef="FilingDateInstant">E00002</ix:nonNumeric>
<ix:nonNumeric name="jpdei_cor:FilerNameInJapaneseDEI" contextRef="FilingDateInstant">ベータ海運株式会社</ix:nonNumeric>
<ix:nonNumeric name="jpdei_cor:FilerNameInEnglishDEI" contextRef="FilingDateInstant">{name}</ix:nonNumeric>
</ix:hidden></ix:header></div></body></html>"""


def _report(name: str | None) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as bundle:
        bundle.writestr("XBRL/PublicDoc/0101010_honbun_jpcrp030000-asr-001_E00002-000_ixbrl.htm", "<html><body><p>本文</p></body></html>")
        if name is not None:
            bundle.writestr("XBRL/PublicDoc/0000000_header_jpcrp030000-asr-001_E00002-000_ixbrl.htm", HEADER.format(name=name))
    return buffer.getvalue()


def test_a_filing_states_its_filers_name_in_english():
    assert company_names.filer_english_name(_report("Beta  Kaiun\n Kabushiki&#160;Kaisha &amp; Co.")) == "Beta Kaiun Kabushiki Kaisha & Co."
    assert company_names.filer_english_name(_report("ＢＥＴＡ　ＫＡＩＵＮ")) == "BETA KAIUN"
    assert company_names.filer_english_name(_report("－")) == ""
    assert company_names.filer_english_name(_report(None)) == ""


@pytest.fixture(params=["split", "legacy"])
def databases(tmp_path, request):
    market, filings = str(tmp_path / "market.db"), str(tmp_path / "filings.db")
    conn = sqlite3.connect(market)
    conn.execute('CREATE TABLE CompanyInfo (Company_Code TEXT, "Submitter Name" TEXT, Company_Name TEXT, Company_Ticker TEXT)')
    conn.executemany("INSERT INTO CompanyInfo VALUES (?, ?, ?, ?)", [
        ("E00001", "アルファ自動車株式会社", "ALPHA MOTOR CORPORATION", "10000"),
        ("E00002", "ベータ海運株式会社", "", "20000"),
        ("E00003", "ガンマ商事株式会社", None, "30000"),
        ("E00004", "デルタ投資事業組合", "", ""),
        ("E00005", "無記名工業株式会社", "", "50000"),
    ])
    conn.commit()
    conn.close()
    reports = [
        ("S1", "E00001", "2026-03-31", "2026-06-20", "030000", _report("Alpha Motor Kabushiki Kaisha")),
        ("S2OLD", "E00002", "2025-03-31", "2025-06-20", "030000", _report("Old Beta Shipping")),
        ("S2", "E00002", "2026-03-31", "2026-06-20", "030000", _report("Beta Kaiun Kabushiki Kaisha")),
        ("S3", "E00003", "2026-03-31", "2026-06-20", "030000", _report("Gamma Trading Co., Ltd.")),
        # Not an annual report: a company that files only these stays as EDINET lists it.
        ("S4", "E00004", "2026-03-31", "2026-06-20", "07A000", _report("Delta Fund")),
        ("S5", "E00005", "2026-03-31", "2026-06-20", "030000", _report(None)),
    ]
    rows = [
        filing_row(doc_id, code, submitted, content=content, form_code=form, period_end=period_end, doc_type_code="120")
        for doc_id, code, period_end, submitted, form, content in reports
    ]
    if request.param == "split":
        catalog = FilingCatalog(filings)
        for row in rows:
            catalog.upsert_filing(row)
    else:
        create_legacy_filings_db(Path(filings), rows)
    return market, filings


def _add(filings: str, rows: list[dict]) -> None:
    """Add filings to either layout (the old single file is written directly, as before the split)."""
    catalog = FilingCatalog(filings)
    if catalog.layout != "legacy":
        for row in rows:
            catalog.upsert_filing(row)
        return
    conn = sqlite3.connect(filings)
    try:
        for row in rows:
            columns = [column for column in row if column != "archive_path"]
            conn.execute(
                f"INSERT INTO filings ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
                [row[column] for column in columns],
            )
        conn.commit()
    finally:
        conn.close()


def _names(market: str) -> dict[str, str | None]:
    conn = sqlite3.connect(market)
    try:
        return dict(conn.execute("SELECT Company_Code, Company_Name FROM CompanyInfo"))
    finally:
        conn.close()


def test_blank_english_names_are_filled_from_each_companys_latest_annual_report(databases):
    market, filings = databases

    assert company_names.fill_english_names(market, filings) == {"read": 3, "named": 2, "filled": 2}
    assert _names(market) == {
        # A name the code list gives is kept, whatever the report says.
        "E00001": "ALPHA MOTOR CORPORATION",
        "E00002": "Beta Kaiun Kabushiki Kaisha",
        "E00003": "Gamma Trading Co., Ltd.",
        "E00004": "",
        "E00005": "",
    }
    # Nothing is opened twice, including the report that states no English name.
    assert company_names.fill_english_names(market, filings) == {"read": 0, "named": 0, "filled": 0}


def test_names_come_back_after_the_code_list_is_imported_again_without_opening_filings(databases):
    market, filings = databases
    company_names.fill_english_names(market, filings)
    conn = sqlite3.connect(market)
    conn.execute("UPDATE CompanyInfo SET Company_Name = '' WHERE Company_Code IN ('E00002', 'E00003')")
    # EDINET has since given Gamma an English name of its own.
    conn.execute("UPDATE CompanyInfo SET Company_Name = 'GAMMA SHOJI' WHERE Company_Code = 'E00003'")
    conn.commit()
    conn.close()

    assert company_names.fill_english_names(market, filings) == {"read": 0, "named": 0, "filled": 1}
    assert _names(market)["E00002"] == "Beta Kaiun Kabushiki Kaisha" and _names(market)["E00003"] == "GAMMA SHOJI"


def test_a_newer_annual_report_updates_a_name_it_filled_before(databases):
    market, filings = databases
    company_names.fill_english_names(market, filings)
    _add(filings, [
        filing_row("S2NEW", "E00002", "2027-06-20", content=_report("Beta Marine Holdings, Inc."), period_end="2027-03-31", doc_type_code="120"),
        filing_row("S1NEW", "E00001", "2027-06-20", content=_report("Alpha Renamed"), period_end="2027-03-31", doc_type_code="120"),
    ])

    assert company_names.fill_english_names(market, filings) == {"read": 1, "named": 1, "filled": 0}
    assert _names(market)["E00002"] == "Beta Marine Holdings, Inc." and _names(market)["E00001"] == "ALPHA MOTOR CORPORATION"


def test_missing_sources_leave_the_company_table_alone(databases, tmp_path):
    market, _ = databases
    assert company_names.fill_english_names(market, str(tmp_path / "none.db")) == {"read": 0, "named": 0, "filled": 0}
    empty = str(tmp_path / "empty.db")
    conn = connect_write(empty)
    try:
        # No company table yet: nothing to fill, and no error.
        assert company_names.fill(conn, None) == {"read": 0, "named": 0, "filled": 0}
    finally:
        conn.close()


def test_a_missing_yearly_file_is_skipped_and_read_once_it_is_back(tmp_path, caplog):
    market, filings = str(tmp_path / "market.db"), tmp_path / "filings.db"
    conn = sqlite3.connect(market)
    conn.execute('CREATE TABLE CompanyInfo (Company_Code TEXT, "Submitter Name" TEXT, Company_Name TEXT, Company_Ticker TEXT)')
    conn.executemany("INSERT INTO CompanyInfo VALUES (?, ?, ?, ?)", [("E00002", "ベータ", "", "2"), ("E00003", "ガンマ", "", "3")])
    conn.commit()
    conn.close()
    catalog = FilingCatalog(filings)
    catalog.upsert_filing(filing_row("S2", "E00002", "2025-06-20", content=_report("Beta Kaiun"), period_end="2025-03-31", doc_type_code="120"))
    catalog.upsert_filing(filing_row("S3", "E00003", "2026-06-20", content=_report("Gamma Trading"), period_end="2026-03-31", doc_type_code="120"))
    shard = catalog.storage.shard_path("2025")
    shard.rename(shard.with_suffix(".away"))

    with caplog.at_level("WARNING"):
        assert company_names.fill_english_names(market, str(filings)) == {"read": 1, "named": 1, "filled": 1}
    assert "filings_2025.db is missing" in caplog.text
    assert _names(market) == {"E00002": "", "E00003": "Gamma Trading"}

    shard.with_suffix(".away").rename(shard)
    assert company_names.fill_english_names(market, str(filings)) == {"read": 1, "named": 1, "filled": 1}
    assert _names(market)["E00002"] == "Beta Kaiun"
