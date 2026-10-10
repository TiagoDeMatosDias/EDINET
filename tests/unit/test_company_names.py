"""English company names read from annual reports for the companies EDINET lists only in Japanese."""

from __future__ import annotations

import io
import sqlite3
import zipfile

import pytest

from src.orchestrator.common import company_names
from src.orchestrator.common.sqlite import connect_write

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


@pytest.fixture
def databases(tmp_path):
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
    conn = sqlite3.connect(filings)
    conn.execute("CREATE TABLE filings (doc_id TEXT PRIMARY KEY, edinet_code TEXT, submitter_name TEXT, period_start TEXT, period_end TEXT, submitted_at TEXT, form_code TEXT, doc_type_code TEXT, archive_content BLOB)")
    conn.executemany("INSERT INTO filings VALUES (?, ?, '', '', ?, ?, ?, '120', ?)", [
        ("S1", "E00001", "2026-03-31", "2026-06-20", "030000", _report("Alpha Motor Kabushiki Kaisha")),
        ("S2OLD", "E00002", "2025-03-31", "2025-06-20", "030000", _report("Old Beta Shipping")),
        ("S2", "E00002", "2026-03-31", "2026-06-20", "030000", _report("Beta Kaiun Kabushiki Kaisha")),
        ("S3", "E00003", "2026-03-31", "2026-06-20", "030000", _report("Gamma Trading Co., Ltd.")),
        # Not an annual report: a company that files only these stays as EDINET lists it.
        ("S4", "E00004", "2026-03-31", "2026-06-20", "07A000", _report("Delta Fund")),
        ("S5", "E00005", "2026-03-31", "2026-06-20", "030000", _report(None)),
    ])
    conn.commit()
    conn.close()
    return market, filings


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
    conn = sqlite3.connect(filings)
    conn.execute("INSERT INTO filings VALUES ('S2NEW', 'E00002', '', '', '2027-03-31', '2027-06-20', '030000', '120', ?)", (_report("Beta Marine Holdings, Inc."),))
    conn.execute("INSERT INTO filings VALUES ('S1NEW', 'E00001', '', '', '2027-03-31', '2027-06-20', '030000', '120', ?)", (_report("Alpha Renamed"),))
    conn.commit()
    conn.close()

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
