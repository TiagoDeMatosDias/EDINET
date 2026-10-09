"""Bond terms from EDINET filings: parsing, merging, valuation, the update step, and the API."""

from __future__ import annotations

import gzip
import io
import json
import sqlite3
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.bonds.api as bonds_api
from src.bonds import build, jsda, parsing, service, update, valuation
from src.bonds.store import BOND_COLUMNS, ensure_bond_tables
from src.filings.acquisition import EdinetAcquisitionError
from src.orchestrator.common.sqlite import connect_write
from src.web_app.security import AppSettings, install_security

TODAY = "2026-10-09"
PASSWORD = "correct horse battery staple"

ISSUANCE_HTML = """<html><body>
<h2>第一部【証券情報】</h2>
<table>
<tr><td>銘柄</td><td>テスト工業株式会社第５回無担保社債（社債間限定同順位特約付）</td></tr>
<tr><td>記名・無記名の別</td><td>―</td></tr>
<tr><td>券面総額又は振替社債の総額(円)</td><td>金10,000百万円</td></tr>
<tr><td>各社債の金額(円)</td><td>金１億円</td></tr>
<tr><td>発行価格(円)</td><td>各社債の金額100円につき金99.98円</td></tr>
<tr><td>利率(％)</td><td>年<span>1</span> .250％</td></tr>
<tr><td>利払日</td><td>毎年３月20日及び９月20日</td></tr>
<tr><td>償還期限</td><td>2031年９月19日</td></tr>
<tr><td>募集の方法</td><td>一般募集</td></tr>
<tr><td>払込期日</td><td>2026年９月20日</td></tr>
<tr><td>担保</td><td>本社債には担保及び保証は付されておらず、また本社債のために特に留保されている資産はない。</td></tr>
<tr><td>財務上の特約(担保提供制限)</td><td>当社は、本社債の未償還残高が存する限り、担保権を設定する場合には本社債にも同順位の担保権を設定する。</td></tr>
</table>
<p>（注）１ 信用格付 株式会社格付投資情報センター（以下Ｒ＆Ｉという。）信用格付：Ａ＋（シングルＡプラス）（取得日 2026年９月14日）</p>
<table>
<tr><td rowspan="2">銘柄</td><td>テスト工業株式会社第１回利払繰延条項・期限前償還条項付無担保社債</td></tr>
<tr><td>（劣後特約付）</td></tr>
<tr><td>券面総額又は振替社債の総額(円)</td><td>金50億円</td></tr>
<tr><td rowspan="2">利率(％)</td><td>１．2026年９月20日の翌日から2031年９月20日まで年2.000％</td></tr>
<tr><td>２．2031年９月20日の翌日以降 １年国債金利に2.5％を加えた値</td></tr>
<tr><td>利払日</td><td>毎年３月20日及び９月20日</td></tr>
<tr><td>償還期限</td><td>2061年９月20日</td></tr>
<tr><td>償還の方法</td><td>１ 償還金額 各社債の金額100円につき金100円</td></tr>
<tr><td></td><td>２ (1) 本社債の元金は、2061年９月20日にその総額を償還する。(2) 当社は、2031年９月20日に本社債の全部を期限前償還することができる。</td></tr>
<tr><td>払込期日</td><td>2026年９月20日</td></tr>
</table>
<p>本社債について、当社は株式会社日本格付研究所（以下ＪＣＲという。）からＡ－（シングルＡマイナス）の信用格付を取得している。</p>
<h2>第三部【参照情報】</h2>
<table><tr><td>銘柄</td><td>参照書類の表は読まない</td></tr></table>
</body></html>"""

ANNUAL_HTML = """<html><body>
<ix:nonNumeric name="jpcrp_cor:AnnexedConsolidatedDetailedScheduleOfCorporateBondsTextBlock" contextRef="CurrentYearDuration" escape="true">
<p>【社債明細表】</p>
<table>
<tr><td>会社名</td><td>銘柄</td><td>発行年月日</td><td>当期首残高<br/>(百万円)</td><td>当期末残高<br/>(百万円)</td><td>利率<br/>(％)</td><td>担保</td><td>償還期限</td></tr>
<tr><td rowspan="3">当社</td><td>第３回無担保社債</td><td>2019.９.20</td><td>10,000</td><td>10,000<br/>(10,000)</td><td>0.300</td><td>なし</td><td>2026.９.18</td></tr>
<tr><td>第４回無担保社債</td><td>2021.９.20</td><td>15,000</td><td>15,000</td><td>0.400</td><td>〃</td><td>2031.９.19</td></tr>
<tr><td>第２回無担保社債</td><td>2016.９.20</td><td>5,000</td><td>―</td><td>0.200</td><td>〃</td><td>2025.９.19</td></tr>
<tr><td>テスト物流㈱</td><td>第１回無担保社債(銀行保証付)</td><td>2022年３月31日</td><td>300</td><td>200<br/>(100)</td><td>日本円 6ヶ月TIBOR</td><td>なし</td><td>2027年３月31日</td></tr>
<tr><td>当社</td><td>普通社債(注)２</td><td>2023.10.10</td><td>14,000</td><td>15,000<br/>[100百万ドル]</td><td>5.000</td><td>なし</td><td>2028.10.10</td></tr>
<tr><td>合計</td><td>―</td><td>―</td><td>44,300</td><td>40,200</td><td>―</td><td>―</td><td>―</td></tr>
</table>
</ix:nonNumeric>
</body></html>"""

IFRS_HTML = """<html><body>
<ix:nonNumeric name="jpigp_cor:NotesBondsAndBorrowingsConsolidatedFinancialStatementsIFRSTextBlock" contextRef="CurrentYearDuration">
<table><tr><td>(単位:百万円)</td></tr></table>
<table>
<tr><td>会社名</td><td>種別</td><td>発行年月日</td><td>前年度 (2025年３月31日)</td><td>当年度 (2026年３月31日)</td><td>償還期限 (利率)</td></tr>
<tr><td>提出会社</td><td>第12回 無担保社債</td><td>2017年 ６月13日</td><td>29,960 (-)</td><td>29,976 (-)</td><td>2027年 ６月11日 (0.330%)</td></tr>
<tr><td>提出会社</td><td>第14回 無担保社債</td><td>2022年 ６月13日</td><td>9,991</td><td>9,995</td><td>2032年 ６月11日 (0.600%)</td></tr>
<tr><td>提出会社</td><td>第14回 無担保社債</td><td>2022年 ６月13日</td><td>-</td><td>(9,995)</td><td>2032年 ６月11日 (0.600%)</td></tr>
<tr><td>提出会社</td><td>コマーシャル・ペーパー</td><td>-</td><td>10,000</td><td>-</td><td>2025年 ６月 (0.100%)</td></tr>
</table>
</ix:nonNumeric>
</body></html>"""


@contextmanager
def _db(path: str | Path) -> Iterator[sqlite3.Connection]:
    """A connection that commits and closes."""
    conn = sqlite3.connect(path)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def _zip(html: str, name: str = "0101010_honbun_jpcrp-rep_ixbrl.htm") -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as bundle:
        bundle.writestr(f"XBRL/PublicDoc/{name}", html)
    return buffer.getvalue()


# --- parsing ---------------------------------------------------------------

def test_dates_amounts_and_rates_are_normalised():
    assert parsing.parse_date("2033年７月29日") == "2033-07-29"
    assert parsing.parse_date("2016.９.30") == "2016-09-30"
    assert parsing.parse_date("令和８年10月１日") == "2026-10-01"
    assert parsing.parse_date("R8.10.1") == "2026-10-01"
    assert parsing.parse_date("平成元年１月８日") == "1989-01-08"
    assert parsing.parse_date("年 月 日") is None
    assert parsing.parse_money("金30,000百万円") == (30e9, "JPY")
    assert parsing.parse_money("１兆2,000億円") == (1.2e12, "JPY")
    assert parsing.parse_money("金500百万米ドル") == (500e6, "USD")
    assert parsing.parse_rate("年1 .314 %") == pytest.approx(0.01314)
    assert parsing.issue_price_per_100("各社債の金額100円につき金99.98円") == pytest.approx(99.98)
    assert parsing.coupon_frequency("毎年１月31日および７月31日") == 2
    assert parsing.coupon_kind("TONAに0.37%を加えた値（ただし0%を下回る場合は0%）", 0.0037) == "floating"
    assert parsing.parse_schedule_coupon("日本円 6ヶ月TIBOR") is None
    assert parsing.parse_schedule_coupon("年 0.27") == pytest.approx(0.0027)
    assert parsing.parse_schedule_coupon("無利息") == 0.0


def test_ratings_use_one_scale_and_both_wordings():
    assert [parsing.rating_notch(value) for value in ("AAA", "AA+", "AA", "AA-", "A+", "BBB-", "Aa1", "A2", "Baa3")] == [1, 2, 3, 4, 5, 10, 2, 6, 10]
    assert [parsing.notch_label(value) for value in (1, 2, 3, 4, 7, 8)] == ["AAA", "AA+", "AA", "AA-", "A-", "BBB+"]
    notes = (
        "株式会社格付投資情報センター（以下Ｒ＆Ｉという。）信用格付：ＡＡ（ダブルＡ） "
        "信用格付は債務履行の確実性についての意見であり事実の表明ではない。 "
        "本社債について、当社はムーディーズ・ジャパン株式会社からA2の信用格付を取得している。"
    )
    assert parsing.parse_ratings(notes) == [{"agency": "R&I", "rating": "AA"}, {"agency": "Moody's", "rating": "A2"}]
    assert build.composite_rating(parsing.parse_ratings(notes)) == ("AA", "R&I", 3.0)


def test_currency_comes_from_the_name_or_the_bracketed_foreign_amount():
    assert parsing.name_currency("2028年満期ユーロ建普通社債") == "EUR"
    assert parsing.name_currency("ユーロ円建転換社債型新株予約権付社債") == "JPY"
    assert parsing.note_currency("( 655,000 千$)") == "USD"
    assert parsing.note_currency("( 111,990 千豪$)") == "AUD"


def test_issuance_tables_become_one_record_per_bond():
    bonds = parsing.parse_issuance(_zip(ISSUANCE_HTML))
    assert len(bonds) == 2
    senior, hybrid = bonds
    assert senior.series == 5 and senior.amount == 10e9 and senior.denomination == 1e8
    assert senior.issue_price == pytest.approx(99.98) and senior.coupon == pytest.approx(0.0125)
    assert (senior.frequency, senior.issue_date, senior.maturity, senior.coupon_kind) == (2, "2026-09-20", "2031-09-19", "fixed")
    assert senior.negative_pledge and senior.seniority == "senior" and senior.offering == "一般募集"
    assert senior.ratings == [{"agency": "R&I", "rating": "A+"}]
    # Two rows of one name cell, the coupon reset, and the call date in the redemption terms.
    assert hybrid.name.endswith("（劣後特約付）") or hybrid.name.endswith("(劣後特約付)")
    assert hybrid.seniority == "hybrid" and {"callable", "deferrable", "subordinated"} <= set(hybrid.features)
    assert hybrid.coupon == pytest.approx(0.02) and hybrid.coupon_kind == "fixed-to-floating"
    assert hybrid.call_date == "2031-09-20" and hybrid.maturity == "2061-09-20" and hybrid.amount == 5e9
    assert hybrid.ratings == [{"agency": "JCR", "rating": "A-"}]


def test_annual_schedule_rows_expand_merged_cells_and_ditto_marks():
    rows = parsing.parse_bond_schedule(_zip(ANNUAL_HTML))
    names = [row.name for row in rows]
    assert names == ["第3回無担保社債", "第4回無担保社債", "第2回無担保社債", "第1回無担保社債(銀行保証付)", "普通社債(注)2"]
    third, fourth, second, guaranteed, dollar = rows
    assert third.issuer == fourth.issuer == "当社" and third.closing == 10e9 and third.current_portion == 10e9
    assert fourth.collateral == "なし" and fourth.maturity == "2031-09-19" and fourth.coupon == pytest.approx(0.004)
    assert second.closing == 0.0 and second.opening == 5e9
    assert guaranteed.issuer == "テスト物流(株)" and guaranteed.coupon is None and guaranteed.closing == 200e6 and "guaranteed" in guaranteed.features
    assert dollar.currency == "USD" and dollar.closing == 15e9


def test_ifrs_notes_supply_the_schedule_when_there_is_no_annexed_one():
    rows = parsing.parse_bond_schedule(_zip(IFRS_HTML))
    assert [row.name for row in rows] == ["第12回 無担保社債", "第14回 無担保社債"]
    first, second = rows
    assert first.maturity == "2027-06-11" and first.coupon == pytest.approx(0.0033) and first.closing == 29_976e6
    assert second.current_portion == 9_995e6


# --- valuation ---------------------------------------------------------------

def test_prices_and_yields_round_trip_and_the_curve_interpolates():
    assert valuation.clean_price(0.02, 5, 0.02) == pytest.approx(100.0, abs=1e-9)
    assert valuation.yield_from_price(0.02, 5, valuation.clean_price(0.02, 5, 0.03)) == pytest.approx(0.03, abs=1e-8)
    book = valuation.CurveBook([("2026-01-05", 1, 0.01), ("2026-01-05", 10, 0.02), ("2026-02-02", 1, 0.011), ("2026-02-02", 10, 0.021)])
    assert book.on("2026-01-31").date == "2026-01-05"
    assert book.on("2026-01-31").at(5.5) == pytest.approx(0.015)
    assert book.on("2025-12-31") is None
    assert book.latest().at(40) == pytest.approx(0.021)


# --- the update step, end to end --------------------------------------------

class FakeClient:
    def __init__(self, archives: dict[str, bytes]):
        self.archives = archives
        self.requests: list[str] = []

    def download_type1(self, doc_id: str) -> bytes:
        self.requests.append(doc_id)
        if doc_id not in self.archives:
            raise EdinetAcquisitionError("EDINET returned an error payload")
        return self.archives[doc_id]


def _sources(tmp_path: Path) -> dict[str, str]:
    db1 = tmp_path / "Base.db"
    with _db(db1) as conn:
        conn.execute("CREATE TABLE DocumentList (docID TEXT, edinetCode TEXT, filerName TEXT, submitDateTime TEXT, periodEnd TEXT, formCode TEXT, docDescription TEXT, docTypeCode TEXT, xbrlFlag TEXT, withdrawalStatus TEXT)")
        conn.executemany("INSERT INTO DocumentList VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
            ("S100ISS1", "E99001", "テスト工業株式会社", "2026-09-14 10:00", "", "120003", "発行登録追補書類（株券､社債券等）", "100", "1", "0"),
            ("S100GONE", "E99001", "テスト工業株式会社", "2025-01-10 10:00", "", "120003", "発行登録追補書類（株券､社債券等）", "100", "1", "0"),
            ("S100ANN1", "E99001", "テスト工業株式会社", "2026-06-25 10:00", "2026-03-31", "030000", "有価証券報告書", "120", "1", "0"),
        ])
    filings = tmp_path / "Filings.db"
    with _db(filings) as conn:
        conn.execute("CREATE TABLE filings (doc_id TEXT PRIMARY KEY, edinet_code TEXT, submitter_name TEXT, period_start TEXT, period_end TEXT, submitted_at TEXT, form_code TEXT, doc_type_code TEXT, archive_content BLOB)")
        conn.executemany("INSERT INTO filings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", [
            ("S100OLD1", "E99001", "テスト工業株式会社", "2024-04-01", "2025-03-31", "2025-06-25T10:00:00", "030000", "1", _zip("<html></html>")),
            ("S100ANN1", "E99001", "テスト工業株式会社", "2025-04-01", "2026-03-31", "2026-06-25T10:00:00", "030000", "1", _zip(ANNUAL_HTML)),
        ])
    db2 = tmp_path / "Standardized.db"
    with _db(db2) as conn:
        conn.execute('CREATE TABLE CompanyInfo (Company_Code TEXT, "Submitter Name" TEXT, Company_Name TEXT, Company_Ticker TEXT, Company_Industry TEXT, Listed TEXT)')
        conn.executemany("INSERT INTO CompanyInfo VALUES (?, ?, ?, ?, ?, ?)", [
            ("E99001", "テスト工業株式会社", "TEST INDUSTRIES", "99990", "Machinery", "Listed company"),
            ("E99002", "比較電機株式会社", "PEER ELECTRIC", "99980", "Electric Appliances", "Listed company"),
        ])
    return {"db1": str(db1), "filings": str(filings), "db2": str(db2), "bonds": str(tmp_path / "Bonds.db")}


def _curve(path: str) -> None:
    ensure_bond_tables(path)
    with _db(path) as conn:
        for day in ("2016-09-20", "2019-09-20", "2021-09-20", "2023-10-10", "2026-09-18", "2026-10-07"):
            conn.executemany("INSERT INTO JGB_Yields VALUES (?, ?, ?)", [(day, 1.0, 0.002), (day, 5.0, 0.004), (day, 10.0, 0.006), (day, 40.0, 0.02)])


def _peers(path: str) -> None:
    """Four recent bonds of another issuer, so the target has peers to be valued against."""
    rows = []
    for index, (years, spread) in enumerate(((4, 0.0030), (5, 0.0035), (6, 0.0040), (7, 0.0045), (5, 0.0050))):
        rows.append((f"E99002-peer{index}", "E99002", "比較電機株式会社", "PEER ELECTRIC", "99980", "Electric Appliances", 1, "比較電機株式会社", 1,
                     f"第{index + 1}回無担保社債", index + 1, "JPY", "senior", "[]", 0.01, "fixed", 2, "2025-10-01", f"{2025 + years}-10-01", "", 0, None,
                     10e9, 100.0, 10e9, "2025-10-01", None, "outstanding", "", "一般募集", 0, '[{"agency": "R&I", "rating": "A"}]', "A", "R&I", 6.0, 0,
                     0.01, years, 0.01 - spread, spread, "S100PEER", "2025-09-25", None, None, "2026-10-01"))
    columns = BOND_COLUMNS[:45]
    with _db(path) as conn:
        conn.executemany(f"INSERT INTO Bonds ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})", rows)


@pytest.fixture
def updated(tmp_path):
    paths = _sources(tmp_path)
    _curve(paths["bonds"])
    client = FakeClient({"S100ISS1": _zip(ISSUANCE_HTML)})
    result = update.update_bonds(
        bonds_db=paths["bonds"], db1_path=paths["db1"], db2_path=paths["db2"], filings_db_path=paths["filings"],
        client=client, curve=False, market_prices=False, today=TODAY,
    )
    return paths, result, client


def test_update_reads_supplements_and_the_latest_schedule(updated):
    paths, result, client = updated
    assert result["issuances"] == {"documents": 1, "bonds": 2, "unavailable": 1, "errors": 0}
    assert result["annual_reports"]["documents"] == 1 and result["annual_reports"]["rows"] == 5
    assert sorted(client.requests) == ["S100GONE", "S100ISS1"]
    with _db(paths["bonds"]) as conn:
        conn.row_factory = sqlite3.Row
        bonds = {row["name"]: dict(row) for row in conn.execute("SELECT * FROM Bonds")}
        stored = conn.execute("SELECT doc_id, status, archive IS NOT NULL FROM Bond_Documents ORDER BY doc_id").fetchall()
    assert [tuple(row) for row in stored] == [("S100ANN1", "parsed", 0), ("S100GONE", "unavailable", 0), ("S100ISS1", "parsed", 1)]

    fourth = bonds["第4回無担保社債"]
    assert fourth["status"] == "outstanding" and fourth["outstanding"] == 15e9 and fourth["outstanding_as_of"] == "2026-03-31"
    assert fourth["company_name"] == "テスト工業株式会社" and fourth["ticker"] == "99990" and fourth["is_parent"] == 1
    # Spread at issue: a 0.4% coupon at par for ten years against that day's 10-year JGB of 0.6%.
    assert fourth["issue_spread"] == pytest.approx(-0.002, abs=2e-5)
    # Bonds the schedule lists as repaid, or that have matured since, are not outstanding.
    assert bonds["第3回無担保社債"]["status"] == "matured"
    assert bonds["第2回無担保社債"]["status"] in ("matured", "redeemed")
    # A subsidiary's bank-guaranteed floating-rate bond: group, private, no fixed coupon.
    guaranteed = bonds["第1回無担保社債(銀行保証付)"]
    assert guaranteed["is_parent"] == 0 and guaranteed["issuer"] == "テスト物流(株)" and guaranteed["private"] == 1 and guaranteed["coupon_kind"] == "floating"
    assert bonds["普通社債(注)2"]["currency"] == "USD" and bonds["普通社債(注)2"]["issue_spread"] is None
    # Issued after the year end: only in its supplement, outstanding at the amount issued, with its rating.
    senior = bonds["テスト工業株式会社第5回無担保社債(社債間限定同順位特約付)"]
    assert senior["outstanding"] == 10e9 and senior["schedule_doc_id"] is None and senior["rating"] == "A+" and senior["issuance_doc_id"] == "S100ISS1"
    assert senior["issue_spread"] == pytest.approx(senior["issue_yield"] - senior["jgb_at_issue"])
    # Earlier bonds of the same ranking carry the issuer's latest rating, marked as inferred.
    assert fourth["rating"] == "A+" and fourth["rating_inferred"] == 1
    hybrid = next(bond for name, bond in bonds.items() if "利払繰延" in name)
    assert hybrid["seniority"] == "hybrid" and hybrid["call_date"] == "2031-09-20" and hybrid["rating"] == "A-" and hybrid["rating_inferred"] == 0
    assert hybrid["issue_tenor"] == pytest.approx(5.0, abs=0.01)


def test_a_second_run_reads_nothing_new_and_overwrite_reads_from_storage(updated):
    paths, _, _ = updated
    client = FakeClient({})
    again = update.update_bonds(bonds_db=paths["bonds"], db1_path=paths["db1"], db2_path=paths["db2"], filings_db_path=paths["filings"], client=client, curve=False, market_prices=False, today=TODAY)
    assert again["issuances"]["documents"] == 0 and again["annual_reports"]["documents"] == 0 and client.requests == []
    reparsed = update.update_bonds(bonds_db=paths["bonds"], db1_path=paths["db1"], db2_path=paths["db2"], filings_db_path=paths["filings"], client=client, curve=False, market_prices=False, reparse=True, today=TODAY)
    # The stored archive is re-read without a download; only the unavailable filing is tried again.
    assert reparsed["issuances"]["bonds"] == 2 and client.requests == ["S100GONE"]


def test_curve_files_from_the_ministry_parse_era_dates():
    content = "国債金利情報,,,(単位 : %)\n基準日,1年,2年,40年\nR8.10.1,1.668,1.939,-\nH31.4.26,-0.15,-0.16,0.5\n".encode("cp932")
    from src.bonds.market import parse_jgb_csv

    assert parse_jgb_csv(content) == [("2026-10-01", 1.0, 0.01668), ("2026-10-01", 2.0, 0.01939), ("2019-04-26", 1.0, -0.0015), ("2019-04-26", 2.0, -0.0016), ("2019-04-26", 40.0, 0.005)]


# --- JSDA reference prices --------------------------------------------------------

def _jsda_csv(*rows: list[str]) -> bytes:
    filler = ["0", "0", "0", "3.063", "98.07", "3.052", "97.93", "3.077", " ", "5", "3.025", "-0.20", "3.048", "-0.19", "3.038", "3.065", "97.99", "-0.20"]
    lines = [",".join([*row, *filler[: 29 - len(row)]]) for row in rows]
    return "\n".join(lines).encode("cp932")


QUOTES = _jsda_csv(
    ["20261007", "01", "013730074", '"国庫短期証券1373"', "20261013", "99.999", "999.999", "99.97", "0.00", '"-----"', '"--"'],
    ["20261007", "40", "041056367", '"ﾃｽﾄ工業 5"', "20310919", "1.25", "1.70", "98.10", "-0.05", '"03/09"', '"20"'],
    ["20261007", "40", "099999999", '"比較電機 5"', "20310919", "1.25", "1.90", "97.20", "0.00", '"03/09"', '"20"'],
)


def test_reference_prices_parse_and_match_by_terms_series_and_name():
    rows = jsda.parse_reference_csv(QUOTES)
    assert [row["code"] for row in rows] == ["041056367", "099999999"]
    daikin = rows[0]
    assert daikin == {**daikin, "price_date": "2026-10-07", "name": "テスト工業5", "maturity": "2031-09-19", "coupon": 1.25, "yield": 1.70, "price": 98.10, "reporters": 5}
    assert jsda.series_from_name("ﾀﾞｲｷﾝ工業 36") == 36 and jsda.series_from_name("商工中金永劣2") == 2
    bonds = [
        {"bond_id": "a", "company_name": "テスト工業株式会社", "is_parent": 1, "series": 5, "maturity": "2031-09-19", "coupon": 0.0125},
        {"bond_id": "b", "company_name": "別会社株式会社", "is_parent": 1, "series": None, "maturity": "2031-09-19", "coupon": 0.0125},
        {"bond_id": "c", "company_name": "テスト工業株式会社", "is_parent": 1, "series": 6, "maturity": "2031-09-19", "coupon": 0.0125},
    ]
    matches = jsda.match_quotes(bonds, rows)
    # Two issuers share the terms; the name decides. A different series, or an unrelated name, matches nothing.
    assert {key: value["code"] for key, value in matches.items()} == {"a": "041056367"}


def test_reference_prices_give_market_yields_and_spreads(updated):
    paths, _, _ = updated
    fetched = []

    def fetch(days, skip):
        fetched.append((days, set(skip)))
        return jsda.parse_reference_csv(QUOTES), {"dates": ["2026-10-07"]}

    conn = connect_write(paths["bonds"])
    try:
        assert update.update_market_prices(conn, 3, fetch=fetch)["rows"] == 2
        update.update_market_prices(conn, 3, fetch=fetch)
        update.rebuild(conn, paths["db2"], today=TODAY)
    finally:
        conn.close()
    assert fetched == [(3, set()), (3, {"2026-10-07"})]
    data = service.company_bonds(paths["bonds"], "E99001", today=TODAY)
    senior = next(bond for bond in data["bonds"] if bond["series"] == 5)
    assert senior["jsda_code"] == "041056367" and senior["market_price"] == 98.10 and senior["market_date"] == "2026-10-07"
    years = valuation.year_fraction("2026-10-07", "2031-09-19")
    assert senior["market_yield"] == pytest.approx(valuation.yield_from_price(0.0125, years, 98.10), abs=1e-7)
    assert senior["spread_basis"] == "market" and senior["spread"] == senior["market_spread"]
    detail = service.bond_detail(paths["bonds"], senior["bond_id"], today=TODAY)
    assert detail["market_history"] == [{"date": "2026-10-07", "price": 98.10, "yield": pytest.approx(0.017)}]


# --- read side ------------------------------------------------------------------

def test_company_view_totals_ladder_and_documents(updated):
    paths, _, _ = updated
    data = service.company_bonds(paths["bonds"], "E99001", today=TODAY)
    summary = data["summary"]
    # Yen bonds outstanding across the group: the 4th (15bn), the 5th (10bn), the hybrid (5bn), and the
    # subsidiary's 0.2bn; the dollar bond is counted apart.
    assert summary["total_outstanding"] == pytest.approx(30.2e9)
    assert summary["foreign_currency_count"] == 1 and summary["rating"] == "A+"
    assert {item["year"] for item in data["ladder"]} >= {2031, 2061}
    assert {document["kind"] for document in data["documents"]} == {"annual", "issuance"}
    issuance = next(document for document in data["documents"] if document["kind"] == "issuance")
    assert issuance["stored"] and issuance["edinet_url"].endswith("S100ISS1,,,")
    senior = next(bond for bond in data["bonds"] if bond["series"] == 5)
    assert senior["label"] == "第5回無担保社債" and senior["model_price"] is not None and senior["horizon_to"] == "maturity"


def test_market_view_and_bond_detail_compare_with_other_issuers(updated):
    paths, _, _ = updated
    _peers(paths["bonds"])
    market = service.universe(paths["bonds"], today=TODAY)
    assert set(market["companies"]) == {"E99001", "E99002"}
    assert all("company_name" not in bond for bond in market["bonds"])
    assert not any(bond.get("edinet_code") == "E99001" and bond.get("private") for bond in market["bonds"])
    target = next(bond for bond in market["bonds"] if bond["edinet_code"] == "E99001" and bond.get("series") == 5)
    detail = service.bond_detail(paths["bonds"], target["bond_id"], today=TODAY)
    assert detail["bond"]["bond_id"] == target["bond_id"] and detail["issuance"]["negative_pledge"] == 1
    assert [item["edinet_code"] for item in detail["similar"]] == ["E99002"]
    peer = detail["valuation"]
    assert peer["peer_count"] == 5 and peer["peer_spread"] == pytest.approx(0.004) and peer["quoted_peers"] == 0
    assert detail["bond"]["spread_basis"] == "issue"
    assert peer["relative_spread"] == pytest.approx(detail["bond"]["issue_spread"] - 0.004)
    assert service.bond_detail(paths["bonds"], "E99001-missing", today=TODAY) is None


# --- API ------------------------------------------------------------------------------

@pytest.fixture
def client(updated, monkeypatch, tmp_path):
    paths, _, _ = updated
    monkeypatch.setattr(bonds_api, "get_bonds_db", lambda: paths["bonds"])
    app = FastAPI()
    app.include_router(bonds_api.router)
    install_security(app, AppSettings(auth_mode="accounts", registration_mode="open", auth_db_path=tmp_path / "auth.db"))
    service_ = app.state.auth_service
    service_.register("alice", PASSWORD)
    headers = {"Authorization": f"Bearer {service_.login('alice', PASSWORD).tokens.access_token}"}
    return TestClient(app), headers


def test_api_serves_company_market_bond_and_stored_documents(client):
    http, headers = client
    assert http.get("/api/bonds/status", headers=headers).json()["companies"] == 1
    company = http.get("/api/bonds/company/E99001", headers=headers).json()
    assert company["summary"]["outstanding_count"] >= 3
    market = http.get("/api/bonds/market", headers={**headers, "Accept-Encoding": "gzip"})
    assert market.status_code == 200 and market.headers["content-type"].startswith("application/json")
    payload = market.json()
    assert payload["bonds"] and "E99001" in payload["companies"]
    bond_id = next(bond["bond_id"] for bond in payload["bonds"] if bond.get("series") == 5)
    assert http.get(f"/api/bonds/bond/{bond_id}", headers=headers).json()["bond"]["rating"] == "A+"
    assert http.get("/api/bonds/bond/E99001-nothing", headers=headers).status_code == 404
    archive = http.get("/api/bonds/documents/S100ISS1", headers=headers)
    assert archive.status_code == 200 and archive.content.startswith(b"PK")
    assert http.get("/api/bonds/documents/S100ANN1", headers=headers).status_code == 404
    assert http.get("/api/bonds/documents/S100ISS1").status_code == 401


def test_api_without_a_bond_database_says_how_to_create_one(monkeypatch, tmp_path):
    monkeypatch.setattr(bonds_api, "get_bonds_db", lambda: str(tmp_path / "missing.db"))
    app = FastAPI()
    app.include_router(bonds_api.router)
    http = TestClient(app)
    assert http.get("/api/bonds/status").json()["bonds"] == 0
    response = http.get("/api/bonds/company/E99001")
    assert response.status_code == 503 and "Update bonds" in response.json()["detail"]


def test_market_payload_is_gzipped_on_request(updated, monkeypatch):
    paths, _, _ = updated
    monkeypatch.setattr(bonds_api, "get_bonds_db", lambda: paths["bonds"])

    class Request:
        headers = {"accept-encoding": "gzip, deflate"}

    response = bonds_api.market(Request())
    assert response.headers["content-encoding"] == "gzip"
    assert json.loads(gzip.decompress(response.body))["bonds"]


def test_the_pipeline_registers_the_bond_step():
    from src.orchestrator.common import build_step_registry

    registry = build_step_registry()
    definitions = registry[1] if isinstance(registry, tuple) else registry
    names = set(definitions) if isinstance(definitions, dict) else {definition.name for definition in definitions}
    assert "update_bonds" in names
