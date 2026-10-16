"""Major shareholders and officers from annual reports: parsing, naming, who is who, the update step, and the API."""

from __future__ import annotations

import io
import sqlite3
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.ownership.api as ownership_api
from src.ownership import careers, service, update
from src.ownership.directory import FilerDirectory, legal_key
from src.ownership.names import (
    account_label,
    association,
    english_person_name,
    fold_name,
    is_individual,
    known_family,
    known_name,
    person_key,
    split_holder,
    title_en,
)
from src.ownership.parsing import career_entries, parse_annual_report, person_name, term_end

TODAY = "2026-10-11"
_NAMESPACE = "jpcrp030000-asr_{code}-000"


@contextmanager
def _db(path: str | Path) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(str(path))
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _zip(files: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as bundle:
        for name, text in files.items():
            bundle.writestr(name, text)
    return buffer.getvalue()


def _context(context_id: str, instant: str, member: str = "") -> str:
    scenario = f'<xbrli:scenario><xbrldi:explicitMember dimension="jpcrp_cor:Axis">{member}</xbrldi:explicitMember></xbrli:scenario>' if member else ""
    return (
        f'<xbrli:context id="{context_id}"><xbrli:entity><xbrli:identifier scheme="http://disclosure.edinet-fsa.go.jp">E</xbrli:identifier></xbrli:entity>'
        f"<xbrli:period><xbrli:instant>{instant}</xbrli:instant></xbrli:period>{scenario}</xbrli:context>"
    )


def _fact(name: str, context: str, text: str, *, scale: str | None = None, block: bool = False) -> str:
    if scale is not None:
        return f'<td><ix:nonFraction name="jpcrp_cor:{name}" contextRef="{context}" unitRef="shares" scale="{scale}" format="ixt:numdotdecimal">{text}</ix:nonFraction></td>'
    escape = ' escape="true"' if block else ""
    return f'<td><ix:nonNumeric name="jpcrp_cor:{name}" contextRef="{context}"{escape}>{text}</ix:nonNumeric></td>'


def _officer_cells(code: str, officer: dict[str, Any], suffix: str = "") -> str:
    context = f"FilingDateInstant_{_NAMESPACE.format(code=code)}{officer['member']}Member"
    base = "InformationAboutDirectorsAndCorporateAuditors"
    career = "<table>" + "".join(f"<tr><td><p>{date}</p></td><td><p>{text}</p></td></tr>" for date, text in officer.get("career", [])) + "</table>"
    shares = officer.get("shares")
    held = (
        f'<td><ix:nonFraction name="jpcrp_cor:NumberOfSharesHeldOrdinaryShares{base}{suffix}" contextRef="{context}" unitRef="shares" xsi:nil="true"></ix:nonFraction></td>'
        if shares is None else _fact(f"NumberOfSharesHeldOrdinaryShares{base}{suffix}", context, f"{shares:,}", scale="3")
    )
    return "<tr>" + "".join((
        _fact(f"OfficialTitleOrPosition{base}{suffix}", context, officer["title"]),
        _fact(f"Name{base}{suffix}", context, officer["name"]),
        f'<td><ix:nonNumeric name="jpcrp_cor:DateOfBirth{base}{suffix}" contextRef="{context}" format="ixt:dateyearmonthdaycjk">{officer["birth"]}</ix:nonNumeric>生</td>',
        _fact(f"CareerSummary{base}{suffix}TextBlock", context, career, block=True),
        _fact(f"TermOfOffice{base}{suffix}", context, officer.get("term", "(注)２")),
        held,
    )) + "</tr>"


def annual_report(
    code: str,
    period_end: str,
    filed: str,
    holders: list[tuple[str, str, int, float]],
    officers: list[dict[str, Any]],
    *,
    proposed: list[dict[str, Any]] | None = None,
    statistics: bool = True,
) -> bytes:
    """An annual report's archive as EDINET lays it out: a header page with contexts, section 4, and label files.

    ``holders`` are (name, address, thousands of shares, percentage); officers' shares are in thousands too.
    """
    namespace = _NAMESPACE.format(code=code)
    contexts = [_context("CurrentYearInstant", period_end), _context("FilingDateInstant", filed), _context("CurrentYearInstant_OrdinaryShareMember", period_end, "jpcrp_cor:OrdinaryShareMember")]
    rows = []
    for rank, (name, address, shares, ratio) in enumerate(holders, start=1):
        context = f"CurrentYearInstant_No{rank}MajorShareholdersMember"
        contexts.append(_context(context, period_end, f"jpcrp_cor:No{rank}MajorShareholdersMember"))
        rows.append("<tr>" + _fact("NameMajorShareholders", context, name) + _fact("AddressMajorShareholders", context, address)
                    + _fact("NumberOfSharesHeld", context, f"{shares:,}", scale="3") + _fact("ShareholdingRatio", context, f"{ratio:.2f}", scale="-2") + "</tr>")
    total = "<tr><td>計</td><td>－</td>" + _fact("NumberOfSharesHeld", "CurrentYearInstant", f"{sum(row[2] for row in holders):,}", scale="3") + _fact("ShareholdingRatio", "CurrentYearInstant", f"{sum(row[3] for row in holders):.2f}", scale="-2") + "</tr>"
    people = [*officers, *(proposed or [])]
    for officer in people:
        contexts.append(_context(f"FilingDateInstant_{namespace}{officer['member']}Member", filed, f"{namespace}:{officer['member']}Member"))
    contexts.extend(_context(f"FilingDateInstant_Row{row}Member", filed, f"jpcrp_cor:Row{row}Member") for row in (1, 2))
    footnotes = (
        _fact("FootnotesDirectorsAndCorporateAuditorsTextBlock", "FilingDateInstant_Row1Member", "（注）１ 取締役 佐藤 次郎は、社外取締役であります。", block=True)
        + _fact("FootnotesDirectorsAndCorporateAuditorsTextBlock", "FilingDateInstant_Row2Member", f"２ 取締役の任期は、{filed[:4]}年６月25日開催の定時株主総会の終結の時から{int(period_end[:4]) + 1}年３月期に係る定時株主総会の終結の時までであります。", block=True)
    )
    numbers = ""
    if statistics:
        numbers = (
            '<p><ix:nonFraction name="jpcrp_cor:NumberOfShareholdersTotal" contextRef="CurrentYearInstant_OrdinaryShareMember" unitRef="pure" scale="0" format="ixt:numdotdecimal">4,321</ix:nonFraction>'
            '<ix:nonFraction name="jpcrp_cor:PercentageOfShareholdingsFinancialInstitutions" contextRef="CurrentYearInstant_OrdinaryShareMember" unitRef="pure" scale="-2" format="ixt:numdotdecimal">30.00</ix:nonFraction>'
            '<ix:nonFraction name="jpcrp_cor:PercentageOfShareholdingsForeignersOtherThanIndividuals" contextRef="CurrentYearInstant_OrdinaryShareMember" unitRef="pure" scale="-2" format="ixt:numdotdecimal">20.00</ix:nonFraction>'
            '<ix:nonFraction name="jpcrp_cor:PercentageOfShareholdingsForeignIndividuals" contextRef="CurrentYearInstant_OrdinaryShareMember" unitRef="pure" scale="-2" format="ixt:numdotdecimal">0.50</ix:nonFraction>'
            '<ix:nonFraction name="jpcrp_cor:PercentageOfShareholdingsIndividualsAndOthers" contextRef="CurrentYearInstant_OrdinaryShareMember" unitRef="pure" scale="-2" format="ixt:numdotdecimal">49.50</ix:nonFraction>'
            '<ix:nonFraction name="jpcrp_cor:NumberOfShareholdersFinancialInstitutions" contextRef="CurrentYearInstant_OrdinaryShareMember" unitRef="pure" scale="0" format="ixt:numdotdecimal">25</ix:nonFraction>'
            '<ix:nonFraction name="jpcrp_cor:NumberOfIssuedSharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc" contextRef="FilingDateInstant" unitRef="shares" scale="0" format="ixt:numdotdecimal">100,000,000</ix:nonFraction>'
            '<ix:nonFraction name="jpcrp_cor:TotalNumberOfSharesHeldTreasurySharesEtc" contextRef="CurrentYearInstant" unitRef="shares" scale="0" format="ixt:numdotdecimal">2,000,000</ix:nonFraction>'
            '<ix:nonFraction name="jpcrp_cor:ShareholdingRatioTreasurySharesEtc" contextRef="CurrentYearInstant" unitRef="pure" scale="-2" format="ixt:numdotdecimal">2.00</ix:nonFraction></p>'
        )
    slate = ""
    if proposed:
        slate = "<table>" + "".join(_officer_cells(code, officer, "Proposal") for officer in proposed) + "</table>"
    body = (
        "<html><body>" + numbers
        + '<ix:nonNumeric name="jpcrp_cor:MajorShareholdersTextBlock" contextRef="FilingDateInstant" escape="true"><table>' + "".join(rows) + total + "</table></ix:nonNumeric>"
        + '<ix:nonNumeric name="jpcrp_cor:InformationAboutOfficersTextBlock" contextRef="FilingDateInstant" escape="true">'
        + '<p>男性<ix:nonFraction name="jpcrp_cor:NumberOfMaleDirectorsAndOtherOfficers" contextRef="FilingDateInstant" unitRef="pure" scale="0">2</ix:nonFraction>名 女性'
        + '<ix:nonFraction name="jpcrp_cor:NumberOfFemaleDirectorsAndOtherOfficers" contextRef="FilingDateInstant" unitRef="pure" scale="0">1</ix:nonFraction>名</p>'
        + "<table>" + "".join(_officer_cells(code, officer) for officer in officers) + "</table>" + "<table><tr>" + footnotes + "</tr></table>" + slate
        + "</ix:nonNumeric></body></html>"
    )

    def linkbase(language: str, key: str) -> str:
        parts = []
        for officer in people:
            element = f"{namespace}_{officer['member']}Member"
            parts.append(
                f'<link:loc xlink:type="locator" xlink:href="x.xsd#{element}" xlink:label="{officer["member"]}Member"/>'
                f'<link:label xml:lang="{language}" xlink:type="resource" xlink:label="label_{officer["member"]}Member" xlink:role="http://www.xbrl.org/2003/role/label">{officer[key]}</link:label>'
                f'<link:label xml:lang="{language}" xlink:type="resource" xlink:label="label_{officer["member"]}Member_2" xlink:role="http://www.xbrl.org/2003/role/verboseLabel">{officer[key]} [member]</link:label>'
                f'<link:labelArc xlink:type="arc" xlink:arcrole="http://www.xbrl.org/2003/arcrole/concept-label" xlink:from="{officer["member"]}Member" xlink:to="label_{officer["member"]}Member"/>'
                f'<link:labelArc xlink:type="arc" xlink:arcrole="http://www.xbrl.org/2003/arcrole/concept-label" xlink:from="{officer["member"]}Member" xlink:to="label_{officer["member"]}Member_2"/>'
            )
        return "<link:linkbase>" + "".join(parts) + "</link:linkbase>"

    stem = f"XBRL/PublicDoc/jpcrp030000-asr-001_{code}-000_{period_end}_01_{filed}"
    return _zip({
        f"XBRL/PublicDoc/0000000_header_jpcrp030000-asr-001_{code}-000_ixbrl.htm": "<html><body><ix:header><ix:resources>" + "".join(contexts) + "</ix:resources></ix:header></body></html>",
        f"XBRL/PublicDoc/0101010_honbun_jpcrp030000-asr-001_{code}-000_ixbrl.htm": "<html><body><p>第１ 企業の概況</p></body></html>",
        f"XBRL/PublicDoc/0104010_honbun_jpcrp030000-asr-001_{code}-000_ixbrl.htm": body,
        f"{stem}_lab-en.xml": linkbase("en", "label"),
        f"{stem}_lab.xml": linkbase("ja", "label_ja"),
    })


YAMADA = {
    "member": "YamadaTaro", "name": "山&#160; 田&#160; 太&#160; 郎", "label": "Yamada, Taro", "label_ja": "山田　太郎", "title": "代表取締役<br/>社長", "birth": "1960年１月２日",
    "shares": 5000, "career": [("1982年４月", "当社入社"), ("2010年６月", "当社取締役"), ("同 年７月", "比較電機㈱取締役"), ("2018年６月", "当社代表取締役社長（現任）")],
}
SATO = {
    "member": "SatoJiro", "name": "佐 藤 次 郎", "label": "Sato, Jiro", "label_ja": "佐藤　次郎", "title": "取締役", "birth": "1955年３月４日",
    "shares": 10, "career": [("1978年４月", "㈱親会社ホールディングス入社"), ("2020年６月", "当社取締役（現任）")],
}
SMITH = {"member": "GeorgeSmith", "name": "George Smith （注）３", "label": "George Smith", "label_ja": "George Smith", "title": "常勤監査役", "birth": "1970年12月31日", "shares": None, "career": []}
TAKAHASHI = {"member": "TakahashiSaburo", "name": "髙橋 三郎", "label": "TAKAHASHI, SABURO", "label_ja": "髙橋　三郎", "title": "専務取締役 営業本部長", "birth": "1950年５月６日", "shares": 300, "career": [("2001年４月", "当社入社")]}
TANAKA = {"member": "TanakaShiro", "name": "田中 四郎", "label": "Shiro Tanaka", "label_ja": "田中　四郎", "title": "取締役 常務執行役員", "birth": "1968年７月８日", "shares": 20, "career": [("1992年４月", "当社入社")]}

HOLDERS_2026 = [
    ("日本マスタートラスト信託銀行㈱（信託口）", "東京都港区赤坂一丁目８番１号", 12500, 12.50),
    ("㈱親会社ホールディングス", "大阪府大阪市北区一丁目１番１号", 10000, 10.00),
    ("山田太郎", "東京都渋谷区", 5000, 5.00),
    ("STATE STREET BANK AND TRUST COMPANY 505001 （常任代理人 ㈱みずほ銀行決済営業部）", "ONE CONGRESS STREET, BOSTON （東京都港区港南二丁目15番１号）", 3200, 3.20),
    ("テスト工業従業員持株会", "東京都千代田区一丁目１番", 2000, 2.00),
    ("鈴木花子", "神奈川県横浜市", 1500, 1.50),
]
HOLDERS_2025 = [
    ("日本マスタートラスト信託銀行株式会社（信託口）", "東京都港区浜松町二丁目11番３号", 11000, 11.00),
    ("株式会社親会社ホールディングス", "大阪府大阪市北区一丁目１番１号", 10000, 10.00),
    ("山田 太郎", "東京都渋谷区", 5500, 5.50),
    ("鈴木花子", "神奈川県横浜市", 1400, 1.40),
]


# --- parsing ------------------------------------------------------------------

def test_shareholders_are_read_with_shares_and_ratios_in_units():
    report = parse_annual_report(annual_report("E99001", "2026-03-31", "2026-06-20", HOLDERS_2026, [YAMADA, SATO, SMITH]))
    first = report.shareholders[0]
    assert (first.rank, first.name, first.address) == (1, "日本マスタートラスト信託銀行(株)(信託口)", "東京都港区赤坂一丁目8番1号")
    assert first.shares == 12_500_000 and first.ratio == pytest.approx(0.125)
    assert [row.rank for row in report.shareholders] == [1, 2, 3, 4, 5, 6]
    assert report.shareholders_as_of == "2026-03-31"
    # The table's own total row is not a shareholder.
    assert report.top_shares == 34_200_000 and report.top_ratio == pytest.approx(0.342)
    assert report.shareholder_count == 4321 and report.issued_shares == 100_000_000
    assert report.treasury_shares == 2_000_000 and report.treasury_ratio == pytest.approx(0.02)
    assert {item["key"]: item["ratio"] for item in report.distribution} == {
        "financial_institutions": pytest.approx(0.30), "foreign_investors": pytest.approx(0.20), "foreign_individuals": pytest.approx(0.005), "individuals": pytest.approx(0.495),
    }
    assert report.distribution[0]["holders"] == 25


def test_officers_are_read_with_names_careers_terms_and_outside_status():
    report = parse_annual_report(annual_report("E99001", "2026-03-31", "2026-06-20", HOLDERS_2026, [YAMADA, SATO, SMITH]))
    president, outside, auditor = report.officers
    # The padded name is one name, split as the filer's label splits it; the label gives the English spelling.
    assert (president.name, president.name_en, president.member) == ("山田 太郎", "Yamada, Taro", "YamadaTaroMember")
    assert president.title == "代表取締役 社長" and president.birth_date == "1960-01-02" and president.shares == 5_000_000
    assert president.career == [("1982-04", "当社入社"), ("2010-06", "当社取締役"), ("2010-07", "比較電機(株)取締役"), ("2018-06", "当社代表取締役社長(現任)")]
    # "(注)２" points at the footnote that says when the term ends.
    assert president.term_end == "2027-03" and president.term_start == "2026-06-25" and president.term == ""
    assert outside.outside and not president.outside
    assert auditor.name == "George Smith" and auditor.shares is None
    assert report.officers_as_of == "2026-06-20" and (report.men, report.women) == (2, 1)
    assert all(not officer.proposed and officer.source == "tagged" for officer in report.officers)


def test_a_report_filed_before_the_meeting_also_carries_the_proposed_slate():
    report = parse_annual_report(annual_report("E99001", "2026-03-31", "2026-06-20", HOLDERS_2026, [YAMADA, SATO], proposed=[YAMADA, TANAKA]))
    assert [(officer.name, officer.proposed) for officer in report.officers] == [("山田 太郎", False), ("佐藤 次郎", False), ("山田 太郎", True), ("田中 四郎", True)]
    assert report.officers[3].name_en == "Shiro Tanaka"


UNTAGGED = """<html><body>
<ix:nonNumeric name="jpcrp_cor:InformationAboutOfficersTextBlock" contextRef="FilingDateInstant" escape="true">
<p>男性３名 女性－名 （役員のうち女性の比率－％）</p>
<table>
<tr><td>役名</td><td>職名</td><td>氏名</td><td>生年月日</td><td colspan="2">略歴</td><td>任期</td><td>所有株式数<br/>(千株)</td></tr>
<tr><td rowspan="2">取締役社長</td><td rowspan="2">代表取締役</td><td rowspan="2">内 山&#160; 竹 志</td><td rowspan="2">昭和21年８月17日生</td><td>昭和44年４月</td><td>当社入社</td><td rowspan="2">(注)２</td><td rowspan="2">普通株式<br/>58</td></tr>
<tr><td>平成25年６月</td><td>当社取締役社長就任</td></tr>
<tr><td>取締役</td><td>社外取締役</td><td>いしだ しょうじ 石田 昭二 (注)１</td><td>1953年10月13日生</td><td>2015年６月</td><td>当社取締役就任</td><td>同上</td><td>－</td></tr>
<tr><td>監査役</td><td>常勤</td><td>水野明人 (昭和24年８月25日生)</td><td></td><td>平成20年４月</td><td>当社入社</td><td>(注)３</td><td>0.5</td></tr>
<tr><td>計</td><td></td><td></td><td></td><td></td><td></td><td></td><td>58</td></tr>
</table>
<p>(注)１ 取締役 石田昭二は、社外取締役であります。</p>
<p>２ 取締役の任期は、平成29年３月期に係る定時株主総会終結の時から平成30年３月期に係る定時株主総会終結の時までであります。</p>
</ix:nonNumeric></body></html>"""


def test_a_report_that_does_not_tag_officers_is_read_from_its_table():
    report = parse_annual_report(_zip({"XBRL/PublicDoc/0104010_honbun_x_ixbrl.htm": UNTAGGED}))
    first, second, third = report.officers
    assert (first.name, first.title, first.birth_date, first.shares, first.source) == ("内山竹志", "取締役社長 代表取締役", "1946-08-17", 58_000, "table")
    assert first.career == [("1969-04", "当社入社"), ("2013-06", "当社取締役社長就任")]
    assert first.term_end == "2018-03"
    # A reading before the name, a footnote mark after it, and "same as above" in the term column.
    assert (second.name, second.outside, second.term_end, second.shares) == ("石田 昭二", True, "2018-03", None)
    # A date of birth written in the name cell.
    assert (third.name, third.birth_date, third.shares, third.term) == ("水野明人", "1949-08-25", 500, "")
    assert (report.men, report.women) == (3, 0)


def test_names_dates_and_terms_are_normalised():
    assert person_name("豊 田 章 男") == "豊田章男"
    assert person_name("渡 邉 光一郎") == "渡邉 光一郎"
    assert person_name("金子陽三 (戸籍名:藤澤陽三)") == "金子陽三"
    assert person_name("Christopher P．Reynolds") == "Christopher P.Reynolds"
    assert career_entries(["1992年４月", "トヨタ自動車㈱入社", "平成12年６月 同社取締役", "同年７月 同社常務取締役", "（現任）"]) == [
        ("1992-04", "トヨタ自動車(株)入社"), ("2000-06", "同社取締役"), ("2000-07", "同社常務取締役 (現任)"),
    ]
    # Dates in one column and texts in the next are paired in order.
    assert career_entries(["1975年４月", "1984年７月", "当社入社", "当社取締役"]) == [("1975-04", "当社入社"), ("1984-07", "当社取締役")]
    assert term_end("2025年６月12日開催の定時株主総会の終結の時から2026年３月期にかかる定時株主総会の終結の時まで") == "2026-03"
    assert term_end("令和７年６月から令和９年３月期に係る定時株主総会終結の時まで") == "2027-03"
    assert term_end("選任後２年以内") is None


# --- names --------------------------------------------------------------------

@pytest.mark.parametrize(("filed", "holder", "account", "label"), [
    ("日本マスタートラスト信託銀行㈱（信託口）", "日本マスタートラスト信託銀行(株)", "信託口", "Trust account"),
    ("㈱日本カストディ銀行（信託口４）", "(株)日本カストディ銀行", "信託口4", "Trust account 4"),
    ("日本生命保険（相）", "日本生命保険(相)", "", ""),
    ("STATE STREET BANK AND TRUST COMPANY 505001 （常任代理人 ㈱みずほ銀行決済営業部）", "STATE STREET BANK AND TRUST COMPANY", "505001; 常任代理人 (株)みずほ銀行決済営業部", "Account 505001; Standing proxy: Mizuho Bank, Ltd."),
    ("㈱日本カストディ銀行（三井住友信託銀行再信託分・トヨタ自動車㈱退職給付信託口）", "(株)日本カストディ銀行", "三井住友信託銀行再信託分・トヨタ自動車(株)退職給付信託口", "Retirement benefit trust account"),
    ("特定有価証券信託受託者 株式会社SMBC信託銀行", "株式会社SMBC信託銀行", "特定有価証券信託受託者", "As trustee"),
    ("山田太郎", "山田太郎", "", ""),
])
def test_a_filed_name_is_split_into_the_holder_and_its_account(filed, holder, account, label):
    assert split_holder(filed) == (holder, account)
    assert account_label(account) == label


def test_well_known_institutions_have_one_english_name_however_they_are_spelled():
    assert known_family("日本マスタートラスト 信託銀行株式会社")[0] == "The Master Trust Bank of Japan, Ltd."
    assert known_family("(株)日本カストディ銀行")[0] == "Custody Bank of Japan, Ltd."
    # A bank merged into another keeps its own name for the years it existed.
    assert known_family("日本トラスティ・サービス信託銀行株式会社")[0].startswith("Japan Trustee Services Bank")
    assert known_family("ステート ストリート バンク アンド トラスト カンパニー") == ("State Street Bank and Trust Company", "", "")
    # What follows the institution's name is a detail of the holding.
    assert known_family("JP MORGAN CHASE BANK 385632")[0] == "JPMorgan Chase Bank, N.A."
    assert known_family("THE CHASE MANHATTAN BANK, N.A. LONDON SPECIAL ACCOUNT NO.1") == ("JPMorgan Chase Bank, N.A.", "", "LONDON SPECIAL ACCOUNT NO.1")
    assert known_family("みずほ信託銀行株式会社 退職給付信託 みずほ銀行口")[2] == "退職給付信託みずほ銀行口"
    assert known_family("株式会社北日本銀行") is None and known_family("山田太郎") is None
    assert known_name("富山県") == "Toyama Prefecture" and known_name("財務大臣").startswith("Minister of Finance")
    assert known_name("株式会社UH Partners 3") == "UH Partners 3 K.K."
    assert known_name("株式会社榎本武平商店") is None
    assert association("トヨタ自動車従業員持株会") == ("トヨタ自動車", "Employee Shareholding Association")


@pytest.mark.parametrize(("name", "expected"), [
    ("田中伸明", True), ("寺田 英司", True), ("ヨシダ トモヒロ", True), ("GEORGE OLCOTT", True),
    ("トヨタ不動産㈱", False), ("光通信株式会社", False), ("富山県", False), ("学校法人帝京大学", False), ("MSIP CLIENT SECURITIES", False),
    ("ステートストリートバンクアンドトラストカンパニー", False), ("京都中央信用金庫", False),
])
def test_individuals_are_told_from_organisations_by_their_names(name, expected):
    assert is_individual(name) is expected


def test_people_are_identified_by_folded_name_and_date_of_birth():
    assert fold_name("髙橋 三郎") == fold_name("高橋三郎") and fold_name("Christopher P．Reynolds") == fold_name("christopher p. reynolds")
    assert person_key("渡 邉 光一郎", "1953-04-16") == person_key("渡辺光一郎", "1953-04-16") != person_key("渡辺光一郎", "1960-01-01")
    assert english_person_name("Toyoda, Akio") == "Akio Toyoda"
    assert english_person_name("SUGANO, NORITAKA") == "Noritaka Sugano"
    assert english_person_name("George Olcott") == "George Olcott"
    # Without a usable label, the element name the filer coined is split into its words.
    assert english_person_name("豊田 章男", "ToyodaAkioMember") == "Toyoda Akio"
    assert english_person_name("", "Row1Member") == ""


@pytest.mark.parametrize(("title", "english"), [
    ("取締役", "Director"),
    ("代表取締役 社長", "President and Representative Director"),
    ("取締役社長 (代表取締役)", "President and Representative Director"),
    ("取締役会長", "Chairman and Director"),
    ("代表取締役 会長兼社長", "Chairman and President, Representative Director"),
    ("代表取締役 副社長執行役員", "Executive Vice President and Representative Director"),
    ("取締役頭取 (代表取締役)", "President and Representative Director"),
    ("常務取締役", "Managing Director"),
    ("専務取締役 営業本部長", "Senior Managing Director, Head of Sales Division"),
    ("取締役 常務執行役員", "Director and Managing Executive Officer"),
    ("取締役 (監査等委員)", "Director (Audit & Supervisory Committee Member)"),
    ("取締役 (常勤監査等委員)", "Director (Audit & Supervisory Committee Member, full-time)"),
    ("社外取締役", "Outside Director"),
    ("取 締 役 ―", "Director"),
    ("取締役 (注)1", "Director"),
    ("常勤監査役", "Full-time Audit & Supervisory Board Member"),
    ("監査役 (非常勤)", "Audit & Supervisory Board Member (part-time)"),
    ("代表執行役社長", "President and Representative Executive Officer"),
    ("常務執行役", "Managing Executive Officer"),
    ("取締役 専務執行役", "Director, Senior Managing Executive Officer"),
    ("代表取締役社長兼CEO", "President and Representative Director, CEO"),
    ("取締役 取締役会議長", "Director, Chair of the Board"),
    ("取締役 経営企画部長", "Director, General Manager, Corporate Planning"),
    ("支配人", ""),
    ("", ""),
])
def test_titles_are_rewritten_in_english_from_the_roles_they_state(title, english):
    assert title_en(title) == english


# --- filers and careers -------------------------------------------------------

def _companies(conn: sqlite3.Connection) -> None:
    conn.execute('CREATE TABLE CompanyInfo (Company_Code TEXT, "Submitter Name" TEXT, Company_Name TEXT, Company_Ticker TEXT, Company_Industry TEXT, Listed TEXT, "Type of Submitter" TEXT, Province TEXT)')
    conn.executemany("INSERT INTO CompanyInfo VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
        ("E99001", "テスト工業株式会社", "TEST INDUSTRIES", "99990", "Machinery", "Listed company", "内国法人・組合", "千代田区一丁目１番"),
        ("E99002", "比較電機株式会社", "PEER ELECTRIC", "99980", "Electric Appliances", "Listed company", "内国法人・組合", "大阪市北区二丁目"),
        ("E99003", "株式会社親会社ホールディングス", "PARENT HOLDINGS", "", "Wholesale Trade", "Unlisted company", "内国法人・組合", "大阪市北区一丁目１番１号"),
        ("E99004", "新社名グループ株式会社", "NEW NAME GROUP", "99970", "Services", "Listed company", "内国法人・組合", "港区"),
        ("E99005", "旧社名株式会社", "", "", "", "", "内国法人・組合", "新宿区"),
        ("E99006", "株式会社平和", "HEIWA", "99960", "Machinery", "Listed company", "内国法人・組合", "台東区"),
        ("E99007", "山田　太郎", "", "", "", "", "個人（組合発行者を除く）", ""),
    ])
    conn.execute("CREATE TABLE DocumentList (docID TEXT, edinetCode TEXT, filerName TEXT, submitDateTime TEXT)")
    conn.executemany("INSERT INTO DocumentList VALUES (?, ?, ?, ?)", [
        ("D1", "E99004", "旧社名株式会社", "2018-06-20 10:00"), ("D2", "E99004", "旧社名株式会社", "2020-06-20 10:00"),
        ("D3", "E99004", "新社名グループ株式会社", "2021-06-20 10:00"), ("D4", "E99005", "旧社名株式会社", "2022-06-20 10:00"),
    ])


def test_filers_are_found_by_legal_name_position_and_the_name_of_the_time(tmp_path):
    with _db(tmp_path / "market.db") as conn:
        _companies(conn)
        directory = FilerDirectory.load(conn)
    assert legal_key("株式会社平和") == "K<平和" and legal_key("平和㈱") == "平和>K" and legal_key("農林中央金庫") == "農林中央金庫"
    assert directory.match("㈱親会社ホールディングス") == "E99003"
    # 株式会社平和 and 平和株式会社 are different companies.
    assert directory.match("株式会社平和") == "E99006" and directory.match("平和株式会社") is None
    # A name filed without its legal form still names one filer.
    assert directory.match("テスト工業") == "E99001"
    # One name, two filers: the group carried it until 2020, a new company carries it now.
    assert directory.match("旧社名株式会社", "2019-03-31") == "E99004"
    assert directory.match("旧社名株式会社", "2025-03-31") == "E99005"
    # People who file with EDINET are not matched as companies.
    assert directory.match("山田 太郎") is None


def test_standard_career_lines_are_put_into_english_by_rule(tmp_path):
    with _db(tmp_path / "market.db") as conn:
        _companies(conn)
        directory = FilerDirectory.load(conn)
    lines = ["テスト工業㈱入社", "同社取締役", "比較電機㈱常務取締役就任", "同社常務取締役退任", "当社代表取締役社長（現任）", "弁護士登録", "S.G.Warburg & Co., Ltd.入社", "当社経営企画部長", "㈱無名商店取締役"]
    sent: list[str] = []

    def machine(texts: list[str]) -> dict[str, str]:
        sent.extend(texts)
        return {text: f"[mt] {text}" for text in texts}

    english = careers.translate_lines(lines, directory, own="TEST INDUSTRIES", translate=machine)
    assert english["テスト工業㈱入社"] == "Joined TEST INDUSTRIES"
    # 同社 is the organisation of the line before.
    assert english["同社取締役"] == "Director, TEST INDUSTRIES"
    assert english["比較電機㈱常務取締役就任"] == "Managing Director, PEER ELECTRIC"
    assert english["同社常務取締役退任"] == "Managing Director, PEER ELECTRIC (until then)"
    assert english["当社代表取締役社長（現任）"] == "President and Representative Director, TEST INDUSTRIES (current)"
    assert english["弁護士登録"] == "Registered as an attorney-at-law"
    assert english["S.G.Warburg & Co., Ltd.入社"] == "Joined S.G.Warburg & Co., Ltd."
    # A title with words no rule knows, and a company no source names, go to the translator whole.
    assert sent == ["当社経営企画部長", "㈱無名商店取締役"] and english["当社経営企画部長"] == "[mt] 当社経営企画部長"
    # Without a translator those lines are simply left as filed.
    assert "当社経営企画部長" not in careers.translate_lines(lines, directory, own="TEST INDUSTRIES")


# --- the update step, end to end ---------------------------------------------

def _sources(tmp_path: Path) -> dict[str, str]:
    market = tmp_path / "market.db"
    with _db(market) as conn:
        _companies(conn)
    filings = tmp_path / "filings.db"
    other_yamada = {**YAMADA, "birth": "1975年９月９日", "title": "取締役", "shares": 1, "career": [("2000年４月", "当社入社")]}
    reports = [
        ("S100A025", "E99001", "テスト工業株式会社", "2025-03-31", "2025-06-20T10:00:00", annual_report("E99001", "2025-03-31", "2025-06-20", HOLDERS_2025, [{**YAMADA, "career": YAMADA["career"][:2]}, TAKAHASHI], statistics=False)),
        ("S100A026", "E99001", "テスト工業株式会社", "2026-03-31", "2026-06-20T10:00:00", annual_report("E99001", "2026-03-31", "2026-06-20", HOLDERS_2026, [YAMADA, SATO, SMITH])),
        ("S100B026", "E99002", "比較電機株式会社", "2026-03-31", "2026-06-25T10:00:00", annual_report("E99002", "2026-03-31", "2026-06-25", [
            ("日本マスタートラスト信託銀行株式会社", "東京都港区赤坂一丁目８番１号", 900, 9.00),
            ("鈴木 花子", "北海道札幌市", 300, 3.00),
            ("テスト工業㈱", "東京都千代田区一丁目１番", 250, 2.50),
        ], [{**SATO, "title": "社外取締役", "shares": 0}, other_yamada])),
        ("S100C026", "E99003", "株式会社親会社ホールディングス", "2026-03-31", "2026-06-26T10:00:00", _zip({"XBRL/PublicDoc/0104010_honbun_x_ixbrl.htm": "<html></html>"})),
    ]
    with _db(filings) as conn:
        conn.execute("CREATE TABLE filings (doc_id TEXT PRIMARY KEY, edinet_code TEXT, submitter_name TEXT, period_start TEXT, period_end TEXT, submitted_at TEXT, form_code TEXT, doc_type_code TEXT, archive_content BLOB)")
        conn.executemany("INSERT INTO filings VALUES (?, ?, ?, '', ?, ?, '030000', '1', ?)", reports)
    return {"market": str(market), "filings": str(filings)}


@pytest.fixture
def updated(tmp_path):
    paths = _sources(tmp_path)
    result = update.update_ownership(market_db=paths["market"], filings_db_path=paths["filings"])
    return paths, result


def _entity(path: str, name: str, born: str | None = None) -> str:
    with _db(path) as conn:
        rows = conn.execute("SELECT entity_id FROM Ownership_Entities WHERE (name = ? OR name_ja = ?) AND (? IS NULL OR birth_date = ?)", (name, name, born, born)).fetchall()
    assert len(rows) == 1, (name, rows)
    return rows[0][0]


def test_update_reads_every_report_and_decides_who_is_who(updated):
    paths, result = updated
    assert result["reports"] == {"documents": 4, "shareholders": 13, "officers": 7, "empty": 1, "errors": 0}
    assert result["entities"]["people"] == 5 and result["entities"]["companies"] == 2
    with _db(paths["market"]) as conn:
        conn.row_factory = sqlite3.Row
        rows = [dict(row) for row in conn.execute("SELECT * FROM Ownership_Entities")]
        careers_kept = conn.execute("SELECT doc_id, name FROM Ownership_Officers WHERE career IS NOT NULL ORDER BY doc_id, seq").fetchall()
    entities = {row["name"]: row for row in rows}
    # One bank under three spellings, holding in two companies.
    bank = entities["The Master Trust Bank of Japan, Ltd."]
    assert bank["kind"] == "organization" and bank["holdings"] == 2 and bank["name_ja"].startswith("日本マスタートラスト信託銀行")
    # A holder that files with EDINET is the company Company Analysis shows.
    assert entities["PARENT HOLDINGS"]["entity_id"] == "E99003" and entities["PARENT HOLDINGS"]["kind"] == "company"
    assert entities["TEST INDUSTRIES"]["holdings"] == 1
    # The president is one person as officer and as shareholder; a namesake born in another year is someone else.
    presidents = [entity for entity in rows if entity["name_ja"] == "山田 太郎"]
    assert sorted(entity["birth_date"] for entity in presidents) == ["1960-01-02", "1975-09-09"]
    president = next(entity for entity in presidents if entity["birth_date"] == "1960-01-02")
    assert president["name"] == "Taro Yamada" and (president["holdings"], president["positions"]) == (1, 1)
    assert president["headline"] == "President and Representative Director · TEST INDUSTRIES"
    # The same outside director sits on two boards.
    assert entities["Jiro Sato"]["positions"] == 2
    # A shareholder who is nobody's officer is known by name alone, across companies.
    investor = entities["鈴木花子"]
    assert investor["kind"] == "individual" and investor["holdings"] == 2
    assert entities["TEST INDUSTRIES Employee Shareholding Association"]["kind"] == "organization"
    assert entities["Saburo Takahashi"]["positions"] == 0 and entities["Saburo Takahashi"]["past_positions"] == 1
    # A career is kept once per person and company, with the newest report that tells it.
    assert [tuple(row) for row in careers_kept] == [("S100A025", "髙橋 三郎"), ("S100A026", "山田 太郎"), ("S100A026", "佐藤 次郎"), ("S100B026", "佐藤 次郎"), ("S100B026", "山田 太郎")]


def test_a_second_run_reads_nothing_new_and_overwrite_reads_everything_again(updated):
    paths, _ = updated
    again = update.update_ownership(market_db=paths["market"], filings_db_path=paths["filings"])
    assert again["reports"]["documents"] == 0 and again["entities"]["people"] == 5
    reread = update.update_ownership(market_db=paths["market"], filings_db_path=paths["filings"], reparse=True)
    assert reread["reports"]["documents"] == 4 and reread["entities"] == again["entities"]
    with _db(paths["market"]) as conn:
        assert conn.execute("SELECT COUNT(*) FROM Ownership_Shareholders").fetchone()[0] == 13
        assert conn.execute("SELECT COUNT(*) FROM Ownership_Officers WHERE career IS NOT NULL").fetchone()[0] == 5


def test_latest_only_reads_one_report_per_company(tmp_path):
    paths = _sources(tmp_path)
    result = update.update_ownership(market_db=paths["market"], filings_db_path=paths["filings"], latest_only=True)
    assert result["reports"]["documents"] == 3
    # The log of reports read is kept beside the filings.
    with _db(paths["filings"]) as conn:
        assert {row[0] for row in conn.execute("SELECT doc_id FROM Ownership_Documents")} == {"S100A026", "S100B026", "S100C026"}
    with _db(paths["market"]) as conn:
        assert not conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'Ownership_Documents'").fetchall()


def test_a_rebuilt_market_database_reads_the_logged_reports_again(updated):
    paths, _ = updated
    with _db(paths["market"]) as conn:
        for table in ("Ownership_Shareholders", "Ownership_Officers", "Ownership_Entities"):
            conn.execute(f"DROP TABLE {table}")
    # The log in filings.db says every report was read, but what they said is gone.
    again = update.update_ownership(market_db=paths["market"], filings_db_path=paths["filings"])
    assert again["reports"]["documents"] == 3 and again["entities"]["people"] == 5


def test_a_document_log_kept_in_market_db_by_an_earlier_version_is_moved_to_filings_db(updated):
    paths, _ = updated
    # An earlier version kept the log in market.db, with fewer columns.
    columns = "doc_id, edinet_code, period_end, submitted_at, status, parser_version, processed_at"
    with _db(paths["filings"]) as conn:
        rows = conn.execute(f"SELECT {columns} FROM Ownership_Documents").fetchall()
        conn.execute("DROP TABLE Ownership_Documents")
    with _db(paths["market"]) as conn:
        conn.execute("CREATE TABLE Ownership_Documents (doc_id TEXT PRIMARY KEY, edinet_code TEXT NOT NULL, period_end TEXT, submitted_at TEXT, "
                     "status TEXT NOT NULL, parser_version INTEGER NOT NULL DEFAULT 0, processed_at TEXT NOT NULL)")
        conn.executemany(f"INSERT INTO Ownership_Documents ({columns}) VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    again = update.update_ownership(market_db=paths["market"], filings_db_path=paths["filings"])
    # Nothing is read again: the moved log still says which reports were read.
    assert again["reports"]["documents"] == 0
    with _db(paths["market"]) as conn:
        assert not conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'Ownership_Documents'").fetchall()
    with _db(paths["filings"]) as conn:
        assert conn.execute("SELECT COUNT(*) FROM Ownership_Documents").fetchone()[0] == 4


def test_company_view_lists_holders_with_changes_and_officers_with_their_stakes(updated):
    paths, _ = updated
    view = service.company_ownership(paths["market"], paths["filings"], "E99001", today=TODAY)
    assert view["company_name"] == "TEST INDUSTRIES" and view["latest_period"] == "2026-03-31" and not view["stale"]
    # Another company's report lists this one among its largest shareholders.
    assert view["holder"] == {"entity_id": "E99001", "holdings": 1, "past_holdings": 0}

    holders = view["shareholders"]
    assert holders["as_of"] == "2026-03-31" and holders["top_ratio"] == pytest.approx(0.342) and holders["issued_shares"] == 100_000_000
    assert holders["treasury_ratio"] == pytest.approx(0.02) and holders["shareholders"] == 4321
    # Foreign institutions and individuals are one slice; slices come in the order they are drawn.
    assert [(item["key"], item["ratio"]) for item in holders["distribution"]] == [
        ("financial_institutions", pytest.approx(0.30)), ("foreign", pytest.approx(0.205)), ("individuals", pytest.approx(0.495)),
    ]
    bank, parent, president, custodian, employees, investor = holders["rows"]
    assert (bank["name"], bank["kind"], bank["account"], bank["account_ja"]) == ("The Master Trust Bank of Japan, Ltd.", "organization", "Trust account", "信託口")
    assert bank["change"] == pytest.approx(0.015) and bank["previous_ratio"] == pytest.approx(0.11) and bank["since"] == "2025-03-31" and bank["other_holdings"] == 1
    assert (parent["name"], parent["kind"], parent["edinet_code"], parent["name_ja"]) == ("PARENT HOLDINGS", "company", "E99003", "株式会社親会社ホールディングス")
    assert parent["change"] == pytest.approx(0.0)
    assert (president["name"], president["kind"], president["positions"]) == ("Taro Yamada", "person", 1) and president["change"] == pytest.approx(-0.005)
    assert custodian["name"] == "State Street Bank and Trust Company" and custodian["new"] and custodian["change"] is None
    assert custodian["account"] == "Account 505001; Standing proxy: Mizuho Bank, Ltd."
    assert employees["name"] == "TEST INDUSTRIES Employee Shareholding Association" and employees["since"] == "2026-03-31"
    assert (investor["name"], investor["kind"], investor["address"]) == ("鈴木花子", "individual", "神奈川県横浜市")
    history = holders["history"]
    assert history["periods"] == ["2025-03-31", "2026-03-31"]
    assert next(item for item in history["holders"] if item["name"] == "State Street Bank and Trust Company")["ratios"] == [None, pytest.approx(0.032)]

    people = view["people"]
    assert people["as_of"] == "2026-06-20" and not people["proposed"] and (people["men"], people["women"], people["outside"]) == (2, 1, 1)
    chief, outside, auditor = people["rows"]
    assert (chief["name"], chief["name_ja"], chief["title"], chief["title_ja"]) == ("Taro Yamada", "山田 太郎", "President and Representative Director", "代表取締役 社長")
    assert chief["representative"] and chief["age"] == 66 and chief["shares"] == 5_000_000 and chief["ratio"] == pytest.approx(0.05)
    assert chief["term_end"] == "2027-03" and chief["since"] is None and chief["other_positions"] == 0 and chief["holdings"] == 1
    assert outside["outside"] and outside["other_positions"] == 1 and outside["since"] == "2026-03-31"
    assert auditor["title"] == "Full-time Audit & Supervisory Board Member" and auditor["auditor"] and auditor["shares"] is None
    assert people["total_shares"] == 5_010_000 and people["total_ratio"] == pytest.approx(0.0501)
    assert [(person["name"], person["title"]) for person in people["departed"]] == [("Saburo Takahashi", "Senior Managing Director, Head of Sales Division")]


def test_a_company_without_a_report_has_no_sections(updated):
    paths, _ = updated
    view = service.company_ownership(paths["market"], paths["filings"], "E99003", today=TODAY)
    assert view["reports"] == 0 and view["shareholders"] is None and view["people"] is None and view["holder"]["holdings"] == 1


def test_an_entity_lists_every_company_it_holds_or_serves(updated):
    paths, _ = updated
    bank = service.entity_detail(paths["market"], paths["filings"], _entity(paths["market"], "The Master Trust Bank of Japan, Ltd."), today=TODAY)
    assert bank["entity"]["kind"] == "organization" and bank["totals"]["holdings"] == 2
    first, second = bank["holdings"]
    assert (first["company_name"], first["ratio"], first["rank"], first["account"], first["change"]) == ("TEST INDUSTRIES", pytest.approx(0.125), 1, "Trust account", pytest.approx(0.015))
    assert first["history"] == [["2025-03-31", pytest.approx(0.11)], ["2026-03-31", pytest.approx(0.125)]] and second["ratio"] == pytest.approx(0.09)
    assert "address" not in first

    director = service.entity_detail(paths["market"], paths["filings"], _entity(paths["market"], "Jiro Sato"), today=TODAY)
    assert director["entity"]["birth_date"] == "1955-03-04" and director["entity"]["age"] == 71
    assert sorted((item["company_name"], item["title"], item["outside"]) for item in director["positions"]) == [("PEER ELECTRIC", "Outside Director", True), ("TEST INDUSTRIES", "Director", True)]
    # Each company's report tells the career in its own words.
    assert sorted(career["company_name"] for career in director["careers"]) == ["PEER ELECTRIC", "TEST INDUSTRIES"]
    assert director["careers"][0]["entries"][0] == {"date": "1978-04", "text": "(株)親会社ホールディングス入社"}

    president = service.entity_detail(paths["market"], paths["filings"], _entity(paths["market"], "Taro Yamada", "1960-01-02"), today=TODAY)
    assert president["totals"]["positions"] == 1 and president["holdings"][0]["ratio"] == pytest.approx(0.05) and president["positions"][0]["ratio"] == pytest.approx(0.05)
    # The namesake on another board is offered, not merged.
    assert [(other["kind"], other["birth_date"]) for other in president["same_name"]] == [("person", "1975-09-09")]

    # A holder known by name only keeps the address filed with each stake, which may tell two namesakes apart.
    investor = service.entity_detail(paths["market"], paths["filings"], _entity(paths["market"], "鈴木花子"), today=TODAY)
    assert {item["address"] for item in investor["holdings"]} == {"神奈川県横浜市", "北海道札幌市"} and investor["totals"]["addresses"] == 2
    assert service.entity_detail(paths["market"], paths["filings"], "Pnobody") is None


def test_stakes_are_valued_for_listed_companies_only(updated):
    paths, _ = updated
    asked: list[list[str]] = []

    def lookup(codes: list[str]) -> dict[str, float | None]:
        asked.append(codes)
        return {code: 100e9 for code in codes}

    # The unlisted parent has no ticker, so no market value is asked for it.
    assert service.market_caps(paths["market"], ["E99001", "E99003", "E99001", ""], lookup) == {"E99001": 100e9}
    assert asked == [["E99001"]]
    assert service.market_caps(paths["market"], [], lookup) == {}


def test_entities_are_searched_by_english_or_filed_name(updated):
    paths, _ = updated
    everyone = service.entities(paths["market"], today=TODAY)
    assert everyone["total"] == 11 and everyone["counts"] == {"company": 2, "individual": 1, "organization": 3, "person": 5}
    # The most connected come first.
    assert everyone["entities"][0]["holdings"] + everyone["entities"][0]["positions"] == 2
    assert [item["name"] for item in service.entities(paths["market"], query="master trust")["entities"]] == ["The Master Trust Bank of Japan, Ltd."]
    assert [item["name"] for item in service.entities(paths["market"], query="マスタートラスト")["entities"]] == ["The Master Trust Bank of Japan, Ltd."]
    assert service.entities(paths["market"], query="yamada", kind="person")["total"] == 2
    assert service.entities(paths["market"], query="E99003")["entities"][0]["name"] == "PARENT HOLDINGS"
    assert service.entities(paths["market"], query="100%_")["total"] == 0


# --- API ----------------------------------------------------------------------

@pytest.fixture
def client(updated, monkeypatch):
    paths, _ = updated
    monkeypatch.setattr(ownership_api, "get_market_db", lambda: paths["market"])
    monkeypatch.setattr(ownership_api, "get_filings_db", lambda: paths["filings"])
    monkeypatch.setattr(ownership_api, "_market_caps", lambda path: lambda codes: {code: 100e9 for code in codes})
    monkeypatch.setattr(ownership_api, "_machine_translation", lambda texts: {text: f"[mt] {text}" for text in texts})
    app = FastAPI()
    app.include_router(ownership_api.router)
    return TestClient(app), paths


def test_api_serves_status_company_entities_and_careers(client):
    http, paths = client
    status = http.get("/api/ownership/status").json()
    assert (status["reports"], status["companies"], status["entities"]) == (3, 2, 11) and "reports" in status["updates"]
    company = http.get("/api/ownership/company/E99001").json()
    assert len(company["shareholders"]["rows"]) == 6 and len(company["people"]["rows"]) == 3
    assert http.get("/api/ownership/company/nonsense").status_code == 404
    found = http.get("/api/ownership/entities", params={"q": "sato", "kind": "person"}).json()
    assert found["total"] == 1
    detail = http.get(f"/api/ownership/entities/{found['entities'][0]['entity_id']}").json()
    assert detail["totals"]["positions"] == 2
    assert http.get("/api/ownership/market-caps", params={"codes": "E99001,E99003,junk"}).json() == {"market_caps": {"E99001": 100e9}}
    assert http.get("/api/ownership/entities/Pmissing00000").status_code == 404
    assert http.get("/api/ownership/entities/not%20an%20id").status_code == 404
    translated = http.post("/api/ownership/careers/translate", json={"lines": ["当社入社", "当社取締役（現任）", "当社経営企画部長"], "company": "E99001", "period_end": "2026-03-31"}).json()
    assert translated["translations"] == [
        {"source": "当社入社", "english": "Joined TEST INDUSTRIES"},
        {"source": "当社取締役（現任）", "english": "Director, TEST INDUSTRIES (current)"},
        {"source": "当社経営企画部長", "english": "[mt] 当社経営企画部長"},
    ]
    assert http.post("/api/ownership/careers/translate", json={"lines": []}).status_code == 422


def test_api_without_ownership_data_says_how_to_create_it(monkeypatch, tmp_path):
    monkeypatch.setattr(ownership_api, "get_market_db", lambda: str(tmp_path / "missing.db"))
    app = FastAPI()
    app.include_router(ownership_api.router)
    http = TestClient(app)
    assert http.get("/api/ownership/status").json()["reports"] == 0
    response = http.get("/api/ownership/company/E99001")
    assert response.status_code == 503 and "Update ownership" in response.json()["detail"]
    # A market database the step has never written to answers the same way.
    with _db(tmp_path / "empty.db") as conn:
        conn.execute("CREATE TABLE CompanyInfo (Company_Code TEXT)")
    monkeypatch.setattr(ownership_api, "get_market_db", lambda: str(tmp_path / "empty.db"))
    assert http.get("/api/ownership/status").json()["reports"] == 0
    assert http.get("/api/ownership/entities").status_code == 503


def test_the_pipeline_registers_the_ownership_step():
    from src.orchestrator.common import build_step_registry

    registry = build_step_registry()
    definitions = registry[1] if isinstance(registry, tuple) else registry
    names = set(definitions) if isinstance(definitions, dict) else {definition.name for definition in definitions}
    assert "update_ownership" in names
