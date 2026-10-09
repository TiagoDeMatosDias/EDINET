"""Bond terms read from EDINET filings.

Two kinds of filing describe a company's bonds:

* A shelf-registration supplement (発行登録追補書類, document type 100) is
  filed when bonds are priced. Its securities section has one label/value
  table per bond: name (銘柄), amount, issue price, coupon (利率), interest
  dates, maturity (償還期限), payment date, collateral, covenants, and notes
  that carry the credit ratings.
* Every annual securities report has a bond schedule (社債明細表) among its
  annexed schedules: one row per bond outstanding during the year, with the
  issuing group company, issue date, opening and closing balance, coupon,
  collateral and maturity.

Both are HTML tables inside inline XBRL text blocks, so this module reads the
tables (expanding merged cells) and normalises Japanese dates, amounts, and
rates. Coupons are decimals (0.0271 for 2.71%) and amounts are in currency
units (yen), never in the table's millions or thousands.
"""

from __future__ import annotations

import io
import re
import unicodedata
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

from bs4 import BeautifulSoup

PARSER_VERSION = 1

_ERA_OFFSETS = {"令和": 2018, "平成": 1988, "昭和": 1925, "R": 2018, "H": 1988, "S": 1925}
_DATE = re.compile(r"(令和|平成|昭和|[RHS])?\s*(\d{1,4}|元)\s*[年./]\s*(\d{1,2})\s*[月./]\s*(\d{1,2})")
_DASHES = "-‐‑‒–—―ー−"
_UNIT_FACTORS = {"兆": 1e12, "億": 1e8, "千万": 1e7, "百万": 1e6, "万": 1e4, "千": 1e3}
_CURRENCIES = (
    ("ニュージーランドドル", "NZD"),
    ("豪ドル", "AUD"),
    ("カナダドル", "CAD"),
    ("米ドル", "USD"),
    ("ユーロ", "EUR"),
    ("英ポンド", "GBP"),
    ("人民元", "CNY"),
    ("円", "JPY"),
)
_MONEY = re.compile(r"((?:\d+(?:\.\d+)?\s*(?:兆|億|千万|百万|万|千)?\s*)+)(ニュージーランドドル|豪ドル|カナダドル|米ドル|ユーロ|英ポンド|人民元|円)")
_MONEY_PART = re.compile(r"(\d+(?:\.\d+)?)\s*(兆|億|千万|百万|万|千)?")
_PERCENT = re.compile(r"(\d+(?:\.\d+)?)\s*(?:%|パーセント)")
_SERIES = re.compile(r"第\s*(\d+)\s*回")
_RATING = r"(Aaa|Aa[123]|A[123]|Baa[123]|Ba[123]|B[123]|Caa[123]|AAA|AA[+-]?|A[+-]?|BBB[+-]?|BB[+-]?|B[+-]?|CCC[+-]?)(?![A-Za-z0-9])"
_RATING_TEXT = re.compile(r"格付\s*(?:[:：]|は)?\s*" + _RATING)
# "本社債について、当社はR&IからAA-の信用格付を取得している。"
_RATING_FROM = re.compile(r"から\s*" + _RATING + r"\s*(?:\([^)]*\))?\s*の(?:信用)?格付")
_AGENCIES = (
    ("R&I", ("格付投資情報センター", "R&I")),
    ("JCR", ("日本格付研究所", "JCR")),
    ("Moody's", ("ムーディーズ", "Moody")),
    ("S&P", ("S&P", "スタンダード・アンド・プアーズ", "スタンダード&プアーズ")),
    ("Fitch", ("フィッチ", "Fitch")),
)
# Notches on the S&P-style scale: AAA = 1, AA+ = 2, ... Moody's grades map one to one.
_GRADES = ("AAA", "AA", "A", "BBB", "BB", "B", "CCC")
_MOODYS = {"Aaa": "AAA", "Aa": "AA", "A": "A", "Baa": "BBB", "Ba": "BB", "B": "B", "Caa": "CCC"}


def clean(text: Any) -> str:
    """Half-width digits and Latin letters, one space between words."""
    value = unicodedata.normalize("NFKC", str(text or ""))
    value = value.replace("〜", "~").replace("～", "~")
    return re.sub(r"\s+", " ", value).strip()


def numeric(text: Any) -> str:
    """``clean`` with numbers rejoined where markup split them (``年1 .314 %``) and no thousands commas."""
    value = re.sub(r"(?<=\d)\s*([.,])\s*(?=\d)", r"\1", clean(text))
    return value.replace(",", "")


def compact(text: Any) -> str:
    return re.sub(r"\s+", "", clean(text))


def is_dash(text: Any) -> bool:
    value = compact(text)
    return bool(value) and all(char in _DASHES for char in value)


def parse_date(text: Any) -> str | None:
    """The first calendar date in ``text`` as ISO ``YYYY-MM-DD``; era years are converted."""
    for match in _DATE.finditer(clean(text)):
        era, year_text, month, day = match.groups()
        year = 1 if year_text == "元" else int(year_text)
        if era:
            year += _ERA_OFFSETS[era]
        elif year < 1900:
            continue
        try:
            return date(year, int(month), int(day)).isoformat()
        except ValueError:
            continue
    return None


def all_dates(text: Any) -> list[str]:
    found = []
    for match in _DATE.finditer(clean(text)):
        parsed = parse_date(match.group(0))
        if parsed:
            found.append(parsed)
    return found


def parse_money(text: Any) -> tuple[float | None, str | None]:
    """An amount written as ``金30,000百万円`` or ``1兆2,000億円``, in currency units."""
    value = numeric(text)
    match = _MONEY.search(value)
    if not match:
        return None, None
    total = 0.0
    for number, unit in _MONEY_PART.findall(match.group(1)):
        total += float(number) * _UNIT_FACTORS.get(unit, 1.0)
    currency = next(code for word, code in _CURRENCIES if word == match.group(2))
    return total, currency


def parse_rate(text: Any) -> float | None:
    """The first percentage in ``text`` as a decimal (``年2.710%`` → 0.0271)."""
    match = _PERCENT.search(numeric(text))
    return round(float(match.group(1)) / 100, 8) if match else None


def series_number(name: Any) -> int | None:
    match = _SERIES.search(clean(name))
    return int(match.group(1)) if match else None


def bond_features(*texts: Any) -> list[str]:
    """Features named in a bond's title or terms."""
    text = compact(" ".join(str(item or "") for item in texts))
    checks = (
        ("convertible", ("新株予約権付社債", "転換社債")),
        ("subordinated", ("劣後",)),
        ("non-viability", ("実質破綻時免除", "債務免除特約")),
        ("deferrable", ("利払繰延",)),
        ("callable", ("期限前償還条項", "繰上償還条項", "期限前償還特約", "任意償還条項", "コール条項")),
        ("general-mortgage", ("一般担保",)),
        ("guaranteed", ("保証付",)),
        ("private", ("私募",)),
        ("green", ("グリーン",)),
        ("social", ("ソーシャル",)),
        ("sustainability", ("サステナビリティ",)),
        ("transition", ("トランジション",)),
        ("blue", ("ブルーボンド",)),
        ("retail", ("個人向",)),
    )
    found = [label for label, words in checks if any(word in text for word in words)]
    if "担保付" in text.replace("無担保", "").replace("一般担保付", "") and "general-mortgage" not in found:
        found.append("secured")
    return found


_NAME_CURRENCIES = (
    ("ユーロ円", "JPY"), ("円貨", "JPY"), ("円建", "JPY"),
    ("香港ドル", "HKD"), ("シンガポールドル", "SGD"), ("ニュージーランドドル", "NZD"), ("豪ドル", "AUD"), ("オーストラリアドル", "AUD"),
    ("カナダドル", "CAD"), ("米ドル", "USD"), ("USドル", "USD"), ("US$", "USD"), ("USD", "USD"), ("ユーロ", "EUR"), ("EUR", "EUR"),
    ("英ポンド", "GBP"), ("ポンド", "GBP"), ("人民元", "CNY"), ("ルピア", "IDR"), ("ルピー", "INR"), ("NCD", "INR"), ("バーツ", "THB"),
    ("ウォン", "KRW"), ("ペソ", "MXN"), ("リンギ", "MYR"), ("ドン建", "VND"), ("レアル", "BRL"), ("リラ", "TRY"), ("ランド", "ZAR"),
    ("米国ドル", "USD"), ("ドル建", "USD"),
)


def name_currency(name: Any) -> str:
    """The currency a bond's title names: ``2028年満期ユーロ建普通社債`` → EUR; Euroyen bonds are yen."""
    text = compact(name)
    for word, code in _NAME_CURRENCIES:
        if word in text:
            return code
    return "JPY"


_NOTE_CURRENCIES = (
    ("豪", "AUD"), ("NZ", "NZD"), ("加", "CAD"), ("香港", "HKD"), ("シンガポール", "SGD"), ("S$", "SGD"),
    ("ユーロ", "EUR"), ("€", "EUR"), ("ポンド", "GBP"), ("£", "GBP"), ("人民元", "CNY"), ("ルピア", "IDR"), ("ルピー", "INR"),
    ("バーツ", "THB"), ("ドル", "USD"), ("$", "USD"),
)


def note_currency(text: Any) -> str | None:
    """The currency of a foreign amount shown beside the yen balance: ``[499百万ドル]``, ``(655,000 千$)``, ``千豪$``."""
    value = clean(text)
    for word, code in _NOTE_CURRENCIES:
        if word in value:
            return code
    return None


def seniority(features: list[str]) -> str:
    if "convertible" in features:
        return "convertible"
    if "deferrable" in features:
        return "hybrid"
    if "subordinated" in features:
        return "subordinated"
    if "secured" in features or "general-mortgage" in features:
        return "secured"
    return "senior"


def rating_notch(rating: str | None) -> int | None:
    """Position on the S&P-style scale: AAA = 1, AA+ = 2, AA = 3, AA- = 4, A+ = 5 ...; Moody's Aa1 = AA+."""
    if not rating:
        return None
    match = re.fullmatch(r"(Aaa|Aa|Baa|Ba|Caa|A|B)([123])?", rating)
    if match and (match.group(2) or match.group(1) == "Aaa"):
        grade = _MOODYS[match.group(1)]
        offset = {"1": -1, "2": 0, "3": 1}.get(match.group(2) or "2", 0)
    else:
        match = re.fullmatch(r"(AAA|AA|A|BBB|BB|B|CCC)([+-]?)", rating)
        if not match:
            return None
        grade = match.group(1)
        offset = {"+": -1, "": 0, "-": 1}[match.group(2)]
    if grade == "AAA":
        return 1
    return 3 * _GRADES.index(grade) + offset


def notch_label(notch: float | None) -> str | None:
    if notch is None:
        return None
    value = max(1, min(21, round(notch)))
    if value == 1:
        return "AAA"
    index = (value + 1) // 3
    return _GRADES[index] + {-1: "+", 0: "", 1: "-"}[value - 3 * index]


def parse_ratings(text: Any) -> list[dict[str, str]]:
    """Ratings named in a bond's notes, one per agency (``信用格付：AA``)."""
    value = clean(text)
    ratings: dict[str, str] = {}
    matches = sorted([*_RATING_TEXT.finditer(value), *_RATING_FROM.finditer(value)], key=lambda match: match.start())
    for match in matches:
        before = value[max(0, match.start() - 160):match.start()]
        positions = [(before.rfind(word), agency) for agency, words in _AGENCIES for word in words if word in before]
        if not positions:
            continue
        agency = max(positions)[1]
        ratings.setdefault(agency, match.group(1))
    return [{"agency": agency, "rating": rating} for agency, rating in ratings.items()]


def coupon_frequency(text: Any) -> int | None:
    """Coupons a year from the interest-date text (``毎年1月31日および7月31日`` → 2)."""
    value = clean(text)
    months = {int(month) for month in re.findall(r"(\d{1,2})\s*月", value) if 1 <= int(month) <= 12}
    if "毎月" in value:
        return 12
    if len(months) in (1, 2, 4, 12):
        return len(months)
    return None


def coupon_kind(text: Any, coupon: float | None) -> str:
    value = clean(text)
    if "無利息" in value or coupon == 0:
        return "zero"
    floating = is_floating(value)
    periods = "以降" in value or "まで" in value
    if floating and not periods:
        # "TONAに0.37%を加えた値": the percentage is the margin, not a coupon.
        return "floating"
    if periods or len(_PERCENT.findall(value)) > 1:
        return "fixed-to-floating" if floating else "step"
    return "fixed"


def first_call_date(redemption: Any, issue_date: str | None, maturity: str | None) -> str | None:
    """The earliest date in the redemption terms after issue other than maturity: when the issuer may first repay early."""
    dates = sorted({day for day in all_dates(redemption) if (not issue_date or day > issue_date) and day != maturity and (not maturity or day < maturity)})
    return dates[0] if dates else None


def issue_price_per_100(text: Any) -> float | None:
    """``各社債の金額100円につき金99.98円`` → 99.98."""
    value = numeric(text)
    match = re.search(r"(\d+(?:\.\d+)?)\s*(?:円|米ドル|ユーロ|豪ドル)?\s*につき\s*金?\s*(\d+(?:\.\d+)?)", value)
    if not match or float(match.group(1)) <= 0:
        return None
    return round(float(match.group(2)) / float(match.group(1)) * 100, 6)


# --- tables ---------------------------------------------------------------

def _span(cell: Any, name: str) -> int:
    try:
        return max(1, min(int(str(cell.get(name, "1")).strip() or "1"), 50))
    except ValueError:
        return 1


def table_grid(table: Any) -> list[list[str]]:
    """Rows of cell text with merged cells repeated in every row and column they cover."""
    rows: list[list[str]] = []
    carried: dict[int, list[Any]] = {}
    for tr in table.find_all("tr"):
        if tr.find_parent("table") is not table:
            continue
        cells = tr.find_all(["td", "th"], recursive=False)
        row: list[str] = []
        column = 0
        index = 0
        while index < len(cells) or any(position >= column for position in carried):
            if column in carried:
                remaining, text = carried[column]
                row.append(text)
                if remaining <= 1:
                    del carried[column]
                else:
                    carried[column][0] -= 1
                column += 1
                continue
            if index >= len(cells):
                row.append("")
                column += 1
                continue
            cell = cells[index]
            index += 1
            text = clean(cell.get_text(" ", strip=True))
            rowspan = _span(cell, "rowspan")
            for _ in range(_span(cell, "colspan")):
                row.append(text)
                if rowspan > 1:
                    carried[column] = [rowspan - 1, text]
                column += 1
        if any(row):
            rows.append(row)
    return rows


def _text_block(html: str, suffix: str) -> str | None:
    """The HTML inside the ``ix:nonNumeric`` element whose name ends with ``suffix``."""
    match = re.search(r'<ix:nonNumeric\b[^>]*\bname="[^"]*' + re.escape(suffix) + r'"[^>]*>', html, re.IGNORECASE)
    if not match:
        return None
    depth = 1
    position = match.end()
    tags = re.compile(r"<(/?)ix:nonNumeric\b[^>]*?(/?)>", re.IGNORECASE)
    for tag in tags.finditer(html, position):
        if tag.group(2):
            continue
        depth += -1 if tag.group(1) else 1
        if depth == 0:
            return html[position:tag.start()]
    return html[position:]


def _documents(archive: bytes) -> list[str]:
    with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
        names = sorted(
            name for name in bundle.namelist()
            if name.lower().endswith((".htm", ".html")) and "publicdoc" in name.lower() and "honbun" in name.lower()
        )
        return [bundle.read(name).decode("utf-8", "replace") for name in names]


# --- shelf-registration supplements --------------------------------------

@dataclass
class IssuedBond:
    seq: int
    name: str
    series: int | None = None
    currency: str = "JPY"
    amount: float | None = None
    denomination: float | None = None
    issue_price: float | None = None
    coupon: float | None = None
    coupon_text: str = ""
    coupon_kind: str = "fixed"
    frequency: int | None = None
    interest_dates: str = ""
    issue_date: str | None = None
    maturity: str | None = None
    maturity_text: str = ""
    perpetual: bool = False
    call_date: str | None = None
    offering: str = ""
    collateral: str = ""
    negative_pledge: bool = False
    covenants: str = ""
    features: list[str] = field(default_factory=list)
    seniority: str = "senior"
    ratings: list[dict[str, str]] = field(default_factory=list)
    notes: str = ""

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


_ISSUE_LABELS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("name", ("銘柄",)),
    ("amount", ("券面総額又は振替社債の総額", "券面総額", "振替社債の総額", "売出券面額の総額又は売出振替社債の総額")),
    ("denomination", ("各社債の金額",)),
    ("issue_price", ("発行価格", "売出価格")),
    ("coupon", ("利率",)),
    ("interest_dates", ("利払日",)),
    ("maturity", ("償還期限",)),
    ("redemption", ("償還の方法",)),
    ("offering", ("募集の方法",)),
    ("subscription", ("申込期間",)),
    ("payment_date", ("払込期日",)),
    ("collateral", ("担保",)),
    ("negative_pledge", ("財務上の特約(担保提供制限)",)),
    ("covenants", ("財務上の特約(その他の条項)", "財務上の特約")),
)


def _issue_label(label: str) -> str | None:
    key = compact(label)
    for name, options in _ISSUE_LABELS:
        for option in options:
            if key == option or (key.startswith(option + "(") and "(" not in option):
                return name
    return None


def _finish_issue(seq: int, values: dict[str, str], notes: list[str]) -> IssuedBond | None:
    name = values.get("name", "")
    if not name or "coupon" not in values and "maturity" not in values:
        return None
    note_text = " ".join(notes)
    amount, currency = parse_money(values.get("amount", ""))
    denomination, _ = parse_money(values.get("denomination", ""))
    coupon_text = values.get("coupon", "")
    coupon = 0.0 if "無利息" in coupon_text else parse_rate(coupon_text)
    maturity_text = values.get("maturity", "")
    perpetual = "定めない" in maturity_text or "定めがない" in maturity_text
    maturity = None if perpetual else parse_date(maturity_text)
    kind = coupon_kind(coupon_text, coupon)
    if kind == "floating":
        coupon = None
    call_date = None
    if kind in ("step", "fixed-to-floating"):
        # The first coupon period ends at the first call date: "2026年7月27日の翌日から2031年7月27日まで年1.2%".
        dates = all_dates(coupon_text.split("以降")[0])
        call_date = dates[1] if len(dates) > 1 else None
    features = bond_features(name, values.get("redemption", "") if kind != "fixed" else "", values.get("collateral", ""))
    if call_date and "callable" not in features:
        features.append("callable")
    issue_date = parse_date(values.get("payment_date", "")) or parse_date(values.get("subscription", ""))
    if call_date is None and ("callable" in features or "deferrable" in features or perpetual):
        call_date = first_call_date(values.get("redemption", ""), issue_date, maturity)
    if not currency or (amount is None and denomination is not None):
        currency = parse_money(values.get("denomination", ""))[1] or "JPY"
    return IssuedBond(
        seq=seq,
        name=name,
        series=series_number(name),
        currency=currency or "JPY",
        amount=amount,
        denomination=denomination,
        issue_price=issue_price_per_100(values.get("issue_price", "")),
        coupon=coupon,
        coupon_text=coupon_text[:400],
        coupon_kind=kind,
        frequency=coupon_frequency(values.get("interest_dates", "")),
        interest_dates=values.get("interest_dates", "")[:200],
        issue_date=issue_date,
        maturity=maturity,
        maturity_text=maturity_text[:200],
        perpetual=perpetual,
        call_date=call_date,
        offering=values.get("offering", "")[:200],
        collateral=values.get("collateral", "")[:400],
        negative_pledge=bool(values.get("negative_pledge")) and "付されていない" not in values.get("negative_pledge", ""),
        covenants=values.get("covenants", "")[:600],
        features=features,
        seniority=seniority(features),
        ratings=parse_ratings(note_text),
        notes=note_text[:4000],
    )


def parse_issuance(archive: bytes) -> list[IssuedBond]:
    """Every new bond described in a shelf-registration supplement's securities section."""
    bonds: list[IssuedBond] = []
    values: dict[str, str] = {}
    notes: list[str] = []

    def flush() -> None:
        bond = _finish_issue(len(bonds), values, notes)
        if bond is not None:
            bonds.append(bond)

    for html in _documents(archive):
        soup = BeautifulSoup(html, "html.parser")
        for element in soup.find_all(["table", "p", "h1", "h2", "h3", "h4", "h5"]):
            if element.find_parent("table") is not None:
                continue
            if element.name != "table":
                text = clean(element.get_text(" ", strip=True))
                if re.match(r"^第[一二三四]部", text) and "証券情報" not in text and values:
                    # Past the securities section: reference documents follow.
                    flush()
                    values, notes = {}, []
                elif values and text:
                    notes.append(text)
                continue
            previous = None
            for row in table_grid(element):
                cells = [cell for index, cell in enumerate(row) if index == 0 or cell != row[index - 1]]
                label = _issue_label(cells[0]) if cells else None
                if label is None and previous and len(cells) >= 2 and not compact(cells[0]):
                    # A row with an empty label cell continues the field above.
                    label = previous
                if label is None or len(cells) < 2:
                    if values:
                        notes.append(" ".join(cells))
                    previous = None
                    continue
                value = cells[-1]
                if label == "name" and previous == "name" and values.get("name"):
                    # The name cell runs over two rows: "…永久社債" then "(債務免除特約…付)".
                    values["name"] = f"{values['name']}{value}"
                elif label == "name":
                    if values.get("name"):
                        flush()
                        values, notes = {}, []
                    values["name"] = value
                elif values.get("name") and label not in values:
                    values[label] = value
                elif label == previous and label != "name" and value not in values.get(label, ""):
                    # A label merged over several rows: "1. ...まで" then "年1.234%".
                    values[label] = f"{values[label]} {value}"
                previous = label
    if values:
        flush()
    return bonds


# --- annual-report bond schedules ----------------------------------------

@dataclass
class ScheduleRow:
    seq: int
    issuer: str
    name: str
    series: int | None = None
    issue_date: str | None = None
    issue_date_text: str = ""
    opening: float | None = None
    closing: float | None = None
    current_portion: float | None = None
    coupon: float | None = None
    coupon_text: str = ""
    collateral: str = ""
    maturity: str | None = None
    maturity_text: str = ""
    note: str = ""
    currency_note: str = ""
    currency: str = "JPY"
    features: list[str] = field(default_factory=list)
    seniority: str = "senior"
    consolidated: bool = True

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


_SCHEDULE_COLUMNS = (
    ("issuer", ("会社名",)),
    ("name", ("銘柄", "種別", "名称")),
    ("issue_date", ("発行年月日", "発行日")),
    # IFRS notes head the balances with the year: 前年度 (2024年12月31日), 当年度 (2025年12月31日).
    ("opening", ("当期首残高", "前期末残高", "期首残高", "前年度", "前期末", "前連結会計年度", "前事業年度")),
    ("closing", ("当期末残高", "期末残高", "当年度", "当期末", "当連結会計年度", "当事業年度")),
    ("coupon", ("利率",)),
    ("collateral", ("担保",)),
    ("maturity", ("償還期限", "償還期日")),
    ("note", ("摘要",)),
)
_NAME_HEADERS = ("銘柄", "種別", "名称")
# IFRS notes list other debt in the same table.
_NOT_BONDS = ("コマーシャル", "借入", "リース", "預り金", "預金", "CP")
_TOTAL_ROWS = {"合計", "計", "小計", "総計", "合計額"}
_DITTO = {"〃", "同上", '"', "''", "″", "′′", "々"}
_UNIT_WORDS = (("百万円", 1e6), ("千円", 1e3), ("円", 1.0))


def _schedule_columns(header: list[str]) -> dict[str, int]:
    columns: dict[str, int] = {}
    for index, text in enumerate(header):
        key = compact(text)
        for name, options in _SCHEDULE_COLUMNS:
            if name not in columns and any(key.startswith(option) for option in options):
                columns[name] = index
                break
    return columns


def _unit_factor(*texts: str) -> float | None:
    for text in texts:
        value = compact(text)
        for word, factor in _UNIT_WORDS:
            if f"({word})" in value or f"単位:{word}" in value or f"単位{word}" in value:
                return factor
    return None


def _strip_markers(text: str) -> str:
    value = numeric(text)
    value = re.sub(r"\[[^\]]*\]", " ", value)
    value = re.sub(r"\(注\s*\d*\)|注\s*\d+|※\s*\d*|\*\s*\d*", " ", value)
    return value


def parse_balance(text: str, factor: float) -> tuple[float | None, float | None]:
    """``10,000(10,000)`` → (balance, of which due within a year), scaled to yen."""
    value = _strip_markers(text).replace(",", "")
    inner: float | None = None
    for content in re.findall(r"\(([^)]*)\)", value):
        if re.search(r"[A-Za-z$€£]|ドル|ユーロ|ポンド|元", content):
            continue
        number = re.search(r"\d+(?:\.\d+)?", content)
        inner = float(number.group(0)) * factor if number else (0.0 if is_dash(content) else inner)
    outer = re.sub(r"\([^)]*\)", " ", value)
    number = re.search(r"\d+(?:\.\d+)?", outer)
    if number:
        return float(number.group(0)) * factor, inner
    return (0.0 if is_dash(outer) or not outer.strip() and inner is not None else None), inner


FLOATING_WORDS = ("変動", "TONA", "TORF", "TIBOR", "LIBOR", "SOFR", "EURIBOR", "BBSW", "基準金利", "参照金利", "国債金利", "スワップ", "ベースレート", "ヶ月", "か月", "カ月")


def is_floating(text: Any) -> bool:
    value = clean(text).upper()
    return any(word.upper() in value for word in FLOATING_WORDS)


def parse_schedule_coupon(text: str) -> float | None:
    value = _strip_markers(text).replace(",", "")
    if "無利息" in value or "ゼロ" in value:
        return 0.0
    if "~" in value or is_floating(value):
        return None
    number = re.search(r"\d+(?:\.\d+)?", value)
    if not number:
        return None
    rate = float(number.group(0))
    return round(rate / 100, 8) if rate < 30 else None


def _row_currency(name: str, row: list[str]) -> str:
    currency = name_currency(name)
    if currency == "JPY":
        notes = " ".join(re.findall(r"\[[^\]]*\]|\([^)]*\)", clean(" ".join(row))))
        currency = note_currency(notes) or "JPY"
    return currency


def _schedule_rows(fragment: str, consolidated: bool) -> list[ScheduleRow]:
    soup = BeautifulSoup(fragment, "html.parser")
    rows: list[ScheduleRow] = []
    outer_text = clean(soup.get_text(" ", strip=True))[:400]
    for table in soup.find_all("table"):
        grid = table_grid(table)
        header_index = next((
            i for i, row in enumerate(grid)
            if any(compact(cell).startswith(_NAME_HEADERS) for cell in row) and any("償還期限" in compact(cell) or "償還期日" in compact(cell) for cell in row)
        ), None)
        if header_index is None:
            continue
        header = grid[header_index]
        columns = _schedule_columns(header)
        if "name" not in columns or "maturity" not in columns:
            continue
        closing_header = header[columns["closing"]] if "closing" in columns else ""
        unit_row = next((row for row in grid[header_index + 1:header_index + 3] if any(compact(cell) in ("百万円", "千円", "円") for cell in row)), [])
        factor = _unit_factor(closing_header, " ".join(header), *(" ".join(row) for row in grid[header_index + 1:header_index + 3]), outer_text) or _unit_factor(*(f"({compact(cell)})" for cell in unit_row)) or 1e6
        # "償還期限 (利率)": one column carries both, the rate in brackets.
        coupon_in_maturity = "coupon" not in columns and "利率" in compact(header[columns["maturity"]])
        issuer = "当社"
        above: list[str] = []
        for row in grid[header_index + 1:]:
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            # Ditto marks repeat the value above: "〃" under "なし" means "なし".
            row = [above[index] if compact(text) in _DITTO and index < len(above) else text for index, text in enumerate(row)]
            above = row

            def cell(name: str, row: list[str] = row, columns: dict[str, int] = columns) -> str:
                return row[columns[name]] if name in columns and columns[name] < len(row) else ""

            name = cell("name")
            issuer_text = cell("issuer")
            if issuer_text and compact(issuer_text) not in _DITTO and not is_dash(issuer_text):
                issuer = issuer_text
            if not name or compact(name) in _TOTAL_ROWS or compact(issuer_text) in _TOTAL_ROWS or is_dash(name):
                continue
            if compact(name) == compact(issuer_text) or any(word in compact(name) for word in _NOT_BONDS):
                continue
            maturity_text = cell("maturity")
            maturity_dates = all_dates(maturity_text)
            if rows and compact(name) == compact(rows[-1].name) and issuer == rows[-1].issuer and not re.search(r"\d", re.sub(r"\([^)]*\)", "", cell("opening") + cell("closing"))):
                # A second line for the same bond holds only the bracketed amount due within a year.
                rows[-1].current_portion = parse_balance(cell("closing"), factor)[1]
                continue
            opening, _ = parse_balance(cell("opening"), factor)
            closing, current = parse_balance(cell("closing"), factor)
            if not maturity_dates and opening is None and closing is None:
                continue
            issue_text = cell("issue_date")
            issue_dates = all_dates(issue_text)
            coupon_text = cell("coupon")
            if coupon_in_maturity:
                coupon_text = " ".join(re.findall(r"\(([^)]*%[^)]*)\)", numeric(maturity_text))) or ""
            features = bond_features(name, cell("collateral"), cell("note"))
            rows.append(ScheduleRow(
                seq=len(rows),
                issuer=issuer,
                name=name,
                series=series_number(name),
                issue_date=issue_dates[0] if len(issue_dates) == 1 else None,
                issue_date_text=issue_text[:120],
                opening=opening,
                closing=closing,
                current_portion=current,
                coupon=parse_schedule_coupon(coupon_text),
                coupon_text=coupon_text[:120],
                collateral=cell("collateral")[:120],
                maturity=maturity_dates[0] if len(maturity_dates) == 1 else None,
                maturity_text=maturity_text[:120],
                note=cell("note")[:200],
                currency_note=" ".join(re.findall(r"\[[^\]]*\]|\([^)]*(?:ドル|ユーロ|ポンド|\$|€)[^)]*\)", clean(" ".join(row))))[:200],
                currency=_row_currency(name, row),
                features=features,
                seniority=seniority(features),
                consolidated=consolidated,
            ))
    return rows


def parse_bond_schedule(archive: bytes) -> list[ScheduleRow]:
    """Rows of an annual report's bond schedule.

    The consolidated annexed schedule comes first; IFRS filers list their
    bonds in the bonds-and-borrowings note instead; a parent-only schedule is
    the fallback.
    """
    sources = (
        (True, "AnnexedConsolidatedDetailedScheduleOfCorporateBondsTextBlock"),
        (True, "NotesBondsAndBorrowingsConsolidatedFinancialStatementsIFRSTextBlock"),
        (False, "AnnexedDetailedScheduleOfCorporateBondsTextBlock"),
    )
    blocks: dict[str, list[str]] = {suffix: [] for _, suffix in sources}
    for html in _documents(archive):
        if "DetailedScheduleOfCorporateBondsTextBlock" not in html and "NotesBondsAndBorrowings" not in html:
            continue
        for _, suffix in sources:
            fragment = _text_block(html, suffix)
            if fragment:
                blocks[suffix].append(fragment)
    for consolidated, suffix in sources:
        rows = [row for fragment in blocks[suffix] for row in _schedule_rows(fragment, consolidated)]
        if rows:
            for index, row in enumerate(rows):
                row.seq = index
            return rows
    return []
