"""Names of shareholders and officers: who a filed name is, and what to call it in English.

Annual reports name everyone in Japanese. This module decides which filed
names are the same holder (``日本マスタートラスト信託銀行㈱（信託口）`` and
``日本マスタートラスト信託銀行株式会社`` are one bank, the trust account a
detail of the holding), and gives an English name wherever a reliable source
has one: the filer's own English label for an officer, the name Company
Analysis shows for a holder that files with EDINET, or the published English
name of an institution that appears in hundreds of shareholder lists. Names
are never machine-translated; a holder with no such source keeps the name it
was filed under. Officers' titles are rewritten from the roles they state.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any

from src.bonds.parsing import clean, compact

from .directory import legal_key

# Old and variant forms of characters common in names. A report may print 髙橋 one year and 高橋 the next;
# folded together (and, for officers, paired with the date of birth) they are the same person.
_VARIANTS = str.maketrans({
    "髙": "高", "﨑": "崎", "濵": "浜", "濱": "浜", "邊": "辺", "邉": "辺", "澤": "沢", "齋": "斎", "齊": "斉", "廣": "広",
    "德": "徳", "國": "国", "櫻": "桜", "眞": "真", "惠": "恵", "榮": "栄", "瀨": "瀬", "藏": "蔵", "條": "条", "實": "実",
    "壽": "寿", "靜": "静", "龍": "竜", "萬": "万", "彌": "弥", "禮": "礼", "圓": "円", "會": "会", "學": "学", "桒": "桑",
    "栁": "柳", "𠮷": "吉", "淸": "清", "黑": "黒", "增": "増", "賴": "頼", "瀧": "滝", "澁": "渋", "讓": "譲", "豐": "豊",
    "橫": "横", "敎": "教", "寬": "寛", "卷": "巻", "縣": "県", "嶽": "岳", "冨": "富", "峯": "峰", "曾": "曽", "證": "証",
    "鐵": "鉄", "嶌": "島", "槇": "槙", "遙": "遥", "顯": "顕", "驒": "騨", "彥": "彦", "晧": "皓",
})
_NAME_NOISE = re.compile(r"[\s.,・･'’\-‐－]+")
_JAPANESE = re.compile(r"[぀-ヿ㐀-鿿]")
_KANA_OR_KANJI_ONLY = re.compile(r"^[぀-ヿ㐀-鿿々〆ヶ]+$")
_CAMEL = re.compile(r"(?<=[a-z])(?=[A-Z])")


def is_japanese(text: Any) -> bool:
    return bool(_JAPANESE.search(str(text or "")))


def fold_name(name: Any) -> str:
    """A personal name without spacing, punctuation, case, or variant characters, for telling people apart."""
    value = unicodedata.normalize("NFKC", str(name or "")).translate(_VARIANTS)
    return _NAME_NOISE.sub("", value).casefold()


def person_key(name: Any, birth_date: Any) -> tuple[str, str]:
    """Who an officer is: the folded name with the date of birth."""
    return fold_name(name), str(birth_date or "")


def _capitalised(word: str) -> str:
    return word if any(char.islower() for char in word) else "-".join(part.capitalize() for part in word.split("-"))


def english_person_name(label: Any, member: Any = "") -> str:
    """An officer's name in English, given name first: ``Toyoda, Akio`` → ``Akio Toyoda``.

    ``label`` is the filer's English label for the person. Most write the
    family name, a comma, and the given name; a label without a comma is kept
    in the order written. Without a usable label the element name the filer
    coined for the person (``ToyodaAkioMember``) is split into its words.
    """
    text = clean(label)
    if not text or is_japanese(text):
        text = clean(_CAMEL.sub(" ", re.sub(r"Member$", "", str(member or ""))))
        if not text or is_japanese(text) or re.fullmatch(r"(?:Row|No)?\s*\d+", text):
            return ""
    if "," in text:
        family, _, given = text.partition(",")
        text = f"{given.strip()} {family.strip()}".strip()
    return " ".join(_capitalised(word) for word in text.split())


# --- holders ------------------------------------------------------------------

# Legal forms as they are abbreviated in brackets beside a name; anything else in brackets after a holder's
# name is a detail of the holding (a trust account, a standing proxy), not part of who the holder is.
_LEGAL_BRACKETS = {"株", "有", "相", "同", "合", "資", "名", "社", "財", "学", "医", "宗", "一社", "公社", "一財", "公財", "特非", "独", "国", "地独", "福", "農", "信"}
_LEGAL_WORDS = re.compile(
    r"株式会社|有限会社|合同会社|合資会社|合名会社|相互会社|\((?:株|有|相|同|合|資|名)\)|"
    r"\b(?:co\.?,?\s*ltd\.?|company,?\s*limited|corporation|incorporated|inc\.?|limited|ltd\.?|llc|l\.l\.c\.|plc|k\.k\.|kabushiki\s+kaisha)(?![a-z])",
    re.IGNORECASE,
)
_KEY_NOISE = re.compile(r"[\s・･.,'’&\-‐－/()]+")
_PROXY = re.compile(r"[\(\[]?\s*常任代理人\s*[:：]?\s*(.*)$")
_TRAILING_NOTE = re.compile(r"\s*(?:\(\s*注\s*\d*\s*\)\s*\d*|注\s*\d+|[※*]\s*\d*)\s*$")
_TRAILING_ACCOUNT = re.compile(r"^(.*\D)\s+((?:\d{4,}[\s\-/]*)+)$")
_TRUSTEE_PREFIX = re.compile(r"^(.*?(?:信託受託者|信託の受託者|受託者))\s*(?=\S)")
_ORGANISATION_WORDS = (
    "会社", "銀行", "信託", "証券", "保険", "金庫", "組合", "持株会", "持ち株会", "財団", "社団", "法人", "協会", "機構", "基金", "公社",
    "公庫", "事業団", "大臣", "商店", "商事", "商会", "工業", "産業", "興業", "興産", "不動産", "ホールディングス", "グループ", "ファンド", "投資",
    "カンパニー", "バンク", "トラスト", "リミテッド", "エルエルシー", "インク", "コーポレーション", "エルピー", "パートナーズ", "キャピタル",
    "アセット", "マネジメント", "インベストメント", "インターナショナル", "セキュリティーズ", "ノミニーズ", "エヌ・エイ", "エス・エー", "ピーエルシー",
    "共済", "連合会", "農業協同", "学園", "大学", "病院", "神社", "自己株", "当社", "市役所", "共栄会", "協力会", "取引先", "従業員",
    "社員", "役員", "厚生会", "互助会", "研究所", "事務所", "企業", "製作所", "本社", "本店", "総業", "物産", "建設", "電機", "電気", "鉄道", "運輸", "倉庫",
)
_ORGANISATION_LATIN = re.compile(
    r"\b(?:bank|trust|fund|funds|securities|capital|partners|company|corp|corporation|inc|ltd|limited|llc|lp|l\.p|plc|sa|s\.a|ag|nv|n\.v|"
    r"gmbh|pte|bv|b\.v|sarl|account|a/c|ac|client|clients|nominees|investment|investments|management|holdings|group|international|"
    r"government|portfolio|treaty|omnibus|custody|association|foundation|university|pension|insurance|asset|assets|global|master|"
    r"equity|street|brokers|morgan|sachs|nomura|ubs|hsbc|bnp|bny|bnym|jpmorgan|citibank|barclays|euroclear|clearstream)\b",
    re.IGNORECASE,
)


def _trailing_bracket(text: str) -> tuple[int, str] | None:
    """Where the bracketed group that ends ``text`` starts, and what is inside it; brackets may nest."""
    stripped = text.rstrip()
    if not stripped.endswith(")"):
        return None
    depth = 0
    for index in range(len(stripped) - 1, -1, -1):
        if stripped[index] == ")":
            depth += 1
        elif stripped[index] == "(":
            depth -= 1
            if depth == 0:
                return index, stripped[index + 1:-1]
    return None


def split_holder(filed: Any) -> tuple[str, str]:
    """A filed shareholder name as (who holds, what account or proxy the name also states).

    ``日本マスタートラスト信託銀行㈱（信託口）`` is the bank and its trust
    account; ``STATE STREET BANK AND TRUST COMPANY 505001（常任代理人 ㈱みずほ銀行）``
    is the bank, an account number, and the standing proxy.
    """
    text = clean(filed)
    details: list[str] = []
    proxy = _PROXY.search(text)
    if proxy:
        details.append("常任代理人 " + proxy.group(1).strip(" )]"))
        text = text[:proxy.start()].strip()
    for _ in range(4):
        text = _TRAILING_NOTE.sub("", text)
        bracket = _trailing_bracket(text)
        if bracket is None or compact(bracket[1]) in _LEGAL_BRACKETS or not text[:bracket[0]].strip():
            break
        details.insert(0, bracket[1].strip())
        text = text[:bracket[0]].strip()
    account = _TRAILING_ACCOUNT.match(text)
    if account:
        details.insert(0, account.group(2).strip())
        text = account.group(1).strip()
    trustee = _TRUSTEE_PREFIX.match(text)
    # "特定有価証券信託受託者 ㈱SMBC信託銀行" names the trustee last; a prefix that itself names a company is the holder.
    if trustee and len(text) > trustee.end() and not re.search(r"会社|銀行|\((?:株|有)\)", trustee.group(1)):
        details.insert(0, trustee.group(1).strip())
        text = text[trustee.end():].strip()
    return text, "; ".join(detail for detail in details if detail)


def holder_key(holder: Any) -> str:
    """An organisation's name without its legal form, spacing, punctuation, or case."""
    value = unicodedata.normalize("NFKC", str(holder or ""))
    return _KEY_NOISE.sub("", _LEGAL_WORDS.sub(" ", value)).casefold()


def is_individual(holder: Any) -> bool:
    """Whether a shareholder's name reads as a person's: a short name in kanji or kana with no word that marks an organisation."""
    text = clean(holder)
    if not text:
        return False
    if any(word in text for word in _ORGANISATION_WORDS) or re.search(r"\((?:株|有|相|同|合|資|名)\)", text):
        return False
    squeezed = compact(text)
    if _KANA_OR_KANJI_ONLY.match(squeezed):
        if squeezed in _PREFECTURE_NAMES or re.fullmatch(r".{1,4}[市町村]", squeezed) and len(text.split()) == 1 and len(squeezed) <= 4:
            return False
        if re.fullmatch(r"[\u30a0-\u30ff]+", squeezed):
            # A name all in katakana is a person when it is written as two short words: ヨシダ トモヒロ.
            words = text.split()
            return len(words) == 2 and all(2 <= len(word) <= 6 for word in words)
        return 2 <= len(squeezed) <= 9
    if is_japanese(text):
        return False
    words = text.replace(".", " ").split()
    return 2 <= len(words) <= 4 and not _ORGANISATION_LATIN.search(text) and not any(char.isdigit() for char in text)


# Institutions that fill shareholder lists. Most do not file annual reports with EDINET, so Company Analysis
# has no name for them: custody and trust banks, insurers, and the global custodians behind foreign investors.
# A family is matched at the start of the holder's name; whatever follows (an account, a fund, a client
# description) is a detail of the holding. Each entry is (pattern, published English name, the Japanese legal
# name when the institution may file with EDINET itself). Japanese patterns see the name without spaces or
# middle dots and without a leading legal form; Latin ones see it in capitals with single spaces.
_JAPANESE_FAMILIES: tuple[tuple[str, str, str], ...] = (
    ("日本マスタートラスト信託銀行", "The Master Trust Bank of Japan, Ltd.", ""),
    ("日本カストディ銀行", "Custody Bank of Japan, Ltd.", ""),
    ("日本トラスティサービス信託銀行", "Japan Trustee Services Bank, Ltd. (now Custody Bank of Japan)", ""),
    ("資産管理サービス信託銀行", "Trust & Custody Services Bank, Ltd. (now Custody Bank of Japan)", ""),
    ("野村信託銀行", "The Nomura Trust and Banking Co., Ltd.", "野村信託銀行株式会社"),
    ("三菱UFJ信託銀行", "Mitsubishi UFJ Trust and Banking Corporation", "三菱UFJ信託銀行株式会社"),
    ("三井住友信託銀行", "Sumitomo Mitsui Trust Bank, Limited", "三井住友信託銀行株式会社"),
    ("みずほ信託銀行", "Mizuho Trust & Banking Co., Ltd.", "みずほ信託銀行株式会社"),
    ("SMBC信託銀行", "SMBC Trust Bank Ltd.", "株式会社SMBC信託銀行"),
    ("三菱東京UFJ銀行", "MUFG Bank, Ltd.", "株式会社三菱UFJ銀行"),
    ("三菱UFJ銀行", "MUFG Bank, Ltd.", "株式会社三菱UFJ銀行"),
    ("三井住友銀行", "Sumitomo Mitsui Banking Corporation", "株式会社三井住友銀行"),
    ("みずほ銀行", "Mizuho Bank, Ltd.", "株式会社みずほ銀行"),
    ("埼玉りそな銀行", "Saitama Resona Bank, Limited", "株式会社埼玉りそな銀行"),
    ("りそな銀行", "Resona Bank, Limited", "株式会社りそな銀行"),
    ("日本生命保険", "Nippon Life Insurance Company", "日本生命保険相互会社"),
    ("明治安田生命保険", "Meiji Yasuda Life Insurance Company", "明治安田生命保険相互会社"),
    ("住友生命保険", "Sumitomo Life Insurance Company", "住友生命保険相互会社"),
    ("第一生命保険", "The Dai-ichi Life Insurance Company, Limited", "第一生命保険株式会社"),
    ("朝日生命保険", "Asahi Mutual Life Insurance Company", "朝日生命保険相互会社"),
    ("富国生命保険", "Fukoku Mutual Life Insurance Company", "富国生命保険相互会社"),
    ("大樹生命保険", "Taiju Life Insurance Company Limited", "大樹生命保険株式会社"),
    ("三井生命保険", "Mitsui Life Insurance Company Limited (now Taiju Life)", ""),
    ("太陽生命保険", "Taiyo Life Insurance Company", "太陽生命保険株式会社"),
    ("大同生命保険", "Daido Life Insurance Company", "大同生命保険株式会社"),
    ("かんぽ生命保険", "Japan Post Insurance Co., Ltd.", "株式会社かんぽ生命保険"),
    ("東京海上日動火災保険", "Tokio Marine & Nichido Fire Insurance Co., Ltd.", "東京海上日動火災保険株式会社"),
    ("三井住友海上火災保険", "Mitsui Sumitomo Insurance Company, Limited", "三井住友海上火災保険株式会社"),
    ("損害保険ジャパン", "Sompo Japan Insurance Inc.", "損害保険ジャパン株式会社"),
    ("損保ジャパン", "Sompo Japan Insurance Inc.", "損害保険ジャパン株式会社"),
    ("あいおいニッセイ同和損害保険", "Aioi Nissay Dowa Insurance Co., Ltd.", "あいおいニッセイ同和損害保険株式会社"),
    ("農林中央金庫", "The Norinchukin Bank", "農林中央金庫"),
    ("ステートストリートバンクアンドトラスト(?:カンパニー)?", "State Street Bank and Trust Company", ""),
    ("ステートストリートバンク", "State Street Bank and Trust Company", ""),
    ("ジェーピーモルガンチェースバンク", "JPMorgan Chase Bank, N.A.", ""),
    ("ジェーピーモルガンバンクルクセンブルグ", "J.P. Morgan SE, Luxembourg Branch", ""),
    ("ザチェースマンハッタンバンク", "JPMorgan Chase Bank, N.A.", ""),
    ("チェースマンハッタンバンク", "JPMorgan Chase Bank, N.A.", ""),
    ("ザバンクオブニューヨークメロン", "The Bank of New York Mellon", ""),
    ("ザバンクオブニューヨーク", "The Bank of New York Mellon", ""),
    ("バンクオブニューヨーク", "The Bank of New York Mellon", ""),
    ("バンクオブニユーヨーク", "The Bank of New York Mellon", ""),
    ("ビーエヌワイエム", "The Bank of New York Mellon", ""),
    ("ノーザントラスト", "The Northern Trust Company", ""),
    ("ゴールドマンサックスインターナショナル", "Goldman Sachs International", ""),
    ("ゴールドマンサックスアンドカンパニー", "Goldman Sachs & Co. LLC", ""),
    ("モルガンスタンレーアンドカンパニーインターナショナル", "Morgan Stanley & Co. International plc", ""),
    ("モルガンスタンレーアンドカンパニー", "Morgan Stanley & Co. LLC", ""),
    ("エムエスアイピ", "Morgan Stanley & Co. International plc", ""),
    ("ビーエヌピーパリバ", "BNP Paribas", ""),
    ("ユービーエスエイジー", "UBS AG", ""),
    ("シティバンク", "Citibank, N.A.", ""),
    ("ビービーエイチ", "Brown Brothers Harriman & Co.", ""),
)
_LATIN_FAMILIES: tuple[tuple[str, str], ...] = (
    (r"STATE STREET BANK(?: AND TRUST(?: COMPANY| CO\.?)?)?|SSBTC", "State Street Bank and Trust Company"),
    (r"STATE STREET (?:LONDON|CLIENT)", "State Street Bank and Trust Company"),
    (r"JP ?MORGAN CHASE BANK(?:,? ?N\.?A\.?)?|JPMCB|JPMC", "JPMorgan Chase Bank, N.A."),
    (r"THE CHASE MANHATTAN BANK(?:,? ?N\.?A\.?)?|CHASE MANHATTAN BANK", "JPMorgan Chase Bank, N.A."),
    (r"J\.? ?P\.? ?MORGAN SECURITIES PLC|JPMSP", "J.P. Morgan Securities plc"),
    (r"J\.? ?P\.? ?MORGAN SECURITIES LLC|JPLLC", "J.P. Morgan Securities LLC"),
    (r"J\.? ?P\.? ?MORGAN BANK LUXEMBOURG(?: S\.?A\.?)?|JPMBL|JP JPMSE LUX|J\.? ?P\.? ?MORGAN SE", "J.P. Morgan SE, Luxembourg Branch"),
    (r"THE BANK OF NEW YORK(?: MELLON)?(?: SA/NV)?|BANK OF NEW YORK(?: MELLON)?|BNYMSANV|BNYM|BNY", "The Bank of New York Mellon"),
    (r"THE NORTHERN TRUST(?: COMPANY| CO\.?)?|NORTHERN TRUST(?: COMPANY| CO\.?)?", "The Northern Trust Company"),
    (r"MSIP|MORGAN STANLEY (?:&|AND) CO\.? INTERNATIONAL(?: PLC)?", "Morgan Stanley & Co. International plc"),
    (r"MSCO|MORGAN STANLEY (?:&|AND) ?CO\.?,? ?LLC|MORGAN STANLEY SMITH BARNEY(?: LLC)?", "Morgan Stanley & Co. LLC"),
    (r"GOLDMAN,? ?SACHS INTERNATIONAL", "Goldman Sachs International"),
    (r"GOLDMAN,? ?SACHS BANK EUROPE(?: SE)?", "Goldman Sachs Bank Europe SE"),
    (r"GOLDMAN,? ?SACHS ?(?:&|AND) ?CO\.?(?:,? ?LLC|,? ?REG)?", "Goldman Sachs & Co. LLC"),
    (r"UBS AG", "UBS AG"),
    (r"CREDIT SUISSE(?: AG)?", "Credit Suisse AG"),
    (r"BNP PARIBAS(?: SECURITIES SERVICES)?", "BNP Paribas"),
    (r"CACEIS(?: BANK)?", "CACEIS Bank"),
    (r"THE HONGKONG AND SHANGHAI BANKING CORPORATION(?: LIMITED| LTD\.?)?|HSBC(?: BANK PLC)?", "HSBC"),
    (r"CITIBANK,? ?N\.?A\.?|CITIGROUP GLOBAL MARKETS(?: LIMITED| LTD\.?| INC\.?)?|CGML|CGMI|CBNY|CBLDN|CBHK", "Citi"),
    (r"BARCLAYS(?: CAPITAL SECURITIES(?: LIMITED| LTD\.?)?| BANK PLC)?|BCSL", "Barclays"),
    (r"DEUTSCHE BANK(?: AG)?", "Deutsche Bank AG"),
    (r"SOCIETE GENERALE", "Société Générale"),
    (r"BOFAS?(?: SECURITIES)?|MERRILL LYNCH(?: INTERNATIONAL)?|MLI|MLPFS|ML INTL", "BofA Securities (Merrill Lynch)"),
    (r"NOMURA INTERNATIONAL(?: PLC)?", "Nomura International plc"),
    (r"NOMURA PB NOMINEES(?: LIMITED| LTD\.?)?", "Nomura PB Nominees Limited"),
    (r"THE NOMURA TRUST AND BANKING CO\.?,? ?LTD\.?", "The Nomura Trust and Banking Co., Ltd."),
    (r"INTERACTIVE BROKERS(?: LLC)?", "Interactive Brokers LLC"),
    (r"GOVERNMENT OF NORWAY|NORGES BANK", "Government of Norway (Norges Bank)"),
    (r"BBH ?\(LUX\)|BBH/?|BROWN BROTHERS HARRIMAN(?: ?& ?CO\.?)?", "Brown Brothers Harriman & Co."),
    (r"DFA ", "Dimensional Fund Advisors funds"),
    (r"EUROCLEAR BANK(?: S\.?A\.?/N\.?V\.?)?", "Euroclear Bank"),
    (r"CLEARSTREAM BANKING(?: S\.?A\.?)?", "Clearstream Banking"),
    (r"RBC ", "RBC Investor Services"),
    (r"KIA FUND", "Kuwait Investment Authority"),
    (r"PERSHING", "Pershing LLC"),
    (r"NATIONAL FINANCIAL SERVICES(?: LLC)?", "National Financial Services LLC"),
    (r"KOREA SECURITIES DEPOSITORY|KSD", "Korea Securities Depository"),
    (r"SIX SIS(?: LTD\.?| AG)?", "SIX SIS Ltd."),
    (r"DBS BANK(?: LTD\.?)?", "DBS Bank Ltd."),
    (r"BANK JULIUS BAER(?: AND CO\.? LTD\.?| & CO\.? LTD\.?)?", "Bank Julius Baer & Co. Ltd."),
    (r"GIC PRIVATE LIMITED", "GIC Private Limited"),
)
_JAPANESE_FAMILY_PATTERNS = tuple((re.compile(pattern), name, legal) for pattern, name, legal in _JAPANESE_FAMILIES)
_LATIN_FAMILY_PATTERNS = tuple((re.compile(rf"(?:{pattern})(?![A-Z])", re.IGNORECASE), name) for pattern, name in _LATIN_FAMILIES)
_LEADING_FORM = re.compile(r"^(?:株式会社|有限会社|\((?:株|有)\))")
_JOINERS = re.compile(r"[\s・･]+")

# Organisations named exactly: (Japanese legal name, published English name).
_KNOWN_NAMES: tuple[tuple[str, str], ...] = (
    ("日本証券金融株式会社", "Japan Securities Finance Co., Ltd."),
    ("野村證券株式会社", "Nomura Securities Co., Ltd."),
    ("野村証券株式会社", "Nomura Securities Co., Ltd."),
    ("大和証券株式会社", "Daiwa Securities Co. Ltd."),
    ("SMBC日興証券株式会社", "SMBC Nikko Securities Inc."),
    ("みずほ証券株式会社", "Mizuho Securities Co., Ltd."),
    ("三菱UFJモルガン・スタンレー証券株式会社", "Mitsubishi UFJ Morgan Stanley Securities Co., Ltd."),
    ("モルガン・スタンレーMUFG証券株式会社", "Morgan Stanley MUFG Securities Co., Ltd."),
    ("JPモルガン証券株式会社", "JPMorgan Securities Japan Co., Ltd."),
    ("ゴールドマン・サックス証券株式会社", "Goldman Sachs Japan Co., Ltd."),
    ("BNPパリバ証券株式会社", "BNP Paribas Securities (Japan) Limited"),
    ("バークレイズ証券株式会社", "Barclays Securities Japan Limited"),
    ("UBS証券株式会社", "UBS Securities Japan Co., Ltd."),
    ("株式会社SBI証券", "SBI SECURITIES Co., Ltd."),
    ("楽天証券株式会社", "Rakuten Securities, Inc."),
    ("マネックス証券株式会社", "Monex, Inc."),
    ("松井証券株式会社", "Matsui Securities Co., Ltd."),
    ("auカブコム証券株式会社", "au Kabucom Securities Co., Ltd."),
    ("カブドットコム証券株式会社", "kabu.com Securities Co., Ltd."),
    ("立花証券株式会社", "The Tachibana Securities Co., Ltd."),
    ("インタラクティブ・ブローカーズ証券株式会社", "Interactive Brokers Securities Japan, Inc."),
    ("東京短資株式会社", "Tokyo Tanshi Co., Ltd."),
    ("商工組合中央金庫", "The Shoko Chukin Bank, Ltd."),
    ("株式会社商工組合中央金庫", "The Shoko Chukin Bank, Ltd."),
    ("信金中央金庫", "Shinkin Central Bank"),
    ("株式会社日本政策投資銀行", "Development Bank of Japan Inc."),
    ("全国共済農業協同組合連合会", "National Mutual Insurance Federation of Agricultural Cooperatives (JA Kyosairen)"),
    ("預金保険機構", "Deposit Insurance Corporation of Japan"),
    ("株式会社整理回収機構", "The Resolution and Collection Corporation"),
    ("日本銀行", "Bank of Japan"),
    ("共栄火災海上保険株式会社", "Kyoei Fire & Marine Insurance Co., Ltd."),
    ("日新火災海上保険株式会社", "Nisshin Fire & Marine Insurance Co., Ltd."),
    ("セコム損害保険株式会社", "Secom General Insurance Co., Ltd."),
    ("AIG損害保険株式会社", "AIG General Insurance Company, Ltd."),
    ("大同火災海上保険株式会社", "The Daido Fire and Marine Insurance Co., Ltd."),
    ("株式会社北海道銀行", "The Hokkaido Bank, Ltd."),
    ("株式会社北陸銀行", "The Hokuriku Bank, Ltd."),
    ("株式会社山口銀行", "The Yamaguchi Bank, Ltd."),
    ("株式会社もみじ銀行", "Momiji Bank, Ltd."),
    ("株式会社北九州銀行", "The Kitakyushu Bank, Ltd."),
    ("株式会社足利銀行", "The Ashikaga Bank, Ltd."),
    ("株式会社常陽銀行", "The Joyo Bank, Ltd."),
    ("株式会社鹿児島銀行", "The Kagoshima Bank, Ltd."),
    ("株式会社肥後銀行", "The Higo Bank, Ltd."),
    ("株式会社十八親和銀行", "The Juhachi-Shinwa Bank, Ltd."),
    ("株式会社福岡銀行", "The Bank of Fukuoka, Ltd."),
    ("株式会社熊本銀行", "The Kumamoto Bank, Ltd."),
    ("株式会社西日本シティ銀行", "The Nishi-Nippon City Bank, Ltd."),
    ("株式会社十六銀行", "The Juroku Bank, Ltd."),
    ("株式会社中国銀行", "The Chugoku Bank, Limited"),
    ("株式会社横浜銀行", "The Bank of Yokohama, Ltd."),
    ("株式会社東日本銀行", "The Higashi-Nippon Bank, Limited"),
    ("株式会社静岡銀行", "The Shizuoka Bank, Ltd."),
    ("株式会社広島銀行", "The Hiroshima Bank, Ltd."),
    ("株式会社伊予銀行", "The Iyo Bank, Ltd."),
    ("株式会社池田泉州銀行", "The Senshu Ikeda Bank, Ltd."),
    ("株式会社関西みらい銀行", "Kansai Mirai Bank, Limited"),
    ("株式会社近畿大阪銀行", "The Kinki Osaka Bank, Ltd. (now Kansai Mirai Bank)"),
    ("株式会社みなと銀行", "The Minato Bank, Ltd."),
    ("株式会社三十三銀行", "San ju San Bank, Ltd."),
    ("株式会社あいち銀行", "Aichi Bank, Ltd."),
    ("株式会社北國銀行", "The Hokkoku Bank, Ltd."),
    ("株式会社第四北越銀行", "Daishi Hokuetsu Bank, Ltd."),
    ("株式会社香川銀行", "The Kagawa Bank, Ltd."),
    ("株式会社徳島大正銀行", "The Tokushima Taisho Bank, Ltd."),
    ("株式会社紀陽銀行", "The Kiyo Bank, Ltd."),
    ("株式会社京都銀行", "The Bank of Kyoto, Ltd."),
    ("株式会社八十二銀行", "The Hachijuni Bank, Ltd."),
    ("株式会社千葉銀行", "The Chiba Bank, Ltd."),
    ("株式会社群馬銀行", "The Gunma Bank, Ltd."),
    ("株式会社七十七銀行", "The 77 Bank, Ltd."),
    ("株式会社きらぼし銀行", "Kiraboshi Bank, Ltd."),
    ("京都中央信用金庫", "The Kyoto Chuo Shinkin Bank"),
    ("岐阜信用金庫", "The Gifu Shinkin Bank"),
    ("岡崎信用金庫", "The Okazaki Shinkin Bank"),
    ("多摩信用金庫", "Tama Shinkin Bank"),
    ("麒麟麦酒株式会社", "Kirin Brewery Company, Limited"),
    ("三井住友カード株式会社", "Sumitomo Mitsui Card Company, Limited"),
    ("株式会社講談社", "Kodansha Ltd."),
    ("株式会社集英社", "Shueisha Inc."),
    ("株式会社TBSテレビ", "Tokyo Broadcasting System Television, Inc."),
    ("株式会社中日新聞社", "The Chunichi Shimbun"),
    ("株式会社毎日新聞社", "The Mainichi Newspapers Co., Ltd."),
    ("株式会社読売新聞グループ本社", "The Yomiuri Shimbun Holdings"),
    ("株式会社イトーヨーカ堂", "Ito-Yokado Co., Ltd."),
    ("株式会社三越伊勢丹", "Isetan Mitsukoshi Ltd."),
    ("株式会社ヨドバシカメラ", "Yodobashi Camera Co., Ltd."),
    ("日清製粉株式会社", "Nisshin Flour Milling Inc."),
    ("三菱ふそうトラック・バス株式会社", "Mitsubishi Fuso Truck and Bus Corporation"),
    ("東日本電信電話株式会社", "Nippon Telegraph and Telephone East Corporation"),
    ("西日本電信電話株式会社", "Nippon Telegraph and Telephone West Corporation"),
    ("大鵬薬品工業株式会社", "Taiho Pharmaceutical Co., Ltd."),
    ("株式会社大塚製薬工場", "Otsuka Pharmaceutical Factory, Inc."),
    ("大塚化学株式会社", "Otsuka Chemical Co., Ltd."),
    ("西武鉄道株式会社", "Seibu Railway Co., Ltd."),
    ("アルフレッサ株式会社", "Alfresa Corporation"),
    ("宝酒造株式会社", "Takara Shuzo Co., Ltd."),
    ("財務大臣", "Minister of Finance (Government of Japan)"),
    ("経済産業大臣", "Minister of Economy, Trade and Industry (Government of Japan)"),
    ("国土交通大臣", "Minister of Land, Infrastructure, Transport and Tourism (Government of Japan)"),
    ("総務大臣", "Minister for Internal Affairs and Communications (Government of Japan)"),
)
_PREFECTURES = {
    "北海道": "Hokkaido", "青森": "Aomori", "岩手": "Iwate", "宮城": "Miyagi", "秋田": "Akita", "山形": "Yamagata", "福島": "Fukushima",
    "茨城": "Ibaraki", "栃木": "Tochigi", "群馬": "Gunma", "埼玉": "Saitama", "千葉": "Chiba", "東京": "Tokyo", "神奈川": "Kanagawa",
    "新潟": "Niigata", "富山": "Toyama", "石川": "Ishikawa", "福井": "Fukui", "山梨": "Yamanashi", "長野": "Nagano", "岐阜": "Gifu",
    "静岡": "Shizuoka", "愛知": "Aichi", "三重": "Mie", "滋賀": "Shiga", "京都": "Kyoto", "大阪": "Osaka", "兵庫": "Hyogo", "奈良": "Nara",
    "和歌山": "Wakayama", "鳥取": "Tottori", "島根": "Shimane", "岡山": "Okayama", "広島": "Hiroshima", "山口": "Yamaguchi",
    "徳島": "Tokushima", "香川": "Kagawa", "愛媛": "Ehime", "高知": "Kochi", "福岡": "Fukuoka", "佐賀": "Saga", "長崎": "Nagasaki",
    "熊本": "Kumamoto", "大分": "Oita", "宮崎": "Miyazaki", "鹿児島": "Kagoshima", "沖縄": "Okinawa",
}
_PREFECTURE_NAMES = {"北海道", *(f"{name}{suffix}" for name in _PREFECTURES for suffix in ("県", "府", "都"))}
_ROMAN_FORMS = (
    ("株式会社", "K.K."), ("(株)", "K.K."), ("有限会社", "Y.K."), ("(有)", "Y.K."), ("合同会社", "G.K."), ("(同)", "G.K."),
    ("投資事業有限責任組合", "Investment Limited Partnership"), ("投資事業組合", "Investment Partnership"),
)
_KNOWN_BY_KEY = {legal_key(name): english for name, english in _KNOWN_NAMES}
_ASSOCIATIONS = (
    (re.compile(r"^(.*?)(?:グループ)?(?:従業員|社員|職員|行員|自社株)持ち?株会$"), "Employee Shareholding Association"),
    (re.compile(r"^(.*?)(?:役員)持ち?株会$"), "Officers' Shareholding Association"),
    (re.compile(r"^(.*?)(?:取引先|協力会社|共栄会|協力会)持ち?株会$"), "Business Partners' Shareholding Association"),
)


def known_family(holder: Any) -> tuple[str, str, str] | None:
    """A well-known institution at the start of a holder's name: (English name, Japanese legal name, the rest).

    The rest is whatever the filing wrote after the institution's name: an
    account, a fund, or the client it holds for.
    """
    text = clean(holder)
    if is_japanese(text):
        squeezed = _LEADING_FORM.sub("", _JOINERS.sub("", text))
        for pattern, name, legal in _JAPANESE_FAMILY_PATTERNS:
            match = pattern.match(squeezed)
            if match:
                rest = re.sub(r"^(?:株式会社|相互会社|\((?:株|相)\))", "", squeezed[match.end():])
                return name, legal, rest
        return None
    for latin, name in _LATIN_FAMILY_PATTERNS:
        match = latin.match(text)
        if match:
            return name, "", text[match.end():].strip(" -,/")
    return None


_BENEFICIARY = re.compile(r"^(?:\([A-Z]+\)\s*)?(?:FOR|RE)\s+(\S.{3,})$", re.IGNORECASE)


def beneficiary(rest: str) -> str | None:
    """The fund or client a custodian says it holds for: ``FOR FIDELITY LOW-PRICED STOCK FUND``."""
    match = _BENEFICIARY.match(rest)
    return match.group(1).strip() if match else None


def known_name(holder: Any) -> str | None:
    """The published English name of an organisation this module lists by its exact legal name, or builds by rule.

    Rules cover what can be said without translating a proper name: a
    prefecture, and a name already in Latin letters apart from its legal form
    (``株式会社UH Partners 3`` → "UH Partners 3 K.K.").
    """
    text = clean(holder)
    found = _KNOWN_BY_KEY.get(legal_key(text))
    if found:
        return found
    squeezed = compact(text)
    if squeezed.endswith(("県", "府", "都")) and squeezed[:-1] in _PREFECTURES:
        return f"{_PREFECTURES[squeezed[:-1]]} Prefecture"
    if squeezed == "北海道":
        return "Hokkaido Prefecture"
    for word, form in _ROMAN_FORMS:
        if word in text:
            core = clean(text.replace(word, " "))
            if core and not is_japanese(core):
                return f"{core} {form}"
    return None


def association(holder: Any) -> tuple[str, str] | None:
    """A shareholding association as (whose it is, what kind in English): ``トヨタ自動車従業員持株会``."""
    text = compact(holder)
    for pattern, label in _ASSOCIATIONS:
        match = pattern.match(text)
        if match:
            return match.group(1), label
    return None


_ACCOUNT_KINDS = (
    ("退職給付信託", "Retirement benefit trust account"),
    ("年金特金", "Pension fund account"),
    ("年金信託", "Pension trust account"),
    ("年金", "Pension account"),
    ("証券投資信託", "Investment trust account"),
    ("投信", "Investment trust account"),
    ("管理信託", "Custody trust account"),
    ("再信託", "Re-trust account"),
    ("役員報酬", "Officers' share compensation trust"),
    ("株式給付信託", "Share benefit trust (employee or officer plan)"),
    ("従業員持株ESOP", "Employee stock ownership trust"),
    ("ESOP", "Employee stock ownership trust"),
    ("BBT", "Board benefit trust"),
    ("J-ESOP", "Employee stock ownership trust"),
    ("特定金銭信託", "Specified money trust account"),
    ("金銭信託", "Money trust account"),
    ("デポジタリ", "Depositary for depositary receipt holders"),
    ("信託受託者", "As trustee"),
    ("受託者", "As trustee"),
    ("信託口", "Trust account"),
    ("信託", "Trust account"),
    ("特別勘定", "Separate account"),
    ("一般勘定", "General account"),
)
_TRUST_NUMBER = re.compile(r"^信託口\s*(\d+)$")
_ACCOUNT_NUMBER = re.compile(r"^[\d\s\-/]+$")


def account_label(account: Any) -> str:
    """What a holding's account or proxy note says, in English, where its words are standard ones."""
    labels: list[str] = []
    for part in str(account or "").split("; "):
        text = compact(part)
        if not text:
            continue
        if text.startswith("常任代理人"):
            proxy = clean(part).removeprefix("常任代理人").strip()
            family = known_family(proxy)
            labels.append(f"Standing proxy: {family[0] if family else known_name(proxy) or proxy}" if proxy else "Standing proxy")
            continue
        numbered = _TRUST_NUMBER.match(text)
        if numbered:
            labels.append(f"Trust account {numbered.group(1)}")
        elif _ACCOUNT_NUMBER.match(text):
            labels.append(f"Account {clean(part)}")
        elif not is_japanese(text):
            labels.append(clean(part))
        else:
            # A note in words this does not know is still a note: the filed text is kept beside the label.
            labels.append(next((label for word, label in _ACCOUNT_KINDS if word in text), "Named account"))
    return "; ".join(dict.fromkeys(labels))


# --- titles -------------------------------------------------------------------

_PLACEHOLDER = re.compile(r"^[\-‐‑‒–—―ー−─]+$")
_TITLE_NOTE = re.compile(r"\(\s*注\s*\d*\s*\)\s*\d*(?:\s*[、,.]\s*\d+)*|注\s*\d+|[※*]\s*\d+")
_C_SUITE = re.compile(r"(?<![A-Za-z])(CEO|COO|CFO|CTO|CIO|CHRO|CSO|CMO|CDO|CRO|CCO|CISO|CAO|CLO|CPO|CXO)(?![A-Za-z])")
# The longer rank first: 副社長執行役員 contains 社長執行役員.
_OFFICER_RANKS = (
    ("副社長執行役員", "Executive Vice President"),
    ("執行役員副社長", "Executive Vice President"),
    ("社長執行役員", "President"),
    ("執行役員社長", "President"),
    ("専務執行役員", "Senior Managing Executive Officer"),
    ("執行役員専務", "Senior Managing Executive Officer"),
    ("常務執行役員", "Managing Executive Officer"),
    ("執行役員常務", "Managing Executive Officer"),
    ("専務役員", "Senior Managing Officer"),
    ("常務役員", "Managing Officer"),
    ("上席執行役員", "Senior Executive Officer"),
    ("上級執行役員", "Senior Executive Officer"),
    ("執行役員", "Executive Officer"),
)
_DEPARTMENTS = (
    ("経営企画", "Corporate Planning"), ("経営管理", "Corporate Management"), ("経営戦略", "Corporate Strategy"), ("事業開発", "Business Development"),
    ("事業戦略", "Business Strategy"), ("事業統括", "Business Operations"), ("営業統括", "Sales"), ("管理統括", "Administration"), ("研究開発", "Research and Development"),
    ("技術開発", "Technology Development"), ("商品開発", "Product Development"), ("製品開発", "Product Development"), ("品質保証", "Quality Assurance"),
    ("品質管理", "Quality Control"), ("生産管理", "Production Control"), ("生産技術", "Production Engineering"), ("情報システム", "Information Systems"),
    ("内部監査", "Internal Audit"), ("内部統制", "Internal Control"), ("財務経理", "Finance and Accounting"), ("経理財務", "Finance and Accounting"),
    ("総務人事", "General Affairs and Human Resources"), ("人事総務", "Human Resources and General Affairs"), ("コーポレート", "Corporate"),
    ("マーケティング", "Marketing"), ("コンプライアンス", "Compliance"), ("サステナビリティ", "Sustainability"), ("リスク管理", "Risk Management"),
    ("海外事業", "Overseas Business"), ("国内営業", "Domestic Sales"), ("海外営業", "Overseas Sales"), ("営業", "Sales"), ("管理", "Administration"),
    ("総務", "General Affairs"), ("経理", "Accounting"), ("財務", "Finance"), ("人事", "Human Resources"), ("企画", "Planning"), ("生産", "Production"),
    ("製造", "Manufacturing"), ("技術", "Technology"), ("開発", "Development"), ("研究", "Research"), ("購買", "Purchasing"), ("調達", "Procurement"),
    ("法務", "Legal"), ("業務", "Operations"), ("事業", "Business"), ("海外", "Overseas"), ("国内", "Domestic"), ("広報", "Public Relations"),
    ("物流", "Logistics"), ("工場", "Plant"), ("店舗", "Stores"), ("審査", "Credit"), ("監査", "Audit"), ("秘書", "Secretarial"), ("IR", "Investor Relations"),
    ("DX", "Digital Transformation"), ("IT", "IT"),
)
_UNIT_HEADS = (
    ("統括本部長", "Head of {} Headquarters"), ("事業本部長", "Head of {} Business Division"), ("本部長", "Head of {} Division"),
    ("事業部長", "Head of {} Business Unit"), ("部門長", "Head of {}"), ("部長", "General Manager, {}"), ("室長", "Head of {} Office"),
    ("グループ長", "Head of {} Group"), ("センター長", "Head of {} Center"), ("統括", "in charge of {}"), ("担当", "in charge of {}"), ("管掌", "overseeing {}"),
)


def _clean_title(title: Any) -> str:
    """A title's words with footnote marks, placeholder dashes, and the padding between single characters removed."""
    words: list[str] = []
    run = ""
    for token in _TITLE_NOTE.sub(" ", clean(title)).split():
        if _PLACEHOLDER.match(token):
            continue
        if len(token) == 1 and is_japanese(token):
            run += token
            continue
        if run:
            words.append(run)
            run = ""
        words.append(token)
    if run:
        words.append(run)
    return " ".join(words)


def _responsibility(word: str) -> str | None:
    """``営業本部長`` → "Head of Sales Division", when the unit is one of the usual ones."""
    for suffix, pattern in _UNIT_HEADS:
        if word.endswith(suffix) and len(word) > len(suffix):
            unit = word[:-len(suffix)]
            parts: list[str] = []
            while unit:
                found = next(((name, label) for name, label in _DEPARTMENTS if unit.startswith(name)), None)
                if found is None:
                    return None
                parts.append(found[1])
                unit = unit[len(found[0]):].lstrip("・兼")
            return pattern.format(" and ".join(dict.fromkeys(parts)))
    return None


def title_roles(title: Any) -> dict[str, bool]:
    """What a filed title says the person is: director, auditor, representative, outside, full-time."""
    text = compact(_clean_title(title))
    return {
        "director": "取締役" in text or "理事" in text,
        "auditor": "監査役" in text or "監事" in text,
        "audit_committee": "監査等委員" in text or "監査委員" in text,
        "representative": "代表取締役" in text or "代表執行役" in text or "代表理事" in text,
        "outside": "社外" in text or "独立" in text,
        "full_time": "常勤" in text and "非常勤" not in text,
        "executive": "執行役" in text,
    }


def title_en(title: Any) -> str:
    """A filed title in English, from the roles it states: ``代表取締役 社長`` → "President and Representative Director".

    Returns "" when the title names no role this recognises; the caller then
    shows the title as filed.
    """
    cleaned = _clean_title(title)
    text = compact(cleaned)
    if not text:
        return ""
    roles = title_roles(cleaned)
    # 取締役会長 is the chairman; 取締役会議長 chairs the board's meetings.
    seat = text.replace("取締役会議長", "").replace("監査役会", "")
    rank = next((label for word, label in _OFFICER_RANKS if word in seat), None)
    rest = seat
    for word, _ in _OFFICER_RANKS:
        rest = rest.replace(word, "")
    # 執行役 (without 員) is an officer of a company with committees, where 専務 and 常務 rank officers, not directors.
    statutory_officer = "執行役" in rest
    managing = "Executive Officer" if statutory_officer and "専務取締役" not in rest and "常務取締役" not in rest else "Director"
    position = None
    if re.search(r"会長兼(?:取締役)?社長|会長・社長", seat):
        position = "Chairman and President"
    elif "副会長" in rest:
        position = "Vice Chairman"
    elif "会長" in rest:
        position = "Chairman"
    elif "副頭取" in rest:
        position = "Deputy President"
    elif "副社長" in rest or rank == "Executive Vice President":
        position = "Executive Vice President"
    elif "社長" in rest or "頭取" in rest or rank == "President":
        position = "President"
    elif "専務" in rest:
        position = f"Senior Managing {managing}"
    elif "常務" in rest:
        position = f"Managing {managing}"
    if rank in ("President", "Executive Vice President"):
        # 社長執行役員 says who is president, which the position already states.
        rank = None
    representative = "Representative Executive Officer" if "代表執行役" in text and "代表取締役" not in text else "Representative Director"

    if position:
        if roles["representative"]:
            base = f"{position}{',' if ' and ' in position else ' and'} {representative}"
        elif position.startswith(("Senior Managing", "Managing")) or not roles["director"]:
            base = position
        else:
            base = f"{position} and Director"
    elif roles["representative"]:
        base = representative
    elif roles["director"]:
        base = "Director and Senior Advisor" if "相談役" in text else "Director and Advisor" if "顧問" in text else "Director"
        base = f"{'Outside ' if roles['outside'] else ''}{base}"
    elif roles["auditor"]:
        kind = "Standing " if "常任" in text else "Full-time " if roles["full_time"] else ""
        base = f"{kind}{'Outside ' if roles['outside'] else ''}Audit & Supervisory Board Member"
    elif roles["audit_committee"]:
        base = "Audit & Supervisory Committee Member" + (" (full-time)" if roles["full_time"] else "")
    elif statutory_officer or rank:
        base, rank, statutory_officer = rank or "Executive Officer", None, False
    elif "会計参与" in text:
        base = "Accounting Advisor"
    elif "相談役" in text:
        base = "Senior Advisor"
    elif "顧問" in text:
        base = "Advisor"
    else:
        return ""
    extra = rank or ("Executive Officer" if statutory_officer else None)
    if extra and not position and "Executive Officer" not in base:
        base = f"{base} and {extra}"
    if roles["director"] and "Director" not in base:
        # A director who is named by an officer's rank: 取締役 専務執行役.
        base = f"Director, {base}"
    if "取締役会議長" in text:
        base += ", Chair of the Board"
    if roles["audit_committee"] and (roles["director"] or position):
        base += " (Audit & Supervisory Committee Member" + (", full-time" if roles["full_time"] else "") + ")"
    elif "非常勤" in text:
        base += " (part-time)"
    codes = list(dict.fromkeys(_C_SUITE.findall(cleaned)))
    if codes:
        base += ", " + ", ".join(codes)
    duties = [duty for duty in (_responsibility(word) for word in re.split(r"[\s兼、,/]+", cleaned)) if duty]
    if duties:
        base += ", " + ", ".join(dict.fromkeys(duties))
    return base
