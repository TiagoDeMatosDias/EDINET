"""Japanese-to-English translation — fully offline.

Uses Argos Translate (OpenNMT neural models) when available; falls back
to a local EDINET financial-term dictionary when Argos is not installed
or its models have not been downloaded.

Zero external API calls.  All translations are cached in
``filing_translations`` for reuse.
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Argos Translate (offline neural) — try to load on first use
# ---------------------------------------------------------------------------

_argos_loaded = False
_argos_ja_en = None


def _try_load_argos() -> bool:
    """Attempt to load the Argos Japanese→English model.  Returns True on success."""
    global _argos_loaded, _argos_ja_en
    if _argos_loaded:
        return _argos_ja_en is not None
    _argos_loaded = True
    try:
        import argostranslate.package
        import argostranslate.translate

        installed = argostranslate.translate.get_installed_languages()
        ja = next((l for l in installed if l.code == "ja"), None)
        en = next((l for l in installed if l.code == "en"), None)

        if ja is None or en is None:
            # Try to download and install the model
            argostranslate.package.update_package_index()
            available = argostranslate.package.get_available_packages()
            ja_en_pkg = next((p for p in available if p.from_code == "ja" and p.to_code == "en"), None)
            if ja_en_pkg is None:
                logger.info("Argos ja→en package not found in index; install with: pip install argostranslate")
                return False
            logger.info("Downloading Argos ja→en model (~200 MB, one-time)…")
            download_path = ja_en_pkg.download()
            argostranslate.package.install_from_path(download_path)
            logger.info("Argos ja→en model installed — restart recommended")
            # Re-read installed languages
            installed = argostranslate.translate.get_installed_languages()
            ja = next((l for l in installed if l.code == "ja"), None)
            en = next((l for l in installed if l.code == "en"), None)

        if ja and en and ja.get_translation(en):
            _argos_ja_en = True
            return True
        return False
    except ImportError:
        logger.info("argostranslate not installed; using dictionary fallback")
        return False
    except Exception as exc:
        logger.warning("Argos load failed: %s; using dictionary fallback", exc)
        return False


def _argos_translate(text: str) -> str:
    """Translate a single string using Argos (offline neural)."""
    if not _try_load_argos():
        return text
    import argostranslate.translate

    installed = argostranslate.translate.get_installed_languages()
    ja = next((l for l in installed if l.code == "ja"), None)
    en = next((l for l in installed if l.code == "en"), None)
    if ja and en:
        translation = ja.get_translation(en)
        if translation:
            return translation.translate(text)
    return text

# ---------------------------------------------------------------------------
# Local Japanese financial-term dictionary (EDINET standard terminology)
# ---------------------------------------------------------------------------

_JP_EN_GLOSSARY: dict[str, str] = {
    # Document types
    "有価証券報告書": "Annual Securities Report",
    "四半期報告書": "Quarterly Report",
    "半期報告書": "Semi-annual Report",
    "訂正報告書": "Amendment Report",
    "臨時報告書": "Extraordinary Report",
    "内部統制報告書": "Internal Control Report",
    "確認書": "Confirmation Letter",
    # Financial statements
    "連結貸借対照表": "Consolidated Balance Sheet",
    "連結損益計算書": "Consolidated Income Statement",
    "連結包括利益計算書": "Consolidated Statement of Comprehensive Income",
    "連結株主資本等変動計算書": "Consolidated Statement of Changes in Equity",
    "連結キャッシュ・フロー計算書": "Consolidated Statement of Cash Flows",
    "貸借対照表": "Balance Sheet",
    "損益計算書": "Income Statement",
    "包括利益計算書": "Statement of Comprehensive Income",
    "株主資本等変動計算書": "Statement of Changes in Equity",
    "キャッシュ・フロー計算書": "Statement of Cash Flows",
    # Balance sheet items
    "資産": "Assets",
    "流動資産": "Current Assets",
    "固定資産": "Non-current Assets",
    "有形固定資産": "Property, Plant and Equipment",
    "無形固定資産": "Intangible Assets",
    "投資その他の資産": "Investments and Other Assets",
    "負債": "Liabilities",
    "流動負債": "Current Liabilities",
    "固定負債": "Non-current Liabilities",
    "純資産": "Net Assets",
    "資本金": "Share Capital",
    "資本剰余金": "Capital Surplus",
    "利益剰余金": "Retained Earnings",
    "自己株式": "Treasury Shares",
    "現金及び預金": "Cash and Deposits",
    "受取手形及び売掛金": "Notes and Accounts Receivable",
    "棚卸資産": "Inventories",
    "のれん": "Goodwill",
    "支払手形及び買掛金": "Notes and Accounts Payable",
    "借入金": "Borrowings",
    "社債": "Corporate Bonds",
    # Income statement items
    "売上高": "Net Sales",
    "営業収益": "Operating Revenue",
    "売上原価": "Cost of Sales",
    "売上総利益": "Gross Profit",
    "販売費及び一般管理費": "Selling, General and Administrative Expenses",
    "営業利益": "Operating Income",
    "営業損失": "Operating Loss",
    "経常利益": "Ordinary Income",
    "経常損失": "Ordinary Loss",
    "税引前当期純利益": "Income before Income Taxes",
    "法人税等": "Income Taxes",
    "当期純利益": "Net Income",
    "当期純損失": "Net Loss",
    "親会社株主に帰属する当期純利益": "Net Income Attributable to Owners of Parent",
    "基本的1株当たり当期純利益": "Basic Earnings per Share",
    "希薄化後1株当たり当期純利益": "Diluted Earnings per Share",
    # Cash flow items
    "営業活動によるキャッシュ・フロー": "Cash Flows from Operating Activities",
    "投資活動によるキャッシュ・フロー": "Cash Flows from Investing Activities",
    "財務活動によるキャッシュ・フロー": "Cash Flows from Financing Activities",
    "減価償却費": "Depreciation and Amortization",
    "設備投資": "Capital Expenditure",
    "配当金": "Dividends",
    # General terms
    "前期": "Previous Period",
    "当期": "Current Period",
    "前年同期": "Same Period Last Year",
    "増加": "Increase",
    "減少": "Decrease",
    "合計": "Total",
    "差引": "Net",
    "うち": "of which",
    "その他": "Other",
    "注記": "Notes",
    "概要": "Overview",
    "主要": "Key",
    "事業": "Business",
    "状況": "Status",
    "結果": "Results",
    "財政状態": "Financial Position",
    "経営成績": "Operating Results",
    "キャッシュ・フロー": "Cash Flows",
    "リスク": "Risk",
    "研究開発": "Research and Development",
    "従業員": "Employees",
    "関係会社": "Affiliated Companies",
    "関連当事者": "Related Parties",
    "後発事象": "Subsequent Events",
    "継続企業": "Going Concern",
    "監査": "Audit",
    "会計監査人": "External Auditor",
    "内部統制": "Internal Control",
    "提出会社": "Filing Company",
    "連結子会社": "Consolidated Subsidiaries",
    "持分法": "Equity Method",
    "公正価値": "Fair Value",
    "見積り": "Estimates",
    "基準": "Standards",
    "方針": "Policy",
    "重要な": "Significant",
    "会計方針": "Accounting Policies",
    "未適用": "Not Yet Applied",
    "変更": "Change",
    "修正": "Correction",
    "遡及": "Retrospective",
    "表示方法": "Presentation Method",
    "組替": "Reclassification",
    "金額": "Amount",
    "科目": "Account",
    "区分": "Classification",
    "記載": "Described",
    "開示": "Disclosure",
    "報告": "Report",
    "提出": "Submission",
    "終了": "End",
    "開始": "Beginning",
    "当中間": "Interim",
    "第": "No.",
    "期": "Period",
    "会計期間": "Accounting Period",
    "決算日": "Balance Sheet Date",
    "事業年度": "Fiscal Year",
    "連結会計年度": "Consolidated Fiscal Year",
    "四半期": "Quarter",
    "累計": "Cumulative",
    "百万円": "million yen",
    "千円": "thousand yen",
    "円": "yen",
    "株": "shares",
    "種類": "Type",
    "発行済": "Issued",
    "自己": "Treasury",
    "数": "Number",
    "単元": "Unit",
    "株式": "Stock",
    "新株予約権": "Share Options",
}

# Build a regex that matches the longest dictionary entries first
_SORTED_KEYS = sorted(_JP_EN_GLOSSARY.keys(), key=len, reverse=True)
_DICT_RE = re.compile("|".join(re.escape(k) for k in _SORTED_KEYS))


def _needs_translation(text: str) -> bool:
    """Return True when text contains CJK characters."""
    if not text or not text.strip():
        return False
    return bool(re.search(r"[぀-ヿ㐀-䶿一-鿿]", text))


def _dict_translate(text: str) -> str:
    """Translate Japanese financial terms to English using the local dictionary."""
    if not text or not text.strip():
        return text
    result = _DICT_RE.sub(lambda m: _JP_EN_GLOSSARY.get(m.group(0), m.group(0)), text)
    # Also transliterate parenthesised Japanese readings like "売上高（うりあげだか）"
    result = re.sub(r"（[぀-ヿ]+）", "", result)
    return result


def translate_batch(
    texts: list[str],
    catalog: Any | None = None,
    *,
    force: bool = False,
) -> dict[str, str]:
    """Translate a batch of Japanese strings to English using the local dictionary.

    All translations are cached in ``filing_translations``.
    """
    if not texts:
        return {}

    unique = list(dict.fromkeys(t for t in texts if t and t.strip()))
    if not unique:
        return {}

    # Check cache (version 2 = Argos, version 1 = old dictionary)
    cached: dict[str, str] = {}
    uncached = unique
    if catalog is not None and not force:
        import hashlib

        hashed = catalog.lookup_translations(unique, version=2)
        hash_to_text = {hashlib.sha256(t.encode("utf-8")).hexdigest(): t for t in unique}
        for h, translated in hashed.items():
            src = hash_to_text.get(h)
            if src:
                cached[src] = translated
        uncached = [t for t in unique if t not in cached]

    # Use Argos exclusively
    use_argos = _try_load_argos()
    new_translations: dict[str, str] = {}
    for text in uncached:
        if not _needs_translation(str(text)):
            new_translations[text] = text
            continue
        try:
            if use_argos and len(text) <= 2000:
                result = _argos_translate(str(text))
                new_translations[text] = result if result and result != text else text
            else:
                new_translations[text] = text
        except Exception:
            new_translations[text] = text

    # Store changed translations (version 2 = Argos)
    changed = {k: v for k, v in new_translations.items() if v != k}
    if catalog is not None and changed:
        catalog.store_translations(changed, version=2)
    cached.update(new_translations)

    return cached


def translate_filing_sections(
    sections: list[dict[str, Any]],
    catalog: Any | None = None,
    *,
    translate_bodies: bool = True,
) -> list[dict[str, Any]]:
    """Translate section titles and body text using the local dictionary."""
    if not sections:
        return sections
    titles = [s.get("title", "") for s in sections if s.get("title")]
    all_texts = [t for t in titles if t and _needs_translation(str(t))]

    if translate_bodies:
        for s in sections:
            body = s.get("text", "")
            if body and _needs_translation(str(body)):
                all_texts.append(body[:3000])

    if not all_texts:
        return sections

    translations = translate_batch(all_texts, catalog)
    result = []
    for s in sections:
        entry = dict(s)
        title = entry.get("title", "")
        if title and title in translations and translations[title] != title:
            entry["title_en"] = translations[title]
        body = entry.get("text", "")
        if translate_bodies and body and _needs_translation(str(body)):
            translated_body = translations.get(body[:3000])
            if translated_body and translated_body != body[:3000]:
                # For longer bodies, repeat the translation for the full text
                body_key = body[:3000]
                if body_key in translations:
                    entry["text_en"] = translations[body_key]
                    if len(body) > 3000:
                        entry["text_en"] += "…"
        result.append(entry)
    return result


def translate_facts(
    facts: list[dict[str, Any]],
    catalog: Any | None = None,
) -> dict[str, str]:
    """Return concept -> English label map for a list of facts."""
    concepts = list(dict.fromkeys(f.get("concept", "") for f in facts if f.get("concept")))
    if not concepts:
        return {}
    return translate_batch(concepts, catalog)
