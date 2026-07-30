"""Complete Japanese-to-English filing translation.

Translations run locally through Argos Translate after its Japanese-to-English
model is installed. The model may be downloaded on first use. A financial-term
glossary handles short EDINET labels that neural translation commonly leaves
empty. Only validated, complete translations are cached.
"""

from __future__ import annotations

import logging
import re
from threading import Lock
from time import monotonic
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Argos Translate (offline neural) — load or install on first use
# ---------------------------------------------------------------------------

TRANSLATOR_VERSION = 3

_ARGOS_RETRY_COOLDOWN_SECONDS = 30.0
_argos_ja_en: Any | None = None
_argos_last_error: str | None = None
_argos_retry_after = 0.0
_argos_init_lock = Lock()
_argos_call_lock = Lock()


class TranslationError(RuntimeError):
    """Base class for a translation that cannot be completed."""


class TranslationUnavailableError(TranslationError):
    """Raised when the local Japanese-to-English engine is unavailable."""


class IncompleteTranslationError(TranslationError):
    """Raised when translated output still contains Japanese text."""


def _unavailable_error() -> TranslationUnavailableError:
    detail = f" Last error: {_argos_last_error}" if _argos_last_error else ""
    return TranslationUnavailableError(
        "The Argos Japanese-to-English model is unavailable. Retry the translation "
        f"or use Retranslate to reinitialize it.{detail}"
    )


def _try_load_argos(*, force_retry: bool = False) -> bool:
    """Load the Japanese→English model, retrying transient initialization failures."""
    global _argos_ja_en, _argos_last_error, _argos_retry_after
    if _argos_ja_en is not None:
        return True
    if not force_retry and monotonic() < _argos_retry_after:
        return False

    with _argos_init_lock:
        if _argos_ja_en is not None:
            return True
        if not force_retry and monotonic() < _argos_retry_after:
            return False
        try:
            import argostranslate.package
            import argostranslate.translate

            def installed_translation() -> Any | None:
                installed = argostranslate.translate.get_installed_languages()
                ja = next((language for language in installed if language.code == "ja"), None)
                en = next((language for language in installed if language.code == "en"), None)
                return ja.get_translation(en) if ja is not None and en is not None else None

            translation = installed_translation()
            if translation is None:
                logger.info("Installing the Argos Japanese-to-English model on first use")
                argostranslate.package.update_package_index()
                package = next(
                    (
                        candidate
                        for candidate in argostranslate.package.get_available_packages()
                        if candidate.from_code == "ja" and candidate.to_code == "en"
                    ),
                    None,
                )
                if package is None:
                    raise RuntimeError("the Argos package index has no ja→en model")
                download_path = package.download()
                argostranslate.package.install_from_path(download_path)
                translation = installed_translation()
            if translation is None:
                raise RuntimeError("the installed ja→en model could not be loaded")
            _argos_ja_en = translation
            _argos_last_error = None
            _argos_retry_after = 0.0
            return True
        except ImportError as exc:
            _argos_last_error = f"argostranslate is not installed ({exc})"
        except Exception as exc:  # noqa: BLE001  # model/package failures are reported to callers
            _argos_last_error = str(exc)
        _argos_retry_after = monotonic() + _ARGOS_RETRY_COOLDOWN_SECONDS
        logger.warning("Argos Japanese-to-English initialization failed: %s", _argos_last_error)
        return False


def _argos_translate(text: str) -> str:
    """Translate a single string using Argos (offline neural)."""
    if not _try_load_argos():
        raise _unavailable_error()
    translation = _argos_ja_en
    if translation is None:  # pragma: no cover - guarded by _try_load_argos
        raise _unavailable_error()
    with _argos_call_lock:
        return str(translation.translate(text) or "")

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
    "内訳": "Breakdown",
    "注": "Note",
    "計": "Total",
    "有": "Yes",
    "無": "None",
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


_CJK_CHAR_CLASS = (
    r"\u3005\u3006\u303b\u3041-\u3096\u309d-\u309f\u30a1-\u30fa"
    r"\u30fc-\u30ff\u31f0-\u31ff\u3400-\u4dbf\u4e00-\u9fff"
    r"\uf900-\ufaff\uff66-\uff9d\U00020000-\U0002fa1f"
)
_CJK_RE = re.compile(f"[{_CJK_CHAR_CLASS}]")
_CJK_RUN_RE = re.compile(f"[{_CJK_CHAR_CLASS}]+")
_CHUNK_BOUNDARIES = "\n。！？!?；;、, "
_MAX_TRANSLATION_CHARS = 600
_MAX_REPAIR_DEPTH = 2
_TRANSLATABLE_ATTRIBUTES = ("alt", "aria-label", "placeholder", "title", "value")


def _needs_translation(text: str) -> bool:
    """Return True when text contains Japanese kana or CJK ideographs."""
    return bool(text and text.strip() and _CJK_RE.search(text))


def _dict_translate(text: str) -> str:
    """Translate Japanese financial terms to English using the local dictionary."""
    if not text or not text.strip():
        return text
    result = _DICT_RE.sub(lambda m: _JP_EN_GLOSSARY.get(m.group(0), m.group(0)), text)
    # Also transliterate parenthesised Japanese readings like "売上高（うりあげだか）"
    result = re.sub(r"（[぀-ヿ]+）", "", result)
    return result


def _is_complete_translation(source: str, translated: str) -> bool:
    if not _needs_translation(source):
        return translated == source
    return bool(translated and translated.strip() and not _needs_translation(translated))


def _split_translation_chunks(text: str, max_chars: int) -> list[str]:
    """Split text without dropping punctuation or whitespace."""
    if len(text) <= max_chars:
        return [text]
    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = min(start + max_chars, len(text))
        if end < len(text):
            candidate = text[start:end]
            boundary = max(candidate.rfind(character) for character in _CHUNK_BOUNDARIES)
            if boundary >= max_chars // 3:
                end = start + boundary + 1
        chunks.append(text[start:end])
        start = end
    return chunks


def _call_argos(text: str) -> str:
    """Call Argos twice for transient runtime failures or empty output."""
    last_error: Exception | None = None
    for _attempt in range(2):
        try:
            translated = _argos_translate(text)
            if translated and translated.strip():
                return translated
        except TranslationUnavailableError:
            raise
        except Exception as exc:  # noqa: BLE001  # retry transient native-model failures
            last_error = exc
    if last_error is not None:
        raise TranslationUnavailableError(
            f"The Argos translation engine failed while translating text: {last_error}"
        ) from last_error
    return ""


def _repair_residual_japanese(translated: str) -> str:
    """Retry residual Japanese runs that Argos left inside otherwise English output."""
    repaired = translated
    residuals = list(dict.fromkeys(_CJK_RUN_RE.findall(repaired)))
    for residual in residuals:
        replacement = _dict_translate(residual)
        if _needs_translation(replacement):
            candidate = _call_argos(residual)
            replacement = _dict_translate(candidate)
        if _is_complete_translation(residual, replacement):
            repaired = repaired.replace(residual, replacement)
    return repaired


def _translate_short_text_with_context(text: str) -> str:
    """Give isolated labels and names enough context for Argos to translate them."""
    contextual = _dict_translate(_call_argos(f"項目：{text}"))
    candidate = re.sub(
        r"^\s*(?:item|field|entry|name)\s*[:：-]\s*",
        "",
        contextual,
        flags=re.IGNORECASE,
    ).strip()
    return candidate if _is_complete_translation(text, candidate) else ""


def _translate_chunk(text: str, *, depth: int = 0) -> str:
    if not _needs_translation(text):
        return text

    glossary_translation = _dict_translate(text)
    if _is_complete_translation(text, glossary_translation):
        return glossary_translation

    candidate = _dict_translate(_call_argos(text))
    if _needs_translation(candidate):
        candidate = _repair_residual_japanese(candidate)
    if _is_complete_translation(text, candidate):
        return candidate


    if len(text) <= 80:
        contextual = _translate_short_text_with_context(text)
        if contextual:
            return contextual

    if depth < _MAX_REPAIR_DEPTH and len(text) > 1:
        retry_size = max(8, min(240, len(text) // 2))
        chunks = _split_translation_chunks(text, retry_size)
        if len(chunks) > 1:
            translated = "".join(
                _translate_chunk(chunk, depth=depth + 1) for chunk in chunks
            )
            if _is_complete_translation(text, translated):
                return translated

    residual_count = len(_CJK_RE.findall(candidate or text))
    raise IncompleteTranslationError(
        "The local translator left Japanese text in its output "
        f"after retries ({residual_count} residual characters)."
    )


def _translate_complete_text(text: str) -> str:
    chunks = _split_translation_chunks(text, _MAX_TRANSLATION_CHARS)
    translated = "".join(_translate_chunk(chunk) for chunk in chunks)
    if not _is_complete_translation(text, translated):
        raise IncompleteTranslationError(
            "The local translator did not produce a complete English translation."
        )
    return translated


def translate_batch(
    texts: list[str],
    catalog: Any | None = None,
    *,
    force: bool = False,
) -> dict[str, str]:
    """Return complete English translations for every supplied string.

    Cached output is accepted only when it contains no residual Japanese. The
    function raises :class:`TranslationError` rather than returning source or
    partially translated text as a successful English result.
    """
    if not texts:
        return {}

    unique = list(dict.fromkeys(t for t in texts if t and t.strip()))
    if not unique:
        return {}

    # Version 3 invalidates the former cache, which could contain partial output.
    cached: dict[str, str] = {}
    uncached = unique
    if catalog is not None and not force:
        import hashlib

        hashed = catalog.lookup_translations(unique, version=TRANSLATOR_VERSION)
        hash_to_text = {hashlib.sha256(t.encode("utf-8")).hexdigest(): t for t in unique}
        for h, translated in hashed.items():
            src = hash_to_text.get(h)
            if src and _is_complete_translation(src, translated):
                cached[src] = translated
            elif src:
                logger.warning("Ignoring incomplete cached translation for source hash %s", h)
        uncached = [t for t in unique if t not in cached]

    needs_model = any(
        _needs_translation(text) and _needs_translation(_dict_translate(text))
        for text in uncached
    )
    if needs_model and not _try_load_argos(force_retry=force):
        raise _unavailable_error()

    new_translations: dict[str, str] = {}
    for text in uncached:
        if not _needs_translation(text):
            new_translations[text] = text
            continue
        new_translations[text] = _translate_complete_text(text)

    complete = {
        source: translated
        for source, translated in new_translations.items()
        if _needs_translation(source) and _is_complete_translation(source, translated)
    }
    if catalog is not None and complete:
        catalog.store_translations(complete, version=TRANSLATOR_VERSION)
    cached.update(new_translations)

    return cached


def translate_html_fragment(
    html: str,
    catalog: Any | None = None,
    *,
    force: bool = False,
) -> tuple[str, int]:
    """Translate every visible text node and user-facing attribute in HTML."""
    from bs4 import BeautifulSoup, Comment, Declaration, Doctype, ProcessingInstruction

    soup = BeautifulSoup(html, "html.parser")
    text_targets: list[tuple[Any, str, str]] = []
    attribute_targets: list[tuple[Any, str, str]] = []

    ignored_string_types = (Comment, Declaration, Doctype, ProcessingInstruction)
    for node in soup.find_all(string=True):
        if isinstance(node, ignored_string_types):
            continue
        raw = str(node)
        source = raw.strip()
        if source and _needs_translation(source):
            text_targets.append((node, raw, source))

    for tag in soup.find_all(True):
        for attribute in _TRANSLATABLE_ATTRIBUTES:
            value = tag.get(attribute)
            if isinstance(value, str) and _needs_translation(value):
                attribute_targets.append((tag, attribute, value))

    sources = list(
        dict.fromkeys(
            [source for _node, _raw, source in text_targets]
            + [source for _tag, _attribute, source in attribute_targets]
        )
    )
    translations = translate_batch(sources, catalog, force=force)

    for node, raw, source in text_targets:
        leading_length = len(raw) - len(raw.lstrip())
        trailing_start = len(raw.rstrip())
        node.replace_with(
            raw[:leading_length] + translations[source] + raw[trailing_start:]
        )
    for tag, attribute, source in attribute_targets:
        tag[attribute] = translations[source]

    remaining = [
        str(node).strip()
        for node in soup.find_all(string=True)
        if not isinstance(node, ignored_string_types) and _needs_translation(str(node))
    ]
    remaining.extend(
        value
        for tag in soup.find_all(True)
        for attribute in _TRANSLATABLE_ATTRIBUTES
        if isinstance((value := tag.get(attribute)), str) and _needs_translation(value)
    )
    if remaining:
        raise IncompleteTranslationError(
            "The translated document still contains Japanese content "
            f"in {len(remaining)} visible locations."
        )
    return str(soup), len(sources)


def translate_filing_sections(
    sections: list[dict[str, Any]],
    catalog: Any | None = None,
    *,
    translate_bodies: bool = True,
    force: bool = False,
) -> list[dict[str, Any]]:
    """Translate complete section titles and bodies without truncation."""
    if not sections:
        return sections
    titles = [str(section.get("title", "")) for section in sections if section.get("title")]
    all_texts = [title for title in titles if _needs_translation(title)]

    if translate_bodies:
        for section in sections:
            body = str(section.get("text", ""))
            if body and _needs_translation(body):
                all_texts.append(body)

    translations = translate_batch(all_texts, catalog, force=force)
    result = []
    for section in sections:
        entry = dict(section)
        title = str(entry.get("title", ""))
        if title:
            entry["title_en"] = translations.get(title, title)
        body = str(entry.get("text", ""))
        if translate_bodies and body:
            entry["text_en"] = translations.get(body, body)
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
