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
    "会計基準": "Accounting Standards",
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
    "改組": "Reorganization",
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
_RESIDUAL_CHAR_LIMIT = 10
_DECOMPOSITION_RANKS = ("\n。！？!?；;", "、,,")
_TRANSLATABLE_ATTRIBUTES = ("alt", "aria-label", "placeholder", "title", "value")
_BLOCK_TAGS = (
    "p", "div", "td", "th", "h1", "h2", "h3", "h4", "h5", "h6",
    "li", "dt", "dd", "caption", "figcaption", "blockquote", "pre",
)
_PROSE_TAGS = ("p", "li", "blockquote", "dt", "dd", "pre", "figcaption")


def _needs_translation(text: str) -> bool:
    """Return True when text contains Japanese kana or CJK ideographs."""
    return bool(text and text.strip() and _CJK_RE.search(text))


def _residual_count(text: str) -> int:
    """Return how many Japanese/CJK characters remain inside ``text``."""
    return len(_CJK_RE.findall(text or ""))


def _is_acceptable_translation(translated: str) -> bool:
    """Return True when a result carries only a tolerable amount of Japanese.

    Fewer than ``_RESIDUAL_CHAR_LIMIT`` residual Japanese characters are
    tolerated so a tokenizer leak of a couple of kanji, or a proper noun that
    the model leaves untouched, does not fail an entire filing.
    """
    return bool(
        translated
        and translated.strip()
        and _residual_count(translated) < _RESIDUAL_CHAR_LIMIT
    )


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


def _decompose(text: str, boundaries: str) -> list[str]:
    """Split text after each boundary character, keeping every character."""
    pieces: list[str] = []
    start = 0
    for index, character in enumerate(text):
        if character in boundaries:
            pieces.append(text[start : index + 1])
            start = index + 1
    if start < len(text):
        pieces.append(text[start:])
    return pieces


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

    # Pre-apply the glossary so compounds whose kanji are missing from the
    # neural model's output vocabulary (e.g. 改組) survive its tokenization.
    candidate = _dict_translate(_call_argos(glossary_translation))
    if _needs_translation(candidate):
        candidate = _repair_residual_japanese(candidate)
    if _is_complete_translation(text, candidate):
        return candidate

    if len(text) <= 80:
        contextual = _translate_short_text_with_context(text)
        if contextual:
            return contextual

    # Escalating decomposition: retry successively finer units (sentences,
    # then clauses) so each region gets an independent tokenization context.
    # The neural model's tokenizer leaks individual kanji only for some
    # segmentations of the same text, and smaller units translate cleanly.
    if len(text) > 1 and depth < len(_DECOMPOSITION_RANKS):
        for rank in range(depth, len(_DECOMPOSITION_RANKS)):
            pieces = _decompose(text, _DECOMPOSITION_RANKS[rank])
            if len(pieces) <= 1:
                continue
            translated = "".join(
                _translate_chunk(piece, depth=rank + 1) for piece in pieces
            )
            if _is_complete_translation(text, translated):
                return translated

    # Never fail over residual Japanese. Return the best available rendering —
    # the model's output when it produced one, otherwise the source itself so
    # short untranslatable tokens (proper nouns) survive verbatim.
    return candidate or text


def _translate_complete_text(text: str) -> str:
    chunks = _split_translation_chunks(text, _MAX_TRANSLATION_CHARS)
    return "".join(_translate_chunk(chunk) for chunk in chunks)


def translate_batch(
    texts: list[str],
    catalog: Any | None = None,
    *,
    force: bool = False,
) -> dict[str, str]:
    """Return complete English translations for every supplied string.

    Cached output is accepted when it is fully English or carries only a
    handful of residual Japanese characters (fewer than ``_RESIDUAL_CHAR_LIMIT``).
    The function raises :class:`TranslationError` rather than returning source or
    substantially untranslated text as a successful English result.
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
            if src and (
                _is_complete_translation(src, translated)
                or _is_acceptable_translation(translated)
            ):
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
        if _needs_translation(source)
        and (
            _is_complete_translation(source, translated)
            or _is_acceptable_translation(translated)
        )
    }
    if catalog is not None and complete:
        catalog.store_translations(complete, version=TRANSLATOR_VERSION)
    cached.update(new_translations)

    return cached


def _inside_block(node: Any) -> bool:
    """Return True when ``node`` has a block-level ancestor."""
    parent = getattr(node, "parent", None)
    while parent is not None:
        if getattr(parent, "name", None) in _BLOCK_TAGS:
            return True
        parent = getattr(parent, "parent", None)
    return False


def _block_text(block: Any) -> str:
    """Return a block element's normalized, whitespace-collapsed text."""
    return re.sub(r"\s+", " ", block.get_text(" ", strip=True)).strip()


def _ends_sentence(text: str) -> bool:
    """Return True when ``text`` ends with sentence-ending punctuation."""
    stripped = text.rstrip(" \t\r\n　）)】」』〕〉》")
    return bool(stripped) and stripped[-1] in "。！？!?"


def _adjacent_prose_siblings(a: Any, b: Any) -> bool:
    """Return True when ``b`` immediately follows ``a`` among their siblings."""
    if a.parent is not b.parent:
        return False
    sibling = a.next_sibling
    while sibling is not None and sibling is not b:
        if getattr(sibling, "name", None) is not None:
            return False
        sibling = sibling.next_sibling
    return sibling is b


def _prose_runs(soup: Any) -> list[list[Any]]:
    """Group consecutive sibling prose blocks into sentence-complete runs.

    iXBRL splits a sentence across several sibling paragraphs (for example
    ``...となっており`` followed by ``ます。``); merge such blocks so they are
    translated as one unit while keeping complete paragraphs separate.
    """
    prose_blocks = [
        block
        for block in soup.find_all(_PROSE_TAGS)
        if block.find(_BLOCK_TAGS) is None
    ]
    runs: list[list[Any]] = []
    for block in prose_blocks:
        if runs:
            last = runs[-1][-1]
            if (
                _adjacent_prose_siblings(last, block)
                and not _ends_sentence(_block_text(last))
            ):
                runs[-1].append(block)
                continue
        runs.append([block])
    return runs


def translate_html_fragment(
    html: str,
    catalog: Any | None = None,
    *,
    force: bool = False,
) -> tuple[str, int]:
    """Translate visible prose and user-facing attributes in HTML.

    Prose is translated per sentence-complete block run: a sentence that iXBRL
    breaks across inline tags, ``<br>``, or sibling paragraphs is joined and
    translated as one unit rather than as isolated fragments such as ``ます。``.
    Short untranslatable tokens (proper nouns) are kept verbatim instead of
    failing the whole document.
    """
    from bs4 import BeautifulSoup, Comment, Declaration, Doctype, ProcessingInstruction

    soup = BeautifulSoup(html, "html.parser")
    ignored_string_types = (Comment, Declaration, Doctype, ProcessingInstruction)

    def text_nodes(element: Any) -> list[Any]:
        return [
            node
            for node in element.find_all(string=True)
            if not isinstance(node, ignored_string_types)
        ]

    attribute_targets: list[tuple[Any, str, str]] = []
    for tag in soup.find_all(True):
        for attribute in _TRANSLATABLE_ATTRIBUTES:
            value = tag.get(attribute)
            if isinstance(value, str) and _needs_translation(value):
                attribute_targets.append((tag, attribute, value))

    # 1. Prose runs: merge consecutive sibling prose blocks whose text does not
    #    end a sentence, so fragments like ``ます。`` join their sentence.
    run_targets: list[tuple[list[Any], str, str, str]] = []
    for run in _prose_runs(soup):
        nodes = [node for block in run for node in text_nodes(block)]
        if not nodes:
            continue
        first = str(nodes[0])
        last = str(nodes[-1])
        leading = first[: len(first) - len(first.lstrip())]
        trailing = last[len(last.rstrip()):]
        source = re.sub(r"\s+", " ", " ".join(_block_text(b) for b in run)).strip()
        if source and _needs_translation(source):
            run_targets.append((run, source, leading, trailing))

    # 2. Non-prose leaf blocks (headings, table cells, ...) translate as units.
    block_targets: list[tuple[Any, str, str, str]] = []
    for block in soup.find_all(_BLOCK_TAGS):
        if block.name in _PROSE_TAGS:
            continue
        if block.find(_BLOCK_TAGS) is not None:
            continue
        nodes = text_nodes(block)
        if not nodes:
            continue
        first = str(nodes[0])
        last = str(nodes[-1])
        leading = first[: len(first) - len(first.lstrip())]
        trailing = last[len(last.rstrip()):]
        source = _block_text(block)
        if source and _needs_translation(source):
            block_targets.append((block, source, leading, trailing))

    # 3. Stray text nodes outside any block element.
    text_targets: list[tuple[Any, str, str]] = []
    for node in soup.find_all(string=True):
        if isinstance(node, ignored_string_types):
            continue
        if _inside_block(node):
            continue
        raw = str(node)
        source = raw.strip()
        if source and _needs_translation(source):
            text_targets.append((node, raw, source))

    sources = list(
        dict.fromkeys(
            [source for _run, source, _leading, _trailing in run_targets]
            + [source for _block, source, _leading, _trailing in block_targets]
            + [source for _tag, _attribute, source in attribute_targets]
            + [source for _node, _raw, source in text_targets]
        )
    )
    translations = translate_batch(sources, catalog, force=force)

    for run, source, leading, trailing in run_targets:
        first_block = run[0]
        first_block.clear()
        first_block.append(leading + translations[source] + trailing)
        for extra in run[1:]:
            extra.decompose()
    for block, source, leading, trailing in block_targets:
        block.clear()
        block.append(leading + translations[source] + trailing)
    for tag, attribute, source in attribute_targets:
        tag[attribute] = translations[source]
    for node, raw, source in text_targets:
        leading_length = len(raw) - len(raw.lstrip())
        trailing_start = len(raw.rstrip())
        node.replace_with(
            raw[:leading_length] + translations[source] + raw[trailing_start:]
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
