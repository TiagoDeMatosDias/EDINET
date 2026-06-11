"""Text processing utilities for security analysis.

Pure text functions: HTML cleaning, description splitting, n-gram extraction,
language detection, summarisation (English TF-IDF and Japanese n-gram based).

No database dependencies — only stdlib, pandas, and optional sklearn.
"""

from __future__ import annotations

import html
import re
from collections import Counter
from typing import Any

import pandas as pd

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
except ImportError:  # pragma: no cover - optional dependency
    TfidfVectorizer = None


def _safe_str(value: Any) -> str:
    """Return a normalised string for display and matching."""
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _safe_float(value: Any) -> float | None:
    """Return a float or ``None`` for missing/invalid input."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def clean_text_block(value: Any) -> str:
    """Return plain text from long-form text blocks such as filing descriptions."""
    text = _safe_str(value)
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"(?is)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)</(?:p|div|li|tr|td|th|section|article|h[1-6])\s*>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = text.replace("\xa0", " ").replace("\u3000", " ")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _split_description_units(text: Any) -> list[str]:
    """Split a long business description into sentence-like units."""
    cleaned = clean_text_block(text)
    if not cleaned:
        return []

    units: list[str] = []
    seen: set[str] = set()
    for block in cleaned.split("\n\n"):
        for line in block.splitlines():
            candidate = line.strip(" \t-・●■◆")
            if not candidate:
                continue
            fragments = [
                fragment.strip()
                for fragment in re.split(r"(?<=[。！？!?])", candidate)
                if fragment.strip()
            ]
            for fragment in fragments or [candidate]:
                normalized = re.sub(r"\s+", "", fragment)
                if not normalized or normalized in seen:
                    continue
                units.append(fragment)
                seen.add(normalized)

    longer_units = [unit for unit in units if len(re.sub(r"\s+", "", unit)) >= 12]
    return longer_units or units


def _description_ngrams(text: str) -> set[str]:
    """Return compact character n-grams suitable for Japanese/English ranking."""
    compact = re.sub(r"\s+", "", clean_text_block(text))
    compact = re.sub(r"[、。・「」『』（）()【】［］\[\]〈〉《》…:：;；,，]", "", compact)
    if not compact:
        return set()

    grams: set[str] = set()
    for size in (2, 3):
        if len(compact) < size:
            continue
        grams.update(compact[index:index + size] for index in range(len(compact) - size + 1))
    if not grams:
        grams.add(compact)
    return grams


def _looks_like_english_text(text: Any) -> bool:
    """Return ``True`` when text appears to be primarily English."""
    cleaned = clean_text_block(text)
    if not cleaned:
        return False
    latin_chars = len(re.findall(r"[A-Za-z]", cleaned))
    cjk_chars = len(re.findall(r"[\u3040-\u30ff\u3400-\u9fff]", cleaned))
    return latin_chars >= 20 and latin_chars >= cjk_chars


def _split_english_sentences(text: Any) -> list[str]:
    """Split translated English text into sentence units for summarisation."""
    cleaned = clean_text_block(text)
    if not cleaned:
        return []

    sentences: list[str] = []
    seen: set[str] = set()
    for block in [paragraph.strip() for paragraph in cleaned.split("\n\n") if paragraph.strip()]:
        fragments = [
            fragment.strip()
            for fragment in re.split(r"(?<=[.!?])\s+", block)
            if fragment.strip()
        ]
        for fragment in fragments or [block]:
            normalized = re.sub(r"\s+", " ", fragment).strip()
            if len(normalized) < 20 or normalized.lower() in seen:
                continue
            sentences.append(normalized)
            seen.add(normalized.lower())
    return sentences


def _english_sentence_tokens(sentence: str) -> set[str]:
    """Return a token set for English sentence redundancy checks."""
    return set(re.findall(r"[A-Za-z][A-Za-z'-]{1,}", sentence.lower()))


def summarize_english_text(text: Any, paragraph_count: int = 2) -> str:
    """Build a deterministic short summary from translated English text."""
    cleaned = clean_text_block(text)
    if not cleaned:
        return ""

    sentences = _split_english_sentences(cleaned)
    if len(sentences) < 2 or len(cleaned) <= 180 or TfidfVectorizer is None:
        return cleaned

    try:
        matrix = TfidfVectorizer(stop_words="english").fit_transform(sentences)
    except ValueError:
        return cleaned
    similarity = (matrix * matrix.T).toarray()
    token_sets = [_english_sentence_tokens(sentence) for sentence in sentences]

    base_scores: list[float] = []
    sentence_count = max(len(sentences), 1)
    for index, row in enumerate(similarity):
        centrality = float(row.sum())
        position_bonus = 0.15 * (1.0 - (index / sentence_count))
        base_scores.append(centrality + position_bonus)

    target_sentences = 4 if len(sentences) >= 6 else min(len(sentences), 3)
    selected_indexes: list[int] = []
    while len(selected_indexes) < target_sentences:
        best_index: int | None = None
        best_score: float | None = None
        for index, token_set in enumerate(token_sets):
            if index in selected_indexes:
                continue
            redundancy = 0.0
            if selected_indexes:
                redundancy = max(
                    len(token_set & token_sets[selected]) / max(len(token_set | token_sets[selected]), 1)
                    for selected in selected_indexes
                )
            candidate_score = base_scores[index] - (redundancy * 0.55)
            if best_index is None or candidate_score > best_score:
                best_index = index
                best_score = candidate_score
        if best_index is None:
            break
        selected_indexes.append(best_index)

    if not selected_indexes:
        return cleaned

    selected_indexes.sort()
    selected_sentences = [sentences[index] for index in selected_indexes]
    paragraph_total = min(paragraph_count, len(selected_sentences))
    if paragraph_total <= 1:
        summary = " ".join(selected_sentences).strip()
        return summary if summary and len(summary) < len(cleaned) * 0.85 else cleaned

    base_size, remainder = divmod(len(selected_sentences), paragraph_total)
    cursor = 0
    paragraphs: list[str] = []
    for paragraph_index in range(paragraph_total):
        chunk_size = base_size + (1 if paragraph_index < remainder else 0)
        chunk = selected_sentences[cursor:cursor + chunk_size]
        cursor += chunk_size
        if chunk:
            paragraphs.append(" ".join(chunk))

    summary = "\n\n".join(paragraphs).strip()
    return summary if summary and len(summary) < len(cleaned) * 0.85 else cleaned


def summarize_preferred_description(text: Any) -> str:
    """Summarize English descriptions with the English summarizer, else Japanese."""
    cleaned = clean_text_block(text)
    if not cleaned:
        return ""
    if _looks_like_english_text(cleaned):
        return summarize_english_text(cleaned)
    return summarize_business_description(cleaned)


def _join_summary_units(units: list[str]) -> str:
    """Join summary units without forcing spaces into Japanese text."""
    combined = ""
    for unit in units:
        clean_unit = unit.strip()
        if not clean_unit:
            continue
        if not combined:
            combined = clean_unit
            continue
        if re.match(r"[A-Za-z0-9]", clean_unit):
            combined += " "
        combined += clean_unit
    return combined.strip()


def summarize_business_description(text: Any, paragraph_count: int = 2) -> str:
    """Build a deterministic short summary for a long business description."""
    cleaned = clean_text_block(text)
    if not cleaned:
        return ""

    units = _split_description_units(cleaned)
    if len(units) < 2 or len(cleaned) <= 140:
        return cleaned

    ngrams_by_unit = [_description_ngrams(unit) for unit in units]
    corpus_counts: Counter[str] = Counter()
    for ngrams in ngrams_by_unit:
        corpus_counts.update(ngrams)

    base_scores: list[float] = []
    unit_count = max(len(units), 1)
    for index, ngrams in enumerate(ngrams_by_unit):
        if not ngrams:
            base_scores.append(0.0)
            continue
        centrality = sum(corpus_counts[ngram] for ngram in ngrams) / len(ngrams)
        position_bonus = 0.15 * (1.0 - (index / unit_count))
        base_scores.append(centrality + position_bonus)

    target_units = 4 if len(units) >= 6 else min(len(units), 3)
    selected_indexes: list[int] = []
    while len(selected_indexes) < target_units:
        best_index: int | None = None
        best_score: float | None = None
        for index, ngrams in enumerate(ngrams_by_unit):
            if index in selected_indexes:
                continue
            redundancy = 0.0
            if selected_indexes:
                redundancy = max(
                    len(ngrams & ngrams_by_unit[selected]) / max(len(ngrams | ngrams_by_unit[selected]), 1)
                    for selected in selected_indexes
                )
            candidate_score = base_scores[index] - (redundancy * 0.55)
            if best_index is None or candidate_score > best_score:
                best_index = index
                best_score = candidate_score
        if best_index is None:
            break
        selected_indexes.append(best_index)

    if not selected_indexes:
        return cleaned

    selected_indexes.sort()
    selected_units = [units[index] for index in selected_indexes]
    paragraph_total = min(paragraph_count, len(selected_units))
    if paragraph_total <= 1:
        summary = _join_summary_units(selected_units)
        return summary if len(summary) < len(cleaned) * 0.85 else cleaned

    base_size, remainder = divmod(len(selected_units), paragraph_total)
    cursor = 0
    paragraphs: list[str] = []
    for paragraph_index in range(paragraph_total):
        chunk_size = base_size + (1 if paragraph_index < remainder else 0)
        chunk = selected_units[cursor:cursor + chunk_size]
        cursor += chunk_size
        if chunk:
            paragraphs.append(_join_summary_units(chunk))

    summary = "\n\n".join(paragraphs).strip()
    return summary if summary and len(summary) < len(cleaned) * 0.85 else cleaned
