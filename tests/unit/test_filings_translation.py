"""Regression coverage for complete filing translation."""

from __future__ import annotations

import sqlite3

import pytest
from fastapi import HTTPException

from src.filings import api as filings_api
from src.filings import translate as translation
from src.filings.catalog import FilingCatalog


def _translation_count(catalog: FilingCatalog) -> int:
    connection = sqlite3.connect(catalog.path)
    try:
        return int(connection.execute("SELECT COUNT(*) FROM filing_translations").fetchone()[0])
    finally:
        connection.close()


def test_short_financial_labels_use_glossary_without_loading_argos(tmp_path, monkeypatch):
    catalog = FilingCatalog(tmp_path / "Filings.db")

    def unexpected_model_load(**_kwargs):
        pytest.fail("dictionary-complete labels must not initialize Argos")

    monkeypatch.setattr(translation, "_try_load_argos", unexpected_model_load)

    assert translation.translate_batch(["円", "資産"], catalog) == {
        "円": "yen",
        "資産": "Assets",
    }
    assert _translation_count(catalog) == 2


def test_japanese_middle_dot_is_punctuation_not_untranslated_text():
    assert translation._needs_translation("・") is False
    assert translation._needs_translation("・Complete English sentence.") is False


def test_isolated_name_uses_context_when_argos_returns_empty(monkeypatch):
    monkeypatch.setattr(translation, "_try_load_argos", lambda **_kwargs: True)

    def contextual_translation(text: str) -> str:
        return "Item: Seiichiro Suyama" if text.startswith("項目：") else ""

    monkeypatch.setattr(translation, "_argos_translate", contextual_translation)

    result = translation.translate_batch(["須山　誠一郎"])

    assert result["須山　誠一郎"] == "Seiichiro Suyama"


def test_unavailable_model_raises_instead_of_returning_japanese(tmp_path, monkeypatch):
    catalog = FilingCatalog(tmp_path / "Filings.db")
    monkeypatch.setattr(translation, "_try_load_argos", lambda **_kwargs: False)

    with pytest.raises(translation.TranslationUnavailableError):
        translation.translate_batch(["当社独自の文章です。"], catalog)

    assert _translation_count(catalog) == 0


def test_long_text_is_fully_chunked_and_cached(tmp_path, monkeypatch):
    catalog = FilingCatalog(tmp_path / "Filings.db")
    calls: list[str] = []
    source = "当社は国内で事業を展開しています。" * 100
    monkeypatch.setattr(translation, "_try_load_argos", lambda **_kwargs: True)

    def complete_chunk(text: str) -> str:
        calls.append(text)
        return "The company operates domestically."

    monkeypatch.setattr(translation, "_argos_translate", complete_chunk)

    translated = translation.translate_batch([source], catalog)[source]

    assert len(calls) >= 2
    assert "当社" not in translated
    assert not translation._needs_translation(translated)
    assert catalog.lookup_translations([source], version=translation.TRANSLATOR_VERSION)


def test_residual_japanese_is_retranslated_before_caching(tmp_path, monkeypatch):
    catalog = FilingCatalog(tmp_path / "Filings.db")
    calls: list[str] = []
    source = "当社は継続的に成長しています。"
    monkeypatch.setattr(translation, "_try_load_argos", lambda **_kwargs: True)

    def partial_then_repair(text: str) -> str:
        calls.append(text)
        if text == "日本語":
            return "Japanese text"
        return "The company is growing 日本語."

    monkeypatch.setattr(translation, "_argos_translate", partial_then_repair)

    translated = translation.translate_batch([source], catalog)[source]

    assert "日本語" in calls
    assert translated == "The company is growing Japanese text."
    assert not translation._needs_translation(translated)
    assert _translation_count(catalog) == 1


def test_incomplete_output_is_rejected_and_never_cached(tmp_path, monkeypatch):
    catalog = FilingCatalog(tmp_path / "Filings.db")
    monkeypatch.setattr(translation, "_try_load_argos", lambda **_kwargs: True)
    monkeypatch.setattr(
        translation,
        "_argos_translate",
        lambda _text: "English with 未翻訳 content",
    )

    with pytest.raises(translation.IncompleteTranslationError):
        translation.translate_batch(["独自表現です。"], catalog)

    assert _translation_count(catalog) == 0


def test_legacy_partial_cache_is_ignored_and_replaced(tmp_path, monkeypatch):
    catalog = FilingCatalog(tmp_path / "Filings.db")
    source = "当社独自の文章です。"
    catalog.store_translations({source: "Partial 日本語"}, version=2)
    monkeypatch.setattr(translation, "_try_load_argos", lambda **_kwargs: True)
    monkeypatch.setattr(translation, "_argos_translate", lambda _text: "Complete English")

    assert translation.translate_batch([source], catalog)[source] == "Complete English"
    assert catalog.lookup_translations([source], version=2) == {}
    assert catalog.lookup_translations([source], version=3)


def test_sections_translate_the_full_body_without_ellipsis(monkeypatch):
    body = "日本語の本文です。" * 500
    captured: list[str] = []

    def complete_batch(texts, _catalog=None, *, force=False):
        captured.extend(texts)
        assert force is True
        return {text: f"English:{len(text)}" for text in texts}

    monkeypatch.setattr(translation, "translate_batch", complete_batch)

    result = translation.translate_filing_sections(
        [{"section_id": "s1", "title": "事業", "text": body}],
        force=True,
    )

    assert body in captured
    assert result[0]["title_en"] == "English:2"
    assert result[0]["text_en"] == f"English:{len(body)}"
    assert not result[0]["text_en"].endswith("…")


def test_html_translates_short_nodes_attributes_and_preserves_spacing(monkeypatch):
    html = (
        '<body><p>  円  </p><span title="資産" aria-label="売上高">'
        "当社は成長しています。</span></body>"
    )
    expected = {
        "円": "yen",
        "資産": "Assets",
        "売上高": "Net Sales",
        "当社は成長しています。": "The company is growing.",
    }

    def complete_batch(texts, _catalog=None, *, force=False):
        assert force is True
        assert set(texts) == set(expected)
        return {text: expected[text] for text in texts}

    monkeypatch.setattr(translation, "translate_batch", complete_batch)

    translated, item_count = translation.translate_html_fragment(html, force=True)

    assert item_count == 4
    assert ">  yen  <" in translated
    assert 'title="Assets"' in translated
    assert 'aria-label="Net Sales"' in translated
    assert "The company is growing." in translated
    assert not translation._needs_translation(translated)


class _HtmlCatalog:
    def get_filing(self, _doc_id):
        return {"doc_id": "S100TEST"}

    def list_artifacts(self, _doc_id):
        return [
            {
                "artifact_id": "artifact-1",
                "member_path": "PublicDoc/report.htm",
            }
        ]

    def get_artifact_content(self, _artifact_id):
        return {"content": b'<html lang="ja"><body><p>Japanese</p></body></html>'}


def test_html_endpoint_marks_only_validated_output_as_complete(monkeypatch):
    monkeypatch.setattr(filings_api, "catalog", _HtmlCatalog())
    monkeypatch.setattr(
        filings_api,
        "translate_html_fragment",
        lambda *_args, **_kwargs: ("<body><p>English</p></body>", 1),
    )

    result = filings_api.get_filing_html("S100TEST", "artifact-1", translate=True)

    assert '<html lang="en">' in result["html_en"]
    assert result["translation"] == {
        "status": "complete",
        "translated_items": 1,
        "translator_version": translation.TRANSLATOR_VERSION,
    }


def test_html_endpoint_reports_incomplete_translation_as_service_unavailable(monkeypatch):
    monkeypatch.setattr(filings_api, "catalog", _HtmlCatalog())

    def fail_translation(*_args, **_kwargs):
        raise translation.IncompleteTranslationError("residual Japanese remains")

    monkeypatch.setattr(filings_api, "translate_html_fragment", fail_translation)

    with pytest.raises(HTTPException) as error:
        filings_api.get_filing_html("S100TEST", "artifact-1", translate=True)

    assert error.value.status_code == 503
    assert "residual Japanese remains" in error.value.detail
