"""Tests for the business-description translation provider helpers."""

from __future__ import annotations

import json

import requests

from src.description_translation import (
    load_translation_providers,
    translate_text_with_providers,
)


class _FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class _FakeArgosTranslation:
    def __init__(self, translated_text):
        self.translated_text = translated_text
        self.calls = []

    def translate(self, text):
        self.calls.append(text)
        return self.translated_text


def test_translate_text_with_providers_falls_back_after_rate_limit(tmp_path, monkeypatch):
    config_path = tmp_path / "providers.json"
    config_path.write_text(
        json.dumps(
            {
                "providers": [
                    {
                        "name": "First Provider",
                        "type": "libretranslate",
                        "base_url": "https://first.example",
                    },
                    {
                        "name": "Second Provider",
                        "type": "mymemory",
                        "base_url": "https://second.example",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    providers, settings = load_translation_providers(str(config_path))

    def _fake_post(self, url, data=None, headers=None, timeout=None):
        assert url == "https://first.example/translate"
        return _FakeResponse(429, {"error": "Rate limit exceeded"})

    def _fake_get(self, url, params=None, timeout=None):
        assert url == "https://second.example/get"
        return _FakeResponse(
            200,
            {
                "responseStatus": 200,
                "responseData": {"translatedText": "Translated by fallback provider."},
            },
        )

    monkeypatch.setattr(requests.Session, "post", _fake_post)
    monkeypatch.setattr(requests.Session, "get", _fake_get)

    translated_text, provider_name = translate_text_with_providers(
        "当社は産業用センサーを製造しています。",
        providers,
        chunk_char_limit=settings["chunk_char_limit"],
    )

    assert translated_text == "Translated by fallback provider."
    assert provider_name == "Second Provider"


def test_translate_text_with_providers_retires_unavailable_provider_for_remainder_of_run(tmp_path, monkeypatch):
    config_path = tmp_path / "providers.json"
    config_path.write_text(
        json.dumps(
            {
                "providers": [
                    {
                        "name": "First Provider",
                        "type": "libretranslate",
                        "base_url": "https://first.example",
                    },
                    {
                        "name": "Second Provider",
                        "type": "mymemory",
                        "base_url": "https://second.example",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    providers, settings = load_translation_providers(str(config_path))
    calls = {"first": 0, "second": 0}

    def _fake_post(self, url, data=None, headers=None, timeout=None):
        calls["first"] += 1
        raise requests.RequestException("dns failure")

    def _fake_get(self, url, params=None, timeout=None):
        calls["second"] += 1
        return _FakeResponse(
            200,
            {
                "responseStatus": 200,
                "responseData": {"translatedText": f"{params['q']} EN"},
            },
        )

    monkeypatch.setattr(requests.Session, "post", _fake_post)
    monkeypatch.setattr(requests.Session, "get", _fake_get)

    first_text, first_provider = translate_text_with_providers(
        "最初の文章。",
        providers,
        chunk_char_limit=settings["chunk_char_limit"],
        retire_failed_providers=True,
    )
    second_text, second_provider = translate_text_with_providers(
        "次の文章。",
        providers,
        chunk_char_limit=settings["chunk_char_limit"],
        retire_failed_providers=True,
    )

    assert first_text == "最初の文章。 EN"
    assert second_text == "次の文章。 EN"
    assert first_provider == "Second Provider"
    assert second_provider == "Second Provider"
    assert calls == {"first": 1, "second": 2}
    assert [provider.name for provider in providers] == ["Second Provider"]


def test_translate_text_with_providers_falls_back_to_local_argos_after_remote_failure(tmp_path, monkeypatch):
    config_path = tmp_path / "providers.json"
    config_path.write_text(
        json.dumps(
            {
                "providers": [
                    {
                        "name": "First Provider",
                        "type": "libretranslate",
                        "base_url": "https://first.example",
                    },
                    {
                        "name": "Local Argos",
                        "type": "argos_translate",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    providers, settings = load_translation_providers(str(config_path))
    fake_argos = _FakeArgosTranslation("Translated by local fallback.")

    def _fake_post(self, url, data=None, headers=None, timeout=None):
        assert url == "https://first.example/translate"
        raise requests.RequestException("dns failure")

    monkeypatch.setattr(requests.Session, "post", _fake_post)
    monkeypatch.setattr("src.description_translation._load_argos_translation", lambda source_language, target_language: fake_argos)

    translated_text, provider_name = translate_text_with_providers(
        "当社は産業用センサーを製造しています。",
        providers,
        chunk_char_limit=settings["chunk_char_limit"],
        retire_failed_providers=True,
    )

    assert translated_text == "Translated by local fallback."
    assert provider_name == "Local Argos"
    assert fake_argos.calls == ["当社は産業用センサーを製造しています。"]
    assert [provider.name for provider in providers] == ["Local Argos"]


def test_translate_text_with_providers_logs_provider_activity(tmp_path, monkeypatch, caplog):
    config_path = tmp_path / "providers.json"
    config_path.write_text(
        json.dumps(
            {
                "providers": [
                    {
                        "name": "First Provider",
                        "type": "libretranslate",
                        "base_url": "https://first.example",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    providers, settings = load_translation_providers(str(config_path))

    def _fake_post(self, url, data=None, headers=None, timeout=None):
        assert url == "https://first.example/translate"
        return _FakeResponse(200, {"translatedText": "Translated text."})

    monkeypatch.setattr(requests.Session, "post", _fake_post)

    caplog.set_level("INFO", logger="src.description_translation")
    translated_text, provider_name = translate_text_with_providers(
        "当社は産業用センサーを製造しています。",
        providers,
        chunk_char_limit=settings["chunk_char_limit"],
        log_context="company 1/100 (docID=DOC1)",
        log_provider_activity=True,
        slow_request_warning_seconds=999.0,
    )

    assert translated_text == "Translated text."
    assert provider_name == "First Provider"
    assert any("Translation provider First Provider started for company 1/100 (docID=DOC1)" in message for message in caplog.messages)
    assert any("Translation provider First Provider completed for company 1/100 (docID=DOC1)" in message for message in caplog.messages)


def test_translate_text_with_providers_warns_on_slow_provider(tmp_path, monkeypatch, caplog):
    config_path = tmp_path / "providers.json"
    config_path.write_text(
        json.dumps(
            {
                "providers": [
                    {
                        "name": "First Provider",
                        "type": "libretranslate",
                        "base_url": "https://first.example",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    providers, settings = load_translation_providers(str(config_path))

    def _fake_post(self, url, data=None, headers=None, timeout=None):
        return _FakeResponse(200, {"translatedText": "Translated text."})

    perf_counter_values = iter([0.0, 12.5])

    monkeypatch.setattr(requests.Session, "post", _fake_post)
    monkeypatch.setattr("src.description_translation.time.perf_counter", lambda: next(perf_counter_values))

    caplog.set_level("WARNING", logger="src.description_translation")
    translated_text, provider_name = translate_text_with_providers(
        "当社は産業用センサーを製造しています。",
        providers,
        chunk_char_limit=settings["chunk_char_limit"],
        slow_request_warning_seconds=10.0,
    )

    assert translated_text == "Translated text."
    assert provider_name == "First Provider"
    assert any("Translation provider First Provider was slow" in message for message in caplog.messages)