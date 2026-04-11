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