"""Tests for the Yahoo company-description cache."""

import sqlite3

from src.security_analysis import company_descriptions


def test_profile_page_description_reads_description_paragraph():
    page = """
    <html><body>
      <section data-testid="description">
        <h3>Description</h3>
      <p>  A company that repairs and reinforces infrastructure.\n
          It operates in Japan. </p>
      </section>
    </body></html>
    """

    assert company_descriptions._extract_profile_page_description(page) == (
        "A company that repairs and reinforces infrastructure. It operates in Japan."
    )


def test_profile_page_description_reads_embedded_summary_payload():
    page = """
    <html><body>
      <script type="application/json">
        {"assetProfile": {"longBusinessSummary": "Description from the profile payload."}}
      </script>
    </body></html>
    """

    assert company_descriptions._extract_profile_page_description(page) == (
        "Description from the profile payload."
    )


def test_profile_page_fetch_uses_requested_yahoo_url(monkeypatch):
    requested: list[str] = []

    class FakeResponse:
        text = "<h3>Description</h3><p>Profile page description.</p>"

        def raise_for_status(self):
            return None

    class FakeSession:
        cookies = {}

        def get(self, url, **kwargs):
            requested.append(url)
            return FakeResponse()

    monkeypatch.setattr(company_descriptions, "_new_yahoo_session", FakeSession)

    assert company_descriptions._fetch_yahoo_profile_page("1414.T") == (
        "Profile page description."
    )
    assert requested == ["https://finance.yahoo.com/quote/1414.T/profile/"]


def test_profile_page_fetch_uses_crumb_asset_profile_payload(monkeypatch):
    requested: list[tuple[str, dict]] = []

    class FakeResponse:
        def __init__(self, text="", payload=None):
            self.text = text
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        cookies = {"A1": "profile-cookie"}

        def get(self, url, **kwargs):
            requested.append((url, kwargs))
            if url == "https://finance.yahoo.com/quote/7575.T/profile/":
                return FakeResponse("<html><body>No server-rendered description</body></html>")
            if url == company_descriptions._YAHOO_CRUMB_URL:
                return FakeResponse("crumb-7575")
            return FakeResponse(
                payload={
                    "quoteSummary": {
                        "result": [
                            {"assetProfile": {"longBusinessSummary": "Japan Lifeline description."}}
                        ]
                    }
                }
            )

    monkeypatch.setattr(company_descriptions, "_new_yahoo_session", FakeSession)

    assert company_descriptions._fetch_yahoo_profile_page("7575.T") == (
        "Japan Lifeline description."
    )
    assert requested[0][0] == "https://finance.yahoo.com/quote/7575.T/profile/"
    assert requested[1][0] == company_descriptions._YAHOO_CRUMB_URL
    assert requested[2][1]["params"]["crumb"] == "crumb-7575"


def test_fetch_yahoo_description_prefers_profile_page(monkeypatch):
    monkeypatch.setattr(
        company_descriptions,
        "_fetch_yahoo_profile_page",
        lambda symbol: f"Profile description for {symbol}.",
    )
    monkeypatch.setattr(company_descriptions, "_fetch_yahoo_http", lambda symbol: "")
    monkeypatch.setattr(company_descriptions, "_fetch_yahoo_yfinance", lambda symbol: "")

    assert company_descriptions.fetch_yahoo_description("14140") == (
        "Profile description for 1414.T."
    )


def test_stale_empty_description_cache_is_retried(tmp_path, monkeypatch):
    database = tmp_path / "standardized.db"
    company_descriptions._ensure_table(str(database))
    with sqlite3.connect(database) as conn:
        conn.execute(
            """INSERT INTO Company_Descriptions
               (cache_key, company_code, ticker, description, provider, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                "company:E00001",
                "E00001",
                "75750",
                "",
                "Yahoo Finance",
                "2020-01-01T00:00:00+00:00",
            ),
        )

    calls: list[str] = []

    def fake_fetch(ticker: str | None) -> str:
        calls.append(str(ticker))
        return "A cached business description."

    monkeypatch.setattr(company_descriptions, "fetch_yahoo_description", fake_fetch)

    assert company_descriptions.get_or_fetch_description(
        str(database), "E00001", "75750"
    ) == "A cached business description."
    assert company_descriptions.get_or_fetch_description(
        str(database), "E00001", "75750"
    ) == "A cached business description."
    assert calls == ["75750"]
