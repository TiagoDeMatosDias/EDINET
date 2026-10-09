import sqlite3
from datetime import date
from unittest.mock import MagicMock

from src.orchestrator.get_documents import get_documents


def _document_list(path, submitted_at):
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "CREATE TABLE DocumentList (docID TEXT, submitDateTime TEXT)"
        )
        connection.executemany(
            "INSERT INTO DocumentList(docID, submitDateTime) VALUES (?, ?)",
            [(str(index), value) for index, value in enumerate(submitted_at)],
        )
        connection.commit()
    finally:
        connection.close()


def test_missing_dates_use_latest_document_date_and_today(monkeypatch, tmp_path):
    db_path = tmp_path / "base.db"
    _document_list(db_path, ["2025-02-01 09:00", "2025-04-15 10:30"])
    client = MagicMock()
    monkeypatch.setattr(get_documents, "get_market_db", lambda: str(db_path))
    monkeypatch.setattr(get_documents, "Edinet", lambda **_: client)

    get_documents.run_get_documents({"API_KEY": "key", "get_documents_config": {}})

    client.get_All_documents_withMetadata.assert_called_once_with(
        "2025-04-15", date.today().isoformat()
    )


def test_explicit_dates_are_preserved(monkeypatch, tmp_path):
    db_path = tmp_path / "base.db"
    _document_list(db_path, ["2025-04-15 10:30"])
    client = MagicMock()
    monkeypatch.setattr(get_documents, "get_market_db", lambda: str(db_path))
    monkeypatch.setattr(get_documents, "Edinet", lambda **_: client)

    get_documents.run_get_documents(
        {
            "API_KEY": "key",
            "get_documents_config": {
                "startDate": "2024-01-01",
                "endDate": "2024-12-31",
            },
        }
    )

    client.get_All_documents_withMetadata.assert_called_once_with(
        "2024-01-01", "2024-12-31"
    )
