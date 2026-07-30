"""Pipeline XBRL step contract tests."""

from src.filings.acquisition import EdinetAcquisitionError
from src.orchestrator.common.sqlite import connect_write
from src.orchestrator.download_xbrl import download_xbrl as download_step


def _create_base(path, *, with_xbrl_status=True):
    conn = connect_write(path)
    try:
        status_column = ', XbrlDownloaded TEXT NOT NULL DEFAULT "False"' if with_xbrl_status else ""
        conn.execute(
            "CREATE TABLE DocumentList ("
            "docID TEXT PRIMARY KEY, edinetCode TEXT, filerName TEXT, "
            "submitDateTime TEXT, periodStart TEXT, periodEnd TEXT, formCode TEXT, "
            "docTypeCode TEXT, xbrlFlag TEXT, legalStatus TEXT, Downloaded TEXT"
            f"{status_column})"
        )
        conn.executemany(
            "INSERT INTO DocumentList "
            "(docID, edinetCode, filerName, submitDateTime, periodStart, periodEnd, "
            "formCode, docTypeCode, xbrlFlag, legalStatus, Downloaded) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("A", "E00001", "A Co", "2025-03-03", "2024-04-01", "2025-03-31", "030", "120", "1", "1", "True"),
                ("B", "E00002", "B Co", "2025-03-02", "2024-04-01", "2025-03-31", "030", "130", "1", "2", "True"),
                ("C", "E00003", "C Co", "2025-03-01", "2024-04-01", "2025-03-31", "030", "120", "1", "1", "True"),
                ("D", "E00004", "D Co", "2025-02-28", "2024-04-01", "2025-03-31", "030", "140", "1", "1", "True"),
                ("E", "E00005", "E Co", "2025-02-27", "2024-04-01", "2025-03-31", "030", "120", "0", "1", "True"),
                ("F", "E00006", "F Co", "2025-02-26", "2024-04-01", "2025-03-31", "030", "120", "1", "0", "True"),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _status_rows(path):
    conn = connect_write(path)
    try:
        return {
            row["docID"]: row["XbrlDownloaded"]
            for row in conn.execute(
                "SELECT docID, XbrlDownloaded FROM DocumentList ORDER BY docID"
            )
        }
    finally:
        conn.close()


def test_xbrl_step_bounds_and_normalizes_document_ids():
    assert download_step._document_ids({"document_ids": " A1, B2 ,, C3 "}) == ["A1", "B2", "C3"]
    assert len(download_step._document_ids({"document_ids": [str(i) for i in range(200)]})) == 100


def test_all_mode_adds_status_column_and_selects_eligible_documents(tmp_path, monkeypatch):
    base_path = tmp_path / "Base.db"
    _create_base(base_path, with_xbrl_status=False)

    from src.orchestrator.common import db_config

    monkeypatch.setattr(db_config, "get_db1", lambda: str(base_path))

    assert download_step._all_ids({"doc_type_code": "120"}) == ["A", "C"]
    conn = connect_write(base_path)
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(DocumentList)").fetchall()}
        indexes = {row[1] for row in conn.execute("PRAGMA index_list(DocumentList)").fetchall()}
    finally:
        conn.close()
    assert "XbrlDownloaded" in columns
    assert "idx_document_list_docid" in indexes
    assert "idx_document_list_xbrl_queue" in indexes

    conn = connect_write(base_path)
    try:
        conn.execute("UPDATE DocumentList SET XbrlDownloaded = 'True' WHERE docID = 'C'")
        conn.commit()
    finally:
        conn.close()

    assert download_step._all_ids({"doc_type_code": ""}) == ["A", "B", "D"]


def test_all_mode_updates_base_statuses_and_retries_failures(tmp_path, monkeypatch):
    base_path = tmp_path / "Base.db"
    _create_base(base_path)

    from src.filings import acquisition, runtime
    from src.orchestrator.common import db_config

    monkeypatch.setattr(db_config, "get_db1", lambda: str(base_path))

    class FakeCatalog:
        def get_filing(self, document_id):
            if document_id == "A":
                return {"status": "parsed"}
            return None

    download_calls = []
    ingest_calls = []

    class FakeClient:
        def __init__(self, token):
            assert token == "token"

        def download_type1(self, document_id):
            download_calls.append(document_id)
            if document_id == "C":
                raise EdinetAcquisitionError("not available")
            if document_id == "D":
                raise RuntimeError("temporary failure")
            return b"PK-B"

        def ingest_type1(self, document_id, content, catalog, metadata):
            assert content == b"PK-B"
            ingest_calls.append((document_id, metadata))

    monkeypatch.setattr(acquisition, "EdinetDownloadClient", FakeClient)
    monkeypatch.setattr(runtime, "catalog", FakeCatalog())

    result = download_step.run_download_xbrl(
        {
            "download_xbrl_config": {
                "mode": "all",
                "doc_type_code": "",
                "provider_token": "token",
            }
        }
    )

    assert result == {
        "mode": "all",
        "candidates": 4,
        "downloaded": 1,
        "skipped": 1,
        "failed": 2,
    }
    assert set(download_calls) == {"B", "C", "D"}
    assert [document_id for document_id, _ in ingest_calls] == ["B"]
    assert ingest_calls[0][1]["edinet_code"] == "E00002"
    assert _status_rows(base_path) == {
        "A": "True",
        "B": "True",
        "C": "Checked_Unavailable",
        "D": "Checked_Error",
        "E": "False",
        "F": "False",
    }


def test_xbrl_download_workers_are_capped_at_five():
    import threading

    active = 0
    maximum = 0
    lock = threading.Lock()
    all_started = threading.Event()

    class FakeClient:
        def download_type1(self, document_id):
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
                if active == 5:
                    all_started.set()
            assert all_started.wait(timeout=2)
            with lock:
                active -= 1
            return document_id.encode()

    document_ids = [str(index) for index in range(10)]
    results = list(download_step._iter_download_results(FakeClient(), document_ids, {}))

    assert len(results) == len(document_ids)
    assert maximum == 5


def test_xbrl_statuses_are_committed_in_batches():
    class FakeConnection:
        def __init__(self):
            self.executemany_calls = []
            self.commit_count = 0

        def executemany(self, statement, parameters):
            self.executemany_calls.append((statement, list(parameters)))

        def commit(self):
            self.commit_count += 1

    connection = FakeConnection()
    pending = []
    for index in range(download_step._STATUS_BATCH_SIZE):
        download_step._queue_xbrl_status(connection, pending, str(index), "True")

    assert len(connection.executemany_calls) == 1
    assert connection.commit_count == 1
    assert pending == []

    download_step._queue_xbrl_status(connection, pending, "last", "Checked_Error")
    download_step._flush_xbrl_statuses(connection, pending)
    assert len(connection.executemany_calls) == 2
    assert connection.commit_count == 2
