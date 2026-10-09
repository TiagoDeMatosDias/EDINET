"""A filing whose facts raise the same quality issue twice still parses."""

from __future__ import annotations

from src.filings.catalog import FilingCatalog
from src.filings.quality import assess_facts


def test_a_repeated_fact_id_records_one_issue_instead_of_failing(tmp_path):
    catalog = FilingCatalog(tmp_path / "filings.db")
    catalog.upsert_filing({
        "doc_id": "S100AJZW", "edinet_code": "E00949", "submitter_name": "Santen", "period_start": "2016-04-01",
        "period_end": "2017-03-31", "submitted_at": "2017-06-23 15:09", "form_code": "030000", "doc_type_code": "120",
        "xbrl_flag": "1", "csv_flag": "0", "archive_path": "", "archive_content": b"", "archive_sha256": "", "archive_size": 0,
        "status": "parsing", "parse_error": None, "created_at": "2017-06-23", "updated_at": "2017-06-23",
    })
    # Two facts without a context, reported under one fact id.
    facts = [{"fact_id": "F1", "numeric_value": 1.0, "unit_id": "JPY"}, {"fact_id": "F1", "numeric_value": 2.0, "unit_id": "JPY"}]
    catalog.replace_quality_issues("S100AJZW", assess_facts("S100AJZW", facts))
    assert [row["code"] for row in catalog.list_quality_issues("S100AJZW")] == ["missing_context"]
