"""Unit tests for the TDnet split-disclosure event capture."""

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

from src.utilities.tdnet import (
    TdnetFetchError,
    ensure_tdnet_tables,
    extract_disclosure_id,
    parse_disclosure_rows,
    record_disclosures,
    run_tdnet_split_check,
    search_split_disclosures,
)

_FIXTURE_HTML = """
<html><body><table id="maintable">
<tr class="odd">
  <td class="time" nowrap>2026/08/21 16:00</td>
  <td class="code" nowrap>47620</td>
  <td class="companyname">ＸＮＥＴ</td>
  <td class="title" align=left><a target="_blank" href="/inbs/140120260821524566.pdf">「株式分割」、株式分割に伴う「定款の一部変更」に関するお知らせ</a></td>
  <td class="xbrl" nowrap align=center><a href="/inbs/091220260821524566.zip">XBRL</a></td>
  <td class="exchange" nowrap>プライム</td>
</tr>
<tr class="even">
  <td class="time" nowrap>2026/08/20 15:30</td>
  <td class="code" nowrap>249A0</td>
  <td class="companyname">テスト社</td>
  <td class="title" align=left>株式分割及び定款の一部変更に関するお知らせ</td>
  <td class="xbrl" nowrap align=center>―</td>
  <td class="exchange" nowrap>グロース</td>
</tr>
<tr><td colspan="6">footer noise without enough cells</td></tr>
</table></body></html>
"""


class TestTdnet(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "std.db")

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_extract_disclosure_id(self):
        self.assertEqual(
            extract_disclosure_id("/inbs/140120260821524566.pdf"),
            "140120260821524566",
        )
        self.assertIsNone(extract_disclosure_id(None))
        self.assertIsNone(extract_disclosure_id("/inbs/.pdf"))

    def test_parse_disclosure_rows(self):
        rows = parse_disclosure_rows(_FIXTURE_HTML)
        self.assertEqual(len(rows), 2)
        first = rows[0]
        self.assertEqual(first["announced_at"], "2026-08-21 16:00")
        self.assertEqual(first["ticker"], "47620")
        self.assertEqual(first["company_name"], "ＸＮＥＴ")
        self.assertIn("株式分割", first["title"])
        self.assertEqual(
            first["pdf_url"],
            "https://www.release.tdnet.info/inbs/140120260821524566.pdf",
        )
        self.assertEqual(
            first["xbrl_url"],
            "https://www.release.tdnet.info/inbs/091220260821524566.zip",
        )
        # Second row has no XBRL attachment.
        self.assertIsNone(rows[1]["xbrl_url"])


    def test_search_dedupes_across_keywords(self):
        response = Mock(status_code=200, text=_FIXTURE_HTML)
        session = Mock()
        session.post.return_value = response
        rows = search_split_disclosures(
            "20260816", "20260822", keywords=("株式分割", "株式併合"), session=session
        )
        # Two keyword searches run; one fixture row has no PDF link and the
        # duplicate disclosure collapses by disclosure_id.
        self.assertEqual(len(rows), 1)
        self.assertEqual({r["disclosure_id"] for r in rows},
                         {"140120260821524566"})
        self.assertEqual(session.post.call_count, 2)

    def test_search_wraps_http_failure(self):
        response = Mock(status_code=503)
        session = Mock()
        session.post.return_value = response
        with patch("src.utilities.tdnet.time.sleep"):
            with self.assertRaises(TdnetFetchError):
                search_split_disclosures("20260816", "20260822", session=session)

    def test_record_disclosures_is_idempotent(self):
        conn = sqlite3.connect(self.db_path)
        try:
            ensure_tdnet_tables(conn=conn)
            rows = [
                {
                    "disclosure_id": "1401", "announced_at": "2026-08-21 16:00",
                    "ticker": "47620", "company_name": "ＸＮＥＴ",
                    "title": "t1", "keyword": "株式分割",
                    "pdf_url": "/inbs/1401.pdf", "xbrl_url": None,
                    "exchange": "プライム",
                },
            ]
            first = record_disclosures(conn, rows)
            second = record_disclosures(conn, rows)
            self.assertEqual(first["events_new"], 1)
            self.assertEqual(second["events_new"], 0)
            count = conn.execute(
                "SELECT COUNT(*) FROM Tdnet_Disclosures"
            ).fetchone()[0]
            self.assertEqual(count, 1)
        finally:
            conn.close()

    def test_run_tdnet_split_check_end_to_end(self):
        response = Mock(status_code=200, text=_FIXTURE_HTML)
        session = Mock()
        session.post.return_value = response
        with patch("src.utilities.tdnet.requests.Session", return_value=session), \
             patch("src.utilities.tdnet.date") as fake_date:
            fake_date.today.return_value = __import__("datetime").date(2026, 8, 22)
            fake_date.side_effect = lambda *a, **k: __import__("datetime").date(*a, **k)
            results = run_tdnet_split_check(self.db_path, lookback_days=7)

        self.assertEqual(results["events_seen"], 1)
        self.assertEqual(results["events_new"], 1)
        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT ticker, announced_at FROM Tdnet_Disclosures "
                "ORDER BY announced_at DESC LIMIT 1"
            ).fetchone()
            self.assertEqual(row, ("47620", "2026-08-21 16:00"))
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
