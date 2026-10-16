import sqlite3

import pytest

from src.orchestrator.common.table_swap import drop_stale_next, next_table_name, swap_in


@pytest.fixture()
def db_path(tmp_path):
    path = tmp_path / "swap.db"
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(
        """
        CREATE TABLE T (docID TEXT PRIMARY KEY, a REAL);
        CREATE INDEX idx_t_a ON T(a);
        INSERT INTO T VALUES ('old1', 1), ('old2', 2);
        CREATE TABLE U (docID TEXT PRIMARY KEY, b REAL);
        INSERT INTO U VALUES ('old1', 10);
        """
    )
    conn.commit()
    conn.close()
    return str(path)


def _build_next(conn, table="T", rows=(("new1", 5.0), ("new2", 6.0), ("new3", 7.0))):
    conn.execute(f'CREATE TABLE "{next_table_name(table)}" (docID TEXT PRIMARY KEY, a REAL)')
    conn.executemany(f'INSERT INTO "{next_table_name(table)}" VALUES (?, ?)', rows)
    conn.commit()


def test_next_table_name():
    assert next_table_name("T") == "T__next"


def test_swap_is_invisible_to_a_reader_until_it_commits(db_path):
    writer = sqlite3.connect(db_path)
    reader = sqlite3.connect(db_path)
    drop_stale_next(writer, "T")
    _build_next(writer)
    assert reader.execute("SELECT COUNT(*) FROM T").fetchone()[0] == 2
    swap_in(writer, ["T"])
    assert reader.execute("SELECT COUNT(*) FROM T").fetchone()[0] == 3
    assert reader.execute("SELECT name FROM sqlite_master WHERE name = 'T__next'").fetchone() is None
    writer.close()
    reader.close()


def test_open_read_snapshot_survives_the_swap(db_path):
    writer = sqlite3.connect(db_path)
    reader = sqlite3.connect(db_path)
    _build_next(writer)
    reader.execute("BEGIN")
    assert reader.execute("SELECT COUNT(*) FROM T").fetchone()[0] == 2
    swap_in(writer, ["T"])
    assert reader.execute("SELECT COUNT(*) FROM T").fetchone()[0] == 2
    reader.commit()
    assert reader.execute("SELECT COUNT(*) FROM T").fetchone()[0] == 3


def test_group_swap_and_index_names_are_kept(db_path):
    conn = sqlite3.connect(db_path)
    _build_next(conn, "T")
    conn.execute('CREATE TABLE "U__next" (docID TEXT PRIMARY KEY, b REAL)')
    conn.commit()
    swap_in(conn, ["T", "U"])
    assert conn.execute("SELECT COUNT(*) FROM U").fetchone()[0] == 0
    indexes = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='T'")}
    assert "idx_t_a" in indexes
    assert not any("__next" in name for name in indexes)
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert not any(name.endswith("__next") for name in tables)


def test_extra_indexes_are_added_to_the_live_ones(db_path):
    conn = sqlite3.connect(db_path)
    _build_next(conn)
    swap_in(
        conn,
        ["T"],
        indexes={"T": ['CREATE INDEX IF NOT EXISTS idx_t_a ON "T"(a)', 'CREATE INDEX idx_extra ON "T"(docID, a)']},
    )
    names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='T'")}
    assert {"idx_t_a", "idx_extra"} <= names


def test_live_index_the_new_table_cannot_hold_is_skipped(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute('CREATE INDEX idx_t_doc ON T(docID)')
    conn.execute('CREATE TABLE "T__next" (docID TEXT PRIMARY KEY)')
    swap_in(conn, ["T"])
    names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='T'")}
    assert "idx_t_a" not in names and "idx_t_doc" in names


def test_explicit_indexes_can_replace_the_live_ones(db_path):
    conn = sqlite3.connect(db_path)
    _build_next(conn)
    swap_in(conn, ["T"], indexes={"T": ['CREATE INDEX idx_other ON "T"(docID, a)']}, keep_live_indexes=False)
    names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='T'")}
    assert "idx_other" in names and "idx_t_a" not in names


def test_view_and_trigger_referencing_the_table_survive(db_path):
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE VIEW V AS SELECT docID, a FROM T;
        CREATE TABLE Log (msg TEXT);
        CREATE TRIGGER trg AFTER INSERT ON T BEGIN INSERT INTO Log VALUES (NEW.docID); END;
        """
    )
    _build_next(conn)
    swap_in(conn, ["T"])
    assert conn.execute("SELECT COUNT(*) FROM V").fetchone()[0] == 3
    conn.execute("INSERT INTO T VALUES ('x', 1)")
    assert conn.execute("SELECT msg FROM Log").fetchall() == [("x",)]
    assert conn.execute("PRAGMA legacy_alter_table").fetchone()[0] == 0


def test_failed_swap_leaves_everything_untouched(db_path):
    conn = sqlite3.connect(db_path)
    _build_next(conn, "T")
    with pytest.raises(sqlite3.Error):
        swap_in(conn, ["T"], indexes={"T": ['CREATE INDEX idx_bad ON "T"(no_such_column)']})
    assert conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'idx_t_a'").fetchone()
    assert conn.execute("SELECT COUNT(*) FROM T").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM T__next").fetchone()[0] == 3
    assert not conn.in_transaction


def test_missing_next_table_is_rejected_before_touching_anything(db_path):
    conn = sqlite3.connect(db_path)
    _build_next(conn, "T")
    with pytest.raises(ValueError):
        swap_in(conn, ["T", "U"])
    assert conn.execute("SELECT COUNT(*) FROM T").fetchone()[0] == 2


def test_swap_creates_a_table_that_did_not_exist(db_path):
    conn = sqlite3.connect(db_path)
    _build_next(conn, "Fresh")
    swap_in(conn, ["Fresh"])
    assert conn.execute("SELECT COUNT(*) FROM Fresh").fetchone()[0] == 3


def test_drop_stale_next(db_path):
    conn = sqlite3.connect(db_path)
    _build_next(conn)
    conn.execute('CREATE INDEX idx_stale ON "T__next"(a)')
    drop_stale_next(conn, "T")
    assert conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '%\\_\\_next' ESCAPE '\\' OR name='idx_stale'").fetchone()[0] == 0
    drop_stale_next(conn, "T")
