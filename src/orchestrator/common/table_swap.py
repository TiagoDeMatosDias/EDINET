"""Build-then-swap for overwrite rebuilds of tables the web app reads.

An overwrite rebuild used to ``DROP TABLE`` the live table and refill it over
minutes, so a concurrent reader (the web server, in WAL mode) saw a missing or
half-filled table. Instead a step builds the new table under
``next_table_name(table)`` (``<table>__next``) and, once everything is built,
calls ``swap_in`` which replaces the live tables in ONE short transaction.
A reader therefore sees either the complete old table or the complete new one.

Usage inside a step::

    drop_stale_next(conn, "T")                  # leftovers of a crashed run
    conn.execute('CREATE TABLE "T__next" (...)')   # build and fill T__next
    ...
    swap_in(conn, ["T", "U"], indexes={"T": ['CREATE INDEX idx_t ON "T"(a)']})

* ``drop_stale_next`` drops a leftover ``<table>__next`` (and with it its
  indexes). Call it at the start of every overwrite rebuild.
* ``swap_in(conn, tables, *, indexes=None)`` expects every ``<t>__next`` to
  exist, then in one ``BEGIN IMMEDIATE`` transaction, per table:
  ``DROP TABLE IF EXISTS t; ALTER TABLE t__next RENAME TO t``, followed by
  re-creating the indexes and triggers that belonged to the live table. By
  default these are copied from the live table's ``sqlite_master`` entries, so
  index names stay stable (a copied index the new table cannot hold, because a
  column is gone, is skipped). ``indexes[t]`` are extra ``CREATE INDEX``
  statements written against the final name ``t``, created after the copied
  ones (use ``IF NOT EXISTS`` for one the live table may already have); with
  ``keep_live_indexes=False`` they replace the copied ones, for a rebuild that
  changes the columns. Indexes already created on ``t__next`` keep their own
  names and move with the table. Index creation happens inside the swap
  transaction, so prefer few/cheap indexes there.
* Views that reference ``t`` stay valid: the swap runs with
  ``PRAGMA legacy_alter_table=ON`` so the rename neither fails on the view
  left dangling by the drop nor rewrites it.
* Any error rolls the whole group back; the live tables are untouched.
  Within a step, code that reads a table being rebuilt must read the
  ``__next`` copy until the swap.

The connection must not hold an open transaction other than its own pending
writes, which ``swap_in`` commits first.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence

NEXT_SUFFIX = "__next"


def _quote(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def next_table_name(table: str) -> str:
    """The name a rebuild of *table* is built under until it is swapped in."""
    return f"{table}{NEXT_SUFFIX}"


def drop_stale_next(conn: sqlite3.Connection, table: str) -> None:
    """Drop a ``<table>__next`` left behind by a crashed or cancelled rebuild."""
    conn.execute(f"DROP TABLE IF EXISTS {_quote(next_table_name(table))}")
    conn.commit()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone() is not None


def _live_dependents(conn: sqlite3.Connection, table: str) -> tuple[list[str], list[str]]:
    """``(index SQL, trigger SQL)`` of the explicit indexes and triggers on *table*."""
    indexes = [
        row[0]
        for row in conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL ORDER BY rowid",
            (table,),
        )
    ]
    triggers = [
        row[0]
        for row in conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ? AND sql IS NOT NULL ORDER BY rowid",
            (table,),
        )
    ]
    return indexes, triggers


def swap_in(
    conn: sqlite3.Connection,
    tables: Sequence[str],
    *,
    indexes: Mapping[str, Sequence[str]] | None = None,
    keep_live_indexes: bool = True,
) -> None:
    """Replace each live table in *tables* by its ``__next`` copy in one transaction."""
    tables = list(tables)
    if not tables:
        return
    for table in tables:
        if not _table_exists(conn, next_table_name(table)):
            raise ValueError(f"Cannot swap in {table!r}: {next_table_name(table)!r} does not exist.")

    conn.commit()
    legacy = conn.execute("PRAGMA legacy_alter_table").fetchone()[0]
    conn.execute("PRAGMA legacy_alter_table = ON")
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            for table in tables:
                live_indexes, live_triggers = _live_dependents(conn, table)
                conn.execute(f"DROP TABLE IF EXISTS {_quote(table)}")
                conn.execute(f"ALTER TABLE {_quote(next_table_name(table))} RENAME TO {_quote(table)}")
                if keep_live_indexes:
                    for statement in live_indexes:
                        try:
                            conn.execute(statement)
                        except sqlite3.OperationalError:
                            pass
                for statement in (indexes or {}).get(table, ()):
                    conn.execute(statement)
                for statement in live_triggers:
                    conn.execute(statement)
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
    finally:
        conn.execute(f"PRAGMA legacy_alter_table = {1 if legacy else 0}")
