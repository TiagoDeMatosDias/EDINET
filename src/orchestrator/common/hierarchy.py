"""Query utilities for the Statement_Hierarchy table.

Provides functions to traverse the financial statement tree, look up columns,
and validate hierarchy integrity.
"""

from __future__ import annotations

import sqlite3
from typing import Any


def get_statement_tree(
    conn: sqlite3.Connection,
    statement_family: str,
) -> list[dict[str, Any]]:
    """Get the full hierarchy tree for a statement type, ordered for display.

    Returns rows ordered by level then display_order, suitable for rendering
    an indented financial statement.
    """
    rows = conn.execute(
        """
        SELECT concept_qname, parent_concept_qname, column_concept_qname,
               level, primary_label_en, display_order, is_column
        FROM Statement_Hierarchy
        WHERE statement_family = ?
        ORDER BY level, display_order, primary_label_en
        """,
        (statement_family,),
    ).fetchall()
    return [dict(row) for row in rows]


def get_columns(
    conn: sqlite3.Connection,
    statement_family: str,
) -> list[dict[str, Any]]:
    """Get all wide-table columns for a statement type, ordered.

    Returns only concepts where is_column=1 (anchor concepts).
    """
    rows = conn.execute(
        """
        SELECT concept_qname, parent_concept_qname, level,
               primary_label_en, display_order
        FROM Statement_Hierarchy
        WHERE statement_family = ? AND is_column = 1
        ORDER BY level, display_order, primary_label_en
        """,
        (statement_family,),
    ).fetchall()
    return [dict(row) for row in rows]


def get_children(
    conn: sqlite3.Connection,
    concept_qname: str,
) -> list[dict[str, Any]]:
    """Get direct children of a concept in the hierarchy."""
    rows = conn.execute(
        """
        SELECT concept_qname, column_concept_qname, level,
               primary_label_en, display_order, is_column
        FROM Statement_Hierarchy
        WHERE parent_concept_qname = ?
        ORDER BY display_order, primary_label_en
        """,
        (concept_qname,),
    ).fetchall()
    return [dict(row) for row in rows]


def get_ancestors(
    conn: sqlite3.Connection,
    concept_qname: str,
) -> list[dict[str, Any]]:
    """Get the full ancestor chain from root to the given concept.

    Returns list ordered from root → ... → parent → concept.
    """
    ancestors: list[dict[str, Any]] = []
    current = concept_qname
    visited: set[str] = set()
    while current and current not in visited:
        visited.add(current)
        row = conn.execute(
            """
            SELECT concept_qname, parent_concept_qname, level,
                   primary_label_en, is_column
            FROM Statement_Hierarchy
            WHERE concept_qname = ?
            """,
            (current,),
        ).fetchone()
        if row is None:
            break
        ancestors.append(dict(row))
        parent = row["parent_concept_qname"]
        if not parent:
            break
        current = parent
    ancestors.reverse()
    return ancestors


def get_descendants(
    conn: sqlite3.Connection,
    concept_qname: str,
) -> list[dict[str, Any]]:
    """Get all descendants of a concept using recursive traversal."""
    result: list[dict[str, Any]] = []
    stack = [concept_qname]
    visited: set[str] = set()
    while stack:
        current = stack.pop()
        if current in visited:
            continue
        visited.add(current)
        children = get_children(conn, current)
        for child in children:
            result.append(child)
            stack.append(child["concept_qname"])
    return result


def validate_no_orphan_parents(conn: sqlite3.Connection) -> list[str]:
    """Return concept_qnames whose parent doesn't exist in Statement_Hierarchy."""
    rows = conn.execute(
        """
        SELECT sh.concept_qname, sh.parent_concept_qname
        FROM Statement_Hierarchy sh
        WHERE sh.parent_concept_qname IS NOT NULL
          AND sh.parent_concept_qname NOT IN (
              SELECT concept_qname FROM Statement_Hierarchy
          )
        """
    ).fetchall()
    return [f"{row[0]} -> missing parent {row[1]}" for row in rows]


def get_column_label_map(
    conn: sqlite3.Connection,
    statement_family: str,
) -> dict[str, str]:
    """Return a mapping of concept_qname → column label for a statement family.

    Includes disambiguated labels (e.g., 'Buildings - Accumulated depreciation').
    Handles the same disambiguation logic as the generator.
    """
    columns = get_columns(conn, statement_family)
    label_counts: dict[str, int] = {}
    for col in columns:
        label = str(col.get("primary_label_en") or "")
        label_counts[label.lower()] = label_counts.get(label.lower(), 0) + 1

    result: dict[str, str] = {}
    for col in columns:
        qname = str(col.get("concept_qname") or "")
        label = str(col.get("primary_label_en") or "")
        if label_counts.get(label.lower(), 0) > 1:
            parent_qname = col.get("parent_concept_qname")
            if parent_qname:
                parent_row = conn.execute(
                    "SELECT primary_label_en FROM Statement_Hierarchy WHERE concept_qname = ?",
                    (parent_qname,),
                ).fetchone()
                if parent_row:
                    parent_label = str(parent_row[0] or "")
                    if parent_label:
                        label = f"{parent_label} - {label}"
        result[qname] = label
    return result
