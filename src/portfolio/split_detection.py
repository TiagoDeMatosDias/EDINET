"""Split detection via price heuristic with ShareMetrics cross-validation.

Scans Stock_Prices for sudden single-day price drops (or spikes for reverse
splits), then optionally verifies candidates against issued-share counts from
the ShareMetrics table in the same database.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date as Date
from fractions import Fraction

logger = logging.getLogger(__name__)

# Days between consecutive price rows that still count as "adjacent" for
# split detection.  Longer gaps are assumed to be delistings / relistings.
_MAX_ADJACENT_DAY_GAP = 10

# Merge candidates whose split_date falls within this many calendar days
# into a single event (handles splits that manifest across a weekend or
# holiday gap).
_MERGE_WINDOW_DAYS = 5

# Tolerance for matching ShareMetrics share-count ratio to the candidate
# split ratio.  Annual share counts can drift due to buybacks / issuance.
_SHARE_COUNT_TOLERANCE = 0.20

# If ShareMetrics ratio is further than this multiple from the candidate
# ratio, the candidate is rejected rather than kept as pending.
_REJECTION_MULTIPLIER = 3.0


# ---------------------------------------------------------------------------
# Column name resolution (flexible, like security_analysis.py)
# ---------------------------------------------------------------------------


def _resolve_column(columns: list[str], candidates: list[str]) -> str | None:
    """Return the first column name in *columns* that matches a candidate."""
    lower = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None


def _get_columns(conn: sqlite3.Connection, table_name: str | None) -> list[str]:
    """Return the column names for a table, or [] if the table doesn't exist."""
    if not table_name:
        return []
    try:
        return [
            str(row[1])
            for row in conn.execute(f'PRAGMA table_info("{table_name}")')
        ]
    except sqlite3.OperationalError:
        return []


# ---------------------------------------------------------------------------
# Price heuristic scanner
# ---------------------------------------------------------------------------


def _parse_date(val: str) -> Date | None:
    try:
        return Date.fromisoformat(val)
    except (ValueError, TypeError):
        return None


def _coerce_number(value: object) -> float | None:
    """Parse EDINET numeric text, including comma-separated report values."""
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _round_split_ratio(raw_ratio: float) -> tuple[int, int]:
    """Convert a raw price ratio to a canonical (from, to) pair.

    For forward splits (price drops), ratio_from is fixed to 1.
    For reverse splits (price spikes), ratio_to is fixed to 1.

    Returns ``(ratio_from, ratio_to)`` as integers.
    """
    if raw_ratio <= 0:
        return (1, 1)
    if raw_ratio >= 1.0:
        # Forward split: price dropped → more shares
        nearest = max(1, round(raw_ratio))
        if abs(raw_ratio - nearest) <= 0.10:
            return (1, nearest)
        fraction = Fraction(raw_ratio).limit_denominator(20)
        return (max(1, fraction.denominator), max(1, fraction.numerator))
    else:
        # Reverse split: price spiked → fewer shares
        inverse = 1.0 / raw_ratio
        nearest = max(1, round(inverse))
        if abs(inverse - nearest) <= 0.10:
            return (nearest, 1)
        fraction = Fraction(raw_ratio).limit_denominator(20)
        return (max(1, fraction.denominator), max(1, fraction.numerator))


def detect_splits_by_price_heuristic(
    conn: sqlite3.Connection,
    ticker: str,
    threshold: float = 0.40,
    start_date: str | None = None,
) -> list[dict]:
    """Scan Stock_Prices for sudden price moves that look like splits.

    Args:
        conn: Read connection to db2 (Standardized.db).
        ticker: The ticker to scan.
        threshold: Minimum price change (as decimal) to flag.
                   Default 0.40 → flags drops ≥ 40% or spikes ≥ 40%.

    Returns:
        List of candidate dicts with keys:
        ``ticker``, ``split_date``, ``ratio_from``, ``ratio_to``,
        ``price_before``, ``price_after``, ``drop_pct``.
    """
    if not 0 < threshold < 1:
        raise ValueError("threshold must be between 0 and 1")
    columns = set(_get_columns(conn, "Stock_Prices"))
    basis_select = ", Price_Basis" if "Price_Basis" in columns else ""
    provider_select = ", Provider" if "Provider" in columns else ""
    if start_date:
        previous = conn.execute(
            f"SELECT Date, Price{basis_select}{provider_select} FROM Stock_Prices "
            "WHERE Ticker = ? AND Date < ? ORDER BY Date DESC LIMIT 1",
            (ticker, start_date),
        ).fetchall()
        rows = conn.execute(
            f"SELECT Date, Price{basis_select}{provider_select} FROM Stock_Prices "
            "WHERE Ticker = ? AND Date >= ? ORDER BY Date ASC",
            (ticker, start_date),
        ).fetchall()
        rows = [*reversed(previous), *rows]
    else:
        rows = conn.execute(
            f"SELECT Date, Price{basis_select}{provider_select} FROM Stock_Prices "
            "WHERE Ticker = ? ORDER BY Date ASC",
            (ticker,),
        ).fetchall()

    if len(rows) < 2:
        return []

    candidates: list[dict] = []
    for i in range(1, len(rows)):
        previous = rows[i - 1]
        current = rows[i]
        prev_date_str, prev_price = previous[:2]
        curr_date_str, curr_price = current[:2]
        basis_offset = 2 if "Price_Basis" in columns else None
        provider_offset = (
            3 if "Price_Basis" in columns and "Provider" in columns else
            2 if "Provider" in columns else None
        )
        prev_basis = str(previous[basis_offset] or "raw").lower() if basis_offset is not None else "raw"
        curr_basis = str(current[basis_offset] or "raw").lower() if basis_offset is not None else "raw"
        if prev_basis == "adjusted" or curr_basis == "adjusted":
            continue

        prev_date = _parse_date(prev_date_str)
        curr_date = _parse_date(curr_date_str)
        if prev_date is None or curr_date is None:
            continue
        if prev_price is None or curr_price is None or prev_price == 0:
            continue

        gap_days = (curr_date - prev_date).days
        if gap_days > _MAX_ADJACENT_DAY_GAP:
            continue

        pct_change = (curr_price - prev_price) / prev_price

        # Forward split: large drop
        if pct_change <= -threshold:
            raw_ratio = abs(prev_price / curr_price)
            ratio_from, ratio_to = _round_split_ratio(raw_ratio)
            candidates.append({
                "ticker": ticker,
                "split_date": curr_date_str,
                "ratio_from": ratio_from,
                "ratio_to": ratio_to,
                "price_before": prev_price,
                "price_after": curr_price,
                "drop_pct": round(pct_change, 4),
                "price_basis_before": prev_basis,
                "price_basis_after": curr_basis,
                "provider_before": previous[provider_offset] if provider_offset is not None else None,
                "provider_after": current[provider_offset] if provider_offset is not None else None,
            })
        # Reverse split: large spike
        elif pct_change >= threshold:
            # For a reverse split, the ratio is inverted.
            # Price 10 → 100 (10x spike) means 10:1 reverse split.
            raw_ratio = abs(curr_price / prev_price)
            ratio_from, ratio_to = _round_split_ratio(1.0 / raw_ratio)
            if ratio_from > 1:
                candidates.append({
                    "ticker": ticker,
                    "split_date": curr_date_str,
                    "ratio_from": ratio_from,
                    # Preserve fractional reverse splits such as 5:2; the
                    # ratio helper already returns the canonical pair.
                    "ratio_to": ratio_to,
                    "price_before": prev_price,
                    "price_after": curr_price,
                    "drop_pct": round(pct_change, 4),
                    "price_basis_before": prev_basis,
                    "price_basis_after": curr_basis,
                    "provider_before": previous[provider_offset] if provider_offset is not None else None,
                    "provider_after": current[provider_offset] if provider_offset is not None else None,
                })

    return _merge_candidates(candidates)


def _merge_candidates(candidates: list[dict]) -> list[dict]:
    """Merge candidates within _MERGE_WINDOW_DAYS of each other.

    Keeps the earliest date in each cluster and the largest ratio.
    """
    if len(candidates) <= 1:
        return candidates

    merged: list[dict] = []
    cluster = [candidates[0]]

    for c in candidates[1:]:
        prev_date = _parse_date(cluster[-1]["split_date"])
        curr_date = _parse_date(c["split_date"])
        if (
            prev_date is not None
            and curr_date is not None
            and (curr_date - prev_date).days <= _MERGE_WINDOW_DAYS
        ):
            cluster.append(c)
        else:
            merged.append(_pick_best_in_cluster(cluster))
            cluster = [c]

    merged.append(_pick_best_in_cluster(cluster))
    return merged


def _pick_best_in_cluster(cluster: list[dict]) -> dict:
    """From a cluster of adjacent candidates, return the best one.

    Keeps the earliest date and the largest magnitude ratio.
    """
    best = cluster[0]
    for c in cluster[1:]:
        if c["split_date"] < best["split_date"]:
            best = c
        current_ratio = max(best["ratio_to"] / best["ratio_from"],
                            best["ratio_from"] / best["ratio_to"])
        candidate_ratio = max(c["ratio_to"] / c["ratio_from"],
                              c["ratio_from"] / c["ratio_to"])
        if candidate_ratio > current_ratio:
            best = c
    return best


# ---------------------------------------------------------------------------
# ShareMetrics cross-validation
# ---------------------------------------------------------------------------


def verify_split_with_share_metrics(
    conn: sqlite3.Connection,
    ticker: str,
    candidate_date_str: str,
    candidate_ratio_from: float,
    candidate_ratio_to: float,
) -> dict:
    """Cross-validate a split candidate against annual report share counts.

    Looks up the EDINET company code for *ticker*, then finds the
    ShareMetrics rows with fiscal-year-end dates straddling the
    candidate split date.

    Returns a dict with keys:
    ``confirmation`` (str), ``confirmed_by`` (str | None),
    ``share_count_before`` (float | None), ``share_count_after`` (float | None),
    ``share_count_ratio`` (float | None), ``detail`` (str).
    """
    candidate_date = _parse_date(candidate_date_str)
    if candidate_date is None:
        return _verdict("pending", detail="Invalid candidate date")

    candidate_ratio = candidate_ratio_to / candidate_ratio_from

    # --- Resolve column names ---
    company_cols = _get_columns(conn, "CompanyInfo")
    if not company_cols:
        return _verdict("pending", detail="CompanyInfo table not found")

    code_col = _resolve_column(company_cols, ["Company_Code", "EdinetCode", "edinetCode"])
    ticker_col = _resolve_column(company_cols, ["Company_Ticker", "Ticker", "ticker"])
    if not code_col or not ticker_col:
        return _verdict("pending", detail="CompanyInfo missing code/ticker columns")

    # --- Find company code ---
    company = conn.execute(
        f'SELECT "{code_col}" FROM CompanyInfo WHERE "{ticker_col}" = ? LIMIT 1',
        (ticker,),
    ).fetchone()
    if not company:
        return _verdict("pending", detail="Ticker not found in CompanyInfo")

    company_code = company[0]

    # --- Resolve FinancialStatements columns ---
    fs_cols = _get_columns(conn, "FinancialStatements")
    if not fs_cols:
        return _verdict("pending", detail="FinancialStatements table not found")

    fs_code_col = _resolve_column(fs_cols, ["Company_Code", "edinetCode", "EdinetCode"])
    fs_docid_col = _resolve_column(fs_cols, ["docID", "DocID"])
    fs_period_col = _resolve_column(fs_cols, ["periodEnd", "PeriodEnd"])
    if not all([fs_code_col, fs_docid_col, fs_period_col]):
        return _verdict("pending", detail="FinancialStatements missing required columns")

    # --- Resolve ShareMetrics column ---
    share_cols = _get_columns(conn, "ShareMetrics")
    if not share_cols:
        return _verdict("pending", detail="ShareMetrics table not found")

    shares_col = _resolve_column(share_cols, [
        "Total number of issued shares",
        "Number of issued shares as of filing date",
        "Number of issued shares as of fiscal year end",
    ])
    if not shares_col:
        return _verdict("pending", detail="No issued-shares column in ShareMetrics")

    # --- Get docIDs with periodEnd straddling the split date ---
    docs = conn.execute(
        f'SELECT fs."{fs_docid_col}", fs."{fs_period_col}" '
        f"FROM FinancialStatements fs "
        f'WHERE fs."{fs_code_col}" = ? '
        f"ORDER BY fs.\"{fs_period_col}\" ASC",
        (company_code,),
    ).fetchall()

    if len(docs) < 2:
        return _verdict("pending", detail="Insufficient FinancialStatements records")

    # Find latest periodEnd before split and earliest after
    before_docid: str | None = None
    after_docid: str | None = None
    before_period: Date | None = None
    after_period: Date | None = None

    for docid, period_end_str in docs:
        pe = _parse_date(period_end_str)
        if pe is None:
            continue
        if pe <= candidate_date:
            if before_period is None or pe > before_period:
                before_period = pe
                before_docid = docid
        else:
            after_docid = docid
            after_period = pe
            break  # first one after is the nearest

    if before_docid is None or after_docid is None:
        return _verdict(
            "pending",
            detail="Could not find ShareMetrics straddling the split date",
        )

    # --- Query share counts ---
    before_val = conn.execute(
        f'SELECT "{shares_col}" FROM ShareMetrics WHERE "{fs_docid_col}" = ?',
        (before_docid,),
    ).fetchone()
    after_val = conn.execute(
        f'SELECT "{shares_col}" FROM ShareMetrics WHERE "{fs_docid_col}" = ?',
        (after_docid,),
    ).fetchone()

    if not before_val or not after_val:
        return _verdict("pending", detail="ShareMetrics rows missing share count")

    share_before = _coerce_number(before_val[0])
    share_after = _coerce_number(after_val[0])

    if not share_before or not share_after or share_before <= 0 or share_after <= 0:
        return _verdict("pending", detail="Share count is zero or null")

    share_ratio = share_after / share_before
    report_context = (
        f"; report docs {before_docid} ({before_period.isoformat()})"
        f" -> {after_docid} ({after_period.isoformat() if after_period else '?'})"
    )

    # When the normalized FinancialStatements table also retains the report's
    # share price, use it as an independent cross-check.  A split should
    # increase shares by the candidate ratio while reducing the reported
    # per-share price by the same ratio.  Missing prices remain non-blocking
    # because older EDINET loads did not retain this field.
    fs_price_col = _resolve_column(fs_cols, ["SharePrice", "sharePrice"])
    report_price_ratio: float | None = None
    if fs_price_col:
        before_price = conn.execute(
            f'SELECT "{fs_price_col}" FROM FinancialStatements WHERE "{fs_docid_col}" = ?',
            (before_docid,),
        ).fetchone()
        after_price = conn.execute(
            f'SELECT "{fs_price_col}" FROM FinancialStatements WHERE "{fs_docid_col}" = ?',
            (after_docid,),
        ).fetchone()
        try:
            if before_price and after_price and before_price[0] and after_price[0]:
                report_price_ratio = float(before_price[0]) / float(after_price[0])
        except (TypeError, ValueError, ZeroDivisionError):
            report_price_ratio = None

    # --- Compare ---
    ratio_deviation = abs(share_ratio - candidate_ratio) / candidate_ratio

    price_matches = (
        report_price_ratio is None
        or abs(report_price_ratio - candidate_ratio) / candidate_ratio
        <= _SHARE_COUNT_TOLERANCE
    )

    if ratio_deviation <= _SHARE_COUNT_TOLERANCE and price_matches:
        price_detail = (
            f"; report price ratio {report_price_ratio:.4f} also matches"
            if report_price_ratio is not None else ""
        )
        return _verdict(
            "confirmed",
            confirmed_by="share_metrics",
            share_before=share_before,
            share_after=share_after,
            share_ratio=round(share_ratio, 6),
            detail=f"Share count ratio {share_ratio:.4f} matches candidate "
                   f"{candidate_ratio:.4f} (deviation {ratio_deviation:.2%})"
                   f"{price_detail}{report_context}",
        )
    if ratio_deviation <= _SHARE_COUNT_TOLERANCE and not price_matches:
        return _verdict(
            "pending",
            share_before=share_before,
            share_after=share_after,
            share_ratio=round(share_ratio, 6),
            detail=f"Share count matches but report price ratio "
                   f"{report_price_ratio:.4f} conflicts with candidate "
                   f"{candidate_ratio:.4f}{report_context}",
        )
    elif ratio_deviation > _SHARE_COUNT_TOLERANCE * _REJECTION_MULTIPLIER:
        return _verdict(
            "rejected",
            share_before=share_before,
            share_after=share_after,
            share_ratio=round(share_ratio, 6),
            detail=f"Share count ratio {share_ratio:.4f} far from candidate "
                   f"{candidate_ratio:.4f} (deviation {ratio_deviation:.2%})"
                   f"{report_context}",
        )
    else:
        return _verdict(
            "pending",
            share_before=share_before,
            share_after=share_after,
            share_ratio=round(share_ratio, 6),
            detail=f"Share count ratio {share_ratio:.4f} close but outside "
                   f"tolerance for candidate {candidate_ratio:.4f} "
                   f"(deviation {ratio_deviation:.2%}){report_context}",
        )


def _verdict(
    confirmation: str,
    *,
    confirmed_by: str | None = None,
    share_before: float | None = None,
    share_after: float | None = None,
    share_ratio: float | None = None,
    detail: str = "",
) -> dict:
    return {
        "confirmation": confirmation,
        "confirmed_by": confirmed_by,
        "share_count_before": share_before,
        "share_count_after": share_after,
        "share_count_ratio": share_ratio,
        "detail": detail,
    }


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


def run_split_detection(
    db2_path: str,
    tickers: list[str] | None = None,
    mode: str = "incremental",
    threshold: float = 0.40,
) -> dict:
    """Run split detection and update the Stock_Splits table.

    Args:
        db2_path: Path to Standardized.db.
        tickers: Tickers to scan. None = all tickers with price data.
        mode: ``"full"`` (rescan all history), ``"incremental"`` (only new
              data since last known split), or ``"verify_pending"`` (re-check
              entries that are still pending).
        threshold: Minimum price change to flag as a potential split.

    Returns:
        ``{"new_pending": N, "confirmed": N, "rejected": N,
           "already_known": N, "tickers_scanned": N}``
    """
    mode = str(mode or "incremental").strip().lower()
    if mode not in {"full", "incremental", "verify_pending"}:
        raise ValueError(
            "mode must be 'full', 'incremental', or 'verify_pending'"
        )
    if not 0 < threshold < 1:
        raise ValueError("threshold must be between 0 and 1")

    from src.portfolio.split_schema import ensure_split_tables

    ensure_split_tables(db2_path)

    conn = sqlite3.connect(db2_path)
    conn.row_factory = sqlite3.Row

    try:
        # Migrate legacy price rows before classification so a heuristic event
        # cannot accidentally be recorded as raw merely because the old table
        # lacked provenance columns.
        from src.utilities.price_provenance import ensure_price_provenance_columns
        ensure_price_provenance_columns(conn, "Stock_Prices")

        # --- Determine tickers ---
        if tickers is None:
            tickers = [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT Ticker FROM Stock_Prices "
                    "WHERE Ticker != 'EUR' AND Ticker NOT LIKE 'Inflation_%' "
                    "ORDER BY Ticker"
                ).fetchall()
            ]

        if not tickers:
            logger.info("No tickers found in Stock_Prices")
            return {
                "new_pending": 0, "confirmed": 0, "rejected": 0,
                "already_known": 0, "tickers_scanned": 0,
            }

        # --- Fetch existing splits for dedup ---
        existing_rows = conn.execute(
            "SELECT ticker, split_date, ratio_from, ratio_to FROM Stock_Splits"
        ).fetchall()
        existing = {
            (r["ticker"], r["split_date"])
            for r in existing_rows
        }
        existing_events = []
        for row in existing_rows:
            try:
                ratio = float(row["ratio_to"]) / float(row["ratio_from"])
            except (TypeError, ValueError, ZeroDivisionError):
                continue
            if ratio > 0:
                existing_events.append(
                    (str(row["ticker"]), str(row["split_date"])[:10], ratio)
                )

        # --- Fetch existing pending entries (for verify_pending mode) ---
        if mode == "verify_pending":
            pending = conn.execute(
                "SELECT id, ticker, split_date, ratio_from, ratio_to "
                "FROM Stock_Splits WHERE confirmation = 'pending'"
            ).fetchall()
            requested_tickers = set(tickers or [])
            if requested_tickers:
                pending = [r for r in pending if r["ticker"] in requested_tickers]
            tickers = sorted({r["ticker"] for r in pending})

        counts = {
            "new_pending": 0, "confirmed": 0, "rejected": 0,
            "already_known": 0, "tickers_scanned": len(tickers),
        }

        for ticker in tickers:
            logger.debug("Scanning ticker %s", ticker)

            if mode == "verify_pending":
                # Re-verify only existing pending entries for this ticker
                for row in [r for r in pending if r["ticker"] == ticker]:
                    _reverify_entry(conn, row, counts)
                continue

            start_date = None
            if mode == "incremental":
                watermark = conn.execute(
                    "SELECT last_price_date FROM Split_Detection_Watermarks "
                    "WHERE ticker = ?",
                    (ticker,),
                ).fetchone()
                start_date = watermark[0] if watermark else None
            candidates = detect_splits_by_price_heuristic(
                conn, ticker, threshold=threshold, start_date=start_date,
            )

            # Filter already-known candidates
            fresh = []
            known_count = 0
            for candidate in candidates:
                candidate_key = (candidate["ticker"], candidate["split_date"])
                candidate_ratio = candidate["ratio_to"] / candidate["ratio_from"]
                candidate_date = _parse_date(candidate["split_date"])
                near_known = any(
                    ticker_name == candidate["ticker"]
                    and candidate_date is not None
                    and _parse_date(known_date) is not None
                    and abs((candidate_date - _parse_date(known_date)).days)
                    <= _MERGE_WINDOW_DAYS
                    and abs(known_ratio - candidate_ratio) / candidate_ratio
                    <= _SHARE_COUNT_TOLERANCE
                    for ticker_name, known_date, known_ratio in existing_events
                )
                if candidate_key in existing or near_known:
                    known_count += 1
                else:
                    fresh.append(candidate)
            counts["already_known"] += known_count

            for cand in fresh:
                # Build source detail
                cand["source_detail"] = (
                    f"heuristic: {cand['drop_pct']:.1%} change, "
                    f"price {cand['price_before']:.2f} → {cand['price_after']:.2f}"
                )

                # Cross-validate
                verdict = verify_split_with_share_metrics(
                    conn,
                    ticker,
                    cand["split_date"],
                    cand["ratio_from"],
                    cand["ratio_to"],
                )

                _insert_split(conn, cand, verdict)
                counts[_status_key(verdict["confirmation"])] += 1

                existing.add((cand["ticker"], cand["split_date"]))
                try:
                    existing_events.append(
                        (
                            cand["ticker"],
                            cand["split_date"],
                            cand["ratio_to"] / cand["ratio_from"],
                        )
                    )
                except (TypeError, ValueError, ZeroDivisionError):
                    pass

            if mode in ("incremental", "full"):
                latest_price_date = conn.execute(
                    "SELECT MAX(Date) FROM Stock_Prices WHERE Ticker = ?",
                    (ticker,),
                ).fetchone()
                if latest_price_date and latest_price_date[0]:
                    conn.execute(
                        "INSERT INTO Split_Detection_Watermarks "
                        "(ticker, last_price_date, updated_at) VALUES (?, ?, datetime('now')) "
                        "ON CONFLICT(ticker) DO UPDATE SET last_price_date = excluded.last_price_date, "
                        "updated_at = excluded.updated_at",
                        (ticker, latest_price_date[0]),
                    )

        from src.utilities.price_provenance import refresh_split_adjusted_prices
        refresh_split_adjusted_prices(conn)
        conn.commit()
        logger.info(
            "Split detection complete: %d new/pending, %d confirmed, "
            "%d rejected, %d already known, %d tickers scanned",
            counts["new_pending"], counts["confirmed"], counts["rejected"],
            counts["already_known"], counts["tickers_scanned"],
        )
        return counts

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _status_key(confirmation: str) -> str:
    if confirmation == "confirmed":
        return "confirmed"
    elif confirmation == "rejected":
        return "rejected"
    return "new_pending"


def _insert_split(
    conn: sqlite3.Connection,
    cand: dict,
    verdict: dict,
) -> None:
    from src.utilities.price_provenance import source_id as build_source_id
    from src.utilities.price_provenance import utc_now

    basis_before = str(cand.get("price_basis_before") or "unknown").lower()
    basis_after = str(cand.get("price_basis_after") or "unknown").lower()
    event_basis = (
        "raw"
        if verdict["confirmation"] == "confirmed"
        else ("raw" if basis_before == basis_after == "raw" else "unknown")
    )
    provider = cand.get("provider_after") or cand.get("provider_before") or "price_heuristic"
    conn.execute(
        """INSERT OR IGNORE INTO Stock_Splits
           (ticker, split_date, ratio_from, ratio_to,
            detection_method, confirmation, confirmed_by,
            source_detail, price_basis, provider, source_id,
            source_revision, retrieved_at,
            share_count_before, share_count_after, share_count_ratio)
           VALUES (?, ?, ?, ?, 'price_heuristic', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            cand["ticker"],
            cand["split_date"],
            cand["ratio_from"],
            cand["ratio_to"],
            verdict["confirmation"],
            verdict["confirmed_by"],
            verdict["detail"],
            event_basis,
            provider,
            build_source_id(provider, cand["ticker"], cand["split_date"]),
            "heuristic-v1",
            utc_now(),
            verdict["share_count_before"],
            verdict["share_count_after"],
            verdict["share_count_ratio"],
        ),
    )


def _reverify_entry(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    counts: dict,
) -> None:
    """Re-run ShareMetrics verification for a single pending entry."""
    verdict = verify_split_with_share_metrics(
        conn,
        row["ticker"],
        row["split_date"],
        row["ratio_from"],
        row["ratio_to"],
    )
    conn.execute(
        "UPDATE Stock_Splits SET "
        "confirmation = ?, confirmed_by = ?, "
        "price_basis = CASE WHEN ? = 'confirmed' THEN 'raw' ELSE price_basis END, "
        "share_count_before = ?, share_count_after = ?, "
        "share_count_ratio = ?, updated_at = datetime('now') "
        "WHERE id = ?",
        (
            verdict["confirmation"],
            verdict["confirmed_by"],
            verdict["confirmation"],
            verdict["share_count_before"],
            verdict["share_count_after"],
            verdict["share_count_ratio"],
            row["id"],
        ),
    )
    counts[_status_key(verdict["confirmation"])] += 1
