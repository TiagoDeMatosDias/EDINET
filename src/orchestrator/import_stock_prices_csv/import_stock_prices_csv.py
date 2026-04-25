import logging
import sqlite3

import pandas as pd

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.utilities import stock_prices

logger = logging.getLogger(__name__)


def import_stock_prices_csv(
    db_name,
    prices_table,
    csv_path,
    default_ticker,
    default_currency,
    date_column,
    price_column,
    ticker_column=None,
    currency_column=None,
):
    """Import stock prices from a user-supplied CSV file into the database."""
    logger.info(
        "Importing stock prices from CSV: %s (default_ticker=%s, default_currency=%s)",
        csv_path, default_ticker, default_currency,
    )

    df = pd.read_csv(csv_path, low_memory=False)
    available_columns = [str(column) for column in df.columns]
    columns_by_lower = {str(column).strip().lower(): str(column) for column in df.columns}

    def _resolve_column(configured_name, column_kind, candidates=None, allow_missing=False):
        configured_text = str(configured_name or "").strip()
        if configured_text:
            actual_name = columns_by_lower.get(configured_text.lower())
            if actual_name:
                return actual_name

        for candidate in candidates or []:
            candidate_text = str(candidate or "").strip()
            if not candidate_text:
                continue
            actual_name = columns_by_lower.get(candidate_text.lower())
            if actual_name:
                if configured_text and actual_name.lower() != configured_text.lower():
                    logger.info(
                        "CSV import: using %s column '%s' instead of configured '%s'.",
                        column_kind,
                        actual_name,
                        configured_text,
                    )
                elif not configured_text:
                    logger.info(
                        "CSV import: auto-detected %s column '%s'.",
                        column_kind,
                        actual_name,
                    )
                return actual_name

        if allow_missing:
            return None

        expected_name = configured_text or (candidates or [column_kind])[0]
        raise ValueError(
            f"{column_kind.capitalize()} column '{expected_name}' not found in CSV. "
            f"Available columns: {available_columns}"
        )

    resolved_date_column = _resolve_column(
        date_column,
        "date",
        candidates=[date_column, "Date", "TradeDate", "Datetime"],
    )
    resolved_price_column = _resolve_column(
        price_column,
        "price",
        candidates=[price_column, "Price", "Close", "Adj Close", "AdjClose", "Adjusted Close", "Last"],
    )
    resolved_ticker_column = _resolve_column(
        ticker_column,
        "ticker",
        candidates=[ticker_column, "Ticker", "Code", "Symbol"],
        allow_missing=True,
    )
    resolved_currency_column = _resolve_column(
        currency_column,
        "currency",
        candidates=[currency_column, "Currency"],
        allow_missing=True,
    )

    if not resolved_ticker_column and not str(default_ticker or "").strip():
        raise ValueError(
            "Ticker column was not found in CSV and no default_ticker was supplied. "
            f"Available columns: {available_columns}"
        )
    if not resolved_currency_column and not str(default_currency or "").strip():
        raise ValueError(
            "Currency column was not found in CSV and no default_currency was supplied. "
            f"Available columns: {available_columns}"
        )

    ticker_series = (
        df[resolved_ticker_column].fillna("").astype(str).str.strip()
        if resolved_ticker_column else pd.Series([default_ticker] * len(df), index=df.index)
    )
    currency_series = (
        df[resolved_currency_column].fillna("").astype(str).str.strip()
        if resolved_currency_column else pd.Series([default_currency] * len(df), index=df.index)
    )

    ticker_series = ticker_series.replace({"nan": "", "None": ""})
    currency_series = currency_series.replace({"nan": "", "None": ""})

    if default_ticker:
        ticker_series = ticker_series.replace("", default_ticker)
    if default_currency:
        currency_series = currency_series.replace("", default_currency)

    out = pd.DataFrame({
        "Date": pd.to_datetime(df[resolved_date_column]).dt.strftime("%Y-%m-%d"),
        "Ticker": ticker_series,
        "Currency": currency_series,
        "Price": pd.to_numeric(df[resolved_price_column], errors="coerce"),
    })

    out = out.dropna(subset=["Date", "Price"]).copy()
    out["Ticker"] = out["Ticker"].astype(str).str.strip()
    out["Currency"] = out["Currency"].astype(str).str.strip()
    out = out[(out["Ticker"] != "") & (out["Currency"] != "")]

    conn = sqlite3.connect(db_name)
    try:
        stock_prices._create_prices_table(conn, prices_table)

        existing_df = pd.DataFrame(columns=["Date", "Ticker"])
        try:
            existing_df = pd.read_sql_query(
                f"SELECT DISTINCT Date, Ticker FROM {prices_table}",
                conn,
            )
        except Exception:
            pass

        if not existing_df.empty:
            out = out.merge(existing_df, on=["Date", "Ticker"], how="left", indicator=True)
            out = out[out["_merge"] == "left_only"].drop(columns=["_merge"])

        if out.empty:
            logger.info("No new rows to insert - all Date+Ticker pairs already exist.")
            return 0

        out.to_sql(prices_table, conn, if_exists="append", index=False)
        conn.commit()
        logger.info("Successfully imported %d price records.", len(out))
        return len(out)
    finally:
        conn.close()


def run_import_stock_prices_csv(config, overwrite=False):
    logger.info("Importing stock prices from CSV...")
    step_cfg = config.get("import_stock_prices_csv_config", {})

    return import_stock_prices_csv(
        db_name=step_cfg.get("Target_Database"),
        prices_table=config.get("DB_STOCK_PRICES_TABLE"),
        csv_path=step_cfg.get("csv_file", ""),
        default_ticker=step_cfg.get("default_ticker", step_cfg.get("ticker", "")),
        default_currency=step_cfg.get("default_currency", step_cfg.get("currency", "JPY")),
        date_column=step_cfg.get("date_column", "Date"),
        price_column=step_cfg.get("price_column", "Price"),
        ticker_column=step_cfg.get("ticker_column", ""),
        currency_column=step_cfg.get("currency_column", ""),
    )


STEP_DEFINITION = StepDefinition(
    name="import_stock_prices_csv",
    handler=run_import_stock_prices_csv,
    display_name="Import Stock Prices (CSV)",
    required_keys=("DB_STOCK_PRICES_TABLE",),
    input_fields=(
        StepFieldDefinition("Target_Database", "database", required=True),
        StepFieldDefinition("csv_file", "file", required=True),
        StepFieldDefinition("default_ticker", "str"),
        StepFieldDefinition("default_currency", "str", default="JPY"),
        StepFieldDefinition("date_column", "str", default="Date"),
        StepFieldDefinition("price_column", "str", default="Price"),
        StepFieldDefinition("ticker_column", "str"),
        StepFieldDefinition("currency_column", "str"),
    ),
)