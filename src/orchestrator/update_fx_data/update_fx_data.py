import io
import logging
import sqlite3
import zipfile

import pandas as pd
import requests

from src.orchestrator.common import StepDefinition
from src.orchestrator.common.db_config import get_db2
from src.utilities import stock_prices

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FX data — European Central Bank
# ---------------------------------------------------------------------------

_ECB_FX_URL = (
    "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"
)

_ECB_HICP_URL = (
    "https://data-api.ecb.europa.eu/service/data/ICP/"
    "M.U2.N.000000.4.INX?format=csvdata"
)

_FRED_BASE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
_DBNOMICS_BASE_URL = "https://api.db.nomics.world/v22/series"

_REQUEST_TIMEOUT = 60  # seconds

# FRED series ID → (ticker, currency)
_FRED_INFLATION_SERIES: dict[str, tuple[str, str]] = {
    "CPIAUCSL":        ("Inflation_USD", "USD"),
    "GBRCPIALLMINMEI": ("Inflation_GBP", "GBP"),
    "CANCPIALLMINMEI": ("Inflation_CAD", "CAD"),
    # JPN and AUS OECD series discontinued — use DBnomics instead
}

# DBnomics IMF/IFS series key → (ticker, currency)
# Format: "FREQ.REF_AREA.INDICATOR" where INDICATOR = PCPI_IX (CPI index)
_DBNOMICS_INFLATION_SERIES: dict[str, tuple[str, str]] = {
    "M.JP.PCPI_IX": ("Inflation_JPY", "JPY"),
}


# ---------------------------------------------------------------------------
# ECB FX download and transform
# ---------------------------------------------------------------------------

def _download_ecb_fx_csv(session: requests.Session | None = None) -> pd.DataFrame:
    """Download ECB historical FX data and parse the CSV into a DataFrame.

    Returns a raw DataFrame with ``Date`` and one column per currency.
    ``N/A`` values are preserved as-is at this stage.
    """
    if session is None:
        session = requests.Session()

    logger.info("Downloading ECB FX data from %s", _ECB_FX_URL)
    response = session.get(_ECB_FX_URL, timeout=_REQUEST_TIMEOUT)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as csv_file:
            df = pd.read_csv(csv_file)

    logger.info(
        "Downloaded ECB FX data: %d rows, %d columns",
        len(df),
        len(df.columns) - 1,  # exclude Date
    )
    return df


def _transform_ecb_fx_to_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Transform ECB FX CSV into Stock_Prices table format.

    The ECB CSV has a ``Date`` column followed by currency columns with rates
    (units of currency per 1 EUR).  ``N/A`` values and the ``EUR`` column
    (if present) are dropped.

    Returns a DataFrame with columns ``Date``, ``Ticker``, ``Currency``, ``Price``.
    """
    # Melt: Date stays as identifier, currency columns become rows.
    # The ECB CSV has trailing commas, which pandas parses as unnamed
    # columns — filter those out.
    id_vars = ["Date"]
    currency_cols = [
        col
        for col in df.columns
        if col != "Date"
        and pd.notna(col)
        and str(col).strip() != ""
        and not str(col).startswith("Unnamed")
    ]

    melted = df.melt(
        id_vars=id_vars,
        value_vars=currency_cols,
        var_name="Ticker",
        value_name="Price",
    )

    melted["Price"] = pd.to_numeric(melted["Price"], errors="coerce")

    before = len(melted)
    melted = melted.dropna(subset=["Price"])
    skipped = before - len(melted)
    if skipped:
        logger.info("Skipped %d ECB FX rows with missing rates.", skipped)

    # Drop EUR/EUR (rate is always 1, not useful)
    melted = melted[melted["Ticker"] != "EUR"].copy()
    logger.info(
        "After filtering: %d FX rows across %s currencies.",
        len(melted),
        melted["Ticker"].nunique(),
    )

    melted["Date"] = pd.to_datetime(melted["Date"], errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )
    melted["Currency"] = "EUR"
    melted = melted[["Date", "Ticker", "Currency", "Price"]]

    return melted


def _fetch_ecb_fx_prices() -> pd.DataFrame:
    """Download and transform ECB FX data in one call."""
    raw_df = _download_ecb_fx_csv()
    return _transform_ecb_fx_to_prices(raw_df)


# ---------------------------------------------------------------------------
# Inflation — FRED (CPI for USD, GBP, CAD)
# ---------------------------------------------------------------------------

def _download_fred_cpi(series_id: str, session: requests.Session | None = None) -> pd.DataFrame:
    """Download a single FRED CPI series.

    Returns a DataFrame with columns ``Date`` and ``Price``, or an empty
    DataFrame on failure.
    """
    if session is None:
        session = requests.Session()

    url = f"{_FRED_BASE_URL}?id={series_id}"
    logger.info("Downloading FRED series %s", series_id)

    try:
        response = session.get(url, timeout=_REQUEST_TIMEOUT)
        response.raise_for_status()
    except Exception as exc:
        logger.warning("Failed to download FRED series %s: %s", series_id, exc)
        return pd.DataFrame(columns=["Date", "Price"])

    df = pd.read_csv(io.StringIO(response.text))
    if df.empty or "observation_date" not in df.columns:
        logger.warning("FRED series %s returned empty or unexpected format.", series_id)
        return pd.DataFrame(columns=["Date", "Price"])

    value_col = [c for c in df.columns if c != "observation_date"][0]

    result = pd.DataFrame({
        "Date": pd.to_datetime(df["observation_date"], errors="coerce").dt.strftime(
            "%Y-%m-%d"
        ),
        "Price": pd.to_numeric(df[value_col], errors="coerce"),
    })
    result = result.dropna(subset=["Date", "Price"])

    logger.info("Downloaded FRED series %s: %d rows.", series_id, len(result))
    return result


# ---------------------------------------------------------------------------
# Inflation — DBnomics IMF/IFS (CPI for JPY, AUD — OECD series discontinued)
# ---------------------------------------------------------------------------

def _download_dbnomics_cpi(series_key: str, session: requests.Session | None = None) -> pd.DataFrame:
    """Download a single CPI series from DBnomics (IMF IFS dataset).

    Returns a DataFrame with columns ``Date`` and ``Price``, or an empty
    DataFrame on failure.
    """
    if session is None:
        session = requests.Session()

    url = f"{_DBNOMICS_BASE_URL}/IMF/IFS/{series_key}?observations=1&format=csv"
    logger.info("Downloading DBnomics series %s", series_key)

    try:
        response = session.get(url, timeout=_REQUEST_TIMEOUT)
        response.raise_for_status()
    except Exception as exc:
        logger.warning("Failed to download DBnomics series %s: %s", series_key, exc)
        return pd.DataFrame(columns=["Date", "Price"])

    # DBnomics CSV format: period, "series description" (header)
    # Data rows: YYYY-MM, value
    df = pd.read_csv(io.StringIO(response.text))
    if df.empty or "period" not in df.columns:
        logger.warning("DBnomics series %s returned unexpected format.", series_key)
        return pd.DataFrame(columns=["Date", "Price"])

    # Value column is the second column (first is period)
    value_col = df.columns[1]
    # Pad date to YYYY-MM-DD format (DBnomics gives YYYY-MM)
    df["Date"] = pd.to_datetime(
        df["period"].astype(str), errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    df["Price"] = pd.to_numeric(df[value_col], errors="coerce")
    result = df[["Date", "Price"]].dropna(subset=["Date", "Price"])

    logger.info("Downloaded DBnomics series %s: %d rows.", series_key, len(result))
    return result


# ---------------------------------------------------------------------------
# Inflation — ECB SDMX (HICP for EUR)
# ---------------------------------------------------------------------------

def _download_ecb_hicp(session: requests.Session | None = None) -> pd.DataFrame:
    """Download ECB HICP (Euro area CPI) via the SDMX API.

    Returns a DataFrame with columns ``Date`` and ``Price`` (index value,
    base 2015=100), or an empty DataFrame on failure.
    """
    if session is None:
        session = requests.Session()

    logger.info("Downloading ECB HICP data from %s", _ECB_HICP_URL)

    try:
        response = session.get(_ECB_HICP_URL, timeout=_REQUEST_TIMEOUT)
        response.raise_for_status()
    except Exception as exc:
        logger.warning("Failed to download ECB HICP: %s", exc)
        return pd.DataFrame(columns=["Date", "Price"])

    df = pd.read_csv(io.StringIO(response.text))
    if df.empty or "TIME_PERIOD" not in df.columns or "OBS_VALUE" not in df.columns:
        logger.warning("ECB HICP returned unexpected format.")
        return pd.DataFrame(columns=["Date", "Price"])

    result = pd.DataFrame({
        "Date": pd.to_datetime(df["TIME_PERIOD"].astype(str), errors="coerce").dt.strftime(
            "%Y-%m-%d"
        ),
        "Price": pd.to_numeric(df["OBS_VALUE"], errors="coerce"),
    })
    result = result.dropna(subset=["Date", "Price"])

    logger.info("Downloaded ECB HICP: %d rows.", len(result))
    return result


# ---------------------------------------------------------------------------
# Assemble inflation data for all configured currencies
# ---------------------------------------------------------------------------

def _fetch_all_inflation_prices() -> pd.DataFrame:
    """Fetch inflation/CPI data for all supported currencies.

    Returns a DataFrame in Stock_Prices format:
    ``Date``, ``Ticker``, ``Currency``, ``Price``.
    """
    session = requests.Session()
    frames: list[pd.DataFrame] = []

    # EUR — ECB HICP
    eur_df = _download_ecb_hicp(session=session)
    if not eur_df.empty:
        eur_df["Ticker"] = "Inflation_EUR"
        eur_df["Currency"] = "EUR"
        frames.append(eur_df)

    # USD, GBP, CAD — FRED
    for series_id, (ticker, currency) in _FRED_INFLATION_SERIES.items():
        df = _download_fred_cpi(series_id, session=session)
        if df.empty:
            logger.warning(
                "Skipping inflation ticker %s — no data from FRED series %s.",
                ticker,
                series_id,
            )
            continue
        df["Ticker"] = ticker
        df["Currency"] = currency
        frames.append(df)

    # JPY — DBnomics IMF/IFS (OECD series discontinued on FRED)
    for series_key, (ticker, currency) in _DBNOMICS_INFLATION_SERIES.items():
        df = _download_dbnomics_cpi(series_key, session=session)
        if df.empty:
            logger.warning(
                "Skipping inflation ticker %s — no data from DBnomics series %s.",
                ticker,
                series_key,
            )
            continue
        # Scale DBnomics data to match existing OECD base if we have overlap.
        # OECD and IMF use different base years, so absolute index values
        # differ even though month-to-month inflation rates are the same.
        _conn = sqlite3.connect(get_db2())
        _existing = _conn.execute(
            "SELECT Date, Price FROM Stock_Prices WHERE Ticker = ? ORDER BY Date DESC LIMIT 1",
            (ticker,),
        ).fetchone()
        _conn.close()
        if _existing:
            _last_existing_date = _existing[0]
            _last_existing_price = _existing[1]
            _overlap = df[df["Date"] == _last_existing_date]
            if not _overlap.empty and _overlap.iloc[0]["Price"] > 0:
                _scale = _last_existing_price / _overlap.iloc[0]["Price"]
                if abs(_scale - 1.0) > 0.001:
                    logger.info(
                        "Scaling DBnomics %s by %.6f to match existing OECD base at %s",
                        ticker, _scale, _last_existing_date,
                    )
                    df = df.copy()
                    df["Price"] = df["Price"] * _scale
        df["Ticker"] = ticker
        df["Currency"] = currency
        frames.append(df)

    if not frames:
        logger.warning("No inflation data downloaded from any source.")
        return pd.DataFrame(columns=["Date", "Ticker", "Currency", "Price"])

    result = pd.concat(frames, ignore_index=True)
    result = result[["Date", "Ticker", "Currency", "Price"]]
    logger.info(
        "Assembled inflation data: %d rows across %d tickers.",
        len(result),
        result["Ticker"].nunique(),
    )
    return result


# ---------------------------------------------------------------------------
# Shared insert helper
# ---------------------------------------------------------------------------

def _insert_new_pairs(
    df: pd.DataFrame,
    db_name: str,
    prices_table: str,
    *,
    label: str = "records",
) -> int:
    """Insert rows into *prices_table*, skipping existing (Date, Ticker) pairs."""
    if df.empty:
        logger.info("No new %s to insert.", label)
        return 0

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

        before_count = len(df)
        if not existing_df.empty:
            df = df.merge(
                existing_df,
                on=["Date", "Ticker"],
                how="left",
                indicator=True,
            )
            df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])

        if df.empty:
            logger.info("No new %s to insert — all Date+Ticker pairs exist.", label)
            return 0

        skipped = before_count - len(df)
        if skipped:
            logger.info("Skipped %d existing %s pairs.", skipped, label)

        df.to_sql(prices_table, conn, if_exists="append", index=False)
        conn.commit()

        logger.info(
            "Inserted %d new %s into %s.",
            len(df),
            label,
            prices_table,
        )
        return len(df)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def update_fx_data(
    db_name: str,
    prices_table: str = "Stock_Prices",
) -> dict[str, int]:
    """Download ECB FX data and insert into the prices table.

    Only new ``(Date, Ticker)`` pairs are inserted.  Returns a dict with
    ``fx`` and ``inflation`` keys holding the count of rows added for each.
    """
    logger.info("Starting FX and inflation data update.")

    # --- FX rates ---
    fx_df = _fetch_ecb_fx_prices()
    fx_inserted = _insert_new_pairs(fx_df, db_name, prices_table, label="FX records")

    # --- Inflation / CPI ---
    inflation_df = _fetch_all_inflation_prices()
    inflation_inserted = _insert_new_pairs(
        inflation_df, db_name, prices_table, label="inflation records",
    )

    logger.info(
        "Update FX Data complete: %d FX rows, %d inflation rows inserted.",
        fx_inserted,
        inflation_inserted,
    )
    return {"fx": fx_inserted, "inflation": inflation_inserted}


def run_update_fx_data(config, overwrite=False):  # noqa: ARG001
    """Handler invoked by the orchestrator."""
    logger.info("Updating FX and inflation data...")
    return update_fx_data(
        db_name=get_db2(),
        prices_table="Stock_Prices",
    )


STEP_DEFINITION = StepDefinition(
    name="update_fx_data",
    handler=run_update_fx_data,
    display_name="Update FX Data",
    required_keys=(),
    input_fields=(),
)
