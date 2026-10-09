import io
import logging
import sqlite3
import zipfile

import pandas as pd
import requests

from src.orchestrator.common import StepDefinition
from src.orchestrator.common.db_config import get_market_db
from src.utilities import stock_prices
from src.utilities.price_provenance import source_id as build_source_id
from src.utilities.price_provenance import utc_now

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

# Short-term "risk-free" rates (annual percent) used for Sharpe and Sortino
# ratios and the cash benchmark.  EUR uses the ECB's three-month AAA
# government yield; the others come from FRED.
_ECB_RISK_FREE_URL = (
    "https://data-api.ecb.europa.eu/service/data/YC/"
    "B.U2.EUR.4F.G_N_A.SV_C_YM.SR_3M?format=csvdata"
)
_FRED_RISK_FREE_SERIES: dict[str, tuple[str, str]] = {
    "DTB3":             ("RiskFree_USD", "USD"),  # 3-month Treasury bill
    "IUDSOIA":          ("RiskFree_GBP", "GBP"),  # SONIA overnight rate
    "IRSTCI01JPM156N":  ("RiskFree_JPY", "JPY"),  # call rate (monthly)
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

    Returns a DataFrame with columns ``Date``, ``Ticker``, ``Currency``, ``Price``
    where **Ticker = "EUR"** (the base) and **Currency = the target currency**.
    Example: Date=2024-01-02, Ticker=EUR, Currency=USD, Price=1.10
    means 1 EUR = 1.10 USD on that date.
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
        var_name="Currency",
        value_name="Price",
    )

    melted["Price"] = pd.to_numeric(melted["Price"], errors="coerce")

    before = len(melted)
    melted = melted.dropna(subset=["Price"])
    skipped = before - len(melted)
    if skipped:
        logger.info("Skipped %d ECB FX rows with missing rates.", skipped)

    # Drop EUR/EUR (rate is always 1, not useful)
    melted = melted[melted["Currency"] != "EUR"].copy()
    logger.info(
        "After filtering: %d FX rows across %s currencies.",
        len(melted),
        melted["Currency"].nunique(),
    )

    melted["Date"] = pd.to_datetime(melted["Date"], errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )
    melted["Ticker"] = "EUR"
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
        _conn = sqlite3.connect(get_market_db())
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
# Risk-free rates — ECB yield curve (EUR) and FRED (USD, GBP, JPY)
# ---------------------------------------------------------------------------

def _download_ecb_risk_free(session: requests.Session | None = None) -> pd.DataFrame:
    """Download the euro area three-month AAA government spot yield (percent)."""
    session = session or requests.Session()
    logger.info("Downloading ECB three-month yield from %s", _ECB_RISK_FREE_URL)
    try:
        response = session.get(_ECB_RISK_FREE_URL, timeout=_REQUEST_TIMEOUT)
        response.raise_for_status()
    except Exception as exc:
        logger.warning("Failed to download the ECB three-month yield: %s", exc)
        return pd.DataFrame(columns=["Date", "Price"])
    df = pd.read_csv(io.StringIO(response.text))
    if df.empty or "TIME_PERIOD" not in df.columns or "OBS_VALUE" not in df.columns:
        logger.warning("ECB yield curve returned an unexpected format.")
        return pd.DataFrame(columns=["Date", "Price"])
    result = pd.DataFrame({
        "Date": pd.to_datetime(df["TIME_PERIOD"].astype(str), errors="coerce").dt.strftime("%Y-%m-%d"),
        "Price": pd.to_numeric(df["OBS_VALUE"], errors="coerce"),
    })
    return result.dropna(subset=["Date", "Price"])


def fetch_risk_free_rates(currencies: set[str] | None = None) -> pd.DataFrame:
    """Short-term rates in Stock_Prices format (``RiskFree_{CUR}``, Price in percent).

    *currencies* limits the download (``None`` fetches every supported one).
    """
    wanted = {code.upper() for code in currencies} if currencies else None
    session = requests.Session()
    frames: list[pd.DataFrame] = []
    if wanted is None or "EUR" in wanted:
        eur = _download_ecb_risk_free(session=session)
        if not eur.empty:
            eur["Ticker"] = "RiskFree_EUR"
            eur["Currency"] = "EUR"
            frames.append(eur)
    for series_id, (ticker, currency) in _FRED_RISK_FREE_SERIES.items():
        if wanted is not None and currency not in wanted:
            continue
        df = _download_fred_cpi(series_id, session=session)
        if df.empty:
            logger.warning("Skipping %s — no data from FRED series %s.", ticker, series_id)
            continue
        df["Ticker"] = ticker
        df["Currency"] = currency
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["Date", "Ticker", "Currency", "Price"])
    return pd.concat(frames, ignore_index=True)[["Date", "Ticker", "Currency", "Price"]]


def update_risk_free_rates(db_name: str, currencies: set[str] | None = None, prices_table: str = "Stock_Prices") -> int:
    """Download short-term rates and insert the dates not yet stored; return rows added."""
    return _insert_new_pairs(
        fetch_risk_free_rates(currencies), db_name, prices_table, label="risk-free rate records",
    )


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

        retrieved_at = utc_now()
        df["Price_Basis"] = "raw"
        df["Provider"] = label
        df["Source_Id"] = [
            build_source_id(label, ticker, row_date)
            for ticker, row_date in zip(df["Ticker"], df["Date"], strict=True)
        ]
        df["Source_Revision"] = "download-v1"
        df["Adjustment_Factor"] = None
        df["Retrieved_At"] = retrieved_at

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
    context=None,
) -> dict[str, int]:
    """Download ECB FX rates, consumer price indexes, and short-term interest rates.

    Only new ``(Date, Ticker)`` pairs are inserted.  Returns a dict with
    ``fx``, ``inflation``, and ``risk_free`` keys holding the rows added.
    """
    logger.info("Starting FX and inflation data update.")
    if context is not None:
        context.report_progress(0, 3, "Fetching foreign-exchange data")

    # --- FX rates ---
    fx_df = _fetch_ecb_fx_prices()
    fx_inserted = _insert_new_pairs(fx_df, db_name, prices_table, label="FX records")

    # --- Inflation / CPI ---
    if context is not None:
        context.report_progress(1, 3, "Fetching inflation data")
    inflation_df = _fetch_all_inflation_prices()
    inflation_inserted = _insert_new_pairs(
        inflation_df, db_name, prices_table, label="inflation records",
    )

    # --- Short-term interest rates (risk-free) ---
    if context is not None:
        context.report_progress(2, 3, "Fetching short-term interest rates")
    risk_free_inserted = update_risk_free_rates(db_name, prices_table=prices_table)

    logger.info(
        "Update FX Data complete: %d FX rows, %d inflation rows, %d interest-rate rows inserted.",
        fx_inserted,
        inflation_inserted,
        risk_free_inserted,
    )
    if context is not None:
        context.report_progress(3, 3, "FX, inflation, and interest-rate update complete")
    return {"fx": fx_inserted, "inflation": inflation_inserted, "risk_free": risk_free_inserted}


def run_update_fx_data(config, overwrite=False, context=None):  # noqa: ARG001
    """Handler invoked by the orchestrator."""
    logger.info("Updating FX and inflation data...")
    kwargs = dict(
        db_name=get_market_db(),
        prices_table="Stock_Prices",
    )
    if context is not None:
        kwargs["context"] = context
    return update_fx_data(**kwargs)


STEP_DEFINITION = StepDefinition(
    name="update_fx_data",
    handler=run_update_fx_data,
    display_name="Update FX Data",
    input_fields=(),
)
