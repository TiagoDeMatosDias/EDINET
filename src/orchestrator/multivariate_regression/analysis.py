# importing modules and packages
from __future__ import annotations

import logging
import re
import pandas as pd
import numpy as np
import statsmodels.api as sm
import sqlite3
import os

logger = logging.getLogger(__name__)


def _infer_primary_source_ref(query: str) -> str | None:
    """Infer the first table reference / alias used in the query's FROM clause.

    This is used when `build_scoring_query()` needs to inject `edinetCode`
    and `periodEnd` into a user-supplied SELECT. Injecting them as bare column
    names can become ambiguous for multi-table JOIN queries, so when possible we
    qualify them with the first FROM source (usually the primary fact table).
    """
    match = re.search(
        r'(?is)\bfrom\s+([A-Za-z_][A-Za-z0-9_\."]*)(?:\s+(?:as\s+)?([A-Za-z_][A-Za-z0-9_]*))?',
        query.strip(),
    )
    if not match:
        return None
    table_ref = match.group(1)
    alias = match.group(2)
    return alias or table_ref



def Run_Model(
    Query: str,
    conn: sqlite3.Connection,
    dependent_variable_df_name: str,
    independent_variables_df_names: list[str],
    winsorize_limits: tuple[float, float] = (0.01, 0.99)
):
    """
    Executes a SQL query to fetch data, cleans it, and runs an OLS regression model.

    Args:
        Query (str): The SQL query to execute to retrieve the regression data.
        conn (sqlite3.Connection): The connection object to the SQLite database.
        dependent_variable_df_name (str): The name of the dependent variable column.
        independent_variables_df_names (list[str]): A list of names for the
            independent variable columns.

    Returns:
        statsmodels.regression.linear_model.RegressionResultsWrapper:
            The fitted OLS model results. In case of an error during execution,
            an empty model is returned to prevent downstream failures.
    """
    try:
        # Execute the SQL query to get the dataset for regression.
        df = pd.read_sql_query(Query, conn)

        # --- Data Cleaning ---
        # Ensure all model variables are numeric (convert from object/string if needed)
        all_vars = [dependent_variable_df_name] + independent_variables_df_names
        for col in all_vars:
            if col in df.columns:
                # Convert to numeric, coercing errors to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Replace infinite values with NaN so they can be dropped.
        df_cleaned = df.replace([np.inf, -np.inf], np.nan)

        # Drop rows with missing values in any of the model's variables.
        # This ensures the regression is run on a complete dataset.
        df_cleaned = df_cleaned.dropna(subset=all_vars)

        # drop the edinetCode and PeriodStart columns as they are not needed for regression
        df_cleaned = df_cleaned.drop(columns=["edinetCode", "periodStart"], errors="ignore")

        # winsorize_limits is a tuple like (0.01, 0.99) representing the lower and upper quantiles for winsorization.
        upper_limit = winsorize_limits[1]
        lower_limit = winsorize_limits[0]

        # Drop rows with extreme values outside the quantile bounds
        lower_bound = df_cleaned.quantile(lower_limit, axis=0)
        upper_bound = df_cleaned.quantile(upper_limit, axis=0)
        df_cleaned = df_cleaned[(df_cleaned >= lower_bound).all(axis=1) & (df_cleaned <= upper_bound).all(axis=1)]

        # --- Prepare data for OLS regression ---
        y_cleaned = df_cleaned[dependent_variable_df_name]
        X_cleaned = df_cleaned[independent_variables_df_names]
        

        # Add a constant (intercept)
        X_cleaned = sm.add_constant(X_cleaned)  



        # --- Fit the OLS model ---
        model = sm.OLS(y_cleaned, X_cleaned)
        results = model.fit()
        return results

    except Exception as e:
        print(f"An error occurred during regression model execution: {e}")
        # In case of any error, return an empty, fitted model. This ensures
        # that the function always returns a results object, which can
        # prevent crashes in subsequent code that expects one.
        try:
            empty_model = sm.OLS(pd.Series(dtype=float), pd.DataFrame(dtype=float)).fit()
        except Exception:
            # numpy 2.x raises ValueError on zero-size arrays; fall back to a
            # two-observation dummy model so the caller always gets a valid object.
            y_dummy = pd.Series([0.0, 0.0])
            X_dummy = sm.add_constant(pd.Series([0.0, 0.0]))
            empty_model = sm.OLS(y_dummy, X_dummy).fit()
        return empty_model


def build_scoring_query(
    results: sm.regression.linear_model.RegressionResultsWrapper,
    query: str,
    company_table: str = "companyInfo",
) -> str:
    """Build a scoring SQL query from fitted OLS results and the original query.

    The original *query* is wrapped as a subquery so that any computed /
    aliased columns (e.g. ``(expr) AS alias``) are available for the score
    expression.  ``edinetCode`` and ``periodEnd`` are injected into the
    inner SELECT when they are not already present, because the outer query
    needs them for the company JOIN and year extraction.

    Args:
        results: Fitted OLS model whose ``params`` supply the coefficients.
        query: The original SQL query used to generate the regression data.
        company_table: Name of the company-info table joined to map
            ``edinetCode`` → ``Company_Ticker``.

    Returns:
        A complete SQL string that scores every row, joined to company info,
        ordered by year and descending score.
    """
    # Build scoring expression and collect independent variable names
    terms: list[str] = []
    ind_vars: list[str] = []
    for var, coef in results.params.items():
        c = round(coef, 4)
        if var == "const":
            terms.append(f"{c}")
        else:
            terms.append(f"r.{var} * {c}")
            ind_vars.append(var)

    score_expr = " + ".join(terms) if terms else "0"

    # WHERE restrictions ensuring all independent variables are not empty
    where_conditions = [f"r.{var} IS NOT NULL" for var in ind_vars]
    where_clause = (
        "\n      AND ".join(where_conditions)
        if where_conditions
        else "1=1"
    )

    # Augment the original query with edinetCode / periodEnd when missing,
    # then use it as a subquery so computed aliases are accessible.
    #
    # Important: for multi-table JOIN queries these identifiers may exist in
    # more than one joined table, so we qualify them with the first FROM source
    # when possible instead of injecting bare `edinetCode` / `periodEnd`.
    query_stripped = query.strip()
    from_match = re.search(r'(?i)\bFROM\b', query_stripped)
    if from_match:
        select_part = query_stripped[:from_match.start()].lower()
        primary_ref = _infer_primary_source_ref(query_stripped)
        additions: list[str] = []
        if 'edinetcode' not in select_part:
            if primary_ref:
                additions.append(f'{primary_ref}.edinetCode AS edinetCode')
            else:
                additions.append('edinetCode')
        if 'periodend' not in select_part:
            if primary_ref:
                additions.append(f'{primary_ref}.periodEnd AS periodEnd')
            else:
                additions.append('periodEnd')

        if additions:
            addition_str = ', ' + ', '.join(additions)
            augmented_query = (
                query_stripped[:from_match.start()]
                + addition_str + '\n'
                + query_stripped[from_match.start():]
            )
        else:
            augmented_query = query_stripped
    else:
        augmented_query = query_stripped

    return (
        f"SELECT\n"
        f"    c.Company_Ticker AS Tickers,\n"
        f"    SUBSTR(r.periodEnd, 1, 4) AS Year,\n"
        f"    ({score_expr}) AS Score\n"
        f"FROM ({augmented_query}) r\n"
        f"JOIN {company_table} c ON c.edinetCode = r.edinetCode\n"
        f"WHERE {where_clause}\n"
        f"ORDER BY Year, Score DESC"
    )


def write_results_to_file(
    results: sm.regression.linear_model.RegressionResultsWrapper,
    query: str,
    output_file: str,
    alpha: float = 0.05,
    conn: sqlite3.Connection | None = None,
    company_table: str = "companyInfo",
) -> None:
    """
    Writes the OLS regression summary and a significance analysis to a file.

    The output file is structured with the following sections:
    1. The SQL query used for the model.
    2. A full scoring SQL query with FROM and WHERE clauses that can be
       executed directly against the database.
    3. The full OLS regression results summary.
    4. A significance analysis, listing variables and predictors that are
       statistically significant at the given alpha level.

    When *conn* is provided, the scoring query is executed and a CSV file
    (``<output_file_stem>_top10.csv``) is written listing the top 10
    companies per year by predicted score.  The CSV has columns
    ``Year``, ``Tickers``, ``Type``, ``Amount`` and is designed to feed
    directly into batch-backtest workflows.

    Args:
        results: The fitted OLS model results from statsmodels.
        query: The SQL query used to generate the data for the model.
        output_file: The path to the file where results will be saved.
        alpha: The significance level for identifying significant variables.
        conn: Optional open SQLite connection.  When provided the scoring
            query is executed and the top-10 CSV is generated.
        company_table: Name of the company-info table (joined to map
            ``edinetCode`` to ``Company_Ticker``).
    """
    p_values = results.pvalues
    significant_variables = p_values[p_values < alpha]

    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(output_file, "w") as f:
        # --- Write Query ---
        f.write("---" + " SQL Query ---" + "\n")
        f.write(query.strip() + "\n\n")

        # --- Scoring Query ---
        f.write("---" + " Scoring Query ---" + "\n")

        scoring_sql = build_scoring_query(results, query, company_table)

        f.write(scoring_sql + "\n")

        # --- Write OLS Summary ---
        f.write("---" + " OLS Regression Results ---" + "\n")
        f.write(str(results.summary()) + "\n\n")

        # --- Write Significance Analysis ---
        f.write("---" + " Significance Analysis ---" + "\n")
        f.write(f"Significance level (alpha): {alpha}" + "\n\n")

        # All significant variables (including constant)
        f.write(f"Variables significant at the {alpha:.1%} level:" + "\n")
        if not significant_variables.empty:
            for var, p_val in significant_variables.items():
                f.write(f"- {var} (P-value: {p_val:.4g})" + "\n")
        else:
            f.write("No variables are significant at this level." + "\n")
        f.write("\n")

        # Significant predictor variables (excluding constant)
        significant_predictors = significant_variables.drop("const", errors="ignore")
        f.write(f"Predictor variables significant at the {alpha:.1%} level:" + "\n")
        if not significant_predictors.empty:
            for var, p_val in significant_predictors.items():
                f.write(f"- {var} (P-value: {p_val:.4g})" + "\n")
        else:
            f.write("No predictor variables are significant at this level." + "\n")

    print(f"\nOLS results summary written to {output_file}")

    # --- Generate top-10 companies CSV by year ---
    if conn is not None:
        csv_output = os.path.splitext(output_file)[0] + "_top10.csv"
        try:
            scoring_df = pd.read_sql_query(scoring_sql, conn)
            if not scoring_df.empty:
                # Rank within each year and keep top 10
                top10 = (
                    scoring_df
                    .sort_values(["Year", "Score"], ascending=[True, False])
                    .groupby("Year")
                    .head(10)
                )
                # Assign equal weights within each year group
                counts = top10.groupby("Year")["Tickers"].transform("count")
                top10 = top10.assign(
                    Type="weight",
                    Amount=(1.0 / counts).round(4),
                )
                top10[["Year", "Tickers", "Type", "Amount"]].to_csv(
                    csv_output, index=False,
                )
                print(f"Top 10 companies by year written to {csv_output}")
            else:
                print("Scoring query returned no rows; no top-10 CSV generated.")
        except Exception as exc:
            print(f"Unable to generate top-10 CSV: {exc}")


def multivariate_regression(
    config: dict,
    db_path: str,
    company_table: str = "companyInfo",
) -> None:
    """Run a multivariate OLS regression defined entirely by a SQL query.

    The first column returned by ``SQL_Query`` is the dependent variable;
    all remaining columns are independent variables.  This lets the model be
    changed purely through the config without touching any code.

    Winsorisation is applied only when ``winsorize_thresholds`` is present in
    *config*.  When the key is absent, limits of ``(0.0, 1.0)`` are used,
    which preserves every row (no clipping).

    Args:
        config: The ``Multivariate_Regression_config`` dict, containing:

            * ``SQL_Query`` (str)  - SQL that returns the regression dataset.
            * ``Output`` (str)     - Path for the results text file.
            * ``winsorize_thresholds`` (dict, *optional*) -
              ``{"lower": float, "upper": float}``.  Omit to skip winsorisation.

        db_path: Fallback path to the SQLite database file.  If *config*
            contains a non-empty ``Source_Database`` key that value is used
            instead, making it possible to run regressions against any
            database (e.g. a dedicated ratios or historical DB) without
            modifying the environment or other pipeline steps.
    """
    sql = config.get("SQL_Query")
    output_file = config.get("Output")

    if not sql:
        raise ValueError("Multivariate_Regression_config must contain 'SQL_Query'.")
    if not output_file:
        raise ValueError("Multivariate_Regression_config must contain 'Output'.")

    # When thresholds are absent, (0.0, 1.0) keeps every row (no clipping).
    thresholds = config.get("winsorize_thresholds")
    winsorize_limits = (
        (thresholds["lower"], thresholds["upper"]) if thresholds else (0.0, 1.0)
    )

    # Allow the config to specify a different database (e.g. a separate
    # ratios/historical DB).  Fall back to the caller-supplied db_path when
    # the key is absent or blank so existing callers are unaffected.
    source_db = config.get("Source_Database") or db_path
    conn = sqlite3.connect(source_db)
    try:
        # Derive variable names without loading the full dataset.
        # Wrapping as a subquery handles any trailing ORDER BY / LIMIT in sql.
        peek = pd.read_sql_query(f"SELECT * FROM ({sql}) LIMIT 0", conn)
        dep_var = peek.columns[0]
        ind_vars = list(peek.columns[1:])

        results = Run_Model(sql, conn, dep_var, ind_vars, winsorize_limits)
        write_results_to_file(
            results, sql, output_file,
            conn=conn,
            company_table=company_table,
        )
    finally:
        conn.close()