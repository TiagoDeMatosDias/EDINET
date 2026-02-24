# importing modules and packages
from __future__ import annotations

import pandas as pd
import numpy as np
import statsmodels.api as sm
import sqlite3
import os



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
        # Replace infinite values with NaN so they can be dropped.
        df_cleaned = df.replace([np.inf, -np.inf], np.nan)


        # Drop rows with missing values in any of the model's variables.
        # This ensures the regression is run on a complete dataset.
        all_vars = [dependent_variable_df_name] + independent_variables_df_names
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


def write_results_to_file(
    results: sm.regression.linear_model.RegressionResultsWrapper,
    query: str,
    output_file: str,
    alpha: float = 0.05,
) -> None:
    """
    Writes the OLS regression summary and a significance analysis to a file.

    The output file is structured with the following sections:
    1. The SQL query used for the model.
    2. The full OLS regression results summary.
    3. A significance analysis, listing variables and predictors that are
       statistically significant at the given alpha level.

    Args:
        results: The fitted OLS model results from statsmodels.
        query: The SQL query used to generate the data for the model.
        output_file: The path to the file where results will be saved.
        alpha: The significance level for identifying significant variables.
    """
    p_values = results.pvalues
    significant_variables = p_values[p_values < alpha]

    with open(output_file, "w") as f:
        # --- Write Query ---
        f.write("---" + " SQL Query ---" + "\n")
        f.write(query.strip() + "\n\n")

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



# ---------------------------------------------------------------------------
# SIGNIFICANT PREDICTOR SEARCH
# ---------------------------------------------------------------------------
# The block below implements an automated exploratory analysis that tests
# every possible single-predictor OLS regression across all numeric columns
# in the financial-ratios table.  The goal is to surface which individual
# variables have the strongest univariate relationship with each potential
# dependent variable, so that more targeted multi-variable models can be
# built afterwards.
#
# The public entry-point is find_significant_predictors().  The four private
# helpers (_get_predictor_columns, _significance_stars,
# _run_single_predictor_regression, _rank_predictor_results, and
# _write_predictor_search_results) are intentionally small so each step
# can be understood and tested in isolation.
# ---------------------------------------------------------------------------

# Columns that identify rows or describe time periods rather than numeric
# financial quantities.  They are excluded from both the dependent-variable
# and independent-variable candidate lists.  Comparison is done in lowercase
# so "PeriodStart" and "periodstart" are treated identically.
_NON_PREDICTOR_COLUMNS: frozenset[str] = frozenset({
    "index",        # row-number artefact sometimes stored by pandas .to_sql()
    "edinetcode",   # company identifier
    "docid",        # filing document identifier
    "doctypecode",  # EDINET document-type code (e.g. 120 = annual report)
    "periodstart",  # start of the reporting period
    "periodend",    # end of the reporting period
    "currency",     # reporting currency (almost always JPY)
})


def _get_predictor_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    """Return every column from *table_name* that is eligible as a regression variable.

    Uses SQLite's ``PRAGMA table_info`` to retrieve column metadata without
    loading any row data, then filters out the non-predictor identifiers defined
    in ``_NON_PREDICTOR_COLUMNS`` (compared case-insensitively).

    ``PRAGMA table_info`` returns one row per column in the form:
        (cid, name, type, notnull, dflt_value, pk)
    We only need ``name`` (index 1).

    Args:
        conn: An active SQLite connection.
        table_name: The table to inspect (e.g. ``'Standard_Data_Ratios'``).

    Returns:
        A list of column-name strings suitable for use as dep/ind variables.
        Returns an empty list when the table does not exist or has no columns.
    """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns_info = cursor.fetchall()

    # col[1] is the column name; compare lowercase to the exclusion set
    return [
        col[1]
        for col in columns_info
        if col[1].lower() not in _NON_PREDICTOR_COLUMNS
    ]


def _significance_stars(p_value: float | None) -> str:
    """Convert a p-value to a star-rating string for human-readable output.

    Uses the conventional three-tier scheme:
        * ``***``  -  p < 0.001  (highly significant)
        * ``**``   -  p < 0.01   (very significant)
        * ``*``    -  p < 0.05   (significant)
        * ``""``   -  p ≥ 0.05 or None (not significant)

    Args:
        p_value: The p-value to evaluate.  ``None`` is treated as non-significant.

    Returns:
        A stars string (``'***'``, ``'**'``, ``'*'``, or ``''``).
    """
    if p_value is None:
        return ""
    if p_value < 0.001:
        return "***"
    if p_value < 0.01:
        return "**"
    if p_value < 0.05:
        return "*"
    return ""


def _run_single_predictor_regression(
    dep_var: str,
    ind_var: str,
    conn: sqlite3.Connection,
    table_name: str,
    winsorize_limits: tuple[float, float] = (0.05, 0.95),
    alpha: float = 0.05,
) -> dict:
    """Run a single-variable OLS regression and return a compact result dict.

    Fits the model:  ``dep_var  ~  const + ind_var``

    A minimal SQL query is constructed that fetches only the two relevant
    columns, then the actual data-cleaning, winsorisation, and model-fitting
    are delegated to the existing :func:`Run_Model` function so that behaviour
    stays consistent with the main :func:`Regression` workflow.

    Args:
        dep_var: Name of the dependent variable column in *table_name*.
        ind_var: Name of the independent variable column in *table_name*.
        conn: An active SQLite connection.
        table_name: The table that contains both variables.
        winsorize_limits: ``(lower_quantile, upper_quantile)`` bounds used to
            trim extreme values before fitting.
        alpha: Significance threshold used to set the ``'is_significant'`` flag.

    Returns:
        A ``dict`` with the following keys:

        ==================  =====================================================
        ``dep_var``         dependent variable name
        ``ind_var``         independent variable name
        ``r_squared``       OLS R² (``None`` on failure)
        ``adj_r_squared``   Adjusted R² (``None`` on failure)
        ``n_obs``           number of observations used in the model
        ``coef``            coefficient of the independent variable
        ``p_value``         p-value of the independent variable coefficient
        ``is_significant``  ``True`` if *p_value* < *alpha*
        ``status``          ``'success'`` or ``'failed'``
        ==================  =====================================================
    """
    # Build a minimal SELECT that fetches only the two columns we need.
    # Run_Model will silently ignore the absent edinetCode / periodStart columns
    # because it drops them with errors='ignore'.
    query = f"""
    SELECT {dep_var}, {ind_var}
    FROM   {table_name}
    WHERE  {dep_var} IS NOT NULL
      AND  {ind_var} IS NOT NULL
    """

    # Delegate cleaning, winsorisation, constant addition, and OLS to Run_Model.
    results = Run_Model(
        query,
        conn,
        dep_var,
        [ind_var],
        winsorize_limits,
    )

    # Detect an empty / failed model by checking the observation count.
    n_obs = int(results.nobs) if results.nobs else 0

    if n_obs == 0:
        # Run_Model already prints the error; we return a 'failed' sentinel so
        # the caller can still record and skip this combination gracefully.
        return {
            "dep_var": dep_var,
            "ind_var": ind_var,
            "r_squared": None,
            "adj_r_squared": None,
            "n_obs": 0,
            "coef": None,
            "p_value": None,
            "is_significant": False,
            "status": "failed",
        }

    # results.pvalues and results.params are pandas Series indexed by variable name.
    # The independent variable's entry will be keyed exactly by ind_var.
    p_value = float(results.pvalues.get(ind_var, 1.0))
    coef = results.params.get(ind_var, None)

    return {
        "dep_var": dep_var,
        "ind_var": ind_var,
        "r_squared": float(results.rsquared),
        "adj_r_squared": float(results.rsquared_adj),
        "n_obs": n_obs,
        "coef": float(coef) if coef is not None else None,
        "p_value": p_value,
        "is_significant": p_value < alpha,
        "status": "success",
    }


def _rank_predictor_results(results: list[dict]) -> list[dict]:
    """Sort and rank a flat list of single-predictor regression result dicts.

    Sorting criteria applied in order:
        1. **R-squared descending** - higher R² means the predictor explains
           more variance in the dependent variable.
        2. **p-value ascending** - lower p-value provides stronger statistical
           evidence that the relationship is non-zero.

    Failed models (``status != 'success'`` or ``r_squared is None``) are
    appended at the end and assigned ``rank = None`` so they do not clutter
    the top of the output.

    Args:
        results: List of result dicts produced by
            :func:`_run_single_predictor_regression`.

    Returns:
        The same list reordered and annotated with a 1-based ``'rank'`` integer
        (or ``None`` for failed entries).
    """
    # Partition into successful and failed models
    successful = [
        r for r in results
        if r["status"] == "success" and r["r_squared"] is not None
    ]
    failed = [
        r for r in results
        if not (r["status"] == "success" and r["r_squared"] is not None)
    ]

    # Primary sort key: highest R² first; tiebreak: lowest p-value first
    successful.sort(
        key=lambda r: (
            -r["r_squared"],
            r["p_value"] if r["p_value"] is not None else 1.0,
        )
    )

    # Assign 1-based ranks to successful models
    for rank, r in enumerate(successful, start=1):
        r["rank"] = rank

    # Failed models get no rank
    for r in failed:
        r["rank"] = None

    return successful + failed


def _write_predictor_search_results(
    ranked_results: list[dict],
    output_file: str,
    conn: sqlite3.Connection,
    results_table_name: str,
    alpha: float = 0.05,
) -> None:
    """Write the ranked predictor search results to a text summary and a DB table.

    **File - Summary text file** (``output_file``)
        Contains a header with overall statistics only: alpha, total models
        evaluated, successful models, models with a significant predictor, and
        the name of the DB table where full results are stored.

    **Database - Results table** (``results_table_name``)
        Every successful model is appended (``if_exists='append'``) to
        *results_table_name* inside the same SQLite database used for the
        analysis.  The table is created automatically on the first run.
        Columns:
        ``rank``, ``dep_var``, ``ind_var``, ``r_squared``, ``adj_r_squared``,
        ``coef``, ``p_value``, ``significance``, ``n_obs``.

    Significance stars follow the convention:
        * ``***``  p < 0.001
        * ``**``   p < 0.01
        * ``*``    p < 0.05

    Args:
        ranked_results: The output of :func:`_rank_predictor_results`.
        output_file: Path to the summary text file.  The parent directory is
            created automatically if it does not exist.
        conn: The open SQLite connection used for the analysis.  Results are
            appended to *results_table_name* inside this same database.
        results_table_name: Name of the SQLite table to append results into
            (e.g. ``'Significant_Predictors'``).
        alpha: The significance level used during the regressions; written into
            the file header for traceability.
    """
    # ── Ensure the text-file output directory exists ──────────────────────────
    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # ── Pre-compute summary statistics ───────────────────────────────────────
    successful_results = [r for r in ranked_results if r["status"] == "success"]
    significant_count = sum(1 for r in successful_results if r.get("is_significant"))

    # =========================================================================
    # FILE - Summary text file: header stats only
    # =========================================================================
    with open(output_file, "w", encoding="utf-8") as f:

        f.write("=" * 80 + "\n")
        f.write("  SIGNIFICANT PREDICTOR SEARCH RESULTS\n")
        f.write("=" * 80 + "\n")
        f.write(f"  Significance level (alpha)  : {alpha}\n")
        f.write(f"  Total models evaluated      : {len(ranked_results)}\n")
        f.write(f"  Successful models           : {len(successful_results)}\n")
        f.write(f"  Models with sig. predictor  : {significant_count}\n")
        f.write(f"  Results DB table            : {results_table_name}\n")
        f.write("=" * 80 + "\n")

    print(f"Summary text written to      {output_file}")

    # =========================================================================
    # DATABASE - Append results to the SQLite results table.
    # =========================================================================

    # Build a DataFrame from successful models only; rank was already assigned
    # by _rank_predictor_results.  Failed models are excluded.
    db_rows = [
        {
            "rank": r["rank"],
            "dep_var": r["dep_var"],
            "ind_var": r["ind_var"],
            "r_squared": r["r_squared"],
            "adj_r_squared": r["adj_r_squared"],
            "coef": r["coef"],
            "p_value": r["p_value"],
            "significance": _significance_stars(r["p_value"]),
            "n_obs": r["n_obs"],
        }
        for r in ranked_results
        if r["status"] == "success"
    ]

    results_df = pd.DataFrame(db_rows, columns=[
        "rank", "dep_var", "ind_var",
        "r_squared", "adj_r_squared", "coef",
        "p_value", "significance", "n_obs",
    ])

    # if_exists='append' creates the table on the first run and appends on
    # subsequent runs, preserving any historical results already stored.
    # index=False prevents pandas from writing its own row-number column.
    results_df.to_sql(results_table_name, conn, if_exists="append", index=False)
    conn.commit()

    print(f"Results appended to DB table {results_table_name} ({len(results_df)} rows)")


def find_significant_predictors(
    db_path: str,
    table_name: str,
    results_table_name: str,
    output_file: str = "data/ols_results/predictor_search_results.txt",
    winsorize_limits: tuple[float, float] = (0.05, 0.95),
    alpha: float = 0.05,
    dependent_variables: list[str] | None = None,
) -> None:
    """Systematically test every single-predictor OLS regression in the ratios table.

    This is an automated exploratory step that fits one independent variable at
    a time against every possible dependent variable, so that the strongest
    *univariate* relationships in the dataset can be identified before building
    more complex multi-variable models.

    Pipeline
    --------
    1. **Discover variables** - query ``PRAGMA table_info`` on *table_name* to
       get all column names, then filter out identifier / metadata columns
       (``edinetCode``, ``docID``, ``docTypeCode``, ``periodStart``,
       ``periodEnd``).

    2. **Generate configs** - for every ordered pair ``(dep_var, ind_var)``
       where ``dep_var ≠ ind_var`` (O(n²) combinations), build a minimal SQL
       query: ``SELECT dep_var, ind_var FROM table WHERE … IS NOT NULL``.

    3. **Run models** - delegate each pair to :func:`Run_Model` (which handles
       winsorisation, constant addition, and OLS fitting).  Results are stored
       as dicts containing R², adjusted R², coefficient, p-value, and
       observation count.

    4. **Rank results** - sort all result dicts by R² descending, then by
       p-value ascending.  Failed models are moved to the end.

    5. **Store output** - call :func:`_write_predictor_search_results` to
       write a brief stats summary to *output_file* and append full results
       to *results_table_name* in the same SQLite database.

    Args:
        db_path: Filesystem path to the SQLite database (e.g. from
            ``DB_PATH`` in ``.env``).
        table_name: Name of the financial-ratios table to analyse
            (e.g. ``'Standard_Data_Ratios'``).
        results_table_name: Name of the SQLite table to append results into
            (e.g. ``'Significant_Predictors'`` from ``DB_SIGNIFICANT_PREDICTORS_TABLE``
            in ``.env``).  The table is created automatically if it does not
            exist yet.
        output_file: Path where the brief stats summary will be written.
            The parent directory is created automatically if absent.
        winsorize_limits: ``(lower_quantile, upper_quantile)`` pair passed to
            :func:`Run_Model` for outlier trimming.  Defaults match the main
            regression config.
        alpha: p-value threshold below which a predictor is flagged as
            statistically significant.
        dependent_variables: Optional explicit list of column names to use as
            dependent variables.  When ``None`` or empty, every eligible column
            in *table_name* is used as a dependent variable (default behaviour).
            Independent variables are always drawn from the full set of eligible
            columns regardless of this filter.

    Returns:
        ``None``.  Results are appended to *results_table_name* in the DB;
        a brief summary is written to *output_file*; progress is printed
        to stdout.

    Notes:
        - Runtime is O(n²) in the number of predictor columns.  A table with
          50 columns produces ~2 450 models; with 100 columns ~9 900.  Each
          model is fast (simple OLS), but wide tables may take several minutes.
        - Individual model failures are caught internally by :func:`Run_Model`
          and recorded as ``'failed'`` entries rather than aborting the search.
    """
    conn = sqlite3.connect(db_path)

    try:
        # ------------------------------------------------------------------
        # Step 1 - Discover all predictor-eligible columns in the ratios table.
        # ------------------------------------------------------------------
        all_variables = _get_predictor_columns(conn, table_name)

        if not all_variables:
            print(f"No predictor columns found in table '{table_name}'. Exiting.")
            return

        # If the caller supplied a non-empty dependent_variables list, restrict
        # the dep_var loop to only those columns.  Unknown column names are
        # warned about but silently skipped so a typo in the config does not
        # silently produce wrong results without any feedback.
        if dependent_variables:
            unknown = [v for v in dependent_variables if v not in all_variables]
            if unknown:
                print(
                    f"Warning: the following dependent_variables were not found "
                    f"in '{table_name}' and will be skipped: {unknown}"
                )
            dep_var_list = [v for v in dependent_variables if v in all_variables]
        else:
            # Default: use every eligible column as a potential dependent variable
            dep_var_list = all_variables

        if not dep_var_list:
            print("No valid dependent variables to search. Exiting.")
            return

        # Independent variables always come from the full eligible column set.
        # This means ind_var candidates are not restricted even when dep_var
        # is filtered, so we still test all possible predictors.
        total_pairs = len(dep_var_list) * (len(all_variables) - 1)
        print(
            f"Found {len(all_variables)} predictor variables in '{table_name}'. "
            f"Running {total_pairs} single-variable regressions "
            f"({len(dep_var_list)} dependent variable(s))..."
        )

        # ------------------------------------------------------------------
        # Steps 2 & 3 - For every (dep_var, ind_var) pair, run a regression
        # and collect the result dict.
        # ------------------------------------------------------------------
        all_results: list[dict] = []
        completed = 0

        for dep_var in dep_var_list:
            for ind_var in all_variables:

                # A variable cannot meaningfully predict itself
                if dep_var == ind_var:
                    continue

                result = _run_single_predictor_regression(
                    dep_var=dep_var,
                    ind_var=ind_var,
                    conn=conn,
                    table_name=table_name,
                    winsorize_limits=winsorize_limits,
                    alpha=alpha,
                )
                all_results.append(result)

                # Print a progress update every 100 completed models so the
                # user can track long-running searches
                completed += 1
                if completed % 100 == 0:
                    print(f"  Progress: {completed}/{total_pairs} models completed...")

        print(f"Finished running {len(all_results)} models.")

        # ------------------------------------------------------------------
        # Step 4 - Rank all results: R² descending, then p-value ascending.
        # ------------------------------------------------------------------
        ranked_results = _rank_predictor_results(all_results)

        significant_count = sum(1 for r in ranked_results if r.get("is_significant"))
        print(f"Models with a significant predictor (alpha={alpha}): {significant_count}")

        # ------------------------------------------------------------------
        # Step 5 - Write summary stats to the text file and append full
        #          results to the DB table.
        # ------------------------------------------------------------------
        _write_predictor_search_results(
            ranked_results,
            output_file,
            conn,
            results_table_name,
            alpha,
        )

    finally:
        # Always release the database connection, even if an error was raised
        conn.close()


def multivariate_regression(config: dict, db_path: str) -> None:
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

        db_path: Path to the SQLite database file.
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

    conn = sqlite3.connect(db_path)
    try:
        # Derive variable names without loading the full dataset.
        # Wrapping as a subquery handles any trailing ORDER BY / LIMIT in sql.
        peek = pd.read_sql_query(f"SELECT * FROM ({sql}) LIMIT 0", conn)
        dep_var = peek.columns[0]
        ind_vars = list(peek.columns[1:])

        results = Run_Model(sql, conn, dep_var, ind_vars, winsorize_limits)
        write_results_to_file(results, sql, output_file)
    finally:
        conn.close()