# importing modules and packages
from __future__ import annotations

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn import preprocessing
import statsmodels.api as sm
import sqlite3
import json as json






def load_config():
    """Loads configuration from a JSON file."""
    try:
        with open("config_Regression.json", "r") as file:
            config = json.load(file)
    except FileNotFoundError:
        config = {}  # Default to empty if file not found
    return config


def Regression():
    """
    Perform regression analysis using data from a SQLite database and configuration from a JSON file.

    Steps:
    1. Load configuration from JSON file.
    2. Connect to the SQLite database.
    3. Generate SQL query based on configuration.
    4. Run regression model using the generated query.
    5. Write regression results to a file.

    Returns:
        None
    """
    # Load configuration
    config = load_config()

    # Connect to the SQLite database
    conn = sqlite3.connect(config["Database"])

    # Generate SQL query based on configuration
    generated_query = Generate_SQL_Query(config)

    # Run regression model
    results = Run_Model(
        generated_query["Query"],
        conn,
        generated_query["DependentVariable"],
        generated_query["IndependentVariables"]
    )

    # Write regression results to a file
    output_file = "files/ols_results/ols_results_summary.txt"
    write_results_to_file(results, generated_query["Query"], output_file)

def Run_Model(
    Query: str,
    conn: sqlite3.Connection,
    dependent_variable_df_name: str,
    independent_variables_df_names: list[str],
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

        # --- Prepare data for OLS regression ---
        y_cleaned = df_cleaned[dependent_variable_df_name]
        X_cleaned = df_cleaned[independent_variables_df_names]
        X_cleaned = sm.add_constant(X_cleaned)  # Add a constant (intercept)

        # --- Fit the OLS model ---
        model = sm.OLS(y_cleaned, X_cleaned)
        results = model.fit()
        return results

    except Exception as e:
        print(f"An error occurred during regression model execution: {e}")
        # In case of any error, return an empty, fitted model. This ensures
        # that the function always returns a results object, which can
        # prevent crashes in subsequent code that expects one.
        empty_model = sm.OLS(pd.Series(dtype=float), pd.DataFrame(dtype=float)).fit()
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
        f.write("--- SQL Query ---\n")
        f.write(query.strip() + "\n\n")

        # --- Write OLS Summary ---
        f.write("--- OLS Regression Results ---\n")
        f.write(str(results.summary()) + "\n\n")

        # --- Write Significance Analysis ---
        f.write("--- Significance Analysis ---\n")
        f.write(f"Significance level (alpha): {alpha}\n\n")

        # All significant variables (including constant)
        f.write(f"Variables significant at the {alpha:.1%} level:\n")
        if not significant_variables.empty:
            for var, p_val in significant_variables.items():
                f.write(f"- {var} (P-value: {p_val:.4g})\n")
        else:
            f.write("No variables are significant at this level.\n")
        f.write("\n")

        # Significant predictor variables (excluding constant)
        significant_predictors = significant_variables.drop("const", errors="ignore")
        f.write(f"Predictor variables significant at the {alpha:.1%} level:\n")
        if not significant_predictors.empty:
            for var, p_val in significant_predictors.items():
                f.write(f"- {var} (P-value: {p_val:.4g})\n")
        else:
            f.write("No predictor variables are significant at this level.\n")

    print(f"\nOLS results summary written to {output_file}")



def Generate_SQL_Query(config: dict) -> dict:
    """
    Dynamically generates an SQL query for a time-series regression model.

    This function constructs a query by joining a table to itself multiple
    times to create lagged variables for the independent features, based on
    the provided configuration.

    Args:
        config: A dictionary containing the configuration for the regression,
                including dependent/independent variables, tables, and the
                number of historical periods to include.

    Returns:
        A dictionary containing the generated 'Query', the 'DependentVariable'
        name, and a list of 'IndependentVariables' names.
    """
    # --- 1. Extract configuration details ---
    dep_var_config = config["DependentVariable"]
    ind_vars_config = config["IndependentVariables"]
    db_tables_config = config["DB_Tables"]
    num_periods = config["NumberOfPeriods"]

    # --- 2. Define variables for the query components ---
    dependent_variable_df_name = dep_var_config["Name"]
    dependent_variable_sql = dep_var_config["Formula"]
    dependent_variable_select = f"{dependent_variable_sql} AS {dependent_variable_df_name}"

    # Lists to store parts of the query for independent variables
    select_aliases = []
    where_conditions = [f"{dependent_variable_sql} IS NOT NULL"]
    df_column_names = []

    # --- 3. Build the FROM and JOIN clauses for lagged data ---
    from_clause = _build_from_clause(db_tables_config, num_periods)

    # --- 4. Build SELECT and WHERE clauses for independent variables ---
    for i in range(num_periods):
        for ind_var in ind_vars_config:
            table_alias = f"{ind_var['Table_Alias']}_{i}"
            col_name = ind_var["Name"]
            df_col_name = f"{col_name}_{i}"
            
            select_aliases.append(f"{table_alias}.{col_name} AS {df_col_name}")
            where_conditions.append(f"{table_alias}.{col_name} IS NOT NULL")
            df_column_names.append(df_col_name)

    # --- 5. Assemble the final query ---
    base_table_alias = db_tables_config[0]["Alias"]
    # Create a list of all columns for the SELECT clause. This avoids a
    # trailing comma if `select_aliases` is empty.
    all_select_parts = [
        f"{base_table_alias}.edinetCode",
        f"{base_table_alias}.PeriodStart",
        dependent_variable_select,
    ] + select_aliases
    all_selects = ", ".join(all_select_parts)

    where_clause = " AND ".join(where_conditions)

    query = f"""
    SELECT {all_selects}
    FROM {from_clause}
    WHERE {where_clause}
    """

    return {
        "Query": query,
        "DependentVariable": dependent_variable_df_name,
        "IndependentVariables": df_column_names,
    }


def _build_from_clause(db_tables_config: list[dict], num_periods: int) -> str:
    """Helper to build the FROM and JOIN part of the SQL query."""
    
    # Start with the base table for the current period
    base_table = db_tables_config[0]
    from_clause = f"{base_table['Name']} AS {base_table['Alias']}"

    # Join for each historical period
    for i in range(num_periods):
        current_alias = f"{base_table['Alias']}" if i == 0 else f"{base_table['Alias']}_{i-1}"
        lagged_alias = f"{base_table['Alias']}_{i}"
        
        join_condition = (
            f"ON {current_alias}.edinetCode = {lagged_alias}.edinetCode "
            f"AND STRFTIME('%J', {current_alias}.PeriodStart) - STRFTIME('%J', {lagged_alias}.PeriodEnd) BETWEEN 1 AND 5" # Approx 1 to 5 years
        )
        
        from_clause += f" LEFT JOIN {base_table['Name']} AS {lagged_alias} {join_condition}"
        
    return from_clause


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    Regression()
    