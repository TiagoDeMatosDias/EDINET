# importing modules and packages
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
    """    This function performs a regression analysis using data from a SQLite database, and configuration from a JSON file.
    It constructs a SQL query to retrieve financial ratios and earnings data, processes the data into a DataFrame,
    and fits an Ordinary Least Squares (OLS) regression model to predict the compound annual growth rate (CAGR) of earnings per share.
    The function also generates a summary of the regression results, including p-values for each variable, and saves the summary to a text file.
    It uses the statsmodels library for regression analysis and pandas for data manipulation.
    Returns:
        None
    """
    # --- 1. Read the JSON File ---
    config = load_config()


    # --- 2. Connect to the SQLite Database ---
    conn = sqlite3.connect(config["Database"])


    # --- 2. Define the SQL Query to retrieve data ---
    # This query retrieves the input data defined in the JSON file.

    # Dependent variable (current period)
    dependent_variable_sql_alias = config["DependentVariable"]["Formula"] + " AS " + config["DependentVariable"]["Name"]
    dependent_variable_sql = config["DependentVariable"]["Formula"] # Name as it will appear in DataFrame
    dependent_variable_df_name = config["DependentVariable"]["Name"] # Name as it will appear in DataFrame

    # Independent variables (prior periods)
    independent_variables_sql_aliases = [] # For the SELECT clause in SQL
    independent_variables_sql = []    # For use in Python (e.g., df.dropna, OLS model)
    independent_variables_df_names = []    # For use in Python (e.g., df.dropna, OLS model)


    # Generate Baseline From Clause 
    FromClause = ""
    for base_Table in config["DB_Tables"]:
        if FromClause == "":
            sql_alias_string = base_Table["Join"]+ base_Table["Name"]+ " AS "+ base_Table["Alias"]
        else:
            sql_alias_string = ", " + base_Table["Join"]+ base_Table["Name"]+ " AS "+ base_Table["Alias"]
        FromClause +=   sql_alias_string


    # Augment the baseline with joins for prior periods
    for i in range(0, config["NumberOfPeriods"] ):
        for DB_Table in config["DB_Tables"]:
            # SQL alias: e.g., s1.PerShare_Earnings AS PerShare_Earnings_S1
            sql_alias_string = " LEFT JOIN "+ DB_Table["Name"]+ " AS "+ DB_Table["Alias"] + "_"+ str(i)
            sql_alias_string += " ON "+ DB_Table["Alias"]+ ".edinetCode = "+ DB_Table["Alias"]+"_"+ str(i)+ ".edinetCode"
            if i == 0:
                sql_alias_string += " AND STRFTIME('%J', "+ DB_Table["Alias"]+ ".PeriodStart) - STRFTIME('%J', "+ DB_Table["Alias"]+ "_"+ str(i)+ ".PeriodEnd) >= 1"
                sql_alias_string += " AND STRFTIME('%J', "+ DB_Table["Alias"]+ ".PeriodStart) - STRFTIME('%J', "+ DB_Table["Alias"]+ "_"+ str(i)+ ".PeriodEnd) < 5"
            else:
                sql_alias_string += " AND STRFTIME('%J', "+ DB_Table["Alias"]+ "_"+ str(i-1)+ ".PeriodStart) - STRFTIME('%J', "+ DB_Table["Alias"]+ "_"+ str(i) + ".PeriodEnd) >= 1"
                sql_alias_string += " AND STRFTIME('%J', "+ DB_Table["Alias"]+ "_"+ str(i-1)+ ".PeriodStart) - STRFTIME('%J', "+ DB_Table["Alias"]+ "_"+ str(i) + ".PeriodEnd) < 5"
            FromClause += sql_alias_string

        for independent_variable in config["IndependentVariables"]:
            # SQL alias: e.g., s1.PerShare_Earnings AS PerShare_Earnings_S1
            if i == 0:
                sql_alias_string = independent_variable["Table_Alias"]+ "."+ independent_variable["Name"]+ " AS "+ independent_variable["Name"]
                name = independent_variable["Name"]
                SQL_Name = independent_variable["Table_Alias"]+ "."+independent_variable["Name"]
            else:
                sql_alias_string = independent_variable["Table_Alias"]+ "_"+ str(i)+ "."+ independent_variable["Name"]+ " AS "+ independent_variable["Name"]+ "_"+ str(i)
                name = independent_variable["Name"]
                SQL_Name = independent_variable["Table_Alias"]+ "_"+ str(i)+ "."+ independent_variable["Name"]

            independent_variables_sql_aliases.append(sql_alias_string)
            independent_variables_df_names.append(name)
            independent_variables_sql.append(SQL_Name)


    # --- 3. Construct the full SELECT clause for the SQL query ---
    # Start with edinetCode and PeriodStart, then dependent variable, then independent variables
    input_Columns_sql = f"s.edinetCode, s.PeriodStart, {dependent_variable_sql_alias}"
    input_Columns_sql += ", " + ", ".join(independent_variables_sql_aliases)

    # --- 4. Construct the WHERE clause for non-null data ---
    # This now uses the DataFrame column names for consistency with Python processing
    # and assumes you want to filter out rows where the *aliased* columns are NULL.
    # Remember to also include the dependent variable in the non-null check.
    all_cols_for_null_check = [dependent_variable_sql] + independent_variables_sql
    non_null_conditions_df_names = [f'{col_name} IS NOT NULL' for col_name in all_cols_for_null_check]
    non_null_where_clause = " AND ".join(non_null_conditions_df_names)

    # --- 6. Assemble the final SQL Query ---
    Query = f"""
    SELECT {input_Columns_sql}
    FROM {FromClause}
    WHERE {non_null_where_clause}
    """
    
    results = Run_Model(Query, conn, dependent_variable_df_name, independent_variables_df_names)

    # Example usage:
    output_file = "files/ols_results/ols_results_summary.txt"
    write_results_to_file(results, Query, output_file)

def Run_Model(Query, conn, dependent_variable_df_name, independent_variables_df_names):
    """Runs the regression model and returns the results."""
    try:
        # --- 7. Execute the SQL Query and process the results ---
        df_d = pd.read_sql_query(Query, conn)
        df = df_d
        df_cleaned = df.replace([np.inf, -np.inf], np.nan)
        df_cleaned = df_cleaned.dropna(subset=[dependent_variable_df_name] + independent_variables_df_names)

        y_cleaned = df_cleaned[dependent_variable_df_name]
        X_cleaned = df_cleaned[independent_variables_df_names]
        X_cleaned = sm.add_constant(X_cleaned)  # Remember to add constant AFTER dropping NaNs

        # --- 4. Fit the OLS model ---
        model = sm.OLS(y_cleaned, X_cleaned)
        results = model.fit()
        return results

    except Exception as e:
        print(f"An error occurred: {e}")
        # Explicitly return an empty model with NaN values to ensure results are always returned
        empty_model = sm.OLS(pd.Series(dtype=float), pd.DataFrame(dtype=float)).fit()
        return empty_model


def write_results_to_file(results, query, output_file):
    """Writes the regression results and significant variables to a file."""
    # Get the p-values
    p_values = results.pvalues

    result_content = ""
    result_content += "\nP-values for each variable:\n"
    result_content += str(p_values)

    # Define significance level (alpha)
    alpha = 0.05

    # Identify significant variables
    significant_variables = p_values[p_values < alpha].index.tolist()

    result_content += f"\nVariables significant at the {alpha * 100}% level:"
    if significant_variables:
        for var in significant_variables:
            value = np.float64(p_values[var])
            value = value[0] if isinstance(value, np.ndarray) else value
            result_content += f"\n- {var} (P-value: {value})"
    else:
        result_content += "\nNo variables are significant at this level."

    # Exclude 'const' if only predictors are needed
    significant_predictors = [var for var in significant_variables if var != 'const']
    result_content += f"\nSignificant predictor variables at the {alpha * 100}% level:"
    if significant_predictors:
        for var in significant_predictors:
            value = np.float64(p_values[var])
            value = value[0] if isinstance(value, np.ndarray) else value
            result_content += f"\n- {var} (P-value: {value})"
    else:
        result_content += "\nNo predictor variables are significant at this level."

    # Write results to file
    with open(output_file, 'w') as f:
        f.write(query + "\n" + str(results.summary()) + result_content)
    print(f"\nOLS results summary written to {output_file}")

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    Regression()
    