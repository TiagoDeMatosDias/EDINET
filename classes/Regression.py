# importing modules and packages
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn import preprocessing
import statsmodels.api as sm
import sqlite3


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    conn = sqlite3.connect("D:\\programming\\EDINET\\dummy_database.db")

    
    # --- 1. Define base column names that appear in s1, s2, s3 ---
    # This makes it easier to generate aliases programmatically
    base_columns = [
        'Ratio_netIncome_Growth',
        'Ratio_EarningsYield',
        'MarketCap',
        'Ratio_GrossMargin',
        'Ratio_OperatingMargin',
        'Ratio_NetProfitMargin',
        'Ratio_ReturnOnEquity',
        'Ratio_ReturnOnAssets',
        'Ratio_QuickRatio',
    ]

    # --- 2. Define your dependent and independent variables with unique aliases ---

    # Dependent variable (current period)
    dependent_variable_sql_alias = 'EXP((1.0/5.0) * LOG(s.PerShare_Earnings / s5.PerShare_Earnings)) - 1 AS CAGR_PerShare_Earnings'
    dependent_variable_df_name = 'CAGR_PerShare_Earnings' # Name as it will appear in DataFrame

    # Independent variables (prior periods)
    independent_variables_sql_aliases = [] # For the SELECT clause in SQL
    independent_variables_df_names = []    # For use in Python (e.g., df.dropna, OLS model)

    # Generate aliases for s1, s2, s3
    for alias_prefix in ['s1', 's2', 's3', 's4', 's5']:
        for col_name in base_columns:
            # SQL alias: e.g., s1.PerShare_Earnings AS PerShare_Earnings_S1
            sql_alias_string = f"{alias_prefix}.{col_name} AS {col_name}_{alias_prefix.upper()}"
            independent_variables_sql_aliases.append(sql_alias_string)

            # DataFrame column name: e.g., PerShare_Earnings_S1
            df_column_name = f"{col_name}_{alias_prefix.upper()}"
            independent_variables_df_names.append(df_column_name)

    # --- 3. Construct the full SELECT clause for the SQL query ---
    # Start with edinetCode and PeriodStart, then dependent variable, then independent variables
    input_Columns_sql = f"s.edinetCode, s.PeriodStart, {dependent_variable_sql_alias}"
    input_Columns_sql += ", " + ", ".join(independent_variables_sql_aliases)

    # --- 4. Construct the WHERE clause for non-null data ---
    # This now uses the DataFrame column names for consistency with Python processing
    # and assumes you want to filter out rows where the *aliased* columns are NULL.
    # Remember to also include the dependent variable in the non-null check.
    all_cols_for_null_check = [dependent_variable_df_name] + independent_variables_df_names
    non_null_conditions_df_names = [f'"{col_name}" IS NOT NULL' for col_name in all_cols_for_null_check]
    non_null_where_clause = " AND ".join(non_null_conditions_df_names)


    # --- 5. Define the FROM and JOIN clauses (unchanged from your original) ---
    input_table_sql = """
    Standard_Data_Ratios s
    LEFT JOIN Standard_Data_Ratios s1 ON s.edinetCode = s1.edinetCode
        AND STRFTIME('%J', s.PeriodStart) - STRFTIME('%J', s1.PeriodEnd) >= 1
        AND STRFTIME('%J', s.PeriodStart) - STRFTIME('%J', s1.PeriodEnd) <= 5
    LEFT JOIN Standard_Data_Ratios s2 ON s.edinetCode = s2.edinetCode
        AND STRFTIME('%J', s1.PeriodStart) - STRFTIME('%J', s2.PeriodEnd) >= 1
        AND STRFTIME('%J', s1.PeriodStart) - STRFTIME('%J', s2.PeriodEnd) <= 5
    LEFT JOIN Standard_Data_Ratios s3 ON s.edinetCode = s3.edinetCode
        AND STRFTIME('%J', s2.PeriodStart) - STRFTIME('%J', s3.PeriodEnd) >= 1
        AND STRFTIME('%J', s2.PeriodStart) - STRFTIME('%J', s3.PeriodEnd) <= 5
    LEFT JOIN Standard_Data_Ratios s4 ON s.edinetCode = s4.edinetCode AND 
                            STRFTIME('%J', s3.PeriodStart) - STRFTIME('%J', s4.PeriodEnd) >= 1 AND 
                            STRFTIME('%J', s3.PeriodStart) - STRFTIME('%J', s4.PeriodEnd) <= 5
    LEFT JOIN Standard_Data_Ratios s5 ON s.edinetCode = s5.edinetCode AND 
                            STRFTIME('%J', s4.PeriodStart) - STRFTIME('%J', s5.PeriodEnd) >= 1 AND 
                            STRFTIME('%J', s4.PeriodStart) - STRFTIME('%J', s5.PeriodEnd) <= 5
    """

    # --- 6. Assemble the final SQL Query ---
    Query = f"""
    SELECT {input_Columns_sql}
    FROM {input_table_sql}
    WHERE {non_null_where_clause}
    """

    print("--- Generated SQL Query ---")
    print(Query)

    df_d = pd.read_sql_query(Query, conn)
    df = df_d
    df_cleaned = df.replace([np.inf, -np.inf], np.nan)
    df_cleaned = df_cleaned.dropna(subset=[dependent_variable_df_name] + independent_variables_df_names)
    print(df_cleaned.isin([np.inf, -np.inf]).sum())

    y_cleaned = df_cleaned[dependent_variable_df_name]
    X_cleaned = df_cleaned[independent_variables_df_names]
    X_cleaned = sm.add_constant(X_cleaned) # Remember to add constant AFTER dropping NaNs

    # --- 4. Fit the OLS model ---
    model = sm.OLS(y_cleaned, X_cleaned)
    results = model.fit()


    # --- 5. Get the OLS summary table ---
    print(results.summary())
    
    # --- 6. Write the results to a file ---
    results_summary_file = "files/ols_results/ols_results_summary.txt"
    with open(results_summary_file, 'w') as f:
        f.write(str(results.summary()))
    print(f"\nOLS results summary written to {results_summary_file}")

    # Assuming you have your 'results' object from statsmodels

    # Get the p-values
    p_values = results.pvalues
    print("\nP-values for each variable:")
    print(p_values)

    # Define your significance level (alpha)
    alpha = 0.05

    # Identify significant variables
    significant_variables = p_values[p_values < alpha].index.tolist()

    print(f"\nVariables significant at the {alpha*100}% level:")
    if significant_variables:
        for var in significant_variables:
            print(f"- {var} (P-value: {p_values[var]:.4f})")
    else:
        print("No variables are significant at this level.")

    # You might also want to exclude the 'const' (intercept) if you only care about predictors
    significant_predictors = [var for var in significant_variables if var != 'const']
    print(f"\nSignificant predictor variables at the {alpha*100}% level:")
    if significant_predictors:
        for var in significant_predictors:
            print(f"- {var} (P-value: {p_values[var]:.4f})")
    else:
        print("No predictor variables are significant at this level.")