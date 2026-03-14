import config as c
import logging
import pandas as pd
import sqlite3
import xml.etree.ElementTree as ET
import random
import numpy as np
import json
import os

logger = logging.getLogger(__name__)

class data:
    def __init__(self):
        self.config = c.Config()
        self.DB_PATH = self.config.get("DB_PATH")
        self.FINANCIAL_RATIOS_CONFIG_PATH = self.config.get("FINANCIAL_RATIOS_CONFIG_PATH")

    def _table_exists(self, conn, table_name):
        """Return True if *table_name* exists in the SQLite database."""
        cur = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return cur.fetchone() is not None

    def _resolve_table_name_in_schema(self, conn, schema_name, table_name):
        """Return actual table name in schema using case-insensitive match, else None."""
        if schema_name == "main":
            sql = (
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND lower(name)=lower(?) LIMIT 1"
            )
        else:
            sql = (
                f"SELECT name FROM {self._sql_ident(schema_name)}.sqlite_master "
                "WHERE type='table' AND lower(name)=lower(?) LIMIT 1"
            )
        row = conn.execute(sql, (table_name,)).fetchone()
        return row[0] if row else None

    def _sql_ident(self, name):
        """Safely quote an SQLite identifier (table/column/schema name)."""
        return '"' + str(name).replace('"', '""') + '"'

    def _sql_literal(self, value):
        """Safely quote a SQL literal value."""
        return "'" + str(value).replace("'", "''") + "'"

    def _load_financial_statement_mappings(self, mappings_config_path):
        """Load mappings config and normalize into {table: {column: mapping_dict}}."""
        with open(mappings_config_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        mappings = raw.get("Mappings", [])
        normalized = {
            "FinancialStatements": {},
            "IncomeStatement": {},
            "BalanceSheet": {},
            "CashflowStatement": {},
        }

        for entry in mappings:
            if not isinstance(entry, dict):
                continue
            table = entry.get("Table")
            name = entry.get("Name")
            if table not in normalized or not name:
                continue
            normalized[table][name] = {
                "Terms": entry.get("Terms", []) or [],
                "periods": entry.get("periods", []) or [],
            }

        return normalized

    def _collect_financial_statement_filters(self, mappings):
        """Return the union of relevant terms/periods across statement mappings."""
        relevant_terms = set()
        relevant_periods = set()
        has_unrestricted_periods = False

        for table_mappings in mappings.values():
            for mapping in table_mappings.values():
                if not isinstance(mapping, dict):
                    continue

                relevant_terms.update(t for t in (mapping.get("Terms", []) or []) if t)

                periods = [p for p in (mapping.get("periods", []) or []) if p]
                if periods:
                    relevant_periods.update(periods)
                else:
                    has_unrestricted_periods = True

        return {
            "terms": sorted(relevant_terms),
            "periods": sorted(relevant_periods),
            "has_unrestricted_periods": has_unrestricted_periods,
        }

    def _build_source_relevance_predicate(self, source_alias, filters, col_names=None):
        """Build SQL predicate limiting rows to mapped accounting terms/periods."""
        col_names = col_names or {}
        col_at = col_names.get("AccountingTerm", "AccountingTerm")
        col_period = col_names.get("Period", "Period")

        terms = filters.get("terms", []) if filters else []
        periods = filters.get("periods", []) if filters else []
        has_unrestricted_periods = bool(filters.get("has_unrestricted_periods")) if filters else False

        if not terms:
            return "1=1"

        term_list = ", ".join(self._sql_literal(term) for term in terms)
        conditions = [f"{source_alias}.{self._sql_ident(col_at)} IN ({term_list})"]

        if periods and not has_unrestricted_periods:
            period_list = ", ".join(self._sql_literal(period) for period in periods)
            conditions.append(f"{source_alias}.{self._sql_ident(col_period)} IN ({period_list})")

        return " AND ".join(conditions)

    def _resolve_source_col_names(self, conn, schema_name, table_name):
        """Detect the actual column names for AccountingTerm, Period, Amount in the source table.

        When the source table uses the raw EDINET column names (e.g. financialdata_full uses
        Japanese names like '要素ID' instead of 'AccountingTerm'), this method returns the
        actual names so SQL expressions can reference them correctly.

        Returns a dict mapping standard names to the actual names used in the table.
        Falls back to the standard names if the table cannot be introspected.
        """
        try:
            if schema_name == "main":
                rows = conn.execute(
                    f"PRAGMA table_info({self._sql_ident(table_name)})"
                ).fetchall()
            else:
                rows = conn.execute(
                    f"PRAGMA {self._sql_ident(schema_name)}.table_info({self._sql_ident(table_name)})"
                ).fetchall()
        except Exception:
            rows = []

        table_cols = {row[1] for row in rows}

        # Fixed mapping: alternative (e.g. raw EDINET Japanese) name → standard name.
        # Keys are the column names used in tables like financialdata_full;
        # values are the project-standard names used everywhere else.
        ALTERNATIVE_TO_STANDARD = {
            "要素ID":      "AccountingTerm",
            "コンテキストID": "Period",
            "ユニットID":   "Currency",
            "値":          "Amount",
        }

        # Build reverse: standard name → alternative name
        reverse_map = {v: k for k, v in ALTERNATIVE_TO_STANDARD.items()}

        result = {}
        for standard_name in ("AccountingTerm", "Period", "Amount"):
            if standard_name in table_cols:
                result[standard_name] = standard_name
            elif standard_name in reverse_map and reverse_map[standard_name] in table_cols:
                result[standard_name] = reverse_map[standard_name]
            else:
                result[standard_name] = standard_name  # fallback to standard

        return result

    def _build_amount_case_expr(self, mapping, source_alias="s",
                               col_accounting_term="AccountingTerm",
                               col_period="Period", col_amount="Amount"):
        """Build MAX(CASE WHEN ... THEN CAST(Amount AS REAL) END) SQL expression.

        Args:
            mapping: Mapping dict with 'Terms' and 'periods' keys.
            source_alias: SQL alias for the source table.
            col_accounting_term: Actual column name for AccountingTerm in the source table.
            col_period: Actual column name for Period in the source table.
            col_amount: Actual column name for Amount in the source table.
        """
        if not mapping:
            return "NULL"

        terms = mapping.get("Terms", []) or []
        periods = mapping.get("periods", []) or []
        if not terms:
            return "NULL"

        term_list = ", ".join(self._sql_literal(t) for t in terms)
        conditions = [f"{source_alias}.{self._sql_ident(col_accounting_term)} IN ({term_list})"]

        if periods:
            period_list = ", ".join(self._sql_literal(p) for p in periods)
            conditions.append(f"{source_alias}.{self._sql_ident(col_period)} IN ({period_list})")

        condition_sql = " AND ".join(conditions)
        return (
            f"MAX(CASE WHEN {condition_sql} "
            f"THEN CAST({source_alias}.{self._sql_ident(col_amount)} AS REAL) END)"
        )

    def _create_financial_statement_tables(self, conn):
        """Create target financial statement tables and docID uniqueness indexes."""
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS FinancialStatements (
              edinetCode TEXT,
              docID TEXT,
              docTypeCode TEXT,
              periodStart TEXT,
              periodEnd TIMESTAMP,
              SharesOutstanding REAL,
              SharePrice REAL
            );

            CREATE TABLE IF NOT EXISTS IncomeStatement (
              docID TEXT,
              netSales REAL,
              costOfSales REAL,
              grossProfit REAL,
              operatingIncome REAL,
              incomeBeforeTaxes REAL,
              incomeTaxes REAL,
              netIncome REAL
            );

            CREATE TABLE IF NOT EXISTS BalanceSheet (
              docID TEXT,
              cash REAL,
              inventories REAL,
              currentAssets REAL,
              ppe REAL,
              intangibleAssets REAL,
              totalAssets REAL,
              shareholdersEquity REAL,
              currentLiabilities REAL,
              NonCurrentLiabilities REAL,
              LongTermDebt REAL,
              TotalLiabilities REAL
            );

            CREATE TABLE IF NOT EXISTS CashflowStatement (
              docID TEXT,
              operatingCashflow REAL,
              depreciation REAL,
              cashflowInventories REAL,
              investmentCashflow REAL,
              capex REAL,
              financingCashflow REAL,
              dividends REAL,
              buybacks REAL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS ux_fs_docid ON FinancialStatements(docID);
            CREATE UNIQUE INDEX IF NOT EXISTS ux_is_docid ON IncomeStatement(docID);
            CREATE UNIQUE INDEX IF NOT EXISTS ux_bs_docid ON BalanceSheet(docID);
            CREATE UNIQUE INDEX IF NOT EXISTS ux_cf_docid ON CashflowStatement(docID);
            """
        )

    def _insert_base_financial_statements(
                self,
                conn,
                source_ref,
                temp_docids,
                mappings,
            company_ref,
            prices_ref,
            col_names=None,
            relevance_predicate="1=1",
        ):
                """Insert/update FinancialStatements base rows for the current docID batch."""
                col_names = col_names or {}
                col_at = col_names.get("AccountingTerm", "AccountingTerm")
                col_period = col_names.get("Period", "Period")
                col_amount = col_names.get("Amount", "Amount")
                fs_map = mappings.get("FinancialStatements", {})
                shares_expr = self._build_amount_case_expr(
                    fs_map.get("SharesOutstanding"),
                    col_accounting_term=col_at,
                    col_period=col_period,
                    col_amount=col_amount,
                )

                sql = f"""
                WITH base AS (
                        SELECT
                            MAX(s.edinetCode) AS edinetCode,
                            s.docID AS docID,
                            MAX(s.docTypeCode) AS docTypeCode,
                            MIN(s.periodStart) AS periodStart,
                            MAX(s.periodEnd) AS periodEnd,
                            {shares_expr} AS SharesOutstanding
                        FROM {source_ref} s
                        INNER JOIN {temp_docids} t ON t.docID = s.docID
                        WHERE {relevance_predicate}
                        GROUP BY s.docID
                )
                INSERT OR REPLACE INTO FinancialStatements
                    (edinetCode, docID, docTypeCode, periodStart, periodEnd, SharesOutstanding, SharePrice)
                SELECT
                    b.edinetCode,
                    b.docID,
                    b.docTypeCode,
                    b.periodStart,
                    b.periodEnd,
                    b.SharesOutstanding,
                    (
                        SELECT sp.Price
                        FROM {prices_ref} sp
                        JOIN {company_ref} c ON c.Company_Ticker = sp.Ticker
                        WHERE c.EdinetCode = b.edinetCode
                            AND sp.Date <= b.periodEnd
                        ORDER BY sp.Date DESC
                        LIMIT 1
                    ) AS SharePrice
                FROM base b
                """
                conn.execute(sql)

    def _insert_statement_table_rows(self, conn, source_ref, temp_docids, table_name, ordered_columns, mappings, col_names=None, relevance_predicate="1=1"):
        """Insert/update one statement table from config mappings for current docID batch."""
        col_names = col_names or {}
        col_at = col_names.get("AccountingTerm", "AccountingTerm")
        col_period = col_names.get("Period", "Period")
        col_amount = col_names.get("Amount", "Amount")
        table_mappings = mappings.get(table_name, {})

        select_exprs = ["s.docID AS docID"]
        for col in ordered_columns:
            expr = self._build_amount_case_expr(
                table_mappings.get(col),
                col_accounting_term=col_at,
                col_period=col_period,
                col_amount=col_amount,
            )
            select_exprs.append(f"{expr} AS {self._sql_ident(col)}")

        col_list = ", ".join([self._sql_ident("docID")] + [self._sql_ident(c) for c in ordered_columns])
        sql = f"""
        INSERT OR REPLACE INTO {self._sql_ident(table_name)} ({col_list})
        SELECT
          {", ".join(select_exprs)}
        FROM {source_ref} s
        INNER JOIN {temp_docids} t ON t.docID = s.docID
                WHERE {relevance_predicate}
        GROUP BY s.docID
        """
        conn.execute(sql)

    def generate_financial_statements(
        self,
        source_database,
        source_table,
        target_database,
        mappings_config,
        company_table=None,
        prices_table=None,
        overwrite=False,
        batch_size=2500,
    ):
        """Generate normalized financial-statement tables from standardized records.

        Supports source and target DB separation by attaching the source
        database when needed. Processing is resumable: only missing/partial
        docIDs are selected, and each chunk is committed atomically.
        """
        cfg = getattr(self, "config", None)
        source_db = source_database or self.DB_PATH
        target_db = target_database or self.DB_PATH
        source_tbl = source_table or (cfg.get("DB_STANDARDIZED_TABLE") if cfg else None) or "Standard_Data"
        company_tbl = company_table or (cfg.get("DB_COMPANY_INFO_TABLE") if cfg else None) or "companyInfo"
        prices_tbl = prices_table or (cfg.get("DB_STOCK_PRICES_TABLE") if cfg else None) or "stock_prices"
        mappings_path = mappings_config
        if not mappings_path:
            raise ValueError("Mappings_Config is required for generate_financial_statements.")

        batch_size = max(int(batch_size or 2500), 1)
        mappings = self._load_financial_statement_mappings(mappings_path)
        filter_config = self._collect_financial_statement_filters(mappings)

        same_db = os.path.abspath(source_db) == os.path.abspath(target_db)
        conn = sqlite3.connect(target_db)
        try:
            conn.execute("PRAGMA busy_timeout = 30000")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")

            source_schema = "main"
            if not same_db:
                conn.execute("ATTACH DATABASE ? AS src", (source_db,))
                source_schema = "src"

            source_ref = f"{self._sql_ident(source_schema)}.{self._sql_ident(source_tbl)}"

            # Detect actual column names (handles financialdata_full Japanese names)
            col_names = self._resolve_source_col_names(conn, source_schema, source_tbl)
            if any(v != k for k, v in col_names.items()):
                logger.info(
                    "generate_financial_statements: source table uses non-standard column names %s",
                    col_names,
                )

            relevance_predicate = self._build_source_relevance_predicate(
                "s",
                filter_config,
                col_names=col_names,
            )

            if overwrite:
                logger.info("Overwrite enabled - resetting financial statement tables.")
                conn.executescript(
                    """
                    DROP TABLE IF EXISTS FinancialStatements;
                    DROP TABLE IF EXISTS IncomeStatement;
                    DROP TABLE IF EXISTS BalanceSheet;
                    DROP TABLE IF EXISTS CashflowStatement;
                    """
                )

            self._create_financial_statement_tables(conn)

            # Resolve company/prices tables with fallback to source DB (if attached)
            company_in_main = self._resolve_table_name_in_schema(conn, "main", company_tbl)
            prices_in_main = self._resolve_table_name_in_schema(conn, "main", prices_tbl)

            company_schema = "main"
            prices_schema = "main"
            company_actual = company_in_main
            prices_actual = prices_in_main

            if not company_actual and source_schema != "main":
                company_actual = self._resolve_table_name_in_schema(conn, source_schema, company_tbl)
                if company_actual:
                    company_schema = source_schema

            if not prices_actual and source_schema != "main":
                prices_actual = self._resolve_table_name_in_schema(conn, source_schema, prices_tbl)
                if prices_actual:
                    prices_schema = source_schema

            if not company_actual:
                raise RuntimeError(
                    f"Company table '{company_tbl}' not found in target or source database; "
                    "required for SharePrice lookup."
                )
            if not prices_actual:
                raise RuntimeError(
                    f"Stock prices table '{prices_tbl}' not found in target or source database; "
                    "required for SharePrice lookup."
                )

            company_ref = f"{self._sql_ident(company_schema)}.{self._sql_ident(company_actual)}"
            prices_ref = f"{self._sql_ident(prices_schema)}.{self._sql_ident(prices_actual)}"

            temp_docids = self._sql_ident("_tmp_fs_docids")
            conn.execute(f"CREATE TEMP TABLE IF NOT EXISTS {temp_docids} (docID TEXT PRIMARY KEY)")

            pending_sql = f"""
            SELECT DISTINCT s.docID
            FROM {source_ref} s
            LEFT JOIN FinancialStatements fs ON fs.docID = s.docID
            LEFT JOIN IncomeStatement is1 ON is1.docID = s.docID
            LEFT JOIN BalanceSheet bs ON bs.docID = s.docID
            LEFT JOIN CashflowStatement cs ON cs.docID = s.docID
            WHERE s.docID IS NOT NULL
                            AND {relevance_predicate}
              AND (fs.docID IS NULL OR is1.docID IS NULL OR bs.docID IS NULL OR cs.docID IS NULL)
            ORDER BY s.docID
            """

            doc_cursor = conn.execute(pending_sql)
            total_docs = 0
            while True:
                batch = [row[0] for row in doc_cursor.fetchmany(batch_size)]
                if not batch:
                    break

                with conn:
                    conn.execute(f"DELETE FROM {temp_docids}")
                    conn.executemany(
                        f"INSERT OR IGNORE INTO {temp_docids}(docID) VALUES (?)",
                        [(d,) for d in batch],
                    )

                    self._insert_base_financial_statements(
                        conn,
                        source_ref,
                        temp_docids,
                        mappings,
                        company_ref,
                        prices_ref,
                        col_names=col_names,
                        relevance_predicate=relevance_predicate,
                    )

                    self._insert_statement_table_rows(
                        conn,
                        source_ref,
                        temp_docids,
                        "IncomeStatement",
                        [
                            "netSales", "costOfSales", "grossProfit", "operatingIncome",
                            "incomeBeforeTaxes", "incomeTaxes", "netIncome",
                        ],
                        mappings,
                        col_names=col_names,
                        relevance_predicate=relevance_predicate,
                    )
                    self._insert_statement_table_rows(
                        conn,
                        source_ref,
                        temp_docids,
                        "BalanceSheet",
                        [
                            "cash", "inventories", "currentAssets", "ppe", "intangibleAssets",
                            "totalAssets", "shareholdersEquity", "currentLiabilities",
                            "NonCurrentLiabilities", "LongTermDebt", "TotalLiabilities",
                        ],
                        mappings,
                        col_names=col_names,
                        relevance_predicate=relevance_predicate,
                    )
                    self._insert_statement_table_rows(
                        conn,
                        source_ref,
                        temp_docids,
                        "CashflowStatement",
                        [
                            "operatingCashflow", "depreciation", "cashflowInventories",
                            "investmentCashflow", "capex", "financingCashflow", "dividends", "buybacks",
                        ],
                        mappings,
                        col_names=col_names,
                        relevance_predicate=relevance_predicate,
                    )

                total_docs += len(batch)
                if total_docs % (batch_size * 10) == 0:
                    logger.info("Generate Financial Statements progress: %d docs processed", total_docs)

            logger.info("Generate Financial Statements completed. Processed %d document(s).", total_docs)
        finally:
            conn.close()


    def Filter_for_Relevant(self, input_table, output_table, conn=None):
        """
        Generates financial statements by querying data from the input table and
        storing the results in the output table, keeping only specific columns.

        Args:
            input_table (str): The name of the input table to query data from.
            output_table (str): The name of the output table to store the results.
            conn (sqlite3.Connection, optional): Existing database connection.
                A new connection is opened and closed automatically when omitted.

        Returns:
            None
        """
        own_conn = conn is None
        if own_conn:
            conn = sqlite3.connect(self.DB_PATH)
        cursor = conn.cursor()

        # Load configuration
        with open(self.FINANCIAL_RATIOS_CONFIG_PATH, 'r') as f:
            config = json.load(f)
        
        accounting_term_conditions = config['accounting_term_conditions']
        period_conditions = config['period_conditions']
        query_template = config['query_template']

        # Build the SQL query dynamically
        accounting_term_query = " OR ".join([f"AccountingTerm LIKE '{term}'" for term in accounting_term_conditions])
        period_query = " OR ".join([f"Period = '{period}'" for period in period_conditions])

        query = query_template.format(
            output_table=output_table,
            input_table=input_table,
            accounting_term_query=accounting_term_query,
            period_query=period_query
        )

        # Execute the query
        cursor.execute(query)
        conn.commit()
        if own_conn:
            conn.close()


    def evaluate_expression(self, df, expression):
        """Recursively evaluate a nested expression config dict against a DataFrame.

        Supports literal values, column references with optional fill-NA, and
        binary arithmetic operators (``+``, ``-``, ``*``, ``/``).

        Args:
            df (pd.DataFrame): The DataFrame containing the columns referenced
                by the expression.
            expression (dict): A config dict describing the computation.  Valid
                forms are:
                - ``{"value": <literal>}`` – return a scalar constant.
                - ``{"column": "<name>", "fillna": ...}`` – return a column
                  series, optionally filling NaN values.
                - ``{"operator": "<op>", "operands": [...]}`` – apply a binary
                  arithmetic operator to two recursively evaluated operands.

        Returns:
            pd.Series or scalar: The result of evaluating the expression.
        """
        if "value" in expression:
            return expression["value"]

        if "column" in expression:
            col_name = expression["column"]
            series = df[col_name]
            if "fillna" in expression:
                fillna_config = expression["fillna"]
                if isinstance(fillna_config, dict) and "column" in fillna_config:
                    fallback_series = self.evaluate_expression(df, fillna_config)
                    series = series.fillna(fallback_series)
                else:
                    series = series.fillna(fillna_config)
            return series

        op = expression["operator"]
        operands = [self.evaluate_expression(df, operand) for operand in expression["operands"]]

        if op == '+':
            return operands[0] + operands[1]
        if op == '-':
            return operands[0] - operands[1]
        if op == '*':
            return operands[0] * operands[1]
        if op == '/':
            return operands[0] / operands[1]
        return None

    def Generate_Financial_Ratios(self, input_table, output_table, overwrite=False):
        """Generate financial ratios for every company and store them in the database.

        Reads data from ``input_table``, pivots it so that each
        accounting-term/period combination becomes a column, applies the ratio
        definitions from the financial-ratios config, calculates rolling
        averages, standard deviations, growth rates, and Z-scores, then appends
        the results to ``output_table``.

        In incremental mode (``overwrite=False``, the default) documents whose
        ``docID`` already appears in *output_table* are skipped.  When
        ``overwrite=True`` the table is dropped first so all data is
        reprocessed.

        Args:
            input_table (str): Name of the source table in the SQLite database.
            output_table (str): Name of the destination table where ratios are stored.
            overwrite (bool): Drop and recreate output table when True.

        Returns:
            None
        """
        # Connect to the database
        conn = sqlite3.connect(self.DB_PATH)

        if overwrite:
            logger.info("Overwrite enabled - dropping '%s' if it exists.", output_table)
            self.delete_table(output_table, conn)

        # Collect already-processed docIDs so we can skip them
        existing_doc_ids: set = set()
        if self._table_exists(conn, output_table):
            existing_df = pd.read_sql_query(
                f"SELECT DISTINCT docID FROM {output_table}", conn,
            )
            existing_doc_ids = set(existing_df["docID"].tolist())

        # Load configuration
        with open(self.FINANCIAL_RATIOS_CONFIG_PATH, 'r') as f:
            config = json.load(f)
        columns_mapping = config['mappings']
        ratios_definitions = config['ratios']

        # Get the list of companies
        companies = self.get_companyList(input_table, conn)
        exists = False
        skipped = 0
        processed = 0
        total_companies = len(companies)
        for i, company in enumerate(companies, 1):
            if i % 100 == 0:
                logger.info(
                    "Generate_Financial_Ratios: progress %d/%d companies processed so far.",
                    i, total_companies,
                )
            # Get the data for the company
            df = pd.read_sql_query(f"""SELECT * FROM {input_table} WHERE edinetCode = '{company}' """, conn)

            # Incremental: drop rows for docIDs already in the output table
            if existing_doc_ids:
                df = df[~df['docID'].isin(existing_doc_ids)]
                if df.empty:
                    skipped += 1
                    continue

            # Create a combined column for AccountingTerm and Period
            df['AccountingTerm_Period'] = df['AccountingTerm'] + '_' + df['Period']

            index_cols = ['edinetCode', 'docID', 'docTypeCode', 'periodStart', 'periodEnd']
                    
            RatiosTable = df.pivot_table(
                index=index_cols,
                columns=['AccountingTerm_Period'],
                values='Amount',
                aggfunc='first'
            ).reset_index()

            # Flatten the columns
            RatiosTable.columns.name = None
            RatiosTable.columns = [col if isinstance(col, str) else col[1] for col in RatiosTable.columns]
            RatiosTable = pd.DataFrame(RatiosTable)

            # Convert any non-numeric columns to numeric
            numeric_columns = RatiosTable.columns.difference(['edinetCode', 'docID', 'Currency', 'docTypeCode', 'periodStart', 'periodEnd'])
            RatiosTable[numeric_columns] = RatiosTable[numeric_columns].apply(pd.to_numeric, errors='coerce')

            # Populate the new columns using a lambda function
            for new_col, relevant_cols in columns_mapping.items():
                RatiosTable[new_col] = RatiosTable.apply(lambda row: next((row[col] for col in relevant_cols if col in row and pd.notnull(row[col])), np.nan), axis=1)

            # Flatten the list of all relevant columns from the columns_mapping dictionary
            columns_to_remove = [col for relevant_cols in columns_mapping.values() for col in relevant_cols]

            # Remove the relevant columns from the RatiosTable
            RatiosTable.drop(columns=columns_to_remove, inplace=True, errors='ignore')

            # Remove any columns whose name begins with jppfs_cor: or jpcrp_cor:
            RatiosTable = RatiosTable.loc[:, ~RatiosTable.columns.str.startswith('jppfs_cor:')]
            RatiosTable = RatiosTable.loc[:, ~RatiosTable.columns.str.startswith('jpcrp_cor:')]

            # Get stock prices and add to RatiosTable
            prices_sql = f"""
            SELECT DISTINCT '{company}' as edinetCode, t.periodEnd,
                   (SELECT s.Price 
                    FROM stock_prices s
                    JOIN companyInfo c ON c.Company_Ticker = s.Ticker
                    WHERE c.EdinetCode = '{company}'
                    AND s.Date <= t.periodEnd
                    ORDER BY s.Date DESC
                    LIMIT 1) as PerShare_SharePrice
            FROM {input_table} t
            WHERE t.edinetCode = '{company}'
            GROUP BY t.periodEnd
            """
            prices_df = pd.read_sql_query(prices_sql, conn)
            prices_df['periodEnd'] = pd.to_datetime(prices_df['periodEnd'])
            RatiosTable['periodEnd'] = pd.to_datetime(RatiosTable['periodEnd'])
            
            RatiosTable = RatiosTable.merge(
                prices_df[['periodEnd', 'PerShare_SharePrice']],
                on='periodEnd',
                how='left'
            )

            RatiosTable.reset_index(drop=True, inplace=True)

            # Calculate ratios from config
            RatiosTable_calcs = RatiosTable.copy()
            for ratio_def in ratios_definitions:
                output_col = ratio_def["output"]
                expression = ratio_def["expression"]
                RatiosTable[output_col] = self.evaluate_expression(RatiosTable_calcs, expression)
                RatiosTable_calcs[output_col] = RatiosTable[output_col]

            # Calculate the 3 year, 5 year and 10 year averages

            # OPTIONAL: Set this at the top of your script to handle the downcasting warning globally
            pd.set_option('future.no_silent_downcasting', True)

            new_cols = {}
            value_cols = [col for col in RatiosTable.columns if col not in index_cols ]

            for output_col in value_cols:
                series = RatiosTable[output_col]
                
                # Convert series to numeric type (handles object dtype)
                series = pd.to_numeric(series, errors='coerce')
                
                # Clean up negative zeros in the series
                series = series.where(series != 0.0, 0.0)

                # Rolling Metrics
                new_cols[f"{output_col}_3Year_Average"] = series.rolling(window=3, min_periods=1).mean()
                new_cols[f"{output_col}_5Year_Average"] = series.rolling(window=5, min_periods=1).mean()
                new_cols[f"{output_col}_10Year_Average"] = series.rolling(window=10, min_periods=1).mean()
                
                
                new_cols[f"{output_col}_3Year_Std"] = series.rolling(window=3, min_periods=1).std()
                new_cols[f"{output_col}_5Year_Std"] = series.rolling(window=5, min_periods=1).std()
                new_cols[f"{output_col}_10Year_Std"] = series.rolling(window=10, min_periods=1).std()
                
                # Growth Metrics - Handle division by zero from pct_change
                # pct_change will handle 0 values but may produce inf when dividing by 0
                growth_1yr = series.pct_change(periods=1,fill_method=None)
                growth_3yr = series.pct_change(periods=3,fill_method=None)
                growth_5yr = series.pct_change(periods=5,fill_method=None)
                growth_10yr = series.pct_change(periods=10,fill_method=None)
                
                # Replace inf values with NaN (occurs when previous value was 0)
                new_cols[f"{output_col}_1Year_Growth"] = growth_1yr.replace([np.inf, -np.inf], np.nan)
                new_cols[f"{output_col}_3Year_Growth"] = growth_3yr.replace([np.inf, -np.inf], np.nan)
                new_cols[f"{output_col}_5Year_Growth"] = growth_5yr.replace([np.inf, -np.inf], np.nan)
                new_cols[f"{output_col}_10Year_Growth"] = growth_10yr.replace([np.inf, -np.inf], np.nan)
                
                # Z-Score Calculation with Safety
                std_5y = new_cols[f"{output_col}_5Year_Std"]
                avg_5y = new_cols[f"{output_col}_5Year_Average"]
                
                
                # We subtract the mean
                diff = series - avg_5y
                
                # Divide safely: 
                # - Where std > 0: calculate z-score normally
                # - Where std == 0: set z-score to 0 (all values equal the mean)
                # - Where std is NaN: set z-score to NaN
                z_score = np.where(
                    std_5y > 0,
                    diff / std_5y,
                    np.where(std_5y == 0, 0, np.nan)
                )
                
                # Clean up Z-Score - replace any remaining inf with NaN, then NaN with 0
                new_cols[f"{output_col}_ZScore"] = (
                    pd.Series(z_score, index=series.index)
                        .replace([np.inf, -np.inf], np.nan)
                        .infer_objects(copy=False)
                )

            RatiosTable = pd.concat([RatiosTable, pd.DataFrame(new_cols)], axis=1)
            # Round the values to 4 decimal places
            RatiosTable = RatiosTable.round(4)

            # Store the data back to the database
            if exists:
                self.add_missing_columns(conn, output_table, RatiosTable)
            RatiosTable.to_sql(output_table, conn, if_exists='append')
            conn.commit()
            exists = True
            processed += 1
        
        logger.info(
            "Generate_Financial_Ratios: processed %d company/ies, "
            "skipped %d (already in '%s').",
            processed, skipped, output_table,
        )
        conn.close()





    def add_missing_columns(self, conn, table_name, df):
        """Add columns to the SQLite table that are present in the DataFrame but not in the table.

        Args:
            conn (sqlite3.Connection): SQLite connection object.
            table_name (str): Name of the table to modify.
            df (pd.DataFrame): DataFrame whose columns are compared against the table.

        Returns:
            None
        """
        cursor = conn.cursor()
        
        # Get existing columns in the table
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_columns = [info[1] for info in cursor.fetchall()]
        
        # Add missing columns
        for column in df.columns:
            if column not in existing_columns:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN '{column}' TEXT")
        
        conn.commit()


    def delete_table(self, table_name, connection=None):
        """Delete a table from the SQLite database if it exists.

        Args:
            table_name (str): Name of the table to delete.
            connection (sqlite3.Connection, optional): Existing database
                connection. A new connection is opened and closed automatically
                when omitted.

        Returns:
            None
        """
        try:
            if connection is None:
                conn = sqlite3.connect(self.DB_PATH)
            else:
                conn = connection
            cursor = conn.cursor()
            
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.commit()
        except Exception as e:
            print(f"An error occurred while deleting table {table_name}: {e}")
        finally:
            if connection is None:
                conn.close()

    def get_companyList(self, input_table, conn, company_column="edinetCode"):
        df = pd.read_sql_query(f"SELECT DISTINCT {company_column} FROM {input_table}", conn)
        return df[company_column].tolist()


    def rename_columns(self, conn, table_name, column_mapping):
        """Rename columns in a SQLite table according to the provided mapping.

        Args:
            conn (sqlite3.Connection): SQLite connection object.
            table_name (str): Name of the table to modify.
            column_mapping (dict): Mapping of old column names (keys) to new
                column names (values).

        Returns:
            None
        """
        cursor = conn.cursor()
        
        for old_name, new_name in column_mapping.items():
            cursor.execute(f"ALTER TABLE {table_name} RENAME COLUMN '{old_name}' TO '{new_name}'")
        
        conn.commit()

    def rename_columns_to_Standard(self, conn, table_name):
        """Rename columns in a table to the project's standard names using the financial-ratios config.

        Args:
            conn (sqlite3.Connection): SQLite connection object.
            table_name (str): Name of the table whose columns will be renamed.

        Returns:
            None
        """
        with open(self.FINANCIAL_RATIOS_CONFIG_PATH, 'r') as f:
            config = json.load(f)
        column_mapping = config['standard_column_mapping']
        self.rename_columns(conn, table_name, column_mapping)

    def copy_table(self, conn, source_table, target_table):
        """Copy all rows and columns from one SQLite table to a new table.

        Args:
            conn (sqlite3.Connection): SQLite connection object.
            source_table (str): Name of the source table.
            target_table (str): Name of the new target table to create.

        Returns:
            None
        """
        cursor = conn.cursor()        
        # Create the target table if it doesn't exist
        cursor.execute(f"CREATE TABLE {target_table} AS SELECT * FROM {source_table}")        
        
        conn.commit()

    def copy_table_to_Standard(self, source_table, target_table, conn=None, overwrite=False):
        """
        Copies data from a source table to a target table with standardized
        column names and generates financial statements.

        In incremental mode (``overwrite=False``, the default) only rows whose
        ``docID`` is not already present in *target_table* are inserted.
        When ``overwrite=True`` the target table is dropped and rebuilt from
        scratch.

        Args:
            source_table (str): Name of the source table.
            target_table (str): Name of the target table.
            conn: Optional database connection.
            overwrite (bool): Drop and recreate target table when True.

        Returns:
            None
        """
        own_conn = conn is None
        if own_conn:
            conn = sqlite3.connect(self.DB_PATH)
        cursor = conn.cursor()

        # 1 - If overwrite, drop the target table
        if overwrite:
            logger.info("Overwrite enabled - dropping '%s' if it exists.", target_table)
            self.delete_table(target_table, conn)

        target_exists = self._table_exists(conn, target_table)

        # 2 - Create a temp table with only rows whose docID is new
        suffix = str(random.randint(1000, 9999))
        temp_table = f"_tmp_std_{suffix}"
        if target_exists:
            cursor.execute(
                f"CREATE TABLE {temp_table} AS SELECT * FROM {source_table} "
                f"WHERE docID NOT IN (SELECT DISTINCT docID FROM {target_table})"
            )
        else:
            cursor.execute(
                f"CREATE TABLE {temp_table} AS SELECT * FROM {source_table}"
            )
        conn.commit()

        # 3 - Rename columns in the temp table to standard names
        self.rename_columns_to_Standard(conn, temp_table)

        # 4 - Filter the temp table for relevant rows (replace in-place via a swap)
        temp_filtered = f"_tmp_filt_{suffix}"
        self.Filter_for_Relevant(temp_table, temp_filtered, conn)
        self.delete_table(temp_table, conn)

        # 5 - Merge into the target table and clean up
        if not target_exists:
            cursor.execute(f"ALTER TABLE {temp_filtered} RENAME TO {target_table}")
            conn.commit()
            logger.info("Created '%s' from '%s'.", target_table, source_table)
        else:
            cursor.execute(
                f"INSERT INTO {target_table} SELECT * FROM {temp_filtered}"
            )
            new_rows = cursor.rowcount
            conn.commit()
            logger.info(
                "Incremental standardize: inserted %d new row(s) into '%s'.",
                new_rows, target_table,
            )
            self.delete_table(temp_filtered, conn)

        if own_conn:
            conn.close()

    def parse_edinet_taxonomy(self, xsd_file, table_name, connection=None):
        """
        Parses an EDINET Taxonomy XSD file and stores relevant elements in an SQLite database.

        Args:
            xsd_file: Path to the EDINET XSD file.
            table_name: Name of the SQLite table to write elements into.
            connection: Optional existing SQLite connection.  A new one is
                opened (and closed) automatically when omitted.
        """
        tree = ET.parse(xsd_file)
        root = tree.getroot()
        namespace = "{http://www.w3.org/2001/XMLSchema}"

        elements = []
        for elem in root.findall(f"{namespace}element"):
            name = elem.get("name")
            elem_id = elem.get("id")
            abstract = elem.get("abstract", "false")
            balance = elem.get("{http://www.xbrl.org/2003/instance}balance")
            period_type = elem.get("{http://www.xbrl.org/2003/instance}periodType")

            elem_id_adjusted = self._adjust_string(elem_id, "jppfs_cor_", "jppfs_cor:")

            if period_type == "instant" and abstract == "false":
                statement = "Balance Sheet"
            elif period_type == "duration" and abstract == "false" and balance is not None:
                statement = "Income Statement"
            elif period_type == "duration" and abstract == "false" and balance is None:
                statement = "Cashflow Statement"
            else:
                statement = "Other Statement"

            if statement == "Balance Sheet" and balance == "credit":
                elem_type = "Liability"
            elif statement == "Balance Sheet" and balance == "debit":
                elem_type = "Asset"
            elif statement == "Income Statement" and balance == "debit":
                elem_type = "Expense"
            elif statement == "Income Statement" and balance == "credit":
                elem_type = "Income"
            else:
                elem_type = "Other"

            if elem_id and name:
                elements.append((elem_id_adjusted, name, statement, elem_type))

        if connection is None:
            conn = sqlite3.connect(self.DB_PATH)
        else:
            conn = connection

        try:
            self._create_table(conn, table_name, ["Id", "Name", "Statement", "Type"])
            self._insert_data(conn, table_name, elements)
            conn.commit()
        finally:
            if connection is None:
                conn.close()

    # ------------------------------------------------------------------
    # Private helpers for parse_edinet_taxonomy
    # ------------------------------------------------------------------

    def _create_table(self, conn, table_name, columns):
        """Create *table_name* with TEXT columns if it does not already exist."""
        column_definitions = ", ".join([f"{col} TEXT" for col in columns])
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions})")

    def _insert_data(self, conn, table_name, rows):
        """Bulk-insert *rows* (list of tuples) into *table_name*."""
        if not rows:
            return
        placeholders = ", ".join(["?" for _ in rows[0]])
        conn.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", rows)

    def _adjust_string(self, input_string, check_substring, replace_substring):
        """Replace the leading *check_substring* with *replace_substring* once."""
        if input_string and input_string.startswith(check_substring):
            return input_string.replace(check_substring, replace_substring, 1)
        return input_string
