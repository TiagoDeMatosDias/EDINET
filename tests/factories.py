"""Small, deterministic data factories shared by the test suite.

The generated data deliberately exercises real SQLite and IBKR parsing paths
without relying on operator-owned files under ``data/``.
"""

from __future__ import annotations

import math
import sqlite3
from datetime import date, timedelta
from pathlib import Path


def sample_ibkr_xml(*, account_id: str = "TEST-ACCOUNT") -> str:
    """Return a representative, synthetic IBKR FlexQuery response."""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse>
  <FlexStatements>
    <FlexStatement accountId="{account_id}">
      <Trades>
        <Trade levelOfDetail="EXECUTION" assetCategory="STK"
          transactionID="trade-aaa-buy" tradeID="trade-1" accountId="{account_id}"
          symbol="AAA" description="Alpha Test Company" isin="TEST00000001"
          conid="1001" currency="USD" tradeDate="2024-01-02"
          settleDateTarget="2024-01-04" quantity="10" tradePrice="100"
          tradeMoney="1000" proceeds="-1000" ibCommission="-1"
          netCash="-1001" buySell="BUY" fxRateToBase="0.9" />
        <Trade levelOfDetail="EXECUTION" assetCategory="STK"
          transactionID="trade-bbb-buy" tradeID="trade-2" accountId="{account_id}"
          symbol="BBB" description="Beta Test Company" isin="TEST00000002"
          conid="1002" currency="EUR" tradeDate="2024-01-03"
          settleDateTarget="2024-01-05" quantity="5" tradePrice="50"
          tradeMoney="250" proceeds="-250" ibCommission="-1"
          netCash="-251" buySell="BUY" fxRateToBase="1" />
        <Trade levelOfDetail="EXECUTION" assetCategory="OPT"
          transactionID="trade-option-buy" tradeID="trade-3" accountId="{account_id}"
          symbol="AAA  280121C00120000" description="AAA synthetic call"
          conid="2001" currency="USD" tradeDate="2024-01-04"
          settleDateTarget="2024-01-05" quantity="1" tradePrice="2"
          tradeMoney="200" proceeds="-200" ibCommission="-1"
          netCash="-201" buySell="BUY" fxRateToBase="0.9" strike="120"
          expiry="2028-01-21" putCall="C" underlyingSymbol="AAA"
          underlyingConid="1001" multiplier="100" />
        <Trade levelOfDetail="ORDER" assetCategory="STK"
          transactionID="ignored-order" accountId="{account_id}" symbol="IGNORED"
          currency="EUR" tradeDate="2024-01-02" />
      </Trades>
      <CashTransactions>
        <CashTransaction levelOfDetail="DETAIL" transactionID="deposit-1"
          accountId="{account_id}" type="Deposits/Withdrawals" currency="EUR"
          dateTime="2024-01-02;090000" amount="5000" fxRateToBase="1"
          description="Synthetic deposit" />
        <CashTransaction levelOfDetail="DETAIL" transactionID="dividend-1"
          accountId="{account_id}" type="Dividends" assetCategory="STK"
          symbol="AAA" currency="USD" dateTime="2024-01-08;120000"
          amount="20" fxRateToBase="0.9" description="AAA dividend" />
        <CashTransaction levelOfDetail="DETAIL" transactionID="tax-1"
          accountId="{account_id}" type="Withholding Tax" assetCategory="STK"
          symbol="AAA" currency="USD" dateTime="2024-01-08;120001"
          amount="-3" fxRateToBase="0.9" description="AAA withholding tax" />
        <CashTransaction levelOfDetail="DETAIL" transactionID="interest-1"
          accountId="{account_id}" type="Broker Interest Paid" currency="EUR"
          dateTime="2024-01-09" amount="2" fxRateToBase="1"
          description="Synthetic interest" />
        <CashTransaction levelOfDetail="DETAIL" transactionID="ignored-cash"
          accountId="{account_id}" type="Unknown Type" currency="EUR"
          dateTime="2024-01-09" amount="999" />
      </CashTransactions>
      <CorporateActions>
        <CorporateAction levelOfDetail="DETAIL" transactionID="spinoff-1"
          accountId="{account_id}" type="SO" assetCategory="STK" symbol="SPIN"
          description="Synthetic spinoff" currency="USD"
          dateTime="2024-01-10;080000" quantity="2" amount="0"
          fxRateToBase="0.9" actionDescription="AAA spins off SPIN"
          actionID="action-1" />
      </CorporateActions>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""


def write_sample_ibkr_xml(path: str | Path, *, account_id: str = "TEST-ACCOUNT") -> Path:
    """Write :func:`sample_ibkr_xml` to *path* and return the resolved path."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(sample_ibkr_xml(account_id=account_id), encoding="utf-8")
    return target.resolve()


def create_market_database(path: str | Path) -> Path:
    """Create a compact standardized database with realistic market series."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(target) as conn:
        conn.executescript(
            """
            CREATE TABLE Stock_Prices (
                Date TEXT NOT NULL,
                Ticker TEXT NOT NULL,
                Currency TEXT NOT NULL,
                Price REAL NOT NULL,
                PRIMARY KEY (Date, Ticker, Currency)
            );
            CREATE INDEX idx_stock_prices_ticker_date
                ON Stock_Prices(Ticker, Date);

            CREATE TABLE CompanyInfo (
                Company_Code TEXT PRIMARY KEY,
                Company_Ticker TEXT NOT NULL,
                Company_Name TEXT NOT NULL,
                Company_Industry TEXT
            );

            CREATE TABLE FinancialStatements (
                docID TEXT PRIMARY KEY,
                Company_Code TEXT NOT NULL,
                periodEnd TEXT NOT NULL
            );

            CREATE TABLE ShareMetrics (
                docID TEXT PRIMARY KEY,
                "Dividend paid per share" REAL
            );
            """
        )
        companies = [
            ("E00001", "AAA", "Alpha Test Company", "Technology"),
            ("E00002", "BBB", "Beta Test Company", "Industrials"),
            ("E00003", "BENCH", "Benchmark Test Company", "Index"),
            ("E00004", "SPIN", "Spinoff Test Company", "Technology"),
        ]
        conn.executemany("INSERT INTO CompanyInfo VALUES (?, ?, ?, ?)", companies)

        statements: list[tuple[str, str, str]] = []
        dividends: list[tuple[str, float]] = []
        for year in range(2019, 2027):
            for company_index, company_code in enumerate(("E00001", "E00002", "E00003")):
                doc_id = f"DOC-{company_index + 1}-{year}"
                statements.append((doc_id, company_code, f"{year}-03-31"))
                dividends.append((doc_id, 1.0 + company_index * 0.25 + (year - 2019) * 0.05))
        conn.executemany("INSERT INTO FinancialStatements VALUES (?, ?, ?)", statements)
        conn.executemany("INSERT INTO ShareMetrics VALUES (?, ?)", dividends)

        start = date(2018, 1, 1)
        end = date(2028, 12, 31)
        stock_rows: list[tuple[str, str, str, float]] = []
        fx_rows: list[tuple[str, str, str, float]] = []
        inflation_rows: list[tuple[str, str, str, float]] = []
        current = start
        day_index = 0
        while current <= end:
            if current.weekday() < 5:
                date_text = current.isoformat()
                stock_rows.extend(
                    (
                        (date_text, "AAA", "USD", 90.0 + day_index * 0.025 + math.sin(day_index / 17)),
                        (date_text, "BBB", "EUR", 55.0 + day_index * 0.012 + math.sin(day_index / 9) * 1.5),
                        (date_text, "BENCH", "EUR", 180.0 + day_index * 0.018),
                        (date_text, "SPIN", "USD", 25.0 + day_index * 0.006),
                    )
                )
                fx_rows.extend(
                    (
                        (date_text, "EUR", "USD", 1.08 + math.sin(day_index / 100) * 0.02),
                        (date_text, "EUR", "JPY", 150.0 + math.sin(day_index / 80) * 3.0),
                    )
                )
                if current.day <= 7:
                    inflation_rows.append(
                        (date_text, "Inflation_EUR", "EUR", 100.0 + day_index * 0.0015)
                    )
            current += timedelta(days=1)
            day_index += 1

        conn.executemany("INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)", stock_rows)
        conn.executemany("INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)", fx_rows)
        conn.executemany("INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)", inflation_rows)
        conn.commit()
    return target.resolve()


def create_docs_market_database(path: str | Path) -> Path:
    """Create the richer, synthetic market database used in documentation.

    The regular test factory intentionally stays small.  Documentation captures
    need populated company snapshots, statement history, screening metrics, and
    comparisons, while still remaining completely detached from operator data.
    """
    target = create_market_database(path)
    with sqlite3.connect(target) as conn:
        conn.executescript(
            """
            ALTER TABLE CompanyInfo ADD COLUMN Listed TEXT;
            ALTER TABLE CompanyInfo ADD COLUMN Description TEXT;
            ALTER TABLE CompanyInfo ADD COLUMN [Submitter Name] TEXT;
            ALTER TABLE CompanyInfo ADD COLUMN EdinetCode TEXT;
            ALTER TABLE FinancialStatements ADD COLUMN DescriptionOfBusiness TEXT;
            ALTER TABLE FinancialStatements ADD COLUMN DescriptionOfBusiness_EN TEXT;
            ALTER TABLE FinancialStatements ADD COLUMN SharesOutstanding REAL;
            ALTER TABLE FinancialStatements ADD COLUMN SharePrice REAL;
            ALTER TABLE ShareMetrics ADD COLUMN SharesOutstanding REAL;

            CREATE TABLE IncomeStatement (
                docID TEXT PRIMARY KEY,
                netSales REAL,
                grossProfit REAL,
                operatingIncome REAL,
                netIncome REAL
            );
            CREATE TABLE BalanceSheet (
                docID TEXT PRIMARY KEY,
                cash REAL,
                currentAssets REAL,
                totalAssets REAL,
                shareholdersEquity REAL,
                currentLiabilities REAL,
                totalLiabilities REAL
            );
            CREATE TABLE CashflowStatement (
                docID TEXT PRIMARY KEY,
                operatingCashflow REAL,
                investmentCashflow REAL,
                financingCashflow REAL,
                capex REAL,
                dividends REAL
            );
            CREATE TABLE PerShare (
                docID TEXT PRIMARY KEY,
                EPS REAL,
                BookValue REAL,
                Dividends REAL
            );
            CREATE TABLE Valuation (
                docID TEXT PRIMARY KEY,
                PERatio REAL,
                PriceToBook REAL,
                PriceToSales REAL,
                EnterpriseValueToSales REAL,
                DividendsYield REAL,
                MarketCap REAL
            );
            CREATE TABLE Quality (
                docID TEXT PRIMARY KEY,
                ReturnOnEquity REAL,
                ReturnOnAssets REAL,
                DebtToEquity REAL,
                CurrentRatio REAL,
                GrossMargin REAL,
                OperatingMargin REAL,
                NetProfitMargin REAL
            );
            """
        )

        company_updates = {
            "E00001": (
                "JPX Prime",
                "Develops industrial sensors, controls, and monitoring software for factories.",
            ),
            "E00002": (
                "JPX Prime",
                "Manufactures precision tools and automation equipment for global customers.",
            ),
            "E00003": (
                "Index",
                "Synthetic benchmark series used by the documentation environment.",
            ),
            "E00004": (
                "JPX Growth",
                "Produces specialist components for advanced manufacturing.",
            ),
        }
        for company_code, (market, description) in company_updates.items():
            conn.execute(
                """UPDATE CompanyInfo
                   SET Listed = ?, Description = ?, [Submitter Name] = Company_Name,
                       EdinetCode = Company_Code
                   WHERE Company_Code = ?""",
                (market, description, company_code),
            )

        profiles = {
            "E00001": {
                "revenue": 7_800_000_000,
                "growth": 0.085,
                "gross_margin": 0.43,
                "operating_margin": 0.165,
                "net_margin": 0.118,
                "assets": 12_500_000_000,
                "equity_ratio": 0.57,
                "shares": 72_000_000,
                "pe": 18.4,
                "pb": 2.1,
                "yield": 0.021,
            },
            "E00002": {
                "revenue": 6_900_000_000,
                "growth": 0.052,
                "gross_margin": 0.37,
                "operating_margin": 0.124,
                "net_margin": 0.086,
                "assets": 10_800_000_000,
                "equity_ratio": 0.49,
                "shares": 81_000_000,
                "pe": 14.7,
                "pb": 1.5,
                "yield": 0.028,
            },
            "E00003": {
                "revenue": 9_000_000_000,
                "growth": 0.035,
                "gross_margin": 0.34,
                "operating_margin": 0.102,
                "net_margin": 0.071,
                "assets": 15_000_000_000,
                "equity_ratio": 0.52,
                "shares": 100_000_000,
                "pe": 16.2,
                "pb": 1.7,
                "yield": 0.024,
            },
        }
        statement_rows = conn.execute(
            "SELECT docID, Company_Code, periodEnd FROM FinancialStatements"
        ).fetchall()
        for doc_id, company_code, period_end in statement_rows:
            profile = profiles[company_code]
            year = int(str(period_end)[:4])
            age = year - 2019
            revenue = profile["revenue"] * (1 + profile["growth"]) ** age
            gross_profit = revenue * profile["gross_margin"]
            operating_income = revenue * profile["operating_margin"]
            net_income = revenue * profile["net_margin"]
            assets = profile["assets"] * (1 + profile["growth"] * 0.55) ** age
            equity = assets * profile["equity_ratio"]
            liabilities = assets - equity
            current_assets = assets * 0.37
            current_liabilities = liabilities * 0.43
            shares = profile["shares"]
            eps = net_income / shares
            book_value = equity / shares
            dividends = eps * 0.34
            price = eps * profile["pe"]
            market_cap = price * shares

            conn.execute(
                """UPDATE FinancialStatements
                   SET DescriptionOfBusiness = ?, DescriptionOfBusiness_EN = ?,
                       SharesOutstanding = ?, SharePrice = ?
                   WHERE docID = ?""",
                (
                    "Synthetic documentation record.",
                    company_updates[company_code][1],
                    shares,
                    price,
                    doc_id,
                ),
            )
            conn.execute(
                "INSERT INTO IncomeStatement VALUES (?, ?, ?, ?, ?)",
                (doc_id, revenue, gross_profit, operating_income, net_income),
            )
            conn.execute(
                "INSERT INTO BalanceSheet VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    doc_id,
                    assets * 0.09,
                    current_assets,
                    assets,
                    equity,
                    current_liabilities,
                    liabilities,
                ),
            )
            conn.execute(
                "INSERT INTO CashflowStatement VALUES (?, ?, ?, ?, ?, ?)",
                (
                    doc_id,
                    net_income * 1.32,
                    -net_income * 0.48,
                    -net_income * 0.31,
                    -net_income * 0.37,
                    dividends * shares,
                ),
            )
            conn.execute(
                "INSERT INTO PerShare VALUES (?, ?, ?, ?)",
                (doc_id, eps, book_value, dividends),
            )
            conn.execute(
                "INSERT INTO Valuation VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    doc_id,
                    profile["pe"],
                    profile["pb"],
                    market_cap / revenue,
                    market_cap * 1.08 / revenue,
                    profile["yield"],
                    market_cap,
                ),
            )
            conn.execute(
                "INSERT INTO Quality VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    doc_id,
                    net_income / equity,
                    net_income / assets,
                    liabilities / equity,
                    current_assets / current_liabilities,
                    profile["gross_margin"],
                    profile["operating_margin"],
                    profile["net_margin"],
                ),
            )
            conn.execute(
                "UPDATE ShareMetrics SET SharesOutstanding = ? WHERE docID = ?",
                (shares, doc_id),
            )
        conn.commit()
    return target.resolve()
