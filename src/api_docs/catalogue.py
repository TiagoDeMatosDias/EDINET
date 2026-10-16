"""What the API docs show for each screen: its endpoints, grouped, and short guides.

An endpoint belongs to the section whose pattern names it most specifically:
``"POST /api/screening/run"`` (method and path) beats ``"/api/screening/history"``
(any method) beats ``"/api/screening/*"`` (the path and everything below it).
``related`` lists endpoints documented under another screen that this screen
also uses. ``tests/unit/test_api_docs.py`` checks that every endpoint has
exactly one home, that every pattern matches a route, and that the Python
examples compile.

Guide text uses a small Markdown subset: paragraphs, ``code``, **bold**, and "- " lists.
Every Python example runs after ``PYTHON_SETUP``; every curl example expects
``SHADE_URL`` and ``SHADE_TOKEN`` in the environment.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Guide:
    title: str
    text: str
    curl: str = ""
    python: str = ""
    #: An abbreviated example of what comes back.
    response: str = ""


@dataclass(frozen=True)
class Group:
    title: str
    patterns: tuple[str, ...]
    intro: str = ""


@dataclass(frozen=True)
class Section:
    id: str
    title: str
    #: The workspace routes whose "API" link opens this section.
    screen_paths: tuple[str, ...]
    summary: str
    intro: str = ""
    groups: tuple[Group, ...] = ()
    guides: tuple[Guide, ...] = ()
    related: tuple[str, ...] = field(default_factory=tuple)


PYTHON_SETUP = '''\
import os, time
import requests

BASE = os.environ["SHADE_URL"]  # for example https://127.0.0.1:8000
api = requests.Session()
api.headers["Authorization"] = "Bearer " + os.environ["SHADE_TOKEN"]


def call(method, path, **kwargs):
    """Send a request; return the JSON answer, waiting for it when the server queues a job."""
    response = api.request(method, BASE + path, **kwargs)
    if response.status_code >= 400:
        raise RuntimeError(f"{response.status_code}: {response.text}")
    if not response.content:
        return None
    data = response.json()
    if response.status_code == 202 and data.get("code") == "job_pending":
        return wait_for_job(data["job_id"])
    return data


def wait_for_job(job_id):
    """Poll a job until it ends and return its result."""
    while True:
        job = call("GET", f"/api/my-jobs/{job_id}")
        if job["status"] == "completed":
            return job["result"]
        if job["status"] in ("failed", "cancelled", "interrupted"):
            raise RuntimeError(job["error_message"] or job["status"])
        time.sleep(2)
'''

AUTH = '-H "Authorization: Bearer $SHADE_TOKEN"'

_SCREEN_JSON = """{
    "criteria": [
      {"table": "Financial_Ratios", "column": "Return on Equity", "operator": ">", "value": 0.15},
      {"table": "ShareMetrics", "column": "Price-earnings ratio", "operator": "BETWEEN", "value": 0, "value2": 12}
    ],
    "columns": ["CompanyInfo.Company_Name", "CompanyInfo.Company_Ticker", "Financial_Ratios.Return on Equity"],
    "sort_by": "Financial_Ratios.Return on Equity"
  }"""


def _curl(path: str, *, method: str = "", body: str = "", extra: str = "") -> str:
    """One curl call against ``$SHADE_URL`` with the token header."""
    parts = ["curl -s"]
    if method:
        parts.append(f"-X {method}")
    parts.append(f'"$SHADE_URL{path}"')
    parts.append(AUTH)
    if body:
        parts.append(f"\\\n  --json '{body}'")
    if extra:
        parts.append(extra)
    return " ".join(parts)


START = Section(
    id="start",
    title="Getting started",
    screen_paths=(),
    summary="Tokens, the server address and certificate, errors, and long-running jobs.",
    intro=(
        "Everything the workspace does goes through this HTTP API, so your own scripts and apps can do the same: "
        "run screens, start backtests, read company data, keep research, and chat. "
        "Each screen has its own page here; this one covers what they share."
    ),
    groups=(
        Group("Server", ("GET /health",)),
        Group("These docs", ("/api/docs/*",), "The docs themselves, as JSON, for tools that generate clients."),
    ),
    guides=(
        Guide(
            "1. Create a personal API token",
            "Open **Account → Personal API tokens**, give the token a name, choose its access and expiry, and select **Create**. "
            "Copy it right away: it is shown once. Keep it in an environment variable, never in code you share.\n\n"
            "- **Full access** can do everything you can do in the workspace.\n"
            "- **Read-only** can only read (GET requests); anything that changes data answers 403 `insufficient_scope`.\n"
            "- A token acts as you, with your current role: if an administrator changes your role or disables your account, "
            "the token follows. Changing your password revokes every token.\n"
            "- A token cannot create other tokens; only a signed-in browser session can.\n\n"
            "Send it in the `Authorization` header of every request: `Authorization: Bearer ed_pat_…`. "
            "Pages that need no sign-in (registration, sign-in, `/health`) need no token.",
            curl=(
                "export SHADE_URL=https://127.0.0.1:8000\n"
                "export SHADE_TOKEN=ed_pat_...\n"
                + _curl("/api/auth/me")
            ),
            python='me = call("GET", "/api/auth/me")\nprint(me["username"], me["role"])',
            response='{"user_id": "3935a8a6-…", "username": "alice", "email": "alice@example.com", "role": "member", "status": "active"}',
        ),
        Guide(
            "2. Trust the server's certificate",
            "The server only speaks HTTPS. Through its Cloudflare tunnel address the certificate is a normal, trusted one: nothing to do. "
            "On your own network the server uses a self-signed certificate, so tell your client to trust it. "
            "Download it with **Download certificate** at the top of this page (or `GET /api/docs/certificate`), then:\n\n"
            "- curl: `export CURL_CA_BUNDLE=$PWD/shade.pem` (or `--cacert shade.pem`)\n"
            "- Python requests: `export REQUESTS_CA_BUNDLE=$PWD/shade.pem`\n"
            "- Node.js: `export NODE_EXTRA_CA_CERTS=$PWD/shade.pem`\n\n"
            "The certificate names `localhost`, `127.0.0.1`, and the address the server listens on, so use one of those in `SHADE_URL`. "
            "Skipping the check (`curl -k`, `verify=False`) is only safe for a quick test on your own machine.",
            curl=(
                _curl("/api/docs/certificate", extra="-k -o shade.pem")
                + "\nexport CURL_CA_BUNDLE=$PWD/shade.pem\ncurl -s \"$SHADE_URL/health\""
            ),
            python=(
                '# Run once with REQUESTS_CA_BUNDLE unset and verify=False, then export it.\n'
                'pem = api.get(BASE + "/api/docs/certificate", verify=False).text\n'
                'open("shade.pem", "w").write(pem)\n'
                '# export REQUESTS_CA_BUNDLE=$PWD/shade.pem\n'
                'print(requests.get(BASE + "/health", verify="shade.pem").json())'
            ),
            response='{"status": "healthy", "version": "1.0.0", "timestamp": "2026-10-16T12:19:04Z"}',
        ),
        Guide(
            "3. Long-running work: jobs",
            "Screens, backtests, translations, and pipeline steps run on workers. "
            "When an answer takes longer than the server waits, you get **202** with `code` `job_pending` and a `job_id` instead of the result. "
            "Poll `GET /api/my-jobs/{job_id}` until `status` is `completed` and read its `result`; "
            "`failed`, `cancelled`, and `interrupted` also end a job. "
            "The `call` helper in the Python setup does this for you. "
            "Backtests also have their own job endpoints (see Backtests).",
            curl=_curl("/api/my-jobs/JOB_ID"),
            python='job = call("GET", "/api/my-jobs?active=true")\nfor item in job["jobs"]:\n    print(item["title"], item["status"], item["progress_percent"])',
            response='{"job_id": "f036b690-…", "kind": "screening", "status": "completed", "progress_percent": 100.0, "result": {"columns": […], "rows": […]}, …}',
        ),
        Guide(
            "4. Errors",
            "Errors are JSON with a `code`, a readable `detail`, and a `correlation_id` to quote when reporting a problem:\n\n"
            "- **401** `unauthorized`: no token, or it is wrong, expired, or revoked.\n"
            "- **403** `insufficient_scope`: a read-only token tried to change something. **403** `forbidden`: your role may not use this endpoint.\n"
            "- **404**: not found, or not yours. **409**: it changed or conflicts; reload and retry.\n"
            "- **422**: the body or a parameter is invalid; `detail` lists each problem with its location.\n"
            "- **429**: too many requests; wait as long as the `Retry-After` header says.",
            curl=_curl("/api/auth/me").replace(AUTH, '-H "Authorization: Bearer wrong"'),
            response='{"code": "unauthorized", "detail": "Authentication required", "correlation_id": "db065365-…"}',
        ),
        Guide(
            "5. Explore every endpoint",
            "Each screen's page lists its endpoints with parameters, body fields, example calls in curl, Python, and JavaScript, "
            "and a **Send** button that runs safe calls with your session. "
            "These pages, and the OpenAPI schema at `GET /api/docs/openapi.json` (**Download OpenAPI schema** above), "
            "show only the endpoints your role may use. Import the schema into Postman or Insomnia, or generate a client from it.",
            curl=_curl("/api/docs/openapi.json", extra="-o shade-openapi.json"),
            python='schema = call("GET", "/api/docs/openapi.json")\nprint(len(schema["paths"]), "paths you can call")',
        ),
    ),
)

OVERVIEW = Section(
    id="overview",
    title="Overview",
    screen_paths=("/overview",),
    summary="The dashboard: portfolio summary, research, data freshness, and recent work.",
    groups=(Group("Overview", ("GET /api/overview", "GET /api/research/recent-work")),),
    guides=(
        Guide(
            "Read the dashboard",
            "`GET /api/overview` returns everything the Overview screen shows in one answer. "
            "`GET /api/research/recent-work` lists what you opened lately (companies, screens, filings, backtests).",
            curl=_curl("/api/overview"),
            python='overview = call("GET", "/api/overview")\nprint(list(overview))\nfor item in call("GET", "/api/research/recent-work?limit=5")["items"]:\n    print(item)',
        ),
    ),
    related=(
        "GET /api/my-jobs",
        "GET /api/chat/feed",
        "GET /api/chat/unread",
        "GET /api/jobs",
        "GET /api/system/status",
    ),
)

JOBS = Section(
    id="jobs",
    title="Jobs",
    screen_paths=(),
    summary="The job tray on every screen: your queued and running work, and the workers' capacity.",
    intro="Every screen shares the job tray in the top bar. These endpoints follow and cancel the jobs you started.",
    groups=(
        Group("Your jobs", ("/api/my-jobs/*",)),
        Group("Workers", ("GET /api/workers/status",)),
    ),
    guides=(
        Guide(
            "Follow and cancel your jobs",
            "`GET /api/my-jobs?active=true` lists what is queued or running; `GET /api/my-jobs/{job_id}` adds the `result` when a job completes. "
            "`status` is one of `open`, `in_progress`, `cancelling`, `completed`, `failed`, `cancelled`, `interrupted`.",
            curl=_curl("/api/my-jobs?active=true") + "\n" + _curl("/api/my-jobs/JOB_ID/cancel", method="POST"),
            python='for job in call("GET", "/api/my-jobs", params={"active": True})["jobs"]:\n    print(job["job_id"], job["title"], job["status"])\n    if job["cancellable"]:\n        call("POST", f"/api/my-jobs/{job[\'job_id\']}/cancel")',
            response='{"jobs": [{"job_id": "…", "kind": "screening", "title": "Screen · 2 criteria", "status": "in_progress", "progress_percent": 40.0, …}], "workers": {"online": 1, "interactive_slots": 1, "batch_slots": 2}}',
        ),
    ),
)

SCREENING = Section(
    id="screening",
    title="Screening",
    screen_paths=("/screen",),
    summary="Filter every listed company by financial criteria and get the matching rows.",
    groups=(
        Group("Run a screen", ("POST /api/screening/run", "GET /api/screening/last-result", "POST /api/screening/export")),
        Group(
            "Metrics and periods",
            ("GET /api/screening/metrics", "GET /api/screening/formulas", "GET /api/screening/periods"),
            "The tables and columns criteria and output columns can name.",
        ),
        Group("Saved screens", ("/api/screening/saved/*", "POST /api/screening/save")),
        Group("History", ("/api/screening/history",)),
        Group("Prices", ("POST /api/screening/update-prices",)),
        Group("Other", ("/api/screening/*",)),
    ),
    guides=(
        Guide(
            "Run a screen",
            "Send criteria and the columns you want to `POST /api/screening/run`. "
            "Each criterion compares a `table` and `column` (see `GET /api/screening/metrics`) with `value`; "
            "`operator` is `>`, `>=`, `<`, `<=`, `=`, `!=`, `BETWEEN` (with `value2`), `IN` (with `values`), or `LIKE`. "
            "Output columns are `Table.Column`. Rows come back as lists in the order of `columns`, and the latest price is added. "
            "A long screen answers 202 `job_pending`; `call` waits for it, and `GET /api/screening/last-result` keeps your latest result.",
            curl=_curl("/api/screening/run", body=_SCREEN_JSON),
            python=(
                "screen = {\n"
                '    "criteria": [\n'
                '        {"table": "Financial_Ratios", "column": "Return on Equity", "operator": ">", "value": 0.15},\n'
                '        {"table": "ShareMetrics", "column": "Price-earnings ratio", "operator": "BETWEEN", "value": 0, "value2": 12},\n'
                "    ],\n"
                '    "columns": ["CompanyInfo.Company_Name", "CompanyInfo.Company_Ticker", "Financial_Ratios.Return on Equity"],\n'
                '    "sort_by": "Financial_Ratios.Return on Equity",\n'
                "}\n"
                'result = call("POST", "/api/screening/run", json=screen)\n'
                'print(result["row_count"], "companies")\n'
                'for row in result["rows"][:5]:\n'
                '    print(dict(zip(result["columns"], row)))'
            ),
            response='{"columns": ["Company_Name", "Company_Ticker", "Return on Equity", "LatestPrice", "PriceDate"], "rows": [["NIHON SEIKO CO.,LTD.", "57290", 0.2729, 1660.0, "2026-10-15"], …], "row_count": 340, "column_formats": {"Return on Equity": "percent"}}',
        ),
        Guide(
            "Find tables and columns",
            "`GET /api/screening/metrics` maps each table to its columns; `formats` says which columns are percentages. "
            "`GET /api/screening/formulas` lists ready-made computed columns (P/E, P/B, dividend yield) to pass in `computed_columns`.",
            curl=_curl("/api/screening/metrics"),
            python='tables = call("GET", "/api/screening/metrics")["tables"]\nprint(tables["Financial_Ratios"])',
            response='{"tables": {"Financial_Ratios": ["Return on Assets", "Return on Equity", "ROIC", …], …}, "formats": {"Financial_Ratios.Return on Equity": "percent", …}}',
        ),
        Guide(
            "Save a screen and run it again",
            "`POST /api/screening/save` stores the criteria and columns under a name (`overwrite` replaces one). "
            "`GET /api/screening/saved/{name}` returns them, ready to send to `/api/screening/run`.",
            curl=_curl("/api/screening/save", body='{"name": "Cheap and profitable", "overwrite": true, "criteria": [{"table": "Financial_Ratios", "column": "Return on Equity", "operator": ">", "value": 0.15}], "columns": ["CompanyInfo.Company_Name"]}'),
            python=(
                'saved = call("GET", "/api/screening/saved/Cheap and profitable")\n'
                'keys = ("criteria", "criteria_match", "columns", "computed_columns", "screening_date")\n'
                'result = call("POST", "/api/screening/run", json={key: saved[key] for key in keys})\n'
                'print(result["row_count"])'
            ),
        ),
        Guide(
            "Export the matches as CSV",
            "`POST /api/screening/export` takes the same body as a run and answers with a CSV file; "
            "`\"format\": \"backtest\"` writes the backtest-set CSV that `POST /api/backtesting/run-from-csv` reads.",
            curl=_curl("/api/screening/export", body='{"criteria": [{"table": "Financial_Ratios", "column": "Return on Equity", "operator": ">", "value": 0.15}], "columns": ["CompanyInfo.Company_Name"]}', extra="-o screen.csv"),
            python='csv_text = api.post(BASE + "/api/screening/export", json={"criteria": [{"table": "Financial_Ratios", "column": "Return on Equity", "operator": ">", "value": 0.15}], "columns": ["CompanyInfo.Company_Name"]}).text\nprint(csv_text[:300])',
        ),
    ),
    related=("GET /api/my-jobs/{job_id}", "POST /api/backtesting/rolling-jobs", "GET /api/tags"),
)

ANALYSIS = Section(
    id="analysis",
    title="Company analysis",
    screen_paths=("/analyze", "/analyze/:companyCode"),
    summary="One company in depth: overview, statement history, prices, bonds, shareholders, officers, and splits.",
    intro=(
        "Companies are identified by their EDINET code (`E02144` is Toyota Motor). "
        "Tickers are stored as five characters: the four-character exchange code plus a check digit (`72030`). "
        "`GET /api/security/search` finds both."
    ),
    groups=(
        Group("Find a company", ("GET /api/security/search",)),
        Group(
            "Company data",
            ("GET /api/security/overview", "GET /api/security/history", "GET /api/security/taxonomy-tree", "GET /api/security/formulas", "GET /api/security/compare"),
        ),
        Group("Prices", ("GET /api/security/price-history", "POST /api/security/update-price")),
        Group("Bonds", ("/api/bonds/*",)),
        Group("Shareholders and officers", ("/api/ownership/*",)),
        Group("Stock splits", ("/api/splits/*",)),
        Group("Other", ("/api/security/*",)),
    ),
    guides=(
        Guide(
            "Find a company and read its overview",
            "Search by name, ticker, or code, then load the overview: company details, the latest price, "
            "valuation and quality metrics, and the period each comes from.",
            curl=_curl("/api/security/search?q=7203&limit=3") + "\n" + _curl("/api/security/overview?company_code=E02144"),
            python=(
                'hit = call("GET", "/api/security/search", params={"q": "7203"})["results"][0]\n'
                'overview = call("GET", "/api/security/overview", params={"company_code": hit["company_code"]})\n'
                'metrics = overview["metrics"]\n'
                'print(overview["company"]["company_name"], metrics["LatestPrice"], metrics["PERatio"], metrics["MarketCap"])'
            ),
            response='{"company": {"company_code": "E02144", "ticker": "72030", "company_name": "TOYOTA MOTOR CORPORATION", "industry": "Transportation Equipments", …}, "market": {"latest_price": 3029.0, "latest_price_date": "2026-10-15", …}, "metrics": {"LatestPrice": 3029.0, "MarketCap": 47843017016340.0, "PERatio": 10.26, …}, "metric_definitions": {…}, …}',
        ),
        Guide(
            "Read the financial history",
            "`GET /api/security/history` returns the last `periods` fiscal years of every statement: "
            "`tables` maps each statement to its metrics, and each metric's `values` line up with `periods`.",
            curl=_curl("/api/security/history?company_code=E02144&periods=5"),
            python=(
                'history = call("GET", "/api/security/history", params={"company_code": "E02144", "periods": 5})\n'
                'for metric in history["tables"]["IncomeStatement"]["metrics"][:5]:\n'
                '    print(metric["display_name"], dict(zip(history["periods"], metric["values"])))'
            ),
            response='{"periods": ["2022-03-31", …, "2026-03-31"], "tables": {"IncomeStatement": {"display_name": "Income Statement", "metrics": [{"field": "Cost of sales", "display_name": "Cost Of Sales", "values": [12919592000000.0, …]}, …]}, …}}',
        ),
        Guide(
            "Get daily prices",
            "`GET /api/security/price-history` takes the stored ticker (`72030`) and optional `start_date` and `end_date`.",
            curl=_curl("/api/security/price-history?ticker=72030&start_date=2026-01-01"),
            python='prices = call("GET", "/api/security/price-history", params={"ticker": "72030", "start_date": "2026-01-01"})["prices"]\nprint(prices[-1]["trade_date"], prices[-1]["price"])',
            response='{"prices": [{"trade_date": "2026-10-01", "price": 2925.0, "adjusted_price": 2925.0, "provider": "JPX quote", …}, …]}',
        ),
        Guide(
            "Shareholders, officers, and bonds",
            "The Ownership and Bonds tabs read `GET /api/ownership/company/{edinet_code}` (largest shareholders and officers from the latest annual report) "
            "and `GET /api/bonds/company/{edinet_code}` (the company's bonds with a maturity ladder).",
            curl=_curl("/api/ownership/company/E02144") + "\n" + _curl("/api/bonds/company/E02144"),
            python='ownership = call("GET", "/api/ownership/company/E02144")\nprint(list(ownership))\nbonds = call("GET", "/api/bonds/company/E02144")\nprint(list(bonds))',
        ),
    ),
    related=(
        "GET /api/filings",
        "GET /api/research/companies/{edinet_code}",
        "GET /api/research/notes",
        "GET /api/tags/{company_code}",
        "GET /api/chat/companies/{edinet_code}/mentions",
        "GET /api/comparison/peers/{company_code}",
    ),
)

BACKTEST = Section(
    id="backtest",
    title="Backtests",
    screen_paths=("/backtest",),
    summary="Simulate portfolios over past prices, alone, from a CSV set, or by re-running a screen over time.",
    intro=(
        "A portfolio maps stored tickers to an allocation: `{\"72030\": {\"mode\": \"weight\", \"value\": 0.5}}` "
        "(modes `weight`, `shares`, `value`). `GET /api/backtesting/benchmarks` lists benchmarks with stored prices (`TPX` is TOPIX)."
    ),
    groups=(
        Group("Run a backtest", ("POST /api/backtesting/run", "POST /api/backtesting/run-from-csv")),
        Group("Background jobs", ("/api/backtesting/jobs/*",), "For long runs: queue, poll, then load the result."),
        Group(
            "Rolling screen backtests",
            (
                "/api/backtesting/rolling-jobs/*",
                "POST /api/backtesting/run-rolling",
                "GET /api/backtesting/rolling-periods",
                "POST /api/backtesting/export-rolling-xlsx",
                "POST /api/backtesting/export-rolling-zip",
            ),
            "Re-run a screen at each period and backtest what it picked.",
        ),
        Group(
            "Saved results",
            ("GET /api/backtesting/list", "/api/backtesting/result/*", "/api/backtesting/download/*", "/api/backtesting/report/*"),
        ),
        Group("Inputs", ("/api/backtesting/*",), "Tickers, currencies, and benchmarks the builder offers."),
    ),
    guides=(
        Guide(
            "Backtest a portfolio",
            "`POST /api/backtesting/run` simulates the portfolio and saves the result. It waits up to five minutes; "
            "for longer runs use a job (next guide).",
            curl=_curl(
                "/api/backtesting/run",
                body='{\n    "portfolio": {"72030": {"mode": "weight", "value": 0.5}, "67580": {"mode": "weight", "value": 0.5}},\n    "start_date": "2019-01-01", "end_date": "2024-12-31", "benchmark_ticker": "TPX"\n  }',
            ),
            python=(
                'result = call("POST", "/api/backtesting/run", json={\n'
                '    "portfolio": {"72030": {"mode": "weight", "value": 0.5}, "67580": {"mode": "weight", "value": 0.5}},\n'
                '    "start_date": "2019-01-01", "end_date": "2024-12-31", "benchmark_ticker": "TPX",\n'
                "})\n"
                'summary = result["summary"]\n'
                'print(result["id"], summary["annualized_return"], summary["max_drawdown"], summary["benchmark_annualized_return"])'
            ),
            response='{"id": "20261016_141942_2f870d28", "summary": {"total_return": 2.04, "annualized_return": 0.204, "volatility": 0.246, "sharpe_ratio": 0.787, "max_drawdown": -0.299, "benchmark_annualized_return": 0.113, …}, "chart_data": {…}, "per_company": […], "yearly_returns": […]}',
        ),
        Guide(
            "Queue a backtest and poll it",
            "`POST /api/backtesting/jobs` with `kind` `single` (or `csv`, `rolling`) and the request body answers at once with a `job_id`. "
            "Poll `GET /api/backtesting/jobs/{job_id}` until `status` is `complete` (`failed` and `cancelled` also end it), "
            "then load `GET /api/backtesting/result/{result_id}`.",
            curl=(
                _curl("/api/backtesting/jobs", body='{"kind": "single", "request": {"portfolio": {"72030": {"mode": "weight", "value": 1}}, "start_date": "2019-01-01", "end_date": "2024-12-31"}}')
                + "\n" + _curl("/api/backtesting/jobs/JOB_ID")
            ),
            python=(
                'job = call("POST", "/api/backtesting/jobs", json={"kind": "single", "request": {\n'
                '    "portfolio": {"72030": {"mode": "weight", "value": 1}}, "start_date": "2019-01-01", "end_date": "2024-12-31"}})\n'
                'while job["status"] not in ("complete", "failed", "cancelled"):\n'
                "    time.sleep(2)\n"
                '    job = call("GET", f"/api/backtesting/jobs/{job[\'job_id\']}")\n'
                'result = call("GET", f"/api/backtesting/result/{job[\'result_id\']}")\n'
                'print(result["summary"]["annualized_return"])'
            ),
            response='{"job_id": "796e67f1-…", "kind": "single", "status": "complete", "result_id": "20261016_141835_1f328479", "error": null, …}',
        ),
        Guide(
            "Backtest a screen over time",
            "A rolling backtest re-runs a screen at every period between `start_period` and `end_period` "
            "(`GET /api/backtesting/rolling-periods` lists them), buys the top `max_companies`, and holds each pick for every duration. "
            "Queue it with `POST /api/backtesting/rolling-jobs` and poll `GET /api/backtesting/rolling-jobs/{job_id}`.",
            curl=_curl(
                "/api/backtesting/rolling-jobs",
                body='{\n    "criteria": [{"table": "Financial_Ratios", "column": "Return on Equity", "operator": ">", "value": 0.15}],\n    "columns": ["CompanyInfo.Company_Name"], "cadence": "yearly", "durations": ["1yr"],\n    "max_companies": 10, "start_period": "2021-12-01", "end_period": "2023-12-01"\n  }',
            ),
            python=(
                'job = call("POST", "/api/backtesting/rolling-jobs", json={\n'
                '    "criteria": [{"table": "Financial_Ratios", "column": "Return on Equity", "operator": ">", "value": 0.15}],\n'
                '    "columns": ["CompanyInfo.Company_Name"], "cadence": "yearly", "durations": ["1yr"],\n'
                '    "max_companies": 10, "start_period": "2021-12-01", "end_period": "2023-12-01"})\n'
                'while job["status"] not in ("complete", "failed", "cancelled"):\n'
                "    time.sleep(3)\n"
                '    job = call("GET", f"/api/backtesting/rolling-jobs/{job[\'job_id\']}")\n'
                'print(job["status"], job.get("result_id"))'
            ),
        ),
        Guide(
            "List and download saved results",
            "`GET /api/backtesting/list` lists your saved results; `GET /api/backtesting/download/{id}` is a ZIP with the data, "
            "and `GET /api/backtesting/report/{id}` an HTML report.",
            curl=_curl("/api/backtesting/list") + "\n" + _curl("/api/backtesting/download/RESULT_ID", extra="-o backtest.zip"),
            python='for item in call("GET", "/api/backtesting/list")["backtests"][:5]:\n    print(item["id"], item["kind"], item["title"])',
        ),
    ),
    related=("GET /api/my-jobs/{job_id}", "POST /api/screening/export"),
)

PORTFOLIO = Section(
    id="portfolio",
    title="Portfolio",
    screen_paths=("/portfolio",),
    summary="Your own holdings: imports, transactions, holdings, performance, income, and charts.",
    intro="Portfolio data is private to your account. Amounts convert to `base_currency` or `display_currency` where an endpoint takes one.",
    groups=(
        Group(
            "Import and records",
            (
                "POST /api/portfolio/upload",
                "GET /api/portfolio/imports",
                "/api/portfolio/transactions/*",
                "GET /api/portfolio/activity-summary",
                "GET /api/portfolio/date-range",
                "POST /api/portfolio/rebuild",
            ),
        ),
        Group("Holdings", ("/api/portfolio/holdings/*",)),
        Group(
            "Performance and returns",
            (
                "GET /api/portfolio/performance",
                "/api/portfolio/returns/*",
                "GET /api/portfolio/benchmarks",
                "GET /api/portfolio/risk-free-rate",
                "POST /api/portfolio/backtest/compare",
            ),
        ),
        Group("Income", ("GET /api/portfolio/income", "/api/portfolio/dividends/*")),
        Group("Charts", ("/api/portfolio/charts/*",)),
        Group("Your own assets and prices", ("/api/portfolio/assets/*", "/api/portfolio/prices")),
        Group("Risk tools", ("POST /api/portfolio/greeks", "POST /api/portfolio/scenarios/evaluate", "POST /api/portfolio/tax-lots")),
        Group("Data", ("/api/portfolio/*",)),
    ),
    guides=(
        Guide(
            "Import an Interactive Brokers statement",
            "Upload a FlexQuery XML file as multipart form data in a field named `file`. "
            "The server reads it, fetches missing prices, and rebuilds holdings and returns.",
            curl=_curl("/api/portfolio/upload", method="POST", extra='-F "file=@flex_statement.xml"'),
            python='with open("flex_statement.xml", "rb") as handle:\n    print(call("POST", "/api/portfolio/upload", files={"file": handle}))',
        ),
        Guide(
            "Holdings and performance",
            "`GET /api/portfolio/holdings` lists what you hold now with market values; "
            "`GET /api/portfolio/performance` computes returns, volatility, and drawdown, optionally against a benchmark.",
            curl=_curl("/api/portfolio/holdings") + "\n" + _curl("/api/portfolio/performance?base_currency=JPY&benchmark_ticker=TPX"),
            python=(
                'for holding in call("GET", "/api/portfolio/holdings"):\n'
                '    print(holding["symbol"], holding.get("market_value"))\n'
                'print(call("GET", "/api/portfolio/performance", params={"base_currency": "JPY"}))'
            ),
        ),
        Guide(
            "Record a trade by hand",
            "`POST /api/portfolio/transactions/manual` records a buy, sell, dividend, deposit, and more (`kind`), then rebuilds. "
            "Amounts are positive; `kind` sets the direction.",
            curl=_curl("/api/portfolio/transactions/manual", body='{"kind": "buy", "trade_date": "2025-04-01", "currency": "JPY", "symbol": "7203", "quantity": 100, "price": 2500}'),
            python=(
                'trade = {"kind": "buy", "trade_date": "2025-04-01", "currency": "JPY", "symbol": "7203", "quantity": 100, "price": 2500}\n'
                'print(call("POST", "/api/portfolio/transactions/manual", json=trade)["transaction"]["id"])'
            ),
        ),
    ),
    related=("GET /api/research/book", "POST /api/research/position-tags/sync"),
)

PIPELINE = Section(
    id="pipeline",
    title="Data pipeline",
    screen_paths=("/pipeline",),
    summary="Update prices, filings, and derived tables. Operators and administrators only.",
    intro="These endpoints change the shared market data, so they need the operator or administrator role.",
    groups=(
        Group("Run steps", ("POST /api/pipeline/run", "/api/steps/*", "GET /api/config")),
        Group("Pipeline jobs", ("/api/jobs/*",)),
        Group("Saved setups", ("/api/admin/pipeline-setups/*",), "Named sets of steps and settings that schedules run (administrators)."),
        Group("System status", ("GET /api/system/status",)),
    ),
    guides=(
        Guide(
            "Run pipeline steps",
            "`GET /api/steps` lists the steps and their settings. `POST /api/pipeline/run` queues the steps you name and returns a job; "
            "follow it with `GET /api/jobs/{job_id}` and, once it has ended, read each step's result with `GET /api/jobs/{job_id}/output`.",
            curl=_curl("/api/steps") + "\n" + _curl("/api/pipeline/run", body='{"config": {}, "steps": [{"name": "update_stock_prices", "overwrite": false}]}'),
            python=(
                'job = call("POST", "/api/pipeline/run", json={"config": {}, "steps": [{"name": "update_stock_prices", "overwrite": False}]})\n'
                'while call("GET", f"/api/jobs/{job[\'job_id\']}")["status"] in ("pending", "running", "cancelling"):\n'
                "    time.sleep(5)\n"
                '# `output` maps each completed step to its result.\n'
                'for step, result in call("GET", f"/api/jobs/{job[\'job_id\']}/output")["output"].items():\n'
                '    print(step, result)'
            ),
        ),
    ),
    related=("GET /api/admin/pipeline-schedules", "GET /api/workers/status"),
)

FILINGS = Section(
    id="filings",
    title="Filings",
    screen_paths=("/filings", "/filings/:docId"),
    summary="EDINET filings: find them, read their statements and sections, translate, and trace figures to their source.",
    intro="Filings are identified by their EDINET document id (`S100Y8NY` is Toyota's annual report for the year to March 2026).",
    groups=(
        Group("Find filings", ("GET /api/filings", "/api/filings/company/*", "GET /api/filings/coverage")),
        Group("Read a filing", ("/api/filings/*",)),
        Group(
            "Translation",
            ("POST /api/filings/translate", "GET /api/filings/{doc_id}/translate-body", "GET /api/filings/{doc_id}/sections-translated"),
            "Japanese text in English; a worker runs the translation engine, and translations are cached.",
        ),
        Group(
            "Data quality and provenance",
            ("/api/filings/data-quality/*", "/api/filings/provenance/*", "GET /api/filings/{doc_id}/quality", "GET /api/filings/{doc_id}/parse-runs"),
        ),
        Group(
            "Download and archive",
            ("POST /api/filings/acquire", "POST /api/filings/xbrl-backfill", "GET /api/filings/xbrl-eligible"),
            "Fetch archives from EDINET (operators and administrators).",
        ),
    ),
    guides=(
        Guide(
            "List a company's filings",
            "`GET /api/filings` lists filings newest first; `company_code` narrows it to one company and `form` to EDINET form codes.",
            curl=_curl("/api/filings?company_code=E02144&limit=5"),
            python='for filing in call("GET", "/api/filings", params={"company_code": "E02144", "limit": 5})["filings"]:\n    print(filing["doc_id"], filing["period_end"], filing["company_name"])',
            response='{"filings": [{"doc_id": "S100Y8NY", "edinet_code": "E02144", "company_name": "TOYOTA MOTOR CORPORATION", "period_start": "2025-04-01", "period_end": "2026-03-31", "form_code": "030000", "status": "parsed", …}], "limit": 5, "offset": 0}',
        ),
        Guide(
            "Read a filing's statements",
            "`GET /api/filings/{doc_id}/statement-tables` returns every statement and note table as the filing structures it: "
            "each statement has its periods and rows of facts.",
            curl=_curl("/api/filings/S100Y8NY/statement-tables"),
            python='tables = call("GET", "/api/filings/S100Y8NY/statement-tables")\nfor statement in tables["statements"][:10]:\n    print(statement["id"], statement["name"])',
            response='{"source": "linkbase", "fact_count": 1378, "statements": [{"id": "rol_BusinessResultsOfGroup", "name": "Business results of group", "periods": [{"key": "D:2025-04-01:2026-03-31", "label": "2026-03-31", …}], …}, …]}',
        ),
    ),
    related=("GET /api/security/search",),
)

COMPARE = Section(
    id="compare",
    title="Compare",
    screen_paths=("/compare",),
    summary="Two to twelve companies side by side: latest values, history, and trends.",
    groups=(
        Group("Compare companies", ("POST /api/comparison/snapshot", "POST /api/comparison/history", "POST /api/comparison/trends", "/api/comparison/*")),
        Group("Choose companies and metrics", ("GET /api/comparison/metrics", "/api/comparison/peers/*", "GET /api/comparison/tags")),
        Group("Saved comparisons", ("/api/research/comparison-templates/*",)),
    ),
    guides=(
        Guide(
            "Compare companies side by side",
            "`POST /api/comparison/snapshot` returns each company's latest metrics (price, market cap, P/E, P/B, margins, returns). "
            "Leave `metrics` empty for the standard set, or name `Table.Column` metrics from `GET /api/comparison/metrics`.",
            curl=_curl("/api/comparison/snapshot", body='{"company_codes": ["E02144", "E01777"], "metrics": []}'),
            python='snapshot = call("POST", "/api/comparison/snapshot", json={"company_codes": ["E02144", "E01777"], "metrics": []})\nfor company in snapshot["companies"]:\n    print(company["company_code"], company["metrics"]["PERatio"], company["metrics"]["ReturnOnEquity"])',
            response='{"companies": [{"company_code": "E02144", "metrics": {"LatestPrice": 3029.0, "MarketCap": 47843017016340.0, "PERatio": 10.26, "PriceToBook": 0.99, "ReturnOnEquity": 0.179, …}, …}, …], "missing": []}',
        ),
        Guide(
            "History and peers",
            "`POST /api/comparison/history` gives each metric's values over the last `periods` years; "
            "`GET /api/comparison/peers/{company_code}` suggests companies of the same industry, closest in size first.",
            curl=_curl("/api/comparison/history", body='{"company_codes": ["E02144", "E01777"], "metrics": [], "periods": 5}') + "\n" + _curl("/api/comparison/peers/E02144?limit=5"),
            python='peers = call("GET", "/api/comparison/peers/E02144", params={"limit": 5})\nprint(peers)',
        ),
    ),
)

RESEARCH = Section(
    id="research",
    title="Research",
    screen_paths=("/research",),
    summary="Your research book: watchlists, notes, theses, tags, alerts, saved screens, reports, and the bond market.",
    intro="Everything here is private to your account.",
    groups=(
        Group("Research book", ("GET /api/research/book", "/api/research/companies/*", "/api/research/*")),
        Group("Watchlists", ("/api/research/watchlists/*",)),
        Group("Notes", ("/api/research/notes/*",)),
        Group("Tags", ("/api/tags/*", "/api/research/tags/*")),
        Group("Alerts", ("/api/research/alerts/*", "/api/research/alert-events/*")),
        Group("Saved screens", ("/api/research/screens/*",)),
        Group("Reports", ("/api/research/report-recipes/*", "/api/reports/*")),
        Group("Bond market", ("GET /api/bonds/market",)),
    ),
    guides=(
        Guide(
            "Keep a watchlist",
            "Create a watchlist, add companies by EDINET code, and read it back in your order.",
            curl=_curl("/api/research/watchlists", body='{"name": "Japan autos"}') + "\n" + _curl("/api/research/watchlists/WATCHLIST_ID/items", body='{"edinet_code": "E02144"}'),
            python=(
                '# Reuse the list if it exists (creating a name twice answers 409).\n'
                'lists = {item["name"]: item for item in call("GET", "/api/research/watchlists")["watchlists"]}\n'
                'watchlist = lists.get("Japan autos") or call("POST", "/api/research/watchlists", json={"name": "Japan autos"})\n'
                'items = f"/api/research/watchlists/{watchlist[\'watchlist_id\']}/items"\n'
                'call("POST", items, json={"edinet_code": "E02144"})\n'
                'print(call("GET", items))'
            ),
        ),
        Guide(
            "Write notes and a thesis",
            "Notes can be about one company (`edinet_code`) or none. The thesis (status, target value, review date) is one record per company.",
            curl=_curl("/api/research/notes", body='{"title": "Margin check", "body": "Operating margin recovered.", "edinet_code": "E02144"}')
            + "\n"
            + _curl("/api/research/companies/E02144", method="PATCH", body='{"thesis_status": "watching", "target_value": 3200, "target_currency": "JPY"}'),
            python=(
                'call("POST", "/api/research/notes", json={"title": "Margin check", "body": "Operating margin recovered.", "edinet_code": "E02144"})\n'
                'print(call("GET", "/api/research/notes", params={"edinet_code": "E02144"})["notes"])'
            ),
        ),
        Guide(
            "Tag companies and set alerts",
            "Tags are named lists of companies (`POST /api/tags/{company_code}/{tag}` adds one). "
            "An alert fires when a company's metric crosses a value; the Research screen shows today's status for each.",
            curl=_curl("/api/tags/E02144/value", method="POST") + "\n" + _curl("/api/research/alerts", body='{"name": "Toyota cheap", "edinet_code": "E02144", "metric": "PER", "operator": "<", "value": 8}'),
            python='call("POST", "/api/tags/E02144/value")\nprint(call("GET", "/api/tags"))\nprint(call("GET", "/api/research/alerts"))',
        ),
    ),
    related=("GET /api/bonds/bond/{bond_id}", "GET /api/research/recent-work"),
)

CHAT = Section(
    id="chat",
    title="Chat",
    screen_paths=("/chat",),
    summary="Company and topic channels, direct and group conversations, and live updates.",
    intro=(
        "Channel ids are `company:<EDINET code>` for a company's channel or a topic's id (see `GET /api/chat/channels`). "
        "Channel messages are plain text through the API. Direct and group conversations are end-to-end encrypted: "
        "the server only stores ciphertext, so your app must decrypt them (see the guide below)."
    ),
    groups=(
        Group("Summary and live updates", ("GET /api/chat/summary", "GET /api/chat/unread", "GET /api/chat/updates", "GET /api/chat/feed", "/api/chat/*")),
        Group(
            "Channels",
            ("/api/chat/channels/*", "GET /api/chat/resolve", "GET /api/chat/companies/{edinet_code}/mentions", "DELETE /api/chat/messages/{message_id}"),
        ),
        Group("Encrypted conversations", ("/api/chat/conversations/*", "/api/chat/keys/*")),
        Group("Blocking", ("/api/chat/blocks/*",)),
    ),
    guides=(
        Guide(
            "Post to a company channel and read it",
            "Write `$7203` or `$E02144` in a message to reference a company. "
            "Messages come back oldest first; page further back with `before` (a `seq`).",
            curl=_curl("/api/chat/channels/company:E02144/messages", body='{"text": "Results look strong this quarter, $7203."}')
            + "\n"
            + _curl("/api/chat/channels/company:E02144/messages?limit=20"),
            python=(
                'call("POST", "/api/chat/channels/company:E02144/messages", json={"text": "Results look strong this quarter."})\n'
                'for message in call("GET", "/api/chat/channels/company:E02144/messages", params={"limit": 20})["messages"]:\n'
                '    print(message["sender"]["username"], message["body"]["text"])'
            ),
            response='{"messages": [{"seq": 12, "message_id": "…", "channel_id": "company:E02144", "sender": {"user_id": "…", "username": "alice"}, "body": {"text": "Results look strong this quarter.", "refs": {}}, "created_at": "…"}], "has_more": false}',
        ),
        Guide(
            "Wait for new messages",
            "`GET /api/chat/updates` is a long poll: it answers as soon as something new arrives after `since`, "
            "or with nothing after `wait` seconds. Send back the `cursor` you received as the next `since`.",
            curl=_curl("/api/chat/updates?since=0&wait=25"),
            python='since = 0\nfor _ in range(3):\n    update = call("GET", "/api/chat/updates", params={"since": since, "wait": 25})\n    since = update.get("cursor", since)\n    print(update)',
        ),
        Guide(
            "Read and send encrypted messages",
            "Conversations use ECDH P-256 identity keys and AES-256-GCM. Your private key is stored wrapped under your chat passphrase "
            "(PBKDF2-SHA-256), so your app needs that passphrase. Each conversation key version is wrapped for you under "
            "HKDF-SHA-256 of the ECDH secret between your key and the wrapper's. All strings are base64, and every encryption binds "
            "associated data, so the exact labels below matter. This example needs `pip install cryptography`.",
            python=(
                "import base64, json, os, unicodedata, uuid\n"
                "from cryptography.hazmat.primitives import hashes, serialization\n"
                "from cryptography.hazmat.primitives.asymmetric import ec\n"
                "from cryptography.hazmat.primitives.ciphers.aead import AESGCM\n"
                "from cryptography.hazmat.primitives.kdf.hkdf import HKDF\n"
                "from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC\n"
                "\n"
                'P, b64, b64e = "shade-chat-v1", base64.b64decode, lambda raw: base64.b64encode(raw).decode()\n'
                "\n"
                "def unlock(passphrase):\n"
                '    key = call("GET", "/api/chat/keys/me")["key"]\n'
                '    kek = PBKDF2HMAC(hashes.SHA256(), 32, b64(key["wrap_salt"]), key["wrap_iterations"])\n'
                '    secret = kek.derive(unicodedata.normalize("NFKC", passphrase).encode())\n'
                '    pkcs8 = AESGCM(secret).decrypt(b64(key["wrap_iv"]), b64(key["wrapped_private_key"]), f"{P}|identity".encode())\n'
                "    return serialization.load_der_private_key(pkcs8, None)\n"
                "\n"
                "def conversation_keys(conversation_id, private_key):\n"
                '    data = call("GET", f"/api/chat/conversations/{conversation_id}/keys")\n'
                '    public = {item["key_id"]: item["public_key"] for item in data["wrapper_keys"]}\n'
                "    keys = {}\n"
                '    for wrap in data["wraps"]:\n'
                '        peer = serialization.load_der_public_key(b64(public[wrap["wrapper_key_id"]]))\n'
                '        info = f"{P}|wrap|conv:{conversation_id}|v:{wrap[\'key_version\']}|from:{wrap[\'wrapper_key_id\']}|to:{wrap[\'recipient_key_id\']}".encode()\n'
                "        kek = HKDF(hashes.SHA256(), 32, P.encode(), info).derive(private_key.exchange(ec.ECDH(), peer))\n"
                '        keys[wrap["key_version"]] = AESGCM(kek).decrypt(b64(wrap["iv"]), b64(wrap["wrapped_key"]), info)\n'
                "    return keys\n"
                "\n"
                "def aad(conversation_id, version, sender_id, message_id):\n"
                '    return f"{P}|message|conv:{conversation_id}|v:{version}|sender:{sender_id}|id:{message_id}".encode()\n'
                "\n"
                'me = call("GET", "/api/auth/me")\n'
                'conversation_id = call("GET", "/api/chat/conversations")["conversations"][0]["conversation_id"]\n'
                'keys = conversation_keys(conversation_id, unlock(os.environ["SHADE_CHAT_PASSPHRASE"]))\n'
                'for message in call("GET", f"/api/chat/conversations/{conversation_id}/messages")["messages"]:\n'
                '    sealed = message.get("encrypted")\n'
                "    if sealed:\n"
                '        plain = AESGCM(keys[sealed["key_version"]]).decrypt(b64(sealed["nonce"]), b64(sealed["ciphertext"]),\n'
                '                                                            aad(conversation_id, sealed["key_version"], message["sender"]["user_id"], message["message_id"]))\n'
                '        print(message["sender"]["username"], json.loads(plain)["text"])\n'
                "\n"
                "version, message_id, nonce = max(keys), str(uuid.uuid4()), os.urandom(12)\n"
                'body = json.dumps({"text": "Sent from my script", "refs": {}}).encode()\n'
                'sealed = AESGCM(keys[version]).encrypt(nonce, body, aad(conversation_id, version, me["user_id"], message_id))\n'
                'call("POST", f"/api/chat/conversations/{conversation_id}/messages",\n'
                '     json={"message_id": message_id, "ciphertext": b64e(sealed), "nonce": b64e(nonce), "key_version": version})'
            ),
        ),
    ),
    related=("GET /api/profiles", "GET /api/profiles/{username}"),
)

PEOPLE = Section(
    id="people",
    title="People",
    screen_paths=("/people/:username",),
    summary="Public profiles: who people are, what they follow, and whether they take messages.",
    groups=(Group("Profiles", ("/api/profiles/*",)),),
    guides=(
        Guide(
            "Look someone up",
            "`GET /api/profiles?q=` searches by name; `GET /api/profiles/{username}` returns one profile. "
            "`PATCH /api/profiles/me` changes yours.",
            curl=_curl("/api/profiles?q=ali") + "\n" + _curl("/api/profiles/alice"),
            python='for person in call("GET", "/api/profiles", params={"q": "ali"})["people"]:\n    print(person["username"], person["display_name"], person["accepts_messages"])',
        ),
    ),
)

ACCOUNT = Section(
    id="account",
    title="Account",
    screen_paths=("/account",),
    summary="Your account, sessions, API tokens, and preferences.",
    groups=(
        Group("Your account", ("GET /api/auth/me", "PATCH /api/auth/me", "POST /api/auth/change-password", "/api/auth/sessions/*", "POST /api/auth/logout")),
        Group("API tokens", ("/api/auth/tokens/*",)),
        Group(
            "Signing in",
            (
                "GET /api/auth/status",
                "POST /api/auth/login",
                "POST /api/auth/refresh",
                "POST /api/auth/register",
                "POST /api/auth/accept-invitation",
                "POST /api/auth/reset-password",
                "/api/auth/*",
            ),
            "Apps should use a personal API token; these are what the sign-in page uses.",
        ),
        Group("Preferences", ("/api/settings/*",)),
    ),
    guides=(
        Guide(
            "Manage your tokens",
            "Create tokens on the Account page. Through the API you can list them (with when each was last used) and revoke one, "
            "for example when a machine is retired. A revoked token stops working at once.",
            curl=_curl("/api/auth/tokens") + "\n" + _curl("/api/auth/tokens/TOKEN_ID", method="DELETE"),
            python='for token in call("GET", "/api/auth/tokens"):\n    print(token["token_id"], token["name"], token["scopes_json"], token["last_used_at"])',
            response='[{"token_id": "…", "name": "Nightly export", "token_prefix": "ed_pat_je88", "scopes_json": "[\\"read\\"]", "created_at": "…", "last_used_at": "…", "expires_at": null, "revoked_at": null}]',
        ),
    ),
)

ADMIN = Section(
    id="admin",
    title="Administration",
    screen_paths=("/admin",),
    summary="Users, invitations, settings, workers, schedules, and the server. Administrators only.",
    intro=(
        "Workers talk to the server through `/api/worker/*` with their own enrollment tokens; that protocol is not part of this API. "
        "Enroll a worker machine with `ShadeWorker enroll` and an enrollment code from `POST /api/admin/workers/enrollments`."
    ),
    groups=(
        Group("Users and invitations", ("/api/admin/auth/*",)),
        Group("Settings", ("/api/admin/settings/*",)),
        Group("Workers", ("/api/admin/workers/*",)),
        Group("Schedules", ("/api/admin/pipeline-schedules/*",)),
        Group("Server and tunnel", ("/api/admin/server/*", "/api/admin/*")),
        Group("Maintenance", ("POST /api/tags/migrate-legacy",)),
    ),
    guides=(
        Guide(
            "Invite someone and manage roles",
            "`POST /api/admin/auth/invitations` returns a one-time invitation token to send to the person. "
            "`PATCH /api/admin/auth/users/{id}/role` sets `admin`, `operator`, or `member`.",
            curl=_curl("/api/admin/auth/invitations", body='{"role": "member", "email": "bob@example.com"}') + "\n" + _curl("/api/admin/auth/users"),
            python='for user in call("GET", "/api/admin/auth/users"):\n    print(user["username"], user["role"], user["status"])',
        ),
        Guide(
            "Read and change settings",
            "`GET /api/admin/settings` lists every setting with its type, current value, and whether it needs a restart; "
            "`PUT /api/admin/settings/{key}` changes one, `DELETE` returns it to the default.",
            curl=_curl("/api/admin/settings") + "\n" + _curl("/api/admin/settings/jobs.retention_hours", method="PUT", body='{"value": 48}'),
            python='settings = call("GET", "/api/admin/settings")\nprint([item["key"] for item in settings["settings"]][:10])',
        ),
    ),
    related=("GET /api/admin/pipeline-setups", "GET /api/jobs", "GET /api/system/status"),
)

SECTIONS: tuple[Section, ...] = (
    START,
    OVERVIEW,
    SCREENING,
    ANALYSIS,
    BACKTEST,
    PORTFOLIO,
    FILINGS,
    COMPARE,
    RESEARCH,
    CHAT,
    PEOPLE,
    JOBS,
    ACCOUNT,
    PIPELINE,
    ADMIN,
)

SECTIONS_BY_ID = {section.id: section for section in SECTIONS}
