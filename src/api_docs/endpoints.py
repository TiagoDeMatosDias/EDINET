"""Per-endpoint descriptions and examples for the API docs.

The docs take each endpoint's summary from its docstring. An entry here
supplies one where the docstring is missing, adds notes, and gives example
parameter values or a request body where the generated example (required
fields filled with sample values) would not show a useful call.
``tests/unit/test_api_docs.py`` checks that every documented endpoint has a
summary and that every JSON example body validates against the endpoint.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


class _Unset:
    def __repr__(self) -> str:
        return "UNSET"


#: ``EndpointDoc.body`` when the example body is generated from the schema.
UNSET: Any = _Unset()


@dataclass(frozen=True)
class EndpointDoc:
    summary: str | None = None
    notes: str | None = None
    #: Example values for path and query parameters (optional ones included).
    path: Mapping[str, Any] | None = None
    query: Mapping[str, Any] | None = None
    #: Example JSON body; ``UNSET`` generates one from the schema.
    body: Any = UNSET
    #: Multipart form fields for upload endpoints: name -> value (``@file`` for files).
    form: Mapping[str, str] | None = None
    #: What comes back, for endpoints without a typed response model.
    returns: str | None = None
    #: Response media type when it is not JSON (a download, a stream, HTML).
    content_type: str | None = None
    #: Offer "Send" on the docs page; defaults to GET endpoints only.
    try_it: bool | None = None
    #: The response is a Server-Sent Events stream.
    streams: bool = False
    #: Notes shown only to callers with at least the given role, e.g. ``{"operator": "..."}``.
    role_notes: Mapping[str, str] | None = None
    #: ``False`` leaves out the route's docstring (when it describes other roles' abilities).
    use_docstring: bool = True


D = EndpointDoc

_SCREEN = {
    "criteria": [
        {"table": "Financial_Ratios", "column": "Return on Equity", "operator": ">", "value": 0.15},
        {"table": "ShareMetrics", "column": "Price-earnings ratio", "operator": "BETWEEN", "value": 0, "value2": 12},
    ],
    "columns": ["CompanyInfo.Company_Name", "CompanyInfo.Company_Ticker", "Financial_Ratios.Return on Equity"],
    "sort_by": "Financial_Ratios.Return on Equity",
    "sort_order": "DESC",
}

ENDPOINTS: dict[str, EndpointDoc] = {
    # ── Getting started ────────────────────────────────────────────────
    "GET /health": D(summary="Whether the server is up (`status` is `healthy`); needs no token, so use it to check the address and certificate.", returns="`status`, `version`, and `timestamp`."),
    # ── Account ────────────────────────────────────────────────────────
    "GET /api/auth/status": D(
        summary="Whether sign-in is required (`mode`), registration is open, and the password minimum.",
        returns="`mode` is `accounts` (tokens required) or `disabled` (a local, single-user server: no token needed).",
    ),
    "POST /api/auth/login": D(
        summary="Sign in with a username or email and password; returns a short-lived access token.",
        notes="Apps should use a personal API token instead of storing a password. The access token expires within the hour; the browser renews it through `/api/auth/refresh` with an HttpOnly cookie.",
        body={"login": "alice", "password": "correct horse battery staple"},
        returns="`access_token`, `expires_in` (seconds), and `user`.",
    ),
    "POST /api/auth/refresh": D(
        summary="Exchange the refresh cookie set at sign-in for a new access token (browser sessions only).",
    ),
    "POST /api/auth/logout": D(summary="Sign out: revoke the access token sent and the refresh cookie."),
    "POST /api/auth/register": D(
        summary="Create an account while registration is open; the first account becomes the administrator.",
        body={"username": "alice", "password": "correct horse battery staple", "email": "alice@example.com"},
    ),
    "GET /api/auth/me": D(summary="The signed-in account: id, username, email, role, and status.", returns="Use it to check a token: it answers with the account the token acts as."),
    "PATCH /api/auth/me": D(summary="Change your username or email.", body={"email": "alice@example.com"}),
    "POST /api/auth/change-password": D(
        summary="Change your password. Every other session and every personal API token is revoked.",
        body={"current_password": "old passphrase", "new_password": "new passphrase"},
    ),
    "GET /api/auth/sessions": D(summary="Your browser sessions (refresh and access tokens), including revoked ones."),
    "DELETE /api/auth/sessions/{session_id}": D(summary="Revoke one of your sessions."),
    "GET /api/auth/tokens": D(
        summary="Your personal API tokens (never the secret itself): name, prefix, scopes, last use, expiry.",
    ),
    "POST /api/auth/tokens": D(
        summary="Create a personal API token. The secret is returned once.",
        notes="`scopes` is `[\"*\"]` for full access or `[\"read\"]` for read-only (GET requests only). `expires_at` is optional (ISO 8601). Only a signed-in browser session can create tokens: a request authenticated with a personal token gets 403, so a leaked token cannot mint replacements.",
        body={"name": "Nightly export", "scopes": ["read"], "expires_at": "2027-01-01T00:00:00Z"},
        returns="`token` (starts with `ed_pat_`; store it now) and a `warning`.",
    ),
    "DELETE /api/auth/tokens/{token_id}": D(summary="Revoke one of your personal API tokens; it stops working immediately."),
    "GET /api/settings/hotkeys": D(),
    "PUT /api/settings/hotkeys": D(
        notes="Keys are `<screen>.<action>` hotkey ids; each maps to one to four keys. The answer is the saved overrides; an empty `overrides` restores every default.",
        body={"overrides": {"screening.run": [{"key": "e"}], "screening.save": [{"key": "s", "ctrl": True, "shift": True}]}},
    ),
    "GET /api/profiles/me": D(summary="Your public profile as you edit it, plus the companies you follow."),
    "PATCH /api/profiles/me": D(
        summary="Change your public profile (display name, bio, interests, followed companies, who may message you).",
        body={"display_name": "Alice", "bio": "Value investor", "companies": ["E02144"]},
    ),
    # ── Admin ──────────────────────────────────────────────────────────
    "GET /api/admin/auth/users": D(summary="Every account with its role, status, and last sign-in."),
    "PATCH /api/admin/auth/users/{target_user_id}/role": D(summary="Change an account's role (`admin`, `operator`, or `member`).", body={"role": "operator"}),
    "PATCH /api/admin/auth/users/{target_user_id}/disable": D(summary="Disable an account; its sessions and API tokens stop working at once."),
    "GET /api/admin/auth/audit": D(summary="The security audit log, newest first: sign-ins, token and role changes.", query={"limit": 50}),
    "POST /api/admin/auth/invitations": D(
        summary="Create a one-time invitation; the token is returned once.",
        body={"role": "member", "email": "bob@example.com"},
    ),
    "DELETE /api/admin/auth/invitations/{invitation_id}": D(summary="Withdraw an unused invitation."),
    "POST /api/admin/auth/credential-resets": D(summary="Create a one-time password-reset token for an account.", query={"target_user_id": "TARGET_USER_ID"}),
    "GET /api/admin/auth/settings": D(summary="Registration mode, default role, password minimum, and token lifetimes."),
    "PATCH /api/admin/auth/settings": D(summary="Change registration and password settings.", body={"registration_mode": "closed"}),
    "PUT /api/admin/settings/{key}": D(
        summary="Change one server setting (see `GET /api/admin/settings` for keys, types, and which need a restart).",
        path={"key": "jobs.retention_hours"},
        body={"value": 48},
    ),
    "DELETE /api/admin/settings/{key}": D(path={"key": "jobs.retention_hours"}),
    "GET /api/admin/pipeline-schedules": D(summary="Scheduled pipeline runs with their next and last run."),
    "POST /api/admin/pipeline-schedules": D(
        summary="Schedule a saved pipeline setup to run daily, weekly, or monthly.",
        body={"name": "Nightly prices", "setup_name": "Daily update", "frequency": "daily"},
    ),
    "PATCH /api/admin/pipeline-schedules/{schedule_id}": D(summary="Change or pause a schedule.", body={"enabled": False}),
    "DELETE /api/admin/pipeline-schedules/{schedule_id}": D(summary="Delete a schedule."),
    "POST /api/admin/pipeline-schedules/{schedule_id}/reset-last-run": D(summary="Forget a schedule's last run so it is due again."),
    "POST /api/admin/pipeline-schedules/check": D(summary="Run the scheduler's due check now instead of waiting for its next tick."),
    "GET /api/admin/pipeline-schedules/status": D(summary="Whether the scheduler is running and when it last checked."),
    "GET /api/admin/workers": D(summary="Every worker: status, slots, lanes, data access, and its current jobs."),
    "POST /api/admin/workers/enrollments": D(summary="Create a one-time enrollment code for a new worker machine.", body={"name": "Office PC"}),
    "POST /api/admin/workers/local/restart": D(summary="Restart the worker process the server runs on its own machine."),
    "PATCH /api/admin/workers/{worker_id}": D(summary="Rename, enable or disable a worker, or change its slots.", body={"enabled": False}),
    "DELETE /api/admin/workers/{worker_id}": D(summary="Revoke an enrolled worker; it stops at its next heartbeat."),
    "POST /api/admin/server/shutdown": D(body={"confirm": True}),
    # ── Pipeline ───────────────────────────────────────────────────────
    "POST /api/pipeline/run": D(
        notes="Send JSON `{\"config\": {...}, \"steps\": [...]}` like the Data pipeline page builds (each step `{\"name\": ..., \"overwrite\": false}`; `GET /api/steps` lists names and fields). A step that reads a file takes it as multipart form data instead: the JSON in a `config` field and each file in an `upload:<field>` part.",
        body={"config": {}, "steps": [{"name": "update_stock_prices", "overwrite": False}]},
        returns="`job_id` and `status`; follow it with `GET /api/jobs/{job_id}`.",
    ),
    "GET /api/admin/pipeline-setups": D(summary="The shared, saved pipeline setups (steps and settings) that schedules run."),
    "PUT /api/admin/pipeline-setups/{name}": D(path={"name": "Daily update"}, body={"steps": [{"name": "update_stock_prices", "overwrite": False}], "config": {}}),
    "DELETE /api/admin/pipeline-setups/{name}": D(summary="Delete a saved pipeline setup.", path={"name": "Daily update"}),
    "GET /api/steps/{step_name}": D(path={"step_name": "update_stock_prices"}),
    "GET /api/jobs/{job_id}": D(
        notes="Pipeline jobs report `status` as `pending` (queued), `running`, or `cancelling`, and end as `completed`, `failed`, `cancelled`, or `interrupted`. `steps` lists each step's own status.",
    ),
    "GET /api/jobs/{job_id}/output": D(
        notes="Answers 409 until the job has ended.",
        returns="`output` maps each completed step's name to its result.",
    ),
    # ── Job tray ───────────────────────────────────────────────────────
    "GET /api/my-jobs": D(
        summary="Your recent jobs (screens, backtests, translations, ...) and the workers' free capacity.",
        notes="`active=true` lists only queued and running jobs.",
        role_notes={"operator": "As an operator or administrator you also see pipeline and system jobs."},
        query={"active": True, "limit": 20},
    ),
    "GET /api/my-jobs/{job_id}": D(
        summary="One job: status, progress, and its `result` once it has completed.",
        notes="`status` moves from `open` (queued) to `in_progress` and ends as `completed`, `failed`, `cancelled`, or `interrupted`.",
    ),
    "POST /api/my-jobs/{job_id}/cancel": D(
        summary="Cancel one of your jobs.",
        role_notes={"operator": "As an operator or administrator, `force: true` abandons a job a worker does not release."},
        body={"force": False},
        use_docstring=False,
    ),
    "GET /api/workers/status": D(summary="How many workers are online and their free interactive and batch slots."),
    # ── Overview ───────────────────────────────────────────────────────
    "GET /api/overview": D(
        summary="Everything the Overview screen shows in one request: portfolio summary, research, data freshness.",
    ),
    "GET /api/research/recent-work": D(summary="Your recently opened companies, screens, filings, comparisons, and backtests.", query={"limit": 10}),
    # ── Screening ──────────────────────────────────────────────────────
    "POST /api/screening/run": D(
        notes="Each criterion compares `table`.`column` with `value` using `operator`; `columns` lists `Table.Column` outputs (`GET /api/screening/metrics` lists both). After a 202, poll `GET /api/my-jobs/{job_id}`: the completed job's `result` is the same answer a direct run gives.",
        body=_SCREEN,
        returns="`columns` (names), `rows` (one list per company, in column order), `row_count`, `column_formats` and `error` (null unless the query failed).",
        try_it=True,
    ),
    "POST /api/screening/save": D(body={"name": "Cheap and profitable", "criteria": _SCREEN["criteria"], "columns": _SCREEN["columns"], "overwrite": True}),
    "POST /api/screening/export": D(body={"criteria": _SCREEN["criteria"], "columns": _SCREEN["columns"], "format": "csv"}, content_type="text/csv", returns="A CSV file of the matching companies."),
    "POST /api/screening/history": D(body={"name": "Cheap and profitable", "criteria_count": 2, "result_count": 41}),
    "GET /api/screening/saved/{identifier}": D(path={"identifier": "Cheap and profitable"}),
    "DELETE /api/screening/saved/{identifier}": D(path={"identifier": "Cheap and profitable"}),
    # ── Company analysis ───────────────────────────────────────────────
    "GET /api/security/search": D(summary="Find companies by name, ticker, or EDINET code.", query={"q": "toyota", "limit": 5}),
    "GET /api/security/overview": D(
        query={"company_code": "E02144"},
        returns="Company details (`company`), the latest price, valuation and profitability metrics, and the periods they come from.",
    ),
    "GET /api/security/history": D(query={"company_code": "E02144", "periods": 10}),
    "GET /api/security/price-history": D(summary="Daily prices of a ticker; `adjusted=true` adjusts for splits.", query={"ticker": "72030", "start_date": "2024-01-01"}),
    "GET /api/security/formulas": D(summary="The derived metrics the Analysis screen computes, with their formulas."),
    "GET /api/security/taxonomy-tree": D(query={"company_code": "E02144", "statement_family": "BS", "periods": 5}),
    "GET /api/security/compare": D(query={"company_codes": "E02144,E01777"}),
    "POST /api/security/update-price": D(body={"ticker": "72030"}),
    "GET /api/bonds/company/{edinet_code}": D(),
    "GET /api/bonds/bond/{bond_id}": D(path={"bond_id": "BOND_ID"}),
    "GET /api/bonds/documents/{doc_id}": D(content_type="application/zip"),
    "GET /api/ownership/entities": D(query={"q": "toyota", "limit": 10}),
    "GET /api/ownership/market-caps": D(query={"codes": "E02144,E01777"}),
    "POST /api/ownership/careers/translate": D(body={"lines": ["1985年4月 当社入社"]}),
    "GET /api/splits/": D(query={"ticker": "72030", "limit": 20}),
    "PUT /api/splits/{split_id}": D(path={"split_id": 1}, body={"confirmation": "confirmed", "reason": "Checked against the company notice"}),
    "POST /api/splits/detect": D(body={"ticker": "72030", "mode": "incremental"}),
    # ── Backtests ──────────────────────────────────────────────────────
    "POST /api/backtesting/run": D(
        notes="`portfolio` maps tickers to an allocation: `{\"mode\": \"weight\", \"value\": 0.5}` (also `shares` or `value`). Waits up to 5 minutes; after that it answers 202 `job_pending`, so prefer `POST /api/backtesting/jobs` for long runs.",
        body={
            "portfolio": {"72030": {"mode": "weight", "value": 0.5}, "67580": {"mode": "weight", "value": 0.5}},
            "start_date": "2019-01-01",
            "end_date": "2024-12-31",
            "benchmark_ticker": "TPX",
            "base_currency": "JPY",
        },
        returns="`id` (the saved result), `summary` (CAGR, volatility, drawdown, Sharpe, ...), `chart_data`, `per_company`, and `yearly_returns`.",
    ),
    "POST /api/backtesting/jobs": D(
        notes="`kind` is `single` (body of `POST /api/backtesting/run`), `csv` (body of `run-from-csv`), or `rolling` (body of `rolling-jobs`). Poll `GET /api/backtesting/jobs/{job_id}` until `status` is `complete`, then load `GET /api/backtesting/result/{result_id}`.",
        body={
            "kind": "single",
            "request": {
                "portfolio": {"72030": {"mode": "weight", "value": 1}},
                "start_date": "2019-01-01",
                "end_date": "2024-12-31",
            },
        },
        returns="The job: `job_id`, `status` (`queued`, `running`, `saving`, `complete`, `failed`, `cancelled`), `progress`, and `result_id` once complete.",
    ),
    "GET /api/backtesting/jobs/{job_id}": D(summary="One of your backtest jobs; `result_id` is set once `status` is `complete`."),
    "POST /api/backtesting/jobs/{job_id}/cancel": D(summary="Cancel one of your backtest jobs."),
    "GET /api/backtesting/rolling-jobs": D(summary="Your rolling-screen backtest jobs from the last 24 hours."),
    "GET /api/backtesting/rolling-jobs/{job_id}": D(summary="One rolling-screen backtest job."),
    "POST /api/backtesting/rolling-jobs/{job_id}/cancel": D(summary="Cancel a rolling-screen backtest job."),
    "POST /api/backtesting/rolling-jobs": D(
        body={
            "criteria": _SCREEN["criteria"],
            "columns": ["CompanyInfo.Company_Name"],
            "cadence": "yearly",
            "durations": ["1yr", "3yr"],
            "max_companies": 20,
            "start_period": "2018-12-01",
            "end_period": "2022-12-01",
        },
    ),
    "POST /api/backtesting/run-rolling": D(
        notes="The answer is a Server-Sent Events stream of `progress` messages ending with `complete`. Quick Cloudflare tunnels do not pass streams; use `POST /api/backtesting/rolling-jobs` and poll instead.",
        body={"criteria": _SCREEN["criteria"], "columns": ["CompanyInfo.Company_Name"], "cadence": "yearly", "durations": ["1yr"]},
        content_type="text/event-stream",
        streams=True,
    ),
    "POST /api/backtesting/run-from-csv": D(
        notes="The CSV has the columns `Year`, `Tickers`, `Type` (`weight`, `shares`, or `value`), and `Amount`; each year starts one backtest per duration, from January 1.",
        body={"csv_content": "Year,Tickers,Type,Amount\n2019,72030,weight,0.5\n2019,67580,weight,0.5\n", "durations": ["1yr", "3yr"], "benchmark_ticker": "TPX"},
    ),
    "GET /api/backtesting/download/{backtest_id}": D(content_type="application/zip"),
    "GET /api/backtesting/report/{backtest_id}": D(content_type="text/html"),
    "POST /api/backtesting/export-rolling-xlsx": D(body={"rolling_result": {"runs": []}}, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
    "POST /api/backtesting/export-rolling-zip": D(body={"rolling_result": {"runs": []}}, content_type="application/zip"),
    "GET /api/backtesting/rolling-periods": D(query={"cadence": "yearly", "durations": "1yr,3yr"}),
    # ── Portfolio ──────────────────────────────────────────────────────
    "POST /api/portfolio/upload": D(form={"file": "@flex_statement.xml"}),
    "GET /api/portfolio/transactions": D(query={"symbol": "7203", "limit": 50}),
    "POST /api/portfolio/transactions/manual": D(
        body={"kind": "buy", "trade_date": "2025-04-01", "currency": "JPY", "symbol": "7203", "quantity": 100, "price": 2500},
    ),
    "POST /api/portfolio/transactions/delete": D(body={"source_files": ["manual"], "confirm": False}),
    "DELETE /api/portfolio/transactions/{source_file}": D(path={"source_file": "flex_statement.xml"}),
    "GET /api/portfolio/holdings/at/{date}": D(path={"date": "2024-12-31"}),
    "GET /api/portfolio/performance": D(query={"start_date": "2024-01-01", "base_currency": "JPY"}),
    "GET /api/portfolio/holdings/history": D(query={"start_date": "2024-01-01", "base_currency": "JPY"}),
    "POST /api/portfolio/assets": D(body={"symbol": "GOLD-BAR", "name": "Gold bar 100 g", "currency": "JPY"}),
    "POST /api/portfolio/prices": D(body={"symbol": "GOLD-BAR", "date": "2025-03-31", "price": 1450000, "currency": "JPY"}),
    "DELETE /api/portfolio/prices": D(query={"symbol": "GOLD-BAR", "date": "2025-03-31"}),
    "DELETE /api/portfolio/assets/{symbol}": D(path={"symbol": "GOLD-BAR"}),
    "POST /api/portfolio/backtest/compare": D(body={"portfolio": {"TPX": 1.0}}),
    "POST /api/portfolio/greeks": D(
        body={"positions": [{"option_type": "call", "spot": 2900, "strike": 3000, "years": 0.5, "rate": 0.01, "volatility": 0.25, "quantity": 10}]},
        try_it=True,
    ),
    "POST /api/portfolio/scenarios/evaluate": D(
        notes="Pass the holdings (symbol: quantity) and prices to value; shocks are fractions, so `-0.2` is a 20% fall.",
        body={
            "holdings": {"72030": 100, "AAPL": 10},
            "prices": {"72030": 2900, "AAPL": 230},
            "currencies": {"72030": "JPY", "AAPL": "USD"},
            "price_shocks": {"72030": -0.2},
            "fx_shocks": {"USD": -0.1},
        },
        try_it=True,
    ),
    "POST /api/portfolio/tax-lots": D(
        body={
            "events": [
                {"action": "buy", "ticker": "72030", "date": "2024-04-01", "shares": 100, "price": 3500},
                {"action": "buy", "ticker": "72030", "date": "2024-10-01", "shares": 100, "price": 2600},
                {"action": "sell", "ticker": "72030", "date": "2025-03-31", "shares": 150, "price": 2900},
            ]
        },
        try_it=True,
    ),
    # ── Filings ────────────────────────────────────────────────────────
    "GET /api/filings": D(query={"company_code": "E02144", "limit": 10}),
    "GET /api/filings/company/{edinet_code}": D(summary="A company's retained filings, newest first.", query={"limit": 10}),
    "GET /api/filings/coverage": D(summary="How many filings are stored and parsed, by form and year."),
    "GET /api/filings/{doc_id}": D(summary="One filing's metadata: company, form, period, submission time, and parse status."),
    "GET /api/filings/{doc_id}/artifact": D(summary="The filing's archived EDINET ZIP, as downloaded.", content_type="application/zip"),
    "GET /api/filings/{doc_id}/sections": D(summary="The filing's narrative sections (titles and text); `query` searches them.", query={"query": "dividend", "limit": 20}),
    "GET /api/filings/{doc_id}/sections/{section_id}": D(summary="One narrative section with its full text.", path={"section_id": "SECTION_ID"}),
    "GET /api/filings/{doc_id}/outline": D(summary="The section titles only, in report order."),
    "GET /api/filings/{doc_id}/facts": D(summary="The filing's XBRL facts; `concept` filters by element name.", query={"concept": "NetSales", "limit": 50}),
    "GET /api/filings/{doc_id}/statements": D(summary="Statement facts (same as `/facts`, kept for older clients).", query={"limit": 50}),
    "GET /api/filings/{doc_id}/taxonomy": D(summary="The taxonomy elements the filing uses, with their labels."),
    "GET /api/filings/{doc_id}/audit-reports": D(summary="The auditor's reports in the filing, with their sections."),
    "GET /api/filings/{doc_id}/quality": D(summary="Data-quality issues found while parsing this filing."),
    "GET /api/filings/{doc_id}/parse-runs": D(summary="The latest parse of this filing: when, which parser version, and its outcome."),
    "GET /api/filings/{doc_id}/html/{artifact_id}": D(path={"artifact_id": "ARTIFACT_ID"}, content_type="text/html"),
    "GET /api/filings/{doc_id}/translate-body": D(query={"section_id": "SECTION_ID"}),
    "GET /api/filings/company/{edinet_code}/export": D(content_type="application/zip"),
    "GET /api/filings/data-quality/summary": D(summary="Parse issues across all filings, counted by issue code."),
    "GET /api/filings/data-quality/issues": D(summary="Parse issues, optionally for one filing or one severity.", query={"severity": "error", "limit": 20}),
    "GET /api/filings/data-quality/coverage": D(summary="How many filings have structured facts, sections, and statements."),
    "POST /api/filings/provenance/resolve": D(
        summary="Trace a standardized figure back to the filing and XBRL fact it came from.",
        notes="Traces exist only for figures recorded in the filing catalogue's `observations` table, which no pipeline step fills yet; until then this answers 404 `No matching observation found`.",
        body={"company_code": "E02144", "metric_id": "NetSales", "period_end": "2025-03-31"},
        try_it=True,
    ),
    "POST /api/filings/provenance/resolve-batch": D(
        summary="Trace up to 100 figures to their filings in one request.",
        notes="Each item is a trace or `null`; see the single-figure endpoint for when traces exist.",
        body={"requests": [{"company_code": "E02144", "metric_id": "NetSales", "period_end": "2025-03-31"}]},
        try_it=True,
    ),
    "POST /api/filings/translate": D(body={"texts": ["連結財務諸表", "配当政策"]}),
    "POST /api/filings/acquire": D(body={"doc_id": "S100Y8NY"}),
    "POST /api/filings/xbrl-backfill": D(query={"limit": 10, "doc_type_code": "120"}),
    "GET /api/filings/xbrl-eligible": D(query={"limit": 10}),
    # ── Comparison ─────────────────────────────────────────────────────
    "POST /api/comparison/snapshot": D(
        summary="The latest value of each metric for 2–12 companies, side by side.",
        notes=(
            "`metrics` are `Table.Column` references from `GET /api/comparison/metrics`; leave it empty for the standard set. "
            "A metric starting with `=` is a formula: bracketed columns, numbers, `+ - * /` and parentheses, "
            "such as `=[Stock_Prices.Price] * [ShareMetrics.Number of issued shares as of filing date]`. "
            "`Stock_Prices.Price` is the latest split-adjusted price; a missing value or a division by zero gives `null`."
        ),
        body={"company_codes": ["E02144", "E01777"], "metrics": ["PERatio", "=[ShareMetrics.Basic earnings (loss) per share] / [Stock_Prices.Price]"]},
        returns=(
            "`companies` (each with `metrics` keyed by metric), `metrics` (the ones kept; unknown ones are dropped), "
            "`missing` codes, and `metric_definitions`: each metric's `label`, `group` and, where it has one, a display `format` such as `percent`."
        ),
        try_it=True,
    ),
    "POST /api/comparison/history": D(
        summary="Each metric's values over the last `periods` fiscal years for 2–12 companies.",
        body={"company_codes": ["E02144", "E01777"], "metrics": [], "periods": 5},
        try_it=True,
    ),
    "POST /api/comparison/trends": D(body={"company_codes": ["E02144", "E01777"], "metrics": [], "periods": 5}, try_it=True),
    "GET /api/comparison/peers": D(query={"codes": "E02144", "limit": 5}),
    "GET /api/comparison/peers/{company_code}": D(summary="Listed companies in the company's industry, closest in market cap first.", query={"limit": 5}),
    "GET /api/research/comparison-templates": D(summary="Your saved comparisons (companies and metrics)."),
    "POST /api/research/comparison-templates": D(summary="Save a comparison to reopen later.", body={"name": "Autos", "company_codes": ["E02144", "E01777"], "metrics": []}),
    "DELETE /api/research/comparison-templates/{template_id}": D(summary="Delete a saved comparison."),
    # ── Research ───────────────────────────────────────────────────────
    "GET /api/research/watchlists": D(summary="Your watchlists."),
    "POST /api/research/watchlists": D(summary="Create a watchlist.", body={"name": "Japan autos"}),
    "PATCH /api/research/watchlists/{watchlist_id}": D(summary="Rename a watchlist.", body={"name": "Autos and parts"}),
    "DELETE /api/research/watchlists/{watchlist_id}": D(summary="Delete a watchlist (its companies are not affected)."),
    "GET /api/research/watchlists/{watchlist_id}/items": D(summary="The companies on a watchlist, in your order."),
    "POST /api/research/watchlists/{watchlist_id}/items": D(summary="Add a company to a watchlist.", body={"edinet_code": "E02144"}),
    "DELETE /api/research/watchlists/{watchlist_id}/items/{edinet_code}": D(summary="Remove a company from a watchlist."),
    "PATCH /api/research/watchlists/{watchlist_id}/members/reorder": D(summary="Set the order of a watchlist's companies.", body={"ordered_codes": ["E01777", "E02144"]}),
    "GET /api/research/notes": D(summary="Your notes, optionally for one company.", query={"edinet_code": "E02144"}),
    "POST /api/research/notes": D(summary="Write a note, optionally about a company.", body={"title": "Margin check", "body": "Operating margin recovered after the yen moved.", "edinet_code": "E02144"}),
    "PATCH /api/research/notes/{note_id}": D(
        summary="Edit a note. Send the `version` you read as `expected_version` to refuse overwriting someone else's newer edit (409).",
        body={"title": "Margin check", "body": "Updated after the Q3 results.", "expected_version": 1},
    ),
    "DELETE /api/research/notes/{note_id}": D(summary="Delete a note."),
    "GET /api/research/companies/{edinet_code}": D(summary="Your thesis for a company: status, target value, review date, and notes."),
    "PATCH /api/research/companies/{edinet_code}": D(
        summary="Set your thesis status, target value, and review date for a company.",
        body={"thesis_status": "watching", "target_value": 3200, "target_currency": "JPY", "review_on": "2026-01-15"},
    ),
    "GET /api/research/tags/{edinet_code}": D(summary="Your tags on one company."),
    "PUT /api/research/tags/{edinet_code}": D(summary="Replace your tags on one company (position tags are kept).", body={"tags": ["value", "watch"]}),
    "GET /api/research/screens": D(summary="Your saved screens with their full definitions."),
    "POST /api/research/screens": D(summary="Save a screen definition under a name.", body={"name": "Cheap and profitable", "definition": {"criteria": _SCREEN["criteria"], "columns": _SCREEN["columns"]}}),
    "DELETE /api/research/screens/{screen_id}": D(summary="Delete a saved screen."),
    "GET /api/research/alerts": D(summary="Your alerts with today's status."),
    "POST /api/research/alerts": D(
        summary="Create an alert that fires when a company's metric crosses a value.",
        body={"name": "Toyota cheap", "edinet_code": "E02144", "metric": "PER", "operator": "<", "value": 8},
    ),
    "DELETE /api/research/alerts/{alert_id}": D(summary="Delete an alert."),
    "POST /api/research/alerts/{alert_id}/evaluate": D(
        summary="Test an alert against values you supply; a hit is recorded as an event.",
        body={"values": {"PER": 7.5}},
    ),
    "GET /api/research/alerts/{alert_id}/events": D(summary="When an alert fired and with which values."),
    "PATCH /api/research/alert-events/{event_id}/acknowledge": D(summary="Mark an alert event as seen."),
    "GET /api/research/report-recipes": D(summary="Your saved report recipes."),
    "POST /api/research/report-recipes": D(summary="Save a report recipe (the definition a report run uses).", body={"name": "Quarterly review", "definition": {"companies": ["E02144"]}}),
    "DELETE /api/research/report-recipes/{recipe_id}": D(summary="Delete a report recipe."),
    "GET /api/reports/runs": D(summary="Your generated research reports."),
    "POST /api/reports/runs": D(
        summary="Generate a reproducible research report ZIP from a recipe or a definition.",
        body={"name": "Quarterly review", "definition": {"companies": ["E02144"]}},
    ),
    "GET /api/reports/runs/{run_id}": D(summary="One report run: status, size, and checksum."),
    "GET /api/reports/runs/{run_id}/manifest": D(summary="The report's manifest: inputs, data versions, and file checksums."),
    "GET /api/reports/runs/{run_id}/download": D(summary="Download the report ZIP.", content_type="application/zip"),
    "DELETE /api/reports/runs/{run_id}": D(summary="Delete a report run and its ZIP."),
    "POST /api/tags": D(summary="Create an empty tag (a named list of companies).", body={"name": "value"}),
    "PATCH /api/tags/{tag_name}": D(summary="Rename a tag or change its colour.", body={"name": "deep value"}),
    "DELETE /api/tags/{tag_name}": D(summary="Delete a tag from every company."),
    "GET /api/tags/{tag_name}/companies": D(summary="The companies carrying a tag."),
    "GET /api/bonds/market": D(query={"include_group": False}),
    # ── Chat ───────────────────────────────────────────────────────────
    "GET /api/chat/channels/{channel_id}": D(summary="A channel's details and whether you follow it."),
    "GET /api/chat/channels/{channel_id}/messages": D(summary="A channel's messages, newest last; `before` pages back by sequence number.", query={"limit": 50}),
    "POST /api/chat/channels/{channel_id}/messages": D(
        summary="Post to a channel. Channel messages are public to members and stored encrypted at rest.",
        notes="Write `$7203` or `$E02144` in `text` to reference a company. At most 20 messages per 10 seconds.",
        body={"text": "Results look strong this quarter."},
    ),
    "POST /api/chat/channels/{channel_id}/read": D(summary="Mark a channel read up to a message sequence number.", body={"seq": 120}),
    "PUT /api/chat/channels/{channel_id}/subscription": D(summary="Follow a channel: its messages appear in your feed and unread counts."),
    "DELETE /api/chat/channels/{channel_id}/subscription": D(summary="Stop following a channel."),
    "DELETE /api/chat/messages/{message_id}": D(
        summary="Delete your own message.",
        role_notes={"admin": "As an administrator you may also remove other people's messages in company channels."},
        use_docstring=False,
    ),
    "GET /api/chat/updates": D(query={"since": 0, "wait": 25}),
    "GET /api/chat/feed": D(query={"limit": 50}),
    "GET /api/chat/resolve": D(query={"tokens": "$7203,$E01777"}),
    "GET /api/chat/blocks": D(summary="The people and channels you have blocked."),
    "DELETE /api/chat/blocks/{kind}/{target_id}": D(summary="Unblock a person (`user`) or channel (`channel`).", path={"kind": "user", "target_id": "USER_ID"}),
    "PUT /api/chat/blocks/{kind}/{target_id}": D(path={"kind": "user", "target_id": "USER_ID"}),
    "GET /api/chat/conversations": D(summary="Your direct and group conversations with unread counts and key versions."),
    "POST /api/chat/conversations": D(
        summary="Start a direct (`dm`) or group conversation; you send its first key, wrapped for every member.",
    ),
    "GET /api/chat/conversations/{conversation_id}": D(summary="A conversation's members, title, and current key version."),
    "PATCH /api/chat/conversations/{conversation_id}": D(summary="Rename a group or hide a conversation from your list.", body={"hidden": True}),
    "GET /api/chat/conversations/{conversation_id}/messages": D(summary="A conversation's encrypted messages (`ciphertext`, `nonce`, `key_version`), newest last.", query={"limit": 50}),
    "POST /api/chat/conversations/{conversation_id}/messages": D(summary="Send an encrypted message (see Encrypted conversations above)."),
    "POST /api/chat/conversations/{conversation_id}/read": D(summary="Mark a conversation read up to a message sequence number.", body={"seq": 42}),
    "POST /api/chat/conversations/{conversation_id}/members": D(summary="Invite people to a group, wrapping the keys you hold for them.", body={"user_ids": ["USER_ID"]}),
    "POST /api/chat/conversations/{conversation_id}/invitation": D(summary="Accept or decline an invitation to a group.", body={"accept": True}),
    "GET /api/chat/keys": D(summary="People's public keys, by user ids or key ids (comma-separated).", query={"user_ids": "USER_ID"}),
    "GET /api/profiles": D(query={"q": "ali", "limit": 10}),
    "GET /api/profiles/{username}": D(path={"username": "alice"}),
    # ── API docs ───────────────────────────────────────────────────────
    "GET /api/docs": D(summary="The list of documented screens (this page's sidebar) and the server's sign-in mode."),
    "GET /api/docs/{section_id}": D(summary="One screen's guides and endpoint reference, as shown on its docs page.", path={"section_id": "screening"}),
    "GET /api/docs/certificate": D(
        summary="The server's TLS certificate (PEM), for clients that must trust a self-signed server.",
        content_type="application/x-pem-file",
    ),
}
