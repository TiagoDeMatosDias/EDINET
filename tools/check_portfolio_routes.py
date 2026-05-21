"""
Startup diagnostic: verify portfolio routes are registered on server start.

Run this BEFORE starting the server to verify route registration:
    python tools/check_portfolio_routes.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient
from src.web_app.server import app

client = TestClient(app)

errors = []
ok = 0

# 1. Count portfolio routes
pf_routes = [r for r in app.router.routes if hasattr(r, 'path') and 'portfolio' in str(r.path)]
print(f"Portfolio routes registered: {len(pf_routes)}")
if len(pf_routes) < 10:
    print("  ERROR: Expected at least 10 portfolio routes")
    errors.append("too few routes")

# 2. Test each endpoint
endpoints = [
    ("GET", "/api/portfolio/symbols"),
    ("GET", "/api/portfolio/holdings"),
    ("GET", "/api/portfolio/transactions?limit=5"),
    ("GET", "/api/portfolio/date-range"),
    ("GET", "/api/portfolio/activity-summary"),
    ("POST", "/api/portfolio/rebuild"),
    ("GET", "/api/portfolio/performance?risk_free_rate=0.02"),
    ("GET", "/api/portfolio/risk-free-rate?base_currency=EUR"),
    ("GET", "/api/portfolio/db-path"),
]
for method, path in endpoints:
    resp = getattr(client, method.lower())(path)
    if resp.status_code == 200:
        ok += 1
        print(f"  OK {method} {path}")
    else:
        errors.append(f"{method} {path} -> {resp.status_code}")

# 3. Test upload with real XML
ibkr_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "ibkr")
xml_path = os.path.join(ibkr_dir, "2024.xml")
if os.path.exists(xml_path):
    with open(xml_path, "rb") as f:
        resp = client.post("/api/portfolio/upload", files={"file": ("2024.xml", f.read(), "application/xml")})
    if resp.status_code == 200:
        data = resp.json()
        ok += 1
        print(f"  OK POST /api/portfolio/upload -> inserted={data.get('inserted')}, skipped={data.get('skipped')}")
    else:
        errors.append(f"POST /api/portfolio/upload -> {resp.status_code} {resp.text[:200]}")
else:
    print("  SKIP upload test: 2024.xml not found at", xml_path)

print(f"\nResults: {ok} OK, {len(errors)} errors")
if errors:
    print("ERRORS:")
    for e in errors:
        print(f"  - {e}")
    sys.exit(1)
else:
    print("ALL CHECKS PASSED — server is ready")
