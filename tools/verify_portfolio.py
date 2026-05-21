"""Quick verification that the portfolio module and all routes work.

Run: python tools/verify_portfolio.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient
from src.web_app.server import app

client = TestClient(app)

print("=== Verifying Portfolio Routes ===\n")

errors = 0

# Test page
r = client.get("/portfolio")
print(f"{'✓' if r.status_code == 200 else '✗'} GET /portfolio -> {r.status_code}")
if r.status_code != 200: errors += 1

# Test API endpoints
for path in ["/api/portfolio/symbols", "/api/portfolio/holdings",
             "/api/portfolio/transactions?limit=5", "/api/portfolio/date-range",
             "/api/portfolio/activity-summary"]:
    r = client.get(path)
    print(f"{'✓' if r.status_code == 200 else '✗'} GET {path} -> {r.status_code}")
    if r.status_code != 200: errors += 1

# Upload test
ibkr = os.path.join(os.path.dirname(__file__), "..", "data", "ibkr", "2024.xml")
if os.path.exists(ibkr):
    with open(ibkr, "rb") as f:
        r = client.post("/api/portfolio/upload",
                       files={"file": ("2024.xml", f.read(), "application/xml")})
    ok = r.status_code == 200
    print(f"{'✓' if ok else '✗'} POST /api/portfolio/upload -> {r.status_code}")
    if ok:
        data = r.json()
        print(f"    inserted={data['inserted']}, skipped={data['skipped']}")
    else:
        errors += 1

# Rebuild
r = client.post("/api/portfolio/rebuild")
print(f"{'✓' if r.status_code == 200 else '✗'} POST /api/portfolio/rebuild -> {r.status_code}")
if r.status_code == 200:
    print(f"    {r.json()['message']}")
else:
    errors += 1

# Holdings after rebuild
r = client.get("/api/portfolio/holdings")
print(f"{'✓' if r.status_code == 200 else '✗'} GET /api/portfolio/holdings -> {r.status_code} ({len(r.json())} items)")

# Performance
r = client.get("/api/portfolio/performance?risk_free_rate=0.02")
if r.status_code == 200:
    d = r.json()
    print(f"✓ GET /api/portfolio/performance")
    print(f"    sharpe={d.get('sharpe_ratio')}, div={d.get('total_dividend_income'):.2f}, dd={d.get('max_drawdown'):.4f}")

print(f"\n=== {'ALL PASSED' if errors == 0 else f'{errors} ERRORS'} ===")
sys.exit(0 if errors == 0 else 1)
