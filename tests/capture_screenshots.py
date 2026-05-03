"""Capture web UI screenshots for README."""
import os, sys, threading, time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import uvicorn
from src.web_app.server import app

PORT = 8765
BASE = f"http://127.0.0.1:{PORT}"

# Start server
config = uvicorn.Config(app, host="127.0.0.1", port=PORT, log_level="error")
server = uvicorn.Server(config)
t = threading.Thread(target=server.run, daemon=True)
t.start()

# Wait for ready
for _ in range(30):
    try:
        import urllib.request
        urllib.request.urlopen(f"{BASE}/health", timeout=0.5)
        break
    except Exception:
        time.sleep(0.3)
else:
    print("Server did not start")
    sys.exit(1)

print(f"Server running at {BASE}")

from playwright.sync_api import sync_playwright

OUT = os.path.join(ROOT, "docs", "images")
os.makedirs(OUT, exist_ok=True)

views = [
    ("/", "web-dashboard.png"),
    ("/orchestrator", "web-orchestrator.png"),
    ("/screening", "web-screening.png"),
    ("/security", "web-security-analysis.png"),
]

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1280, "height": 800})
    for path, fname in views:
        url = f"{BASE}{path}"
        print(f"Capturing {url} -> {fname}")
        page.goto(url, wait_until="networkidle")
        page.wait_for_timeout(1500)
        filepath = os.path.join(OUT, fname)
        page.screenshot(path=filepath)
        size = os.path.getsize(filepath)
        print(f"  saved ({size} bytes)")
    browser.close()

print("All screenshots captured")
server.should_exit = True
