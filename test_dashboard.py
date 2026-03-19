"""
test_dashboard.py — Dashboard API health check.
Run while bot is running:  python test_dashboard.py
"""
import requests, sys

BASE = "http://localhost:5000"

# (path, expected_keys, timeout_seconds)
TESTS = [
    ("/api/status",                                      ["bot_ready"],                    10),
    ("/api/system-status",                               ["balance", "engine_ready"],      10),
    ("/api/command-center",                              ["balance", "signals"],            30),
    ("/api/chart/assets",                                ["assets"],                       10),
    ("/api/chart/candles?asset=BTC-USD&interval=1d",     ["candles"],                      15),
    ("/api/market/heatmap",                              ["items"],                        20),
    ("/api/correlation-matrix",                          ["labels", "matrix"],             30),
    ("/api/predictions/summary",                         ["predictions", "accuracy"],      10),
    ("/api/accuracy",                                    ["data"],                         10),
    ("/api/whale/summary",                               ["alerts", "total_volume_usd"],   15),
    ("/api/sentiment/dashboard",                         ["composite_score"],              60),
    ("/api/sentiment/by-asset",                          ["assets"],                       60),
    ("/api/market/events",                               [],                               15),
    ("/api/risk/portfolio",                              ["balance", "win_rate"],          10),
    ("/api/strategy/performance",                        [],                               10),
    ("/api/backtest/strategies",                         [],                               10),
    ("/api/phase3/imbalance",                            ["imbalances"],                   10),
    ("/api/phase3/walls",                                ["walls"],                        10),
    ("/api/phase3/stop-hunts",                           ["hunts"],                        10),
    ("/api/phase7/alerts",                               ["alerts"],                       10),
    ("/api/phase7/signal-journal",                       [],                               10),
    ("/api/system/health",                               ["processes", "ram_pct"],         10),
    ("/api/monitoring/snapshot",                         [],                               10),
    ("/api/monitoring/metrics",                          [],                               10),
    ("/api/monitoring/errors",                           [],                               10),
]

PAGES = [
    "/command-center", "/market-intelligence", "/ai-predictions",
    "/whale-intelligence", "/sentiment-intelligence", "/risk-dashboard",
    "/strategy-lab", "/order-flow", "/intelligence-alerts", "/system-monitor",
]

def chk(ok): return "\033[92m✓\033[0m" if ok else "\033[91m✗\033[0m"

print(f"\n{'─'*60}")
print(f"  Dashboard API Health Check — {BASE}")
print(f"{'─'*60}\n")

passed = failed = 0

print("API ENDPOINTS:")
for path, keys, timeout in TESTS:
    url = BASE + path
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code != 200:
            print(f"  {chk(False)} {path}  →  HTTP {r.status_code}")
            failed += 1
            continue
        data = r.json()
        missing = [k for k in keys if k not in data]
        ok = not missing
        print(f"  {chk(ok)} {path}  →  {'OK' if ok else 'missing: ' + str(missing)}")
        passed += ok; failed += not ok
    except requests.exceptions.ConnectionError:
        print(f"  {chk(False)} {path}  →  CONNECTION REFUSED (is bot running?)")
        failed += 1
        break
    except requests.exceptions.Timeout:
        print(f"  {chk(False)} {path}  →  TIMEOUT after {timeout}s (slow external API)")
        failed += 1
    except Exception as e:
        print(f"  {chk(False)} {path}  →  {type(e).__name__}: {e}")
        failed += 1

print("\nPAGE ROUTES:")
for path in PAGES:
    try:
        r = requests.get(BASE + path, timeout=10)
        ok = r.status_code == 200
        print(f"  {chk(ok)} {path}  →  HTTP {r.status_code}")
        passed += ok; failed += not ok
    except Exception as e:
        print(f"  {chk(False)} {path}  →  {e}")
        failed += 1

total = passed + failed
print(f"\n{'─'*60}")
print(f"  Result: {passed}/{total} passed  |  {failed} failed")
print(f"{'─'*60}\n")
sys.exit(0 if failed == 0 else 1)
