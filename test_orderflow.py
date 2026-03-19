"""
test_orderflow.py — Test that order flow data is actually flowing.
Run while bot is running:  python test_orderflow.py
"""
import requests, json, time, sys

BASE = "http://localhost:5000"

def chk(ok): return "\033[92m✓\033[0m" if ok else "\033[91m✗\033[0m"
def warn(ok): return "\033[93m⚠\033[0m" if not ok else "\033[92m✓\033[0m"

print(f"\n{'─'*60}")
print(f"  Order Flow Data Test — {BASE}")
print(f"{'─'*60}\n")

# 1. Check imbalance endpoint returns data
print("1. Imbalance scores:")
try:
    r = requests.get(f"{BASE}/api/phase3/imbalance", timeout=10)
    d = r.json()
    imbalances = d.get("imbalances", {})
    all_zero = all(v == 0.0 for v in imbalances.values())
    has_data = bool(imbalances)

    for asset, score in imbalances.items():
        nonzero = score != 0.0
        print(f"   {warn(nonzero)} {asset}: {score:+.4f} {'← has data' if nonzero else '← still 0.0 (no WS data yet)'}")

    if all_zero:
        print("\n   ⚠  All scores are 0.0 — Binance WebSocket hasn't delivered")
        print("      order book data yet, OR order_flow._running is False.")
        print("      Wait 30-60s after startup and re-run this test.\n")
    else:
        print(f"\n   ✓ Real imbalance data flowing ({sum(1 for v in imbalances.values() if v != 0)} non-zero assets)\n")

except Exception as e:
    print(f"   {chk(False)} Failed: {e}\n")

# 2. Check order_flow._running via system health
print("2. Phase 3 running status:")
try:
    r = requests.get(f"{BASE}/api/system/health", timeout=10)
    d = r.json()
    phases = d.get("phase_health", {})
    p3 = phases.get("phase3_order_flow", False)
    print(f"   {chk(p3)} phase3_order_flow = {p3}")
    if not p3:
        print("      Order flow module not started. Check bot.py Phase 3 startup.")
except Exception as e:
    print(f"   {chk(False)} Failed: {e}")

# 3. Check Redis ORDER_BOOK_UPDATE channel activity
print("\n3. Redis ORDER_BOOK_UPDATE channel (10s listen):")
try:
    import redis as _redis
    import subprocess, os

    # Read REDIS_URL from .env
    redis_url = "redis://localhost:6379"
    env_path = ".env"
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                if line.startswith("REDIS_URL="):
                    redis_url = line.strip().split("=", 1)[1].strip().strip('"').strip("'")

    r = _redis.from_url(redis_url)
    ps = r.pubsub()
    ps.subscribe("ORDER_BOOK_UPDATE")

    print("   Listening for 10 seconds...")
    count = 0
    deadline = time.time() + 10
    for msg in ps.listen():
        if time.time() > deadline:
            break
        if msg["type"] == "message":
            count += 1
            if count == 1:
                try:
                    data = json.loads(msg["data"])
                    print(f"   ✓ First message — asset: {data.get('asset','?')} bids: {len(data.get('bids',[]))} asks: {len(data.get('asks',[]))}")
                except Exception:
                    print(f"   ✓ First message received (raw)")

    ps.unsubscribe()
    ok = count > 0
    print(f"   {chk(ok)} Received {count} ORDER_BOOK_UPDATE messages in 10s")
    if not ok:
        print("      No messages — Binance WebSocket may not be connected")
        print("      or exchange_stream_manager hasn't published to Redis.\n")

except ImportError:
    print("   ⚠  redis-py not installed — skipping Redis check")
except Exception as e:
    print(f"   {chk(False)} Redis check failed: {e}")

# 4. Walls and stop hunts buffer
print("\n4. Walls and stop hunts (accumulate over time):")
try:
    r = requests.get(f"{BASE}/api/phase3/walls", timeout=5)
    walls = r.json().get("walls", [])
    r2 = requests.get(f"{BASE}/api/phase3/stop-hunts", timeout=5)
    hunts = r2.json().get("hunts", [])
    print(f"   {warn(len(walls)>0)} Liquidity walls buffered: {len(walls)}")
    print(f"   {warn(len(hunts)>0)} Stop hunts buffered: {len(hunts)}")
    if not walls and not hunts:
        print("      These only populate when walls/hunts are actively detected.")
        print("      Normal during low-volatility periods.\n")
except Exception as e:
    print(f"   {chk(False)} Failed: {e}")

print(f"\n{'─'*60}")
print("  If scores are all 0.0 but phase3 is running and Redis has")
print("  messages, wait 1-2 min — it takes time for the order book")
print("  processor to build enough data for imbalance scoring.")
print(f"{'─'*60}\n")
