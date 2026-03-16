PHASE 2 INTEGRATION GUIDE — Windows
=====================================
Trading Intelligence Platform Upgrade
Phase 2: Whale Wallet Intelligence Engine

Files in this package
---------------------
whale_intelligence/__init__.py
whale_intelligence/wallet_tracker.py
whale_intelligence/wallet_behavior_classifier.py
whale_intelligence/wallet_database.py
whale_intelligence/wallet_cluster_analyzer.py
tests/test_whale_intelligence.py

════════════════════════════════════════════════════════════
STEP 1 — COPY FILES
════════════════════════════════════════════════════════════

Copy the whale_intelligence/ folder to your project root:

  forex_prediction_bot-main/
    data_ingestion/          ← Phase 1 (already done)
    whale_intelligence/      ← NEW — copy this folder in
      __init__.py
      wallet_tracker.py
      wallet_behavior_classifier.py
      wallet_database.py
      wallet_cluster_analyzer.py
    tests/
      test_whale_intelligence.py   ← NEW — copy into existing tests/ folder

════════════════════════════════════════════════════════════
STEP 2 — OPTIONAL: add API keys to your .env
════════════════════════════════════════════════════════════

ETH wallet tracking requires a free Etherscan key.
BTC tracking uses Blockchair (no key needed, rate-limited).

Add to your .env:

  ETHERSCAN_API_KEY=your_key_here

Get a free key at: https://etherscan.io/myapikey

Without this key, BTC wallets still track — ETH wallets are skipped.

════════════════════════════════════════════════════════════
STEP 3 — WIRE INTO bot.py
════════════════════════════════════════════════════════════

Open bot.py and find the Phase 1 block you added:

  # ── Phase 1 — Institutional data feeds ─────────────────

Add the following block DIRECTLY AFTER it:

    # ── Phase 2 — Whale Wallet Intelligence ──────────────────────────────────
    try:
        from whale_intelligence import start_all as start_whale_intelligence
        start_whale_intelligence()
        logger.info("[bot] Phase 2 whale intelligence started")
    except Exception as e:
        logger.warning(f"[bot] Phase 2 whale intelligence failed to start: {e}")

════════════════════════════════════════════════════════════
STEP 4 — RUN THE TESTS
════════════════════════════════════════════════════════════

Unit tests (no Redis or DB needed):

  pytest tests/test_whale_intelligence.py -v -m "not integration"

Full tests (requires live Redis):

  pytest tests/test_whale_intelligence.py -v

Expected output:
  PASSED tests/test_whale_intelligence.py::TestClassifier::test_accumulator_label
  PASSED tests/test_whale_intelligence.py::TestClassifier::test_distributor_label
  PASSED tests/test_whale_intelligence.py::TestClassifier::test_exchange_override
  PASSED tests/test_whale_intelligence.py::TestClassifier::test_dormant_label
  PASSED tests/test_whale_intelligence.py::TestClassifier::test_insufficient_data
  PASSED tests/test_whale_intelligence.py::TestClusterAnalyzer::test_cluster_fires_at_threshold
  PASSED tests/test_whale_intelligence.py::TestClusterAnalyzer::test_duplicate_address_does_not_inflate_cluster
  ... (11 tests total)

════════════════════════════════════════════════════════════
STEP 5 — VERIFY IT IS WORKING
════════════════════════════════════════════════════════════

Start your bot normally:

  python bot.py

You should see these log lines within ~10 seconds:

  [WalletDB] Tables ready          ← DB connected (or "using in-memory")
  [WalletTracker] Tracking 4 wallets
  [ClusterAnalyzer] Ready

Within ~5 minutes you will see balance checks:

  [WalletTracker] Binance BTC Hot Wallet: balance fetched (X.XXXX BTC)

When a whale moves:

  [WalletTracker] Unknown BTC Whale A: +12.5000 BTC → [WHALE_ACCUMULATION]

════════════════════════════════════════════════════════════
REDIS EVENTS PUBLISHED BY PHASE 2
════════════════════════════════════════════════════════════

Channel                  Published by
WHALE_ACCUMULATION       wallet_tracker
WHALE_DISTRIBUTION       wallet_tracker
EXCHANGE_INFLOW_ALERT    wallet_tracker
EXCHANGE_OUTFLOW_ALERT   wallet_tracker
WHALE_CLUSTER_ALERT      wallet_cluster_analyzer

These events are consumed by your existing layers/layer6_whale.py
(already wired in your bot — no changes needed there).

════════════════════════════════════════════════════════════
ADDING YOUR OWN WALLETS (optional)
════════════════════════════════════════════════════════════

Option A — Edit whale_intelligence/wallet_tracker.py
  Add entries to the SEED_WALLETS list at the top of the file.

Option B — At runtime via Python
  from whale_intelligence import tracker
  tracker.add_wallet(
      address="0xYOUR_ADDRESS",
      label="My Tracked Wallet",
      chain="eth",       # "btc" or "eth"
      wallet_type="unknown"
  )

════════════════════════════════════════════════════════════
TROUBLESHOOTING
════════════════════════════════════════════════════════════

Problem: "[WalletDB] Database unavailable — using in-memory storage"
This is fine. The tracker still works. Wallet history resets on restart.
To persist history, ensure PostgreSQL is running and DATABASE_URL is set.

Problem: "[WalletTracker] BTC fetch 34xp4vRoCGJym3x...: 402"
Blockchair free tier hit rate limit (1 req/sec). The tracker already
sleeps 1s between calls. Wait 60s and it will resume automatically.

Problem: ETH wallets never update
Add ETHERSCAN_API_KEY to your .env — ETH tracking is disabled without it.

════════════════════════════════════════════════════════════
NEXT: PHASE 3 — ORDER FLOW INTELLIGENCE
════════════════════════════════════════════════════════════

Once tests pass and you see wallet tracking in logs,
tell Claude "Phase 2 working, give me Phase 3".