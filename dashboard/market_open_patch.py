"""
dashboard/market_open_patch.py

This file shows the EXACT changes to make in web_app_live.py to fix the
hardcoded "market_open": True issue (Section 4).

SEARCH for these patterns in web_app_live.py and replace:

────────────────────────────────────────────────────────────────
CHANGE 1 — Add import at the top of web_app_live.py
────────────────────────────────────────────────────────────────

ADD after other imports:
    from dashboard.market_hours import is_market_open_for_asset, all_market_statuses

────────────────────────────────────────────────────────────────
CHANGE 2 — Replace hardcoded market_open in /api/assets or any
           place that returns asset info dicts.
────────────────────────────────────────────────────────────────

BEFORE:
    "market_open": True

AFTER:
    "market_open": is_market_open_for_asset(asset)[0]

Or for full status with reason:
    **is_market_open_for_asset(asset)   # adds market_open + reason

────────────────────────────────────────────────────────────────
CHANGE 3 — Add a dedicated /api/market-status endpoint
────────────────────────────────────────────────────────────────

ADD this route to web_app_live.py:

@app.route("/api/market-status")
def api_market_status():
    from dashboard.market_hours import all_market_statuses
    return jsonify(all_market_statuses())

@app.route("/api/market-status/<path:asset>")
def api_market_status_asset(asset: str):
    from dashboard.market_hours import market_status
    return jsonify(market_status(asset))

────────────────────────────────────────────────────────────────
"""

# This module can also be imported for testing:

from dashboard.market_hours import is_market_open_for_asset


def get_asset_market_open(asset: str) -> bool:
    """Drop-in replacement for hardcoded True."""
    return is_market_open_for_asset(asset)[0]
