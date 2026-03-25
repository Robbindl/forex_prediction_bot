"""risk/position_sizer.py — JustMarkets/TIOmarkets-accurate position sizer.

Contract specs sourced from TIOmarkets (same model as JustMarkets):
  - BTC: 1 lot = 1 BTC, tick=$0.01
  - ETH: 1 lot = 10 ETH, tick=$0.10
  - SOL: 1 lot = 100 SOL, tick=$0.10
  - XRP: 1 lot = 10,000 XRP, tick=$1.00
  - BNB: 1 lot = 10 BNB, tick=$0.10

Base lot sizes calculated so that a medium realistic move = ~$2,000 P&L
(Gold standard: $100 move at 0.2 lots = $2,000)

Confidence scaling: linear 1.0× → 2.0× from conf 0.62 → 0.90
"""
from __future__ import annotations
from utils.logger import get_logger

logger = get_logger()

MIN_CONF = 0.62   # minimum confidence to trade
MAX_CONF = 0.90   # confidence at which lot size doubles

# ── JustMarkets/TIOmarkets contract specs ────────────────────────────────────
# contract  = coins/units per 1 standard lot
# pip       = minimum price movement
# pip_val   = USD value per pip per 1 standard lot
# base_lots = lots needed so medium move ≈ $2,000 P&L (Gold standard)
MT5_SPECS = {
    # ── FOREX ─────────────────────────────────────────────────────────────────
    # 1 lot = 100,000 units, USD quote pairs = $10/pip/lot
    "EUR/USD": {"contract": 100_000, "pip": 0.0001, "pip_val": 10.00, "base_lots": 4.000},
    "GBP/USD": {"contract": 100_000, "pip": 0.0001, "pip_val": 10.00, "base_lots": 4.000},
    "AUD/USD": {"contract": 100_000, "pip": 0.0001, "pip_val": 10.00, "base_lots": 5.000},
    "GBP/JPY": {"contract": 100_000, "pip": 0.01,   "pip_val":  6.80, "base_lots": 3.676},
    "USD/JPY": {"contract": 100_000, "pip": 0.01,   "pip_val":  6.80, "base_lots": 4.902},
    "USD/CAD": {"contract": 100_000, "pip": 0.0001, "pip_val":  7.50, "base_lots": 5.333},

    # ── COMMODITIES ───────────────────────────────────────────────────────────
    # Gold: 1 lot = 100 oz, $1/pip/lot — $100 move at 0.2 lots = $2,000
    "GC=F":  {"contract": 100,   "pip": 0.01,  "pip_val":  1.00, "base_lots": 0.200},
    # Silver: 1 lot = 5,000 oz, $5/pip/lot
    "SI=F":  {"contract": 5_000, "pip": 0.001, "pip_val":  5.00, "base_lots": 0.800},
    # Oil: 1 lot = 1,000 bbl, $10/pip/lot
    "CL=F":  {"contract": 1_000, "pip": 0.01,  "pip_val": 10.00, "base_lots": 1.000},

    # ── INDICES ───────────────────────────────────────────────────────────────
    # S&P 500: 1 lot = $50/pt, 20 pt move at 2 lots = $2,000
    "^GSPC": {"contract":  50,  "pip": 0.25, "pip_val": 12.50, "base_lots": 2.000},
    # Dow Jones: 1 lot = $5/pt, 200 pt move at 2 lots = $2,000
    "^DJI":  {"contract":   5,  "pip": 1.0,  "pip_val":  5.00, "base_lots": 2.000},
    # Nasdaq: 1 lot = $20/pt, 50 pt move at 2 lots = $2,000
    "^IXIC": {"contract":  20,  "pip": 0.25, "pip_val":  5.00, "base_lots": 2.000},
    # FTSE: 1 lot = £10/pt (~$12.60), 160 pt move at 1 lot = $2,016
    "^FTSE": {"contract":  10,  "pip": 1.0,  "pip_val": 12.60, "base_lots": 0.992},

    # ── CRYPTO (JustMarkets confirmed specs from MT5 Properties screenshots) ────
    # BTC: 1 lot = 1 BTC — $2,000 move at 1.0 lot = $2,000
    "BTC-USD": {"contract":   1,     "pip": 0.01,   "pip_val":  0.01, "base_lots":  1.000},
    # ETH: 1 lot = 1 ETH — $150 move at 13.333 lots = $2,000
    "ETH-USD": {"contract":   1,     "pip": 0.01,   "pip_val":  0.01, "base_lots": 13.333},
    # BNB: 1 lot = 1 BNB (Deriv standard) — $40 move at 50 lots = $2,000
    "BNB-USD": {"contract":   1,     "pip": 0.01,   "pip_val":  0.01, "base_lots": 50.000},
    # SOL: 1 lot = 100 SOL — $10 move at 2.0 lots = $2,000
    "SOL-USD": {"contract": 100,     "pip": 0.01,   "pip_val":  1.00, "base_lots":  2.000},
    # XRP: 1 lot = 1,000 XRP — $0.15 move at 13.333 lots = $2,000
    "XRP-USD": {"contract": 1_000,   "pip": 0.0001, "pip_val":  0.10, "base_lots": 13.333},
}

# Category defaults for any unlisted asset
_DEFAULTS = {
    "forex":       {"contract": 100_000, "pip": 0.0001, "pip_val": 10.00, "base_lots": 4.0},
    "commodities": {"contract": 100,     "pip": 0.01,   "pip_val":  1.00, "base_lots": 0.2},
    "indices":     {"contract":  50,     "pip": 0.25,   "pip_val": 12.50, "base_lots": 2.0},
    "crypto":      {"contract":   1,     "pip": 1.0,    "pip_val":  1.00, "base_lots": 1.0},
}


def _confidence_lots(base_lots: float, confidence: float) -> float:
    """
    Scale lot size linearly with confidence.
    0.62 conf → 1.0× base (minimum)
    0.90 conf → 2.0× base (maximum)
    Above 0.90 → capped at 2.0×
    """
    if confidence <= MIN_CONF:
        factor = 1.0
    elif confidence >= MAX_CONF:
        factor = 2.0
    else:
        factor = 1.0 + (confidence - MIN_CONF) / (MAX_CONF - MIN_CONF)
    return round(base_lots * factor, 3)


class PositionSizer:
    """
    JustMarkets/TIOmarkets-accurate position sizer with confidence scaling.

    Position size = confidence_scaled_lots × contract_size
    P&L = price_change × position_size (in units)

    Gold standard:
      0.2 base lots → 20 oz → $100 move = $2,000
      At max confidence (0.90): 0.4 lots → 40 oz → $100 move = $4,000
    """

    def __init__(self, account_balance: float):
        self.account_balance = account_balance

    def calculate(
        self,
        entry_price: float,
        stop_loss: float,
        category: str = "forex",
        confidence: float = 0.7,
        asset: str = "",
    ) -> float:
        """
        Returns position size in base asset units (coins/oz/contracts).

        FIX CRITICAL: MT5_SPECS are calibrated for a $10,000+ account.
        On a $30 account, raw specs produce absurd notional exposure:
          EUR/USD: 400,000 units = $432,000 notional (14,400× leverage)
          BTC:     1.0 BTC       = $80,000 notional  (2,666× leverage)

        Fix: multiply base_lots by (account_balance / REFERENCE_BALANCE)
        so a $30 account gets 0.003× the lot size of a $10,000 account.
        This keeps the P&L-per-move proportional to actual account size.
        A $100 Gold move on a $30 account now gives ~$0.60 P&L, not $2,000.
        """
        if not entry_price:
            return 0.0

        spec      = MT5_SPECS.get(asset) or _DEFAULTS.get(category, _DEFAULTS["forex"])
        base_lots = spec["base_lots"]
        contract  = spec["contract"]
        pip_val   = spec["pip_val"]
        pip       = spec["pip"]

        # FIX: Scale lot size proportionally to actual account balance.
        # REFERENCE_BALANCE = 10_000 is the account size the specs were
        # calibrated for.  A $30 account gets factor = 30/10000 = 0.003.
        REFERENCE_BALANCE = 10_000.0
        balance_factor    = max(0.0001, self.account_balance / REFERENCE_BALANCE)
        scaled_base_lots  = base_lots * balance_factor

        lots = _confidence_lots(scaled_base_lots, confidence)
        size = lots * contract

        if entry_price and stop_loss:
            sl_pips  = abs(entry_price - stop_loss) / pip
            risk_usd = sl_pips * pip_val * lots
            tp_pips  = sl_pips * 1.5
            tp_usd   = tp_pips * pip_val * lots
            logger.debug(
                f"[PositionSizer] {asset} bal=${self.account_balance:.2f} "
                f"factor={balance_factor:.4f} conf={confidence:.3f} → "
                f"{lots:.4f} lots ({size:.4f} units) | "
                f"SL={sl_pips:.0f} pips risk=${risk_usd:.2f} | TP≈${tp_usd:.2f}"
            )

        return round(size, 6)

    @staticmethod
    def pnl(asset: str, category: str,
            entry: float, current: float,
            size: float, direction: str) -> float:
        """
        MT5-accurate P&L using pip-based calculation.
        Handles cross pairs (JPY, CAD) correctly.

        Formula: lots × (price_diff / pip_size) × pip_val_usd
        This correctly converts JPY/CAD pip values to USD.

        For USD-quoted pairs (EUR/USD, Gold, Crypto):
          pip_val = $10/lot (forex) or asset-specific
          price_diff / pip_size = number of pips
          P&L = lots × pips × pip_val ✅

        For cross pairs (GBP/JPY):
          pip_val = $6.80/lot (already in USD)
          P&L = lots × pips × 6.80 ✅
        """
        spec = MT5_SPECS.get(asset) or _DEFAULTS.get(category, _DEFAULTS["forex"])
        pip_size = spec["pip"]
        pip_val  = spec["pip_val"]
        contract = spec["contract"]

        # Derive lots from size
        lots = size / contract if contract > 0 else size

        price_diff = (current - entry) if direction == "BUY" else (entry - current)
        pips = price_diff / pip_size
        return round(lots * pips * pip_val, 2)


# Convenience function used in tests
def get_lot_size(asset: str, confidence: float = 0.70) -> float:
    spec = MT5_SPECS.get(asset) or _DEFAULTS.get("forex")
    return _confidence_lots(spec["base_lots"], confidence)
