from __future__ import annotations
import math
from utils.logger import get_logger

logger = get_logger()

MIN_CONF = 0.62   # minimum confidence to trade
BASE_CONF = 0.70  # neutral confidence — base lots should hold here
MAX_CONF = 0.90   # confidence at which lot size doubles
REFERENCE_BALANCE = 10_000.0
DEFAULT_MIN_LOT = 0.01
DEFAULT_LOT_STEP = 0.01
GOLD_STANDARD_LOTS = 0.10
GOLD_STANDARD_ASSET = "XAU/USD"
GOLD_REFERENCE_PRICE = 2300.0
GOLD_REFERENCE_MOVE_PCT = 0.0100

# ── Broker-style symbol specs ────────────────────────────────────────────────
# contract  = coins/units/contracts per 1 standard lot
# pip       = minimum price movement used by the local P&L model
# pip_val   = USD value per pip per 1 standard lot
# base_lots = compatibility fallback when dynamic sizing cannot be derived
#
# These specs are separated from the gold-standard calibration.  The broker
# contract math should reflect symbol mechanics, while the gold-standard layer
# decides what neutral lot size "feels like" across assets.
CONTRACT_SPECS = {
    # ── FOREX ─────────────────────────────────────────────────────────────────
    # Reference lots below come from the user's MT5 screenshots so these assets
    # feel economically closer to XAUUSD 0.10 as the benchmark instrument.
    "EUR/USD": {"contract": 100_000, "pip": 0.0001, "pip_val": 10.00, "base_lots": 1.00, "min_lot": 0.01, "lot_step": 0.01},
    "EUR/JPY": {"contract": 100_000, "pip": 0.01,   "pip_val":  6.80, "base_lots": 1.60, "min_lot": 0.01, "lot_step": 0.01},
    "EUR/GBP": {"contract": 100_000, "pip": 0.0001, "pip_val": 12.50, "base_lots": 1.20, "min_lot": 0.01, "lot_step": 0.01},
    "GBP/USD": {"contract": 100_000, "pip": 0.0001, "pip_val": 10.00, "base_lots": 1.00, "min_lot": 0.01, "lot_step": 0.01},
    "AUD/USD": {"contract": 100_000, "pip": 0.0001, "pip_val": 10.00, "base_lots": 1.00, "min_lot": 0.01, "lot_step": 0.01},
    "NZD/USD": {"contract": 100_000, "pip": 0.0001, "pip_val": 10.00, "base_lots": 1.00, "min_lot": 0.01, "lot_step": 0.01},
    "GBP/JPY": {"contract": 100_000, "pip": 0.01,   "pip_val":  6.80, "base_lots": 1.60, "min_lot": 0.01, "lot_step": 0.01},
    "USD/JPY": {"contract": 100_000, "pip": 0.01,   "pip_val":  6.80, "base_lots": 1.60, "min_lot": 0.01, "lot_step": 0.01},
    "USD/CAD": {"contract": 100_000, "pip": 0.0001, "pip_val":  7.50, "base_lots": 1.40, "min_lot": 0.01, "lot_step": 0.01},
    "USD/CHF": {"contract": 100_000, "pip": 0.0001, "pip_val": 11.00, "base_lots": 1.25, "min_lot": 0.01, "lot_step": 0.01},

    # ── COMMODITIES ───────────────────────────────────────────────────────────
    "XAU/USD": {"contract": 100,   "pip": 0.01,  "pip_val":  1.00, "base_lots": 0.10,   "min_lot": 0.01, "lot_step": 0.01},
    "GC=F":    {"contract": 100,   "pip": 0.01,  "pip_val":  1.00, "base_lots": 0.10,   "min_lot": 0.01, "lot_step": 0.01},
    "XAG/USD": {"contract": 5_000, "pip": 0.001, "pip_val":  5.00, "base_lots": 0.08,   "min_lot": 0.01, "lot_step": 0.01},
    "SI=F":    {"contract": 5_000, "pip": 0.001, "pip_val":  5.00, "base_lots": 0.08,   "min_lot": 0.01, "lot_step": 0.01},
    "WTI":     {"contract": 1_000, "pip": 0.01,  "pip_val": 10.00, "base_lots": 0.22,   "min_lot": 0.01, "lot_step": 0.01},
    "WTI/USD": {"contract": 1_000, "pip": 0.01,  "pip_val": 10.00, "base_lots": 0.22,   "min_lot": 0.01, "lot_step": 0.01},
    "CL=F":    {"contract": 1_000, "pip": 0.01,  "pip_val": 10.00, "base_lots": 0.22,   "min_lot": 0.01, "lot_step": 0.01},

    # ── INDICES ───────────────────────────────────────────────────────────────
    "US500": {"contract":   1,  "pip": 1.0,  "pip_val":  1.00, "base_lots": 10.20, "min_lot": 0.01, "lot_step": 0.01},
    "^GSPC": {"contract":   1,  "pip": 1.0,  "pip_val":  1.00, "base_lots": 10.20, "min_lot": 0.01, "lot_step": 0.01},
    "US30":  {"contract":   1,  "pip": 1.0,  "pip_val":  1.00, "base_lots": 1.44,  "min_lot": 0.01, "lot_step": 0.01},
    "^DJI":  {"contract":   1,  "pip": 1.0,  "pip_val":  1.00, "base_lots": 1.44,  "min_lot": 0.01, "lot_step": 0.01},
    "US100": {"contract":   1,  "pip": 1.0,  "pip_val":  1.00, "base_lots": 2.56,  "min_lot": 0.01, "lot_step": 0.01},
    "^IXIC": {"contract":   1,  "pip": 1.0,  "pip_val":  1.00, "base_lots": 2.56,  "min_lot": 0.01, "lot_step": 0.01},
    "UK100": {"contract":   1,  "pip": 1.0,  "pip_val":  1.26, "base_lots": 2.56,  "min_lot": 0.01, "lot_step": 0.01},
    "^FTSE": {"contract":   1,  "pip": 1.0,  "pip_val":  1.26, "base_lots": 2.56,  "min_lot": 0.01, "lot_step": 0.01},
    "GER40": {"contract":   1,  "pip": 1.0,  "pip_val":  1.15, "base_lots": 1.92,  "min_lot": 0.01, "lot_step": 0.01},
    "AUS200": {"contract":  1,  "pip": 1.0,  "pip_val":  0.72, "base_lots": 4.53,  "min_lot": 0.01, "lot_step": 0.01},
    "JPN225": {"contract": 100,  "pip": 1.0,  "pip_val":  0.65, "base_lots": 1.18,  "min_lot": 0.01, "lot_step": 0.01},

    # ── CRYPTO ────────────────────────────────────────────────────────────────
    "BTC-USD": {"contract":   1,     "pip": 0.01,   "pip_val":  0.01, "base_lots": 0.11,  "min_lot": 0.01, "lot_step": 0.01},
    "ETH-USD": {"contract":   1,     "pip": 0.01,   "pip_val":  0.01, "base_lots": 5.00,  "min_lot": 0.01, "lot_step": 0.01},
    "BNB-USD": {"contract":   1,     "pip": 0.01,   "pip_val":  0.01, "base_lots": 15.33, "min_lot": 0.01, "lot_step": 0.01},
    # Keep SOL in the same crypto output band as BTC/ETH/BNB/XRP so the
    # crypto block scales consistently off the same reference style.
    "SOL-USD": {"contract": 100,     "pip": 0.01,   "pip_val":  1.00, "base_lots": 0.43,  "min_lot": 0.01, "lot_step": 0.01},
    # XRP needs a materially larger reference lot to produce output that is
    # comparable to gold-style position sizing on the same account scale.
    "XRP-USD": {"contract": 1_000,   "pip": 0.0001, "pip_val":  0.10, "base_lots": 11.50, "min_lot": 0.01, "lot_step": 0.01},
}

# Category defaults for any unlisted asset
_DEFAULTS = {
    "forex":       {"contract": 100_000, "pip": 0.0001, "pip_val": 10.00, "base_lots": 0.60, "min_lot": 0.01, "lot_step": 0.01},
    "commodities": {"contract": 100,     "pip": 0.01,   "pip_val":  1.00, "base_lots": 0.10, "min_lot": 0.01, "lot_step": 0.01},
    "indices":     {"contract":   1,     "pip": 1.0,    "pip_val":  1.00, "base_lots": 2.00, "min_lot": 0.01, "lot_step": 0.01},
    "crypto":      {"contract":   1,     "pip": 0.01,   "pip_val":  0.01, "base_lots": 5.00, "min_lot": 0.01, "lot_step": 0.01},
}

REFERENCE_MOVE_PCT_DEFAULTS = {
    "forex": 0.0035,
    "commodities": 0.0100,
    "indices": 0.0050,
    "crypto": 0.0200,
}

REFERENCE_MOVE_PCT_OVERRIDES = {
    "XAU/USD": 0.0100,
    "GC=F": 0.0100,
    "XAG/USD": 0.0180,
    "SI=F": 0.0180,
    "WTI": 0.0150,
    "WTI/USD": 0.0150,
    "CL=F": 0.0150,
    "US30": 0.0040,
    "^DJI": 0.0040,
    "US100": 0.0050,
    "^IXIC": 0.0050,
    "US500": 0.0045,
    "^GSPC": 0.0045,
    "UK100": 0.0050,
    "^FTSE": 0.0050,
    "GER40": 0.0060,
    "AUS200": 0.0060,
    "JPN225": 0.0060,
    "BTC-USD": 0.0300,
    "ETH-USD": 0.0200,
    "BNB-USD": 0.0250,
    "SOL-USD": 0.0300,
    "XRP-USD": 0.0400,
}


def _risk_budget_fraction(category: str) -> float:
    from config.config import (
        COMMODITIES_RISK_PER_TRADE,
        CRYPTO_RISK_PER_TRADE,
        DEFAULT_RISK_PER_TRADE,
        INDICES_RISK_PER_TRADE,
        MAX_RISK_PER_TRADE,
    )

    pct_map = {
        "forex": DEFAULT_RISK_PER_TRADE,
        "crypto": CRYPTO_RISK_PER_TRADE,
        "commodities": COMMODITIES_RISK_PER_TRADE,
        "indices": INDICES_RISK_PER_TRADE,
    }
    pct = float(pct_map.get((category or "").lower(), DEFAULT_RISK_PER_TRADE) or DEFAULT_RISK_PER_TRADE)
    pct = max(0.10, min(float(MAX_RISK_PER_TRADE or pct), pct))
    return pct / 100.0


def _round_down_to_step(value: float, step: float) -> float:
    step = float(step or DEFAULT_LOT_STEP)
    if step <= 0:
        return round(float(value or 0.0), 6)
    floored = math.floor((float(value or 0.0) + 1e-12) / step) * step
    return round(floored, 6)


def _reference_move_pct(asset: str, category: str) -> float:
    if asset in REFERENCE_MOVE_PCT_OVERRIDES:
        return float(REFERENCE_MOVE_PCT_OVERRIDES[asset])
    return float(REFERENCE_MOVE_PCT_DEFAULTS.get((category or "").lower(), 0.0100))


def _gold_reference_cash_move_usd() -> float:
    gold_spec = CONTRACT_SPECS[GOLD_STANDARD_ASSET]
    gold_move = (float(GOLD_REFERENCE_PRICE) * float(GOLD_REFERENCE_MOVE_PCT)) / float(gold_spec["pip"])
    return round(float(GOLD_STANDARD_LOTS) * gold_move * float(gold_spec["pip_val"]), 6)


def _reference_lots_for_entry(asset: str, category: str, entry_price: float) -> float:
    spec = PositionSizer.get_spec(asset, category)
    if asset in {GOLD_STANDARD_ASSET, "GC=F"}:
        return GOLD_STANDARD_LOTS
    if entry_price <= 0:
        return float(spec.get("base_lots", 0.0) or 0.0)
    move_pct = _reference_move_pct(asset, category)
    pip = float(spec.get("pip", 0.0) or 0.0)
    pip_val = float(spec.get("pip_val", 0.0) or 0.0)
    if pip <= 0 or pip_val <= 0 or move_pct <= 0:
        return float(spec.get("base_lots", 0.0) or 0.0)
    reference_price_move = float(entry_price) * float(move_pct)
    cash_per_lot = (reference_price_move / pip) * pip_val
    if cash_per_lot <= 0:
        return float(spec.get("base_lots", 0.0) or 0.0)
    return round(_gold_reference_cash_move_usd() / cash_per_lot, 6)


def _confidence_lots(base_lots: float, confidence: float) -> float:
    """
    Scale lot size with confidence, but keep the configured base lot as the
    neutral size at BASE_CONF. Higher confidence lifts size proportionally;
    lower confidence trims it slightly instead of inflating the baseline.

    0.62 conf → 0.8× base
    0.70 conf → 1.0× base
    0.90 conf → 2.0× base
    """
    if confidence <= MIN_CONF:
        factor = 0.8
    elif confidence >= MAX_CONF:
        factor = 2.0
    elif confidence <= BASE_CONF:
        factor = 0.8 + ((confidence - MIN_CONF) / max(1e-9, (BASE_CONF - MIN_CONF))) * 0.2
    else:
        factor = 1.0 + ((confidence - BASE_CONF) / max(1e-9, (MAX_CONF - BASE_CONF)))
    # Keep extra precision here so small-reference assets like XRP do not
    # collapse back to a coarse 0.01 lot after confidence scaling.
    return round(base_lots * factor, 6)


class PositionSizer:
    """
    Contract-spec position sizer with confidence scaling.

    Position size = confidence_scaled_lots × contract_size
    P&L = price_change × position_size (in units)

    Gold standard:
      0.10 base lots on XAU/USD at $10,000 balance.
      Other assets inherit that benchmark through a broker-style reference
      move model: "how many lots does this symbol need so its typical move
      produces the same cash effect as gold 0.10?"
    """

    def __init__(self, account_balance: float):
        self.account_balance = account_balance

    @staticmethod
    def get_spec(asset: str, category: str = "forex") -> dict:
        spec = dict(CONTRACT_SPECS.get(asset) or _DEFAULTS.get(category, _DEFAULTS["forex"]))
        spec.setdefault("min_lot", DEFAULT_MIN_LOT)
        spec.setdefault("lot_step", DEFAULT_LOT_STEP)
        return spec

    @classmethod
    def lots_from_size(cls, asset: str, category: str, size: float) -> float:
        spec = cls.get_spec(asset, category)
        contract = float(spec.get("contract", 1.0) or 1.0)
        if contract <= 0:
            return 0.0
        return round(float(size or 0.0) / contract, 6)

    @classmethod
    def reference_lots(cls, asset: str, category: str, entry_price: float) -> float:
        return round(float(_reference_lots_for_entry(asset, category, entry_price) or 0.0), 6)

    def calculate(
        self,
        entry_price: float,
        stop_loss: float,
        category: str = "forex",
        confidence: float = 0.7,
        asset: str = "",
        risk_multiplier: float = 1.0,
    ) -> float:
        """
        Returns position size in base asset units (coins/oz/contracts).

        FIX CRITICAL: CONTRACT_SPECS are calibrated for a $10,000+ account.
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

        spec      = self.get_spec(asset, category)
        base_lots = _reference_lots_for_entry(asset, category, float(entry_price))
        contract  = spec["contract"]
        pip_val   = spec["pip_val"]
        pip       = spec["pip"]
        min_lot   = max(float(spec.get("min_lot", DEFAULT_MIN_LOT) or DEFAULT_MIN_LOT), 0.0)
        lot_step  = max(float(spec.get("lot_step", DEFAULT_LOT_STEP) or DEFAULT_LOT_STEP), 0.0001)

        # Scale lot size proportionally to account balance first, then cap it
        # by actual stop-risk so the final lots stay MT5-like and sane.
        risk_scale = max(0.50, min(1.50, float(risk_multiplier or 1.0)))
        balance_factor    = max(0.0001, self.account_balance / REFERENCE_BALANCE)
        scaled_base_lots  = base_lots * balance_factor * risk_scale
        target_lots       = _confidence_lots(scaled_base_lots, confidence)

        risk_budget_usd = max(0.0, self.account_balance * _risk_budget_fraction(category) * risk_scale)
        sl_pips = 0.0
        risk_per_lot = 0.0
        if entry_price and stop_loss:
            sl_pips = abs(entry_price - stop_loss) / pip if pip else 0.0
            risk_per_lot = sl_pips * pip_val
            if risk_per_lot > 0:
                max_lots_by_risk = risk_budget_usd / risk_per_lot
                target_lots = min(target_lots, max_lots_by_risk)

        lots = _round_down_to_step(target_lots, lot_step)
        effective_lots = lots
        if lots < min_lot:
            min_lot_safe = False
            if risk_per_lot > 0:
                min_lot_safe = (risk_per_lot * min_lot) <= (risk_budget_usd + 1e-9)
            elif risk_budget_usd > 0:
                min_lot_safe = True
            if min_lot_safe:
                # Keep proportional exposure for floor-limited instruments.
                # This prevents XAG/USD, WTI, XRP-USD and similar assets from
                # inflating to a distorted 0.01-lot equivalent when the
                # intended gold-normalized size is smaller than the broker
                # floor. The paper model keeps the effective exposure aligned
                # with the requested target lots instead of over-sizing it.
                effective_lots = max(target_lots, 0.0)
                lots = min_lot
            else:
                logger.debug(
                    f"[PositionSizer] {asset} bal=${self.account_balance:.2f} "
                    f"riskx={risk_scale:.2f} target_lots={target_lots:.4f} below min lot {min_lot:.2f}; size=0"
                )
                return 0.0
        else:
            effective_lots = lots

        size = effective_lots * contract

        if entry_price and stop_loss:
            sl_pips  = abs(entry_price - stop_loss) / pip
            risk_usd = sl_pips * pip_val * effective_lots
            tp_pips  = sl_pips * 1.5
            tp_usd   = tp_pips * pip_val * effective_lots
            logger.debug(
                f"[PositionSizer] {asset} bal=${self.account_balance:.2f} "
                f"factor={balance_factor:.4f} riskx={risk_scale:.2f} conf={confidence:.3f} "
                f"ref_move={_reference_move_pct(asset, category):.4%} base={base_lots:.4f} → "
                f"display={lots:.4f} eff={effective_lots:.4f} lots ({size:.4f} units) | "
                f"SL={sl_pips:.0f} pips risk=${risk_usd:.2f} | TP≈${tp_usd:.2f}"
            )

        return round(size, 6)

    @staticmethod
    def pnl(asset: str, category: str,
            entry: float, current: float,
            size: float, direction: str) -> float:
        """
        Pip-based P&L using the configured contract specs.
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
        spec = PositionSizer.get_spec(asset, category)
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
    spec = PositionSizer.get_spec(asset, "forex")
    return _confidence_lots(spec["base_lots"], confidence)
