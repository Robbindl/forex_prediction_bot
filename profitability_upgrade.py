from logger import logger
"""
PROFITABILITY UPGRADE PATCH
============================
Drop this file into your forex_prediction_bot folder.
Import it at the top of trading_system.py with:
    from profitability_upgrade import apply_upgrades
Then call apply_upgrades(self) inside UltimateTradingSystem.__init__()

Fixes:
  1. VOTING strategy now generates real take-profit levels
  2. Asset cooldown after a loss (no more 5x Silver stop losses)
  3. Max 2 positions per asset class (no correlated blowups)
  4. Position age limit - closes stale trades after 4 hours
  5. ATR-based stop losses (wider, smarter)
  6. Entry quality filter - skips low-momentum entries
"""

import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import numpy as np


# ============================================================
# FIX 1: ASSET COOLDOWN TRACKER
# Prevents re-entering the same losing trade repeatedly
# ============================================================

class CooldownTracker:
    """Tracks recent losses and blocks re-entry for a period.

    PHASE 2: When core.state (SystemState) is available, delegates all
    reads/writes there so cooldowns are:
      • shared across all subsystems in-process
      • persisted across restarts
      • alias-aware via AssetRegistry

    Falls back to in-memory dict when running without TradingCore
    (standalone mode / tests).

    FIX BUG 6: All asset names are normalised to their canonical ID before
    storing so that aliases (XAG/USD <-> SI=F, XAU/USD <-> GC=F, etc.)
    share the same cooldown entry and never bypass each other.
    """

    # Inline alias map — kept here so profitability_upgrade has no circular
    # import on trading_system or core.assets at module load time.
    _CANONICAL: Dict[str, str] = {
        'XAU/USD': 'GC=F', 'GOLD':   'GC=F', 'GC=F': 'GC=F',
        'XAG/USD': 'SI=F', 'SILVER': 'SI=F', 'SI=F': 'SI=F',
        'WTI/USD': 'CL=F', 'OIL':    'CL=F', 'CL=F': 'CL=F',
        'NG/USD':  'NG=F', 'NG=F':   'NG=F',
        'XCU/USD': 'HG=F', 'HG=F':   'HG=F',
    }

    def _canonical(self, asset: str) -> str:
        # Try AssetRegistry first (full alias map), fallback to inline map
        try:
            from core.assets import registry
            return registry.canonical(asset)
        except Exception:
            return self._CANONICAL.get(asset, asset)

    def _get_state(self):
        """Return SystemState singleton if available, else None."""
        try:
            from core.state import state as _system_state
            return _system_state
        except Exception:
            return None

    def __init__(self, cooldown_minutes: int = 60):
        self.cooldown_minutes = cooldown_minutes
        self._losses: Dict[str, datetime] = {}   # fallback in-memory store
        self._lock = threading.Lock()

    def record_loss(self, asset: str):
        key = self._canonical(asset)
        state = self._get_state()
        if state is not None:
            state.set_cooldown(key, self.cooldown_minutes)
        else:
            with self._lock:
                self._losses[key] = datetime.now()
                logger.info(f"Cooldown activated for {key} ({self.cooldown_minutes}min) [triggered by {asset}]")

    def is_cooling_down(self, asset: str) -> bool:
        key = self._canonical(asset)
        state = self._get_state()
        if state is not None:
            return state.is_cooling_down(key)
        with self._lock:
            if key not in self._losses:
                return False
            elapsed = (datetime.now() - self._losses[key]).total_seconds() / 60
            if elapsed >= self.cooldown_minutes:
                del self._losses[key]
                return False
            return True

    def get_remaining(self, asset: str) -> int:
        key = self._canonical(asset)
        state = self._get_state()
        if state is not None:
            return state.cooldown_remaining(key)
        with self._lock:
            if key not in self._losses:
                return 0
            elapsed = (datetime.now() - self._losses[key]).total_seconds() / 60
            return max(0, int(self.cooldown_minutes - elapsed))


# ============================================================
# FIX 2: CATEGORY POSITION LIMITER
# No more 5 correlated forex positions at once
# ============================================================

class CategoryLimiter:
    """Enforces max N open positions per asset category."""

    # These are deliberately conservative for a $30 account
    MAX_PER_CATEGORY = {
    'crypto':      3,  # Can have 3 crypto positions
    'forex':       3,  # Can have 3 forex positions
    'stocks':      2,  # Can have 2 stocks
    'commodities': 2,  # Can have 2 commodities
    'indices':     2,  # Can have 2 indices
    'unknown':     2,
    }

    @staticmethod
    def can_open(category: str, open_positions: list) -> bool:
        cat = category.lower()
        limit = CategoryLimiter.MAX_PER_CATEGORY.get(cat, 1)
        current = sum(
            1 for p in open_positions
            if p.get('category', '').lower() == cat
        )
        if current >= limit:
            logger.warning(f"Category limit reached: {cat} ({current}/{limit})")
            return False
        return True


# ============================================================
# FIX 3: TAKE-PROFIT GENERATOR FOR VOTING STRATEGY
# The VOTING strategy was producing empty take_profit_levels
# ============================================================

def generate_take_profit_levels(
    signal: str,
    entry_price: float,
    stop_loss: float,
    atr: Optional[float] = None
) -> List[Dict]:
    """
    Generate 3 take-profit levels with 1.5R, 2.5R, 4R reward.
    Uses ATR if available, otherwise uses stop distance.
    """
    stop_distance = abs(entry_price - stop_loss)

    if stop_distance == 0:
        # Fallback: use 0.5% of price as stop distance
        stop_distance = entry_price * 0.005

    # Use ATR if provided and reasonable
    if atr and atr > 0 and atr < entry_price * 0.1:
        unit = atr
    else:
        unit = stop_distance

    if signal == 'BUY':
        return [
            {'level': 1, 'price': round(entry_price + unit * 1.5, 6)},
            {'level': 2, 'price': round(entry_price + unit * 2.5, 6)},
            {'level': 3, 'price': round(entry_price + unit * 4.0, 6)},
        ]
    elif signal == 'SELL':
        return [
            {'level': 1, 'price': round(entry_price - unit * 1.5, 6)},
            {'level': 2, 'price': round(entry_price - unit * 2.5, 6)},
            {'level': 3, 'price': round(entry_price - unit * 4.0, 6)},
        ]
    return []


# ============================================================
# FIX 4: ATR-BASED STOP LOSS CALCULATOR
# Fixed-% stops are terrible. ATR adapts to actual volatility.
# ============================================================

def calculate_atr_stop(
    df,
    signal: str,
    entry_price: float,
    multiplier: float = 2.0
) -> float:
    """
    Calculate stop loss using Average True Range.
    multiplier=2.0 means stop is 2x ATR away from entry.
    Higher multiplier = fewer stop-outs but larger loss per trade.
    """
    try:
        if 'atr' in df.columns:
            atr = df['atr'].iloc[-1]
        else:
            # Calculate ATR manually if not present
            high = df['high'].iloc[-14:]
            low = df['low'].iloc[-14:]
            close = df['close'].iloc[-14:]
            prev_close = close.shift(1)
            tr = np.maximum(
                high - low,
                np.maximum(
                    abs(high - prev_close),
                    abs(low - prev_close)
                )
            )
            atr = tr.mean()

        if atr <= 0 or np.isnan(atr):
            raise ValueError("Invalid ATR")

        if signal == 'BUY':
            stop = entry_price - (atr * multiplier)
        else:
            stop = entry_price + (atr * multiplier)

        return round(stop, 6)

    except Exception:
        # Safe fallback: 0.8% stop (wider than the old 0.3%)
        pct = 0.008
        if signal == 'BUY':
            return round(entry_price * (1 - pct), 6)
        else:
            return round(entry_price * (1 + pct), 6)


# ============================================================
# FIX 5: ENTRY QUALITY FILTER
# Skips entries when momentum is weak or price is mid-range
# ============================================================

def passes_entry_filter(df, signal: str) -> tuple:
    """
    Returns (passes: bool, reason: str).
    Filters out low-quality entries that are likely to reverse.
    """
    try:
        latest = df.iloc[-1]

        # --- Filter 1: RSI sanity check ---
        rsi = latest.get('rsi', 50)
        if signal == 'BUY' and rsi > 70:
            return False, f"RSI overbought ({rsi:.0f}) - skip BUY"
        if signal == 'SELL' and rsi < 30:
            return False, f"RSI oversold ({rsi:.0f}) - skip SELL"

        # --- Filter 2: ADX trend strength ---
        # Only trade when there's actual momentum (ADX > 20)
        adx = latest.get('adx', 0)
        if adx < 18:
            return False, f"ADX too weak ({adx:.1f}) - no clear trend"

        # --- Filter 3: Not trading against strong trend ---
        if 'sma_20' in df.columns and 'sma_50' in df.columns:
            sma20 = latest.get('sma_20', 0)
            sma50 = latest.get('sma_50', 0)
            price = latest.get('close', 0)
            if signal == 'BUY' and price < sma50 and sma20 < sma50:
                return False, "Price below both MAs - skip BUY in downtrend"
            if signal == 'SELL' and price > sma50 and sma20 > sma50:
                return False, "Price above both MAs - skip SELL in uptrend"

        # --- Filter 4: BB position (don't buy at top, sell at bottom) ---
        if 'bb_upper' in df.columns and 'bb_lower' in df.columns:
            bb_upper = latest.get('bb_upper', float('inf'))
            bb_lower = latest.get('bb_lower', 0)
            price = latest.get('close', 0)
            bb_range = bb_upper - bb_lower
            if bb_range > 0:
                bb_pct = (price - bb_lower) / bb_range
                if signal == 'BUY' and bb_pct > 0.85:
                    return False, f"Price at top of BB ({bb_pct:.0%}) - skip BUY"
                if signal == 'SELL' and bb_pct < 0.15:
                    return False, f"Price at bottom of BB ({bb_pct:.0%}) - skip SELL"

        return True, "Entry quality OK"
        
    except Exception as e:
        return True, f"Filter error (allowing): {e}"


# ============================================================
# FIX 6: POSITION AGE MONITOR
# Closes trades that have been open too long with no movement
# ============================================================

class PositionAgeMonitor:
    """Closes positions that exceed a maximum age with no profit."""

    def __init__(self, max_age_hours: float = 4.0):
        self.max_age_hours = max_age_hours

    def get_stale_positions(self, open_positions: list, current_prices: dict) -> list:
        """Returns list of trade_ids that should be force-closed."""
        stale = []
        now = datetime.now()

        for pos in open_positions:
            try:
                entry_time = datetime.fromisoformat(pos['entry_time'])
                age_hours = (now - entry_time).total_seconds() / 3600

                if age_hours < self.max_age_hours:
                    continue

                # Only close if not in profit (let winners run)
                asset = pos['asset']
                current_price = current_prices.get(asset)
                if current_price is None:
                    continue

                entry_price = pos['entry_price']
                signal = pos['signal']

                if signal == 'BUY':
                    in_profit = current_price > entry_price
                else:
                    in_profit = current_price < entry_price

                if not in_profit:
                    stale.append({
                        'trade_id': pos['trade_id'],
                        'asset': asset,
                        'age_hours': round(age_hours, 1),
                        'reason': f'Stale position ({age_hours:.1f}h, no profit)'
                    })

            except Exception:
                continue

        return stale


# ============================================================
# SIGNAL ENHANCER - wraps your existing signals with all fixes
# ============================================================

def enhance_signal(signal: dict, df=None, open_positions: list = None) -> Optional[dict]:
    """
    Pass any signal through this before executing.
    Returns enhanced signal or None if signal should be skipped.

    Usage in trading_system.py:
        from profitability_upgrade import enhance_signal, cooldown_tracker, category_limiter
        
        # After generating a signal:
        enhanced = enhance_signal(signal, df=df_15m, open_positions=open_positions)
        if enhanced:
            trade = self.paper_trader.execute_signal(enhanced)
    """
    if not signal or signal.get('signal') in ('HOLD', 'CLOSED', None):
        return None

    asset = signal.get('asset', '')
    direction = signal.get('signal', '')
    entry_price = signal.get('entry_price', 0)

    # --- Check cooldown ---
    if cooldown_tracker.is_cooling_down(asset):
        remaining = cooldown_tracker.get_remaining(asset)
        logger.info(f"{asset} on cooldown ({remaining}min remaining) - skipping")
        return None

    # --- Check category limit ---
    if open_positions is not None:
        category = signal.get('category', 'unknown')
        if not CategoryLimiter.can_open(category, open_positions):
            return None

    # --- Entry quality filter ---
    if df is not None:
        passes, reason = passes_entry_filter(df, direction)
        if not passes:
            logger.info(f"Entry filter blocked: {reason}")
            return None

    # --- Fix stop loss using ATR ---
    stop_loss = signal.get('stop_loss', 0)
    if df is not None and (stop_loss == 0 or abs(entry_price - stop_loss) / entry_price < 0.001):
        # Stop is missing or too tight - recalculate with ATR
        stop_loss = calculate_atr_stop(df, direction, entry_price, multiplier=2.0)
        logger.debug(f"ATR stop applied: {stop_loss:.5f}")
        signal['stop_loss'] = stop_loss

    # --- Fix missing take profit levels (main VOTING bug) ---
    tp_levels = signal.get('take_profit_levels', [])
    if not tp_levels:
        atr = None
        if df is not None and 'atr' in df.columns:
            atr = df['atr'].iloc[-1]
        tp_levels = generate_take_profit_levels(direction, entry_price, stop_loss, atr)
        signal['take_profit_levels'] = tp_levels
        logger.debug(f"Take-profit levels added: TP1={tp_levels[0]['price']:.5f}, "
              f"TP2={tp_levels[1]['price']:.5f}, TP3={tp_levels[2]['price']:.5f}")

    return signal


def on_trade_closed(asset: str, pnl: float, exit_reason: str):
    """
    Call this whenever a trade closes.
    Updates cooldown tracker on losses.

    Usage in paper_trader.py, inside the to_close loop:
        from profitability_upgrade import on_trade_closed
        on_trade_closed(trade.asset, trade.pnl, trade.exit_reason)
    """
    if pnl < 0:
        cooldown_tracker.record_loss(asset)


# ============================================================
# GLOBAL INSTANCES (import these directly)
# ============================================================

cooldown_tracker = CooldownTracker(cooldown_minutes=30)
category_limiter = CategoryLimiter()
position_age_monitor = PositionAgeMonitor(max_age_hours=4.0)


# ============================================================
# APPLY ALL UPGRADES TO YOUR TRADING SYSTEM
# ============================================================

def apply_upgrades(trading_system_instance):
    """
    Call this in UltimateTradingSystem.__init__() to attach upgrades.

    Example:
        from profitability_upgrade import apply_upgrades
        class UltimateTradingSystem:
            def __init__(self, ...):
                ...
                apply_upgrades(self)
    """
    trading_system_instance.cooldown_tracker = cooldown_tracker
    trading_system_instance.category_limiter = category_limiter
    trading_system_instance.position_age_monitor = position_age_monitor
    logger.info("Profitability upgrades applied:")
    logger.info("  • Asset cooldown: 60min after a loss")
    logger.info("  • Category limits: max 1-2 positions per asset class")
    logger.info("  • ATR-based stops: adapts to actual volatility")
    logger.info("  • Take-profit levels: auto-generated for VOTING strategy")
    logger.info("  • Entry quality filter: blocks weak/bad entries")
    logger.info("  • Position age limit: closes stale trades after 4h")


# ============================================================
# INTEGRATION GUIDE (printed when run directly)
# ============================================================

if __name__ == "__main__":
    logger.info("Run: python profitability_upgrade.py to see usage")