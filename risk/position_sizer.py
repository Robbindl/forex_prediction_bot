"""risk/position_sizer.py — Dynamic position sizer with pip value awareness."""
from __future__ import annotations
from utils.logger import get_logger
from config.config import (
    DEFAULT_RISK_PER_TRADE, CRYPTO_RISK_PER_TRADE, MAX_RISK_PER_TRADE,
    COMMODITIES_RISK_PER_TRADE, INDICES_RISK_PER_TRADE,
    CRYPTO_MAX_POSITION_SIZE,
)

logger = get_logger()


class PositionSizer:
    """
    Calculates position size using pip value per asset.
    Ensures consistent risk regardless of asset's pip value.
    """
    
    # Pip values per asset (price move per pip/point)
    ASSET_PIP_VALUES = {
        # Forex
        "EUR/USD": 0.0001,
        "GBP/USD": 0.0001,
        "USD/JPY": 0.01,
        "AUD/USD": 0.0001,
        "USD/CAD": 0.0001,
        "GBP/JPY": 0.01,
        
        # Commodities
        "GC=F":    0.10,    # Gold: $0.10 per pip
        "SI=F":    0.01,    # Silver: $0.01 per pip
        "CL=F":    0.01,    # Oil: $0.01 per pip
        
        # Crypto (price move per unit)
        "BTC-USD": 1.0,
        "ETH-USD": 0.01,
        "SOL-USD": 0.01,
        "BNB-USD": 0.01,
        "XRP-USD": 0.0001,
        
        # Indices
        "^DJI":    1.0,
        "^IXIC":   1.0,
        "^GSPC":   1.0,
        "^FTSE":   1.0,
    }
    
    # Pip value per 1 standard lot (for profit calculation)
    PIP_VALUE_PER_LOT = {
        # Forex: 1 standard lot = 100,000 units = $10 per pip for USD pairs
        "EUR/USD": 10.0,
        "GBP/USD": 10.0,
        "USD/JPY": 8.33,     # Approximate, varies with exchange rate
        "AUD/USD": 10.0,
        "USD/CAD": 10.0,
        "GBP/JPY": 8.33,
        
        # Commodities
        "GC=F":    100.0,    # Gold: 1 lot (100 oz) = $100 per $1 move
        "SI=F":    50.0,     # Silver: 1 lot (5,000 oz) = $50 per $0.01 move
        "CL=F":    10.0,     # Oil: 1 lot (1,000 bbl) = $10 per $0.01 move
        
        # Crypto & Indices: 1 unit = 1 unit
        "BTC-USD": 1.0,
        "ETH-USD": 1.0,
        "SOL-USD": 1.0,
        "BNB-USD": 1.0,
        "XRP-USD": 1.0,
        "^DJI":    1.0,
        "^IXIC":   1.0,
        "^GSPC":   1.0,
        "^FTSE":   1.0,
    }
    
    # Minimum position sizes
    MIN_POSITION_UNITS = {
        "forex":    1000,    # 0.01 lots (1,000 units)
        "crypto":   0.001,
        "commodities": 0.01,
        "indices":  0.1,
    }

    def __init__(self, account_balance: float):
        self.account_balance = account_balance

    def calculate(
        self,
        entry_price: float,
        stop_loss: float,
        category: str = "forex",
        confidence: float = 0.7,
        asset: str = "",  # Added for pip value calculation
    ) -> float:
        """
        Returns position size (units) based on risk per trade.
        Uses pip value to ensure consistent risk across all assets.
        """
        if not entry_price or not stop_loss or entry_price == stop_loss:
            return 0.0

        # Risk percentage based on category — each has its own tuned setting
        if category == "crypto":
            risk_pct = CRYPTO_RISK_PER_TRADE
        elif category == "commodities":
            risk_pct = COMMODITIES_RISK_PER_TRADE
        elif category == "indices":
            risk_pct = INDICES_RISK_PER_TRADE
        else:
            risk_pct = DEFAULT_RISK_PER_TRADE  # forex
        risk_pct = min(risk_pct * (0.7 + confidence * 0.6), MAX_RISK_PER_TRADE)
        
        # Risk amount in dollars
        risk_amount = self.account_balance * risk_pct / 100
        
        # Stop distance in price units
        stop_distance = abs(entry_price - stop_loss)
        
        if category == "crypto":
            # Crypto: direct dollar-based sizing — no pip conversion needed
            # size = risk_amount / stop_distance_in_dollars
            size = risk_amount / stop_distance

        elif category == "forex":
            # Forex: pip-based sizing → convert lots to units
            pip_value = self.ASSET_PIP_VALUES.get(asset, 0.0001)
            stop_pips = stop_distance / pip_value if pip_value > 0 else 0
            if stop_pips <= 0:
                size = risk_amount / stop_distance * 100000
            else:
                pip_value_per_lot = self.PIP_VALUE_PER_LOT.get(asset, 10.0)
                size = (risk_amount / (stop_pips * pip_value_per_lot)) * 100000

        elif category in ("commodities", "indices"):
            # Commodities/Indices: pip-based but no lot conversion
            pip_value = self.ASSET_PIP_VALUES.get(asset, 1.0)
            stop_pips = stop_distance / pip_value if pip_value > 0 else 0
            if stop_pips <= 0:
                size = risk_amount / stop_distance
            else:
                pip_value_per_lot = self.PIP_VALUE_PER_LOT.get(asset, 10.0)
                size = risk_amount / (stop_pips * pip_value_per_lot)

        else:
            # Fallback
            size = risk_amount / stop_distance
        
        # Note: CRYPTO_MAX_POSITION_SIZE cap removed — risk % controls sizing directly.
        # Each crypto trade risks exactly CRYPTO_RISK_PER_TRADE % of balance.
        # This is paper trading — position value is notional, actual risk = $200 max.
        
        # Apply minimum size protection
        min_size = self.MIN_POSITION_UNITS.get(category, 0.001)
        size = max(min_size, size)
        
        return round(size, 6)