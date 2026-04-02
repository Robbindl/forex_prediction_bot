from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.confidence import boost_confidence, clamp_confidence
from core.signal_journal import SignalJournal


@dataclass
class Signal:
    """
    Universal signal object. Created by strategies, reviewed by the decision engine,
    and consumed by execution. If any review step sets alive=False the trade is killed.
    """
    # ── Identity ──────────────────────────────────────────────────────────
    asset:      str
    direction:  str           # "BUY" | "SELL"
    category:   str           # "forex" | "crypto" | "stocks" | "commodities" | "indices"

    # ── Confidence ────────────────────────────────────────────────────────
    confidence: float         # 0.0 – 1.0

    # ── Prices ────────────────────────────────────────────────────────────
    entry_price:  float = 0.0
    stop_loss:    float = 0.0
    take_profit:  float = 0.0
    take_profit_levels: List[float] = field(default_factory=list)

    # ── Risk ──────────────────────────────────────────────────────────────
    risk_parameters: Dict[str, Any] = field(default_factory=dict)
    position_size:   float = 0.0
    risk_reward:     float = 0.0

    # ── Source ────────────────────────────────────────────────────────────
    strategy_id:  str = ""
    indicators:   Dict[str, Any] = field(default_factory=dict)
    timestamp:    datetime = field(default_factory=datetime.utcnow)

    # ── Decision state ────────────────────────────────────────────────────
    alive:         bool = True          # set False to kill the trade
    kill_reason:   str  = ""
    step_reached:  int  = 0             # primary runtime name
    metadata:      Dict[str, Any] = field(default_factory=dict)

    # ── Canonical asset ───────────────────────────────────────────────────
    canonical_asset: str = ""

    # ── Signal Journal ────────────────────────────────────────────────────
    # Automatically created — records every stage decision for Telegram reporting.
    # Access via signal.journal.record(...) from any review step or phase.
    journal: SignalJournal = field(init=False)

    def __post_init__(self) -> None:
        self.confidence = clamp_confidence(self.confidence)
        self.journal = SignalJournal(
            asset     = self.asset,
            direction = self.direction,
        )

    def kill(self, reason: str, layer: int) -> None:
        """Mark signal as dead. Once dead it cannot be revived."""
        if self.alive:
            self.alive        = False
            self.kill_reason  = reason
            self.step_reached = int(layer)

    def boost(self, delta: float) -> None:
        """Increase confidence, capped to the configured maximum conviction score."""
        self.confidence = boost_confidence(self.confidence, delta)

    def reduce(self, delta: float) -> None:
        """Decrease confidence, floor at 0.0."""
        self.confidence = max(0.0, self.confidence - delta)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "asset":              self.asset,
            "canonical_asset":    self.canonical_asset,
            "direction":          self.direction,
            "category":           self.category,
            "confidence":         round(self.confidence, 4),
            "entry_price":        self.entry_price,
            "stop_loss":          self.stop_loss,
            "take_profit":        self.take_profit,
            "take_profit_levels": self.take_profit_levels,
            "risk_parameters":    self.risk_parameters,
            "position_size":      self.position_size,
            "risk_reward":        self.risk_reward,
            "strategy_id":        self.strategy_id,
            "indicators":         self.indicators,
            "timestamp":          self.timestamp.isoformat(),
            "alive":              self.alive,
            "kill_reason":        self.kill_reason,
            "step_reached":       self.step_reached,
            "metadata":           self.metadata,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Signal":
        ts = d.get("timestamp")
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts)
            except Exception:
                ts = datetime.utcnow()
        return cls(
            asset             = d.get("asset", ""),
            direction         = d.get("direction", "BUY"),
            category          = d.get("category", "forex"),
            confidence        = float(d.get("confidence", 0.0)),
            entry_price       = float(d.get("entry_price", 0.0)),
            stop_loss         = float(d.get("stop_loss", 0.0)),
            take_profit       = float(d.get("take_profit", 0.0)),
            take_profit_levels= d.get("take_profit_levels", []),
            risk_parameters   = d.get("risk_parameters", {}),
            position_size     = float(d.get("position_size", 0.0)),
            risk_reward       = float(d.get("risk_reward", 0.0)),
            strategy_id       = d.get("strategy_id", ""),
            indicators        = d.get("indicators", {}),
            timestamp         = ts or datetime.utcnow(),
            alive             = bool(d.get("alive", True)),
            kill_reason       = d.get("kill_reason", ""),
            step_reached      = int(d.get("step_reached", 0)),
            metadata          = d.get("metadata", {}),
            canonical_asset   = d.get("canonical_asset", d.get("asset", "")),
        )

    def __repr__(self) -> str:
        status = "ALIVE" if self.alive else f"DEAD({self.kill_reason})"
        return (
            f"Signal({self.asset} {self.direction} "
            f"score={self.confidence:.3f} {status})"
        )
