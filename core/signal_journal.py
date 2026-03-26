from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ── Decision constants ────────────────────────────────────────────────────────
PASS    = "PASS"
KILLED  = "KILLED"
SKIPPED = "SKIPPED"
BOOSTED = "BOOSTED"
REDUCED = "REDUCED"
INFO    = "INFO"       # non-layer entries (backtest, phase data)

# ── Telegram emoji map ────────────────────────────────────────────────────────
_EMOJI = {
    PASS:    "✅",
    KILLED:  "❌",
    SKIPPED: "⏭",
    BOOSTED: "⬆",
    REDUCED: "⬇",
    INFO:    "📊",
}


@dataclass
class JournalEntry:
    """A single recorded decision from one layer or phase."""
    layer:       int            # 0 = pre-pipeline / post-pipeline
    name:        str            # layer or phase name
    decision:    str            # PASS | KILLED | SKIPPED | BOOSTED | REDUCED | INFO
    reason:      str            # human-readable explanation
    conf_before: float          # confidence before this stage
    conf_after:  float          # confidence after this stage
    data:        Dict[str, Any] = field(default_factory=dict)
    elapsed_ms:  float          = 0.0
    ts:          float          = field(default_factory=time.time)

    @property
    def conf_delta(self) -> float:
        return round(self.conf_after - self.conf_before, 4)

    def emoji(self) -> str:
        return _EMOJI.get(self.decision, "•")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "layer":       self.layer,
            "name":        self.name,
            "decision":    self.decision,
            "reason":      self.reason,
            "conf_before": round(self.conf_before, 4),
            "conf_after":  round(self.conf_after,  4),
            "conf_delta":  self.conf_delta,
            "data":        self.data,
            "elapsed_ms":  round(self.elapsed_ms, 2),
            "ts":          self.ts,
        }


class SignalJournal:
    """
    Mutable log attached to a Signal. Every layer writes one entry.
    Immutable once the signal is dead or executed.
    """

    def __init__(self, asset: str, direction: str) -> None:
        self.asset      = asset
        self.direction  = direction
        self.entries:   List[JournalEntry] = []
        self._start_ts  = time.time()

    # ── Public API ────────────────────────────────────────────────────────────

    def record(
        self,
        layer:       int,
        name:        str,
        decision:    str,
        reason:      str,
        conf_before: float,
        conf_after:  float,
        data:        Optional[Dict[str, Any]] = None,
        elapsed_ms:  float = 0.0,
    ) -> None:
        """Add one entry. Thread-safe — called from pipeline layers."""
        self.entries.append(JournalEntry(
            layer       = layer,
            name        = name,
            decision    = decision,
            reason      = reason,
            conf_before = conf_before,
            conf_after  = conf_after,
            data        = data or {},
            elapsed_ms  = elapsed_ms,
        ))

    def total_elapsed_ms(self) -> float:
        return round((time.time() - self._start_ts) * 1000, 1)

    def final_decision(self) -> str:
        """SURVIVED or KILLED"""
        # Allow manual debug override (e.g. DEBUG_FORCE_SURVIVE) to preserve
        # a surviving signal for announcement even if earlier stages recorded kills.
        for e in reversed(self.entries):
            if e.name == "debug_force" and e.decision == PASS:
                return "SURVIVED"
        for e in reversed(self.entries):
            if e.decision == KILLED:
                return "KILLED"
        return "SURVIVED"

    def kill_entry(self) -> Optional[JournalEntry]:
        for e in self.entries:
            if e.decision == KILLED:
                return e
        return None

    def to_list(self) -> List[Dict]:
        return [e.to_dict() for e in self.entries]

    # ── Telegram formatting ───────────────────────────────────────────────────

    def _escape_markdown(self, text: str) -> str:
        if not isinstance(text, str):
            return str(text)
        return (text.replace("\\", "\\\\")
                    .replace("_", "\\_")
                    .replace("*", "\\*")
                    .replace("`", "\\`")
                    .replace("[", "\\[")
                    .replace("]", "\\]"))

    def to_telegram(self, signal=None) -> str:
        """
        Format the full journal as a Telegram Markdown message.
        Called by pipeline_reporter.py after the pipeline completes.
        """
        survived = self.final_decision() == "SURVIVED"
        direction = self._escape_markdown(self.direction)
        asset = self._escape_markdown(self.asset)

        if survived:
            header = f"🔔 *NEW SIGNAL — {asset} {direction}*"
        else:
            kill   = self.kill_entry()
            reason = self._escape_markdown(kill.reason if kill else 'unknown')
            header = (
                f"💀 *SIGNAL KILLED — {asset} {direction}*\n"
                f"_Reason: {reason}_"
            )

        lines = [header, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]

        for entry in self.entries:
            emoji = entry.emoji()
            name  = self._escape_markdown(entry.name.upper().replace("_", " "))

            # Confidence delta display
            if entry.conf_delta > 0:
                conf_str = f"conf {entry.conf_before:.2f} → {entry.conf_after:.2f} ⬆"
            elif entry.conf_delta < 0:
                conf_str = f"conf {entry.conf_before:.2f} → {entry.conf_after:.2f} ⬇"
            else:
                conf_str = f"conf {entry.conf_before:.2f}"

            reason_str = f"  _{self._escape_markdown(entry.reason)}_" if entry.reason else ""
            lines.append(f"{emoji} *{name}*   {conf_str}{reason_str}")

            # Show phase data inline if available
            if entry.data:
                data_parts = []
                for k, v in entry.data.items():
                    if isinstance(v, float):
                        data_parts.append(f"{self._escape_markdown(k)}={v:.3f}")
                    elif v is not None:
                        data_parts.append(f"{self._escape_markdown(k)}={self._escape_markdown(v)}")
                if data_parts:
                    lines.append(f"   `{'  '.join(data_parts[:4])}`")

        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

        # Execution details for surviving signals
        if survived and signal:
            entry_p = float(getattr(signal, "entry_price", 0))
            sl      = float(getattr(signal, "stop_loss",   0))
            tp      = float(getattr(signal, "take_profit", 0))
            conf    = float(getattr(signal, "confidence",  0))
            size    = float(getattr(signal, "position_size", 0))
            rr      = float(getattr(signal, "risk_reward",  0))

            lines.append(
                f"🚀 *EXECUTING*\n"
                f"   Entry: `{entry_p:.5f}`\n"
                f"   SL:    `{sl:.5f}`\n"
                f"   TP:    `{tp:.5f}`\n"
                f"   R:R:   {rr:.1f}:1\n"
                f"   Conf:  {conf:.0%}\n"
                f"   Size:  {size:.4f}"
            )

        lines.append(f"\n_Pipeline: {self.total_elapsed_ms():.0f}ms_")
        return "\n".join(lines)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "asset":     self.asset,
            "direction": self.direction,
            "decision":  self.final_decision(),
            "entries":   self.to_list(),
            "elapsed_ms": self.total_elapsed_ms(),
        }
