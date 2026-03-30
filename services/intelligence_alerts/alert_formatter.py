from __future__ import annotations

from typing import Dict, Optional

from utils.logger import get_logger

logger = get_logger()

# ── Priority emoji headers ────────────────────────────────────────────────────
_PRIORITY_HEADER = {
    "CRITICAL": "🚨 *CRITICAL ALERT*",
    "HIGH":     "⚠️ *HIGH PRIORITY*",
    "MEDIUM":   "📢 *MARKET INTELLIGENCE*",
    "LOW":      "ℹ️ *MARKET UPDATE*",
}


def _fmt_price(price: float) -> str:
    """Smart price formatting — no hardcoded decimal places."""
    if price == 0:
        return "0"
    if price >= 1000:
        return f"{price:,.2f}"
    if price >= 10:
        return f"{price:.2f}"
    if price >= 0.1:
        return f"{price:.4f}"
    return f"{price:.5f}"


class AlertFormatter:
    """
    Formats event dicts into Telegram Markdown messages.
    All methods are stateless and safe to call concurrently.
    """

    def format(self, channel: str, event: dict, priority: str) -> Optional[str]:
        """
        Main entry point. Returns formatted Telegram message or None if
        the event should be silently dropped.
        """
        try:
            method_name = f"_format_{channel}"
            method      = getattr(self, method_name, self._format_generic)
            message     = method(event)
            if not message:
                return None
            header = _PRIORITY_HEADER.get(priority, "📊 *INTEL*")
            return f"{header}\n\n{message}"
        except Exception as e:
            logger.debug(f"[AlertFormatter] format error [{channel}]: {e}")
            return None

    # ── Phase 1 formatters ────────────────────────────────────────────────────

    def _format_LIQUIDATION_CASCADE_ALERT(self, e: dict) -> str:
        usd   = float(e.get("usd_total", 0))
        sev   = e.get("severity", "HIGH")
        asset = e.get("asset", "?")
        win   = e.get("window_s", 60)
        icon  = "💥" if sev == "CRITICAL" else "🔥"
        return (
            f"{icon} *Liquidation Cascade*\n\n"
            f"Asset:    `{asset}`\n"
            f"Wiped:    `${usd:,.0f}`\n"
            f"Window:   `{win}s`\n"
            f"Severity: `{sev}`\n\n"
            f"_Large long/short positions being force-closed._"
        )

    def _format_FUNDING_RATE_ALERT(self, e: dict) -> str:
        asset = e.get("asset", "?")
        rate  = float(e.get("rate_pct", e.get("rate", 0) * 100))
        bias  = e.get("bias", "UNKNOWN")
        impl  = e.get("implication", "")
        icon  = "💸"
        return (
            f"{icon} *Funding Rate Alert*\n\n"
            f"Asset:       `{asset}`\n"
            f"Rate:        `{rate:.4f}%` per 8hr\n"
            f"Bias:        `{bias}`\n\n"
            f"_{impl}_"
        )

    def _format_OI_CHANGE_ALERT(self, e: dict) -> str:
        asset   = e.get("asset", "?")
        chg_pct = float(e.get("change_pct", 0))
        signal  = e.get("signal", "NEUTRAL")
        icon    = "📈" if chg_pct > 0 else "📉"
        return (
            f"{icon} *Open Interest Spike*\n\n"
            f"Asset:   `{asset}`\n"
            f"Change:  `{chg_pct:+.1f}%`\n"
            f"Signal:  `{signal}`\n\n"
            f"_{self._oi_description(signal)}_"
        )

    def _format_MACRO_NEWS_EVENT(self, e: dict) -> str:
        label  = e.get("label",      "Economic Data")
        prev   = e.get("prev",       0)
        curr   = e.get("current",    0)
        chg    = float(e.get("change_pct", 0))
        impact = e.get("impact",     "LOW")
        if impact == "LOW":
            return ""   # skip low-impact macro noise
        icon = "🌍"
        return (
            f"{icon} *Macro Event*\n\n"
            f"Event:   `{label}`\n"
            f"Prev:    `{prev}`\n"
            f"Current: `{curr}`\n"
            f"Change:  `{chg:+.3f}%`\n"
            f"Impact:  `{impact}`"
        )

    # ── Whale intelligence formatters ─────────────────────────────────────────

    def _format_WHALE_ACCUMULATION(self, e: dict) -> str:
        return self._format_whale_move(e, "BUY")

    def _format_WHALE_DISTRIBUTION(self, e: dict) -> str:
        return self._format_whale_move(e, "SELL")

    def _format_whale_move(self, e: dict, direction: str) -> str:
        label    = e.get("label",    "Unknown Whale")
        asset    = e.get("asset",    "BTC")
        delta    = abs(float(e.get("delta", e.get("delta_btc", 0))))
        behavior = e.get("behavior", "unknown")
        icon     = "🐋🟢" if direction == "BUY" else "🐋🔴"
        action   = "Accumulated" if direction == "BUY" else "Distributed"
        return (
            f"{icon} *Whale {action}*\n\n"
            f"Wallet:   `{label}`\n"
            f"Asset:    `{asset}`\n"
            f"Amount:   `{delta:.4f}`\n"
            f"Behavior: `{behavior}`\n\n"
            f"_{self._behavior_description(behavior, direction)}_"
        )

    def _format_WHALE_CLUSTER_ALERT(self, e: dict) -> str:
        direction = e.get("direction",    "BUY")
        count     = int(e.get("wallet_count", 0))
        total     = float(e.get("total_asset", e.get("total_btc", 0)))
        asset     = e.get("asset",        "BTC")
        conf      = float(e.get("confidence", 0))
        labels    = e.get("labels",       [])
        icon      = "🐋🐋🐋"
        return (
            f"{icon} *Whale Cluster Alert*\n\n"
            f"Direction:  `{direction}`\n"
            f"Wallets:    `{count}`\n"
            f"Total:      `{total:.4f} {asset}`\n"
            f"Confidence: `{conf:.0%}`\n"
            f"Wallets:    _{', '.join(labels[:3])}_\n\n"
            f"_Coordinated whale movement — high market impact expected._"
        )

    def _format_EXCHANGE_INFLOW_ALERT(self, e: dict) -> str:
        return self._format_exchange_flow(e, "Inflow")

    def _format_EXCHANGE_OUTFLOW_ALERT(self, e: dict) -> str:
        return self._format_exchange_flow(e, "Outflow")

    def _format_exchange_flow(self, e: dict, flow_type: str) -> str:
        label = e.get("label", "Exchange")
        asset = e.get("asset", "BTC")
        delta = abs(float(e.get("delta", 0)))
        icon  = "🏦➡️" if flow_type == "Inflow" else "🏦⬅️"
        impl  = (
            "Large inflow to exchange — potential sell pressure ahead."
            if flow_type == "Inflow"
            else "Large withdrawal from exchange — potential accumulation."
        )
        return (
            f"{icon} *Exchange {flow_type}*\n\n"
            f"Exchange: `{label}`\n"
            f"Asset:    `{asset}`\n"
            f"Amount:   `{delta:.4f}`\n\n"
            f"_{impl}_"
        )

    # ── Phase 3 formatters ────────────────────────────────────────────────────

    def _format_LIQUIDITY_WALL_DETECTED(self, e: dict) -> str:
        asset    = e.get("asset",     "?")
        side     = e.get("side",      "BID")
        price    = float(e.get("price",    0))
        ratio    = float(e.get("size_ratio", 0))
        strength = e.get("strength",  "MODERATE")
        impl     = e.get("implication", "")
        # Only alert on STRONG and EXTREME walls — skip MODERATE
        if strength == "MODERATE":
            return ""
        icon = "🧱"
        return (
            f"{icon} *Liquidity Wall*\n\n"
            f"Asset:    `{asset}`\n"
            f"Side:     `{side}`\n"
            f"Level:    `{_fmt_price(price)}`\n"
            f"Size:     `{ratio:.1f}× average`\n"
            f"Strength: `{strength}`\n\n"
            f"_{impl}_"
        )

    def _format_BID_ASK_IMBALANCE_ALERT(self, e: dict) -> str:
        asset   = e.get("asset",   "?")
        score   = float(e.get("rolling_score", 0))
        bias    = e.get("bias",    "NEUTRAL")
        bid_vol = float(e.get("bid_vol", 0))
        ask_vol = float(e.get("ask_vol", 0))
        impl    = e.get("implication", "")
        icon    = "📊"
        return (
            f"{icon} *Order Book Imbalance*\n\n"
            f"Asset:    `{asset}`\n"
            f"Score:    `{score:+.3f}`\n"
            f"Bias:     `{bias}`\n"
            f"Bids:     `{bid_vol:.2f}`\n"
            f"Asks:     `{ask_vol:.2f}`\n\n"
            f"_{impl}_"
        )

    def _format_STOP_HUNT_DETECTED(self, e: dict) -> str:
        asset      = e.get("asset",       "?")
        wall_price = float(e.get("wall_price", 0))
        wall_side  = e.get("wall_side",   "?")
        wick_pct   = float(e.get("wick_pct",  0))
        revert_ms  = int(e.get("revert_ms",   0))
        impl       = e.get("implication", "BUY")
        conf       = float(e.get("confidence", 0))
        icon       = "⚡"
        return (
            f"{icon} *Stop Hunt Detected*\n\n"
            f"Asset:      `{asset}`\n"
            f"Wall level: `{_fmt_price(wall_price)}` ({wall_side})\n"
            f"Wick:       `{wick_pct:.3f}%` through level\n"
            f"Reverted:   `{revert_ms}ms`\n"
            f"Signal:     `{impl}`\n"
            f"Confidence: `{conf:.0%}`\n\n"
            f"_Retail stops cleared — expect {impl} continuation._"
        )

    # ── Phase 4 formatters ────────────────────────────────────────────────────

    def _format_NARRATIVE_TREND_DETECTED(self, e: dict) -> str:
        narrative = e.get("narrative", "UNKNOWN")
        velocity  = float(e.get("velocity",  0))
        strength  = e.get("strength",  "MILD")
        count     = int(e.get("count",    0))
        keywords  = e.get("keywords_matched", e.get("keywords", []))
        icon      = "📣"
        return (
            f"{icon} *Narrative Trend*\n\n"
            f"Narrative: `{narrative}`\n"
            f"Velocity:  `{velocity:.1f}×` acceleration\n"
            f"Strength:  `{strength}`\n"
            f"Mentions:  `{count}`\n"
            f"Keywords:  _{', '.join(str(k) for k in keywords[:4])}_"
        )

    def _format_REDDIT_TOPIC_SPIKE(self, e: dict) -> str:
        subreddit  = e.get("subreddit",  "?")
        narrative  = e.get("narrative",  "?")
        post_count = int(e.get("post_count", 0))
        samples    = e.get("sample_titles", [])
        icon       = "🤖"
        sample_str = f"\n_\"{samples[0][:60]}...\"_" if samples else ""
        return (
            f"{icon} *Reddit Spike — r/{subreddit}*\n\n"
            f"Narrative: `{narrative}`\n"
            f"Posts:     `{post_count}`{sample_str}"
        )

    def _format_TWITTER_TOPIC_SPIKE(self, e: dict) -> str:
        narrative  = e.get("narrative",   "?")
        tweet_count = int(e.get("tweet_count", 0))
        icon        = "🐦"
        return (
            f"{icon} *Twitter Spike*\n\n"
            f"Narrative: `{narrative}`\n"
            f"Tweets:    `{tweet_count}` matching"
        )

    # ── Generic fallback ──────────────────────────────────────────────────────

    def _format_generic(self, e: dict) -> str:
        event_type = e.get("type", "UNKNOWN")
        asset      = e.get("asset", e.get("symbol", ""))
        asset_str  = f"\nAsset: `{asset}`" if asset else ""
        return f"*{event_type}*{asset_str}"

    # ── Helper descriptions ───────────────────────────────────────────────────

    @staticmethod
    def _oi_description(signal: str) -> str:
        return {
            "TREND_CONTINUATION": "New positions opening — market gaining conviction.",
            "POTENTIAL_REVERSAL": "Positions closing — potential trend exhaustion.",
            "NEUTRAL":            "Open interest stable.",
        }.get(signal, "")

    @staticmethod
    def _behavior_description(behavior: str, direction: str) -> str:
        action = "buying" if direction == "BUY" else "selling"
        return {
            "accumulator": f"Known accumulator {action} — high-conviction move.",
            "distributor": f"Known distributor {action} — smart money repositioning.",
            "dormant":     f"Dormant wallet waking up and {action} — rare, significant.",
            "flipper":     f"Active trader {action} — lower signal weight.",
            "exchange":    f"Exchange wallet {action} — routine transfer.",
            "unknown":     f"Unknown wallet {action}.",
        }.get(behavior, f"Whale {action}.")
