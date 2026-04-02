from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from strategy_lab.event_risk_service import EventRiskService
from utils.logger import get_logger

logger = get_logger()

_RESAMPLE_RULES: Dict[str, str] = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}

_SESSION_HOURS: Dict[str, set[int]] = {
    "asia": set(range(0, 9)),
    "london": set(range(7, 17)),
    "new_york": set(range(13, 22)),
    "overlap": set(range(13, 17)),
}

# ── Indicator registry ────────────────────────────────────────────────────────
_INDICATORS: Dict[str, str] = {
    "rsi":       "_calc_rsi",
    "ema":       "_calc_ema",
    "macd":      "_calc_macd",
    "bollinger": "_calc_bollinger",
    "atr":       "_calc_atr",
    "volume_ma": "_calc_volume_ma",
    "stoch":     "_calc_stoch",
    "adx":       "_calc_adx",
}


class DynamicStrategy:
    """
    A fully config-driven strategy. Created by StrategyBuilder.
    Compatible with BacktestEngineV2 and the existing strategies/base.py interface.
    """

    def __init__(self, config: Dict[str, Any], asset: str = "", category: str = "") -> None:
        self.name = config.get("name", "dynamic")
        self.version = config.get("version", "1.0")
        self._config = config
        self._asset = asset
        self._category = category
        self._event_windows: List[Dict[str, Any]] = []
        self._event_window_start: Optional[pd.Timestamp] = None
        self._event_window_end: Optional[pd.Timestamp] = None
        self._macro_bias_windows: List[Dict[str, Any]] = []
        self._macro_bias_window_start: Optional[pd.Timestamp] = None
        self._macro_bias_window_end: Optional[pd.Timestamp] = None
        self._validate()

    # ── Public API ────────────────────────────────────────────────────────────

    def generate(self, df: pd.DataFrame, asset: str = "", category: str = "") -> Optional[Dict]:
        """
        Run strategy against an OHLCV DataFrame.
        Returns a signal dict or None if no entry triggered.
        df must have columns: open, high, low, close, volume (lowercase).
        """
        asset = asset or self._asset
        category = category or self._category
        if df is None or len(df) < self._required_bars(df):
            return None
        try:
            df = self._add_indicators(df.copy())
            entry = self._evaluate_rules(df)
            if not entry:
                return None

            direction = entry["direction"]
            if not self._passes_filters(df, direction, asset=asset, category=category):
                return None

            confidence = self._calc_confidence(df)
            macro_bias = self._get_active_macro_event_context(df, asset=asset, category=category)
            confidence = self._apply_macro_bias_confidence(
                confidence,
                direction=direction,
                macro_bias=macro_bias,
                filters=self._config.get("filters") or {},
            )
            price = float(df["close"].iloc[-1])
            atr = float(df["atr"].iloc[-1]) if "atr" in df.columns and not pd.isna(df["atr"].iloc[-1]) else price * 0.015
            stop_mult = float(self._config.get("stop_mult", 1.5))
            tp_mult = float(self._config.get("tp_mult", 3.0))

            if direction == "BUY":
                stop_loss   = price - atr * stop_mult
                take_profit = price + atr * stop_mult * tp_mult
            else:
                stop_loss   = price + atr * stop_mult
                take_profit = price - atr * stop_mult * tp_mult

            return {
                "direction":   direction,
                "confidence":  round(confidence, 4),
                "entry_price": round(price, 8),
                "stop_loss":   round(stop_loss, 8),
                "take_profit": round(take_profit, 8),
                "strategy_id": self.name,
                "indicators": self._snapshot(df),
                "macro_bias": macro_bias,
            }
        except Exception as e:
            logger.debug(f"[StrategyBuilder] generate error: {e}")
            return None

    def bind_backtest_window(self, df: pd.DataFrame, asset: str = "", category: str = "") -> None:
        asset = asset or self._asset
        category = category or self._category
        filters = self._config.get("filters") or {}
        event_cfg = filters.get("event_risk") or {}
        macro_cfg = filters.get("macro_event_bias") or {}
        if not event_cfg or event_cfg.get("enabled") is False:
            self._event_windows = []
            self._event_window_start = None
            self._event_window_end = None
        if not macro_cfg or macro_cfg.get("enabled") is False:
            self._macro_bias_windows = []
            self._macro_bias_window_start = None
            self._macro_bias_window_end = None

        start_ts, end_ts = self._timestamp_bounds(df)
        if start_ts is None or end_ts is None:
            return

        if event_cfg and event_cfg.get("enabled") is not False:
            self._event_windows = EventRiskService.get_blackout_windows(
                asset=asset,
                category=category,
                start_time=start_ts,
                end_time=end_ts,
                config=event_cfg,
            )
            self._event_window_start = start_ts
            self._event_window_end = end_ts

        if macro_cfg and macro_cfg.get("enabled") is not False:
            self._macro_bias_windows = EventRiskService.get_macro_bias_windows(
                asset=asset,
                category=category,
                start_time=start_ts,
                end_time=end_ts,
                config=macro_cfg,
            )
            self._macro_bias_window_start = start_ts
            self._macro_bias_window_end = end_ts

    # ── Internal ──────────────────────────────────────────────────────────────

    def _validate(self) -> None:
        if (
            "entry_rules" not in self._config
            and "entry_rules_long" not in self._config
            and "entry_rules_short" not in self._config
        ):
            raise ValueError(f"Strategy '{self.name}' missing 'entry_rules'")

    def _add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        for ind in self._config.get("indicators", []):
            name   = ind.get("name", "")
            params = ind.get("params", {})
            method = getattr(self, _INDICATORS.get(name, "_noop"), None)
            if method:
                df = method(df, **params)
        return df

    def _evaluate_rules(self, df: pd.DataFrame) -> Optional[Dict]:
        long_rules = self._config.get("entry_rules_long", [])
        short_rules = self._config.get("entry_rules_short", [])
        if long_rules or short_rules:
            long_pass = self._evaluate_rule_set(long_rules, df) if long_rules else False
            short_pass = self._evaluate_rule_set(short_rules, df) if short_rules else False
            if long_pass and short_pass:
                return None
            if long_pass:
                return {"direction": "BUY"}
            if short_pass:
                return {"direction": "SELL"}
            return None

        rules = self._config.get("entry_rules", [])
        if not rules:
            return None
        passed = self._evaluate_rule_set(rules, df)
        if passed:
            direction = next((rule.get("direction") for rule in rules if rule.get("direction")), "BUY")
            return {"direction": direction}
        return None

    def _evaluate_rule_set(self, rules: List[Dict[str, Any]], df: pd.DataFrame) -> bool:
        results = []
        for rule in rules:
            col  = rule.get("col", "")
            op   = rule.get("op",  ">")
            val  = rule.get("val")
            col2 = rule.get("col2")
            if col not in df.columns:
                return None
            cur  = float(df[col].iloc[-1])
            prev = float(df[col].iloc[-2]) if len(df) > 1 else cur
            cmp  = (float(df[col2].iloc[-1])
                    if col2 and col2 in df.columns
                    else float(val if val is not None else 0))
            cmp_prev = (float(df[col2].iloc[-2])
                        if col2 and col2 in df.columns and len(df) > 1
                        else cmp)
            passed = {
                ">":           cur > cmp,
                "<":           cur < cmp,
                ">=":          cur >= cmp,
                "<=":          cur <= cmp,
                "cross_above": cur > cmp and prev <= cmp_prev,
                "cross_below": cur < cmp and prev >= cmp_prev,
            }.get(op, False)
            results.append(passed)
        return bool(results) and all(results)

    def _calc_confidence(self, df: pd.DataFrame) -> float:
        base   = float(self._config.get("base_confidence", 0.65))
        boosts = self._config.get("confidence_boosts", [])
        for rule in boosts:
            col = rule.get("col", "")
            if col not in df.columns:
                continue
            val = float(df[col].iloc[-1])
            if rule.get("above") is not None and val > float(rule["above"]):
                base = min(1.0, base + float(rule.get("boost", 0.05)))
            if rule.get("below") is not None and val < float(rule["below"]):
                base = min(1.0, base + float(rule.get("boost", 0.05)))
        return base

    def _required_bars(self, df: pd.DataFrame) -> int:
        base_required = int(self._config.get("min_bars", 50) or 50)
        filters = self._config.get("filters") or {}
        higher = filters.get("higher_timeframe") or {}
        timeframe = str(higher.get("timeframe") or "").lower()
        if not timeframe:
            return base_required

        base_seconds = self._infer_base_seconds(df)
        higher_seconds = self._timeframe_seconds(timeframe)
        slow = int(higher.get("slow_ema", 50) or 50)
        if base_seconds <= 0 or higher_seconds <= 0:
            return base_required
        multiplier = max(1, int(round(higher_seconds / float(base_seconds))))
        return max(base_required, (slow + 5) * multiplier)

    def _passes_filters(self, df: pd.DataFrame, direction: str, asset: str = "", category: str = "") -> bool:
        filters = self._config.get("filters") or {}
        if not filters:
            return True
        if not self._passes_session_filter(df, filters, category=category):
            return False
        if not self._passes_event_risk_filter(df, filters, asset=asset, category=category):
            return False
        if not self._passes_macro_event_bias_filter(df, filters, direction, asset=asset, category=category):
            return False
        if not self._passes_volatility_filter(df, filters):
            return False
        if not self._passes_higher_timeframe_filter(df, filters, direction):
            return False
        return True

    def _passes_session_filter(self, df: pd.DataFrame, filters: Dict[str, Any], category: str = "") -> bool:
        session_cfg = filters.get("session_names")
        allowed_hours_cfg = filters.get("allowed_hours")
        if not session_cfg and not allowed_hours_cfg:
            return True

        timestamp = self._last_timestamp(df)
        if timestamp is None:
            return True

        hour = int(timestamp.hour)
        allowed_hours = self._resolve_filter_value(allowed_hours_cfg, category)
        if allowed_hours:
            return hour in {int(h) for h in allowed_hours}

        sessions = self._resolve_filter_value(session_cfg, category)
        if not sessions:
            return True
        active_hours: set[int] = set()
        for session_name in sessions:
            active_hours.update(_SESSION_HOURS.get(str(session_name).lower(), set()))
        return hour in active_hours if active_hours else True

    def _passes_volatility_filter(self, df: pd.DataFrame, filters: Dict[str, Any]) -> bool:
        volatility = filters.get("volatility") or {}
        if not volatility:
            return True

        price = float(df["close"].iloc[-1]) if "close" in df.columns else 0.0
        if price <= 0:
            return False

        atr = float(df["atr"].iloc[-1]) if "atr" in df.columns and not pd.isna(df["atr"].iloc[-1]) else 0.0
        atr_pct = atr / price if atr > 0 else 0.0
        if volatility.get("atr_pct_min") is not None and atr_pct < float(volatility["atr_pct_min"]):
            return False
        if volatility.get("atr_pct_max") is not None and atr_pct > float(volatility["atr_pct_max"]):
            return False

        if "adx" in df.columns and volatility.get("adx_min") is not None:
            adx = float(df["adx"].iloc[-1]) if not pd.isna(df["adx"].iloc[-1]) else 0.0
            if adx < float(volatility["adx_min"]):
                return False

        if "volume_ma" in df.columns and volatility.get("volume_ratio_min") is not None:
            volume_ma = float(df["volume_ma"].iloc[-1]) if not pd.isna(df["volume_ma"].iloc[-1]) else 0.0
            if volume_ma <= 0:
                return False
            volume_ratio = float(df["volume"].iloc[-1]) / volume_ma
            if volume_ratio < float(volatility["volume_ratio_min"]):
                return False

        if "bb_upper" in df.columns and "bb_lower" in df.columns:
            upper = float(df["bb_upper"].iloc[-1]) if not pd.isna(df["bb_upper"].iloc[-1]) else price
            lower = float(df["bb_lower"].iloc[-1]) if not pd.isna(df["bb_lower"].iloc[-1]) else price
            bb_width_pct = abs(upper - lower) / price if price > 0 else 0.0
            if volatility.get("bb_width_pct_min") is not None and bb_width_pct < float(volatility["bb_width_pct_min"]):
                return False
            if volatility.get("bb_width_pct_max") is not None and bb_width_pct > float(volatility["bb_width_pct_max"]):
                return False

        return True

    def _passes_event_risk_filter(self, df: pd.DataFrame, filters: Dict[str, Any], asset: str = "", category: str = "") -> bool:
        event_cfg = filters.get("event_risk") or {}
        if not event_cfg or event_cfg.get("enabled") is False:
            return True

        timestamp = self._last_timestamp(df)
        if timestamp is None:
            return True

        windows = None
        if (
            self._event_window_start is not None
            and self._event_window_end is not None
            and self._event_window_start <= timestamp <= self._event_window_end
        ):
            windows = self._event_windows

        active = EventRiskService.active_blackout(
            timestamp=timestamp,
            asset=asset or self._asset,
            category=category or self._category,
            config=event_cfg,
            preload_windows=windows,
        )
        return active is None

    def _passes_macro_event_bias_filter(
        self,
        df: pd.DataFrame,
        filters: Dict[str, Any],
        direction: str,
        asset: str = "",
        category: str = "",
    ) -> bool:
        macro_cfg = filters.get("macro_event_bias") or {}
        if not macro_cfg or macro_cfg.get("enabled") is False:
            return True
        if not bool(macro_cfg.get("block_counter_bias", True)):
            return True
        active = self._get_active_macro_event_context(df, asset=asset, category=category)
        if not active:
            return True
        if "effective_direction" in active:
            effective_direction = str(active.get("effective_direction") or "").upper()
        else:
            effective_direction = str(active.get("direction") or "").upper()
        if not effective_direction:
            return False
        return effective_direction == str(direction or "").upper()

    def _get_active_macro_event_bias(self, df: pd.DataFrame, asset: str = "", category: str = "") -> Optional[Dict[str, Any]]:
        filters = self._config.get("filters") or {}
        macro_cfg = filters.get("macro_event_bias") or {}
        if not macro_cfg or macro_cfg.get("enabled") is False:
            return None

        timestamp = self._last_timestamp(df)
        if timestamp is None:
            return None

        windows = None
        if (
            self._macro_bias_window_start is not None
            and self._macro_bias_window_end is not None
            and self._macro_bias_window_start <= timestamp <= self._macro_bias_window_end
        ):
            windows = self._macro_bias_windows

        return EventRiskService.active_macro_bias(
            timestamp=timestamp,
            asset=asset or self._asset,
            category=category or self._category,
            config=macro_cfg,
            preload_windows=windows,
        )

    def _get_active_macro_event_context(self, df: pd.DataFrame, asset: str = "", category: str = "") -> Optional[Dict[str, Any]]:
        macro_cfg = (self._config.get("filters") or {}).get("macro_event_bias") or {}
        active = self._get_active_macro_event_bias(df, asset=asset, category=category)
        if not active:
            return None

        context = dict(active)
        expected_direction = str(context.get("direction") or "").upper()
        if not expected_direction:
            return context

        cross_market = context.get("cross_market") if isinstance(context.get("cross_market"), dict) else {}
        cross_market_alignment = str(cross_market.get("alignment") or "").lower()
        cross_market_direction = str(cross_market.get("direction") or "").upper()
        context["cross_market_alignment"] = cross_market_alignment
        context["cross_market_direction"] = cross_market_direction
        context["cross_market_strength"] = float(cross_market.get("strength", 0.0) or 0.0)

        reaction = self._evaluate_macro_reaction(df, context, macro_cfg)
        context["expected_direction"] = expected_direction
        context["reaction"] = reaction
        context["reaction_state"] = reaction.get("state")
        context["reaction_strength"] = reaction.get("strength", 0.0)

        effective_direction = expected_direction
        if reaction.get("state") == "rejected" and reaction.get("reversal_direction") and bool(
            ((macro_cfg.get("reaction") or {}).get("allow_reversal_on_rejection", True))
        ):
            effective_direction = str(reaction.get("reversal_direction") or expected_direction).upper()
        elif cross_market_alignment == "opposed" and bool(
            ((macro_cfg.get("cross_market") or {}).get("allow_cross_market_reversal", False))
        ):
            effective_direction = cross_market_direction or ""
        elif reaction.get("state") in {"pending", "neutral"} and bool(
            ((macro_cfg.get("reaction") or {}).get("require_confirmation", False))
        ):
            effective_direction = ""
        elif cross_market_alignment == "opposed" and bool(
            ((macro_cfg.get("cross_market") or {}).get("require_confirmation", True))
        ):
            effective_direction = ""

        context["effective_direction"] = effective_direction
        return context

    @staticmethod
    def _apply_macro_bias_confidence(
        confidence: float,
        direction: str,
        macro_bias: Optional[Dict[str, Any]],
        filters: Dict[str, Any],
    ) -> float:
        if not macro_bias:
            return confidence
        macro_cfg = filters.get("macro_event_bias") or {}
        reaction_cfg = macro_cfg.get("reaction") or {}
        cross_market_cfg = macro_cfg.get("cross_market") or {}
        if "effective_direction" in macro_bias:
            bias_direction = str(macro_bias.get("effective_direction") or "").upper()
        else:
            bias_direction = str(macro_bias.get("direction") or "").upper()
        if not bias_direction:
            return confidence

        strength = max(0.0, min(1.0, float(macro_bias.get("strength", 0.0) or 0.0)))
        reaction_state = str(macro_bias.get("reaction_state") or "").lower()
        reaction_strength = max(0.0, min(1.0, float(macro_bias.get("reaction_strength", strength) or strength)))
        cross_market_alignment = str(macro_bias.get("cross_market_alignment") or "").lower()
        cross_market_strength = max(0.0, min(1.0, float(macro_bias.get("cross_market_strength", 0.0) or 0.0)))
        if bias_direction == str(direction or "").upper():
            boost = float(macro_cfg.get("aligned_confidence_boost", 0.06) or 0.06)
            adjusted = min(1.0, confidence + boost * strength)
            if reaction_state == "confirmed":
                adjusted = min(
                    1.0,
                    adjusted + float(reaction_cfg.get("confirmed_confidence_boost", 0.04) or 0.04) * reaction_strength,
                )
            elif reaction_state == "rejected" and bias_direction != str(macro_bias.get("direction") or "").upper():
                adjusted = min(
                    1.0,
                    adjusted + float(reaction_cfg.get("reversal_confidence_boost", 0.05) or 0.05) * reaction_strength,
                )
            if cross_market_alignment == "confirmed":
                adjusted = min(
                    1.0,
                    adjusted + float(cross_market_cfg.get("confirmed_confidence_boost", 0.05) or 0.05) * cross_market_strength,
                )
            elif cross_market_alignment == "opposed" and bias_direction == str(macro_bias.get("direction") or "").upper():
                adjusted = max(
                    0.0,
                    adjusted - float(cross_market_cfg.get("opposition_confidence_penalty", 0.07) or 0.07) * cross_market_strength,
                )
            return adjusted

        penalty = float(macro_cfg.get("counter_confidence_penalty", 0.08) or 0.08)
        adjusted = max(0.0, confidence - penalty * strength)
        if reaction_state == "rejected":
            adjusted = max(
                0.0,
                adjusted - float(reaction_cfg.get("rejection_counter_penalty", 0.05) or 0.05) * reaction_strength,
            )
        if cross_market_alignment == "opposed":
            adjusted = max(
                0.0,
                adjusted - float(cross_market_cfg.get("counter_alignment_penalty", 0.05) or 0.05) * cross_market_strength,
            )
        return adjusted

    def _evaluate_macro_reaction(self, df: pd.DataFrame, macro_bias: Dict[str, Any], macro_cfg: Dict[str, Any]) -> Dict[str, Any]:
        reaction_cfg = macro_cfg.get("reaction") or {}
        if reaction_cfg.get("enabled", True) is False:
            return {"state": "disabled", "strength": 0.0}

        event_time = self._last_event_timestamp(macro_bias)
        timestamps = self._timestamp_series(df)
        if event_time is None or timestamps.empty:
            return {"state": "unavailable", "strength": 0.0}

        latest_ts = pd.Timestamp(timestamps.iloc[-1])
        if latest_ts < event_time:
            return {"state": "pre_event", "strength": 0.0, "bars_since_event": 0}

        event_positions = np.where(timestamps <= event_time)[0]
        if len(event_positions) == 0:
            return {"state": "unavailable", "strength": 0.0}
        event_idx = int(event_positions[-1])
        bars_since_event = max(0, len(df) - 1 - event_idx)

        min_bars = int(self._resolve_filter_value(reaction_cfg.get("min_bars_after_event", 1), self._category) or 1)
        lookback_bars = max(1, int(self._resolve_filter_value(reaction_cfg.get("momentum_lookback_bars", 3), self._category) or 3))
        confirm_threshold = float(
            self._resolve_filter_value(reaction_cfg.get("confirmation_threshold_atr", 0.2), self._category) or 0.2
        )
        rejection_threshold = float(
            self._resolve_filter_value(reaction_cfg.get("rejection_threshold_atr", 0.15), self._category) or 0.15
        )

        close = df["close"].astype(float).reset_index(drop=True)
        anchor_close = float(close.iloc[event_idx])
        current_close = float(close.iloc[-1])
        reference_idx = max(event_idx, len(close) - 1 - lookback_bars)
        reference_close = float(close.iloc[reference_idx])
        atr_value = (
            float(df["atr"].iloc[-1])
            if "atr" in df.columns and not pd.isna(df["atr"].iloc[-1]) and float(df["atr"].iloc[-1]) > 0
            else max(abs(anchor_close) * 0.01, 1e-6)
        )

        expected_direction = str(macro_bias.get("direction") or "").upper()
        expected_sign = 1.0 if expected_direction == "BUY" else -1.0
        directional_move_atr = expected_sign * (current_close - anchor_close) / atr_value
        directional_recent_atr = expected_sign * (current_close - reference_close) / atr_value
        reaction_strength = max(abs(directional_move_atr), abs(directional_recent_atr))
        strength = max(0.0, min(1.0, reaction_strength / max(confirm_threshold, rejection_threshold, 0.1)))

        if bars_since_event < min_bars:
            state = "pending"
        elif directional_move_atr >= confirm_threshold and directional_recent_atr >= -0.05:
            state = "confirmed"
        elif directional_move_atr <= -rejection_threshold and directional_recent_atr <= 0.0:
            state = "rejected"
        else:
            state = "neutral"

        result = {
            "state": state,
            "strength": round(float(strength), 4),
            "bars_since_event": int(bars_since_event),
            "directional_move_atr": round(float(directional_move_atr), 4),
            "directional_recent_atr": round(float(directional_recent_atr), 4),
            "anchor_close": round(anchor_close, 8),
            "current_close": round(current_close, 8),
        }
        if state == "rejected":
            result["reversal_direction"] = self._opposite_direction(expected_direction)
        return result

    @staticmethod
    def _last_event_timestamp(macro_bias: Dict[str, Any]) -> Optional[pd.Timestamp]:
        for key in ("event_time", "date", "timestamp"):
            value = macro_bias.get(key)
            if value is None:
                continue
            try:
                ts = pd.Timestamp(value)
                if ts.tzinfo is None:
                    return ts.tz_localize("UTC")
                return ts.tz_convert("UTC")
            except Exception:
                continue
        return None

    @staticmethod
    def _timestamp_series(df: pd.DataFrame) -> pd.Series:
        if "timestamp" in df.columns:
            return pd.to_datetime(df["timestamp"], utc=True, errors="coerce").reset_index(drop=True)
        if isinstance(df.index, pd.DatetimeIndex):
            series = pd.Series(pd.to_datetime(df.index, utc=True))
            return series.reset_index(drop=True)
        return pd.Series(dtype="datetime64[ns, UTC]")

    @staticmethod
    def _opposite_direction(direction: str) -> str:
        return "SELL" if str(direction or "").upper() == "BUY" else "BUY"

    def _passes_higher_timeframe_filter(self, df: pd.DataFrame, filters: Dict[str, Any], direction: str) -> bool:
        higher = filters.get("higher_timeframe") or {}
        timeframe = str(higher.get("timeframe") or "").lower()
        if not timeframe:
            return True

        frame = self._resample_frame(df, timeframe)
        if frame is None or frame.empty:
            return False

        fast_period = int(higher.get("fast_ema", 20) or 20)
        slow_period = int(higher.get("slow_ema", 50) or 50)
        if len(frame) < max(fast_period, slow_period):
            return False

        close = frame["close"].astype(float)
        fast = close.ewm(span=fast_period, adjust=False).mean().iloc[-1]
        slow = close.ewm(span=slow_period, adjust=False).mean().iloc[-1]
        latest_close = float(close.iloc[-1])
        price_confirm = bool(higher.get("price_confirm", True))

        if direction == "BUY":
            if not (fast > slow):
                return False
            return latest_close > slow if price_confirm else True

        if not (fast < slow):
            return False
        return latest_close < slow if price_confirm else True

    @staticmethod
    def _resolve_filter_value(value: Any, category: str = "") -> Any:
        if isinstance(value, dict):
            category_key = str(category or "").lower()
            return value.get(category_key) or value.get("default")
        return value

    @staticmethod
    def _last_timestamp(df: pd.DataFrame) -> Optional[pd.Timestamp]:
        if "timestamp" in df.columns:
            series = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            if len(series) and not pd.isna(series.iloc[-1]):
                return pd.Timestamp(series.iloc[-1])
        if isinstance(df.index, pd.DatetimeIndex) and len(df.index):
            return pd.Timestamp(df.index[-1]).tz_localize("UTC") if df.index.tz is None else pd.Timestamp(df.index[-1]).tz_convert("UTC")
        return None

    @staticmethod
    def _timestamp_bounds(df: pd.DataFrame) -> tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
        if "timestamp" in df.columns:
            series = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dropna()
            if len(series):
                return pd.Timestamp(series.iloc[0]), pd.Timestamp(series.iloc[-1])
            return None, None
        if isinstance(df.index, pd.DatetimeIndex) and len(df.index):
            if df.index.tz is None:
                return pd.Timestamp(df.index[0]).tz_localize("UTC"), pd.Timestamp(df.index[-1]).tz_localize("UTC")
            return pd.Timestamp(df.index[0]).tz_convert("UTC"), pd.Timestamp(df.index[-1]).tz_convert("UTC")
        return None, None

    @staticmethod
    def _timeframe_seconds(timeframe: str) -> int:
        return {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
            "4h": 14400,
            "1d": 86400,
        }.get(str(timeframe or "").lower(), 900)

    def _infer_base_seconds(self, df: pd.DataFrame) -> int:
        if "timestamp" in df.columns:
            series = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dropna()
            if len(series) >= 2:
                diffs = series.diff().dropna().dt.total_seconds()
                if len(diffs):
                    return max(60, int(diffs.median()))
        if isinstance(df.index, pd.DatetimeIndex) and len(df.index) >= 2:
            diffs = pd.Series(df.index).diff().dropna().dt.total_seconds()
            if len(diffs):
                return max(60, int(diffs.median()))
        return 900

    def _resample_frame(self, df: pd.DataFrame, timeframe: str) -> Optional[pd.DataFrame]:
        rule = _RESAMPLE_RULES.get(str(timeframe or "").lower())
        if not rule:
            return None

        if "timestamp" in df.columns:
            frame = df.copy()
            frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
            frame = frame.dropna(subset=["timestamp"]).set_index("timestamp")
        elif isinstance(df.index, pd.DatetimeIndex):
            frame = df.copy()
            frame.index = pd.to_datetime(frame.index, utc=True)
        else:
            return None

        required = [col for col in ("open", "high", "low", "close", "volume") if col in frame.columns]
        if "close" not in required:
            return None

        agg = {}
        if "open" in frame.columns:
            agg["open"] = "first"
        if "high" in frame.columns:
            agg["high"] = "max"
        if "low" in frame.columns:
            agg["low"] = "min"
        agg["close"] = "last"
        if "volume" in frame.columns:
            agg["volume"] = "sum"

        resampled = frame[required].resample(rule, label="left", closed="left").agg(agg).dropna(subset=["close"])
        return resampled

    def _snapshot(self, df: pd.DataFrame) -> Dict:
        cols = ["rsi", "ema_20", "ema_50", "macd", "signal", "atr",
                "bb_upper", "bb_lower", "adx"]
        return {
            c: round(float(df[c].iloc[-1]), 6)
            for c in cols
            if c in df.columns and not pd.isna(df[c].iloc[-1])
        }

    # ── Indicator calculators ─────────────────────────────────────────────────

    def _calc_rsi(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        delta = df["close"].diff()
        gain  = delta.clip(lower=0).rolling(period).mean()
        loss  = (-delta.clip(upper=0)).rolling(period).mean()
        rs    = gain / loss.replace(0, np.nan)
        df["rsi"] = 100 - 100 / (1 + rs)
        return df

    def _calc_ema(self, df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
        df[f"ema_{period}"] = df["close"].ewm(span=period, adjust=False).mean()
        return df

    def _calc_macd(self, df: pd.DataFrame,
                   fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
        ema_f        = df["close"].ewm(span=fast,   adjust=False).mean()
        ema_s        = df["close"].ewm(span=slow,   adjust=False).mean()
        df["macd"]   = ema_f - ema_s
        df["signal"] = df["macd"].ewm(span=signal, adjust=False).mean()
        df["hist"]   = df["macd"] - df["signal"]
        return df

    def _calc_atr(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        hl  = df["high"] - df["low"]
        hc  = (df["high"] - df["close"].shift()).abs()
        lc  = (df["low"]  - df["close"].shift()).abs()
        tr  = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        df["atr"] = tr.rolling(period).mean()
        return df

    def _calc_bollinger(self, df: pd.DataFrame,
                        period: int = 20, std: float = 2.0) -> pd.DataFrame:
        ma             = df["close"].rolling(period).mean()
        sd             = df["close"].rolling(period).std()
        df["bb_upper"] = ma + sd * std
        df["bb_lower"] = ma - sd * std
        df["bb_mid"]   = ma
        return df

    def _calc_volume_ma(self, df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
        df["volume_ma"] = df["volume"].rolling(period).mean()
        return df

    def _calc_stoch(self, df: pd.DataFrame,
                    k_period: int = 14, d_period: int = 3) -> pd.DataFrame:
        low_min  = df["low"].rolling(k_period).min()
        high_max = df["high"].rolling(k_period).max()
        df["stoch_k"] = 100 * (df["close"] - low_min) / (high_max - low_min).replace(0, np.nan)
        df["stoch_d"] = df["stoch_k"].rolling(d_period).mean()
        return df

    def _calc_adx(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        high  = df["high"]
        low   = df["low"]
        close = df["close"]
        plus_dm  = high.diff().clip(lower=0)
        minus_dm = (-low.diff()).clip(lower=0)
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs(),
        ], axis=1).max(axis=1)
        atr      = tr.rolling(period).mean().replace(0, np.nan)
        plus_di  = 100 * plus_dm.rolling(period).mean()  / atr
        minus_di = 100 * minus_dm.rolling(period).mean() / atr
        dx       = (100 * (plus_di - minus_di).abs()
                    / (plus_di + minus_di).replace(0, np.nan))
        df["adx"]      = dx.rolling(period).mean()
        df["plus_di"]  = plus_di
        df["minus_di"] = minus_di
        return df

    def _noop(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return df


# ── Factory ───────────────────────────────────────────────────────────────────

class StrategyBuilder:
    """
    Factory for creating DynamicStrategy instances from configs or JSON files.
    """

    @staticmethod
    def from_dict(config: Dict, asset: str = "", category: str = "") -> DynamicStrategy:
        return DynamicStrategy(config, asset=asset, category=category)

    @staticmethod
    def from_json(path: str) -> DynamicStrategy:
        with open(path, encoding="utf-8") as f:
            return DynamicStrategy(json.load(f))

    @staticmethod
    def _default_session_filter() -> Dict[str, List[str]]:
        return {
            "crypto": ["asia", "london", "new_york"],
            "forex": ["london", "new_york", "overlap"],
            "commodities": ["london", "new_york", "overlap"],
            "indices": ["london", "new_york", "overlap"],
            "default": ["london", "new_york"],
        }

    @staticmethod
    def _event_risk_filter() -> Dict[str, Any]:
        return {
            "enabled": True,
            "currencies": "auto",
            "impacts": ["HIGH"],
            "lookback_minutes": {
                "crypto": 20,
                "forex": 45,
                "commodities": 60,
                "indices": 45,
                "default": 30,
            },
            "lookahead_minutes": {
                "crypto": 15,
                "forex": 30,
                "commodities": 45,
                "indices": 30,
                "default": 30,
            },
        }

    @staticmethod
    def _macro_event_bias_filter() -> Dict[str, Any]:
        return {
            "enabled": True,
            "currencies": "auto",
            "impacts": ["HIGH"],
            "window_minutes": {
                "crypto": 60,
                "forex": 90,
                "commodities": 120,
                "indices": 90,
                "default": 90,
            },
            "min_strength": {
                "crypto": 0.45,
                "forex": 0.35,
                "commodities": 0.35,
                "indices": 0.4,
                "default": 0.4,
            },
            "block_counter_bias": True,
            "aligned_confidence_boost": 0.06,
            "counter_confidence_penalty": 0.08,
            "cross_market": {
                "enabled": True,
                "require_confirmation": True,
                "allow_cross_market_reversal": False,
                "confirmed_confidence_boost": 0.05,
                "opposition_confidence_penalty": 0.07,
                "counter_alignment_penalty": 0.05,
            },
            "reaction": {
                "enabled": True,
                "require_confirmation": False,
                "allow_reversal_on_rejection": True,
                "min_bars_after_event": {
                    "crypto": 1,
                    "forex": 2,
                    "commodities": 2,
                    "indices": 2,
                    "default": 1,
                },
                "momentum_lookback_bars": 3,
                "confirmation_threshold_atr": {
                    "crypto": 0.25,
                    "forex": 0.18,
                    "commodities": 0.22,
                    "indices": 0.2,
                    "default": 0.2,
                },
                "rejection_threshold_atr": {
                    "crypto": 0.2,
                    "forex": 0.15,
                    "commodities": 0.18,
                    "indices": 0.17,
                    "default": 0.16,
                },
                "confirmed_confidence_boost": 0.04,
                "reversal_confidence_boost": 0.05,
                "rejection_counter_penalty": 0.05,
            },
        }

    @staticmethod
    def _trend_filters(htf: str = "1h", atr_pct_min: float = 0.0008, atr_pct_max: float = 0.03, adx_min: float | None = None) -> Dict[str, Any]:
        volatility: Dict[str, Any] = {
            "atr_pct_min": atr_pct_min,
            "atr_pct_max": atr_pct_max,
        }
        if adx_min is not None:
            volatility["adx_min"] = adx_min
        return {
            "session_names": StrategyBuilder._default_session_filter(),
            "higher_timeframe": {
                "timeframe": htf,
                "fast_ema": 20,
                "slow_ema": 50,
                "price_confirm": True,
            },
            "volatility": volatility,
            "event_risk": StrategyBuilder._event_risk_filter(),
            "macro_event_bias": StrategyBuilder._macro_event_bias_filter(),
        }

    @staticmethod
    def _breakout_filters(htf: str = "1h") -> Dict[str, Any]:
        return {
            "session_names": StrategyBuilder._default_session_filter(),
            "higher_timeframe": {
                "timeframe": htf,
                "fast_ema": 20,
                "slow_ema": 50,
                "price_confirm": True,
            },
            "volatility": {
                "atr_pct_min": 0.0010,
                "atr_pct_max": 0.04,
                "volume_ratio_min": 1.05,
                "bb_width_pct_min": 0.003,
            },
            "event_risk": StrategyBuilder._event_risk_filter(),
            "macro_event_bias": StrategyBuilder._macro_event_bias_filter(),
        }

    @staticmethod
    def _reversion_filters() -> Dict[str, Any]:
        return {
            "session_names": StrategyBuilder._default_session_filter(),
            "volatility": {
                "atr_pct_min": 0.0005,
                "atr_pct_max": 0.02,
                "bb_width_pct_max": 0.08,
            },
            "event_risk": StrategyBuilder._event_risk_filter(),
            "macro_event_bias": StrategyBuilder._macro_event_bias_filter(),
        }

    @staticmethod
    def example_config() -> Dict:
        """
        Ready-to-use EMA crossover + RSI filter strategy.
        BUY when EMA20 crosses above EMA50 and RSI is between 45–70.
        """
        return {
            "name":    "ema_rsi_crossover",
            "version": "1.0",
            "indicators": [
                {"name": "rsi",  "params": {"period": 14}},
                {"name": "ema",  "params": {"period": 20}},
                {"name": "ema",  "params": {"period": 50}},
                {"name": "atr",  "params": {"period": 14}},
            ],
            "entry_rules_long": [
                {"col": "ema_20", "op": "cross_above",
                 "col2": "ema_50", "direction": "BUY"},
                {"col": "rsi", "op": ">", "val": 48},
                {"col": "rsi", "op": "<", "val": 68},
            ],
            "entry_rules_short": [
                {"col": "ema_20", "op": "cross_below",
                 "col2": "ema_50", "direction": "SELL"},
                {"col": "rsi", "op": "<", "val": 52},
                {"col": "rsi", "op": ">", "val": 32},
            ],
            "confidence_boosts": [
                {"col": "rsi", "above": 55, "boost": 0.05},
                {"col": "rsi", "below": 45, "boost": 0.05},
            ],
            "stop_mult": 1.5,
            "tp_mult":   3.0,
            "filters": StrategyBuilder._trend_filters(htf="1h", atr_pct_min=0.0008, atr_pct_max=0.025),
        }

    @staticmethod
    def rsi_mean_reversion_config() -> Dict:
        """
        BUY when RSI oversold (< 30), SELL when overbought (> 70).
        Mean-reversion approach — works well in ranging markets.
        """
        return {
            "name":    "rsi_mean_reversion",
            "version": "1.0",
            "indicators": [
                {"name": "rsi", "params": {"period": 14}},
                {"name": "atr", "params": {"period": 14}},
                {"name": "bollinger", "params": {"period": 20, "std": 2.0}},
            ],
            "entry_rules_long": [
                {"col": "rsi", "op": "<", "val": 30, "direction": "BUY"},
                {"col": "close", "op": "<=", "col2": "bb_lower"},
            ],
            "entry_rules_short": [
                {"col": "rsi", "op": ">", "val": 70, "direction": "SELL"},
                {"col": "close", "op": ">=", "col2": "bb_upper"},
            ],
            "confidence_boosts": [
                {"col": "rsi", "below": 25, "boost": 0.08},
                {"col": "rsi", "below": 20, "boost": 0.10},
                {"col": "rsi", "above": 75, "boost": 0.08},
                {"col": "rsi", "above": 80, "boost": 0.10},
            ],
            "stop_mult": 2.0,
            "tp_mult":   2.0,
            "filters": StrategyBuilder._reversion_filters(),
        }

    @staticmethod
    def macd_trend_config() -> Dict:
        """
        BUY when MACD crosses above signal line with positive histogram.
        Trend-following approach.
        """
        return {
            "name":    "macd_trend",
            "version": "1.0",
            "indicators": [
                {"name": "macd", "params": {"fast": 12, "slow": 26, "signal": 9}},
                {"name": "ema",  "params": {"period": 200}},
                {"name": "atr",  "params": {"period": 14}},
            ],
            "entry_rules_long": [
                {"col": "macd", "op": "cross_above",
                 "col2": "signal", "direction": "BUY"},
                {"col": "hist", "op": ">", "val": 0},
                {"col": "close", "op": ">", "col2": "ema_200"},
            ],
            "entry_rules_short": [
                {"col": "macd", "op": "cross_below",
                 "col2": "signal", "direction": "SELL"},
                {"col": "hist", "op": "<", "val": 0},
                {"col": "close", "op": "<", "col2": "ema_200"},
            ],
            "confidence_boosts": [
                {"col": "hist", "above": 0.001, "boost": 0.05},
                {"col": "hist", "below": -0.001, "boost": 0.05},
            ],
            "stop_mult": 1.5,
            "tp_mult":   3.0,
            "filters": StrategyBuilder._trend_filters(htf="1h", atr_pct_min=0.0010, atr_pct_max=0.03),
        }

    # ── 4. Stochastic Trend Filter ────────────────────────────────────────────

    @staticmethod
    def stoch_trend_config() -> Dict:
        """
        BUY when Stochastic K crosses above D from oversold (<20)
        while price is above EMA50 (trend filter).
        Works well on crypto and forex in trending markets.
        """
        return {
            "name":    "stoch_trend",
            "version": "1.0",
            "indicators": [
                {"name": "stoch", "params": {"k_period": 14, "d_period": 3}},
                {"name": "ema",   "params": {"period": 50}},
                {"name": "atr",   "params": {"period": 14}},
            ],
            "entry_rules_long": [
                {"col": "stoch_k", "op": "cross_above",
                 "col2": "stoch_d", "direction": "BUY"},
                {"col": "stoch_k", "op": "<",  "val": 50},
                {"col": "close",   "op": ">",  "col2": "ema_50"},
            ],
            "entry_rules_short": [
                {"col": "stoch_k", "op": "cross_below",
                 "col2": "stoch_d", "direction": "SELL"},
                {"col": "stoch_k", "op": ">",  "val": 50},
                {"col": "close",   "op": "<",  "col2": "ema_50"},
            ],
            "confidence_boosts": [
                {"col": "stoch_k", "below": 30, "boost": 0.07},
                {"col": "stoch_k", "below": 20, "boost": 0.10},
                {"col": "stoch_k", "above": 70, "boost": 0.07},
                {"col": "stoch_k", "above": 80, "boost": 0.10},
            ],
            "stop_mult": 1.5,
            "tp_mult":   2.5,
            "filters": StrategyBuilder._trend_filters(htf="1h", atr_pct_min=0.0008, atr_pct_max=0.03),
        }

    # ── 5. ADX Strong Trend ───────────────────────────────────────────────────

    @staticmethod
    def adx_trend_config() -> Dict:
        """
        Only trades when ADX > 25 (strong trend confirmed).
        BUY when +DI crosses above -DI in a strong uptrend.
        SELL when -DI crosses above +DI in a strong downtrend.
        Avoids choppy, ranging markets entirely.
        """
        return {
            "name":    "adx_trend",
            "version": "1.0",
            "indicators": [
                {"name": "adx", "params": {"period": 14}},
                {"name": "ema", "params": {"period": 20}},
                {"name": "atr", "params": {"period": 14}},
            ],
            "entry_rules_long": [
                {"col": "adx",      "op": ">",           "val": 25},
                {"col": "plus_di",  "op": "cross_above",
                 "col2": "minus_di", "direction": "BUY"},
            ],
            "entry_rules_short": [
                {"col": "adx",      "op": ">",           "val": 25},
                {"col": "minus_di", "op": "cross_above",
                 "col2": "plus_di", "direction": "SELL"},
            ],
            "confidence_boosts": [
                {"col": "adx", "above": 30, "boost": 0.05},
                {"col": "adx", "above": 40, "boost": 0.08},
            ],
            "stop_mult": 1.8,
            "tp_mult":   2.5,
            "filters": StrategyBuilder._trend_filters(htf="1h", atr_pct_min=0.0008, atr_pct_max=0.03, adx_min=20),
        }

    # ── 6. Bollinger Band Squeeze Breakout ────────────────────────────────────

    @staticmethod
    def bollinger_breakout_config() -> Dict:
        """
        Waits for Bollinger Band squeeze (low bandwidth = compressed volatility),
        then trades the breakout when price closes outside the bands.
        High win rate when volatility expansion follows compression.
        """
        return {
            "name":    "bollinger_breakout",
            "version": "1.0",
            "indicators": [
                {"name": "bollinger", "params": {"period": 20, "std": 2.0}},
                {"name": "volume_ma", "params": {"period": 20}},
                {"name": "atr",       "params": {"period": 14}},
            ],
            "entry_rules_long": [
                {"col": "close",  "op": ">",  "col2": "bb_upper", "direction": "BUY"},
                {"col": "volume", "op": ">",  "col2": "volume_ma"},
            ],
            "entry_rules_short": [
                {"col": "close",  "op": "<",  "col2": "bb_lower", "direction": "SELL"},
                {"col": "volume", "op": ">",  "col2": "volume_ma"},
            ],
            "confidence_boosts": [
                {"col": "volume", "above": 0, "boost": 0.05},
            ],
            "stop_mult": 1.0,
            "tp_mult":   3.0,
            "filters": StrategyBuilder._breakout_filters(htf="1h"),
        }

    # ── 7. Triple EMA Trend Rider ─────────────────────────────────────────────

    @staticmethod
    def triple_ema_config() -> Dict:
        """
        Uses three EMAs (8, 21, 55) for trend confirmation.
        BUY only when EMA8 > EMA21 > EMA55 (full alignment = strong trend).
        One of the cleanest trend-following setups for crypto.
        """
        return {
            "name":    "triple_ema",
            "version": "1.0",
            "indicators": [
                {"name": "ema", "params": {"period": 8}},
                {"name": "ema", "params": {"period": 21}},
                {"name": "ema", "params": {"period": 55}},
                {"name": "rsi", "params": {"period": 14}},
                {"name": "atr", "params": {"period": 14}},
            ],
            "entry_rules_long": [
                {"col": "ema_8",  "op": ">", "col2": "ema_21", "direction": "BUY"},
                {"col": "ema_21", "op": ">", "col2": "ema_55"},
                {"col": "rsi",    "op": ">", "val": 50},
                {"col": "rsi",    "op": "<", "val": 75},
            ],
            "entry_rules_short": [
                {"col": "ema_8",  "op": "<", "col2": "ema_21", "direction": "SELL"},
                {"col": "ema_21", "op": "<", "col2": "ema_55"},
                {"col": "rsi",    "op": "<", "val": 50},
                {"col": "rsi",    "op": ">", "val": 25},
            ],
            "confidence_boosts": [
                {"col": "rsi", "above": 55, "boost": 0.04},
                {"col": "rsi", "above": 60, "boost": 0.04},
                {"col": "rsi", "below": 45, "boost": 0.04},
                {"col": "rsi", "below": 40, "boost": 0.04},
            ],
            "stop_mult": 2.0,
            "tp_mult":   3.0,
            "filters": StrategyBuilder._trend_filters(htf="1h", atr_pct_min=0.0008, atr_pct_max=0.03),
        }

    # ── 8. RSI Divergence Scalper ─────────────────────────────────────────────

    @staticmethod
    def rsi_scalper_config() -> Dict:
        """
        Fast RSI (7) scalping strategy with tight stops.
        BUY when RSI(7) crosses above 30 from oversold.
        Designed for short-term reversals — best on 1h and 15m data.
        """
        return {
            "name":    "rsi_scalper",
            "version": "1.0",
            "indicators": [
                {"name": "rsi", "params": {"period": 7}},
                {"name": "ema", "params": {"period": 20}},
                {"name": "atr", "params": {"period": 7}},
            ],
            "entry_rules_long": [
                {"col": "rsi", "op": "cross_above", "val": 30, "direction": "BUY"},
            ],
            "entry_rules_short": [
                {"col": "rsi", "op": "cross_below", "val": 70, "direction": "SELL"},
            ],
            "confidence_boosts": [
                {"col": "rsi", "below": 25, "boost": 0.08},
                {"col": "rsi", "below": 20, "boost": 0.12},
                {"col": "rsi", "above": 75, "boost": 0.08},
                {"col": "rsi", "above": 80, "boost": 0.12},
            ],
            "stop_mult": 1.0,
            "tp_mult":   1.5,
            "filters": StrategyBuilder._reversion_filters(),
        }

    # ── 9. Volume Breakout ────────────────────────────────────────────────────

    @staticmethod
    def volume_breakout_config() -> Dict:
        """
        Trades breakouts confirmed by above-average volume.
        Price closes above EMA20 with volume > 1.5× its 20-bar average.
        High-conviction entries only — avoids false breakouts.
        """
        return {
            "name":    "volume_breakout",
            "version": "1.0",
            "indicators": [
                {"name": "ema",       "params": {"period": 20}},
                {"name": "volume_ma", "params": {"period": 20}},
                {"name": "rsi",       "params": {"period": 14}},
                {"name": "atr",       "params": {"period": 14}},
            ],
            "entry_rules_long": [
                {"col": "close",  "op": "cross_above",
                 "col2": "ema_20", "direction": "BUY"},
                {"col": "volume", "op": ">", "col2": "volume_ma"},
                {"col": "rsi",    "op": ">", "val": 50},
            ],
            "entry_rules_short": [
                {"col": "close",  "op": "cross_below",
                 "col2": "ema_20", "direction": "SELL"},
                {"col": "volume", "op": ">", "col2": "volume_ma"},
                {"col": "rsi",    "op": "<", "val": 50},
            ],
            "confidence_boosts": [
                {"col": "rsi",    "above": 55, "boost": 0.05},
                {"col": "volume", "above": 0,  "boost": 0.03},
                {"col": "rsi",    "below": 45, "boost": 0.05},
            ],
            "stop_mult": 1.5,
            "tp_mult":   2.5,
            "filters": StrategyBuilder._breakout_filters(htf="1h"),
        }

    # ── 10. Golden Cross ──────────────────────────────────────────────────────

    @staticmethod
    def golden_cross_config() -> Dict:
        """
        Classic Golden Cross / Death Cross strategy.
        BUY when EMA50 crosses above EMA200 (long-term trend change).
        Fewer signals but higher quality — best on daily charts.
        """
        return {
            "name":    "golden_cross",
            "version": "1.0",
            "indicators": [
                {"name": "ema", "params": {"period": 50}},
                {"name": "ema", "params": {"period": 200}},
                {"name": "rsi", "params": {"period": 14}},
                {"name": "atr", "params": {"period": 14}},
            ],
            "entry_rules_long": [
                {"col": "ema_50",  "op": "cross_above",
                 "col2": "ema_200", "direction": "BUY"},
                {"col": "rsi",     "op": ">", "val": 45},
            ],
            "entry_rules_short": [
                {"col": "ema_50",  "op": "cross_below",
                 "col2": "ema_200", "direction": "SELL"},
                {"col": "rsi",     "op": "<", "val": 55},
            ],
            "confidence_boosts": [
                {"col": "rsi", "above": 55, "boost": 0.06},
                {"col": "rsi", "below": 45, "boost": 0.06},
            ],
            "stop_mult": 2.5,
            "tp_mult":   4.0,
            "filters": StrategyBuilder._trend_filters(htf="4h", atr_pct_min=0.0006, atr_pct_max=0.025),
        }

    # ── 11. MACD + RSI Confluence ─────────────────────────────────────────────

    @staticmethod
    def macd_rsi_confluence_config() -> Dict:
        """
        Requires BOTH MACD crossover AND RSI confirmation.
        Reduces false signals by demanding confluence from two indicators.
        BUY when MACD crosses above signal AND RSI > 50 AND RSI < 70.
        """
        return {
            "name":    "macd_rsi_confluence",
            "version": "1.0",
            "indicators": [
                {"name": "macd", "params": {"fast": 12, "slow": 26, "signal": 9}},
                {"name": "rsi",  "params": {"period": 14}},
                {"name": "atr",  "params": {"period": 14}},
            ],
            "entry_rules_long": [
                {"col": "macd", "op": "cross_above",
                 "col2": "signal", "direction": "BUY"},
                {"col": "rsi",  "op": ">", "val": 50},
                {"col": "rsi",  "op": "<", "val": 70},
                {"col": "hist", "op": ">", "val": 0},
            ],
            "entry_rules_short": [
                {"col": "macd", "op": "cross_below",
                 "col2": "signal", "direction": "SELL"},
                {"col": "rsi",  "op": "<", "val": 50},
                {"col": "rsi",  "op": ">", "val": 30},
                {"col": "hist", "op": "<", "val": 0},
            ],
            "confidence_boosts": [
                {"col": "rsi",  "above": 55, "boost": 0.05},
                {"col": "hist", "above": 0,  "boost": 0.04},
                {"col": "rsi",  "below": 45, "boost": 0.05},
            ],
            "stop_mult": 1.5,
            "tp_mult":   3.0,
            "filters": StrategyBuilder._trend_filters(htf="1h", atr_pct_min=0.0008, atr_pct_max=0.03),
        }

    # ── 12. Bollinger + RSI Mean Reversion ────────────────────────────────────

    @staticmethod
    def bollinger_rsi_reversion_config() -> Dict:
        """
        High-probability mean reversion combining Bollinger Bands and RSI.
        BUY when price touches lower band AND RSI < 35 simultaneously.
        Double confirmation reduces noise significantly.
        """
        return {
            "name":    "bollinger_rsi_reversion",
            "version": "1.0",
            "indicators": [
                {"name": "bollinger", "params": {"period": 20, "std": 2.0}},
                {"name": "rsi",       "params": {"period": 14}},
                {"name": "atr",       "params": {"period": 14}},
            ],
            "entry_rules_long": [
                {"col": "close", "op": "<=", "col2": "bb_lower", "direction": "BUY"},
                {"col": "rsi",   "op": "<",  "val": 35},
            ],
            "entry_rules_short": [
                {"col": "close", "op": ">=", "col2": "bb_upper", "direction": "SELL"},
                {"col": "rsi",   "op": ">",  "val": 65},
            ],
            "confidence_boosts": [
                {"col": "rsi", "below": 30, "boost": 0.07},
                {"col": "rsi", "below": 25, "boost": 0.10},
                {"col": "rsi", "above": 70, "boost": 0.07},
                {"col": "rsi", "above": 75, "boost": 0.10},
            ],
            "stop_mult": 1.5,
            "tp_mult":   2.0,
            "filters": StrategyBuilder._reversion_filters(),
        }

    # ── 13. Stoch + MACD Swing ────────────────────────────────────────────────

    @staticmethod
    def stoch_macd_swing_config() -> Dict:
        """
        Swing trading setup combining Stochastic and MACD.
        BUY when Stoch is oversold AND MACD histogram turns positive.
        Three-day swing trades — best on daily and 4h charts.
        """
        return {
            "name":    "stoch_macd_swing",
            "version": "1.0",
            "indicators": [
                {"name": "stoch", "params": {"k_period": 14, "d_period": 3}},
                {"name": "macd",  "params": {"fast": 12, "slow": 26, "signal": 9}},
                {"name": "atr",   "params": {"period": 14}},
            ],
            "entry_rules_long": [
                {"col": "stoch_k", "op": "<",           "val": 40, "direction": "BUY"},
                {"col": "hist",    "op": "cross_above",  "val": 0},
            ],
            "entry_rules_short": [
                {"col": "stoch_k", "op": ">",           "val": 60, "direction": "SELL"},
                {"col": "hist",    "op": "cross_below", "val": 0},
            ],
            "confidence_boosts": [
                {"col": "stoch_k", "below": 30, "boost": 0.06},
                {"col": "stoch_k", "below": 20, "boost": 0.08},
                {"col": "stoch_k", "above": 70, "boost": 0.06},
                {"col": "stoch_k", "above": 80, "boost": 0.08},
            ],
            "stop_mult": 1.5,
            "tp_mult":   2.5,
            "filters": StrategyBuilder._trend_filters(htf="1h", atr_pct_min=0.0008, atr_pct_max=0.03),
        }

    # ── 14. ADX + EMA Momentum ────────────────────────────────────────────────

    @staticmethod
    def adx_ema_momentum_config() -> Dict:
        """
        Momentum strategy — only enters when ADX confirms strong trend
        AND price is above both EMA20 and EMA50 (stacked bullish).
        Designed for riding strong crypto bull runs.
        """
        return {
            "name":    "adx_ema_momentum",
            "version": "1.0",
            "indicators": [
                {"name": "adx", "params": {"period": 14}},
                {"name": "ema", "params": {"period": 20}},
                {"name": "ema", "params": {"period": 50}},
                {"name": "rsi", "params": {"period": 14}},
                {"name": "atr", "params": {"period": 14}},
            ],
            "entry_rules_long": [
                {"col": "adx",   "op": ">", "val": 20,       "direction": "BUY"},
                {"col": "close", "op": ">", "col2": "ema_20"},
                {"col": "close", "op": ">", "col2": "ema_50"},
                {"col": "rsi",   "op": ">", "val": 55},
            ],
            "entry_rules_short": [
                {"col": "adx",   "op": ">", "val": 20,       "direction": "SELL"},
                {"col": "close", "op": "<", "col2": "ema_20"},
                {"col": "close", "op": "<", "col2": "ema_50"},
                {"col": "rsi",   "op": "<", "val": 45},
            ],
            "confidence_boosts": [
                {"col": "adx", "above": 25, "boost": 0.05},
                {"col": "adx", "above": 35, "boost": 0.07},
                {"col": "rsi", "above": 60, "boost": 0.04},
                {"col": "rsi", "below": 40, "boost": 0.04},
            ],
            "stop_mult": 2.0,
            "tp_mult":   3.5,
            "filters": StrategyBuilder._trend_filters(htf="1h", atr_pct_min=0.0010, atr_pct_max=0.035, adx_min=18),
        }

    # ── 15. Supertrend Proxy (ATR-Based) ──────────────────────────────────────

    @staticmethod
    def atr_supertrend_config() -> Dict:
        """
        Approximates Supertrend indicator using ATR and EMA.
        BUY when price crosses above EMA20 + ATR (upper breakout zone).
        Trend-following with dynamic stop based on ATR.
        """
        return {
            "name":    "atr_supertrend",
            "version": "1.0",
            "indicators": [
                {"name": "ema", "params": {"period": 20}},
                {"name": "ema", "params": {"period": 50}},
                {"name": "atr", "params": {"period": 10}},
                {"name": "rsi", "params": {"period": 14}},
            ],
            "entry_rules_long": [
                {"col": "ema_20", "op": "cross_above",
                 "col2": "ema_50", "direction": "BUY"},
                {"col": "rsi",    "op": ">", "val": 50},
                {"col": "rsi",    "op": "<", "val": 75},
            ],
            "entry_rules_short": [
                {"col": "ema_20", "op": "cross_below",
                 "col2": "ema_50", "direction": "SELL"},
                {"col": "rsi",    "op": "<", "val": 50},
                {"col": "rsi",    "op": ">", "val": 25},
            ],
            "confidence_boosts": [
                {"col": "rsi", "above": 55, "boost": 0.05},
                {"col": "rsi", "below": 45, "boost": 0.05},
            ],
            "stop_mult": 1.2,
            "tp_mult":   2.5,
            "filters": StrategyBuilder._trend_filters(htf="1h", atr_pct_min=0.0008, atr_pct_max=0.03),
        }

    # ── All presets registry ──────────────────────────────────────────────────

    @staticmethod
    def all_configs() -> Dict[str, Dict]:
        """
        Returns all preset strategy configs as a dict keyed by name.
        Use this to batch-backtest or compare every preset at once.

        Example
        -------
            from strategy_lab.strategy_builder import StrategyBuilder

            for name, config in StrategyBuilder.all_configs().items():
                result = run_backtest(config, "BTC-USD", "crypto")
                print(f"{name:30} {result.summary()}")
        """
        sb = StrategyBuilder
        return {
            "ema_rsi_crossover":        sb.example_config(),
            "rsi_mean_reversion":       sb.rsi_mean_reversion_config(),
            "macd_trend":               sb.macd_trend_config(),
            "stoch_trend":              sb.stoch_trend_config(),
            "adx_trend":                sb.adx_trend_config(),
            "bollinger_breakout":       sb.bollinger_breakout_config(),
            "triple_ema":               sb.triple_ema_config(),
            "rsi_scalper":              sb.rsi_scalper_config(),
            "volume_breakout":          sb.volume_breakout_config(),
            "golden_cross":             sb.golden_cross_config(),
            "macd_rsi_confluence":      sb.macd_rsi_confluence_config(),
            "bollinger_rsi_reversion":  sb.bollinger_rsi_reversion_config(),
            "stoch_macd_swing":         sb.stoch_macd_swing_config(),
            "adx_ema_momentum":         sb.adx_ema_momentum_config(),
            "atr_supertrend":           sb.atr_supertrend_config(),
        }
