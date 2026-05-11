from __future__ import annotations
import time
from typing import Dict, Optional
from execution.exchange_adapter import ExchangeAdapter, OrderRequest, OrderResult
from utils.logger import get_logger

logger = get_logger()

_DEFAULT_ROUTING = {
    "crypto":      "paper",
    "forex":       "paper",
    "commodities": "paper",
    "stocks":      "paper",
    "indices":     "paper",
}

_MAX_RETRIES    = 3
_RETRY_BASE_SEC = 0.5   # exponential: 0.5, 1.0, 2.0


class ExchangeRouter:
    """
    Maintains a registry of exchange adapters and routes
    order requests based on asset category.
    """

    def __init__(self):
        self._adapters: Dict[str, ExchangeAdapter] = {}
        self._routing:  Dict[str, str]             = dict(_DEFAULT_ROUTING)
        self._asset_routing: Dict[str, str]         = {}

    def register(self, name: str, adapter: ExchangeAdapter) -> None:
        self._adapters[name] = adapter
        logger.info(f"[Router] Registered exchange adapter: {name}")

    def has_adapter(self, name: str) -> bool:
        return str(name or "") in self._adapters

    def set_route(self, category: str, adapter_name: str) -> None:
        """Override routing for a category. E.g. route crypto to Binance."""
        self._routing[category] = adapter_name
        logger.info(f"[Router] Route {category} → {adapter_name}")

    def set_asset_route(self, asset: str, adapter_name: str) -> None:
        """Override routing for one canonical asset."""
        self._asset_routing[str(asset or "").upper()] = adapter_name
        logger.info(f"[Router] Route asset {asset} → {adapter_name}")

    def reset_routes(self) -> None:
        self._routing = dict(_DEFAULT_ROUTING)
        self._asset_routing = {}
        logger.info("[Router] Execution routes reset to paper defaults")

    def route_snapshot(self) -> dict:
        return {
            "routing": dict(self._routing),
            "asset_routing": dict(self._asset_routing),
            "adapters": sorted(self._adapters.keys()),
        }

    def submit(self, signal: dict) -> Optional[OrderResult]:
        """
        Convert a signal dict to an OrderRequest and route it.
        Retries on transient failure with exponential backoff.
        """
        category = signal.get("category", "crypto")
        adapter  = self._get_adapter(category, asset=signal.get("asset", ""))
        if adapter is None:
            logger.error(f"[Router] No adapter for category: {category}")
            return None

        if not adapter.is_available:
            logger.warning(f"[Router] Adapter {adapter.name} unavailable (circuit open)")
            return None

        local_quantity = float(signal.get("position_size", 0) or 0)
        broker_quantity = signal.get("broker_position_size")
        quantity = float(broker_quantity if broker_quantity is not None else local_quantity)
        symbol = str(signal.get("broker_symbol") or signal.get("asset", ""))
        metadata = dict(signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {})
        metadata.setdefault("confidence", signal.get("confidence"))
        metadata.setdefault("strategy_id", signal.get("strategy_id"))
        metadata.setdefault("take_profit_levels", signal.get("take_profit_levels", []))
        req = OrderRequest(
            symbol      = symbol,
            side        = (signal.get("direction") or signal.get("signal", "BUY")).upper(),
            quantity    = quantity,
            asset       = str(signal.get("asset", "")),
            category    = str(category or ""),
            local_quantity = local_quantity,
            broker_quantity = float(broker_quantity) if broker_quantity is not None else None,
            order_type  = signal.get("order_type", "MARKET"),
            price       = signal.get("entry_price"),
            stop_loss   = signal.get("stop_loss"),
            take_profit = signal.get("take_profit"),
            metadata    = metadata,
        )

        result = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                result = adapter.place_order(req)
            except Exception as exc:
                logger.warning(
                    f"[Router] Attempt {attempt}/{_MAX_RETRIES} raised exception "
                    f"for {req.symbol}: {exc}"
                )
                wait = _RETRY_BASE_SEC * (2 ** (attempt - 1))
                time.sleep(wait)
                continue

            if result.status == "FILLED":
                raw = result.raw if isinstance(result.raw, dict) else {}
                broker_sizing = raw.get("broker_sizing") if isinstance(raw.get("broker_sizing"), dict) else {}
                broker_size = broker_sizing.get("broker_size")
                broker_part = f" broker_size={broker_size}" if broker_size not in (None, "") else ""
                logger.info(
                    f"[Router] {adapter.name} filled {req.side} {req.symbol} "
                    f"local_qty={result.filled_qty}{broker_part} @ {result.avg_price}"
                )
                return result
            if result.status == "FAILED" and self._is_no_retry_error(result.error):
                logger.warning(f"[Router] No-retry broker error for {req.symbol}: {result.error}")
                return result
            if result.status == "FAILED" and self._is_permanent_error(result.error):
                logger.error(f"[Router] Permanent error for {req.symbol}: {result.error}")
                return result
            # Transient failure — retry with backoff
            wait = _RETRY_BASE_SEC * (2 ** (attempt - 1))
            logger.warning(
                f"[Router] Attempt {attempt}/{_MAX_RETRIES} failed for {req.symbol}: "
                f"{result.error} — retrying in {wait:.1f}s"
            )
            time.sleep(wait)

        logger.error(f"[Router] All {_MAX_RETRIES} attempts failed for {req.symbol}")
        # result is guaranteed to be set (None if all attempts raised, else last OrderResult)
        return result

    def close_position(self, position: dict, *, reason: str = "Manual Close") -> Optional[OrderResult]:
        category = str(position.get("category") or "forex")
        asset = str(position.get("asset") or "")
        broker_name = str(position.get("broker") or "").lower()
        adapter = self._adapters.get(broker_name) if broker_name else None
        if adapter is None:
            adapter = self._get_adapter(category, asset=asset)
        if adapter is None:
            logger.error(f"[Router] No close adapter for {asset or category}")
            return None
        close_fn = getattr(adapter, "close_position", None)
        if not callable(close_fn):
            return OrderResult(order_id="", status="FAILED", error=f"{adapter.name} does not support broker close")
        try:
            return close_fn(position, reason=reason)
        except Exception as exc:
            logger.error(f"[Router] Close failed for {asset}: {exc}")
            return OrderResult(order_id="", status="FAILED", error=str(exc))

    def partial_close_position(
        self,
        position: dict,
        *,
        local_close_size: float,
        reason: str = "Partial Close",
    ) -> Optional[OrderResult]:
        category = str(position.get("category") or "forex")
        asset = str(position.get("asset") or "")
        broker_name = str(position.get("broker") or "").lower()
        adapter = self._adapters.get(broker_name) if broker_name else None
        if adapter is None:
            adapter = self._get_adapter(category, asset=asset)
        if adapter is None:
            logger.error(f"[Router] No partial-close adapter for {asset or category}")
            return None
        close_fn = getattr(adapter, "partial_close_position", None)
        if not callable(close_fn):
            return OrderResult(order_id="", status="FAILED", error=f"{adapter.name} does not support partial broker close")
        try:
            return close_fn(position, local_close_size=local_close_size, reason=reason)
        except Exception as exc:
            logger.error(f"[Router] Partial close failed for {asset}: {exc}")
            return OrderResult(order_id="", status="FAILED", error=str(exc))

    def update_position_stop(
        self,
        position: dict,
        *,
        stop_level: float,
        reason: str = "Managed Stop Update",
    ) -> Optional[OrderResult]:
        category = str(position.get("category") or "forex")
        asset = str(position.get("asset") or "")
        broker_name = str(position.get("broker") or "").lower()
        adapter = self._adapters.get(broker_name) if broker_name else None
        if adapter is None:
            adapter = self._get_adapter(category, asset=asset)
        if adapter is None:
            logger.error(f"[Router] No stop-update adapter for {asset or category}")
            return None
        update_fn = getattr(adapter, "update_position_stop", None)
        if not callable(update_fn):
            return OrderResult(order_id="", status="FAILED", error=f"{adapter.name} does not support stop updates")
        try:
            return update_fn(position, stop_level=stop_level, reason=reason)
        except Exception as exc:
            logger.error(f"[Router] Stop update failed for {asset}: {exc}")
            return OrderResult(order_id="", status="FAILED", error=str(exc))

    def list_open_positions(self, broker_name: str) -> list[dict]:
        adapter = self._adapters.get(str(broker_name or "").lower())
        if adapter is None:
            return []
        list_fn = getattr(adapter, "list_open_positions", None)
        if not callable(list_fn):
            return []
        try:
            return list(list_fn() or [])
        except Exception as exc:
            logger.warning(f"[Router] Broker position list failed for {broker_name}: {exc}")
            raise

    def check_support(self, signal: dict) -> tuple[bool, str]:
        category = str(signal.get("category") or "crypto")
        asset = str(signal.get("asset") or signal.get("symbol") or "")
        adapter = self._get_adapter(category, asset=asset)
        if adapter is None:
            return False, f"no adapter for {asset or category}"
        supports_fn = getattr(adapter, "supports_asset", None)
        if not callable(supports_fn):
            return True, ""
        try:
            return supports_fn(asset, category)
        except Exception as exc:
            return False, str(exc)

    def get_balance(self, category: str = "forex", asset: str = "", currency: str = "USD") -> float:
        adapter = self._get_adapter(category, asset=asset)
        if adapter is None:
            return 0.0
        return float(adapter.get_balance(currency) or 0.0)

    def _get_adapter(self, category: str, asset: str = "") -> Optional[ExchangeAdapter]:
        name = self._asset_routing.get(str(asset or "").upper()) or self._routing.get(category, "paper")
        adapter = self._adapters.get(name)
        if adapter is None:
            logger.warning(f"[Router] Adapter '{name}' not registered — trying 'paper'")
            adapter = self._adapters.get("paper")
        return adapter

    # FIX: Add this missing static method
    @staticmethod
    def _is_no_retry_error(error: str) -> bool:
        if not error:
            return False
        error_lower = error.lower()
        return any(
            phrase in error_lower
            for phrase in (
                "broker_temporarily_unavailable",
                "exceeded-api-key-allowance",
                "ig_confirm_pending",
                "ctrader_execution_unknown",
            )
        )

    @staticmethod
    def _is_permanent_error(error: str) -> bool:
        """Check if an error is permanent (no point retrying)."""
        if not error:
            return False
        error_lower = error.lower()
        permanent_phrases = [
            "insufficient balance",
            "invalid symbol",
            "symbol not found",
            "min order size",
            "account suspended",
            "invalid api key",
            "permission denied",
            "broker_permission_denied",
            "unauthorised access",
            "unauthorized access",
            "no access to the relevant exchange",
            "apiuser has no access",
            "market closed",
            "below_broker_min_size",
            "broker_min_size",
            "dry-run",
            "read-only",
            "epic not found",
            "contract spec missing",
            "attached_order_level_error",
            "attached order rejected locally",
            "attached stop missing",
            "attached limit missing",
            "requested stop distance too wide",
            "broker minimum stop distance too wide",
        ]
        return any(phrase in error_lower for phrase in permanent_phrases)
