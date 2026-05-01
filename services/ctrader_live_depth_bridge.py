from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from config.config import (
    CTRADER_LIVE_DEPTH_ACCESS_TOKEN,
    CTRADER_LIVE_DEPTH_ACCOUNT_ID,
    CTRADER_LIVE_DEPTH_ASSETS,
    CTRADER_LIVE_DEPTH_CLIENT_ID,
    CTRADER_LIVE_DEPTH_CLIENT_SECRET,
    CTRADER_LIVE_DEPTH_CMD,
    CTRADER_LIVE_DEPTH_ENABLED,
    CTRADER_LIVE_DEPTH_ENVIRONMENT,
    CTRADER_LIVE_DEPTH_MAX_LEVELS,
    CTRADER_LIVE_DEPTH_MIN_EMIT_MS,
    CTRADER_LIVE_DEPTH_REDIRECT_URI,
    CTRADER_LIVE_DEPTH_REFRESH_TOKEN,
    CTRADER_LIVE_DEPTH_STORE_PATH,
    CTRADER_LIVE_DEPTH_TOKEN_CACHE_PATH,
)
from core.assets import registry
from utils.logger import get_logger

logger = get_logger()

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

if sys.platform == "linux":
    _SIDECAR_SCRIPT = Path("/opt/forex_prediction_bot/integrations/ctrader_depth_bridge/ctrader_depth_bridge.py")
else:
    _SIDECAR_SCRIPT = Path("integrations/ctrader_depth_bridge/ctrader_depth_bridge.py")

_STORE_WRITE_MIN_INTERVAL = 0.75
_DEFAULT_STALE_SECONDS = 30.0
_RESTART_COOLDOWN_SECONDS = 45.0

_SUPPORTED_LIVE_SYMBOLS: Dict[str, Dict[str, Any]] = {
    "EUR/USD": {"category": "forex", "aliases": ("EURUSD",)},
    "EUR/JPY": {"category": "forex", "aliases": ("EURJPY",)},
    "EUR/GBP": {"category": "forex", "aliases": ("EURGBP",)},
    "GBP/JPY": {"category": "forex", "aliases": ("GBPJPY",)},
    "GBP/USD": {"category": "forex", "aliases": ("GBPUSD",)},
    "AUD/USD": {"category": "forex", "aliases": ("AUDUSD",)},
    "NZD/USD": {"category": "forex", "aliases": ("NZDUSD",)},
    "USD/JPY": {"category": "forex", "aliases": ("USDJPY",)},
    "USD/CAD": {"category": "forex", "aliases": ("USDCAD",)},
    "USD/CHF": {"category": "forex", "aliases": ("USDCHF",)},
    "XAU/USD": {"category": "commodities", "aliases": ("XAUUSD", "GOLD")},
    "XAG/USD": {"category": "commodities", "aliases": ("XAGUSD", "SILVER")},
    "WTI": {"category": "commodities", "aliases": ("USOIL", "WTI", "CRUDE", "USCRUDE")},
    "US30": {"category": "indices", "aliases": ("US30", "DJ30", "WALLSTREET30")},
    "US100": {"category": "indices", "aliases": ("US100", "USTEC", "NAS100", "NASDAQ100")},
    "US500": {"category": "indices", "aliases": ("US500", "SPX500", "SP500")},
    "UK100": {"category": "indices", "aliases": ("UK100", "FTSE100")},
    "GER40": {"category": "indices", "aliases": ("GER40", "DE40", "DAX40")},
    "AUS200": {"category": "indices", "aliases": ("AUS200", "AU200")},
    "JPN225": {"category": "indices", "aliases": ("JPN225", "JP225", "JAP225", "NI225")},
}


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value if value not in (None, "") else default)
    except Exception:
        return default


def _resolve_runtime_path(path_like: Path | str) -> Path:
    candidate = Path(path_like).expanduser()
    if not candidate.is_absolute():
        candidate = _PROJECT_ROOT / candidate
    return candidate.resolve(strict=False)


def _normalize_asset_list(raw: str) -> tuple[str, ...]:
    values = [item.strip() for item in str(raw or "").split(",") if item.strip()]
    if not values:
        return tuple(_SUPPORTED_LIVE_SYMBOLS.keys())

    assets: List[str] = []
    for item in values:
        canonical = registry.canonical(item)
        if canonical in _SUPPORTED_LIVE_SYMBOLS and canonical not in assets:
            assets.append(canonical)
    return tuple(assets)


def _normalize_levels(levels: Any, *, max_levels: int = 0) -> List[Dict[str, float]]:
    result: List[Dict[str, float]] = []
    for raw in list(levels or []):
        if isinstance(raw, dict):
            bid = raw.get("bid")
            ask = raw.get("ask")
            bid_size = raw.get("bid_size")
            ask_size = raw.get("ask_size")
        elif isinstance(raw, (list, tuple)) and len(raw) >= 4:
            bid, ask, bid_size, ask_size = raw[:4]
        else:
            continue
        entry = {
            "bid": _safe_float(bid, 0.0) if bid not in (None, "") else None,
            "ask": _safe_float(ask, 0.0) if ask not in (None, "") else None,
            "bid_size": _safe_float(bid_size, 0.0) if bid_size not in (None, "") else None,
            "ask_size": _safe_float(ask_size, 0.0) if ask_size not in (None, "") else None,
        }
        result.append(entry)
        if max_levels > 0 and len(result) >= max_levels:
            break
    return result


def _clip_text(text: str, limit: int = 300) -> str:
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 1)].rstrip() + "…"


class CTraderLiveDepthBridge:
    def __init__(
        self,
        *,
        enabled: Optional[bool] = None,
        store_path: Path | str = CTRADER_LIVE_DEPTH_STORE_PATH,
        token_cache_path: Path | str = CTRADER_LIVE_DEPTH_TOKEN_CACHE_PATH,
        assets: Optional[tuple[str, ...]] = None,
        environment: str = CTRADER_LIVE_DEPTH_ENVIRONMENT,
        client_id: str = CTRADER_LIVE_DEPTH_CLIENT_ID,
        client_secret: str = CTRADER_LIVE_DEPTH_CLIENT_SECRET,
        access_token: str = CTRADER_LIVE_DEPTH_ACCESS_TOKEN,
        refresh_token: str = CTRADER_LIVE_DEPTH_REFRESH_TOKEN,
        account_id: str = CTRADER_LIVE_DEPTH_ACCOUNT_ID,
        redirect_uri: str = CTRADER_LIVE_DEPTH_REDIRECT_URI,
        command_text: str = CTRADER_LIVE_DEPTH_CMD,
        min_emit_ms: int = CTRADER_LIVE_DEPTH_MIN_EMIT_MS or 150,
        max_levels: int = CTRADER_LIVE_DEPTH_MAX_LEVELS or 20,
    ) -> None:
        self._enabled = bool(CTRADER_LIVE_DEPTH_ENABLED if enabled is None else enabled)
        self._store_path = _resolve_runtime_path(store_path)
        self._store_path.parent.mkdir(parents=True, exist_ok=True)
        self._token_cache_path = _resolve_runtime_path(token_cache_path)
        self._token_cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._assets = tuple(assets or _normalize_asset_list(CTRADER_LIVE_DEPTH_ASSETS))
        self._environment = str(environment or "demo").strip().lower() or "demo"
        self._client_id = str(client_id or "").strip()
        self._client_secret = str(client_secret or "").strip()
        self._access_token = str(access_token or "").strip()
        self._refresh_token = str(refresh_token or "").strip()
        self._account_id = str(account_id or "").strip()
        self._redirect_uri = str(redirect_uri or "http://localhost").strip() or "http://localhost"
        self._command_text = str(command_text or "").strip()
        self._min_emit_ms = max(50, int(min_emit_ms or 150))
        self._max_levels = max(1, int(max_levels or 20))
        self._lock = threading.RLock()
        self._latest: Dict[str, Dict[str, Any]] = {}
        self._process: Optional[subprocess.Popen[str]] = None
        self._stdout_thread: Optional[threading.Thread] = None
        self._stderr_thread: Optional[threading.Thread] = None
        self._last_store_mtime = 0.0
        self._last_persist = 0.0
        self._last_restart_attempt = 0.0
        self._last_started_at = 0.0
        self._restart_count = 0
        self._load_store(force=True)

    def list_profiles(self) -> list[str]:
        if not self._enabled or not self._assets:
            return []
        if self._store_path.exists() or self._command_text or self._credentials_ready(log_warning=False):
            return ["ctrader_live_depth"]
        return []

    def supports(self, asset: str, category: str = "") -> bool:
        canonical = registry.canonical(str(asset or "").strip())
        if canonical not in _SUPPORTED_LIVE_SYMBOLS:
            return False
        if category and str(category).strip().lower() == "crypto":
            return False
        return True

    def _credentials_ready(self, *, log_warning: bool = True) -> bool:
        ready = bool(self._client_id and self._client_secret)
        if not ready and log_warning:
            logger.warning("[CTraderDepth] credentials missing — set CTRADER_LIVE_DEPTH_CLIENT_ID/CLIENT_SECRET")
        return ready

    def _sidecar_env(self) -> Dict[str, str]:
        env = os.environ.copy()
        env["CTRADER_LIVE_DEPTH_ENVIRONMENT"] = self._environment
        env["CTRADER_LIVE_DEPTH_CLIENT_ID"] = self._client_id
        env["CTRADER_LIVE_DEPTH_CLIENT_SECRET"] = self._client_secret
        env["CTRADER_LIVE_DEPTH_ACCESS_TOKEN"] = self._access_token
        env["CTRADER_LIVE_DEPTH_REFRESH_TOKEN"] = self._refresh_token
        env["CTRADER_LIVE_DEPTH_ACCOUNT_ID"] = self._account_id
        env["CTRADER_LIVE_DEPTH_REDIRECT_URI"] = self._redirect_uri
        env["CTRADER_LIVE_DEPTH_ASSETS"] = ",".join(self._assets)
        env["CTRADER_LIVE_DEPTH_MIN_EMIT_MS"] = str(self._min_emit_ms)
        env["CTRADER_LIVE_DEPTH_MAX_LEVELS"] = str(self._max_levels)
        env["CTRADER_LIVE_DEPTH_STORE_PATH"] = str(self._store_path)
        env["CTRADER_LIVE_DEPTH_TOKEN_CACHE_PATH"] = str(self._token_cache_path)
        return env

    def _default_command(self) -> list[str]:
        return [sys.executable, str(_SIDECAR_SCRIPT)]

    def _sidecar_command(self) -> list[str]:
        if self._command_text:
            return shlex.split(self._command_text, posix=os.name != "nt")
        return self._default_command()

    def start_background(self) -> bool:
        if not self._enabled:
            return False
        with self._lock:
            proc = self._process
            if proc is not None and proc.poll() is None:
                return True
        if not self._command_text and not self._credentials_ready(log_warning=True):
            return False
        command = self._sidecar_command()
        if not command:
            return False
        try:
            # Use project root as cwd to avoid relative path issues
            project_root = Path(__file__).parent.parent.resolve()
            proc = subprocess.Popen(
                command,
                cwd=str(project_root),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                env=self._sidecar_env(),
            )
        except Exception as exc:
            logger.warning(f"[CTraderDepth] sidecar start failed: {exc}")
            return False

        with self._lock:
            self._process = proc
            self._last_started_at = time.time()
            self._stdout_thread = threading.Thread(target=self._stdout_loop, daemon=True, name="CTraderDepthStdout")
            self._stderr_thread = threading.Thread(target=self._stderr_loop, daemon=True, name="CTraderDepthStderr")
            self._stdout_thread.start()
            self._stderr_thread.start()
        logger.info(f"[CTraderDepth] sidecar started (PID {proc.pid}) for {len(self._assets)} assets")
        return True

    def stop(self) -> None:
        with self._lock:
            proc = self._process
            self._process = None
        if proc is None or proc.poll() is not None:
            return
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        except Exception:
            pass

    def _stdout_loop(self) -> None:
        proc = self._process
        if proc is None or proc.stdout is None:
            return
        for raw_line in proc.stdout:
            line = str(raw_line or "").strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                logger.debug(f"[CTraderDepth] non-JSON stdout: {_clip_text(line, 200)}")
                continue
            if isinstance(payload, dict):
                try:
                    self.ingest_snapshot(payload)
                except Exception as exc:
                    logger.warning(f"[CTraderDepth] snapshot ingest failed: {exc}")

    def _stderr_loop(self) -> None:
        proc = self._process
        if proc is None or proc.stderr is None:
            return
        for raw_line in proc.stderr:
            line = str(raw_line or "").strip()
            if line:
                if line.startswith("[DEBUG]"):
                    logger.debug(f"[CTraderDepth] {line}")
                else:
                    logger.info(f"[CTraderDepth] {line}")

    def _load_store(self, *, force: bool = False) -> None:
        if not self._store_path.exists():
            return
        try:
            stat = self._store_path.stat()
        except Exception:
            return
        if not force and stat.st_mtime <= self._last_store_mtime:
            return
        try:
            payload = json.loads(self._store_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.debug(f"[CTraderDepth] store read failed: {exc}")
            return

        assets = payload.get("assets") if isinstance(payload, dict) else {}
        if not isinstance(assets, dict):
            return
        with self._lock:
            self._latest = {
                str(asset): dict(snapshot)
                for asset, snapshot in assets.items()
                if isinstance(snapshot, dict)
            }
            self._last_store_mtime = stat.st_mtime

    def _persist_store_locked(self, *, force: bool = False) -> None:
        now = time.time()
        if not force and now - self._last_persist < _STORE_WRITE_MIN_INTERVAL:
            return
        payload = {"updated_at": _utc_now_iso(), "assets": self._latest}
        tmp = self._store_path.with_name(f"{self._store_path.name}.{os.getpid()}.tmp")
        try:
            tmp.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(json.dumps(payload, ensure_ascii=True, separators=(",", ":")), encoding="utf-8")
            tmp.replace(self._store_path)
            self._last_persist = now
            try:
                self._last_store_mtime = self._store_path.stat().st_mtime
            except Exception:
                pass
        except Exception as exc:
            logger.warning(f"[CTraderDepth] store persist failed: {exc}")
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass

    def ingest_snapshot(self, payload: Dict[str, Any]) -> None:
        canonical = registry.canonical(str(payload.get("asset") or "").strip())
        if canonical not in _SUPPORTED_LIVE_SYMBOLS:
            return
        spec = _SUPPORTED_LIVE_SYMBOLS[canonical]
        levels = _normalize_levels(payload.get("levels"), max_levels=self._max_levels)
        bid = _safe_float(payload.get("bid"), 0.0) or None
        ask = _safe_float(payload.get("ask"), 0.0) or None
        price = _safe_float(payload.get("price"), 0.0)
        if price <= 0.0:
            if bid is not None and ask is not None:
                price = (bid + ask) / 2.0
            else:
                price = ask or bid or 0.0
        if price <= 0.0:
            return

        total_bid_volume = _safe_float(payload.get("total_bid_volume"), 0.0)
        total_ask_volume = _safe_float(payload.get("total_ask_volume"), 0.0)
        if total_bid_volume <= 0.0:
            total_bid_volume = sum(_safe_float(level.get("bid_size"), 0.0) for level in levels)
        if total_ask_volume <= 0.0:
            total_ask_volume = sum(_safe_float(level.get("ask_size"), 0.0) for level in levels)

        event = {
            "asset": canonical,
            "category": spec["category"],
            "symbol_name": str(payload.get("symbol_name") or ""),
            "symbol_id": str(payload.get("symbol_id") or ""),
            "bid": bid,
            "ask": ask,
            "price": float(price),
            "bid_size": _safe_float(payload.get("bid_size"), 0.0) or None,
            "ask_size": _safe_float(payload.get("ask_size"), 0.0) or None,
            "total_bid_volume": float(total_bid_volume),
            "total_ask_volume": float(total_ask_volume),
            "levels": levels,
            "as_of_utc": str(payload.get("as_of_utc") or _utc_now_iso()),
            "timestamp": _safe_float(payload.get("timestamp"), time.time()),
            "environment": str(payload.get("environment") or self._environment or "demo"),
            "realtime": True,
            "source": "cTrader",
            "source_class": "sidecar",
            "exchange": "ctrader",
            "broker": "IC Markets",
        }

        try:
            from services.live_microstructure_service import get_service as get_live_microstructure_service

            get_live_microstructure_service().record_quote(
                "ctrader",
                canonical,
                bid=event["bid"],
                ask=event["ask"],
                price=event["price"],
                bid_size=event["bid_size"],
                ask_size=event["ask_size"],
                levels=event["levels"],
                timestamp=event["timestamp"],
                flags="depth_snapshot,stream_snapshot",
            )
        except Exception:
            pass
        try:
            from monitoring.system_health_service import monitor as _monitor

            _monitor.ping_source("order_book")
            _monitor.ping_source("ctrader_live_depth")
        except Exception:
            pass

        with self._lock:
            self._latest[canonical] = event
            self._persist_store_locked()

    def get_latest_snapshot(self, asset: str) -> Dict[str, Any]:
        canonical = registry.canonical(str(asset or "").strip())
        self._load_store(force=False)
        with self._lock:
            return dict(self._latest.get(canonical, {}) or {})

    def get_microstructure(self, asset: str, category: str = "") -> Dict[str, Any]:
        canonical = registry.canonical(str(asset or "").strip())
        if not self.supports(canonical, category=category):
            return {}

        snapshot = self.get_latest_snapshot(canonical)
        if not snapshot:
            return {}

        price = _safe_float(snapshot.get("price"), 0.0)
        bid = snapshot.get("bid")
        ask = snapshot.get("ask")
        spread = max(0.0, _safe_float(ask) - _safe_float(bid)) if ask not in (None, "") and bid not in (None, "") else 0.0
        age_seconds = max(0.0, time.time() - _safe_float(snapshot.get("timestamp"), time.time()))

        try:
            from services.live_microstructure_service import (
                estimate_true_depth_metrics,
                get_service as get_live_microstructure_service,
            )

            metrics = get_live_microstructure_service().get_snapshot(
                "ctrader",
                canonical,
                price=price,
                spread=spread,
            )
        except Exception:
            metrics = {}
            estimate_true_depth_metrics = None

        top_bids = [
            [level["bid"], level["bid_size"]]
            for level in list(snapshot.get("levels") or [])
            if level.get("bid") not in (None, "") and level.get("bid_size") not in (None, "")
        ]
        top_asks = [
            [level["ask"], level["ask_size"]]
            for level in list(snapshot.get("levels") or [])
            if level.get("ask") not in (None, "") and level.get("ask_size") not in (None, "")
        ]
        depth_mid_price = round(float(price), 8) if price > 0.0 else 0.0
        depth_spread_bps = round((spread / depth_mid_price) * 10000.0, 4) if depth_mid_price > 0.0 else 0.0

        payload = {
            "source": "cTrader",
            "source_class": "sidecar",
            "delayed": False,
            "realtime": age_seconds <= _DEFAULT_STALE_SECONDS,
            "from_cache": False,
            "as_of_utc": str(snapshot.get("as_of_utc") or _utc_now_iso()),
            "exchange": "ctrader",
            "broker": str(snapshot.get("broker") or "IC Markets"),
            "environment": str(snapshot.get("environment") or self._environment or "demo"),
            "asset": canonical,
            "depth_provider": "IC Markets cTrader",
            "depth_live_age_seconds": round(age_seconds, 3),
            "depth_bid": _safe_float(bid, 0.0) if bid not in (None, "") else None,
            "depth_ask": _safe_float(ask, 0.0) if ask not in (None, "") else None,
            "depth_mid_price": depth_mid_price,
            "depth_spread_bps": depth_spread_bps,
            "bid_vol": _safe_float(snapshot.get("total_bid_volume"), 0.0),
            "ask_vol": _safe_float(snapshot.get("total_ask_volume"), 0.0),
            "orderbook_top_bids": top_bids,
            "orderbook_top_asks": top_asks,
            "visible_bid_levels": int(len(top_bids)),
            "visible_ask_levels": int(len(top_asks)),
            "microstructure_source": "ctrader_live_depth",
            "symbol_name": str(snapshot.get("symbol_name") or ""),
            "symbol_id": str(snapshot.get("symbol_id") or ""),
        }
        fallback_depth = (
            estimate_true_depth_metrics(
                snapshot.get("levels"),
                bid_size=snapshot.get("bid_size"),
                ask_size=snapshot.get("ask_size"),
            )
            if callable(estimate_true_depth_metrics)
            else {}
        )
        payload.update(fallback_depth or {})
        payload.update(metrics or {})
        if (fallback_depth or {}) and float(payload.get("depth_quality", 0.0) or 0.0) <= 0.0:
            payload["depth_quality"] = round(float((fallback_depth or {}).get("depth_quality", 0.0) or 0.0), 4)
            payload["depth_quality_tier"] = str((fallback_depth or {}).get("depth_quality_tier") or "none")
        if (fallback_depth or {}) and float(payload.get("book_imbalance", 0.0) or 0.0) == 0.0:
            payload["book_imbalance"] = round(float((fallback_depth or {}).get("book_imbalance", 0.0) or 0.0), 4)
        payload["depth_available"] = bool((metrics or {}).get("depth_available")) or bool(top_bids or top_asks)
        payload["synthetic_depth_available"] = bool((metrics or {}).get("synthetic_depth_available"))
        payload["depth_levels"] = int((metrics or {}).get("depth_levels") or max(len(top_bids), len(top_asks)))
        payload["microstructure_source"] = "ctrader_live_depth" if payload["depth_available"] else str(
            (metrics or {}).get("microstructure_source") or "ctrader_live"
        )
        return payload

    def status(self) -> Dict[str, Any]:
        self._load_store(force=False)
        with self._lock:
            proc = self._process
            running = bool(proc is not None and proc.poll() is None)
            pid = int(proc.pid) if proc is not None else None
            exit_code = None if proc is None or running else proc.poll()
            assets = sorted(self._latest.keys())
            latest_timestamps = [
                _safe_float(snapshot.get("timestamp"), 0.0)
                for snapshot in self._latest.values()
                if isinstance(snapshot, dict)
            ]
        store_exists = self._store_path.exists()
        try:
            store_mtime = self._store_path.stat().st_mtime if store_exists else 0.0
        except Exception:
            store_mtime = 0.0
        freshest_ts = max((ts for ts in latest_timestamps if ts > 0.0), default=0.0)
        snapshot_age = round(max(0.0, time.time() - freshest_ts), 3) if freshest_ts > 0.0 else None
        profiles = self.list_profiles()
        expected_assets = sorted(self._assets)
        missing_assets = sorted(set(expected_assets) - set(assets))
        stale = bool(expected_assets) and (snapshot_age is None or snapshot_age > _DEFAULT_STALE_SECONDS)
        healthy = bool(self._enabled and running and not stale)
        if not self._enabled:
            state = "disabled"
        elif healthy:
            state = "streaming"
        elif stale:
            state = "stale"
        elif running:
            state = "warming"
        else:
            state = "stopped"
        return {
            "enabled": self._enabled,
            "running": running,
            "pid": pid,
            "exit_code": exit_code,
            "assets": assets,
            "environment": self._environment,
            "store_path": str(self._store_path),
            "store_exists": store_exists,
            "store_age_seconds": round(max(0.0, time.time() - store_mtime), 3) if store_mtime > 0.0 else None,
            "last_snapshot_age_seconds": snapshot_age,
            "profiles": profiles,
            "expected_assets": expected_assets,
            "missing_assets": missing_assets,
            "stale": stale,
            "healthy": healthy,
            "state": state,
            "restart_count": self._restart_count,
            "has_client": bool(self._client_id and self._client_secret),
            "has_token": bool(self._access_token or self._refresh_token or self._token_cache_path.exists()),
            "account_id": self._account_id,
        }

    def ensure_running(self, *, max_snapshot_age: float = _DEFAULT_STALE_SECONDS) -> Dict[str, Any]:
        status = self.status()
        profiles = list(status.get("profiles") or [])
        now = time.time()
        restart_attempted = False
        restart_succeeded = False
        restart_reason = ""

        if not status.get("enabled") or not profiles:
            status.update(
                {
                    "restart_attempted": restart_attempted,
                    "restart_succeeded": restart_succeeded,
                    "restart_reason": restart_reason,
                }
            )
            return status

        # A freshly started sidecar often needs a few seconds before the first
        # persisted snapshot lands. Treat that as warmup, not stale data.
        warming_no_snapshot = bool(
            status.get("running")
            and status.get("last_snapshot_age_seconds") is None
            and self._last_started_at > 0.0
            and (now - self._last_started_at) < max_snapshot_age
        )
        stale = bool(
            status.get("last_snapshot_age_seconds") is None
            or float(status.get("last_snapshot_age_seconds") or 0.0) > max_snapshot_age
        )
        needs_restart = bool(not status.get("running") or (stale and not warming_no_snapshot))
        if needs_restart and (now - self._last_restart_attempt) >= _RESTART_COOLDOWN_SECONDS:
            restart_attempted = True
            restart_reason = "stale_depth" if stale else "process_not_running"
            self._last_restart_attempt = now
            if status.get("running"):
                self.stop()
            restart_succeeded = bool(self.start_background())
            if restart_succeeded:
                self._restart_count += 1
                logger.warning(f"[CTraderDepth] sidecar restarted ({restart_reason})")
            else:
                logger.warning(f"[CTraderDepth] sidecar restart failed ({restart_reason})")
            status = self.status()

        status.update(
            {
                "restart_attempted": restart_attempted,
                "restart_succeeded": restart_succeeded,
                "restart_reason": restart_reason,
            }
        )
        return status


ctrader_live_depth_bridge = CTraderLiveDepthBridge()
