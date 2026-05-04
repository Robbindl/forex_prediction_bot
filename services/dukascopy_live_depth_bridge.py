from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from config.config import (
    DUKASCOPY_LIVE_DEPTH_ASSETS,
    DUKASCOPY_LIVE_DEPTH_AUTO_BUILD,
    DUKASCOPY_LIVE_DEPTH_CMD,
    DUKASCOPY_LIVE_DEPTH_ENABLED,
    DUKASCOPY_LIVE_DEPTH_JAVA_BIN,
    DUKASCOPY_LIVE_DEPTH_JNLP_URL,
    DUKASCOPY_LIVE_DEPTH_MAX_LEVELS,
    DUKASCOPY_LIVE_DEPTH_MIN_EMIT_MS,
    DUKASCOPY_LIVE_DEPTH_PASSWORD,
    DUKASCOPY_LIVE_DEPTH_PIN,
    DUKASCOPY_LIVE_DEPTH_STORE_PATH,
    DUKASCOPY_LIVE_DEPTH_USERNAME,
)
from core.assets import registry
from services.market_hours_guard import session_market_status
from utils.logger import get_logger

logger = get_logger()

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

import sys

if sys.platform == "linux":
    _SIDEcar_PROJECT_DIR = Path("/opt/forex_prediction_bot/integrations/dukascopy_depth_bridge")
else:
    _SIDEcar_PROJECT_DIR = Path("integrations/dukascopy_depth_bridge")
_SIDECAR_JAR = _SIDEcar_PROJECT_DIR / "target" / "dukascopy-depth-bridge-1.0-shaded.jar"
_STORE_WRITE_MIN_INTERVAL = 0.75
_DEFAULT_STALE_SECONDS = 30.0
_RESTART_COOLDOWN_SECONDS = 45.0

_SUPPORTED_LIVE_SYMBOLS: Dict[str, Dict[str, str]] = {
    "EUR/USD": {"symbol": "EUR/USD", "category": "forex"},
    "EUR/JPY": {"symbol": "EUR/JPY", "category": "forex"},
    "EUR/GBP": {"symbol": "EUR/GBP", "category": "forex"},
    "GBP/JPY": {"symbol": "GBP/JPY", "category": "forex"},
    "GBP/USD": {"symbol": "GBP/USD", "category": "forex"},
    "AUD/USD": {"symbol": "AUD/USD", "category": "forex"},
    "NZD/USD": {"symbol": "NZD/USD", "category": "forex"},
    "USD/JPY": {"symbol": "USD/JPY", "category": "forex"},
    "USD/CAD": {"symbol": "USD/CAD", "category": "forex"},
    "USD/CHF": {"symbol": "USD/CHF", "category": "forex"},
    "XAU/USD": {"symbol": "XAU/USD", "category": "commodities"},
    "XAG/USD": {"symbol": "XAG/USD", "category": "commodities"},
    "WTI": {"symbol": "LIGHT.CMD/USD", "category": "commodities"},
    "US30": {"symbol": "USA30.IDX/USD", "category": "indices"},
    "US100": {"symbol": "USATECH.IDX/USD", "category": "indices"},
    "US500": {"symbol": "USA500.IDX/USD", "category": "indices"},
    "UK100": {"symbol": "GBR.IDX/GBP", "category": "indices"},
    "GER40": {"symbol": "DEU.IDX/EUR", "category": "indices"},
    "AUS200": {"symbol": "AUS.IDX/AUD", "category": "indices"},
    "JPN225": {"symbol": "JPN.IDX/JPY", "category": "indices"},
}


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value if value not in (None, "") else default)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value if value not in (None, "") else default)
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


def _market_session_state(assets: List[str]) -> Dict[str, Any]:
    open_assets: List[str] = []
    closed_assets: List[Dict[str, str]] = []
    for asset in assets:
        spec = _SUPPORTED_LIVE_SYMBOLS.get(asset, {})
        try:
            is_open, reason = session_market_status(asset, str(spec.get("category") or ""))
        except Exception as exc:
            is_open, reason = True, f"market status unavailable: {exc}"
        if is_open:
            open_assets.append(asset)
        else:
            closed_assets.append({"asset": asset, "reason": str(reason or "closed")})
    return {
        "open_assets": open_assets,
        "closed_assets": closed_assets,
        "market_quiet": bool(assets) and not open_assets,
    }


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


class DukascopyLiveDepthBridge:
    def __init__(
        self,
        *,
        enabled: Optional[bool] = None,
        store_path: Path | str = DUKASCOPY_LIVE_DEPTH_STORE_PATH,
        assets: Optional[tuple[str, ...]] = None,
        jnlp_url: str = DUKASCOPY_LIVE_DEPTH_JNLP_URL,
        username: str = DUKASCOPY_LIVE_DEPTH_USERNAME,
        password: str = DUKASCOPY_LIVE_DEPTH_PASSWORD,
        pin: str = DUKASCOPY_LIVE_DEPTH_PIN,
        command_text: str = DUKASCOPY_LIVE_DEPTH_CMD,
        java_bin: str = DUKASCOPY_LIVE_DEPTH_JAVA_BIN,
        auto_build: Optional[bool] = None,
        min_emit_ms: int = DUKASCOPY_LIVE_DEPTH_MIN_EMIT_MS or 150,
        max_levels: int = DUKASCOPY_LIVE_DEPTH_MAX_LEVELS or 20,
    ) -> None:
        self._enabled = bool(DUKASCOPY_LIVE_DEPTH_ENABLED if enabled is None else enabled)
        self._store_path = _resolve_runtime_path(store_path)
        self._store_path.parent.mkdir(parents=True, exist_ok=True)
        self._assets = tuple(assets or _normalize_asset_list(DUKASCOPY_LIVE_DEPTH_ASSETS))
        self._jnlp_url = str(jnlp_url or DUKASCOPY_LIVE_DEPTH_JNLP_URL).strip()
        self._username = str(username or "").strip()
        self._password = str(password or "").strip()
        self._pin = str(pin or "").strip()
        self._command_text = str(command_text or "").strip()
        self._java_bin = str(java_bin or "").strip()
        self._auto_build = bool(DUKASCOPY_LIVE_DEPTH_AUTO_BUILD if auto_build is None else auto_build)
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
        self._restart_count = 0
        self._load_store(force=True)

    def list_profiles(self) -> list[str]:
        if not self._enabled:
            return []
        if not self._assets:
            return []
        if self._store_path.exists() or self._credentials_ready(log_warning=False) or self._command_text:
            return ["dukascopy_live_depth"]
        return []

    def supports(self, asset: str, category: str = "") -> bool:
        canonical = registry.canonical(str(asset or "").strip())
        if canonical not in _SUPPORTED_LIVE_SYMBOLS:
            return False
        if category and str(category).strip().lower() == "crypto":
            return False
        return True

    def resolve_symbol_info(self, asset: str, category: str = "") -> Optional[Dict[str, Any]]:
        canonical = registry.canonical(str(asset or "").strip())
        spec = _SUPPORTED_LIVE_SYMBOLS.get(canonical)
        if not spec:
            return None
        return {
            "symbol": str(spec["symbol"]),
            "display_name": canonical,
            "market": str(category or spec["category"]),
            "exchange": "dukascopy",
        }

    def _credentials_ready(self, *, log_warning: bool = True) -> bool:
        ready = bool(self._username and self._password and self._jnlp_url)
        if not ready and log_warning:
            logger.warning("[DukascopyDepth] credentials missing — set DUKASCOPY_LIVE_DEPTH_USERNAME/PASSWORD/JNLP_URL")
        return ready

    def _sidecar_env(self) -> Dict[str, str]:
        env = os.environ.copy()
        env["DUKASCOPY_BRIDGE_JNLP_URL"] = self._jnlp_url
        env["DUKASCOPY_BRIDGE_USERNAME"] = self._username
        env["DUKASCOPY_BRIDGE_PASSWORD"] = self._password
        env["DUKASCOPY_BRIDGE_PIN"] = self._pin
        env["DUKASCOPY_BRIDGE_ASSET_MAP"] = ";".join(
            f"{asset}={_SUPPORTED_LIVE_SYMBOLS[asset]['symbol']}" for asset in self._assets
        )
        env["DUKASCOPY_BRIDGE_MIN_EMIT_MS"] = str(self._min_emit_ms)
        env["DUKASCOPY_BRIDGE_MAX_LEVELS"] = str(self._max_levels)
        return env

    def _ensure_sidecar_built(self) -> Optional[Path]:
        if _SIDECAR_JAR.exists():
            return _SIDECAR_JAR.resolve()
        if not self._auto_build:
            logger.warning(f"[DukascopyDepth] sidecar jar missing at {_SIDECAR_JAR}")
            return None

        mvn = shutil.which("mvn.cmd" if os.name == "nt" else "mvn") or shutil.which("mvn")
        if not mvn:
            logger.warning("[DukascopyDepth] Maven not found — cannot build Java sidecar")
            return None

        try:
            logger.info("[DukascopyDepth] building Java sidecar with Maven...")
            proc = subprocess.run(
                [mvn, "-q", "-DskipTests", "package"],
                cwd=str(_SIDEcar_PROJECT_DIR),
                capture_output=True,
                text=True,
                timeout=300,
                check=False,
            )
        except Exception as exc:
            logger.warning(f"[DukascopyDepth] sidecar build failed: {exc}")
            return None

        if proc.returncode != 0:
            stderr = str(proc.stderr or "").strip()
            logger.warning(f"[DukascopyDepth] sidecar build failed ({proc.returncode}): {_clip_text(stderr, 400)}")
            return None
        if not _SIDECAR_JAR.exists():
            logger.warning(f"[DukascopyDepth] sidecar build completed but jar is missing: {_SIDECAR_JAR}")
            return None
        return _SIDECAR_JAR.resolve()

    def _default_command(self) -> list[str]:
        jar = self._ensure_sidecar_built()
        if jar is None:
            return []
        java_bin = self._java_bin or shutil.which("java") or "java"
        return [java_bin, "-jar", str(jar)]

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
            proc = subprocess.Popen(
                command,
                cwd=str(_SIDEcar_PROJECT_DIR if not self._command_text else Path.cwd()),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                env=self._sidecar_env(),
            )
        except Exception as exc:
            logger.warning(f"[DukascopyDepth] sidecar start failed: {exc}")
            return False

        with self._lock:
            self._process = proc
            self._stdout_thread = threading.Thread(target=self._stdout_loop, daemon=True, name="DukascopyDepthStdout")
            self._stderr_thread = threading.Thread(target=self._stderr_loop, daemon=True, name="DukascopyDepthStderr")
            self._stdout_thread.start()
            self._stderr_thread.start()
        logger.info(f"[DukascopyDepth] sidecar started (PID {proc.pid}) for {len(self._assets)} assets")
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
                logger.debug(f"[DukascopyDepth] non-JSON stdout: {_clip_text(line, 200)}")
                continue
            if isinstance(payload, dict):
                self.ingest_snapshot(payload)

    def _stderr_loop(self) -> None:
        proc = self._process
        if proc is None or proc.stderr is None:
            return
        for raw_line in proc.stderr:
            line = str(raw_line or "").strip()
            if line:
                logger.info(f"[DukascopyDepth] {line}")

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
            logger.debug(f"[DukascopyDepth] store read failed: {exc}")
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
        payload = {
            "updated_at": _utc_now_iso(),
            "assets": self._latest,
        }
        payload_text = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
        tmp = self._store_path.with_name(
            f"{self._store_path.name}.{os.getpid()}.{threading.get_ident()}.{time.time_ns()}.tmp"
        )
        wrote = False
        last_error: Optional[BaseException] = None
        try:
            tmp.parent.mkdir(parents=True, exist_ok=True)
            for attempt in range(3):
                try:
                    tmp.write_text(payload_text, encoding="utf-8")
                    tmp.replace(self._store_path)
                    wrote = True
                    break
                except OSError as exc:
                    last_error = exc
                    time.sleep(0.05 * (attempt + 1))
            if not wrote:
                try:
                    # Windows can deny atomic replace while another local reader has the file open.
                    self._store_path.write_text(payload_text, encoding="utf-8")
                    wrote = True
                except Exception as exc:
                    last_error = exc
            if not wrote:
                logger.warning(f"[DukascopyDepth] store persist failed: {last_error}")
                return
            self._last_persist = now
            try:
                self._last_store_mtime = self._store_path.stat().st_mtime
            except Exception:
                pass
        except Exception as exc:
            logger.warning(f"[DukascopyDepth] store persist failed: {exc}")
        finally:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass

    def ingest_snapshot(self, payload: Dict[str, Any]) -> None:
        canonical = registry.canonical(str(payload.get("asset") or "").strip())
        if canonical not in _SUPPORTED_LIVE_SYMBOLS:
            canonical = self._canonical_from_symbol(str(payload.get("dukascopy_symbol") or "").strip())
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
            "dukascopy_symbol": str(payload.get("dukascopy_symbol") or spec["symbol"]),
            "instrument_name": str(payload.get("instrument_name") or ""),
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
            "environment": str(payload.get("environment") or "demo"),
            "realtime": True,
            "source": "Dukascopy",
            "source_class": "sidecar",
            "exchange": "dukascopy",
        }

        try:
            from services.live_microstructure_service import get_service as get_live_microstructure_service

            get_live_microstructure_service().record_quote(
                "dukascopy",
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
            _monitor.ping_source("dukascopy_live_depth")
        except Exception:
            pass

        with self._lock:
            self._latest[canonical] = event
            self._persist_store_locked()

    @staticmethod
    def _canonical_from_symbol(symbol: str) -> str:
        target = str(symbol or "").strip().upper()
        for canonical, spec in _SUPPORTED_LIVE_SYMBOLS.items():
            if str(spec["symbol"]).strip().upper() == target:
                return canonical
        return ""

    def get_latest_snapshot(self, asset: str) -> Dict[str, Any]:
        canonical = registry.canonical(str(asset or "").strip())
        self._load_store(force=False)
        with self._lock:
            snapshot = dict(self._latest.get(canonical, {}) or {})
        return snapshot

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
                "dukascopy",
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
            "source": "Dukascopy",
            "source_class": "sidecar",
            "delayed": False,
            "realtime": age_seconds <= _DEFAULT_STALE_SECONDS,
            "from_cache": False,
            "as_of_utc": str(snapshot.get("as_of_utc") or _utc_now_iso()),
            "exchange": "dukascopy",
            "dukascopy_symbol": str(snapshot.get("dukascopy_symbol") or _SUPPORTED_LIVE_SYMBOLS[canonical]["symbol"]),
            "environment": str(snapshot.get("environment") or "demo"),
            "asset": canonical,
            "depth_provider": "Dukascopy",
            "depth_provider_class": "broker_l2",
            "depth_transport_class": "sidecar",
            "depth_environment": str(snapshot.get("environment") or "demo"),
            "depth_provider_trust_score": 0.78,
            "depth_update_mode": "stream_snapshot",
            "dom_stream_snapshot_ready": True,
            "dom_event_backed": False,
            "dom_ladder_ready": False,
            "dom_source_fidelity": "stream_snapshot",
            "dom_authority_tier": "snapshot_depth",
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
            "microstructure_source": "dukascopy_live_depth",
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
        payload["depth_feed_class"] = "broker_l2" if payload["depth_available"] else "quote_only"
        payload["depth_normalization_scope"] = f"{canonical}:dukascopy:{payload['depth_feed_class']}"
        payload["depth_max_expected_levels"] = 10
        payload["microstructure_source"] = "dukascopy_live_depth" if payload["depth_available"] else str(
            (metrics or {}).get("microstructure_source") or "dukascopy_live"
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
        market_state = _market_session_state(expected_assets)
        market_quiet = bool(market_state.get("market_quiet"))
        quiet_stale = bool(expected_assets) and market_quiet and (
            snapshot_age is None or snapshot_age > _DEFAULT_STALE_SECONDS
        )
        stale = bool(expected_assets) and not market_quiet and (
            snapshot_age is None or snapshot_age > _DEFAULT_STALE_SECONDS
        )
        fresh_store_snapshot = bool(
            assets and snapshot_age is not None and snapshot_age <= _DEFAULT_STALE_SECONDS
        )
        running_elsewhere = bool(self._enabled and not running and fresh_store_snapshot and not stale)
        healthy = bool(self._enabled and (running or market_quiet or running_elsewhere) and not stale)
        if not self._enabled:
            state = "disabled"
        elif healthy:
            if market_quiet:
                state = "market_closed"
            elif running_elsewhere:
                state = "bot-owned"
            else:
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
            "running_elsewhere": running_elsewhere,
            "process_owner": "external" if running_elsewhere else "local",
            "pid": pid,
            "exit_code": exit_code,
            "assets": assets,
            "store_path": str(self._store_path),
            "store_exists": store_exists,
            "store_age_seconds": round(max(0.0, time.time() - store_mtime), 3) if store_mtime > 0.0 else None,
            "last_snapshot_age_seconds": snapshot_age,
            "profiles": profiles,
            "expected_assets": expected_assets,
            "missing_assets": missing_assets,
            "market_quiet": market_quiet,
            "market_open_assets": list(market_state.get("open_assets") or []),
            "market_closed_assets": list(market_state.get("closed_assets") or []),
            "quiet_stale": quiet_stale,
            "stale": stale,
            "healthy": healthy,
            "state": state,
            "restart_count": self._restart_count,
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

        stale = bool(
            status.get("last_snapshot_age_seconds") is None
            or float(status.get("last_snapshot_age_seconds") or 0.0) > max_snapshot_age
        )
        market_quiet = bool(status.get("market_quiet"))
        running_elsewhere = bool(status.get("running_elsewhere"))
        needs_restart = bool(
            (not status.get("running") and not running_elsewhere and not market_quiet)
            or (stale and not market_quiet)
        )
        if market_quiet and not restart_reason:
            restart_reason = "market_closed"
        if needs_restart and (now - self._last_restart_attempt) >= _RESTART_COOLDOWN_SECONDS:
            restart_attempted = True
            restart_reason = "stale_depth" if stale else "process_not_running"
            self._last_restart_attempt = now
            if status.get("running"):
                self.stop()
            restart_succeeded = bool(self.start_background())
            if restart_succeeded:
                self._restart_count += 1
                logger.warning(f"[DukascopyDepth] sidecar restarted ({restart_reason})")
            else:
                logger.warning(f"[DukascopyDepth] sidecar restart failed ({restart_reason})")
            status = self.status()

        status.update(
            {
                "restart_attempted": restart_attempted,
                "restart_succeeded": restart_succeeded,
                "restart_reason": restart_reason,
            }
        )
        return status


def _clip_text(text: str, limit: int = 300) -> str:
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 1)].rstrip() + "…"


dukascopy_live_depth_bridge = DukascopyLiveDepthBridge()
