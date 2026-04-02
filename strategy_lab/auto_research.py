from __future__ import annotations

import copy
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from utils.logger import get_logger

logger = get_logger()

AUTO_RESEARCH_RUNTIME_PATH = Path("config/bot_runtime.json")
AUTO_RESEARCH_STATUS_PATH = Path("config/auto_strategy_research_status.json")
AUTO_RESEARCH_SOURCE = "bot_auto_research"
_AUTO_RESEARCH_RUN_LOCK = threading.Lock()
AUTO_RESEARCH_DEFAULT_ASSETS = [
    {"asset": "BTC-USD", "category": "crypto"},
    {"asset": "ETH-USD", "category": "crypto"},
    {"asset": "SOL-USD", "category": "crypto"},
    {"asset": "XRP-USD", "category": "crypto"},
    {"asset": "BNB-USD", "category": "crypto"},
    {"asset": "EUR/USD", "category": "forex"},
    {"asset": "EUR/JPY", "category": "forex"},
    {"asset": "GBP/USD", "category": "forex"},
    {"asset": "GBP/JPY", "category": "forex"},
    {"asset": "USD/JPY", "category": "forex"},
    {"asset": "AUD/USD", "category": "forex"},
    {"asset": "USD/CAD", "category": "forex"},
    {"asset": "XAU/USD", "category": "commodities"},
    {"asset": "XAG/USD", "category": "commodities"},
    {"asset": "US30", "category": "indices"},
    {"asset": "US100", "category": "indices"},
    {"asset": "US500", "category": "indices"},
    {"asset": "UK100", "category": "indices"},
]
AUTO_RESEARCH_DEFAULTS = {
    "enabled": False,
    "run_on_startup": True,
    "startup_delay_seconds": 180,
    "interval_hours": 24.0,
    "screening_profile": "standard",
    "final_profile": "deep",
    "max_parallel_assets": 2,
    "shortlist": 2,
    "assets": AUTO_RESEARCH_DEFAULT_ASSETS,
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _format_utc(value) -> str:
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M UTC")
    return str(value)


def _auto_research_pressure_policy(cpu_percent: float | None, ram_percent: float | None) -> Dict[str, Any]:
    try:
        from config.config import (
            AUTO_RESEARCH_DEFER_ON_RESOURCE_PRESSURE,
            AUTO_RESEARCH_MAX_CPU_PERCENT,
            AUTO_RESEARCH_MAX_RAM_PERCENT,
            AUTO_RESEARCH_PRESSURE_RETRY_SECONDS,
        )
    except Exception:
        AUTO_RESEARCH_DEFER_ON_RESOURCE_PRESSURE = True
        AUTO_RESEARCH_MAX_CPU_PERCENT = 75.0
        AUTO_RESEARCH_MAX_RAM_PERCENT = 82.0
        AUTO_RESEARCH_PRESSURE_RETRY_SECONDS = 300

    cpu = None if cpu_percent is None else float(cpu_percent)
    ram = None if ram_percent is None else float(ram_percent)
    cpu_hot = cpu is not None and cpu >= float(AUTO_RESEARCH_MAX_CPU_PERCENT)
    ram_hot = ram is not None and ram >= float(AUTO_RESEARCH_MAX_RAM_PERCENT)
    defer = bool(AUTO_RESEARCH_DEFER_ON_RESOURCE_PRESSURE) and (cpu_hot or ram_hot)
    return {
        "enabled": bool(AUTO_RESEARCH_DEFER_ON_RESOURCE_PRESSURE),
        "defer": defer,
        "cpu_percent": cpu,
        "ram_percent": ram,
        "cpu_limit": float(AUTO_RESEARCH_MAX_CPU_PERCENT),
        "ram_limit": float(AUTO_RESEARCH_MAX_RAM_PERCENT),
        "retry_seconds": max(60, int(AUTO_RESEARCH_PRESSURE_RETRY_SECONDS or 300)),
    }


def _get_auto_research_pressure_snapshot() -> Dict[str, Any]:
    try:
        import psutil  # type: ignore

        cpu_percent = float(psutil.cpu_percent(interval=0.2))
        ram_percent = float(psutil.virtual_memory().percent)
        return _auto_research_pressure_policy(cpu_percent, ram_percent)
    except Exception:
        return _auto_research_pressure_policy(None, None)


def _default_status_payload() -> Dict[str, Any]:
    return {
        "running": False,
        "last_started_at": "",
        "last_completed_at": "",
        "last_trigger": "",
        "last_error": "",
        "last_summary": {},
        "promoted_names": [],
        "promoted_count": 0,
        "asset_summaries": [],
        "settings": {},
    }


def _read_status_payload(status_path: Path | None = None) -> Dict[str, Any]:
    path = Path(status_path or AUTO_RESEARCH_STATUS_PATH)
    if not path.exists():
        return _default_status_payload()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning(f"[AutoResearch] Invalid status file at {path}: {exc}")
        return _default_status_payload()
    if not isinstance(payload, dict):
        return _default_status_payload()
    base = _default_status_payload()
    base.update(payload)
    return base


def _write_status_payload(payload: Dict[str, Any], status_path: Path | None = None) -> Dict[str, Any]:
    path = Path(status_path or AUTO_RESEARCH_STATUS_PATH)
    safe = _default_status_payload()
    safe.update(copy.deepcopy(payload or {}))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(safe, indent=2, sort_keys=True), encoding="utf-8")
    return safe


def update_auto_research_status(*, status_path: Path | None = None, **updates: Any) -> Dict[str, Any]:
    payload = _read_status_payload(status_path)
    payload.update(copy.deepcopy(updates))
    return _write_status_payload(payload, status_path)


def load_auto_research_status(status_path: Path | None = None) -> Dict[str, Any]:
    payload = _read_status_payload(status_path)
    payload["running"] = bool(payload.get("running")) or _AUTO_RESEARCH_RUN_LOCK.locked()
    payload.setdefault("settings", load_auto_research_settings())
    return payload


def _sort_key(item) -> tuple:
    result = item["result"]
    trades = int(getattr(result, "total_trades", 0) or 0)
    return (
        int(trades > 0),
        float(getattr(result, "sharpe_ratio", 0.0) or 0.0),
        float(getattr(result, "total_pnl", 0.0) or 0.0),
        -float(getattr(result, "max_drawdown", 0.0) or 0.0),
        float(getattr(result, "win_rate", 0.0) or 0.0),
        trades,
    )


def _is_research_acceptable(report: dict | None) -> bool:
    if not report:
        return False
    if bool(report.get("insufficient_data")):
        return False
    return str(report.get("verdict", "")).lower() in {"mixed", "robust"} and float(report.get("overall_score", 0.0) or 0.0) >= 55.0


def _infer_category(asset: str) -> str:
    from config.config import ASSET_CATEGORIES

    target = str(asset or "").strip()
    for category, assets in (ASSET_CATEGORIES or {}).items():
        if target in list(assets or []):
            return str(category)
    return ""


def _normalise_assets(raw_assets: Any) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for item in list(raw_assets or []):
        if isinstance(item, str):
            asset = item.strip()
            category = _infer_category(asset)
        elif isinstance(item, dict):
            asset = str(item.get("asset") or "").strip()
            category = str(item.get("category") or "").strip() or _infer_category(asset)
        else:
            continue
        if not asset or not category:
            continue
        rows.append({"asset": asset, "category": category})
    return rows


def load_auto_research_settings(runtime_path: Path | None = None) -> dict:
    try:
        from config.config import AUTO_RESEARCH_MAX_PARALLEL_ASSETS
    except Exception:
        AUTO_RESEARCH_MAX_PARALLEL_ASSETS = AUTO_RESEARCH_DEFAULTS["max_parallel_assets"]

    path = Path(runtime_path or AUTO_RESEARCH_RUNTIME_PATH)
    settings = copy.deepcopy(AUTO_RESEARCH_DEFAULTS)
    settings["max_parallel_assets"] = max(1, int(AUTO_RESEARCH_MAX_PARALLEL_ASSETS or settings["max_parallel_assets"]))
    payload: dict[str, Any] = {}
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning(f"[AutoResearch] Invalid runtime config at {path}: {exc}")
            payload = {}
    raw = payload.get("auto_strategy_research") if isinstance(payload, dict) else {}
    if isinstance(raw, dict):
        settings["enabled"] = bool(raw.get("enabled", settings["enabled"]))
        settings["run_on_startup"] = bool(raw.get("run_on_startup", settings["run_on_startup"]))
        settings["startup_delay_seconds"] = max(0, int(raw.get("startup_delay_seconds", settings["startup_delay_seconds"]) or 0))
        settings["interval_hours"] = max(1.0, float(raw.get("interval_hours", settings["interval_hours"]) or 1.0))
        settings["screening_profile"] = str(raw.get("screening_profile", settings["screening_profile"]) or settings["screening_profile"]).strip().lower()
        settings["final_profile"] = str(raw.get("final_profile", settings["final_profile"]) or settings["final_profile"]).strip().lower()
        settings["max_parallel_assets"] = max(1, int(raw.get("max_parallel_assets", settings["max_parallel_assets"]) or 1))
        settings["shortlist"] = max(1, int(raw.get("shortlist", settings["shortlist"]) or 1))
        assets = _normalise_assets(raw.get("assets"))
        if assets:
            settings["assets"] = assets
    settings["runtime_path"] = str(path)
    return settings


def _screen_asset(
    row: Dict[str, str],
    *,
    configs: Dict[str, dict],
    screening_profile: str,
    shortlist_size: int,
    cycle_anchor: datetime,
) -> Dict[str, Any]:
    from strategy_lab import resolve_backtest_end_time, resolve_backtest_periods, run_backtest

    asset = row["asset"]
    category = row["category"]
    periods = resolve_backtest_periods(category)
    snapshot_end = resolve_backtest_end_time(category, cycle_anchor)
    candidates: List[Dict[str, Any]] = []

    logger.info(
        f"[AutoResearch] Screening {asset} ({category}) using {periods} bars through "
        f"{_format_utc(snapshot_end)}"
    )

    for name, config in configs.items():
        try:
            result = run_backtest(config, asset, category, periods=periods, end_time=snapshot_end)
            candidates.append({"name": name, "config": config, "result": result})
        except Exception as exc:
            logger.debug(f"[AutoResearch] Backtest {name} on {asset} failed: {exc}")

    ranked = sorted(candidates, key=_sort_key, reverse=True)
    shortlist = [item for item in ranked if int(getattr(item["result"], "total_trades", 0) or 0) > 0][:shortlist_size]
    if not shortlist:
        shortlist = ranked[:shortlist_size]
    if not shortlist:
        return {
            "asset": asset,
            "category": category,
            "periods": periods,
            "snapshot_end": snapshot_end,
            "winner": "",
            "report": None,
            "result": None,
            "config": None,
            "summary": {
                "asset": asset,
                "category": category,
                "winner": "",
                "verdict": "no_candidates",
                "overall_score": 0.0,
                "promoted": False,
            },
        }

    research_rows: List[Dict[str, Any]] = []
    for candidate in shortlist:
        report = _run_research(
            candidate["config"],
            asset,
            category,
            periods=periods,
            end_time=snapshot_end,
            profile=screening_profile,
        )
        research_rows.append({**candidate, "report": report})

    eligible = [candidate for candidate in research_rows if not bool(candidate["report"].get("insufficient_data"))]
    pool = eligible or research_rows
    best = max(
        pool,
        key=lambda item: (
            float(item["report"].get("overall_score", 0.0) or 0.0),
            float(getattr(item["result"], "sharpe_ratio", 0.0) or 0.0),
            float(getattr(item["result"], "total_pnl", 0.0) or 0.0),
        ),
    )
    return {
        "asset": asset,
        "category": category,
        "periods": periods,
        "snapshot_end": snapshot_end,
        "winner": best["name"],
        "report": best["report"],
        "result": best["result"],
        "config": best["config"],
    }


def _run_research(
    config: dict,
    asset: str,
    category: str,
    *,
    periods: int,
    end_time,
    profile: str,
) -> dict:
    from strategy_lab import resolve_research_profile, run_robustness_analysis

    settings = resolve_research_profile(profile)
    return run_robustness_analysis(
        strategy_config=config,
        asset=asset,
        category=category,
        periods=periods,
        end_time=end_time,
        research_profile=str(settings.get("profile", profile)),
        monte_carlo_iterations=int(settings.get("monte_carlo_iterations", 80) or 80),
        max_walk_forward_folds=int(settings.get("max_walk_forward_folds", 3) or 3),
        max_sensitivity_params=int(settings.get("max_sensitivity_params", 3) or 3),
        include_cross_asset_validation=bool(settings.get("include_cross_asset_validation", False)),
        max_cross_asset_peers=int(settings.get("max_cross_asset_peers", 0) or 0),
    )


def _execute_auto_research_cycle(settings: dict | None = None, *, trigger: str = "manual") -> dict:
    from strategy_lab import StrategyBuilder
    from strategy_lab.live_bridge import sync_promoted_strategies

    resolved = copy.deepcopy(settings or load_auto_research_settings())
    screening_profile = str(resolved.get("screening_profile", "standard") or "standard").strip().lower()
    final_profile = str(resolved.get("final_profile", "deep") or "deep").strip().lower()
    max_parallel_assets = max(1, int(resolved.get("max_parallel_assets", 1) or 1))
    shortlist_size = max(1, int(resolved.get("shortlist", 2) or 1))
    assets = _normalise_assets(resolved.get("assets")) or copy.deepcopy(AUTO_RESEARCH_DEFAULT_ASSETS)
    configs = StrategyBuilder.all_configs()
    cycle_started_at = _utc_now()
    logger.info(
        f"[AutoResearch] Cycle started — trigger={trigger} assets={len(assets)} "
        f"screening={screening_profile} final={final_profile} "
        f"parallel={max_parallel_assets} shortlist={shortlist_size}"
    )
    update_auto_research_status(
        running=True,
        last_started_at=cycle_started_at.isoformat(),
        last_trigger=str(trigger or "manual"),
        last_error="",
        settings=resolved,
    )

    try:
        promoted_rows: List[Dict[str, Any]] = []
        asset_summaries: List[Dict[str, Any]] = []
        screened_by_asset: Dict[str, Dict[str, Any]] = {}
        ordered_assets = [row["asset"] for row in assets]

        worker_count = min(max_parallel_assets, max(1, len(assets)))
        if worker_count <= 1:
            for row in assets:
                screened_by_asset[row["asset"]] = _screen_asset(
                    row,
                    configs=configs,
                    screening_profile=screening_profile,
                    shortlist_size=shortlist_size,
                    cycle_anchor=cycle_started_at,
                )
        else:
            with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="AutoResearchScreen") as executor:
                futures = {
                    executor.submit(
                        _screen_asset,
                        row,
                        configs=configs,
                        screening_profile=screening_profile,
                        shortlist_size=shortlist_size,
                        cycle_anchor=cycle_started_at,
                    ): row["asset"]
                    for row in assets
                }
                for future in as_completed(futures):
                    asset = futures[future]
                    screened_by_asset[asset] = future.result()

        for asset in ordered_assets:
            screened = screened_by_asset[asset]
            summary = screened.get("summary")
            if summary is not None:
                asset_summaries.append(summary)
                continue

            final_report = screened["report"]
            if _is_research_acceptable(final_report) and final_profile != screening_profile:
                logger.info(f"[AutoResearch] Deep validation on {screened['winner']} for {asset}")
                final_report = _run_research(
                    screened["config"],
                    asset,
                    screened["category"],
                    periods=int(screened["periods"]),
                    end_time=screened["snapshot_end"],
                    profile=final_profile,
                )

            promoted = _is_research_acceptable(final_report)
            if promoted:
                promoted_rows.append(
                    {
                        "config": screened["config"],
                        "report": final_report,
                        "asset": asset,
                        "category": screened["category"],
                    }
                )
            logger.info(
                f"[AutoResearch] Asset result — {asset} winner={screened['winner']} "
                f"score={float(final_report.get('overall_score', 0.0) or 0.0):.1f} "
                f"verdict={str(final_report.get('verdict', 'unknown') or 'unknown')} "
                f"promoted={'yes' if promoted else 'no'}"
            )

            asset_summaries.append(
                {
                    "asset": asset,
                    "category": screened["category"],
                    "winner": screened["winner"],
                    "verdict": str(final_report.get("verdict", "unknown") or "unknown"),
                    "overall_score": float(final_report.get("overall_score", 0.0) or 0.0),
                    "promoted": promoted,
                }
            )

        synced = sync_promoted_strategies(promoted_rows, source=AUTO_RESEARCH_SOURCE)
        if synced:
            logger.info(
                "[AutoResearch] Promoted strategies — " +
                ", ".join(str(row.get("name") or "unknown") for row in synced)
            )
        else:
            logger.info("[AutoResearch] Promoted strategies — none")
        completed_at = _utc_now()
        summary = {
            "started_at": cycle_started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "trigger": str(trigger or "manual"),
            "assets_scanned": len(assets),
            "promoted_count": len(synced),
            "promoted_names": [row.get("name", "") for row in synced],
            "asset_summaries": asset_summaries,
            "screening_profile": screening_profile,
            "final_profile": final_profile,
            "max_parallel_assets": worker_count,
            "elapsed_seconds": round(max(0.0, (completed_at - cycle_started_at).total_seconds()), 2),
        }
        update_auto_research_status(
            running=False,
            last_completed_at=completed_at.isoformat(),
            last_error="",
            promoted_names=summary["promoted_names"],
            promoted_count=summary["promoted_count"],
            asset_summaries=asset_summaries,
            last_summary=summary,
            settings=resolved,
        )
        logger.info(
            f"[AutoResearch] Cycle complete — promoted={summary['promoted_count']} "
            f"assets_scanned={summary['assets_scanned']} elapsed={summary['elapsed_seconds']:.1f}s"
        )
        return summary
    except Exception as exc:
        update_auto_research_status(running=False, last_error=str(exc or "unknown"), settings=resolved)
        raise


def is_auto_research_running() -> bool:
    return _AUTO_RESEARCH_RUN_LOCK.locked()


def run_auto_research_cycle(settings: dict | None = None, *, trigger: str = "manual") -> dict:
    if not _AUTO_RESEARCH_RUN_LOCK.acquire(blocking=False):
        raise RuntimeError("Auto research cycle already running")
    try:
        return _execute_auto_research_cycle(settings, trigger=trigger)
    finally:
        _AUTO_RESEARCH_RUN_LOCK.release()


def trigger_auto_research_cycle_async(settings: dict | None = None, *, trigger: str = "manual_button") -> Dict[str, Any]:
    resolved = copy.deepcopy(settings or load_auto_research_settings())
    if not _AUTO_RESEARCH_RUN_LOCK.acquire(blocking=False):
        return {
            "started": False,
            "running": True,
            "message": "Auto research cycle already running",
        }

    def _worker() -> None:
        try:
            _execute_auto_research_cycle(resolved, trigger=trigger)
        except Exception as exc:
            logger.warning(f"[AutoResearch] Async cycle failed: {exc}")
        finally:
            _AUTO_RESEARCH_RUN_LOCK.release()

    threading.Thread(
        target=_worker,
        name=f"AutoResearch-{trigger}",
        daemon=True,
    ).start()
    return {
        "started": True,
        "running": True,
        "message": "Auto research cycle started",
        "trigger": str(trigger or "manual_button"),
    }


class AutoResearchScheduler:
    def __init__(self, runtime_path: Path | None = None) -> None:
        self._runtime_path = Path(runtime_path or AUTO_RESEARCH_RUNTIME_PATH)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> bool:
        settings = load_auto_research_settings(self._runtime_path)
        if not bool(settings.get("enabled")):
            logger.info("[AutoResearch] Scheduler disabled in runtime config")
            update_auto_research_status(running=False, settings=settings)
            return False
        if self._thread and self._thread.is_alive():
            return True
        self._thread = threading.Thread(
            target=self._loop,
            name="AutoStrategyResearch",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            f"[AutoResearch] Scheduler thread started — assets={len(_normalise_assets(settings.get('assets')))} "
            f"startup_delay={int(settings.get('startup_delay_seconds', 0) or 0)}s "
            f"interval_hours={float(settings.get('interval_hours', 24.0) or 24.0):.1f} "
            f"parallel={int(settings.get('max_parallel_assets', 1) or 1)} "
            f"screening={settings.get('screening_profile', 'standard')} "
            f"final={settings.get('final_profile', 'deep')}"
        )
        update_auto_research_status(running=False, settings=settings)
        return True

    def stop(self) -> None:
        self._stop_event.set()
        update_auto_research_status(running=False)

    def _sleep(self, seconds: float) -> bool:
        return self._stop_event.wait(max(0.0, float(seconds)))

    def _wait_for_resource_window(self, settings: Dict[str, Any], *, trigger: str) -> bool:
        while not self._stop_event.is_set():
            pressure = _get_auto_research_pressure_snapshot()
            if not bool(pressure.get("defer")):
                return True

            cpu = pressure.get("cpu_percent")
            ram = pressure.get("ram_percent")
            retry_seconds = max(60, int(pressure.get("retry_seconds", 300) or 300))
            cpu_text = "n/a" if cpu is None else f"{float(cpu):.0f}%"
            ram_text = "n/a" if ram is None else f"{float(ram):.0f}%"
            logger.warning(
                f"[AutoResearch] Deferring {trigger} cycle — system under pressure "
                f"(cpu={cpu_text}/{float(pressure.get('cpu_limit', 75.0)):.0f}%, "
                f"ram={ram_text}/{float(pressure.get('ram_limit', 82.0)):.0f}%) "
                f"retry in {retry_seconds}s"
            )
            update_auto_research_status(
                running=False,
                settings=settings,
                last_summary={
                    "trigger": str(trigger or "scheduled"),
                    "deferred": True,
                    "reason": "resource_pressure",
                    "cpu_percent": cpu,
                    "ram_percent": ram,
                    "retry_seconds": retry_seconds,
                    "checked_at": _utc_now_iso(),
                },
            )
            if self._sleep(retry_seconds):
                return False
        return False

    def _loop(self) -> None:
        first = True
        while not self._stop_event.is_set():
            settings = load_auto_research_settings(self._runtime_path)
            if not bool(settings.get("enabled")):
                if self._sleep(300):
                    return
                continue

            wait_seconds = float(settings.get("interval_hours", 24.0) or 24.0) * 3600.0
            if first:
                first = False
                if bool(settings.get("run_on_startup")):
                    delay = float(settings.get("startup_delay_seconds", 0) or 0)
                    if delay > 0 and self._sleep(delay):
                        return
                    if not self._wait_for_resource_window(settings, trigger="startup"):
                        return
                    try:
                        run_auto_research_cycle(settings, trigger="startup")
                    except Exception as exc:
                        if "already running" in str(exc).lower():
                            logger.info("[AutoResearch] Startup cycle skipped — another cycle is already running")
                            continue
                        update_auto_research_status(running=False, last_error=str(exc or "unknown"))
                        logger.warning(f"[AutoResearch] Startup cycle failed: {exc}")
                if self._sleep(wait_seconds):
                    return
                continue

            if not self._wait_for_resource_window(settings, trigger="scheduled"):
                return
            try:
                run_auto_research_cycle(settings, trigger="scheduled")
            except Exception as exc:
                if "already running" in str(exc).lower():
                    logger.info("[AutoResearch] Scheduled cycle skipped — another cycle is already running")
                    continue
                update_auto_research_status(running=False, last_error=str(exc or "unknown"))
                logger.warning(f"[AutoResearch] Scheduled cycle failed: {exc}")
            if self._sleep(wait_seconds):
                return


def start_auto_research_scheduler(runtime_path: Path | None = None) -> AutoResearchScheduler | None:
    scheduler = AutoResearchScheduler(runtime_path)
    if scheduler.start():
        return scheduler
    return None


__all__ = [
    "AUTO_RESEARCH_DEFAULTS",
    "AUTO_RESEARCH_DEFAULT_ASSETS",
    "AUTO_RESEARCH_RUNTIME_PATH",
    "AUTO_RESEARCH_STATUS_PATH",
    "AUTO_RESEARCH_SOURCE",
    "AutoResearchScheduler",
    "is_auto_research_running",
    "load_auto_research_settings",
    "load_auto_research_status",
    "run_auto_research_cycle",
    "start_auto_research_scheduler",
    "trigger_auto_research_cycle_async",
    "update_auto_research_status",
]
