from __future__ import annotations

import argparse
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from core.assets import registry
from services.binance_market_bridge import binance_market_bridge
from services.deriv_bridge import deriv_bridge
from services.dukascopy_history_bridge import dukascopy_history_bridge
from services.fmp_history_bridge import fmp_history_bridge
from services.local_candle_store import local_candle_store

DEFAULT_INTERVALS = ("5m", "15m", "30m", "1h", "4h", "1d")
SUPPORTED_INTERVALS = {"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
UTC = timezone.utc


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill the local candle store with the earliest history available from configured providers.",
    )
    parser.add_argument(
        "--assets",
        nargs="*",
        default=[],
        help="Canonical asset ids to backfill. Default: every configured asset with a history provider.",
    )
    parser.add_argument(
        "--intervals",
        nargs="*",
        default=list(DEFAULT_INTERVALS),
        help="Intervals to backfill exactly. Default: 5m 15m 30m 1h 4h 1d",
    )
    parser.add_argument(
        "--include-1m-days",
        type=int,
        default=30,
        help="Also backfill exact 1m candles for the recent N days. Set 0 to skip.",
    )
    parser.add_argument(
        "--start",
        default="",
        help="Optional UTC start date/time. If omitted, the script backfills as far back as each provider allows.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Bars to request per provider call. Keep this <=1000 for Binance compatibility.",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=0.2,
        help="Sleep between provider calls.",
    )
    parser.add_argument(
        "--max-chunks-per-asset",
        type=int,
        default=0,
        help="Optional safety limit per asset/interval. 0 means unlimited.",
    )
    return parser.parse_args()


def _parse_start(value: str) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed = pd.to_datetime(raw, utc=True, errors="coerce")
    if pd.isna(parsed):
        raise SystemExit(f"Invalid --start value: {value}")
    return parsed.to_pydatetime()


def _provider_candidates(asset: str, category: str) -> List[Tuple[str, Any]]:
    candidates: List[Tuple[str, Any]] = []

    def _append(label: str, bridge: Any) -> None:
        if bridge is None:
            return
        try:
            supports = getattr(bridge, "supports", None)
            if callable(supports) and supports(asset, category=category):
                candidates.append((label, bridge))
        except Exception:
            return

    if str(category or "").lower() == "crypto":
        _append("Binance", binance_market_bridge)
        _append("FMP", fmp_history_bridge)
        _append("Deriv", deriv_bridge)
    else:
        _append("Dukascopy", dukascopy_history_bridge)
        _append("FMP", fmp_history_bridge)
        _append("Deriv", deriv_bridge)
    return candidates


def _canonical_assets(requested: Sequence[str]) -> List[Tuple[str, str]]:
    if requested:
        selected: List[Tuple[str, str]] = []
        for raw in requested:
            canonical = registry.canonical(str(raw or "").strip())
            category = registry.category(canonical)
            if canonical and category != "unknown":
                selected.append((canonical, category))
        seen = set()
        result = []
        for item in selected:
            if item[0] in seen:
                continue
            seen.add(item[0])
            result.append(item)
        return result

    rows: List[Tuple[str, str]] = []
    for asset, category in registry.all_assets():
        if _provider_candidates(asset, category):
            rows.append((asset, category))
    return rows


def _validate_intervals(intervals: Iterable[str]) -> List[str]:
    normalized: List[str] = []
    for raw in intervals:
        token = str(raw or "").strip().lower()
        if not token:
            continue
        if token not in SUPPORTED_INTERVALS:
            raise SystemExit(f"Unsupported interval: {raw}")
        if token not in normalized:
            normalized.append(token)
    return normalized


def _store_path() -> Path:
    return Path(getattr(local_candle_store, "_path", Path("data/local_candles.sqlite3")))


def _existing_bounds(asset: str, interval: str) -> Tuple[Optional[datetime], Optional[datetime], int]:
    path = _store_path()
    if not path.exists():
        return None, None, 0
    conn = sqlite3.connect(str(path))
    try:
        row = conn.execute(
            """
            SELECT MIN(timestamp), MAX(timestamp), COUNT(*)
            FROM ohlcv_bars
            WHERE asset = ? AND interval = ?
            """,
            (asset, interval),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None, None, 0
    oldest_raw, newest_raw, count_raw = row
    oldest = datetime.fromtimestamp(int(oldest_raw), tz=UTC) if oldest_raw not in (None, "") else None
    newest = datetime.fromtimestamp(int(newest_raw), tz=UTC) if newest_raw not in (None, "") else None
    return oldest, newest, int(count_raw or 0)


def _end_cursor_for_backfill(asset: str, interval: str) -> datetime:
    oldest, _newest, _count = _existing_bounds(asset, interval)
    if oldest is not None:
        return oldest - timedelta(seconds=1)
    return datetime.now(tz=UTC)


def _fetch_chunk(
    bridge: Any,
    asset: str,
    category: str,
    interval: str,
    periods: int,
    end_time: datetime,
) -> Tuple[Optional[pd.DataFrame], dict]:
    try:
        frame, meta = bridge.get_ohlcv(
            asset,
            interval,
            periods,
            category=category,
            end_time=end_time,
            closed_only=True,
        )
    except TypeError:
        frame, meta = bridge.get_ohlcv(asset, interval, periods, category=category)
    return frame, dict(meta or {})


def _store_frame(asset: str, category: str, interval: str, frame: Optional[pd.DataFrame], meta: dict) -> int:
    if frame is None or frame.empty:
        return 0
    return int(local_candle_store.store_ohlcv(asset, category, interval, frame, meta))


def _backfill_asset_interval(
    asset: str,
    category: str,
    interval: str,
    *,
    start_time: Optional[datetime],
    chunk_size: int,
    pause_seconds: float,
    max_chunks: int,
) -> Tuple[int, str]:
    providers = _provider_candidates(asset, category)
    if not providers:
        return 0, "no-provider"

    end_cursor = _end_cursor_for_backfill(asset, interval)
    total_rows = 0
    attempts = 0
    provider_label = providers[0][0]

    while True:
        if start_time is not None and end_cursor <= start_time:
            break
        if max_chunks and attempts >= max_chunks:
            break

        frame: Optional[pd.DataFrame] = None
        meta: dict = {}
        provider_label = providers[0][0]
        for label, bridge in providers:
            provider_label = label
            frame, meta = _fetch_chunk(
                bridge,
                asset,
                category,
                interval,
                chunk_size,
                end_cursor,
            )
            if frame is not None and not frame.empty:
                break
        if frame is None or frame.empty:
            break

        if start_time is not None:
            frame = frame[frame.index >= pd.Timestamp(start_time)]
            if frame.empty:
                break

        stored = _store_frame(asset, category, interval, frame, meta)
        total_rows += stored
        attempts += 1
        oldest = pd.Timestamp(frame.index.min()).to_pydatetime()
        newest = pd.Timestamp(frame.index.max()).to_pydatetime()
        print(
            f"[{asset} {interval}] {provider_label:<9} stored={stored:<5} "
            f"oldest={oldest.isoformat()} newest={newest.isoformat()}"
        )

        next_end = oldest - timedelta(seconds=1)
        if next_end >= end_cursor:
            break
        end_cursor = next_end
        if pause_seconds > 0:
            time.sleep(pause_seconds)

    return total_rows, provider_label


def main() -> int:
    args = _parse_args()
    if not local_candle_store.enabled():
        raise SystemExit("Local candle store is disabled. Set LOCAL_CANDLE_STORE_ENABLED=true first.")

    intervals = _validate_intervals(args.intervals)
    if args.include_1m_days > 0 and "1m" not in intervals:
        intervals = ["1m", *intervals]

    requested_start = _parse_start(args.start)
    one_minute_start = (
        datetime.now(tz=UTC) - timedelta(days=max(1, int(args.include_1m_days)))
        if int(args.include_1m_days or 0) > 0
        else None
    )
    assets = _canonical_assets(args.assets)
    if not assets:
        raise SystemExit("No supported assets found for backfill.")

    print(f"Using local store: {_store_path()}")
    print(f"Assets: {', '.join(asset for asset, _category in assets)}")
    print(f"Intervals: {', '.join(intervals)}")

    for asset, category in assets:
        print(f"\n=== {asset} ({category}) ===")
        for interval in intervals:
            start_time = one_minute_start if interval == "1m" else requested_start
            stored, provider_label = _backfill_asset_interval(
                asset,
                category,
                interval,
                start_time=start_time,
                chunk_size=max(2, min(int(args.chunk_size or 1000), 1000)),
                pause_seconds=max(0.0, float(args.pause_seconds or 0.0)),
                max_chunks=max(0, int(args.max_chunks_per_asset or 0)),
            )
            oldest, newest, count = _existing_bounds(asset, interval)
            print(
                f"[{asset} {interval}] done via {provider_label} | stored_now={stored} "
                f"| total_rows={count} | oldest={oldest.isoformat() if oldest else '—'} "
                f"| newest={newest.isoformat() if newest else '—'}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
