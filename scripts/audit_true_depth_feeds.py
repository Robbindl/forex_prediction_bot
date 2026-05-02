from __future__ import annotations

import argparse
import json
import statistics
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


DEFAULT_ASSETS = (
    "EUR/USD",
    "EUR/JPY",
    "EUR/GBP",
    "GBP/JPY",
    "GBP/USD",
    "AUD/USD",
    "NZD/USD",
    "USD/JPY",
    "USD/CAD",
    "USD/CHF",
    "US30",
    "US100",
    "US500",
    "UK100",
    "GER40",
    "AUS200",
    "JPN225",
)


@dataclass
class DepthSample:
    timestamp: float
    bid: float
    ask: float
    bid_levels: int
    ask_levels: int
    total_bid: float
    total_ask: float
    top_bid_size: float
    top_ask_size: float
    spread_bps: float


@dataclass
class AssetAudit:
    provider: str
    asset: str
    symbol: str = ""
    samples: list[DepthSample] = field(default_factory=list)
    latest_age_seconds: float | None = None

    def add(self, payload: dict[str, Any], *, now: float) -> None:
        timestamp = _to_float(payload.get("timestamp"), 0.0)
        levels = payload.get("levels")
        if not isinstance(levels, list):
            levels = []
        bid_levels = [
            item for item in levels
            if isinstance(item, dict)
            and _to_float(item.get("bid"), 0.0) > 0.0
            and _to_float(item.get("bid_size"), 0.0) > 0.0
        ]
        ask_levels = [
            item for item in levels
            if isinstance(item, dict)
            and _to_float(item.get("ask"), 0.0) > 0.0
            and _to_float(item.get("ask_size"), 0.0) > 0.0
        ]
        bid = _to_float(payload.get("bid"), 0.0)
        ask = _to_float(payload.get("ask"), 0.0)
        price = _to_float(payload.get("price"), 0.0)
        if price <= 0.0 and bid > 0.0 and ask > 0.0:
            price = (bid + ask) / 2.0
        spread_bps = ((ask - bid) / price * 10000.0) if price > 0.0 and ask >= bid > 0.0 else 0.0
        total_bid = _to_float(payload.get("total_bid_volume"), sum(_to_float(item.get("bid_size"), 0.0) for item in bid_levels))
        total_ask = _to_float(payload.get("total_ask_volume"), sum(_to_float(item.get("ask_size"), 0.0) for item in ask_levels))
        top_bid_size = _to_float(bid_levels[0].get("bid_size"), 0.0) if bid_levels else _to_float(payload.get("bid_size"), 0.0)
        top_ask_size = _to_float(ask_levels[0].get("ask_size"), 0.0) if ask_levels else _to_float(payload.get("ask_size"), 0.0)
        self.latest_age_seconds = max(0.0, now - timestamp) if timestamp > 0.0 else None
        self.symbol = self.symbol or str(
            payload.get("symbol_name")
            or payload.get("dukascopy_symbol")
            or payload.get("instrument_name")
            or payload.get("symbol_id")
            or ""
        )
        self.samples.append(
            DepthSample(
                timestamp=timestamp,
                bid=bid,
                ask=ask,
                bid_levels=len(bid_levels),
                ask_levels=len(ask_levels),
                total_bid=total_bid,
                total_ask=total_ask,
                top_bid_size=top_bid_size,
                top_ask_size=top_ask_size,
                spread_bps=spread_bps,
            )
        )

    def verdict(self, *, stale_after_seconds: float) -> tuple[str, str]:
        if not self.samples:
            return "missing", "no snapshots observed"
        latest = self.samples[-1]
        if self.latest_age_seconds is None or self.latest_age_seconds > stale_after_seconds:
            age = "unknown" if self.latest_age_seconds is None else f"{self.latest_age_seconds:.1f}s"
            return "stale", f"latest snapshot age={age}"

        median_bid_levels = _median_int(sample.bid_levels for sample in self.samples)
        median_ask_levels = _median_int(sample.ask_levels for sample in self.samples)
        min_side_levels = min(median_bid_levels, median_ask_levels)
        has_sizes = latest.total_bid > 0.0 and latest.total_ask > 0.0
        top_only = max(latest.bid_levels, latest.ask_levels) <= 1
        no_spread = latest.bid <= 0.0 or latest.ask <= 0.0 or latest.ask <= latest.bid
        volume_changes = _unique_count((round(sample.total_bid, 4), round(sample.total_ask, 4)) for sample in self.samples)
        price_changes = _unique_count((round(sample.bid, 8), round(sample.ask, 8)) for sample in self.samples)

        if no_spread:
            return "thin_or_top_book", "missing or crossed bid/ask"
        if not has_sizes:
            return "thin_or_top_book", "levels present but sizes are zero/missing"
        if top_only:
            return "thin_or_top_book", "only one usable level per side"
        if min_side_levels >= 5 and volume_changes >= 2:
            return "strong_true_depth", f"{median_bid_levels}x{median_ask_levels} median levels with changing depth"
        if min_side_levels >= 3:
            reason = f"{median_bid_levels}x{median_ask_levels} median levels"
            if price_changes < 2 and len(self.samples) >= 4:
                reason += "; quiet/sticky prices during sample"
            return "usable_true_depth", reason
        return "thin_or_top_book", f"only {median_bid_levels}x{median_ask_levels} median usable levels"

    def row(self, *, stale_after_seconds: float) -> dict[str, Any]:
        verdict, reason = self.verdict(stale_after_seconds=stale_after_seconds)
        latest = self.samples[-1] if self.samples else None
        return {
            "provider": self.provider,
            "asset": self.asset,
            "symbol": self.symbol,
            "verdict": verdict,
            "reason": reason,
            "samples": len(self.samples),
            "age_s": None if self.latest_age_seconds is None else round(self.latest_age_seconds, 2),
            "bid_levels": latest.bid_levels if latest else 0,
            "ask_levels": latest.ask_levels if latest else 0,
            "total_bid": round(latest.total_bid, 4) if latest else 0.0,
            "total_ask": round(latest.total_ask, 4) if latest else 0.0,
            "spread_bps": round(latest.spread_bps, 4) if latest else 0.0,
        }


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value if value not in (None, "") else default)
    except Exception:
        return default


def _median_int(values: Iterable[int]) -> int:
    data = [int(value) for value in values]
    if not data:
        return 0
    return int(round(statistics.median(data)))


def _unique_count(values: Iterable[Any]) -> int:
    return len({value for value in values})


def _load_assets(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    assets = payload.get("assets") if isinstance(payload, dict) else None
    if isinstance(assets, dict):
        return {str(asset): data for asset, data in assets.items() if isinstance(data, dict)}
    if isinstance(payload, dict) and payload.get("asset"):
        return {str(payload.get("asset")): payload}
    return {}


def _parse_assets(raw: str) -> list[str]:
    if not raw.strip():
        return list(DEFAULT_ASSETS)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _print_table(rows: list[dict[str, Any]]) -> None:
    columns = ("provider", "asset", "verdict", "samples", "age_s", "bid_levels", "ask_levels", "spread_bps", "reason")
    widths = {column: len(column) for column in columns}
    for row in rows:
        for column in columns:
            widths[column] = max(widths[column], len(str(row.get(column, ""))))
    header = "  ".join(column.ljust(widths[column]) for column in columns)
    print(header)
    print("  ".join("-" * widths[column] for column in columns))
    for row in rows:
        print("  ".join(str(row.get(column, "")).ljust(widths[column]) for column in columns))


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit cTrader and Dukascopy live depth stores for true depth quality.")
    parser.add_argument("--seconds", type=float, default=60.0, help="How long to watch the store files.")
    parser.add_argument("--interval", type=float, default=1.0, help="Sampling interval in seconds.")
    parser.add_argument("--stale-after", type=float, default=30.0, help="Snapshot age threshold for stale verdict.")
    parser.add_argument("--assets", default="", help="Comma-separated asset list. Defaults to FX and index assets.")
    parser.add_argument("--ctrader-store", default="data/ctrader_live_depth.json", help="cTrader depth store path.")
    parser.add_argument("--dukascopy-store", default="data/dukascopy_live_depth.json", help="Dukascopy depth store path.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of a table.")
    args = parser.parse_args()

    assets = _parse_assets(args.assets)
    stores = {
        "ctrader": Path(args.ctrader_store),
        "dukascopy": Path(args.dukascopy_store),
    }
    audits = {
        (provider, asset): AssetAudit(provider=provider, asset=asset)
        for provider in stores
        for asset in assets
    }

    deadline = time.time() + max(0.0, args.seconds)
    while True:
        now = time.time()
        for provider, path in stores.items():
            snapshots = _load_assets(path)
            for asset in assets:
                payload = snapshots.get(asset)
                if payload:
                    audits[(provider, asset)].add(payload, now=now)
        if now >= deadline:
            break
        time.sleep(max(0.1, args.interval))

    rows = [audit.row(stale_after_seconds=args.stale_after) for audit in audits.values()]
    rows.sort(key=lambda row: (str(row["asset"]), str(row["provider"])))
    if args.json:
        print(json.dumps({"generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "rows": rows}, indent=2))
    else:
        _print_table(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
