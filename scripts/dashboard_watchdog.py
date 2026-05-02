from __future__ import annotations

import argparse
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Iterable


@dataclass
class ProbeResult:
    endpoint: str
    ok: bool
    status: int | None
    elapsed_ms: float
    error: str | None = None


def _normalise_base_url(base_url: str) -> str:
    return str(base_url or "http://127.0.0.1:5000").rstrip("/")


def _probe(base_url: str, endpoint: str, timeout: float) -> ProbeResult:
    endpoint = endpoint if endpoint.startswith("/") else f"/{endpoint}"
    url = f"{_normalise_base_url(base_url)}{endpoint}"
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "forex-dashboard-watchdog/1.0",
            "Cache-Control": "no-cache",
        },
        method="GET",
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            # Read a small prefix so the probe confirms the server is sending a body
            # without turning the watchdog into another heavy dashboard client.
            response.read(1024)
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            status = int(getattr(response, "status", 0) or 0)
            return ProbeResult(endpoint=endpoint, ok=200 <= status < 500, status=status, elapsed_ms=elapsed_ms)
    except urllib.error.HTTPError as exc:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        return ProbeResult(endpoint=endpoint, ok=exc.code < 500, status=exc.code, elapsed_ms=elapsed_ms, error=str(exc))
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        return ProbeResult(endpoint=endpoint, ok=False, status=None, elapsed_ms=elapsed_ms, error=str(exc))


def _run_probes(base_url: str, endpoints: Iterable[str], timeout: float) -> list[ProbeResult]:
    return [_probe(base_url, endpoint, timeout) for endpoint in endpoints]


def _restart_service(service_name: str) -> int:
    return subprocess.call(["systemctl", "restart", service_name])


def main() -> int:
    parser = argparse.ArgumentParser(description="Restart the dashboard service if local HTTP probes hang or fail.")
    parser.add_argument("--base-url", default="http://127.0.0.1:5000")
    parser.add_argument("--timeout", type=float, default=4.0)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--restart-service", default="forex-dashboard")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--endpoint",
        action="append",
        default=None,
        help="Endpoint to probe. Can be repeated.",
    )
    args = parser.parse_args()

    endpoints = args.endpoint or ["/command-center", "/sentiment-intelligence", "/playbook-intel"]
    attempts = max(1, int(args.retries) + 1)
    last_results: list[ProbeResult] = []

    for attempt in range(1, attempts + 1):
        last_results = _run_probes(args.base_url, endpoints, max(0.5, float(args.timeout)))
        failures = [result for result in last_results if not result.ok]
        if not failures:
            for result in last_results:
                print(f"ok endpoint={result.endpoint} status={result.status} elapsed_ms={result.elapsed_ms:.1f}")
            return 0
        if attempt < attempts:
            time.sleep(1.0)

    for result in last_results:
        state = "ok" if result.ok else "fail"
        detail = f" error={result.error}" if result.error else ""
        print(
            f"{state} endpoint={result.endpoint} status={result.status} "
            f"elapsed_ms={result.elapsed_ms:.1f}{detail}",
            file=sys.stderr if not result.ok else sys.stdout,
        )

    if args.dry_run:
        print(f"dry-run: would restart {args.restart_service}", file=sys.stderr)
        return 2

    print(f"restarting {args.restart_service}", file=sys.stderr)
    return _restart_service(args.restart_service)


if __name__ == "__main__":
    raise SystemExit(main())
