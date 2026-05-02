from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    try:
        for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    except Exception:
        return


def _request_json(
    url: str,
    *,
    token: str = "",
    timeout: float = 8.0,
    method: str = "GET",
    body: Optional[Dict[str, Any]] = None,
) -> Tuple[int, bytes, Optional[Dict[str, Any]], str]:
    headers = {"Accept": "application/json"}
    data = None
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            parsed = None
            try:
                parsed = json.loads(raw.decode("utf-8"))
            except Exception:
                parsed = None
            return int(resp.status), raw, parsed, ""
    except HTTPError as exc:
        raw = exc.read()
        parsed = None
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except Exception:
            parsed = None
        return int(exc.code), raw, parsed, str(exc)
    except URLError as exc:
        return 0, b"", None, str(exc.reason)
    except TimeoutError:
        return 0, b"", None, "timeout"
    except Exception as exc:
        return 0, b"", None, str(exc)


def _request_bytes(url: str, *, timeout: float = 8.0) -> Tuple[int, bytes, str]:
    req = Request(url, headers={"Accept": "text/html"})
    try:
        with urlopen(req, timeout=timeout) as resp:
            return int(resp.status), resp.read(), ""
    except HTTPError as exc:
        return int(exc.code), exc.read(), str(exc)
    except URLError as exc:
        return 0, b"", str(exc.reason)
    except TimeoutError:
        return 0, b"", "timeout"
    except Exception as exc:
        return 0, b"", str(exc)


def _login(base_url: str, timeout: float) -> str:
    api_key = os.getenv("DASHBOARD_API_KEY", "").strip()
    if not api_key:
        status, _, payload, _ = _request_json(f"{base_url}/api/login", timeout=timeout, method="POST", body={})
    else:
        status, _, payload, _ = _request_json(
            f"{base_url}/api/login",
            timeout=timeout,
            method="POST",
            body={"api_key": api_key},
        )
    if status == 200 and isinstance(payload, dict) and payload.get("success") and payload.get("token"):
        return str(payload["token"])
    return ""


def _summarize_payload(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    summary: Dict[str, Any] = {
        "success": payload.get("success"),
        "degraded": payload.get("degraded"),
        "stale": payload.get("stale"),
        "partial": payload.get("partial"),
    }
    reason = payload.get("degraded_reason") or payload.get("stale_reason") or payload.get("error")
    if reason:
        summary["reason"] = str(reason)[:120]
    if isinstance(payload.get("trades"), list):
        summary["trades"] = len(payload.get("trades") or [])
    if isinstance(payload.get("assets"), list):
        summary["assets"] = len(payload.get("assets") or [])
    if isinstance(payload.get("alerts"), list):
        summary["alerts"] = len(payload.get("alerts") or [])
    if isinstance(payload.get("events"), list):
        summary["events"] = len(payload.get("events") or [])
    page = payload.get("page")
    if page:
        summary["page"] = page
    return {k: v for k, v in summary.items() if v is not None}


def _probe_html(base_url: str, paths: Iterable[str], timeout: float) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for path in paths:
        url = f"{base_url}{path}"
        started = time.perf_counter()
        status, raw, error = _request_bytes(url, timeout=timeout)
        elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
        rows.append(
            {
                "kind": "html",
                "path": path,
                "status": status,
                "ms": elapsed_ms,
                "bytes": len(raw),
                "error": error,
            }
        )
    return rows


def _probe_api(base_url: str, paths: Iterable[str], token: str, timeout: float) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for path in paths:
        url = f"{base_url}{path}"
        started = time.perf_counter()
        status, raw, payload, error = _request_json(url, token=token, timeout=timeout)
        elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
        rows.append(
            {
                "kind": "api",
                "path": path,
                "status": status,
                "ms": elapsed_ms,
                "bytes": len(raw),
                "summary": _summarize_payload(payload),
                "error": error,
            }
        )
    return rows


def main() -> int:
    _load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description="Probe dashboard pages and API endpoints from the production host.")
    parser.add_argument("--base-url", default=os.getenv("DASHBOARD_AUDIT_BASE_URL", "http://127.0.0.1:5000"))
    parser.add_argument("--timeout", type=float, default=float(os.getenv("DASHBOARD_AUDIT_TIMEOUT", "8")))
    parser.add_argument("--json", action="store_true", help="Print raw JSON rows only")
    args = parser.parse_args()

    base_url = str(args.base_url).rstrip("/")
    timeout = max(1.0, float(args.timeout))
    token = _login(base_url, timeout)

    pages = [
        "/",
        "/command-center",
        "/sentiment-intelligence",
        "/order-flow",
        "/risk-dashboard",
        "/market-intelligence",
        "/playbook-intel",
        "/system-monitor",
        "/whale-intelligence",
        "/intelligence-alerts",
        "/architecture-lab",
    ]
    overview_pages = [
        "command_center",
        "sentiment_intelligence",
        "order_flow",
        "risk_dashboard",
        "market_intelligence",
        "playbook_intel",
        "system_monitor",
        "whale_intelligence",
        "intelligence_alerts",
        "architecture_lab",
    ]
    api_paths = [
        "/api/status",
        "/api/command-center",
        "/api/system/health",
        "/api/system-monitor/overview",
        "/api/risk/portfolio",
        "/api/trade-history?limit=50",
        "/api/trade-history?limit=200",
        "/api/playbook-intel/overview?days=30",
        "/api/sentiment/dashboard",
        "/api/sentiment/by-asset",
        "/api/whale/summary",
        "/api/market/events",
        "/api/market/heatmap",
        "/api/chart/assets",
        "/api/intelligence-alerts/overview",
        "/api/phase3/imbalance",
        "/api/phase3/walls",
        "/api/phase3/stop-hunts",
        "/api/phase3/live-depth",
    ]
    api_paths.extend(
        f"/api/page-overview?{urlencode({'page': page, 'days': 30})}"
        for page in overview_pages
    )

    rows = _probe_html(base_url, pages, timeout)
    rows.extend(_probe_api(base_url, api_paths, token, timeout))

    if args.json:
        print(json.dumps(rows, indent=2, default=str))
    else:
        print(f"base_url={base_url}")
        print(f"authenticated={'yes' if token else 'no'}")
        print("")
        for row in rows:
            error = f" error={row['error']}" if row.get("error") else ""
            summary = ""
            if row.get("summary"):
                summary = " " + json.dumps(row["summary"], separators=(",", ":"), default=str)
            print(f"{row['kind']:4} {row['status']:>3} {row['ms']:>8.1f}ms {row['bytes']:>7}b {row['path']}{summary}{error}")

    bad = [
        row
        for row in rows
        if int(row.get("status") or 0) >= 500
        or int(row.get("status") or 0) == 0
        or float(row.get("ms") or 0.0) >= timeout * 1000
    ]
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
