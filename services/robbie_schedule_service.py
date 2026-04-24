from __future__ import annotations

import json
import threading
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from utils.display_time import DISPLAY_TIMEZONE, format_display_datetime, now_in_display_timezone
from utils.logger import get_logger

logger = get_logger()

_STORE_PATH = Path("data/robbie_schedules.json")
_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_utc_iso(value: datetime) -> str:
    dt = value if isinstance(value, datetime) else _utc_now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=DISPLAY_TIMEZONE)
    return dt.astimezone(timezone.utc).isoformat()


def _parse_utc(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _next_daily_run(*, hour: int, minute: int, now_display: Optional[datetime] = None) -> datetime:
    current = now_display or now_in_display_timezone()
    candidate = current.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= current:
        candidate += timedelta(days=1)
    return candidate.astimezone(timezone.utc)


def _next_weekly_run(
    *,
    weekday: int,
    hour: int,
    minute: int,
    now_display: Optional[datetime] = None,
) -> datetime:
    current = now_display or now_in_display_timezone()
    days_ahead = (int(weekday) - current.weekday()) % 7
    candidate = current.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=days_ahead)
    if candidate <= current:
        candidate += timedelta(days=7)
    return candidate.astimezone(timezone.utc)


class RobbieScheduleService:
    def __init__(self, *, path: Optional[Path] = None) -> None:
        self._path = Path(path or _STORE_PATH)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._items: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        with self._lock:
            try:
                raw = self._path.read_text(encoding="utf-8")
                payload = json.loads(raw or "{}")
            except FileNotFoundError:
                payload = {}
            except Exception as exc:
                logger.warning(f"[RobbieSchedule] Could not read schedule store: {exc}")
                payload = {}
            items = payload.get("items") if isinstance(payload, dict) else {}
            if not isinstance(items, dict):
                items = {}
            self._items = {
                str(schedule_id): dict(item)
                for schedule_id, item in items.items()
                if isinstance(item, dict)
            }

    def _persist(self) -> None:
        with self._lock:
            payload = {"items": self._items}
            self._path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")

    def status(self) -> Dict[str, Any]:
        with self._lock:
            rows = list(self._items.values())
        upcoming = sorted(
            (_parse_utc(item.get("next_run_at")) for item in rows),
            key=lambda dt: dt or datetime.max.replace(tzinfo=timezone.utc),
        )
        next_run = next((dt for dt in upcoming if dt is not None), None)
        counts = Counter(str(item.get("kind") or "unknown") for item in rows)
        return {
            "enabled": True,
            "count": len(rows),
            "next_run_at": _to_utc_iso(next_run) if next_run else "",
            "next_run_display": format_display_datetime(next_run, "%Y-%m-%d %H:%M", default=""),
            "kinds": dict(counts),
        }

    def list_chat_schedules(self, chat_id: str) -> List[Dict[str, Any]]:
        key = str(chat_id or "").strip()
        with self._lock:
            rows = [dict(item) for item in self._items.values() if str(item.get("chat_id") or "") == key]
        return sorted(rows, key=lambda item: str(item.get("next_run_at") or ""))

    def delete_schedule(self, schedule_id: str, *, chat_id: str = "") -> bool:
        target_id = str(schedule_id or "").strip()
        if not target_id:
            return False
        with self._lock:
            existing = self._items.get(target_id)
            if not isinstance(existing, dict):
                return False
            if chat_id and str(existing.get("chat_id") or "") != str(chat_id):
                return False
            self._items.pop(target_id, None)
            self._persist()
        return True

    def schedule_reminder(
        self,
        *,
        chat_id: str,
        message: str,
        run_at: datetime,
        title: str = "Reminder",
    ) -> Dict[str, Any]:
        return self._create_item(
            chat_id=chat_id,
            kind="reminder",
            title=title,
            message=message,
            next_run_at=run_at,
            recurrence={"kind": "once"},
        )

    def schedule_ai_update(
        self,
        *,
        chat_id: str,
        prompt: str,
        title: str,
        next_run_at: datetime,
        recurrence: Dict[str, Any],
    ) -> Dict[str, Any]:
        return self._create_item(
            chat_id=chat_id,
            kind="ai_update",
            title=title,
            prompt=prompt,
            next_run_at=next_run_at,
            recurrence=recurrence,
        )

    def schedule_weekly_report(
        self,
        *,
        chat_id: str,
        weekday: int,
        hour: int,
        minute: int,
        title: str = "Weekly Report",
    ) -> Dict[str, Any]:
        next_run = _next_weekly_run(weekday=weekday, hour=hour, minute=minute)
        return self._create_item(
            chat_id=chat_id,
            kind="weekly_report",
            title=title,
            next_run_at=next_run,
            recurrence={"kind": "weekly", "weekday": int(weekday), "hour": int(hour), "minute": int(minute)},
        )

    def claim_due(self, *, now_utc: Optional[datetime] = None) -> List[Dict[str, Any]]:
        now = now_utc or _utc_now()
        due: List[Dict[str, Any]] = []
        changed = False
        with self._lock:
            current = dict(self._items)
            for schedule_id, item in list(current.items()):
                next_run = _parse_utc(item.get("next_run_at"))
                if next_run is None or next_run > now:
                    continue
                due.append(dict(item))
                changed = True
                advanced = self._advance_item(item)
                if advanced is None:
                    current.pop(schedule_id, None)
                else:
                    current[schedule_id] = advanced
            if changed:
                self._items = current
                self._persist()
        due.sort(key=lambda item: str(item.get("next_run_at") or ""))
        return due

    def _create_item(
        self,
        *,
        chat_id: str,
        kind: str,
        title: str,
        next_run_at: datetime,
        recurrence: Dict[str, Any],
        message: str = "",
        prompt: str = "",
    ) -> Dict[str, Any]:
        chat_key = str(chat_id or "").strip()
        if not chat_key:
            raise ValueError("chat_id is required")
        schedule_id = uuid.uuid4().hex[:12]
        row = {
            "id": schedule_id,
            "chat_id": chat_key,
            "kind": str(kind or "reminder").strip().lower(),
            "title": str(title or "Schedule").strip() or "Schedule",
            "message": str(message or "").strip(),
            "prompt": str(prompt or "").strip(),
            "created_at": _to_utc_iso(_utc_now()),
            "last_fired_at": "",
            "next_run_at": _to_utc_iso(next_run_at),
            "recurrence": dict(recurrence or {"kind": "once"}),
        }
        with self._lock:
            self._items[schedule_id] = row
            self._persist()
        return dict(row)

    def _advance_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        recurrence = item.get("recurrence") if isinstance(item.get("recurrence"), dict) else {}
        kind = str(recurrence.get("kind") or "once").strip().lower()
        fired_at = _utc_now()
        updated = dict(item)
        updated["last_fired_at"] = _to_utc_iso(fired_at)
        last_next_run = _parse_utc(item.get("next_run_at"))

        if kind == "daily":
            anchor = (last_next_run or fired_at).astimezone(DISPLAY_TIMEZONE) + timedelta(days=1)
            updated["next_run_at"] = _to_utc_iso(anchor)
            return updated
        if kind == "weekly":
            anchor = (last_next_run or fired_at).astimezone(DISPLAY_TIMEZONE) + timedelta(days=7)
            updated["next_run_at"] = _to_utc_iso(anchor)
            return updated
        return None


_schedule_service: Optional[RobbieScheduleService] = None
_schedule_lock = threading.Lock()


def get_schedule_service() -> RobbieScheduleService:
    global _schedule_service
    with _schedule_lock:
        if _schedule_service is None:
            _schedule_service = RobbieScheduleService()
        return _schedule_service


__all__ = ["RobbieScheduleService", "get_schedule_service"]
