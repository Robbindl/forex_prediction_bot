from __future__ import annotations

import importlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

from services.robbie_schedule_service import RobbieScheduleService


def test_robbie_schedule_service_advances_weekly_report(tmp_path: Path) -> None:
    service = RobbieScheduleService(path=tmp_path / "robbie_schedules.json")
    item = service.schedule_weekly_report(chat_id="123", weekday=4, hour=18, minute=0)

    first_due = service.claim_due(now_utc=datetime.fromisoformat(str(item["next_run_at"])))

    assert len(first_due) == 1
    rows = service.list_chat_schedules("123")
    assert len(rows) == 1
    next_run = datetime.fromisoformat(str(rows[0]["next_run_at"]).replace("Z", "+00:00"))
    prev_run = datetime.fromisoformat(str(item["next_run_at"]).replace("Z", "+00:00"))
    assert next_run - prev_run >= timedelta(days=6, hours=23)


def test_telegram_dispatches_due_weekly_schedule(monkeypatch) -> None:
    tg_mod = importlib.import_module("telegram_commander")
    schedule_mod = importlib.import_module("services.robbie_schedule_service")

    class _FakeScheduleService:
        @staticmethod
        def claim_due():
            return [{"chat_id": "321", "kind": "weekly_report", "title": "Weekly Report"}]

    commander = object.__new__(tg_mod.TelegramCommander)
    commander.is_running = True
    commander.chat_id = "321"
    commander.trading_system = object()
    commander._build_weekly_report = lambda: "📬 *Weekly Report*\n\nNet P&L: $+50.00"
    sent: dict[str, str] = {}
    commander.send_message = lambda text, **kwargs: sent.setdefault("text", text) or True

    monkeypatch.setattr(schedule_mod, "get_schedule_service", lambda: _FakeScheduleService(), raising=False)

    commander._dispatch_due_schedules_once()

    assert "Weekly Report" in sent["text"]
    assert "Net P&L" in sent["text"]
