from datetime import datetime, timezone

import services.playbook_service as playbook_module
from services.playbook_service import PlaybookService


def test_precious_metals_are_not_session_blocked_during_asia(monkeypatch):
    monkeypatch.setattr(
        playbook_module,
        "_utc_now",
        lambda: datetime(2026, 5, 6, 1, 30, tzinfo=timezone.utc),
    )
    service = PlaybookService()

    xau_allowed, xau_session, _ = service._session_allowed("XAU/USD", "commodities")
    xag_allowed, xag_session, _ = service._session_allowed("XAG/USD", "commodities")
    wti_allowed, wti_session, _ = service._session_allowed("WTI", "commodities")

    assert xau_session == "asia_core"
    assert xag_session == "asia_core"
    assert wti_session == "asia_core"
    assert xau_allowed is True
    assert xag_allowed is True
    assert wti_allowed is False
