from __future__ import annotations

import telegram_commander as tg_mod


def test_telegram_commander_sanitise_markdown_strips_heading_markers() -> None:
    text = "## Snapshot\nBalance: 100"
    rendered = tg_mod.TelegramCommander._sanitise_markdown(text)

    assert rendered == "Snapshot\nBalance: 100"
