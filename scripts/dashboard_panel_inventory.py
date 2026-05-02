"""Inventory dashboard panel surfaces across HTML templates.

This is a static markup audit: it counts top-level template panels plus
card/widget blocks rendered inside those panels. It intentionally excludes
styling-only tokens such as panel-header, panel-body, and panel-title.
"""

from __future__ import annotations

import argparse
import json
import re
from html.parser import HTMLParser
from pathlib import Path
from typing import Any


TEMPLATE_DIR = Path("templates")

PANEL_CLASSES = {
    "panel",
    "chart-panel",
    "inspector-panel",
    "node-detail-panel",
}

WIDGET_CLASSES = {
    "metric-card",
    "mc-card",
    "pulse-card",
    "state-card",
    "mix-card",
    "translation-card",
    "outcome-card",
    "summary-card",
    "bias-card",
    "event-risk-card",
    "flow-bridge-card",
    "meter-card",
    "fresh-card",
    "source-card",
    "train-card",
    "prediction-card",
    "pred-card",
    "kill-card",
    "replay-card",
    "heat-card",
    "film-card",
    "topop-card",
    "depth-tape-card",
    "inspector-card",
}

IGNORE_CLASSES = {
    "panel-header",
    "panel-body",
    "panel-title",
    "panel-badge",
    "panel-grid",
    "panel-columns",
    "metric-grid",
}


def _class_tokens(attrs: list[tuple[str, str | None]]) -> set[str]:
    for key, value in attrs:
        if key == "class" and value:
            return set(value.split())
    return set()


def _id_attr(attrs: list[tuple[str, str | None]]) -> str:
    for key, value in attrs:
        if key == "id" and value:
            return value
    return ""


class PanelParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.blocks: list[dict[str, Any]] = []
        self._stack: list[int | None] = []
        self._capture_title_for: int | None = None
        self._capture_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        classes = _class_tokens(attrs)
        if classes & IGNORE_CLASSES:
            self._stack.append(None)
            if "panel-title" in classes and self.blocks:
                self._capture_title_for = len(self.blocks) - 1
                self._capture_text = []
            return

        kind = ""
        if classes & PANEL_CLASSES:
            kind = "panel"
        elif classes & WIDGET_CLASSES:
            kind = "widget"

        if kind:
            self.blocks.append(
                {
                    "kind": kind,
                    "tag": tag,
                    "id": _id_attr(attrs),
                    "classes": sorted(classes),
                    "title": "",
                }
            )
            self._stack.append(len(self.blocks) - 1)
            return

        self._stack.append(None)

    def handle_data(self, data: str) -> None:
        if self._capture_title_for is not None:
            self._capture_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if self._capture_title_for is not None:
            text = re.sub(r"\s+", " ", "".join(self._capture_text)).strip()
            if text and not self.blocks[self._capture_title_for]["title"]:
                self.blocks[self._capture_title_for]["title"] = text
            self._capture_title_for = None
            self._capture_text = []
        if self._stack:
            self._stack.pop()


def inventory_template(path: Path) -> dict[str, Any]:
    parser = PanelParser()
    parser.feed(path.read_text(encoding="utf-8", errors="replace"))
    panels = [block for block in parser.blocks if block["kind"] == "panel"]
    widgets = [block for block in parser.blocks if block["kind"] == "widget"]
    return {
        "template": path.name,
        "top_level_panels": len(panels),
        "panel_widgets": len(widgets),
        "total_panel_blocks": len(parser.blocks),
        "panel_titles": [block["title"] or block["id"] or "untitled" for block in panels],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")
    args = parser.parse_args()

    rows = [inventory_template(path) for path in sorted(TEMPLATE_DIR.glob("*.html"))]
    if args.json:
        print(json.dumps(rows, indent=2))
        return

    print(f"{'template':32} {'panels':>6} {'widgets':>7} {'total':>6}")
    print("-" * 56)
    for row in rows:
        print(
            f"{row['template']:32} "
            f"{row['top_level_panels']:6d} "
            f"{row['panel_widgets']:7d} "
            f"{row['total_panel_blocks']:6d}"
        )


if __name__ == "__main__":
    main()
