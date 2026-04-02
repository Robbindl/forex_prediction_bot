from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

from config.config import AUTO_RESEARCH_ALLOW_IN_BOT_RUNTIME, AUTO_RESEARCH_ALLOW_SEPARATE_WORKER

AUTO_RESEARCH_WORKER_SCRIPT = Path("strategy_lab/auto_research_worker.py")


def should_start_separate_auto_research_worker(settings: Dict[str, Any] | None = None) -> bool:
    resolved = dict(settings or {})
    return (
        bool(resolved.get("enabled"))
        and not AUTO_RESEARCH_ALLOW_IN_BOT_RUNTIME
        and AUTO_RESEARCH_ALLOW_SEPARATE_WORKER
    )


def build_auto_research_worker_command(python_executable: str | None = None) -> List[str]:
    python_path = str(python_executable or sys.executable)
    return [python_path, str(AUTO_RESEARCH_WORKER_SCRIPT)]


__all__ = [
    "AUTO_RESEARCH_WORKER_SCRIPT",
    "build_auto_research_worker_command",
    "should_start_separate_auto_research_worker",
]
