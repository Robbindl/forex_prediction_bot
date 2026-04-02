from __future__ import annotations

import os
import signal
import sys
import time

from utils.logger import get_logger

logger = get_logger()
_STOP = False


def _request_stop(signum, frame) -> None:  # pragma: no cover - signal handler
    global _STOP
    _STOP = True
    logger.info(f"[AutoResearchWorker] Stop requested (signal={signum})")


def main() -> int:
    global _STOP
    os.environ["BOT_AUTO_RESEARCH_WORKER"] = "1"

    try:
        from strategy_lab.auto_research import load_auto_research_settings, start_auto_research_scheduler

        settings = load_auto_research_settings()
        if not bool(settings.get("enabled")):
            logger.info("[AutoResearchWorker] Disabled in runtime config — exiting")
            return 0

        scheduler = start_auto_research_scheduler()
        if scheduler is None:
            logger.info("[AutoResearchWorker] Scheduler did not start — exiting")
            return 0

        logger.info(
            "[AutoResearchWorker] Started "
            f"(startup_delay={int(settings.get('startup_delay_seconds', 0) or 0)}s, "
            f"interval_hours={float(settings.get('interval_hours', 24.0) or 24.0):.1f}, "
            f"parallel={int(settings.get('max_parallel_assets', 1) or 1)})"
        )

        for sig in ("SIGINT", "SIGTERM"):
            if hasattr(signal, sig):
                signal.signal(getattr(signal, sig), _request_stop)

        while not _STOP:
            time.sleep(1.0)

        scheduler.stop()
        logger.info("[AutoResearchWorker] Stopped")
        return 0
    except KeyboardInterrupt:
        logger.info("[AutoResearchWorker] Interrupted")
        return 0
    except Exception as exc:
        logger.error(f"[AutoResearchWorker] Fatal error: {exc}", exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
