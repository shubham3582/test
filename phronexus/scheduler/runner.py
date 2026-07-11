"""Standalone scheduler: ``python -m phronexus.scheduler.runner``.

Run one or more replicas for availability — the Aerospike CAS lease ensures each
occurrence fires exactly once regardless of replica count. Shuts down gracefully
on SIGTERM/SIGINT.
"""

from __future__ import annotations

import structlog

from phronexus.config import Settings
from phronexus.core import Phronexus
from phronexus.scheduler.engine import Scheduler
from phronexus.statemachine.io import build_output_publisher

log = structlog.get_logger("phronexus.scheduler")


def main() -> None:  # pragma: no cover - process entrypoint
    settings = Settings()
    px = Phronexus(settings)
    scheduler = Scheduler(px, output=build_output_publisher(settings))
    log.info("scheduler.start", poll_seconds=settings.scheduler.poll_seconds)
    try:
        scheduler.run()
    finally:
        scheduler.output.close()
        px.close()
        log.info("scheduler.stopped")


if __name__ == "__main__":  # pragma: no cover
    main()
