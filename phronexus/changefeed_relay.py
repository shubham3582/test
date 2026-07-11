"""Standalone change-feed relay: ``python -m phronexus.changefeed_relay``.

Relays durably-staged change-feed events (the ``_cf_outbox`` set) to the sink,
independently of the write path. Run this with ``changefeed.inline_relay: false``
so a slow broker never adds latency to writes — the document is durable on
commit, and the relay publishes at-least-once (rows are removed only after a
successful emit; consumers dedup on ``txn_id``).
"""

from __future__ import annotations

import signal
import threading

import structlog

from phronexus.config import Settings
from phronexus.core import Phronexus

log = structlog.get_logger("phronexus.changefeed")


def main() -> None:  # pragma: no cover - process entrypoint
    settings = Settings()
    px = Phronexus(settings)
    poll = settings.changefeed.relay_poll_seconds
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    log.info("changefeed_relay.start", poll_seconds=poll)
    try:
        while not stop.is_set():
            if px.manifest.drain_changefeed() == 0:
                stop.wait(poll)
    finally:
        px.manifest.drain_changefeed()
        px.close()
        log.info("changefeed_relay.stopped")


if __name__ == "__main__":  # pragma: no cover
    main()
