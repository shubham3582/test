"""Standalone outbox relay: ``python -m phronexus.statemachine.relay``.

Drains the transactional outbox to the output publisher, independently of the
write path. Run this (with ``statemachine.inline_relay: false``) so a slow
broker or webhook never adds latency to the sync REST endpoint or the runner —
the transition commits durably, and the relay publishes at-least-once.

The relay is safe to run with multiple replicas: each outbox row is removed only
after a successful publish, and downstream consumers dedup on event id.
"""

from __future__ import annotations

import time

import structlog

from phronexus.config import Settings
from phronexus.core import Phronexus
from phronexus.statemachine.io import build_output_publisher
from phronexus.statemachine.machine import StateMachine

log = structlog.get_logger("phronexus.relay")


def run_once(machine: StateMachine, batch_size: int = 1000) -> int:
    return machine.drain_outbox(max_events=batch_size)


def main() -> None:  # pragma: no cover - process entrypoint
    settings = Settings()
    px = Phronexus(settings)
    machine = StateMachine(px, output=build_output_publisher(settings))
    poll = settings.statemachine.relay_poll_seconds
    log.info("relay.start", poll_seconds=poll)
    try:
        while True:
            published = run_once(machine)
            if published == 0:
                time.sleep(poll)
    except KeyboardInterrupt:
        pass
    finally:
        machine.output.close()
        px.close()


if __name__ == "__main__":  # pragma: no cover
    main()
