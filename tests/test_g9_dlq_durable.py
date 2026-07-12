"""G9 — a dead-lettered event is durably captured, never silently dropped.

Before F9 the DLQ published directly to the output publisher and returned False
on failure; the runner commits the input offset regardless, so a failed DLQ
publish lost the event. Now the DLQ event is staged into the outbox first and
relayed with retry.
"""

from __future__ import annotations

import pytest

from phronexus import Settings
from phronexus.statemachine import InputEvent
from phronexus.statemachine.models import OutputEvent
from tests.harness import phronexus_with
from phronexus.kv.memory import InMemoryKV


class FlakyPublisher:
    """Fails the first ``fail_times`` publishes, then records the rest."""

    def __init__(self, fail_times: int = 0) -> None:
        self.events: list[OutputEvent] = []
        self._fail = fail_times

    def publish(self, oe: OutputEvent) -> None:
        if self._fail > 0:
            self._fail -= 1
            raise RuntimeError("broker down")
        self.events.append(oe)


def _settings():
    s = Settings(backend="memory")
    s.observability.log_level = "WARNING"
    s.statemachine.dlq_topic = "kafka://trades.dlq"
    return s


@pytest.mark.chaos
def test_dlq_survives_publish_failure():
    s = _settings()
    out = FlakyPublisher(fail_times=1)  # the DLQ publish will fail once
    px = phronexus_with(InMemoryKV(), settings=s)
    sm = px.state_machine(output=out)

    # An event with no matching transition -> rejected.
    ev = InputEvent(entity="trade", event_type="NoSuchEvent", key="T-DLQ",
                    payload={}, event_id="e-dlq")
    result = sm.process(ev)
    assert result.status == "rejected"

    # Dead-letter it: the publish fails, but it is captured durably (True).
    assert sm.dead_letter(ev, result) is True
    assert out.events == []  # nothing published yet
    outbox = s.statemachine.outbox_set
    assert any(k.startswith("dlq:") for k, _ in px.store.scan(outbox)), "DLQ row retained"

    # A later drain (publisher healthy) relays it — no loss.
    assert sm.drain_outbox() >= 1
    assert [e.topic for e in out.events] == ["kafka://trades.dlq"]
    assert not any(k.startswith("dlq:") for k, _ in px.store.scan(outbox))  # cleared
    px.close()
