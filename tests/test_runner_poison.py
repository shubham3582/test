"""A structurally-broken inbound message (bad JSON / missing envelope field)
must not wedge the partition: KafkaInputSource.poll routes the raw bytes to the
poison handler and skips, so the offset advances and following messages process.
"""
from __future__ import annotations

import json

from phronexus.statemachine.io import KafkaInputSource
from phronexus.statemachine.runner import run

BOOK = {"trade_id": "T-1", "counterparty": "GS", "notional": 1e6,
        "ccy": "USD", "trade_date": 20250115, "book": "R"}


class _Msg:
    def __init__(self, value: bytes):
        self._v = value

    def error(self):
        return None

    def value(self):
        return self._v

    def topic(self):
        return "trades"

    def partition(self):
        return 0

    def offset(self):
        return 0

    def timestamp(self):
        return (1, 0)


class _FakeConsumer:
    """Redelivers from the last committed offset — the at-least-once semantics
    the runner relies on."""

    def __init__(self, values: list[bytes]):
        self._log = values
        self.committed_offset = 0
        self._pos = 0

    def poll(self, timeout):
        if self._pos < len(self._log):
            msg = _Msg(self._log[self._pos])
            self._pos += 1
            return msg
        return None

    def commit(self, asynchronous=False):
        self.committed_offset = self._pos

    def close(self):
        pass


def _source(consumer, on_poison) -> KafkaInputSource:
    src = KafkaInputSource.__new__(KafkaInputSource)  # skip real-broker __init__
    src._c = consumer
    src._journal = None
    src._on_poison = on_poison
    return src


def _good(event_id: str) -> bytes:
    return json.dumps({
        "entity": "trade", "event_type": "TradeBooked", "key": "T-1",
        "event_id": event_id, "payload": BOOK,
    }).encode()


def test_poison_message_is_routed_and_skipped(px):
    poison = b'{"event_type": "TradeBooked", "key": "T-1", "payload": {}}'  # no entity/event_id
    consumer = _FakeConsumer([poison, _good("e-good")])

    routed: list[tuple[bytes, str]] = []
    src = _source(consumer, on_poison=lambda raw, meta, exc: routed.append((raw, str(exc))))
    sm = px.state_machine()

    tally = run(px, src, sm, max_batches=1)

    # The poison was quarantined (raw bytes preserved), not raised.
    assert len(routed) == 1 and routed[0][0] == poison
    # The following good message still applied.
    assert tally["applied"] == 1
    assert px.get("trade", "T-1") is not None
    # Offset advanced past BOTH messages — the partition is not stuck.
    assert consumer.committed_offset == 2


def test_bad_json_is_also_quarantined(px):
    consumer = _FakeConsumer([b"{not json at all", _good("e2")])
    routed: list = []
    src = _source(consumer, on_poison=lambda raw, meta, exc: routed.append(raw))
    sm = px.state_machine()

    tally = run(px, src, sm, max_batches=1)

    assert routed == [b"{not json at all"]
    assert tally["applied"] == 1
    assert consumer.committed_offset == 2


def test_default_poison_handler_logs_and_drops(px):
    # No handler supplied -> the log-and-drop default; must not raise or wedge.
    from phronexus.statemachine.io import _log_poison

    consumer = _FakeConsumer([b"garbage"])
    src = _source(consumer, on_poison=_log_poison)
    sm = px.state_machine()

    tally = run(px, src, sm, max_batches=1)

    assert sum(tally.values()) == 0        # nothing processed, nothing raised
    assert consumer.committed_offset == 1  # offset advanced past the poison
