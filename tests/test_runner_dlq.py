from __future__ import annotations

from phronexus.errors import GenerationConflict
from phronexus.statemachine import InputEvent, MemoryOutputPublisher
from phronexus.statemachine.io import MemoryInputSource
from phronexus.statemachine.runner import run

BOOK = {"trade_id": "T-1", "counterparty": "GS", "notional": 1e6, "ccy": "USD", "trade_date": 20250115, "book": "R"}


def test_runner_dead_letters_rejected(px):
    px.settings.statemachine.dlq_topic = "kafka://phronexus.dlq"
    out = MemoryOutputPublisher()
    sm = px.state_machine(output=out)
    # TradeSettled with no prior state -> no valid transition -> rejected.
    bad = InputEvent(entity="trade", event_type="TradeSettled", key="T-9", payload={}, event_id="e1")
    tally = run(px, MemoryInputSource([bad]), sm, max_batches=1)
    assert tally["rejected"] == 1 and tally["dead_lettered"] == 1
    dlq = [e for e in out.events if e.topic == "kafka://phronexus.dlq"]
    assert len(dlq) == 1 and dlq[0].type == "DeadLetter"
    assert dlq[0].payload["reason"].startswith("no transition")


def test_runner_applies_and_does_not_dlq_valid(px):
    px.settings.statemachine.dlq_topic = "kafka://phronexus.dlq"
    out = MemoryOutputPublisher()
    sm = px.state_machine(output=out)
    good = InputEvent(entity="trade", event_type="TradeBooked", key="T-1", payload=BOOK, event_id="e1")
    tally = run(px, MemoryInputSource([good]), sm, max_batches=1)
    assert tally["applied"] == 1 and tally["dead_lettered"] == 0
    assert not any(e.topic == "kafka://phronexus.dlq" for e in out.events)


def test_no_dlq_topic_configured_is_noop(px):
    out = MemoryOutputPublisher()
    sm = px.state_machine(output=out)                     # dlq_topic defaults to None
    bad = InputEvent(entity="trade", event_type="TradeSettled", key="T-9", payload={}, event_id="e1")
    tally = run(px, MemoryInputSource([bad]), sm, max_batches=1)
    assert tally["rejected"] == 1 and tally["dead_lettered"] == 0


def test_write_retries_on_conflict(px, monkeypatch):
    calls = {"n": 0}
    orig = px.manifest.stage_write

    def flaky(entity, document, txn):
        calls["n"] += 1
        if calls["n"] == 1:
            raise GenerationConflict("simulated CAS conflict")
        return orig(entity, document, txn)

    monkeypatch.setattr(px.manifest, "stage_write", flaky)
    px.put("trade", BOOK)                                 # first attempt conflicts, retry succeeds
    assert calls["n"] == 2
    assert px.get("trade", "T-1") == BOOK
