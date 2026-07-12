"""G6 — the state machine never loses an update and never crashes the runner
on a concurrent-write (generation) conflict.

Before F4 the transaction block caught DocumentAlreadyExists / ValidationError
but not GenerationConflict, so a competing writer's CAS conflict propagated out
of process() and killed the runner process. Now a transient conflict is retried
(re-read + re-evaluate) and a persistent one is rejected (the runner DLQs it).
"""

from __future__ import annotations

import pytest

from phronexus.errors import GenerationConflict
from phronexus.kv.memory import InMemoryKV
from phronexus.statemachine import InputEvent
from tests.harness import FaultKV, FaultRule, phronexus_with

BOOK = {"trade_id": "T-G6", "counterparty": "GS", "notional": 1e6, "ccy": "USD",
        "trade_date": 20250115, "book": "R"}


def _ev(eid):
    return InputEvent(entity="trade", event_type="TradeBooked", key="T-G6", payload=BOOK, event_id=eid)


@pytest.mark.chaos
def test_transient_conflict_is_retried_then_applies():
    fault = FaultKV(InMemoryKV())
    px = phronexus_with(fault)
    sm = px.state_machine()
    # One transient conflict on the first commit; the retry must succeed.
    fault.arm(FaultRule("commit", at_call=1, error=GenerationConflict("transient")))

    res = sm.process(_ev("e1"))

    assert res.status == "applied" and res.to_state == "booked"
    assert px.get("trade", "T-G6")["status"] == "booked"
    px.close()


@pytest.mark.chaos
def test_persistent_conflict_rejects_without_crashing():
    fault = FaultKV(InMemoryKV())
    px = phronexus_with(fault)
    sm = px.state_machine()
    # Every commit conflicts -> retries exhaust -> reject (NOT an escaped exception).
    fault.arm(FaultRule("commit", at_call=1, repeat=True, error=GenerationConflict("persistent")))

    res = sm.process(_ev("e2"))

    assert res.status == "rejected" and "conflict" in res.reason
    # nothing was committed
    assert px.get("trade", "T-G6") is None
    px.close()
