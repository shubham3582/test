from __future__ import annotations

import pytest

from phronexus.statemachine import InputEvent
from phronexus.statemachine.guard import safe_eval
from phronexus.statemachine.io import MemoryOutputPublisher
from phronexus.statemachine.node import PhronexusStateMachineNode


@pytest.fixture()
def sm(px):
    out = MemoryOutputPublisher()
    return px.state_machine(output=out), out


def _ev(etype, key, payload, eid):
    return InputEvent(entity="trade", event_type=etype, key=key, payload=payload, event_id=eid)


BOOK = {"trade_id": "T-1", "counterparty": "GS", "notional": 1_000_000.0, "ccy": "USD", "trade_date": 20250115, "book": "R"}


def test_creation_transition_sets_state_and_emits(sm, px):
    machine, out = sm
    r = machine.process(_ev("TradeBooked", "T-1", BOOK, "e1"))
    assert r.status == "applied" and r.from_state is None and r.to_state == "booked"
    assert px.get("trade", "T-1")["status"] == "booked"
    assert [o.topic for o in out.events] == ["kafka://trades.booked"]


def test_full_lifecycle(sm, px):
    machine, out = sm
    machine.process(_ev("TradeBooked", "T-1", BOOK, "e1"))
    r2 = machine.process(_ev("TradeConfirmed", "T-1", {}, "e2"))
    r3 = machine.process(_ev("TradeSettled", "T-1", {}, "e3"))
    assert (r2.to_state, r3.to_state) == ("confirmed", "settled")
    assert px.get("trade", "T-1")["status"] == "settled"
    assert [o.topic for o in out.events] == [
        "kafka://trades.booked", "kafka://settlement.requests", "kafka://trades.settled",
    ]


def test_wrong_transition_rejected_and_atomic(sm, px):
    machine, out = sm
    machine.process(_ev("TradeBooked", "T-1", BOOK, "e1"))
    # Settling straight from booked is not allowed (needs confirmed).
    r = machine.process(_ev("TradeSettled", "T-1", {}, "e2"))
    assert r.status == "rejected"
    # Nothing changed: state still booked, no extra output, no dedup marker.
    assert px.get("trade", "T-1")["status"] == "booked"
    assert len(out.events) == 1
    assert px.store.get(px.settings.statemachine.dedup_set, "e2") is None


def test_guard_failure_rejects_without_side_effects(sm, px):
    machine, out = sm
    machine.process(_ev("TradeBooked", "T-1", BOOK, "e1"))
    machine.process(_ev("TradeConfirmed", "T-1", {}, "e2"))
    # Guard notional > 0 fails when we override notional to 0.
    r = machine.process(_ev("TradeSettled", "T-1", {"notional": 0}, "e3"))
    assert r.status == "rejected" and "guard" in r.reason
    assert px.get("trade", "T-1")["status"] == "confirmed"
    assert px.store.get(px.settings.statemachine.dedup_set, "e3") is None


def test_duplicate_event_is_idempotent(sm, px):
    machine, out = sm
    machine.process(_ev("TradeBooked", "T-1", BOOK, "e1"))
    r_dup = machine.process(_ev("TradeBooked", "T-1", BOOK, "e1"))  # same event_id
    assert r_dup.status == "duplicate"
    # No second output emitted despite redelivery.
    assert [o.topic for o in out.events] == ["kafka://trades.booked"]


def test_wildcard_cancel_from_any_state(sm, px):
    machine, out = sm
    machine.process(_ev("TradeBooked", "T-1", BOOK, "e1"))
    r = machine.process(_ev("TradeCancelled", "T-1", {}, "e2"))
    assert r.status == "applied" and r.to_state == "cancelled"


def test_outbox_drained_only_after_commit(sm, px):
    machine, out = sm
    machine.process(_ev("TradeBooked", "T-1", BOOK, "e1"))
    # After a successful process the outbox is empty (relayed) and output holds it.
    assert list(px.store.scan(px.settings.statemachine.outbox_set)) == []
    assert len(out.events) == 1


def test_committed_state_flows_to_change_feed(sm, px):
    machine, _ = sm
    machine.process(_ev("TradeBooked", "T-1", BOOK, "e1"))
    # The state document is also emitted on the Phronexus change-feed (retention).
    assert any(e.entity == "trade" and e.doc_id == "T-1" for e in px.sink.events)


def test_node_face_delegates_to_same_engine(sm, px):
    machine, out = sm
    node = PhronexusStateMachineNode(machine, entity="trade")
    res = node.calculate({"event_type": "TradeBooked", "key": "T-1", "event_id": "e1", "payload": BOOK})
    assert res["status"] == "applied" and res["to"] == "booked"
    assert px.get("trade", "T-1")["status"] == "booked"
    assert node.details()["processed"] == 1


def test_guard_evaluator_is_sandboxed():
    assert safe_eval("a > 1 and b == 'x'", {"a": 2, "b": "x"}) is True
    assert safe_eval("a in [1,2,3]", {"a": 2}) is True
    from phronexus.errors import GuardError
    with pytest.raises(GuardError):
        safe_eval("__import__('os').system('ls')", {})
