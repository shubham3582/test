from __future__ import annotations

import pytest

from phronexus.statemachine import InputEvent, MemoryOutputPublisher

BOOK = {"trade_id": "T-1", "counterparty": "GS", "notional": 1e6, "ccy": "USD", "trade_date": 20250115, "book": "R"}


def _stream(mode="enforce"):
    # Require every emitted TradeBooked event to carry a settle_ref.
    return {
        "kind": "stream", "entity": "trade", "version": 1, "mode": mode,
        "events": [{
            "type": "TradeBooked",
            "json_schema": {"type": "object", "required": ["settle_ref"],
                            "properties": {"settle_ref": {"type": "string"}}},
        }],
    }


def _ev(payload, eid="e1"):
    return InputEvent(entity="trade", event_type="TradeBooked", key="T-1", payload=payload, event_id=eid)


def test_validate_event_directly(px):
    px.publish_contract(_stream())
    assert px.validate_event("trade", "TradeBooked", {**BOOK, "settle_ref": "S1"}).ok
    rep = px.validate_event("trade", "TradeBooked", BOOK)
    assert not rep.ok and any("settle_ref" in e for e in rep.errors)


def test_no_schema_for_type_passes(px):
    px.publish_contract(_stream())
    assert px.validate_event("trade", "SomeOtherEvent", {}).ok   # no schema -> ok


def test_invalid_event_rejects_transition(px):
    px.publish_contract(_stream())
    out = MemoryOutputPublisher()
    sm = px.state_machine(output=out)
    r = sm.process(_ev(BOOK))                                    # payload lacks settle_ref
    assert r.status == "rejected" and "event schema" in r.reason
    assert px.get("trade", "T-1") is None                        # nothing committed
    assert out.events == []                                      # nothing published


def test_valid_event_applies(px):
    px.publish_contract(_stream())
    out = MemoryOutputPublisher()
    sm = px.state_machine(output=out)
    r = sm.process(_ev({**BOOK, "settle_ref": "S1"}))
    assert r.status == "applied"
    assert [e.topic for e in out.events] == ["kafka://trades.booked"]


def test_warn_only_publishes_with_warning(px):
    px.publish_contract(_stream(mode="warn_only"))
    out = MemoryOutputPublisher()
    sm = px.state_machine(output=out)
    r = sm.process(_ev(BOOK))                                    # missing settle_ref, but warn only
    assert r.status == "applied" and out.events                  # published anyway


def test_bond_stream_example_roundtrips(px):
    # The shipped bond stream schema accepts the activated bond document.
    px.load_contract_dir("examples/bond")
    sm = px.state_machine()
    apple = {"isin": "US0378331005", "issuer": "APPLE", "coupon": 3.85,
             "currency": "USD", "maturity_date": 20310215, "callable": True}
    r = sm.process(InputEvent(entity="bond", event_type="BondIssued", key="US0378331005",
                              payload=apple, event_id="b1"))
    assert r.status == "applied" and r.emitted == ["kafka://bonds.active"]
