"""CCR1 — trade & counterparty onboarding through validation + reference DQ + saga."""

from __future__ import annotations

import pytest

from ccrsupport import a_trade, build_ccr
from phronexus.statemachine import InputEvent, MemoryOutputPublisher


@pytest.mark.ccr
def test_counterparty_onboards_through_saga():
    import ccr_ops  # examples/ccr/ccr_ops.py

    px = build_ccr()
    sm = px.state_machine(output=MemoryOutputPublisher())
    status = ccr_ops.onboard_counterparty(sm, {
        "counterparty_id": "MS", "name": "Morgan Stanley",
        "jurisdiction": "US", "rating": "A"})
    assert status == "active"
    assert px.get("counterparty", "MS")["status"] == "active"
    px.close()


@pytest.mark.ccr
def test_trade_onboards_when_reference_data_resolves():
    px = build_ccr()
    out = MemoryOutputPublisher()
    sm = px.state_machine(output=out)
    r = sm.process(InputEvent(entity="ccr_trade", event_type="TradeReceived",
                              key="CCR-T-1", payload=a_trade(), event_id="e1"))
    assert r.status == "applied" and r.to_state == "cube_requested"
    assert [e.topic for e in out.events] == ["kafka://mfl.cube.requests"]
    px.close()


@pytest.mark.ccr
def test_trade_rejected_when_counterparty_not_onboarded():
    px = build_ccr()
    sm = px.state_machine(output=MemoryOutputPublisher())
    # counterparty "XX" was never onboarded -> reference DQ fails -> rejected.
    r = sm.process(InputEvent(entity="ccr_trade", event_type="TradeReceived",
                              key="CCR-T-9", payload=a_trade("CCR-T-9", counterparty="XX"),
                              event_id="e9"))
    assert r.status == "rejected" and "counterparty" in r.reason.lower()
    assert px.get("ccr_trade", "CCR-T-9") is None
    px.close()


@pytest.mark.ccr
def test_trade_rejected_when_netting_set_unknown():
    px = build_ccr()
    sm = px.state_machine(output=MemoryOutputPublisher())
    r = sm.process(InputEvent(entity="ccr_trade", event_type="TradeReceived",
                              key="CCR-T-8", payload=a_trade("CCR-T-8", netting_set_id="NS-UNKNOWN"),
                              event_id="e8"))
    assert r.status == "rejected" and "netting" in r.reason.lower()
    px.close()
