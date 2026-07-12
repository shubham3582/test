"""CCR3 — netting-set lifecycle, trade membership, and served aggregation."""

from __future__ import annotations

import pytest

from ccrsupport import a_trade, build_ccr
from phronexus.statemachine import InputEvent, MemoryOutputPublisher

COB = 20260711


def _ns_event(etype, nsid, eid, **payload):
    return InputEvent(entity="netting_set", event_type=etype, key=nsid,
                      payload={"netting_set_id": nsid, "counterparty": "GS", **payload},
                      event_id=eid)


@pytest.mark.ccr
def test_netting_set_lifecycle_open_amend_close():
    px = build_ccr()
    sm = px.state_machine(output=MemoryOutputPublisher())
    nsid = "NS-LIFE-1"
    steps = [
        ("NettingSetOpened", "open", {"csa_id": "CSA-9"}),
        ("CsaConfirmed", "active", {}),
        ("NettingSetAmended", "active", {"threshold": 1_000_000}),
        ("NettingSetClosed", "closed", {}),
    ]
    for i, (etype, to, payload) in enumerate(steps):
        r = sm.process(_ns_event(etype, nsid, f"ns{i}", **payload))
        assert r.status == "applied" and r.to_state == to, (etype, r.status, r.reason)
    assert px.get("netting_set", nsid)["status"] == "closed"
    px.close()


@pytest.mark.ccr
def test_membership_and_served_aggregation():
    import ccr_ops

    px = build_ccr()
    # two trades in the same netting set, each with a published exposure
    for tid, exp in (("CCR-T-1", 100.0), ("CCR-T-2", 200.0)):
        px.put("ccr_trade", a_trade(tid))
        px.put("exposure_result", {"trade_id": tid, "cob": COB, "exposure": exp,
                                   "currency": "USD", "netting_set_id": "NS-GS-USD"})

    # membership is index-listable
    members = {t["trade_id"] for t in px.find("ccr_trade", "netting_set_id", "NS-GS-USD")}
    assert members == {"CCR-T-1", "CCR-T-2"}

    # served aggregation sums member exposures into a netting-set exposure
    agg = ccr_ops.aggregate_netting_set(px, "NS-GS-USD", COB)
    assert agg["trade_count"] == 2 and agg["exposure"] == 300.0
    assert px.get("ns_exposure", "NS-GS-USD", as_of=COB)["exposure"] == 300.0
    px.close()
