"""CCR9 — lineage from source event to exposure result.

px.lineage(trade_id) stitches the source event → saga steps (audit trace +
interactions) → cube inputs → exposure result → served netting-set exposure,
correlated and ordered, with the producing contract versions.
"""

from __future__ import annotations

import pytest

from ccrsupport import a_trade, build_ccr
from phronexus.statemachine import InputEvent, MemoryOutputPublisher

COB = 20260711


def _ev(etype, eid, **payload):
    return InputEvent(entity="ccr_trade", event_type=etype, key="CCR-T-1",
                      payload=payload, event_id=eid)


@pytest.mark.ccr
def test_lineage_stitches_source_event_to_exposure():
    import ccr_ops

    px = build_ccr(journal=True)
    px.governance.set_cob("ops", COB)
    sm = px.state_machine(output=MemoryOutputPublisher())

    # source event → saga → published
    sm.process(_ev("TradeReceived", "e1", **a_trade("CCR-T-1")))
    sm.process(_ev("CubeReady", "e2", as_of=COB))
    sm.process(_ev("CalcComplete", "e3", exposure=100.0))
    # cube inputs + exposure result + served aggregation
    for tenor in (1, 7, 30):
        px.put("value_cube", {"trade_id": "CCR-T-1", "scenario_id": "BASE", "tenor": tenor,
                              "value": 1000.0 + tenor, "currency": "USD", "as_of": COB})
    px.put("exposure_result", {"trade_id": "CCR-T-1", "cob": COB, "exposure": 100.0,
                               "currency": "USD", "source_event_id": "e1"})
    ccr_ops.aggregate_netting_set(px, "NS-GS-USD", COB, source_event_id="e1")

    lin = px.lineage("CCR-T-1")

    assert lin["trade_id"] == "CCR-T-1" and lin["as_of"] == COB
    assert lin["source"]["trade"]["status"] == "published"
    assert lin["source"]["netting_set_id"] == "NS-GS-USD"
    # the saga's commit/transition history is present (audit trace)
    assert len(lin["saga"]) >= 1
    # request/reply interactions correlated to this trade
    assert any(i["type"] == "TradeReceived" for i in lin["interactions"])
    # cube inputs that fed the calc
    assert lin["cube"]["point_count"] == 3 and lin["cube"]["scenarios"] == ["BASE"]
    # the result artifacts
    assert lin["exposure_result"]["exposure"] == 100.0
    assert lin["exposure_result"]["source_event_id"] == "e1"
    assert lin["ns_exposure"]["exposure"] == 100.0
    # provenance: producing contract versions
    assert lin["contract_versions"]["ccr_trade"] == 1
    assert lin["contract_versions"]["exposure_result"] == 1
    px.close()
