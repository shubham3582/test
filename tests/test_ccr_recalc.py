"""CCR4 — intraday recalculation.

A RecalcRequested trigger re-drives the saga (re-request cube -> calc) and the
calculator publishes a NEW exposure version at the SAME COB but a later tx-time.
The morning value stays reproducible as-known-at its tx-time.
"""

from __future__ import annotations

import time

import pytest

from ccrsupport import a_trade, build_ccr
from phronexus.statemachine import InputEvent, MemoryOutputPublisher

COB = 20260711


def _ev(etype, eid, **payload):
    return InputEvent(entity="ccr_trade", event_type=etype, key="CCR-T-1",
                      payload=payload, event_id=eid)


@pytest.mark.ccr
def test_intraday_recalc_new_version_same_cob_later_tx():
    px = build_ccr()
    px.governance.set_cob("ops", COB)
    sm = px.state_machine(output=MemoryOutputPublisher())

    # Morning: onboard + first calc, calculator publishes the morning exposure.
    sm.process(_ev("TradeReceived", "e1", **a_trade("CCR-T-1")))
    sm.process(_ev("CubeReady", "e2", as_of=COB))
    sm.process(_ev("CalcComplete", "e3", exposure=100.0))
    px.put("exposure_result", {"trade_id": "CCR-T-1", "cob": COB, "exposure": 100.0, "currency": "USD"})
    assert px.get("ccr_trade", "CCR-T-1")["status"] == "published"
    t_morning = time.time()
    time.sleep(0.01)

    # Intraday: a recalc trigger re-drives the saga to a fresh calc.
    r = sm.process(_ev("RecalcRequested", "e4"))
    assert r.to_state == "cube_requested"
    sm.process(_ev("CubeReady", "e5", as_of=COB))
    sm.process(_ev("CalcComplete", "e6", exposure=130.0))
    px.put("exposure_result", {"trade_id": "CCR-T-1", "cob": COB, "exposure": 130.0, "currency": "USD"})
    assert px.get("ccr_trade", "CCR-T-1")["status"] == "published"

    # Same COB now reads the recalculated value ...
    assert px.get("exposure_result", "CCR-T-1", as_of=COB)["exposure"] == 130.0
    # ... but the morning value is still reproducible as-known-at the morning.
    assert px.get("exposure_result", "CCR-T-1", as_of=COB, tx_as_of=t_morning)["exposure"] == 100.0
    px.close()
