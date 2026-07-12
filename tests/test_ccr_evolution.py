"""CCR2 — governed contract evolution + reference-data change.

A ccr_trade contract change goes through draft→submit→approve→publish and a
backfill re-projects existing trades. A reference-data update (adding a currency)
changes what the trade DQ admits.
"""

from __future__ import annotations

import pytest

from ccrsupport import a_trade, build_ccr
from phronexus.admin import BackfillJob
from phronexus.statemachine import InputEvent, MemoryOutputPublisher


def _ccr_trade_v2(px) -> dict:
    d = px.registry.active_storage("ccr_trade").model_dump(mode="json")
    d["version"] = 2
    d["projections"].append({
        "name": "by_product", "set": "ccr_trade_by_product",
        "key": "{product_type}:{trade_id}",
        "fields": ["trade_id", "product_type", "counterparty", "notional", "status"],
    })
    return d


@pytest.mark.ccr
def test_governed_evolution_then_backfill_reprojects():
    px = build_ccr(approval_policy={"*": {"*": 1}})
    gov = px.governance
    px.put("ccr_trade", a_trade("CCR-T-1"))
    assert px.store.get("ccr_trade_by_product", "IRS:CCR-T-1") is None  # new projection absent

    # Governed evolution to v2 (adds a projection — non-breaking).
    cr = gov.draft("alice", _ccr_trade_v2(px))
    cr = gov.submit("alice", cr["id"])
    assert cr["compat"]["compatible"] is True
    gov.approve("bob", cr["id"])
    gov.publish("bob", cr["id"])
    assert px.registry.active_storage("ccr_trade").version == 2

    # Backfill re-projects existing trades under the new active contract.
    assert BackfillJob(px).run("ccr_trade") == 1
    assert px.store.get("ccr_trade_by_product", "IRS:CCR-T-1") is not None
    px.close()


@pytest.mark.ccr
def test_reference_data_update_gates_then_admits():
    px = build_ccr()
    sm = px.state_machine(output=MemoryOutputPublisher())
    jpy_trade = a_trade("CCR-T-JPY", currency="JPY")

    # JPY is a supported code but not yet in the reference data -> rejected.
    r = sm.process(InputEvent(entity="ccr_trade", event_type="TradeReceived",
                              key="CCR-T-JPY", payload=jpy_trade, event_id="j1"))
    assert r.status == "rejected" and "currency" in r.reason.lower()

    # Reference-data change: add JPY. Now the same trade is admitted.
    px.put("currency", {"code": "JPY", "usd_rate": 0.0067})
    r2 = sm.process(InputEvent(entity="ccr_trade", event_type="TradeReceived",
                               key="CCR-T-JPY", payload=jpy_trade, event_id="j2"))
    assert r2.status == "applied"
    px.close()
