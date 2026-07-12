"""CCR8 — hot/cold tier movement.

Committed trades and (bitemporal) exposures move to the warehouse via the
retention change-feed pipeline. Hot (Aerospike/store) and cold (reconciled log)
agree; expiry drops rows past their horizon.
"""

from __future__ import annotations

import pytest

from ccrsupport import a_trade, build_ccr
from phronexus.retention import InMemoryWarehouse, MemoryEventSource, RetentionWorker, decode

COB = 20260711


@pytest.mark.ccr
def test_trades_and_exposures_move_to_cold_tier():
    px = build_ccr()
    px.put("ccr_trade", a_trade("CCR-T-1"))
    px.put("exposure_result", {"trade_id": "CCR-T-1", "cob": COB, "exposure": 100.0, "currency": "USD"})

    worker = RetentionWorker(px.registry, InMemoryWarehouse())
    worker.run(MemoryEventSource(px.sink))

    # trades landed cold; hot and cold agree
    trades = worker._warehouse.latest_state("warehouse.ccr_trades")
    assert [r["_doc_id"] for r in trades] == ["CCR-T-1"]
    hot = px.get("ccr_trade", "CCR-T-1")
    assert decode(trades[0])["trade_id"] == hot["trade_id"]

    # bitemporal exposure landed cold too
    exp = worker._warehouse.latest_state("warehouse.exposures")
    assert [r["_doc_id"] for r in exp] == ["CCR-T-1"]
    assert decode(exp[0])["exposure"] == 100.0
    px.close()


@pytest.mark.ccr
def test_cold_rows_expire_past_horizon():
    px = build_ccr()
    px.put("ccr_trade", a_trade("CCR-T-1"))
    worker = RetentionWorker(px.registry, InMemoryWarehouse())
    worker.run(MemoryEventSource(px.sink))

    row = worker._warehouse.scan("warehouse.ccr_trades")[0]
    assert row["_expire_at"] is not None
    removed = worker.expire_all(now=row["_expire_at"] + 1)
    assert removed >= 1 and worker._warehouse.count("warehouse.ccr_trades") == 0
    px.close()
