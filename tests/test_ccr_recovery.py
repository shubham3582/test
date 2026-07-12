"""CCR10 — recovery from partial infrastructure failure.

Drives the CCR pipeline through the Stage-1 fault harness and asserts the
operational surface loses nothing, never double-effects, and catches up:
  - a torn trade write (Aerospike-CE crash before the manifest) is invisible and
    reaped — no phantom trade ever appears on the risk surface;
  - a broker redelivery of a saga event does not re-request the cube;
  - a retention replay lands exactly one cold-tier row.
"""

from __future__ import annotations

import pytest

from ccrsupport import CCR, a_trade, seed_reference_data
from phronexus import Settings
from phronexus.retention import RetentionWorker
from phronexus.statemachine import InputEvent, MemoryOutputPublisher
from tests.harness import FaultKV, FaultRule, SequentialKV, SimulatedCrash, phronexus_with
from tests.harness.broker import Broker, BrokerInputSource
from tests.harness.warehouses import PhysicalWarehouse

from ccrsupport import build_ccr  # noqa: E402


@pytest.mark.ccr
def test_torn_trade_write_is_invisible_and_reaped():
    s = Settings(backend="memory")
    s.observability.log_level = "WARNING"
    s.changefeed.verify_commit = True  # CE deployment guards phantom events
    fault = FaultKV(SequentialKV())
    px = phronexus_with(fault, contracts=str(CCR / "contracts"), settings=s)
    seed_reference_data(px)
    sc = px.registry.active_storage("ccr_trade")

    fault.arm(FaultRule("put", set_name=sc.manifest_set))  # crash before manifest
    with pytest.raises(SimulatedCrash):
        px.put("ccr_trade", a_trade("CCR-T-1"))

    # The trade never becomes visible ...
    assert px.get("ccr_trade", "CCR-T-1") is None
    # ... and its orphan projections are reaped past the grace period.
    canon = sc.canonical_projection.set
    ts = list(fault.scan(canon))[0][1].bins["_ts"]
    px.reaper.sweep_entity("ccr_trade", now=ts + px.settings.reaper.orphan_grace_seconds + 1)
    assert all(list(fault.scan(p.set)) == [] for p in sc.projections)
    px.close()


@pytest.mark.ccr
def test_broker_redelivery_does_not_double_request_cube():
    px = build_ccr()
    out = MemoryOutputPublisher()
    sm = px.state_machine(output=out)
    broker = Broker()
    ev = InputEvent(entity="ccr_trade", event_type="TradeReceived", key="CCR-T-1",
                    payload=a_trade("CCR-T-1"), event_id="e1")
    broker.produce("ccr.trade.events", ev, key="CCR-T-1")

    # Consumer processes but "crashes" before committing the offset.
    for e in BrokerInputSource(broker, "ccr", ["ccr.trade.events"]).poll(10):
        sm.process(e)
    # A fresh consumer in the same group redelivers the uncommitted event.
    redelivered = BrokerInputSource(broker, "ccr", ["ccr.trade.events"]).poll(10)
    assert len(redelivered) == 1
    result = [sm.process(e) for e in redelivered][-1]

    assert result.status == "duplicate"
    assert [x.topic for x in out.events] == ["kafka://mfl.cube.requests"]  # once, not twice
    px.close()


@pytest.mark.ccr
def test_retention_replay_lands_one_cold_row():
    px = build_ccr()
    px.put("ccr_trade", a_trade("CCR-T-1"))
    wh = PhysicalWarehouse(dedup=True)  # Iceberg-like flat log
    worker = RetentionWorker(px.registry, wh)
    events = list(px.sink.events)

    worker.process(events); wh.flush()
    worker.process(events); wh.flush()  # replay the same batch (crash-recovery)

    rows = [r for r in wh.scan("warehouse.ccr_trades") if r["_doc_id"] == "CCR-T-1"]
    assert len(rows) == 1  # exactly one physical row despite the replay
    px.close()
