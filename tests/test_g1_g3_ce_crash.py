"""G1/G3 — under a non-transactional (Aerospike CE) crash, a write that did NOT
commit must never produce a downstream event, and a committed one always does.

SequentialKV models CE (each op applies immediately, no rollback), so a crash
right before the manifest commit point leaves a torn write: the change-feed
outbox row exists, but no manifest. The relay must not emit a phantom event for
it, and the orphan row must be reaped after the grace period. The same holds for
state-machine output events.
"""

from __future__ import annotations

import pytest

from phronexus import Settings
from phronexus.statemachine import InputEvent
from phronexus.statemachine.io import MemoryOutputPublisher
from tests.harness import FaultKV, FaultRule, SequentialKV, SimulatedCrash, phronexus_with

TRADE = {"trade_id": "T-CE", "counterparty": "GS", "notional": 1e6, "ccy": "USD",
         "trade_date": 20250115, "book": "R"}


def _ce_settings():
    s = Settings(backend="memory")
    s.observability.log_level = "WARNING"
    s.audit.enabled = False                 # keep px.sink a plain MemorySink
    s.changefeed.verify_commit = True       # CE deployments enable this
    return s


@pytest.mark.chaos
def test_uncommitted_write_produces_no_phantom_change_event():
    fault = FaultKV(SequentialKV())
    px = phronexus_with(fault, settings=_ce_settings())
    sc = px.registry.active_storage("trade")
    outbox_set = px.settings.aerospike.changefeed_outbox_set

    fault.arm(FaultRule("put", set_name=sc.manifest_set))  # crash before manifest
    with pytest.raises(SimulatedCrash):
        px.put("trade", TRADE)

    # A torn write left an outbox row, but the doc never committed.
    assert list(px.store.scan(outbox_set)), "torn write should stage an outbox row"
    before = len(px.sink.events)
    assert px.manifest.drain_changefeed() == 0          # no phantom relayed
    assert len(px.sink.events) == before
    assert list(px.store.scan(outbox_set)), "kept within grace (may yet commit)"

    # Past the grace period the orphan outbox row is reaped.
    px.manifest.drain_changefeed(now=1e12)
    assert list(px.store.scan(outbox_set)) == []
    px.close()


@pytest.mark.chaos
def test_committed_write_still_delivers_its_event_under_verify_commit():
    px = phronexus_with(SequentialKV(), settings=_ce_settings())
    px.put("trade", TRADE)  # inline relay drains on commit
    # exactly one event for the committed doc reached the sink
    assert [e.doc_id for e in px.sink.events] == ["T-CE"]
    px.close()


@pytest.mark.chaos
def test_uncommitted_transition_emits_no_phantom_output():
    s = Settings(backend="memory")
    s.observability.log_level = "WARNING"
    s.statemachine.require_atomic = False  # CE best-effort: opt into non-atomic
    fault = FaultKV(SequentialKV())
    px = phronexus_with(fault, settings=s)
    out = MemoryOutputPublisher()
    sm = px.state_machine(output=out)
    sc = px.registry.active_storage("trade")

    fault.arm(FaultRule("put", set_name=sc.manifest_set))  # crash before manifest
    with pytest.raises(SimulatedCrash):
        sm.process(InputEvent(entity="trade", event_type="TradeBooked",
                              key="T-CE", payload=TRADE, event_id="e1"))

    # Outputs were staged before the manifest, so they exist — but the transition
    # never committed (no dedup marker), so the relay must not emit them.
    assert sm.drain_outbox() == 0
    assert out.events == []
    assert px.get("trade", "T-CE") is None
    px.close()
