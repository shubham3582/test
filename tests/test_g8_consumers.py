"""G8 — consumers never lose a message and never crash the process.

- The consume loop survives a transient broker/store error (backoff + retry).
- Offsets are committed only AFTER a batch is processed; a mid-batch failure or
  a rebalance replays uncommitted messages (at-least-once, no loss).
"""

from __future__ import annotations

import threading

import pytest

from phronexus.kv.memory import InMemoryKV
from phronexus.statemachine import InputEvent
from phronexus.statemachine.runner import run, serve
from tests.harness import phronexus_with
from tests.harness.broker import Broker, BrokerInputSource

BOOK = {"trade_id": "T", "counterparty": "GS", "notional": 1e6, "ccy": "USD",
        "trade_date": 20250115, "book": "R"}


def _ev(i):
    return InputEvent(entity="trade", event_type="TradeBooked", key=f"T-{i}",
                      payload={**BOOK, "trade_id": f"T-{i}"}, event_id=f"e{i}")


# --- F8: loop resilience ---------------------------------------------------

@pytest.mark.chaos
def test_runner_loop_survives_transient_errors():
    stop = threading.Event()
    px = phronexus_with(InMemoryKV())
    machine = px.state_machine()

    class FlakySource:
        def __init__(self):
            self.calls = 0

        def poll(self, n):
            self.calls += 1
            if self.calls <= 2:
                raise RuntimeError("broker down")   # transient outage
            if self.calls == 3:
                return [_ev(0)]
            stop.set()
            return []

        def commit(self):
            pass

    serve(px, FlakySource(), machine, stop, initial_backoff=0.001, idle=0.001)
    # despite two consecutive poll failures, the event was processed (no crash)
    assert px.get("trade", "T-0")["status"] == "booked"
    px.close()


# --- F10: commit-after-processing / redelivery / rebalance -----------------

@pytest.mark.chaos
def test_uncommitted_batch_is_redelivered_committed_is_not():
    broker = Broker()
    for i in range(3):
        broker.produce("trade", _ev(i), key=f"T-{i}")

    # poll without committing == a crash before commit
    s1 = BrokerInputSource(broker, "g1", ["trade"])
    assert len(s1.poll(10)) == 3

    # a fresh consumer in the same group re-reads from the last commit (0)
    s2 = BrokerInputSource(broker, "g1", ["trade"])
    assert len(s2.poll(10)) == 3
    s2.commit()

    # after commit, nothing is redelivered
    s3 = BrokerInputSource(broker, "g1", ["trade"])
    assert s3.poll(10) == []


@pytest.mark.chaos
def test_rebalance_midbatch_redelivers_uncommitted():
    broker = Broker()
    for i in range(3):
        broker.produce("trade", _ev(i), key=f"T-{i}")
    c = BrokerInputSource(broker, "g1", ["trade"])
    assert len(c.poll(10)) == 3          # fetched, not committed
    c.revoke()                           # rebalance mid-batch
    assert len(c.poll(10)) == 3          # uncommitted work redelivered


@pytest.mark.chaos
def test_run_commits_only_after_full_batch_processed():
    broker = Broker()
    for i in range(3):
        broker.produce("trade", _ev(i), key=f"T-{i}")
    src = BrokerInputSource(broker, "g1", ["trade"])
    px = phronexus_with(InMemoryKV())
    machine = px.state_machine()

    # fail on the 2nd event so the batch does not complete
    orig, calls = machine.process, {"n": 0}
    def boom(ev):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("kaboom")
        return orig(ev)
    machine.process = boom

    with pytest.raises(RuntimeError):
        run(px, src, machine, max_batches=1)

    # offset was NOT committed -> the whole batch is redelivered (no loss)
    assert len(BrokerInputSource(broker, "g1", ["trade"]).poll(10)) == 3
    px.close()
