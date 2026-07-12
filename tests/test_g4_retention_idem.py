"""G4 — replaying the same txn never creates a duplicate PHYSICAL row.

A crash after an Iceberg flush but before the Kafka offset commit causes the
event to be redelivered in a *later* batch. Deduping only within one flush
buffer is not enough; the flush must anti-join on (_doc_id,_txn) against rows
already in the table.
"""

from __future__ import annotations

import pytest

from phronexus.retention.warehouse import dedup_new, reconcile
from tests.harness.warehouses import PhysicalWarehouse

ROW = {"_doc_id": "d1", "_txn": "t1", "_version": 1, "_op": "upsert", "counterparty": "GS"}


def test_dedup_new_filters_existing_keys():
    existing = {("d1", "t1")}
    assert dedup_new([ROW], existing) == []                    # already present
    assert dedup_new([{**ROW, "_txn": "t2"}], existing) == [{**ROW, "_txn": "t2"}]
    # also dedups within the same call
    two = [ROW, dict(ROW)]
    assert len(dedup_new(two, set())) == 1


@pytest.mark.chaos
def test_cross_batch_replay_no_physical_duplicate():
    # Pre-fix behaviour (raw append) DOES duplicate across batches — the bug.
    buggy = PhysicalWarehouse(dedup=False)
    buggy.append("trades", "d1:t1", ROW); buggy.flush()
    buggy.append("trades", "d1:t1", ROW); buggy.flush()  # redelivered in a later batch
    assert len(buggy.scan("trades")) == 2

    # Fixed behaviour: anti-join on (_doc_id,_txn) -> exactly one physical row.
    fixed = PhysicalWarehouse(dedup=True)
    fixed.append("trades", "d1:t1", ROW); fixed.flush()
    fixed.append("trades", "d1:t1", ROW); fixed.flush()
    assert len(fixed.scan("trades")) == 1
    # ... and the reconciled view is still correct.
    state = reconcile(fixed.scan("trades"))
    assert len(state) == 1 and state[0]["counterparty"] == "GS"
