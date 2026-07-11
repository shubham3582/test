"""Retention compaction: dedup, tombstone drop, expiry, and version collapse."""

from __future__ import annotations

from phronexus.retention import InMemoryWarehouse, MemoryEventSource, RetentionWorker


def _trades(stats):
    return next(s for s in stats if s["table"] == "warehouse.trades")

TRADE = {"trade_id": "T-1", "counterparty": "GS", "notional": 1e6, "ccy": "USD",
         "trade_date": 20250115, "book": "R"}


def _worker(px):
    return RetentionWorker(px.registry, InMemoryWarehouse()), MemoryEventSource(px.sink)


def test_compaction_keeps_history_but_drops_tombstoned_docs(px):
    px.put("trade", TRADE)
    px.put("trade", {**TRADE, "counterparty": "MS"})   # amend -> 2 versions
    px.put("trade", {**TRADE, "trade_id": "T-2"})
    px.delete("trade", "T-2")                           # tombstone T-2
    worker, src = _worker(px)
    worker.run(src)
    wh = worker._warehouse

    # Log holds every event: T-1 (v1, v2), T-2 (upsert, delete) = 4 rows.
    assert wh.count("warehouse.trades") == 4

    stats = _trades(worker.compact_all())
    # T-2 (tombstoned) is gone entirely; T-1's two versions are kept (history).
    assert stats["before"] == 4
    rows = wh.scan("warehouse.trades")
    assert {r["_doc_id"] for r in rows} == {"T-1"}
    assert len(rows) == 2                               # both T-1 versions survive
    # Current state unchanged by compaction.
    state = wh.latest_state("warehouse.trades")
    assert len(state) == 1 and state[0]["counterparty"] == "MS"


def test_compaction_collapse_keeps_only_latest_version(px):
    px.put("trade", TRADE)
    px.put("trade", {**TRADE, "counterparty": "MS"})
    px.put("trade", {**TRADE, "counterparty": "BARC"})
    worker, src = _worker(px)
    worker.run(src)
    wh = worker._warehouse
    assert wh.count("warehouse.trades") == 3

    worker.compact_all(keep_history=False)
    rows = wh.scan("warehouse.trades")
    assert len(rows) == 1 and rows[0]["counterparty"] == "BARC"   # only the latest


def test_compaction_is_idempotent(px):
    px.put("trade", TRADE)
    px.put("trade", {**TRADE, "counterparty": "MS"})
    worker, src = _worker(px)
    worker.run(src)
    a = _trades(worker.compact_all(keep_history=False))["after"]
    b = _trades(worker.compact_all(keep_history=False))
    assert b["before"] == a and b["removed"] == 0        # second run is a no-op


def test_compaction_drops_expired_rows(px):
    px.put("trade", TRADE)
    worker, src = _worker(px)
    worker.run(src)
    wh = worker._warehouse
    exp = wh.scan("warehouse.trades")[0]["_expire_at"]
    stats = _trades(worker.compact_all(now=exp + 1))
    assert stats["removed"] == 1 and wh.count("warehouse.trades") == 0
