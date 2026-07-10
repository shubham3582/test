from __future__ import annotations

from phronexus.retention import InMemoryWarehouse, MemoryEventSource, RetentionWorker

TRADE = {"trade_id": "T-1", "counterparty": "GS", "notional": 1e6, "ccy": "USD", "trade_date": 20250115, "book": "R"}


def _worker(px):
    return RetentionWorker(px.registry, InMemoryWarehouse()), MemoryEventSource(px.sink)


def test_upsert_lands_in_iceberg_table(px):
    px.put("trade", TRADE)
    worker, src = _worker(px)
    stats = worker.run(src)
    assert stats["upserts"] == 1
    rows = worker._warehouse.scan("warehouse.trades")
    assert len(rows) == 1 and rows[0]["_doc_id"] == "T-1" and rows[0]["counterparty"] == "GS"


def test_disabled_entity_is_skipped(px):
    # fx_spot has iceberg.enabled = false
    px.put("fx_spot", {"deal_id": "F1", "pair": "EURUSD", "notional": 1e6, "rate": 1.08, "trade_date": 20250115})
    worker, src = _worker(px)
    stats = worker.run(src)
    assert stats["upserts"] == 0 and stats["skipped"] == 1


def test_delete_removes_row(px):
    px.put("trade", TRADE)
    worker, src = _worker(px)
    worker.run(src)
    px.delete("trade", "T-1")
    worker.run(MemoryEventSource(px.sink))
    assert worker._warehouse.count("warehouse.trades") == 0
    assert worker.stats["deletes"] == 1


def test_replays_are_idempotent(px):
    px.put("trade", TRADE)
    worker = RetentionWorker(px.registry, InMemoryWarehouse())
    events = list(px.sink.events)
    worker.process(events)
    worker.process(events)  # replay same batch
    assert worker._warehouse.count("warehouse.trades") == 1


def test_retention_expiry(px):
    px.put("trade", TRADE)
    worker, src = _worker(px)
    worker.run(src)
    row = worker._warehouse.scan("warehouse.trades")[0]
    # trade contract retains 3650 days; expire far in the future removes it.
    assert row["_expire_at"] is not None
    removed = worker.expire_all(now=row["_expire_at"] + 1)
    assert removed == 1 and worker._warehouse.count("warehouse.trades") == 0
