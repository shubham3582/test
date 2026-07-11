from __future__ import annotations

from phronexus import Phronexus, Settings

TRADE = {"trade_id": "T-1", "counterparty": "GS", "notional": 1e6, "ccy": "USD", "trade_date": 20250115, "book": "R"}


def _px(inline: bool) -> Phronexus:
    s = Settings(backend="memory")
    s.observability.log_level = "ERROR"
    s.changefeed.inline_relay = inline
    p = Phronexus(s)
    p.load_contract_dir("contracts_examples")
    return p


def _cf_set(px):
    return px.settings.aerospike.changefeed_outbox_set


def test_changefeed_event_durably_staged_then_relayed():
    px = _px(inline=False)                       # standalone relay mode
    px.put("trade", TRADE)
    assert px.sink.events == []                   # not relayed yet
    assert list(px.store.scan(_cf_set(px)))       # durably queued in the outbox
    n = px.manifest.drain_changefeed()            # the relay
    assert n == 1 and px.sink.events[-1].op == "upsert"
    assert list(px.store.scan(_cf_set(px))) == []
    px.close()


def test_changefeed_survives_crash_after_commit(monkeypatch):
    px = _px(inline=True)
    # Crash window: post-commit side effects never run.
    monkeypatch.setattr(px.manifest, "post_write", lambda *a, **k: None)
    px.put("trade", TRADE)
    assert px.get("trade", "T-1") == TRADE        # committed
    assert px.sink.events == []                    # relay never ran
    # The event was staged inside the write txn, so it's recoverable — not lost.
    assert px.manifest.drain_changefeed() == 1
    assert px.sink.events[-1].doc_id == "T-1"
    px.close()


def test_changefeed_delete_event():
    px = _px(inline=True)
    px.put("trade", TRADE)
    px.delete("trade", "T-1")
    ops = [e.op for e in px.sink.events]
    assert ops == ["upsert", "delete"]
    px.close()
