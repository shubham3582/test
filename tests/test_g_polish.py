"""Wave-4 polish guarantees.

F11 — the change-feed relay emits a document's versions in version order.
F12 — a late redelivery (dedup marker expired) is reject-safe: it is reprocessed
      against current state and rejected, never applied twice.
"""

from __future__ import annotations

import pytest

from phronexus import Settings
from phronexus.statemachine import InputEvent
from tests.harness import phronexus_with
from phronexus.kv.memory import InMemoryKV

TRADE = {"trade_id": "T-1", "counterparty": "GS", "notional": 1e6, "ccy": "USD",
         "trade_date": 20250115, "book": "R"}
BOOK = {**TRADE, "trade_id": "T-P"}


# --- F11 -------------------------------------------------------------------

@pytest.mark.chaos
def test_changefeed_relay_is_version_ordered_per_doc():
    s = Settings(backend="memory")
    s.observability.log_level = "WARNING"
    s.audit.enabled = False
    s.changefeed.inline_relay = False  # stage several versions, drain once
    px = phronexus_with(InMemoryKV(), settings=s)

    for cp in ("GS", "MS", "DB"):        # three versions of the same document
        px.put("trade", {**TRADE, "counterparty": cp})
    px.manifest.drain_changefeed()

    versions = [e.version for e in px.sink.events if e.doc_id == "T-1"]
    assert versions == sorted(versions), f"per-doc events must be version-ordered: {versions}"
    px.close()


# --- F12 -------------------------------------------------------------------

@pytest.mark.chaos
def test_late_redelivery_after_dedup_expiry_is_reject_safe():
    px = phronexus_with(InMemoryKV())
    sm = px.state_machine()
    dedup_set = px.settings.statemachine.dedup_set

    r1 = sm.process(InputEvent(entity="trade", event_type="TradeBooked",
                               key="T-P", payload=BOOK, event_id="e1"))
    assert r1.status == "applied"

    # Simulate the dedup marker's TTL expiring, then the same event redelivered.
    px.store.remove(dedup_set, "e1")
    r2 = sm.process(InputEvent(entity="trade", event_type="TradeBooked",
                               key="T-P", payload=BOOK, event_id="e1"))

    # Reprocessed against current state (already booked) -> rejected, not re-applied.
    assert r2.status == "rejected"
    assert px.get("trade", "T-P")["status"] == "booked"
    px.close()
