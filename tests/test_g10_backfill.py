"""G10 — backfill resumes from its checkpoint after a crash, idempotently.

A crash mid-backfill must not force a full rescan or re-project already-done
documents: the persisted cursor lets a resume pick up exactly where it stopped.
"""

from __future__ import annotations

import pytest

from phronexus.admin import BackfillJob
from phronexus.kv.memory import InMemoryKV
from tests.harness import phronexus_with

TRADE = {"trade_id": "T", "counterparty": "GS", "notional": 1e6, "ccy": "USD",
         "trade_date": 20250115, "book": "R"}


@pytest.mark.chaos
def test_backfill_resumes_from_checkpoint_after_crash():
    px = phronexus_with(InMemoryKV())
    ids = [f"T-{i}" for i in range(5)]
    for tid in ids:
        px.put("trade", {**TRADE, "trade_id": tid})

    job = BackfillJob(px)

    # First run crashes after re-projecting 2 documents.
    orig, calls = px.put, {"n": 0}
    def crash_put(entity, doc):
        calls["n"] += 1
        if calls["n"] == 3:
            raise RuntimeError("crash mid-backfill")
        return orig(entity, doc)
    px.put = crash_put
    with pytest.raises(RuntimeError):
        job.run("trade")
    px.put = orig

    # Resume: only the remaining 3 are processed (not all 5, no rescan-from-top).
    assert job.run("trade") == 3

    # A subsequent run (previous one completed) re-projects everything again.
    assert job.run("trade") == 5
    px.close()
