"""GOV7/GOV10 — backfill control + fleet visibility.

- GOV7: pause/resume/cancel are honored cooperatively; status is accurate.
- GOV10: the fleet view reports active version per entity, drift, COB, backfill.
"""

from __future__ import annotations

import pytest

from phronexus import Phronexus, Settings
from phronexus.admin import BackfillJob

TRADE = {"trade_id": "T", "counterparty": "GS", "notional": 1e6, "ccy": "USD",
         "trade_date": 20250115, "book": "R"}


def _px():
    s = Settings(backend="memory")
    s.observability.log_level = "WARNING"
    px = Phronexus(s)
    px.load_contract_dir("contracts_examples")
    return px


def _storage(entity, version=1):
    return {"kind": "storage", "entity": entity, "version": version, "primary_key": ["id"],
            "manifest_set": f"{entity}_manifest",
            "projections": [{"name": "main", "set": f"{entity}_main", "key": "{id}",
                             "fields": ["*"], "canonical": True}]}


# --- GOV7 -----------------------------------------------------------------

@pytest.mark.governance
def test_backfill_pause_then_resume_to_completion():
    px = _px()
    gov = px.governance
    for i in range(6):
        px.put("trade", {**TRADE, "trade_id": f"T-{i}"})
    job = BackfillJob(px)

    # Request a pause mid-run (during the 2nd re-projection).
    orig, n = px.put, {"c": 0}
    def wrapped(entity, doc):
        n["c"] += 1
        if n["c"] == 2:
            gov.control_backfill("ops", "trade", "pause")
        return orig(entity, doc)
    px.put = wrapped

    done = job.run("trade")
    px.put = orig
    assert done == 2  # stopped cooperatively at the checkpoint
    assert gov.backfill_status()["trade"]["state"] == "paused"

    # Resume: a re-run continues from the checkpoint to completion.
    gov.control_backfill("ops", "trade", "resume")
    assert job.run("trade") == 4  # the remaining docs
    assert gov.backfill_status()["trade"]["state"] == "completed"
    assert any(e["action"] == "backfill.pause" for e in gov.log.entries())


@pytest.mark.governance
def test_backfill_cancel_requested_before_start():
    px = _px()
    gov = px.governance
    for i in range(4):
        px.put("trade", {**TRADE, "trade_id": f"C-{i}"})
    gov.control_backfill("ops", "trade", "cancel")
    assert BackfillJob(px).run("trade") == 0
    assert gov.backfill_status()["trade"]["state"] == "cancelled"


# --- GOV10 ----------------------------------------------------------------

@pytest.mark.governance
def test_fleet_reports_active_cob_and_backfill():
    px = _px()
    gov = px.governance
    px.publish_contract(_storage("wid", 1))
    gov.set_cob("ops", 20250115)

    f = gov.fleet()
    assert f["environment"] == "dev"
    assert f["cob"] == 20250115
    assert f["active"].get("active:storage:wid") == "storage:wid:v1"
    assert f["history"]["ok"] is True
    assert "cache_age_seconds" in f

    for i in range(2):
        px.put("trade", {**TRADE, "trade_id": f"F-{i}"})
    BackfillJob(px).run("trade")
    assert gov.fleet()["backfills"]["trade"]["state"] == "completed"
