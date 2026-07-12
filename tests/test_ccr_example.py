"""Guards the CCR reference (examples/ccr) — saga, hot cube query, scheduler."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from phronexus import Phronexus, Settings
from phronexus.query.models import QueryDoc, SortKey
from phronexus.scheduler.engine import Scheduler
from phronexus.scheduler.models import ScheduleSpec
from phronexus.statemachine import InputEvent, MemoryOutputPublisher

CCR = Path(__file__).resolve().parent.parent / "examples" / "ccr"


def seed_reference_data(px: Phronexus) -> None:
    """Seed the reference entities the trade DQ resolves against."""
    px.put("counterparty", {"counterparty_id": "GS", "name": "Goldman Sachs",
                            "jurisdiction": "US", "rating": "A", "status": "active"})
    px.put("currency", {"code": "USD", "usd_rate": 1.0})
    px.put("netting_set", {"netting_set_id": "NS-GS-USD", "counterparty": "GS",
                           "csa_id": "CSA-GS-1", "status": "active"})


@pytest.fixture()
def ccr() -> Phronexus:
    settings = Settings(backend="memory")
    settings.observability.log_level = "WARNING"
    px = Phronexus(settings)
    px.load_contract_dir(str(CCR / "contracts"))
    seed_reference_data(px)
    yield px
    px.close()


TRADE = {
    "trade_id": "CCR-T-001", "counterparty": "GS", "netting_set_id": "NS-GS-USD",
    "book": "IRD-1", "product_type": "IRS", "notional": 25_000_000.0, "currency": "USD",
    "trade_date": 20260711, "maturity_date": 20360711,
}


def test_saga_drives_request_reply_pipeline(ccr):
    out = MemoryOutputPublisher()
    sm = ccr.state_machine(output=out)

    r1 = sm.process(InputEvent(entity="ccr_trade", event_type="TradeReceived",
                               key="CCR-T-001", payload=TRADE, event_id="e1"))
    r2 = sm.process(InputEvent(entity="ccr_trade", event_type="CubeReady",
                               key="CCR-T-001", payload={"as_of": 20260711}, event_id="e2"))
    r3 = sm.process(InputEvent(entity="ccr_trade", event_type="CalcComplete",
                               key="CCR-T-001", payload={"exposure": 3_100_000.0, "cva": 41_250.0},
                               event_id="e3"))

    assert [r.status for r in (r1, r2, r3)] == ["applied", "applied", "applied"]
    assert [r.to_state for r in (r1, r2, r3)] == ["cube_requested", "calc_requested", "published"]
    # Each step routed one request to the next service, in order.
    assert [e.topic for e in out.events] == [
        "kafka://mfl.cube.requests", "kafka://calc.requests", "kafka://ccr.display",
    ]
    assert ccr.get("ccr_trade", "CCR-T-001")["cva"] == 41_250.0


def test_redelivered_reply_does_not_double_request(ccr):
    out = MemoryOutputPublisher()
    sm = ccr.state_machine(output=out)
    sm.process(InputEvent(entity="ccr_trade", event_type="TradeReceived",
                          key="CCR-T-001", payload=TRADE, event_id="e1"))
    dup = sm.process(InputEvent(entity="ccr_trade", event_type="TradeReceived",
                                key="CCR-T-001", payload=TRADE, event_id="e1"))
    assert dup.status == "duplicate"
    assert len(out.events) == 1  # no second cube request


def test_guard_blocks_calc_without_cube(ccr):
    out = MemoryOutputPublisher()
    sm = ccr.state_machine(output=out)
    sm.process(InputEvent(entity="ccr_trade", event_type="TradeReceived",
                          key="CCR-T-001", payload=TRADE, event_id="e1"))
    # as_of == 0 fails the "as_of > 0" guard on CubeReady.
    r = sm.process(InputEvent(entity="ccr_trade", event_type="CubeReady",
                              key="CCR-T-001", payload={"as_of": 0}, event_id="e2"))
    assert r.status == "rejected"
    assert ccr.get("ccr_trade", "CCR-T-001")["status"] == "cube_requested"


def test_value_cube_sorted_clipped_hot_query(ccr):
    for sc in ("BASE", "STRESS_UP"):
        for t in (365, 1, 90, 30, 7):  # deliberately unsorted
            ccr.put("value_cube", {
                "trade_id": "CCR-T-001", "scenario_id": sc, "tenor": t,
                "value": 1000.0 + t, "currency": "USD", "as_of": 20260711,
            })
    page = ccr.query_page(QueryDoc(
        entity="value_cube",
        where=[{"field": "trade_id", "op": "eq", "value": "CCR-T-001"},
               {"field": "scenario_id", "op": "eq", "value": "BASE"}],
        sort=[SortKey(field="tenor", order="asc")],
        limit=3,
    ))
    assert [d["tenor"] for d in page["documents"]] == [1, 7, 30]  # sorted + clipped
    assert page["has_more"] is True  # 5 BASE points, only 3 returned


def test_eod_schedule_file_fires_exactly_once(ccr):
    spec = ScheduleSpec.model_validate(
        json.loads((CCR / "schedules" / "eod.json").read_text())
    )
    a = Scheduler(ccr, output=MemoryOutputPublisher())
    b = Scheduler(ccr, output=MemoryOutputPublisher())
    a.upsert_schedule(spec)

    from datetime import datetime
    from zoneinfo import ZoneInfo
    now = datetime(2026, 7, 11, 18, 30, 5, tzinfo=ZoneInfo("America/New_York")).timestamp()

    fired = a.tick(now=now) + b.tick(now=now)
    assert fired == ["ccr-eod"]  # exactly one replica won
    assert a.get_state("ccr-eod")["count"] == 1
