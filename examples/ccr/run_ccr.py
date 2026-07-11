"""Counterparty-Credit-Risk on Phronexus — the whole reference, end to end.

    python examples/ccr/run_ccr.py

Runs on the in-memory backend (no services required) and shows:

  1. A UDM trade received and validated, driving the CCR saga:
       TradeReceived -> RequestValueCube (to MFL)
       CubeReady     -> RequestCalc      (to the calculator)
       CalcComplete  -> DisplayUpdate    (to the display service)
     Each step persists state transactionally with the outbound request, so a
     redelivered reply never double-fires a downstream request.

  2. The future-value cube streamed into Aerospike (here, the memory backend)
     and read back with a sorted, clipped hot query.

  3. A distributed scheduler firing the EOD job exactly once across two
     replicas — the state lives in the store, so duplicate events cannot be
     generated no matter how many schedulers run.

There is no CCR-specific Python in the framework: the saga, the schemas, and the
hot-query shapes are all the config under examples/ccr/.
"""

from __future__ import annotations

import json
from pathlib import Path

from phronexus import Phronexus, Settings
from phronexus.query.models import QueryDoc, SortKey
from phronexus.scheduler.engine import Scheduler
from phronexus.scheduler.models import ScheduleSpec
from phronexus.statemachine import InputEvent, MemoryOutputPublisher

HERE = Path(__file__).parent


def banner(text: str) -> None:
    print(f"\n=== {text} ===")


def show_emitted(out: MemoryOutputPublisher, since: int) -> int:
    for ev in out.events[since:]:
        print(f"    -> {ev.topic:26} {ev.type:16} trade={ev.payload.get('trade_id')}")
    return len(out.events)


def main() -> None:
    settings = Settings(backend="memory")
    settings.observability.log_level = "ERROR"
    px = Phronexus(settings)
    px.load_contract_dir(str(HERE / "contracts"))

    out = MemoryOutputPublisher()
    sm = px.state_machine(output=out)
    seen = 0

    # --- 1) trade received -> drive the saga --------------------------------
    banner("1) UDM trade received")
    trade = {
        "trade_id": "CCR-T-001", "counterparty": "GS", "book": "IRD-1",
        "product_type": "IRS", "notional": 25_000_000.0, "currency": "USD",
        "trade_date": 20260711, "maturity_date": 20360711,
    }
    print("validate:", px.validate("ccr_trade", trade).ok)

    r = sm.process(InputEvent(entity="ccr_trade", event_type="TradeReceived",
                              key=trade["trade_id"], payload=trade, event_id="evt-recv-1"))
    print(f"TradeReceived -> {r.status} ({r.from_state} -> {r.to_state})")
    seen = show_emitted(out, seen)

    # Redelivery of the same event must be a no-op (effectively-once).
    dup = sm.process(InputEvent(entity="ccr_trade", event_type="TradeReceived",
                                key=trade["trade_id"], payload=trade, event_id="evt-recv-1"))
    print("redelivered TradeReceived ->", dup.status, "(no second cube request)")

    # --- 2) MFL streams the value cube into the hot store -------------------
    banner("2) MFL streams the future-value cube into Aerospike")
    scenarios = ["BASE", "STRESS_UP", "STRESS_DN"]
    tenors = [1, 7, 30, 90, 180, 365, 730, 1825]  # days
    n = 0
    for sc in scenarios:
        for t in tenors:
            px.put("value_cube", {
                "trade_id": trade["trade_id"], "scenario_id": sc, "tenor": t,
                "value": round(25_000_000.0 * (1 + t / 3650) * (1.1 if sc == "STRESS_UP" else 0.9 if sc == "STRESS_DN" else 1.0), 2),
                "currency": "USD", "as_of": 20260711,
            })
            n += 1
    print(f"landed {n} cube points for {trade['trade_id']}")

    # Hot read: BASE scenario, sorted by tenor, clipped to the nearest 4 points.
    page = px.query_page(QueryDoc(
        entity="value_cube",
        where=[{"field": "trade_id", "op": "eq", "value": trade["trade_id"]},
               {"field": "scenario_id", "op": "eq", "value": "BASE"}],
        sort=[SortKey(field="tenor", order="asc")],
        limit=4,
    ))
    print(f"nearest {page['count']} BASE points (sorted by tenor, clipped):")
    for pt in page["documents"]:
        print(f"    tenor={pt['tenor']:>4}d  value={pt['value']:,}")
    print("more tenors available:", page["has_more"])

    # --- 3) MFL replies: cube ready -> request calc -------------------------
    banner("3) MFL reply -> request risk calculation")
    r = sm.process(InputEvent(entity="ccr_trade", event_type="CubeReady",
                              key=trade["trade_id"],
                              payload={"as_of": 20260711, "cube_points": n},
                              event_id="evt-cube-1"))
    print(f"CubeReady -> {r.status} ({r.from_state} -> {r.to_state})")
    seen = show_emitted(out, seen)

    # --- 4) calculator replies: calc complete -> publish to display ---------
    banner("4) calculator reply -> publish to display")
    r = sm.process(InputEvent(entity="ccr_trade", event_type="CalcComplete",
                              key=trade["trade_id"],
                              payload={"exposure": 3_100_000.0, "cva": 41_250.0, "pfe": 5_800_000.0},
                              event_id="evt-calc-1"))
    print(f"CalcComplete -> {r.status} ({r.from_state} -> {r.to_state})")
    seen = show_emitted(out, seen)

    final = px.get("ccr_trade", trade["trade_id"])
    print("final saga state:", final["status"], "| cva:", final.get("cva"))

    # --- 5) EOD scheduler: exactly-once across replicas ---------------------
    banner("5) EOD scheduler — exactly-once across two replicas")
    spec = ScheduleSpec.model_validate(json.loads((HERE / "schedules" / "eod.json").read_text()))
    # Two scheduler replicas sharing the same store (as two pods would).
    out_a, out_b = MemoryOutputPublisher(), MemoryOutputPublisher()
    replica_a = Scheduler(px, output=out_a)
    replica_b = Scheduler(px, output=out_b)
    replica_a.upsert_schedule(spec)

    # Force "now" just past today's 18:30 NY so the occurrence is due.
    from datetime import datetime
    from zoneinfo import ZoneInfo
    now = datetime(2026, 7, 11, 18, 30, 5, tzinfo=ZoneInfo("America/New_York")).timestamp()

    fired_a = replica_a.tick(now=now)
    fired_b = replica_b.tick(now=now)     # same occurrence — must lose the race
    total = len(out_a.events) + len(out_b.events)
    print(f"replica A fired: {fired_a} | replica B fired: {fired_b}")
    print(f"total EOD triggers published: {total} (exactly once)")
    print("EOD state:", replica_a.get_state("ccr-eod"))

    px.close()


if __name__ == "__main__":
    main()
