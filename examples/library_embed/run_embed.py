"""Phronexus as a library — the WHOLE API embedded in another app (e.g. DishtaYantra).

    python examples/library_embed/run_embed.py                              # in-memory + in-process Iceberg
    PHRONEXUS_BACKEND=aerospike python examples/library_embed/run_embed.py   # real Aerospike

Runs in-process with no services, yet exercises the full surface an embedding app
uses — operational store (the Aerospike face), the state machine as a calculator
node with a request/response journal, a query/enrichment calculator, and
retention to Iceberg driven off the change feed. Point Settings at a real cluster
and Iceberg catalog and the same calls talk to Aerospike + Iceberg/S3.
"""

from __future__ import annotations

import os
from pathlib import Path

from phronexus import Phronexus, Settings
from phronexus.retention.source import MemoryEventSource
from phronexus.retention.warehouse import build_warehouse
from phronexus.retention.worker import RetentionWorker
from phronexus.statemachine.node import PhronexusQueryCalculator, PhronexusStateMachineNode

ROOT = Path(__file__).resolve().parents[2]


def banner(t: str) -> None:
    print(f"\n=== {t} ===")


def main() -> None:
    settings = Settings(backend=os.environ.get("PHRONEXUS_BACKEND", "memory"))
    settings.observability.log_level = "ERROR"
    settings.journal.enabled = True           # turn on the request/response journal
    settings.journal.journal_requests = True
    px = Phronexus(settings)
    px.load_contract_dir(str(ROOT / "contracts_examples"))

    # 1) operational store — the Aerospike face, as plain library calls -------
    banner("1) operational store: put / get / query / find")
    px.put("trade", {"trade_id": "T-1", "counterparty": "CP-GS", "notional": 1_000_000, "ccy": "USD", "trade_date": 20260711})
    px.put("trade", {"trade_id": "T-2", "counterparty": "CP-GS", "notional": 2_000_000, "ccy": "USD", "trade_date": 20260712})
    print("get T-1:", px.get("trade", "T-1"))
    print("find CP-GS:", [t["trade_id"] for t in px.find("trade", "counterparty", "CP-GS")])

    # 2) the state machine as a DishtaYantra calculator node + journal --------
    banner("2) state-machine calculator node + request/response journal")
    node = PhronexusStateMachineNode(px.state_machine())   # journaled (enabled above)
    dag_record = {
        "entity": "trade", "event_type": "TradeBooked", "key": "T-9", "event_id": "evt-9",
        "payload": {"trade_id": "T-9", "counterparty": "CP-JPM", "notional": 5_000_000, "ccy": "EUR", "trade_date": 20260711},
    }
    print("node.calculate ->", node.calculate(dag_record))
    j = px.request_journal().read("evt-9")            # the stored request + response
    print("journal[evt-9] request :", j["request"]["event_type"], "->", j["meta"]["status"])
    print("journal[evt-9] response:", j["response"]["to_state"],
          "emitted:", [e["topic"] for e in j["response"]["emitted_events"]])

    # 3) a query/enrichment calculator node -----------------------------------
    banner("3) query calculator node: enrich a DAG record with store data")
    enrich = PhronexusQueryCalculator(px, "trade", key_from="trade_id", mode="enrich")
    print("enriched:", enrich.calculate({"trade_id": "T-1", "source": "upstream-node"}))

    # 4) retention to Iceberg — driven in-process off the change feed ---------
    banner("4) retention → Iceberg (in-process warehouse) off the change feed")
    if settings.kafka.enabled:
        # With Kafka on, the change feed goes to the broker — land Iceberg by
        # running the retention worker (`python -m phronexus.retention.main`),
        # which consumes it via KafkaEventSource. Same worker, different source.
        print("Kafka enabled → change feed on the broker; run phronexus.retention.main to land Iceberg")
    else:
        warehouse = build_warehouse(settings.iceberg)  # InMemoryWarehouse, or real Iceberg
        RetentionWorker(px.registry, warehouse).run(MemoryEventSource(px.sink))
        rows = warehouse.scan("warehouse.trades")
        print(f"landed {len(rows)} trade rows in Iceberg table warehouse.trades:",
              sorted(r.get("_doc_id") for r in rows))

    banner("done")
    print("operational (Aerospike) + retention (Iceberg) + state machine + journals — "
          "all via library calls, no services.")
    px.close()


if __name__ == "__main__":
    main()
