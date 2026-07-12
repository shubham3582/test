"""Counterparty-Credit-Risk — production-depth end-to-end scenario.

Proves Phronexus can reliably GOVERN and SERVE the operational data around a risk
engine (it computes no risk — the quant engine owns that). One narrated run walks
the ten Stage-3 proofs:

  CCR1 onboarding      CCR2 evolution        CCR3 netting-set lifecycle
  CCR4 intraday recalc CCR5 cube projection  CCR6 late/corrected events
  CCR7 COB reproducibility  CCR8 hot/cold tiering  CCR9 lineage  CCR10 recovery

    python examples/ccr/run_ccr.py                              # in-memory
    PHRONEXUS_BACKEND=aerospike python examples/ccr/run_ccr.py  # live stack
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import ccr_ops  # noqa: E402

from phronexus import Phronexus, Settings  # noqa: E402
from phronexus.admin import BackfillJob  # noqa: E402
from phronexus.query.models import QueryDoc, SortKey  # noqa: E402
from phronexus.retention import (  # noqa: E402
    InMemoryWarehouse, MemoryEventSource, RetentionWorker, decode,
)
from phronexus.statemachine import InputEvent, MemoryOutputPublisher  # noqa: E402

COB = 20260711


def h(title):
    print("\n" + "=" * 74 + f"\n{title}\n" + "=" * 74)


def ev(entity, etype, key, payload, eid):
    return InputEvent(entity=entity, event_type=etype, key=key, payload=payload, event_id=eid)


def main() -> None:
    s = Settings()
    s.observability.log_level = "ERROR"
    s.journal.enabled = True
    s.journal.journal_requests = True
    s.governance.approval_policy = {"*": {"*": 1}}
    px = Phronexus(s)
    px.load_contract_dir(str(HERE / "contracts"))
    px.governance.set_cob("ops", COB)
    out = MemoryOutputPublisher()
    sm = px.state_machine(output=out)
    print(f"backend={s.backend}  COB={COB}")

    # reference data
    px.put("currency", {"code": "USD", "usd_rate": 1.0})
    px.put("currency", {"code": "EUR", "usd_rate": 1.08})
    px.put("netting_set", {"netting_set_id": "NS-GS-USD", "counterparty": "GS",
                           "csa_id": "CSA-GS-1", "status": "active"})

    trade = {"trade_id": "CCR-T-1", "counterparty": "GS", "netting_set_id": "NS-GS-USD",
             "book": "IRD-1", "product_type": "IRS", "notional": 25_000_000.0,
             "currency": "USD", "trade_date": 20260711, "maturity_date": 20360711}

    # --- CCR1 ---------------------------------------------------------------
    h("CCR1  Onboarding (validation + reference DQ + lifecycle saga)")
    print("counterparty GS ->", ccr_ops.onboard_counterparty(sm, {
        "counterparty_id": "GS", "name": "Goldman Sachs", "jurisdiction": "US", "rating": "A"}))
    r = sm.process(ev("ccr_trade", "TradeReceived", "CCR-T-1", trade, "e1"))
    print(f"trade CCR-T-1 -> {r.status} ({r.to_state}); routed {[e.topic for e in out.events]}")
    bad = sm.process(ev("ccr_trade", "TradeReceived", "CCR-T-X",
                        {**trade, "trade_id": "CCR-T-X", "counterparty": "XX"}, "eX"))
    print(f"unknown-counterparty trade -> {bad.status}: {bad.reason}")

    # --- CCR3 ---------------------------------------------------------------
    h("CCR3  Netting-set lifecycle + membership + served aggregation")
    sm.process(ev("netting_set", "NettingSetOpened", "NS-GS-USD",
                  {"netting_set_id": "NS-GS-USD", "counterparty": "GS", "csa_id": "CSA-GS-1"}, "ns0"))
    sm.process(ev("netting_set", "CsaConfirmed", "NS-GS-USD", {"netting_set_id": "NS-GS-USD"}, "ns1"))
    px.put("ccr_trade", {**trade, "trade_id": "CCR-T-2"})
    for tid, exp in (("CCR-T-1", 3_100_000.0), ("CCR-T-2", 1_900_000.0)):
        px.put("exposure_result", {"trade_id": tid, "cob": COB, "exposure": exp,
                                   "currency": "USD", "netting_set_id": "NS-GS-USD", "source_event_id": "e1"})
    members = [t["trade_id"] for t in px.find("ccr_trade", "netting_set_id", "NS-GS-USD")]
    agg = ccr_ops.aggregate_netting_set(px, "NS-GS-USD", COB, source_event_id="e1")
    print(f"members via idx_ns: {sorted(members)}")
    print(f"served NS exposure: {agg['exposure']:,.0f} across {agg['trade_count']} trades")

    # --- CCR5 ---------------------------------------------------------------
    h("CCR5  Cube projection — point-per-tenor (hot) + transposed per-date bins")
    for tenor in (365, 1, 90, 30, 7):
        px.put("value_cube", {"trade_id": "CCR-T-1", "scenario_id": "BASE", "tenor": tenor,
                              "value": 1000.0 + tenor, "currency": "USD", "as_of": COB})
    page = px.query_page(QueryDoc(entity="value_cube",
        where=[{"field": "trade_id", "op": "eq", "value": "CCR-T-1"},
               {"field": "scenario_id", "op": "eq", "value": "BASE"}],
        sort=[SortKey(field="tenor", order="asc")], limit=3))
    print(f"hot sorted/clipped -> tenors {[d['tenor'] for d in page['documents']]} has_more={page['has_more']}")
    px.put("fvcube", {"trade_id": "CCR-T-1", "scenario_id": "BASE", "as_of": COB,
                      "currency": "USD", "curve": {"20260712": 1.00, "20260718": 1.05}})
    wide = px.store.get("fvc_wide", "CCR-T-1:BASE")
    print(f"transposed date bins: {[k for k in wide.bins if k.startswith('d')]}")

    # --- CCR4 ---------------------------------------------------------------
    h("CCR4  Intraday recalculation (new exposure version, same COB, later tx)")
    sm.process(ev("ccr_trade", "CubeReady", "CCR-T-1", {"as_of": COB}, "e2"))
    sm.process(ev("ccr_trade", "CalcComplete", "CCR-T-1", {"exposure": 3_100_000.0, "cva": 41_250.0}, "e3"))
    t_morning = time.time()
    time.sleep(0.01)
    sm.process(ev("ccr_trade", "RecalcRequested", "CCR-T-1", {}, "e4"))
    sm.process(ev("ccr_trade", "CubeReady", "CCR-T-1", {"as_of": COB}, "e5"))
    sm.process(ev("ccr_trade", "CalcComplete", "CCR-T-1", {"exposure": 3_400_000.0}, "e6"))
    px.put("exposure_result", {"trade_id": "CCR-T-1", "cob": COB, "exposure": 3_400_000.0, "currency": "USD"})
    print(f"exposure now (COB {COB})       = {px.get('exposure_result', 'CCR-T-1', as_of=COB)['exposure']:,.0f}")
    print(f"exposure as-known this morning = {px.get('exposure_result', 'CCR-T-1', as_of=COB, tx_as_of=t_morning)['exposure']:,.0f}")

    # --- CCR6 / CCR7 --------------------------------------------------------
    h("CCR6/CCR7  Late & corrected events + COB reproducibility")
    tt = time.time()
    time.sleep(0.01)
    px.put("exposure_result", {"trade_id": "CCR-T-1", "cob": COB, "exposure": 3_250_000.0, "currency": "USD"})
    print(f"a late correction lands; as-of COB now = {px.get('exposure_result', 'CCR-T-1', as_of=COB)['exposure']:,.0f}")
    print(f"as-known before the correction         = {px.get('exposure_result', 'CCR-T-1', as_of=COB, tx_as_of=tt)['exposure']:,.0f}  (reproducible)")

    # --- CCR2 ---------------------------------------------------------------
    h("CCR2  Governed contract evolution + backfill")
    d = px.registry.active_storage("ccr_trade").model_dump(mode="json")
    d["version"] = 2
    d["projections"].append({"name": "by_product", "set": "ccr_trade_by_product",
                             "key": "{product_type}:{trade_id}",
                             "fields": ["trade_id", "product_type", "counterparty", "status"]})
    cr = px.governance.submit("alice", px.governance.draft("alice", d)["id"])
    print(f"change submitted; compatible={cr['compat']['compatible']}, needs {cr['required_approvals']} approval(s)")
    px.governance.approve("bob", cr["id"])
    px.governance.publish("bob", cr["id"])
    n = BackfillJob(px).run("ccr_trade")
    print(f"published v2; backfilled {n} trade(s); new projection present="
          f"{px.store.get('ccr_trade_by_product', 'IRS:CCR-T-1') is not None}")

    # --- CCR8 ---------------------------------------------------------------
    h("CCR8  Hot/cold tier movement (retention -> warehouse)")
    worker = RetentionWorker(px.registry, InMemoryWarehouse())
    worker.run(MemoryEventSource(px.sink))
    cold = worker._warehouse.latest_state("warehouse.ccr_trades")
    print(f"cold-tier trades: {sorted(decode(r)['trade_id'] for r in cold)}")
    print(f"cold-tier exposures: {worker._warehouse.count('warehouse.exposures')} row(s)")

    # --- CCR9 ---------------------------------------------------------------
    h("CCR9  Lineage: source event -> saga -> cube -> exposure")
    lin = px.lineage("CCR-T-1")
    print(f"trade {lin['trade_id']} ({lin['source']['trade']['status']}) in {lin['source']['netting_set_id']}")
    print(f"  saga_steps={len(lin['saga'])}  interactions={len(lin['interactions'])}  cube_points={lin['cube']['point_count']}")
    print(f"  exposure={lin['exposure_result']['exposure']:,.0f}  ns_exposure={lin['ns_exposure']['exposure']:,.0f}")
    print(f"  produced by contract versions {lin['contract_versions']}")

    # --- CCR10 --------------------------------------------------------------
    h("CCR10  Recovery — effectively-once under redelivery")
    dup = sm.process(ev("ccr_trade", "TradeReceived", "CCR-T-1", trade, "e1"))
    print(f"redelivered TradeReceived (event_id e1) -> {dup.status} (no double effect)")
    print("  (torn-write, broker-rebalance and retention-replay recovery are in tests/test_ccr_recovery.py)")

    px.close()
    print("\nCCR scenario complete — all ten proofs demonstrated.")


if __name__ == "__main__":
    main()
