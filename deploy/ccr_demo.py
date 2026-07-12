"""Self-contained CCR demo server.

Builds an in-memory Phronexus engine, loads the CCR contracts, runs the CCR
flow so there is real data to explore (trace / find / lineage / bitemporal
as-of), then serves the REST API + management UI on top of that same engine —
so everything you do in the browser is backed by the data produced here.

    python -m deploy.ccr_demo            # serve on :8080 (UI at /ui)
    uvicorn deploy.ccr_demo:app          # same, via uvicorn directly

Login (local password auth, RBAC via JWT):
    admin / admin        — everything (draft → approve → publish, self-approve on)
    author / author      — governance:read, contract:draft/submit
    approver / approver  — governance:read, contract:approve/publish/rollback
    viewer / viewer      — governance:read (read-only)
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

_CCR = Path(__file__).resolve().parent.parent / "examples" / "ccr"
sys.path.insert(0, str(_CCR))
import ccr_ops  # noqa: E402

from phronexus import Phronexus, Settings  # noqa: E402
from phronexus.admin import BackfillJob  # noqa: E402
from phronexus.api.app import create_app  # noqa: E402
from phronexus.statemachine import InputEvent, MemoryOutputPublisher  # noqa: E402

COB = 20260711
TRADE_ID = "CCR-T-1"


def _ev(entity, etype, key, payload, eid):
    return InputEvent(entity=entity, event_type=etype, key=key, payload=payload, event_id=eid)


def _settings() -> Settings:
    s = Settings(backend="memory")
    s.observability.log_level = "WARNING"
    # Audit trace populates inline (no Kafka), journal keeps request/response
    # interactions — both feed the Trace / Lineage screens.
    s.audit.enabled = True
    s.journal.enabled = True
    s.journal.journal_requests = True
    # One approval everywhere; allow self-approve so a single admin can drive the
    # whole governed draft → approve → publish in the demo.
    s.governance.approval_policy = {"*": {"*": 1}}
    s.governance.allow_self_approve = True
    # UI auth: local password login -> JWT, config-driven RBAC.
    a = s.api.auth
    a.schemes = ["jwt"]
    a.provider = "local"
    a.jwt_secret = "phronexus-ccr-demo-secret"
    a.users = {
        "admin": {"password": "admin", "roles": ["admin"]},
        "author": {"password": "author", "roles": ["author"]},
        "approver": {"password": "approver", "roles": ["approver"]},
        "viewer": {"password": "viewer", "roles": ["viewer"]},
    }
    a.roles = {
        "admin": ["*"],
        "author": ["governance:read", "contract:draft", "contract:submit"],
        "approver": ["governance:read", "contract:approve", "contract:publish", "contract:rollback"],
        "viewer": ["governance:read"],
    }
    return s


def _run_flow(px: Phronexus) -> None:
    """Produce explorable CCR data centered on trade CCR-T-1."""
    px.load_contract_dir(str(_CCR / "contracts"))
    px.governance.set_cob("ops", COB)
    sm = px.state_machine(output=MemoryOutputPublisher())

    # Reference data + netting set.
    px.put("currency", {"code": "USD", "usd_rate": 1.0})
    px.put("currency", {"code": "EUR", "usd_rate": 1.08})
    px.put("netting_set", {"netting_set_id": "NS-GS-USD", "counterparty": "GS",
                           "csa_id": "CSA-GS-1", "status": "active"})

    trade = {"trade_id": TRADE_ID, "counterparty": "GS", "netting_set_id": "NS-GS-USD",
             "book": "IRD-1", "product_type": "IRS", "notional": 25_000_000.0,
             "currency": "USD", "trade_date": 20260711, "maturity_date": 20360711}

    # CCR1 — counterparty onboarding saga + trade received (saga => audit trace).
    ccr_ops.onboard_counterparty(sm, {"counterparty_id": "GS", "name": "Goldman Sachs",
                                       "jurisdiction": "US", "rating": "A"})
    sm.process(_ev("ccr_trade", "TradeReceived", TRADE_ID, trade, "e1"))

    # CCR3 — netting-set lifecycle + a second member + served aggregation.
    sm.process(_ev("netting_set", "NettingSetOpened", "NS-GS-USD",
                   {"netting_set_id": "NS-GS-USD", "counterparty": "GS", "csa_id": "CSA-GS-1"}, "ns0"))
    sm.process(_ev("netting_set", "CsaConfirmed", "NS-GS-USD", {"netting_set_id": "NS-GS-USD"}, "ns1"))
    px.put("ccr_trade", {**trade, "trade_id": "CCR-T-2"})
    for tid, exp in (("CCR-T-1", 3_100_000.0), ("CCR-T-2", 1_900_000.0)):
        px.put("exposure_result", {"trade_id": tid, "cob": COB, "exposure": exp, "currency": "USD",
                                   "netting_set_id": "NS-GS-USD", "source_event_id": "e1"})
    ccr_ops.aggregate_netting_set(px, "NS-GS-USD", COB, source_event_id="e1")

    # CCR5 — value-cube points (per-tenor) for querying / find.
    for tenor in (365, 1, 90, 30, 7):
        px.put("value_cube", {"trade_id": TRADE_ID, "scenario_id": "BASE", "tenor": tenor,
                              "value": 1000.0 + tenor, "currency": "USD", "as_of": COB})

    # CCR4 — intraday recalculation via the saga (new exposure version, later tx).
    for et, pl, eid in (("CubeReady", {"as_of": COB}, "e2"),
                        ("CalcComplete", {"exposure": 3_100_000.0, "cva": 41_250.0}, "e3"),
                        ("RecalcRequested", {}, "e4"),
                        ("CubeReady", {"as_of": COB}, "e5"),
                        ("CalcComplete", {"exposure": 3_400_000.0}, "e6")):
        sm.process(_ev("ccr_trade", et, TRADE_ID, pl, eid))
    px.put("exposure_result", {"trade_id": TRADE_ID, "cob": COB, "exposure": 3_400_000.0, "currency": "USD"})

    # CCR2 — a governed contract evolution lands in the Change Requests + History.
    d = px.registry.active_storage("ccr_trade").model_dump(mode="json")
    d["version"] = 2
    d["projections"].append({"name": "by_product", "set": "ccr_trade_by_product",
                             "key": "{product_type}:{trade_id}",
                             "fields": ["trade_id", "product_type", "counterparty", "status"]})
    cr = px.governance.submit("admin", px.governance.draft("admin", d)["id"])
    px.governance.approve("admin", cr["id"])
    px.governance.publish("admin", cr["id"])
    BackfillJob(px).run("ccr_trade")

    lin = px.lineage(TRADE_ID)
    print(f"[ccr-demo] seeded trade {TRADE_ID}: status={lin['source']['trade']['status']} "
          f"exposure={lin['exposure_result']['exposure']:,.0f} "
          f"cube_points={lin['cube']['point_count']} saga_steps={len(lin['saga'])}", flush=True)


def build_app():
    px = Phronexus(_settings())
    _run_flow(px)
    return create_app(px=px)


app = build_app()


def main() -> None:
    import uvicorn
    print("[ccr-demo] UI at http://localhost:8080/ui  (login admin/admin)", flush=True)
    print(f"[ccr-demo] trace/find this id -> {TRADE_ID} (entity 'ccr_trade')", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()
