"""End-to-end walkthrough of onboarding a new entity ('bond') by config only.

    python examples/bond/run_bond.py                              # in-memory, no services
    PHRONEXUS_BACKEND=aerospike python examples/bond/run_bond.py  # same code, live stack

Defaults to the always-available in-memory backend — no services required. Set
PHRONEXUS_BACKEND=aerospike (with the usual PHRONEXUS_AEROSPIKE__*/KAFKA__* env)
to run the identical walkthrough against a live stack. Every contract that shapes
the 'bond' entity (storage, query, view, validation, transition) lives in
examples/bond/*.yaml; there is no bond-specific Python.
"""

from __future__ import annotations

import os

from phronexus import Phronexus, Settings
from phronexus.statemachine import InputEvent, MemoryOutputPublisher


def main() -> None:
    settings = Settings(backend=os.environ.get("PHRONEXUS_BACKEND", "memory"))
    settings.observability.log_level = "ERROR"
    px = Phronexus(settings)

    # 1) Onboard the entity: load its contracts (config, not code).
    px.load_contract_dir("examples/bond")

    apple = {
        "isin": "US0378331005", "issuer": "APPLE", "coupon": 3.85,
        "currency": "USD", "maturity_date": 20310215, "callable": True,
    }

    # 2) Validate (JSON Schema + DQ) — dry run.
    print("validate ok:", px.validate("bond", apple).ok)
    print("bad coupon :", px.validate("bond", {**apple, "coupon": 99}).errors)

    # 3) Write through the manifest, then read back and query.
    px.put("bond", apple)
    px.put("bond", {**apple, "isin": "US0378331006", "issuer": "APPLE", "coupon": 4.10})
    print("get        :", px.get("bond", "US0378331005")["issuer"])
    print("by_issuer  :", [b["isin"] for b in px.query_pattern("bond", "by_issuer", issuer="APPLE")])
    print("coupon>=4  :", [b["isin"] for b in px.query_pattern("bond", "issuer_min_coupon", issuer="APPLE", min_coupon=4.0)])

    # 4) Consumer view (field projection + transform).
    print("desk view  :", px.view("bond", "desk", "US0378331005"))

    # 5) Drive the lifecycle through the state machine (accept/reject + emit).
    out = MemoryOutputPublisher()
    sm = px.state_machine(output=out)
    r1 = sm.process(InputEvent(entity="bond", event_type="BondIssued", key="US0378331005",
                               payload=apple, event_id="e1"))
    r2 = sm.process(InputEvent(entity="bond", event_type="BondCalled", key="US0378331005",
                               payload={}, event_id="e2"))
    print("issued     :", r1.status, r1.to_state, "->", r1.emitted)
    print("called     :", r2.status, r2.to_state, "->", r2.emitted)
    print("final state:", px.get("bond", "US0378331005")["status"])

    px.close()


if __name__ == "__main__":
    main()
