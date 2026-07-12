"""GOV4/GOV8 — environment promotion + audit evidence export.

- GOV4: a promoted version is byte-identical to the approved source and carries
  verifiable provenance; a tampered bundle is rejected.
- GOV8: an evidence export covers the requested window and is tamper-evident
  (content hash + gov-log head), and the export is itself logged.
"""

from __future__ import annotations

import pytest

from phronexus import Phronexus, Settings
from phronexus.governance import GovernanceError
from phronexus.governance.evidence import build_evidence
from phronexus.governance.promote import content_hash


def _px(env, *, secret="shared-secret"):
    s = Settings(backend="memory")
    s.observability.log_level = "WARNING"
    s.governance.environment = env
    s.governance.approval_policy = {"*": {"*": 1}}
    s.governance.bundle_secret = secret
    px = Phronexus(s)
    px.load_contract_dir("contracts_examples")
    return px


def _storage(entity, version=1):
    return {"kind": "storage", "entity": entity, "version": version, "primary_key": ["id"],
            "manifest_set": f"{entity}_manifest",
            "projections": [{"name": "main", "set": f"{entity}_main", "key": "{id}",
                             "fields": ["*"], "canonical": True}]}


def _publish(gov, contract):
    cr = gov.draft("alice", contract)
    gov.submit("alice", cr["id"])
    gov.approve("bob", cr["id"])
    gov.publish("bob", cr["id"])


# --- GOV4 -----------------------------------------------------------------

@pytest.mark.governance
def test_promotion_is_byte_identical_with_provenance():
    src, tgt = _px("uat"), _px("prod")
    _publish(src.governance, _storage("widget", 1))

    exported = src.governance.export_bundle("ops", "storage:widget:v1")
    cr = tgt.governance.import_bundle("ops", exported["bundle"], exported["signature"])

    assert cr["environment"] == "prod"
    assert cr["provenance"]["promoted_from"] == "uat"
    # approve + publish in the target, then the bytes match the source exactly
    tgt.governance.approve("rev", cr["id"])
    tgt.governance.publish("rev", cr["id"])
    assert "storage:widget:v1" in tgt.list_contracts()["contracts"]
    src_doc = src.registry.get_version("storage:widget:v1").model_dump(mode="json")
    tgt_doc = tgt.registry.get_version("storage:widget:v1").model_dump(mode="json")
    assert src_doc == tgt_doc


@pytest.mark.governance
def test_tampered_promotion_bundle_is_rejected():
    src, tgt = _px("uat"), _px("prod")
    _publish(src.governance, _storage("widget", 1))
    exported = src.governance.export_bundle("ops", "storage:widget:v1")

    tampered = dict(exported["bundle"])
    tampered["contract"] = {**tampered["contract"], "version": 99}  # alter the payload
    with pytest.raises(GovernanceError):
        tgt.governance.import_bundle("ops", tampered, exported["signature"])

    # wrong secret also fails
    other = _px("prod", secret="different")
    with pytest.raises(GovernanceError):
        other.governance.import_bundle("ops", exported["bundle"], exported["signature"])


@pytest.mark.governance
def test_direct_promotion_between_stores():
    src, tgt = _px("uat"), _px("prod")
    _publish(src.governance, _storage("gadget", 1))
    cr = src.governance.promote_direct("ops", "storage:gadget:v1", tgt.governance)
    assert cr["environment"] == "prod" and cr["status"] == "submitted"


# --- GOV8 -----------------------------------------------------------------

@pytest.mark.governance
def test_evidence_export_is_complete_and_tamper_evident():
    px = _px("prod")
    # generate audit trail + governance history
    _publish(px.governance, _storage("trade2", 1))  # (storage contract; not the example trade)
    px.put("trade", {"trade_id": "E-1", "counterparty": "GS", "notional": 1e6,
                     "ccy": "USD", "trade_date": 20250115, "book": "R"})
    px.put("trade", {"trade_id": "E-2", "counterparty": "MS", "notional": 2e6,
                     "ccy": "USD", "trade_date": 20250115, "book": "R"})

    bundle = build_evidence(px, "auditor", entity="trade")
    # completeness: both trade audit rows present
    audit_docs = {r["doc_id"] for r in bundle["records"]["audit"]}
    assert {"E-1", "E-2"} <= audit_docs
    # tamper-evident: recomputing the hash over the records matches the manifest
    assert content_hash(bundle["records"]) == bundle["manifest"]["content_hash"]
    # mutating a record breaks the hash
    bundle["records"]["audit"][0]["doc_id"] = "TAMPERED"
    assert content_hash(bundle["records"]) != bundle["manifest"]["content_hash"]
    # the export itself was logged
    assert any(e["action"] == "evidence.exported" for e in px.governance.log.entries())
