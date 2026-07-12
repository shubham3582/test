"""GOV1/GOV2/GOV6/GOV9 — the governance workflow at the service level.

- GOV1 approval integrity: N *distinct* approvals; self-approval blocked by SoD.
- GOV2 immutable history: every action is hash-chained; tampering is detectable.
- GOV6 governed rollback: audited, reversible, non-destructive.
- GOV9 compatibility explained: structured breaking/non-breaking changes + diff.
"""

from __future__ import annotations

import pytest

from phronexus import Phronexus, Settings
from phronexus.contracts.loader import parse_contract
from phronexus.governance import GovernanceError


def _px(policy=None, *, allow_self=False, env="dev"):
    s = Settings(backend="memory")
    s.observability.log_level = "WARNING"
    s.governance.environment = env
    s.governance.approval_policy = policy or {"*": {"*": 1}}
    s.governance.allow_self_approve = allow_self
    px = Phronexus(s)
    px.load_contract_dir("contracts_examples")
    return px


def _storage(entity, version=1, *, manifest_set=None, projections=None):
    return {
        "kind": "storage", "entity": entity, "version": version, "primary_key": ["id"],
        "manifest_set": manifest_set or f"{entity}_manifest",
        "projections": projections or [
            {"name": "main", "set": f"{entity}_main", "key": "{id}",
             "fields": ["*"], "canonical": True}],
    }


# --- GOV1 -----------------------------------------------------------------

@pytest.mark.governance
def test_two_distinct_approvals_required_with_sod():
    gov = _px({"dev": {"storage": 2}}).governance
    cr = gov.draft("alice", _storage("widget"))
    cr = gov.submit("alice", cr["id"])
    assert cr["status"] == "submitted" and cr["required_approvals"] == 2

    with pytest.raises(GovernanceError):       # author can't self-approve (SoD)
        gov.approve("alice", cr["id"])
    cr = gov.approve("bob", cr["id"])
    assert cr["status"] == "submitted"          # 1 of 2
    with pytest.raises(GovernanceError):        # same approver can't double-count
        gov.approve("bob", cr["id"])
    with pytest.raises(GovernanceError):        # not approved yet -> can't publish
        gov.publish("carol", cr["id"])

    cr = gov.approve("carol", cr["id"])         # 2 of 2 distinct
    assert cr["status"] == "approved"
    cr = gov.publish("dave", cr["id"])
    assert cr["status"] == "published"


@pytest.mark.governance
def test_publish_activates_the_approved_bytes():
    px = _px({"*": {"*": 1}})
    gov = px.governance
    cr = gov.draft("alice", _storage("gizmo"))
    gov.submit("alice", cr["id"])
    gov.approve("bob", cr["id"])
    gov.publish("bob", cr["id"])
    assert "storage:gizmo:v1" in px.list_contracts()["contracts"]
    assert px.registry.active_storage("gizmo").version == 1


@pytest.mark.governance
def test_self_approve_allowed_when_policy_permits():
    gov = _px({"*": {"*": 1}}, allow_self=True).governance
    cr = gov.draft("alice", _storage("gadget"))
    gov.submit("alice", cr["id"])
    assert gov.approve("alice", cr["id"])["status"] == "approved"


# --- GOV2 -----------------------------------------------------------------

@pytest.mark.governance
def test_history_is_hash_chained_and_tamper_evident():
    px = _px({"*": {"*": 1}})
    gov = px.governance
    cr = gov.draft("alice", _storage("thing"))
    gov.submit("alice", cr["id"])
    gov.approve("bob", cr["id"])
    gov.publish("bob", cr["id"])

    v = gov.log.verify()
    assert v["ok"] and v["count"] >= 4
    entries = gov.log.entries()
    assert entries[0]["action"] == "change.drafted"

    # Tamper with an entry at rest -> verify detects it at that seq.
    lset = px.settings.governance.log_set
    seq = entries[1]["seq"]
    rec = px.store.get(lset, f"entry:{seq:012d}")
    tampered = dict(rec.bins)
    tampered["actor"] = "mallory"
    px.store.put(lset, f"entry:{seq:012d}", tampered)
    bad = gov.log.verify()
    assert bad["ok"] is False and bad["bad_seq"] == seq


# --- GOV6 -----------------------------------------------------------------

@pytest.mark.governance
def test_governed_rollback_is_reversible_and_logged():
    px = _px({"*": {"*": 1}})
    gov = px.governance
    for v in (1, 2):
        cr = gov.draft("alice", _storage("wid", v))
        gov.submit("alice", cr["id"])
        gov.approve("bob", cr["id"])
        gov.publish("bob", cr["id"])
    assert px.registry.active_storage("wid").version == 2

    r = gov.rollback("carol", "storage:wid:v1", reason="regression")
    assert (r["from"], r["to"]) == ("storage:wid:v2", "storage:wid:v1")
    assert px.registry.active_storage("wid").version == 1
    # version history intact (both versions still stored)
    ids = px.list_contracts()["contracts"]
    assert "storage:wid:v1" in ids and "storage:wid:v2" in ids
    # reversible
    gov.rollback("carol", "storage:wid:v2")
    assert px.registry.active_storage("wid").version == 2
    assert any(e["action"] == "contract.rolledback" for e in gov.log.entries())


# --- GOV9 -----------------------------------------------------------------

@pytest.mark.governance
def test_compat_report_explains_breaking_and_nonbreaking():
    px = _px()
    px.publish_contract(_storage("cx", 1))

    breaking = parse_contract(_storage("cx", 2, manifest_set="cx_elsewhere"))
    rep = px.registry.compat_report(breaking)
    assert rep["compatible"] is False
    assert "manifest_set" in {c["field"] for c in rep["changes"] if c["breaking"]}

    added = _storage("cx", 2, projections=[
        {"name": "main", "set": "cx_main", "key": "{id}", "fields": ["*"], "canonical": True},
        {"name": "by_x", "set": "cx_by_x", "key": "{id}", "fields": ["id"]},
    ])
    ok = px.registry.compat_report(parse_contract(added))
    assert ok["compatible"] is True
    assert any(c["field"] == "projection_count" and not c["breaking"] for c in ok["changes"])


@pytest.mark.governance
def test_diff_between_versions():
    px = _px()
    px.publish_contract(_storage("dz", 1))
    px.publish_contract(_storage("dz", 2))
    d = px.registry.diff("storage:dz:v1", "storage:dz:v2")
    assert any(c["field"] == "version" for c in d["changes"])
