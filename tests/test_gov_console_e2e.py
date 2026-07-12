"""End-to-end governance flow through the HTTP API the console drives:
draft → submit → approve → publish, then fleet / history / evidence — with RBAC
enforced and the hash chain intact throughout.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from phronexus import Phronexus, Settings
from phronexus.api import create_app

ROLES = {
    "author": ["governance:read", "contract:draft", "contract:submit"],
    "approver": ["governance:read", "contract:approve", "contract:publish"],
    "operator": ["governance:read", "cob:set", "evidence:export", "backfill:control"],
}
CONTRACT = {
    "kind": "storage", "entity": "e2ewid", "version": 1, "primary_key": ["id"],
    "manifest_set": "e2ewid_manifest",
    "projections": [{"name": "main", "set": "e2ewid_main", "key": "{id}",
                     "fields": ["*"], "canonical": True}],
}


def _client():
    s = Settings(backend="memory")
    s.observability.log_level = "ERROR"
    s.api.auth.schemes = ["jwt"]
    s.api.auth.jwt_secret = "k"
    s.api.auth.users = {u: {"password": "p", "roles": [u]} for u in ROLES}
    s.api.auth.roles = ROLES
    s.governance.approval_policy = {"*": {"*": 1}}
    px = Phronexus(s)
    px.load_contract_dir("contracts_examples")
    return TestClient(create_app(px=px))


def _tok(c, u):
    return c.post("/auth/login", json={"username": u, "password": "p"}).json()["token"]


@pytest.mark.governance
def test_full_console_flow_over_http():
    c = _client()
    author = {"Authorization": f"Bearer {_tok(c, 'author')}"}
    approver = {"Authorization": f"Bearer {_tok(c, 'approver')}"}
    operator = {"Authorization": f"Bearer {_tok(c, 'operator')}"}

    # draft → submit (author) → approve → publish (approver)
    cid = c.post("/governance/changes", json={"contract": CONTRACT}, headers=author).json()["id"]
    assert c.post(f"/governance/changes/{cid}/submit", headers=author).status_code == 200
    assert c.post(f"/governance/changes/{cid}/approve", headers=approver).status_code == 200
    assert c.post(f"/governance/changes/{cid}/publish", headers=approver).status_code == 200

    # fleet shows it active, history chain verifies
    fleet = c.get("/governance/fleet", headers=operator).json()
    assert fleet["active"].get("active:storage:e2ewid") == "storage:e2ewid:v1"
    assert fleet["history"]["ok"] is True

    # operator sets COB, exports evidence (both permissioned + logged)
    assert c.post("/governance/cob", json={"cob": 20250115}, headers=operator).status_code == 200
    ev = c.post("/governance/evidence", json={"entity": "e2ewid"}, headers=operator)
    assert ev.status_code == 200 and "content_hash" in ev.json()["manifest"]

    # author cannot set COB (no cob:set) -> 403
    assert c.post("/governance/cob", json={"cob": 20250116}, headers=author).status_code == 403

    # the log still verifies after all activity
    assert c.get("/governance/log", headers=operator).json()["verify"]["ok"] is True
