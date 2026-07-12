"""GOV3 — least privilege: config-driven role->permission RBAC.

Every governance action is permission-gated; the map is pure config. Here we
prove the RBAC primitives (union, wildcard, namespace wildcard, unknown role)
and that the effective permissions are surfaced to callers (login + /auth/me).
HTTP 403 enforcement through require_permission is exercised against the
governance router in the Wave-2 tests.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from phronexus import Phronexus, Settings
from phronexus.api import create_app
from phronexus.api.auth import Authenticator, Principal
from phronexus.config import AuthSettings

ROLES = {
    "admin": ["*"],
    "viewer": ["governance:read"],
    "author": ["governance:read", "contract:draft", "contract:submit"],
    "approver": ["contract:*"],  # namespace wildcard
}


def _authn(roles=ROLES):
    return Authenticator(AuthSettings(roles=roles))


def _p(*roles):
    return Principal(name="u", scheme="jwt", roles=tuple(roles))


@pytest.mark.governance
def test_permissions_union_across_roles():
    authn = _authn()
    assert authn.permissions(_p("author")) == {
        "governance:read", "contract:draft", "contract:submit"}
    # union of two roles
    assert "backfill:run" not in authn.permissions(_p("author", "viewer"))
    assert authn.permissions(_p("nonexistent")) == set()  # unknown role -> nothing


@pytest.mark.governance
def test_has_permission_wildcards_and_exact():
    authn = _authn()
    admin, viewer, approver = _p("admin"), _p("viewer"), _p("approver")
    # global wildcard
    assert authn.has_permission(admin, "contract:publish")
    assert authn.is_admin(admin)
    # exact grant + deny
    assert authn.has_permission(viewer, "governance:read")
    assert not authn.has_permission(viewer, "contract:publish")
    assert not authn.is_admin(viewer)
    # namespace wildcard contract:* grants any contract:*
    assert authn.has_permission(approver, "contract:approve")
    assert authn.has_permission(approver, "contract:rollback")
    assert not authn.has_permission(approver, "cob:set")


def _client():
    s = Settings(backend="memory")
    s.observability.log_level = "ERROR"
    s.api.auth.schemes = ["jwt"]
    s.api.auth.jwt_secret = "k"
    s.api.auth.users = {"bob": {"password": "pw", "roles": ["approver"]}}
    s.api.auth.roles = ROLES
    px = Phronexus(s)
    px.load_contract_dir("contracts_examples")
    return TestClient(create_app(px=px))


@pytest.mark.governance
def test_login_and_me_expose_effective_permissions():
    c = _client()
    r = c.post("/auth/login", json={"username": "bob", "password": "pw"})
    assert r.status_code == 200
    body = r.json()
    assert body["roles"] == ["approver"]
    # approver -> contract:* namespace expands to the literal grant string
    assert body["permissions"] == ["contract:*"]

    me = c.get("/auth/me", headers={"Authorization": f"Bearer {body['token']}"})
    assert me.status_code == 200 and me.json()["permissions"] == ["contract:*"]


# --- HTTP enforcement through the governance router -----------------------

def _gov_client():
    s = Settings(backend="memory")
    s.observability.log_level = "ERROR"
    s.api.auth.schemes = ["jwt"]
    s.api.auth.jwt_secret = "k"
    s.api.auth.users = {
        "al": {"password": "p", "roles": ["author"]},
        "bo": {"password": "p", "roles": ["approver"]},
        "vi": {"password": "p", "roles": ["viewer"]},
    }
    s.api.auth.roles = ROLES
    s.governance.approval_policy = {"*": {"*": 1}}
    px = Phronexus(s)
    px.load_contract_dir("contracts_examples")
    return TestClient(create_app(px=px))


def _tok(c, u):
    return c.post("/auth/login", json={"username": u, "password": "p"}).json()["token"]


def _h(t):
    return {"Authorization": f"Bearer {t}"}


CONTRACT = {
    "kind": "storage", "entity": "rbacwid", "version": 1, "primary_key": ["id"],
    "manifest_set": "rbacwid_manifest",
    "projections": [{"name": "main", "set": "rbacwid_main", "key": "{id}",
                     "fields": ["*"], "canonical": True}],
}


@pytest.mark.governance
def test_http_permission_gates_enforced():
    c = _gov_client()
    al, bo, vi = _tok(c, "al"), _tok(c, "bo"), _tok(c, "vi")

    # viewer lacks contract:draft -> 403
    assert c.post("/governance/changes", json={"contract": CONTRACT}, headers=_h(vi)).status_code == 403
    # author drafts + submits
    cid = c.post("/governance/changes", json={"contract": CONTRACT}, headers=_h(al)).json()["id"]
    assert c.post(f"/governance/changes/{cid}/submit", headers=_h(al)).status_code == 200
    # viewer lacks contract:approve -> 403
    assert c.post(f"/governance/changes/{cid}/approve", headers=_h(vi)).status_code == 403
    # approver approves + publishes
    assert c.post(f"/governance/changes/{cid}/approve", headers=_h(bo)).status_code == 200
    assert c.post(f"/governance/changes/{cid}/publish", headers=_h(bo)).status_code == 200
    # everyone with governance:read can see the tamper-evident log
    logr = c.get("/governance/log", headers=_h(vi))
    assert logr.status_code == 200 and logr.json()["verify"]["ok"] is True
