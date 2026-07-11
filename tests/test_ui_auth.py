from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from phronexus import Phronexus, Settings
from phronexus.api import create_app


def _client(schemes, **auth):
    s = Settings(backend="memory")
    s.observability.log_level = "ERROR"
    s.api.auth.schemes = schemes
    for k, v in auth.items():
        setattr(s.api.auth, k, v)
    px = Phronexus(s)
    px.load_contract_dir("contracts_examples")
    return TestClient(create_app(px=px))


def _login(c, username, password):
    r = c.post("/auth/login", json={"username": username, "password": password})
    return r


# --- login / JWT ---------------------------------------------------------

def test_login_success_and_me():
    c = _client(["jwt"], users={"admin": {"password": "secret", "roles": ["admin"]}}, jwt_secret="k")
    r = _login(c, "admin", "secret")
    assert r.status_code == 200 and r.json()["roles"] == ["admin"]
    tok = r.json()["token"]
    me = c.get("/auth/me", headers={"Authorization": f"Bearer {tok}"})
    assert me.status_code == 200 and me.json()["principal"] == "admin" and me.json()["scheme"] == "jwt"


def test_login_bad_credentials():
    c = _client(["jwt"], users={"admin": {"password": "secret", "roles": ["admin"]}})
    assert _login(c, "admin", "wrong").status_code == 401
    assert _login(c, "ghost", "x").status_code == 401


def test_jwt_scheme_enforced_on_protected_routes():
    c = _client(["jwt"], users={"admin": {"password": "secret", "roles": ["admin"]}}, jwt_secret="k")
    assert c.get("/entities/trade/documents/none").status_code == 401     # no token
    tok = _login(c, "admin", "secret").json()["token"]
    r = c.get("/entities/trade/documents/none", headers={"Authorization": f"Bearer {tok}"})
    assert r.status_code == 404                                           # authenticated, doc absent


def test_password_sha256_login():
    import hashlib
    h = hashlib.sha256(b"hunter2").hexdigest()
    c = _client(["jwt"], users={"ops": {"password_sha256": h, "roles": ["admin"]}}, jwt_secret="k")
    assert _login(c, "ops", "hunter2").status_code == 200
    assert _login(c, "ops", "nope").status_code == 401


# --- roles / RBAC --------------------------------------------------------

def test_viewer_role_cannot_publish_admin_can():
    c = _client(["jwt"], jwt_secret="k",
                users={"a": {"password": "x", "roles": ["admin"]},
                       "v": {"password": "y", "roles": ["viewer"]}})
    contract = {"kind": "view", "entity": "trade", "view": "tiny", "version": 1, "fields": ["trade_id"]}
    v = _login(c, "v", "y").json()["token"]
    a = _login(c, "a", "x").json()["token"]
    assert c.post("/contracts", json=contract, headers={"Authorization": f"Bearer {v}"}).status_code == 403
    assert c.post("/contracts", json=contract, headers={"Authorization": f"Bearer {a}"}).status_code == 200


# --- contract dry-run validate ------------------------------------------

def test_contract_validate_endpoint():
    c = _client(["none"])
    good = c.post("/contracts/validate", json={
        "kind": "view", "entity": "trade", "view": "x", "version": 1, "fields": ["trade_id"]})
    assert good.json()["ok"] is True and good.json()["identity"] == "view:trade:x:v1"
    bad = c.post("/contracts/validate", json={"kind": "view", "entity": "trade"})  # missing fields
    assert bad.json()["ok"] is False and bad.json()["errors"]


def test_contract_validate_rejects_incompatible_storage():
    c = _client(["none"])
    sc = c.get("/contracts/storage:trade:v1").json()
    sc["version"] = 2
    sc["primary_key"] = ["trade_id", "counterparty"]
    r = c.post("/contracts/validate", json=sc)
    assert r.json()["ok"] is False and any("primary_key" in e for e in r.json()["errors"])


# --- UI serving ----------------------------------------------------------

def test_ui_served():
    c = _client(["none"])
    r = c.get("/ui/")
    assert r.status_code == 200 and "Phronexus" in r.text


def test_root_redirects_to_ui():
    c = _client(["none"])
    r = c.get("/", follow_redirects=False)
    assert r.status_code in (307, 308) and r.headers["location"] == "/ui/"


def test_auth_config_endpoint():
    c = _client(["none"])
    assert c.get("/auth/config").json()["provider"] == "local"
