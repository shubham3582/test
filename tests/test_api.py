from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from phronexus import Phronexus, Settings
from phronexus.api import create_app


def _make_client(schemes, **auth_kwargs) -> TestClient:
    settings = Settings(backend="memory")
    settings.observability.log_level = "ERROR"
    settings.api.auth.schemes = schemes
    for k, v in auth_kwargs.items():
        setattr(settings.api.auth, k, v)
    px = Phronexus(settings)
    px.load_contract_dir("contracts_examples")
    app = create_app(px=px)
    return TestClient(app)


@pytest.fixture()
def client() -> TestClient:
    return _make_client(["none"])


TRADE = {
    "trade_id": "T-1", "counterparty": "GS", "notional": 1_000_000.005,
    "ccy": "USD", "trade_date": 20250115, "book": "RATES",
}


def test_health_and_ready(client):
    assert client.get("/healthz").json()["status"] == "ok"
    assert client.get("/readyz").json()["status"] == "ready"


def test_write_read_roundtrip(client):
    r = client.put("/entities/trade/documents", json={"document": TRADE})
    assert r.status_code == 200 and r.json()["doc_id"] == "T-1"
    got = client.get("/entities/trade/documents/T-1")
    assert got.status_code == 200 and got.json()["counterparty"] == "GS"


def test_read_missing_maps_to_404(client):
    r = client.get("/entities/trade/documents/nope")
    assert r.status_code == 404 and r.json()["error"] == "document_not_found"


def test_query_and_view(client):
    client.put("/entities/trade/documents", json={"document": TRADE})
    r = client.post("/entities/trade/query", json={"where": [{"field": "counterparty", "op": "eq", "value": "GS"}]})
    assert r.json()["count"] == 1
    # Query through a view (masking applied).
    r = client.post("/entities/trade/query?view=public", json={"where": [{"field": "counterparty", "op": "eq", "value": "GS"}]})
    assert r.json()["documents"][0]["counterparty"] == "****"


def test_pattern_endpoint(client):
    client.put("/entities/trade/documents", json={"document": TRADE})
    r = client.post("/entities/trade/patterns/cpty_since", json={"params": {"cpty": "GS", "since": 20250101}})
    assert {d["trade_id"] for d in r.json()["documents"]} == {"T-1"}


def test_view_endpoint(client):
    client.put("/entities/trade/documents", json={"document": TRADE})
    r = client.get("/entities/trade/views/risk_consumer/documents/T-1")
    assert r.json()["notional"] == 1_000_000.01


def test_bad_query_maps_to_400(client):
    r = client.post("/entities/trade/query", json={"where": [{"field": "book", "op": "eq", "value": "X"}]})
    assert r.status_code == 400 and r.json()["error"] == "query_error"


def test_api_key_auth_enforced():
    client = _make_client(["api_key"], api_keys={"secret-key": "svc-risk"})
    # No key -> 401
    assert client.put("/entities/trade/documents", json={"document": TRADE}).status_code == 401
    # Valid key -> 200
    r = client.put("/entities/trade/documents", json={"document": TRADE}, headers={"X-API-Key": "secret-key"})
    assert r.status_code == 200


def test_bearer_auth_enforced():
    client = _make_client(["bearer"], bearer_tokens={"tok-123": "svc-fx"})
    assert client.get("/entities/trade/documents/T-1").status_code == 401
    r = client.get("/entities/trade/documents/T-1", headers={"Authorization": "Bearer tok-123"})
    assert r.status_code in (404,)  # authenticated, doc simply absent


def test_mtls_cn_header_auth():
    client = _make_client(["mtls"], mtls_allowed_cns={"risk.svc.internal": "svc-risk"})
    assert client.get("/readyz").status_code == 200  # health is open
    r = client.put(
        "/entities/trade/documents", json={"document": TRADE},
        headers={"X-Client-Cert-CN": "risk.svc.internal"},
    )
    assert r.status_code == 200
    assert client.put("/entities/trade/documents", json={"document": TRADE},
                      headers={"X-Client-Cert-CN": "unknown"}).status_code == 401


def test_admin_contract_publish_requires_admin():
    client = _make_client(["api_key"], api_keys={"admin-key": "admin", "user-key": "user"},
                          admin_principals=["admin"])
    contract = {
        "kind": "view", "entity": "trade", "view": "tiny", "version": 1,
        "fields": ["trade_id"],
    }
    # Non-admin principal -> 403
    r = client.post("/contracts", json=contract, headers={"X-API-Key": "user-key"})
    assert r.status_code == 403
    # Admin -> 200
    r = client.post("/contracts", json=contract, headers={"X-API-Key": "admin-key"})
    assert r.status_code == 200 and r.json()["published"] == "view:trade:tiny:v1"
