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
    r = client.get("/readyz")
    assert r.status_code == 200 and r.json()["status"] == "ready" and r.json()["store"] == "ok"


def test_query_pagination_endpoint(client):
    for i, n in enumerate([300.0, 100.0, 200.0]):
        client.put("/entities/trade/documents", json={"document": {
            "trade_id": f"T{i}", "counterparty": "GS", "notional": n, "ccy": "USD", "trade_date": 20250101 + i}})
    r = client.post("/entities/trade/query", json={
        "where": [{"field": "counterparty", "op": "eq", "value": "GS"}],
        "sort": [{"field": "notional", "order": "asc"}], "limit": 2, "offset": 0})
    body = r.json()
    assert body["count"] == 2 and body["has_more"] is True
    assert [d["notional"] for d in body["documents"]] == [100.0, 200.0]


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


def test_validate_endpoint_reports_errors(client):
    r = client.post("/entities/trade/validate", json={"document": {**TRADE, "ccy": "ZZZ"}})
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is False and any("ccy_supported" in e for e in body["errors"])


def test_write_invalid_document_maps_422(client):
    r = client.put("/entities/trade/documents", json={"document": {**TRADE, "notional": -1}})
    assert r.status_code == 422 and r.json()["error"] == "validation_error"


def test_sync_event_validation_rejection_maps_422(client):
    r = client.post("/entities/trade/events", json={"event_type": "TradeBooked", "key": "T-1", "event_id": "v", "payload": {**TRADE, "ccy": "ZZZ"}})
    assert r.status_code == 422 and "validation" in r.json()["reason"]


def test_sync_event_accept_reject_lifecycle(client):
    book = dict(TRADE)
    # Book -> 200 applied
    r = client.post("/entities/trade/events", json={"event_type": "TradeBooked", "key": "T-1", "event_id": "e1", "payload": book})
    assert r.status_code == 200 and r.json()["status"] == "applied" and r.json()["to_state"] == "booked"
    # Confirm -> 200
    r = client.post("/entities/trade/events", json={"event_type": "TradeConfirmed", "key": "T-1", "event_id": "e2", "payload": {}})
    assert r.status_code == 200 and r.json()["to_state"] == "confirmed"
    # Wrong transition (settle before... it's confirmed so settle is valid); instead try booking again -> rejected 409
    r = client.post("/entities/trade/events", json={"event_type": "TradeBooked", "key": "T-1", "event_id": "e3", "payload": book})
    assert r.status_code == 409 and r.json()["status"] == "rejected"


def test_sync_event_guard_rejection_maps_422(client):
    client.post("/entities/trade/events", json={"event_type": "TradeBooked", "key": "T-2", "event_id": "b", "payload": dict(TRADE, trade_id="T-2")})
    client.post("/entities/trade/events", json={"event_type": "TradeConfirmed", "key": "T-2", "event_id": "c", "payload": {}})
    # guard notional > 0 fails
    r = client.post("/entities/trade/events", json={"event_type": "TradeSettled", "key": "T-2", "event_id": "s", "payload": {"notional": 0}})
    assert r.status_code == 422 and "guard" in r.json()["reason"]


def test_sync_event_duplicate_is_idempotent(client):
    body = {"event_type": "TradeBooked", "key": "T-3", "event_id": "dup", "payload": dict(TRADE, trade_id="T-3")}
    assert client.post("/entities/trade/events", json=body).json()["status"] == "applied"
    r = client.post("/entities/trade/events", json=body)  # replay same event_id
    assert r.status_code == 200 and r.json()["status"] == "duplicate"


def test_sync_event_requires_auth():
    c = _make_client(["api_key"], api_keys={"k": "svc"})
    r = c.post("/entities/trade/events", json={"event_type": "TradeBooked", "key": "T-1", "payload": TRADE})
    assert r.status_code == 401


def test_events_endpoint_in_openapi(client):
    schema = client.get("/openapi.json").json()
    path = schema["paths"]["/entities/{entity}/events"]["post"]
    assert 200 in [int(k) for k in path["responses"]]
    assert 409 in [int(k) for k in path["responses"]]
    assert 422 in [int(k) for k in path["responses"]]


def test_list_contracts_endpoint(client):
    r = client.get("/contracts")
    assert r.status_code == 200 and "storage:trade:v1" in r.json()["contracts"]


def test_get_contract_endpoint(client):
    r = client.get("/contracts/storage:trade:v1")
    assert r.status_code == 200 and r.json()["entity"] == "trade"


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


def test_schedule_crud_and_tick(client):
    r = client.put("/schedules", json={"name": "s1", "topic": "kafka://t", "interval_seconds": 60})
    assert r.status_code == 200 and r.json()["upserted"] == "s1"
    assert client.get("/schedules").json()["schedules"][0]["name"] == "s1"
    assert client.post("/schedules/tick").json()["fired"] == ["s1"]
    # State records the single firing.
    state = client.get("/schedules").json()["schedules"][0]["state"]
    assert state["count"] == 1
    assert client.delete("/schedules/s1").json()["deleted"] == "s1"
    assert client.get("/schedules").json()["schedules"] == []


def test_schedule_admin_guarded():
    client = _make_client(["api_key"], api_keys={"admin-key": "admin", "user-key": "user"},
                          admin_principals=["admin"])
    body = {"name": "s1", "topic": "kafka://t", "interval_seconds": 60}
    assert client.put("/schedules", json=body, headers={"X-API-Key": "user-key"}).status_code == 403
    assert client.put("/schedules", json=body, headers={"X-API-Key": "admin-key"}).status_code == 200
