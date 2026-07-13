"""Admin resync: date-scoped movement between the hot (Aerospike) and cold
(Iceberg) tiers, in both directions.

Covers the guarantees the design promised:
  - hot->cold re-lands a valid-time window as idempotent retention rows;
  - cold->hot rehydrates the exact as-of view, SILENTLY (no new version, no new
    cold-tier row) and preserving the original write identity;
  - both directions are idempotent and honour a date window;
  - non-bitemporal entities window on commit-time and reconcile to latest state.
"""

from __future__ import annotations

import pytest

from phronexus.admin import ResyncJob
from phronexus.kv.memory import InMemoryKV
from phronexus.retention.warehouse import InMemoryWarehouse
from tests.harness import phronexus_with

NSX = {  # bitemporal, iceberg-enabled — windows on valid-time (cob)
    "kind": "storage", "entity": "nsx", "version": 1, "primary_key": ["id"],
    "manifest_set": "nsx_manifest", "temporal": "bitemporal", "valid_time_field": "cob",
    "projections": [{"name": "main", "set": "nsx_main", "key": "{id}",
                     "fields": ["*"], "canonical": True}],
    "iceberg": {"enabled": True, "table": "warehouse.nsx", "retention_days": 3650},
}

TRD = {  # latest-wins, iceberg-enabled — windows on commit-time
    "kind": "storage", "entity": "trd", "version": 1, "primary_key": ["id"],
    "manifest_set": "trd_manifest",
    "projections": [{"name": "main", "set": "trd_main", "key": "{id}",
                     "fields": ["*"], "canonical": True}],
    "iceberg": {"enabled": True, "table": "warehouse.trd", "retention_days": 365},
}


def _px(contract):
    px = phronexus_with(InMemoryKV(), contracts=None)
    px.publish_contract(contract)
    return px


def _wipe_hot(px, *sets):
    for s in sets:
        for key, _ in list(px.store.scan(s)):
            px.store.remove(s, key)


def test_bitemporal_round_trip_cold_to_hot_is_silent_and_faithful():
    px = _px(NSX)
    wh = InMemoryWarehouse()
    # Two business dates -> two immutable versions of the same logical doc.
    px.put("nsx", {"id": "A", "cob": 20260101, "ev": 10})
    px.put("nsx", {"id": "A", "cob": 20260102, "ev": 20})

    # hot -> cold: land the whole window.
    assert ResyncJob(px, warehouse=wh).run("nsx", direction="hot-to-cold") == 2
    assert len(wh.scan("warehouse.nsx")) == 2

    # Lose the hot tier entirely.
    _wipe_hot(px, "nsx_manifest", "nsx_main")
    assert px.get("nsx", "A", as_of=20260102) is None

    # cold -> hot: rehydrate.
    assert ResyncJob(px, warehouse=wh).run("nsx", direction="cold-to-hot") == 2

    # The exact as-of view is reproduced.
    assert px.get("nsx", "A", as_of=20260101)["ev"] == 10
    assert px.get("nsx", "A", as_of=20260102)["ev"] == 20

    # Silent + coords-preserving: re-landing the rehydrated doc yields NO new
    # cold rows (same txns dedup) — proving no new version was minted.
    assert ResyncJob(px, warehouse=wh).run("nsx", direction="hot-to-cold") == 2
    assert len(wh.scan("warehouse.nsx")) == 2
    px.close()


def test_cold_to_hot_is_idempotent():
    px = _px(NSX)
    wh = InMemoryWarehouse()
    px.put("nsx", {"id": "A", "cob": 20260101, "ev": 10})
    ResyncJob(px, warehouse=wh).run("nsx", direction="hot-to-cold")
    _wipe_hot(px, "nsx_manifest", "nsx_main")

    assert ResyncJob(px, warehouse=wh).run("nsx", direction="cold-to-hot") == 1
    # Second run restores nothing new (txn already present).
    assert ResyncJob(px, warehouse=wh).run("nsx", direction="cold-to-hot") == 0
    px.close()


def test_date_window_filters_versions():
    px = _px(NSX)
    wh = InMemoryWarehouse()
    px.put("nsx", {"id": "A", "cob": 20260101, "ev": 10})
    px.put("nsx", {"id": "A", "cob": 20260115, "ev": 20})
    px.put("nsx", {"id": "A", "cob": 20260201, "ev": 30})

    # Only January versions land in the cold tier.
    n = ResyncJob(px, warehouse=wh).run("nsx", direction="hot-to-cold",
                                        date_from=20260101, date_to=20260131)
    assert n == 2
    cobs = sorted(r["cob"] for r in wh.scan("warehouse.nsx"))
    assert cobs == [20260101, 20260115]
    px.close()


def test_dry_run_writes_nothing():
    px = _px(NSX)
    wh = InMemoryWarehouse()
    px.put("nsx", {"id": "A", "cob": 20260101, "ev": 10})
    assert ResyncJob(px, warehouse=wh).run("nsx", direction="hot-to-cold", dry_run=True) == 1
    assert wh.scan("warehouse.nsx") == []
    px.close()


def test_non_bitemporal_windows_on_commit_time_and_reconciles():
    px = _px(TRD)
    wh = InMemoryWarehouse()
    px.put("trd", {"id": "T", "px": 1})
    px.put("trd", {"id": "T", "px": 2})  # latest-wins update

    ResyncJob(px, warehouse=wh).run("trd", direction="hot-to-cold")
    _wipe_hot(px, "trd_manifest", "trd_main")
    assert ResyncJob(px, warehouse=wh).run("trd", direction="cold-to-hot") == 1
    assert px.get("trd", "T")["px"] == 2  # reconciled to current state
    px.close()


def test_cold_to_hot_is_non_destructive_unless_overwrite():
    px = _px(TRD)
    wh = InMemoryWarehouse()
    px.put("trd", {"id": "T", "px": 1})
    ResyncJob(px, warehouse=wh).run("trd", direction="hot-to-cold")

    # A live hot doc has since moved on.
    px.put("trd", {"id": "T", "px": 99})
    # Default: skip (don't clobber the live value).
    ResyncJob(px, warehouse=wh).run("trd", direction="cold-to-hot")
    assert px.get("trd", "T")["px"] == 99
    px.close()


def test_unknown_direction_rejected():
    px = _px(TRD)
    with pytest.raises(ValueError):
        ResyncJob(px).run("trd", direction="sideways")
    px.close()


# --- REST surface + RBAC --------------------------------------------------

def _api_client():
    from fastapi.testclient import TestClient

    from phronexus import Phronexus, Settings
    from phronexus.api import create_app

    s = Settings(backend="memory")
    s.observability.log_level = "ERROR"
    s.api.auth.schemes = ["jwt"]
    s.api.auth.jwt_secret = "k"
    s.api.auth.users = {
        "op": {"password": "p", "roles": ["ops"]},
        "vi": {"password": "p", "roles": ["viewer"]},
    }
    s.api.auth.roles = {"ops": ["resync:control"], "viewer": ["governance:read"]}
    px = Phronexus(s)
    px.publish_contract(NSX)
    px.put("nsx", {"id": "A", "cob": 20260101, "ev": 10})
    return TestClient(create_app(px=px))


def _tok(c, u):
    return c.post("/auth/login", json={"username": u, "password": "p"}).json()["token"]


def test_rest_resync_requires_permission_and_runs():
    c = _api_client()
    body = {"entity": "nsx", "direction": "hot-to-cold"}
    # A viewer lacking resync:control is refused.
    r = c.post("/admin/resync", json=body,
               headers={"Authorization": f"Bearer {_tok(c, 'vi')}"})
    assert r.status_code == 403
    # An operator with the permission runs it.
    r = c.post("/admin/resync", json=body,
               headers={"Authorization": f"Bearer {_tok(c, 'op')}"})
    assert r.status_code == 200 and r.json()["resynced"] == 1
    # Status surfaces the run.
    st = c.get("/admin/resync/status",
               headers={"Authorization": f"Bearer {_tok(c, 'op')}"})
    assert st.status_code == 200
    assert any(v["direction"] == "hot-to-cold" for v in st.json().values())
