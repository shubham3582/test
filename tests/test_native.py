"""Native Aerospike access: config passthrough + managed-set guard."""

from __future__ import annotations

import pytest

from phronexus.config import AerospikeSettings
from phronexus.errors import ConfigError
from phronexus.kv.aerospike import build_client_config
from phronexus.kv.memory import InMemoryKV
from phronexus.native import managed_sets


# --- native client-config passthrough ------------------------------------

def test_client_config_passthrough_merges_native_options():
    cfg = AerospikeSettings(
        hosts="h1:3000,h2:3001",
        policies={"total_timeout": 200, "max_retries": 4},
        client_config={"rack_id": 7, "use_services_alternate": True,
                       "policies": {"read": {"replica": 1}}},
    )
    c = build_client_config(cfg)
    assert c["hosts"] == [("h1", 3000), ("h2", 3001)]
    # top-level native keys pass through
    assert c["rack_id"] == 7 and c["use_services_alternate"] is True
    # policies merge (both the flat map and a nested client_config['policies'])
    assert c["policies"]["total_timeout"] == 200
    assert c["policies"]["max_retries"] == 4
    assert c["policies"]["read"] == {"replica": 1}


def test_tls_passthrough_shapes_hosts_and_tls():
    cfg = AerospikeSettings(
        hosts="secure:4333", tls_enable=True, tls_cafile="/ca.pem",
        tls_certfile="/c.pem", tls_keyfile="/k.pem", tls_name="cluster.tls",
    )
    c = build_client_config(cfg)
    assert c["tls"]["enable"] is True and c["tls"]["cafile"] == "/ca.pem"
    assert c["hosts"] == [("secure", 4333, "cluster.tls")]


# --- native accessor guards ----------------------------------------------

def test_native_requires_aerospike_backend(px):
    # px fixture is the in-memory backend.
    with pytest.raises(ConfigError):
        px.native_aerospike()


def test_memory_backend_has_no_native_client():
    with pytest.raises(NotImplementedError):
        InMemoryKV().native_client()


def test_managed_sets_cover_framework_and_contract_sets(px):
    managed = managed_sets(px)
    # framework sets
    for s in ("_contracts", "_inv", "_sm_dedup", "_sched_state", "_messages", "_interactions"):
        assert s in managed
    # per-contract manifest + projection sets are managed too
    sc = px.registry.active_storage("trade")
    assert sc.manifest_set in managed
    assert all(p.set in managed for p in sc.projections)


def test_assert_writable_blocks_managed_sets_via_guard(px):
    # Exercise the guard logic without a live cluster: build the facade directly
    # (bypassing the AerospikeKV isinstance check) and wire the managed-set list.
    from phronexus import native as native_mod
    from phronexus.errors import StorageError

    nx = native_mod.NativeAerospike.__new__(native_mod.NativeAerospike)
    nx._px = px
    nx.client = object()
    nx.namespace = "phronexus"
    nx._managed = managed_sets(px)

    sc = px.registry.active_storage("trade")
    # A managed set is refused for writes...
    with pytest.raises(StorageError):
        nx.assert_writable(sc.manifest_set)
    with pytest.raises(StorageError):
        nx.assert_writable("_cf_outbox")
    # ...but a user's own set is fine.
    nx.assert_writable("my_positions")
    assert nx.is_managed(sc.manifest_set) and not nx.is_managed("my_positions")
