from __future__ import annotations

import pytest

from phronexus.errors import ContractValidationError


def _storage_v2(px):
    sc = px.registry.active_storage("trade").model_dump(mode="json", by_alias=True)
    sc["version"] = 2
    return sc


def test_pk_change_rejected(px):
    sc = _storage_v2(px)
    # Model-valid (canonical key {trade_id} still ⊆ PK) but changes addressing.
    sc["primary_key"] = ["trade_id", "counterparty"]
    with pytest.raises(ContractValidationError) as e:
        px.publish_contract(sc)
    assert "primary_key" in str(e.value)


def test_manifest_set_change_rejected(px):
    sc = _storage_v2(px)
    sc["manifest_set"] = "trade_manifest_v2"
    with pytest.raises(ContractValidationError):
        px.publish_contract(sc)


def test_canonical_projection_change_rejected(px):
    sc = _storage_v2(px)
    sc["projections"][0]["set"] = "trade_main_v2"        # moves the canonical set
    with pytest.raises(ContractValidationError):
        px.publish_contract(sc)


def test_compatible_change_allowed(px):
    sc = _storage_v2(px)
    sc["projections"].append(
        {"name": "by_book", "set": "trade_by_book", "key": "{book}:{trade_id}",
         "fields": ["trade_id", "book"]}
    )
    px.publish_contract(sc)
    assert px.registry.active_storage("trade").version == 2


def test_force_overrides_gate(px):
    sc = _storage_v2(px)
    sc["primary_key"] = ["trade_id", "counterparty"]
    px.publish_contract(sc, force=True)                  # explicit override
    assert px.registry.active_storage("trade").primary_key == ["trade_id", "counterparty"]
