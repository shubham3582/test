from __future__ import annotations

import pytest

from phronexus.contracts.loader import parse_contract
from phronexus.errors import ContractValidationError


def test_storage_contract_requires_single_canonical():
    doc = {
        "kind": "storage",
        "entity": "x",
        "version": 1,
        "primary_key": ["id"],
        "manifest_set": "x_m",
        "projections": [
            {"name": "a", "set": "sa", "key": "{id}", "fields": ["*"]},
            {"name": "b", "set": "sb", "key": "{id}", "fields": ["*"]},
        ],
    }
    with pytest.raises(ContractValidationError):
        parse_contract(doc)


def test_canonical_key_must_be_pk_only():
    doc = {
        "kind": "storage",
        "entity": "x",
        "version": 1,
        "primary_key": ["id"],
        "manifest_set": "x_m",
        "projections": [
            {"name": "a", "set": "sa", "key": "{cpty}:{id}", "fields": ["*"], "canonical": True},
        ],
    }
    with pytest.raises(ContractValidationError):
        parse_contract(doc)


def test_query_pattern_must_reference_searchable_field():
    doc = {
        "kind": "query",
        "entity": "x",
        "version": 1,
        "searchable": [{"field": "a", "index": "string"}],
        "patterns": [{"name": "p", "where": [{"field": "b", "op": "eq", "value": "1"}]}],
    }
    with pytest.raises(ContractValidationError):
        parse_contract(doc)


def test_registry_serves_active_and_versions(px):
    sc = px.registry.active_storage("trade")
    assert sc.version == 1
    assert sc.canonical_projection.name == "main"
    # Publish v2 and confirm the active pointer moves while v1 stays fetchable.
    doc = sc.model_dump(mode="json")
    doc["version"] = 2
    px.publish_contract(doc)
    assert px.registry.active_storage("trade").version == 2
    assert px.registry.get_version("storage:trade:v1").version == 1
