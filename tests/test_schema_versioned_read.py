"""Versioned JSON Schema for read/write payloads.

Covers the three payoffs of pinning the validation-schema version per document:
  - reproducible parse: a validated read checks the payload against the schema
    version it was WRITTEN under, so schema evolution can't fail an old read;
  - defensive read validation: opt-in ``validate=True`` catches a payload that
    no longer matches its own recorded schema version;
  - consumer interop: the versioned schema is fetchable as a registry surface.
"""
from __future__ import annotations

import pytest

from phronexus import Phronexus, Settings
from phronexus.errors import ValidationError

GOOD = {"trade_id": "T-1", "counterparty": "GS", "notional": 1_000_000.0,
        "ccy": "USD", "trade_date": 20250115, "book": "R"}


def _validation_contract(version: int, extra_required: list[str] | None = None) -> dict:
    schema = {
        "type": "object",
        "required": ["trade_id", "counterparty", "notional", "ccy", "trade_date"]
                    + (extra_required or []),
        "properties": {
            "trade_id": {"type": "string", "minLength": 1},
            "counterparty": {"type": "string"},
            "notional": {"type": "number"},
            "ccy": {"type": "string"},
            "trade_date": {"type": "integer"},
            "book": {"type": "string"},
            "desk": {"type": "string"},
        },
        "additionalProperties": True,
    }
    return {"kind": "validation", "entity": "trade", "version": version,
            "mode": "enforce", "json_schema": schema, "dq_checks": []}


# --- write-side pin + default read is unchanged --------------------------

def test_default_read_is_not_validated(px):
    px.put("trade", GOOD)
    assert px.get("trade", "T-1") == GOOD            # hot path untouched, no schema step


def test_validated_read_of_good_doc_passes(px):
    px.put("trade", GOOD)
    assert px.get("trade", "T-1", validate=True) == GOOD


# --- reproducible parse across schema evolution --------------------------

def test_read_validates_against_pinned_version_not_active(px):
    # Written under v1 (the examples' validation contract).
    px.put("trade", GOOD)
    # Evolve: v2 tightens the schema by requiring a new field the old doc lacks.
    px.publish_contract(_validation_contract(2, extra_required=["desk"]))

    # Validating against the ACTIVE (v2) schema fails...
    assert not px.validate("trade", GOOD).ok
    # ...but a validated READ passes, because it uses the pinned v1 the doc was
    # written under. Schema evolution does not retroactively fail old reads.
    assert px.get("trade", "T-1", validate=True) == GOOD


# --- defensive read: payload no longer matches its recorded schema -------

def test_validated_read_raises_when_payload_drifts_from_pinned_schema(px):
    px.put("trade", GOOD)                                   # pins vver = 1
    # Simulate drift/corruption: force-republish v1 with a stricter body (same
    # version identity) that the already-stored doc no longer satisfies.
    px.publish_contract(_validation_contract(1, extra_required=["desk"]), force=True)

    with pytest.raises(ValidationError):
        px.get("trade", "T-1", validate=True)
    # Unvalidated read still returns the stored payload as-is.
    assert px.get("trade", "T-1") == GOOD


# --- consumer interop: fetch the versioned schema ------------------------

def test_schema_registry_surface(px):
    active = px.schema("trade")
    assert active["entity"] == "trade" and active["version"] == 1
    assert active["json_schema"]["required"]  # the write schema is exposed

    px.publish_contract(_validation_contract(2, extra_required=["desk"]))
    assert px.schema("trade")["version"] == 2                       # active moved
    assert px.schema("trade", version=1)["version"] == 1           # old still fetchable
    assert "desk" in px.schema("trade", version=2)["json_schema"]["required"]


# --- bitemporal entities pin + validate on read too ----------------------

_BT_STORAGE = {
    "kind": "storage", "entity": "price", "version": 1, "primary_key": ["sym"],
    "manifest_set": "price_manifest", "temporal": "bitemporal", "valid_time_field": "as_of",
    "projections": [{"name": "main", "set": "price_main", "key": "{sym}",
                     "fields": ["*"], "canonical": True}],
}


def _bt_validation(version: int, extra_required: list[str] | None = None) -> dict:
    return {"kind": "validation", "entity": "price", "version": version, "mode": "enforce",
            "json_schema": {
                "type": "object",
                "required": ["sym", "as_of", "px"] + (extra_required or []),
                "properties": {"sym": {"type": "string"}, "as_of": {"type": "integer"},
                               "px": {"type": "number"}, "src": {"type": "string"}},
                "additionalProperties": True,
            }, "dq_checks": []}


@pytest.fixture()
def bt_px():
    s = Settings(backend="memory")
    s.observability.log_level = "WARNING"
    s.audit.enabled = False
    px = Phronexus(s)
    px.publish_contract(_BT_STORAGE)
    px.publish_contract(_bt_validation(1))
    yield px
    px.close()


def test_bitemporal_read_validates_against_pinned_version(bt_px):
    bt_px.put("price", {"sym": "AAPL", "as_of": 20250110, "px": 100.0})   # pins vver = 1
    doc = bt_px.get("price", "AAPL", as_of=20250112, validate=True)
    assert doc["px"] == 100.0                                             # good doc passes

    # Evolve to a stricter v2; the old bitemporal version still reads under v1.
    bt_px.publish_contract(_bt_validation(2, extra_required=["src"]))
    assert not bt_px.validate("price", {"sym": "AAPL", "as_of": 20250110, "px": 100.0}).ok
    assert bt_px.get("price", "AAPL", as_of=20250112, validate=True)["px"] == 100.0


def test_bitemporal_validated_read_raises_on_drift(bt_px):
    bt_px.put("price", {"sym": "MSFT", "as_of": 20250110, "px": 50.0})    # pins vver = 1
    # Force-tighten v1 in place: the stored version no longer satisfies its pin.
    bt_px.publish_contract(_bt_validation(1, extra_required=["src"]), force=True)
    with pytest.raises(ValidationError):
        bt_px.get("price", "MSFT", as_of=20250112, validate=True)
    assert bt_px.get("price", "MSFT", as_of=20250112)["px"] == 50.0       # plain read unaffected
