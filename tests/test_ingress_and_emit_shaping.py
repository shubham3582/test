"""Inbound ingress validation + outbound emit shaping.

Both reuse the one versioned/governed JSON-Schema store:
  - ``kind: ingress`` — a JSON Schema per INBOUND event type, checked at the
    state-machine boundary before a transition runs (a failure is rejected and
    the runner dead-letters it).
  - ``EmitSpec.fields/rename/transform`` — an outbound event payload built from
    SPECIFIC fields of the candidate document, then validated against the stream
    schema at produce time.
"""
from __future__ import annotations

import pytest

from phronexus import Phronexus, Settings
from phronexus.statemachine import InputEvent, MemoryOutputPublisher
from phronexus.statemachine.runner import run
from phronexus.statemachine.io import MemoryInputSource

# A tiny self-contained entity: a "widget" with a create->active transition that
# emits a shaped notification.
STORAGE = {"kind": "storage", "entity": "widget", "version": 1, "primary_key": ["id"],
           "manifest_set": "widget_manifest",
           "projections": [{"name": "main", "set": "widget_main", "key": "{id}",
                            "fields": ["*"], "canonical": True}]}
TRANSITION = {
    "kind": "transition", "entity": "widget", "version": 1, "state_field": "status",
    "transitions": [{
        "event": "WidgetCreated", "from": None, "to": "active",
        "emit": [{
            "topic": "kafka://widget.activated", "type": "WidgetActivated",
            # SHAPE the outbound message: only these fields, one renamed, one transformed.
            "fields": ["id", "name", "status"],
            "rename": {"id": "widget_id"},
            "transform": {"name": "upper"},
        }],
    }],
}
STREAM = {"kind": "stream", "entity": "widget", "version": 1, "mode": "enforce", "events": [{
    "type": "WidgetActivated",
    "json_schema": {"type": "object", "required": ["widget_id", "name", "status"],
                    "properties": {"widget_id": {"type": "string"}, "name": {"type": "string"},
                                   "status": {"const": "active"}},
                    "additionalProperties": False},  # strict: extra fields would fail
}]}
INGRESS = {"kind": "ingress", "entity": "widget", "version": 1, "mode": "enforce", "events": [{
    "type": "WidgetCreated",
    "json_schema": {"type": "object", "required": ["id", "name"],
                    "properties": {"id": {"type": "string"}, "name": {"type": "string"},
                                   "secret": {"type": "string"}},
                    "additionalProperties": True},
}]}


@pytest.fixture()
def px():
    s = Settings(backend="memory")
    s.observability.log_level = "ERROR"
    s.audit.enabled = False
    p = Phronexus(s)
    for c in (STORAGE, TRANSITION, STREAM, INGRESS):
        p.publish_contract(c)
    yield p
    p.close()


def _ev(etype, key, payload, eid):
    return InputEvent(entity="widget", event_type=etype, key=key, payload=payload, event_id=eid)


# --- outbound emit shaping ----------------------------------------------

def test_emit_payload_is_shaped_from_specific_fields(px):
    out = MemoryOutputPublisher()
    sm = px.state_machine(output=out)
    r = sm.process(_ev("WidgetCreated", "W-1", {"id": "W-1", "name": "gadget", "secret": "x"}, "e1"))
    assert r.status == "applied"
    ev = out.events[0]
    # Only the selected fields, renamed + transformed; the stream schema (strict)
    # would have REJECTED the full doc (which carries 'secret'/'id').
    assert ev.payload == {"widget_id": "W-1", "name": "GADGET", "status": "active"}
    assert "secret" not in ev.payload and "id" not in ev.payload


def test_default_emit_is_whole_document(px):
    # A second transition with a default (unshaped) emit still sends the full doc.
    px.publish_contract({**TRANSITION, "version": 2, "transitions": [
        {"event": "WidgetCreated", "from": None, "to": "active",
         "emit": [{"topic": "kafka://widget.raw"}]}]})
    out = MemoryOutputPublisher()
    sm = px.state_machine(output=out)
    sm.process(_ev("WidgetCreated", "W-2", {"id": "W-2", "name": "g", "secret": "s"}, "e2"))
    assert out.events[0].payload["secret"] == "s"      # nothing stripped


# --- inbound ingress validation -----------------------------------------

def test_inbound_valid_message_passes(px):
    sm = px.state_machine(output=MemoryOutputPublisher())
    r = sm.process(_ev("WidgetCreated", "W-3", {"id": "W-3", "name": "ok"}, "e3"))
    assert r.status == "applied"


def test_inbound_invalid_message_rejected_before_transition(px):
    sm = px.state_machine(output=MemoryOutputPublisher())
    # Missing required 'name' -> ingress schema fails -> rejected, no state change.
    r = sm.process(_ev("WidgetCreated", "W-4", {"id": "W-4"}, "e4"))
    assert r.status == "rejected"
    assert "inbound schema" in r.reason and "name" in r.reason
    assert px.get("widget", "W-4") is None                 # never written


def test_inbound_reject_is_dead_lettered(px):
    px.settings.statemachine.dlq_topic = "kafka://widget.dlq"
    out = MemoryOutputPublisher()
    sm = px.state_machine(output=out)
    bad = _ev("WidgetCreated", "W-5", {"id": "W-5"}, "e5")   # invalid inbound
    tally = run(px, MemoryInputSource([bad]), sm, max_batches=1)
    assert tally["rejected"] == 1 and tally["dead_lettered"] == 1
    assert any(e.topic == "kafka://widget.dlq" for e in out.events)


def test_ingress_warn_only_does_not_block(px):
    # A warn_only ingress that requires an extra 'region' (which the downstream
    # stream schema doesn't care about) — the violation must not block the write.
    px.publish_contract({"kind": "ingress", "entity": "widget", "version": 2,
                         "mode": "warn_only", "events": [{
                             "type": "WidgetCreated",
                             "json_schema": {"type": "object", "required": ["id", "name", "region"],
                                             "additionalProperties": True}}]})
    sm = px.state_machine(output=MemoryOutputPublisher())
    r = sm.process(_ev("WidgetCreated", "W-6", {"id": "W-6", "name": "ok"}, "e6"))  # no region
    assert r.status == "applied"


def test_validate_inbound_via_library_api(px):
    # Library surface: dry-run inbound validation without processing.
    assert px.validator.validate_inbound("widget", "WidgetCreated", {"id": "W", "name": "n"}).ok
    bad = px.validator.validate_inbound("widget", "WidgetCreated", {"id": "W"})
    assert not bad.ok and any("name" in e for e in bad.errors)
