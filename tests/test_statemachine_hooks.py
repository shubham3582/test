from __future__ import annotations

import httpx
import pytest

from phronexus.errors import TransitionRejected
from phronexus.statemachine import (
    HttpOutputPublisher,
    InputEvent,
    MemoryOutputPublisher,
    NullOutputPublisher,
    ProcessingHook,
    RoutingOutputPublisher,
)
from phronexus.statemachine.models import OutputEvent

BOOK = {"trade_id": "T-1", "counterparty": "GS", "notional": 1_000_000.0, "ccy": "USD", "trade_date": 20250115, "book": "R"}


def _ev(etype, key, payload, eid):
    return InputEvent(entity="trade", event_type=etype, key=key, payload=payload, event_id=eid)


# --- hooks ---------------------------------------------------------------

def test_on_event_enriches_payload(px):
    class Enrich(ProcessingHook):
        def on_event(self, event):
            event.payload = {**event.payload, "desk": "LDN-RATES"}
            return event

    sm = px.state_machine(hooks=[Enrich()])
    sm.process(_ev("TradeBooked", "T-1", BOOK, "e1"))
    assert px.get("trade", "T-1")["desk"] == "LDN-RATES"


def test_on_event_drop_acks_and_skips(px):
    class DropCancels(ProcessingHook):
        def on_event(self, event):
            return None if event.event_type == "TradeBooked" else event

    sm = px.state_machine(hooks=[DropCancels()])
    r = sm.process(_ev("TradeBooked", "T-1", BOOK, "e1"))
    assert r.status == "dropped"
    assert px.get("trade", "T-1") is None                       # nothing written
    assert px.store.get(px.settings.statemachine.dedup_set, "e1") is None  # no marker


def test_on_transition_enriches_before_commit(px):
    class Stamp(ProcessingHook):
        def on_transition(self, ctx):
            ctx.new_doc["risk_weight"] = ctx.new_doc["notional"] * 0.08

    sm = px.state_machine(hooks=[Stamp()])
    sm.process(_ev("TradeBooked", "T-1", BOOK, "e1"))
    assert px.get("trade", "T-1")["risk_weight"] == pytest.approx(80_000.0)


def test_on_transition_can_reject(px):
    class Veto(ProcessingHook):
        def on_transition(self, ctx):
            if ctx.new_doc["counterparty"] == "GS":
                raise TransitionRejected("counterparty blocked")

    sm = px.state_machine(hooks=[Veto()])
    r = sm.process(_ev("TradeBooked", "T-1", BOOK, "e1"))
    assert r.status == "rejected" and "blocked" in r.reason
    assert px.get("trade", "T-1") is None


def test_on_committed_runs_after_commit(px):
    seen = []

    class Audit(ProcessingHook):
        def on_committed(self, event, result):
            seen.append((event.event_id, result.status, result.to_state))

    sm = px.state_machine(hooks=[Audit()])
    sm.process(_ev("TradeBooked", "T-1", BOOK, "e1"))
    assert seen == [("e1", "applied", "booked")]


# --- output topologies ---------------------------------------------------

def test_routing_dispatches_by_scheme():
    kafka, fallback, calls = MemoryOutputPublisher(), MemoryOutputPublisher(), []

    class Rec(NullOutputPublisher):
        def publish(self, event):
            calls.append(event.topic)

    router = RoutingOutputPublisher({"kafka": kafka, "null": Rec()}, default=fallback)

    def emit(topic):
        router.publish(OutputEvent(topic=topic, type="x", key="k", payload={}, ts=0, cause_event_id="c"))

    emit("kafka://a")
    emit("null://drop")
    emit("bare-topic")       # no scheme -> treated as a kafka topic
    emit("unknown://x")      # scheme not routed -> default/fallback
    assert [e.topic for e in kafka.events] == ["kafka://a", "bare-topic"]
    assert calls == ["null://drop"]
    assert [e.topic for e in fallback.events] == ["unknown://x"]


def test_http_output_posts_event():
    posted = {}

    def handler(request: httpx.Request) -> httpx.Response:
        posted["url"] = str(request.url)
        posted["json"] = request.read().decode()
        return httpx.Response(200)

    pub = HttpOutputPublisher()
    pub._client = httpx.Client(transport=httpx.MockTransport(handler))
    pub.publish(OutputEvent(topic="https://risk.svc/hook", type="TradeBooked", key="T-1", payload={"a": 1}, ts=0, cause_event_id="c"))
    assert posted["url"] == "https://risk.svc/hook" and '"a":1' in posted["json"]


def test_http_output_non_2xx_raises_and_keeps_outbox(px):
    def handler(request):
        return httpx.Response(503)

    http = HttpOutputPublisher()
    http._client = httpx.Client(transport=httpx.MockTransport(handler))
    router = RoutingOutputPublisher({"https": http, "null": NullOutputPublisher()},
                                    default=MemoryOutputPublisher())

    # A transition emitting to an http target that fails: state commits, outbox retained.
    px.publish_contract({
        "kind": "transition", "entity": "trade", "version": 2, "state_field": "status",
        "transitions": [{"event": "TradeBooked", "from": None, "to": "booked",
                         "emit": [{"topic": "https://down.svc/hook"}]}],
    })
    sm = px.state_machine(output=router)
    r = sm.process(_ev("TradeBooked", "T-1", BOOK, "e1"))
    assert r.status == "applied"                                  # state committed
    assert px.get("trade", "T-1")["status"] == "booked"
    # publish failed -> event still queued for retry
    assert list(px.store.scan(px.settings.statemachine.outbox_set))


def test_standalone_relay_split(px):
    # With inline_relay off, process() commits but does NOT publish; a separate
    # relay drain does. Insulates the write path from slow brokers.
    from phronexus.statemachine.relay import run_once

    px.settings.statemachine.inline_relay = False
    out = MemoryOutputPublisher()
    sm = px.state_machine(output=out)
    r = sm.process(_ev("TradeBooked", "T-1", BOOK, "e1"))
    assert r.status == "applied"                          # committed
    assert out.events == []                               # not published inline
    assert list(px.store.scan(px.settings.statemachine.outbox_set))  # queued

    published = run_once(sm)                               # the relay drains it
    assert published >= 1 and [e.topic for e in out.events] == ["kafka://trades.booked"]
    assert list(px.store.scan(px.settings.statemachine.outbox_set)) == []


def test_consumer_only_no_output(px):
    # A transition with no emit — consume + process, nothing published.
    px.publish_contract({
        "kind": "transition", "entity": "trade", "version": 2, "state_field": "status",
        "transitions": [{"event": "TradeBooked", "from": None, "to": "booked", "emit": []}],
    })
    out = MemoryOutputPublisher()
    sm = px.state_machine(output=out)
    r = sm.process(_ev("TradeBooked", "T-1", BOOK, "e1"))
    assert r.status == "applied" and r.emitted == []
    assert out.events == []
