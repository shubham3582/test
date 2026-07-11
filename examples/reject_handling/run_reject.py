"""What to do when (syntax) validation fails — four strategies, all config/hook.

    python examples/reject_handling/run_reject.py

Validation runs at the write boundary (JSON Schema = syntax, then DQ). When it
fails the write is rejected. This shows the built-in ways to handle the reject:

  A) do nothing (drop)                 — no dlq_topic (or null://)
  B) whole message -> Kafka reject     — dlq_topic: kafka://<topic>
  C) whole message -> HTTP webhook     — dlq_topic: http://<url>   (POSTed as JSON)
  D) a SPECIFIC message -> anywhere     — a reject hook (on_event) you control

A–C use the state machine's built-in dead-letter: the runner routes every
rejected event to `dlq_topic`, whose URI scheme decides the transport
(`RoutingOutputPublisher`: kafka:// | http(s):// | null://). D uses a hook that
gates syntax up front and emits exactly the message you want.
"""

from __future__ import annotations

import time
from pathlib import Path

from phronexus import Phronexus, Settings
from phronexus.statemachine import InputEvent, MemoryOutputPublisher
from phronexus.statemachine.hooks import ProcessingHook
from phronexus.statemachine.models import OutputEvent

ROOT = Path(__file__).resolve().parents[2]

# A trade that fails JSON Schema — missing required counterparty/notional/ccy/trade_date.
BAD = InputEvent(entity="trade", event_type="TradeBooked", key="BAD-1",
                 payload={"trade_id": "BAD-1"}, event_id="bad-1")


def banner(t: str) -> None:
    print(f"\n=== {t} ===")


def fresh() -> Phronexus:
    settings = Settings(backend="memory")
    settings.observability.log_level = "ERROR"   # set before construction (configures logging)
    px = Phronexus(settings)
    px.load_contract_dir(str(ROOT / "contracts_examples"))
    return px


class SyntaxRejectHook(ProcessingHook):
    """Gate events on JSON Schema (syntax) up front. On failure, publish exactly
    the message you want to a destination you choose, then DROP the event
    (return None) so the machine never processes it."""

    def __init__(self, px, publisher, topic: str, *, whole: bool = False):
        self._px, self._out, self._topic, self._whole = px, publisher, topic, whole

    def on_event(self, event: InputEvent):
        report = self._px.validate(event.entity, event.payload)
        syntax = [e for e in report.errors if e.startswith("schema:")]
        if not syntax:
            return event  # syntax OK — let it proceed (DQ etc. handled normally)
        body = ({"event_id": event.event_id, "entity": event.entity, "payload": event.payload}
                if self._whole else
                {"event_id": event.event_id, "errors": syntax})  # a specific message
        self._out.publish(OutputEvent(topic=self._topic, type="SyntaxRejected", key=event.key,
                                      payload=body, ts=time.time(), cause_event_id=event.event_id))
        return None  # DROP


def main() -> None:
    # First: what a syntax failure looks like.
    banner("validation report for the bad trade")
    px = fresh()
    rep = px.validate("trade", BAD.payload)
    print("ok:", rep.ok, "| errors:", rep.errors)
    px.close()

    # A) do nothing — no dlq_topic: the event is rejected, acked, and dropped.
    banner("A) do nothing (drop) — dlq_topic unset")
    px = fresh(); out = MemoryOutputPublisher()
    px.settings.statemachine.dlq_topic = None
    sm = px.state_machine(output=out)
    r = sm.process(BAD)
    print("status:", r.status, "| dead_letter routed:", sm.dead_letter(BAD, r),
          "| published:", len(out.events))
    px.close()

    # B) whole message -> a Kafka reject topic.
    banner("B) whole message -> Kafka reject topic — dlq_topic: kafka://trade.reject")
    px = fresh(); out = MemoryOutputPublisher()
    px.settings.statemachine.dlq_topic = "kafka://trade.reject"
    sm = px.state_machine(output=out)
    r = sm.process(BAD); sm.dead_letter(BAD, r)
    print("routed to:", out.events[0].topic)
    print("payload:", out.events[0].payload)
    px.close()

    # C) whole message -> an HTTP webhook (same call; only the scheme changes).
    banner("C) whole message -> HTTP webhook — dlq_topic: http://risk.svc/reject")
    px = fresh(); out = MemoryOutputPublisher()
    px.settings.statemachine.dlq_topic = "http://risk.svc/reject"
    sm = px.state_machine(output=out)
    r = sm.process(BAD); sm.dead_letter(BAD, r)
    print("routed to:", out.events[0].topic, "(RoutingOutputPublisher POSTs http:// as JSON)")
    px.close()

    # D) a SPECIFIC message via a reject hook (here: just the schema errors).
    banner("D) specific message via a reject hook (syntax gate + drop)")
    px = fresh(); out = MemoryOutputPublisher()
    hook = SyntaxRejectHook(px, out, "kafka://trade.syntax_reject", whole=False)
    sm = px.state_machine(output=out, hooks=[hook])
    r = sm.process(BAD)
    print("status:", r.status, "(dropped by hook)")
    print("routed to:", out.events[0].topic)
    print("specific payload:", out.events[0].payload)
    px.close()


if __name__ == "__main__":
    main()
