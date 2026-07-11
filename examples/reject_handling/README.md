# Handling validation failures (reject routing)

Validation runs at the write boundary: **JSON Schema (syntax)**, then **DQ**. A
failure rejects the write. What happens next is config (or a small hook) — four
common strategies, all shown in [`run_reject.py`](run_reject.py):

```bash
python examples/reject_handling/run_reject.py
```

| Want | How |
|---|---|
| **Do nothing** (drop) | leave `statemachine.dlq_topic` unset (or `null://drop`) |
| **Whole message → Kafka reject topic** | `statemachine.dlq_topic: kafka://trade.reject` |
| **Whole message → HTTP webhook** | `statemachine.dlq_topic: http://risk.svc/reject` (POSTed as JSON) |
| **A specific message anywhere** | a reject **hook** you control (below) |

The first three use the state machine's built-in dead-letter: the runner routes
every rejected event to `dlq_topic`, and `RoutingOutputPublisher` picks the
transport from the URI scheme (`kafka://` · `http(s)://` · `null://`). The
dead-letter payload is the whole message plus the failure `reason`.

```yaml
# config/statemachine.yaml — pick one
dlq_topic: kafka://trade.reject        # B) whole message to Kafka
# dlq_topic: http://risk.svc/reject    # C) whole message POSTed over HTTP
# dlq_topic: null://drop               # A) drop
```

For a **specific** message (a subset, a custom shape, or a syntax-only gate),
add a `ProcessingHook.on_event` that validates up front, publishes exactly what
you want, and returns `None` to drop the event:

```python
class SyntaxRejectHook(ProcessingHook):
    def on_event(self, event):
        rep = px.validate(event.entity, event.payload)
        syntax = [e for e in rep.errors if e.startswith("schema:")]
        if not syntax:
            return event                       # syntax OK — proceed
        out.publish(OutputEvent(topic="kafka://trade.syntax_reject",
            type="SyntaxRejected", key=event.key,
            payload={"event_id": event.event_id, "errors": syntax}, ...))
        return None                            # drop
```

> On the direct write path (`px.put`), a syntax failure raises `ValidationError`
> instead — the caller's `try/except` chooses the same strategies. Set
> `validation.mode: warn_only` to downgrade failures to warnings (write proceeds),
> or `off` to skip validation entirely.
